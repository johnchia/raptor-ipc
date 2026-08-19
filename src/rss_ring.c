/*
 * rss_ring.c -- SHM ring buffer implementation.
 *
 * Lock-free, single-producer multi-consumer ring buffer over POSIX SHM.
 *
 * Memory layout (single contiguous mmap):
 *
 *   +-------------------+  offset 0
 *   | rss_ring_header_t |  control block (cache-line aligned)
 *   +-------------------+  offset PAGE_SIZE (page-aligned)
 *   | slot[0]           |  rss_ring_slot_t
 *   | slot[1]           |
 *   | ...               |
 *   | slot[N-1]         |
 *   +-------------------+  offset PAGE_SIZE + N * sizeof(slot)
 *   | data region       |  raw frame payload storage (circular)
 *   +-------------------+
 *
 * The data region is circular. Frames are written contiguously; if the
 * remaining space before the end of the region is less than the frame
 * size, the tail is skipped and the frame is written at offset 0.
 * This guarantees contiguous reads.
 */

#include "rss_ipc.h"

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <limits.h>
#include <linux/futex.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

/* Cross-process futex on SHM: FUTEX_WAIT/FUTEX_WAKE without FUTEX_PRIVATE_FLAG.
 * Private flag would use process-local hash — must NOT be set for shared memory. */

/*
 * The timeout cannot be libc's struct timespec on a 32-bit target.
 *
 * Legacy SYS_futex reads a kernel timespec whose width follows the
 * architecture: two 32-bit fields on a 32-bit machine. A libc whose time_t is
 * 32 bits lays struct timespec out identically, so passing it straight through
 * happens to work -- which is what glibc/armhf does. musl makes time_t 64 bits
 * on every target including 32-bit ones, so its struct timespec opens with an
 * 8-byte tv_sec and the kernel reads tv_nsec out of that field's upper half.
 * For any timeout under a second that yields {0, 0}, so the wait returns
 * ETIMEDOUT immediately and a caller pacing itself on the timeout spins
 * instead of sleeping.
 *
 * SYS_futex_time64 is the kernel's other answer to this, but it arrives in 5.1
 * and the SigmaStar targets run 4.9. Converting explicitly works on both libcs
 * and every kernel: these timeouts are relative and seconds-scale, so 32 bits
 * is not a range worth worrying about.
 */
static int futex_wait(uint32_t *addr, uint32_t expected, const struct timespec *timeout)
{
#if __SIZEOF_POINTER__ == 4
    struct {
        int32_t tv_sec;
        int32_t tv_nsec;
    } kts;
    const void *tp = NULL;

    if (timeout) {
        kts.tv_sec = (int32_t)timeout->tv_sec;
        kts.tv_nsec = (int32_t)timeout->tv_nsec;
        tp = &kts;
    }
    return (int)syscall(SYS_futex, addr, FUTEX_WAIT, expected, tp, NULL, 0);
#else
    return (int)syscall(SYS_futex, addr, FUTEX_WAIT, expected, timeout, NULL, 0);
#endif
}

static int futex_wake(uint32_t *addr, int count)
{
    return (int)syscall(SYS_futex, addr, FUTEX_WAKE, count, NULL, NULL, 0);
}

/* Futex operates on the dedicated futex_seq field (uint32_t) in the ring
 * header, updated by the producer alongside write_seq.  This avoids the
 * strict-aliasing violation of casting &write_seq (uint64_t) to uint32_t*
 * and is endianness-independent. */

/* Page size for alignment. */
#ifndef PAGE_SIZE
#define PAGE_SIZE 4096
#endif

#define ALIGN_UP(x, a) (((x) + (a) - 1) & ~((a) - 1))

struct rss_ring {
    rss_ring_header_t *header;
    rss_ring_slot_t *slots;
    uint32_t open_incarnation; /* incarnation at time of open */
    uint32_t own_data_size;    /* producer: data region THIS handle mapped */
    uint32_t own_slot_count;   /* producer: slots THIS handle mapped */
    uint8_t *data;
    size_t total_size;
    int shm_fd;
    bool is_producer;
    char name[64];
    uint8_t *ref_data;        /* consumer: /dev/rmem mmap (NULL if embedded) */
    int ref_fd;               /* consumer: /dev/rmem fd (-1 if not open)     */
    uint8_t ref_open_gen;     /* consumer: header ref_gen this mapping matches */
    uint32_t ref_mapped_size; /* consumer: size THIS mapping was made with */
};

/* Map (or re-map) the refmode backing region per the CURRENT header:
 * named enc SHM first, /dev/rmem fallback. Records the header's ref_gen
 * so the read path can tell when the producer swapped the region. */
static int ring_map_ref(rss_ring_t *ring)
{
    rss_ring_header_t *hdr = ring->header;
    char enc_shm[128];
    snprintf(enc_shm, sizeof(enc_shm), "/rss_enc_%s", ring->name);
    ring->ref_open_gen = atomic_load_explicit(&hdr->ref_gen, memory_order_acquire);
    ring->ref_fd = shm_open(enc_shm, O_RDONLY, 0);
    if (ring->ref_fd < 0) {
        RSS_IPC_TRACE("ring %s: shm_open(%s) failed, trying /dev/rmem", ring->name, enc_shm);
        ring->ref_fd = open("/dev/rmem", O_RDONLY);
        if (ring->ref_fd >= 0)
            ring->ref_data = mmap(NULL, hdr->ref_rmem_size, PROT_READ, MAP_SHARED, ring->ref_fd,
                                  (off_t)hdr->ref_rmem_offset);
        else
            RSS_IPC_WARN("ring %s: refmode: no shm and no /dev/rmem", ring->name);
    } else {
        ring->ref_data = mmap(NULL, hdr->ref_rmem_size, PROT_READ, MAP_SHARED, ring->ref_fd, 0);
    }
    if (!ring->ref_data || ring->ref_data == MAP_FAILED) {
        RSS_IPC_WARN("ring %s: refmode mmap failed: size=%u offset=%u: %s", ring->name,
                     hdr->ref_rmem_size, hdr->ref_rmem_offset, strerror(errno));
        ring->ref_data = NULL;
        if (ring->ref_fd >= 0)
            close(ring->ref_fd);
        ring->ref_fd = -1;
        return -1;
    }
    ring->ref_mapped_size = hdr->ref_rmem_size;
    return 0;
}

static void ring_unmap_ref(rss_ring_t *ring)
{
    if (ring->ref_data)
        munmap(ring->ref_data, ring->ref_mapped_size);
    ring->ref_data = NULL;
    ring->ref_mapped_size = 0;
    if (ring->ref_fd >= 0)
        close(ring->ref_fd);
    ring->ref_fd = -1;
}

/* The producer bumped ref_gen (encoder restart replaced the region):
 * remap before resolving any frame data through the old mapping. On a
 * transient failure the caller reports EOVERFLOW and retries later. */
static int ring_ref_sync(rss_ring_t *ring)
{
    if (!ring->ref_data)
        return 0;
    uint8_t gen = atomic_load_explicit(&ring->header->ref_gen, memory_order_acquire);
    if (gen == ring->ref_open_gen)
        return 0;
    RSS_IPC_WARN("ring %s: ref region replaced (gen %u -> %u), remapping", ring->name,
                 ring->ref_open_gen, gen);
    ring_unmap_ref(ring);
    return ring_map_ref(ring);
}

/*
 * Compute mmap layout offsets:
 *   header:  0 .. PAGE_SIZE-1
 *   slots:   PAGE_SIZE .. PAGE_SIZE + slot_count*sizeof(slot) - 1
 *   data:    PAGE_SIZE + slot_count*sizeof(slot) .. end
 */
static size_t ring_total_size(uint32_t slot_count, uint32_t data_size)
{
    size_t slots_bytes = (size_t)slot_count * sizeof(rss_ring_slot_t);
    return PAGE_SIZE + slots_bytes + data_size;
}

static void ring_set_pointers(rss_ring_t *ring, void *base, uint32_t slot_count)
{
    ring->header = (rss_ring_header_t *)base;
    ring->slots = (rss_ring_slot_t *)((uint8_t *)base + PAGE_SIZE);
    ring->data = (uint8_t *)ring->slots + (size_t)slot_count * sizeof(rss_ring_slot_t);
}

static void make_shm_name(char *buf, size_t bufsz, const char *name)
{
    snprintf(buf, bufsz, "%s%s", RSS_RING_SHM_PREFIX, name);
}

/* ------------------------------------------------------------------ */
/*  Producer API                                                      */
/* ------------------------------------------------------------------ */

rss_ring_t *rss_ring_create(const char *name, uint32_t slot_count, uint32_t data_size)
{
    if (!name || slot_count == 0 || slot_count > RSS_RING_MAX_SLOTS || data_size == 0)
        return NULL;

    /* slot_count must be a power of 2 */
    if ((slot_count & (slot_count - 1)) != 0)
        return NULL;

    rss_ring_t *ring = calloc(1, sizeof(*ring));
    if (!ring)
        return NULL;

    snprintf(ring->name, sizeof(ring->name), "%s", name);
    ring->is_producer = true;
    ring->shm_fd = -1;
    ring->ref_fd = -1;

    char shm_name[128];
    make_shm_name(shm_name, sizeof(shm_name), name);

    ring->total_size = ring_total_size(slot_count, data_size);

    /* Reuse existing SHM inode so consumer mmaps that opened the same
     * name see the new header via the shared page cache. No O_TRUNC
     * (SIGBUS risk on consumers reading during truncate) and no O_EXCL
     * + shm_unlink (new inode orphans existing consumer mmaps).
     * 0666: all daemons run as root on a single-user embedded camera. */
    ring->shm_fd = shm_open(shm_name, O_CREAT | O_RDWR, 0666);
    if (ring->shm_fd < 0)
        goto fail;

    if (ftruncate(ring->shm_fd, (off_t)ring->total_size) < 0)
        goto fail;

    void *base = mmap(NULL, ring->total_size, PROT_READ | PROT_WRITE, MAP_SHARED, ring->shm_fd, 0);
    if (base == MAP_FAILED)
        goto fail;

    ring_set_pointers(ring, base, slot_count);

    /* Initialise header. Magic written LAST with release ordering
     * so consumers see all fields initialized before magic becomes valid.
     * Save incarnation before memset so crash-restart detection works
     * (producer reusing the same SHM inode increments from old value). */
    uint32_t prev_inc = atomic_load_explicit(&ring->header->incarnation, memory_order_relaxed);
    memset(ring->header, 0, PAGE_SIZE);
    atomic_store_explicit(&ring->header->write_seq, 0, memory_order_relaxed);
    atomic_store_explicit(&ring->header->futex_seq, 0, memory_order_relaxed);
    ring->header->slot_count = slot_count;
    ring->header->data_size = data_size;
    atomic_store_explicit(&ring->header->data_head, 0, memory_order_relaxed);
    ring->header->version = RSS_RING_VERSION;
    atomic_store_explicit(&ring->header->incarnation, prev_inc + 1, memory_order_relaxed);
    /* Remember the geometry this handle actually mapped. The header is
     * shared and a later create() may enlarge it; publishing must be
     * bounded by what we mapped, never by what the header now claims. */
    ring->own_data_size = data_size;
    ring->own_slot_count = slot_count;
    ring->open_incarnation = prev_inc + 1;

    /* Initialise all slot sequences to UINT64_MAX (sentinel — never matches
     * a valid read_seq). Prevents phantom reads when a fresh consumer starts
     * at read_seq=0: validation fails, EOVERFLOW syncs to latest write_seq. */
    for (uint32_t i = 0; i < slot_count; i++)
        atomic_store_explicit(&ring->slots[i].seq, UINT64_MAX, memory_order_relaxed);

    /* Write magic last — release fence ensures all above writes are
     * visible to consumers before they see valid magic. */
    atomic_store_explicit(&ring->header->magic, RSS_RING_MAGIC, memory_order_release);

    /* Producer uses futex on futex_seq for consumer notification. */

    return ring;

fail:
    if (ring->shm_fd >= 0) {
        shm_unlink(shm_name);
        close(ring->shm_fd);
    }
    free(ring);
    return NULL;
}

void rss_ring_destroy(rss_ring_t *ring)
{
    if (!ring)
        return;

    /* Wake blocked consumers before unmapping so they don't wait
     * out the full timeout on a destroyed ring. */
    if (ring->header && ring->header != MAP_FAILED) {
        atomic_fetch_add_explicit(&ring->header->futex_seq, 1, memory_order_release);
        futex_wake((uint32_t *)&ring->header->futex_seq, INT_MAX);
        munmap(ring->header, ring->total_size);
    }

    if (ring->shm_fd >= 0) {
        char shm_name[128];
        make_shm_name(shm_name, sizeof(shm_name), ring->name);
        shm_unlink(shm_name);
        close(ring->shm_fd);
    }

    free(ring);
}

int rss_ring_publish_iov(rss_ring_t *ring, const rss_iov_t *iov, uint32_t iov_count,
                         int64_t timestamp, uint16_t nal_type, uint8_t is_key)
{
    if (!ring || !ring->is_producer || !iov || iov_count == 0)
        return -EINVAL;

    /* Compute total length with overflow check */
    uint32_t length = 0;
    for (uint32_t i = 0; i < iov_count; i++) {
        if (__builtin_add_overflow(length, iov[i].length, &length))
            return -EOVERFLOW;
    }

    if (length == 0)
        return -EINVAL;

    rss_ring_header_t *hdr = ring->header;

    /* Someone re-created this ring underneath us: the shm was
     * ftruncated and the header now describes a region this handle
     * never mapped. Publishing on those terms walks off the end of our
     * own mapping -- a wild write ASan caught as a SEGV inside the
     * memcpy below. The producer is superseded; say so. */
    if (atomic_load_explicit(&hdr->incarnation, memory_order_acquire) != ring->open_incarnation)
        return -EPIPE;

    /* Bound by what this handle mapped, not by the shared header. */
    uint32_t data_size = ring->own_data_size ? ring->own_data_size : hdr->data_size;
    uint32_t slot_count = ring->own_slot_count ? ring->own_slot_count : hdr->slot_count;

    /* Frame larger than entire data region — reject. */
    if (length > data_size)
        return -ENOSPC;

    /* Allocate space in the circular data region.
     * If the frame doesn't fit in the remaining tail, skip to offset 0
     * (waste the tail, bounded at one frame per wrap). Frames are NOT
     * split across the wrap boundary — each frame is contiguous.
     * Consumers MUST follow slots in sequence order; data_offset is
     * not monotonically increasing due to wrap-back. */
    uint32_t head = atomic_load_explicit(&hdr->data_head, memory_order_relaxed);
    uint32_t remaining = data_size - head;
    uint32_t offset;

    if (length <= remaining) {
        offset = head;
        atomic_store_explicit(&hdr->data_head, head + length, memory_order_relaxed);
    } else {
        offset = 0;
        atomic_store_explicit(&hdr->data_head, length, memory_order_relaxed);
    }

    /* Copy iov segments contiguously into the data region. */
    uint32_t off = offset;
    for (uint32_t i = 0; i < iov_count; i++) {
        memcpy(ring->data + off, iov[i].data, iov[i].length);
        off += iov[i].length;
    }

    /* Determine the slot and sequence number. */
    uint64_t seq = atomic_load_explicit(&hdr->write_seq, memory_order_relaxed) + 1;
    uint32_t slot_idx = (uint32_t)(seq % slot_count);

    rss_ring_slot_t *slot = &ring->slots[slot_idx];

    /* Invalidate the slot BEFORE overwriting data. A consumer that reads
     * this slot during the memcpy will see UINT64_MAX, fail its validation
     * check, and retry. */
    atomic_store_explicit(&slot->seq, UINT64_MAX, memory_order_relaxed);

    slot->data_offset = offset;
    slot->data_length = length;
    slot->timestamp = timestamp;
    slot->nal_type = nal_type;
    slot->is_key = is_key;
    slot->buf_idx = 0;
    slot->buf_gen = 0;

    /* Write the valid slot sequence AFTER data is fully written. */
    atomic_store_explicit(&slot->seq, seq, memory_order_relaxed);

    /* Publish: release fence then store write_seq. */
    atomic_store_explicit(&hdr->write_seq, seq, memory_order_release);

    /* Update futex word and wake consumers. */
    atomic_store_explicit(&hdr->futex_seq, (uint32_t)seq, memory_order_release);
    futex_wake((uint32_t *)&hdr->futex_seq, INT_MAX);

    return 0;
}

int rss_ring_publish(rss_ring_t *ring, const uint8_t *data, uint32_t length, int64_t timestamp,
                     uint16_t nal_type, uint8_t is_key)
{
    rss_iov_t iov = {.data = data, .length = length};
    return rss_ring_publish_iov(ring, &iov, 1, timestamp, nal_type, is_key);
}

int rss_ring_enable_refmode(rss_ring_t *ring, uint32_t rmem_size, uint32_t rmem_offset,
                            uint8_t buf_count, uint32_t buf_stride)
{
    if (!ring || !ring->is_producer)
        return -EINVAL;
    if (buf_count == 0 || buf_count > RSS_RING_MAX_REF_BUFS || rmem_size == 0)
        return -EINVAL;

    rss_ring_header_t *hdr = ring->header;
    hdr->flags = RSS_RING_FLAG_REFMODE;
    hdr->ref_buf_count = buf_count;
    hdr->ref_rmem_size = rmem_size;
    hdr->ref_rmem_offset = rmem_offset;
    hdr->ref_buf_stride = buf_stride;

    for (uint8_t i = 0; i < RSS_RING_MAX_REF_BUFS; i++)
        atomic_store_explicit(&hdr->ref_buf_gen[i], 0, memory_order_relaxed);

    /* Every (re-)enable moves the region generation: a consumer holding a
     * mapping from before this call must remap before trusting offsets
     * into the region (see ring_ref_sync). Release-ordered after the
     * region fields above so a consumer that observes the new generation
     * also observes the geometry it describes. */
    atomic_store_explicit(&hdr->ref_gen,
                          (uint8_t)(atomic_load_explicit(&hdr->ref_gen, memory_order_relaxed) + 1),
                          memory_order_release);

    /* Re-publish magic with release ordering so consumers see the flags
     * update. Without this, a consumer opening between create() and
     * enable_refmode() could see flags=0 despite magic being valid. */
    atomic_store_explicit(&hdr->magic, RSS_RING_MAGIC, memory_order_release);

    return 0;
}

int rss_ring_publish_ref(rss_ring_t *ring, uint32_t rmem_offset, uint32_t length, int64_t timestamp,
                         uint16_t nal_type, uint8_t is_key, uint8_t buf_idx)
{
    if (!ring || !ring->is_producer || length == 0)
        return -EINVAL;

    rss_ring_header_t *hdr = ring->header;

    if (!(hdr->flags & RSS_RING_FLAG_REFMODE))
        return -EINVAL;
    if (buf_idx >= hdr->ref_buf_count)
        return -EINVAL;

    /* Same supersede check as the embedded path: refmode keeps frame
     * data in rmem, but the slot array still lives in this handle's
     * mapping, and a re-create with more slots would push the index
     * past its end. This is the path devices use. */
    if (atomic_load_explicit(&hdr->incarnation, memory_order_acquire) != ring->open_incarnation)
        return -EPIPE;

    uint32_t slot_count = ring->own_slot_count ? ring->own_slot_count : hdr->slot_count;
    uint64_t seq = atomic_load_explicit(&hdr->write_seq, memory_order_relaxed) + 1;
    uint32_t slot_idx = (uint32_t)(seq % slot_count);
    rss_ring_slot_t *slot = &ring->slots[slot_idx];

    /* Bump generation for this buffer — invalidates any in-progress consumer
     * reads of older frames in the same encoder buffer. */
    uint32_t gen =
        atomic_fetch_add_explicit(&hdr->ref_buf_gen[buf_idx], 1, memory_order_release) + 1;

    atomic_store_explicit(&slot->seq, UINT64_MAX, memory_order_relaxed);

    slot->data_offset = rmem_offset;
    slot->data_length = length;
    slot->timestamp = timestamp;
    slot->nal_type = nal_type;
    slot->is_key = is_key;
    slot->buf_idx = buf_idx;
    slot->buf_gen = gen;

    atomic_store_explicit(&slot->seq, seq, memory_order_relaxed);
    atomic_store_explicit(&hdr->write_seq, seq, memory_order_release);
    atomic_store_explicit(&hdr->futex_seq, (uint32_t)seq, memory_order_release);
    futex_wake((uint32_t *)&hdr->futex_seq, INT_MAX);

    return 0;
}

void rss_ring_set_stream_info(rss_ring_t *ring, uint32_t stream_id, uint32_t codec, uint32_t width,
                              uint32_t height, uint32_t fps_num, uint32_t fps_den, uint8_t profile,
                              uint8_t level)
{
    if (!ring || !ring->is_producer)
        return;

    rss_ring_header_t *h = ring->header;
    uint16_t gen = atomic_load_explicit(&h->info_gen, memory_order_relaxed);

    /* Same seqlock shape as set_utc below: this cluster is rewritten in
     * place across an encoder restart (the ring itself is reused), and a
     * consumer polling it live must never see the new width beside the
     * old height. */
    atomic_store_explicit(&h->info_gen, (uint16_t)(gen + 1), memory_order_relaxed);
    atomic_thread_fence(memory_order_release);
    h->stream_id = stream_id;
    h->codec = codec;
    h->width = width;
    h->height = height;
    h->fps_num = fps_num;
    h->fps_den = fps_den;
    h->profile = profile;
    h->level = level;
    atomic_store_explicit(&h->info_gen, (uint16_t)(gen + 2), memory_order_release);
}

int rss_ring_get_stream_info(rss_ring_t *ring, rss_stream_info_t *out)
{
    if (!ring || !out)
        return -EINVAL;

    const rss_ring_header_t *h = ring->header;

    for (int retry = 0; retry < 8; retry++) {
        uint16_t g1 = atomic_load_explicit(&h->info_gen, memory_order_acquire);
        if (g1 & 1)
            continue;

        rss_stream_info_t v = {
            .stream_id = h->stream_id,
            .codec = h->codec,
            .width = h->width,
            .height = h->height,
            .fps_num = h->fps_num,
            .fps_den = h->fps_den,
            .profile = h->profile,
            .level = h->level,
        };

        atomic_thread_fence(memory_order_acquire);
        uint16_t g2 = atomic_load_explicit(&h->info_gen, memory_order_relaxed);
        if (g1 != g2)
            continue;

        *out = v;
        return 0;
    }
    return -EAGAIN;
}

void rss_ring_set_utc(rss_ring_t *ring, int64_t offset_us, uint8_t status)
{
    if (!ring || !ring->is_producer)
        return;

    rss_ring_header_t *h = ring->header;
    uint32_t gen = atomic_load_explicit(&h->utc_gen, memory_order_relaxed);

    /* Linux-style seqlock write: odd marks in-progress. The release fence
     * after the odd store keeps the data writes from moving above it; the
     * release store of the even value keeps them from moving below. */
    atomic_store_explicit(&h->utc_gen, gen + 1, memory_order_relaxed);
    atomic_thread_fence(memory_order_release);
    h->utc_offset_us = offset_us;
    h->utc_status = status;
    atomic_store_explicit(&h->utc_gen, gen + 2, memory_order_release);
}

int rss_ring_get_utc(rss_ring_t *ring, int64_t *offset_us, uint8_t *status)
{
    if (!ring || !offset_us)
        return -EINVAL;

    const rss_ring_header_t *h = ring->header;

    for (int retry = 0; retry < 8; retry++) {
        uint32_t g1 = atomic_load_explicit(&h->utc_gen, memory_order_acquire);
        if (g1 & 1)
            continue;

        int64_t off = h->utc_offset_us;
        uint8_t st = h->utc_status;

        atomic_thread_fence(memory_order_acquire);
        uint32_t g2 = atomic_load_explicit(&h->utc_gen, memory_order_relaxed);
        if (g1 != g2)
            continue;

        if (off == 0)
            return -ENOENT;
        *offset_us = off;
        if (status)
            *status = st;
        return 0;
    }

    return -EAGAIN;
}

/* ------------------------------------------------------------------ */
/*  Consumer API                                                      */
/* ------------------------------------------------------------------ */

rss_ring_t *rss_ring_open(const char *name)
{
    if (!name)
        return NULL;

    rss_ring_t *ring = calloc(1, sizeof(*ring));
    if (!ring)
        return NULL;

    snprintf(ring->name, sizeof(ring->name), "%s", name);
    ring->is_producer = false;
    ring->shm_fd = -1;
    ring->ref_fd = -1;

    char shm_name[128];
    make_shm_name(shm_name, sizeof(shm_name), name);

    /* O_RDWR needed for consumer IDR request (atomic write to header) */
    ring->shm_fd = shm_open(shm_name, O_RDWR, 0);
    if (ring->shm_fd < 0) {
        RSS_IPC_TRACE("ring_open %s: shm_open(%s): %s", name, shm_name, strerror(errno));
        goto fail;
    }

    /* Stat to get the total size. */
    struct stat st;
    if (fstat(ring->shm_fd, &st) < 0) {
        RSS_IPC_ERROR("ring_open %s: fstat: %s", name, strerror(errno));
        goto fail;
    }

    ring->total_size = (size_t)st.st_size;

    /* PROT_WRITE needed for IDR request flag (atomic store to header) */
    void *base = mmap(NULL, ring->total_size, PROT_READ | PROT_WRITE, MAP_SHARED, ring->shm_fd, 0);
    if (base == MAP_FAILED) {
        RSS_IPC_ERROR("ring_open %s: mmap(%zu): %s", name, ring->total_size, strerror(errno));
        goto fail;
    }

    /* Peek at the header — acquire-load magic to ensure all header
     * fields are visible (pairs with producer's release store). */
    rss_ring_header_t *hdr = (rss_ring_header_t *)base;
    uint32_t m = atomic_load_explicit(&hdr->magic, memory_order_acquire);
    if (m != RSS_RING_MAGIC || hdr->version != RSS_RING_VERSION) {
        RSS_IPC_WARN("ring_open %s: magic/version mismatch: magic=0x%08x (expect 0x%08x) "
                     "version=%u (expect %u)",
                     name, m, RSS_RING_MAGIC, hdr->version, RSS_RING_VERSION);
        munmap(base, ring->total_size);
        goto fail;
    }

    /* Validate header fields before using them in pointer arithmetic */
    uint32_t sc = hdr->slot_count;
    uint32_t ds = hdr->data_size;
    if (sc == 0 || sc > RSS_RING_MAX_SLOTS || (sc & (sc - 1)) != 0 || ds == 0 ||
        ring_total_size(sc, ds) > ring->total_size) {
        RSS_IPC_WARN("ring_open %s: validation failed: slots=%u data=%u total=%zu (file=%zu)", name,
                     sc, ds, ring_total_size(sc, ds), ring->total_size);
        munmap(base, ring->total_size);
        goto fail;
    }

    ring_set_pointers(ring, base, sc);
    ring->open_incarnation = atomic_load_explicit(&hdr->incarnation, memory_order_relaxed);

    /* Reference mode: frame data in external shared memory.
     * Try named POSIX SHM first (universal, works on all SoCs).
     * Fall back to /dev/rmem for T31/T40/T41 backward compat. */
    if (hdr->flags & RSS_RING_FLAG_REFMODE) {
        if (ring_map_ref(ring) != 0) {
            munmap(base, ring->total_size);
            goto fail;
        }
    }

    return ring;

fail:
    if (ring->shm_fd >= 0)
        close(ring->shm_fd);
    free(ring);
    return NULL;
}

void rss_ring_close(rss_ring_t *ring)
{
    if (!ring)
        return;

    ring_unmap_ref(ring);

    if (ring->header && ring->header != MAP_FAILED)
        munmap(ring->header, ring->total_size);

    if (ring->shm_fd >= 0)
        close(ring->shm_fd);

    free(ring);
}

int rss_ring_read(rss_ring_t *ring, uint64_t *read_seq, uint8_t *dest, uint32_t dest_size,
                  uint32_t *length, rss_ring_slot_t *meta)
{
    if (!ring || !read_seq || !dest || !length)
        return -EINVAL;

    rss_ring_header_t *hdr = ring->header;

    /* Check incarnation — if the producer recreated the ring, our
     * state (read_seq, mmap) is stale. Consumer must re-open. */
    uint32_t inc = atomic_load_explicit(&hdr->incarnation, memory_order_acquire);
    if (inc != ring->open_incarnation)
        return RSS_EOVERFLOW;

    uint32_t slot_count = hdr->slot_count;

    /* Load write_seq with acquire -- pairs with the producer's release
     * store, ensuring all slot and data writes are visible. */
    uint64_t wseq = atomic_load_explicit(&hdr->write_seq, memory_order_acquire);

    /* Sequences are 1-based and write_seq is release-stored after the
     * slot is fully written, so seq == write_seq is complete and safe
     * to read. Excluding it made every consumer run one frame behind
     * and lose the final frame of a finite stream. */
    if (wseq == 0 || *read_seq > wseq)
        return -EAGAIN;

    /* Check for overflow: consumer fell behind by >= slot_count frames. */
    if (wseq - *read_seq >= slot_count) {
        *read_seq = wseq;
        return RSS_EOVERFLOW;
    }

    /* Normal read: consume the next frame at read_seq. */
    uint32_t idx = (uint32_t)(*read_seq % slot_count);
    const rss_ring_slot_t *slot = &ring->slots[idx];

    /* Validate that the slot's sequence matches what we expect. */
    uint64_t slot_seq = atomic_load_explicit((_Atomic uint64_t *)&slot->seq, memory_order_acquire);
    if (slot_seq != *read_seq) {
        *read_seq = wseq;
        return RSS_EOVERFLOW;
    }

    /* Read slot metadata before copy (producer could recycle after). */
    uint32_t data_offset = slot->data_offset;
    uint32_t data_length = slot->data_length;

    if (data_length > dest_size) {
        (*read_seq)++;
        *length = data_length;
        return -ENOSPC;
    }

    /* The producer may have replaced the ref region (encoder restart on a
     * reused ring); resolving through the old mapping returns frozen bytes
     * under fresh metadata. */
    if (ring_ref_sync(ring) != 0) {
        *read_seq = wseq;
        return RSS_EOVERFLOW;
    }

    /* Bounds check: ensure offset+length doesn't exceed the backing region. */
    uint32_t region_size = ring->ref_data ? hdr->ref_rmem_size : hdr->data_size;
    if (data_offset > region_size || data_length > region_size - data_offset) {
        *read_seq = wseq;
        return RSS_EOVERFLOW;
    }

    const uint8_t *src = ring->ref_data ? ring->ref_data + data_offset : ring->data + data_offset;
    memcpy(dest, src, data_length);

    if (meta)
        *meta = *slot;

    *length = data_length;

    /* Re-validate AFTER copy: if the producer recycled this slot during
     * our memcpy, the data we copied may be corrupt.
     *
     * ABA hazard note: producer wrapping by exactly N * slot_count during
     * a memcpy changes recheck != slot_seq, so it IS detected. */
    uint64_t recheck = atomic_load_explicit((_Atomic uint64_t *)&slot->seq, memory_order_acquire);
    if (recheck != slot_seq) {
        *read_seq = wseq;
        return RSS_EOVERFLOW;
    }

    /* Refmode: also validate that the encoder buffer wasn't reused during
     * the copy. The producer increments ref_buf_gen[buf_idx] before each
     * new publish to the same buffer. */
    if (ring->ref_data) {
        uint8_t bi = slot->buf_idx;
        if (bi >= RSS_RING_MAX_REF_BUFS) {
            *read_seq = wseq;
            return RSS_EOVERFLOW;
        }
        uint32_t gen = slot->buf_gen;
        uint32_t cur_gen = atomic_load_explicit(&hdr->ref_buf_gen[bi], memory_order_acquire);
        if (cur_gen != gen) {
            *read_seq = wseq;
            return RSS_EOVERFLOW;
        }
    }

    (*read_seq)++;
    return 0;
}

int rss_ring_peek(rss_ring_t *ring, uint64_t *read_seq, const uint8_t **data_ptr, uint32_t *length,
                  rss_ring_slot_t *meta)
{
    if (!ring || !read_seq || !data_ptr || !length)
        return -EINVAL;

    rss_ring_header_t *hdr = ring->header;

    uint32_t inc = atomic_load_explicit(&hdr->incarnation, memory_order_acquire);
    if (inc != ring->open_incarnation)
        return RSS_EOVERFLOW;

    uint32_t slot_count = hdr->slot_count;
    uint64_t wseq = atomic_load_explicit(&hdr->write_seq, memory_order_acquire);

    /* Sequences are 1-based and write_seq is release-stored after the
     * slot is fully written, so seq == write_seq is complete and safe
     * to read. Excluding it made every consumer run one frame behind
     * and lose the final frame of a finite stream. */
    if (wseq == 0 || *read_seq > wseq)
        return -EAGAIN;

    if (wseq - *read_seq >= slot_count) {
        *read_seq = wseq;
        return RSS_EOVERFLOW;
    }

    uint32_t idx = (uint32_t)(*read_seq % slot_count);
    const rss_ring_slot_t *slot = &ring->slots[idx];

    uint64_t slot_seq = atomic_load_explicit((_Atomic uint64_t *)&slot->seq, memory_order_acquire);
    if (slot_seq != *read_seq) {
        *read_seq = wseq;
        return RSS_EOVERFLOW;
    }

    uint32_t data_offset = slot->data_offset;
    uint32_t data_length = slot->data_length;

    /* Same region-replacement guard as the copying read. */
    if (ring_ref_sync(ring) != 0) {
        *read_seq = wseq;
        return RSS_EOVERFLOW;
    }

    uint32_t region_size = ring->ref_data ? hdr->ref_rmem_size : hdr->data_size;
    if (data_offset > region_size || data_length > region_size - data_offset) {
        *read_seq = wseq;
        return RSS_EOVERFLOW;
    }

    *data_ptr = ring->ref_data ? ring->ref_data + data_offset : ring->data + data_offset;
    *length = data_length;

    if (meta)
        *meta = *slot;

    (*read_seq)++;
    return 0;
}

int rss_ring_peek_done(rss_ring_t *ring, const rss_ring_slot_t *meta)
{
    if (!ring || !meta)
        return -EINVAL;

    rss_ring_header_t *hdr = ring->header;

    /* For refmode: check that the encoder buffer wasn't reused */
    if (ring->ref_data) {
        uint8_t bi = meta->buf_idx;
        if (bi >= RSS_RING_MAX_REF_BUFS)
            return RSS_EOVERFLOW;
        uint32_t cur_gen = atomic_load_explicit(&hdr->ref_buf_gen[bi], memory_order_acquire);
        if (cur_gen != meta->buf_gen)
            return RSS_EOVERFLOW;
    }

    /* For embedded: check that the slot wasn't recycled */
    uint32_t slot_count = hdr->slot_count;
    uint32_t idx = (uint32_t)(meta->seq % slot_count);
    uint64_t slot_seq =
        atomic_load_explicit((_Atomic uint64_t *)&ring->slots[idx].seq, memory_order_acquire);
    if (slot_seq != meta->seq)
        return RSS_EOVERFLOW;

    return 0;
}

/* A producer restart creates a NEW shm file at the same name; the old
 * mapping stays valid but frozen, so a consumer polling it sees
 * nothing change -- and never learns the producer was reborn. Compare
 * the mapped file's identity against the name's current file: a
 * mismatch or a missing file means this handle is stale and the
 * consumer must close and reopen. */
bool rss_ring_stale(rss_ring_t *ring)
{
    if (!ring || ring->shm_fd < 0)
        return false;

    struct stat mapped;
    if (fstat(ring->shm_fd, &mapped) != 0)
        return true;

    char shm_name[128];
    make_shm_name(shm_name, sizeof(shm_name), ring->name);
    int fd = shm_open(shm_name, O_RDONLY, 0);
    if (fd < 0)
        return true;

    struct stat current;
    bool same = fstat(fd, &current) == 0 && current.st_ino == mapped.st_ino &&
                current.st_dev == mapped.st_dev;
    close(fd);
    return !same;
}

int rss_ring_wait(rss_ring_t *ring, uint32_t timeout_ms)
{
    if (!ring)
        return -EINVAL;

    rss_ring_header_t *hdr = ring->header;

    /* Wait on the dedicated futex_seq field (updated by producer on publish). */
    uint32_t expected = atomic_load_explicit(&hdr->futex_seq, memory_order_acquire);

    struct timespec ts = {.tv_sec = timeout_ms / 1000, .tv_nsec = (timeout_ms % 1000) * 1000000L};

    int ret = futex_wait((uint32_t *)&hdr->futex_seq, expected, &ts);
    if (ret < 0) {
        if (errno == ETIMEDOUT)
            return -ETIMEDOUT;
        if (errno == EFAULT || errno == EINVAL)
            return -errno;
        /* EAGAIN (value changed) and EINTR (signal) are both "check now" */
    }
    return 0;
}

const rss_ring_header_t *rss_ring_get_header(rss_ring_t *ring)
{
    return ring ? ring->header : NULL;
}

uint32_t rss_ring_max_frame_size(rss_ring_t *ring)
{
    if (!ring || !ring->header)
        return 0;
    const rss_ring_header_t *hdr = ring->header;
    if (hdr->flags & RSS_RING_FLAG_REFMODE)
        return hdr->ref_buf_stride ? hdr->ref_buf_stride : 262144;
    return hdr->data_size;
}

void rss_ring_request_idr(rss_ring_t *ring)
{
    if (ring && ring->header)
        atomic_store_explicit(&ring->header->idr_request, 1, memory_order_relaxed);
}

int rss_ring_check_idr(rss_ring_t *ring)
{
    if (!ring || !ring->header)
        return 0;
    /* Atomic exchange: read and clear in one operation */
    return atomic_exchange_explicit(&ring->header->idr_request, 0, memory_order_relaxed);
}

void rss_ring_acquire(rss_ring_t *ring)
{
    if (!ring || !ring->header)
        return;

    /* Register PID first — only bump reader_count if we got a slot.
     * Without a slot, reap_dead_readers can't track us and would
     * reset reader_count, causing underflow on our later release. */
    uint32_t pid = (uint32_t)getpid();
    bool registered = false;
    for (int i = 0; i < RSS_RING_MAX_READERS; i++) {
        uint32_t expected = 0;
        if (atomic_compare_exchange_strong_explicit(&ring->header->reader_pids[i], &expected, pid,
                                                    memory_order_relaxed, memory_order_relaxed)) {
            registered = true;
            break;
        }
    }
    if (registered)
        atomic_fetch_add_explicit(&ring->header->reader_count, 1, memory_order_relaxed);
}

void rss_ring_release(rss_ring_t *ring)
{
    if (!ring || !ring->header)
        return;

    /* Unregister PID — only decrement reader_count if we were registered.
     * Symmetric with acquire: no slot → no count bump → no count sub. */
    uint32_t pid = (uint32_t)getpid();
    for (int i = 0; i < RSS_RING_MAX_READERS; i++) {
        uint32_t expected = pid;
        if (atomic_compare_exchange_strong_explicit(&ring->header->reader_pids[i], &expected, 0,
                                                    memory_order_relaxed, memory_order_relaxed)) {
            atomic_fetch_sub_explicit(&ring->header->reader_count, 1, memory_order_release);
            break;
        }
    }
}

uint32_t rss_ring_reader_count(rss_ring_t *ring)
{
    if (!ring || !ring->header)
        return 0;
    return atomic_load_explicit(&ring->header->reader_count, memory_order_relaxed);
}

uint32_t rss_ring_reap_dead_readers(rss_ring_t *ring)
{
    if (!ring || !ring->header)
        return 0;

    uint32_t reaped = 0;
    uint32_t live = 0;
    for (int i = 0; i < RSS_RING_MAX_READERS; i++) {
        uint32_t pid = atomic_load_explicit(&ring->header->reader_pids[i], memory_order_relaxed);
        if (pid == 0)
            continue;
        if (kill((pid_t)pid, 0) == -1 && errno == ESRCH) {
            /* Process is dead — clear slot.
             * Note: if the PID was recycled to an unrelated process, kill()
             * returns success and the slot stays occupied until that process
             * also exits.  Acceptable on embedded with few processes and
             * pid_max=32768 — recycling to the same PID is very unlikely. */
            atomic_store_explicit(&ring->header->reader_pids[i], 0, memory_order_relaxed);
            reaped++;
        } else {
            live++;
        }
    }

    /* Reconcile: reset reader_count to match live PIDs.
     * Handles orphaned counts from unclean shutdowns. */
    uint32_t count = atomic_load_explicit(&ring->header->reader_count, memory_order_relaxed);
    if (count != live)
        atomic_store_explicit(&ring->header->reader_count, live, memory_order_relaxed);

    return reaped + (count > live ? count - live : 0);
}

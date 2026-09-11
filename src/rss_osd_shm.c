/*
 * rss_osd_shm.c -- OSD double-buffered SHM implementation.
 *
 * Double-buffered BGRA bitmap transport for OSD overlays.
 * ROD renders into the inactive buffer, atomically swaps, and
 * signals RVD via eventfd. RVD reads the active buffer and
 * pushes it to the HAL OSD region.
 *
 * Memory layout (single contiguous mmap):
 *
 *   +-------------------+  offset 0
 *   | rss_osd_header_t  |  control block
 *   +-------------------+  offset PAGE_SIZE (page-aligned)
 *   | buf[0]            |  BGRA bitmap (stride * height bytes)
 *   +-------------------+
 *   | buf[1]            |  BGRA bitmap (stride * height bytes)
 *   +-------------------+
 */

#include "rss_ipc.h"

#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#ifndef PAGE_SIZE
#define PAGE_SIZE 4096
#endif

#define RSS_OSD_MAGIC 0x52534F44 /* "RSOD" */
#define RSS_OSD_VERSION 1

#define RSS_OSD_SHM_PREFIX "/rss_osd_"

typedef struct {
    uint32_t width;
    uint32_t height;
    uint32_t stride;        /* bytes per row (width * 4 for BGRA) */
    uint32_t buf_size;      /* single buffer size = stride * height */
    _Atomic int active_buf; /* 0 or 1: which buffer consumer reads */
    _Atomic int dirty;      /* 1 = active_buf has new data */
    _Atomic uint32_t magic; /* written last with release ordering */
    uint32_t version;
    uint32_t _reserved;
} rss_osd_header_t;

struct rss_osd_shm {
    rss_osd_header_t *header;
    uint8_t *buf[2];
    size_t total_size;
    int shm_fd;
    int event_fd;
    bool is_producer;
    char name[64];
};

static size_t osd_total_size(uint32_t buf_size)
{
    return PAGE_SIZE + (size_t)buf_size * 2;
}

static void osd_set_pointers(rss_osd_shm_t *shm, void *base, uint32_t buf_size)
{
    shm->header = (rss_osd_header_t *)base;
    shm->buf[0] = (uint8_t *)base + PAGE_SIZE;
    shm->buf[1] = shm->buf[0] + buf_size;
}

static void make_shm_name(char *buf, size_t bufsz, const char *name)
{
    snprintf(buf, bufsz, "%s%s", RSS_OSD_SHM_PREFIX, name);
}

/* ------------------------------------------------------------------ */
/*  Producer API (ROD)                                                */
/* ------------------------------------------------------------------ */

rss_osd_shm_t *rss_osd_create(const char *name, uint32_t width, uint32_t height)
{
    if (!name || width == 0 || height == 0)
        return NULL;

    /* Guard against integer overflow in stride (width*4) and
     * buf_size (stride*height). 8192x8192 is far beyond any OSD
     * use case and keeps both products within uint32_t range. */
    if (width > 8192 || height > 8192)
        return NULL;

    rss_osd_shm_t *shm = calloc(1, sizeof(*shm));
    if (!shm)
        return NULL;

    snprintf(shm->name, sizeof(shm->name), "%s", name);
    shm->is_producer = true;
    shm->shm_fd = -1;
    shm->event_fd = -1;

    uint32_t stride = width * 4; /* BGRA */
    uint32_t buf_size = stride * height;

    shm->total_size = osd_total_size(buf_size);

    char shm_name[128];
    make_shm_name(shm_name, sizeof(shm_name), name);

    /* 0666: single-user embedded camera; see rss_ring.c for rationale. */
    shm->shm_fd = shm_open(shm_name, O_CREAT | O_RDWR | O_TRUNC, 0666);
    if (shm->shm_fd < 0) {
        RSS_IPC_ERROR("osd_create %s: shm_open: %s", name, strerror(errno));
        goto fail;
    }

    if (ftruncate(shm->shm_fd, (off_t)shm->total_size) < 0) {
        RSS_IPC_ERROR("osd_create %s: ftruncate: %s", name, strerror(errno));
        goto fail;
    }

    void *base = mmap(NULL, shm->total_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm->shm_fd, 0);
    if (base == MAP_FAILED) {
        RSS_IPC_ERROR("osd_create %s: mmap: %s", name, strerror(errno));
        goto fail;
    }

    osd_set_pointers(shm, base, buf_size);

    /* Initialise header. */
    memset(shm->header, 0, PAGE_SIZE);
    shm->header->width = width;
    shm->header->height = height;
    shm->header->stride = stride;
    shm->header->buf_size = buf_size;
    atomic_store_explicit(&shm->header->active_buf, 0, memory_order_relaxed);
    atomic_store_explicit(&shm->header->dirty, 0, memory_order_relaxed);
    shm->header->version = RSS_OSD_VERSION;
    shm->header->_reserved = 0;

    /* Write magic LAST with release — consumer acquire-loads it to
     * ensure all header fields are visible (same pattern as ring). */
    atomic_store_explicit(&shm->header->magic, RSS_OSD_MAGIC, memory_order_release);

    /* Clear both buffers. */
    memset(shm->buf[0], 0, buf_size);
    memset(shm->buf[1], 0, buf_size);

    /* Create eventfd for consumer notification. */
    shm->event_fd = eventfd(0, EFD_NONBLOCK);
    if (shm->event_fd < 0) {
        RSS_IPC_ERROR("osd_create %s: eventfd: %s", name, strerror(errno));
        goto fail;
    }

    return shm;

fail:
    if (shm->header && shm->header != MAP_FAILED)
        munmap(shm->header, shm->total_size);
    if (shm->shm_fd >= 0) {
        shm_unlink(shm_name);
        close(shm->shm_fd);
    }
    free(shm);
    return NULL;
}

void rss_osd_destroy(rss_osd_shm_t *shm)
{
    if (!shm)
        return;

    if (shm->header && shm->header != (void *)MAP_FAILED)
        munmap(shm->header, shm->total_size);

    if (shm->event_fd >= 0)
        close(shm->event_fd);

    if (shm->shm_fd >= 0) {
        char shm_name[128];
        make_shm_name(shm_name, sizeof(shm_name), shm->name);
        shm_unlink(shm_name);
        close(shm->shm_fd);
    }

    free(shm);
}

uint8_t *rss_osd_get_draw_buffer(rss_osd_shm_t *shm)
{
    if (!shm || !shm->is_producer)
        return NULL;

    /* Return the inactive buffer: the one the consumer is NOT reading. */
    int active = atomic_load_explicit(&shm->header->active_buf, memory_order_relaxed);
    return shm->buf[1 - active];
}

void rss_osd_publish(rss_osd_shm_t *shm)
{
    if (!shm || !shm->is_producer)
        return;

    /* Swap: the buffer we just drew into becomes the active one. */
    int active = atomic_load_explicit(&shm->header->active_buf, memory_order_relaxed);
    int new_active = 1 - active;

    /* Two stores with release ordering. The consumer acquires on dirty,
     * which transitively publishes the active_buf store (sequenced-before
     * in program order). C11 guarantees: if the consumer sees dirty=1
     * via acquire, all prior stores (including active_buf) are visible.
     *
     * Double-buffer note: if the producer publishes twice before the
     * consumer reads, it draws into the buffer the consumer may be
     * reading — causing a single frame of OSD tearing. This is inherent
     * to double-buffering; acceptable for OSD overlays at 2-5 fps. */
    atomic_store_explicit(&shm->header->active_buf, new_active, memory_order_release);
    atomic_store_explicit(&shm->header->dirty, 1, memory_order_release);

    /* Signal the consumer via eventfd. */
    uint64_t val = 1;
    ssize_t r;
    do {
        r = write(shm->event_fd, &val, sizeof(val));
    } while (r < 0 && errno == EINTR);
}

/* ------------------------------------------------------------------ */
/*  Consumer API (RVD)                                                */
/* ------------------------------------------------------------------ */

rss_osd_shm_t *rss_osd_open(const char *name)
{
    if (!name)
        return NULL;

    rss_osd_shm_t *shm = calloc(1, sizeof(*shm));
    if (!shm)
        return NULL;

    snprintf(shm->name, sizeof(shm->name), "%s", name);
    shm->is_producer = false;
    shm->shm_fd = -1;
    shm->event_fd = -1;

    char shm_name[128];
    make_shm_name(shm_name, sizeof(shm_name), name);

    shm->shm_fd = shm_open(shm_name, O_RDWR, 0);
    if (shm->shm_fd < 0) {
        RSS_IPC_DEBUG("osd_open %s: shm_open: %s", name, strerror(errno));
        goto fail;
    }

    struct stat st;
    if (fstat(shm->shm_fd, &st) < 0) {
        RSS_IPC_ERROR("osd_open %s: fstat: %s", name, strerror(errno));
        goto fail;
    }

    shm->total_size = (size_t)st.st_size;

    /* Consumer needs PROT_WRITE for clear_dirty (atomic_store on dirty flag) */
    void *base = mmap(NULL, shm->total_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm->shm_fd, 0);
    if (base == MAP_FAILED) {
        RSS_IPC_ERROR("osd_open %s: mmap: %s", name, strerror(errno));
        goto fail;
    }

    rss_osd_header_t *hdr = (rss_osd_header_t *)base;
    uint32_t m = atomic_load_explicit(&hdr->magic, memory_order_acquire);
    if (m != RSS_OSD_MAGIC || hdr->version != RSS_OSD_VERSION) {
        RSS_IPC_WARN("osd_open %s: magic/version mismatch", name);
        munmap(base, shm->total_size);
        goto fail;
    }

    /* Validate header fields before pointer arithmetic */
    if (hdr->width == 0 || hdr->height == 0 || hdr->width > 8192 || hdr->height > 8192 ||
        hdr->stride != hdr->width * 4 || hdr->buf_size != hdr->stride * hdr->height ||
        osd_total_size(hdr->buf_size) > shm->total_size) {
        RSS_IPC_WARN("osd_open %s: header validation failed", name);
        munmap(base, shm->total_size);
        goto fail;
    }

    osd_set_pointers(shm, base, hdr->buf_size);

    /* Consumer notification is via the atomic dirty flag in SHM
     * (polled by rss_osd_check_dirty), not eventfd. eventfd is
     * per-process and cannot be shared across the SHM boundary. */

    return shm;

fail:
    if (shm->shm_fd >= 0)
        close(shm->shm_fd);
    free(shm);
    return NULL;
}

void rss_osd_close(rss_osd_shm_t *shm)
{
    if (!shm)
        return;

    if (shm->header && shm->header != (void *)MAP_FAILED)
        munmap(shm->header, shm->total_size);

    if (shm->event_fd >= 0)
        close(shm->event_fd);
    if (shm->shm_fd >= 0)
        close(shm->shm_fd);

    free(shm);
}

const uint8_t *rss_osd_get_active_buffer(rss_osd_shm_t *shm, uint32_t *width, uint32_t *height)
{
    if (!shm)
        return NULL;

    /* Acquire ordering: see the bitmap data that the producer wrote
     * before setting active_buf. */
    int active = atomic_load_explicit(&shm->header->active_buf, memory_order_acquire);

    if (width)
        *width = shm->header->width;
    if (height)
        *height = shm->header->height;

    return shm->buf[active];
}

int rss_osd_check_dirty(rss_osd_shm_t *shm)
{
    if (!shm)
        return 0;

    return atomic_load_explicit(&shm->header->dirty, memory_order_acquire);
}

void rss_osd_clear_dirty(rss_osd_shm_t *shm)
{
    if (!shm)
        return;

    atomic_store_explicit(&shm->header->dirty, 0, memory_order_relaxed);
}

void rss_osd_heartbeat(rss_osd_shm_t *shm)
{
    if (!shm || !shm->is_producer)
        return;
    /* Set dirty without swapping buffers — tells consumer we're alive
     * without changing displayed content. */
    atomic_store_explicit(&shm->header->dirty, 1, memory_order_release);
}

int rss_osd_get_fd(rss_osd_shm_t *shm)
{
    return shm ? shm->shm_fd : -1;
}

int rss_osd_get_eventfd(rss_osd_shm_t *shm)
{
    return shm ? shm->event_fd : -1;
}

/*
 * A font size as a config spells it. See rss_ipc.h for why this is here.
 *
 * Tenths of a percent, because whole ones are too coarse to steer with: one
 * percent of a 1520-line encode is fifteen pixels, so a range wide enough to
 * hold every size anybody wants would have about nine positions in it.
 */
bool rss_osd_parse_font_size(const char *spec, int *px, int *pct)
{
    char *end;
    long whole, tenths = 0;

    if (!px || !pct)
        return false;
    *px = 0;
    *pct = 0;
    if (!spec)
        return false;

    whole = strtol(spec, &end, 10);
    if (end == spec || whole < 0 || whole > 10000)
        return false;

    if (*end == '.') {
        if (end[1] < '0' || end[1] > '9')
            return false;
        tenths = end[1] - '0';
        /* Anything past the first decimal is below a pixel on any picture
         * this encodes. */
        for (end += 2; *end >= '0' && *end <= '9'; end++)
            ;
    }

    if (*end == '%') {
        *pct = (int)(whole * 10 + tenths);
        return end[1] == '\0' && *pct > 0;
    }

    /* A fractional pixel is not a size a glyph cache has. */
    if (*end != '\0' || tenths)
        return false;
    *px = (int)whole;
    return *px > 0;
}

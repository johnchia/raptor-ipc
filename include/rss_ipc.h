/*
 * rss_ipc.h -- Raptor Streaming System IPC library public API.
 *
 * Three IPC primitives:
 *   1. SHM ring buffer  -- lock-free SPMC frame transport
 *   2. OSD SHM double-buffer -- latest-wins bitmap passing
 *   3. Control socket    -- length-prefixed JSON command/response
 *
 * Pure POSIX. No vendor dependencies.
 */

#ifndef RSS_IPC_H
#define RSS_IPC_H

#include <stdbool.h>
#include <stdint.h>
#include <stdatomic.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------------------------------------------------ */
/*  Logging                                                           */
/* ------------------------------------------------------------------ */

#define RSS_IPC_LOG_ERROR 1
#define RSS_IPC_LOG_WARN 2
#define RSS_IPC_LOG_INFO 3
#define RSS_IPC_LOG_DEBUG 4
#define RSS_IPC_LOG_TRACE 5

typedef void (*rss_ipc_log_fn)(int level, const char *file, int line, const char *fmt, ...)
    __attribute__((format(printf, 4, 5)));

void rss_ipc_set_log(rss_ipc_log_fn fn);
void rss_ipc_log(int level, const char *file, int line, const char *fmt, ...)
    __attribute__((format(printf, 4, 5)));

#define RSS_IPC_ERROR(fmt, ...)                                                                    \
    rss_ipc_log(RSS_IPC_LOG_ERROR, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define RSS_IPC_WARN(fmt, ...) rss_ipc_log(RSS_IPC_LOG_WARN, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define RSS_IPC_INFO(fmt, ...) rss_ipc_log(RSS_IPC_LOG_INFO, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define RSS_IPC_DEBUG(fmt, ...)                                                                    \
    rss_ipc_log(RSS_IPC_LOG_DEBUG, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define RSS_IPC_TRACE(fmt, ...)                                                                    \
    rss_ipc_log(RSS_IPC_LOG_TRACE, __FILE__, __LINE__, fmt, ##__VA_ARGS__)

/* ------------------------------------------------------------------ */
/*  SHM Ring Buffer (section 2.1)                                     */
/* ------------------------------------------------------------------ */

#define RSS_RING_MAGIC 0x52535352 /* "RSSR" */
#define RSS_RING_VERSION 4
#define RSS_RING_MAX_SLOTS 64
#define RSS_RING_SHM_PREFIX "/rss_ring_"

#define RSS_EOVERFLOW (-75) /* consumer fell behind */

#define RSS_RING_FLAG_REFMODE 0x01 /* data lives in external SHM or /dev/rmem */
#define RSS_RING_MAX_REF_BUFS 8    /* max encoder output buffers for refmode */

/*
 * Ring header — lives at offset 0 of the SHM segment, cache-line aligned.
 *
 * Memory ordering contract:
 *   write_seq: producer stores with memory_order_release after writing
 *              slot + data. Consumer loads with memory_order_acquire.
 *   magic:     producer stores with memory_order_release LAST during init.
 *              Consumer loads with memory_order_acquire to gate header reads.
 *   data_head: producer-only, relaxed ordering (no consumer reads).
 *   incarnation: relaxed (checked against stored copy in consumer struct).
 */
typedef struct __attribute__((aligned(64))) {
    _Atomic uint64_t write_seq; /* monotonic frame sequence (release/acquire) */
    _Atomic uint32_t futex_seq; /* low 32 bits of write_seq for futex wake/wait */
    uint32_t slot_count;        /* number of slots (power of 2)               */
    uint32_t data_size;         /* total data region size in bytes             */
    _Atomic uint32_t data_head; /* next write offset in data region (relaxed)  */
    uint32_t stream_id;         /* 0=main, 1=sub, 2=jpeg, 0x10=audio          */
    uint32_t codec;             /* rss_codec_t value                           */
    uint32_t width, height;
    uint32_t fps_num, fps_den;
    uint8_t profile; /* H.264 profile_idc (66=Base,77=Main,100=High) */
    uint8_t level;   /* H.264 level_idc (30,31,40,51...)            */
    /* Seqlock generation for the stream-info cluster above (stream_id..
     * level) — odd while a producer rewrite is in progress. Lives in what
     * was reserved padding, so the layout and version are unchanged: an
     * old producer simply never moves it and readers of such a ring see a
     * constant even value, which degrades to the historical plain read. A
     * one-shot read at open needs none of this — the magic release/acquire
     * contract already orders it; the seqlock exists for LIVE re-reads,
     * where rvd rewrites the cluster in place across an encoder restart
     * and a torn (width,height) pair is otherwise observable. */
    _Atomic uint16_t info_gen;
    _Atomic uint32_t magic; /* RSS_RING_MAGIC — written last (release)     */
    uint32_t version;
    _Atomic uint32_t incarnation;  /* incremented each create (crash detection)  */
    _Atomic uint32_t idr_request;  /* consumer sets to 1 to request keyframe  */
    _Atomic uint32_t reader_count; /* demand count (acquire/release, not open)  */
#define RSS_RING_MAX_READERS 4
    _Atomic uint32_t reader_pids[RSS_RING_MAX_READERS]; /* PIDs for crash detection */

    /* v3: reference mode — frame data in /dev/rmem, ring is metadata-only */
    uint32_t flags;                                      /* RSS_RING_FLAG_*           */
    uint32_t ref_buf_count;                              /* encoder output buffer cnt */
    uint32_t ref_rmem_size;                              /* /dev/rmem mmap size       */
    uint32_t ref_rmem_offset;                            /* /dev/rmem mmap file offset */
    uint32_t ref_buf_stride;                             /* per-buffer size in bytes  */
    _Atomic uint32_t ref_buf_gen[RSS_RING_MAX_REF_BUFS]; /* per-buffer generation   */

    /* v4: producer UTC mapping — UTC(frame) = slot.timestamp + utc_offset_us.
     * Seqlock-guarded: MIPS32 has no lock-free 64-bit atomics, and a torn
     * read across a refresh would yield a garbage timestamp. utc_offset_us
     * of 0 means no mapping is published (producer has no wall clock). */
    _Atomic uint32_t utc_gen; /* seqlock generation — odd while updating     */
    uint8_t utc_status;       /* ST 0603 time status (RSS_UTC_STATUS_*)      */
    uint8_t _pad_utc[3];
    int64_t utc_offset_us; /* CLOCK_REALTIME − producer media clock, µs   */
} rss_ring_header_t;

/* MISB ST 0603 §7.4 Time Status byte values for utc_status */
#define RSS_UTC_STATUS_LOCKED 0x1F   /* clock locked to absolute reference   */
#define RSS_UTC_STATUS_UNLOCKED 0x9F /* lock unknown (not NTP-disciplined)   */

_Static_assert(sizeof(rss_ring_header_t) <= 192, "ring header must fit in header page");

typedef struct {
    _Atomic uint64_t seq; /* sequence number when written         */
    uint32_t data_offset; /* offset into data/rmem region         */
    uint32_t data_length; /* frame payload size in bytes          */
    int64_t timestamp;    /* HAL capture timestamp (us, IMP_System clock) */
    uint16_t nal_type;    /* NAL type for video, codec for audio  */
    uint8_t is_key;       /* 1 if IDR / keyframe                  */
    uint8_t buf_idx;      /* refmode: encoder buffer index        */
    uint32_t buf_gen;     /* refmode: generation at publish time  */
} rss_ring_slot_t;

typedef struct rss_ring rss_ring_t; /* opaque */

/* Scatter-gather I/O vector for rss_ring_publish_iov */
typedef struct {
    const uint8_t *data;
    uint32_t length;
} rss_iov_t;

/* Producer API */
rss_ring_t *rss_ring_create(const char *name, uint32_t slot_count, uint32_t data_size);
void rss_ring_destroy(rss_ring_t *ring);
int rss_ring_publish(rss_ring_t *ring, const uint8_t *data, uint32_t length, int64_t timestamp,
                     uint16_t nal_type, uint8_t is_key);
int rss_ring_publish_iov(rss_ring_t *ring, const rss_iov_t *iov, uint32_t iov_count,
                         int64_t timestamp, uint16_t nal_type, uint8_t is_key);
void rss_ring_set_stream_info(rss_ring_t *ring, uint32_t stream_id, uint32_t codec, uint32_t width,
                              uint32_t height, uint32_t fps_num, uint32_t fps_den, uint8_t profile,
                              uint8_t level);

/* Consistent snapshot of the header's stream-info cluster. */
typedef struct {
    uint32_t stream_id;
    uint32_t codec;
    uint32_t width, height;
    uint32_t fps_num, fps_den;
    uint8_t profile;
    uint8_t level;
} rss_stream_info_t;

/* Read the stream-info cluster under its seqlock so a live producer
 * rewrite (encoder restart reusing the ring) can never hand back a torn
 * pair like the new width with the old height. Returns 0 on success;
 * -EAGAIN if a rewrite persisted across the bounded retry (keep the
 * previous values and try again next pass). Rings from producers that
 * predate info_gen read as a constant even generation and always
 * succeed, exactly like the historical plain read. */
int rss_ring_get_stream_info(rss_ring_t *ring, rss_stream_info_t *out);

/* Reference mode: frame data in external /dev/rmem instead of ring data region.
 * Eliminates the publish-side memcpy. Consumers transparently mmap /dev/rmem
 * and read from it via the standard rss_ring_read API. */
int rss_ring_enable_refmode(rss_ring_t *ring, uint32_t rmem_size, uint32_t rmem_offset,
                            uint8_t buf_count, uint32_t buf_stride);

/* Publish the producer's media-clock→UTC mapping (see utc_offset_us in the
 * header). Producer-only. Refresh periodically so NTP steps propagate. */
void rss_ring_set_utc(rss_ring_t *ring, int64_t offset_us, uint8_t status);

/* Read the UTC mapping. Returns 0 on success, -ENOENT if the producer has
 * not published one, -EAGAIN if a concurrent refresh persisted across the
 * bounded retry (skip and retry next frame). */
int rss_ring_get_utc(rss_ring_t *ring, int64_t *offset_us, uint8_t *status);
int rss_ring_publish_ref(rss_ring_t *ring, uint32_t rmem_offset, uint32_t length, int64_t timestamp,
                         uint16_t nal_type, uint8_t is_key, uint8_t buf_idx);

/* Consumer API */
rss_ring_t *rss_ring_open(const char *name);
void rss_ring_close(rss_ring_t *ring);

/*
 * rss_ring_read -- copy-on-read from the ring buffer.
 *
 * Copies frame data into the caller's buffer, then re-validates that
 * the slot wasn't recycled during the copy. This eliminates the race
 * window between reading the data pointer and the producer overwriting
 * the data region.
 *
 * dest:      caller-owned buffer to copy frame data into
 * dest_size: size of dest buffer in bytes
 * length:    [out] actual frame data length (may be < dest_size)
 * meta:      [out] slot metadata (timestamp, nal_type, is_key)
 *
 * Returns 0 on success, RSS_EOVERFLOW if consumer fell behind,
 * -ENOSPC if frame is larger than dest_size, -EAGAIN if no new data.
 */
int rss_ring_read(rss_ring_t *ring, uint64_t *read_seq, uint8_t *dest, uint32_t dest_size,
                  uint32_t *length, rss_ring_slot_t *meta);

/*
 * rss_ring_peek -- zero-copy read: returns a pointer to frame data in
 * the ring's backing memory (rmem for refmode, data region for embedded).
 *
 * The returned pointer is valid until rss_ring_peek_done() is called.
 * The caller MUST call rss_ring_peek_done() after it finishes using
 * the data — this re-validates that the producer didn't overwrite
 * the buffer and advances the read sequence.
 *
 * Returns 0 on success, RSS_EOVERFLOW, -EAGAIN (same as rss_ring_read).
 */
int rss_ring_peek(rss_ring_t *ring, uint64_t *read_seq, const uint8_t **data_ptr, uint32_t *length,
                  rss_ring_slot_t *meta);

/*
 * rss_ring_peek_done -- validate and release a peek'd frame.
 *
 * Returns 0 if the data was valid throughout the peek, RSS_EOVERFLOW
 * if the producer overwrote the buffer (data sent may be corrupt).
 */
int rss_ring_peek_done(rss_ring_t *ring, const rss_ring_slot_t *meta);

int rss_ring_wait(rss_ring_t *ring, uint32_t timeout_ms);

/* True when the shm file behind this handle is no longer the file the
 * name resolves to (producer restarted) or is gone (producer dead and
 * cleaned up). Stale handles never produce again: close and reopen. */
bool rss_ring_stale(rss_ring_t *ring);
const rss_ring_header_t *rss_ring_get_header(rss_ring_t *ring);
uint32_t rss_ring_max_frame_size(rss_ring_t *ring);

/*
 * rss_ring_version_ok -- check if an opened ring's version is compatible.
 *
 * Returns true if the ring version matches RSS_RING_VERSION compiled into
 * the caller. Returns false and sets *ring_ver to the actual version if
 * there is a mismatch (ring_ver may be NULL if not needed).
 */
static inline bool rss_ring_version_ok(rss_ring_t *ring, uint32_t *ring_ver)
{
    const rss_ring_header_t *h = rss_ring_get_header(ring);
    if (!h)
        return false;
    if (ring_ver)
        *ring_ver = h->version;
    return h->version == RSS_RING_VERSION;
}

/*
 * rss_ring_check_version -- check version and warn on mismatch.
 *
 * Convenience wrapper: logs a warning if the ring's version does not
 * match the caller's compiled RSS_RING_VERSION. Returns true if OK.
 */
static inline bool rss_ring_check_version(rss_ring_t *ring, const char *name)
{
    uint32_t rv = 0;
    if (rss_ring_version_ok(ring, &rv))
        return true;
    RSS_IPC_WARN("%s: ring version mismatch: %u vs %u", name, rv, RSS_RING_VERSION);
    return false;
}

/* Consumer → producer IDR request via atomic flag in ring header.
 * Consumer calls request_idr after EOVERFLOW to get a keyframe fast.
 * Producer calls check_idr in its encode loop and clears the flag. */
void rss_ring_request_idr(rss_ring_t *ring);
int rss_ring_check_idr(rss_ring_t *ring); /* returns 1 and clears if set */

/* Demand signaling: consumers call acquire/release to indicate they
 * actively want data. Producers can check reader_count to decide
 * whether to encode. open/close do NOT touch the counter. */
void rss_ring_acquire(rss_ring_t *ring);
void rss_ring_release(rss_ring_t *ring);
uint32_t rss_ring_reader_count(rss_ring_t *ring);
uint32_t rss_ring_reap_dead_readers(rss_ring_t *ring);

/* ------------------------------------------------------------------ */
/*  OSD SHM Double-Buffer (section 2.2)                               */
/* ------------------------------------------------------------------ */

typedef struct rss_osd_shm rss_osd_shm_t; /* opaque */

/* Producer API (ROD) */
rss_osd_shm_t *rss_osd_create(const char *name, uint32_t width, uint32_t height);
void rss_osd_destroy(rss_osd_shm_t *shm);
uint8_t *rss_osd_get_draw_buffer(rss_osd_shm_t *shm);
void rss_osd_publish(rss_osd_shm_t *shm);

/* Consumer API (RVD) */
rss_osd_shm_t *rss_osd_open(const char *name);
void rss_osd_close(rss_osd_shm_t *shm);
const uint8_t *rss_osd_get_active_buffer(rss_osd_shm_t *shm, uint32_t *width, uint32_t *height);
int rss_osd_check_dirty(rss_osd_shm_t *shm);
void rss_osd_clear_dirty(rss_osd_shm_t *shm);
void rss_osd_heartbeat(rss_osd_shm_t *shm);
int rss_osd_get_fd(rss_osd_shm_t *shm);
int rss_osd_get_eventfd(rss_osd_shm_t *shm);

/* ------------------------------------------------------------------ */
/*  Control Socket (section 2.3)                                      */
/* ------------------------------------------------------------------ */

typedef struct rss_ctrl rss_ctrl_t; /* opaque */

/* Server API (daemon side) */
rss_ctrl_t *rss_ctrl_listen(const char *sock_path);
void rss_ctrl_destroy(rss_ctrl_t *ctrl);
int rss_ctrl_get_fd(rss_ctrl_t *ctrl);
int rss_ctrl_accept_and_handle(rss_ctrl_t *ctrl,
                               int (*handler)(const char *cmd_json, char *resp_buf,
                                              int resp_buf_size, void *userdata),
                               void *userdata);

/* Client API (raptorctl side) */
int rss_ctrl_send_command(const char *sock_path, const char *cmd_json, char *resp_buf,
                          int resp_buf_size, uint32_t timeout_ms);

#ifdef __cplusplus
}
#endif

#endif /* RSS_IPC_H */

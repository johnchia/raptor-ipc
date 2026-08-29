#include "greatest.h"
#include "rss_ipc.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

/*
 * Ring buffer protocol note:
 *   write_seq = latest published sequence number (starts at 0 = empty).
 *   read_seq  = next seq to read. Consumer reads seq [read_seq, write_seq).
 *   First real frame has seq=1 at slot[1 % count].
 *   Slot[0] is initialized (seq=0, data_length=0) and acts as a phantom.
 *   To read the frame at seq N, write_seq must be > N (not just == N).
 */

/* Publish a 1-byte sentinel frame to advance write_seq.
 * Uses 0xFF so tests can distinguish from real data. */
static void publish_dummy(rss_ring_t *ring)
{
    uint8_t d = 0xFF;
    rss_ring_publish(ring, &d, 1, 0, 0, 0, RSS_SRC_SEQ_NONE);
}

TEST ring_create_open(void)
{
    rss_ring_t *p = rss_ring_create("t_co", 8, 65536);
    ASSERT(p);
    const rss_ring_header_t *hdr = rss_ring_get_header(p);
    ASSERT(hdr);
    ASSERT_EQ(RSS_RING_MAGIC, atomic_load(&hdr->magic));
    ASSERT_EQ(RSS_RING_VERSION, hdr->version);
    ASSERT_EQ(8u, hdr->slot_count);

    rss_ring_t *c = rss_ring_open("t_co");
    ASSERT(c);
    rss_ring_close(c);
    rss_ring_destroy(p);
    PASS();
}

TEST ring_publish_read(void)
{
    rss_ring_t *p = rss_ring_create("t_pr", 8, 65536);
    ASSERT(p);
    rss_ring_t *c = rss_ring_open("t_pr");
    ASSERT(c);

    uint8_t data[] = {0xDE, 0xAD, 0xBE, 0xEF};
    ASSERT_EQ(0, rss_ring_publish(p, data, sizeof(data), 12345, 5, 1, RSS_SRC_SEQ_NONE));
    publish_dummy(p); /* advance write_seq so frame at seq 1 is readable */

    uint64_t rseq = 1; /* skip phantom seq 0 */
    uint8_t buf[256];
    uint32_t len = 0;
    rss_ring_slot_t meta;
    int ret = rss_ring_read(c, &rseq, buf, sizeof(buf), &len, &meta);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(4u, len);
    ASSERT_MEM_EQ(data, buf, 4);
    ASSERT_EQ(12345, meta.timestamp);
    ASSERT_EQ(5, meta.nal_type);
    ASSERT_EQ(1, meta.is_key);
    ASSERT_EQ(2u, (uint32_t)rseq);

    rss_ring_close(c);
    rss_ring_destroy(p);
    PASS();
}

TEST ring_sequential(void)
{
    rss_ring_t *p = rss_ring_create("t_seq", 8, 65536);
    ASSERT(p);
    rss_ring_t *c = rss_ring_open("t_seq");
    ASSERT(c);

    for (int i = 0; i < 5; i++) {
        uint8_t d = (uint8_t)(0x10 + i);
        ASSERT_EQ(0, rss_ring_publish(p, &d, 1, i * 100, 0, 0, RSS_SRC_SEQ_NONE));
    }
    publish_dummy(p); /* make last frame readable */

    uint64_t rseq = 1;
    for (int i = 0; i < 5; i++) {
        uint8_t buf[16];
        uint32_t len = 0;
        rss_ring_slot_t meta;
        ASSERT_EQ(0, rss_ring_read(c, &rseq, buf, sizeof(buf), &len, &meta));
        ASSERT_EQ(1u, len);
        ASSERT_EQ((uint8_t)(0x10 + i), buf[0]);
        ASSERT_EQ(i * 100, meta.timestamp);
    }
    ASSERT_EQ(6u, (uint32_t)rseq);

    rss_ring_close(c);
    rss_ring_destroy(p);
    PASS();
}

TEST ring_overflow(void)
{
    rss_ring_t *p = rss_ring_create("t_ovf", 4, 65536);
    ASSERT(p);
    rss_ring_t *c = rss_ring_open("t_ovf");
    ASSERT(c);

    /* Publish 8 frames into 4-slot ring */
    for (int i = 0; i < 8; i++) {
        uint8_t d = (uint8_t)i;
        ASSERT_EQ(0, rss_ring_publish(p, &d, 1, 0, 0, 0, RSS_SRC_SEQ_NONE));
    }

    /* Consumer at seq 1 should get overflow (wseq=8, 8-1=7 >= 4) */
    uint64_t rseq = 1;
    uint8_t buf[16];
    uint32_t len = 0;
    int ret = rss_ring_read(c, &rseq, buf, sizeof(buf), &len, NULL);
    ASSERT_EQ(RSS_EOVERFLOW, ret);

    rss_ring_close(c);
    rss_ring_destroy(p);
    PASS();
}

TEST ring_overflow_recovery(void)
{
    rss_ring_t *p = rss_ring_create("t_rec", 4, 65536);
    ASSERT(p);
    rss_ring_t *c = rss_ring_open("t_rec");
    ASSERT(c);

    /* Fill and overflow */
    for (int i = 0; i < 8; i++) {
        uint8_t d = (uint8_t)i;
        rss_ring_publish(p, &d, 1, 0, 0, 0, RSS_SRC_SEQ_NONE);
    }

    uint64_t rseq = 1;
    uint8_t buf[16];
    uint32_t len = 0;
    rss_ring_read(c, &rseq, buf, sizeof(buf), &len, NULL); /* overflow, rseq = write_seq = 8 */

    /* Publish 2 more to make one readable */
    uint8_t fresh = 0xAA;
    rss_ring_publish(p, &fresh, 1, 0, 0, 0, RSS_SRC_SEQ_NONE); /* seq 9, makes seq 8 readable */
    publish_dummy(p);                                          /* seq 10, makes seq 9 readable */

    /* rseq=8, wseq=10, reads slot[8%4=0] which has seq=8 */
    int ret = rss_ring_read(c, &rseq, buf, sizeof(buf), &len, NULL);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1u, len);
    /* Slot[8%4=0] was written at seq=8 with d=7 (the 8th publish, i=7) */
    ASSERT_EQ(7, buf[0]);

    /* Now read seq 9 which is our fresh 0xAA frame */
    ret = rss_ring_read(c, &rseq, buf, sizeof(buf), &len, NULL);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(0xAA, buf[0]);

    rss_ring_close(c);
    rss_ring_destroy(p);
    PASS();
}

TEST ring_publish_iov(void)
{
    rss_ring_t *p = rss_ring_create("t_iov", 8, 65536);
    ASSERT(p);
    rss_ring_t *c = rss_ring_open("t_iov");
    ASSERT(c);

    uint8_t a[] = {1, 2, 3};
    uint8_t b[] = {4, 5};
    uint8_t e[] = {6};
    rss_iov_t iov[3] = {
        {a, sizeof(a)},
        {b, sizeof(b)},
        {e, sizeof(e)},
    };
    ASSERT_EQ(0, rss_ring_publish_iov(p, iov, 3, 999, 0, 0, RSS_SRC_SEQ_NONE));
    publish_dummy(p);

    uint64_t rseq = 1;
    uint8_t buf[64];
    uint32_t len = 0;
    ASSERT_EQ(0, rss_ring_read(c, &rseq, buf, sizeof(buf), &len, NULL));
    ASSERT_EQ(6u, len);
    uint8_t expected[] = {1, 2, 3, 4, 5, 6};
    ASSERT_MEM_EQ(expected, buf, 6);

    rss_ring_close(c);
    rss_ring_destroy(p);
    PASS();
}

TEST ring_stream_info(void)
{
    rss_ring_t *p = rss_ring_create("t_si", 8, 65536);
    ASSERT(p);
    rss_ring_set_stream_info(p, 0, 96, 1920, 1080, 30, 1, 100, 40);

    rss_ring_t *c = rss_ring_open("t_si");
    ASSERT(c);
    const rss_ring_header_t *hdr = rss_ring_get_header(c);
    ASSERT_EQ(0u, hdr->stream_id);
    ASSERT_EQ(96u, hdr->codec);
    ASSERT_EQ(1920u, hdr->width);
    ASSERT_EQ(1080u, hdr->height);
    ASSERT_EQ(30u, hdr->fps_num);
    ASSERT_EQ(1u, hdr->fps_den);
    ASSERT_EQ(100, hdr->profile);
    ASSERT_EQ(40, hdr->level);

    rss_ring_close(c);
    rss_ring_destroy(p);
    PASS();
}

TEST ring_idr_request(void)
{
    rss_ring_t *p = rss_ring_create("t_idr", 8, 65536);
    ASSERT(p);
    rss_ring_t *c = rss_ring_open("t_idr");
    ASSERT(c);

    ASSERT_EQ(0, rss_ring_check_idr(p));
    rss_ring_request_idr(c);
    ASSERT_EQ(1, rss_ring_check_idr(p));
    ASSERT_EQ(0, rss_ring_check_idr(p));

    rss_ring_close(c);
    rss_ring_destroy(p);
    PASS();
}

TEST ring_demand_count(void)
{
    rss_ring_t *p = rss_ring_create("t_dc", 8, 65536);
    ASSERT(p);
    rss_ring_t *c1 = rss_ring_open("t_dc");
    ASSERT(c1);

    ASSERT_EQ(0u, rss_ring_reader_count(p));
    rss_ring_acquire(c1);
    ASSERT_EQ(1u, rss_ring_reader_count(p));

    rss_ring_t *c2 = rss_ring_open("t_dc");
    ASSERT(c2);
    rss_ring_acquire(c2);
    ASSERT_EQ(2u, rss_ring_reader_count(p));

    rss_ring_release(c1);
    ASSERT_EQ(1u, rss_ring_reader_count(p));
    rss_ring_release(c2);
    ASSERT_EQ(0u, rss_ring_reader_count(p));

    rss_ring_close(c2);
    rss_ring_close(c1);
    rss_ring_destroy(p);
    PASS();
}

TEST ring_reap_dead(void)
{
    rss_ring_t *p = rss_ring_create("t_reap", 8, 65536);
    ASSERT(p);

    pid_t child = fork();
    ASSERT(child >= 0);
    if (child == 0) {
        rss_ring_t *c = rss_ring_open("t_reap");
        if (c) {
            rss_ring_acquire(c);
            rss_ring_close(c);
        }
        _exit(0);
    }

    int status;
    waitpid(child, &status, 0);

    ASSERT_EQ(1u, rss_ring_reader_count(p));
    uint32_t reaped = rss_ring_reap_dead_readers(p);
    ASSERT(reaped > 0);
    ASSERT_EQ(0u, rss_ring_reader_count(p));

    rss_ring_destroy(p);
    PASS();
}

TEST ring_large_frame(void)
{
    uint32_t data_size = 8192;
    rss_ring_t *p = rss_ring_create("t_lg", 4, data_size);
    ASSERT(p);
    rss_ring_t *c = rss_ring_open("t_lg");
    ASSERT(c);

    uint32_t frame_size = data_size - 64;
    uint8_t *frame = malloc(frame_size);
    ASSERT(frame);
    memset(frame, 0xAB, frame_size);
    ASSERT_EQ(0, rss_ring_publish(p, frame, frame_size, 0, 0, 0, RSS_SRC_SEQ_NONE));
    publish_dummy(p);

    uint8_t *dest = malloc(frame_size);
    ASSERT(dest);
    uint64_t rseq = 1;
    uint32_t len = 0;
    ASSERT_EQ(0, rss_ring_read(c, &rseq, dest, frame_size, &len, NULL));
    ASSERT_EQ(frame_size, len);
    ASSERT_MEM_EQ(frame, dest, frame_size);

    free(dest);
    free(frame);
    rss_ring_close(c);
    rss_ring_destroy(p);
    PASS();
}

TEST ring_frame_too_large(void)
{
    rss_ring_t *p = rss_ring_create("t_ftl", 4, 1024);
    ASSERT(p);

    uint8_t buf[2048];
    memset(buf, 0, sizeof(buf));
    int ret = rss_ring_publish(p, buf, 2048, 0, 0, 0, RSS_SRC_SEQ_NONE);
    ASSERT_EQ(-ENOSPC, ret);

    rss_ring_destroy(p);
    PASS();
}

TEST ring_read_no_data(void)
{
    rss_ring_t *p = rss_ring_create("t_nd", 8, 65536);
    ASSERT(p);
    rss_ring_t *c = rss_ring_open("t_nd");
    ASSERT(c);

    uint64_t rseq = 1; /* seq 1 doesn't exist yet */
    uint8_t buf[16];
    uint32_t len = 0;
    ASSERT_EQ(-EAGAIN, rss_ring_read(c, &rseq, buf, sizeof(buf), &len, NULL));

    rss_ring_close(c);
    rss_ring_destroy(p);
    PASS();
}

TEST ring_slot_count_power2(void)
{
    ASSERT_EQ(NULL, rss_ring_create("t_p2", 3, 65536));
    ASSERT_EQ(NULL, rss_ring_create("t_p2", 5, 65536));
    ASSERT_EQ(NULL, rss_ring_create("t_p2", 6, 65536));
    rss_ring_t *p = rss_ring_create("t_p2", 4, 65536);
    ASSERT(p);
    rss_ring_destroy(p);
    PASS();
}

TEST ring_wait_timeout(void)
{
    rss_ring_t *p = rss_ring_create("t_wt", 8, 65536);
    ASSERT(p);
    rss_ring_t *c = rss_ring_open("t_wt");
    ASSERT(c);

    int ret = rss_ring_wait(c, 50);
    ASSERT(ret == 0 || ret == -ETIMEDOUT);

    rss_ring_close(c);
    rss_ring_destroy(p);
    PASS();
}

TEST ring_data_wrap(void)
{
    /* Small data region forces wrap-around.
     * 8 slots, 256 bytes data. Publish frames that fill the tail,
     * then a frame that must wrap to offset 0. Read the wrapped frame
     * immediately before anything overwrites it. */
    rss_ring_t *p = rss_ring_create("t_wrap", 8, 256);
    ASSERT(p);
    rss_ring_t *c = rss_ring_open("t_wrap");
    ASSERT(c);

    /* Fill data region: 100 + 100 = 200 bytes used, 56 remaining */
    uint8_t a[100];
    memset(a, 0xAA, sizeof(a));
    rss_ring_publish(p, a, sizeof(a), 1000, 0, 0, RSS_SRC_SEQ_NONE); /* seq 1, offset 0 */

    uint8_t b[100];
    memset(b, 0xBB, sizeof(b));
    rss_ring_publish(p, b, sizeof(b), 2000, 0, 0, RSS_SRC_SEQ_NONE); /* seq 2, offset 100 */

    /* Frame C: 100 bytes won't fit in remaining 56 → wraps to offset 0 */
    uint8_t cc[100];
    memset(cc, 0xCC, sizeof(cc));
    rss_ring_publish(p, cc, sizeof(cc), 3000, 0, 0, RSS_SRC_SEQ_NONE); /* seq 3, offset 0 (wrap!) */

    /* Frame D: goes after C at offset 100 */
    uint8_t dd[80];
    memset(dd, 0xDD, sizeof(dd));
    rss_ring_publish(p, dd, sizeof(dd), 4000, 0, 0, RSS_SRC_SEQ_NONE); /* seq 4, offset 100 */

    publish_dummy(p); /* seq 5, makes seq 4 readable */

    /* Read frame C (seq 3, wrapped to offset 0) — verify data survived the wrap */
    uint64_t rseq = 3;
    uint8_t buf[128];
    uint32_t len = 0;
    rss_ring_slot_t meta;
    ASSERT_EQ(0, rss_ring_read(c, &rseq, buf, sizeof(buf), &len, &meta));
    ASSERT_EQ(100u, len);
    ASSERT_EQ(3000, meta.timestamp);
    for (uint32_t i = 0; i < len; i++)
        ASSERT_EQ(0xCC, buf[i]);

    /* Read frame D (seq 4, after wrap) — verify post-wrap data is correct */
    ASSERT_EQ(0, rss_ring_read(c, &rseq, buf, sizeof(buf), &len, &meta));
    ASSERT_EQ(80u, len);
    ASSERT_EQ(4000, meta.timestamp);
    for (uint32_t i = 0; i < len; i++)
        ASSERT_EQ(0xDD, buf[i]);

    rss_ring_close(c);
    rss_ring_destroy(p);
    PASS();
}

TEST ring_multi_consumer(void)
{
    /* Two consumers on the same ring. Fast consumer reads in lockstep
     * with publishes. Slow consumer falls behind and overflows.
     * Verify fast consumer's data is never corrupted. */
    rss_ring_t *p = rss_ring_create("t_mc", 8, 4096);
    ASSERT(p);
    rss_ring_t *fast = rss_ring_open("t_mc");
    ASSERT(fast);
    rss_ring_t *slow = rss_ring_open("t_mc");
    ASSERT(slow);

    uint64_t fast_seq = 1;
    uint64_t slow_seq = 1;

    /* Publish 20 frames. Fast reads after every 2 publishes.
     * Slow never reads → will overflow. */
    int fast_read_count = 0;
    for (int i = 0; i < 20; i++) {
        uint8_t d = (uint8_t)(0x40 + i);
        rss_ring_publish(p, &d, 1, (i + 1) * 100, 0, 0, RSS_SRC_SEQ_NONE);

        /* Fast consumer reads every 2 publishes to stay current */
        if ((i % 2) == 1) {
            publish_dummy(p);
            i++; /* account for dummy in loop count */

            uint8_t buf[16];
            uint32_t len = 0;
            while (rss_ring_read(fast, &fast_seq, buf, sizeof(buf), &len, NULL) == 0) {
                if (len == 1 && buf[0] != 0xFF) {
                    /* Real frame — verify pattern, not garbage */
                    ASSERT(buf[0] >= 0x40 && buf[0] < 0x80);
                    fast_read_count++;
                }
            }
        }
    }
    /* Fast consumer successfully read frames */
    ASSERT(fast_read_count > 0);

    /* Slow consumer should have overflowed (20+ frames behind in 8-slot ring) */
    uint8_t buf[16];
    uint32_t len = 0;
    int ret = rss_ring_read(slow, &slow_seq, buf, sizeof(buf), &len, NULL);
    ASSERT_EQ(RSS_EOVERFLOW, ret);

    rss_ring_close(slow);
    rss_ring_close(fast);
    rss_ring_destroy(p);
    PASS();
}

TEST ring_version_mismatch_open(void)
{
    rss_ring_t *p = rss_ring_create("t_vm", 8, 65536);
    ASSERT(p);

    rss_ring_header_t *hdr = (rss_ring_header_t *)rss_ring_get_header(p);
    ASSERT(hdr);
    hdr->version = RSS_RING_VERSION + 1;

    rss_ring_t *c = rss_ring_open("t_vm");
    ASSERT_EQ(NULL, c);

    hdr->version = RSS_RING_VERSION;
    rss_ring_destroy(p);
    PASS();
}

TEST ring_magic_mismatch_open(void)
{
    rss_ring_t *p = rss_ring_create("t_mm", 8, 65536);
    ASSERT(p);

    rss_ring_header_t *hdr = (rss_ring_header_t *)rss_ring_get_header(p);
    ASSERT(hdr);
    atomic_store(&hdr->magic, 0xDEADBEEF);

    rss_ring_t *c = rss_ring_open("t_mm");
    ASSERT_EQ(NULL, c);

    atomic_store(&hdr->magic, RSS_RING_MAGIC);
    rss_ring_destroy(p);
    PASS();
}

TEST ring_version_check_helpers(void)
{
    rss_ring_t *p = rss_ring_create("t_vch", 8, 65536);
    ASSERT(p);

    uint32_t ver = 0;
    ASSERT(rss_ring_version_ok(p, &ver));
    ASSERT_EQ(RSS_RING_VERSION, ver);
    ASSERT(rss_ring_check_version(p, "t_vch"));

    rss_ring_header_t *hdr = (rss_ring_header_t *)rss_ring_get_header(p);
    hdr->version = 999;

    ver = 0;
    ASSERT_FALSE(rss_ring_version_ok(p, &ver));
    ASSERT_EQ(999u, ver);
    ASSERT_FALSE(rss_ring_check_version(p, "t_vch"));
    ASSERT_FALSE(rss_ring_version_ok(p, NULL));

    hdr->version = RSS_RING_VERSION;
    rss_ring_destroy(p);
    PASS();
}

TEST ring_utc_mapping(void)
{
    rss_ring_t *p = rss_ring_create("t_utc", 8, 65536);
    ASSERT(p);

    rss_ring_t *c = rss_ring_open("t_utc");
    ASSERT(c);

    /* No mapping published yet */
    int64_t off = -1;
    uint8_t status = 0;
    ASSERT_EQ(-ENOENT, rss_ring_get_utc(c, &off, &status));

    rss_ring_set_utc(p, 1752561234567890LL, RSS_UTC_STATUS_LOCKED);
    ASSERT_EQ(0, rss_ring_get_utc(c, &off, &status));
    ASSERT_EQ(1752561234567890LL, off);
    ASSERT_EQ(RSS_UTC_STATUS_LOCKED, status);

    /* Refresh with new offset and status */
    rss_ring_set_utc(p, -987654321LL, RSS_UTC_STATUS_UNLOCKED);
    ASSERT_EQ(0, rss_ring_get_utc(c, &off, &status));
    ASSERT_EQ(-987654321LL, off);
    ASSERT_EQ(RSS_UTC_STATUS_UNLOCKED, status);

    /* NULL status pointer is allowed */
    ASSERT_EQ(0, rss_ring_get_utc(c, &off, NULL));

    /* Consumer cannot publish */
    rss_ring_set_utc(c, 42, RSS_UTC_STATUS_LOCKED);
    ASSERT_EQ(0, rss_ring_get_utc(c, &off, &status));
    ASSERT_EQ(-987654321LL, off);

    rss_ring_close(c);
    rss_ring_destroy(p);
    PASS();
}

/* v5: source-sequence loss accounting */
TEST ring_src_seq_loss(void)
{
    /* 16 slots: this test publishes 9 frames and still reads slot 1
     * at the end (read would EOVERFLOW with an 8-slot ring). */
    rss_ring_t *p = rss_ring_create("t_srcseq", 16, 65536);
    ASSERT(p);
    rss_ring_t *c = rss_ring_open("t_srcseq");
    ASSERT(c);

    uint8_t d[8] = {0};
    rss_ring_loss_t loss;

    /* Baseline starts at NONE: first stamped frame sets it, no loss */
    rss_ring_publish(p, d, sizeof(d), 0, 0, 1, 100);
    rss_ring_get_loss(c, &loss);
    ASSERT_EQ(0, loss.drop_source);
    ASSERT_EQ(0, loss.drop_publish);
    ASSERT_EQ(0, loss.src_seq_resets);
    ASSERT_EQ(100, loss.src_seq_last);

    /* Normal advance: seq + 1, still no loss */
    rss_ring_publish(p, d, sizeof(d), 0, 0, 1, 101);
    rss_ring_get_loss(c, &loss);
    ASSERT_EQ(0, loss.drop_source);
    ASSERT_EQ(0, loss.drop_publish);

    /* Jump of 3: two source frames never fetched */
    rss_ring_publish(p, d, sizeof(d), 0, 0, 1, 104);
    rss_ring_get_loss(c, &loss);
    ASSERT_EQ(2, loss.drop_source);
    ASSERT_EQ(0, loss.drop_publish);

    /* Fetched but dropped before publish */
    rss_ring_report_drop(p, 105);
    rss_ring_get_loss(c, &loss);
    ASSERT_EQ(2, loss.drop_source);
    ASSERT_EQ(1, loss.drop_publish);
    ASSERT_EQ(105, loss.src_seq_last);

    /* Publish resumes at the next frame: no double count */
    rss_ring_publish(p, d, sizeof(d), 0, 0, 1, 106);
    rss_ring_get_loss(c, &loss);
    ASSERT_EQ(2, loss.drop_source);
    ASSERT_EQ(1, loss.drop_publish);

    /* Backward jump: reset, not loss */
    rss_ring_publish(p, d, sizeof(d), 0, 0, 1, 5);
    rss_ring_get_loss(c, &loss);
    ASSERT_EQ(2, loss.drop_source);
    ASSERT_EQ(1, loss.drop_publish);
    ASSERT_EQ(1, loss.src_seq_resets);
    ASSERT_EQ(5, loss.src_seq_last);

    /* 32-bit wrap reads as +1 advance, not a giant loss */
    rss_ring_publish(p, d, sizeof(d), 0, 0, 1, 0xFFFFFFFFu);
    rss_ring_publish(p, d, sizeof(d), 0, 0, 1, 0);
    rss_ring_get_loss(c, &loss);
    ASSERT_EQ(2, loss.drop_source);
    ASSERT_EQ(1, loss.drop_publish);
    ASSERT_EQ(0, loss.src_seq_last);

    /* Legacy publish after stamped frames: NONE is stamped, accounting
     * paused (baseline preserved) */
    rss_ring_publish(p, d, sizeof(d), 0, 0, 1, RSS_SRC_SEQ_NONE);
    rss_ring_get_loss(c, &loss);
    ASSERT_EQ(0, loss.src_seq_last);
    /* stamped frame resuming from the preserved baseline */
    rss_ring_publish(p, d, sizeof(d), 0, 0, 1, 1);
    rss_ring_get_loss(c, &loss);
    ASSERT_EQ(2, loss.drop_source);
    ASSERT_EQ(1, loss.drop_publish);
    ASSERT_EQ(1, loss.src_seq_last);

    /* Consumer can read the src_seq back from the slot */
    uint64_t read_seq = 1;
    uint32_t len;
    rss_ring_slot_t meta;
    ASSERT_EQ(0, rss_ring_read(c, &read_seq, d, sizeof(d), &len, &meta));
    ASSERT_EQ(100, meta.src_seq);

    rss_ring_close(c);
    rss_ring_destroy(p);
    PASS();
}

/* v5: refmode publish stamps src_seq too */
TEST ring_ref_seq_loss(void)
{
    /* 2 ref bufs of 4096 bytes. The consumer maps the named enc SHM
     * (tried before /dev/rmem), so create it here and the test runs on
     * any host, not only on a device with reserved memory. */
    int rfd = shm_open("/rss_enc_t_refseq", O_CREAT | O_RDWR, 0666);
    ASSERT(rfd >= 0);
    ASSERT_EQ(0, ftruncate(rfd, 8192));
    close(rfd);
    rss_ring_t *p = rss_ring_create("t_refseq", 8, 4096);
    ASSERT(p);
    ASSERT_EQ(0, rss_ring_enable_refmode(p, 8192, 0, 2, 4096));
    rss_ring_t *c = rss_ring_open("t_refseq");
    ASSERT(c);

    rss_ring_loss_t loss;
    ASSERT_EQ(0, rss_ring_publish_ref(p, 0, 100, 10, 0, 1, 0, 10));
    ASSERT_EQ(0, rss_ring_publish_ref(p, 4096, 100, 20, 0, 1, 1, 12));
    rss_ring_get_loss(c, &loss);
    ASSERT_EQ(1, loss.drop_source);
    ASSERT_EQ(0, loss.drop_publish);
    ASSERT_EQ(12, loss.src_seq_last);

    rss_ring_close(c);
    rss_ring_destroy(p);
    shm_unlink("/rss_enc_t_refseq");
    PASS();
}

/* A drop reported across a sequence-domain jump is a boundary, not a
 * loss: without the MAX_GAP guard one dropped frame after an encoder
 * restart booked the whole jump as drop_source. */
TEST ring_report_drop_domain_jump(void)
{
    rss_ring_t *p = rss_ring_create("t_rdjump", 8, 65536);
    ASSERT(p);
    uint8_t d[8] = {0};
    rss_ring_loss_t loss;

    rss_ring_publish(p, d, sizeof(d), 0, 0, 1, 100);
    rss_ring_report_drop(p, 100 + 300000); /* absurd forward: domain change */
    rss_ring_get_loss(p, &loss);
    ASSERT_EQ(0, loss.drop_source);
    ASSERT_EQ(1, loss.drop_publish);
    ASSERT_EQ(1, loss.src_seq_resets);

    rss_ring_report_drop(p, 7); /* backward: domain change too */
    rss_ring_get_loss(p, &loss);
    ASSERT_EQ(0, loss.drop_source);
    ASSERT_EQ(2, loss.drop_publish);
    ASSERT_EQ(2, loss.src_seq_resets);

    rss_ring_report_drop(p, 7); /* duplicate: a drop, not a reset */
    rss_ring_get_loss(p, &loss);
    ASSERT_EQ(3, loss.drop_publish);
    ASSERT_EQ(2, loss.src_seq_resets);

    rss_ring_destroy(p);
    PASS();
}

SUITE(ring_suite)
{
    RUN_TEST(ring_create_open);
    RUN_TEST(ring_publish_read);
    RUN_TEST(ring_sequential);
    RUN_TEST(ring_overflow);
    RUN_TEST(ring_overflow_recovery);
    RUN_TEST(ring_publish_iov);
    RUN_TEST(ring_stream_info);
    RUN_TEST(ring_idr_request);
    RUN_TEST(ring_demand_count);
    RUN_TEST(ring_reap_dead);
    RUN_TEST(ring_large_frame);
    RUN_TEST(ring_frame_too_large);
    RUN_TEST(ring_read_no_data);
    RUN_TEST(ring_slot_count_power2);
    RUN_TEST(ring_wait_timeout);
    RUN_TEST(ring_data_wrap);
    RUN_TEST(ring_multi_consumer);
    RUN_TEST(ring_version_mismatch_open);
    RUN_TEST(ring_magic_mismatch_open);
    RUN_TEST(ring_version_check_helpers);
    RUN_TEST(ring_utc_mapping);
    RUN_TEST(ring_src_seq_loss);
    RUN_TEST(ring_report_drop_domain_jump);
    RUN_TEST(ring_ref_seq_loss);
}

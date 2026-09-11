#include "greatest.h"
#include "rss_ipc.h"

#include <string.h>

TEST osd_create_open(void)
{
	rss_osd_shm_t *p = rss_osd_create("t_osd_co", 320, 240);
	ASSERT(p);

	rss_osd_shm_t *c = rss_osd_open("t_osd_co");
	ASSERT(c);

	uint32_t w = 0, h = 0;
	const uint8_t *buf = rss_osd_get_active_buffer(c, &w, &h);
	ASSERT(buf);
	ASSERT_EQ(320u, w);
	ASSERT_EQ(240u, h);

	rss_osd_close(c);
	rss_osd_destroy(p);
	PASS();
}

TEST osd_publish_read(void)
{
	rss_osd_shm_t *p = rss_osd_create("t_osd_pr", 4, 2);
	ASSERT(p);
	rss_osd_shm_t *c = rss_osd_open("t_osd_pr");
	ASSERT(c);

	/* Draw a pattern into the draw buffer */
	uint8_t *draw = rss_osd_get_draw_buffer(p);
	ASSERT(draw);
	/* 4x2 BGRA = 32 bytes */
	memset(draw, 0xAA, 32);
	rss_osd_publish(p);

	/* Consumer reads active buffer */
	const uint8_t *active = rss_osd_get_active_buffer(c, NULL, NULL);
	ASSERT(active);
	/* Verify pattern */
	for (int i = 0; i < 32; i++)
		ASSERT_EQ(0xAA, active[i]);

	rss_osd_close(c);
	rss_osd_destroy(p);
	PASS();
}

TEST osd_dirty_flag(void)
{
	rss_osd_shm_t *p = rss_osd_create("t_osd_df", 4, 2);
	ASSERT(p);
	rss_osd_shm_t *c = rss_osd_open("t_osd_df");
	ASSERT(c);

	ASSERT_EQ(0, rss_osd_check_dirty(c));
	rss_osd_publish(p);
	ASSERT_EQ(1, rss_osd_check_dirty(c));
	rss_osd_clear_dirty(c);
	ASSERT_EQ(0, rss_osd_check_dirty(c));

	rss_osd_close(c);
	rss_osd_destroy(p);
	PASS();
}

TEST osd_double_buffer(void)
{
	rss_osd_shm_t *p = rss_osd_create("t_osd_db", 4, 2);
	ASSERT(p);
	rss_osd_shm_t *c = rss_osd_open("t_osd_db");
	ASSERT(c);

	/* First publish: fill with 0x11 */
	uint8_t *draw = rss_osd_get_draw_buffer(p);
	memset(draw, 0x11, 32);
	rss_osd_publish(p);

	/* Second publish: fill with 0x22 */
	draw = rss_osd_get_draw_buffer(p);
	memset(draw, 0x22, 32);
	rss_osd_publish(p);

	/* Consumer sees second publish */
	const uint8_t *active = rss_osd_get_active_buffer(c, NULL, NULL);
	ASSERT_EQ(0x22, active[0]);

	rss_osd_close(c);
	rss_osd_destroy(p);
	PASS();
}

TEST osd_heartbeat(void)
{
	rss_osd_shm_t *p = rss_osd_create("t_osd_hb", 4, 2);
	ASSERT(p);
	rss_osd_shm_t *c = rss_osd_open("t_osd_hb");
	ASSERT(c);

	ASSERT_EQ(0, rss_osd_check_dirty(c));
	rss_osd_heartbeat(p);
	ASSERT_EQ(1, rss_osd_check_dirty(c));

	/* Buffer content should still be zeros (no swap happened) */
	const uint8_t *active = rss_osd_get_active_buffer(c, NULL, NULL);
	ASSERT_EQ(0, active[0]);

	rss_osd_close(c);
	rss_osd_destroy(p);
	PASS();
}

/*
 * The spelling both ends of the OSD transport read. What matters is not that
 * the two forms parse but that nothing is silently taken for the other one: a
 * percentage read as pixels sizes a pool for a twenty-fifth of the bitmap rod
 * is about to ask for.
 */
TEST osd_font_size_spelling(void)
{
    int px = -1, pct = -1;

    ASSERT(rss_osd_parse_font_size("24", &px, &pct));
    ASSERT_EQ(24, px);
    ASSERT_EQ(0, pct);

    ASSERT(rss_osd_parse_font_size("4%", &px, &pct));
    ASSERT_EQ(0, px);
    ASSERT_EQ(40, pct);

    /* Tenths, because whole percent is a fifteen-pixel step on a 1520-line
     * encode. */
    ASSERT(rss_osd_parse_font_size("4.5%", &px, &pct));
    ASSERT_EQ(45, pct);

    /* A trailing '%' is the whole difference between the two, so nothing
     * that merely looks like one may be taken for it. */
    ASSERT_FALSE(rss_osd_parse_font_size("4 %", &px, &pct));
    ASSERT_FALSE(rss_osd_parse_font_size("4%%", &px, &pct));
    ASSERT_FALSE(rss_osd_parse_font_size("%", &px, &pct));
    ASSERT_FALSE(rss_osd_parse_font_size("24px", &px, &pct));
    ASSERT_FALSE(rss_osd_parse_font_size("24.5", &px, &pct));
    ASSERT_FALSE(rss_osd_parse_font_size("-4%", &px, &pct));
    ASSERT_FALSE(rss_osd_parse_font_size("", &px, &pct));
    ASSERT_FALSE(rss_osd_parse_font_size(NULL, &px, &pct));

    /* And a refusal leaves nothing behind for a caller that does not
     * check: zero in both is "no size given", which is what an element
     * without one is. */
    ASSERT_EQ(0, px);
    ASSERT_EQ(0, pct);
    PASS();
}

SUITE(osd_suite)
{
	RUN_TEST(osd_create_open);
	RUN_TEST(osd_publish_read);
	RUN_TEST(osd_dirty_flag);
	RUN_TEST(osd_double_buffer);
	RUN_TEST(osd_heartbeat);
    RUN_TEST(osd_font_size_spelling);
}

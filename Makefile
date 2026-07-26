CC      ?= gcc
AR      ?= ar
CFLAGS  := -Wall -Wextra -Werror -std=c11 -D_GNU_SOURCE -Os -fPIC
CFLAGS  += -ffunction-sections -fdata-sections
CFLAGS  += -fno-asynchronous-unwind-tables -fmerge-all-constants -fno-ident
CFLAGS  += -Iinclude
SRCS    := src/rss_ring.c src/rss_osd_shm.c src/rss_ctrl.c src/rss_ipc_log.c
OBJS    := $(SRCS:.c=.o)
LIB_SO  := librss_ipc.so
LIB_A   := librss_ipc.a

all: $(LIB_SO) $(LIB_A)

$(LIB_SO): $(OBJS)
	$(CC) -shared -Wl,-soname,librss_ipc.so -Wl,-Bsymbolic -Wl,--gc-sections -o $@ $^

$(LIB_A): $(OBJS)
	$(AR) rcs $@ $^

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	# .built is the stamp raptor/Makefile builds this directory through, and
	# must not outlive the library it stands for.
	rm -f $(OBJS) $(LIB_SO) $(LIB_A) .built

.PHONY: all clean

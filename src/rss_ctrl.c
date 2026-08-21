/*
 * rss_ctrl.c -- Control socket implementation.
 *
 * Wire protocol: 2-byte big-endian length prefix + JSON body.
 *
 *   +--------+--------+-----...-----+
 *   | len_hi | len_lo |  JSON body  |
 *   +--------+--------+-----...-----+
 *      2 bytes           len bytes
 *
 * The protocol is synchronous: one request, one response per connection.
 * raptorctl connects, sends a request, reads the response, and closes.
 */

#include "rss_ipc.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <sys/un.h>
#include <unistd.h>

#define RSS_CTRL_MAX_MSG 65535 /* maximum JSON body size */
#define RSS_CTRL_BACKLOG 5

struct rss_ctrl {
    int listen_fd;
    char sock_path[108]; /* sun_path limit */
};

/* ------------------------------------------------------------------ */
/*  Helpers                                                           */
/* ------------------------------------------------------------------ */

static int64_t mono_ms(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

/*
 * Connect, bounded by the caller's timeout.
 *
 * A blocking connect() to a unix socket does not fail when the daemon has
 * stopped answering -- it waits. The listener is still there and the socket
 * still exists, so the kernel queues the connection; once RSS_CTRL_BACKLOG of
 * them are queued and unaccepted, the next connect() blocks with no timeout of
 * any kind, and every caller here inherits that.
 *
 * That is worse than the failure it looks like, because the timeout every
 * caller passes covers only the read. A daemon that is running, listening and
 * not accepting -- stopped, wedged, or busy in something long -- would hold
 * the caller indefinitely on a call it believed was bounded, and rcd's serve
 * loop is single-threaded, so holding rcd holds the camera's whole
 * configuration interface.
 *
 * Non-blocking and retried instead. On AF_UNIX a full backlog is EAGAIN rather
 * than EINPROGRESS, and there is nothing to poll for -- no event fires when a
 * queue slot frees -- so the wait is a short sleep and another attempt until
 * the deadline. EINPROGRESS is handled anyway: it does not arise for unix
 * sockets today and costs four lines to not depend on that.
 */
#define CTRL_CONNECT_RETRY_MS 20

static int ctrl_connect(int fd, const struct sockaddr_un *addr, uint32_t timeout_ms)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0)
        return -errno;
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
        return -errno;

    int64_t deadline = mono_ms() + (int64_t)timeout_ms;
    int rc = -ETIMEDOUT;

    for (;;) {
        if (connect(fd, (const struct sockaddr *)addr, sizeof(*addr)) == 0) {
            rc = 0;
            break;
        }

        int err = errno;

        if (err == EINTR)
            continue;
        if (err == EISCONN) {
            rc = 0;
            break;
        }

        if (err == EINPROGRESS) {
            int left = (int)(deadline - mono_ms());
            struct pollfd pfd = {.fd = fd, .events = POLLOUT};
            int pr = poll(&pfd, 1, left > 0 ? left : 0);

            if (pr <= 0) {
                rc = pr < 0 ? -errno : -ETIMEDOUT;
                break;
            }

            int soerr = 0;
            socklen_t slen = sizeof(soerr);

            if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &soerr, &slen) < 0) {
                rc = -errno;
                break;
            }
            rc = soerr ? -soerr : 0;
            break;
        }

        if (err != EAGAIN) {
            rc = -err;
            break;
        }

        /* Backlog full. Nothing to wait on, so wait a little and ask again. */
        if (mono_ms() >= deadline) {
            rc = -ETIMEDOUT;
            break;
        }
        poll(NULL, 0, CTRL_CONNECT_RETRY_MS);
    }

    /*
     * Blocking again either way. Everything below this expects it, and the
     * timeouts they use are their own.
     */
    if (fcntl(fd, F_SETFL, flags) < 0 && rc == 0)
        rc = -errno;

    return rc;
}

/*
 * Read exactly `count` bytes from fd into buf, against an absolute deadline.
 *
 * A deadline rather than a timeout per poll, which is what this used to take.
 * The difference does not show against a peer that says nothing -- that costs
 * one timeout either way -- but a peer that keeps sending, slowly, restarted
 * the clock with every byte it sent. Measured: a 300 ms budget against a peer
 * writing one byte every 250 ms returned after 10.8 seconds, thirty-six times
 * the budget, and returned *successfully*. It scales with the length of the
 * message, so a full-sized reply is thousands of times the budget rather than
 * tens.
 *
 * `deadline_ms` of 0 means no deadline, which is what a timeout of 0 has
 * always meant here.
 *
 * Returns 0 on success, -errno on error, -ECONNRESET on EOF.
 */
static int read_exact(int fd, void *buf, size_t count, int64_t deadline_ms)
{
    uint8_t *p = buf;
    size_t remaining = count;

    while (remaining > 0) {
        if (deadline_ms > 0) {
            struct pollfd pfd = {.fd = fd, .events = POLLIN};
            int pr;

            /* Recomputed every time round, including after a signal, so
             * neither a slow sender nor a stream of EINTR buys more time. */
            for (;;) {
                int64_t left = deadline_ms - mono_ms();

                if (left <= 0)
                    return -ETIMEDOUT;
                pr = poll(&pfd, 1, (int)left);
                if (pr >= 0 || errno != EINTR)
                    break;
            }

            if (pr < 0)
                return -errno;
            if (pr == 0)
                return -ETIMEDOUT;
        }

        ssize_t n;
        do {
            n = read(fd, p, remaining);
        } while (n < 0 && errno == EINTR);

        if (n < 0)
            return -errno;
        if (n == 0)
            return -ECONNRESET;

        p += n;
        remaining -= (size_t)n;
    }

    return 0;
}

/*
 * Write exactly `count` bytes from buf to fd.
 * Returns 0 on success, -errno on error.
 */
static int write_exact(int fd, const void *buf, size_t count)
{
    const uint8_t *p = buf;
    size_t remaining = count;

    while (remaining > 0) {
        ssize_t n;
        do {
            n = write(fd, p, remaining);
        } while (n < 0 && errno == EINTR);

        if (n < 0)
            return -errno;

        p += n;
        remaining -= (size_t)n;
    }

    return 0;
}

/*
 * Read a length-prefixed message from fd.
 * Allocates the message buffer; caller must free().
 * Returns message length on success, -errno on error.
 */
static int read_message(int fd, char **out_buf, uint32_t timeout_ms)
{
    /* One deadline for the whole message. The length and the body are two
     * reads of one thing, and giving each its own budget is what let a
     * trickling peer hold a caller for a multiple of it. */
    int64_t deadline = timeout_ms > 0 ? mono_ms() + (int64_t)timeout_ms : 0;

    uint8_t len_buf[2];
    int ret = read_exact(fd, len_buf, 2, deadline);
    if (ret < 0)
        return ret;

    uint16_t msg_len = ((uint16_t)len_buf[0] << 8) | len_buf[1];
    if (msg_len == 0) {
        *out_buf = NULL;
        return 0;
    }

    char *buf = malloc((size_t)msg_len + 1);
    if (!buf)
        return -ENOMEM;

    ret = read_exact(fd, buf, msg_len, deadline);
    if (ret < 0) {
        free(buf);
        return ret;
    }

    buf[msg_len] = '\0';
    *out_buf = buf;
    return msg_len;
}

/*
 * Write a length-prefixed message to fd.
 * Returns 0 on success, -errno on error.
 */
static int write_message(int fd, const char *buf, size_t len)
{
    if (len > RSS_CTRL_MAX_MSG)
        return -EMSGSIZE;

    uint8_t len_buf[2];
    len_buf[0] = (uint8_t)(len >> 8);
    len_buf[1] = (uint8_t)(len & 0xFF);

    int ret = write_exact(fd, len_buf, 2);
    if (ret < 0)
        return ret;

    return write_exact(fd, buf, len);
}

/* ------------------------------------------------------------------ */
/*  Server API (daemon side)                                          */
/* ------------------------------------------------------------------ */

rss_ctrl_t *rss_ctrl_listen(const char *sock_path)
{
    if (!sock_path)
        return NULL;

    rss_ctrl_t *ctrl = calloc(1, sizeof(*ctrl));
    if (!ctrl)
        return NULL;

    ctrl->listen_fd = -1;
    snprintf(ctrl->sock_path, sizeof(ctrl->sock_path), "%s", sock_path);

    /* Remove stale socket file. */
    unlink(sock_path);

    ctrl->listen_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctrl->listen_fd < 0) {
        RSS_IPC_ERROR("ctrl_listen %s: socket: %s", sock_path, strerror(errno));
        goto fail;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", sock_path);

    /* Set umask to 0 around bind so the socket is created with 0666
     * atomically — no window where umask-derived restrictive permissions
     * would prevent other daemons from connecting. */
    mode_t old_umask = umask(0);
    int bind_ret = bind(ctrl->listen_fd, (struct sockaddr *)&addr, sizeof(addr));
    umask(old_umask);
    if (bind_ret < 0) {
        RSS_IPC_ERROR("ctrl_listen %s: bind: %s", sock_path, strerror(errno));
        goto fail;
    }

    if (listen(ctrl->listen_fd, RSS_CTRL_BACKLOG) < 0) {
        RSS_IPC_ERROR("ctrl_listen %s: listen: %s", sock_path, strerror(errno));
        goto fail;
    }

    /* Non-blocking listener: accept() must never park a daemon. An
     * event loop can dispatch a read handler spuriously (live555's
     * select path falls through with stale fd_sets on EINTR), and a
     * blocking accept() with no pending connection then sleeps
     * forever — immune to further signals via its EINTR retry. */
    fcntl(ctrl->listen_fd, F_SETFL, fcntl(ctrl->listen_fd, F_GETFL, 0) | O_NONBLOCK);

    return ctrl;

fail:
    if (ctrl->listen_fd >= 0)
        close(ctrl->listen_fd);
    free(ctrl);
    return NULL;
}

void rss_ctrl_destroy(rss_ctrl_t *ctrl)
{
    if (!ctrl)
        return;

    if (ctrl->listen_fd >= 0)
        close(ctrl->listen_fd);

    unlink(ctrl->sock_path);
    free(ctrl);
}

int rss_ctrl_get_fd(rss_ctrl_t *ctrl)
{
    return ctrl ? ctrl->listen_fd : -1;
}

int rss_ctrl_accept_and_handle(rss_ctrl_t *ctrl,
                               int (*handler)(const char *cmd_json, char *resp_buf,
                                              int resp_buf_size, void *userdata),
                               void *userdata)
{
    if (!ctrl || !handler)
        return -EINVAL;

    int client_fd;
    do {
        client_fd = accept(ctrl->listen_fd, NULL, NULL);
    } while (client_fd < 0 && errno == EINTR);

    if (client_fd < 0)
        return -errno;

    /* Set write timeout so a slow/stalled client can't block the daemon
     * indefinitely. The read side is bounded by the deadline in read_exact. */
    struct timeval snd_tv = {.tv_sec = 5, .tv_usec = 0};
    (void)setsockopt(client_fd, SOL_SOCKET, SO_SNDTIMEO, &snd_tv, sizeof(snd_tv));

    int ret = -1;
    char *cmd = NULL;
    char *resp_buf = NULL;

    /*
     * Read the request, bounded at five seconds for the whole message rather
     * than for each poll of it. It used to be per poll, excused on the
     * grounds that the socket was root-only and local -- which was never true
     * of this socket: it is created 0666 on purpose, a few lines above, so
     * that a non-root client can use it. A local process trickling one byte
     * every 4.9 s could hold a daemon's serve loop for as long as it cared
     * to, and rcd's loop is the camera's whole configuration interface.
     */
    int msg_len = read_message(client_fd, &cmd, 5000);
    if (msg_len < 0) {
        ret = msg_len;
        goto done;
    }

    if (!cmd || msg_len == 0) {
        ret = -EPROTO;
        goto done;
    }

    /* Invoke the handler callback. Heap-allocated — 64KB exceeds what
     * MIPS can allocate in a single stack frame adjustment. */
    resp_buf = malloc(RSS_CTRL_MAX_MSG);
    if (!resp_buf) {
        ret = -ENOMEM;
        goto done;
    }
    int resp_len = handler(cmd, resp_buf, RSS_CTRL_MAX_MSG, userdata);

    if (resp_len < 0) {
        /* Handler returned an error -- send a generic error response. */
        resp_len = snprintf(resp_buf, RSS_CTRL_MAX_MSG,
                            "{\"status\":\"error\",\"code\":%d,"
                            "\"msg\":\"handler error\"}",
                            resp_len);
    }

    /* Write the response. */
    ret = write_message(client_fd, resp_buf, (size_t)resp_len);

done:
    free(resp_buf);
    free(cmd);
    close(client_fd);
    return ret;
}

/* ------------------------------------------------------------------ */
/*  Client API (raptorctl side)                                       */
/* ------------------------------------------------------------------ */

/*
 * The transport, shared by both entry points below: connect, send, and read
 * the whole reply onto the heap. `*out` is the caller's on success.
 */
static int ctrl_exchange(const char *sock_path, const char *cmd_json, char **out,
                         uint32_t timeout_ms)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0)
        return -errno;

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", sock_path);

    uint32_t budget = timeout_ms ? timeout_ms : 5000;
    int cerr = ctrl_connect(fd, &addr, budget);

    if (cerr < 0) {
        close(fd);
        return cerr;
    }

    /*
     * And the write, for the same reason: a peer that never reads will fill
     * the socket buffer, and write_exact() would sit in it. Small messages
     * almost never get that far, which is exactly why it would be the one
     * that is never noticed.
     */
    struct timeval snd = {.tv_sec = budget / 1000, .tv_usec = (budget % 1000) * 1000};

    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &snd, sizeof(snd));

    size_t cmd_len = strlen(cmd_json);
    int ret = write_message(fd, cmd_json, cmd_len);
    if (ret < 0) {
        close(fd);
        return ret;
    }

    /* Shutdown write side to signal end of request. */
    shutdown(fd, SHUT_WR);

    /* Read the response. */
    char *resp = NULL;
    int rlen = read_message(fd, &resp, budget);
    close(fd);

    if (rlen < 0)
        return rlen;

    if (!resp || rlen == 0) {
        free(resp);
        return -EPROTO;
    }

    *out = resp;
    return rlen;
}

int rss_ctrl_send_command(const char *sock_path, const char *cmd_json, char *resp_buf,
                          int resp_buf_size, uint32_t timeout_ms)
{
    if (!sock_path || !cmd_json || !resp_buf || resp_buf_size <= 0)
        return -EINVAL;

    char *resp = NULL;
    int rlen = ctrl_exchange(sock_path, cmd_json, &resp, timeout_ms);
    if (rlen < 0)
        return rlen;

    /*
     * Copy into the caller's buffer, truncating if it does not fit.
     *
     * Truncating silently is deliberate rather than merely old: the common
     * use of this call is a liveness probe that passes a small buffer and
     * cares only that something answered, and rvd's status reply is far
     * larger than any of them. Returning an error for a short buffer would
     * turn every one of those probes into a false negative.
     *
     * The cost is that a caller which does parse the reply gets invalid JSON
     * with nothing about it to show that it was cut. Such a caller should use
     * rss_ctrl_send_command_alloc() instead, and must whenever the reply's
     * size is not known in advance.
     */
    int copy_len = rlen < resp_buf_size - 1 ? rlen : resp_buf_size - 1;
    memcpy(resp_buf, resp, (size_t)copy_len);
    resp_buf[copy_len] = '\0';
    free(resp);

    return copy_len;
}

int rss_ctrl_send_command_alloc(const char *sock_path, const char *cmd_json, char **resp_out,
                                uint32_t timeout_ms)
{
    if (!sock_path || !cmd_json || !resp_out)
        return -EINVAL;

    *resp_out = NULL;
    return ctrl_exchange(sock_path, cmd_json, resp_out, timeout_ms);
}

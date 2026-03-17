#include <sys/socket.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <assert.h>
#include <stdio.h>
#include <inttypes.h>
#include <stdint.h>
#include <time.h>

#ifdef DEBUG
#include <unistd.h>
#define debug(...) \
    do { \
        time_t now = time(NULL); \
        char t[30]; \
        ctime_r(&now, t); \
        t[24] = '\0'; \
        fprintf(stderr, "[greenify] [%s] [%d] ", t, getpid()); \
        fprintf(stderr, __VA_ARGS__); \
    } while(0)
#else  /* #define DEBUG */
#define debug(...)
#endif /* #define DEBUG */

#include "libgreenify.h"

#define EVENT_READ 0x01
#define EVENT_WRITE 0x02

static greenify_wait_callback_func_t g_wait_callback = NULL;

/* return 1 means the flags changed */
static int
set_nonblock(int fd, int *old_flags)
{
    *old_flags = fcntl(fd, F_GETFL, 0);
    if ((*old_flags) & O_NONBLOCK) {
        return 0;
    } else {
        fcntl(fd, F_SETFL, *old_flags | O_NONBLOCK);
        return 1;
    }
}

static void
restore_flags(int fd, int flags)
{
    fcntl(fd, F_SETFL, flags);
}

void greenify_set_wait_callback(greenify_wait_callback_func_t callback)
{
    g_wait_callback = callback;
}

int callback_multiple_watchers(struct greenify_watcher* watchers, int nwatchers, int timeout)
{
    int retval;
    assert(g_wait_callback != NULL);
    retval = g_wait_callback(watchers, nwatchers, timeout);
    return retval;
}

int callback_single_watcher(int fd, int events, int timeout)
{
    struct greenify_watcher watchers[1];
    int retval;

    assert(g_wait_callback != NULL);

    watchers[0].fd = fd;
    watchers[0].events = events;
    retval = g_wait_callback(watchers, 1, timeout);
    return retval;
}
static int
is_not_socket(int fd) 
{
    int opt;
    socklen_t len = sizeof(opt);
    if(-1 == getsockopt(fd, SOL_SOCKET, SO_DEBUG, &opt, &len) && errno == ENOTSOCK)
    {
        errno = 0;
        return 1;
    }
    return 0;
}

int
green_connect(int socket, const struct sockaddr *address, socklen_t address_len)
{
    int flags, s_err, retval;

    debug("Enter green_connect\n");

    if (g_wait_callback == NULL || !set_nonblock(socket, &flags)) {
        retval = connect(socket, address, address_len);
        return retval;
    }

    retval = connect(socket, address, address_len);
    s_err = errno;
    if (retval < 0 && (s_err == EWOULDBLOCK || s_err == EALREADY || s_err == EINPROGRESS)) {
        callback_single_watcher(socket, EVENT_WRITE, 0);
        getsockopt(socket, SOL_SOCKET, SO_ERROR, &s_err, &address_len);
        retval = s_err ? -1 : 0;
    }

    restore_flags(socket, flags);
    errno = s_err;
    return retval;
}


#define _IMPL_SOCKET_ONLY_GREEN_FN(FN, EV, FID, ...) \
    do { \
        int flags, s_err; \
        ssize_t retval; \
        \
        debug("Enter green_" #FN "\n"); \
        \
        if (g_wait_callback == NULL || is_not_socket((FID)) || !set_nonblock((FID), &flags)) { \
            return FN((FID), __VA_ARGS__); \
        } \
        \
        do { \
            retval = FN((FID), __VA_ARGS__); \
            s_err = errno; \
            debug(#FN ", return %zd, errno: %d\n", retval, s_err); \
        } while (retval < 0 && (s_err == EWOULDBLOCK || s_err == EAGAIN) \
                && !(retval = callback_single_watcher((FID), (EV), 0))); \
        \
        restore_flags((FID), flags); \
        errno = s_err; \
        return retval; \
    } while(0)

ssize_t green_read(int fildes, void *buf, size_t nbyte)
{
    _IMPL_SOCKET_ONLY_GREEN_FN(read, EVENT_READ, fildes, buf, nbyte);
}

ssize_t green_write(int fildes, const void *buf, size_t nbyte)
{
    _IMPL_SOCKET_ONLY_GREEN_FN(write, EVENT_WRITE, fildes, buf, nbyte);
}

ssize_t green_pread(int fd, void *buf, size_t count, off_t offset)
{
    _IMPL_SOCKET_ONLY_GREEN_FN(pread, EVENT_READ, fd, buf, count, offset);
}

ssize_t green_pwrite(int fd, const void *buf, size_t count, off_t offset)
{
    _IMPL_SOCKET_ONLY_GREEN_FN(pwrite, EVENT_WRITE, fd, buf, count, offset);
}

ssize_t green_readv(int fd, const struct iovec *iov, int iovcnt)
{
    _IMPL_SOCKET_ONLY_GREEN_FN(readv, EVENT_READ, fd, iov, iovcnt);
}

ssize_t green_writev(int fd, const struct iovec *iov, int iovcnt)
{
    _IMPL_SOCKET_ONLY_GREEN_FN(writev, EVENT_WRITE, fd, iov, iovcnt);
}

#undef _IMPL_SOCKET_ONLY_GREEN_FN


#define _IMPL_GREEN_FN(FN, EV, FID, ...) \
    do { \
        int sock_flags, s_err; \
        ssize_t retval; \
        \
        debug("Enter green_" #FN "\n"); \
        \
        if (g_wait_callback == NULL || !set_nonblock((FID), &sock_flags)) { \
            return FN((FID), __VA_ARGS__); \
        } \
        \
        do { \
            retval = FN((FID), __VA_ARGS__); \
            s_err = errno; \
            debug(#FN ", return %zd, errno: %d\n", retval, s_err); \
        } while (retval < 0 && (s_err == EWOULDBLOCK || s_err == EAGAIN) \
                && !(retval = callback_single_watcher((FID), (EV), 0))); \
        \
        restore_flags((FID), sock_flags); \
        errno = s_err; \
        return retval; \
    } while(0)

ssize_t green_recv(int socket, void *buffer, size_t length, int flags) {
    _IMPL_GREEN_FN(recv, EVENT_READ, socket, buffer, length, flags);
}

ssize_t green_send(int socket, const void *buffer, size_t length, int flags) {
    _IMPL_GREEN_FN(send, EVENT_WRITE, socket, buffer, length, flags);
}

ssize_t green_recvmsg(int socket, struct msghdr *message, int flags) {
    _IMPL_GREEN_FN(recvmsg, EVENT_READ, socket, message, flags);
}

ssize_t green_sendmsg(int socket, const struct msghdr* message, int flags) {
    _IMPL_GREEN_FN(sendmsg, EVENT_WRITE, socket, message, flags);
}

ssize_t green_recvfrom(int sockfd, void *buf, size_t len, int flags,
        struct sockaddr *src_addr, socklen_t *addrlen)
{
    _IMPL_GREEN_FN(recvfrom, EVENT_READ, sockfd, buf, len, flags, src_addr, addrlen);
}

ssize_t green_sendto(int sockfd, const void *buf, size_t len, int flags,
        const struct sockaddr *dest_addr, socklen_t addrlen)
{
    _IMPL_GREEN_FN(sendto, EVENT_WRITE, sockfd, buf, len, flags, dest_addr, addrlen);
}

#undef _IMPL_GREEN_FN

int
green_select(int nfds, fd_set *readfds, fd_set *writefds, fd_set *exceptfds, struct timeval *timeout)
{
    struct greenify_watcher watchers[nfds];
    int count = 0, i = 0;

    debug("Enter green_select\n");
    if (g_wait_callback == NULL)
        return select(nfds, readfds, writefds, exceptfds, timeout);

    for (i = 0; i < nfds; ++i) {
        if (FD_ISSET(i, readfds) || FD_ISSET(i, exceptfds)) {
            watchers[count].fd = i;
            watchers[count].events = EVENT_READ;
            count++;
        }
        if (FD_ISSET(i, writefds)) {
            watchers[count].fd = i;
            watchers[count].events = EVENT_WRITE;
            count++;
        }
    }

    float timeout_in_ms = timeout->tv_usec / 1000.0;
    callback_multiple_watchers(watchers, count, timeout_in_ms);
    return select(nfds, readfds, writefds, exceptfds, timeout);
}

#ifndef NO_POLL
int
green_poll(struct pollfd *fds, nfds_t nfds, int timeout)
{
    nfds_t i;
    struct greenify_watcher watchers[nfds];

    debug("Enter green_poll\n");

    if (g_wait_callback == NULL || timeout == 0)
        return poll(fds, nfds, timeout);

    for (i = 0; i < nfds; i++) {
        if (fds[i].events & ~(POLLIN | POLLPRI | POLLOUT)) {
            fprintf(stderr, "[greenify] support POLLIN|POLLPRI|POLLOUT only, got 0x%x, may block.\n",
                    fds[i].events);
            return poll(fds, nfds, timeout);
        }

        watchers[i].fd = fds[i].fd;
        watchers[i].events = 0;

        if (fds[i].events & POLLIN || fds[i].events & POLLPRI) {
            watchers[i].events |= EVENT_READ;
        }

        if (fds[i].events & POLLOUT) {
            watchers[i].events |= EVENT_WRITE;
        }
    }

    callback_multiple_watchers(watchers, nfds, timeout);
    return poll(fds, nfds, 0);
}
#endif

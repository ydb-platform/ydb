/*
 * Copyright (c) 2008-2018 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2009 Sandia National Laboratories. All rights reserved.
 * Copyright (c) 2017      Mellanox Technologies. All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif
#ifdef HAVE_ARPA_INET_H
#include <arpa/inet.h>
#endif
#ifdef HAVE_NETINET_IN_H
#include <netinet/in.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>

#include "opal/util/fd.h"
#include "opal/constants.h"


/*
 * Simple loop over reading from a fd
 */
int opal_fd_read(int fd, int len, void *buffer)
{
    int rc;
    char *b = buffer;

    while (len > 0) {
        rc = read(fd, b, len);
        if (rc < 0 && (EAGAIN == errno || EINTR == errno)) {
            continue;
        } else if (rc > 0) {
            len -= rc;
            b += rc;
        } else if (0 == rc) {
            return OPAL_ERR_TIMEOUT;
        } else {
            return OPAL_ERR_IN_ERRNO;
        }
    }
    return OPAL_SUCCESS;
}


/*
 * Simple loop over writing to an fd
 */
int opal_fd_write(int fd, int len, const void *buffer)
{
    int rc;
    const char *b = buffer;

    while (len > 0) {
        rc = write(fd, b, len);
        if (rc < 0 && (EAGAIN == errno || EINTR == errno)) {
            continue;
        } else if (rc > 0) {
            len -= rc;
            b += rc;
        } else {
            return OPAL_ERR_IN_ERRNO;
        }
    }

    return OPAL_SUCCESS;
}


int opal_fd_set_cloexec(int fd)
{
#ifdef FD_CLOEXEC
    int flags;

    /* Stevens says that we should get the fd's flags before we set
       them.  So say we all. */
    flags = fcntl(fd, F_GETFD, 0);
    if (-1 == flags) {
        return OPAL_ERR_IN_ERRNO;
    }

    if (fcntl(fd, F_SETFD, FD_CLOEXEC | flags) == -1) {
        return OPAL_ERR_IN_ERRNO;
    }
#endif

    return OPAL_SUCCESS;
}

bool opal_fd_is_regular(int fd)
{
    struct stat buf;
    if (fstat(fd, &buf)) {
        return false;
    }
    return S_ISREG(buf.st_mode);
}

bool opal_fd_is_chardev(int fd)
{
    struct stat buf;
    if (fstat(fd, &buf)) {
        return false;
    }
    return S_ISCHR(buf.st_mode);
}

bool opal_fd_is_blkdev(int fd)
{
    struct stat buf;
    if (fstat(fd, &buf)) {
        return false;
    }
    return S_ISBLK(buf.st_mode);
}

const char *opal_fd_get_peer_name(int fd)
{
    char *str;
    const char *ret = NULL;
    struct sockaddr sa;
    socklen_t slt = (socklen_t) sizeof(sa);

    int rc = getpeername(fd, &sa, &slt);
    if (0 != rc) {
        ret = strdup("Unknown");
        return ret;
    }

    size_t len = INET_ADDRSTRLEN;
#if OPAL_ENABLE_IPV6
    len = INET6_ADDRSTRLEN;
#endif
    str = calloc(1, len);
    if (NULL == str) {
        return NULL;
    }

    if (sa.sa_family == AF_INET) {
        struct sockaddr_in *si;
        si = (struct sockaddr_in*) &sa;
        ret = inet_ntop(AF_INET, &(si->sin_addr), str, INET_ADDRSTRLEN);
        if (NULL == ret) {
            free(str);
        }
    }
#if OPAL_ENABLE_IPV6
    else if (sa.sa_family == AF_INET6) {
        struct sockaddr_in6 *si6;
        si6 = (struct sockaddr_in6*) &sa;
        ret = inet_ntop(AF_INET6, &(si6->sin6_addr), str, INET6_ADDRSTRLEN);
        if (NULL == ret) {
            free(str);
        }
    }
#endif
    else {
        // This string is guaranteed to be <= INET_ADDRSTRLEN
        strncpy(str, "Unknown", len);
        ret = str;
    }

    return ret;
}

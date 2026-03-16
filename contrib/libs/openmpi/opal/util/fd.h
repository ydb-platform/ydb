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

/* @file */

#ifndef OPAL_UTIL_FD_H_
#define OPAL_UTIL_FD_H_

#include "opal_config.h"

BEGIN_C_DECLS

/**
 * Read a complete buffer from a file descriptor.
 *
 * @param fd File descriptor
 * @param len Number of bytes to read
 * @param buffer Pre-allocated buffer (large enough to hold len bytes)
 *
 * @returns OPAL_SUCCESS upon success.
 * @returns OPAL_ERR_TIMEOUT if the fd closes before reading the full amount.
 * @returns OPAL_ERR_IN_ERRNO otherwise.
 *
 * Loop over reading from the fd until len bytes are read or an error
 * occurs.  EAGAIN and EINTR are transparently handled.
 */
OPAL_DECLSPEC int opal_fd_read(int fd, int len, void *buffer);

/**
 * Write a complete buffer to a file descriptor.
 *
 * @param fd File descriptor
 * @param len Number of bytes to write
 * @param buffer Buffer to write from
 *
 * @returns OPAL_SUCCESS upon success.
 * @returns OPAL_ERR_IN_ERRNO otherwise.
 *
 * Loop over writing to the fd until len bytes are written or an error
 * occurs.  EAGAIN and EINTR are transparently handled.
 */
OPAL_DECLSPEC int opal_fd_write(int fd, int len, const void *buffer);

/**
 * Convenience function to set a file descriptor to be close-on-exec.
 *
 * @param fd File descriptor
 *
 * @returns OPAL_SUCCESS upon success (or if the system does not
 * support close-on-exec behavior).
 * @returns OPAL_ERR_IN_ERRNO otherwise.
 *
 * This is simply a convenience function because there's a few steps
 * to setting a file descriptor to be close-on-exec.
 */
OPAL_DECLSPEC int opal_fd_set_cloexec(int fd);

/**
 * Convenience function to check if fd point to an accessible regular file.
 *
 * @param fd File descriptor
 *
 * @returns true if "fd" points to a regular file.
 * @returns false otherwise.
 */
OPAL_DECLSPEC bool opal_fd_is_regular(int fd);

/**
 * Convenience function to check if fd point to an accessible character device.
 *
 * @param fd File descriptor
 *
 * @returns true if "fd" points to a regular file.
 * @returns false otherwise.
 */
OPAL_DECLSPEC bool opal_fd_is_chardev(int fd);

/**
 * Convenience function to check if fd point to an accessible block device.
 *
 * @param fd File descriptor
 *
 * @returns true if "fd" points to a regular file.
 * @returns false otherwise.
 */
OPAL_DECLSPEC bool opal_fd_is_blkdev(int fd);

/**
 * Convenience function to get a string name of the peer on the other
 * end of this internet socket.
 *
 * @param fd File descriptor of an AF_INET/AF_INET6 socket
 *
 * @returns resolvable IP name, or "a.b.c.d".  This string must be freed by the caller.
 */
OPAL_DECLSPEC const char *opal_fd_get_peer_name(int fd);

END_C_DECLS

#endif

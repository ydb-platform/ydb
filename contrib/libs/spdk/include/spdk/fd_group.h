/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * \file
 * File descriptor group utility functions
 */

#ifndef SPDK_FD_GROUP_H
#define SPDK_FD_GROUP_H

#include "spdk/stdinc.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Callback function registered for the event source file descriptor.
 *
 * \param ctx Context passed as arg to spdk_fd_group_add().
 *
 * \return 0 to indicate that event notification took place but no events were found;
 * positive to indicate that event notification took place and some events were processed;
 * negative if no event information is provided.
 */
typedef int (*spdk_fd_fn)(void *ctx);

/**
 * A file descriptor group of event sources which gather the events to an epoll instance.
 *
 * Taking "fgrp" as short name for file descriptor group of event sources.
 */
struct spdk_fd_group;

/**
 * Initialize one fd_group.
 *
 * \param fgrp A pointer to return the initialized fgrp.
 *
 * \return 0 if success or -errno if failed
 */
int spdk_fd_group_create(struct spdk_fd_group **fgrp);

/**
 * Release all resources associated with this fgrp.
 *
 * Users need to remove all event sources from the fgrp before destroying it.
 *
 * \param fgrp The fgrp to destroy.
 */
void spdk_fd_group_destroy(struct spdk_fd_group *fgrp);

/**
 * Wait for new events generated inside fgrp, and process them with their
 * registered spdk_fd_fn.
 *
 * \param fgrp The fgrp to wait and process.
 * \param timeout Specifies the number of milliseconds that will block.
 * -1 causes indefinitedly blocking; 0 causes immediately return.
 *
 * \return 0 if any events get processed
 * or -errno if failed
 */
int spdk_fd_group_wait(struct spdk_fd_group *fgrp, int timeout);

/**
 * Return the internal epoll_fd of specific fd_group
 *
 * \param fgrp The pointer of specified fgrp.
 *
 * \return The epoll_fd of specific fgrp.
 */
int spdk_fd_group_get_fd(struct spdk_fd_group *fgrp);

/**
 * Register one event source to specified fgrp.
 *
 * \param fgrp The fgrp registered to.
 * \param efd File descriptor of the event source.
 * \param fn Called each time there are events in event source.
 * \param arg Function argument for fn.
 *
 * \return 0 if success or -errno if failed
 */
int spdk_fd_group_add(struct spdk_fd_group *fgrp,
		      int efd, spdk_fd_fn fn, void *arg);

/**
 * Unregister one event source from one fgrp.
 *
 * \param fgrp The fgrp registered to.
 * \param efd File descriptor of the event source.
 */
void spdk_fd_group_remove(struct spdk_fd_group *fgrp, int efd);

/**
 * Change the event notification types associated with the event source.
 *
 * Modules like nbd, need this api to add EPOLLOUT when having data to send, and remove EPOLLOUT if no data to send.
 *
 * \param fgrp The fgrp registered to.
 * \param efd File descriptor of the event source.
 * \param event_types The event notification types.
 *
 * \return 0 if success or -errno if failed
 */
int spdk_fd_group_event_modify(struct spdk_fd_group *fgrp,
			       int efd, int event_types);

#ifdef __cplusplus
}
#endif

#endif /* SPDK_FD_GROUP_H */

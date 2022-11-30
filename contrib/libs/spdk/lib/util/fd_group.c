#include <contrib/libs/spdk/ndebug.h>
/*-
 *   BSD LICENSE
 *
 *   Copyright(c) Intel Corporation. All rights reserved.
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

#include "spdk/queue.h"
#include "spdk/log.h"
#include "spdk/fd_group.h"

#ifdef __linux__
#include <sys/epoll.h>
#endif

enum event_handler_state {
	/* The event_handler is added into an fd_group waiting for event,
	 * but not currently in the execution of a wait loop.
	 */
	EVENT_HANDLER_STATE_WAITING,

	/* The event_handler is currently in the execution of a wait loop. */
	EVENT_HANDLER_STATE_RUNNING,

	/* The event_handler was removed during the execution of a wait loop. */
	EVENT_HANDLER_STATE_REMOVED,
};

/* file descriptor of the interrupt event */

/* Taking "ehdlr" as short name for file descriptor handler of the interrupt event. */
struct event_handler {
	TAILQ_ENTRY(event_handler)	next;
	enum event_handler_state	state;

	spdk_fd_fn			fn;
	void				*fn_arg;
	/* file descriptor of the interrupt event */
	int				fd;
};

struct spdk_fd_group {
	int epfd;
	int num_fds; /* Number of fds registered in this group. */

	/* interrupt sources list */
	TAILQ_HEAD(, event_handler) event_handlers;
};

int
spdk_fd_group_get_fd(struct spdk_fd_group *fgrp)
{
	return fgrp->epfd;
}

#ifdef __linux__

int
spdk_fd_group_add(struct spdk_fd_group *fgrp,
		  int efd, spdk_fd_fn fn, void *arg)
{
	struct event_handler *ehdlr = NULL;
	struct epoll_event epevent = {0};
	int rc;

	/* parameter checking */
	if (fgrp == NULL || efd < 0 || fn == NULL) {
		return -EINVAL;
	}

	/* check if there is already one function registered for this fd */
	TAILQ_FOREACH(ehdlr, &fgrp->event_handlers, next) {
		if (ehdlr->fd == efd) {
			return -EEXIST;
		}
	}

	/* create a new event src */
	ehdlr = calloc(1, sizeof(*ehdlr));
	if (ehdlr == NULL) {
		return -errno;
	}

	ehdlr->fd = efd;
	ehdlr->fn = fn;
	ehdlr->fn_arg = arg;
	ehdlr->state = EVENT_HANDLER_STATE_WAITING;

	epevent.events = EPOLLIN;
	epevent.data.ptr = ehdlr;
	rc = epoll_ctl(fgrp->epfd, EPOLL_CTL_ADD, efd, &epevent);
	if (rc < 0) {
		free(ehdlr);
		return -errno;
	}

	TAILQ_INSERT_TAIL(&fgrp->event_handlers, ehdlr, next);
	fgrp->num_fds++;

	return 0;
}

void
spdk_fd_group_remove(struct spdk_fd_group *fgrp, int efd)
{
	struct event_handler *ehdlr;
	int rc;

	if (fgrp == NULL || efd < 0) {
		SPDK_ERRLOG("Invalid to remvoe efd(%d) from fd_group(%p).\n", efd, fgrp);
		assert(0);
		return;
	}


	TAILQ_FOREACH(ehdlr, &fgrp->event_handlers, next) {
		if (ehdlr->fd == efd) {
			break;
		}
	}

	if (ehdlr == NULL) {
		SPDK_ERRLOG("efd(%d) is not existed in fgrp(%p)\n", efd, fgrp);
		return;
	}

	assert(ehdlr->state != EVENT_HANDLER_STATE_REMOVED);

	rc = epoll_ctl(fgrp->epfd, EPOLL_CTL_DEL, ehdlr->fd, NULL);
	if (rc < 0) {
		SPDK_ERRLOG("Failed to delete the fd(%d) from the epoll group(%p)\n", efd, fgrp);
		return;
	}

	assert(fgrp->num_fds > 0);
	fgrp->num_fds--;
	TAILQ_REMOVE(&fgrp->event_handlers, ehdlr, next);

	/* Delay ehdlr's free in case it is waiting for execution in fgrp wait loop */
	if (ehdlr->state == EVENT_HANDLER_STATE_RUNNING) {
		ehdlr->state = EVENT_HANDLER_STATE_REMOVED;
	} else {
		free(ehdlr);
	}
}

int
spdk_fd_group_event_modify(struct spdk_fd_group *fgrp,
			   int efd, int event_types)
{
	struct epoll_event epevent;
	struct event_handler *ehdlr;

	if (fgrp == NULL || efd < 0) {
		return -EINVAL;
	}

	TAILQ_FOREACH(ehdlr, &fgrp->event_handlers, next) {
		if (ehdlr->fd == efd) {
			break;
		}
	}

	if (ehdlr == NULL) {
		return -EINVAL;
	}

	assert(ehdlr->state != EVENT_HANDLER_STATE_REMOVED);

	epevent.events = event_types;
	epevent.data.ptr = ehdlr;

	return epoll_ctl(fgrp->epfd, EPOLL_CTL_MOD, ehdlr->fd, &epevent);
}

int
spdk_fd_group_create(struct spdk_fd_group **_egrp)
{
	struct spdk_fd_group *fgrp;

	if (_egrp == NULL) {
		return -EINVAL;
	}

	fgrp = calloc(1, sizeof(*fgrp));
	if (fgrp == NULL) {
		return -ENOMEM;
	}

	/* init the event source head */
	TAILQ_INIT(&fgrp->event_handlers);

	fgrp->num_fds = 0;
	fgrp->epfd = epoll_create1(EPOLL_CLOEXEC);
	if (fgrp->epfd < 0) {
		free(fgrp);
		return -errno;
	}

	*_egrp = fgrp;

	return 0;
}

void
spdk_fd_group_destroy(struct spdk_fd_group *fgrp)
{
	if (fgrp == NULL || fgrp->num_fds > 0) {
		SPDK_ERRLOG("Invalid fd_group(%p) to destroy.\n", fgrp);
		assert(0);
		return;
	}

	close(fgrp->epfd);
	free(fgrp);

	return;
}

int
spdk_fd_group_wait(struct spdk_fd_group *fgrp, int timeout)
{
	int totalfds = fgrp->num_fds;
	struct epoll_event events[totalfds];
	struct event_handler *ehdlr;
	int n;
	int nfds;

	nfds = epoll_wait(fgrp->epfd, events, totalfds, timeout);
	if (nfds < 0) {
		if (errno != EINTR) {
			SPDK_ERRLOG("fgrp epoll_wait returns with fail. errno is %d\n", errno);
		}

		return -errno;
	} else if (nfds == 0) {
		return 0;
	}

	for (n = 0; n < nfds; n++) {
		/* find the event_handler */
		ehdlr = events[n].data.ptr;

		if (ehdlr == NULL) {
			continue;
		}

		/* Tag ehdlr as running state in case that it is removed
		 * during this wait loop but before or when it get executed.
		 */
		assert(ehdlr->state == EVENT_HANDLER_STATE_WAITING);
		ehdlr->state = EVENT_HANDLER_STATE_RUNNING;
	}

	for (n = 0; n < nfds; n++) {
		/* find the event_handler */
		ehdlr = events[n].data.ptr;

		if (ehdlr == NULL || ehdlr->fn == NULL) {
			continue;
		}

		/* It is possible that the ehdlr was removed
		 * during this wait loop but before it get executed.
		 */
		if (ehdlr->state == EVENT_HANDLER_STATE_REMOVED) {
			free(ehdlr);
			continue;
		}

		/* call the interrupt response function */
		ehdlr->fn(ehdlr->fn_arg);

		/* It is possible that the ehdlr was removed
		 * during this wait loop when it get executed.
		 */
		if (ehdlr->state == EVENT_HANDLER_STATE_REMOVED) {
			free(ehdlr);
		} else {
			ehdlr->state = EVENT_HANDLER_STATE_WAITING;
		}
	}

	return nfds;
}

#else

int
spdk_fd_group_add(struct spdk_fd_group *fgrp,
		  int efd, spdk_fd_fn fn, void *arg)
{
	return -ENOTSUP;
}

void
spdk_fd_group_remove(struct spdk_fd_group *fgrp, int efd)
{
}

int
spdk_fd_group_event_modify(struct spdk_fd_group *fgrp,
			   int efd, int event_types)
{
	return -ENOTSUP;
}

int
spdk_fd_group_create(struct spdk_fd_group **fgrp)
{
	return -ENOTSUP;
}

void
spdk_fd_group_destroy(struct spdk_fd_group *fgrp)
{
}

int
spdk_fd_group_wait(struct spdk_fd_group *fgrp, int timeout)
{
	return -ENOTSUP;
}

#endif

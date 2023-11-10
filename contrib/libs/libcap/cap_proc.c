/*
 * Copyright (c) 1997-8,2007,2011 Andrew G Morgan <morgan@kernel.org>
 *
 * This file deals with getting and setting capabilities on processes.
 */

#include <sys/prctl.h>

#include "libcap.h"

cap_t cap_get_proc(void)
{
    cap_t result;

    /* allocate a new capability set */
    result = cap_init();
    if (result) {
	_cap_debug("getting current process' capabilities");

	/* fill the capability sets via a system call */
	if (capget(&result->head, &result->u[0].set)) {
	    cap_free(result);
	    result = NULL;
	}
    }

    return result;
}

int cap_set_proc(cap_t cap_d)
{
    int retval;

    if (!good_cap_t(cap_d)) {
	errno = EINVAL;
	return -1;
    }

    _cap_debug("setting process capabilities");
    retval = capset(&cap_d->head, &cap_d->u[0].set);

    return retval;
}

/* the following two functions are not required by POSIX */

/* read the caps on a specific process */

int capgetp(pid_t pid, cap_t cap_d)
{
    int error;

    if (!good_cap_t(cap_d)) {
	errno = EINVAL;
	return -1;
    }

    _cap_debug("getting process capabilities for proc %d", pid);

    cap_d->head.pid = pid;
    error = capget(&cap_d->head, &cap_d->u[0].set);
    cap_d->head.pid = 0;

    return error;
}

/* allocate space for and return capabilities of target process */

cap_t cap_get_pid(pid_t pid)
{
    cap_t result;

    result = cap_init();
    if (result) {
	if (capgetp(pid, result) != 0) {
	    int my_errno;

	    my_errno = errno;
	    cap_free(result);
	    errno = my_errno;
	    result = NULL;
	}
    }

    return result;
}

/* set the caps on a specific process/pg etc.. */

int capsetp(pid_t pid, cap_t cap_d)
{
    int error;

    if (!good_cap_t(cap_d)) {
	errno = EINVAL;
	return -1;
    }

    _cap_debug("setting process capabilities for proc %d", pid);
    cap_d->head.pid = pid;
    error = capset(&cap_d->head, &cap_d->u[0].set);
    cap_d->head.version = _LIBCAP_CAPABILITY_VERSION;
    cap_d->head.pid = 0;

    return error;
}

/* get a capability from the bounding set */

int cap_get_bound(cap_value_t cap)
{
    int result;

    result = prctl(PR_CAPBSET_READ, cap);
    return result;
}

/* drop a capability from the bounding set */

int cap_drop_bound(cap_value_t cap)
{
    int result;

    result = prctl(PR_CAPBSET_DROP, cap);
    return result;
}

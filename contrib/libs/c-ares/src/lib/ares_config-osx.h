/* Copyright (C) The c-ares project and its contributors
 * SPDX-License-Identifier: MIT
 */

#pragma once

#include "ares_config-linux.h"

/* Use resolver library to configure cares */
#define CARES_USE_LIBRESOLV

/* Define to 1 if you have the pipe2 function. */
#undef HAVE_PIPE2

/* Define to 1 if you have the kqueue function. */
#define HAVE_KQUEUE

/* Define to 1 if you have the epoll{_create,ctl,wait} functions. */
#undef HAVE_EPOLL

/* Define to 1 if you have the getrandom function. */
#undef HAVE_GETRANDOM

/* Define to 1 if you have the getservbyport_r function. */
#undef HAVE_GETSERVBYPORT_R

/* Define to 1 if you have the getservbyname_r function. */
#undef HAVE_GETSERVBYNAME_R

/* Define to 1 if you have the `resolve' library (-lresolve). */
#define HAVE_LIBRESOLV

/* Define to 1 if you have the malloc.h header file. */
#undef HAVE_MALLOC_H

/* Define to 1 if you have the AvailabilityMacros.h header file. */
#define HAVE_AVAILABILITYMACROS_H

/* Define to 1 if you have the <sys/random.h> header file. */
#undef HAVE_SYS_RANDOM_H

/* Define to 1 if you have the <sys/event.h> header file. */
#define HAVE_SYS_EVENT_H

/* Define to 1 if you have the <sys/epoll.h> header file. */
#undef HAVE_SYS_EPOLL_H

/* Define if have arc4random_buf() */
#define HAVE_ARC4RANDOM_BUF

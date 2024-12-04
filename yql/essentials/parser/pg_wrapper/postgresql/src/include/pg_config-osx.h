#pragma once

#include "pg_config-linux.h"

/* Define to 1 if you have the declaration of `strlcat', and to 0 if you
   don't. */
#undef HAVE_DECL_STRLCAT
#define HAVE_DECL_STRLCAT 1

/* Define to 1 if you have the declaration of `strlcpy', and to 0 if you
   don't. */
#undef HAVE_DECL_STRLCPY
#define HAVE_DECL_STRLCPY 1

/* Define to 1 if the system has the type `locale_t'. */
#undef HAVE_LOCALE_T

/* Define to 1 if you have the <sys/prctl.h> header file. */
#undef HAVE_SYS_PRCTL_H

/* Define to 1 if you have the `sync_file_range' function. */
#undef HAVE_SYNC_FILE_RANGE

/* Define to 1 if you have the <sys/ucred.h> header file. */
#define HAVE_SYS_UCRED_H 1

/* Define to 1 if you have the <sys/epoll.h> header file. */
#undef HAVE_SYS_EPOLL_H

/* Define to 1 if you have the `strchrnul' function. */
#undef HAVE_STRCHRNUL

/* Define to 1 if you have the `syncfs' function. */
#undef HAVE_SYNCFS

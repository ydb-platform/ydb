#pragma once

#include "curl_config-linux.h"

/* Define to 1 if you have the clock_gettime function and raw monotonic timer.
   */
#undef HAVE_CLOCK_GETTIME_MONOTONIC_RAW

/* Define to 1 if you have the fsetxattr function. */
#undef HAVE_FSETXATTR

/* fsetxattr() takes 5 args */
#undef HAVE_FSETXATTR_5

/* Define to 1 if you have the <linux/tcp.h> header file. */
#undef HAVE_LINUX_TCP_H

/* cpu-machine-OS */
#define OS "x86_64-pc-freebsd"

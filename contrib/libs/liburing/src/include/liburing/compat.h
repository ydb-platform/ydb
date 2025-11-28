/* SPDX-License-Identifier: MIT */
#ifndef LIBURING_COMPAT_H
#define LIBURING_COMPAT_H

#if defined(__has_include)
/* introduced in C++17 & C23 */
/* quotes "" quotes needed for GCC < 10 */
#if __has_include("linux/time_types.h")
#include <linux/time_types.h>
#else
struct __kernel_timespec {
	int64_t		tv_sec;
	long long	tv_nsec;
};
#endif

#define UAPI_LINUX_IO_URING_H_SKIP_LINUX_TIME_TYPES_H 1
#endif

#if !defined(__has_include)
#include <linux/time_types.h>
/* <linux/time_types.h> is included above and not needed again */
#define UAPI_LINUX_IO_URING_H_SKIP_LINUX_TIME_TYPES_H 1
#endif
#include <linux/openat2.h>


#include <linux/ioctl.h>

#ifndef BLOCK_URING_CMD_DISCARD
#define BLOCK_URING_CMD_DISCARD                        _IO(0x12, 0)
#endif

#endif

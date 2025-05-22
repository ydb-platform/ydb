/* SPDX-License-Identifier: MIT */
#ifndef LIBURING_COMPAT_H
#define LIBURING_COMPAT_H

#include <linux/time_types.h>
/* <linux/time_types.h> is included above and not needed again */
#define UAPI_LINUX_IO_URING_H_SKIP_LINUX_TIME_TYPES_H 1

#include <linux/openat2.h>


#include <linux/ioctl.h>

#ifndef BLOCK_URING_CMD_DISCARD
#define BLOCK_URING_CMD_DISCARD                        _IO(0x12, 0)
#endif

#endif

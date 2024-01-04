/* SPDX-License-Identifier: GPL-2.0 WITH Linux-syscall-note */
/*
 * This is based on the include/uapi/asm-generic/unistd.h header file
 * in the kernel, which is a generic syscall schema for new architectures.
 */

#define __NR_io_setup			0
#define __NR_io_destroy			1
#define __NR_io_submit			2
#define __NR_io_cancel			3
#define __NR_io_getevents		4

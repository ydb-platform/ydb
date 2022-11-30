/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2012,2013 Intel Corporation
 */

#ifndef _RTE_RTM_H_
#define _RTE_RTM_H_ 1


/* Official RTM intrinsics interface matching gcc/icc, but works
   on older gcc compatible compilers and binutils. */

#include <rte_common.h>

#ifdef __cplusplus
extern "C" {
#endif


#define RTE_XBEGIN_STARTED		(~0u)
#define RTE_XABORT_EXPLICIT		(1 << 0)
#define RTE_XABORT_RETRY		(1 << 1)
#define RTE_XABORT_CONFLICT		(1 << 2)
#define RTE_XABORT_CAPACITY		(1 << 3)
#define RTE_XABORT_DEBUG		(1 << 4)
#define RTE_XABORT_NESTED		(1 << 5)
#define RTE_XABORT_CODE(x)		(((x) >> 24) & 0xff)

static __rte_always_inline
unsigned int rte_xbegin(void)
{
	unsigned int ret = RTE_XBEGIN_STARTED;

	asm volatile(".byte 0xc7,0xf8 ; .long 0" : "+a" (ret) :: "memory");
	return ret;
}

static __rte_always_inline
void rte_xend(void)
{
	 asm volatile(".byte 0x0f,0x01,0xd5" ::: "memory");
}

/* not an inline function to workaround a clang bug with -O0 */
#define rte_xabort(status) do { \
	asm volatile(".byte 0xc6,0xf8,%P0" :: "i" (status) : "memory"); \
} while (0)

static __rte_always_inline
int rte_xtest(void)
{
	unsigned char out;

	asm volatile(".byte 0x0f,0x01,0xd6 ; setnz %0" :
		"=r" (out) :: "memory");
	return out;
}

#ifdef __cplusplus
}
#endif

#endif /* _RTE_RTM_H_ */

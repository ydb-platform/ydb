/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#ifndef _RTE_BYTEORDER_X86_H_
#error do not include this file directly, use <rte_byteorder.h> instead
#endif

#ifndef _RTE_BYTEORDER_X86_64_H_
#define _RTE_BYTEORDER_X86_64_H_

#include <stdint.h>
#include <rte_common.h>

/*
 * An architecture-optimized byte swap for a 64-bit value.
 *
  * Do not use this function directly. The preferred function is rte_bswap64().
 */
/* 64-bit mode */
static inline uint64_t rte_arch_bswap64(uint64_t _x)
{
	uint64_t x = _x;
	asm volatile ("bswap %[x]"
		      : [x] "+r" (x)
		      );
	return x;
}

#endif /* _RTE_BYTEORDER_X86_64_H_ */

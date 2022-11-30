/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2015 RehiveTech. All rights reserved.
 */

#ifndef _RTE_BYTEORDER_ARM_H_
#define _RTE_BYTEORDER_ARM_H_

#ifndef RTE_FORCE_INTRINSICS
#  error Platform must be built with RTE_FORCE_INTRINSICS
#endif

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <rte_common.h>
#include "generic/rte_byteorder.h"

/* fix missing __builtin_bswap16 for gcc older then 4.8 */
#if !(__GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 8))

static inline uint16_t rte_arch_bswap16(uint16_t _x)
{
	uint16_t x = _x;

	asm volatile ("rev16 %w0,%w1"
		      : "=r" (x)
		      : "r" (x)
		      );
	return x;
}

#define rte_bswap16(x) ((uint16_t)(__builtin_constant_p(x) ? \
				   rte_constant_bswap16(x) : \
				   rte_arch_bswap16(x)))
#endif

/* ARM architecture is bi-endian (both big and little). */
#if RTE_BYTE_ORDER == RTE_LITTLE_ENDIAN

#define rte_cpu_to_le_16(x) (x)
#define rte_cpu_to_le_32(x) (x)
#define rte_cpu_to_le_64(x) (x)

#define rte_cpu_to_be_16(x) rte_bswap16(x)
#define rte_cpu_to_be_32(x) rte_bswap32(x)
#define rte_cpu_to_be_64(x) rte_bswap64(x)

#define rte_le_to_cpu_16(x) (x)
#define rte_le_to_cpu_32(x) (x)
#define rte_le_to_cpu_64(x) (x)

#define rte_be_to_cpu_16(x) rte_bswap16(x)
#define rte_be_to_cpu_32(x) rte_bswap32(x)
#define rte_be_to_cpu_64(x) rte_bswap64(x)

#else /* RTE_BIG_ENDIAN */

#define rte_cpu_to_le_16(x) rte_bswap16(x)
#define rte_cpu_to_le_32(x) rte_bswap32(x)
#define rte_cpu_to_le_64(x) rte_bswap64(x)

#define rte_cpu_to_be_16(x) (x)
#define rte_cpu_to_be_32(x) (x)
#define rte_cpu_to_be_64(x) (x)

#define rte_le_to_cpu_16(x) rte_bswap16(x)
#define rte_le_to_cpu_32(x) rte_bswap32(x)
#define rte_le_to_cpu_64(x) rte_bswap64(x)

#define rte_be_to_cpu_16(x) (x)
#define rte_be_to_cpu_32(x) (x)
#define rte_be_to_cpu_64(x) (x)
#endif

#ifdef __cplusplus
}
#endif

#endif /* _RTE_BYTEORDER_ARM_H_ */

/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#ifndef _RTE_BYTEORDER_X86_H_
#define _RTE_BYTEORDER_X86_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <rte_common.h>
#include <rte_config.h>
#include "generic/rte_byteorder.h"

#ifndef RTE_BYTE_ORDER
#define RTE_BYTE_ORDER RTE_LITTLE_ENDIAN
#endif

/*
 * An architecture-optimized byte swap for a 16-bit value.
 *
 * Do not use this function directly. The preferred function is rte_bswap16().
 */
static inline uint16_t rte_arch_bswap16(uint16_t _x)
{
	uint16_t x = _x;
	asm volatile ("xchgb %b[x1],%h[x2]"
		      : [x1] "=Q" (x)
		      : [x2] "0" (x)
		      );
	return x;
}

/*
 * An architecture-optimized byte swap for a 32-bit value.
 *
 * Do not use this function directly. The preferred function is rte_bswap32().
 */
static inline uint32_t rte_arch_bswap32(uint32_t _x)
{
	uint32_t x = _x;
	asm volatile ("bswap %[x]"
		      : [x] "+r" (x)
		      );
	return x;
}

#ifndef RTE_FORCE_INTRINSICS
#define rte_bswap16(x) ((uint16_t)(__builtin_constant_p(x) ?		\
				   rte_constant_bswap16(x) :		\
				   rte_arch_bswap16(x)))

#define rte_bswap32(x) ((uint32_t)(__builtin_constant_p(x) ?		\
				   rte_constant_bswap32(x) :		\
				   rte_arch_bswap32(x)))

#define rte_bswap64(x) ((uint64_t)(__builtin_constant_p(x) ?		\
				   rte_constant_bswap64(x) :		\
				   rte_arch_bswap64(x)))
#else
/*
 * __builtin_bswap16 is only available gcc 4.8 and upwards
 */
#if __GNUC__ < 4 || (__GNUC__ == 4 && __GNUC_MINOR__ < 8)
#define rte_bswap16(x) ((uint16_t)(__builtin_constant_p(x) ?		\
				   rte_constant_bswap16(x) :		\
				   rte_arch_bswap16(x)))
#endif
#endif

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

#ifdef RTE_ARCH_I686
#error #include "rte_byteorder_32.h"
#else
#include "rte_byteorder_64.h"
#endif

#ifdef __cplusplus
}
#endif

#endif /* _RTE_BYTEORDER_X86_H_ */

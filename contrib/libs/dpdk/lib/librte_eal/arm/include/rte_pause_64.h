/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2017 Cavium, Inc
 * Copyright(c) 2019 Arm Limited
 */

#ifndef _RTE_PAUSE_ARM64_H_
#define _RTE_PAUSE_ARM64_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <rte_common.h>

#ifdef RTE_ARM_USE_WFE
#define RTE_WAIT_UNTIL_EQUAL_ARCH_DEFINED
#endif

#include "generic/rte_pause.h"

static inline void rte_pause(void)
{
	asm volatile("yield" ::: "memory");
}

#ifdef RTE_WAIT_UNTIL_EQUAL_ARCH_DEFINED

/* Send an event to quit WFE. */
#define __SEVL() { asm volatile("sevl" : : : "memory"); }

/* Put processor into low power WFE(Wait For Event) state. */
#define __WFE() { asm volatile("wfe" : : : "memory"); }

static __rte_always_inline void
rte_wait_until_equal_16(volatile uint16_t *addr, uint16_t expected,
		int memorder)
{
	uint16_t value;

	assert(memorder == __ATOMIC_ACQUIRE || memorder == __ATOMIC_RELAXED);

	/*
	 * Atomic exclusive load from addr, it returns the 16-bit content of
	 * *addr while making it 'monitored',when it is written by someone
	 * else, the 'monitored' state is cleared and a event is generated
	 * implicitly to exit WFE.
	 */
#define __LOAD_EXC_16(src, dst, memorder) {               \
	if (memorder == __ATOMIC_RELAXED) {               \
		asm volatile("ldxrh %w[tmp], [%x[addr]]"  \
			: [tmp] "=&r" (dst)               \
			: [addr] "r"(src)                 \
			: "memory");                      \
	} else {                                          \
		asm volatile("ldaxrh %w[tmp], [%x[addr]]" \
			: [tmp] "=&r" (dst)               \
			: [addr] "r"(src)                 \
			: "memory");                      \
	} }

	__LOAD_EXC_16(addr, value, memorder)
	if (value != expected) {
		__SEVL()
		do {
			__WFE()
			__LOAD_EXC_16(addr, value, memorder)
		} while (value != expected);
	}
#undef __LOAD_EXC_16
}

static __rte_always_inline void
rte_wait_until_equal_32(volatile uint32_t *addr, uint32_t expected,
		int memorder)
{
	uint32_t value;

	assert(memorder == __ATOMIC_ACQUIRE || memorder == __ATOMIC_RELAXED);

	/*
	 * Atomic exclusive load from addr, it returns the 32-bit content of
	 * *addr while making it 'monitored',when it is written by someone
	 * else, the 'monitored' state is cleared and a event is generated
	 * implicitly to exit WFE.
	 */
#define __LOAD_EXC_32(src, dst, memorder) {              \
	if (memorder == __ATOMIC_RELAXED) {              \
		asm volatile("ldxr %w[tmp], [%x[addr]]"  \
			: [tmp] "=&r" (dst)              \
			: [addr] "r"(src)                \
			: "memory");                     \
	} else {                                         \
		asm volatile("ldaxr %w[tmp], [%x[addr]]" \
			: [tmp] "=&r" (dst)              \
			: [addr] "r"(src)                \
			: "memory");                     \
	} }

	__LOAD_EXC_32(addr, value, memorder)
	if (value != expected) {
		__SEVL()
		do {
			__WFE()
			__LOAD_EXC_32(addr, value, memorder)
		} while (value != expected);
	}
#undef __LOAD_EXC_32
}

static __rte_always_inline void
rte_wait_until_equal_64(volatile uint64_t *addr, uint64_t expected,
		int memorder)
{
	uint64_t value;

	assert(memorder == __ATOMIC_ACQUIRE || memorder == __ATOMIC_RELAXED);

	/*
	 * Atomic exclusive load from addr, it returns the 64-bit content of
	 * *addr while making it 'monitored',when it is written by someone
	 * else, the 'monitored' state is cleared and a event is generated
	 * implicitly to exit WFE.
	 */
#define __LOAD_EXC_64(src, dst, memorder) {              \
	if (memorder == __ATOMIC_RELAXED) {              \
		asm volatile("ldxr %x[tmp], [%x[addr]]"  \
			: [tmp] "=&r" (dst)              \
			: [addr] "r"(src)                \
			: "memory");                     \
	} else {                                         \
		asm volatile("ldaxr %x[tmp], [%x[addr]]" \
			: [tmp] "=&r" (dst)              \
			: [addr] "r"(src)                \
			: "memory");                     \
	} }

	__LOAD_EXC_64(addr, value, memorder)
	if (value != expected) {
		__SEVL()
		do {
			__WFE()
			__LOAD_EXC_64(addr, value, memorder)
		} while (value != expected);
	}
}
#undef __LOAD_EXC_64

#undef __SEVL
#undef __WFE

#endif

#ifdef __cplusplus
}
#endif

#endif /* _RTE_PAUSE_ARM64_H_ */

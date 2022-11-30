/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2015 RehiveTech. All rights reserved.
 */

#ifndef _RTE_CYCLES_ARM32_H_
#define _RTE_CYCLES_ARM32_H_

/* ARM v7 does not have suitable source of clock signals. The only clock counter
   available in the core is 32 bit wide. Therefore it is unsuitable as the
   counter overlaps every few seconds and probably is not accessible by
   userspace programs. Therefore we use clock_gettime(CLOCK_MONOTONIC_RAW) to
   simulate counter running at 1GHz.
*/

#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

#include "generic/rte_cycles.h"

/**
 * Read the time base register.
 *
 * @return
 *   The time base for this lcore.
 */
#ifndef RTE_ARM_EAL_RDTSC_USE_PMU

/**
 * This call is easily portable to any architecture, however,
 * it may require a system call and inprecise for some tasks.
 */
static inline uint64_t
__rte_rdtsc_syscall(void)
{
	struct timespec val;
	uint64_t v;

	while (clock_gettime(CLOCK_MONOTONIC_RAW, &val) != 0)
		/* no body */;

	v  = (uint64_t) val.tv_sec * 1000000000LL;
	v += (uint64_t) val.tv_nsec;
	return v;
}
#define rte_rdtsc __rte_rdtsc_syscall

#else

/**
 * This function requires to configure the PMCCNTR and enable
 * userspace access to it:
 *
 *      asm volatile("mcr p15, 0, %0, c9, c14, 0" : : "r"(1));
 *      asm volatile("mcr p15, 0, %0, c9, c12, 0" : : "r"(29));
 *      asm volatile("mcr p15, 0, %0, c9, c12, 1" : : "r"(0x8000000f));
 *
 * which is possible only from the privileged mode (kernel space).
 */
static inline uint64_t
__rte_rdtsc_pmccntr(void)
{
	unsigned tsc;
	uint64_t final_tsc;

	/* Read PMCCNTR */
	asm volatile("mrc p15, 0, %0, c9, c13, 0" : "=r"(tsc));
	/* 1 tick = 64 clocks */
	final_tsc = ((uint64_t)tsc) << 6;

	return (uint64_t)final_tsc;
}
#define rte_rdtsc __rte_rdtsc_pmccntr

#endif /* RTE_ARM_EAL_RDTSC_USE_PMU */

static inline uint64_t
rte_rdtsc_precise(void)
{
	rte_mb();
	return rte_rdtsc();
}

static inline uint64_t
rte_get_tsc_cycles(void) { return rte_rdtsc(); }

#ifdef __cplusplus
}
#endif

#endif /* _RTE_CYCLES_ARM32_H_ */

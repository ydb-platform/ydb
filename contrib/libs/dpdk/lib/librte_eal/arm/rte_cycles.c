/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2015 Cavium, Inc
 */

#include "eal_private.h"
#include "rte_cycles.h"

uint64_t
get_tsc_freq_arch(void)
{
#if defined RTE_ARCH_ARM64 && !defined RTE_ARM_EAL_RDTSC_USE_PMU
	return __rte_arm64_cntfrq();
#elif defined RTE_ARCH_ARM64 && defined RTE_ARM_EAL_RDTSC_USE_PMU
#define CYC_PER_1MHZ 1E6
	/* Use the generic counter ticks to calculate the PMU
	 * cycle frequency.
	 */
	uint64_t ticks;
	uint64_t start_ticks, cur_ticks;
	uint64_t start_pmu_cycles, end_pmu_cycles;

	/* Number of ticks for 1/10 second */
	ticks = __rte_arm64_cntfrq() / 10;

	start_ticks = __rte_arm64_cntvct_precise();
	start_pmu_cycles = rte_rdtsc_precise();
	do {
		cur_ticks = __rte_arm64_cntvct();
	} while ((cur_ticks - start_ticks) < ticks);
	end_pmu_cycles = rte_rdtsc_precise();

	/* Adjust the cycles to next 1Mhz */
	return RTE_ALIGN_MUL_CEIL(end_pmu_cycles - start_pmu_cycles,
			CYC_PER_1MHZ) * 10;
#else
	return 0;
#endif
}

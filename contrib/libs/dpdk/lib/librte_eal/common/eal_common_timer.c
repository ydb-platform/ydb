#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <inttypes.h>
#include <sys/types.h>
#include <time.h>
#include <errno.h>

#include <rte_common.h>
#include <rte_compat.h>
#include <rte_log.h>
#include <rte_cycles.h>
#include <rte_pause.h>
#include <rte_eal.h>

#include "eal_private.h"
#include "eal_memcfg.h"

/* The frequency of the RDTSC timer resolution */
static uint64_t eal_tsc_resolution_hz;

/* Pointer to user delay function */
void (*rte_delay_us)(unsigned int) = NULL;

void
rte_delay_us_block(unsigned int us)
{
	const uint64_t start = rte_get_timer_cycles();
	const uint64_t ticks = (uint64_t)us * rte_get_timer_hz() / 1E6;
	while ((rte_get_timer_cycles() - start) < ticks)
		rte_pause();
}

uint64_t
rte_get_tsc_hz(void)
{
	return eal_tsc_resolution_hz;
}

static uint64_t
estimate_tsc_freq(void)
{
#define CYC_PER_10MHZ 1E7
	RTE_LOG(WARNING, EAL, "WARNING: TSC frequency estimated roughly"
		" - clock timings may be less accurate.\n");
	/* assume that the sleep(1) will sleep for 1 second */
	uint64_t start = rte_rdtsc();
	sleep(1);
	/* Round up to 10Mhz. 1E7 ~ 10Mhz */
	return RTE_ALIGN_MUL_NEAR(rte_rdtsc() - start, CYC_PER_10MHZ);
}

void
set_tsc_freq(void)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	uint64_t freq;

	if (rte_eal_process_type() == RTE_PROC_SECONDARY) {
		/*
		 * Just use the primary process calculated TSC rate in any
		 * secondary process.  It avoids any unnecessary overhead on
		 * systems where arch-specific frequency detection is not
		 * available.
		 */
		eal_tsc_resolution_hz = mcfg->tsc_hz;
		return;
	}

	freq = get_tsc_freq_arch();
	if (!freq)
		freq = get_tsc_freq();
	if (!freq)
		freq = estimate_tsc_freq();

	RTE_LOG(DEBUG, EAL, "TSC frequency is ~%" PRIu64 " KHz\n", freq / 1000);
	eal_tsc_resolution_hz = freq;
	mcfg->tsc_hz = freq;
}

void rte_delay_us_callback_register(void (*userfunc)(unsigned int))
{
	rte_delay_us = userfunc;
}

RTE_INIT(rte_timer_init)
{
	/* set rte_delay_us_block as a delay function */
	rte_delay_us_callback_register(rte_delay_us_block);
}

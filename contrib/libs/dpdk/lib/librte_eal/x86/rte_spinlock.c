#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#include <stdint.h>

#include "rte_cpuflags.h"

uint8_t rte_rtm_supported; /* cache the flag to avoid the overhead
			      of the rte_cpu_get_flag_enabled function */

RTE_INIT(rte_rtm_init)
{
	rte_rtm_supported = rte_cpu_get_flag_enabled(RTE_CPUFLAG_RTM);
}

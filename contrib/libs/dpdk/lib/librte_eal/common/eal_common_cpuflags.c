#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#include <stdio.h>

#include <rte_common.h>
#include <rte_cpuflags.h>

int
rte_cpu_is_supported(void)
{
	/* This is generated at compile-time by the build system */
	static const enum rte_cpu_flag_t compile_time_flags[] = {
			RTE_COMPILE_TIME_CPUFLAGS
	};
	unsigned count = RTE_DIM(compile_time_flags), i;
	int ret;

	for (i = 0; i < count; i++) {
		ret = rte_cpu_get_flag_enabled(compile_time_flags[i]);

		if (ret < 0) {
			fprintf(stderr,
				"ERROR: CPU feature flag lookup failed with error %d\n",
				ret);
			return 0;
		}
		if (!ret) {
			fprintf(stderr,
			        "ERROR: This system does not support \"%s\".\n"
			        "Please check that RTE_MACHINE is set correctly.\n",
			        rte_cpu_get_flag_name(compile_time_flags[i]));
			return 0;
		}
	}

	return 1;
}

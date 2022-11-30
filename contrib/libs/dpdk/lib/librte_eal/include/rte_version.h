/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

/**
 * @file
 * Definitions of DPDK version numbers
 */

#ifndef _RTE_VERSION_H_
#define _RTE_VERSION_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <rte_common.h>

/**
 * Macro to compute a version number usable for comparisons
 */
#define RTE_VERSION_NUM(a,b,c,d) ((a) << 24 | (b) << 16 | (c) << 8 | (d))

/**
 * All version numbers in one to compare with RTE_VERSION_NUM()
 */
#define RTE_VERSION RTE_VERSION_NUM( \
			RTE_VER_YEAR, \
			RTE_VER_MONTH, \
			RTE_VER_MINOR, \
			RTE_VER_RELEASE)

/**
 * Function returning version string
 * @return
 *     string
 */
static inline const char *
rte_version(void)
{
	static char version[32];
	if (version[0] != 0)
		return version;
	if (strlen(RTE_VER_SUFFIX) == 0)
		snprintf(version, sizeof(version), "%s %d.%02d.%d",
			RTE_VER_PREFIX,
			RTE_VER_YEAR,
			RTE_VER_MONTH,
			RTE_VER_MINOR);
	else
		snprintf(version, sizeof(version), "%s %d.%02d.%d%s%d",
			RTE_VER_PREFIX,
			RTE_VER_YEAR,
			RTE_VER_MONTH,
			RTE_VER_MINOR,
			RTE_VER_SUFFIX,
			RTE_VER_RELEASE);
	return version;
}

#ifdef __cplusplus
}
#endif

#endif /* RTE_VERSION_H */

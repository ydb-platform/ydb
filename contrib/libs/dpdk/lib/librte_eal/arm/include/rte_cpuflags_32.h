/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2015 RehiveTech. All rights reserved.
 */

#ifndef _RTE_CPUFLAGS_ARM32_H_
#define _RTE_CPUFLAGS_ARM32_H_

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Enumeration of all CPU features supported
 */
enum rte_cpu_flag_t {
	RTE_CPUFLAG_SWP = 0,
	RTE_CPUFLAG_HALF,
	RTE_CPUFLAG_THUMB,
	RTE_CPUFLAG_A26BIT,
	RTE_CPUFLAG_FAST_MULT,
	RTE_CPUFLAG_FPA,
	RTE_CPUFLAG_VFP,
	RTE_CPUFLAG_EDSP,
	RTE_CPUFLAG_JAVA,
	RTE_CPUFLAG_IWMMXT,
	RTE_CPUFLAG_CRUNCH,
	RTE_CPUFLAG_THUMBEE,
	RTE_CPUFLAG_NEON,
	RTE_CPUFLAG_VFPv3,
	RTE_CPUFLAG_VFPv3D16,
	RTE_CPUFLAG_TLS,
	RTE_CPUFLAG_VFPv4,
	RTE_CPUFLAG_IDIVA,
	RTE_CPUFLAG_IDIVT,
	RTE_CPUFLAG_VFPD32,
	RTE_CPUFLAG_LPAE,
	RTE_CPUFLAG_EVTSTRM,
	RTE_CPUFLAG_AES,
	RTE_CPUFLAG_PMULL,
	RTE_CPUFLAG_SHA1,
	RTE_CPUFLAG_SHA2,
	RTE_CPUFLAG_CRC32,
	RTE_CPUFLAG_V7L,
	/* The last item */
	RTE_CPUFLAG_NUMFLAGS,/**< This should always be the last! */
};

#include "generic/rte_cpuflags.h"

#ifdef __cplusplus
}
#endif

#endif /* _RTE_CPUFLAGS_ARM32_H_ */

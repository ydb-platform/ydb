/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2015 Cavium, Inc
 */

#ifndef _RTE_CPUFLAGS_ARM64_H_
#define _RTE_CPUFLAGS_ARM64_H_

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Enumeration of all CPU features supported
 */
enum rte_cpu_flag_t {
	RTE_CPUFLAG_FP = 0,
	RTE_CPUFLAG_NEON,
	RTE_CPUFLAG_EVTSTRM,
	RTE_CPUFLAG_AES,
	RTE_CPUFLAG_PMULL,
	RTE_CPUFLAG_SHA1,
	RTE_CPUFLAG_SHA2,
	RTE_CPUFLAG_CRC32,
	RTE_CPUFLAG_ATOMICS,
	RTE_CPUFLAG_SVE,
	RTE_CPUFLAG_SVE2,
	RTE_CPUFLAG_SVEAES,
	RTE_CPUFLAG_SVEPMULL,
	RTE_CPUFLAG_SVEBITPERM,
	RTE_CPUFLAG_SVESHA3,
	RTE_CPUFLAG_SVESM4,
	RTE_CPUFLAG_FLAGM2,
	RTE_CPUFLAG_FRINT,
	RTE_CPUFLAG_SVEI8MM,
	RTE_CPUFLAG_SVEF32MM,
	RTE_CPUFLAG_SVEF64MM,
	RTE_CPUFLAG_SVEBF16,
	RTE_CPUFLAG_AARCH64,
	/* The last item */
	RTE_CPUFLAG_NUMFLAGS,/**< This should always be the last! */
};

#include "generic/rte_cpuflags.h"

#ifdef __cplusplus
}
#endif

#endif /* _RTE_CPUFLAGS_ARM64_H_ */

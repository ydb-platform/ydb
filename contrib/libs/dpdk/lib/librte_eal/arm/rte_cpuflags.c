#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (C) Cavium, Inc. 2015.
 * Copyright(c) 2015 RehiveTech. All rights reserved.
 */

#include "rte_cpuflags.h"

#include <elf.h>
#include <fcntl.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>

#ifndef AT_HWCAP
#define AT_HWCAP 16
#endif

#ifndef AT_HWCAP2
#define AT_HWCAP2 26
#endif

#ifndef AT_PLATFORM
#define AT_PLATFORM 15
#endif

enum cpu_register_t {
	REG_NONE = 0,
	REG_HWCAP,
	REG_HWCAP2,
	REG_PLATFORM,
	REG_MAX
};

typedef uint32_t hwcap_registers_t[REG_MAX];

/**
 * Struct to hold a processor feature entry
 */
struct feature_entry {
	uint32_t reg;
	uint32_t bit;
#define CPU_FLAG_NAME_MAX_LEN 64
	char name[CPU_FLAG_NAME_MAX_LEN];
};

#define FEAT_DEF(name, reg, bit) \
	[RTE_CPUFLAG_##name] = {reg, bit, #name},

#ifdef RTE_ARCH_ARMv7
#define PLATFORM_STR "v7l"
typedef Elf32_auxv_t _Elfx_auxv_t;

const struct feature_entry rte_cpu_feature_table[] = {
	FEAT_DEF(SWP,       REG_HWCAP,    0)
	FEAT_DEF(HALF,      REG_HWCAP,    1)
	FEAT_DEF(THUMB,     REG_HWCAP,    2)
	FEAT_DEF(A26BIT,    REG_HWCAP,    3)
	FEAT_DEF(FAST_MULT, REG_HWCAP,    4)
	FEAT_DEF(FPA,       REG_HWCAP,    5)
	FEAT_DEF(VFP,       REG_HWCAP,    6)
	FEAT_DEF(EDSP,      REG_HWCAP,    7)
	FEAT_DEF(JAVA,      REG_HWCAP,    8)
	FEAT_DEF(IWMMXT,    REG_HWCAP,    9)
	FEAT_DEF(CRUNCH,    REG_HWCAP,   10)
	FEAT_DEF(THUMBEE,   REG_HWCAP,   11)
	FEAT_DEF(NEON,      REG_HWCAP,   12)
	FEAT_DEF(VFPv3,     REG_HWCAP,   13)
	FEAT_DEF(VFPv3D16,  REG_HWCAP,   14)
	FEAT_DEF(TLS,       REG_HWCAP,   15)
	FEAT_DEF(VFPv4,     REG_HWCAP,   16)
	FEAT_DEF(IDIVA,     REG_HWCAP,   17)
	FEAT_DEF(IDIVT,     REG_HWCAP,   18)
	FEAT_DEF(VFPD32,    REG_HWCAP,   19)
	FEAT_DEF(LPAE,      REG_HWCAP,   20)
	FEAT_DEF(EVTSTRM,   REG_HWCAP,   21)
	FEAT_DEF(AES,       REG_HWCAP2,   0)
	FEAT_DEF(PMULL,     REG_HWCAP2,   1)
	FEAT_DEF(SHA1,      REG_HWCAP2,   2)
	FEAT_DEF(SHA2,      REG_HWCAP2,   3)
	FEAT_DEF(CRC32,     REG_HWCAP2,   4)
	FEAT_DEF(V7L,       REG_PLATFORM, 0)
};

#elif defined RTE_ARCH_ARM64
#define PLATFORM_STR "aarch64"
typedef Elf64_auxv_t _Elfx_auxv_t;

const struct feature_entry rte_cpu_feature_table[] = {
	FEAT_DEF(FP,		REG_HWCAP,    0)
	FEAT_DEF(NEON,		REG_HWCAP,    1)
	FEAT_DEF(EVTSTRM,	REG_HWCAP,    2)
	FEAT_DEF(AES,		REG_HWCAP,    3)
	FEAT_DEF(PMULL,		REG_HWCAP,    4)
	FEAT_DEF(SHA1,		REG_HWCAP,    5)
	FEAT_DEF(SHA2,		REG_HWCAP,    6)
	FEAT_DEF(CRC32,		REG_HWCAP,    7)
	FEAT_DEF(ATOMICS,	REG_HWCAP,    8)
	FEAT_DEF(SVE,		REG_HWCAP,    22)
	FEAT_DEF(SVE2,		REG_HWCAP2,   1)
	FEAT_DEF(SVEAES,	REG_HWCAP2,   2)
	FEAT_DEF(SVEPMULL,	REG_HWCAP2,   3)
	FEAT_DEF(SVEBITPERM,	REG_HWCAP2,   4)
	FEAT_DEF(SVESHA3,	REG_HWCAP2,   5)
	FEAT_DEF(SVESM4,	REG_HWCAP2,   6)
	FEAT_DEF(FLAGM2,	REG_HWCAP2,   7)
	FEAT_DEF(FRINT,		REG_HWCAP2,   8)
	FEAT_DEF(SVEI8MM,	REG_HWCAP2,   9)
	FEAT_DEF(SVEF32MM,	REG_HWCAP2,   10)
	FEAT_DEF(SVEF64MM,	REG_HWCAP2,   11)
	FEAT_DEF(SVEBF16,	REG_HWCAP2,   12)
	FEAT_DEF(AARCH64,	REG_PLATFORM, 1)
};
#endif /* RTE_ARCH */

/*
 * Read AUXV software register and get cpu features for ARM
 */
static void
rte_cpu_get_features(hwcap_registers_t out)
{
	out[REG_HWCAP] = rte_cpu_getauxval(AT_HWCAP);
	out[REG_HWCAP2] = rte_cpu_getauxval(AT_HWCAP2);
	if (!rte_cpu_strcmp_auxval(AT_PLATFORM, PLATFORM_STR))
		out[REG_PLATFORM] = 0x0001;
}

/*
 * Checks if a particular flag is available on current machine.
 */
int
rte_cpu_get_flag_enabled(enum rte_cpu_flag_t feature)
{
	const struct feature_entry *feat;
	hwcap_registers_t regs = {0};

	if (feature >= RTE_CPUFLAG_NUMFLAGS)
		return -ENOENT;

	feat = &rte_cpu_feature_table[feature];
	if (feat->reg == REG_NONE)
		return -EFAULT;

	rte_cpu_get_features(regs);
	return (regs[feat->reg] >> feat->bit) & 1;
}

const char *
rte_cpu_get_flag_name(enum rte_cpu_flag_t feature)
{
	if (feature >= RTE_CPUFLAG_NUMFLAGS)
		return NULL;
	return rte_cpu_feature_table[feature].name;
}

void
rte_cpu_get_intrinsics_support(struct rte_cpu_intrinsics *intrinsics)
{
	memset(intrinsics, 0, sizeof(*intrinsics));
}

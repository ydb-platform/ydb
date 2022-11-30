/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#ifndef _RTE_CPUFLAGS_X86_64_H_
#define _RTE_CPUFLAGS_X86_64_H_

#ifdef __cplusplus
extern "C" {
#endif

enum rte_cpu_flag_t {
	/* (EAX 01h) ECX features*/
	RTE_CPUFLAG_SSE3 = 0,               /**< SSE3 */
	RTE_CPUFLAG_PCLMULQDQ,              /**< PCLMULQDQ */
	RTE_CPUFLAG_DTES64,                 /**< DTES64 */
	RTE_CPUFLAG_MONITOR,                /**< MONITOR */
	RTE_CPUFLAG_DS_CPL,                 /**< DS_CPL */
	RTE_CPUFLAG_VMX,                    /**< VMX */
	RTE_CPUFLAG_SMX,                    /**< SMX */
	RTE_CPUFLAG_EIST,                   /**< EIST */
	RTE_CPUFLAG_TM2,                    /**< TM2 */
	RTE_CPUFLAG_SSSE3,                  /**< SSSE3 */
	RTE_CPUFLAG_CNXT_ID,                /**< CNXT_ID */
	RTE_CPUFLAG_FMA,                    /**< FMA */
	RTE_CPUFLAG_CMPXCHG16B,             /**< CMPXCHG16B */
	RTE_CPUFLAG_XTPR,                   /**< XTPR */
	RTE_CPUFLAG_PDCM,                   /**< PDCM */
	RTE_CPUFLAG_PCID,                   /**< PCID */
	RTE_CPUFLAG_DCA,                    /**< DCA */
	RTE_CPUFLAG_SSE4_1,                 /**< SSE4_1 */
	RTE_CPUFLAG_SSE4_2,                 /**< SSE4_2 */
	RTE_CPUFLAG_X2APIC,                 /**< X2APIC */
	RTE_CPUFLAG_MOVBE,                  /**< MOVBE */
	RTE_CPUFLAG_POPCNT,                 /**< POPCNT */
	RTE_CPUFLAG_TSC_DEADLINE,           /**< TSC_DEADLINE */
	RTE_CPUFLAG_AES,                    /**< AES */
	RTE_CPUFLAG_XSAVE,                  /**< XSAVE */
	RTE_CPUFLAG_OSXSAVE,                /**< OSXSAVE */
	RTE_CPUFLAG_AVX,                    /**< AVX */
	RTE_CPUFLAG_F16C,                   /**< F16C */
	RTE_CPUFLAG_RDRAND,                 /**< RDRAND */
	RTE_CPUFLAG_HYPERVISOR,             /**< Running in a VM */

	/* (EAX 01h) EDX features */
	RTE_CPUFLAG_FPU,                    /**< FPU */
	RTE_CPUFLAG_VME,                    /**< VME */
	RTE_CPUFLAG_DE,                     /**< DE */
	RTE_CPUFLAG_PSE,                    /**< PSE */
	RTE_CPUFLAG_TSC,                    /**< TSC */
	RTE_CPUFLAG_MSR,                    /**< MSR */
	RTE_CPUFLAG_PAE,                    /**< PAE */
	RTE_CPUFLAG_MCE,                    /**< MCE */
	RTE_CPUFLAG_CX8,                    /**< CX8 */
	RTE_CPUFLAG_APIC,                   /**< APIC */
	RTE_CPUFLAG_SEP,                    /**< SEP */
	RTE_CPUFLAG_MTRR,                   /**< MTRR */
	RTE_CPUFLAG_PGE,                    /**< PGE */
	RTE_CPUFLAG_MCA,                    /**< MCA */
	RTE_CPUFLAG_CMOV,                   /**< CMOV */
	RTE_CPUFLAG_PAT,                    /**< PAT */
	RTE_CPUFLAG_PSE36,                  /**< PSE36 */
	RTE_CPUFLAG_PSN,                    /**< PSN */
	RTE_CPUFLAG_CLFSH,                  /**< CLFSH */
	RTE_CPUFLAG_DS,                     /**< DS */
	RTE_CPUFLAG_ACPI,                   /**< ACPI */
	RTE_CPUFLAG_MMX,                    /**< MMX */
	RTE_CPUFLAG_FXSR,                   /**< FXSR */
	RTE_CPUFLAG_SSE,                    /**< SSE */
	RTE_CPUFLAG_SSE2,                   /**< SSE2 */
	RTE_CPUFLAG_SS,                     /**< SS */
	RTE_CPUFLAG_HTT,                    /**< HTT */
	RTE_CPUFLAG_TM,                     /**< TM */
	RTE_CPUFLAG_PBE,                    /**< PBE */

	/* (EAX 06h) EAX features */
	RTE_CPUFLAG_DIGTEMP,                /**< DIGTEMP */
	RTE_CPUFLAG_TRBOBST,                /**< TRBOBST */
	RTE_CPUFLAG_ARAT,                   /**< ARAT */
	RTE_CPUFLAG_PLN,                    /**< PLN */
	RTE_CPUFLAG_ECMD,                   /**< ECMD */
	RTE_CPUFLAG_PTM,                    /**< PTM */

	/* (EAX 06h) ECX features */
	RTE_CPUFLAG_MPERF_APERF_MSR,        /**< MPERF_APERF_MSR */
	RTE_CPUFLAG_ACNT2,                  /**< ACNT2 */
	RTE_CPUFLAG_ENERGY_EFF,             /**< ENERGY_EFF */

	/* (EAX 07h, ECX 0h) EBX features */
	RTE_CPUFLAG_FSGSBASE,               /**< FSGSBASE */
	RTE_CPUFLAG_BMI1,                   /**< BMI1 */
	RTE_CPUFLAG_HLE,                    /**< Hardware Lock elision */
	RTE_CPUFLAG_AVX2,                   /**< AVX2 */
	RTE_CPUFLAG_SMEP,                   /**< SMEP */
	RTE_CPUFLAG_BMI2,                   /**< BMI2 */
	RTE_CPUFLAG_ERMS,                   /**< ERMS */
	RTE_CPUFLAG_INVPCID,                /**< INVPCID */
	RTE_CPUFLAG_RTM,                    /**< Transactional memory */
	RTE_CPUFLAG_AVX512F,                /**< AVX512F */
	RTE_CPUFLAG_RDSEED,                 /**< RDSEED instruction */

	/* (EAX 80000001h) ECX features */
	RTE_CPUFLAG_LAHF_SAHF,              /**< LAHF_SAHF */
	RTE_CPUFLAG_LZCNT,                  /**< LZCNT */

	/* (EAX 80000001h) EDX features */
	RTE_CPUFLAG_SYSCALL,                /**< SYSCALL */
	RTE_CPUFLAG_XD,                     /**< XD */
	RTE_CPUFLAG_1GB_PG,                 /**< 1GB_PG */
	RTE_CPUFLAG_RDTSCP,                 /**< RDTSCP */
	RTE_CPUFLAG_EM64T,                  /**< EM64T */

	/* (EAX 80000007h) EDX features */
	RTE_CPUFLAG_INVTSC,                 /**< INVTSC */

	RTE_CPUFLAG_AVX512DQ,               /**< AVX512 Doubleword and Quadword */
	RTE_CPUFLAG_AVX512IFMA,             /**< AVX512 Integer Fused Multiply-Add */
	RTE_CPUFLAG_AVX512CD,               /**< AVX512 Conflict Detection*/
	RTE_CPUFLAG_AVX512BW,               /**< AVX512 Byte and Word */
	RTE_CPUFLAG_AVX512VL,               /**< AVX512 Vector Length */
	RTE_CPUFLAG_AVX512VBMI,             /**< AVX512 Vector Bit Manipulation */
	RTE_CPUFLAG_AVX512VBMI2,            /**< AVX512 Vector Bit Manipulation 2 */
	RTE_CPUFLAG_GFNI,                   /**< Galois Field New Instructions */
	RTE_CPUFLAG_VAES,                   /**< Vector AES */
	RTE_CPUFLAG_VPCLMULQDQ,             /**< Vector Carry-less Multiply */
	RTE_CPUFLAG_AVX512VNNI,
	/**< AVX512 Vector Neural Network Instructions */
	RTE_CPUFLAG_AVX512BITALG,           /**< AVX512 Bit Algorithms */
	RTE_CPUFLAG_AVX512VPOPCNTDQ,        /**< AVX512 Vector Popcount */
	RTE_CPUFLAG_CLDEMOTE,               /**< Cache Line Demote */
	RTE_CPUFLAG_MOVDIRI,                /**< Direct Store Instructions */
	RTE_CPUFLAG_MOVDIR64B,              /**< Direct Store Instructions 64B */
	RTE_CPUFLAG_AVX512VP2INTERSECT,     /**< AVX512 Two Register Intersection */

	RTE_CPUFLAG_WAITPKG,                /**< UMONITOR/UMWAIT/TPAUSE */

	/* The last item */
	RTE_CPUFLAG_NUMFLAGS,               /**< This should always be the last! */
};

#include "generic/rte_cpuflags.h"

#ifdef __cplusplus
}
#endif

#endif /* _RTE_CPUFLAGS_X86_64_H_ */

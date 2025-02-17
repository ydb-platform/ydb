/**********************************************************************
  Copyright(c) 2020 Arm Corporation All rights reserved.

  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions
  are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in
      the documentation and/or other materials provided with the
      distribution.
    * Neither the name of Arm Corporation nor the names of its
      contributors may be used to endorse or promote products derived
      from this software without specific prior written permission.

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
  A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
  THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
**********************************************************************/
#ifndef __AARCH64_MULTIBINARY_H__
#define __AARCH64_MULTIBINARY_H__
#ifndef __aarch64__
#error "This file is for aarch64 only"
#endif
#include "aarch64_label.h"
#ifdef __ASSEMBLY__
/**
 * # mbin_interface : the wrapper layer for isal-l api
 *
 * ## references:
 * * https://sourceware.org/git/gitweb.cgi?p=glibc.git;a=blob;f=sysdeps/aarch64/dl-trampoline.S
 * * http://infocenter.arm.com/help/topic/com.arm.doc.ihi0055b/IHI0055B_aapcs64.pdf
 * * https://static.docs.arm.com/ihi0057/b/IHI0057B_aadwarf64.pdf?_ga=2.80574487.1870739014.1564969896-1634778941.1548729310
 *
 * ## Usage:
 * 	1. Define dispather function
 * 	2. name must be \name\()_dispatcher
 * 	3. Prototype should be *"void * \name\()_dispatcher"*
 * 	4. The dispather should return the right function pointer , revision and a string information .
 **/
.macro mbin_interface name:req
	.extern cdecl(\name\()_dispatcher)
	.data
	.balign 8
	.global cdecl(\name\()_dispatcher_info)
#ifndef __APPLE__
	.type   \name\()_dispatcher_info,%object
#endif
	cdecl(\name\()_dispatcher_info):
		.quad   \name\()_mbinit         //func_entry
#ifndef __APPLE__
	.size   \name\()_dispatcher_info,. - \name\()_dispatcher_info
#endif
	.balign 8
	.text
	\name\()_mbinit:
		//save lp fp, sub sp
		.cfi_startproc
		stp     x29, x30, [sp, -224]!

		//add cfi directive to avoid GDB bt cmds error
		//set cfi(Call Frame Information)
		.cfi_def_cfa_offset 224
		.cfi_offset 	    29, -224
		.cfi_offset 	    30, -216

		//save parameter/result/indirect result registers
		stp	x8,  x9,  [sp,   16]
		.cfi_offset 	    8, -208
		.cfi_offset 	    9, -200
		stp	x0,  x1,  [sp,   32]
		.cfi_offset 	    0, -192
		.cfi_offset 	    1, -184
		stp	x2,  x3,  [sp,   48]
		.cfi_offset 	    2, -176
		.cfi_offset 	    3, -168
		stp	x4,  x5,  [sp,   64]
		.cfi_offset 	    4, -160
		.cfi_offset 	    5, -152
		stp	x6,  x7,  [sp,   80]
		.cfi_offset 	    6, -144
		.cfi_offset 	    7, -136
		stp	q0,  q1,  [sp,   96]
		.cfi_offset 	   64, -128
		.cfi_offset 	   65, -112
		stp	q2,  q3,  [sp,  128]
		.cfi_offset 	   66,  -96
		.cfi_offset 	   67,  -80
		stp	q4,  q5,  [sp,  160]
		.cfi_offset 	   68,  -64
		.cfi_offset 	   69,  -48
		stp	q6,  q7,  [sp,  192]
		.cfi_offset 	   70,  -32
		.cfi_offset 	   71,  -16

		/**
		 * The dispatcher functions have the following prototype:
		 * 	void * function_dispatcher(void)
		 * As the dispatcher is returning a struct, by the AAPCS,
		 */


		bl cdecl(\name\()_dispatcher)
		//restore temp/indirect result registers
		ldp	x8,  x9,  [sp,    16]
		.cfi_restore 8
		.cfi_restore 9

		//	save function entry
		str	x0,  [x9]

		//restore parameter/result registers
		ldp	x0,  x1,  [sp,    32]
		.cfi_restore 0
		.cfi_restore 1
		ldp	x2,  x3,  [sp,    48]
		.cfi_restore 2
		.cfi_restore 3
		ldp	x4,  x5,  [sp,    64]
		.cfi_restore 4
		.cfi_restore 5
		ldp	x6,  x7,  [sp,    80]
		.cfi_restore 6
		.cfi_restore 7
		ldp	q0,  q1,  [sp,    96]
		.cfi_restore 64
		.cfi_restore 65
		ldp	q2,  q3,  [sp,   128]
		.cfi_restore 66
		.cfi_restore 67
		ldp	q4,  q5,  [sp,   160]
		.cfi_restore 68
		.cfi_restore 69
		ldp	q6,  q7,  [sp,   192]
		.cfi_restore 70
		.cfi_restore 71
		//save lp fp and sp
		ldp     x29, x30, [sp], 224
		//restore cfi setting
		.cfi_restore 30
		.cfi_restore 29
		.cfi_def_cfa_offset 0
		.cfi_endproc

	.global cdecl(\name)
#ifndef __APPLE__
	.type \name,%function
#endif
	.align  2
	cdecl(\name\()):
#ifndef __APPLE__
		adrp    x9, :got:\name\()_dispatcher_info
		ldr     x9, [x9, #:got_lo12:\name\()_dispatcher_info]
#else
		adrp    x9, cdecl(\name\()_dispatcher_info)@GOTPAGE
		ldr     x9, [x9, #cdecl(\name\()_dispatcher_info)@GOTPAGEOFF]
#endif
		ldr     x10,[x9]
		br      x10
#ifndef __APPLE__
	.size \name,. - \name
#endif
.endm

/**
 * mbin_interface_base is used for the interfaces which have only
 * noarch implementation
 */
.macro mbin_interface_base name:req, base:req
	.extern \base
	.data
	.balign 8
	.global cdecl(\name\()_dispatcher_info)
#ifndef __APPLE__
	.type   \name\()_dispatcher_info,%object
#endif
	cdecl(\name\()_dispatcher_info):
		.quad   \base         //func_entry
#ifndef __APPLE__
	.size   \name\()_dispatcher_info,. - \name\()_dispatcher_info
#endif
	.balign 8
	.text
	.global cdecl(\name)
#ifndef __APPLE__
	.type \name,%function
#endif
	.align  2
	cdecl(\name\()):
#ifndef __APPLE__
		adrp    x9, :got:cdecl(_\name\()_dispatcher_info)
		ldr     x9, [x9, #:got_lo12:cdecl(_\name\()_dispatcher_info)]
#else
		adrp    x9, cdecl(_\name\()_dispatcher_info)@GOTPAGE
		ldr     x9, [x9, #cdecl(_\name\()_dispatcher_info)@GOTPAGEOFF]
#endif
		ldr     x10,[x9]
		br      x10
#ifndef __APPLE__
	.size \name,. - \name
#endif
.endm

#else /* __ASSEMBLY__ */
#include <stdint.h>
#if defined(__linux__)
#include <sys/auxv.h>
#include <asm/hwcap.h>
#elif defined(__APPLE__)
#define SYSCTL_PMULL_KEY "hw.optional.arm.FEAT_PMULL" // from macOS 12 FEAT_* sysctl infos are available
#define SYSCTL_CRC32_KEY "hw.optional.armv8_crc32"
#define SYSCTL_SVE_KEY "hw.optional.arm.FEAT_SVE" // this one is just a guess and need to check macOS update
#include <sys/sysctl.h>
#include <stddef.h>
static inline int sysctlEnabled(const char* name){
	int enabled;
	size_t size = sizeof(enabled);
	int status = sysctlbyname(name, &enabled, &size, NULL, 0);
	return status ? 0 : enabled;
}
#endif


#define DEFINE_INTERFACE_DISPATCHER(name)                               \
	void * name##_dispatcher(void)

#define PROVIDER_BASIC(name)                                            \
	PROVIDER_INFO(name##_base)

#define DO_DIGNOSTIC(x)	_Pragma GCC diagnostic ignored "-W"#x
#define DO_PRAGMA(x) _Pragma (#x)
#define DIGNOSTIC_IGNORE(x) DO_PRAGMA(GCC diagnostic ignored #x)
#define DIGNOSTIC_PUSH()	DO_PRAGMA(GCC diagnostic push)
#define DIGNOSTIC_POP()		DO_PRAGMA(GCC diagnostic pop)


#define PROVIDER_INFO(_func_entry)                                  	\
	({	DIGNOSTIC_PUSH()					\
		DIGNOSTIC_IGNORE(-Wnested-externs)			\
		extern void  _func_entry(void);				\
		DIGNOSTIC_POP()						\
		_func_entry;						\
	})

/**
 * Micro-Architector definitions
 * Reference: https://developer.arm.com/docs/ddi0595/f/aarch64-system-registers/midr_el1
 */

#define CPU_IMPLEMENTER_RESERVE			0x00
#define CPU_IMPLEMENTER_ARM			0x41


#define CPU_PART_CORTEX_A57		0xD07
#define CPU_PART_CORTEX_A72		0xD08
#define CPU_PART_NEOVERSE_N1		0xD0C

#define MICRO_ARCH_ID(imp,part)	\
	(((CPU_IMPLEMENTER_##imp&0xff)<<24)|((CPU_PART_##part&0xfff)<<4))

#ifndef HWCAP_CPUID
#define HWCAP_CPUID (1<<11)
#endif

/**
 * @brief  get_micro_arch_id
 *
 * read micro-architector register instruction if possible.This function
 * provides microarchitecture information and make microarchitecture optimization
 * possible.
 *
 * Read system registers(MRS) is forbidden in userspace. If executed, it
 * will raise illegal instruction error. Kernel provides a solution for
 * this issue. The solution depends on HWCAP_CPUID flags. Reference(1)
 * describes how to use it. It provides a "illegal insstruction" handler
 * in kernel space, the handler will execute MRS and return the correct
 * value to userspace.
 *
 * To avoid too many kernel trap, this function MUST be only called in
 * dispatcher. And HWCAP must be match,That will make sure there are no
 * illegal instruction errors. HWCAP_CPUID should be available to get the
 * best performance.
 *
 * NOTICE:
 *     - HWCAP_CPUID should be available. Otherwise it returns reserve value
 *     - It MUST be called inside dispather.
 *     - It MUST meet the HWCAP requirements
 *
 * Example:
 *      DEFINE_INTERFACE_DISPATCHER(crc32_iscsi)
 *      {
 *              unsigned long auxval = getauxval(AT_HWCAP);
 *              // MUST do the judgement is MUST.
 *              if ((HWCAP_CRC32 | HWCAP_PMULL) == (auxval & (HWCAP_CRC32 | HWCAP_PMULL))) {
 *                      switch (get_micro_arch_id()) {
 *                      case MICRO_ARCH_ID(ARM, CORTEX_A57):
 *                              return PROVIDER_INFO(crc32_pmull_crc_for_a57);
 *                      case MICRO_ARCH_ID(ARM, CORTEX_A72):
 *                              return PROVIDER_INFO(crc32_pmull_crc_for_a72);
 *                      case MICRO_ARCH_ID(ARM, NEOVERSE_N1):
 *                              return PROVIDER_INFO(crc32_pmull_crc_for_n1);
 *                      case default:
 *                              return PROVIDER_INFO(crc32_pmull_crc_for_others);
 *                      }
 *              }
 *              return PROVIDER_BASIC(crc32_iscsi);
 *      }
 * KNOWN ISSUE:
 *   On a heterogeneous system (big.LITTLE), it will work but the performance
 *   might not be the best one as expected.
 *
 *   If this function is called on the big core, it will return the function
 *   optimized for the big core.
 *
 *   If execution is then scheduled to the little core. It will still work (1),
 *   but the function won't be optimized for the little core, thus the performance
 *   won't be as expected.
 *
 * References:
 * -  [CPU Feature detection](https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/tree/Documentation/arm64/cpu-feature-registers.rst?h=v5.5)
 *
 */
static inline uint32_t get_micro_arch_id(void)
{
	uint32_t id=CPU_IMPLEMENTER_RESERVE;
#ifndef __APPLE__
	if ((getauxval(AT_HWCAP) & HWCAP_CPUID)) {
		/** Here will trap into kernel space */
		asm("mrs %0, MIDR_EL1 " : "=r" (id));
	}
#endif
	return id&0xff00fff0;
}



#endif /* __ASSEMBLY__ */
#endif

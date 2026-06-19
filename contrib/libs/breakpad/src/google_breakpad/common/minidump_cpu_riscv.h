/* minidump_format.h: A cross-platform reimplementation of minidump-related
 * portions of DbgHelp.h from the Windows Platform SDK.
 *
 * (This is C99 source, please don't corrupt it with C++.)
 *
 * This file contains the necessary definitions to read minidump files
 * produced on RISCV and RISCV64. These files may be read on any platform
 * provided that the alignments of these structures on the processing system
 * are identical to the alignments of these structures on the producing
 * system. For this reason, precise-sized types are used. The structures
 * defined by this file have been laid out to minimize alignment problems by
 * ensuring that all members are aligned on their natural boundaries.
 * In some cases, tail-padding may be significant when different ABIs specify
 * different tail-padding behaviors. To avoid problems when reading or
 * writing affected structures, MD_*_SIZE macros are provided where needed,
 * containing the useful size of the structures without padding.
 *
 * Structures that are defined by Microsoft to contain a zero-length array
 * are instead defined here to contain an array with one element, as
 * zero-length arrays are forbidden by standard C and C++. In these cases,
 * *_minsize constants are provided to be used in place of sizeof. For a
 * cleaner interface to these sizes when using C++, see minidump_size.h.
 *
 * These structures are also sufficient to populate minidump files.
 *
 * Because precise data type sizes are crucial for this implementation to
 * function properly and portably, a set of primitive types with known sizes
 * are used as the basis of each structure defined by this file.
 *
 * Author: Iacopo Colonnelli
 */

/*
 * RISCV and RISCV64 support
 */

#ifndef GOOGLE_BREAKPAD_COMMON_MINIDUMP_CPU_RISCV_H__
#define GOOGLE_BREAKPAD_COMMON_MINIDUMP_CPU_RISCV_H__

#include "google_breakpad/common/breakpad_types.h"

#define MD_FLOATINGSAVEAREA_RISCV_FPR_COUNT 32
#if defined(__riscv)
# if __riscv_flen == 32
typedef uint32_t riscv_fpr_size;
# elif __riscv_flen == 64
typedef uint64_t riscv_fpr_size;
# elif __riscv_flen == 128
typedef uint128_struct riscv_fpr_size;
# else
#  error "Unexpected __riscv_flen"
# endif
#else
typedef uint32_t riscv_fpr_size;
#endif

#define MD_CONTEXT_RISCV_GPR_COUNT 32

typedef struct {
    /* 32 floating point registers, f0 .. f31. */
    riscv_fpr_size regs[MD_FLOATINGSAVEAREA_RISCV_FPR_COUNT];
    uint32_t fpcsr;
} MDFloatingSaveAreaRISCV;

enum MDRISCVRegisterNumbers {
  MD_CONTEXT_RISCV_REG_PC     = 0,
  MD_CONTEXT_RISCV_REG_RA     = 1,
  MD_CONTEXT_RISCV_REG_SP     = 2,
};

/* For (MDRawContextRISCV).context_flags.  These values indicate the type of
 * context stored in the structure. */
#define MD_CONTEXT_RISCV 0x00800000
#define MD_CONTEXT_RISCV_INTEGER (MD_CONTEXT_RISCV | 0x00000001)
#define MD_CONTEXT_RISCV_FLOATING_POINT (MD_CONTEXT_RISCV | 0x00000004)
#define MD_CONTEXT_RISCV_FULL (MD_CONTEXT_RISCV_INTEGER | \
                               MD_CONTEXT_RISCV_FLOATING_POINT)

typedef struct {
  /* Determines which fields of this struct are populated */
  uint32_t context_flags;

  uint32_t pc;
  uint32_t ra;
  uint32_t sp;
  uint32_t gp;
  uint32_t tp;
  uint32_t t0;
  uint32_t t1;
  uint32_t t2;
  uint32_t s0;
  uint32_t s1;
  uint32_t a0;
  uint32_t a1;
  uint32_t a2;
  uint32_t a3;
  uint32_t a4;
  uint32_t a5;
  uint32_t a6;
  uint32_t a7;
  uint32_t s2;
  uint32_t s3;
  uint32_t s4;
  uint32_t s5;
  uint32_t s6;
  uint32_t s7;
  uint32_t s8;
  uint32_t s9;
  uint32_t s10;
  uint32_t s11;
  uint32_t t3;
  uint32_t t4;
  uint32_t t5;
  uint32_t t6;

  MDFloatingSaveAreaRISCV float_save;
} MDRawContextRISCV;

/* For (MDRawContextRISCV64).context_flags.  These values indicate the type of
 * context stored in the structure. */
#define MD_CONTEXT_RISCV64 0x08000000
#define MD_CONTEXT_RISCV64_INTEGER (MD_CONTEXT_RISCV64 | 0x00000001)
#define MD_CONTEXT_RISCV64_FLOATING_POINT (MD_CONTEXT_RISCV64 | 0x00000004)
#define MD_CONTEXT_RISCV64_FULL (MD_CONTEXT_RISCV64_INTEGER | \
                                 MD_CONTEXT_RISCV64_FLOATING_POINT)

typedef struct {
  /* Determines which fields of this struct are populated */
  uint32_t context_flags;

  uint64_t pc;
  uint64_t ra;
  uint64_t sp;
  uint64_t gp;
  uint64_t tp;
  uint64_t t0;
  uint64_t t1;
  uint64_t t2;
  uint64_t s0;
  uint64_t s1;
  uint64_t a0;
  uint64_t a1;
  uint64_t a2;
  uint64_t a3;
  uint64_t a4;
  uint64_t a5;
  uint64_t a6;
  uint64_t a7;
  uint64_t s2;
  uint64_t s3;
  uint64_t s4;
  uint64_t s5;
  uint64_t s6;
  uint64_t s7;
  uint64_t s8;
  uint64_t s9;
  uint64_t s10;
  uint64_t s11;
  uint64_t t3;
  uint64_t t4;
  uint64_t t5;
  uint64_t t6;

  MDFloatingSaveAreaRISCV float_save;
} MDRawContextRISCV64;


#endif  /* GOOGLE_BREAKPAD_COMMON_MINIDUMP_CPU_RISCV_H__ */

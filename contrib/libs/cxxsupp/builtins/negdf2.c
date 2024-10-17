//===-- lib/negdf2.c - double-precision negation ------------------*- C -*-===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is dual licensed under the MIT and the University of Illinois Open
// Source Licenses. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//
//
// This file implements double-precision soft-float negation.
//
//===----------------------------------------------------------------------===//

#define DOUBLE_PRECISION
#include "fp_lib.h"

ARM_EABI_FNALIAS(dneg, negdf2)

COMPILER_RT_ABI fp_t
__negdf2(fp_t a) {
    return fromRep(toRep(a) ^ signBit);
}

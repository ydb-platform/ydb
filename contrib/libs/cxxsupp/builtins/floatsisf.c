//===-- lib/floatsisf.c - integer -> single-precision conversion --*- C -*-===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is dual licensed under the MIT and the University of Illinois Open
// Source Licenses. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//
//
// This file implements integer to single-precision conversion for the
// compiler-rt library in the IEEE-754 default round-to-nearest, ties-to-even
// mode.
//
//===----------------------------------------------------------------------===//

#define SINGLE_PRECISION
#include "fp_lib.h"

#include "int_lib.h"

ARM_EABI_FNALIAS(i2f, floatsisf)

COMPILER_RT_ABI fp_t
__floatsisf(int a) {
    
    const int aWidth = sizeof a * CHAR_BIT;
    
    // Handle zero as a special case to protect clz
    if (a == 0)
        return fromRep(0);
    
    // All other cases begin by extracting the sign and absolute value of a
    rep_t sign = 0;
    if (a < 0) {
        sign = signBit;
        a = -a;
    }
    
    // Exponent of (fp_t)a is the width of abs(a).
    const int exponent = (aWidth - 1) - __builtin_clz(a);
    rep_t result;
    
    // Shift a into the significand field, rounding if it is a right-shift
    if (exponent <= significandBits) {
        const int shift = significandBits - exponent;
        result = (rep_t)a << shift ^ implicitBit;
    } else {
        const int shift = exponent - significandBits;
        result = (rep_t)a >> shift ^ implicitBit;
        rep_t round = (rep_t)a << (typeWidth - shift);
        if (round > signBit) result++;
        if (round == signBit) result += result & 1;
    }
    
    // Insert the exponent
    result += (rep_t)(exponent + exponentBias) << significandBits;
    // Insert the sign bit and return
    return fromRep(result | sign);
}

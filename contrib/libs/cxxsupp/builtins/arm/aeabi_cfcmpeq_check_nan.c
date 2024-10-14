//===-- lib/arm/aeabi_cfcmpeq_helper.c - Helper for cdcmpeq ---------------===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is dual licensed under the MIT and the University of Illinois Open
// Source Licenses. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//

#include <stdint.h>

__attribute__((pcs("aapcs")))
__attribute__((visibility("hidden")))
int __aeabi_cfcmpeq_check_nan(float a, float b) {
    return __builtin_isnan(a) || __builtin_isnan(b);
}

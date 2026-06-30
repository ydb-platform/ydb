//===----------------------------------------------------------------------===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//
///
/// \file
/// Compatibility header for __arm_streaming_compatible attribute.
/// This attribute is only available in clang 18 and later.
///
//===----------------------------------------------------------------------===//

#ifndef SME_COMPAT_H
#define SME_COMPAT_H

// __arm_streaming_compatible attribute is available in clang 18+
#if defined(__clang__) && (__clang_major__ >= 18)
  // Attribute is supported, use as-is
  #define ARM_STREAMING_COMPATIBLE __arm_streaming_compatible
#else
  // Attribute not supported in older clang versions, define as empty
  #define ARM_STREAMING_COMPATIBLE
#endif

#endif // SME_COMPAT_H

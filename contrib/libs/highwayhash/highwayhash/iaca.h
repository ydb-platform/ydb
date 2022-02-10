// Copyright 2017 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef HIGHWAYHASH_IACA_H_
#define HIGHWAYHASH_IACA_H_

// WARNING: this is a "restricted" header because it is included from
// translation units compiled with different flags. This header and its
// dependencies must not define any function unless it is static inline and/or
// within namespace HH_TARGET_NAME. See arch_specific.h for details.

#include "highwayhash/compiler_specific.h"

// IACA (Intel's Code Analyzer, go/intel-iaca) analyzes instruction latencies,
// but only for code between special markers. These functions embed such markers
// in an executable, but only for reading via IACA - they deliberately trigger
// a crash if executed to ensure they are removed in normal builds.

// Default off; callers must `#define HH_ENABLE_IACA 1` before including this.
#ifndef HH_ENABLE_IACA
#define HH_ENABLE_IACA 0
#endif

namespace highwayhash {

#if HH_ENABLE_IACA && (HH_GCC_VERSION || HH_CLANG_VERSION)

// Call before the region of interest. Fences hopefully prevent reordering.
static HH_INLINE void BeginIACA() {
  HH_COMPILER_FENCE;
  asm volatile(
      ".byte 0x0F, 0x0B\n\t"  // UD2
      "movl $111, %ebx\n\t"
      ".byte 0x64, 0x67, 0x90\n\t");
  HH_COMPILER_FENCE;
}

// Call after the region of interest. Fences hopefully prevent reordering.
static HH_INLINE void EndIACA() {
  HH_COMPILER_FENCE;
  asm volatile(
      "movl $222, %ebx\n\t"
      ".byte 0x64, 0x67, 0x90\n\t"
      ".byte 0x0F, 0x0B\n\t");  // UD2
  HH_COMPILER_FENCE;
}

#endif

}  // namespace highwayhash

#endif  // HIGHWAYHASH_IACA_H_

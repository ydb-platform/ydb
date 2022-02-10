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

#ifndef HIGHWAYHASH_INSTRUCTION_SETS_H_
#define HIGHWAYHASH_INSTRUCTION_SETS_H_

// Calls the best specialization of a template supported by the current CPU.
//
// Usage: for each dispatch site, declare a Functor template with a 'Target'
// argument, add a source file defining its operator() and instantiating
// Functor<HH_TARGET>, add a cc_library_for_targets rule for that source file,
// and call InstructionSets::Run<Functor>(/*args*/).

#include <utility>  // std::forward

#include "highwayhash/arch_specific.h"  // HH_TARGET_*
#include "highwayhash/compiler_specific.h"

namespace highwayhash {

// Detects TargetBits and calls specializations of a user-defined functor.
class InstructionSets {
 public:
// Returns bit array of HH_TARGET_* supported by the current CPU.
// The HH_TARGET_Portable bit is guaranteed to be set.
#if HH_ARCH_X64
  static TargetBits Supported();
#else
  static HH_INLINE TargetBits Supported() { return HH_TARGET_Portable; }
#endif

  // Chooses the best available "Target" for the current CPU, runs the
  // corresponding Func<Target>::operator()(args) and returns that Target
  // (a single bit). The overhead of dispatching is low, about 4 cycles, but
  // this should only be called infrequently (e.g. hoisting it out of loops).
  template <template <TargetBits> class Func, typename... Args>
  static HH_INLINE TargetBits Run(Args&&... args) {
#if HH_ARCH_X64
    const TargetBits supported = Supported();
    if (supported & HH_TARGET_AVX2) {
      Func<HH_TARGET_AVX2>()(std::forward<Args>(args)...);
      return HH_TARGET_AVX2;
    }
    if (supported & HH_TARGET_SSE41) {
      Func<HH_TARGET_SSE41>()(std::forward<Args>(args)...);
      return HH_TARGET_SSE41;
    }
#endif  // HH_ARCH_X64

    Func<HH_TARGET_Portable>()(std::forward<Args>(args)...);
    return HH_TARGET_Portable;
  }

  // Calls Func<Target>::operator()(args) for all Target supported by the
  // current CPU, and returns their HH_TARGET_* bits.
  template <template <TargetBits> class Func, typename... Args>
  static HH_INLINE TargetBits RunAll(Args&&... args) {
#if HH_ARCH_X64
    const TargetBits supported = Supported();
    if (supported & HH_TARGET_AVX2) {
      Func<HH_TARGET_AVX2>()(std::forward<Args>(args)...);
    }
    if (supported & HH_TARGET_SSE41) {
      Func<HH_TARGET_SSE41>()(std::forward<Args>(args)...);
    }
#else
    const TargetBits supported = HH_TARGET_Portable;
#endif  // HH_ARCH_X64

    Func<HH_TARGET_Portable>()(std::forward<Args>(args)...);
    return supported;  // i.e. all that were run
  }
};

}  // namespace highwayhash

#endif  // HIGHWAYHASH_INSTRUCTION_SETS_H_

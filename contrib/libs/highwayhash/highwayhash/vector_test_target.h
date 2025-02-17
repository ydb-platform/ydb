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

#ifndef HIGHWAYHASH_VECTOR_TEST_TARGET_H_
#define HIGHWAYHASH_VECTOR_TEST_TARGET_H_

// WARNING: this is a "restricted" header because it is included from
// translation units compiled with different flags. This header and its
// dependencies must not define any function unless it is static inline and/or
// within namespace HH_TARGET_NAME. See arch_specific.h for details.

#include "highwayhash/arch_specific.h"
#include "highwayhash/hh_types.h"

namespace highwayhash {

// Usage: InstructionSets::RunAll<VectorTest>(). Calls "notify" for each test
// failure.
template <TargetBits Target>
struct VectorTest {
  void operator()(const HHNotify notify) const;
};

}  // namespace highwayhash

#endif  // HIGHWAYHASH_VECTOR_TEST_TARGET_H_

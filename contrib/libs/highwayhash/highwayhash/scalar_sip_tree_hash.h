// Copyright 2015 Google Inc. All Rights Reserved.
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

#ifndef HIGHWAYHASH_SCALAR_SIP_TREE_HASH_H_
#define HIGHWAYHASH_SCALAR_SIP_TREE_HASH_H_

// Scalar (non-vector/SIMD) version for comparison purposes.

#include "highwayhash/state_helpers.h"

#ifdef __cplusplus
namespace highwayhash {
extern "C" {
#endif

HH_U64 ScalarSipTreeHash(const HH_U64 (&key)[4], const char* bytes,
                         const HH_U64 size);
HH_U64 ScalarSipTreeHash13(const HH_U64 (&key)[4], const char* bytes,
                           const HH_U64 size);

#ifdef __cplusplus
}  // extern "C"
}  // namespace highwayhash
#endif

#endif  // HIGHWAYHASH_SCALAR_SIP_TREE_HASH_H_

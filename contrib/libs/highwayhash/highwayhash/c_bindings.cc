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

#include "highwayhash/c_bindings.h"

#include "highwayhash/highwayhash_target.h"
#include "highwayhash/instruction_sets.h"

using highwayhash::InstructionSets;
using highwayhash::HighwayHash;

extern "C" {

// Ideally this would reside in highwayhash_target.cc, but that file is
// compiled multiple times and we must only define this function once.
uint64_t HighwayHash64(const HHKey key, const char* bytes,
                       const uint64_t size) {
  HHResult64 result;
  InstructionSets::Run<HighwayHash>(*reinterpret_cast<const HHKey*>(key), bytes,
                                    size, &result);
  return result;
}

}  // extern "C"

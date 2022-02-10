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

// WARNING: this is a "restricted" source file; avoid including any headers
// unless they are also restricted. See arch_specific.h for details.

#include "highwayhash/highwayhash_target.h"

#include "highwayhash/highwayhash.h"

#ifndef HH_DISABLE_TARGET_SPECIFIC
namespace highwayhash {

extern "C" {
uint64_t HH_ADD_TARGET_SUFFIX(HighwayHash64_)(const HHKey key,
                                              const char* bytes,
                                              const uint64_t size) {
  HHStateT<HH_TARGET> state(key);
  HHResult64 result;
  HighwayHashT(&state, bytes, size, &result);
  return result;
}
}  // extern "C"

template <TargetBits Target>
void HighwayHash<Target>::operator()(const HHKey& key,
                                     const char* HH_RESTRICT bytes,
                                     const size_t size,
                                     HHResult64* HH_RESTRICT hash) const {
  HHStateT<Target> state(key);
  HighwayHashT(&state, bytes, size, hash);
}

template <TargetBits Target>
void HighwayHash<Target>::operator()(const HHKey& key,
                                     const char* HH_RESTRICT bytes,
                                     const size_t size,
                                     HHResult128* HH_RESTRICT hash) const {
  HHStateT<Target> state(key);
  HighwayHashT(&state, bytes, size, hash);
}

template <TargetBits Target>
void HighwayHash<Target>::operator()(const HHKey& key,
                                     const char* HH_RESTRICT bytes,
                                     const size_t size,
                                     HHResult256* HH_RESTRICT hash) const {
  HHStateT<Target> state(key);
  HighwayHashT(&state, bytes, size, hash);
}

template <TargetBits Target>
void HighwayHashCat<Target>::operator()(const HHKey& key,
                                        const StringView* HH_RESTRICT fragments,
                                        const size_t num_fragments,
                                        HHResult64* HH_RESTRICT hash) const {
  HighwayHashCatT<Target> cat(key);
  for (size_t i = 0; i < num_fragments; ++i) {
    cat.Append(fragments[i].data, fragments[i].num_bytes);
  }
  cat.Finalize(hash);
}

template <TargetBits Target>
void HighwayHashCat<Target>::operator()(const HHKey& key,
                                        const StringView* HH_RESTRICT fragments,
                                        const size_t num_fragments,
                                        HHResult128* HH_RESTRICT hash) const {
  HighwayHashCatT<Target> cat(key);
  for (size_t i = 0; i < num_fragments; ++i) {
    cat.Append(fragments[i].data, fragments[i].num_bytes);
  }
  cat.Finalize(hash);
}

template <TargetBits Target>
void HighwayHashCat<Target>::operator()(const HHKey& key,
                                        const StringView* HH_RESTRICT fragments,
                                        const size_t num_fragments,
                                        HHResult256* HH_RESTRICT hash) const {
  HighwayHashCatT<Target> cat(key);
  for (size_t i = 0; i < num_fragments; ++i) {
    cat.Append(fragments[i].data, fragments[i].num_bytes);
  }
  cat.Finalize(hash);
}

// Instantiate for the current target.
template struct HighwayHash<HH_TARGET>;
template struct HighwayHashCat<HH_TARGET>;

}  // namespace highwayhash
#endif  // HH_DISABLE_TARGET_SPECIFIC

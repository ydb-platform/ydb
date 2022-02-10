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

#ifndef HIGHWAYHASH_HIGHWAYHASH_TARGET_H_
#define HIGHWAYHASH_HIGHWAYHASH_TARGET_H_

// Adapter for the InstructionSets::Run dispatcher, which invokes the best
// implementations available on the current CPU.

// WARNING: this is a "restricted" header because it is included from
// translation units compiled with different flags. This header and its
// dependencies must not define any function unless it is static inline and/or
// within namespace HH_TARGET_NAME. See arch_specific.h for details.

#include "highwayhash/arch_specific.h"
#include "highwayhash/compiler_specific.h"
#include "highwayhash/hh_types.h"

namespace highwayhash {

// Usage: InstructionSets::Run<HighwayHash>(key, bytes, size, hash).
// This incurs some small dispatch overhead. If the entire program is compiled
// for the target CPU, you can instead call HighwayHashT directly to avoid any
// overhead. This template is instantiated in the source file, which is
// compiled once for every target with the required flags (e.g. -mavx2).
template <TargetBits Target>
struct HighwayHash {
  // Stores a 64/128/256 bit hash of "bytes" using the HighwayHashT
  // implementation for the "Target" CPU. The hash result is identical
  // regardless of which implementation is used.
  //
  // "key" is a (randomly generated or hard-coded) HHKey.
  // "bytes" is the data to hash (possibly unaligned).
  // "size" is the number of bytes to hash; we do not read any additional bytes.
  // "hash" is a HHResult* (either 64, 128 or 256 bits).
  //
  // HighwayHash is a strong pseudorandom function with security claims
  // [https://arxiv.org/abs/1612.06257]. It is intended as a safer
  // general-purpose hash, 5x faster than SipHash and 10x faster than BLAKE2.
  void operator()(const HHKey& key, const char* HH_RESTRICT bytes,
                  const size_t size, HHResult64* HH_RESTRICT hash) const;
  void operator()(const HHKey& key, const char* HH_RESTRICT bytes,
                  const size_t size, HHResult128* HH_RESTRICT hash) const;
  void operator()(const HHKey& key, const char* HH_RESTRICT bytes,
                  const size_t size, HHResult256* HH_RESTRICT hash) const;
};

// Replacement for C++17 std::string_view that avoids dependencies.
// A struct requires fewer allocations when calling HighwayHashCat with
// non-const "num_fragments".
struct StringView {
  const char* data;  // not necessarily aligned/padded
  size_t num_bytes;  // possibly zero
};

// Note: this interface avoids dispatch overhead per fragment.
template <TargetBits Target>
struct HighwayHashCat {
  // Stores a 64/128/256 bit hash of all "num_fragments" "fragments" using the
  // HighwayHashCatT implementation for "Target". The hash result is identical
  // to HighwayHash of the flattened data, regardless of Target.
  //
  // "key" is a (randomly generated or hard-coded) HHKey.
  // "fragments" contain unaligned pointers and the number of valid bytes.
  // "num_fragments" indicates the number of entries in "fragments".
  // "hash" is a HHResult* (either 64, 128 or 256 bits).
  void operator()(const HHKey& key, const StringView* HH_RESTRICT fragments,
                  const size_t num_fragments,
                  HHResult64* HH_RESTRICT hash) const;
  void operator()(const HHKey& key, const StringView* HH_RESTRICT fragments,
                  const size_t num_fragments,
                  HHResult128* HH_RESTRICT hash) const;
  void operator()(const HHKey& key, const StringView* HH_RESTRICT fragments,
                  const size_t num_fragments,
                  HHResult256* HH_RESTRICT hash) const;
};

}  // namespace highwayhash

#endif  // HIGHWAYHASH_HIGHWAYHASH_TARGET_H_

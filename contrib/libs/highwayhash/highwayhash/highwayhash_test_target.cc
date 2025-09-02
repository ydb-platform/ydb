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

#include "highwayhash/highwayhash_test_target.h"

#include "highwayhash/highwayhash.h"

#ifndef HH_DISABLE_TARGET_SPECIFIC
namespace highwayhash {
namespace {

void NotifyIfUnequal(const size_t size, const HHResult64& expected,
                     const HHResult64& actual, const HHNotify notify) {
  if (expected != actual) {
    (*notify)(TargetName(HH_TARGET), size);
  }
}

// Overload for HHResult128 or HHResult256 (arrays).
template <size_t kNumLanes>
void NotifyIfUnequal(const size_t size, const uint64_t (&expected)[kNumLanes],
                     const uint64_t (&actual)[kNumLanes],
                     const HHNotify notify) {
  for (size_t i = 0; i < kNumLanes; ++i) {
    if (expected[i] != actual[i]) {
      (*notify)(TargetName(HH_TARGET), size);
      return;
    }
  }
}

// Shared logic for all HighwayHashTest::operator() overloads.
template <typename Result>
void TestHighwayHash(HHStateT<HH_TARGET>* HH_RESTRICT state,
                     const char* HH_RESTRICT bytes, const size_t size,
                     const Result* expected, const HHNotify notify) {
  Result actual;
  HighwayHashT(state, bytes, size, &actual);
  NotifyIfUnequal(size, *expected, actual, notify);
}

// Shared logic for all HighwayHashCatTest::operator() overloads.
template <typename Result>
void TestHighwayHashCat(const HHKey& key, const char* HH_RESTRICT bytes,
                        const size_t size, const Result* expected,
                        const HHNotify notify) {
  // Slightly faster to compute the expected prefix hashes only once.
  // Use new instead of vector to avoid headers with inline functions.
  Result* results = new Result[size + 1];
  for (size_t i = 0; i <= size; ++i) {
    HHStateT<HH_TARGET> state_flat(key);
    HighwayHashT(&state_flat, bytes, i, &results[i]);
  }

  // Splitting into three fragments/Append should cover all codepaths.
  const size_t max_fragment_size = size / 3;
  for (size_t size1 = 0; size1 < max_fragment_size; ++size1) {
    for (size_t size2 = 0; size2 < max_fragment_size; ++size2) {
      for (size_t size3 = 0; size3 < max_fragment_size; ++size3) {
        HighwayHashCatT<HH_TARGET> cat(key);
        const char* pos = bytes;
        cat.Append(pos, size1);
        pos += size1;
        cat.Append(pos, size2);
        pos += size2;
        cat.Append(pos, size3);
        pos += size3;

        Result result_cat;
        cat.Finalize(&result_cat);

        const size_t total_size = pos - bytes;
        NotifyIfUnequal(total_size, results[total_size], result_cat, notify);
      }
    }
  }

  delete[] results;
}

}  // namespace

template <TargetBits Target>
void HighwayHashTest<Target>::operator()(const HHKey& key,
                                         const char* HH_RESTRICT bytes,
                                         const size_t size,
                                         const HHResult64* expected,
                                         const HHNotify notify) const {
  HHStateT<Target> state(key);
  TestHighwayHash(&state, bytes, size, expected, notify);
}

template <TargetBits Target>
void HighwayHashTest<Target>::operator()(const HHKey& key,
                                         const char* HH_RESTRICT bytes,
                                         const size_t size,
                                         const HHResult128* expected,
                                         const HHNotify notify) const {
  HHStateT<Target> state(key);
  TestHighwayHash(&state, bytes, size, expected, notify);
}

template <TargetBits Target>
void HighwayHashTest<Target>::operator()(const HHKey& key,
                                         const char* HH_RESTRICT bytes,
                                         const size_t size,
                                         const HHResult256* expected,
                                         const HHNotify notify) const {
  HHStateT<Target> state(key);
  TestHighwayHash(&state, bytes, size, expected, notify);
}

template <TargetBits Target>
void HighwayHashCatTest<Target>::operator()(const HHKey& key,
                                            const char* HH_RESTRICT bytes,
                                            const uint64_t size,
                                            const HHResult64* expected,
                                            const HHNotify notify) const {
  TestHighwayHashCat(key, bytes, size, expected, notify);
}

template <TargetBits Target>
void HighwayHashCatTest<Target>::operator()(const HHKey& key,
                                            const char* HH_RESTRICT bytes,
                                            const uint64_t size,
                                            const HHResult128* expected,
                                            const HHNotify notify) const {
  TestHighwayHashCat(key, bytes, size, expected, notify);
}

template <TargetBits Target>
void HighwayHashCatTest<Target>::operator()(const HHKey& key,
                                            const char* HH_RESTRICT bytes,
                                            const uint64_t size,
                                            const HHResult256* expected,
                                            const HHNotify notify) const {
  TestHighwayHashCat(key, bytes, size, expected, notify);
}

// Instantiate for the current target.
template struct HighwayHashTest<HH_TARGET>;
template struct HighwayHashCatTest<HH_TARGET>;

//-----------------------------------------------------------------------------
// benchmark

namespace {

template <TargetBits Target>
uint64_t RunHighway(const size_t size) {
  static const HHKey key HH_ALIGNAS(32) = {0, 1, 2, 3};
  char in[kMaxBenchmarkInputSize];
  in[0] = static_cast<char>(size & 0xFF);
  HHResult64 result;
  HHStateT<Target> state(key);
  HighwayHashT(&state, in, size, &result);
  return result;
}

template <TargetBits Target>
uint64_t RunHighwayCat(const size_t size) {
  static const HHKey key HH_ALIGNAS(32) = {0, 1, 2, 3};
  HH_ALIGNAS(64) HighwayHashCatT<Target> cat(key);
  char in[kMaxBenchmarkInputSize];
  in[0] = static_cast<char>(size & 0xFF);
  const size_t half_size = size / 2;
  cat.Append(in, half_size);
  cat.Append(in + half_size, size - half_size);
  HHResult64 result;
  cat.Finalize(&result);
  return result;
}

}  // namespace

template <TargetBits Target>
void HighwayHashBenchmark<Target>::operator()(DurationsForInputs* input_map,
                                              NotifyBenchmark notify,
                                              void* context) const {
  MeasureDurations(&RunHighway<Target>, input_map);
  notify("HighwayHash", TargetName(Target), input_map, context);
}

template <TargetBits Target>
void HighwayHashCatBenchmark<Target>::operator()(DurationsForInputs* input_map,
                                                 NotifyBenchmark notify,
                                                 void* context) const {
  MeasureDurations(&RunHighwayCat<Target>, input_map);
  notify("HighwayHashCat", TargetName(Target), input_map, context);
}

// Instantiate for the current target.
template struct HighwayHashBenchmark<HH_TARGET>;
template struct HighwayHashCatBenchmark<HH_TARGET>;

}  // namespace highwayhash
#endif  // HH_DISABLE_TARGET_SPECIFIC

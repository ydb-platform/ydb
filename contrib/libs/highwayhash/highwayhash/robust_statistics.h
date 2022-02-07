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

#ifndef HIGHWAYHASH_ROBUST_STATISTICS_H_
#define HIGHWAYHASH_ROBUST_STATISTICS_H_

// Robust statistics: Mode, Median, MedianAbsoluteDeviation.

#include <stddef.h>
#include <algorithm>
#include <cassert>
#include <cmath>
#include <limits>
#include <vector>

#include "highwayhash/arch_specific.h"
#include "highwayhash/compiler_specific.h"

namespace highwayhash {

// @return i in [idx_begin, idx_begin + half_count) that minimizes
// sorted[i + half_count] - sorted[i].
template <typename T>
size_t MinRange(const T* const HH_RESTRICT sorted, const size_t idx_begin,
                const size_t half_count) {
  T min_range = std::numeric_limits<T>::max();
  size_t min_idx = 0;

  for (size_t idx = idx_begin; idx < idx_begin + half_count; ++idx) {
    assert(sorted[idx] <= sorted[idx + half_count]);
    const T range = sorted[idx + half_count] - sorted[idx];
    if (range < min_range) {
      min_range = range;
      min_idx = idx;
    }
  }

  return min_idx;
}

// Returns an estimate of the mode by calling MinRange on successively
// halved intervals. "sorted" must be in ascending order. This is the
// Half Sample Mode estimator proposed by Bickel in "On a fast, robust
// estimator of the mode", with complexity O(N log N). The mode is less
// affected by outliers in highly-skewed distributions than the median.
// The averaging operation below assumes "T" is an unsigned integer type.
template <typename T>
T Mode(const T* const HH_RESTRICT sorted, const size_t num_values) {
  size_t idx_begin = 0;
  size_t half_count = num_values / 2;
  while (half_count > 1) {
    idx_begin = MinRange(sorted, idx_begin, half_count);
    half_count >>= 1;
  }

  const T x = sorted[idx_begin + 0];
  if (half_count == 0) {
    return x;
  }
  assert(half_count == 1);
  const T average = (x + sorted[idx_begin + 1] + 1) / 2;
  return average;
}

// Sorts integral values in ascending order. About 3x faster than std::sort for
// input distributions with very few unique values.
template <class T>
void CountingSort(T* begin, T* end) {
  // Unique values and their frequency (similar to flat_map).
  using Unique = std::pair<T, int>;
  std::vector<Unique> unique;
  for (const T* p = begin; p != end; ++p) {
    const T value = *p;
    const auto pos =
        std::find_if(unique.begin(), unique.end(),
                     [value](const Unique& u) { return u.first == value; });
    if (pos == unique.end()) {
      unique.push_back(std::make_pair(*p, 1));
    } else {
      ++pos->second;
    }
  }

  // Sort in ascending order of value (pair.first).
  std::sort(unique.begin(), unique.end());

  // Write that many copies of each unique value to the array.
  T* HH_RESTRICT p = begin;
  for (const auto& value_count : unique) {
    std::fill(p, p + value_count.second, value_count.first);
    p += value_count.second;
  }
  assert(p == end);
}

// Returns the median value. Side effect: sorts "samples".
template <typename T>
T Median(std::vector<T>* samples) {
  assert(!samples->empty());
  std::sort(samples->begin(), samples->end());
  const size_t half = samples->size() / 2;
  // Odd count: return middle
  if (samples->size() % 2) {
    return (*samples)[half];
  }
  // Even count: return average of middle two.
  return ((*samples)[half] + (*samples)[half - 1]) / 2;
}

// Returns a robust measure of variability.
template <typename T>
T MedianAbsoluteDeviation(const std::vector<T>& samples, const T median) {
  assert(!samples.empty());
  std::vector<T> abs_deviations;
  abs_deviations.reserve(samples.size());
  for (const T sample : samples) {
    abs_deviations.push_back(std::abs(sample - median));
  }
  return Median(&abs_deviations);
}

}  // namespace highwayhash

#endif  // HIGHWAYHASH_ROBUST_STATISTICS_H_

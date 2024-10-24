/*          Copyright Andrei Alexandrescu, 2016-.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          https://boost.org/LICENSE_1_0.txt)
 */
/*          Copyright Danila Kutenin, 2020-.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          https://boost.org/LICENSE_1_0.txt)
 */
// Adjusted from Alexandrescu paper to support arbitrary comparators.
#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <functional>
#include <iterator>
#include <utility>

#include "private/median_common.h"

namespace miniselect {
namespace median_of_ninthers_detail {

template <class Iter, class Compare,
          class DiffType = typename std::iterator_traits<Iter>::difference_type>
void adaptive_quickselect(Iter r, DiffType n, DiffType length, Compare&& comp);

/**
Median of minima
*/
template <class Iter, class Compare,
          class DiffType = typename std::iterator_traits<Iter>::difference_type>
inline DiffType median_of_minima(Iter const r, const DiffType n,
                                 const DiffType length, Compare&& comp) {
  assert(length >= 2);
  assert(n <= length / 6);
  assert(n > 0);
  const DiffType subset = n * 2, computeMinOver = (length - subset) / subset;
  assert(computeMinOver > 0);
  for (DiffType i = 0, j = subset; i < subset; ++i) {
    const DiffType limit = j + computeMinOver;
    DiffType minIndex = j;
    while (++j < limit)
      if (comp(r[j], r[minIndex])) minIndex = j;
    if (comp(r[minIndex], r[i])) std::swap(r[i], r[minIndex]);
    assert(j < length || i + 1 == subset);
  }
  adaptive_quickselect(r, n, subset, comp);
  return median_common_detail::expand_partition(r, DiffType{0}, n, subset,
                                                length, comp);
}

/**
Median of maxima
*/
template <class Iter, class Compare,
          class DiffType = typename std::iterator_traits<Iter>::difference_type>
inline DiffType median_of_maxima(Iter const r, const DiffType n,
                                 const DiffType length, Compare&& comp) {
  assert(length >= 2);
  assert(n < length && n / 5 >= length - n);
  const DiffType subset = (length - n) * 2, subsetStart = length - subset,
                 computeMaxOver = subsetStart / subset;
  assert(computeMaxOver > 0);
  for (DiffType i = subsetStart, j = i - subset * computeMaxOver; i < length;
       ++i) {
    const DiffType limit = j + computeMaxOver;
    DiffType maxIndex = j;
    while (++j < limit)
      if (comp(r[maxIndex], r[j])) maxIndex = j;
    if (comp(r[i], r[maxIndex])) std::swap(r[i], r[maxIndex]);
    assert(j != 0 || i + 1 == length);
  }
  adaptive_quickselect(r + subsetStart, static_cast<DiffType>(length - n),
                       subset, comp);
  return median_common_detail::expand_partition(r, subsetStart, n, length,
                                                length, comp);
}

/**
Partitions r[0 .. length] using a pivot of its own choosing. Attempts to pick a
pivot that approximates the median. Returns the position of the pivot.
*/
template <class Iter, class Compare,
          class DiffType = typename std::iterator_traits<Iter>::difference_type>
inline DiffType median_of_ninthers(Iter const r, const DiffType length,
                                   Compare&& comp) {
  assert(length >= 12);
  const DiffType frac = length <= 1024         ? length / 12
                        : length <= 128 * 1024 ? length / 64
                                               : length / 1024;
  DiffType pivot = frac / 2;
  const DiffType lo = length / 2 - pivot, hi = lo + frac;
  assert(lo >= frac * 4);
  assert(length - hi >= frac * 4);
  assert(lo / 2 >= pivot);
  const DiffType gap = (length - 9 * frac) / 4;
  DiffType a = lo - 4 * frac - gap, b = hi + gap;
  for (DiffType i = lo; i < hi; ++i, a += 3, b += 3) {
    median_common_detail::ninther(
        r, a, static_cast<DiffType>(i - frac), b, static_cast<DiffType>(a + 1),
        i, static_cast<DiffType>(b + 1), static_cast<DiffType>(a + 2),
        static_cast<DiffType>(i + frac), static_cast<DiffType>(b + 2), comp);
  }

  adaptive_quickselect(r + lo, pivot, frac, comp);
  return median_common_detail::expand_partition(
      r, lo, static_cast<DiffType>(lo + pivot), hi, length, comp);
}

/**
Quickselect driver for median_of_ninthers, median_of_minima, and
median_of_maxima. Dispathes to each depending on the relationship between n (the
sought order statistics) and length.
*/
template <class Iter, class Compare, class DiffType>
inline void adaptive_quickselect(Iter r, DiffType n, DiffType length,
                                 Compare&& comp) {
  assert(n < length);
  for (;;) {
    // Decide strategy for partitioning
    if (n == 0) {
      // That would be the max
      DiffType pivot = n;
      for (++n; n < length; ++n)
        if (comp(r[n], r[pivot])) pivot = n;
      std::swap(r[0], r[pivot]);
      return;
    }
    if (n + 1 == length) {
      // That would be the min
      DiffType pivot = 0;
      for (n = 1; n < length; ++n)
        if (comp(r[pivot], r[n])) pivot = n;
      std::swap(r[pivot], r[length - 1]);
      return;
    }
    assert(n < length);
    DiffType pivot;
    if (length <= 16)
      pivot = median_common_detail::pivot_partition(r, n, length, comp) - r;
    else if (n <= length / 6)
      pivot = median_of_minima(r, n, length, comp);
    else if (n / 5 >= length - n)
      pivot = median_of_maxima(r, n, length, comp);
    else
      pivot = median_of_ninthers(r, length, comp);

    // See how the pivot fares
    if (pivot == n) {
      return;
    }
    if (pivot > n) {
      length = pivot;
    } else {
      ++pivot;
      r += pivot;
      length -= pivot;
      n -= pivot;
    }
  }
}

}  // namespace median_of_ninthers_detail

template <class Iter, class Compare>
inline void median_of_ninthers_select(Iter begin, Iter mid, Iter end,
                                      Compare comp) {
  if (mid == end) return;
  using CompType = typename median_common_detail::CompareRefType<Compare>::type;

  median_of_ninthers_detail::adaptive_quickselect<Iter, CompType>(
      begin, mid - begin, end - begin, comp);
}

template <class Iter>
inline void median_of_ninthers_select(Iter begin, Iter mid, Iter end) {
  using T = typename std::iterator_traits<Iter>::value_type;
  median_of_ninthers_select(begin, mid, end, std::less<T>());
}

template <class Iter, class Compare>
inline void median_of_ninthers_partial_sort(Iter begin, Iter mid, Iter end,
                                            Compare comp) {
  if (begin == mid) return;
  using CompType = typename median_common_detail::CompareRefType<Compare>::type;
  using DiffType = typename std::iterator_traits<Iter>::difference_type;

  median_of_ninthers_detail::adaptive_quickselect<Iter, CompType>(
      begin, static_cast<DiffType>(mid - begin - 1), end - begin, comp);
  std::sort<Iter, CompType>(begin, mid, comp);
}

template <class Iter>
inline void median_of_ninthers_partial_sort(Iter begin, Iter mid, Iter end) {
  typedef typename std::iterator_traits<Iter>::value_type T;
  median_of_ninthers_partial_sort(begin, mid, end, std::less<T>());
}

}  // namespace miniselect

/*          Copyright Danila Kutenin, 2020-.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          https://boost.org/LICENSE_1_0.txt)
 */
#pragma once

#include <algorithm>
#include <functional>
#include <iterator>
#include <type_traits>
#include <utility>

namespace miniselect {
namespace heap_select_detail {

template <class Compare>
struct CompareRefType {
  // Pass the comparator by lvalue reference. Or in debug mode, using a
  // debugging wrapper that stores a reference.
  using type = typename std::add_lvalue_reference<Compare>::type;
};

template <class Compare, class Iter>
inline void sift_down(Iter first, Compare comp,
                      typename std::iterator_traits<Iter>::difference_type len,
                      Iter start) {
  using difference_type = typename std::iterator_traits<Iter>::difference_type;
  using value_type = typename std::iterator_traits<Iter>::value_type;
  difference_type child = start - first;

  if (len < 2 || (len - 2) / 2 < child) return;

  child = 2 * child + 1;
  Iter child_i = first + child;

  if ((child + 1) < len && comp(*child_i, *(child_i + 1))) {
    ++child_i;
    ++child;
  }

  if (comp(*child_i, *start)) {
    return;
  }

  value_type top(std::move(*start));
  do {
    *start = std::move(*child_i);
    start = child_i;

    if ((len - 2) / 2 < child) {
      break;
    }

    child = 2 * child + 1;
    child_i = first + child;

    if ((child + 1) < len && comp(*child_i, *(child_i + 1))) {
      ++child_i;
      ++child;
    }
  } while (!comp(*child_i, top));
  *start = std::move(top);
}

template <class Compare, class Iter>
inline void heap_select_loop(Iter first, Iter middle, Iter last, Compare comp) {
  std::make_heap(first, middle, comp);
  typename std::iterator_traits<Iter>::difference_type len = middle - first;
  for (Iter i = middle; i != last; ++i) {
    if (comp(*i, *first)) {
      std::swap(*i, *first);
      sift_down<Compare>(first, comp, len, first);
    }
  }
}

}  // namespace heap_select_detail

template <class Iter, class Compare>
inline void heap_select(Iter first, Iter middle, Iter end, Compare comp) {
  if (middle == end) return;
  heap_select_detail::heap_select_loop(first, middle + 1, end, comp);
  std::swap(*first, *middle);
}

template <class Iter>
inline void heap_select(Iter first, Iter middle, Iter end) {
  heap_select(first, middle, end,
              std::less<typename std::iterator_traits<Iter>::value_type>());
}

template <class Iter, class Compare>
inline void heap_partial_sort(Iter first, Iter middle, Iter end, Compare comp) {
  heap_select_detail::heap_select_loop(first, middle, end, comp);
  std::sort_heap(first, middle, comp);
}

template <class Iter>
inline void heap_partial_sort(Iter first, Iter middle, Iter end) {
  heap_partial_sort(
      first, middle, end,
      std::less<typename std::iterator_traits<Iter>::value_type>());
}

}  // namespace miniselect

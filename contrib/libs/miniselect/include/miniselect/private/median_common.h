/*          Copyright Andrei Alexandrescu, 2016-,
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          https://boost.org/LICENSE_1_0.txt)
 */
/*          Copyright Danila Kutenin, 2020-.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          https://boost.org/LICENSE_1_0.txt)
 */
#pragma once

#include <cassert>
#include <iterator>
#include <type_traits>
#include <utility>

namespace miniselect {
namespace median_common_detail {

template <class Compare>
struct CompareRefType {
  // Pass the comparator by lvalue reference. Or in debug mode, using a
  // debugging wrapper that stores a reference.
  using type = typename std::add_lvalue_reference<Compare>::type;
};
/**
Swaps the median of r[a], r[b], and r[c] into r[b].
*/
template <class Iter, class Compare,
          class DiffType = typename std::iterator_traits<Iter>::difference_type>
inline void median3(Iter r, DiffType a, DiffType b, DiffType c,
                    Compare&& comp) {
  if (comp(r[b], r[a]))  // b < a
  {
    if (comp(r[b], r[c]))  // b < a, b < c
    {
      if (comp(r[c], r[a]))  // b < c < a
        std::swap(r[b], r[c]);
      else  // b < a <= c
        std::swap(r[b], r[a]);
    }
  } else if (comp(r[c], r[b]))  // a <= b, c < b
  {
    if (comp(r[c], r[a]))  // c < a <= b
      std::swap(r[b], r[a]);
    else  // a <= c < b
      std::swap(r[b], r[c]);
  }
}

/**
Sorts in place r[a], r[b], and r[c].
*/
template <class Iter, class Compare,
          class DiffType = typename std::iterator_traits<Iter>::difference_type>
inline void sort3(Iter r, DiffType a, DiffType b, DiffType c, Compare&& comp) {
  typedef typename std::iterator_traits<Iter>::value_type T;
  if (comp(r[b], r[a]))  // b < a
  {
    if (comp(r[c], r[b]))  // c < b < a
    {
      std::swap(r[a], r[c]);  // a < b < c
    } else                    // b < a, b <= c
    {
      T t = std::move(r[a]);
      r[a] = std::move(r[b]);
      if (comp(r[c], t))  // b <= c < a
      {
        r[b] = std::move(r[c]);
        r[c] = std::move(t);
      } else  // b < a <= c
      {
        r[b] = std::move(t);
      }
    }
  } else if (comp(r[c], r[b]))  // a <= b, c < b
  {
    T t = std::move(r[c]);
    r[c] = std::move(r[b]);
    if (comp(t, r[a]))  // c < a < b
    {
      r[b] = std::move(r[a]);
      r[a] = std::move(t);
    } else  // a <= c < b
    {
      r[b] = std::move(t);
    }
  }

  assert(!comp(r[b], r[a]) && !comp(r[c], r[b]));
}

/**
If leanRight == false, swaps the lower median of r[a]...r[d] into r[b] and
the minimum into r[a]. If leanRight == true, swaps the upper median of
r[a]...r[d] into r[c] and the minimum into r[d].
*/
template <bool leanRight, class Iter, class Compare,
          class DiffType = typename std::iterator_traits<Iter>::difference_type>
inline void partition4(Iter r, DiffType a, DiffType b, DiffType c, DiffType d,
                       Compare&& comp) {
  assert(a != b && a != c && a != d && b != c && b != d && c != d);
  /* static */ if (leanRight) {
    // In the median of 5 algorithm, consider r[e] infinite
    if (comp(r[c], r[a])) {
      std::swap(r[a], r[c]);
    }  // a <= c
    if (comp(r[d], r[b])) {
      std::swap(r[b], r[d]);
    }  // a <= c, b <= d
    if (comp(r[d], r[c])) {
      std::swap(r[c], r[d]);  // a <= d, b <= c < d
      std::swap(r[a], r[b]);  // b <= d, a <= c < d
    }                         // a <= c <= d, b <= d
    if (comp(r[c], r[b])) {   // a <= c <= d, c < b <= d
      std::swap(r[b], r[c]);  // a <= b <= c <= d
    }                         // a <= b <= c <= d
  } else {
    // In the median of 5 algorithm consider r[a] infinitely small, then
    // change b->a. c->b, d->c, e->d
    if (comp(r[c], r[a])) {
      std::swap(r[a], r[c]);
    }
    if (comp(r[c], r[b])) {
      std::swap(r[b], r[c]);
    }
    if (comp(r[d], r[a])) {
      std::swap(r[a], r[d]);
    }
    if (comp(r[d], r[b])) {
      std::swap(r[b], r[d]);
    } else {
      if (comp(r[b], r[a])) {
        std::swap(r[a], r[b]);
      }
    }
  }
}

/**
Places the median of r[a]...r[e] in r[c] and partitions the other elements
around it.
*/
template <class Iter, class Compare,
          class DiffType = typename std::iterator_traits<Iter>::difference_type>
inline void partition5(Iter r, DiffType a, DiffType b, DiffType c, DiffType d,
                       DiffType e, Compare&& comp) {
  assert(a != b && a != c && a != d && a != e && b != c && b != d && b != e &&
         c != d && c != e && d != e);
  if (comp(r[c], r[a])) {
    std::swap(r[a], r[c]);
  }
  if (comp(r[d], r[b])) {
    std::swap(r[b], r[d]);
  }
  if (comp(r[d], r[c])) {
    std::swap(r[c], r[d]);
    std::swap(r[a], r[b]);
  }
  if (comp(r[e], r[b])) {
    std::swap(r[b], r[e]);
  }
  if (comp(r[e], r[c])) {
    std::swap(r[c], r[e]);
    if (comp(r[c], r[a])) {
      std::swap(r[a], r[c]);
    }
  } else {
    if (comp(r[c], r[b])) {
      std::swap(r[b], r[c]);
    }
  }
}

/**
Implements Hoare partition.
*/
template <class Iter, class Compare,
          class DiffType = typename std::iterator_traits<Iter>::difference_type>
inline Iter pivot_partition(Iter r, DiffType k, DiffType length,
                            Compare&& comp) {
  assert(k < length);
  std::swap(*r, r[k]);
  DiffType lo = 1, hi = length - 1;
  for (;; ++lo, --hi) {
    for (;; ++lo) {
      if (lo > hi) goto loop_done;
      if (!comp(r[lo], *r)) break;
    }
    // found the left bound:  r[lo] >= r[0]
    assert(lo <= hi);
    for (; comp(*r, r[hi]); --hi) {
    }
    if (lo >= hi) break;
    // found the right bound: r[hi] <= r[0], swap & make progress
    std::swap(r[lo], r[hi]);
  }
loop_done:
  --lo;
  std::swap(r[lo], *r);
  return r + lo;
}

/**
Implements the quickselect algorithm, parameterized with a partition function.
*/
template <class Iter, class Compare, Iter (*partition)(Iter, Iter, Compare)>
inline void quickselect(Iter r, Iter mid, Iter end, Compare&& comp) {
  if (r == end || mid >= end) return;
  assert(r <= mid && mid < end);
  for (;;) switch (end - r) {
      case 1:
        return;
      case 2:
        if (comp(r[1], *r)) std::swap(*r, r[1]);
        return;
      case 3:
        sort3(r, 0, 1, 2, comp);
        return;
      case 4:
        switch (mid - r) {
          case 0:
            goto select_min;
          case 1:
            partition4<false>(r, 0, 1, 2, 3, comp);
            break;
          case 2:
            partition4<true>(r, 0, 1, 2, 3, comp);
            break;
          case 3:
            goto select_max;
          default:
            assert(false);
        }
        return;
      default:
        assert(end - r > 4);
        if (r == mid) {
        select_min:
          auto pivot = r;
          for (++mid; mid < end; ++mid)
            if (comp(*mid, *pivot)) pivot = mid;
          std::swap(*r, *pivot);
          return;
        }
        if (mid + 1 == end) {
        select_max:
          auto pivot = r;
          for (mid = r + 1; mid < end; ++mid)
            if (comp(*pivot, *mid)) pivot = mid;
          std::swap(*pivot, *(end - 1));
          return;
        }
        auto pivot = partition(r, end, comp);
        if (pivot == mid) return;
        if (mid < pivot) {
          end = pivot;
        } else {
          r = pivot + 1;
        }
    }
}

/**
Returns the index of the median of r[a], r[b], and r[c] without writing
anything.
*/
template <class Iter, class Compare,
          class DiffType = typename std::iterator_traits<Iter>::difference_type>
inline DiffType median_index(const Iter r, DiffType a, DiffType b, DiffType c,
                             Compare&& comp) {
  if (comp(r[c], r[a])) std::swap(a, c);
  if (comp(r[c], r[b])) return c;
  if (comp(r[b], r[a])) return a;
  return b;
}

/**
Returns the index of the median of r[a], r[b], r[c], and r[d] without writing
anything. If leanRight is true, computes the upper median. Otherwise, conputes
the lower median.
*/
template <bool leanRight, class Iter, class Compare,
          class DiffType = typename std::iterator_traits<Iter>::difference_type>
inline DiffType median_index(Iter r, DiffType a, DiffType b, DiffType c,
                             DiffType d, Compare&& comp) {
  if (comp(r[d], r[c])) std::swap(c, d);
  if (leanRight) {
    if (comp(r[c], r[a])) {
      assert(comp(r[c], r[a]) && !comp(r[d], r[c]));  // so r[c]) is out
      return median_index(r, a, b, d, comp);
    }
  } else {
    if (!comp(r[d], r[a])) {
      return median_index(r, a, b, c, comp);
    }
  }
  // Could return median_index(r, b, c, d) but we already know r[c] <= r[d]
  if (!comp(r[c], r[b])) return c;
  if (comp(r[d], r[b])) return d;
  return b;
}

/**
Tukey's Ninther: compute the median of r[_1], r[_2], r[_3], then the median of
r[_4], r[_5], r[_6], then the median of r[_7], r[_8], r[_9], and then swap the
median of those three medians into r[_5].
*/
template <class Iter, class Compare,
          class DiffType = typename std::iterator_traits<Iter>::difference_type>
inline void ninther(Iter r, DiffType _1, DiffType _2, DiffType _3, DiffType _4,
                    DiffType _5, DiffType _6, DiffType _7, DiffType _8,
                    DiffType _9, Compare&& comp) {
  _2 = median_index(r, _1, _2, _3, comp);
  _8 = median_index(r, _7, _8, _9, comp);
  if (comp(r[_8], r[_2])) std::swap(_2, _8);
  if (comp(r[_6], r[_4])) std::swap(_4, _6);
  // Here we know that r[_2] and r[_8] are the other two medians and that
  // r[_2] <= r[_8]. We also know that r[_4] <= r[_6]
  if (comp(r[_5], r[_4])) {
    // r[_4] is the median of r[_4], r[_5], r[_6]
  } else if (comp(r[_6], r[_5])) {
    // r[_6] is the median of r[_4], r[_5], r[_6]
    _4 = _6;
  } else {
    // Here we know r[_5] is the median of r[_4], r[_5], r[_6]
    if (comp(r[_5], r[_2])) return std::swap(r[_5], r[_2]);
    if (comp(r[_8], r[_5])) return std::swap(r[_5], r[_8]);
    // This is the only path that returns with no swap
    return;
  }
  // Here we know r[_4] is the median of r[_4], r[_5], r[_6]
  if (comp(r[_4], r[_2]))
    _4 = _2;
  else if (comp(r[_8], r[_4]))
    _4 = _8;
  std::swap(r[_5], r[_4]);
}

/**
Input assumptions:
(a) hi <= rite
(c) the range r[0 .. hi] contains elements no smaller than r[0]
Output guarantee: same as Hoare partition using r[0] as pivot. Returns the new
position of the pivot.
*/
template <class Iter, class Compare,
          class DiffType = typename std::iterator_traits<Iter>::difference_type>
inline DiffType expand_partition_right(Iter r, DiffType hi, DiffType rite,
                                       Compare&& comp) {
  DiffType pivot = 0;
  assert(pivot <= hi);
  assert(hi <= rite);
  // First loop: spend r[pivot .. hi]
  for (; pivot < hi; --rite) {
    if (rite == hi) goto done;
    if (!comp(r[rite], r[0])) continue;
    ++pivot;
    std::swap(r[rite], r[pivot]);
  }
  // Second loop: make left and pivot meet
  for (; rite > pivot; --rite) {
    if (!comp(r[rite], r[0])) continue;
    while (rite > pivot) {
      ++pivot;
      if (comp(r[0], r[pivot])) {
        std::swap(r[rite], r[pivot]);
        break;
      }
    }
  }

done:
  std::swap(r[0], r[pivot]);
  return pivot;
}

/**
Input assumptions:
(a) lo > 0, lo <= pivot
(b) the range r[lo .. pivot] already contains elements no greater than r[pivot]
Output guarantee: Same as Hoare partition around r[pivot]. Returns the new
position of the pivot.
*/
template <class Iter, class Compare,
          class DiffType = typename std::iterator_traits<Iter>::difference_type>
inline DiffType expand_partition_left(Iter r, DiffType lo, DiffType pivot,
                                      Compare&& comp) {
  assert(lo > 0 && lo <= pivot);
  DiffType left = 0;
  const auto oldPivot = pivot;
  for (; lo < pivot; ++left) {
    if (left == lo) goto done;
    if (!comp(r[oldPivot], r[left])) continue;
    --pivot;
    std::swap(r[left], r[pivot]);
  }
  // Second loop: make left and pivot meet
  for (;; ++left) {
    if (left == pivot) break;
    if (!comp(r[oldPivot], r[left])) continue;
    for (;;) {
      if (left == pivot) goto done;
      --pivot;
      if (comp(r[pivot], r[oldPivot])) {
        std::swap(r[left], r[pivot]);
        break;
      }
    }
  }

done:
  std::swap(r[oldPivot], r[pivot]);
  return pivot;
}

/**
Input assumptions:
(a) lo <= pivot, pivot < hi, hi <= length
(b) the range r[lo .. pivot] already contains elements no greater than
r[pivot]
(c) the range r[pivot .. hi] already contains elements no smaller than
r[pivot]
Output guarantee: Same as Hoare partition around r[pivot], returning the new
position of the pivot.
*/
template <class Iter, class Compare,
          class DiffType = typename std::iterator_traits<Iter>::difference_type>
inline DiffType expand_partition(Iter r, DiffType lo, DiffType pivot,
                                 DiffType hi, DiffType length, Compare&& comp) {
  assert(lo <= pivot && pivot < hi && hi <= length);
  --hi;
  --length;
  DiffType left = 0;
  for (;; ++left, --length) {
    for (;; ++left) {
      if (left == lo)
        return pivot + expand_partition_right(r + pivot, hi - pivot,
                                              length - pivot, comp);
      if (comp(r[pivot], r[left])) break;
    }
    for (;; --length) {
      if (length == hi)
        return left +
               expand_partition_left(r + left, lo - left, pivot - left, comp);
      if (!comp(r[pivot], r[length])) break;
    }
    std::swap(r[left], r[length]);
  }
}

}  // namespace median_common_detail
}  // namespace miniselect

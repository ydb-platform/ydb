/*******************************************************************************
 * tlx/sort/strings/multikey_quicksort.hpp
 *
 * Generic multikey quicksort for strings. This is an internal implementation
 * header, see tlx/sort/strings.hpp for public front-end functions.
 *
 * Based on multikey quicksort, a quick sort algorithm for arrays of character
 * strings by Bentley and Sedgewick.
 *
 * J. Bentley and R. Sedgewick. "Fast Algorithms for Sorting and Searching
 * Strings." In Proceedings of 8th Annual ACM-SIAM Symposium on Discrete
 * Algorithms, 1997.
 *
 * http://www.cs.princeton.edu/~rs/strings/index.html
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2015-2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_SORT_STRINGS_MULTIKEY_QUICKSORT_HEADER
#define TLX_SORT_STRINGS_MULTIKEY_QUICKSORT_HEADER

#include "../../sort/strings/insertion_sort.hpp"

#include <algorithm>
#include <cstddef>
#include <utility>

namespace tlx {

//! \addtogroup tlx_sort
//! \{

namespace sort_strings_detail {

template <typename StringSet>
static inline void vec_swap(
    typename StringSet::Iterator a, typename StringSet::Iterator b, size_t n) {
    while (n-- > 0)
        std::swap(*a++, *b++);
}

template <typename StringSet>
static inline typename StringSet::Iterator med3func(
    const StringSet& ss,
    typename StringSet::Iterator a, typename StringSet::Iterator b,
    typename StringSet::Iterator c, size_t depth) {
    typename StringSet::Char va = ss.get_char(*a, depth);
    typename StringSet::Char vb = ss.get_char(*b, depth);
    if (va == vb)
        return a;
    typename StringSet::Char vc = ss.get_char(*c, depth);
    if (vc == va || vc == vb)
        return c;
    return va < vb
           ? (vb < vc ? b : (va < vc ? c : a))
           : (vb > vc ? b : (va < vc ? a : c));
}

/*!
 * Generic multikey quicksort for strings. Based on multikey quicksort, a quick
 * sort algorithm for arrays of character strings by Bentley and Sedgewick. This
 * method requires up to O(maxlcp) memory due to the recursion stack and it runs
 * in expected time O(D + n log n) and worst-case time O(D + n^2).
 *
 * J. Bentley and R. Sedgewick. Fast algorithms for sorting and searching
 * strings. In Proceedings of 8th Annual ACM-SIAM Symposium on Discrete
 * Algorithms, 1997.
 */
template <typename StringPtr>
static inline void multikey_quicksort(
    const StringPtr& strptr, size_t depth, size_t memory) {

    typedef typename StringPtr::StringSet StringSet;
    typedef typename StringSet::Iterator Iterator;

    const StringSet& ss = strptr.active();
    const Iterator a = ss.begin();
    size_t n = ss.size();

    // try to estimate the amount of memory in a stack frame
    static const size_t memory_use =
        2 * sizeof(size_t) + sizeof(StringSet) + 5 * sizeof(Iterator);

    if (n < 32 || (memory != 0 && memory < memory_use + 1)) {
        return insertion_sort(strptr, depth, memory);
    }

    ptrdiff_t r;
    Iterator pa, pb, pc, pd, pn;

    {
        Iterator pl = a;
        Iterator pm = a + (n / 2);
        pn = a + (n - 1);
        if (n > 30) {
            // on big arrays: pseudomedian of 9
            size_t d = (n / 8);
            pl = med3func(ss, pl, pl + d, pl + 2 * d, depth);
            pm = med3func(ss, pm - d, pm, pm + d, depth);
            pn = med3func(ss, pn - 2 * d, pn - d, pn, depth);
        }
        pm = med3func(ss, pl, pm, pn, depth);
        std::swap(*a, *pm);
        int pivot = ss.get_char(*a, depth);
        pa = pb = a + 1;
        pc = pd = a + n - 1;
        for ( ; ; ) {
            while (pb <= pc && (r = static_cast<int>(ss.get_char(*pb, depth)) - pivot) <= 0) {
                if (r == 0) std::swap(*pa++, *pb);
                pb++;
            }
            while (pb <= pc && (r = static_cast<int>(ss.get_char(*pc, depth)) - pivot) >= 0) {
                if (r == 0) std::swap(*pc, *pd--);
                pc--;
            }
            if (pb > pc) break;
            std::swap(*pb++, *pc--);
        }
        pn = a + n;

        size_t pe_start_index, pe_end_index;
        r = std::min(pa - a, pb - pa);
        vec_swap<StringSet>(a, pb - r, r);
        pe_start_index = r;
        r = std::min(pd - pc, pn - pd - 1);
        pe_end_index = (pn - a) - r;
        vec_swap<StringSet>(pb, pn - r, r);
        if (pivot == 0) {
            for (size_t it = pe_start_index + 1; it < pe_end_index; ++it)
                strptr.set_lcp(it, depth);
        }
    }

    r = pb - pa;
    if (r > 0) {
        strptr.set_lcp(a - ss.begin() + r, depth);
    }
    if (r > 1) {
        multikey_quicksort(strptr.sub(a - ss.begin(), r),
                           depth, memory - memory_use);
    }
    if (ss.get_char(*(a + r), depth) != 0) {
        multikey_quicksort(
            strptr.sub(a - ss.begin() + r, (pa - a) + (pn - pd - 1)),
            depth + 1, memory - memory_use);
    }
    r = pd - pc;
    if (r > 0) {
        strptr.set_lcp(a - ss.begin() + n - r, depth);
    }
    if ((r = pd - pc) > 1) {
        multikey_quicksort(strptr.sub(a - ss.begin() + n - r, r),
                           depth, memory - memory_use);
    }
}

} // namespace sort_strings_detail

//! \}

} // namespace tlx

#endif // !TLX_SORT_STRINGS_MULTIKEY_QUICKSORT_HEADER

/******************************************************************************/

/*******************************************************************************
 * tlx/algorithm/is_sorted_cmp.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_ALGORITHM_IS_SORTED_CMP_HEADER
#define TLX_ALGORITHM_IS_SORTED_CMP_HEADER

#include <functional>

namespace tlx {

//! \addtogroup tlx_algorithm
//! \{

/*!
 * Checks if a range is sorted using a three-way Comparator (with memcmp()
 * semantics). Returns an iterator to the first items not in order.
 */
template <typename ForwardIterator, typename Comparator>
ForwardIterator is_sorted_until_cmp(ForwardIterator first, ForwardIterator last,
                                    Comparator cmp) {
    if (first != last) {
        ForwardIterator next = first;
        while (++next != last) {
            if (cmp(*first, *next) > 0)
                return next;
            first = next;
        }
    }
    return last;
}

/*!
 * Checks if a range is sorted using a three-way Comparator (with memcmp()
 * semantics).
 */
template <typename ForwardIterator, typename Comparator>
bool is_sorted_cmp(ForwardIterator first, ForwardIterator last,
                   Comparator cmp) {
    return is_sorted_until_cmp(first, last, cmp) == last;
}

//! \}

} // namespace tlx

#endif // !TLX_ALGORITHM_IS_SORTED_CMP_HEADER

/******************************************************************************/

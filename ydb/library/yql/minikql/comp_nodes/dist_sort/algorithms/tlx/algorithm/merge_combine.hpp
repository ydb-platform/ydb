/*******************************************************************************
 * tlx/algorithm/merge_combine.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_ALGORITHM_MERGE_COMBINE_HEADER
#define TLX_ALGORITHM_MERGE_COMBINE_HEADER

#include <algorithm>
#include <functional>
#include <iterator>

namespace tlx {

//! \addtogroup tlx_algorithm
//! \{

/*!
 * Merge two sorted ranges and add all items comparing equal. Both ranges must
 * be sorted using the three-way Comparator (with memcmp() semantics). Item
 * pairs comparing equal (cmp returns 0) are combined together using a combine
 * operator.
 *
 * \warning This method does not check if the ranges are sorted. Also make sure
 * that the comparator does not work with unsigned types.
 */
template <typename InputIterator1, typename InputIterator2,
          typename OutputIterator,
          typename Comparator,
          typename Combine = std::plus<
              typename std::iterator_traits<InputIterator1>::value_type> >
OutputIterator merge_combine(
    InputIterator1 first1, InputIterator1 last1,
    InputIterator2 first2, InputIterator2 last2,
    OutputIterator result, Comparator cmp = Comparator(),
    Combine combine = Combine()) {

    while (true)
    {
        // if either range is done -> copy the rest of the other
        if (first1 == last1)
            return std::copy(first2, last2, result);
        if (first2 == last2)
            return std::copy(first1, last1, result);

        // compare both items, copy or combine.
        if (cmp(*first1, *first2) < 0) {
            *result = *first1;
            ++first1;
        }
        else if (cmp(*first1, *first2) > 0) {
            *result = *first2;
            ++first2;
        }
        else {
            *result = combine(*first1, *first2);
            ++first1, ++first2;
        }
        ++result;
    }
}

//! \}

} // namespace tlx

#endif // !TLX_ALGORITHM_MERGE_COMBINE_HEADER

/******************************************************************************/

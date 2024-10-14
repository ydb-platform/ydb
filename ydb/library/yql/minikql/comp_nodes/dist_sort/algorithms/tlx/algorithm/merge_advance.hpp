/*******************************************************************************
 * tlx/algorithm/merge_advance.hpp
 *
 * Variants of binary merge with output size limit.
 *
 * Copied and modified from STXXL, see http://stxxl.org, which itself extracted
 * it from MCSTL http://algo2.iti.uni-karlsruhe.de/singler/mcstl/. Both are
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007 Johannes Singler <singler@ira.uka.de>
 * Copyright (C) 2014-2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_ALGORITHM_MERGE_ADVANCE_HEADER
#define TLX_ALGORITHM_MERGE_ADVANCE_HEADER

#include <algorithm>

namespace tlx {

//! \addtogroup tlx_algorithm
//! \{

/*!
 * Merge routine being able to merge only the \c max_size smallest elements.
 *
 * The \c begin iterators are advanced accordingly, they might not reach \c end,
 * in contrast to the usual variant.
 *
 * \param begin1 Begin iterator of first sequence.
 * \param end1 End iterator of first sequence.
 * \param begin2 Begin iterator of second sequence.
 * \param end2 End iterator of second sequence.
 * \param target Target begin iterator.
 * \param max_size Maximum number of elements to merge.
 * \param comp Comparator.
 * \return Output end iterator.
 */
template <typename RandomAccessIterator1, typename RandomAccessIterator2,
          typename OutputIterator,
          typename DiffType, typename Comparator>
OutputIterator
merge_advance_usual(RandomAccessIterator1& begin1, RandomAccessIterator1 end1,
                    RandomAccessIterator2& begin2, RandomAccessIterator2 end2,
                    OutputIterator target, DiffType max_size,
                    Comparator comp) {
    while (begin1 != end1 && begin2 != end2 && max_size > 0)
    {
        // array1[i1] < array0[i0]
        if (comp(*begin2, *begin1))
            *target++ = *begin2++;
        else
            *target++ = *begin1++;
        --max_size;
    }

    if (begin1 != end1)
    {
        target = std::copy(begin1, begin1 + max_size, target);
        begin1 += max_size;
    }
    else
    {
        target = std::copy(begin2, begin2 + max_size, target);
        begin2 += max_size;
    }
    return target;
}

/*!
 * Merge routine being able to merge only the \c max_size smallest elements.
 *
 * The \c begin iterators are advanced accordingly, they might not reach \c end,
 * in contrast to the usual variant.  Specially designed code should allow the
 * compiler to generate conditional moves instead of branches.
 *
 * \param begin1 Begin iterator of first sequence.
 * \param end1 End iterator of first sequence.
 * \param begin2 Begin iterator of second sequence.
 * \param end2 End iterator of second sequence.
 * \param target Target begin iterator.
 * \param max_size Maximum number of elements to merge.
 * \param comp Comparator.
 * \return Output end iterator.
 */
template <typename RandomAccessIterator1, typename RandomAccessIterator2,
          typename OutputIterator,
          typename DiffType, typename Comparator>
OutputIterator
merge_advance_movc(RandomAccessIterator1& begin1, RandomAccessIterator1 end1,
                   RandomAccessIterator2& begin2, RandomAccessIterator2 end2,
                   OutputIterator target,
                   DiffType max_size, Comparator comp) {
    using ValueType1 = typename std::iterator_traits<RandomAccessIterator1>::value_type;
    using ValueType2 = typename std::iterator_traits<RandomAccessIterator2>::value_type;

    while (begin1 != end1 && begin2 != end2 && max_size > 0)
    {
        RandomAccessIterator1 next1 = begin1 + 1;
        RandomAccessIterator2 next2 = begin2 + 1;
        ValueType1 element1 = *begin1;
        ValueType2 element2 = *begin2;

        if (comp(element2, element1))
        {
            element1 = element2;
            begin2 = next2;
        }
        else
        {
            begin1 = next1;
        }

        *target = element1;

        ++target;
        --max_size;
    }

    if (begin1 != end1)
    {
        target = std::copy(begin1, begin1 + max_size, target);
        begin1 += max_size;
    }
    else
    {
        target = std::copy(begin2, begin2 + max_size, target);
        begin2 += max_size;
    }

    return target;
}

/*!
 * Merge routine being able to merge only the \c max_size smallest elements.
 *
 * The \c begin iterators are advanced accordingly, they might not reach \c end,
 * in contrast to the usual variant.  Static switch on whether to use the
 * conditional-move variant.
 *
 * \param begin1 Begin iterator of first sequence.
 * \param end1 End iterator of first sequence.
 * \param begin2 Begin iterator of second sequence.
 * \param end2 End iterator of second sequence.
 * \param target Target begin iterator.
 * \param max_size Maximum number of elements to merge.
 * \param comp Comparator.
 * \return Output end iterator.
 */
template <typename RandomAccessIterator1, typename RandomAccessIterator2,
          typename OutputIterator,
          typename DiffType, typename Comparator>
OutputIterator
merge_advance(RandomAccessIterator1& begin1, RandomAccessIterator1 end1,
              RandomAccessIterator2& begin2, RandomAccessIterator2 end2,
              OutputIterator target,
              DiffType max_size, Comparator comp) {
    return merge_advance_movc(
        begin1, end1, begin2, end2, target, max_size, comp);
}

//! \}

} // namespace tlx

#endif // !TLX_ALGORITHM_MERGE_ADVANCE_HEADER

/******************************************************************************/

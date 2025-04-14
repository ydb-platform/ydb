/*******************************************************************************
 * tlx/algorithm/exclusive_scan.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018 Michael Axtmann <michael.axtmann@kit.edu>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_ALGORITHM_EXCLUSIVE_SCAN_HEADER
#define TLX_ALGORITHM_EXCLUSIVE_SCAN_HEADER

#include <functional>
#include <iterator>

namespace tlx {

//! \addtogroup tlx_algorithm
//! \{

/*!
 * Computes an exclusive prefix sum operation using binary_op the range [first,
 * last), using init as the initial value, and writes the results to the range
 * beginning at result. The term "exclusive" means that the i-th input element
 * is not included in the i-th sum.
 */
template <typename InputIterator, typename OutputIterator,
          typename T, typename BinaryOperation = std::plus<T> >
OutputIterator exclusive_scan(InputIterator first, InputIterator last,
                              OutputIterator result, T init,
                              BinaryOperation binary_op = BinaryOperation()) {
    *result++ = init;
    if (first != last) {
        typename std::iterator_traits<InputIterator>::value_type value =
            binary_op(init, *first);
        *result = value;
        while (++first != last) {
            value = binary_op(value, *first);
            *++result = value;
        }
        ++result;
    }
    return result;
}

//! \}

} // namespace tlx

#endif // !TLX_ALGORITHM_EXCLUSIVE_SCAN_HEADER

/******************************************************************************/

/*******************************************************************************
 * tlx/algorithm/random_bipartition_shuffle.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018 Manuel Penschuck <tlx@manuel.jetzt>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_ALGORITHM_RANDOM_BIPARTITION_SHUFFLE_HEADER
#define TLX_ALGORITHM_RANDOM_BIPARTITION_SHUFFLE_HEADER

#include <cassert>
#include <iterator>
#include <random>

namespace tlx {

//! \addtogroup tlx_algorithm
//! \{

/*!
 * Similar to std::shuffle, but only generates a random bi-partition.
 * In the result, the
 *   the left  partition class is stored at positions 0 to size_left_partition-1
 *   the right partition class is stored at positions size_left_partition to n
 * where n = std::distance(begin, end).
 *
 * Each element is moved to the left partition with probability
 * (size_left_partition / n). There are no guarantees on the order WITHIN a
 * partition (which makes this function considerable faster than std::shuffle
 * especially if partition sizes are unbalanced). The runtime complexity is
 * linear in the size of the smaller partition class.
 *
 * \param begin                Iterator to the begin of the data frame
 * \param end                  Iterator to the end of the data frame
 * \param size_left_partition  Number of elements to be put into the left
 *                             partition: 0 <= size_left_partition <= n
 * \param urng                 Random number generator compatible with STL
 *                             interface, e.g. std::mt19937
 */
template <typename RandomAccessIt, typename RandomBits>
void random_bipartition_shuffle(RandomAccessIt begin, RandomAccessIt end,
                                size_t size_left_partition, RandomBits& urng) {
    const size_t size = std::distance(begin, end);
    assert(size_left_partition <= size);

    // ensure that both paritions are non-empty
    const size_t size_right_partition = size - size_left_partition;
    if (!size_left_partition || !size_right_partition) return;

    // this idea is borrow from GNU stdlibc++ and generates two random
    // variates uniform on [0, a) and [0, b) respectively.
    auto two_random_variates =
        [&urng](size_t a, size_t b) {
            auto x = std::uniform_int_distribution<size_t>{ 0, (a * b) - 1 }(urng);
            return std::make_pair(x / b, x % b);
        };

    using std::swap; // allow ADL

    // Perform a Fisher-Yates shuffle, but iterate only over the positions
    // of the smaller partition.
    if (size_left_partition < size_right_partition) {
        auto it = begin;

        // To avoid wasted random bits, we draw two variates at once
        // and shuffle two positions per iteration
        for (size_t i = 1; i < size_left_partition; i += 2) {
            auto rand = two_random_variates(size - i + 1, size - i);
            swap(*it++, *std::next(begin, size - 1 - rand.first));
            swap(*it++, *std::next(begin, size - 1 - rand.second));
        }

        // In case the partition contains an odd number of elements,
        // there's a special case for the last element
        if (size_left_partition % 2) {
            auto x = std::uniform_int_distribution<size_t>{ size_left_partition - 1, size - 1 }(urng);
            swap(*it, *std::next(begin, x));
        }
    }
    else {
        // Symmetric case to above, this time shuffling the right partition
        auto it = std::next(begin, size - 1);

        for (size_t i = size - 2; i >= size_left_partition; i -= 2) {
            auto rand = two_random_variates(i + 2, i + 1);
            swap(*it--, *std::next(begin, rand.first));
            swap(*it--, *std::next(begin, rand.second));
        }

        if (size_right_partition % 2) {
            auto x = std::uniform_int_distribution<size_t>{ 0, size_left_partition }(urng);
            swap(*it--, *std::next(begin, x));
        }
    }
}

//! \}

} // namespace tlx

#endif // !TLX_ALGORITHM_RANDOM_BIPARTITION_SHUFFLE_HEADER

/******************************************************************************/

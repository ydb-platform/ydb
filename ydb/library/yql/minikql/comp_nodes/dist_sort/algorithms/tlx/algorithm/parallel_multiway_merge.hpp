/*******************************************************************************
 * tlx/algorithm/parallel_multiway_merge.hpp
 *
 * Parallel multiway merge.
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

#ifndef TLX_ALGORITHM_PARALLEL_MULTIWAY_MERGE_HEADER
#define TLX_ALGORITHM_PARALLEL_MULTIWAY_MERGE_HEADER

#include <algorithm>
#include <functional>
#include <thread>
#include <vector>

#if defined(_OPENMP)
#include <omp.h>
#endif

#include "multiway_merge.hpp"
#include "multiway_merge_splitting.hpp"
#include "../simple_vector.hpp"

namespace tlx {

    bool parallel_multiway_merge_force_sequential = false;

    bool parallel_multiway_merge_force_parallel = true;

    size_t parallel_multiway_merge_minimal_k = 1;

    size_t parallel_multiway_merge_minimal_n = 100;

    size_t parallel_multiway_merge_oversampling = 10;

//! \addtogroup tlx_algorithm
//! \{

//! default oversampling factor for parallel_multiway_merge
extern size_t parallel_multiway_merge_oversampling;

/*!
 * Parallel multi-way merge routine.
 *
 * Implemented either using OpenMP or with std::threads, depending on if
 * compiled with -fopenmp or not. The OpenMP version uses the implicit thread
 * pool, which is faster when using this method often.
 *
 * \param seqs_begin Begin iterator of iterator pair input sequence.
 * \param seqs_end End iterator of iterator pair input sequence.
 * \param target Begin iterator out output sequence.
 * \param size Maximum size to merge.
 * \param comp Comparator.
 * \param mwma MultiwayMergeAlgorithm set to use.
 * \param mwmsa MultiwayMergeSplittingAlgorithm to use.
 * \param num_threads Number of threads to use (defaults to all cores)
 * \tparam Stable Stable merging incurs a performance penalty.
 * \return End iterator of output sequence.
 */
template <
    bool Stable,
    typename RandomAccessIteratorIterator,
    typename RandomAccessIterator3,
    typename Comparator = std::less<
        typename std::iterator_traits<
            typename std::iterator_traits<RandomAccessIteratorIterator>
            ::value_type::first_type>::value_type> >
RandomAccessIterator3 parallel_multiway_merge_base(
    RandomAccessIteratorIterator seqs_begin,
    RandomAccessIteratorIterator seqs_end,
    RandomAccessIterator3 target,
    const typename std::iterator_traits<
        typename std::iterator_traits<
            RandomAccessIteratorIterator>::value_type::first_type>::
    difference_type size,
    Comparator comp = Comparator(),
    MultiwayMergeAlgorithm mwma = MWMA_ALGORITHM_DEFAULT,
    MultiwayMergeSplittingAlgorithm mwmsa = MWMSA_DEFAULT,
    size_t num_threads = std::thread::hardware_concurrency()) {

    using RandomAccessIteratorPair =
        typename std::iterator_traits<RandomAccessIteratorIterator>
        ::value_type;
    using RandomAccessIterator =
        typename RandomAccessIteratorPair::first_type;
    using DiffType = typename std::iterator_traits<RandomAccessIterator>
                     ::difference_type;

    // leave only non-empty sequences
    std::vector<RandomAccessIteratorPair> seqs_ne;
    seqs_ne.reserve(static_cast<size_t>(seqs_end - seqs_begin));
    DiffType total_size = 0;

    for (RandomAccessIteratorIterator ii = seqs_begin; ii != seqs_end; ++ii)
    {
        if (ii->first != ii->second) {
            total_size += ii->second - ii->first;
            seqs_ne.push_back(*ii);
        }
    }

    size_t num_seqs = seqs_ne.size();

    if (total_size == 0 || num_seqs == 0)
        return target;

    if (static_cast<DiffType>(num_threads) > total_size)
        num_threads = total_size;

    // thread t will have to merge chunks[iam][0..k - 1]

    simple_vector<std::vector<RandomAccessIteratorPair> > chunks(num_threads);

    for (size_t s = 0; s < num_threads; ++s)
        chunks[s].resize(num_seqs);

    if (mwmsa == MWMSA_SAMPLING)
    {
        multiway_merge_sampling_splitting<Stable>(
            seqs_ne.begin(), seqs_ne.end(),
            static_cast<DiffType>(size), total_size, comp,
            chunks.data(), num_threads,
            parallel_multiway_merge_oversampling);
    }
    else // (mwmsa == MWMSA_EXACT)
    {
        multiway_merge_exact_splitting<Stable>(
            seqs_ne.begin(), seqs_ne.end(),
            static_cast<DiffType>(size), total_size, comp,
            chunks.data(), num_threads);
    }

#if defined(_OPENMP)
#pragma omp parallel num_threads(num_threads)
    {
        size_t iam = omp_get_thread_num();

        DiffType target_position = 0, local_size = 0;

        for (size_t s = 0; s < num_seqs; ++s)
        {
            target_position += chunks[iam][s].first - seqs_ne[s].first;
            local_size += chunks[iam][s].second - chunks[iam][s].first;
        }

        multiway_merge_base<Stable, false>(
            chunks[iam].begin(), chunks[iam].end(),
            target + target_position,
            std::min(local_size, static_cast<DiffType>(size) - target_position),
            comp, mwma);
    }
#else
    std::vector<std::thread> threads(num_threads);

    for (size_t iam = 0; iam < num_threads; ++iam) {
        threads[iam] = std::thread(
            [&, iam]() {
                DiffType target_position = 0, local_size = 0;

                for (size_t s = 0; s < num_seqs; ++s)
                {
                    target_position += chunks[iam][s].first - seqs_ne[s].first;
                    local_size += chunks[iam][s].second - chunks[iam][s].first;
                }

                multiway_merge_base<Stable, false>(
                    chunks[iam].begin(), chunks[iam].end(),
                    target + target_position,
                    std::min(local_size, static_cast<DiffType>(size) - target_position),
                    comp, mwma);
            });
    }

    for (size_t i = 0; i < num_threads; ++i)
        threads[i].join();
#endif

    // update ends of sequences
    size_t count_seqs = 0;
    for (RandomAccessIteratorIterator ii = seqs_begin; ii != seqs_end; ++ii)
    {
        if (ii->first != ii->second)
            ii->first = chunks[num_threads - 1][count_seqs++].second;
    }

    return target + size;
}

/******************************************************************************/
// parallel_multiway_merge() Frontends

//! setting to force all parallel_multiway_merge() calls to run sequentially
extern bool parallel_multiway_merge_force_sequential;

//! setting to force parallel_multiway_merge() calls to run with parallel code
extern bool parallel_multiway_merge_force_parallel;

//! minimal number of sequences for switching to parallel merging
extern size_t parallel_multiway_merge_minimal_k;

//! minimal number of items for switching to parallel merging
extern size_t parallel_multiway_merge_minimal_n;

/*!
 * Parallel multi-way merge routine.
 *
 * Implemented either using OpenMP or with std::threads, depending on if
 * compiled with -fopenmp or not. The OpenMP version uses the implicit thread
 * pool, which is faster when using this method often.
 *
 * \param seqs_begin Begin iterator of iterator pair input sequence.
 * \param seqs_end End iterator of iterator pair input sequence.
 * \param target Begin iterator out output sequence.
 * \param size Maximum size to merge.
 * \param comp Comparator.
 * \param mwma MultiwayMergeAlgorithm set to use.
 * \param mwmsa MultiwayMergeSplittingAlgorithm to use.
 * \param num_threads Number of threads to use (defaults to all cores)
 * \tparam Stable Stable merging incurs a performance penalty.
 * \return End iterator of output sequence.
 */
template <
    typename RandomAccessIteratorIterator,
    typename RandomAccessIterator3,
    typename Comparator = std::less<
        typename std::iterator_traits<
            typename std::iterator_traits<RandomAccessIteratorIterator>
            ::value_type::first_type>::value_type> >
RandomAccessIterator3 parallel_multiway_merge(
    RandomAccessIteratorIterator seqs_begin,
    RandomAccessIteratorIterator seqs_end,
    RandomAccessIterator3 target,
    const typename std::iterator_traits<
        typename std::iterator_traits<
            RandomAccessIteratorIterator>::value_type::first_type>::
    difference_type size,
    Comparator comp = Comparator(),
    MultiwayMergeAlgorithm mwma = MWMA_ALGORITHM_DEFAULT,
    MultiwayMergeSplittingAlgorithm mwmsa = MWMSA_DEFAULT,
    size_t num_threads = std::thread::hardware_concurrency()) {

    if (seqs_begin == seqs_end)
        return target;

    if (!parallel_multiway_merge_force_sequential &&
        (parallel_multiway_merge_force_parallel ||
         (num_threads > 1 &&
          (static_cast<size_t>(seqs_end - seqs_begin)
           >= parallel_multiway_merge_minimal_k) &&
          static_cast<size_t>(size) >= parallel_multiway_merge_minimal_n))) {
        return parallel_multiway_merge_base</* Stable */ false>(
            seqs_begin, seqs_end, target, size, comp,
            mwma, mwmsa, num_threads);
    }
    else {
        return multiway_merge_base</* Stable */ false, /* Sentinels */ false>(
            seqs_begin, seqs_end, target, size, comp, mwma);
    }
}

/*!
 * Stable parallel multi-way merge routine.
 *
 * Implemented either using OpenMP or with std::threads, depending on if
 * compiled with -fopenmp or not. The OpenMP version uses the implicit thread
 * pool, which is faster when using this method often.
 *
 * \param seqs_begin Begin iterator of iterator pair input sequence.
 * \param seqs_end End iterator of iterator pair input sequence.
 * \param target Begin iterator out output sequence.
 * \param size Maximum size to merge.
 * \param comp Comparator.
 * \param mwma MultiwayMergeAlgorithm set to use.
 * \param mwmsa MultiwayMergeSplittingAlgorithm to use.
 * \param num_threads Number of threads to use (defaults to all cores)
 * \tparam Stable Stable merging incurs a performance penalty.
 * \return End iterator of output sequence.
 */
template <
    typename RandomAccessIteratorIterator,
    typename RandomAccessIterator3,
    typename Comparator = std::less<
        typename std::iterator_traits<
            typename std::iterator_traits<RandomAccessIteratorIterator>
            ::value_type::first_type>::value_type> >
RandomAccessIterator3 stable_parallel_multiway_merge(
    RandomAccessIteratorIterator seqs_begin,
    RandomAccessIteratorIterator seqs_end,
    RandomAccessIterator3 target,
    const typename std::iterator_traits<
        typename std::iterator_traits<
            RandomAccessIteratorIterator>::value_type::first_type>::
    difference_type size,
    Comparator comp = Comparator(),
    MultiwayMergeAlgorithm mwma = MWMA_ALGORITHM_DEFAULT,
    MultiwayMergeSplittingAlgorithm mwmsa = MWMSA_DEFAULT,
    size_t num_threads = std::thread::hardware_concurrency()) {

    if (seqs_begin == seqs_end)
        return target;

    if (!parallel_multiway_merge_force_sequential &&
        (parallel_multiway_merge_force_parallel ||
         (num_threads > 1 &&
          (static_cast<size_t>(seqs_end - seqs_begin)
           >= parallel_multiway_merge_minimal_k) &&
          static_cast<size_t>(size) >= parallel_multiway_merge_minimal_n))) {
        return parallel_multiway_merge_base</* Stable */ true>(
            seqs_begin, seqs_end, target, size, comp,
            mwma, mwmsa, num_threads);
    }
    else {
        return multiway_merge_base</* Stable */ true, /* Sentinels */ false>(
            seqs_begin, seqs_end, target, size, comp, mwma);
    }
}

/*!
 * Parallel multi-way merge routine with sentinels.
 *
 * Implemented either using OpenMP or with std::threads, depending on if
 * compiled with -fopenmp or not. The OpenMP version uses the implicit thread
 * pool, which is faster when using this method often.
 *
 * \param seqs_begin Begin iterator of iterator pair input sequence.
 * \param seqs_end End iterator of iterator pair input sequence.
 * \param target Begin iterator out output sequence.
 * \param size Maximum size to merge.
 * \param comp Comparator.
 * \param mwma MultiwayMergeAlgorithm set to use.
 * \param mwmsa MultiwayMergeSplittingAlgorithm to use.
 * \param num_threads Number of threads to use (defaults to all cores)
 * \tparam Stable Stable merging incurs a performance penalty.
 * \return End iterator of output sequence.
 */
template <
    typename RandomAccessIteratorIterator,
    typename RandomAccessIterator3,
    typename Comparator = std::less<
        typename std::iterator_traits<
            typename std::iterator_traits<RandomAccessIteratorIterator>
            ::value_type::first_type>::value_type> >
RandomAccessIterator3 parallel_multiway_merge_sentinels(
    RandomAccessIteratorIterator seqs_begin,
    RandomAccessIteratorIterator seqs_end,
    RandomAccessIterator3 target,
    const typename std::iterator_traits<
        typename std::iterator_traits<
            RandomAccessIteratorIterator>::value_type::first_type>::
    difference_type size,
    Comparator comp = Comparator(),
    MultiwayMergeAlgorithm mwma = MWMA_ALGORITHM_DEFAULT,
    MultiwayMergeSplittingAlgorithm mwmsa = MWMSA_DEFAULT,
    size_t num_threads = std::thread::hardware_concurrency()) {

    if (seqs_begin == seqs_end)
        return target;

    if (!parallel_multiway_merge_force_sequential &&
        (parallel_multiway_merge_force_parallel ||
         (num_threads > 1 &&
          (static_cast<size_t>(seqs_end - seqs_begin)
           >= parallel_multiway_merge_minimal_k) &&
          static_cast<size_t>(size) >= parallel_multiway_merge_minimal_n))) {
        return parallel_multiway_merge_base</* Stable */ false>(
            seqs_begin, seqs_end, target, size, comp,
            mwma, mwmsa, num_threads);
    }
    else {
        return multiway_merge_base</* Stable */ false, /* Sentinels */ true>(
            seqs_begin, seqs_end, target, size, comp, mwma);
    }
}

/*!
 * Stable parallel multi-way merge routine with sentinels.
 *
 * Implemented either using OpenMP or with std::threads, depending on if
 * compiled with -fopenmp or not. The OpenMP version uses the implicit thread
 * pool, which is faster when using this method often.
 *
 * \param seqs_begin Begin iterator of iterator pair input sequence.
 * \param seqs_end End iterator of iterator pair input sequence.
 * \param target Begin iterator out output sequence.
 * \param size Maximum size to merge.
 * \param comp Comparator.
 * \param mwma MultiwayMergeAlgorithm set to use.
 * \param mwmsa MultiwayMergeSplittingAlgorithm to use.
 * \param num_threads Number of threads to use (defaults to all cores)
 * \tparam Stable Stable merging incurs a performance penalty.
 * \return End iterator of output sequence.
 */
template <
    typename RandomAccessIteratorIterator,
    typename RandomAccessIterator3,
    typename Comparator = std::less<
        typename std::iterator_traits<
            typename std::iterator_traits<RandomAccessIteratorIterator>
            ::value_type::first_type>::value_type> >
RandomAccessIterator3 stable_parallel_multiway_merge_sentinels(
    RandomAccessIteratorIterator seqs_begin,
    RandomAccessIteratorIterator seqs_end,
    RandomAccessIterator3 target,
    const typename std::iterator_traits<
        typename std::iterator_traits<
            RandomAccessIteratorIterator>::value_type::first_type>::
    difference_type size,
    Comparator comp = Comparator(),
    MultiwayMergeAlgorithm mwma = MWMA_ALGORITHM_DEFAULT,
    MultiwayMergeSplittingAlgorithm mwmsa = MWMSA_DEFAULT,
    size_t num_threads = std::thread::hardware_concurrency()) {

    if (seqs_begin == seqs_end)
        return target;

    if (!parallel_multiway_merge_force_sequential &&
        (parallel_multiway_merge_force_parallel ||
         (num_threads > 1 &&
          (static_cast<size_t>(seqs_end - seqs_begin)
           >= parallel_multiway_merge_minimal_k) &&
          static_cast<size_t>(size) >= parallel_multiway_merge_minimal_n))) {
        return parallel_multiway_merge_base</* Stable */ true>(
            seqs_begin, seqs_end, target, size, comp,
            mwma, mwmsa, num_threads);
    }
    else {
        return multiway_merge_base</* Stable */ true, /* Sentinels */ true>(
            seqs_begin, seqs_end, target, size, comp, mwma);
    }
}

//! \}

} // namespace tlx

#endif // !TLX_ALGORITHM_PARALLEL_MULTIWAY_MERGE_HEADER

/******************************************************************************/

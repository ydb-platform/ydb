/*******************************************************************************
 * tlx/sort/parallel_mergesort.hpp
 *
 * **EXPERIMENTAL** Parallel multiway mergesort **EXPERIMENTAL**
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

#ifndef TLX_SORT_PARALLEL_MERGESORT_HEADER
#define TLX_SORT_PARALLEL_MERGESORT_HEADER

#include <algorithm>
#include <functional>
#include <thread>
#include <utility>

#if defined(_OPENMP)
#include <omp.h>
#endif

#include <tlx/algorithm/multisequence_selection.hpp>
#include <tlx/algorithm/parallel_multiway_merge.hpp>
#include <tlx/simple_vector.hpp>
#include <tlx/thread_barrier_mutex.hpp>

namespace tlx {

//! \addtogroup tlx_sort
//! \{

namespace parallel_mergesort_detail {

//! Subsequence description.
template <typename DiffType>
struct PMWMSPiece {
    //! Begin of subsequence.
    DiffType begin;
    //! End of subsequence.
    DiffType end;
};

/*!
 * Data accessed by all threads.
 *
 * PMWMS = parallel multiway mergesort
 */
template <typename RandomAccessIterator>
struct PMWMSSortingData {
    using ValueType =
        typename std::iterator_traits<RandomAccessIterator>::value_type;
    using DiffType =
        typename std::iterator_traits<RandomAccessIterator>::difference_type;

    //! Input begin.
    RandomAccessIterator source;
    //! Start indices, per thread.
    simple_vector<DiffType> starts;

    /** Storage in which to sort. */
    simple_vector<ValueType*> temporary;
    /** Samples. */
    simple_vector<ValueType> samples;
    /** Offsets to add to the found positions. */
    simple_vector<DiffType> offsets;
    /** PMWMSPieces of data to merge \c [thread][sequence] */
    simple_vector<simple_vector<PMWMSPiece<DiffType> > > pieces;

    explicit PMWMSSortingData(size_t num_threads)
        : starts(num_threads + 1),
          temporary(num_threads),
          offsets(num_threads - 1),
          pieces(num_threads)
    { }
};

/*!
 * Select samples from a sequence.
 * \param sd Pointer to sorting data struct. Result will be placed in \c sd->samples.
 * \param num_samples Number of samples to select.
 * \param iam my thread number
 * \param num_threads number of threads in group
 */
template <typename RandomAccessIterator, typename DiffType>
void determine_samples(PMWMSSortingData<RandomAccessIterator>* sd,
                       DiffType& num_samples,
                       size_t iam,
                       size_t num_threads) {

    num_samples = parallel_multiway_merge_oversampling * num_threads - 1;

    simple_vector<DiffType> es(num_samples + 2);
    multiway_merge_detail::equally_split(
        sd->starts[iam + 1] - sd->starts[iam],
        static_cast<size_t>(num_samples + 1), es.begin());

    for (DiffType i = 0; i < num_samples; i++) {
        sd->samples[iam * num_samples + i] = sd->source[sd->starts[iam] + es[i + 1]];
    }
}

/*!
 * PMWMS code executed by each thread.
 * \param sd Pointer to sorting data struct.
 * \param iam my thread number
 * \param num_threads number of threads in group
 * \param barrier thread barrier from main function
 * \param comp Comparator.
 * \param mwmsa MultiwayMergeSplittingAlgorithm to use.
 */
template <bool Stable, typename RandomAccessIterator, typename Comparator>
void parallel_sort_mwms_pu(PMWMSSortingData<RandomAccessIterator>* sd,
                           size_t iam,
                           size_t num_threads,
                           ThreadBarrierMutex& barrier,
                           Comparator& comp,
                           MultiwayMergeSplittingAlgorithm mwmsa) {
    using ValueType =
        typename std::iterator_traits<RandomAccessIterator>::value_type;
    using DiffType =
        typename std::iterator_traits<RandomAccessIterator>::difference_type;

    // length of this thread's chunk, before merging
    DiffType length_local = sd->starts[iam + 1] - sd->starts[iam];

    using SortingPlacesIterator = ValueType*;

    // sort in temporary storage, leave space for sentinel
    sd->temporary[iam] = static_cast<ValueType*>(
        ::operator new (sizeof(ValueType) * (length_local + 1)));

    // copy there
    std::uninitialized_copy(sd->source + sd->starts[iam],
                            sd->source + sd->starts[iam] + length_local,
                            sd->temporary[iam]);

    // sort locally
    if (Stable)
        std::stable_sort(sd->temporary[iam],
                         sd->temporary[iam] + length_local, comp);
    else
        std::sort(sd->temporary[iam],
                  sd->temporary[iam] + length_local, comp);

    // invariant: locally sorted subsequence in sd->temporary[iam],
    // sd->temporary[iam] + length_local

    if (mwmsa == MWMSA_SAMPLING)
    {
        DiffType num_samples;
        determine_samples(sd, num_samples, iam, num_threads);

        barrier.wait(
            [&]() {
                std::sort(sd->samples.begin(), sd->samples.end(), comp);
            });

        for (size_t s = 0; s < num_threads; s++)
        {
            // for each sequence
            if (num_samples * iam > 0)
                sd->pieces[iam][s].begin =
                    std::lower_bound(sd->temporary[s],
                                     sd->temporary[s] + sd->starts[s + 1] - sd->starts[s],
                                     sd->samples[num_samples * iam],
                                     comp)
                    - sd->temporary[s];
            else
                // absolute beginning
                sd->pieces[iam][s].begin = 0;

            if ((num_samples * (iam + 1)) < (num_samples * num_threads))
                sd->pieces[iam][s].end =
                    std::lower_bound(sd->temporary[s],
                                     sd->temporary[s] + sd->starts[s + 1] - sd->starts[s],
                                     sd->samples[num_samples * (iam + 1)],
                                     comp)
                    - sd->temporary[s];
            else
                // absolute end
                sd->pieces[iam][s].end = sd->starts[s + 1] - sd->starts[s];
        }
    }
    else if (mwmsa == MWMSA_EXACT)
    {
        barrier.wait();

        simple_vector<std::pair<SortingPlacesIterator,
                                SortingPlacesIterator> > seqs(num_threads);

        for (size_t s = 0; s < num_threads; s++)
            seqs[s] = std::make_pair(
                sd->temporary[s],
                sd->temporary[s] + sd->starts[s + 1] - sd->starts[s]);

        simple_vector<SortingPlacesIterator> offsets(num_threads);

        // if not last thread
        if (iam < num_threads - 1)
            multisequence_partition(seqs.begin(), seqs.end(),
                                    sd->starts[iam + 1], offsets.begin(), comp);

        for (size_t seq = 0; seq < num_threads; seq++)
        {
            // for each sequence
            if (iam < (num_threads - 1))
                sd->pieces[iam][seq].end = offsets[seq] - seqs[seq].first;
            else
                // absolute end of this sequence
                sd->pieces[iam][seq].end = sd->starts[seq + 1] - sd->starts[seq];
        }

        barrier.wait();

        for (size_t seq = 0; seq < num_threads; seq++)
        {
            // for each sequence
            if (iam > 0)
                sd->pieces[iam][seq].begin = sd->pieces[iam - 1][seq].end;
            else
                // absolute beginning
                sd->pieces[iam][seq].begin = 0;
        }
    }

    // offset from target begin, length after merging
    DiffType offset = 0, length_am = 0;
    for (size_t s = 0; s < num_threads; s++)
    {
        length_am += sd->pieces[iam][s].end - sd->pieces[iam][s].begin;
        offset += sd->pieces[iam][s].begin;
    }

    // merge directly to target

    simple_vector<std::pair<SortingPlacesIterator,
                            SortingPlacesIterator> > seqs(num_threads);

    for (size_t s = 0; s < num_threads; s++)
    {
        seqs[s] = std::make_pair(
            sd->temporary[s] + sd->pieces[iam][s].begin,
            sd->temporary[s] + sd->pieces[iam][s].end);
    }

    multiway_merge_base<Stable, /* Sentinels */ false>(
        seqs.begin(), seqs.end(),
        sd->source + offset, length_am, comp);

    barrier.wait();

    operator delete (sd->temporary[iam]);
}

} // namespace parallel_mergesort_detail

//! \name Parallel Sorting Algorithms
//! \{

/*!
 * Parallel multiway mergesort main call.
 *
 * \param begin Begin iterator of sequence.
 * \param end End iterator of sequence.
 * \param comp Comparator.
 * \param num_threads Number of threads to use.
 * \param mwmsa MultiwayMergeSplittingAlgorithm to use.
 * \tparam Stable Stable sorting.
 */
template <bool Stable,
          typename RandomAccessIterator, typename Comparator>
void parallel_mergesort_base(
    RandomAccessIterator begin,
    RandomAccessIterator end,
    Comparator comp,
    size_t num_threads = std::thread::hardware_concurrency(),
    MultiwayMergeSplittingAlgorithm mwmsa = MWMSA_DEFAULT) {

    using namespace parallel_mergesort_detail;

    using DiffType =
        typename std::iterator_traits<RandomAccessIterator>::difference_type;

    DiffType n = end - begin;

    if (n <= 1)
        return;

    // at least one element per thread
    if (num_threads > static_cast<size_t>(n))
        num_threads = static_cast<size_t>(n);

    PMWMSSortingData<RandomAccessIterator> sd(num_threads);
    sd.source = begin;

    if (mwmsa == MWMSA_SAMPLING) {
        sd.samples.resize(
            num_threads * (parallel_multiway_merge_oversampling * num_threads - 1));
    }

    for (size_t s = 0; s < num_threads; s++)
        sd.pieces[s].resize(num_threads);

    DiffType* starts = sd.starts.data();

    DiffType chunk_length = n / num_threads, split = n % num_threads, start = 0;
    for (size_t i = 0; i < num_threads; i++)
    {
        starts[i] = start;
        start += (i < static_cast<size_t>(split))
                 ? (chunk_length + 1) : chunk_length;
    }
    starts[num_threads] = start;

    // now sort in parallel

    ThreadBarrierMutex barrier(num_threads);

#if defined(_OPENMP)
#pragma omp parallel num_threads(num_threads)
    {
        size_t iam = omp_get_thread_num();
        parallel_sort_mwms_pu<Stable>(
            &sd, iam, num_threads, barrier, comp, mwmsa);
    }
#else
    simple_vector<std::thread> threads(num_threads);
    for (size_t iam = 0; iam < num_threads; ++iam) {
        threads[iam] = std::thread(
            [&, iam]() {
                parallel_sort_mwms_pu<Stable>(
                    &sd, iam, num_threads, barrier, comp, mwmsa);
            });
    }
    for (size_t i = 0; i < num_threads; i++) {
        threads[i].join();
    }
#endif // defined(_OPENMP)
}

/*!
 * Parallel multiway mergesort.
 *
 * \param begin Begin iterator of sequence.
 * \param end End iterator of sequence.
 * \param comp Comparator.
 * \param num_threads Number of threads to use.
 * \param mwmsa MultiwayMergeSplittingAlgorithm to use.
 */
template <typename RandomAccessIterator,
          typename Comparator = std::less<
              typename std::iterator_traits<RandomAccessIterator>::value_type> >
void parallel_mergesort(
    RandomAccessIterator begin,
    RandomAccessIterator end,
    Comparator comp = Comparator(),
    size_t num_threads = std::thread::hardware_concurrency(),
    MultiwayMergeSplittingAlgorithm mwmsa = MWMSA_DEFAULT) {

    return parallel_mergesort_base</* Stable */ false>(
        begin, end, comp, num_threads, mwmsa);
}

/*!
 * Stable parallel multiway mergesort.
 *
 * \param begin Begin iterator of sequence.
 * \param end End iterator of sequence.
 * \param comp Comparator.
 * \param num_threads Number of threads to use.
 * \param mwmsa MultiwayMergeSplittingAlgorithm to use.
 */
template <typename RandomAccessIterator,
          typename Comparator = std::less<
              typename std::iterator_traits<RandomAccessIterator>::value_type> >
void stable_parallel_mergesort(
    RandomAccessIterator begin,
    RandomAccessIterator end,
    Comparator comp = Comparator(),
    size_t num_threads = std::thread::hardware_concurrency(),
    MultiwayMergeSplittingAlgorithm mwmsa = MWMSA_DEFAULT) {

    return parallel_mergesort_base</* Stable */ true>(
        begin, end, comp, num_threads, mwmsa);
}

//! \}
//! \}

} // namespace tlx

#endif // !TLX_SORT_PARALLEL_MERGESORT_HEADER

/******************************************************************************/

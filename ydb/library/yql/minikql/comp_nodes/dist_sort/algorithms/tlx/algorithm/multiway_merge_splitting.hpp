/*******************************************************************************
 * tlx/algorithm/multiway_merge_splitting.hpp
 *
 * Two splitting variants for balancing parallel multiway merge.
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

#ifndef TLX_ALGORITHM_MULTIWAY_MERGE_SPLITTING_HEADER
#define TLX_ALGORITHM_MULTIWAY_MERGE_SPLITTING_HEADER

#include <algorithm>
#include <vector>

#include "../algorithm/multisequence_partition.hpp"
#include "../simple_vector.hpp"

namespace tlx {

//! \addtogroup tlx_algorithm
//! \{

/*!
 * Different splitting strategies for sorting/merging: by sampling, exact
*/
enum MultiwayMergeSplittingAlgorithm {
    MWMSA_SAMPLING,
    MWMSA_EXACT,
    MWMSA_LAST,
    MWMSA_DEFAULT = MWMSA_EXACT
};

namespace multiway_merge_detail {

/*!
 * Split a sequence into parts of almost equal size.
 *
 * The resulting sequence s of length p+1 contains the splitting positions when
 * splitting the range [0,n) into parts of almost equal size (plus minus 1).
 * The first entry is 0, the last one n. There may result empty parts.
 *
 * \param n Number of elements
 * \param p Number of parts
 * \param s Splitters
 * \returns End of splitter sequence, i. e. \c s+p+1
 */
template <typename DiffType, typename DiffTypeOutputIterator>
DiffTypeOutputIterator equally_split(
    DiffType n, size_t p, DiffTypeOutputIterator s) {

    DiffType chunk_length = n / p, split = n % p, start = 0;
    for (size_t i = 0; i < p; i++)
    {
        *s++ = start;
        start += (static_cast<DiffType>(i) < split) ? (chunk_length + 1) : chunk_length;
        if (start >= n)
            start = n - 1;
    }
    *s++ = n;

    return s;
}

} // namespace multiway_merge_detail

/*!
 * Splitting method for parallel multi-way merge routine: use sampling and
 * binary search for in-exact splitting.
 *
 * \param seqs_begin Begin iterator of iterator pair input sequence.
 * \param seqs_end End iterator of iterator pair input sequence.
 * \param size Maximum size to merge.
 * \param total_size Total size of all sequences combined.
 * \param comp Comparator.
 * \param chunks Output subsequences for num_threads.
 * \param num_threads Split the sequences into for num_threads.
 * \param merge_oversampling oversampling factor
 * \tparam Stable Stable merging incurs a performance penalty.
 * \return End iterator of output sequence.
 */
template <
    bool Stable,
    typename RandomAccessIteratorIterator,
    typename Comparator>
void multiway_merge_sampling_splitting(
    const RandomAccessIteratorIterator& seqs_begin,
    const RandomAccessIteratorIterator& seqs_end,
    typename std::iterator_traits<
        typename std::iterator_traits<
            RandomAccessIteratorIterator>::value_type::first_type>::
    difference_type size,
    typename std::iterator_traits<
        typename std::iterator_traits<
            RandomAccessIteratorIterator>::value_type::first_type>::
    difference_type total_size,
    Comparator comp,
    std::vector<typename std::iterator_traits<
                    RandomAccessIteratorIterator>::value_type>* chunks,
    const size_t num_threads,
    const size_t merge_oversampling) {

    using RandomAccessIterator =
        typename std::iterator_traits<RandomAccessIteratorIterator>
        ::value_type::first_type;
    using value_type = typename std::iterator_traits<RandomAccessIterator>
                       ::value_type;
    using DiffType = typename std::iterator_traits<RandomAccessIterator>
                     ::difference_type;

    const DiffType num_seqs = seqs_end - seqs_begin;
    const DiffType num_samples =
        num_threads * static_cast<DiffType>(merge_oversampling);

    // pick samples
    simple_vector<value_type> samples(num_seqs * num_samples);

    for (DiffType s = 0; s < num_seqs; ++s)
    {
        for (DiffType i = 0; i < num_samples; ++i)
        {
            DiffType sample_index = static_cast<DiffType>(
                double(seqs_begin[s].second - seqs_begin[s].first)
                * (double(i + 1) / double(num_samples + 1))
                * (double(size) / double(total_size)));
            samples[s * num_samples + i] = seqs_begin[s].first[sample_index];
        }
    }

    if (Stable)
        std::stable_sort(samples.begin(), samples.end(), comp);
    else
        std::sort(samples.begin(), samples.end(), comp);

    // for each processor
    for (size_t slab = 0; slab < num_threads; ++slab)
    {
        // for each sequence
        for (DiffType seq = 0; seq < num_seqs; ++seq)
        {
            if (slab > 0) {
                chunks[slab][static_cast<size_t>(seq)].first =
                    std::upper_bound(
                        seqs_begin[seq].first, seqs_begin[seq].second,
                        samples[num_samples * num_seqs * slab / num_threads],
                        comp);
            }
            else        // absolute beginning
                chunks[slab][static_cast<size_t>(seq)].first = seqs_begin[seq].first;

            if ((slab + 1) < num_threads) {
                chunks[slab][static_cast<size_t>(seq)].second =
                    std::upper_bound(
                        seqs_begin[seq].first, seqs_begin[seq].second,
                        samples[num_samples * num_seqs * (slab + 1) / num_threads],
                        comp);
            }
            else        // absolute ending
                chunks[slab][static_cast<size_t>(seq)].second = seqs_begin[seq].second;
        }
    }
}

/*!
 * Splitting method for parallel multi-way merge routine: use multisequence
 * selection for exact splitting.
 *
 * \param seqs_begin Begin iterator of iterator pair input sequence.
 * \param seqs_end End iterator of iterator pair input sequence.
 * \param size Maximum size to merge.
 * \param total_size Total size of all sequences combined.
 * \param comp Comparator.
 * \param chunks Output subsequences for num_threads.
 * \param num_threads Split the sequences into for num_threads.
 * \tparam Stable Stable merging incurs a performance penalty.
 * \return End iterator of output sequence.
 */
template <
    bool Stable,
    typename RandomAccessIteratorIterator,
    typename Comparator>
void multiway_merge_exact_splitting(
    const RandomAccessIteratorIterator& seqs_begin,
    const RandomAccessIteratorIterator& seqs_end,
    typename std::iterator_traits<
        typename std::iterator_traits<
            RandomAccessIteratorIterator>::value_type::first_type>::
    difference_type size,
    typename std::iterator_traits<
        typename std::iterator_traits<
            RandomAccessIteratorIterator>::value_type::first_type>::
    difference_type total_size,
    Comparator comp,
    std::vector<typename std::iterator_traits<
                    RandomAccessIteratorIterator>::value_type>* chunks,
    const size_t num_threads) {

    using RandomAccessIteratorPair =
        typename std::iterator_traits<RandomAccessIteratorIterator>
        ::value_type;
    using RandomAccessIterator = typename RandomAccessIteratorPair
                                 ::first_type;
    using DiffType = typename std::iterator_traits<RandomAccessIterator>
                     ::difference_type;

    const size_t num_seqs = static_cast<size_t>(seqs_end - seqs_begin);
    const bool tight = (total_size == size);

    simple_vector<std::vector<RandomAccessIterator> > offsets(num_threads);

    std::vector<DiffType> ranks(static_cast<size_t>(num_threads + 1));
    multiway_merge_detail::equally_split(size, num_threads, ranks.begin());

    for (size_t s = 0; s < (num_threads - 1); ++s)
    {
        offsets[s].resize(num_seqs);
        multisequence_partition(
            seqs_begin, seqs_end,
            ranks[static_cast<size_t>(s + 1)], offsets[s].begin(), comp);

        if (!tight) // last one also needed and available
        {
            offsets[num_threads - 1].resize(num_seqs);
            multisequence_partition(
                seqs_begin, seqs_end,
                size, offsets[num_threads - 1].begin(), comp);
        }
    }

    // for each processor
    for (size_t slab = 0; slab < num_threads; ++slab)
    {
        // for each sequence
        for (size_t s = 0; s < num_seqs; ++s)
        {
            if (slab == 0) // absolute beginning
                chunks[slab][s].first = seqs_begin[static_cast<DiffType>(s)].first;
            else
                chunks[slab][s].first = offsets[slab - 1][s];

            if (!tight || slab < (num_threads - 1))
                chunks[slab][s].second = offsets[slab][s];
            else        // slab == num_threads - 1
                chunks[slab][s].second = seqs_begin[static_cast<DiffType>(s)].second;
        }
    }
}

//! \}

} // namespace tlx

#endif // !TLX_ALGORITHM_MULTIWAY_MERGE_SPLITTING_HEADER

/******************************************************************************/

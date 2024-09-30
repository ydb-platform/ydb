/*******************************************************************************
 * tlx/algorithm/multisequence_partition.hpp
 *
 * Implementation of multisequence partition and selection.
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

#ifndef TLX_ALGORITHM_MULTISEQUENCE_PARTITION_HEADER
#define TLX_ALGORITHM_MULTISEQUENCE_PARTITION_HEADER

#include <algorithm>
#include <cassert>
#include <queue>
#include <utility>
#include <vector>

#include "../container/simple_vector.hpp"
#include "../math/round_to_power_of_two.hpp"

namespace tlx {

//! \addtogroup tlx_algorithm
//! \{

namespace multisequence_partition_detail {

//! Compare a pair of types lexicographically, ascending.
template <typename T1, typename T2, typename Comparator>
class lexicographic
{
protected:
    Comparator& comp_;

public:
    explicit lexicographic(Comparator& comp) : comp_(comp) { }

    inline bool operator () (const std::pair<T1, T2>& p1,
                             const std::pair<T1, T2>& p2) {
        if (comp_(p1.first, p2.first))
            return true;

        if (comp_(p2.first, p1.first))
            return false;

        // firsts are equal
        return p1.second < p2.second;
    }
};

//! Compare a pair of types lexicographically, descending.
template <typename T1, typename T2, typename Comparator>
class lexicographic_rev
{
protected:
    Comparator& comp_;

public:
    explicit lexicographic_rev(Comparator& comp) : comp_(comp) { }

    inline bool operator () (const std::pair<T1, T2>& p1,
                             const std::pair<T1, T2>& p2) {
        if (comp_(p2.first, p1.first))
            return true;

        if (comp_(p1.first, p2.first))
            return false;

        // firsts are equal
        return p2.second < p1.second;
    }
};

} // namespace multisequence_partition_detail

/*!
 * Splits several sorted sequences at a certain global rank, resulting in a
 * splitting point for each sequence.  The sequences are passed via a sequence
 * of random-access iterator pairs, none of the sequences may be empty.  If
 * there are several equal elements across the split, the ones on the left side
 * will be chosen from sequences with smaller number.
 *
 * \param begin_seqs Begin of the sequence of iterator pairs.
 * \param end_seqs End of the sequence of iterator pairs.
 * \param rank The global rank to partition at.
 * \param begin_offsets A random-access sequence begin where the result will be
 * stored in. Each element of the sequence is an iterator that points to the
 * first element on the greater part of the respective sequence.
 * \param comp The ordering functor, defaults to std::less<T>.
 */
template <typename RanSeqs, typename RankType, typename RankIterator,
          typename Comparator = std::less<
              typename std::iterator_traits<
                  typename std::iterator_traits<RanSeqs>
                  ::value_type::first_type>::value_type>
          >
void multisequence_partition(
    const RanSeqs& begin_seqs, const RanSeqs& end_seqs,
    const RankType& rank,
    RankIterator begin_offsets,
    Comparator comp = Comparator()) {

    using iterator = typename std::iterator_traits<RanSeqs>
                     ::value_type::first_type;
    using diff_type = typename std::iterator_traits<iterator>
                      ::difference_type;
    using value_type = typename std::iterator_traits<iterator>
                       ::value_type;

    using SamplePair = std::pair<value_type, diff_type>;

    using namespace multisequence_partition_detail;

    // comparators for SamplePair
    lexicographic<value_type, diff_type, Comparator> lcomp(comp);
    lexicographic_rev<value_type, diff_type, Comparator> lrcomp(comp);

    // number of sequences, number of elements in total (possibly including
    // padding)
    const diff_type m = std::distance(begin_seqs, end_seqs);
    diff_type nmax, n;
    RankType N = 0;

    for (diff_type i = 0; i < m; ++i)
    {
        N += std::distance(begin_seqs[i].first, begin_seqs[i].second);
        assert(std::distance(begin_seqs[i].first, begin_seqs[i].second) > 0);
    }

    if (rank == N)
    {
        for (diff_type i = 0; i < m; ++i)
            begin_offsets[i] = begin_seqs[i].second; // very end
        return;
    }

    assert(m != 0 && N != 0 && rank < N);

    simple_vector<diff_type> seqlen(m);

    seqlen[0] = std::distance(begin_seqs[0].first, begin_seqs[0].second);
    nmax = seqlen[0];
    for (diff_type i = 1; i < m; ++i)
    {
        seqlen[i] = std::distance(begin_seqs[i].first, begin_seqs[i].second);
        nmax = std::max(nmax, seqlen[i]);
    }

    // pad all lists to this length, at least as long as any ns[i], equality
    // iff nmax = 2^k - 1
    diff_type l = round_up_to_power_of_two(nmax + 1) - 1;

    simple_vector<diff_type> a(m), b(m);

    for (diff_type i = 0; i < m; ++i)
        a[i] = 0, b[i] = l;

    n = l / 2;

    // invariants:
    // 0 <= a[i] <= seqlen[i], 0 <= b[i] <= l

    // initial partition

    std::vector<SamplePair> sample;

    for (diff_type i = 0; i < m; ++i) {
        if (n < seqlen[i]) {
            // sequence long enough
            sample.push_back(SamplePair(begin_seqs[i].first[n], i));
        }
    }

    std::sort(sample.begin(), sample.end(), lcomp);

    for (diff_type i = 0; i < m; ++i) {
        // conceptual infinity
        if (n >= seqlen[i]) {
            // sequence too short, conceptual infinity
            sample.push_back(
                SamplePair(begin_seqs[i].first[0] /* dummy element */, i));
        }
    }

    size_t localrank = static_cast<size_t>(rank / l);

    size_t j;
    for (j = 0; j < localrank && ((n + 1) <= seqlen[sample[j].second]); ++j)
        a[sample[j].second] += n + 1;
    for ( ; j < static_cast<size_t>(m); ++j)
        b[sample[j].second] -= n + 1;

    // further refinement

    while (n > 0)
    {
        n /= 2;

        const value_type* lmax = nullptr; // impossible to avoid the warning?
        for (diff_type i = 0; i < m; ++i)
        {
            if (a[i] > 0)
            {
                if (!lmax)
                    lmax = &(begin_seqs[i].first[a[i] - 1]);
                else
                {
                    // max, favor rear sequences
                    if (!comp(begin_seqs[i].first[a[i] - 1], *lmax))
                        lmax = &(begin_seqs[i].first[a[i] - 1]);
                }
            }
        }

        for (diff_type i = 0; i < m; ++i)
        {
            diff_type middle = (b[i] + a[i]) / 2;
            if (lmax && middle < seqlen[i] &&
                comp(begin_seqs[i].first[middle], *lmax))
                a[i] = std::min(a[i] + n + 1, seqlen[i]);
            else
                b[i] -= n + 1;
        }

        diff_type leftsize = 0;
        for (diff_type i = 0; i < m; ++i)
            leftsize += a[i] / (n + 1);

        diff_type skew = rank / (n + 1) - leftsize;

        if (skew > 0)
        {
            // move to the left, find smallest
            std::priority_queue<
                SamplePair, std::vector<SamplePair>,
                lexicographic_rev<value_type, diff_type, Comparator> >
            pq(lrcomp);

            for (diff_type i = 0; i < m; ++i) {
                if (b[i] < seqlen[i])
                    pq.push(SamplePair(begin_seqs[i].first[b[i]], i));
            }

            for ( ; skew != 0 && !pq.empty(); --skew)
            {
                diff_type src = pq.top().second;
                pq.pop();

                a[src] = std::min(a[src] + n + 1, seqlen[src]);
                b[src] += n + 1;

                if (b[src] < seqlen[src])
                    pq.push(SamplePair(begin_seqs[src].first[b[src]], src));
            }
        }
        else if (skew < 0)
        {
            // move to the right, find greatest
            std::priority_queue<
                SamplePair, std::vector<SamplePair>,
                lexicographic<value_type, diff_type, Comparator> >
            pq(lcomp);

            for (diff_type i = 0; i < m; ++i) {
                if (a[i] > 0)
                    pq.push(SamplePair(begin_seqs[i].first[a[i] - 1], i));
            }

            for ( ; skew != 0; ++skew)
            {
                diff_type src = pq.top().second;
                pq.pop();

                a[src] -= n + 1;
                b[src] -= n + 1;

                if (a[src] > 0)
                    pq.push(SamplePair(begin_seqs[src].first[a[src] - 1], src));
            }
        }
    }

    // postconditions: a[i] == b[i] in most cases, except when a[i] has been
    // clamped because of having reached the boundary

    // now return the result, calculate the offset, compare the keys on both
    // edges of the border

    // maximum of left edge, minimum of right edge
    value_type* maxleft = nullptr, * minright = nullptr;
    for (diff_type i = 0; i < m; ++i)
    {
        if (a[i] > 0)
        {
            if (!maxleft)
            {
                maxleft = &(begin_seqs[i].first[a[i] - 1]);
            }
            else
            {
                // max, favor rear sequences
                if (!comp(begin_seqs[i].first[a[i] - 1], *maxleft))
                    maxleft = &(begin_seqs[i].first[a[i] - 1]);
            }
        }
        if (b[i] < seqlen[i])
        {
            if (!minright)
            {
                minright = &(begin_seqs[i].first[b[i]]);
            }
            else
            {
                // min, favor fore sequences
                if (comp(begin_seqs[i].first[b[i]], *minright))
                    minright = &(begin_seqs[i].first[b[i]]);
            }
        }
    }

    for (diff_type i = 0; i < m; ++i)
        begin_offsets[i] = begin_seqs[i].first + a[i];
}

//! \}

} // namespace tlx

#endif // !TLX_ALGORITHM_MULTISEQUENCE_PARTITION_HEADER

/******************************************************************************/

/*******************************************************************************
 * tlx/algorithm/multiway_merge.hpp
 *
 * Many variants of multiway merge.
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

#ifndef TLX_ALGORITHM_MULTIWAY_MERGE_HEADER
#define TLX_ALGORITHM_MULTIWAY_MERGE_HEADER

#include <algorithm>
#include <functional>
#include <numeric>
#include <utility>
#include <vector>

#include "../algorithm/merge_advance.hpp"
#include "../container/loser_tree.hpp"
#include "../container/simple_vector.hpp"
#include "../unused.hpp"

namespace tlx {

//! \addtogroup tlx_algorithm
//! \{

namespace multiway_merge_detail {

//! Size of a sequence described by a pair of iterators.
template <typename RandomAccessIteratorPair>
typename std::iterator_traits<
    typename RandomAccessIteratorPair::first_type
    >::difference_type
iterpair_size(const RandomAccessIteratorPair& p) {
    return p.second - p.first;
}

/*!
 * Iterator wrapper supporting an implicit supremum at the end of the sequence,
 * dominating all comparisons.  Deriving from RandomAccessIterator is not
 * possible since RandomAccessIterator need not be a class.
*/
template <typename RandomAccessIterator, typename Comparator>
class guarded_iterator
{
public:
    //! Our own type
    using self_type = guarded_iterator<RandomAccessIterator, Comparator>;

    //! Value type of the iterator
    using value_type =
        typename std::iterator_traits<RandomAccessIterator>::value_type;

protected:
    //! Current iterator position.
    RandomAccessIterator current;
    //! End iterator of the sequence.
    RandomAccessIterator end_;
    //! Comparator.
    Comparator& comp_;

public:
    /*!
     * Constructor. Sets iterator to beginning of sequence.
     * \param begin Begin iterator of sequence.
     * \param end End iterator of sequence.
     * \param comp Comparator provided for associated overloaded compare
     * operators.
     */
    guarded_iterator(RandomAccessIterator begin, RandomAccessIterator end,
                     Comparator& comp)
        : current(begin), end_(end), comp_(comp)
    { }

    /*!
     * Pre-increment operator.
     * \return This.
     */
    self_type& operator ++ () {
        ++current;
        return *this;
    }

    /*!
     * Dereference operator.
     * \return Referenced element.
     */
    value_type& operator * () {
        return *current;
    }

    /*!
     * Convert to wrapped iterator.
     * \return Wrapped iterator.
     */
    RandomAccessIterator& iterator() {
        return current;
    }

    /*!
     * Compare two elements referenced by guarded iterators.
     * \param bi1 First iterator.
     * \param bi2 Second iterator.
     * \return \c True if less.
     */
    friend bool operator < (self_type& bi1, self_type& bi2) {
        if (bi1.current == bi1.end_)             // bi1 is sup
            return bi2.current == bi2.end_;      // bi2 is not sup
        if (bi2.current == bi2.end_)             // bi2 is sup
            return true;
        return bi1.comp_(*bi1, *bi2);            // normal compare
    }

    /*!
     * Compare two elements referenced by guarded iterators.
     * \param bi1 First iterator.
     * \param bi2 Second iterator.
     * \return \c True if less equal.
     */
    friend bool operator <= (self_type& bi1, self_type& bi2) {
        if (bi2.current == bi2.end_)         // bi1 is sup
            return bi1.current != bi1.end_;  // bi2 is not sup
        if (bi1.current == bi1.end_)         // bi2 is sup
            return false;
        return !bi1.comp_(*bi2, *bi1);       // normal compare
    }
};

template <typename RandomAccessIterator, typename Comparator>
class unguarded_iterator
{
public:
    //! Our own type
    using self_type = unguarded_iterator<RandomAccessIterator, Comparator>;

    //! Value type of the iterator
    using value_type =
        typename std::iterator_traits<RandomAccessIterator>::value_type;

protected:
    //! Current iterator position.
    RandomAccessIterator current;
    //! Comparator.
    Comparator& comp_;

public:
    /*!
     * Constructor. Sets iterator to beginning of sequence.
     * \param begin Begin iterator of sequence.
     * param end Unused, only for compatibility.
     * \param comp Unused, only for compatibility.
     */
    unguarded_iterator(RandomAccessIterator begin,
                       RandomAccessIterator /* end */,
                       Comparator& comp)
        : current(begin), comp_(comp)
    { }

    /*!
     * Pre-increment operator.
     * \return This.
     */
    self_type& operator ++ () {
        ++current;
        return *this;
    }

    /*!
     * Dereference operator.
     * \return Referenced element.
     */
    value_type& operator * () {
        return *current;
    }

    /*!
     * Convert to wrapped iterator.
     * \return Wrapped iterator.
     */
    RandomAccessIterator& iterator() {
        return current;
    }

    /*!
     * Compare two elements referenced by unguarded iterators.
     * \param bi1 First iterator.
     * \param bi2 Second iterator.
     * \return \c True if less.
     */
    friend bool operator < (self_type& bi1, self_type& bi2) {
        return bi1.comp_(*bi1, *bi2);    // normal compare, unguarded
    }

    /*!
     * Compare two elements referenced by unguarded iterators.
     * \param bi1 First iterator.
     * \param bi2 Second iterator.
     * \return \c True if less equal.
     */
    friend bool operator <= (self_type& bi1, self_type& bi2) {
        return !bi1.comp_(*bi2, *bi1);   // normal compare, unguarded
    }
};

/*!
 * Prepare a set of sequences to be merged without a (end) guard
 *
 * \param seqs_begin
 * \param seqs_end
 * \param comp
 * \param min_sequence
 * \tparam Stable
 * \pre (seqs_end - seqs_begin > 0)
 */
template <
    bool Stable,
    typename RandomAccessIteratorIterator,
    typename Comparator>
typename std::iterator_traits<
    typename std::iterator_traits<
        RandomAccessIteratorIterator>::value_type::first_type>::difference_type
prepare_unguarded(
    RandomAccessIteratorIterator seqs_begin,
    RandomAccessIteratorIterator seqs_end,
    Comparator comp,
    int& min_sequence) {
    using RandomAccessIterator =
        typename std::iterator_traits<RandomAccessIteratorIterator>
        ::value_type::first_type;
    using value_type = typename std::iterator_traits<RandomAccessIterator>
                       ::value_type;
    using diff_type = typename std::iterator_traits<RandomAccessIterator>
                      ::difference_type;

    if ((*seqs_begin).first == (*seqs_begin).second)
    {
        // empty sequence found, it's the first one
        min_sequence = 0;
        return -1;
    }

    // last element in sequence
    value_type min = *((*seqs_begin).second - 1);
    min_sequence = 0;
    for (RandomAccessIteratorIterator s = seqs_begin + 1; s != seqs_end; ++s)
    {
        if ((*s).first == (*s).second)
        {
            // empty sequence found
            min_sequence = static_cast<int>(s - seqs_begin);
            return -1;
        }
        const value_type& v = *((*s).second - 1);
        if (comp(v, min))
        {
            // last element in sequence is strictly smaller
            min = v;
            min_sequence = static_cast<int>(s - seqs_begin);
        }
    }

    diff_type overhang_size = 0;

    int s = 0;
    for (s = 0; s <= min_sequence; ++s)
    {
        RandomAccessIterator split;
        if (Stable)
            split = std::upper_bound(seqs_begin[s].first, seqs_begin[s].second,
                                     min, comp);
        else
            split = std::lower_bound(seqs_begin[s].first, seqs_begin[s].second,
                                     min, comp);

        overhang_size += seqs_begin[s].second - split;
    }

    for ( ; s < (seqs_end - seqs_begin); ++s)
    {
        RandomAccessIterator split =
            std::lower_bound(seqs_begin[s].first, seqs_begin[s].second,
                             min, comp);
        overhang_size += seqs_begin[s].second - split;
    }

    // so many elements will be left over afterwards
    return overhang_size;
}

/*!
 * Prepare a set of sequences to be merged with a (end) guard (sentinel)
 * \param seqs_begin
 * \param seqs_end
 * \param comp
 */
template <typename RandomAccessIteratorIterator, typename Comparator>
typename std::iterator_traits<
    typename std::iterator_traits<
        RandomAccessIteratorIterator>::value_type::first_type>::difference_type
prepare_unguarded_sentinel(
    RandomAccessIteratorIterator seqs_begin,
    RandomAccessIteratorIterator seqs_end,
    Comparator comp) {
    using RandomAccessIterator =
        typename std::iterator_traits<RandomAccessIteratorIterator>
        ::value_type::first_type;
    using value_type = typename std::iterator_traits<RandomAccessIterator>
                       ::value_type;
    using diff_type = typename std::iterator_traits<RandomAccessIterator>
                      ::difference_type;

    value_type* max_value = nullptr;   // last element in sequence
    for (RandomAccessIteratorIterator s = seqs_begin; s != seqs_end; ++s)
    {
        if ((*s).first == (*s).second)
            continue;
        value_type& v = *((*s).second - 1);     // last element in sequence
        if (!max_value || comp(*max_value, v))  // strictly greater
            max_value = &v;
    }

    diff_type overhang_size = 0;

    for (RandomAccessIteratorIterator s = seqs_begin; s != seqs_end; ++s)
    {
        RandomAccessIterator split = std::lower_bound(
            (*s).first, (*s).second, *max_value, comp);
        overhang_size += (*s).second - split;
        *((*s).second) = *max_value; // set sentinel
    }

    // so many elements will be left over afterwards
    return overhang_size;
}

/*!
 * Highly efficient 3-way merging procedure.
 *
 * Merging is done with the algorithm implementation described by Peter
 * Sanders.  Basically, the idea is to minimize the number of necessary
 * comparison after merging an element.  The implementation trick that makes
 * this fast is that the order of the sequences is stored in the instruction
 * pointer (translated into labels in C++).
 *
 * This works well for merging up to 3 sequences.
 *
 * Note that making the merging stable does \a not come at a performance hit.
 *
 * Whether the merging is done guarded or unguarded is selected by the used
 * iterator class.
 *
 * \param seqs_begin Begin iterator of iterator pair input sequence.
 * \param seqs_end End iterator of iterator pair input sequence.
 * \param target Begin iterator out output sequence.
 * \param size Maximum size to merge.
 * \param comp Comparator.
 * \return End iterator of output sequence.
 */
template <
    template <typename RAI, typename C> class Iterator,
    typename RandomAccessIteratorIterator,
    typename RandomAccessIterator3,
    typename Comparator>
RandomAccessIterator3 multiway_merge_3_variant(
    RandomAccessIteratorIterator seqs_begin,
    RandomAccessIteratorIterator seqs_end,
    RandomAccessIterator3 target,
    typename std::iterator_traits<
        typename std::iterator_traits<
            RandomAccessIteratorIterator>::value_type::first_type>::
    difference_type size,
    Comparator comp) {

    assert(seqs_end - seqs_begin == 3);
    unused(seqs_end);

    using RandomAccessIterator =
        typename std::iterator_traits<RandomAccessIteratorIterator>
        ::value_type::first_type;

    if (size == 0)
        return target;

    Iterator<RandomAccessIterator, Comparator>
    seq0(seqs_begin[0].first, seqs_begin[0].second, comp),
    seq1(seqs_begin[1].first, seqs_begin[1].second, comp),
    seq2(seqs_begin[2].first, seqs_begin[2].second, comp);

    if (seq0 <= seq1)
    {
        if (seq1 <= seq2)
            goto s012;
        else if (seq2 < seq0)
            goto s201;
        else
            goto s021;
    }
    else
    {
        if (seq1 <= seq2)
        {
            if (seq0 <= seq2)
                goto s102;
            else
                goto s120;
        }
        else
            goto s210;
    }

#define TLX_MERGE3CASE(a, b, c, c0, c1)              \
    s ## a ## b ## c:                                \
    *target = *seq ## a;                             \
    ++target;                                        \
    --size;                                          \
    ++seq ## a;                                      \
    if (size == 0) goto finish;                      \
    if (seq ## a c0 seq ## b) goto s ## a ## b ## c; \
    if (seq ## a c1 seq ## c) goto s ## b ## a ## c; \
    goto s ## b ## c ## a;

    TLX_MERGE3CASE(0, 1, 2, <=, <=);
    TLX_MERGE3CASE(1, 2, 0, <=, <);
    TLX_MERGE3CASE(2, 0, 1, <, <);
    TLX_MERGE3CASE(1, 0, 2, <, <=);
    TLX_MERGE3CASE(0, 2, 1, <=, <=);
    TLX_MERGE3CASE(2, 1, 0, <, <);

#undef TLX_MERGE3CASE

finish:
    seqs_begin[0].first = seq0.iterator();
    seqs_begin[1].first = seq1.iterator();
    seqs_begin[2].first = seq2.iterator();

    return target;
}

template <
    typename RandomAccessIteratorIterator,
    typename RandomAccessIterator3,
    typename Comparator>
RandomAccessIterator3 multiway_merge_3_combined(
    RandomAccessIteratorIterator seqs_begin,
    RandomAccessIteratorIterator seqs_end,
    RandomAccessIterator3 target,
    typename std::iterator_traits<
        typename std::iterator_traits<
            RandomAccessIteratorIterator>::value_type::first_type>::
    difference_type size,
    Comparator comp) {
    using RandomAccessIterator =
        typename std::iterator_traits<RandomAccessIteratorIterator>
        ::value_type::first_type;
    using DiffType = typename std::iterator_traits<RandomAccessIterator>
                     ::difference_type;

    assert(seqs_end - seqs_begin == 3);

    int min_seq;
    RandomAccessIterator3 target_end;
    DiffType overhang = prepare_unguarded<true>(seqs_begin, seqs_end, comp, min_seq);

    DiffType total_size = 0;
    for (RandomAccessIteratorIterator s = seqs_begin; s != seqs_end; ++s)
        total_size += iterpair_size(*s);

    if (overhang != static_cast<DiffType>(-1))
    {
        DiffType unguarded_size = std::min(size, total_size - overhang);
        target_end = multiway_merge_3_variant<unguarded_iterator>
                         (seqs_begin, seqs_end, target, unguarded_size, comp);
        overhang = size - unguarded_size;
    }
    else
    {
        // empty sequence found
        overhang = size;
        target_end = target;
    }

    switch (min_seq)
    {
    case 0:
        // iterators will be advanced accordingly
        target_end = merge_advance(
            seqs_begin[1].first, seqs_begin[1].second,
            seqs_begin[2].first, seqs_begin[2].second,
            target_end, overhang, comp);
        break;
    case 1:
        target_end = merge_advance(
            seqs_begin[0].first, seqs_begin[0].second,
            seqs_begin[2].first, seqs_begin[2].second,
            target_end, overhang, comp);
        break;
    case 2:
        target_end = merge_advance(
            seqs_begin[0].first, seqs_begin[0].second,
            seqs_begin[1].first, seqs_begin[1].second,
            target_end, overhang, comp);
        break;
    default:
        assert(false);
    }

    return target_end;
}

/*!
 * Highly efficient 4-way merging procedure.
 *
 * Merging is done with the algorithm implementation described by Peter
 * Sanders. Basically, the idea is to minimize the number of necessary
 * comparison after merging an element.  The implementation trick that makes
 * this fast is that the order of the sequences is stored in the instruction
 * pointer (translated into goto labels in C++).
 *
 * This works well for merging up to 4 sequences.
 *
 * Note that making the merging stable does \a not come at a performance hit.
 *
 * Whether the merging is done guarded or unguarded is selected by the used
 * iterator class.
 *
 * \param seqs_begin Begin iterator of iterator pair input sequence.
 * \param seqs_end End iterator of iterator pair input sequence.
 * \param target Begin iterator out output sequence.
 * \param size Maximum size to merge.
 * \param comp Comparator.
 * \return End iterator of output sequence.
 */
template <
    template <typename RAI, typename C> class iterator,
    typename RandomAccessIteratorIterator,
    typename RandomAccessIterator3,
    typename Comparator>
RandomAccessIterator3 multiway_merge_4_variant(
    RandomAccessIteratorIterator seqs_begin,
    RandomAccessIteratorIterator seqs_end,
    RandomAccessIterator3 target,
    typename std::iterator_traits<
        typename std::iterator_traits<
            RandomAccessIteratorIterator>::value_type::first_type>::
    difference_type size,
    Comparator comp) {
    assert(seqs_end - seqs_begin == 4);
    unused(seqs_end);
    using RandomAccessIterator =
        typename std::iterator_traits<RandomAccessIteratorIterator>
        ::value_type::first_type;

    if (size == 0)
        return target;

    iterator<RandomAccessIterator, Comparator>
    seq0(seqs_begin[0].first, seqs_begin[0].second, comp),
    seq1(seqs_begin[1].first, seqs_begin[1].second, comp),
    seq2(seqs_begin[2].first, seqs_begin[2].second, comp),
    seq3(seqs_begin[3].first, seqs_begin[3].second, comp);

#define TLX_DECISION(a, b, c, d) do {                        \
        if (seq ## d < seq ## a) goto s ## d ## a ## b ## c; \
        if (seq ## d < seq ## b) goto s ## a ## d ## b ## c; \
        if (seq ## d < seq ## c) goto s ## a ## b ## d ## c; \
        goto s ## a ## b ## c ## d;                          \
}                                                            \
    while (0)

    if (seq0 <= seq1)
    {
        if (seq1 <= seq2)
            TLX_DECISION(0, 1, 2, 3);
        else if (seq2 < seq0)
            TLX_DECISION(2, 0, 1, 3);
        else
            TLX_DECISION(0, 2, 1, 3);
    }
    else
    {
        if (seq1 <= seq2)
        {
            if (seq0 <= seq2)
                TLX_DECISION(1, 0, 2, 3);
            else
                TLX_DECISION(1, 2, 0, 3);
        }
        else
            TLX_DECISION(2, 1, 0, 3);
    }

#define TLX_MERGE4CASE(a, b, c, d, c0, c1, c2)            \
    s ## a ## b ## c ## d:                                \
    *target = *seq ## a;                                  \
    ++target;                                             \
    --size;                                               \
    ++seq ## a;                                           \
    if (size == 0) goto finish;                           \
    if (seq ## a c0 seq ## b) goto s ## a ## b ## c ## d; \
    if (seq ## a c1 seq ## c) goto s ## b ## a ## c ## d; \
    if (seq ## a c2 seq ## d) goto s ## b ## c ## a ## d; \
    goto s ## b ## c ## d ## a;

    TLX_MERGE4CASE(0, 1, 2, 3, <=, <=, <=);
    TLX_MERGE4CASE(0, 1, 3, 2, <=, <=, <=);
    TLX_MERGE4CASE(0, 2, 1, 3, <=, <=, <=);
    TLX_MERGE4CASE(0, 2, 3, 1, <=, <=, <=);
    TLX_MERGE4CASE(0, 3, 1, 2, <=, <=, <=);
    TLX_MERGE4CASE(0, 3, 2, 1, <=, <=, <=);
    TLX_MERGE4CASE(1, 0, 2, 3, <, <=, <=);
    TLX_MERGE4CASE(1, 0, 3, 2, <, <=, <=);
    TLX_MERGE4CASE(1, 2, 0, 3, <=, <, <=);
    TLX_MERGE4CASE(1, 2, 3, 0, <=, <=, <);
    TLX_MERGE4CASE(1, 3, 0, 2, <=, <, <=);
    TLX_MERGE4CASE(1, 3, 2, 0, <=, <=, <);
    TLX_MERGE4CASE(2, 0, 1, 3, <, <, <=);
    TLX_MERGE4CASE(2, 0, 3, 1, <, <=, <);
    TLX_MERGE4CASE(2, 1, 0, 3, <, <, <=);
    TLX_MERGE4CASE(2, 1, 3, 0, <, <=, <);
    TLX_MERGE4CASE(2, 3, 0, 1, <=, <, <);
    TLX_MERGE4CASE(2, 3, 1, 0, <=, <, <);
    TLX_MERGE4CASE(3, 0, 1, 2, <, <, <);
    TLX_MERGE4CASE(3, 0, 2, 1, <, <, <);
    TLX_MERGE4CASE(3, 1, 0, 2, <, <, <);
    TLX_MERGE4CASE(3, 1, 2, 0, <, <, <);
    TLX_MERGE4CASE(3, 2, 0, 1, <, <, <);
    TLX_MERGE4CASE(3, 2, 1, 0, <, <, <);

#undef TLX_MERGE4CASE
#undef TLX_DECISION

finish:
    seqs_begin[0].first = seq0.iterator();
    seqs_begin[1].first = seq1.iterator();
    seqs_begin[2].first = seq2.iterator();
    seqs_begin[3].first = seq3.iterator();

    return target;
}

template <
    typename RandomAccessIteratorIterator,
    typename RandomAccessIterator3,
    typename Comparator>
RandomAccessIterator3 multiway_merge_4_combined(
    RandomAccessIteratorIterator seqs_begin,
    RandomAccessIteratorIterator seqs_end,
    RandomAccessIterator3 target,
    typename std::iterator_traits<
        typename std::iterator_traits<
            RandomAccessIteratorIterator>::value_type::first_type>::
    difference_type size,
    Comparator comp) {

    assert(seqs_end - seqs_begin == 4);
    using RandomAccessIteratorPair =
        typename std::iterator_traits<RandomAccessIteratorIterator>
        ::value_type;
    using DiffType = typename std::iterator_traits<RandomAccessIteratorIterator>
                     ::difference_type;

    int min_seq;
    RandomAccessIterator3 target_end;
    DiffType overhang = prepare_unguarded<true>(seqs_begin, seqs_end, comp, min_seq);

    DiffType total_size = 0;
    for (RandomAccessIteratorIterator s = seqs_begin; s != seqs_end; ++s)
        total_size += iterpair_size(*s);

    if (overhang != static_cast<DiffType>(-1))
    {
        DiffType unguarded_size = std::min(size, total_size - overhang);
        target_end = multiway_merge_4_variant<unguarded_iterator>(
            seqs_begin, seqs_end, target, unguarded_size, comp);
        overhang = size - unguarded_size;
    }
    else
    {
        // empty sequence found
        overhang = size;
        target_end = target;
    }

    std::vector<RandomAccessIteratorPair> one_missing(seqs_begin, seqs_end);
    // remove
    one_missing.erase(one_missing.begin() + min_seq);

    target_end = multiway_merge_3_variant<guarded_iterator>(
        one_missing.begin(), one_missing.end(), target_end, overhang, comp);

    // insert back again
    one_missing.insert(one_missing.begin() + min_seq, seqs_begin[min_seq]);
    // write back modified iterators
    std::copy(one_missing.begin(), one_missing.end(), seqs_begin);

    return target_end;
}

/*!
 * Basic multi-way merging procedure.
 *
 * The head elements are kept in a sorted array, new heads are inserted
 * linearly.
 *
 * \param seqs_begin Begin iterator of iterator pair input sequence.
 * \param seqs_end End iterator of iterator pair input sequence.
 * \param target Begin iterator out output sequence.
 * \param size Maximum size to merge.
 * \param comp Comparator.
 * \tparam Stable Stable merging incurs a performance penalty.
 * \return End iterator of output sequence.
 */
template <
    bool Stable,
    typename RandomAccessIteratorIterator,
    typename RandomAccessIterator3,
    typename Comparator>
RandomAccessIterator3 multiway_merge_bubble(
    RandomAccessIteratorIterator seqs_begin,
    RandomAccessIteratorIterator seqs_end,
    RandomAccessIterator3 target,
    typename std::iterator_traits<
        typename std::iterator_traits<
            RandomAccessIteratorIterator>::value_type::first_type>::
    difference_type size,
    Comparator comp) {
    using RandomAccessIterator =
        typename std::iterator_traits<RandomAccessIteratorIterator>
        ::value_type::first_type;
    using value_type = typename std::iterator_traits<RandomAccessIterator>
                       ::value_type;
    using DiffType = typename std::iterator_traits<RandomAccessIterator>
                     ::difference_type;

    // num remaining pieces
    int num_seqs = static_cast<int>(seqs_end - seqs_begin), nrp;

    simple_vector<value_type> pl(num_seqs);
    simple_vector<int> source(num_seqs);
    DiffType total_size = 0;

#define TLX_POS(i) seqs_begin[(i)].first
#define TLX_STOPS(i) seqs_begin[(i)].second

    // write entries into queue
    nrp = 0;
    for (int pi = 0; pi < num_seqs; ++pi)
    {
        if (TLX_STOPS(pi) != TLX_POS(pi))
        {
            pl[nrp] = *(TLX_POS(pi));
            source[nrp] = pi;
            ++nrp;
            total_size += iterpair_size(seqs_begin[pi]);
        }
    }

    if (Stable)
    {
        for (int k = 0; k < nrp - 1; ++k)
            for (int pi = nrp - 1; pi > k; --pi)
                if (comp(pl[pi], pl[pi - 1]) ||
                    (!comp(pl[pi - 1], pl[pi]) && source[pi] < source[pi - 1]))
                {
                    std::swap(pl[pi - 1], pl[pi]);
                    std::swap(source[pi - 1], source[pi]);
                }
    }
    else
    {
        for (int k = 0; k < nrp - 1; ++k)
            for (int pi = nrp - 1; pi > k; --pi)
                if (comp(pl[pi], pl[pi - 1]))
                {
                    std::swap(pl[pi - 1], pl[pi]);
                    std::swap(source[pi - 1], source[pi]);
                }
    }

    // iterate
    if (Stable)
    {
        int j;
        while (nrp > 0 && size > 0)
        {
            if (source[0] < source[1])
            {
                // pl[0] <= pl[1] ?
                while ((nrp == 1 || !(comp(pl[1], pl[0]))) && size > 0)
                {
                    *target = pl[0];
                    ++target;
                    ++TLX_POS(source[0]);
                    --size;
                    if (TLX_POS(source[0]) == TLX_STOPS(source[0]))
                    {
                        // move everything to the left
                        for (int s = 0; s < nrp - 1; ++s)
                        {
                            pl[s] = pl[s + 1];
                            source[s] = source[s + 1];
                        }
                        --nrp;
                        break;
                    }
                    else
                        pl[0] = *(TLX_POS(source[0]));
                }
            }
            else
            {
                // pl[0] < pl[1] ?
                while ((nrp == 1 || comp(pl[0], pl[1])) && size > 0)
                {
                    *target = pl[0];
                    ++target;
                    ++TLX_POS(source[0]);
                    --size;
                    if (TLX_POS(source[0]) == TLX_STOPS(source[0]))
                    {
                        for (int s = 0; s < nrp - 1; ++s)
                        {
                            pl[s] = pl[s + 1];
                            source[s] = source[s + 1];
                        }
                        --nrp;
                        break;
                    }
                    else
                        pl[0] = *(TLX_POS(source[0]));
                }
            }

            // sink down
            j = 1;
            while ((j < nrp) && (comp(pl[j], pl[j - 1]) ||
                                 (!comp(pl[j - 1], pl[j]) && (source[j] < source[j - 1]))))
            {
                std::swap(pl[j - 1], pl[j]);
                std::swap(source[j - 1], source[j]);
                ++j;
            }
        }
    }
    else
    {
        int j;
        while (nrp > 0 && size > 0)
        {
            // pl[0] <= pl[1] ?
            while ((nrp == 1 || !comp(pl[1], pl[0])) && size > 0)
            {
                *target = pl[0];
                ++target;
                ++TLX_POS(source[0]);
                --size;
                if (TLX_POS(source[0]) == TLX_STOPS(source[0]))
                {
                    for (int s = 0; s < (nrp - 1); ++s)
                    {
                        pl[s] = pl[s + 1];
                        source[s] = source[s + 1];
                    }
                    --nrp;
                    break;
                }
                else
                    pl[0] = *(TLX_POS(source[0]));
            }

            // sink down
            j = 1;
            while ((j < nrp) && comp(pl[j], pl[j - 1]))
            {
                std::swap(pl[j - 1], pl[j]);
                std::swap(source[j - 1], source[j]);
                ++j;
            }
        }
    }

#undef TLX_POS
#undef TLX_STOPS

    return target;
}

/*!
 * Multi-way merging procedure for a high branching factor, guarded case.
 *
 * The head elements are kept in a loser tree.
 * \param seqs_begin Begin iterator of iterator pair input sequence.
 * \param seqs_end End iterator of iterator pair input sequence.
 * \param target Begin iterator out output sequence.
 * \param size Maximum size to merge.
 * \param comp Comparator.
 * \tparam Stable Stable merging incurs a performance penalty.
 * \return End iterator of output sequence.
 */
template <
    typename LoserTreeType,
    typename RandomAccessIteratorIterator,
    typename RandomAccessIterator3,
    typename Comparator>
RandomAccessIterator3 multiway_merge_loser_tree(
    RandomAccessIteratorIterator seqs_begin,
    RandomAccessIteratorIterator seqs_end,
    RandomAccessIterator3 target,
    typename std::iterator_traits<
        typename std::iterator_traits<
            RandomAccessIteratorIterator>::value_type::first_type>::
    difference_type size,
    Comparator comp) {
    using Source = typename LoserTreeType::Source;
    using size_type = typename LoserTreeType::Source;
    using RandomAccessIteratorPair =
        typename std::iterator_traits<RandomAccessIteratorIterator>::value_type;
    using RandomAccessIterator = typename RandomAccessIteratorPair::first_type;
    using DiffType = typename std::iterator_traits<RandomAccessIterator>
                     ::difference_type;

    const Source k = static_cast<Source>(seqs_end - seqs_begin);

    LoserTreeType lt(static_cast<size_type>(k), comp);

    const DiffType total_size = std::min<DiffType>(
        size,
        std::accumulate(seqs_begin, seqs_end, DiffType(0),
                        [](DiffType sum, const RandomAccessIteratorPair& x) {
                            return sum + iterpair_size(x);
                        }));

    for (Source t = 0; t < k; ++t)
    {
        if (TLX_UNLIKELY(seqs_begin[t].first == seqs_begin[t].second))
            lt.insert_start(nullptr, t, true);
        else
            lt.insert_start(&*seqs_begin[t].first, t, false);
    }

    lt.init();

    if (total_size == 0)
        return target;

    // take out first
    Source source = lt.min_source();

    *target = *seqs_begin[source].first;
    ++target;
    ++seqs_begin[source].first;

    for (DiffType i = 1; i < total_size; ++i)
    {
        // feed
        if (seqs_begin[source].first == seqs_begin[source].second)
            lt.delete_min_insert(nullptr, true);
        else
            // replace from same source
            lt.delete_min_insert(&*seqs_begin[source].first, false);

        // take out following
        source = lt.min_source();

        *target = *seqs_begin[source].first;
        ++target;
        ++seqs_begin[source].first;
    }

    return target;
}

/*!
 * Multi-way merging procedure for a high branching factor, unguarded case.
 * The head elements are kept in a loser tree.
 *
 * \param seqs_begin Begin iterator of iterator pair input sequence.
 * \param seqs_end End iterator of iterator pair input sequence.
 * \param target Begin iterator out output sequence.
 * \param size Maximum size to merge.
 * \param comp Comparator.
 * \tparam Stable Stable merging incurs a performance penalty.
 * \return End iterator of output sequence.
 * \pre No input will run out of elements during the merge.
 */
template <
    typename LoserTreeType,
    typename RandomAccessIteratorIterator,
    typename RandomAccessIterator3,
    typename Comparator>
RandomAccessIterator3 multiway_merge_loser_tree_unguarded(
    RandomAccessIteratorIterator seqs_begin,
    RandomAccessIteratorIterator seqs_end,
    RandomAccessIterator3 target,
    typename std::iterator_traits<
        typename std::iterator_traits<
            RandomAccessIteratorIterator>::value_type::first_type>::
    difference_type size,
    Comparator comp) {
    using Source = typename LoserTreeType::Source;
    using size_type = typename LoserTreeType::Source;
    using RandomAccessIteratorPair =
        typename std::iterator_traits<RandomAccessIteratorIterator>
        ::value_type;
    using RandomAccessIterator = typename RandomAccessIteratorPair
                                 ::first_type;
    using DiffType = typename std::iterator_traits<RandomAccessIterator>
                     ::difference_type;

    Source k = static_cast<Source>(seqs_end - seqs_begin);

    // sentinel is item at end of first sequence.
    LoserTreeType lt(static_cast<size_type>(k), *(seqs_begin->second - 1), comp);

    DiffType total_size = 0;

    for (Source t = 0; t < k; ++t)
    {
        assert(seqs_begin[t].first != seqs_begin[t].second);

        lt.insert_start(&*seqs_begin[t].first, t, false);

        total_size += iterpair_size(seqs_begin[t]);
    }

    lt.init();

    // do not go past end
    size = std::min(total_size, size);

    RandomAccessIterator3 target_end = target + size;
    if (target == target_end)
        return target;

    // take out first
    int source = lt.min_source();

    *target = *seqs_begin[source].first;
    ++seqs_begin[source].first;
    ++target;

    while (target < target_end)
    {
        // feed. replace from same source
        lt.delete_min_insert(&*seqs_begin[source].first, false);

        // take out following
        source = lt.min_source();

        *target = *seqs_begin[source].first;
        ++seqs_begin[source].first;
        ++target;
    }

    return target;
}

template <
    bool Stable,
    typename RandomAccessIteratorIterator,
    typename RandomAccessIterator3,
    typename Comparator>
RandomAccessIterator3 multiway_merge_loser_tree_combined(
    RandomAccessIteratorIterator seqs_begin,
    RandomAccessIteratorIterator seqs_end,
    RandomAccessIterator3 target,
    typename std::iterator_traits<
        typename std::iterator_traits<
            RandomAccessIteratorIterator>::value_type::first_type>::
    difference_type size,
    Comparator comp) {
    using RandomAccessIterator =
        typename std::iterator_traits<RandomAccessIteratorIterator>
        ::value_type::first_type;
    using value_type = typename std::iterator_traits<RandomAccessIterator>
                       ::value_type;
    using DiffType = typename std::iterator_traits<RandomAccessIterator>
                     ::difference_type;

    int min_seq;
    RandomAccessIterator3 target_end;
    DiffType overhang = prepare_unguarded<Stable>(seqs_begin, seqs_end, comp, min_seq);

    DiffType total_size = 0;
    for (RandomAccessIteratorIterator s = seqs_begin; s != seqs_end; ++s)
        total_size += iterpair_size(*s);

    if (overhang != static_cast<DiffType>(-1))
    {
        DiffType unguarded_size = std::min(size, total_size - overhang);
        target_end = multiway_merge_loser_tree_unguarded<
            LoserTreeUnguarded<Stable, value_type, Comparator> >(
            seqs_begin, seqs_end, target, unguarded_size, comp);
        overhang = size - unguarded_size;
    }
    else
    {
        // empty sequence found
        overhang = size;
        target_end = target;
    }

    target_end = multiway_merge_loser_tree<
        LoserTree<Stable, value_type, Comparator> >(
        seqs_begin, seqs_end, target_end, overhang, comp);

    return target_end;
}

template <
    bool Stable,
    typename RandomAccessIteratorIterator,
    typename RandomAccessIterator3,
    typename Comparator>
RandomAccessIterator3 multiway_merge_loser_tree_sentinel(
    RandomAccessIteratorIterator seqs_begin,
    RandomAccessIteratorIterator seqs_end,
    RandomAccessIterator3 target,
    typename std::iterator_traits<
        typename std::iterator_traits<
            RandomAccessIteratorIterator>::value_type::first_type>::
    difference_type size,
    Comparator comp) {
    using RandomAccessIterator =
        typename std::iterator_traits<RandomAccessIteratorIterator>
        ::value_type::first_type;
    using value_type = typename std::iterator_traits<RandomAccessIterator>
                       ::value_type;

    // move end of sequences to include the sentinel for merging
    for (RandomAccessIteratorIterator s = seqs_begin; s != seqs_end; ++s)
        ++(*s).second;

    RandomAccessIterator3 target_end
        = multiway_merge_loser_tree_unguarded<
              LoserTreeUnguarded<Stable, value_type, Comparator> >(
              seqs_begin, seqs_end, target, size, comp);

    // restore end of sequences
    for (RandomAccessIteratorIterator s = seqs_begin; s != seqs_end; ++s)
        --(*s).second;

    return target_end;
}

} // namespace multiway_merge_detail

/******************************************************************************/
// Multiway Merge Algorithm Switch

/*!
 * Different merging algorithms: bubblesort-alike, loser-tree variants, enum
 * sentinel
 */
enum MultiwayMergeAlgorithm {
    MWMA_LOSER_TREE,
    MWMA_LOSER_TREE_COMBINED,
    MWMA_LOSER_TREE_SENTINEL,
    MWMA_BUBBLE,
    MWMA_ALGORITHM_LAST,
    MWMA_ALGORITHM_DEFAULT = MWMA_LOSER_TREE_COMBINED
};

/*!
 * Sequential multi-way merging switch.
 *
 * The decision if based on the branching factor and runtime settings.
 *
 * \param seqs_begin Begin iterator of iterator pair input sequence.
 * \param seqs_end End iterator of iterator pair input sequence.
 * \param target Begin iterator out output sequence.
 * \param size Maximum size to merge.
 * \param comp Comparator.
 * \param mwma MultiwayMergeAlgorithm set to use.
 * \tparam Stable Stable merging incurs a performance penalty.
 * \tparam Sentinels The sequences have a sentinel element.
 * \return End iterator of output sequence.
 */
template <
    bool Stable,
    bool Sentinels,
    typename RandomAccessIteratorIterator,
    typename RandomAccessIterator3,
    typename Comparator = std::less<
        typename std::iterator_traits<
            typename std::iterator_traits<RandomAccessIteratorIterator>
            ::value_type::first_type>::value_type> >
RandomAccessIterator3 multiway_merge_base(
    RandomAccessIteratorIterator seqs_begin,
    RandomAccessIteratorIterator seqs_end,
    RandomAccessIterator3 target,
    typename std::iterator_traits<
        typename std::iterator_traits<
            RandomAccessIteratorIterator>::value_type::first_type>::
    difference_type size,
    Comparator comp = Comparator(),
    MultiwayMergeAlgorithm mwma = MWMA_ALGORITHM_DEFAULT) {

    using RandomAccessIterator =
        typename std::iterator_traits<RandomAccessIteratorIterator>
        ::value_type::first_type;
    using value_type = typename std::iterator_traits<RandomAccessIterator>
                       ::value_type;

    RandomAccessIterator3 return_target = target;
    int k = static_cast<int>(seqs_end - seqs_begin);

    if (!Sentinels && mwma == MWMA_LOSER_TREE_SENTINEL)
        mwma = MWMA_LOSER_TREE_COMBINED;

    using namespace multiway_merge_detail;

    switch (k)
    {
    case 0:
        break;
    case 1:
        return_target = std::copy(
            seqs_begin[0].first, seqs_begin[0].first + size, target);
        seqs_begin[0].first += size;
        break;
    case 2:
        return_target = merge_advance(
            seqs_begin[0].first, seqs_begin[0].second,
            seqs_begin[1].first, seqs_begin[1].second,
            target, size, comp);
        break;
    case 3:
        switch (mwma)
        {
        case MWMA_LOSER_TREE_COMBINED:
            return_target = multiway_merge_3_combined(
                seqs_begin, seqs_end, target, size, comp);
            break;
        case MWMA_LOSER_TREE_SENTINEL:
            return_target = multiway_merge_3_variant<unguarded_iterator>(
                seqs_begin, seqs_end, target, size, comp);
            break;
        default:
            return_target = multiway_merge_3_variant<guarded_iterator>(
                seqs_begin, seqs_end, target, size, comp);
            break;
        }
        break;
    case 4:
        switch (mwma)
        {
        case MWMA_LOSER_TREE_COMBINED:
            return_target = multiway_merge_4_combined(
                seqs_begin, seqs_end, target, size, comp);
            break;
        case MWMA_LOSER_TREE_SENTINEL:
            return_target = multiway_merge_4_variant<unguarded_iterator>(
                seqs_begin, seqs_end, target, size, comp);
            break;
        default:
            return_target = multiway_merge_4_variant<guarded_iterator>(
                seqs_begin, seqs_end, target, size, comp);
            break;
        }
        break;
    default:
    {
        switch (mwma)
        {
        case MWMA_BUBBLE:
            return_target = multiway_merge_bubble<Stable>(
                seqs_begin, seqs_end, target, size, comp);
            break;
        case MWMA_LOSER_TREE:
            return_target = multiway_merge_loser_tree<
                LoserTree<Stable, value_type, Comparator> >(
                seqs_begin, seqs_end, target, size, comp);
            break;
        case MWMA_LOSER_TREE_COMBINED:
            return_target = multiway_merge_loser_tree_combined<Stable>(
                seqs_begin, seqs_end, target, size, comp);
            break;
        case MWMA_LOSER_TREE_SENTINEL:
            return_target = multiway_merge_loser_tree_sentinel<Stable>(
                seqs_begin, seqs_end, target, size, comp);
            break;
        default:
            assert(0 && "multiway_merge algorithm not implemented");
            break;
        }
    }
    }

    return return_target;
}

/******************************************************************************/
// multiway_merge() Frontends

/*!
 * Sequential multi-way merge.
 *
 * \param seqs_begin Begin iterator of iterator pair input sequence.
 * \param seqs_end End iterator of iterator pair input sequence.
 * \param target Begin iterator out output sequence.
 * \param size Maximum size to merge.
 * \param comp Comparator.
 * \param mwma MultiwayMergeAlgorithm set to use.
 * \return End iterator of output sequence.
 */
template <
    typename RandomAccessIteratorIterator,
    typename RandomAccessIterator3,
    typename Comparator = std::less<
        typename std::iterator_traits<
            typename std::iterator_traits<RandomAccessIteratorIterator>
            ::value_type::first_type>::value_type> >
RandomAccessIterator3 multiway_merge(
    RandomAccessIteratorIterator seqs_begin,
    RandomAccessIteratorIterator seqs_end,
    RandomAccessIterator3 target,
    typename std::iterator_traits<
        typename std::iterator_traits<
            RandomAccessIteratorIterator>::value_type::first_type>::
    difference_type size,
    Comparator comp = Comparator(),
    MultiwayMergeAlgorithm mwma = MWMA_ALGORITHM_DEFAULT) {

    return multiway_merge_base</* Stable */ false, /* Sentinels */ false>(
        seqs_begin, seqs_end, target, size, comp, mwma);
}

/*!
 * Stable sequential multi-way merge.
 *
 * \param seqs_begin Begin iterator of iterator pair input sequence.
 * \param seqs_end End iterator of iterator pair input sequence.
 * \param target Begin iterator out output sequence.
 * \param size Maximum size to merge.
 * \param comp Comparator.
 * \param mwma MultiwayMergeAlgorithm set to use.
 * \return End iterator of output sequence.
 */
template <
    typename RandomAccessIteratorIterator,
    typename RandomAccessIterator3,
    typename Comparator = std::less<
        typename std::iterator_traits<
            typename std::iterator_traits<RandomAccessIteratorIterator>
            ::value_type::first_type>::value_type> >
RandomAccessIterator3 stable_multiway_merge(
    RandomAccessIteratorIterator seqs_begin,
    RandomAccessIteratorIterator seqs_end,
    RandomAccessIterator3 target,
    typename std::iterator_traits<
        typename std::iterator_traits<
            RandomAccessIteratorIterator>::value_type::first_type>::
    difference_type size,
    Comparator comp = Comparator(),
    MultiwayMergeAlgorithm mwma = MWMA_ALGORITHM_DEFAULT) {

    return multiway_merge_base</* Stable */ true, /* Sentinels */ false>(
        seqs_begin, seqs_end, target, size, comp, mwma);
}

/*!
 * Sequential multi-way merge with sentinels in sequences.
 *
 * \param seqs_begin Begin iterator of iterator pair input sequence.
 * \param seqs_end End iterator of iterator pair input sequence.
 * \param target Begin iterator out output sequence.
 * \param size Maximum size to merge.
 * \param comp Comparator.
 * \param mwma MultiwayMergeAlgorithm set to use.
 * \return End iterator of output sequence.
 */
template <
    typename RandomAccessIteratorIterator,
    typename RandomAccessIterator3,
    typename Comparator = std::less<
        typename std::iterator_traits<
            typename std::iterator_traits<RandomAccessIteratorIterator>
            ::value_type::first_type>::value_type> >
RandomAccessIterator3 multiway_merge_sentinels(
    RandomAccessIteratorIterator seqs_begin,
    RandomAccessIteratorIterator seqs_end,
    RandomAccessIterator3 target,
    typename std::iterator_traits<
        typename std::iterator_traits<
            RandomAccessIteratorIterator>::value_type::first_type>::
    difference_type size,
    Comparator comp = Comparator(),
    MultiwayMergeAlgorithm mwma = MWMA_ALGORITHM_DEFAULT) {

    return multiway_merge_base</* Stable */ false, /* Sentinels */ true>(
        seqs_begin, seqs_end, target, size, comp, mwma);
}

/*!
 * Stable sequential multi-way merge with sentinels in sequences.
 *
 * \param seqs_begin Begin iterator of iterator pair input sequence.
 * \param seqs_end End iterator of iterator pair input sequence.
 * \param target Begin iterator out output sequence.
 * \param size Maximum size to merge.
 * \param comp Comparator.
 * \param mwma MultiwayMergeAlgorithm set to use.
 * \return End iterator of output sequence.
 */
template <
    typename RandomAccessIteratorIterator,
    typename RandomAccessIterator3,
    typename Comparator = std::less<
        typename std::iterator_traits<
            typename std::iterator_traits<RandomAccessIteratorIterator>
            ::value_type::first_type>::value_type> >
RandomAccessIterator3 stable_multiway_merge_sentinels(
    RandomAccessIteratorIterator seqs_begin,
    RandomAccessIteratorIterator seqs_end,
    RandomAccessIterator3 target,
    typename std::iterator_traits<
        typename std::iterator_traits<
            RandomAccessIteratorIterator>::value_type::first_type>::
    difference_type size,
    Comparator comp = Comparator(),
    MultiwayMergeAlgorithm mwma = MWMA_ALGORITHM_DEFAULT) {

    return multiway_merge_base</* Stable */ true, /* Sentinels */ true>(
        seqs_begin, seqs_end, target, size, comp, mwma);
}

//! \}

} // namespace tlx

#endif // !TLX_ALGORITHM_MULTIWAY_MERGE_HEADER

/******************************************************************************/

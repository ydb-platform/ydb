/*******************************************************************************
 * tlx/container/loser_tree.hpp
 *
 * Many generic loser tree variants.
 *
 * Copied and modified from STXXL, see http://stxxl.org, which itself extracted
 * it from MCSTL http://algo2.iti.uni-karlsruhe.de/singler/mcstl/. Both are
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007 Johannes Singler <singler@ira.uka.de>
 * Copyright (C) 2014-2017 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Huyen Chau Nguyen <hello@chau-nguyen.de>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_CONTAINER_LOSER_TREE_HEADER
#define TLX_CONTAINER_LOSER_TREE_HEADER

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <functional>
#include <utility>

#include "../container/simple_vector.hpp"
#include "../define/likely.hpp"
#include "../math/round_to_power_of_two.hpp"
#include "../unused.hpp"

namespace tlx {

//! \addtogroup tlx_container
//! \{
//! \defgroup tlx_container_loser_tree Loser Trees
//! Loser/Tournament tree variants
//! \{

/*!
 * Guarded loser tree/tournament tree, either copying the whole element into the
 * tree structure, or looking up the element via the index.
 *
 * This is a base class for the LoserTreeCopy\<true> and \<false> classes.
 *
 * Guarding is done explicitly through one flag sup per element, inf is not
 * needed due to a better initialization routine.  This is a well-performing
 * variant.
 *
 * \tparam ValueType the element type
 * \tparam Comparator comparator to use for binary comparisons.
 */
template <typename ValueType, typename Comparator = std::less<ValueType> >
class LoserTreeCopyBase
{
public:
    //! size of counters and array indexes
    using Source = std::uint32_t;

    //! sentinel for invalid or finished Sources
    static constexpr Source invalid_ = Source(-1);

protected:
    //! Internal representation of a loser tree player/node
    struct Loser {
        //! flag, true iff is a virtual maximum sentinel
        bool sup;
        //! index of source
        Source source;
        //! copy of key value of the element in this node
        ValueType key;
    };

    //! number of nodes
    const Source ik_;
    //! log_2(ik) next greater power of 2
    const Source k_;
    //! array containing loser tree nodes -- avoid default-constructing
    //! losers[].key
    SimpleVector<Loser> losers_;
    //! the comparator object
    Comparator cmp_;
    //! still have to construct keys
    bool first_insert_;

public:
    explicit LoserTreeCopyBase(const Source& k,
                               const Comparator& cmp = Comparator())
        : ik_(k), k_(round_up_to_power_of_two(ik_)),
          losers_(2 * k_), cmp_(cmp), first_insert_(true) {

        for (Source i = ik_ - 1; i < k_; ++i) {
            losers_[i + k_].sup = true;
            losers_[i + k_].source = invalid_;
        }
    }

    //! return the index of the player with the smallest element.
    Source min_source() { return losers_[0].source; }

    /*!
     * Initializes the player source with the element key.
     *
     * \param keyp the element to insert
     * \param source index of the player
     * \param sup flag that determines whether the value to insert is an
     *   explicit supremum sentinel.
     */
    void insert_start(const ValueType* keyp, const Source& source, bool sup) {
        Source pos = k_ + source;

        assert(pos < losers_.size());
        assert(sup == (keyp == nullptr));

        losers_[pos].sup = sup;
        losers_[pos].source = source;

        if (TLX_UNLIKELY(first_insert_)) {
            // copy construct all keys from this first key
            for (Source i = 0; i < 2 * k_; ++i) {
                if (keyp)
                    losers_[i].key = *keyp;
                else
                    losers_[i].key = ValueType();
            }
            first_insert_ = false;
        }
        else {
            losers_[pos].key = keyp ? *keyp : ValueType();
        }
    }

    /*!
     * Computes the winner of the competition at player root.  Called
     * recursively (starting at 0) to build the initial tree.
     *
     * \param root index of the game to start.
     */
    Source init_winner(const Source& root) {
        if (root >= k_)
            return root;

        Source left = init_winner(2 * root);
        Source right = init_winner(2 * root + 1);
        if (losers_[right].sup ||
            (!losers_[left].sup &&
             !cmp_(losers_[right].key, losers_[left].key))) {
            // left one is less or equal
            losers_[root] = losers_[right];
            return left;
        }
        else {
            // right one is less
            losers_[root] = losers_[left];
            return right;
        }
    }

    void init() {
        if (TLX_UNLIKELY(k_ == 0))
            return;
        losers_[0] = losers_[init_winner(1)];
    }
};

/*!
 * Guarded loser tree/tournament tree, either copying the whole element into the
 * tree structure, or looking up the element via the index.
 *
 * Unstable specialization of LoserTreeCopyBase.
 *
 * Guarding is done explicitly through one flag sup per element, inf is not
 * needed due to a better initialization routine.  This is a well-performing
 * variant.
 *
 * \tparam ValueType the element type
 * \tparam Comparator comparator to use for binary comparisons.
 */
template <bool Stable /* == false */, typename ValueType,
          typename Comparator = std::less<ValueType> >
class LoserTreeCopy : public LoserTreeCopyBase<ValueType, Comparator>
{
public:
    using Super = LoserTreeCopyBase<ValueType, Comparator>;
    using Source = typename Super::Source;

protected:
    using Super::k_;
    using Super::losers_;
    using Super::cmp_;

public:
    explicit LoserTreeCopy(const Source& k,
                           const Comparator& cmp = Comparator())
        : Super(k, cmp) { }

    // do not pass const reference since key will be used as local variable
    void delete_min_insert(const ValueType* keyp, bool sup) {
        using std::swap;
        assert(sup == (keyp == nullptr));

        Source source = losers_[0].source;
        ValueType key = keyp ? *keyp : ValueType();
        Source pos = (k_ + source) / 2;

        while (pos > 0) {
            if (TLX_UNLIKELY(sup)) {
                // the other candidate is smaller
                swap(losers_[pos].sup, sup);
                swap(losers_[pos].source, source);
                swap(losers_[pos].key, key);
            }
            else if (TLX_UNLIKELY(losers_[pos].sup)) {
                // this candidate is smaller
            }
            else if (cmp_(losers_[pos].key, key)) {
                // the other one is smaller
                swap(losers_[pos].source, source);
                swap(losers_[pos].key, key);
            }
            else {
                // this candidate is smaller
            }
            pos /= 2;
        }

        losers_[0].sup = sup;
        losers_[0].source = source;
        losers_[0].key = key;
    }
};

/*!
 * Guarded loser tree/tournament tree, either copying the whole element into the
 * tree structure, or looking up the element via the index.
 *
 * Stable specialization of LoserTreeCopyBase.
 *
 * Guarding is done explicitly through one flag sup per element, inf is not
 * needed due to a better initialization routine.  This is a well-performing
 * variant.
 *
 * \tparam ValueType the element type
 * \tparam Comparator comparator to use for binary comparisons.
 */
template <typename ValueType, typename Comparator>
class LoserTreeCopy</* Stable == */ true, ValueType, Comparator>
    : public LoserTreeCopyBase<ValueType, Comparator>
{
public:
    using Super = LoserTreeCopyBase<ValueType, Comparator>;
    using Source = typename Super::Source;

protected:
    using Super::k_;
    using Super::losers_;
    using Super::cmp_;

public:
    explicit LoserTreeCopy(const Source& k,
                           const Comparator& cmp = Comparator())
        : Super(k, cmp) { }

    // do not pass const reference since key will be used as local variable
    void delete_min_insert(const ValueType* keyp, bool sup) {
        using std::swap;
        assert(sup == (keyp == nullptr));

        Source source = losers_[0].source;
        ValueType key = keyp ? *keyp : ValueType();
        Source pos = (k_ + source) / 2;

        while (pos > 0) {
            if ((TLX_UNLIKELY(sup) && (
                     !TLX_UNLIKELY(losers_[pos].sup) ||
                     losers_[pos].source < source)) ||
                (!TLX_UNLIKELY(sup) && !TLX_UNLIKELY(losers_[pos].sup) &&
                 ((cmp_(losers_[pos].key, key)) ||
                  (!cmp_(key, losers_[pos].key) &&
                   losers_[pos].source < source)))) {
                // the other one is smaller
                swap(losers_[pos].sup, sup);
                swap(losers_[pos].source, source);
                swap(losers_[pos].key, key);
            }
            pos /= 2;
        }

        losers_[0].sup = sup;
        losers_[0].source = source;
        losers_[0].key = key;
    }
};

/*!
 * Guarded loser tree, using pointers to the elements instead of copying them
 * into the tree nodes.
 *
 * This is a base class for the LoserTreePointer\<true> and \<false> classes.
 *
 * Guarding is done explicitly through one flag sup per element, inf is not
 * needed due to a better initialization routine.  This is a well-performing
 * variant.
 */
template <typename ValueType, typename Comparator = std::less<ValueType> >
class LoserTreePointerBase
{
public:
    //! size of counters and array indexes
    using Source = std::uint32_t;

    //! sentinel for invalid or finished Sources
    static constexpr Source invalid_ = Source(-1);

protected:
    //! Internal representation of a loser tree player/node
    struct Loser {
        //! index of source
        Source source;
        //! pointer to key value of the element in this node
        const ValueType* keyp;
    };

    //! number of nodes
    const Source ik_;
    //! log_2(ik) next greater power of 2
    const Source k_;
    //! array containing loser tree nodes
    SimpleVector<Loser> losers_;
    //! the comparator object
    Comparator cmp_;

public:
    explicit LoserTreePointerBase(
        Source k, const Comparator& cmp = Comparator())
        : ik_(k), k_(round_up_to_power_of_two(ik_)), losers_(k_ * 2),
          cmp_(cmp) {
        for (Source i = ik_ - 1; i < k_; i++) {
            losers_[i + k_].keyp = nullptr;
            losers_[i + k_].source = invalid_;
        }
    }

    LoserTreePointerBase(const LoserTreePointerBase&) = delete;
    LoserTreePointerBase& operator = (const LoserTreePointerBase&) = delete;
    LoserTreePointerBase(LoserTreePointerBase&&) = default;
    LoserTreePointerBase& operator = (LoserTreePointerBase&&) = default;

    //! return the index of the player with the smallest element.
    Source min_source() {
        return losers_[0].keyp ? losers_[0].source : invalid_;
    }

    /*!
     * Initializes the player source with the element key.
     *
     * \param keyp the element to insert
     * \param source index of the player
     * \param sup flag that determines whether the value to insert is an
     *   explicit supremum sentinel.
     */
    void insert_start(const ValueType* keyp, const Source& source, bool sup) {
        Source pos = k_ + source;

        assert(pos < losers_.size());
        assert(sup == (keyp == nullptr));
        unused(sup);

        losers_[pos].source = source;
        losers_[pos].keyp = keyp;
    }

    /*!
     * Computes the winner of the competition at player root.  Called
     * recursively (starting at 0) to build the initial tree.
     *
     * \param root index of the game to start.
     */
    Source init_winner(const Source& root) {
        if (root >= k_)
            return root;

        Source left = init_winner(2 * root);
        Source right = init_winner(2 * root + 1);
        if (!losers_[right].keyp ||
            (losers_[left].keyp &&
             !cmp_(*losers_[right].keyp, *losers_[left].keyp))) {
            // left one is less or equal
            losers_[root] = losers_[right];
            return left;
        }
        else {
            // right one is less
            losers_[root] = losers_[left];
            return right;
        }
    }

    void init() {
        if (TLX_UNLIKELY(k_ == 0))
            return;
        losers_[0] = losers_[init_winner(1)];
    }
};

/*!
 * Guarded loser tree, using pointers to the elements instead of copying them
 * into the tree nodes.
 *
 * Unstable specialization of LoserTreeCopyBase.
 *
 * Guarding is done explicitly through one flag sup per element, inf is not
 * needed due to a better initialization routine.  This is a well-performing
 * variant.
 */
template <bool Stable /* == false */, typename ValueType,
          typename Comparator = std::less<ValueType> >
class LoserTreePointer : public LoserTreePointerBase<ValueType, Comparator>
{
public:
    using Super = LoserTreePointerBase<ValueType, Comparator>;
    using Source = typename Super::Source;

protected:
    using Super::k_;
    using Super::losers_;
    using Super::cmp_;

public:
    explicit LoserTreePointer(Source k, const Comparator& cmp = Comparator())
        : Super(k, cmp) { }

    void delete_min_insert(const ValueType* keyp, bool sup) {
        using std::swap;
        assert(sup == (keyp == nullptr));
        unused(sup);

        Source source = losers_[0].source;
        Source pos = (k_ + source) / 2;

        while (pos > 0) {
            if (TLX_UNLIKELY(!keyp)) {
                // the other candidate is smaller
                swap(losers_[pos].source, source);
                swap(losers_[pos].keyp, keyp);
            }
            else if (TLX_UNLIKELY(!losers_[pos].keyp)) {
                // this candidate is smaller
            }
            else if (cmp_(*losers_[pos].keyp, *keyp)) {
                // the other one is smaller
                swap(losers_[pos].source, source);
                swap(losers_[pos].keyp, keyp);
            }
            else {
                // this candidate is smaller
            }
            pos /= 2;
        }

        losers_[0].source = source;
        losers_[0].keyp = keyp;
    }
};

/*!
 * Guarded loser tree, using pointers to the elements instead of copying them
 * into the tree nodes.
 *
 * Unstable specialization of LoserTreeCopyBase.
 *
 * Guarding is done explicitly through one flag sup per element, inf is not
 * needed due to a better initialization routine.  This is a well-performing
 * variant.
 */
template <typename ValueType, typename Comparator>
class LoserTreePointer</* Stable == */ true, ValueType, Comparator>
    : public LoserTreePointerBase<ValueType, Comparator>
{
public:
    using Super = LoserTreePointerBase<ValueType, Comparator>;
    using Source = typename Super::Source;

protected:
    using Super::k_;
    using Super::losers_;
    using Super::cmp_;

public:
    explicit LoserTreePointer(Source k, const Comparator& cmp = Comparator())
        : Super(k, cmp) { }

    void delete_min_insert(const ValueType* keyp, bool sup) {
        using std::swap;
        assert(sup == (keyp == nullptr));
        unused(sup);

        Source source = losers_[0].source;
        for (Source pos = (k_ + source) / 2; pos > 0; pos /= 2) {
            // the smaller one gets promoted, ties are broken by source
            if ((!keyp &&
                 (losers_[pos].keyp || losers_[pos].source < source)) ||
                (keyp && losers_[pos].keyp &&
                 ((cmp_(*losers_[pos].keyp, *keyp)) ||
                  (!cmp_(*keyp, *losers_[pos].keyp) &&
                   losers_[pos].source < source)))) {
                // the other one is smaller
                swap(losers_[pos].source, source);
                swap(losers_[pos].keyp, keyp);
            }
        }

        losers_[0].source = source;
        losers_[0].keyp = keyp;
    }
};

/*!
 * Unguarded loser tree, copying the whole element into the tree structure.
 *
 * This is a base class for the LoserTreeCopyUnguarded\<true> and \<false>
 * classes.
 *
 * No guarding is done, therefore not a single input sequence must run empty.
 * This is a very fast variant.
 */
template <typename ValueType, typename Comparator = std::less<ValueType> >
class LoserTreeCopyUnguardedBase
{
public:
    //! size of counters and array indexes
    using Source = std::uint32_t;

    //! sentinel for invalid or finished Sources
    static constexpr Source invalid_ = Source(-1);

protected:
    //! Internal representation of a loser tree player/node
    struct Loser {
        //! index of source
        Source source;
        //! copy of key value of the element in this node
        ValueType key;
    };

    //! number of nodes
    Source ik_;
    //! log_2(ik) next greater power of 2
    Source k_;
    //! array containing loser tree nodes
    SimpleVector<Loser> losers_;
    //! the comparator object
    Comparator cmp_;

public:
    LoserTreeCopyUnguardedBase(Source k, const ValueType& sentinel,
                               const Comparator& cmp = Comparator())
        : ik_(k), k_(round_up_to_power_of_two(ik_)), losers_(k_ * 2),
          cmp_(cmp) {
        for (Source i = 0; i < 2 * k_; i++) {
            losers_[i].source = invalid_;
            losers_[i].key = sentinel;
        }
    }

    //! return the index of the player with the smallest element.
    Source min_source() {
        assert(losers_[0].source != invalid_ &&
               "Data underrun in unguarded merging.");
        return losers_[0].source;
    }

    void insert_start(const ValueType* keyp, const Source& source, bool sup) {
        Source pos = k_ + source;

        assert(pos < losers_.size());
        assert(sup == (keyp == nullptr));
        unused(sup);

        losers_[pos].source = source;
        losers_[pos].key = *keyp;
    }

    Source init_winner(const Source& root) {
        if (root >= k_)
            return root;

        Source left = init_winner(2 * root);
        Source right = init_winner(2 * root + 1);
        if (!cmp_(losers_[right].key, losers_[left].key)) {
            // left one is less or equal
            losers_[root] = losers_[right];
            return left;
        }
        else {
            // right one is less
            losers_[root] = losers_[left];
            return right;
        }
    }

    void init() {
        if (TLX_UNLIKELY(k_ == 0))
            return;
        losers_[0] = losers_[init_winner(1)];
    }
};

template <bool Stable /* == false */, typename ValueType,
          typename Comparator = std::less<ValueType> >
class LoserTreeCopyUnguarded
    : public LoserTreeCopyUnguardedBase<ValueType, Comparator>
{
public:
    using Super = LoserTreeCopyUnguardedBase<ValueType, Comparator>;
    using Source = typename Super::Source;

private:
    using Super::k_;
    using Super::losers_;
    using Super::cmp_;

public:
    LoserTreeCopyUnguarded(Source k, const ValueType& sentinel,
                           const Comparator& cmp = Comparator())
        : Super(k, sentinel, cmp) { }

    // do not pass const reference since key will be used as local variable
    void delete_min_insert(const ValueType* keyp, bool sup) {
        using std::swap;
        assert(sup == (keyp == nullptr));
        unused(sup);

        Source source = losers_[0].source;
        ValueType key = keyp ? *keyp : ValueType();

        for (Source pos = (k_ + source) / 2; pos > 0; pos /= 2) {
            // the smaller one gets promoted
            if (cmp_(losers_[pos].key, key)) {
                // the other one is smaller
                swap(losers_[pos].source, source);
                swap(losers_[pos].key, key);
            }
        }

        losers_[0].source = source;
        losers_[0].key = key;
    }
};

template <typename ValueType, typename Comparator>
class LoserTreeCopyUnguarded</* Stable == */ true, ValueType, Comparator>
    : public LoserTreeCopyUnguardedBase<ValueType, Comparator>
{
public:
    using Super = LoserTreeCopyUnguardedBase<ValueType, Comparator>;
    using Source = typename Super::Source;

private:
    using Super::k_;
    using Super::losers_;
    using Super::cmp_;

public:
    LoserTreeCopyUnguarded(Source k, const ValueType& sentinel,
                           const Comparator& comp = Comparator())
        : Super(k, sentinel, comp) { }

    // do not pass const reference since key will be used as local variable
    void delete_min_insert(const ValueType* keyp, bool sup) {
        using std::swap;
        assert(sup == (keyp == nullptr));
        unused(sup);

        Source source = losers_[0].source;
        ValueType key = keyp ? *keyp : ValueType();

        for (Source pos = (k_ + source) / 2; pos > 0; pos /= 2) {
            if (cmp_(losers_[pos].key, key) ||
                (!cmp_(key, losers_[pos].key) &&
                 losers_[pos].source < source)) {
                // the other one is smaller
                swap(losers_[pos].source, source);
                swap(losers_[pos].key, key);
            }
        }

        losers_[0].source = source;
        losers_[0].key = key;
    }
};

/*!
 * Unguarded loser tree, keeping only pointers to the elements in the tree
 * structure.
 *
 * This is a base class for the LoserTreePointerUnguarded\<true> and \<false>
 * classes.
 *
 * No guarding is done, therefore not a single input sequence must run empty.
 * This is a very fast variant.
 */
template <typename ValueType, typename Comparator = std::less<ValueType> >
class LoserTreePointerUnguardedBase
{
public:
    //! size of counters and array indexes
    using Source = std::uint32_t;

    //! sentinel for invalid or finished Sources
    static constexpr Source invalid_ = Source(-1);

protected:
    //! Internal representation of a loser tree player/node
    struct Loser {
        //! index of source
        Source source;
        //! copy of key value of the element in this node
        const ValueType* keyp;
    };

    //! number of nodes
    Source ik_;
    //! log_2(ik) next greater power of 2
    Source k_;
    //! array containing loser tree nodes
    SimpleVector<Loser> losers_;
    //! the comparator object
    Comparator cmp_;

public:
    LoserTreePointerUnguardedBase(const Source& k, const ValueType& sentinel,
                                  const Comparator& cmp = Comparator())
        : ik_(k), k_(round_up_to_power_of_two(ik_)),
          losers_(k_ * 2), cmp_(cmp) {
        for (Source i = ik_ - 1; i < k_; i++) {
            losers_[i + k_].source = invalid_;
            losers_[i + k_].keyp = &sentinel;
        }
    }

    // non construction-copyable
    LoserTreePointerUnguardedBase(
        const LoserTreePointerUnguardedBase& other) = delete;
    // non copyable
    LoserTreePointerUnguardedBase& operator = (
        const LoserTreePointerUnguardedBase&) = delete;

    Source min_source() { return losers_[0].source; }

    void insert_start(const ValueType* keyp, const Source& source, bool sup) {
        Source pos = k_ + source;

        assert(pos < losers_.size());
        assert(sup == (keyp == nullptr));
        unused(sup);

        losers_[pos].source = source;
        losers_[pos].keyp = keyp;
    }

    Source init_winner(const Source& root) {
        if (root >= k_)
            return root;

        Source left = init_winner(2 * root);
        Source right = init_winner(2 * root + 1);
        if (!cmp_(*losers_[right].keyp, *losers_[left].keyp)) {
            // left one is less or equal
            losers_[root] = losers_[right];
            return left;
        }
        else {
            // right one is less
            losers_[root] = losers_[left];
            return right;
        }
    }

    void init() {
        if (TLX_UNLIKELY(k_ == 0))
            return;
        losers_[0] = losers_[init_winner(1)];
    }
};

template <bool Stable /* == false */, typename ValueType,
          typename Comparator = std::less<ValueType> >
class LoserTreePointerUnguarded
    : public LoserTreePointerUnguardedBase<ValueType, Comparator>
{
public:
    using Super = LoserTreePointerUnguardedBase<ValueType, Comparator>;
    using Source = typename Super::Source;

protected:
    using Super::k_;
    using Super::losers_;
    using Super::cmp_;

public:
    LoserTreePointerUnguarded(const Source& k, const ValueType& sentinel,
                              const Comparator& cmp = Comparator())
        : Super(k, sentinel, cmp) { }

    void delete_min_insert(const ValueType* keyp, bool sup) {
        using std::swap;
        assert(sup == (keyp == nullptr));
        unused(sup);

        Source source = losers_[0].source;
        for (Source pos = (k_ + source) / 2; pos > 0; pos /= 2) {
            // the smaller one gets promoted
            if (cmp_(*losers_[pos].keyp, *keyp)) {
                // the other one is smaller
                swap(losers_[pos].source, source);
                swap(losers_[pos].keyp, keyp);
            }
        }

        losers_[0].source = source;
        losers_[0].keyp = keyp;
    }
};

template <typename ValueType, typename Comparator>
class LoserTreePointerUnguarded</* Stable == */ true, ValueType, Comparator>
    : public LoserTreePointerUnguardedBase<ValueType, Comparator>
{
public:
    using Super = LoserTreePointerUnguardedBase<ValueType, Comparator>;
    using Source = typename Super::Source;

protected:
    using Super::k_;
    using Super::losers_;
    using Super::cmp_;

public:
    LoserTreePointerUnguarded(const Source& k, const ValueType& sentinel,
                              const Comparator& cmp = Comparator())
        : Super(k, sentinel, cmp) { }

    void delete_min_insert(const ValueType* keyp, bool sup) {
        using std::swap;
        assert(sup == (keyp == nullptr));
        unused(sup);

        Source source = losers_[0].source;
        for (Source pos = (k_ + source) / 2; pos > 0; pos /= 2) {
            // the smaller one gets promoted, ties are broken by source
            if (cmp_(*losers_[pos].keyp, *keyp) ||
                (!cmp_(*keyp, *losers_[pos].keyp) &&
                 losers_[pos].source < source)) {
                // the other one is smaller
                swap(losers_[pos].source, source);
                swap(losers_[pos].keyp, keyp);
            }
        }

        losers_[0].source = source;
        losers_[0].keyp = keyp;
    }
};

/******************************************************************************/
// LoserTreeSwitch selects loser tree by size of value type

template <bool Stable, typename ValueType, typename Comparator,
          typename Enable = void>
class LoserTreeSwitch
{
public:
    using Type = LoserTreePointer<Stable, ValueType, Comparator>;
};

template <bool Stable, typename ValueType, typename Comparator>
class LoserTreeSwitch<
    Stable, ValueType, Comparator,
    typename std::enable_if<sizeof(ValueType) <= 2 * sizeof(size_t)>::type>
{
public:
    using Type = LoserTreeCopy<Stable, ValueType, Comparator>;
};

template <bool Stable, typename ValueType, typename Comparator>
using LoserTree = typename LoserTreeSwitch<Stable, ValueType, Comparator>::Type;

/*----------------------------------------------------------------------------*/

template <bool Stable, typename ValueType, typename Comparator,
          typename Enable = void>
class LoserTreeUnguardedSwitch
{
public:
    using Type = LoserTreePointerUnguarded<Stable, ValueType, Comparator>;
};

template <bool Stable, typename ValueType, typename Comparator>
class LoserTreeUnguardedSwitch<
    Stable, ValueType, Comparator,
    typename std::enable_if<sizeof(ValueType) <= 2 * sizeof(size_t)>::type>
{
public:
    using Type = LoserTreeCopyUnguarded<Stable, ValueType, Comparator>;
};

template <bool Stable, typename ValueType, typename Comparator>
using LoserTreeUnguarded =
    typename LoserTreeUnguardedSwitch<Stable, ValueType, Comparator>::Type;

//! \}
//! \}

} // namespace tlx

#endif // !TLX_CONTAINER_LOSER_TREE_HEADER

/******************************************************************************/

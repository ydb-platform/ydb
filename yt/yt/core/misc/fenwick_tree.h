#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A data structure for storing dynamic cumulative sums.
//! NB: TItem::operator+ is assumed to be associative and commutative.
template <class TItem>
class TFenwickTree
{
public:
    void PushBack(const TItem& item);

    template <class... TArgs>
    void EmplaceBack(TArgs&&... args);

    //! |index| must be in [0, Size()).
    void SetValue(int index, const TItem& item);

    //! Add |delta| to the value at the position |index|.
    //! |index| must be in [0, Size()).
    void Increment(int index, const TItem& delta);

    void PopBack();

    //! Returns cumulative sum of all items in [0, |index|).
    //! |index| must be in [0, Size()].
    TItem GetCumulativeSum(int index) const;

    //! Returns the smallest index such that the sum of items in [0, index) is not less than |sum|.
    //! If the comparer is provided, returns the smallest index such that
    //! comparer(sum([0, index), |sum|) is false.
    //!
    //! Is equivalent to std::lower_bound on the array of cumulative sums.
    //!
    //! NB: Works in assumption that all values are nonnegative.
    template <class TValue, class TComparer = std::less<>>
    int LowerBound(const TValue& sum, TComparer&& comparer = TComparer{}) const;

    //! Returns the smallest index such that the sum of items in [0, index) is greater than |sum|.
    //! If the comparer is provided, returns the smallest index such that
    //! comparer(|sum|, sum([0, index)) is true.
    //!
    //! Is equivalent to std::upper_bound on the array of cumulative sums.
    //!
    //! NB: Works in assumption that all values are nonnegative.
    template <class TValue, class TComparer = std::less<>>
    int UpperBound(const TValue& sum, TComparer&& comparer = TComparer{}) const;

    i64 Size() const;

    void Clear();

    void Persist(const NYT::TStreamPersistenceContext& context);

private:
    static constexpr ssize_t MaxLength = 1<<30;

    std::vector<TItem> FenwickSums_;

    //! Returns the value that would be stored in FenwickSums_[index] if only
    //! values up to |index|, exclusive, were present.
    TItem CalculateIncompleteFenwickSum(int index) const;

    TItem GetValue(int index) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define FENWICK_TREE_INL_H_
#include "fenwick_tree-inl.h"
#undef FENWICK_TREE_INL_H_

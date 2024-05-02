#ifndef FENWICK_TREE_INL_H_
#error "Direct inclusion of this file is not allowed, include fenwick_tree.h"
// For the sake of sane code completion.
#include "fenwick_tree.h"
#endif

#include "serialize.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/*
 *  Fenwick tree is based on partial sum array FenwickSums_.
 *
 *  FenwickSums_[i] contains a partial sum from index (i&(i+1)) to index i inclusive.
 *  Note that if i = xxx0111b, then (i&(i+1)) is xxx0000b.
 *  Thus FenwickSums_[i] contains a sum of elements whose index equals i except
    for several least significant 1's of i.
 *
 *  With FenwickSums_ we can calculate a prefix sum in O(log n):
 *  FenwickSums_[i] gives us a partial sum from i = xxx0111b down to j = xxx0000b.
 *  Let j = xx10000b, then j-1 is xx01111b. We add FenwickSums_[j-1] to the result and so on.
 *  Note that after each iteration the number of least significant zeros of j increases,
 *  so the complexity follows.
 *
 *  To update an element with index i we need to update all FenwickSums_[j] which contain i.
 *  Let j be xxx0111b. Then xxx0 part must be exactly the same as in i.  Thus to find such j
 *  we need to iteratively replace 0 by 1 in i starting from the least significant 0.
 *
 *  To insert a new element with index i = xx01111b we add FenwickSums_[xx01110b],
 *  FenwickSums_[xx01101b], FenwickSums_[xx01011b] and FenwickSums_[xx00111b]
 *  with the new element, obtaining FenwickSums_[i].
 */

template <class TItem>
void TFenwickTree<TItem>::PushBack(const TItem& item)
{
    int newItemIndex = Size();
    YT_VERIFY(Size() <= MaxLength);
    FenwickSums_.push_back(CalculateIncompleteFenwickSum(newItemIndex) + item);
}

template <class TItem>
template <class... TArgs>
void TFenwickTree<TItem>::EmplaceBack(TArgs&&... args)
{
    int newItemIndex = Size();
    YT_VERIFY(Size() <= MaxLength);
    FenwickSums_.push_back(CalculateIncompleteFenwickSum(newItemIndex) + TItem(std::forward<TArgs>(args)...));
}

template <class TItem>
void TFenwickTree<TItem>::SetValue(int index, const TItem& item)
{
    YT_VERIFY(index >= 0);
    YT_VERIFY(index < Size());
    Increment(index, item - GetValue(index));
}

template <class TItem>
void TFenwickTree<TItem>::PopBack()
{
    YT_VERIFY(Size() > 0);
    FenwickSums_.pop_back();
}

template <class TItem>
TItem TFenwickTree<TItem>::GetCumulativeSum(int index) const
{
    YT_VERIFY(index >= 0);
    YT_VERIFY(index <= Size());

    --index;
    TItem result{};
    while (index >= 0) {
        result = result + FenwickSums_[index];
        index = (index & (index + 1)) - 1;
    }
    return result;
}

template <class TItem>
template <class TValue, class TComparer>
int TFenwickTree<TItem>::LowerBound(const TValue& sum, TComparer&& comparer) const
{
    int resultIndex = 0;
    TItem foundSum{};

    if (!comparer(foundSum, sum)) {
        return resultIndex;
    }

    int step = 1;
    while (step <= Size()) {
        step *= 2;
    }

    while (step >= 1) {
        int newIndex = resultIndex + step - 1;
        if (newIndex < Size() && comparer(foundSum + FenwickSums_[newIndex], sum)) {
            foundSum = foundSum + FenwickSums_[newIndex];
            resultIndex += step;
        }
        step /= 2;
    }

    return resultIndex + 1;
}

template <class TItem>
template <class TValue, class TComparer>
int TFenwickTree<TItem>::UpperBound(const TValue& sum, TComparer&& comparer) const
{
    return LowerBound(sum, [&comparer] (const auto& lhs, const auto& rhs) {
        return !comparer(rhs, lhs);
    });
}

template <class TItem>
i64 TFenwickTree<TItem>::Size() const
{
    return FenwickSums_.size();
}

template <class TItem>
void TFenwickTree<TItem>::Clear()
{
    FenwickSums_.clear();
}

template <class TItem>
void TFenwickTree<TItem>::Persist(const NYT::TStreamPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, FenwickSums_);
}

template <class TItem>
void TFenwickTree<TItem>::Increment(int index, const TItem& delta)
{
    while (index < Size()) {
        FenwickSums_[index] = FenwickSums_[index] + delta;
        index |= index + 1;
    }
}

template <class TItem>
TItem TFenwickTree<TItem>::CalculateIncompleteFenwickSum(int index) const
{
    YT_VERIFY(index >= 0);
    YT_VERIFY(index <= Size());
    TItem sum{};
    int bit = 1;
    while (index & bit) {
        sum = sum + FenwickSums_[index - 1];
        index ^= bit;
        bit <<= 1;
    }
    return sum;
}

template <class TItem>
TItem TFenwickTree<TItem>::GetValue(int index) const
{
    YT_VERIFY(index >= 0);
    YT_VERIFY(index < Size());
    return FenwickSums_[index] - CalculateIncompleteFenwickSum(index);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

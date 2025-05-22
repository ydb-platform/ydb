#ifndef SUMMARY_INL_H_
#error "Direct inclusion of this file is not allowed, include summary.h"
// For the sake of sane code completion.
#include "summary.h"
#endif
#undef SUMMARY_INL_H_

#include <util/generic/utility.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TSummarySnapshot<T>::TSummarySnapshot(T sum, T min, T max, T last, i64 count)
    : Sum_(sum)
    , Min_(min)
    , Max_(max)
    , Last_(last)
    , Count_(count)
{ }

template <class T>
void TSummarySnapshot<T>::Record(T value)
{
    if (Count_ == 0) {
        Sum_ = Min_ = Max_ = value;
    } else {
        Sum_ += value;
        Min_ = ::Min(Min_, value);
        Max_ = ::Max(Max_, value);
    }
    Count_++;
}

template <class T>
void TSummarySnapshot<T>::Add(const TSummarySnapshot& other)
{
    if (other.Count_ == 0) {
        return;
    }

    if (Count_ == 0) {
        *this = other;
    } else {
        Sum_ += other.Sum_;
        Min_ = ::Min(Min_, other.Min_);
        Max_ = ::Max(Max_, other.Max_);
        Count_ += other.Count_;
    }
}

template <class T>
TSummarySnapshot<T>& TSummarySnapshot<T>::operator += (const TSummarySnapshot<T>& other)
{
    Add(other);
    return *this;
}

template <class T>
T TSummarySnapshot<T>::Sum() const
{
    return Sum_;
}

template <class T>
T TSummarySnapshot<T>::Min() const
{
    return Min_;
}

template <class T>
T TSummarySnapshot<T>::Max() const
{
    return Max_;
}

template <class T>
T TSummarySnapshot<T>::Last() const
{
    return Last_;
}

template <class T>
i64 TSummarySnapshot<T>::Count() const
{
    return Count_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling

#ifndef MOVING_AVERAGE_INL_H_
#error "Direct inclusion of this file is not allowed, include moving_average.h"
// For the sake of sane code completion.
#include "moving_average.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
TMovingAverage<TValue>::TMovingAverage(int windowSize)
{
    SetWindowSize(windowSize);
}

template <class TValue>
void TMovingAverage<TValue>::AddValue(TValue value)
{
    Total_ += value;
    Values_.push_back(std::move(value));

    RemoveOldValues();
}

template <class TValue>
std::optional<TValue> TMovingAverage<TValue>::GetAverage() const
{
    if (Values_.empty()) {
        return {};
    }

    return Total_ / std::ssize(Values_);
}

template <class TValue>
void TMovingAverage<TValue>::SetWindowSize(int windowSize)
{
    YT_VERIFY(windowSize >= 0);

    WindowSize_ = windowSize;

    RemoveOldValues();
}

template <class TValue>
void TMovingAverage<TValue>::Reset()
{
    Values_.clear();
    Total_ = {};
}

template <class TValue>
void TMovingAverage<TValue>::RemoveOldValues()
{
    while (std::ssize(Values_) > WindowSize_) {
        Total_ -= Values_.front();
        Values_.pop_front();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

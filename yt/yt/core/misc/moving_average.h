#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TMovingAverage
{
public:
    TMovingAverage() = default;
    explicit TMovingAverage(int windowSize);

    void AddValue(TValue value);

    std::optional<TValue> GetAverage() const;

    void SetWindowSize(int windowSize);

    void Reset();

private:
    int WindowSize_ = 0;
    std::deque<TValue> Values_;
    TValue Total_{};

    void RemoveOldValues();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define MOVING_AVERAGE_INL_H_
#include "moving_average-inl.h"
#undef MOVING_AVERAGE_INL_H_

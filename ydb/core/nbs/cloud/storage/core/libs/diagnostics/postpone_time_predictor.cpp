#include "postpone_time_predictor.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/ring_buffer.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/timer.h>

#include <util/generic/list.h>
#include <util/generic/maybe.h>
#include <util/system/guard.h>
#include <util/system/spinlock.h>

namespace NYdb::NBS {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TPostponeTimePredictor: public IPostponeTimePredictor
{
private:
    struct TRequestState
    {
        ui64 TotalCount = 0;
        ui64 ThrottledCount = 0;
        TDuration ThrottledDelay = TDuration::Zero();
    } TotalRequestState;

    struct TPostponeData
    {
        TInstant Timestamp = TInstant::Zero();
        TRequestState RequestState;
    };

    const ITimerPtr Timer;
    const TDuration DelayWindowInterval;
    const double DelayWindowPercentage;
    const TMaybe<TDuration> DelayUpperBound;

    TAdaptiveLock Lock;
    TRingBuffer<TPostponeData> PostponedBySecond;

public:
    TPostponeTimePredictor(
        ITimerPtr timer,
        TDuration delayWindowInterval,
        double delayWindowPercentage,
        TMaybe<TDuration> delayUpperBound)
        : Timer(std::move(timer))
        , DelayWindowInterval(delayWindowInterval)
        , DelayWindowPercentage(delayWindowPercentage)
        , DelayUpperBound(std::move(delayUpperBound))
        , PostponedBySecond(
              Max(1, static_cast<int>(DelayWindowInterval.Seconds())))
    {}

    void Register(TDuration postponeDelay) override
    {
        with_lock (Lock) {
            const auto currentTimestamp = Timer->Now();

            RemoveStaleRequests(currentTimestamp);
            UpdateBuffer(currentTimestamp, postponeDelay);
        }
    }

    TDuration GetPossiblePostponeDuration() override
    {
        with_lock (Lock) {
            RemoveStaleRequests(Timer->Now());
            return CalculatePossiblePostponeDuration();
        }
    }

private:
    bool IsThrottled(TDuration postponeDelay) const
    {
        return postponeDelay > TDuration::Zero();
    }

    TRequestState SumStates(
        const TRequestState& lhs,
        const TRequestState& rhs) const
    {
        return TRequestState{
            .TotalCount = lhs.TotalCount + rhs.TotalCount,
            .ThrottledCount = lhs.ThrottledCount + rhs.ThrottledCount,
            .ThrottledDelay = lhs.ThrottledDelay + rhs.ThrottledDelay};
    }

    TRequestState SubStates(
        const TRequestState& lhs,
        const TRequestState& rhs) const
    {
        return TRequestState{
            .TotalCount = lhs.TotalCount - rhs.TotalCount,
            .ThrottledCount = lhs.ThrottledCount - rhs.ThrottledCount,
            .ThrottledDelay = lhs.ThrottledDelay - rhs.ThrottledDelay};
    }

    void RemoveStaleRequests(TInstant current)
    {
        TRequestState removedState;
        while (!PostponedBySecond.IsEmpty()) {
            const auto& latest = PostponedBySecond.Front();
            if (current - latest.Timestamp <
                TDuration::Seconds(PostponedBySecond.Capacity()))
            {
                break;
            }
            removedState = SumStates(removedState, latest.RequestState);
            PostponedBySecond.PopFront();
        }
        TotalRequestState = SubStates(TotalRequestState, removedState);
    }

    void UpdateBuffer(TInstant current, TDuration postpone)
    {
        TRequestState addedState;
        addedState.TotalCount = 1;
        if (IsThrottled(postpone)) {
            addedState.ThrottledCount = 1;
            addedState.ThrottledDelay = postpone;
        }

        while (!PostponedBySecond.IsEmpty()) {
            const auto& freshest = PostponedBySecond.Back();
            if (current - freshest.Timestamp < TDuration::Seconds(1)) {
                break;
            }
            if (PostponedBySecond.IsFull()) {
                Y_DEBUG_ABORT_UNLESS(
                    false,
                    "postpone time predictor's buffer is overflowed");
                break;
            }
            PostponedBySecond.PushBack(TPostponeData{
                .Timestamp = freshest.Timestamp + TDuration::Seconds(1)});
        }

        if (PostponedBySecond.IsEmpty()) {
            PostponedBySecond.PushBack(TPostponeData{.Timestamp = current});
        }

        const auto newData = TPostponeData{
            .Timestamp = PostponedBySecond.Back().Timestamp,
            .RequestState =
                SumStates(addedState, PostponedBySecond.Back().RequestState)};
        PostponedBySecond.PopBack();
        PostponedBySecond.PushBack(newData);

        TotalRequestState = SumStates(TotalRequestState, addedState);
    }

    TDuration CalculatePossiblePostponeDuration() const
    {
        if (!TotalRequestState.TotalCount || !TotalRequestState.ThrottledCount)
        {
            return TDuration::Zero();
        }

        const double throttledRequestCountD =
            static_cast<double>(TotalRequestState.ThrottledCount);
        const double totalRequestCountD =
            static_cast<double>(TotalRequestState.TotalCount);
        if (throttledRequestCountD < totalRequestCountD * DelayWindowPercentage)
        {
            return TDuration::Zero();
        }

        const ui64 postponeDurationSum =
            TotalRequestState.ThrottledDelay.MicroSeconds();

        const auto result = TDuration::MicroSeconds(
            postponeDurationSum / TotalRequestState.ThrottledCount);
        return Min(result, DelayUpperBound.GetOrElse(result));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TPostponeTimePredictorStub: public IPostponeTimePredictor
{
public:
    void Register(TDuration postponeDelay) override
    {
        Y_UNUSED(postponeDelay);
    }

    TDuration GetPossiblePostponeDuration() override
    {
        return TDuration::Zero();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IPostponeTimePredictorPtr CreatePostponeTimePredictor(
    ITimerPtr timer,
    TDuration delayWindowInterval,
    double delayWindowPercentage,
    TMaybe<TDuration> delayUpperBound)
{
    return std::make_shared<TPostponeTimePredictor>(
        std::move(timer),
        delayWindowInterval,
        delayWindowPercentage,
        std::move(delayUpperBound));
}

IPostponeTimePredictorPtr CreatePostponeTimePredictorStub()
{
    return std::make_shared<TPostponeTimePredictorStub>();
}

}   // namespace NYdb::NBS

#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/public.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

struct IPostponeTimePredictor
{
    virtual ~IPostponeTimePredictor() = default;

    virtual void Register(TDuration postponeDelay) = 0;
    virtual TDuration GetPossiblePostponeDuration() = 0;
};

////////////////////////////////////////////////////////////////////////////////

IPostponeTimePredictorPtr CreatePostponeTimePredictor(
    ITimerPtr timer,
    TDuration delayWindowInterval,
    double delayWindowPercentage,
    TMaybe<TDuration> delayUpperBound);
IPostponeTimePredictorPtr CreatePostponeTimePredictorStub();

}   // namespace NYdb::NBS

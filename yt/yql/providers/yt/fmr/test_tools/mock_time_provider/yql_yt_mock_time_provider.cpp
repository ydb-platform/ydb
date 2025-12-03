#include "yql_yt_mock_time_provider.h"

namespace NYql::NFmr {

TMockTimeProvider::TMockTimeProvider(TDuration sleepTime)
    : CurrentTime_(TInstant::Zero())
    , SleepTime_(sleepTime)
{
}

TInstant TMockTimeProvider::Now() {
    return CurrentTime_;
}

void TMockTimeProvider::Advance(TDuration duration, TMaybe<TDuration> sleepTime) {
    CurrentTime_ += duration;
    Sleep(sleepTime.GetOrElse(SleepTime_));
}

} // namespace NYql::NFmr


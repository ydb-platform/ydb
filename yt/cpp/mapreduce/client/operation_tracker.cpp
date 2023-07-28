#include "operation_tracker.h"

#include <yt/cpp/mapreduce/interface/config.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void TOperationExecutionTimeTracker::Start(const TOperationId& operationId) {
    with_lock(Lock_) {
        StartTimes_[operationId] = TInstant::Now();
    }
}

TMaybe<TDuration> TOperationExecutionTimeTracker::Finish(const TOperationId& operationId) {
    TDuration duration;
    with_lock(Lock_) {
        auto i = StartTimes_.find(operationId);
        if (i == StartTimes_.end()) {
            return Nothing();
        }
        duration = TInstant::Now() - i->second;
        StartTimes_.erase(i);
    }
    return duration;
}

TOperationExecutionTimeTracker* TOperationExecutionTimeTracker::Get() {
    return Singleton<TOperationExecutionTimeTracker>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

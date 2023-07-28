#pragma once

#include <yt/cpp/mapreduce/interface/operation.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/system/mutex.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TOperationExecutionTimeTracker {
public:
    void Start(const TOperationId& operationId);
    TMaybe<TDuration> Finish(const TOperationId& operationId);
    static TOperationExecutionTimeTracker* Get();

private:
    THashMap<TOperationId, TInstant> StartTimes_;
    TMutex Lock_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

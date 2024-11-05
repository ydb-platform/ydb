#pragma once

#include <ydb/library/yql/core/yql_execution.h>

#include <yt/cpp/mapreduce/interface/fwd.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/mutex.h>
#include <util/system/thread.h>

#include <atomic>
#include <functional>

namespace NYql {

namespace NNative {

class TOperationTracker: public TThrRefBase {
public:
    using TPtr = ::TIntrusivePtr<TOperationTracker>;

    TOperationTracker();
    ~TOperationTracker();

    void Stop();

    NThreading::TFuture<void> MakeOperationWaiter(const NYT::IOperationPtr& operation, TMaybe<ui32> publicId,
        const TString& ytServer, const TString& ytClusterName, const TOperationProgressWriter& writer, const TStatWriter& statWriter);

private:
    static void* Tracker(void* param);

    void Tracker();

private:
    TMutex Mutex_;
    TVector<std::function<bool()>> RunningOperations_;
    std::atomic<bool> Running_{false};
    TThread Thread_;
};

} // NNative

} // NYql

#pragma once

#include "yql_yt_op_tracker.h"

#include <ydb/library/yql/providers/yt/gateway/lib/transaction_cache.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_gateway.h>

#include <ydb/library/yql/core/yql_execution.h>
#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/utils/threading/async_queue.h>

#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/threading/future/async_semaphore.h>
#include <library/cpp/time_provider/time_provider.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/mutex.h>
#include <util/system/sem.h>
#include <util/system/tempfile.h>
#include <util/thread/pool.h>

#include <utility>

namespace NYql {

namespace NNative {

struct TSession: public TThrRefBase {
    using TPtr = TIntrusivePtr<TSession>;

    TSession(IYtGateway::TOpenSessionOptions&& options, size_t numThreads);
    ~TSession() = default;

    void Close();
    NYT::TNode CreateSpecWithDesc(const TVector<std::pair<TString, TString>>& code = {}) const;
    NYT::TNode CreateTableAttrs() const;

    void EnsureInitializedSemaphore(const TYtSettings::TConstPtr& settings);

    const TString UserName_;
    const TOperationProgressWriter ProgressWriter_;
    const TStatWriter StatWriter_;
    const TYqlOperationOptions OperationOptions_;
    const TIntrusivePtr<IRandomProvider> RandomProvider_;
    const TIntrusivePtr<ITimeProvider> TimeProvider_;
    const bool DeterministicMode_;
    TAsyncQueue::TPtr Queue_;
    TOperationTracker::TPtr OpTracker_;
    NThreading::TAsyncSemaphore::TPtr OperationSemaphore;
    TMutex Mutex_;

    TTransactionCache TxCache_;
    TString SessionId_;

private:
    void StopQueueAndTracker();
};

} // NNative

} // NYql

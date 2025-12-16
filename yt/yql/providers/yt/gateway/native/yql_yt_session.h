#pragma once

#include "yql_yt_op_tracker.h"

#include <yt/yql/providers/yt/gateway/lib/session.h>
#include <yt/yql/providers/yt/provider/yql_yt_gateway.h>

#include <yql/essentials/core/yql_execution.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/utils/threading/async_queue.h>

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

struct TSession: public TSessionBase {
    using TPtr = TIntrusivePtr<TSession>;

    TSession(IYtGateway::TOpenSessionOptions&& options, size_t numThreads);
    ~TSession() = default;

    void Close();
    NYT::TNode CreateTableAttrs() const;

    void EnsureInitializedSemaphore(const TYtSettings::TConstPtr& settings);
    void InitLocalCalcSemaphore(const TYtSettings::TConstPtr& settings);

    const TStatWriter StatWriter_;
    const bool DeterministicMode_;
    TAsyncQueue::TPtr Queue_;
    TOperationTracker::TPtr OpTracker_;
    NThreading::TAsyncSemaphore::TPtr OperationSemaphore;
    TMutex Mutex_;
    THolder<TFastSemaphore> LocalCalcSemaphore_;

    TTransactionCache TxCache_;

    const TQContext QContext_;
    const IYtFullCapture::TPtr FullCapture_;

private:
    void StopQueueAndTracker();
};

} // NNative

} // NYql

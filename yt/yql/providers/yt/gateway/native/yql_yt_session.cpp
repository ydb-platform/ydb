#include "yql_yt_session.h"

#include <yt/yql/providers/yt/gateway/lib/yt_helpers.h>
#include <yt/yql/providers/yt/lib/init_yt_api/init.h>

#include <yql/essentials/utils/log/log.h>

#include <util/system/env.h>

namespace NYql {

namespace NNative {

TSession::TSession(IYtGateway::TOpenSessionOptions&& options, size_t numThreads)
    : TSessionBase(options.SessionId_, std::move(options.UserName()), std::move(options.RandomProvider()))
    , ProgressWriter_(std::move(options.ProgressWriter()))
    , StatWriter_(std::move(options.StatWriter()))
    , OperationOptions_(std::move(options.OperationOptions()))
    , TimeProvider_(std::move(options.TimeProvider()))
    , DeterministicMode_(GetEnv("YQL_DETERMINISTIC_MODE"))
    , OperationSemaphore(nullptr)
    , LocalCalcSemaphore_(nullptr)
    , TxCache_(UserName_)
{
    InitYtApiOnce(OperationOptions_.AttrsYson);

    Queue_ = TAsyncQueue::Make(numThreads, "YtGateway");
    if (options.CreateOperationTracker()) {
        OpTracker_ = MakeIntrusive<TOperationTracker>();
    }
}

void TSession::StopQueueAndTracker() {
    if (OpTracker_) {
        OpTracker_->Stop();
    }
    Queue_->Stop();
}

void TSession::Close() {
    if (OperationSemaphore) {
        OperationSemaphore->Cancel();
    }

    try {
        TxCache_.AbortAll();
    } catch (...) {
        YQL_CLOG(ERROR, ProviderYt) << CurrentExceptionMessage();
        StopQueueAndTracker();
        throw;
    }

    StopQueueAndTracker();
}

NYT::TNode TSession::CreateSpecWithDesc(const TVector<std::pair<TString, TString>>& code) const {
    return YqlOpOptionsToSpec(OperationOptions_, UserName_, code);
}

NYT::TNode TSession::CreateTableAttrs() const {
    return YqlOpOptionsToAttrs(OperationOptions_);
}

void TSession::EnsureInitializedSemaphore(const TYtSettings::TConstPtr& settings) {
    with_lock(Mutex_) {
        if (!OperationSemaphore) {
            const size_t parallelOperationsLimit = settings->ParallelOperationsLimit.Get().GetOrElse(1U << 20);
            OperationSemaphore = NThreading::TAsyncSemaphore::Make(parallelOperationsLimit);
        }
    }
}

void TSession::InitLocalCalcSemaphore(const TYtSettings::TConstPtr& settings) {
    with_lock(Mutex_) {
        if(!LocalCalcSemaphore_) {
            const size_t localCalcLimit = settings->LocalCalcLimit.Get().GetOrElse(1U);
            LocalCalcSemaphore_ = MakeHolder<TFastSemaphore>(localCalcLimit);
        }
    }
}

} // NNative

} // NYql

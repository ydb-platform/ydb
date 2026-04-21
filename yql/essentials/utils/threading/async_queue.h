#pragma once

#include <library/cpp/threading/future/future.h>
#include <library/cpp/threading/future/async.h>

#include <util/thread/pool.h>
#include <util/generic/ptr.h>
#include <util/generic/function.h>
#include <util/system/guard.h>
#include <util/generic/yexception.h>

#include <exception>
#include <memory>

namespace NYql {

class TAsyncQueue: public std::enable_shared_from_this<TAsyncQueue> {
public:
    using TWeakPtr = std::weak_ptr<TAsyncQueue>;
    using TPtr = std::shared_ptr<TAsyncQueue>;

    static TPtr Make(size_t numThreads, const TString& poolName);

    virtual ~TAsyncQueue() {
        MtpQueue_->Stop();
        MtpQueue_.Destroy();
    }

    template <typename TCallable>
    [[nodiscard]]
    static ::NThreading::TFuture<::NThreading::TFutureType<::TFunctionResult<TCallable>>> Async(const TWeakPtr& pool, TCallable&& func) {
        if (auto queue = pool.lock()) {
            return ::NThreading::Async(std::forward<TCallable>(func), *queue->MtpQueue_);
        }

        return ::NThreading::MakeErrorFuture<::NThreading::TFutureType<::TFunctionResult<TCallable>>>(std::make_exception_ptr(yexception() << "Thread pool is already stopped"));
    }

private:
    TAsyncQueue(size_t numThreads, const TString& poolName);

    THolder<IThreadPool> MtpQueue_;
};

} // namespace NYql

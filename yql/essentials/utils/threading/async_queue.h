#pragma once

#include <library/cpp/threading/future/future.h>
#include <library/cpp/threading/future/async.h>

#include <util/thread/pool.h>
#include <util/generic/ptr.h>
#include <util/generic/function.h>
#include <util/system/guard.h>
#include <util/system/rwlock.h>

#include <exception>

namespace NYql {

class TAsyncQueue: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TAsyncQueue>;

    static TPtr Make(size_t numThreads, const TString& poolName);

    void Stop() {
        auto guard = TWriteGuard(Lock_);
        if (MtpQueue_) {
            MtpQueue_->Stop();
            MtpQueue_.Destroy();
        }
    }

    template <typename TCallable>
    [[nodiscard]]
    ::NThreading::TFuture<::NThreading::TFutureType<::TFunctionResult<TCallable>>> Async(TCallable&& func) {
        {
            auto guard = TReadGuard(Lock_);
            if (MtpQueue_) {
                return ::NThreading::Async(std::move(func), *MtpQueue_);
            }
        }

        return ::NThreading::MakeErrorFuture<::NThreading::TFutureType<::TFunctionResult<TCallable>>>(std::make_exception_ptr(yexception() << "Thread pool is already stopped"));
    }

private:
    TAsyncQueue(size_t numThreads, const TString& poolName);

private:
    TRWMutex Lock_;
    THolder<IThreadPool> MtpQueue_;
};

} // NYql

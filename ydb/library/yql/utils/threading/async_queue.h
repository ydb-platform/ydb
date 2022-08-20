#pragma once

#include <library/cpp/threading/future/future.h>
#include <library/cpp/threading/future/async.h>

#include <util/thread/pool.h>
#include <util/generic/ptr.h>
#include <util/generic/function.h>


namespace NYql {

class TAsyncQueue: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TAsyncQueue>;

    static TPtr Make(size_t numThreads, const TString& poolName);

    void Stop() {
        MtpQueue_->Stop();
    }

    template <typename TCallable>
    [[nodiscard]]
    ::NThreading::TFuture<::NThreading::TFutureType<::TFunctionResult<TCallable>>> Async(TCallable&& func) {
        return ::NThreading::Async(std::move(func), *MtpQueue_);
    }

private:
    TAsyncQueue(size_t numThreads, const TString& poolName);

private:
    THolder<IThreadPool> MtpQueue_;
};

} // NYql

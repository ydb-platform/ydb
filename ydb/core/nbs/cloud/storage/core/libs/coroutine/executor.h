#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/affinity.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/task_queue.h>

#include <library/cpp/coroutine/engine/events.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>
#include <util/system/sanitizers.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

class TExecutor final
    : public ITaskQueue
    , public std::enable_shared_from_this<TExecutor>
{
private:
    struct TThread;
    std::unique_ptr<TThread> Thread;

public:
    static const size_t DefaultStackSize = (NSan::ASanIsOn() ? 128 : 32) * 1024;

    static TExecutorPtr Create(
        TString name,
        TAffinity affinity = {},
        size_t contStackSize = DefaultStackSize);

    ~TExecutor();

    void Start() override;
    void Stop() override;

    void Enqueue(ITaskPtr task) override;
    TContExecutor* GetContExecutor();

    template <typename T>
    const T& WaitFor(const NThreading::TFuture<T>& future)
    {
        struct TRequestCancelled: T
        {
            TRequestCancelled()
                : T(static_cast<T>(TErrorResponse(E_REJECTED, "request cancelled")))
            {}
        };

        if (!future.HasValue() && !WaitForI(future)) {
            return Default<TRequestCancelled>();
        }
        return future.GetValue();
    }

    template <typename T>
    void WaitFor(NThreading::TFuture<T>&& future) = delete;

    void WaitFor(NThreading::TFuture<void> future)
    {
        if (!future.HasValue()) {
            WaitForI(future);
        }
    }

    template <typename T>
    T ExtractResponse(NThreading::TFuture<T> future)
    {
        if (!future.HasValue() && !WaitForI(future)) {
            return TErrorResponse(E_REJECTED, "request cancelled");
        }
        return NYdb::NBS::ExtractResponse(future);
    }

    template <typename T>
    TResultOrError<T> ResultOrError(NThreading::TFuture<T> future)
    {
        if (!future.HasValue() && !WaitForI(future)) {
            return TErrorResponse(E_REJECTED, "request cancelled");
        }
        return NYdb::NBS::ResultOrError(future);
    }

private:
    TExecutor(
        TString name,
        TAffinity affinity = {},
        size_t contStackSize = DefaultStackSize);

    template <typename T>
    bool WaitForI(const NThreading::TFuture<T>& future)
    {
        auto* executor = GetContExecutor();
        TContSimpleEvent event(executor);

        auto weakPtr = weak_from_this();

        future.Subscribe([&, weakPtr = std::move(weakPtr)] (const auto&) {
            if (auto ptr = weakPtr.lock()) {
                // switch to executor thread
                ptr->ExecuteSimple([&] { event.Signal(); });
            }
        });

        int result = event.WaitI();
        return (result != ECANCELED);
    }
};

}   // namespace NYdb::NBS

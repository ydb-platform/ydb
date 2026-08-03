#include "durable_wrapper.h"

#include "context.h"
#include "storage.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range_map.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/backoff_delay_provider.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/scheduler.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/timer.h>

#include <util/string/builder.h>

#include <memory>

namespace NYdb::NBS::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr auto InitialDelay = TDuration::MilliSeconds(100);
constexpr auto MaxDelay = TDuration::Seconds(10);

////////////////////////////////////////////////////////////////////////////////

bool HasNeverRetriableErrors(const NProto::TError& error)
{
    constexpr EWellKnownResultCodes NeverRetriableErrors[] = {
        E_CANCELLED,   // Request is canceled,
                       // no point in retrying
        E_ARGUMENT,    // Request is ill-formed,
                       // no point in retrying
        E_IO_SILENT,   // Unrecoverable error, that must be
                       // passed up to the client
        E_IO,          // Unrecoverable error, that must be
                       // passed up to the client
    };

    for (auto code: NeverRetriableErrors) {
        if (code == error.GetCode()) {
            return true;
        }
    }

    return false;
}

////////////////////////////////////////////////////////////////////////////////

template <typename TRequest, typename TResponse>
class TDurable
    : public std::enable_shared_from_this<TDurable<TRequest, TResponse>>
{
public:
    TDurable(IStoragePtr storage, ITimerPtr timer, ISchedulerPtr scheduler)
        : Storage(std::move(storage))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
    {}

    NThreading::TFuture<TResponse> Execute(
        TCallContextPtr callContext,
        std::shared_ptr<TRequest> request)
    {
        Lock.Acquire();
        const ui64 requestId = ++RequestIdGenerator;

        auto [it, inserted] = Inflights.emplace(
            requestId,
            TInflight{.CallContex = callContext, .Request = request});
        Y_ABORT_UNLESS(inserted);
        auto result = it->second.Promise.GetFuture();
        Lock.Release();

        DoExecute(requestId, std::move(callContext), std::move(request));
        return result;
    }

private:
    struct TInflight
    {
        TCallContextPtr CallContex;
        std::shared_ptr<TRequest> Request;
        NThreading::TPromise<TResponse> Promise =
            NThreading::NewPromise<TResponse>();
        TBackoffDelayProvider BackoffDelay{InitialDelay, MaxDelay};
        size_t RetryCount = 0;
    };

    void DoExecute(
        ui64 requestId,
        TCallContextPtr callContext,
        std::shared_ptr<TRequest> request)
    {
        TStorageAdapter::Execute(
            Storage.get(),
            std::move(callContext),
            std::move(request))
            .Subscribe(
                [requestId, weakSelf = this->weak_from_this()]   //
                (const NThreading::TFuture<TResponse>& f)
                {
                    if (auto self = weakSelf.lock()) {
                        self->OnResponse(requestId, UnsafeExtractValue(f));
                    } else {
                        // TODO(drbasic). Durable client is destroyed
                    }
                });
    }

    void OnResponse(ui64 requestId, TResponse response)
    {
        const bool shouldReply = !HasError(response.Error) ||
                                 HasNeverRetriableErrors(response.Error);

        NThreading::TPromise<TResponse> promise;
        TDuration delay;

        {
            auto guard = Guard(Lock);
            auto it = Inflights.find(requestId);
            if (it == Inflights.end()) {
                // Belated response.
                return;
            }
            auto& request = it->second;
            if (shouldReply) {
                promise = std::move(request.Promise);
                Inflights.erase(it);
            } else {
                delay = request.BackoffDelay.GetDelayAndIncrease();
            }
        }

        if (shouldReply) {
            promise.SetValue(std::move(response));
        } else {
            Scheduler->Schedule(
                Timer->Now() + delay,
                [requestId, weakSelf = this->weak_from_this()]()
                {
                    if (auto self = weakSelf.lock()) {
                        self->RetryRequest(requestId);
                    }
                });
        }
    }

    void RetryRequest(ui64 requestId)
    {
        TCallContextPtr callContext;
        std::shared_ptr<TRequest> request;

        {
            auto guard = Guard(Lock);
            auto it = Inflights.find(requestId);
            if (it == Inflights.end()) {
                // Belated retry.
                return;
            }
            auto& r = it->second;
            ++r.RetryCount;
            callContext = r.CallContex;
            request = r.Request;
        }

        DoExecute(requestId, std::move(callContext), std::move(request));
    }

    const IStoragePtr Storage;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;

    TAdaptiveLock Lock;
    ui64 RequestIdGenerator = 0;

    THashMap<ui64, TInflight> Inflights{};
};

using TDurableRead =
    TDurable<TReadBlocksLocalRequest, TReadBlocksLocalResponse>;
using TDurableWrite =
    TDurable<TWriteBlocksLocalRequest, TWriteBlocksLocalResponse>;
using TDurableZero =
    TDurable<TZeroBlocksLocalRequest, TZeroBlocksLocalResponse>;

////////////////////////////////////////////////////////////////////////////////

class TDurableStorageWrapper final
    : public IStorage
    , public std::enable_shared_from_this<TDurableStorageWrapper>
{
private:
    const IStoragePtr Storage;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;

    std::shared_ptr<TDurableRead> DurableReads{
        std::make_shared<TDurableRead>(Storage, Timer, Scheduler)};
    std::shared_ptr<TDurableWrite> DurableWrites{
        std::make_shared<TDurableWrite>(Storage, Timer, Scheduler)};
    std::shared_ptr<TDurableZero> DurableZeroes{
        std::make_shared<TDurableZero>(Storage, Timer, Scheduler)};

public:
    TDurableStorageWrapper(
        IStoragePtr storage,
        ITimerPtr timer,
        ISchedulerPtr scheduler);
    ~TDurableStorageWrapper() override;

    // implements IStorage
    NThreading::TFuture<TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request) override;

    NThreading::TFuture<TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TWriteBlocksLocalRequest> request) override;

    NThreading::TFuture<TZeroBlocksLocalResponse> ZeroBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TZeroBlocksLocalRequest> request) override;

    void ReportIOError() override;
};

////////////////////////////////////////////////////////////////////////////////

TDurableStorageWrapper::TDurableStorageWrapper(
    IStoragePtr storage,
    ITimerPtr timer,
    ISchedulerPtr scheduler)
    : Storage(std::move(storage))
    , Timer(std::move(timer))
    , Scheduler(std::move(scheduler))
{}

TDurableStorageWrapper::~TDurableStorageWrapper() = default;

NThreading::TFuture<TReadBlocksLocalResponse>
TDurableStorageWrapper::ReadBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request)
{
    return DurableReads->Execute(std::move(callContext), std::move(request));
}

NThreading::TFuture<TWriteBlocksLocalResponse>
TDurableStorageWrapper::WriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request)
{
    return DurableWrites->Execute(std::move(callContext), std::move(request));
}

NThreading::TFuture<TZeroBlocksLocalResponse>
TDurableStorageWrapper::ZeroBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TZeroBlocksLocalRequest> request)
{
    return DurableZeroes->Execute(std::move(callContext), std::move(request));
}

void TDurableStorageWrapper::ReportIOError()
{
    Storage->ReportIOError();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStoragePtr CreateDurableStorageWrapper(
    IStoragePtr storage,
    ITimerPtr timer,
    ISchedulerPtr scheduler)
{
    return std::make_shared<TDurableStorageWrapper>(
        std::move(storage),
        std::move(timer),
        std::move(scheduler));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore

#include "durable_wrapper.h"

#include "context.h"
#include "storage.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range_map.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/backoff_delay_provider.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/format.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/scheduler.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/timer.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/logging.h>

#include <util/string/builder.h>

#include <memory>

namespace NYdb::NBS::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr auto InitialDelay = TDuration::MilliSeconds(100);
constexpr auto MaxDelay = TDuration::Seconds(10);

////////////////////////////////////////////////////////////////////////////////

template <typename TRequest, typename TResponse>
class TDurable
{
public:
    using TSelf = TDurable<TRequest, TResponse>;

    TDurable(
        TLog log,
        TString requestName,
        IStoragePtr storage,
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        ui32 generation)
        : RequestName(std::move(requestName))
        , Storage(std::move(storage))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Log(std::move(log))
        , Generation(generation)
    {}

    virtual ~TDurable() = default;

    NThreading::TFuture<TResponse> Execute(
        TCallContextPtr callContext,
        std::shared_ptr<TRequest> request)
    {
        Lock.Acquire();
        const ui64 requestId = ++RequestIdGenerator;
        const ui32 generation = Generation;

        auto [it, inserted] = InflightRequests.emplace(
            requestId,
            TInflight{.CallContext = callContext, .Request = request});
        Y_ABORT_UNLESS(inserted);
        auto result = it->second.Promise.GetFuture();
        Lock.Release();

        DoExecute(
            requestId,
            generation,
            std::move(callContext),
            std::move(request));
        return result;
    }

    void RestartRequests(ui32 generation)
    {
        TVector<ui64> requestsToRetry;
        {   // Getting necessary data from the shared state under lock.
            auto guard = Guard(Lock);
            requestsToRetry.reserve(InflightRequests.size());
            for (auto& [requestId, inflight]: InflightRequests) {
                requestsToRetry.emplace_back(requestId);
            }
            Generation = generation;
        }

        for (auto requestId: requestsToRetry) {
            RetryRequest(requestId);
        }
    }

    virtual std::weak_ptr<TSelf> GetWeakPtr(const TSelf& self) = 0;

private:
    struct TInflight
    {
        TCallContextPtr CallContext;
        std::shared_ptr<TRequest> Request;
        NThreading::TPromise<TResponse> Promise =
            NThreading::NewPromise<TResponse>();
        TBackoffDelayProvider BackoffDelay{InitialDelay, MaxDelay};
        size_t RetryCount = 0;
    };

    void DoExecute(
        ui64 requestId,
        ui32 generation,
        TCallContextPtr callContext,
        std::shared_ptr<TRequest> request)
    {
        TStorageAdapter::Execute(
            Storage.get(),
            std::move(callContext),
            std::move(request))
            .Subscribe(
                [requestId, generation, weakSelf = GetWeakPtr(*this)]   //
                (const NThreading::TFuture<TResponse>& f)
                {
                    if (auto self = weakSelf.lock()) {
                        self->OnResponse(
                            requestId,
                            generation,
                            UnsafeExtractValue(f));
                    } else {
                        // TODO(drbasic). Durable client is destroyed
                    }
                });
    }

    void OnResponse(ui64 requestId, ui32 generation, TResponse response)
    {
        const bool shouldReply =
            !HasError(response.Error) || IsNeverRetriableError(response.Error);

        NThreading::TPromise<TResponse> promise;
        TDuration delay;
        size_t retryCount = 0;

        {   // Getting necessary data from the shared state under lock.
            auto guard = Guard(Lock);
            auto it = InflightRequests.find(requestId);
            if (it == InflightRequests.end()) {
                // Belated response.
                return;
            }

            auto& request = it->second;
            if (generation != Generation) {
                // Received response from outdated generation.
                return;
            }

            retryCount = request.RetryCount;
            if (shouldReply) {
                promise = std::move(request.Promise);
                InflightRequests.erase(it);
            } else {
                delay = request.BackoffDelay.GetDelayAndIncrease();
            }
        }

        if (shouldReply) {
            STORAGE_LOG(
                HasError(response.Error)
                    ? TLOG_CRIT
                    : (retryCount == 0 ? TLOG_DEBUG : TLOG_INFO),
                "[%lu] %s request completed on retry #%lu gen: %lu with %s",
                requestId,
                RequestName.c_str(),
                retryCount + 1,
                static_cast<size_t>(generation),
                FormatError(response.Error).Quote().c_str());

            promise.SetValue(std::move(response));
        } else {
            STORAGE_WARN(
                "[%lu] %s request failed with a retriable error %s, "
                "scheduling retry #%lu gen: %lu in %s",
                requestId,
                RequestName.c_str(),
                FormatError(response.Error).Quote().c_str(),
                retryCount + 1,
                static_cast<size_t>(generation),
                FormatDuration(delay).c_str());

            Scheduler->Schedule(
                Timer->Now() + delay,
                [requestId, weakSelf = GetWeakPtr(*this)]()
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
        size_t retryCount = 0;
        ui32 generation = 0;

        {   // Getting necessary data from the shared state under lock.
            auto guard = Guard(Lock);
            auto it = InflightRequests.find(requestId);
            if (it == InflightRequests.end()) {
                // Belated retry.
                return;
            }
            auto& r = it->second;

            ++r.RetryCount;
            retryCount = r.RetryCount;
            callContext = r.CallContext;
            request = r.Request;
            generation = Generation;
        }

        STORAGE_DEBUG(
            "[%lu] retrying %s request (attempt #%lu) gen: %lu",
            requestId,
            RequestName.c_str(),
            retryCount,
            static_cast<size_t>(generation));

        DoExecute(
            requestId,
            generation,
            std::move(callContext),
            std::move(request));
    }

    const TString RequestName;
    const IStoragePtr Storage;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;

    TLog Log;
    TAdaptiveLock Lock;
    ui64 RequestIdGenerator = 0;
    ui32 Generation = 0;
    THashMap<ui64, TInflight> InflightRequests{};
};

using TDurableRead =
    TDurable<TReadBlocksLocalRequest, TReadBlocksLocalResponse>;
using TDurableWrite =
    TDurable<TWriteBlocksLocalRequest, TWriteBlocksLocalResponse>;
using TDurableZero =
    TDurable<TZeroBlocksLocalRequest, TZeroBlocksLocalResponse>;

////////////////////////////////////////////////////////////////////////////////

class TDurableStorageWrapper final
    : public IDurableStorage
    , public TDurableRead
    , public TDurableWrite
    , public TDurableZero
    , public std::enable_shared_from_this<TDurableStorageWrapper>
{
public:
    TDurableStorageWrapper(
        const TLog& log,
        IStoragePtr storage,
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        ui32 generation);
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

    // implements IDurableStorage
    void RestartRequests(ui32 generation) override;

    // implements TDurable
    std::weak_ptr<TDurableRead> GetWeakPtr(const TDurableRead& tag) override;
    std::weak_ptr<TDurableWrite> GetWeakPtr(const TDurableWrite& tag) override;
    std::weak_ptr<TDurableZero> GetWeakPtr(const TDurableZero& tag) override;

private:
    const TLog Log;
    const IStoragePtr Storage;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
};

////////////////////////////////////////////////////////////////////////////////

TDurableStorageWrapper::TDurableStorageWrapper(
    const TLog& log,
    IStoragePtr storage,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ui32 generation)
    : TDurableRead(log, "Read", storage, timer, scheduler, generation)
    , TDurableWrite(log, "Write", storage, timer, scheduler, generation)
    , TDurableZero(log, "Zero", storage, timer, scheduler, generation)
    , Log(log)
    , Storage(std::move(storage))
    , Timer(std::move(timer))
    , Scheduler(std::move(scheduler))
{}

TDurableStorageWrapper::~TDurableStorageWrapper() = default;

NThreading::TFuture<TReadBlocksLocalResponse>
TDurableStorageWrapper::ReadBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request)
{
    return static_cast<TDurableRead*>(this)->Execute(
        std::move(callContext),
        std::move(request));
}

NThreading::TFuture<TWriteBlocksLocalResponse>
TDurableStorageWrapper::WriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request)
{
    return static_cast<TDurableWrite*>(this)->Execute(
        std::move(callContext),
        std::move(request));
}

NThreading::TFuture<TZeroBlocksLocalResponse>
TDurableStorageWrapper::ZeroBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TZeroBlocksLocalRequest> request)
{
    return static_cast<TDurableZero*>(this)->Execute(
        std::move(callContext),
        std::move(request));
}

void TDurableStorageWrapper::ReportIOError()
{
    Storage->ReportIOError();
}

void TDurableStorageWrapper::RestartRequests(ui32 generation)
{
    TDurableRead::RestartRequests(generation);
    TDurableWrite::RestartRequests(generation);
    TDurableZero::RestartRequests(generation);
}

std::weak_ptr<TDurableRead> TDurableStorageWrapper::GetWeakPtr(
    const TDurableRead& tag)
{
    Y_UNUSED(tag);
    return weak_from_this();
}

std::weak_ptr<TDurableWrite> TDurableStorageWrapper::GetWeakPtr(
    const TDurableWrite& tag)
{
    Y_UNUSED(tag);
    return weak_from_this();
}

std::weak_ptr<TDurableZero> TDurableStorageWrapper::GetWeakPtr(
    const TDurableZero& tag)
{
    Y_UNUSED(tag);
    return weak_from_this();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IDurableStoragePtr CreateDurableStorageWrapper(
    ILoggingServicePtr logging,
    IStoragePtr storage,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ui32 generation)
{
    return std::make_shared<TDurableStorageWrapper>(
        logging->CreateLog("BLOCKSTORE_DURABLE"),
        std::move(storage),
        std::move(timer),
        std::move(scheduler),
        generation);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore

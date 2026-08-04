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
{
public:
    using TSelf = TDurable<TRequest, TResponse>;

    TDurable(
        TLog log,
        TString requestName,
        IStoragePtr storage,
        ITimerPtr timer,
        ISchedulerPtr scheduler)
        : RequestName(std::move(requestName))
        , Storage(std::move(storage))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Log(std::move(log))
    {}

    virtual ~TDurable() = default;

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

    virtual std::weak_ptr<TSelf> GetWeakPtr(const TSelf& self) = 0;

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
                [requestId, weakSelf = GetWeakPtr(*this)]   //
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
        size_t retryCount = 0;

        {   // Getting necessary data from the shared state under lock.
            auto guard = Guard(Lock);
            auto it = Inflights.find(requestId);
            if (it == Inflights.end()) {
                // Belated response.
                return;
            }

            auto& request = it->second;
            retryCount = request.RetryCount;
            if (shouldReply) {
                promise = std::move(request.Promise);
                Inflights.erase(it);
            } else {
                delay = request.BackoffDelay.GetDelayAndIncrease();
            }
        }

        if (shouldReply) {
            STORAGE_LOG(
                retryCount == 0 ? TLOG_DEBUG : TLOG_INFO,
                "[%lu] %s request copleted on retry #%lu with %s",
                requestId,
                RequestName.c_str(),
                retryCount + 1,
                FormatError(response.Error).c_str());

            promise.SetValue(std::move(response));
        } else {
            STORAGE_WARN(
                "[%lu] %s request failed with a retriable error %s, "
                "scheduling retry #%lu in %s",
                requestId,
                RequestName.c_str(),
                FormatError(response.Error).c_str(),
                retryCount + 1,
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

        {   // Getting necessary data from the shared state under lock.
            auto guard = Guard(Lock);
            auto it = Inflights.find(requestId);
            if (it == Inflights.end()) {
                // Belated retry.
                return;
            }
            auto& r = it->second;

            ++r.RetryCount;
            retryCount = r.RetryCount;
            callContext = r.CallContex;
            request = r.Request;
        }

        STORAGE_DEBUG(
            "[%lu] retrying %s request (attempt #%lu)",
            requestId,
            RequestName.c_str(),
            retryCount);

        DoExecute(requestId, std::move(callContext), std::move(request));
    }

    const TString RequestName;
    const IStoragePtr Storage;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;

    TLog Log;
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
    , public TDurableRead
    , public TDurableWrite
    , public TDurableZero
    , public std::enable_shared_from_this<TDurableStorageWrapper>
{
private:
    const TLog Log;
    const IStoragePtr Storage;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;

public:
    TDurableStorageWrapper(
        const TLog& log,
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

    // implements TDurable
    std::weak_ptr<TDurableRead> GetWeakPtr(const TDurableRead& tag) override;
    std::weak_ptr<TDurableWrite> GetWeakPtr(const TDurableWrite& tag) override;
    std::weak_ptr<TDurableZero> GetWeakPtr(const TDurableZero& tag) override;
};

////////////////////////////////////////////////////////////////////////////////

TDurableStorageWrapper::TDurableStorageWrapper(
    const TLog& log,
    IStoragePtr storage,
    ITimerPtr timer,
    ISchedulerPtr scheduler)
    : TDurableRead(log, "Read", storage, timer, scheduler)
    , TDurableWrite(log, "Write", storage, timer, scheduler)
    , TDurableZero(log, "Zero", storage, timer, scheduler)
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

IStoragePtr CreateDurableStorageWrapper(
    ILoggingServicePtr logging,
    IStoragePtr storage,
    ITimerPtr timer,
    ISchedulerPtr scheduler)
{
    return std::make_shared<TDurableStorageWrapper>(
        logging->CreateLog("BLOCKSTORE_DURABLE"),
        std::move(storage),
        std::move(timer),
        std::move(scheduler));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore

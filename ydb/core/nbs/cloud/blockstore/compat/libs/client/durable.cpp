#include "durable.h"

#include "config.h"

#include <ydb/core/nbs/cloud/blockstore/compat/libs/diagnostics/request_stats.h>
#include <ydb/core/nbs/cloud/blockstore/compat/libs/diagnostics/volume_stats.h>
#include <ydb/core/nbs/cloud/blockstore/compat/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/compat/libs/service/request_helpers.h>
#include <ydb/core/nbs/cloud/blockstore/compat/libs/service/service.h>
#include <ydb/core/nbs/cloud/blockstore/compat/libs/service/service_method.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/probes.h>

#include <ydb/core/nbs/cloud/storage/core/compat/libs/common/media.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/format.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/scheduler.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/timer.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/logging.h>

#include <util/stream/format.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NClient {

using namespace NThreading;

using NYdb::NBS::E_ARGUMENT;
using NYdb::NBS::E_BS_INVALID_SESSION;
using NYdb::NBS::E_BS_MOUNT_CONFLICT;
using NYdb::NBS::E_CANCELLED;
using NYdb::NBS::E_IO;
using NYdb::NBS::E_IO_SILENT;
using NYdb::NBS::E_REJECTED;
using NYdb::NBS::E_RETRY_TIMEOUT;
using NYdb::NBS::E_TIMEOUT;
using NYdb::NBS::EDiagnosticsErrorKind;
using NYdb::NBS::EErrorKind;
using NYdb::NBS::EWellKnownResultCodes;
using NYdb::NBS::ExtractResponse;
using NYdb::NBS::FormatDuration;
using NYdb::NBS::FormatError;
using NYdb::NBS::GetDiagnosticsErrorKind;
using NYdb::NBS::GetErrorKind;
using NYdb::NBS::HasError;
using NYdb::NBS::HasProtoFlag;
using NYdb::NBS::IsConnectionError;
using NYdb::NBS::TErrorResponse;
using NYdb::NBS::TGuardedSgList;

LWTRACE_USING(BLOCKSTORE_SERVER_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

// List of errors that must never be retried and semantically have
// no point in retrying
// Note: changes is this list must me reflected in
//     cloud/blockstore/libs/client/durable_ut.cpp:
//         ShouldUseNonRetriableListWhenEnabledAndNotRetryNeverRetriable
const TVector<EWellKnownResultCodes> NeverRetriableErrors = {
    E_BS_INVALID_SESSION,   // This error must never be retried as
                            // not passing in up may break
                            // the remounting logic
    E_CANCELLED,            // Request is canceled,
                            // no point in retrying
    E_ARGUMENT,             // Request is ill-formed,
                            // no point in retrying
    E_IO_SILENT,            // Unrecoverable error, that must be
                            // passed up to the client
    E_IO,                   // Unrecoverable error, that must be
                            // passed up to the client
    E_BS_MOUNT_CONFLICT,    // Must not be retriable, as it may break
                            // mounting/remounting logic
};

////////////////////////////////////////////////////////////////////////////////

ELogPriority GetDetailsLogPriority(const NProto::TError& error)
{
    const auto kind = GetDiagnosticsErrorKind(error);

    switch (kind) {
        case EDiagnosticsErrorKind::ErrorAborted:
        case EDiagnosticsErrorKind::ErrorFatal:
        case EDiagnosticsErrorKind::ErrorRetriable:
        case EDiagnosticsErrorKind::ErrorSession:
        case EDiagnosticsErrorKind::ErrorSilent:
            return TLOG_INFO;
        default:
            return TLOG_DEBUG;
    }
}

ELogPriority GetNoRetryLogPriority(const NProto::TError& error)
{
    return error.GetCode() == E_IO_SILENT ? TLOG_WARNING : TLOG_ERR;
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
struct TRequestStateBase: public TRetryState
{
    TCallContextPtr CallContext;
    std::shared_ptr<typename T::TRequest> Request;
    TPromise<typename T::TResponse> Response;
    ELogPriority DetailsLogPriority = TLOG_DEBUG;

    TRequestStateBase(
        TCallContextPtr callContext,
        std::shared_ptr<typename T::TRequest> request)
        : CallContext(std::move(callContext))
        , Request(std::move(request))
        , Response(NewPromise<typename T::TResponse>())
    {}
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
struct TRequestState
    : public TRequestStateBase<T>
    , public TAtomicRefCount<TRequestState<T>>
{
    using TRequestStateBase<T>::TRequestStateBase;
};

template <>
struct TRequestState<TBlockStoreReadBlocksLocalMethod>
    : public TRequestStateBase<TBlockStoreReadBlocksLocalMethod>
    , public TAtomicRefCount<TRequestState<TBlockStoreReadBlocksLocalMethod>>
{
    TGuardedSgList SentSgList;

    using TRequestStateBase<
        TBlockStoreReadBlocksLocalMethod>::TRequestStateBase;
};

template <typename T>
using TRequestStatePtr = TIntrusivePtr<TRequestState<T>>;

////////////////////////////////////////////////////////////////////////////////

class TDurableClient final
    : public TBlockStoreImpl<TDurableClient, IBlockStore>
    , public std::enable_shared_from_this<TDurableClient>
{
protected:
    const TClientAppConfigPtr Config;
    const IBlockStorePtr Client;
    const IRetryPolicyPtr RetryPolicy;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const IRequestStatsPtr RequestStats;
    const IVolumeStatsPtr VolumeStats;

    TLog Log;

public:
    TDurableClient(
        TClientAppConfigPtr config,
        IBlockStorePtr client,
        IRetryPolicyPtr retryPolicy,
        ILoggingServicePtr logging,
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        IRequestStatsPtr requestStats,
        IVolumeStatsPtr volumeStats)
        : Config(std::move(config))
        , Client(std::move(client))
        , RetryPolicy(std::move(retryPolicy))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , RequestStats(std::move(requestStats))
        , VolumeStats(std::move(volumeStats))
        , Log(logging->CreateLog("BLOCKSTORE_CLIENT"))
    {}

    void Start() override
    {
        Client->Start();
    }

    void Stop() override
    {
        Client->Stop();
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Client->AllocateBuffer(bytesCount);
    }

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        auto state = MakeIntrusive<TRequestState<TMethod>>(
            std::move(callContext),
            std::move(request));

        ExecuteRequest(state);
        return state->Response;
    }

private:
    template <typename TMethod>
    void ExecuteRequest(TRequestStatePtr<TMethod> state)
    {
        auto request = CreateRequestToSend(*state);
        if (!request) {
            Y_DEBUG_ABORT_UNLESS(state->Response.HasValue());
            return;
        }

        EnsureRequestId(*request);

        TMethod::Execute(Client.get(), state->CallContext, std::move(request))
            .Subscribe(
                [state = std::move(state), weakSelf = this->weak_from_this()](
                    TFuture<typename TMethod::TResponse> future) mutable
                {
                    if (auto p = weakSelf.lock()) {
                        p->HandleResponse(
                            std::move(state),
                            ExtractResponse(future));
                    } else {
                        state->Response.SetValue(TErrorResponse(
                            E_REJECTED,
                            "Durable client is destroyed"));
                    }
                });
    }

    template <typename T>
    void IncrementRequestTimeout(T& request)
    {
        auto& headers = *request.MutableHeaders();
        const auto timeout =
            TDuration::MilliSeconds(headers.GetRequestTimeout());
        const auto newTimeout = Min<TDuration>(
            timeout + Config->GetRequestTimeoutIncrementOnRetry(),
            Config->GetRequestTimeoutMax());
        headers.SetRequestTimeout(newTimeout.MilliSeconds());
    }

    template <typename T>
    void IncrementRetryNumber(T& request)
    {
        auto& headers = *request.MutableHeaders();
        headers.SetRetryNumber(headers.GetRetryNumber() + 1);
    }

    template <typename TMethod>
    void HandleResponse(
        TRequestStatePtr<TMethod> state,
        typename TMethod::TResponse response)
    {
        const ui64 requestId = GetRequestId(*state->Request);
        const auto& diskId = GetDiskId(*state->Request);
        const auto& clientId = GetClientId(*state->Request);

        if (HasError(response)) {
            const auto& error = response.GetError();
            const auto retrySpec = RetryPolicy->ShouldRetry(*state, error);
            state->DetailsLogPriority =
                Min(GetDetailsLogPriority(error), state->DetailsLogPriority);
            if (retrySpec.ShouldRetry) {
                // retry request
                ++state->Retries;

                // TODO: adapt RequestRetry trace event to NBS2/Wilson tracing.
                // LWTRACK(
                //     RequestRetry,
                //     state->CallContext->LWOrbit,
                //     TMethod::Name,
                //     requestId,
                //     diskId,
                //     state->Retries,
                //     retrySpec.Backoff.MilliSeconds(),
                //     error.GetCode());

                const auto errorKind = GetDiagnosticsErrorKind(error);
                const auto errorFlags = error.GetFlags();
                const bool throttling =
                    (errorKind == EDiagnosticsErrorKind::ErrorThrottling);

                bool doLogging = true;
                switch (errorKind) {
                    case EDiagnosticsErrorKind::ErrorThrottling: {
                        doLogging = false;
                        state->CallContext->SetHasUncountableRejects();
                        break;
                    }
                    case EDiagnosticsErrorKind::
                        ErrorWriteRejectedByCheckpoint: {
                        // Do not flood in the log. One message in the log is
                        // enough.
                        doLogging = state->Retries == 1;
                        state->CallContext->SetHasUncountableRejects();
                        break;
                    }
                    default:
                        break;
                }

                IncrementRetryNumber(*state->Request);
                if (error.GetCode() == E_TIMEOUT) {
                    IncrementRequestTimeout(*state->Request);
                }

                if (doLogging) {
                    STORAGE_WARN(
                        TRequestInfo(
                            TMethod::BlockStoreRequest,
                            requestId,
                            diskId,
                            clientId,
                            Config->GetInstanceId())
                        << GetRequestDetails(*state->Request)
                        << " retry request"
                        << " (retries: " << state->Retries
                        << ", timeout: " << FormatDuration(retrySpec.Backoff)
                        << ", error: " << FormatError(error) << ")");
                }

                auto volumeInfo = VolumeStats->GetVolumeInfo(diskId, clientId);
                if (volumeInfo) {
                    volumeInfo->AddRetryStats(
                        TMethod::BlockStoreRequest,
                        errorKind,
                        errorFlags);
                }

                RequestStats->AddRetryStats(
                    VolumeStats->GetStorageMediaKind(diskId),
                    TMethod::BlockStoreRequest,
                    errorKind,
                    errorFlags);

                auto postponeCycles = GetCycleCount();
                if (throttling) {
                    state->CallContext->Postpone(postponeCycles);
                }

                Scheduler->Schedule(
                    Timer->Now() + retrySpec.Backoff,
                    [state = std::move(state),
                     postponeCycles,
                     throttling,
                     weakSelf = this->weak_from_this()]
                    {
                        auto nowCycles = GetCycleCount();
                        if (throttling) {
                            state->CallContext->Advance(nowCycles);
                        } else {
                            state->CallContext->AddTime(
                                EProcessingStage::Backoff,
                                CyclesToDurationSafe(
                                    nowCycles - postponeCycles));
                        }

                        if (auto p = weakSelf.lock()) {
                            p->ExecuteRequest(state);
                        } else {
                            state->Response.SetValue(TErrorResponse(
                                E_REJECTED,
                                "Durable client is destroyed"));
                        }
                    });
                return;
            }

            if (retrySpec.IsRetriableError) {
                auto& error = *response.MutableError();
                auto errorStr = FormatError(error);
                error.SetCode(E_RETRY_TIMEOUT);
                error.SetMessage(
                    TStringBuilder() << "Retry timeout: " << errorStr);
            }

            auto duration = TInstant::Now() - state->Started;
            STORAGE_LOG(
                GetNoRetryLogPriority(response.GetError()),
                TRequestInfo(
                    TMethod::BlockStoreRequest,
                    requestId,
                    diskId,
                    clientId,
                    Config->GetInstanceId())
                    << GetRequestDetails(*state->Request)
                    << " will not retry error: "
                    << FormatError(response.GetError())
                    << " (retries: " << state->Retries
                    << ", duration: " << FormatDuration(duration) << ")");
        } else {
            // log successful request
            if (state->Retries) {
                auto duration = TInstant::Now() - state->Started;
                STORAGE_LOG(
                    state->DetailsLogPriority,
                    TRequestInfo(
                        TMethod::BlockStoreRequest,
                        requestId,
                        diskId,
                        clientId,
                        Config->GetInstanceId())
                        << GetRequestDetails(*state->Request)
                        << " request completed"
                        << " (retries: " << state->Retries
                        << ", duration: " << FormatDuration(duration) << ")");
            }
        }

        try {
            state->Response.SetValue(std::move(response));
        } catch (...) {
            STORAGE_ERROR(
                TRequestInfo(
                    TMethod::BlockStoreRequest,
                    requestId,
                    diskId,
                    clientId,
                    Config->GetInstanceId())
                << GetRequestDetails(*state->Request)
                << " exception in callback: " << CurrentExceptionMessage());
        }
    }

    template <typename T>
    std::shared_ptr<typename T::TRequest> CreateRequestToSend(
        TRequestState<T>& state)
    {
        // send the same request for non local requests.
        return state.Request;
    }

    template <>
    std::shared_ptr<NProto::TReadBlocksLocalRequest> CreateRequestToSend(
        TRequestState<TBlockStoreReadBlocksLocalMethod>& state)
    {
        const auto& request = state.Request;

        if (!request->Sglist.Acquire()) {
            state.Response.SetValue(TErrorResponse(
                E_CANCELLED,
                "failed to acquire sglist in DurableClient"));
            return nullptr;
        }

        auto copy = std::make_shared<NProto::TReadBlocksLocalRequest>(*request);
        copy->Sglist = request->Sglist.CreateDepender();

        state.SentSgList.Close();
        state.SentSgList = copy->Sglist;

        return copy;
    }

    template <>
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> CreateRequestToSend(
        TRequestState<TBlockStoreWriteBlocksLocalMethod>& state)
    {
        const auto& request = state.Request;

        if (!request->Sglist.Acquire()) {
            state.Response.SetValue(TErrorResponse(
                E_CANCELLED,
                "failed to acquire sglist in DurableClient"));
            return nullptr;
        }

        // copy request without data (only TSgList).
        return std::make_shared<NProto::TWriteBlocksLocalRequest>(
            *request,
            NProto::TWriteBlocksLocalRequest::TDependentTag{});
    }

    template <>
    std::shared_ptr<NProto::TZeroBlocksRequest> CreateRequestToSend(
        TRequestState<TBlockStoreZeroBlocksMethod>& state)
    {
        return std::make_shared<NProto::TZeroBlocksRequest>(*state.Request);
    }

    template <>
    std::shared_ptr<NProto::TMountVolumeRequest> CreateRequestToSend(
        TRequestState<TBlockStoreMountVolumeMethod>& state)
    {
        return std::make_shared<NProto::TMountVolumeRequest>(*state.Request);
    }

    template <>
    std::shared_ptr<NProto::TUnmountVolumeRequest> CreateRequestToSend(
        TRequestState<TBlockStoreUnmountVolumeMethod>& state)
    {
        return std::make_shared<NProto::TUnmountVolumeRequest>(*state.Request);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRetryPolicy final: public IRetryPolicy
{
private:
    TClientAppConfigPtr Config;
    const TDuration InitialRetryTimeout;
    const bool MediaIsReliable;

public:
    TRetryPolicy(
        TClientAppConfigPtr config,
        const TDuration initialRetryTimeout,
        const bool mediaIsReliable)
        : Config(std::move(config))
        , InitialRetryTimeout(initialRetryTimeout)
        , MediaIsReliable(mediaIsReliable)
    {}

    TRetrySpec ShouldRetry(
        TRetryState& state,
        const NProto::TError& error) override
    {
        TRetrySpec spec;

        spec.IsRetriableError =
            GetErrorKind(error) == EErrorKind::ErrorRetriable;
        if (Config->GetEnableListBasedRetryRules()) {
            spec.IsRetriableError =
                spec.IsRetriableError || !IsInNonRetriableList(error);
        }

        if (!spec.IsRetriableError ||
            TInstant::Now() - state.Started >= Config->GetRetryTimeout())
        {
            return spec;
        }

        spec.ShouldRetry = true;
        if (HasProtoFlag(
                error.GetFlags(),
                NYdb::NBS::NProto::EF_INSTANT_RETRIABLE) &&
            !state.DoneInstantRetry)
        {
            spec.Backoff = TDuration::Zero();
            state.DoneInstantRetry = true;
            return spec;
        }

        const auto newRetryTimeout =
            state.Retries > 0
                ? (state.RetryTimeout + Config->GetRetryTimeoutIncrement())
                : InitialRetryTimeout;

        spec.Backoff = newRetryTimeout;
        if (IsConnectionError(error) &&
            spec.Backoff > Config->GetConnectionErrorMaxRetryTimeout())
        {
            spec.Backoff = Config->GetConnectionErrorMaxRetryTimeout();
            return spec;
        }

        state.RetryTimeout = newRetryTimeout;
        return spec;
    }

private:
    bool IsInNonRetriableList(const NProto::TError& error) const
    {
        if (FindPtr(NeverRetriableErrors, error.GetCode())) {
            return true;
        }
        auto nonRetriableErrorsList =
            MediaIsReliable ? Config->GetNonRetriableErrorsForReliableMedia()
                            : Config->GetNonRetriableErrorsForUnreliableMedia();
        return FindPtr(nonRetriableErrorsList, error.GetCode()) != nullptr;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IRetryPolicyPtr CreateRetryPolicy(
    TClientAppConfigPtr config,
    std::optional<NProto::EStorageMediaKind> mediaKind)
{
    TDuration initialRetryTimeout = config->GetRetryTimeoutIncrement();
    if (mediaKind.has_value()) {
        if (IsDiskRegistryMediaKind(*mediaKind)) {
            initialRetryTimeout =
                config->GetDiskRegistryBasedDiskInitialRetryTimeout();
        } else {
            initialRetryTimeout = config->GetYDBBasedDiskInitialRetryTimeout();
        }
    }

    return std::make_shared<TRetryPolicy>(
        std::move(config),
        initialRetryTimeout,
        IsReliableMediaKind(mediaKind.value_or(NProto::STORAGE_MEDIA_DEFAULT)));
}

IBlockStorePtr CreateDurableClient(
    TClientAppConfigPtr config,
    IBlockStorePtr client,
    IRetryPolicyPtr retryPolicy,
    ILoggingServicePtr logging,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    IRequestStatsPtr requestStats,
    IVolumeStatsPtr volumeStats)
{
    return std::make_shared<TDurableClient>(
        std::move(config),
        std::move(client),
        std::move(retryPolicy),
        std::move(logging),
        std::move(timer),
        std::move(scheduler),
        std::move(requestStats),
        std::move(volumeStats));
}

}   // namespace NCloud::NBlockStore::NClient

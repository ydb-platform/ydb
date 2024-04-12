#include "hedging_channel.h"
#include "channel.h"
#include "client.h"
#include "private.h"

#include <yt/yt/core/misc/hedging_manager.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

#include <atomic>

namespace NYT::NRpc {

using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = RpcClientLogger;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(THedgingResponseHandler)
DECLARE_REFCOUNTED_CLASS(THedgingSession)

////////////////////////////////////////////////////////////////////////////////

class THedgingResponseHandler
    : public IClientResponseHandler
{
public:
    THedgingResponseHandler(
        THedgingSessionPtr session,
        bool backup)
        : Session_(std::move(session))
        , Backup_(backup)
    { }

    // IClientResponseHandler implementation.
    void HandleAcknowledgement() override;
    void HandleResponse(TSharedRefArray message, TString address) override;
    void HandleError(TError error) override;
    void HandleStreamingPayload(const TStreamingPayload& /*payload*/) override;
    void HandleStreamingFeedback(const TStreamingFeedback& /*feedback*/) override;

private:
    const THedgingSessionPtr Session_;
    const bool Backup_;
};

DEFINE_REFCOUNTED_TYPE(THedgingResponseHandler)

////////////////////////////////////////////////////////////////////////////////

class THedgingSession
    : public IClientRequestControl
{
public:
    THedgingSession(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& sendOptions,
        IChannelPtr primaryChannel,
        IChannelPtr backupChannel,
        const THedgingChannelOptions& hedgingOptions)
        : Request_(std::move(request))
        , ResponseHandler_(std::move(responseHandler))
        , SendOptions_(sendOptions)
        , PrimaryChannel_(std::move(primaryChannel))
        , BackupChannel_(std::move(backupChannel))
        , HedgingOptions_(hedgingOptions)
    {
        HedgingDelay_ = HedgingOptions_.HedgingManager->OnPrimaryRequestsStarted(/*requestCount*/ 1);

        auto hedgingResponseHandler = New<THedgingResponseHandler>(this, false);

        auto requestControl = PrimaryChannel_->Send(
            Request_,
            std::move(hedgingResponseHandler),
            SendOptions_);

        RegisterSentRequest(std::move(requestControl));

        if (HedgingDelay_ == TDuration::Zero()) {
            OnDeadlineReached(false);
        } else {
            DeadlineCookie_ = TDelayedExecutor::Submit(
                BIND(&THedgingSession::OnDeadlineReached, MakeWeak(this)),
                HedgingDelay_);
        }
    }

    void HandleAcknowledgement(bool backup)
    {
        IClientResponseHandlerPtr responseHandler;
        {
            auto guard = Guard(SpinLock_);
            if (Acknowledged_ || !ResponseHandler_) {
                return;
            }
            Acknowledged_ = true;
            responseHandler = ResponseHandler_;
        }

        YT_LOG_DEBUG_IF(backup, "Request acknowledged by backup (RequestId: %v)",
            Request_->GetRequestId());

        responseHandler->HandleAcknowledgement();
    }

    void HandleResponse(TSharedRefArray message, TString address, bool backup)
    {
        IClientResponseHandlerPtr responseHandler;
        {
            auto guard = Guard(SpinLock_);
            if (Responded_ || !ResponseHandler_) {
                return;
            }
            Responded_ = true;
            std::swap(ResponseHandler_, responseHandler);
            TDelayedExecutor::CancelAndClear(DeadlineCookie_);
        }

        if (backup) {
            YT_LOG_DEBUG("Response received from backup (RequestId: %v)",
                Request_->GetRequestId());

            NRpc::NProto::TResponseHeader header;
            if (!TryParseResponseHeader(message, &header)) {
                ResponseHandler_->HandleError(TError(
                    NRpc::EErrorCode::ProtocolError,
                    "Error parsing response header from backup")
                    << TErrorAttribute(BackupFailedKey, Request_->GetRequestId())
                    << TErrorAttribute("request_id", Request_->GetRequestId()));
                return;
            }

            auto* ext = header.MutableExtension(NRpc::NProto::THedgingExt::hedging_ext);
            ext->set_backup_responded(true);
            message = SetResponseHeader(std::move(message), header);
        }

        responseHandler->HandleResponse(std::move(message), std::move(address));
    }

    void HandleError(TError error, bool backup)
    {
        IClientResponseHandlerPtr responseHandler;
        {
            auto guard = Guard(SpinLock_);
            if (Responded_ || !ResponseHandler_) {
                return;
            }
            if (!backup && error.GetCode() == NYT::EErrorCode::Canceled && PrimaryCanceled_) {
                return;
            }
            Responded_ = true;
            std::swap(ResponseHandler_, responseHandler);
            TDelayedExecutor::CancelAndClear(DeadlineCookie_);
        }

        YT_LOG_DEBUG_IF(backup, "Request failed at backup (RequestId: %v)",
            Request_->GetRequestId());

        if (backup) {
            error <<= TErrorAttribute(BackupFailedKey, true);
        }
        responseHandler->HandleError(std::move(error));
    }

    // IClientRequestControl implementation.
    void Cancel() override
    {
        IClientResponseHandlerPtr responseHandler;
        {
            auto guard = Guard(SpinLock_);
            std::swap(ResponseHandler_, responseHandler);
            TDelayedExecutor::CancelAndClear(DeadlineCookie_);
            CancelSentRequests(std::move(guard));
        }
    }

    TFuture<void> SendStreamingPayload(const TStreamingPayload& /*payload*/) override
    {
        YT_ABORT();
    }

    TFuture<void> SendStreamingFeedback(const TStreamingFeedback& /*feedback*/) override
    {
        YT_ABORT();
    }

private:
    const IClientRequestPtr Request_;
    IClientResponseHandlerPtr ResponseHandler_;
    const TSendOptions SendOptions_;
    const IChannelPtr PrimaryChannel_;
    const IChannelPtr BackupChannel_;
    const THedgingChannelOptions HedgingOptions_;

    TDuration HedgingDelay_;

    TDelayedExecutorCookie DeadlineCookie_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    bool Acknowledged_ = false;
    bool Responded_ = false;
    bool PrimaryCanceled_ = false;
    TCompactVector<IClientRequestControlPtr, 2> RequestControls_; // always at most 2 items


    void RegisterSentRequest(IClientRequestControlPtr requestControl)
    {
        auto guard = Guard(SpinLock_);
        RequestControls_.push_back(std::move(requestControl));
    }

    void CancelSentRequests(TGuard<NThreading::TSpinLock>&& guard)
    {
        TCompactVector<IClientRequestControlPtr, 2> requestControls;
        std::swap(RequestControls_, requestControls);

        guard.Release();

        for (const auto& control : requestControls) {
            control->Cancel();
        }
    }

    std::optional<TDuration> GetBackupTimeout()
    {
        if (!SendOptions_.Timeout) {
            return std::nullopt;
        }

        auto timeout = *SendOptions_.Timeout;
        if (timeout < HedgingDelay_) {
            return TDuration::Zero();
        }

        return timeout - HedgingDelay_;
    }

    void OnDeadlineReached(bool aborted)
    {
        if (aborted) {
            return;
        }

        auto backupTimeout = GetBackupTimeout();
        if (backupTimeout == TDuration::Zero()) {
            // Makes no sense to send the request anyway.
            return;
        }

        if (!HedgingOptions_.HedgingManager->OnHedgingDelayPassed(/*requestCount*/ 1)) {
            YT_LOG_DEBUG("Hedging manager restrained sending backup request (RequestId: %v)",
                Request_->GetRequestId());
            return;
        }

        {
            auto guard = Guard(SpinLock_);
            if (Responded_ || !ResponseHandler_) {
                return;
            }
            if (HedgingOptions_.CancelPrimaryOnHedging && HedgingDelay_ != TDuration::Zero()) {
                PrimaryCanceled_ = true;
                CancelSentRequests(std::move(guard));
            }
        }

        YT_LOG_DEBUG("Resending request to backup (RequestId: %v)",
            Request_->GetRequestId());

        auto responseHandler = New<THedgingResponseHandler>(this, true);

        auto backupOptions = SendOptions_;
        backupOptions.Timeout = backupTimeout;

        auto requestControl = BackupChannel_->Send(
            Request_,
            std::move(responseHandler),
            backupOptions);

        RegisterSentRequest(std::move(requestControl));
    }
};

DEFINE_REFCOUNTED_TYPE(THedgingSession)

////////////////////////////////////////////////////////////////////////////////

void THedgingResponseHandler::HandleAcknowledgement()
{
    Session_->HandleAcknowledgement(Backup_);
}

void THedgingResponseHandler::HandleError(TError error)
{
    Session_->HandleError(std::move(error), Backup_);
}

void THedgingResponseHandler::HandleResponse(TSharedRefArray message, TString address)
{
    Session_->HandleResponse(std::move(message), std::move(address), Backup_);
}

void THedgingResponseHandler::HandleStreamingPayload(const TStreamingPayload& /*payload*/)
{
    YT_ABORT();
}

void THedgingResponseHandler::HandleStreamingFeedback(const TStreamingFeedback& /*feedback*/)
{
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

class THedgingChannel
    : public IChannel
{
public:
    THedgingChannel(
        IChannelPtr primaryChannel,
        IChannelPtr backupChannel,
        THedgingChannelOptions options)
        : PrimaryChannel_(std::move(primaryChannel))
        , BackupChannel_(std::move(backupChannel))
        , Options_(std::move(options))
        , EndpointDescription_(Format("Hedging(%v,%v)",
            PrimaryChannel_->GetEndpointDescription(),
            BackupChannel_->GetEndpointDescription()))
        , EndpointAttributes_(ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Item("primary").Value(PrimaryChannel_->GetEndpointAttributes())
                .Item("backup").Value(BackupChannel_->GetEndpointAttributes())
            .EndMap()))
    { }

    const TString& GetEndpointDescription() const override
    {
        return EndpointDescription_;
    }

    const NYTree::IAttributeDictionary& GetEndpointAttributes() const override
    {
        return *EndpointAttributes_;
    }

    IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) override
    {
        return New<THedgingSession>(
            std::move(request),
            std::move(responseHandler),
            options,
            PrimaryChannel_,
            BackupChannel_,
            Options_);
    }

    void Terminate(const TError& error) override
    {
        PrimaryChannel_->Terminate(error);
        BackupChannel_->Terminate(error);
    }

    void SubscribeTerminated(const TCallback<void(const TError&)>& callback) override
    {
        PrimaryChannel_->SubscribeTerminated(callback);
        BackupChannel_->SubscribeTerminated(callback);
    }

    void UnsubscribeTerminated(const TCallback<void(const TError&)>& callback) override
    {
        PrimaryChannel_->UnsubscribeTerminated(callback);
        BackupChannel_->UnsubscribeTerminated(callback);
    }

    int GetInflightRequestCount() override
    {
        YT_UNIMPLEMENTED();
    }

private:
    const IChannelPtr PrimaryChannel_;
    const IChannelPtr BackupChannel_;

    const THedgingChannelOptions Options_;

    const TString EndpointDescription_;
    const IAttributeDictionaryPtr EndpointAttributes_;
};

////////////////////////////////////////////////////////////////////////////////

IChannelPtr CreateHedgingChannel(
    IChannelPtr primaryChannel,
    IChannelPtr backupChannel,
    const THedgingChannelOptions& options)
{
    YT_VERIFY(primaryChannel);
    YT_VERIFY(backupChannel);
    YT_VERIFY(options.HedgingManager);

    return New<THedgingChannel>(
        std::move(primaryChannel),
        std::move(backupChannel),
        options);
}

bool IsBackup(const TClientResponsePtr& response)
{
    const auto& ext = response->Header().GetExtension(NRpc::NProto::THedgingExt::hedging_ext);
    return ext.backup_responded();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

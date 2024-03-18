#include "roaming_channel.h"
#include "channel_detail.h"
#include "client.h"

#include <yt/yt/core/actions/future.h>

namespace NYT::NRpc {

using namespace NBus;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TRoamingRequestControl
    : public TClientRequestControlThunk
{
public:
    TRoamingRequestControl(
        TFuture<IChannelPtr> asyncChannel,
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options)
        : Request_(std::move(request))
        , ResponseHandler_(std::move(responseHandler))
        , Options_(options)
        , StartTime_(TInstant::Now())
    {
        if (Options_.Timeout) {
            asyncChannel = asyncChannel.WithTimeout(*Options_.Timeout);
        }

        asyncChannel.Subscribe(BIND(&TRoamingRequestControl::OnGotChannel, MakeStrong(this)));
    }

    void Cancel() override
    {
        if (!TryAcquireSemaphore()) {
            TClientRequestControlThunk::Cancel();
            return;
        }

        ResponseHandler_->HandleError(TError(NYT::EErrorCode::Canceled, "RPC request canceled")
            << TErrorAttribute("request_id", Request_->GetRequestId())
            << TErrorAttribute("realm_id", Request_->GetRealmId())
            << TErrorAttribute("service", Request_->GetService())
            << TErrorAttribute("method", Request_->GetMethod()));

        Request_.Reset();
        ResponseHandler_.Reset();
    }

private:
    IClientRequestPtr Request_;
    IClientResponseHandlerPtr ResponseHandler_;
    const TSendOptions Options_;
    const TInstant StartTime_;

    std::atomic<bool> Semaphore_ = false;


    bool TryAcquireSemaphore()
    {
        bool expected = false;
        return Semaphore_.compare_exchange_strong(expected, true);
    }

    void OnGotChannel(const TErrorOr<IChannelPtr>& result)
    {
        if (!TryAcquireSemaphore()) {
            return;
        }

        auto request = std::move(Request_);
        auto responseHandler = std::move(ResponseHandler_);

        if (!result.IsOK()) {
            responseHandler->HandleError(result);
            return;
        }

        auto adjustedOptions = Options_;
        if (Options_.Timeout) {
            auto now = TInstant::Now();
            auto deadline = StartTime_ + *Options_.Timeout;
            adjustedOptions.Timeout = now > deadline ? TDuration::Zero() : deadline - now;
        }

        const auto& channel = result.Value();
        auto requestControl = channel->Send(
            request,
            responseHandler,
            adjustedOptions);

        SetUnderlying(std::move(requestControl));
    }
};

class TSyncRoamingRequestControl
    : public TClientRequestControlThunk
{
public:
    TSyncRoamingRequestControl(
        IClientRequestControlPtr requestControl,
        IChannelPtr channel)
        : Channel_(std::move(channel))
    {
        SetUnderlying(std::move(requestControl));
    }

private:
    const IChannelPtr Channel_;
};

////////////////////////////////////////////////////////////////////////////////

class TRoamingChannel
    : public IChannel
{
public:
    explicit TRoamingChannel(IRoamingChannelProviderPtr provider)
        : Provider_(std::move(provider))
    { }

    const TString& GetEndpointDescription() const override
    {
        return Provider_->GetEndpointDescription();
    }

    const IAttributeDictionary& GetEndpointAttributes() const override
    {
        return Provider_->GetEndpointAttributes();
    }

    IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) override
    {
        YT_ASSERT(request);
        YT_ASSERT(responseHandler);

        auto asyncChannel = Provider_->GetChannel(request);

        // NB: Optimize for the typical case of sync channel acquisition.
        if (auto channelOrError = asyncChannel.TryGet()) {
            if (channelOrError->IsOK()) {
                const auto& channel = channelOrError->Value();
                return New<TSyncRoamingRequestControl>(
                    channel->Send(
                        std::move(request),
                        std::move(responseHandler),
                        options),
                    channel);
            } else {
                responseHandler->HandleError(std::move(*channelOrError));
                return New<TClientRequestControlThunk>();
            }
        }

        return New<TRoamingRequestControl>(
            std::move(asyncChannel),
            std::move(request),
            std::move(responseHandler),
            options);
    }

    void Terminate(const TError& error) override
    {
        Provider_->Terminate(error);
    }

    void SubscribeTerminated(const TCallback<void(const TError&)>& /*callback*/) override
    { }

    void UnsubscribeTerminated(const TCallback<void(const TError&)>& /*callback*/) override
    { }

    int GetInflightRequestCount() override
    {
        return 0;
    }

private:
    const IRoamingChannelProviderPtr Provider_;
};

IChannelPtr CreateRoamingChannel(IRoamingChannelProviderPtr provider)
{
    YT_VERIFY(provider);

    return New<TRoamingChannel>(std::move(provider));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

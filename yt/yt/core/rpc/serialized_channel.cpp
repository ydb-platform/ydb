#include "serialized_channel.h"
#include "channel_detail.h"
#include "client.h"

#include <queue>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

class TSerializedChannel;
using TSerializedChannelPtr = TIntrusivePtr<TSerializedChannel>;

class TSerializedChannel
    : public TChannelWrapper
{
public:
    explicit TSerializedChannel(IChannelPtr underlyingChannel)
        : TChannelWrapper(std::move(underlyingChannel))
    { }

    IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) override
    {
        auto entry = New<TEntry>(
            request,
            responseHandler,
            options);

        {
            auto guard = Guard(SpinLock_);
            Queue_.push(entry);
        }

        TrySendQueuedRequests();

        return entry->RequestControlThunk;
    }

    void Terminate(const TError& /*error*/) override
    {
        YT_ABORT();
    }

    void OnRequestCompleted()
    {
        {
            auto guard = Guard(SpinLock_);
            YT_VERIFY(RequestInProgress_);
            RequestInProgress_ = false;
        }

        TrySendQueuedRequests();
    }

private:
    class TResponseHandler
        : public IClientResponseHandler
    {
    public:
        TResponseHandler(
            IClientResponseHandlerPtr underlyingHandler,
            TSerializedChannelPtr owner)
            : UnderlyingHandler_(std::move(underlyingHandler))
            , Owner_(std::move(owner))
        { }

        void HandleAcknowledgement() override
        {
            UnderlyingHandler_->HandleAcknowledgement();
        }

        void HandleResponse(TSharedRefArray message, TString address) override
        {
            UnderlyingHandler_->HandleResponse(std::move(message), std::move(address));
            Owner_->OnRequestCompleted();
        }

        void HandleError(TError error) override
        {
            UnderlyingHandler_->HandleError(std::move(error));
            Owner_->OnRequestCompleted();
        }

        void HandleStreamingPayload(const TStreamingPayload& payload) override
        {
            UnderlyingHandler_->HandleStreamingPayload(payload);
        }

        void HandleStreamingFeedback(const TStreamingFeedback& feedback) override
        {
            UnderlyingHandler_->HandleStreamingFeedback(feedback);
        }

    private:
        const IClientResponseHandlerPtr UnderlyingHandler_;
        const TSerializedChannelPtr Owner_;

    };

    struct TEntry
        : public TRefCounted
    {
        TEntry(
            IClientRequestPtr request,
            IClientResponseHandlerPtr handler,
            const TSendOptions& options)
            : Request(std::move(request))
            , Handler(std::move(handler))
            , Options(options)
        { }

        IClientRequestPtr Request;
        IClientResponseHandlerPtr Handler;
        TSendOptions Options;
        TClientRequestControlThunkPtr RequestControlThunk = New<TClientRequestControlThunk>();
    };

    using TEntryPtr = TIntrusivePtr<TEntry>;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    std::queue<TEntryPtr> Queue_;
    bool RequestInProgress_ = false;


    void TrySendQueuedRequests()
    {
        auto guard = Guard(SpinLock_);
        while (!RequestInProgress_ && !Queue_.empty()) {
            auto entry = Queue_.front();
            Queue_.pop();
            RequestInProgress_ = true;
            guard.Release();

            auto serializedHandler = New<TResponseHandler>(entry->Handler, this);
            auto requestControl = UnderlyingChannel_->Send(
                entry->Request,
                serializedHandler,
                entry->Options);
            entry->RequestControlThunk->SetUnderlying(std::move(requestControl));
            entry->Request.Reset();
            entry->Handler.Reset();
            entry->RequestControlThunk.Reset();
        }
    }

};

IChannelPtr CreateSerializedChannel(IChannelPtr underlyingChannel)
{
    YT_VERIFY(underlyingChannel);

    return New<TSerializedChannel>(std::move(underlyingChannel));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

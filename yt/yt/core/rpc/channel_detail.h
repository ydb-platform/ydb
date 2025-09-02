#pragma once

#include "channel.h"

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

class TChannelWrapper
    : public virtual IChannel
{
public:
    explicit TChannelWrapper(IChannelPtr underlyingChannel);

    const std::string& GetEndpointDescription() const override;
    const NYTree::IAttributeDictionary& GetEndpointAttributes() const override;

    IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) override;

    void Terminate(const TError& error) override;

    void SubscribeTerminated(const TCallback<void(const TError&)>& callback) override;
    void UnsubscribeTerminated(const TCallback<void(const TError&)>& callback) override;

    int GetInflightRequestCount() override;

    const IMemoryUsageTrackerPtr& GetChannelMemoryTracker() override;

protected:
    const IChannelPtr UnderlyingChannel_;
};

DEFINE_REFCOUNTED_TYPE(TChannelWrapper)

////////////////////////////////////////////////////////////////////////////////

class TClientRequestControlThunk
    : public IClientRequestControl
{
public:
    void SetUnderlying(IClientRequestControlPtr underlying);

    void Cancel() override;

    TFuture<void> SendStreamingPayload(const TStreamingPayload& payload) override;
    TFuture<void> SendStreamingFeedback(const TStreamingFeedback& feedback) override;

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);

    bool Canceled_ = false;

    struct TPendingStreamingPayload
    {
        TStreamingPayload Payload;
        TPromise<void> Promise;
    };
    std::vector<TPendingStreamingPayload> PendingStreamingPayloads_;

    struct TPendingStreamingFeedback
    {
        TStreamingFeedback Feedback{-1};
        TPromise<void> Promise;
    };
    TPendingStreamingFeedback PendingStreamingFeedback_;

    bool UnderlyingCanceled_ = false;

    IClientRequestControlPtr Underlying_;
};

DEFINE_REFCOUNTED_TYPE(TClientRequestControlThunk)

////////////////////////////////////////////////////////////////////////////////

class TClientRequestPerformanceProfiler
    : public IClientRequestControl
{
public:
    TClientRequestPerformanceProfiler(std::string service, std::string method);

    void ProfileRequest(const TSharedRefArray& requestMessage);
    void ProfileAcknowledgement();
    void ProfileReply(const TSharedRefArray& responseMessage);
    void ProfileCancel();
    void ProfileTimeout();
    void ProfileError(const TError& error);

    TDuration ProfileComplete();

    struct TMethodPerformanceCounters;
    static const TMethodPerformanceCounters* FindPerformanceCounters(std::string service, std::string method);
    static const TMethodPerformanceCounters* GetPerformanceCounters(std::string service, std::string method);
    static void ProfileReplyWithoutContext(
        const TSharedRefArray& responseMessage,
        const TMethodPerformanceCounters* counters,
        bool recognized);

private:
    const TMethodPerformanceCounters* const MethodCounters_;

    NProfiling::TWallTimer Timer_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

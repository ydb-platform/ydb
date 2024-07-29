#include "channel_detail.h"

#include "message.h"
#include "private.h"

#include <library/cpp/yt/memory/leaky_singleton.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/library/syncmap/map.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

TChannelWrapper::TChannelWrapper(IChannelPtr underlyingChannel)
    : UnderlyingChannel_(std::move(underlyingChannel))
{
    YT_ASSERT(UnderlyingChannel_);
}

const TString& TChannelWrapper::GetEndpointDescription() const
{
    return UnderlyingChannel_->GetEndpointDescription();
}

const NYTree::IAttributeDictionary& TChannelWrapper::GetEndpointAttributes() const
{
    return UnderlyingChannel_->GetEndpointAttributes();
}

IClientRequestControlPtr TChannelWrapper::Send(
    IClientRequestPtr request,
    IClientResponseHandlerPtr responseHandler,
    const TSendOptions& options)
{
    return UnderlyingChannel_->Send(
        std::move(request),
        std::move(responseHandler),
        options);
}

void TChannelWrapper::Terminate(const TError& error)
{
    UnderlyingChannel_->Terminate(error);
}

void TChannelWrapper::SubscribeTerminated(const TCallback<void(const TError&)>& callback)
{
    UnderlyingChannel_->SubscribeTerminated(callback);
}

void TChannelWrapper::UnsubscribeTerminated(const TCallback<void(const TError&)>& callback)
{
    UnderlyingChannel_->UnsubscribeTerminated(callback);
}

int TChannelWrapper::GetInflightRequestCount()
{
    return UnderlyingChannel_->GetInflightRequestCount();
}

IMemoryUsageTrackerPtr TChannelWrapper::GetChannelMemoryTracker()
{
    return UnderlyingChannel_->GetChannelMemoryTracker();
}

////////////////////////////////////////////////////////////////////////////////

void TClientRequestControlThunk::SetUnderlying(IClientRequestControlPtr underlying)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!underlying) {
        return;
    }

    auto guard = Guard(SpinLock_);

    // NB: SetUnderlying can only be invoked once.
    // This protects from races on unguarded reads since once Underlying_ is non-null, it never changes.
    YT_VERIFY(!Underlying_);
    Underlying_ = std::move(underlying);

    auto canceled = UnderlyingCanceled_ = Canceled_;
    auto streamingPayloads = std::move(PendingStreamingPayloads_);
    auto streamingFeedback = PendingStreamingFeedback_;

    guard.Release();

    if (canceled) {
        Underlying_->Cancel();
    }

    for (auto& payload : streamingPayloads) {
        payload.Promise.SetFrom(Underlying_->SendStreamingPayload(payload.Payload));
    }

    if (streamingFeedback.Feedback.ReadPosition >= 0) {
        streamingFeedback.Promise.SetFrom(Underlying_->SendStreamingFeedback(streamingFeedback.Feedback));
    }
}

void TClientRequestControlThunk::Cancel()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = Guard(SpinLock_);

    if (Canceled_) {
        return;
    }

    Canceled_ = true;

    if (Underlying_ && !UnderlyingCanceled_) {
        UnderlyingCanceled_ = true;
        guard.Release();
        Underlying_->Cancel();
    }
}

TFuture<void> TClientRequestControlThunk::SendStreamingPayload(const TStreamingPayload& payload)
{
    auto guard = Guard(SpinLock_);

    if (Underlying_) {
        guard.Release();
        return Underlying_->SendStreamingPayload(payload);
    }

    auto promise = NewPromise<void>();
    PendingStreamingPayloads_.push_back({
        payload,
        promise
    });
    return promise.ToFuture();
}

TFuture<void> TClientRequestControlThunk::SendStreamingFeedback(const TStreamingFeedback& feedback)
{
    auto guard = Guard(SpinLock_);

    if (Underlying_) {
        guard.Release();
        return Underlying_->SendStreamingFeedback(feedback);
    }

    if (!PendingStreamingFeedback_.Promise) {
        PendingStreamingFeedback_.Promise = NewPromise<void>();
    }
    auto promise = PendingStreamingFeedback_.Promise;

    PendingStreamingFeedback_.Feedback = TStreamingFeedback{
        std::max(PendingStreamingFeedback_.Feedback.ReadPosition, feedback.ReadPosition)
    };

    return promise;
}

////////////////////////////////////////////////////////////////////////////////

struct TClientRequestPerformanceProfiler::TPerformanceCounters
{
    TPerformanceCounters(const NProfiling::TProfiler& profiler)
        : AckTimeCounter(profiler.Timer("/request_time/ack"))
        , ReplyTimeCounter(profiler.Timer("/request_time/reply"))
        , TimeoutTimeCounter(profiler.Timer("/request_time/timeout"))
        , CancelTimeCounter(profiler.Timer("/request_time/cancel"))
        , TotalTimeCounter(profiler.Timer("/request_time/total"))
        , RequestCounter(profiler.Counter("/request_count"))
        , FailedRequestCounter(profiler.Counter("/failed_request_count"))
        , TimedOutRequestCounter(profiler.Counter("/timed_out_request_count"))
        , CancelledRequestCounter(profiler.Counter("/cancelled_request_count"))
        , RequestMessageBodySizeCounter(profiler.Counter("/request_message_body_bytes"))
        , RequestMessageAttachmentSizeCounter(profiler.Counter("/request_message_attachment_bytes"))
        , ResponseMessageBodySizeCounter(profiler.Counter("/response_message_body_bytes"))
        , ResponseMessageAttachmentSizeCounter(profiler.Counter("/response_message_attachment_bytes"))
    { }

    NProfiling::TEventTimer AckTimeCounter;
    NProfiling::TEventTimer ReplyTimeCounter;
    NProfiling::TEventTimer TimeoutTimeCounter;
    NProfiling::TEventTimer CancelTimeCounter;
    NProfiling::TEventTimer TotalTimeCounter;

    NProfiling::TCounter RequestCounter;
    NProfiling::TCounter FailedRequestCounter;
    NProfiling::TCounter TimedOutRequestCounter;
    NProfiling::TCounter CancelledRequestCounter;
    NProfiling::TCounter RequestMessageBodySizeCounter;
    NProfiling::TCounter RequestMessageAttachmentSizeCounter;
    NProfiling::TCounter ResponseMessageBodySizeCounter;
    NProfiling::TCounter ResponseMessageAttachmentSizeCounter;
};

auto TClientRequestPerformanceProfiler::GetPerformanceCounters(
    std::string service,
    std::string method) -> const TPerformanceCounters*
{
    using TCountersMap = NConcurrency::TSyncMap<std::pair<std::string, std::string>, TPerformanceCounters>;

    auto [counter, _] = LeakySingleton<TCountersMap>()->FindOrInsert(std::pair(service, method), [&] {
        auto profiler = RpcClientProfiler
            .WithHot()
            .WithTag("yt_service", TString(service))
            .WithTag("method", TString(method), -1);
        return TPerformanceCounters(profiler);
    });
    return counter;
}

TClientRequestPerformanceProfiler::TClientRequestPerformanceProfiler(std::string service, std::string method)
    : MethodCounters_(GetPerformanceCounters(std::move(service), std::move(method)))
{ }

void TClientRequestPerformanceProfiler::ProfileRequest(const TSharedRefArray& requestMessage)
{
    MethodCounters_->RequestCounter.Increment();
    MethodCounters_->RequestMessageBodySizeCounter.Increment(GetMessageBodySize(requestMessage));
    MethodCounters_->RequestMessageAttachmentSizeCounter.Increment(GetTotalMessageAttachmentSize(requestMessage));
}

void TClientRequestPerformanceProfiler::ProfileReply(const TSharedRefArray& responseMessage)
{
    MethodCounters_->ReplyTimeCounter.Record(Timer_.GetElapsedTime());
    MethodCounters_->ResponseMessageBodySizeCounter.Increment(GetMessageBodySize(responseMessage));
    MethodCounters_->ResponseMessageAttachmentSizeCounter.Increment(GetTotalMessageAttachmentSize(responseMessage));
}

void TClientRequestPerformanceProfiler::ProfileAcknowledgement()
{
    MethodCounters_->AckTimeCounter.Record(Timer_.GetElapsedTime());
}

void TClientRequestPerformanceProfiler::ProfileCancel()
{
    MethodCounters_->CancelTimeCounter.Record(Timer_.GetElapsedTime());
    MethodCounters_->CancelledRequestCounter.Increment();
}

void TClientRequestPerformanceProfiler::ProfileTimeout()
{
    MethodCounters_->TimeoutTimeCounter.Record(Timer_.GetElapsedTime());
    MethodCounters_->TimedOutRequestCounter.Increment();
}

void TClientRequestPerformanceProfiler::ProfileError(const TError& error)
{
    const auto errorCode = error.GetCode();
    if (errorCode == NYT::EErrorCode::Canceled) {
        ProfileCancel();
    } else if (errorCode == NYT::EErrorCode::Timeout) {
        ProfileTimeout();
    } else {
        MethodCounters_->FailedRequestCounter.Increment();
    }
}

TDuration TClientRequestPerformanceProfiler::ProfileComplete()
{
    auto elapsed = Timer_.GetElapsedTime();
    MethodCounters_->TotalTimeCounter.Record(elapsed);
    return elapsed;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

#include "service_detail.h"
#include "private.h"
#include "authenticator.h"
#include "authentication_identity.h"
#include "config.h"
#include "dispatcher.h"
#include "helpers.h"
#include "message.h"
#include "request_queue_provider.h"
#include "response_keeper.h"
#include "server_detail.h"
#include "stream.h"

#include <yt/yt/core/bus/bus.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/lease_manager.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/misc/crash_handler.h>
#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/misc/tls.h>

namespace NYT::NRpc {

using namespace NBus;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NTracing;
using namespace NConcurrency;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto InfiniteRequestThrottlerConfig = New<TThroughputThrottlerConfig>();
static const auto DefaultLoggingSuppressionFailedRequestThrottlerConfig = TThroughputThrottlerConfig::Create(1'000);

constexpr int MaxUserAgentLength = 200;
constexpr auto ServiceLivenessCheckPeriod = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

TRequestQueuePtr CreateRequestQueue(TString name, const NProfiling::TProfiler& profiler)
{
    return New<TRequestQueue>(name, profiler.WithTag("user", name));
}

////////////////////////////////////////////////////////////////////////////////

THandlerInvocationOptions THandlerInvocationOptions::SetHeavy(bool value) const
{
    auto result = *this;
    result.Heavy = value;
    return result;
}

THandlerInvocationOptions THandlerInvocationOptions::SetResponseCodec(NCompression::ECodec value) const
{
    auto result = *this;
    result.ResponseCodec = value;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicConcurrencyLimit::Reconfigure(int limit)
{
    ConfigLimit_.store(limit, std::memory_order::relaxed);
    SetDynamicLimit(limit);
}

int TDynamicConcurrencyLimit::GetLimitFromConfiguration() const
{
    return ConfigLimit_.load(std::memory_order::relaxed);
}

int TDynamicConcurrencyLimit::GetDynamicLimit() const
{
    return DynamicLimit_.load(std::memory_order::relaxed);
}

void TDynamicConcurrencyLimit::SetDynamicLimit(std::optional<int> dynamicLimit)
{
    auto limit = dynamicLimit.has_value() ? *dynamicLimit : ConfigLimit_.load(std::memory_order::relaxed);
    auto oldLimit = DynamicLimit_.exchange(limit, std::memory_order::relaxed);

    if (oldLimit != limit) {
        Updated_.Fire();
    }
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicConcurrencyByteLimit::Reconfigure(i64 limit)
{
    ConfigByteLimit_.store(limit, std::memory_order::relaxed);
    SetDynamicByteLimit(limit);
}

i64 TDynamicConcurrencyByteLimit::GetByteLimitFromConfiguration() const
{
    return ConfigByteLimit_.load(std::memory_order::relaxed);
}

i64 TDynamicConcurrencyByteLimit::GetDynamicByteLimit() const
{
    return DynamicByteLimit_.load(std::memory_order::relaxed);
}

void TDynamicConcurrencyByteLimit::SetDynamicByteLimit(std::optional<i64> dynamicLimit)
{
    auto limit = dynamicLimit.has_value() ? *dynamicLimit : ConfigByteLimit_.load(std::memory_order::relaxed);
    auto oldLimit = DynamicByteLimit_.exchange(limit, std::memory_order::relaxed);

    if (oldLimit != limit) {
        Updated_.Fire();
    }
}

////////////////////////////////////////////////////////////////////////////////

TServiceBase::TMethodDescriptor::TMethodDescriptor(
    TString method,
    TLiteHandler liteHandler,
    THeavyHandler heavyHandler)
    : Method(std::move(method))
    , LiteHandler(std::move(liteHandler))
    , HeavyHandler(std::move(heavyHandler))
{ }

auto TServiceBase::TMethodDescriptor::SetRequestQueueProvider(IRequestQueueProviderPtr value) const -> TMethodDescriptor
{
    auto result = *this;
    result.RequestQueueProvider = std::move(value);
    return result;
}

auto TServiceBase::TMethodDescriptor::SetInvoker(IInvokerPtr value) const -> TMethodDescriptor
{
    auto result = *this;
    result.Invoker = std::move(value);
    return result;
}

auto TServiceBase::TMethodDescriptor::SetInvokerProvider(TInvokerProvider value) const -> TMethodDescriptor
{
    auto result = *this;
    result.InvokerProvider = std::move(value);
    return result;
}

auto TServiceBase::TMethodDescriptor::SetHeavy(bool value) const -> TMethodDescriptor
{
    auto result = *this;
    result.Options.Heavy = value;
    return result;
}

auto TServiceBase::TMethodDescriptor::SetResponseCodec(NCompression::ECodec value) const -> TMethodDescriptor
{
    auto result = *this;
    result.Options.ResponseCodec = value;
    return result;
}

auto TServiceBase::TMethodDescriptor::SetQueueSizeLimit(int value) const -> TMethodDescriptor
{
    auto result = *this;
    result.QueueSizeLimit = value;
    return result;
}

auto TServiceBase::TMethodDescriptor::SetQueueByteSizeLimit(i64 value) const -> TMethodDescriptor
{
    auto result = *this;
    result.QueueByteSizeLimit = value;
    return result;
}

auto TServiceBase::TMethodDescriptor::SetConcurrencyLimit(int value) const -> TMethodDescriptor
{
    auto result = *this;
    result.ConcurrencyLimit = value;
    return result;
}

auto TServiceBase::TMethodDescriptor::SetConcurrencyByteLimit(i64 value) const -> TMethodDescriptor
{
    auto result = *this;
    result.ConcurrencyByteLimit = value;
    return result;
}

auto TServiceBase::TMethodDescriptor::SetSystem(bool value) const -> TMethodDescriptor
{
    auto result = *this;
    result.System = value;
    return result;
}

auto TServiceBase::TMethodDescriptor::SetLogLevel(NLogging::ELogLevel value) const -> TMethodDescriptor
{
    auto result = *this;
    result.LogLevel = value;
    return result;
}

auto TServiceBase::TMethodDescriptor::SetLoggingSuppressionTimeout(TDuration value) const -> TMethodDescriptor
{
    auto result = *this;
    result.LoggingSuppressionTimeout = value;
    return result;
}

auto TServiceBase::TMethodDescriptor::SetCancelable(bool value) const -> TMethodDescriptor
{
    auto result = *this;
    result.Cancelable = value;
    return result;
}

auto TServiceBase::TMethodDescriptor::SetGenerateAttachmentChecksums(bool value) const -> TMethodDescriptor
{
    auto result = *this;
    result.GenerateAttachmentChecksums = value;
    return result;
}

auto TServiceBase::TMethodDescriptor::SetStreamingEnabled(bool value) const -> TMethodDescriptor
{
    auto result = *this;
    result.StreamingEnabled = value;
    return result;
}

auto TServiceBase::TMethodDescriptor::SetPooled(bool value) const -> TMethodDescriptor
{
    auto result = *this;
    result.Pooled = value;
    return result;
}

auto TServiceBase::TMethodDescriptor::SetHandleMethodError(bool value) const -> TMethodDescriptor
{
    auto result = *this;
    result.HandleMethodError = value;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TServiceBase::TErrorCodeCounter::TErrorCodeCounter(NProfiling::TProfiler profiler)
    : Profiler_(std::move(profiler))
{ }

void TServiceBase::TErrorCodeCounter::Increment(TErrorCode code)
{
    CodeToCounter_.FindOrInsert(code, [&] {
        return Profiler_.WithTag("code", ToString(code)).Counter("/code_count");
    }).first->Increment();
}

////////////////////////////////////////////////////////////////////////////////

TServiceBase::TMethodPerformanceCounters::TMethodPerformanceCounters(
    const NProfiling::TProfiler& profiler,
    const TTimeHistogramConfigPtr& timeHistogramConfig)
    : RequestCounter(profiler.Counter("/request_count"))
    , CanceledRequestCounter(profiler.Counter("/canceled_request_count"))
    , FailedRequestCounter(profiler.Counter("/failed_request_count"))
    , TimedOutRequestCounter(profiler.Counter("/timed_out_request_count"))
    , TraceContextTimeCounter(profiler.TimeCounter("/request_time/trace_context"))
    , RequestMessageBodySizeCounter(profiler.Counter("/request_message_body_bytes"))
    , RequestMessageAttachmentSizeCounter(profiler.Counter("/request_message_attachment_bytes"))
    , ResponseMessageBodySizeCounter(profiler.Counter("/response_message_body_bytes"))
    , ResponseMessageAttachmentSizeCounter(profiler.Counter("/response_message_attachment_bytes"))
    , ErrorCodeCounter(profiler)
{
    if (timeHistogramConfig && timeHistogramConfig->CustomBounds) {
        const auto& customBounds = *timeHistogramConfig->CustomBounds;
        ExecutionTimeCounter = profiler.TimeHistogram("/request_time_histogram/execution", customBounds);
        RemoteWaitTimeCounter = profiler.TimeHistogram("/request_time_histogram/remote_wait", customBounds);
        LocalWaitTimeCounter = profiler.TimeHistogram("/request_time_histogram/local_wait", customBounds);
        TotalTimeCounter = profiler.TimeHistogram("/request_time_histogram/total", customBounds);
    } else if (timeHistogramConfig && timeHistogramConfig->ExponentialBounds) {
        const auto& exponentialBounds = *timeHistogramConfig->ExponentialBounds;
        ExecutionTimeCounter = profiler.TimeHistogram("/request_time_histogram/execution", exponentialBounds->Min, exponentialBounds->Max);
        RemoteWaitTimeCounter = profiler.TimeHistogram("/request_time_histogram/remote_wait", exponentialBounds->Min, exponentialBounds->Max);
        LocalWaitTimeCounter = profiler.TimeHistogram("/request_time_histogram/local_wait", exponentialBounds->Min, exponentialBounds->Max);
        TotalTimeCounter = profiler.TimeHistogram("/request_time_histogram/total", exponentialBounds->Min, exponentialBounds->Max);
    } else {
        ExecutionTimeCounter = profiler.Timer("/request_time/execution");
        RemoteWaitTimeCounter = profiler.Timer("/request_time/remote_wait");
        LocalWaitTimeCounter = profiler.Timer("/request_time/local_wait");
        TotalTimeCounter = profiler.Timer("/request_time/total");
    }
}

TServiceBase::TRuntimeMethodInfo::TRuntimeMethodInfo(
    TServiceId serviceId,
    TMethodDescriptor descriptor,
    const NProfiling::TProfiler& profiler)
    : ServiceId(std::move(serviceId))
    , Descriptor(std::move(descriptor))
    , Profiler(profiler.WithTag("method", Descriptor.Method, -1))
    , DefaultRequestQueue(CreateRequestQueue("default"))
    , RequestLoggingAnchor(NLogging::TLogManager::Get()->RegisterDynamicAnchor(
        Format("%v.%v <-", ServiceId.ServiceName, Descriptor.Method)))
    , ResponseLoggingAnchor(NLogging::TLogManager::Get()->RegisterDynamicAnchor(
        Format("%v.%v ->", ServiceId.ServiceName, Descriptor.Method)))
    , RequestQueueSizeLimitErrorCounter(Profiler.Counter("/request_queue_size_errors"))
    , RequestQueueByteSizeLimitErrorCounter(Profiler.Counter("/request_queue_byte_size_errors"))
    , UnauthenticatedRequestsCounter(Profiler.Counter("/unauthenticated_requests"))
    , LoggingSuppressionFailedRequestThrottler(
        CreateReconfigurableThroughputThrottler(
            DefaultLoggingSuppressionFailedRequestThrottlerConfig))
{ }

TRequestQueue* TServiceBase::TRuntimeMethodInfo::GetDefaultRequestQueue()
{
    return DefaultRequestQueue.Get();
}

////////////////////////////////////////////////////////////////////////////////

class TServiceBase::TServiceContext
    : public TServiceContextBase
{
public:
    TServiceContext(
        TServiceBasePtr&& service,
        TAcceptedRequest&& acceptedRequest,
        NLogging::TLogger logger)
        : TServiceContextBase(
            std::move(acceptedRequest.Header),
            std::move(acceptedRequest.Message),
            std::move(acceptedRequest.MemoryGuard),
            std::move(acceptedRequest.MemoryUsageTracker),
            std::move(logger),
            acceptedRequest.RuntimeInfo->LogLevel.load(std::memory_order::relaxed))
        , Service_(std::move(service))
        , RequestId_(acceptedRequest.RequestId)
        , ReplyBus_(std::move(acceptedRequest.ReplyBus))
        , RuntimeInfo_(acceptedRequest.RuntimeInfo)
        , TraceContext_(std::move(acceptedRequest.TraceContext))
        , RequestQueue_(acceptedRequest.RequestQueue)
        , ThrottledError_(std::move(acceptedRequest.ThrottledError))
        , MethodPerformanceCounters_(Service_->GetMethodPerformanceCounters(
            RuntimeInfo_,
            {GetAuthenticationIdentity().UserTag, RequestQueue_}))
        , PerformanceCounters_(Service_->GetPerformanceCounters())
        , ArriveInstant_(NProfiling::GetInstant())
    {
        YT_ASSERT(RequestMessage_);
        YT_ASSERT(ReplyBus_);
        YT_ASSERT(Service_);
        YT_ASSERT(RuntimeInfo_);

        Initialize();
    }

    ~TServiceContext()
    {
        if (!Replied_) {
            // Prevent alerting.
            RequestInfoSet_ = true;
            if (CanceledList_.IsFired()) {
                if (TimedOutLatch_) {
                    Reply(TError(NYT::EErrorCode::Timeout, "Request timed out"));
                } else {
                    Reply(TError(NYT::EErrorCode::Canceled, "Request canceled"));
                }
            } else {
                Reply(TError(NRpc::EErrorCode::Unavailable, "Service is unable to complete your request"));
            }
        }

        Finish();
    }

    TRequestQueue* GetRequestQueue() const
    {
        return RequestQueue_;
    }

    bool IsPooled() const override
    {
        return RuntimeInfo_->Pooled.load();
    }

    TBusNetworkStatistics GetBusNetworkStatistics() const override
    {
        return ReplyBus_->GetNetworkStatistics();
    }

    const IAttributeDictionary& GetEndpointAttributes() const override
    {
        return ReplyBus_->GetEndpointAttributes();
    }

    const TString& GetEndpointDescription() const override
    {
        return ReplyBus_->GetEndpointDescription();
    }

    TRuntimeMethodInfo* GetRuntimeInfo() const
    {
        return RuntimeInfo_;
    }

    const IBusPtr& GetReplyBus() const
    {
        return ReplyBus_;
    }

    void CheckAndRun(const TErrorOr<TLiteHandler>& handlerOrError)
    {
        if (!handlerOrError.IsOK()) {
            Reply(TError(handlerOrError));
            return;
        }

        const auto& handler = handlerOrError.Value();
        if (!handler) {
            return;
        }

        Run(handler);
    }

    void MarkRequestRun()
    {
        RequestRun_ = true;
    }

    void Run(const TLiteHandler& handler)
    {
        // TODO(shakurov): replace with YT_VERIFY.
        if (!RequestRun_) {
            YT_LOG_ALERT("A request not marked as run has been run (RequestId: %v)",
                RequestId_);
            RequestRun_ = true;
        }

        const auto& descriptor = RuntimeInfo_->Descriptor;
        // NB: Try to avoid contention on invoker ref-counter.
        IInvoker* invoker = nullptr;
        IInvokerPtr invokerHolder;
        if (descriptor.InvokerProvider) {
            invokerHolder = descriptor.InvokerProvider(RequestHeader());
            invoker = invokerHolder.Get();
        }
        if (!invoker) {
            invoker = descriptor.Invoker.Get();
        }
        if (!invoker) {
            invoker = Service_->DefaultInvoker_.Get();
        }
        invoker->Invoke(BIND(&TServiceContext::DoRun, MakeStrong(this), handler));
    }

    void SubscribeCanceled(const TCanceledCallback& callback) override
    {
        CanceledList_.Subscribe(callback);
    }

    void UnsubscribeCanceled(const TCanceledCallback& callback) override
    {
        CanceledList_.Unsubscribe(callback);
    }

    void SubscribeReplied(const TClosure& callback) override
    {
        RepliedList_.Subscribe(callback);
    }

    void UnsubscribeReplied(const TClosure& callback) override
    {
        RepliedList_.Unsubscribe(callback);
    }

    bool IsCanceled() const override
    {
        return CanceledList_.IsFired();
    }

    TError GetCanceledError() const
    {
        return TError(NYT::EErrorCode::Canceled, "RPC request is canceled")
            << ThrottledError_;
    }

    void Cancel() override
    {
        if (!CanceledList_.Fire(GetCanceledError())) {
            return;
        }

        YT_LOG_DEBUG("Request canceled (RequestId: %v)",
            RequestId_);

        if (RuntimeInfo_->Descriptor.StreamingEnabled) {
            static const auto CanceledError = TError(NYT::EErrorCode::Canceled, "Request canceled");
            AbortStreamsUnlessClosed(CanceledError);
        }

        CancelInstant_ = NProfiling::GetInstant();

        MethodPerformanceCounters_->CanceledRequestCounter.Increment();
    }

    void SetComplete() override
    {
        DoSetComplete();
    }

    void HandleTimeout(ERequestProcessingStage stage)
    {
        if (TimedOutLatch_.exchange(true)) {
            return;
        }

        YT_LOG_DEBUG("Request timed out, canceling (RequestId: %v, Stage: %v)",
            RequestId_,
            stage);

        if (RuntimeInfo_->Descriptor.StreamingEnabled) {
            static const auto TimedOutError = TError(NYT::EErrorCode::Timeout, "Request timed out");
            AbortStreamsUnlessClosed(TimedOutError);
        }

        CanceledList_.Fire(GetCanceledError());

        MethodPerformanceCounters_->TimedOutRequestCounter.Increment();

        // Guards from race with DoGuardedRun.
        // We can only mark as complete those requests that will not be run
        // as there's no guarantee that, if started,  the method handler will respond promptly to cancelation.
        if (!RunLatch_.exchange(true)) {
            SetComplete();
        }
    }

    TInstant GetArriveInstant() const override
    {
        return ArriveInstant_;
    }

    std::optional<TInstant> GetRunInstant() const override
    {
        return RunInstant_;
    }

    std::optional<TInstant> GetFinishInstant() const override
    {
        if (ReplyInstant_) {
            return ReplyInstant_;
        } else if (CancelInstant_) {
            return CancelInstant_;
        } else {
            return std::nullopt;
        }
    }

    std::optional<TDuration> GetWaitDuration() const override
    {
        return LocalWaitTime_;
    }

    std::optional<TDuration> GetExecutionDuration() const override
    {
        return ExecutionTime_;
    }

    TTraceContextPtr GetTraceContext() const override
    {
        return TraceContext_;
    }

    IAsyncZeroCopyInputStreamPtr GetRequestAttachmentsStream() override
    {
        if (!RuntimeInfo_->Descriptor.StreamingEnabled) {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::StreamingNotSupported, "Streaming is not supported");
        }
        CreateRequestAttachmentsStream();
        return RequestAttachmentsStream_;
    }

    void SetResponseCodec(NCompression::ECodec codec) override
    {
        auto guard = Guard(StreamsLock_);
        if (ResponseAttachmentsStream_) {
            THROW_ERROR_EXCEPTION("Cannot update response codec after response attachments stream is accessed");
        }
        TServiceContextBase::SetResponseCodec(codec);
    }

    IAsyncZeroCopyOutputStreamPtr GetResponseAttachmentsStream() override
    {
        if (!RuntimeInfo_->Descriptor.StreamingEnabled) {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::StreamingNotSupported, "Streaming is not supported");
        }
        CreateResponseAttachmentsStream();
        return ResponseAttachmentsStream_;
    }

    void HandleStreamingPayload(const TStreamingPayload& payload)
    {
        if (!RuntimeInfo_->Descriptor.StreamingEnabled) {
            YT_LOG_DEBUG("Received streaming payload for a method that does not support streaming; ignored "
                "(Method: %v.%v, RequestId: %v)",
                Service_->ServiceId_.ServiceName,
                RuntimeInfo_->Descriptor.Method,
                RequestId_);
            return;
        }
        CreateRequestAttachmentsStream();
        try {
            RequestAttachmentsStream_->EnqueuePayload(payload);
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Error handling streaming payload (RequestId: %v)",
                RequestId_);
            RequestAttachmentsStream_->Abort(ex);
        }
    }

    void HandleStreamingFeedback(const TStreamingFeedback& feedback)
    {
        TAttachmentsOutputStreamPtr stream;
        {
            auto guard = Guard(StreamsLock_);
            stream = ResponseAttachmentsStream_;
        }

        if (!stream) {
            YT_LOG_DEBUG("Received streaming feedback for a method that does not support streaming; ignored "
                "(Method: %v.%v, RequestId: %v)",
                Service_->ServiceId_.ServiceName,
                RuntimeInfo_->Descriptor.Method,
                RequestId_);
            return;
        }

        try {
            stream->HandleFeedback(feedback);
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Error handling streaming feedback (RequestId: %v)",
                RequestId_);
            stream->Abort(ex);
        }
    }

    void BeforeEnqueued()
    {
        auto requestTimeout = GetTimeout();
        if (!requestTimeout) {
            return;
        }

        auto waitingTimeoutFraction = RuntimeInfo_->WaitingTimeoutFraction.load(std::memory_order::relaxed);
        if (waitingTimeoutFraction == 0) {
            return;
        }

        auto waitingTimeout = *requestTimeout * waitingTimeoutFraction;
        WaitingTimeoutCookie_ = TDelayedExecutor::Submit(
            BIND(&TServiceBase::OnRequestTimeout, Service_, RequestId_, ERequestProcessingStage::Waiting),
            waitingTimeout);
    }

    void AfterDequeued()
    {
        TDelayedExecutor::CancelAndClear(WaitingTimeoutCookie_);
    }

private:
    const TServiceBasePtr Service_;
    const TRequestId RequestId_;
    const IBusPtr ReplyBus_;
    TRuntimeMethodInfo* const RuntimeInfo_;
    const TTraceContextPtr TraceContext_;
    TRequestQueue* const RequestQueue_;
    std::optional<TError> ThrottledError_;
    TMethodPerformanceCounters* const MethodPerformanceCounters_;
    TPerformanceCounters* const PerformanceCounters_;

    NCompression::ECodec RequestCodec_;

    TDelayedExecutorCookie WaitingTimeoutCookie_;
    TDelayedExecutorCookie ExecutingTimeoutCookie_;

    bool Cancelable_ = false;
    TSingleShotCallbackList<void(const TError&)> CanceledList_;

    const TInstant ArriveInstant_;
    std::optional<TInstant> RunInstant_;
    std::optional<TInstant> ReplyInstant_;
    std::optional<TInstant> CancelInstant_;

    std::optional<TDuration> ExecutionTime_;
    std::optional<TDuration> TotalTime_;
    std::optional<TDuration> LocalWaitTime_;

    std::atomic<bool> CompletedLatch_ = false;
    std::atomic<bool> TimedOutLatch_ = false;
    std::atomic<bool> RunLatch_ = false;
    bool FinishLatch_ = false;
    bool RequestRun_ = false;
    bool ActiveRequestCountIncremented_ = false;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, StreamsLock_);
    TError StreamsError_;
    TAttachmentsInputStreamPtr RequestAttachmentsStream_;
    TAttachmentsOutputStreamPtr ResponseAttachmentsStream_;

    bool IsRegistrable()
    {
        if (Cancelable_) {
            return true;
        }

        if (RuntimeInfo_->Descriptor.StreamingEnabled) {
            return true;
        }

        return false;
    }

    void Initialize()
    {
        constexpr TStringBuf UnknownUserAgent = "unknown";
        auto userAgent = RequestHeader_->has_user_agent()
            ? TStringBuf(RequestHeader_->user_agent())
            : UnknownUserAgent;
        PerformanceCounters_->IncrementRequestsPerUserAgent(userAgent.SubString(0, MaxUserAgentLength));

        MethodPerformanceCounters_->RequestCounter.Increment();
        MethodPerformanceCounters_->RequestMessageBodySizeCounter.Increment(
            GetMessageBodySize(RequestMessage_));
        MethodPerformanceCounters_->RequestMessageAttachmentSizeCounter.Increment(
            GetTotalMessageAttachmentSize(RequestMessage_));

        if (RequestHeader_->has_start_time()) {
            auto retryStart = FromProto<TInstant>(RequestHeader_->start_time());
            auto now = NProfiling::GetInstant();
            MethodPerformanceCounters_->RemoteWaitTimeCounter.Record(now - retryStart);
        }

        // COMPAT(danilalexeev): legacy RPC codecs
        RequestCodec_ = RequestHeader_->has_request_codec()
            ? CheckedEnumCast<NCompression::ECodec>(RequestHeader_->request_codec())
            : NCompression::ECodec::None;
        ResponseCodec_ = RequestHeader_->has_response_codec()
            ? CheckedEnumCast<NCompression::ECodec>(RequestHeader_->response_codec())
            : NCompression::ECodec::None;

        Service_->IncrementActiveRequestCount();
        ActiveRequestCountIncremented_ = true;

        BuildGlobalRequestInfo();

        Cancelable_ = RuntimeInfo_->Descriptor.Cancelable && !RequestHeader_->uncancelable();

        if (IsRegistrable()) {
            Service_->RegisterRequest(this);
        }
    }

    void BuildGlobalRequestInfo()
    {
        TStringBuilder builder;
        TDelimitedStringBuilderWrapper delimitedBuilder(&builder);

        if (RequestHeader_->has_request_id()) {
            delimitedBuilder->AppendFormat("RequestId: %v", FromProto<TRequestId>(RequestHeader_->request_id()));
        }

        if (RequestHeader_->has_realm_id()) {
            delimitedBuilder->AppendFormat("RealmId: %v", FromProto<TRealmId>(RequestHeader_->realm_id()));
        }

        if (RequestHeader_->has_user()) {
            delimitedBuilder->AppendFormat("User: %v", RequestHeader_->user());
        }

        if (RequestHeader_->has_user_tag() && RequestHeader_->user_tag() != RequestHeader_->user()) {
            delimitedBuilder->AppendFormat("UserTag: %v", RequestHeader_->user_tag());
        }

        if (RequestHeader_->has_mutation_id()) {
            delimitedBuilder->AppendFormat("MutationId: %v", FromProto<TMutationId>(RequestHeader_->mutation_id()));
        }

        if (RequestHeader_->has_start_time()) {
            delimitedBuilder->AppendFormat("StartTime: %v", FromProto<TInstant>(RequestHeader_->start_time()));
        }

        delimitedBuilder->AppendFormat("Retry: %v", RequestHeader_->retry());

        if (RequestHeader_->has_user_agent()) {
            delimitedBuilder->AppendFormat("UserAgent: %v", RequestHeader_->user_agent());
        }

        if (RequestHeader_->has_timeout()) {
            delimitedBuilder->AppendFormat("Timeout: %v", FromProto<TDuration>(RequestHeader_->timeout()));
        }

        if (RequestHeader_->tos_level() != NBus::DefaultTosLevel) {
            delimitedBuilder->AppendFormat("TosLevel: %x", RequestHeader_->tos_level());
        }

        delimitedBuilder->AppendFormat("Endpoint: %v", ReplyBus_->GetEndpointDescription());

        delimitedBuilder->AppendFormat("BodySize: %v, AttachmentsSize: %v/%v",
            GetMessageBodySize(RequestMessage_),
            GetTotalMessageAttachmentSize(RequestMessage_),
            GetMessageAttachmentCount(RequestMessage_));

        // COMPAT(danilalexeev): legacy RPC codecs
        if (RequestHeader_->has_request_codec() && RequestHeader_->has_response_codec()) {
            delimitedBuilder->AppendFormat("RequestCodec: %v, ResponseCodec: %v",
                RequestCodec_,
                ResponseCodec_);
        }

        RequestInfos_.push_back(builder.Flush());
    }

    void Finish()
    {
        // Finish is called from DoReply and ~TServiceContext.
        // Clearly there could be no race between these two and thus no atomics are needed.
        if (FinishLatch_) {
            return;
        }
        FinishLatch_ = true;

        TDelayedExecutor::CancelAndClear(WaitingTimeoutCookie_);
        TDelayedExecutor::CancelAndClear(ExecutingTimeoutCookie_);

        if (IsRegistrable()) {
            Service_->UnregisterRequest(this);
        }

        if (RuntimeInfo_->Descriptor.StreamingEnabled) {
            static const auto FinishedError = TError("Request finished");
            AbortStreamsUnlessClosed(Error_.IsOK() ? Error_ : FinishedError);
        }

        DoSetComplete();
    }

    void AbortStreamsUnlessClosed(const TError& error)
    {
        auto guard = Guard(StreamsLock_);

        if (!StreamsError_.IsOK()) {
            return;
        }

        StreamsError_ = error;

        auto requestAttachmentsStream = RequestAttachmentsStream_;
        auto responseAttachmentsStream = ResponseAttachmentsStream_;

        guard.Release();

        if (requestAttachmentsStream) {
            requestAttachmentsStream->AbortUnlessClosed(Error_);
        }

        if (responseAttachmentsStream) {
            responseAttachmentsStream->AbortUnlessClosed(Error_);
        }
    }


    void DoRun(const TLiteHandler& handler)
    {
        RunInstant_ = NProfiling::GetInstant();
        LocalWaitTime_ = *RunInstant_ - ArriveInstant_;
        MethodPerformanceCounters_->LocalWaitTimeCounter.Record(*LocalWaitTime_);

        try {
            TCurrentTraceContextGuard guard(TraceContext_);
            DoGuardedRun(handler);
        } catch (const std::exception& ex) {
            const auto& descriptor = RuntimeInfo_->Descriptor;
            if (descriptor.HandleMethodError) {
                Service_->OnMethodError(ex, descriptor.Method);
            }

            Reply(ex);
        }
    }

    void DoGuardedRun(const TLiteHandler& handler)
    {
        const auto& descriptor = RuntimeInfo_->Descriptor;

        if (Service_->IsStopped()) {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable, "Service is stopped");
        }

        if (!descriptor.System) {
            Service_->BeforeInvoke(this);
        }

        if (auto timeout = GetTimeout()) {
            auto remainingTimeout = *timeout - (*RunInstant_ - ArriveInstant_);
            if (remainingTimeout == TDuration::Zero()) {
                if (!TimedOutLatch_.exchange(true)) {
                    Reply(TError(NYT::EErrorCode::Timeout, "Request dropped due to timeout before being run"));
                    MethodPerformanceCounters_->TimedOutRequestCounter.Increment();
                }
                return;
            }
            if (Cancelable_) {
                ExecutingTimeoutCookie_ = TDelayedExecutor::Submit(
                    BIND(&TServiceBase::OnRequestTimeout, Service_, RequestId_, ERequestProcessingStage::Executing),
                    remainingTimeout);
            }
        }

        if (Cancelable_) {
            // TODO(lukyan): Wrap in CancelableExecution.
            auto fiberCanceler = GetCurrentFiberCanceler();
            if (fiberCanceler) {
                auto cancelationHandler = BIND([fiberCanceler = std::move(fiberCanceler)] (const TError& error) {
                    fiberCanceler(error);
                });
                if (!CanceledList_.TrySubscribe(std::move(cancelationHandler))) {
                    YT_LOG_DEBUG("Request was canceled before being run (RequestId: %v)",
                        RequestId_);
                    return;
                }
            }
        }

        // Guards from race with HandleTimeout.
        if (RunLatch_.exchange(true)) {
            return;
        }

        {
            const auto& authenticationIdentity = GetAuthenticationIdentity();
            TCodicilGuard codicilGuard(Format("RequestId: %v, Method: %v.%v, AuthenticationIdentity: %v",
                GetRequestId(),
                GetService(),
                GetMethod(),
                authenticationIdentity));
            TCurrentAuthenticationIdentityGuard identityGuard(&authenticationIdentity);
            handler(this, descriptor.Options);
        }
    }

    std::optional<TDuration> GetTraceContextTime() const override
    {
        if (TraceContext_) {
            FlushCurrentTraceContextElapsedTime();
            return TraceContext_->GetElapsedTime();
        } else {
            return std::nullopt;
        }
    }

    void DoReply() override
    {
        auto responseMessage = GetResponseMessage();

        NBus::TSendOptions busOptions;
        busOptions.TrackingLevel = EDeliveryTrackingLevel::None;
        busOptions.ChecksummedPartCount = RuntimeInfo_->Descriptor.GenerateAttachmentChecksums
            ? NBus::TSendOptions::AllParts
            : 2; // RPC header + response body
        busOptions.EnableSendCancelation = Cancelable_;

        auto replySent = ReplyBus_->Send(responseMessage, busOptions);
        if (Cancelable_ && replySent) {
            if (auto timeout = GetTimeout()) {
                auto timeoutCookie = TDelayedExecutor::Submit(
                    BIND([replySent] {
                        replySent.Cancel(TError());
                    }),
                    ArriveInstant_ + *timeout);

                replySent.Subscribe(BIND([timeoutCookie] (const TError& /*error*/) {
                    TDelayedExecutor::Cancel(timeoutCookie);
                }));
            }

            Service_->RegisterQueuedReply(RequestId_, replySent);
            replySent.Subscribe(BIND([weakService = MakeWeak(Service_), requestId = RequestId_] (const TError& /*error*/) {
                if (auto service = weakService.Lock()) {
                    service->UnregisterQueuedReply(requestId);
                }
            }));
        }

        if (auto traceContextTime = GetTraceContextTime()) {
            MethodPerformanceCounters_->TraceContextTimeCounter.Add(*traceContextTime);
        }

        ReplyInstant_ = NProfiling::GetInstant();
        ExecutionTime_ = RunInstant_ ? *ReplyInstant_ - *RunInstant_ : TDuration();
        TotalTime_ = *ReplyInstant_ - ArriveInstant_;

        MethodPerformanceCounters_->ExecutionTimeCounter.Record(*ExecutionTime_);
        MethodPerformanceCounters_->TotalTimeCounter.Record(*TotalTime_);
        if (!Error_.IsOK()) {
            if (Service_->EnableErrorCodeCounter_.load()) {
                MethodPerformanceCounters_->ErrorCodeCounter.Increment(Error_.GetNonTrivialCode());
            } else {
                MethodPerformanceCounters_->FailedRequestCounter.Increment();
            }
        }
        HandleLoggingSuppression();

        MethodPerformanceCounters_->ResponseMessageBodySizeCounter.Increment(
            GetMessageBodySize(responseMessage));
        MethodPerformanceCounters_->ResponseMessageAttachmentSizeCounter.Increment(
            GetTotalMessageAttachmentSize(responseMessage));

        if (!Error_.IsOK() && TraceContext_ && TraceContext_->IsRecorded()) {
            TraceContext_->AddErrorTag();
        }

        Finish();
    }

    void DoFlush() override
    {
        if (TraceContext_) {
            TraceContext_->Finish();
        }
    }

    void HandleLoggingSuppression()
    {
        auto timeout = RequestHeader_->has_logging_suppression_timeout()
            ? FromProto<TDuration>(RequestHeader_->logging_suppression_timeout())
            : RuntimeInfo_->LoggingSuppressionTimeout.load(std::memory_order::relaxed);

        if (*TotalTime_ >= timeout) {
            return;
        }

        if (!Error_.IsOK() &&
            (RequestHeader_->disable_logging_suppression_if_request_failed() ||
            RuntimeInfo_->LoggingSuppressionFailedRequestThrottler->TryAcquire(1)))
        {
            return;
        }

        {
            TNullTraceContextGuard nullGuard;
            YT_LOG_DEBUG("Request logging suppressed (RequestId: %v)", GetRequestId());
        }
        NLogging::TLogManager::Get()->SuppressRequest(GetRequestId());
    }

    void DoSetComplete()
    {
        // DoSetComplete could be called from anywhere so it is racy.
        if (CompletedLatch_.exchange(true)) {
            return;
        }

        if (RequestRun_) {
            RequestQueue_->OnRequestFinished(TotalSize_);
        }

        if (ActiveRequestCountIncremented_) {
            Service_->DecrementActiveRequestCount();
        }
    }

    void LogRequest() override
    {
        TStringBuilder builder;
        builder.AppendFormat("%v.%v <- ",
            GetService(),
            GetMethod());

        TDelimitedStringBuilderWrapper delimitedBuilder(&builder);

        for (const auto& info : RequestInfos_) {
            delimitedBuilder->AppendString(info);
        }

        if (RuntimeInfo_->Descriptor.Cancelable && !Cancelable_) {
            delimitedBuilder->AppendFormat("Cancelable: %v", Cancelable_);
        }

        auto logMessage = builder.Flush();
        if (TraceContext_ && TraceContext_->IsRecorded()) {
            TraceContext_->AddTag(RequestInfoAnnotation, logMessage);
            const auto& authenticationIdentity = GetAuthenticationIdentity();
            if (authenticationIdentity.User) {
                TStringBuilder builder;
                builder.AppendString(authenticationIdentity.User);
                if (authenticationIdentity.UserTag && authenticationIdentity.UserTag != authenticationIdentity.User) {
                    builder.AppendChar(':');
                    builder.AppendString(authenticationIdentity.UserTag);
                }
                TraceContext_->AddTag(RequestUser, builder.Flush());
            }
        }
        YT_LOG_EVENT_WITH_ANCHOR(Logger, LogLevel_, RuntimeInfo_->RequestLoggingAnchor, logMessage);
    }

    void LogResponse() override
    {
        TStringBuilder builder;
        builder.AppendFormat("%v.%v -> ",
            GetService(),
            GetMethod());

        TDelimitedStringBuilderWrapper delimitedBuilder(&builder);

        if (RequestId_) {
            delimitedBuilder->AppendFormat("RequestId: %v", RequestId_);
        }

        if (RequestHeader_->has_user()) {
            delimitedBuilder->AppendFormat("User: %v", RequestHeader_->user());
        }

        if (RequestHeader_->has_user_tag() && RequestHeader_->user_tag() != RequestHeader_->user()) {
            delimitedBuilder->AppendFormat("UserTag: %v", RequestHeader_->user_tag());
        }

        auto responseMessage = GetResponseMessage();
        delimitedBuilder->AppendFormat("Error: %v, BodySize: %v, AttachmentsSize: %v/%v",
            Error_,
            GetMessageBodySize(responseMessage),
            GetTotalMessageAttachmentSize(responseMessage),
            GetMessageAttachmentCount(responseMessage));

        for (const auto& info : ResponseInfos_) {
            delimitedBuilder->AppendString(info);
        }

        delimitedBuilder->AppendFormat("ExecutionTime: %v, TotalTime: %v",
            *ExecutionTime_,
            *TotalTime_);

        if (auto traceContextTime = GetTraceContextTime()) {
            delimitedBuilder->AppendFormat("CpuTime: %v", traceContextTime);
        }

        auto logMessage = builder.Flush();
        if (TraceContext_ && TraceContext_->IsRecorded()) {
            TraceContext_->AddTag(ResponseInfoAnnotation, logMessage);
        }
        YT_LOG_EVENT_WITH_ANCHOR(Logger, LogLevel_, RuntimeInfo_->ResponseLoggingAnchor, logMessage);
    }


    void CreateRequestAttachmentsStream()
    {
        auto guard = Guard(StreamsLock_);

        if (!RequestAttachmentsStream_) {
            auto parameters = FromProto<TStreamingParameters>(RequestHeader_->server_attachments_streaming_parameters());
            RequestAttachmentsStream_ =  New<TAttachmentsInputStream>(
                BIND(&TServiceContext::OnRequestAttachmentsStreamRead, MakeWeak(this)),
                TDispatcher::Get()->GetCompressionPoolInvoker(),
                parameters.ReadTimeout);
        }

        auto error = StreamsError_;

        guard.Release();

        if (!error.IsOK()) {
            RequestAttachmentsStream_->AbortUnlessClosed(error);
        }
    }

    void CreateResponseAttachmentsStream()
    {
        auto guard = Guard(StreamsLock_);

        if (!ResponseAttachmentsStream_) {
            auto parameters = FromProto<TStreamingParameters>(RequestHeader_->server_attachments_streaming_parameters());
            ResponseAttachmentsStream_ = New<TAttachmentsOutputStream>(
                ResponseCodec_,
                TDispatcher::Get()->GetCompressionPoolInvoker(),
                BIND(&TServiceContext::OnPullResponseAttachmentsStream, MakeWeak(this)),
                parameters.WindowSize,
                parameters.WriteTimeout);
        }

        auto error = StreamsError_;

        guard.Release();

        if (!error.IsOK()) {
            ResponseAttachmentsStream_->AbortUnlessClosed(error);
        }
    }

    void OnPullResponseAttachmentsStream()
    {
        YT_VERIFY(ResponseAttachmentsStream_);
        auto payload = ResponseAttachmentsStream_->TryPull();
        if (!payload) {
            return;
        }

        YT_LOG_DEBUG("Response streaming attachments pulled (RequestId: %v, SequenceNumber: %v, Sizes: %v, Closed: %v)",
            RequestId_,
            payload->SequenceNumber,
            MakeFormattableView(payload->Attachments, [] (auto* builder, const auto& attachment) {
                builder->AppendFormat("%v", GetStreamingAttachmentSize(attachment));
            }),
            !payload->Attachments.back());

        NProto::TStreamingPayloadHeader header;
        ToProto(header.mutable_request_id(), RequestId_);
        ToProto(header.mutable_service(), GetService());
        ToProto(header.mutable_method(), GetMethod());
        if (GetRealmId()) {
            ToProto(header.mutable_realm_id(), GetRealmId());
        }
        header.set_sequence_number(payload->SequenceNumber);
        header.set_codec(static_cast<int>(payload->Codec));

        auto message = CreateStreamingPayloadMessage(header, payload->Attachments);

        NBus::TSendOptions options;
        options.TrackingLevel = EDeliveryTrackingLevel::Full;
        ReplyBus_->Send(std::move(message), options).Subscribe(
            BIND(&TServiceContext::OnResponseStreamingPayloadAcked, MakeStrong(this), payload->SequenceNumber));
    }

    void OnResponseStreamingPayloadAcked(int sequenceNumber, const TError& error)
    {
        YT_VERIFY(ResponseAttachmentsStream_);
        if (error.IsOK()) {
            YT_LOG_DEBUG("Response streaming payload delivery acknowledged (RequestId: %v, SequenceNumber: %v)",
                RequestId_,
                sequenceNumber);
        } else {
            YT_LOG_DEBUG(error, "Response streaming payload delivery failed (RequestId: %v, SequenceNumber: %v)",
                RequestId_,
                sequenceNumber);
            ResponseAttachmentsStream_->Abort(error);
        }
    }

    void OnRequestAttachmentsStreamRead()
    {
        YT_VERIFY(RequestAttachmentsStream_);
        auto feedback = RequestAttachmentsStream_->GetFeedback();

        YT_LOG_DEBUG("Request streaming attachments read (RequestId: %v, ReadPosition: %v)",
            RequestId_,
            feedback.ReadPosition);

        NProto::TStreamingFeedbackHeader header;
        ToProto(header.mutable_request_id(), RequestId_);
        header.set_service(GetService());
        header.set_method(GetMethod());
        if (GetRealmId()) {
            ToProto(header.mutable_realm_id(), GetRealmId());
        }
        header.set_read_position(feedback.ReadPosition);

        auto message = CreateStreamingFeedbackMessage(header);

        NBus::TSendOptions options;
        options.TrackingLevel = EDeliveryTrackingLevel::Full;
        ReplyBus_->Send(std::move(message), options).Subscribe(
            BIND(&TServiceContext::OnRequestStreamingFeedbackAcked, MakeStrong(this)));
    }

    void OnRequestStreamingFeedbackAcked(const TError& error)
    {
        YT_VERIFY(RequestAttachmentsStream_);
        if (error.IsOK()) {
            YT_LOG_DEBUG("Request streaming feedback delivery acknowledged (RequestId: %v)",
                RequestId_);
        } else {
            YT_LOG_DEBUG(error, "Request streaming feedback delivery failed (RequestId: %v)",
                RequestId_);
            RequestAttachmentsStream_->Abort(error);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TRequestQueue::TRequestQueue(TString name, NProfiling::TProfiler profiler)
    : Name_(std::move(name))
    , BytesThrottler_{CreateReconfigurableThroughputThrottler(InfiniteRequestThrottlerConfig,
        NLogging::TLogger(),
        profiler.WithPrefix("/bytes_throttler"))}
    , WeightThrottler_{CreateReconfigurableThroughputThrottler(InfiniteRequestThrottlerConfig,
        NLogging::TLogger(),
        profiler.WithPrefix("/weight_throttler"))}
{ }

bool TRequestQueue::Register(TServiceBase* service, TServiceBase::TRuntimeMethodInfo* runtimeInfo)
{
    // Fast path.
    if (Registered_.load(std::memory_order::acquire)) {
        YT_ASSERT(service == Service_);
        YT_ASSERT(runtimeInfo == RuntimeInfo_);
        return false;
    }

    // Slow path.
    {
        auto guard = Guard(RegisterLock_);
        if (!Registered_.load(std::memory_order::relaxed)) {
            Service_ = service;
            RuntimeInfo_ = runtimeInfo;

            RuntimeInfo_->ConcurrencyLimit.SubscribeUpdated(BIND(
                &TRequestQueue::OnConcurrencyLimitChanged,
                MakeWeak(this)));

            RuntimeInfo_->ConcurrencyByteLimit.SubscribeUpdated(BIND(
                &TRequestQueue::OnConcurrencyByteLimitChanged,
                MakeWeak(this)));
        }
        Registered_.store(true, std::memory_order::release);
    }

    return true;
}

void TRequestQueue::OnConcurrencyLimitChanged()
{
    if (QueueSize_.load() > 0) {
        ScheduleRequestsFromQueue();
    }
}

void TRequestQueue::OnConcurrencyByteLimitChanged()
{
    if (QueueByteSize_.load() > 0) {
        ScheduleRequestsFromQueue();
    }
}

void TRequestQueue::TRequestThrottler::Reconfigure(const TThroughputThrottlerConfigPtr& config)
{
    Throttler->Reconfigure(config ? config : New<TThroughputThrottlerConfig>());
    Specified.store(config.operator bool(), std::memory_order::release);
}

void TRequestQueue::Configure(const TMethodConfigPtr& config)
{
    BytesThrottler_.Reconfigure(config->RequestBytesThrottler);
    WeightThrottler_.Reconfigure(config->RequestWeightThrottler);

    ScheduleRequestsFromQueue();
    SubscribeToThrottlers();
}

const TString& TRequestQueue::GetName() const
{
    return Name_;
}

void TRequestQueue::ConfigureBytesThrottler(const TThroughputThrottlerConfigPtr& config)
{
    BytesThrottler_.Reconfigure(config);
}

void TRequestQueue::ConfigureWeightThrottler(const TThroughputThrottlerConfigPtr& config)
{
    WeightThrottler_.Reconfigure(config);
}

bool TRequestQueue::IsQueueSizeLimitExceeded() const
{
    return QueueSize_.load(std::memory_order::relaxed) >
        RuntimeInfo_->QueueSizeLimit.load(std::memory_order::relaxed);
}

bool TRequestQueue::IsQueueByteSizeLimitExceeded() const
{
    return QueueByteSize_.load(std::memory_order::relaxed) >
        RuntimeInfo_->QueueByteSizeLimit.load(std::memory_order::relaxed);
}

int TRequestQueue::GetQueueSize() const
{
    return QueueSize_.load(std::memory_order::relaxed);
}

i64 TRequestQueue::GetQueueByteSize() const
{
    return QueueByteSize_.load(std::memory_order::relaxed);
}

int TRequestQueue::GetConcurrency() const
{
    return Concurrency_.load(std::memory_order::relaxed);
}

i64 TRequestQueue::GetConcurrencyByte() const
{
    return ConcurrencyByte_.load(std::memory_order::relaxed);
}

void TRequestQueue::OnRequestArrived(const TServiceBase::TServiceContextPtr& context)
{
    // Fast path.
    auto concurrencyExceeded = IncrementConcurrency(context);
    if (concurrencyExceeded &&
        !AreThrottlersOverdrafted())
    {
        RunRequest(std::move(context));
        return;
    }

    // Slow path.
    DecrementConcurrency(context->GetTotalSize());
    IncrementQueueSize(context);

    context->BeforeEnqueued();
    YT_VERIFY(Queue_.enqueue(std::move(context)));

    ScheduleRequestsFromQueue();
}

void TRequestQueue::OnRequestFinished(i64 requestTotalSize)
{
    DecrementConcurrency(requestTotalSize);

    if (QueueSize_.load() > 0) {
        // Slow path.
        ScheduleRequestsFromQueue();
    }
}

// Prevents reentrant invocations.
// One case is: RunRequest calling the handler synchronously, which replies the
// context, which calls context->Finish, and we're back here again.
YT_DEFINE_THREAD_LOCAL(bool, ScheduleRequestsLatch, false);

void TRequestQueue::ScheduleRequestsFromQueue()
{
    if (ScheduleRequestsLatch()) {
        return;
    }

    ScheduleRequestsLatch() = true;
    auto latchGuard = Finally([&] {
        ScheduleRequestsLatch() = false;
    });

#ifndef NDEBUG
    // Method handlers are allowed to run via sync invoker;
    // however these handlers must not yield.
    TForbidContextSwitchGuard contextSwitchGuard;
#endif

    // NB: Racy, may lead to overcommit in concurrency semaphore and request bytes throttler.
    auto concurrencyLimit = RuntimeInfo_->ConcurrencyLimit.GetDynamicLimit();
    auto concurrencyByteLimit = RuntimeInfo_->ConcurrencyByteLimit.GetDynamicByteLimit();
    while (QueueSize_.load() > 0 && Concurrency_.load() < concurrencyLimit && ConcurrencyByte_.load() < concurrencyByteLimit) {
        if (AreThrottlersOverdrafted()) {
            SubscribeToThrottlers();
            return;
        }

        TServiceBase::TServiceContextPtr context;
        while (!context) {
            if (!Queue_.try_dequeue(context)) {
                // False negatives are possible in Moody Camel.
                if (QueueSize_.load() > 0) {
                    continue;
                }
                return;
            }

            DecrementQueueSize(context);
            context->AfterDequeued();
            if (context->IsCanceled()) {
                context.Reset();
            }
        }

        IncrementConcurrency(context);
        RunRequest(std::move(context));
    }
}

void TRequestQueue::RunRequest(TServiceBase::TServiceContextPtr context)
{
    context->MarkRequestRun();

    AcquireThrottlers(context);

    auto options = RuntimeInfo_->Descriptor.Options;
    options.SetHeavy(RuntimeInfo_->Heavy.load(std::memory_order::relaxed));

    if (options.Heavy) {
        BIND([context, options, heavyHandler = RuntimeInfo_->Descriptor.HeavyHandler] {
            return heavyHandler.Run(context, options);
        })
            .AsyncVia(TDispatcher::Get()->GetHeavyInvoker())
            .Run()
            .Subscribe(BIND(&TServiceBase::TServiceContext::CheckAndRun, context));
    } else {
        context->Run(RuntimeInfo_->Descriptor.LiteHandler);
    }
}

void TRequestQueue::IncrementQueueSize(const TServiceBase::TServiceContextPtr& context)
{
    ++QueueSize_;
    QueueByteSize_.fetch_add(context->GetTotalSize());
}

void  TRequestQueue::DecrementQueueSize(const TServiceBase::TServiceContextPtr& context)
{
    auto newQueueSize = --QueueSize_;
    auto oldQueueByteSize = QueueByteSize_.fetch_sub(context->GetTotalSize());

    YT_ASSERT(newQueueSize >= 0);
    YT_ASSERT(oldQueueByteSize >= 0);
}

bool TRequestQueue::IncrementConcurrency(const TServiceBase::TServiceContextPtr& context)
{
    auto resultSize = ++Concurrency_ <= RuntimeInfo_->ConcurrencyLimit.GetDynamicLimit();
    auto resultByteSize = ConcurrencyByte_.fetch_add(context->GetTotalSize()) <= RuntimeInfo_->ConcurrencyByteLimit.GetDynamicByteLimit();
    return resultSize && resultByteSize;
}

void TRequestQueue::DecrementConcurrency(i64 requestTotalSize)
{
    auto newConcurrencySemaphore = --Concurrency_;
    auto newConcurrencyByteSemaphore = ConcurrencyByte_.fetch_sub(requestTotalSize);

    YT_ASSERT(newConcurrencySemaphore >= 0);
    YT_ASSERT(newConcurrencyByteSemaphore >= 0);
}

bool TRequestQueue::AreThrottlersOverdrafted() const
{
    auto isOverdrafted = [] (const TRequestThrottler& throttler) {
        return throttler.Specified.load(std::memory_order::relaxed) && throttler.Throttler->IsOverdraft();
    };
    return isOverdrafted(BytesThrottler_) || isOverdrafted(WeightThrottler_);
}

void TRequestQueue::AcquireThrottlers(const TServiceBase::TServiceContextPtr& context)
{
    if (BytesThrottler_.Specified.load(std::memory_order::acquire)) {
        // Slow path.
        auto requestSize = context->GetTotalSize();
        BytesThrottler_.Throttler->Acquire(requestSize);
    }
    if (WeightThrottler_.Specified.load(std::memory_order::acquire)) {
        // Slow path.
        const auto& header = context->GetRequestHeader();
        if (header.has_logical_request_weight()) {
            WeightThrottler_.Throttler->Acquire(header.logical_request_weight());
        }
    }
}

void TRequestQueue::SubscribeToThrottlers()
{
    if (Throttled_.load(std::memory_order::relaxed) || Throttled_.exchange(true)) {
        return;
    }

    std::vector<TFuture<void>> futures;
    futures.reserve(2);
    if (BytesThrottler_.Specified.load(std::memory_order::acquire)) {
        futures.push_back(BytesThrottler_.Throttler->GetAvailableFuture());
    }
    if (WeightThrottler_.Specified.load(std::memory_order::acquire)) {
        futures.push_back(WeightThrottler_.Throttler->GetAvailableFuture());
    }

    AllSucceeded(std::move(futures))
        .Subscribe(BIND([=, this, this_ = MakeStrong(this), weakService = MakeWeak(Service_)] (const TError&) {
            if (auto service = weakService.Lock()) {
                Throttled_.store(false, std::memory_order::release);
                ScheduleRequestsFromQueue();
            }
        })
        .Via(GetCurrentInvoker()));
}

////////////////////////////////////////////////////////////////////////////////

struct TServiceBase::TRuntimeMethodInfo::TPerformanceCountersKeyEquals
{
    bool operator()(
        const TNonowningPerformanceCountersKey& lhs,
        const TNonowningPerformanceCountersKey& rhs) const
    {
        return lhs == rhs;
    }

    bool operator()(
        const TOwningPerformanceCountersKey& lhs,
        const TNonowningPerformanceCountersKey& rhs) const
    {
        const auto& [lhsUserTag, lhsRequestQueue] = lhs;
        return TNonowningPerformanceCountersKey{lhsUserTag, lhsRequestQueue} == rhs;
    }
};

////////////////////////////////////////////////////////////////////////////////

TServiceBase::TServiceBase(
    IInvokerPtr defaultInvoker,
    const TServiceDescriptor& descriptor,
    const NLogging::TLogger& logger,
    TRealmId realmId,
    IAuthenticatorPtr authenticator)
    : TServiceBase(
        std::move(defaultInvoker),
        descriptor,
        GetNullMemoryUsageTracker(),
        logger,
        realmId,
        authenticator)
{ }

TServiceBase::TServiceBase(
    IInvokerPtr defaultInvoker,
    const TServiceDescriptor& descriptor,
    IMemoryUsageTrackerPtr memoryUsageTracker,
    const NLogging::TLogger& logger,
    TRealmId realmId,
    IAuthenticatorPtr authenticator)
    : Logger(logger)
    , DefaultInvoker_(std::move(defaultInvoker))
    , Authenticator_(std::move(authenticator))
    , ServiceDescriptor_(descriptor)
    , ServiceId_(descriptor.FullServiceName, realmId)
    , MemoryUsageTracker_(std::move(memoryUsageTracker))
    , Profiler_(RpcServerProfiler.WithHot().WithTag("yt_service", TString(ServiceId_.ServiceName)))
    , AuthenticationTimer_(Profiler_.Timer("/authentication_time"))
    , ServiceLivenessChecker_(New<TPeriodicExecutor>(
        TDispatcher::Get()->GetLightInvoker(),
        BIND(&TServiceBase::OnServiceLivenessCheck, MakeWeak(this)),
        ServiceLivenessCheckPeriod))
    , PerformanceCounters_(New<TServiceBase::TPerformanceCounters>(RpcServerProfiler))
{
    RegisterMethod(RPC_SERVICE_METHOD_DESC(Discover)
        .SetInvoker(TDispatcher::Get()->GetHeavyInvoker())
        .SetConcurrencyLimit(1'000'000)
        .SetSystem(true));

    Profiler_.AddFuncGauge("/authentication_queue_size", MakeStrong(this), [this] {
        return AuthenticationQueueSize_.load(std::memory_order::relaxed);
    });

    ServiceLivenessChecker_->Start();
}

const TServiceId& TServiceBase::GetServiceId() const
{
    return ServiceId_;
}

void TServiceBase::HandleRequest(
    std::unique_ptr<NProto::TRequestHeader> header,
    TSharedRefArray message,
    IBusPtr replyBus)
{
    SetActive();

    auto method = FromProto<TString>(header->method());
    auto requestId = FromProto<TRequestId>(header->request_id());

    auto replyError = [&] (TError error) {
        ReplyError(std::move(error), *header, replyBus);
    };

    if (IsStopped()) {
        replyError(TError(
            NRpc::EErrorCode::Unavailable,
            "Service is stopped"));
        return;
    }

    if (auto error = DoCheckRequestCompatibility(*header); !error.IsOK()) {
        replyError(std::move(error));
        return;
    }

    auto* runtimeInfo = FindMethodInfo(method);
    if (!runtimeInfo) {
        replyError(TError(
            NRpc::EErrorCode::NoSuchMethod,
            "Unknown method"));
        return;
    }

    auto memoryGuard = TMemoryUsageTrackerGuard::Acquire(MemoryUsageTracker_, TypicalRequestSize);
    message = TrackMemory(MemoryUsageTracker_, std::move(message));
    if (MemoryUsageTracker_ && MemoryUsageTracker_->IsExceeded()) {
        return replyError(TError(
            NRpc::EErrorCode::MemoryPressure,
            "Request is dropped due to high memory pressure"));
    }

    auto tracingMode = runtimeInfo->TracingMode.load(std::memory_order::relaxed);
    auto traceContext = tracingMode == ERequestTracingMode::Disable
        ? NTracing::TTraceContextPtr()
        : GetOrCreateHandlerTraceContext(*header, tracingMode == ERequestTracingMode::Force);
    if (traceContext && traceContext->IsRecorded()) {
        traceContext->AddTag(EndpointAnnotation, replyBus->GetEndpointDescription());
    }

    auto* requestQueue = GetRequestQueue(runtimeInfo, *header);
    RegisterRequestQueue(runtimeInfo, requestQueue);

    auto maybeThrottled = GetThrottledError(*header);

    if (requestQueue->IsQueueSizeLimitExceeded()) {
        runtimeInfo->RequestQueueSizeLimitErrorCounter.Increment();
        replyError(TError(
            NRpc::EErrorCode::RequestQueueSizeLimitExceeded,
            "Request queue size limit exceeded")
            << TErrorAttribute("limit", runtimeInfo->QueueSizeLimit.load())
            << TErrorAttribute("queue", requestQueue->GetName())
            << maybeThrottled);
        return;
    }

    if (requestQueue->IsQueueByteSizeLimitExceeded()) {
        runtimeInfo->RequestQueueByteSizeLimitErrorCounter.Increment();
        replyError(TError(
            NRpc::EErrorCode::RequestQueueSizeLimitExceeded,
            "Request queue bytes size limit exceeded")
            << TErrorAttribute("limit", runtimeInfo->QueueByteSizeLimit.load())
            << TErrorAttribute("queue", requestQueue->GetName())
            << maybeThrottled);
        return;
    }

    TCurrentTraceContextGuard traceContextGuard(traceContext);

    // NOTE: Do not use replyError() after this line.
    TAcceptedRequest acceptedRequest{
        requestId,
        std::move(replyBus),
        std::move(runtimeInfo),
        std::move(traceContext),
        std::move(header),
        std::move(message),
        requestQueue,
        maybeThrottled,
        std::move(memoryGuard),
        MemoryUsageTracker_,
    };

    if (!IsAuthenticationNeeded(acceptedRequest)) {
        HandleAuthenticatedRequest(std::move(acceptedRequest));
        return;
    }

    // Not actually atomic but should work fine as long as some small error is OK.
    auto authenticationQueueSizeLimit = AuthenticationQueueSizeLimit_.load(std::memory_order::relaxed);
    auto authenticationQueueSize = AuthenticationQueueSize_.load(std::memory_order::relaxed);
    if (authenticationQueueSize > authenticationQueueSizeLimit) {
        auto error = TError(
            NRpc::EErrorCode::RequestQueueSizeLimitExceeded,
            "Authentication request queue size limit exceeded")
            << TErrorAttribute("limit", authenticationQueueSizeLimit);
        ReplyError(error, *acceptedRequest.Header, acceptedRequest.ReplyBus);
        return;
    }
    ++AuthenticationQueueSize_;

    NProfiling::TWallTimer timer;

    TAuthenticationContext authenticationContext{
        .Header = acceptedRequest.Header.get(),
        .UserIP = acceptedRequest.ReplyBus->GetEndpointNetworkAddress(),
        .IsLocal = acceptedRequest.ReplyBus->IsEndpointLocal(),
    };
    if (Authenticator_->CanAuthenticate(authenticationContext)) {
        auto asyncAuthResult = Authenticator_->AsyncAuthenticate(authenticationContext);
        if (asyncAuthResult.IsSet()) {
            OnRequestAuthenticated(timer, std::move(acceptedRequest), asyncAuthResult.Get());
        } else {
            asyncAuthResult.Subscribe(
                BIND(&TServiceBase::OnRequestAuthenticated, MakeStrong(this), timer, Passed(std::move(acceptedRequest))));
        }
    } else {
        OnRequestAuthenticated(timer, std::move(acceptedRequest), TError(
            NYT::NRpc::EErrorCode::AuthenticationError,
            "Request is missing credentials"));
    }
}

void TServiceBase::ReplyError(
    TError error,
    const NProto::TRequestHeader& header,
    const IBusPtr& replyBus)
{
    auto requestId = FromProto<TRequestId>(header.request_id());
    auto richError = std::move(error)
        << TErrorAttribute("request_id", requestId)
        << TErrorAttribute("realm_id", ServiceId_.RealmId)
        << TErrorAttribute("service", ServiceId_.ServiceName)
        << TErrorAttribute("method", header.method())
        << TErrorAttribute("endpoint", replyBus->GetEndpointDescription());

    auto code = richError.GetCode();
    auto logLevel =
        code == NRpc::EErrorCode::NoSuchMethod || code == NRpc::EErrorCode::ProtocolError
        ? NLogging::ELogLevel::Warning
        : NLogging::ELogLevel::Debug;
    YT_LOG_EVENT(Logger, logLevel, richError);

    auto errorMessage = CreateErrorResponseMessage(requestId, richError);
    YT_UNUSED_FUTURE(replyBus->Send(errorMessage));
}

void TServiceBase::OnMethodError(const TError& /*error*/, const TString& /*method*/)
{ }

void TServiceBase::OnRequestAuthenticated(
    const NProfiling::TWallTimer& timer,
    TAcceptedRequest&& acceptedRequest,
    const TErrorOr<TAuthenticationResult>& authResultOrError)
{
    AuthenticationTimer_.Record(timer.GetElapsedTime());
    --AuthenticationQueueSize_;

    auto& requestHeader = *acceptedRequest.Header;

    if (authResultOrError.IsOK()) {
        const auto& authResult = authResultOrError.Value();
        const auto& Logger = RpcServerLogger;
        YT_LOG_DEBUG("Request authenticated (RequestId: %v, User: %v, Realm: %v)",
            acceptedRequest.RequestId,
            authResult.User,
            authResult.Realm);
        const auto& authenticatedUser = authResult.User;
        if (requestHeader.has_user()) {
            const auto& user = requestHeader.user();
            if (user != authenticatedUser) {
                ReplyError(
                    TError(
                        NRpc::EErrorCode::AuthenticationError,
                        "Manually specified and authenticated users mismatch")
                        << TErrorAttribute("user", user)
                        << TErrorAttribute("authenticated_user", authenticatedUser),
                    requestHeader,
                    acceptedRequest.ReplyBus);
                return;
            }
        }
        requestHeader.set_user(std::move(authResult.User));

        auto* credentialsExt = requestHeader.MutableExtension(
            NRpc::NProto::TCredentialsExt::credentials_ext);
        if (credentialsExt->user_ticket().empty()) {
            credentialsExt->set_user_ticket(std::move(authResult.UserTicket));
        }
        HandleAuthenticatedRequest(std::move(acceptedRequest));
    } else {
        ReplyError(
            TError(
                NRpc::EErrorCode::AuthenticationError,
                "Request authentication failed")
                << authResultOrError,
            requestHeader,
            acceptedRequest.ReplyBus);
    }
}

bool TServiceBase::IsAuthenticationNeeded(const TAcceptedRequest& acceptedRequest)
{
    return
        Authenticator_.operator bool() &&
        !acceptedRequest.RuntimeInfo->Descriptor.System;
}

void TServiceBase::HandleAuthenticatedRequest(TAcceptedRequest&& acceptedRequest)
{
    if (!acceptedRequest.ReplyBus->IsEndpointLocal()) {
        bool authenticated = acceptedRequest.Header->HasExtension(NRpc::NProto::TCredentialsExt::credentials_ext) &&
            acceptedRequest.Header->GetExtension(NRpc::NProto::TCredentialsExt::credentials_ext).has_service_ticket();
        if (!authenticated) {
            acceptedRequest.RuntimeInfo->UnauthenticatedRequestsCounter.Increment();
        }
    }

    auto context = New<TServiceContext>(
        this,
        std::move(acceptedRequest),
        Logger);
    auto* requestQueue = context->GetRequestQueue();
    requestQueue->OnRequestArrived(std::move(context));
}

TRequestQueue* TServiceBase::GetRequestQueue(
    TRuntimeMethodInfo* runtimeInfo,
    const NRpc::NProto::TRequestHeader& requestHeader)
{
    TRequestQueue* requestQueue = nullptr;
    if (auto& provider = runtimeInfo->Descriptor.RequestQueueProvider) {
        requestQueue = provider->GetQueue(requestHeader);
    }
    if (!requestQueue) {
        requestQueue = runtimeInfo->DefaultRequestQueue.Get();
    }
    return requestQueue;
}

void TServiceBase::RegisterRequestQueue(
    TRuntimeMethodInfo* runtimeInfo,
    TRequestQueue* requestQueue)
{
    if (!requestQueue->Register(this, runtimeInfo)) {
        return;
    }

    const auto& method = runtimeInfo->Descriptor.Method;
    YT_LOG_DEBUG("Request queue registered (Method: %v, Queue: %v)",
        method,
        requestQueue->GetName());

    auto profiler = runtimeInfo->Profiler.WithSparse();
    if (runtimeInfo->Descriptor.RequestQueueProvider) {
        profiler = profiler.WithTag("queue", requestQueue->GetName());
    }
    profiler.AddFuncGauge("/request_queue_size", MakeStrong(this), [=] {
        return requestQueue->GetQueueSize();
    });
    profiler.AddFuncGauge("/request_queue_byte_size", MakeStrong(this), [=] {
        return requestQueue->GetQueueByteSize();
    });
    profiler.AddFuncGauge("/concurrency", MakeStrong(this), [=] {
        return requestQueue->GetConcurrency();
    });
    profiler.AddFuncGauge("/concurrency_byte", MakeStrong(this), [=] {
        return requestQueue->GetConcurrencyByte();
    });

    TMethodConfigPtr methodConfig;
    if (auto config = Config_.Acquire()) {
        methodConfig = GetOrDefault(config->Methods, method);
    }
    ConfigureRequestQueue(runtimeInfo, requestQueue, methodConfig);

    {
        auto guard = Guard(runtimeInfo->RequestQueuesLock);
        runtimeInfo->RequestQueues.push_back(requestQueue);
    }
}

void TServiceBase::ConfigureRequestQueue(
    TRuntimeMethodInfo* runtimeInfo,
    TRequestQueue* requestQueue,
    const TMethodConfigPtr& config)
{
    if (auto& provider = runtimeInfo->Descriptor.RequestQueueProvider;
        provider &&
        requestQueue != runtimeInfo->DefaultRequestQueue.Get())
    {
        provider->ConfigureQueue(requestQueue, config);
    } else if (config) {
        requestQueue->Configure(config);
    }
}

void TServiceBase::HandleRequestCancellation(TRequestId requestId)
{
    SetActive();

    if (TryCancelQueuedReply(requestId)) {
        return;
    }

    if (auto context = FindRequest(requestId)) {
        context->Cancel();
        return;
    }

    YT_LOG_DEBUG("Received cancelation for an unknown request, ignored (RequestId: %v)",
        requestId);
}

void TServiceBase::HandleStreamingPayload(
    TRequestId requestId,
    const TStreamingPayload& payload)
{
    SetActive();

    auto* bucket = GetRequestBucket(requestId);
    auto guard = Guard(bucket->Lock);
    auto context = DoFindRequest(bucket, requestId);
    if (context) {
        guard.Release();
        context->HandleStreamingPayload(payload);
    } else {
        auto* entry = DoGetOrCreatePendingPayloadsEntry(bucket, requestId);
        entry->Payloads.emplace_back(payload);
        guard.Release();
        YT_LOG_DEBUG("Received streaming payload for an unknown request, saving (RequestId: %v)",
            requestId);
    }
}

void TServiceBase::HandleStreamingFeedback(
    TRequestId requestId,
    const TStreamingFeedback& feedback)
{
    auto context = FindRequest(requestId);
    if (!context) {
        YT_LOG_DEBUG("Received streaming feedback for an unknown request, ignored (RequestId: %v)",
            requestId);
        return;
    }

    context->HandleStreamingFeedback(feedback);
}

void TServiceBase::DoDeclareServerFeature(int featureId)
{
    ValidateInactive();

    // Failure here means that such feature is already registered.
    YT_VERIFY(SupportedServerFeatureIds_.insert(featureId).second);
}

TError TServiceBase::DoCheckRequestCompatibility(const NRpc::NProto::TRequestHeader& header)
{
    if (auto error = DoCheckRequestProtocol(header); !error.IsOK()) {
        return error;
    }
    if (auto error = DoCheckRequestFeatures(header); !error.IsOK()) {
        return error;
    }
    if (auto error = DoCheckRequestCodecs(header); !error.IsOK()) {
        return error;
    }
    return {};
}

TError TServiceBase::DoCheckRequestProtocol(const NRpc::NProto::TRequestHeader& header)
{
    TProtocolVersion requestProtocolVersion{
        header.protocol_version_major(),
        header.protocol_version_minor()
    };

    if (requestProtocolVersion.Major != GenericProtocolVersion.Major) {
        if (ServiceDescriptor_.ProtocolVersion.Major != requestProtocolVersion.Major) {
            return TError(
                NRpc::EErrorCode::ProtocolError,
                "Server major protocol version differs from client major protocol version: %v != %v",
                requestProtocolVersion.Major,
                ServiceDescriptor_.ProtocolVersion.Major);
        }

        if (ServiceDescriptor_.ProtocolVersion.Minor < requestProtocolVersion.Minor) {
            return TError(
                NRpc::EErrorCode::ProtocolError,
                "Server minor protocol version is less than minor protocol version required by client: %v < %v",
                ServiceDescriptor_.ProtocolVersion.Minor,
                requestProtocolVersion.Minor);
        }
    }
    return {};
}

TError TServiceBase::DoCheckRequestFeatures(const NRpc::NProto::TRequestHeader& header)
{
    for (auto featureId : header.required_server_feature_ids()) {
        if (!SupportedServerFeatureIds_.contains(featureId)) {
            return TError(
                NRpc::EErrorCode::UnsupportedServerFeature,
                "Server does not support the feature requested by client")
                << TErrorAttribute("feature_id", featureId);
        }
    }
    return {};
}

TError TServiceBase::DoCheckRequestCodecs(const NRpc::NProto::TRequestHeader& header)
{
    if (header.has_request_codec()) {
        NCompression::ECodec requestCodec;
        if (!TryEnumCast(header.request_codec(), &requestCodec)) {
            return TError(
                NRpc::EErrorCode::ProtocolError,
                "Request codec %v is not supported",
                header.request_codec());
        }
    }
    if (header.has_response_codec()) {
        NCompression::ECodec responseCodec;
        if (!TryEnumCast(header.response_codec(), &responseCodec)) {
            return TError(
                NRpc::EErrorCode::ProtocolError,
                "Response codec %v is not supported",
                header.response_codec());
        }
    }
    return {};
}

void TServiceBase::OnRequestTimeout(TRequestId requestId, ERequestProcessingStage stage, bool /*aborted*/)
{
    auto context = FindRequest(requestId);
    if (!context) {
        return;
    }

    context->HandleTimeout(stage);
}

void TServiceBase::OnReplyBusTerminated(const NYT::TWeakPtr<NYT::NBus::IBus>& busWeak, const TError& error)
{
    std::vector<TServiceContextPtr> contexts;
    if (auto bus = busWeak.Lock()) {
        auto* bucket = GetReplyBusBucket(bus);
        auto guard = Guard(bucket->Lock);
        auto it = bucket->ReplyBusToContexts.find(bus);
        if (it == bucket->ReplyBusToContexts.end()) {
            return;
        }

        for (auto* rawContext : it->second) {
            auto context = DangerousGetPtr(rawContext);
            if (context) {
                contexts.push_back(context);
            }
        }

        bucket->ReplyBusToContexts.erase(it);
    }

    for (auto context : contexts) {
        YT_LOG_DEBUG(error, "Reply bus terminated, canceling request (RequestId: %v)",
            context->GetRequestId());
        context->Cancel();
    }
}

TServiceBase::TRequestBucket* TServiceBase::GetRequestBucket(TRequestId requestId)
{
    return &RequestBuckets_[THash<TRequestId>()(requestId) % RequestBucketCount];
}

TServiceBase::TReplyBusBucket* TServiceBase::GetReplyBusBucket(const IBusPtr& bus)
{
    return &ReplyBusBuckets_[THash<IBusPtr>()(bus) % ReplyBusBucketCount];
}

TServiceBase::TQueuedReplyBucket* TServiceBase::GetQueuedReplyBucket(TRequestId requestId)
{
    return &QueuedReplyBusBuckets_[THash<TRequestId>()(requestId) % QueuedReplyBucketCount];
}

void TServiceBase::RegisterRequest(TServiceContext* context)
{
    auto requestId = context->GetRequestId();
    {
        auto* bucket = GetRequestBucket(requestId);
        auto guard = Guard(bucket->Lock);
        // NB: We're OK with duplicate request ids.
        bucket->RequestIdToContext.emplace(requestId, context);
    }

    const auto& replyBus = context->GetReplyBus();
    bool subscribe = false;
    {
        auto* bucket = GetReplyBusBucket(replyBus);
        auto guard = Guard(bucket->Lock);
        auto [it, inserted] = bucket->ReplyBusToContexts.try_emplace(replyBus);
        subscribe = inserted;
        auto& contexts = it->second;
        contexts.insert(context);
    }

    if (subscribe) {
        replyBus->SubscribeTerminated(BIND(&TServiceBase::OnReplyBusTerminated, MakeWeak(this), MakeWeak(replyBus.Get())));
    }

    auto pendingPayloads = GetAndErasePendingPayloads(requestId);
    if (!pendingPayloads.empty()) {
        YT_LOG_DEBUG("Pulling pending streaming payloads for a late request (RequestId: %v, PayloadCount: %v)",
            requestId,
            pendingPayloads.size());
        for (const auto& payload : pendingPayloads) {
            context->HandleStreamingPayload(payload);
        }
    }
}

void TServiceBase::UnregisterRequest(TServiceContext* context)
{
    auto requestId = context->GetRequestId();
    {
        auto* bucket = GetRequestBucket(requestId);
        auto guard = Guard(bucket->Lock);
        // NB: We're OK with duplicate request ids.
        bucket->RequestIdToContext.erase(requestId);
    }

    const auto& replyBus = context->GetReplyBus();
    {
        auto* bucket = GetReplyBusBucket(replyBus);
        auto guard = Guard(bucket->Lock);
        auto it = bucket->ReplyBusToContexts.find(replyBus);
        // Missing replyBus in ReplyBusToContexts is OK; see OnReplyBusTerminated.
        if (it != bucket->ReplyBusToContexts.end()) {
            auto& contexts = it->second;
            contexts.erase(context);
            if (contexts.empty()) {
                bucket->ReplyBusToContexts.erase(it);
            }
        }
    }
}

TServiceBase::TServiceContextPtr TServiceBase::FindRequest(TRequestId requestId)
{
    auto* bucket = GetRequestBucket(requestId);
    auto guard = Guard(bucket->Lock);
    return DoFindRequest(bucket, requestId);
}

TServiceBase::TServiceContextPtr TServiceBase::DoFindRequest(TRequestBucket* bucket, TRequestId requestId)
{
    auto it = bucket->RequestIdToContext.find(requestId);
    return it == bucket->RequestIdToContext.end() ? nullptr : DangerousGetPtr(it->second);
}

void TServiceBase::RegisterQueuedReply(TRequestId requestId, TFuture<void> reply)
{
    auto* bucket = GetQueuedReplyBucket(requestId);
    auto guard = Guard(bucket->Lock);
    bucket->QueuedReplies.emplace(requestId, std::move(reply));
}

void TServiceBase::UnregisterQueuedReply(TRequestId requestId)
{
    auto* bucket = GetQueuedReplyBucket(requestId);
    auto guard = Guard(bucket->Lock);
    bucket->QueuedReplies.erase(requestId);
}

bool TServiceBase::TryCancelQueuedReply(TRequestId requestId)
{
    TFuture<void> queuedReply;

    {
        auto* bucket = GetQueuedReplyBucket(requestId);
        auto guard = Guard(bucket->Lock);
        if (auto it = bucket->QueuedReplies.find(requestId); it != bucket->QueuedReplies.end()) {
            queuedReply = it->second;
            bucket->QueuedReplies.erase(it);
        }
    }

    if (queuedReply) {
        queuedReply.Cancel(TError());
        return true;
    } else {
        return false;
    }
}

TServiceBase::TPendingPayloadsEntry* TServiceBase::DoGetOrCreatePendingPayloadsEntry(TRequestBucket* bucket, TRequestId requestId)
{
    auto& entry = bucket->RequestIdToPendingPayloads[requestId];
    if (!entry.Lease) {
        entry.Lease = NConcurrency::TLeaseManager::CreateLease(
            PendingPayloadsTimeout_.load(std::memory_order::relaxed),
            BIND(&TServiceBase::OnPendingPayloadsLeaseExpired, MakeWeak(this), requestId));
    }

    return &entry;
}

std::vector<TStreamingPayload> TServiceBase::GetAndErasePendingPayloads(TRequestId requestId)
{
    auto* bucket = GetRequestBucket(requestId);

    TPendingPayloadsEntry entry;
    {
        auto guard = Guard(bucket->Lock);
        auto it = bucket->RequestIdToPendingPayloads.find(requestId);
        if (it == bucket->RequestIdToPendingPayloads.end()) {
            return {};
        }
        entry = std::move(it->second);
        bucket->RequestIdToPendingPayloads.erase(it);
    }

    NConcurrency::TLeaseManager::CloseLease(entry.Lease);
    return std::move(entry.Payloads);
}

void TServiceBase::OnPendingPayloadsLeaseExpired(TRequestId requestId)
{
    auto payloads = GetAndErasePendingPayloads(requestId);
    if (!payloads.empty()) {
        YT_LOG_DEBUG("Pending payloads lease expired, erasing (RequestId: %v, PayloadCount: %v)",
            requestId,
            payloads.size());
    }
}

TServiceBase::TMethodPerformanceCountersPtr TServiceBase::CreateMethodPerformanceCounters(
    TRuntimeMethodInfo* runtimeInfo,
    const TRuntimeMethodInfo::TNonowningPerformanceCountersKey& key)
{
    const auto& [userTag, requestQueue] = key;

    auto profiler = runtimeInfo->Profiler.WithSparse();
    if (userTag) {
        profiler = profiler.WithTag("user", TString(userTag));
    }
    if (runtimeInfo->Descriptor.RequestQueueProvider) {
        profiler = profiler.WithTag("queue", requestQueue->GetName());
    }
    return New<TMethodPerformanceCounters>(profiler, TimeHistogramConfig_.Acquire());
}

TServiceBase::TMethodPerformanceCounters* TServiceBase::GetMethodPerformanceCounters(
    TRuntimeMethodInfo* runtimeInfo,
    const TRuntimeMethodInfo::TNonowningPerformanceCountersKey& key)
{
    auto [userTag, requestQueue] = key;

    // Fast path.
    if (userTag == RootUserName && requestQueue == runtimeInfo->DefaultRequestQueue.Get()) {
        if (EnablePerUserProfiling_.load(std::memory_order::relaxed)) {
            return runtimeInfo->RootPerformanceCounters.Get();
        } else {
            return runtimeInfo->BasePerformanceCounters.Get();
        }
    }

    if (!EnablePerUserProfiling_.load(std::memory_order::relaxed)) {
        userTag = {};
    }
    auto actualKey = TRuntimeMethodInfo::TNonowningPerformanceCountersKey{userTag, requestQueue};
    return runtimeInfo->PerformanceCountersMap.FindOrInsert(actualKey, [&] {
        return CreateMethodPerformanceCounters(runtimeInfo, actualKey);
    }).first->Get();
}

TServiceBase::TPerformanceCounters* TServiceBase::GetPerformanceCounters()
{
    return PerformanceCounters_.Get();
}

void TServiceBase::SetActive()
{
    // Fast path.
    if (Active_.load(std::memory_order::relaxed)) {
        return;
    }

    // Slow path.
    Active_.store(true);
}

void TServiceBase::ValidateInactive()
{
    // Failure here means that some service metadata (e.g. registered methods or supported
    // features) are changing after the service has started serving requests.
    YT_VERIFY(!Active_.load(std::memory_order::relaxed));
}

bool TServiceBase::IsStopped()
{
    return Stopped_.load();
}

void TServiceBase::IncrementActiveRequestCount()
{
    ++ActiveRequestCount_;
}

void TServiceBase::DecrementActiveRequestCount()
{
    if (--ActiveRequestCount_ == 0 && IsStopped()) {
        StopResult_.TrySet();
    }
}

void TServiceBase::InitContext(IServiceContext* /*context*/)
{ }

void TServiceBase::RegisterDiscoverRequest(const TCtxDiscoverPtr& context)
{
    auto payload = GetDiscoverRequestPayload(context);
    auto replyDelay = FromProto<TDuration>(context->Request().reply_delay());

    auto readerGuard = ReaderGuard(DiscoverRequestsByPayloadLock_);
    auto it = DiscoverRequestsByPayload_.find(payload);
    if (it == DiscoverRequestsByPayload_.end()) {
        readerGuard.Release();
        auto writerGuard = WriterGuard(DiscoverRequestsByPayloadLock_);
        DiscoverRequestsByPayload_[payload].Insert(context, 0);
    } else {
        auto& requestSet = it->second;
        requestSet.Insert(context, 0);
    }

    TDelayedExecutor::Submit(
        BIND(&TServiceBase::OnDiscoverRequestReplyDelayReached, MakeStrong(this), context),
        replyDelay,
        TDispatcher::Get()->GetHeavyInvoker());
}

void TServiceBase::ReplyDiscoverRequest(const TCtxDiscoverPtr& context, bool isUp)
{
    auto& response = context->Response();
    response.set_up(isUp);
    ToProto(response.mutable_suggested_addresses(), SuggestAddresses());

    context->SetResponseInfo("Up: %v, SuggestedAddresses: %v",
        response.up(),
        response.suggested_addresses());

    context->Reply();
}

void TServiceBase::OnDiscoverRequestReplyDelayReached(TCtxDiscoverPtr context)
{
    auto readerGuard = ReaderGuard(DiscoverRequestsByPayloadLock_);

    if (context->IsReplied()) {
        return;
    }

    auto payload = GetDiscoverRequestPayload(context);
    auto it = DiscoverRequestsByPayload_.find(payload);
    if (it != DiscoverRequestsByPayload_.end()) {
        auto& requestSet = it->second;
        if (requestSet.Has(context)) {
            requestSet.Remove(context);
            ReplyDiscoverRequest(context, IsUp(context));
        }
    }
}

TString TServiceBase::GetDiscoverRequestPayload(const TCtxDiscoverPtr& context)
{
    auto request = context->Request();
    request.set_reply_delay(0);
    return SerializeProtoToString(request);
}

void TServiceBase::OnServiceLivenessCheck()
{
    std::vector<TDiscoverRequestSet> requestsToReply;

    {
        auto writerGuard = WriterGuard(DiscoverRequestsByPayloadLock_);

        std::vector<TString> payloadsToReply;
        for (const auto& [payload, requests] : DiscoverRequestsByPayload_) {
            auto empty = true;
            auto isUp = false;
            for (const auto& bucket : requests.Buckets) {
                const auto& map = bucket.GetMap();
                if (!map.empty()) {
                    empty = false;
                    const auto& request = map.begin()->first;
                    isUp = IsUp(request);
                    break;
                }
            }
            if (empty || isUp) {
                payloadsToReply.push_back(payload);
            }
        }

        for (const auto& payload : payloadsToReply) {
            auto& requests = DiscoverRequestsByPayload_[payload];
            requestsToReply.push_back(std::move(requests));
            YT_VERIFY(DiscoverRequestsByPayload_.erase(payload) > 0);
        }
    }

    TDispatcher::Get()->GetHeavyInvoker()->Invoke(
        BIND([this, this_ = MakeStrong(this), requestsToReply = std::move(requestsToReply)] {
            for (const auto& requests : requestsToReply) {
                for (const auto& bucket : requests.Buckets) {
                    for (const auto& [request, _] : bucket.GetMap()) {
                        ReplyDiscoverRequest(request, /*isUp*/ true);
                    }
                    NConcurrency::Yield();
                }
            }
        }));
}

TServiceBase::TRuntimeMethodInfoPtr TServiceBase::RegisterMethod(const TMethodDescriptor& descriptor)
{
    ValidateInactive();

    auto runtimeInfo = New<TRuntimeMethodInfo>(ServiceId_, descriptor, Profiler_);

    runtimeInfo->BasePerformanceCounters = CreateMethodPerformanceCounters(
        runtimeInfo.Get(),
        {{}, runtimeInfo->DefaultRequestQueue.Get()});
    runtimeInfo->RootPerformanceCounters = CreateMethodPerformanceCounters(
        runtimeInfo.Get(),
        {RootUserName, runtimeInfo->DefaultRequestQueue.Get()});

    runtimeInfo->Heavy.store(descriptor.Options.Heavy);
    runtimeInfo->QueueSizeLimit.store(descriptor.QueueSizeLimit);
    runtimeInfo->QueueByteSizeLimit.store(descriptor.QueueByteSizeLimit);
    runtimeInfo->ConcurrencyLimit.Reconfigure(descriptor.ConcurrencyLimit);
    runtimeInfo->ConcurrencyByteLimit.Reconfigure(descriptor.ConcurrencyByteLimit);
    runtimeInfo->LogLevel.store(descriptor.LogLevel);
    runtimeInfo->LoggingSuppressionTimeout.store(descriptor.LoggingSuppressionTimeout);

    // Failure here means that such method is already registered.
    YT_VERIFY(MethodMap_.emplace(descriptor.Method, runtimeInfo).second);

    auto& profiler = runtimeInfo->Profiler;
    profiler.AddFuncGauge("/request_queue_size_limit", MakeStrong(this), [=] {
        return runtimeInfo->QueueSizeLimit.load(std::memory_order::relaxed);
    });
    profiler.AddFuncGauge("/request_queue_byte_size_limit", MakeStrong(this), [=] {
        return runtimeInfo->QueueByteSizeLimit.load(std::memory_order::relaxed);
    });
    profiler.AddFuncGauge("/concurrency_limit", MakeStrong(this), [=] {
        return runtimeInfo->ConcurrencyLimit.GetDynamicLimit();
    });
    profiler.AddFuncGauge("/concurrency_byte_limit", MakeStrong(this), [=] {
        return runtimeInfo->ConcurrencyByteLimit.GetDynamicByteLimit();
    });

    return runtimeInfo;
}

void TServiceBase::ValidateRequestFeatures(const IServiceContextPtr& context)
{
    const auto& header = context->RequestHeader();
    if (auto error = DoCheckRequestFeatures(header); !error.IsOK()) {
        auto requestId = FromProto<TRequestId>(header.request_id());
        THROW_ERROR std::move(error)
            << TErrorAttribute("request_id", requestId)
            << TErrorAttribute("service", header.service())
            << TErrorAttribute("method", header.method());
    }
}

void TServiceBase::DoConfigureHistogramTimer(
    const TServiceCommonConfigPtr& configDefaults,
    const TServiceConfigPtr& config)
{
    TTimeHistogramConfigPtr newTimeHistogramConfig;
    if (config->TimeHistogram) {
        newTimeHistogramConfig = config->TimeHistogram;
    } else if (configDefaults->TimeHistogram) {
        newTimeHistogramConfig = configDefaults->TimeHistogram;
    }
    if (newTimeHistogramConfig) {
        TimeHistogramConfig_.Store(std::move(newTimeHistogramConfig));
    }
}

void TServiceBase::DoConfigure(
    const TServiceCommonConfigPtr& configDefaults,
    const TServiceConfigPtr& config)
{
    try {
        YT_LOG_DEBUG("Configuring RPC service (Service: %v)",
            ServiceId_.ServiceName);

        // Validate configuration.
        for (const auto& [methodName, _] : config->Methods) {
            auto* method = FindMethodInfo(methodName);
            if (!method) {
                // TODO(don-dron): Split service configs by realmid.
                YT_LOG_WARNING(
                    "Method is not registered (Service: %v, RealmId: %v, Method: %v)",
                    ServiceId_.ServiceName,
                    ServiceId_.RealmId,
                    methodName);
            }
        }

        EnablePerUserProfiling_.store(config->EnablePerUserProfiling.value_or(configDefaults->EnablePerUserProfiling));
        AuthenticationQueueSizeLimit_.store(config->AuthenticationQueueSizeLimit.value_or(DefaultAuthenticationQueueSizeLimit));
        PendingPayloadsTimeout_.store(config->PendingPayloadsTimeout.value_or(DefaultPendingPayloadsTimeout));
        EnableErrorCodeCounter_.store(config->EnableErrorCodeCounter.value_or(configDefaults->EnableErrorCodeCounter));

        DoConfigureHistogramTimer(configDefaults, config);

        for (const auto& [methodName, runtimeInfo] : MethodMap_) {
            auto methodIt = config->Methods.find(methodName);
            auto methodConfig = methodIt ? methodIt->second : New<TMethodConfig>();

            const auto& descriptor = runtimeInfo->Descriptor;

            runtimeInfo->Heavy.store(methodConfig->Heavy.value_or(descriptor.Options.Heavy));
            runtimeInfo->QueueSizeLimit.store(methodConfig->QueueSizeLimit.value_or(descriptor.QueueSizeLimit));
            runtimeInfo->QueueByteSizeLimit.store(methodConfig->QueueByteSizeLimit.value_or(descriptor.QueueByteSizeLimit));
            runtimeInfo->ConcurrencyLimit.Reconfigure(methodConfig->ConcurrencyLimit.value_or(descriptor.ConcurrencyLimit));
            runtimeInfo->ConcurrencyByteLimit.Reconfigure(methodConfig->ConcurrencyByteLimit.value_or(descriptor.ConcurrencyByteLimit));
            runtimeInfo->LogLevel.store(methodConfig->LogLevel.value_or(descriptor.LogLevel));
            runtimeInfo->LoggingSuppressionTimeout.store(methodConfig->LoggingSuppressionTimeout.value_or(descriptor.LoggingSuppressionTimeout));
            runtimeInfo->Pooled.store(methodConfig->Pooled.value_or(config->Pooled.value_or(descriptor.Pooled)));

            {
                auto guard = Guard(runtimeInfo->RequestQueuesLock);
                for (auto* requestQueue : runtimeInfo->RequestQueues) {
                    ConfigureRequestQueue(runtimeInfo.Get(), requestQueue, methodConfig);
                }
            }

            auto loggingSuppressionFailedRequestThrottlerConfig = methodConfig->LoggingSuppressionFailedRequestThrottler
                ? methodConfig->LoggingSuppressionFailedRequestThrottler
                : DefaultLoggingSuppressionFailedRequestThrottlerConfig;
            runtimeInfo->LoggingSuppressionFailedRequestThrottler->Reconfigure(loggingSuppressionFailedRequestThrottlerConfig);

            runtimeInfo->TracingMode.store(
                methodConfig->TracingMode.value_or(
                    config->TracingMode.value_or(
                        configDefaults->TracingMode)));
        }

        Config_.Store(config);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error configuring RPC service %v",
            ServiceId_.ServiceName)
            << TError(ex);
    }
}

std::optional<TError> TServiceBase::GetThrottledError(const NProto::TRequestHeader& /*requestHeader*/)
{
    return {};
}

void TServiceBase::Configure(
    const TServiceCommonConfigPtr& configDefaults,
    const INodePtr& configNode)
{
    TServiceConfigPtr config;
    if (configNode) {
        try {
            config = ConvertTo<TServiceConfigPtr>(configNode);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing RPC service %v config",
                ServiceId_.ServiceName)
                << TError(ex);
        }
    } else {
        config = New<TServiceConfig>();
    }
    DoConfigure(configDefaults, config);
}

TFuture<void> TServiceBase::Stop()
{
    if (!Stopped_.exchange(true)) {
        if (ActiveRequestCount_.load() == 0) {
            StopResult_.TrySet();
        }
    }

    YT_UNUSED_FUTURE(ServiceLivenessChecker_->Stop());

    return StopResult_.ToFuture();
}

TServiceBase::TRuntimeMethodInfo* TServiceBase::FindMethodInfo(const TString& method)
{
    auto it = MethodMap_.find(method);
    return it == MethodMap_.end() ? nullptr : it->second.Get();
}

TServiceBase::TRuntimeMethodInfo* TServiceBase::GetMethodInfoOrThrow(const TString& method)
{
    auto* runtimeInfo = FindMethodInfo(method);
    if (!runtimeInfo) {
        THROW_ERROR_EXCEPTION("Method %Qv is not registered",
            method);
    }
    return runtimeInfo;
}

const IInvokerPtr& TServiceBase::GetDefaultInvoker() const
{
    return DefaultInvoker_;
}

void TServiceBase::BeforeInvoke(NRpc::IServiceContext* /*context*/)
{ }

bool TServiceBase::IsUp(const TCtxDiscoverPtr& /*context*/)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return true;
}

void TServiceBase::EnrichDiscoverResponse(TRspDiscover* /*response*/)
{ }

std::vector<TString> TServiceBase::SuggestAddresses()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return std::vector<TString>();
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TServiceBase, Discover)
{
    auto replyDelay = FromProto<TDuration>(request->reply_delay());

    context->SetRequestInfo("ReplyDelay: %v",
        replyDelay);

    auto isUp = IsUp(context);
    EnrichDiscoverResponse(response);

    // Fast path.
    if (replyDelay == TDuration::Zero() || isUp) {
        ReplyDiscoverRequest(context, isUp);
        return;
    }

    RegisterDiscoverRequest(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

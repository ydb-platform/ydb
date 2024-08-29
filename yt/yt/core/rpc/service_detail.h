#pragma once

#include "public.h"

#include "client.h"
#include "dispatcher.h"
#include "server_detail.h"
#include "service.h"
#include "config.h"

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/moody_camel_concurrent_queue.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/yson/protobuf_interop.h>

#include <yt/yt/core/misc/object_pool.h>
#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/ring_queue.h>
#include <yt/yt/core/misc/memory_usage_tracker.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/rpc/message_format.h>

#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/library/syncmap/map.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>
#include <library/cpp/yt/memory/ref.h>

#include <library/cpp/containers/concurrent_hash/concurrent_hash.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>
#include <library/cpp/yt/threading/spin_lock.h>

#include <atomic>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ERequestProcessingStage,
    (Waiting)
    (Executing)
);

////////////////////////////////////////////////////////////////////////////////

template <class TRequestMessage>
class TTypedServiceRequest
    : public TRequestMessage
{
public:
    using TMessage = TRequestMessage;

    DEFINE_BYREF_RW_PROPERTY(std::vector<TSharedRef>, Attachments);

    NConcurrency::IAsyncZeroCopyInputStreamPtr GetAttachmentsStream()
    {
        return Context_->GetRequestAttachmentsStream();
    }

    void Reset()
    {
        TRequestMessage::Clear();
        Attachments_.clear();
        Context_ = nullptr;
    }

private:
    template <
        class TServiceContext,
        class TServiceContextWrapper,
        class TRequestMessage_,
        class TResponseMessage
    >
    friend class TGenericTypedServiceContext;

    IServiceContext* Context_ = nullptr;
};

template <class TRequestMessage>
struct TPooledTypedRequestTraits
    : public TPooledObjectTraitsBase<TTypedServiceRequest<TRequestMessage>>
{
    static void Clean(TTypedServiceRequest<TRequestMessage>* message)
    {
        message->Clear();
        message->Attachments().clear();
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TResponseMessage>
class TTypedServiceResponse
    : public TResponseMessage
{
public:
    using TMessage = TResponseMessage;

    DEFINE_BYREF_RW_PROPERTY(std::vector<TSharedRef>, Attachments);

    NConcurrency::IAsyncZeroCopyOutputStreamPtr GetAttachmentsStream()
    {
        return Context_->GetResponseAttachmentsStream();
    }

private:
    template <
        class TServiceContext,
        class TServiceContextWrapper,
        class TRequestMessage,
        class TResponseMessage_
    >
    friend class TGenericTypedServiceContext;

    IServiceContext* Context_ = nullptr;
};

template <class TResponseMessage>
struct TPooledTypedResponseTraits
    : public TPooledObjectTraitsBase<TTypedServiceResponse<TResponseMessage>>
{
    static void Clean(TTypedServiceResponse<TResponseMessage>* message)
    {
        message->Clear();
        message->Attachments().clear();
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Describes request handling options.
struct THandlerInvocationOptions
{
    //! Should we be deserializing the request and serializing the request
    //! in a separate thread?
    bool Heavy = false;

    //! In case the client has provided "none" response codec, this value is used instead.
    NCompression::ECodec ResponseCodec = NCompression::ECodec::None;

    THandlerInvocationOptions SetHeavy(bool value) const;
    THandlerInvocationOptions SetResponseCodec(NCompression::ECodec value) const;
};

////////////////////////////////////////////////////////////////////////////////

template <
    class TServiceContext,
    class TServiceContextWrapper,
    class TRequestMessage,
    class TResponseMessage
>
class TGenericTypedServiceContext
    : public TServiceContextWrapper
{
public:
    using TTypedRequest = TTypedServiceRequest<TRequestMessage>;
    using TTypedResponse = TTypedServiceResponse<TResponseMessage>;

    TGenericTypedServiceContext(
        TIntrusivePtr<TServiceContext> context,
        const THandlerInvocationOptions& options)
        : TServiceContextWrapper(std::move(context))
        , Options_(options)
    {
        const auto& underlyingContext = this->GetUnderlyingContext();
        Response_ = underlyingContext->IsPooled()
            ? ObjectPool<TTypedResponse, TPooledTypedResponseTraits<TResponseMessage>>().Allocate()
            : std::make_shared<TTypedResponse>();
        Response_->Context_ = underlyingContext.Get();

        if (this->GetResponseCodec() == NCompression::ECodec::None) {
            this->SetResponseCodec(Options_.ResponseCodec);
        }
    }

    bool DeserializeRequest()
    {
        const auto& underlyingContext = this->GetUnderlyingContext();
        if (underlyingContext->IsPooled()) {
            Request_ = ObjectPool<TTypedRequest, TPooledTypedRequestTraits<TRequestMessage>>().Allocate();
        } else {
            Request_ = std::make_shared<TTypedRequest>();
        }

        Request_->Context_ = underlyingContext.Get();
        const auto& tracker = Request_->Context_->GetMemoryUsageTracker();

        const auto& requestHeader = this->GetRequestHeader();
        // COMPAT(danilalexeev): legacy RPC codecs
        std::optional<NCompression::ECodec> bodyCodecId;
        NCompression::ECodec attachmentCodecId;
        if (requestHeader.has_request_codec()) {
            int intCodecId = requestHeader.request_codec();
            NCompression::ECodec codecId;
            if (!TryEnumCast(intCodecId, &codecId)) {
                underlyingContext->Reply(TError(
                    NRpc::EErrorCode::ProtocolError,
                    "Request codec %v is not supported",
                    intCodecId));
                return false;
            }
            bodyCodecId = codecId;
            attachmentCodecId = codecId;
        } else {
            bodyCodecId = std::nullopt;
            attachmentCodecId = NCompression::ECodec::None;
        }

        auto body = underlyingContext->GetRequestBody();
        if (requestHeader.has_request_format()) {
            auto format = static_cast<EMessageFormat>(requestHeader.request_format());

            NYson::TYsonString formatOptionsYson;
            if (requestHeader.has_request_format_options()) {
                formatOptionsYson = NYson::TYsonString(requestHeader.request_format_options());
            }
            if (format != EMessageFormat::Protobuf) {
                body = TrackMemory(tracker, ConvertMessageFromFormat(
                    body,
                    format,
                    NYson::ReflectProtobufMessageType<TRequestMessage>(),
                    formatOptionsYson,
                    !bodyCodecId.has_value()));
            }
        }

        bool deserializationSucceeded = bodyCodecId
            ? TryDeserializeProtoWithCompression(Request_.get(), body, *bodyCodecId)
            : TryDeserializeProtoWithEnvelope(Request_.get(), body);
        if (!deserializationSucceeded) {
            underlyingContext->Reply(TError(
                NRpc::EErrorCode::ProtocolError,
                "Error deserializing request body"));
            return false;
        }

        std::vector<TSharedRef> requestAttachments;
        try {
            if (attachmentCodecId == NCompression::ECodec::None) {
                requestAttachments = underlyingContext->RequestAttachments();
            } else {
                requestAttachments = DecompressAttachments(
                    underlyingContext->RequestAttachments(),
                    attachmentCodecId);

                // For decompressed blocks, memory tracking must be used again,
                // since they are allocated in a new allocation.
                for (auto& attachment : requestAttachments) {
                    attachment = TrackMemory(tracker, attachment);
                }
            }
        } catch (const std::exception& ex) {
            underlyingContext->Reply(TError(
                NRpc::EErrorCode::ProtocolError,
                "Error deserializing request attachments")
                << TError(ex));
            return false;
        }

        Request_->Attachments() = std::move(requestAttachments);

        return true;
    }

    const TTypedRequest& Request() const
    {
        return *Request_;
    }

    TTypedRequest& Request()
    {
        return *Request_;
    }

    const TTypedResponse& Response() const
    {
        return *Response_;
    }

    TTypedResponse& Response()
    {
        return *Response_;
    }


    using IServiceContext::Reply;

    void Reply()
    {
        Reply(TError());
    }

    void Reply(const TError& error)
    {
        if (this->Options_.Heavy) {
            TDispatcher::Get()->GetHeavyInvoker()->Invoke(BIND(
                &TGenericTypedServiceContext::DoReply,
                MakeStrong(this),
                error));
        } else {
            this->DoReply(error);
        }
    }


    const THandlerInvocationOptions& GetOptions() const
    {
        return Options_;
    }

protected:
    const THandlerInvocationOptions Options_;

    typename TObjectPool<TTypedRequest, TPooledTypedRequestTraits<TRequestMessage>>::TObjectPtr Request_;
    typename TObjectPool<TTypedResponse, TPooledTypedResponseTraits<TResponseMessage>>::TObjectPtr Response_;

    struct TSerializedResponse
    {
        TSharedRef Body;
        std::vector<TSharedRef> Attachments;
    };

    TSerializedResponse SerializeResponse()
    {
        const auto& underlyingContext = this->GetUnderlyingContext();
        const auto& requestHeader = underlyingContext->GetRequestHeader();

        auto codecId = underlyingContext->GetResponseCodec();
        auto serializedBody = SerializeProtoToRefWithCompression(*Response_, codecId);
        underlyingContext->SetResponseBodySerializedWithCompression();

        if (requestHeader.has_response_format()) {
            int intFormat = requestHeader.response_format();
            EMessageFormat format;
            if (!TryEnumCast(intFormat, &format)) {
                THROW_ERROR_EXCEPTION(
                    EErrorCode::ProtocolError,
                    "Message format %v is not supported",
                    intFormat);
            }

            NYson::TYsonString formatOptionsYson;
            if (requestHeader.has_response_format_options()) {
                formatOptionsYson = NYson::TYsonString(requestHeader.response_format_options());
            }

            if (format != EMessageFormat::Protobuf) {
                serializedBody = ConvertMessageToFormat(
                    serializedBody,
                    format,
                    NYson::ReflectProtobufMessageType<TResponseMessage>(),
                    formatOptionsYson);
            }
        }

        auto responseAttachments = CompressAttachments(Response_->Attachments(), codecId);

        return TSerializedResponse{
            .Body = std::move(serializedBody),
            .Attachments = std::move(responseAttachments),
        };
    }

    void DoReply(const TError& error)
    {
        const auto& underlyingContext = this->GetUnderlyingContext();

        if (error.IsOK()) {
            TSerializedResponse response;
            try {
                response = SerializeResponse();
            } catch (const std::exception& ex) {
                underlyingContext->Reply(TError(ex));
                return;
            }

            underlyingContext->SetResponseBody(std::move(response.Body));
            underlyingContext->ResponseAttachments() = std::move(response.Attachments);
        }

        underlyingContext->Reply(error);
    }
};

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_RPC_SERVICE_METHOD_THUNK_VIA_MESSAGES(requestMessage, responseMessage, method) \
    using TCtx##method = TTypedServiceContextImpl<requestMessage, responseMessage>; \
    using TCtx##method##Ptr = ::NYT::TIntrusivePtr<TCtx##method>; \
    using TReq##method = typename TCtx##method::TTypedRequest; \
    using TRsp##method = typename TCtx##method::TTypedResponse; \
    \
    void method##LiteThunk( \
        const ::NYT::NRpc::IServiceContextPtr& context, \
        const ::NYT::NRpc::THandlerInvocationOptions& options) \
    { \
        auto typedContext = ::NYT::New<TCtx##method>(context, options); \
        if (!typedContext->DeserializeRequest()) { \
            return; \
        } \
        InitContext(typedContext.Get());          \
        auto* request = &typedContext->Request(); \
        auto* response = &typedContext->Response(); \
        this->method(request, response, typedContext); \
    } \
    \
    ::NYT::NRpc::TServiceBase::TLiteHandler method##HeavyThunk( \
        const ::NYT::NRpc::IServiceContextPtr& context, \
        const ::NYT::NRpc::THandlerInvocationOptions& options) \
    { \
        auto typedContext = ::NYT::New<TCtx##method>(context, options); \
        if (!typedContext->DeserializeRequest()) { \
            return ::NYT::NRpc::TServiceBase::TLiteHandler(); \
        } \
        return \
            BIND([this, typedContext = std::move(typedContext)] ( \
                const ::NYT::NRpc::IServiceContextPtr&, \
                const ::NYT::NRpc::THandlerInvocationOptions&) \
            { \
                InitContext(typedContext.Get());          \
                auto* request = &typedContext->Request(); \
                auto* response = &typedContext->Response(); \
                this->method(request, response, typedContext); \
            }); \
    }

#define DEFINE_RPC_SERVICE_METHOD_THUNK(ns, method) \
    DEFINE_RPC_SERVICE_METHOD_THUNK_VIA_MESSAGES(ns::TReq##method, ns::TRsp##method, method)

#define DECLARE_RPC_SERVICE_METHOD_VIA_MESSAGES(requestMessage, responseMessage, method) \
    DEFINE_RPC_SERVICE_METHOD_THUNK_VIA_MESSAGES(requestMessage, responseMessage, method) \
    \
    void method( \
        [[maybe_unused]] TReq##method* request, \
        [[maybe_unused]] TRsp##method* response, \
        [[maybe_unused]] const TCtx##method##Ptr& context)

#define DECLARE_RPC_SERVICE_METHOD(ns, method) \
    DECLARE_RPC_SERVICE_METHOD_VIA_MESSAGES(ns::TReq##method, ns::TRsp##method, method)

#define DEFINE_RPC_SERVICE_METHOD(type, method) \
    void type::method( \
        [[maybe_unused]] TReq##method* request, \
        [[maybe_unused]] TRsp##method* response, \
        [[maybe_unused]] const TCtx##method##Ptr& context)

#define RPC_SERVICE_METHOD_DESC(method) \
    ::NYT::NRpc::TServiceBase::TMethodDescriptor( \
        #method, \
        BIND_NO_PROPAGATE(&std::remove_reference<decltype(*this)>::type::method##LiteThunk, ::NYT::Unretained(this)), \
        BIND_NO_PROPAGATE(&std::remove_reference<decltype(*this)>::type::method##HeavyThunk, ::NYT::Unretained(this)))


////////////////////////////////////////////////////////////////////////////////

TRequestQueuePtr CreateRequestQueue(
    const std::string& name,
    const NProfiling::TProfiler& profiler = {});

////////////////////////////////////////////////////////////////////////////////

class TDynamicConcurrencyLimit
{
public:
    DEFINE_SIGNAL(void(), Updated);

    void Reconfigure(int limit);
    int GetLimitFromConfiguration() const;

    int GetDynamicLimit() const;
    void SetDynamicLimit(std::optional<int> dynamicLimit);

private:
    std::atomic<int> ConfigLimit_ = 0;
    std::atomic<int> DynamicLimit_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TDynamicConcurrencyByteLimit
{
public:
    DEFINE_SIGNAL(void(), Updated);

    void Reconfigure(i64 limit);
    i64 GetByteLimitFromConfiguration() const;

    i64 GetDynamicByteLimit() const;
    void SetDynamicByteLimit(std::optional<i64> dynamicLimit);

private:
    std::atomic<i64> ConfigByteLimit_ = 0;
    std::atomic<i64> DynamicByteLimit_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Provides a base for implementing IService.
class TServiceBase
    : public virtual IService
{
public:
    void Configure(
        const TServiceCommonConfigPtr& configDefaults,
        const NYTree::INodePtr& configNode) override;
    TFuture<void> Stop() override;

    const TServiceId& GetServiceId() const override;

    void HandleRequest(
        std::unique_ptr<NProto::TRequestHeader> header,
        TSharedRefArray message,
        NYT::NBus::IBusPtr replyBus) override;
    void HandleRequestCancellation(TRequestId requestId) override;
    void HandleStreamingPayload(
        TRequestId requestId,
        const TStreamingPayload& payload) override;
    void HandleStreamingFeedback(
        TRequestId requestId,
        const TStreamingFeedback& feedback) override;

protected:
    const NLogging::TLogger Logger;

    using TLiteHandler = TCallback<void(const IServiceContextPtr&, const THandlerInvocationOptions&)>;
    using THeavyHandler = TCallback<TLiteHandler(const IServiceContextPtr&, const THandlerInvocationOptions&)>;

    class TServiceContext;
    using TServiceContextPtr = TIntrusivePtr<TServiceContext>;

    //! This alias provides an option to replace default typed service context class
    //! with a custom one which may be richer in some way. If a init-like procedure
    //! is needed to customize specific service context before invoking method handler,
    //! one may use #InitContext method below.
    //! See api_service.cpp for an example.
    template <class TRequestMessage, class TResponseMessage>
    using TTypedServiceContextImpl = TTypedServiceContext<TRequestMessage, TResponseMessage>;

    //! By default, this method does nothing. You may hide this method by a custom implementation
    //! (possibly switching argument type to a proper typed context class) in order to customize
    //! specific service context before invoking method handler.
    void InitContext(IServiceContext* context);

    //! Information needed to a register a service method.
    struct TMethodDescriptor
    {
        // Defaults.
        TMethodDescriptor(
            TString method,
            TLiteHandler liteHandler,
            THeavyHandler heavyHandler);

        //! When a request is received and parsed, RPC service puts it into a queue.
        //! This enables specifying a relevant queue on a per-request basis.
        //! If not given or provides null, the default (per-method) queue is used.
        IRequestQueueProviderPtr RequestQueueProvider;

        //! When a request is dequeued from its queue, it is delegated to an appropriate
        //! invoker for the actual execution.
        //! If not given then the per-service default is used.
        IInvokerPtr Invoker;

        //! Enables overriding #Invoker on a per-request basis.
        //! If not given or returns null then #Invoker is used as a fallback.
        TInvokerProvider InvokerProvider;

        //! Service method name.
        TString Method;

        //! A handler that will serve lite requests.
        TLiteHandler LiteHandler;

        //! A handler that will serve heavy requests.
        THeavyHandler HeavyHandler;

        //! Options to pass to the handler.
        THandlerInvocationOptions Options;

        //! Maximum number of requests in queue (both waiting and executing).
        int QueueSizeLimit = 10'000;

        //! Maximum total size of requests in queue (both waiting and executing).
        i64 QueueByteSizeLimit = 2_GB;

        //! Maximum number of requests executing concurrently.
        int ConcurrencyLimit = 10'000;

        //! Maximum total size of requests executing concurrently.
        i64 ConcurrencyByteLimit = 4_GB;

        //! System requests are completely transparent to derived classes;
        //! in particular, |BeforeInvoke| is not called.
        //! Also system methods do not require authentication.
        bool System = false;

        //! Log level for events emitted via |Set(Request|Response)Info|-like functions.
        NLogging::ELogLevel LogLevel = NLogging::ELogLevel::Debug;

        //! Logging suppression timeout for this method requests.
        TDuration LoggingSuppressionTimeout = TDuration::Zero();

        //! Cancelable requests can be canceled by clients.
        //! This, however, requires additional book-keeping at server-side so one is advised
        //! to only mark cancelable those methods taking a considerable time to complete.
        bool Cancelable = false;

        //! If |true| then Bus is expected to be generating checksums for the whole response content,
        //! including attachments (unless the connection is local or the checksums are explicitly disabled).
        //! If |false| then Bus will only be generating such checksums for RPC header and response body
        //! but not attachments.
        bool GenerateAttachmentChecksums = true;

        //! If |true| then the method supports attachments streaming.
        bool StreamingEnabled = false;

        //! If |true| then requests and responses are pooled.
        bool Pooled = true;

        // If |true| then method exception will be handled by |OnMethodError|.
        bool HandleMethodError = false;

        TMethodDescriptor SetRequestQueueProvider(IRequestQueueProviderPtr value) const;
        TMethodDescriptor SetInvoker(IInvokerPtr value) const;
        TMethodDescriptor SetInvokerProvider(TInvokerProvider value) const;
        TMethodDescriptor SetHeavy(bool value) const;
        TMethodDescriptor SetResponseCodec(NCompression::ECodec value) const;
        TMethodDescriptor SetQueueSizeLimit(int value) const;
        TMethodDescriptor SetQueueByteSizeLimit(i64 value) const;
        TMethodDescriptor SetConcurrencyLimit(int value) const;
        TMethodDescriptor SetConcurrencyByteLimit(i64 value) const;
        TMethodDescriptor SetSystem(bool value) const;
        TMethodDescriptor SetLogLevel(NLogging::ELogLevel value) const;
        TMethodDescriptor SetLoggingSuppressionTimeout(TDuration value) const;
        TMethodDescriptor SetCancelable(bool value) const;
        TMethodDescriptor SetGenerateAttachmentChecksums(bool value) const;
        TMethodDescriptor SetStreamingEnabled(bool value) const;
        TMethodDescriptor SetPooled(bool value) const;
        TMethodDescriptor SetHandleMethodError(bool value) const;
    };

    struct TErrorCodeCounter
    {
        explicit TErrorCodeCounter(NProfiling::TProfiler profiler);

        void Increment(TErrorCode code);

    private:
        const NProfiling::TProfiler Profiler_;

        NConcurrency::TSyncMap<TErrorCode, NProfiling::TCounter> CodeToCounter_;
    };

    //! Per-user and per-method profiling counters.
    struct TMethodPerformanceCounters
        : public TRefCounted
    {
        TMethodPerformanceCounters(
            const NProfiling::TProfiler& profiler,
            const TTimeHistogramConfigPtr& timeHistogramConfig);

        //! Counts the number of method calls.
        NProfiling::TCounter RequestCounter;

        //! Counts the number of canceled method calls.
        NProfiling::TCounter CanceledRequestCounter;

        //! Counts the number of failed method calls.
        NProfiling::TCounter FailedRequestCounter;

        //! Counts the number of timed out method calls.
        NProfiling::TCounter TimedOutRequestCounter;

        //! Time spent while handling the request.
        NProfiling::TEventTimer ExecutionTimeCounter;

        //! Time spent at the caller's side before the request arrived into the server queue.
        NProfiling::TEventTimer RemoteWaitTimeCounter;

        //! Time spent while the request was waiting in the server queue.
        NProfiling::TEventTimer LocalWaitTimeCounter;

        //! Time between the request arrival and the moment when it is fully processed.
        NProfiling::TEventTimer TotalTimeCounter;

        //! CPU time spent in the trace context associated with the request (locally).
        NProfiling::TTimeCounter TraceContextTimeCounter;

        //! Counts the number of bytes in requests message body.
        NProfiling::TCounter RequestMessageBodySizeCounter;

        //! Counts the number of bytes in request message attachment.
        NProfiling::TCounter RequestMessageAttachmentSizeCounter;

        //! Counts the number of bytes in response message body.
        NProfiling::TCounter ResponseMessageBodySizeCounter;

        //! Counts the number of bytes in response message attachment.
        NProfiling::TCounter ResponseMessageAttachmentSizeCounter;

        //! Counts the number of errors, per error code.
        TErrorCodeCounter ErrorCodeCounter;
    };

    using TMethodPerformanceCountersPtr = TIntrusivePtr<TMethodPerformanceCounters>;

    //! Describes a service method and its runtime statistics.
    struct TRuntimeMethodInfo
        : public TRefCounted
    {
        TRuntimeMethodInfo(
            TServiceId serviceId,
            TMethodDescriptor descriptor,
            const NProfiling::TProfiler& profiler);

        // TODO(kvk1920): Generalize as default queue provider.
        TRequestQueue* GetDefaultRequestQueue();

        const TServiceId ServiceId;
        const TMethodDescriptor Descriptor;
        const NProfiling::TProfiler Profiler;

        const TRequestQueuePtr DefaultRequestQueue;

        NLogging::TLoggingAnchor* const RequestLoggingAnchor;
        NLogging::TLoggingAnchor* const ResponseLoggingAnchor;

        std::atomic<bool> Heavy = false;
        std::atomic<bool> Pooled = true;

        std::atomic<int> QueueSizeLimit = 0;
        std::atomic<i64> QueueByteSizeLimit = 0;

        TDynamicConcurrencyLimit ConcurrencyLimit;
        TDynamicConcurrencyByteLimit ConcurrencyByteLimit;
        std::atomic<double> WaitingTimeoutFraction = 0;

        NProfiling::TCounter RequestQueueSizeLimitErrorCounter;
        NProfiling::TCounter RequestQueueByteSizeLimitErrorCounter;
        NProfiling::TCounter UnauthenticatedRequestsCounter;

        std::atomic<NLogging::ELogLevel> LogLevel = {};
        std::atomic<TDuration> LoggingSuppressionTimeout = {};

        using TNonowningPerformanceCountersKey = std::tuple<TStringBuf, TRequestQueue*>;
        using TOwningPerformanceCountersKey = std::tuple<TString, TRequestQueue*>;
        using TPerformanceCountersKeyHash = THash<TNonowningPerformanceCountersKey>;
        struct TPerformanceCountersKeyEquals;
        using TPerformanceCountersMap = NConcurrency::TSyncMap<
            TOwningPerformanceCountersKey,
            TMethodPerformanceCountersPtr,
            TPerformanceCountersKeyHash,
            TPerformanceCountersKeyEquals
        >;
        TPerformanceCountersMap PerformanceCountersMap;
        TMethodPerformanceCountersPtr BasePerformanceCounters;
        TMethodPerformanceCountersPtr RootPerformanceCounters;

        NConcurrency::IReconfigurableThroughputThrottlerPtr LoggingSuppressionFailedRequestThrottler;

        std::atomic<ERequestTracingMode> TracingMode = ERequestTracingMode::Enable;

        YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, RequestQueuesLock);
        std::vector<TRequestQueue*> RequestQueues;
    };

    using TRuntimeMethodInfoPtr = TIntrusivePtr<TRuntimeMethodInfo>;

    //! Service-level performance counters.
    class TPerformanceCounters
        : public TRefCounted
    {
    public:
        explicit TPerformanceCounters(const NProfiling::TProfiler& profiler)
            : Profiler_(profiler.WithHot().WithSparse())
        { }

        void IncrementRequestsPerUserAgent(TStringBuf userAgent)
        {
            RequestsPerUserAgent_.FindOrInsert(userAgent, [&] {
                return Profiler_.WithRequiredTag("user_agent", TString(userAgent)).Counter("/user_agent");
            }).first->Increment();
        }

    private:
        const NProfiling::TProfiler Profiler_;

        //! Number of requests per user agent.
        NConcurrency::TSyncMap<TString, NProfiling::TCounter> RequestsPerUserAgent_;
    };

    using TPerformanceCountersPtr = TIntrusivePtr<TPerformanceCounters>;


    DECLARE_RPC_SERVICE_METHOD(NProto, Discover);

    //! Initializes the instance.
    /*!
     *  \param defaultInvoker
     *  An invoker that will be used for serving method invocations unless
     *  configured otherwise (see #RegisterMethod).
     *
     *  \param serviceName
     *  A name of the service.
     *
     *  \param logger
     *  A logger that will be used to log various debugging information
     *  regarding service activity.
     */
    TServiceBase(
        IInvokerPtr defaultInvoker,
        const TServiceDescriptor& descriptor,
        const NLogging::TLogger& logger,
        TRealmId realmId = NullRealmId,
        IAuthenticatorPtr authenticator = nullptr);

    TServiceBase(
        IInvokerPtr defaultInvoker,
        const TServiceDescriptor& descriptor,
        IMemoryUsageTrackerPtr memoryUsageTracker,
        const NLogging::TLogger& logger,
        TRealmId realmId = NullRealmId,
        IAuthenticatorPtr authenticator = nullptr);

    //! Registers a method handler.
    //! This call is must be performed prior to service registration.
    virtual TRuntimeMethodInfoPtr RegisterMethod(const TMethodDescriptor& descriptor);

    //! Register a feature as being supported by server.
    //! This call is must be performed prior to service registration.
    template <class E>
    void DeclareServerFeature(E featureId);

    //! Validates the required server features against the set of supported ones.
    //! Throws on failure.
    void ValidateRequestFeatures(const IServiceContextPtr& context);

    //! Returns a (non-owning!) pointer to TRuntimeMethodInfo for a given method's name
    //! or |nullptr| if no such method is registered.
    TRuntimeMethodInfo* FindMethodInfo(const TString& method);

    //! Similar to #FindMethodInfo but throws if no method is found.
    TRuntimeMethodInfo* GetMethodInfoOrThrow(const TString& method);

    //! Returns the default invoker passed during construction.
    const IInvokerPtr& GetDefaultInvoker() const;

    //! Called right before each method handler invocation.
    virtual void BeforeInvoke(IServiceContext* context);

    //! Used by peer discovery.
    //! Returns |true| is this service instance is up, i.e. can handle requests.
    /*!
     *  \note
     *  Thread affinity: any
     */
    virtual bool IsUp(const TCtxDiscoverPtr& context);

    //! Used by peer discovery.
    //! Fills response message extensions with additional info.
    /*!
     *  \note
     *  Thread affinity: any
     */
    virtual void EnrichDiscoverResponse(TRspDiscover* response);

    //! Used by peer discovery.
    //! Returns addresses of neighboring peers to be suggested to the client.
    /*!
     *  \note
     *  Thread affinity: any
     */
    virtual std::vector<TString> SuggestAddresses();

    //! Part of #DoConfigure
    //! #DoConfigure configures already registered methods.
    //! Configures sensor types for methods which will be registered after this function call.
    void DoConfigureHistogramTimer(
        const TServiceCommonConfigPtr& configDefaults,
        const TServiceConfigPtr& config);

    //! A typed implementation of #Configure.
    void DoConfigure(
        const TServiceCommonConfigPtr& configDefaults,
        const TServiceConfigPtr& config);

    virtual std::optional<TError> GetThrottledError(const NProto::TRequestHeader& requestHeader);

protected:
    void ReplyError(
        TError error,
        const NProto::TRequestHeader& header,
        const NYT::NBus::IBusPtr& replyBus);

    virtual void OnMethodError(const TError& error, const TString& method);

private:
    friend class TRequestQueue;

    const IInvokerPtr DefaultInvoker_;
    const IAuthenticatorPtr Authenticator_;
    const TServiceDescriptor ServiceDescriptor_;
    const TServiceId ServiceId_;
    const IMemoryUsageTrackerPtr MemoryUsageTracker_;

    const NProfiling::TProfiler Profiler_;

    TAtomicIntrusivePtr<TServiceConfig> Config_;

    struct TPendingPayloadsEntry
    {
        std::vector<TStreamingPayload> Payloads;
        NConcurrency::TLease Lease;
    };

    std::atomic<bool> Active_ = false;

    THashMap<TString, TRuntimeMethodInfoPtr> MethodMap_;

    THashSet<int> SupportedServerFeatureIds_;

    struct TRequestBucket
    {
        YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock);
        THashMap<TRequestId, TServiceContext*> RequestIdToContext;
        THashMap<TRequestId, TPendingPayloadsEntry> RequestIdToPendingPayloads;
    };

    static constexpr size_t RequestBucketCount = 64;
    std::array<TRequestBucket, RequestBucketCount> RequestBuckets_;

    struct TReplyBusData
    {
        THashSet<TServiceContext*> Contexts;
        TCallback<void(const TError&)> BusTerminationHandler;
    };

    struct TReplyBusBucket
    {
        YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock);
        THashMap<NYT::NBus::IBusPtr, TReplyBusData> ReplyBusToData;
    };

    static constexpr size_t ReplyBusBucketCount = 64;
    std::array<TReplyBusBucket, ReplyBusBucketCount> ReplyBusBuckets_;

    struct TQueuedReplyBucket
    {
        YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock);
        THashMap<TRequestId, TFuture<void>> QueuedReplies;
    };

    static constexpr size_t QueuedReplyBucketCount = 64;
    std::array<TQueuedReplyBucket, ReplyBusBucketCount> QueuedReplyBusBuckets_;

    static constexpr auto DefaultPendingPayloadsTimeout = TDuration::Seconds(30);
    std::atomic<TDuration> PendingPayloadsTimeout_ = DefaultPendingPayloadsTimeout;

    std::atomic<bool> Stopped_ = false;
    const TPromise<void> StopResult_ = NewPromise<void>();
    std::atomic<int> ActiveRequestCount_ = 0;

    std::atomic<int> AuthenticationQueueSize_ = 0;
    NProfiling::TEventTimer AuthenticationTimer_;

    static constexpr auto DefaultAuthenticationQueueSizeLimit = 10'000;
    std::atomic<int> AuthenticationQueueSizeLimit_ = DefaultAuthenticationQueueSizeLimit;

    std::atomic<bool> EnablePerUserProfiling_ = false;

    TAtomicIntrusivePtr<TTimeHistogramConfig> TimeHistogramConfig_;

    std::atomic<bool> EnableErrorCodeCounter_ = false;

    const NConcurrency::TPeriodicExecutorPtr ServiceLivenessChecker_;

    using TDiscoverRequestSet = TConcurrentHashMap<TCtxDiscoverPtr, int>;
    THashMap<TString, TDiscoverRequestSet> DiscoverRequestsByPayload_;
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, DiscoverRequestsByPayloadLock_);

    TPerformanceCountersPtr PerformanceCounters_;

    struct TAcceptedRequest
    {
        TRequestId RequestId;
        NYT::NBus::IBusPtr ReplyBus;
        TRuntimeMethodInfo* RuntimeInfo;
        NTracing::TTraceContextPtr TraceContext;
        std::unique_ptr<NRpc::NProto::TRequestHeader> Header;
        TSharedRefArray Message;
        TRequestQueue* RequestQueue;
        std::optional<TError> ThrottledError;
        TMemoryUsageTrackerGuard MemoryGuard;
        IMemoryUsageTrackerPtr MemoryUsageTracker;
    };

    void DoDeclareServerFeature(int featureId);
    TError DoCheckRequestCompatibility(const NRpc::NProto::TRequestHeader& header);
    TError DoCheckRequestProtocol(const NRpc::NProto::TRequestHeader& header);
    TError DoCheckRequestFeatures(const NRpc::NProto::TRequestHeader& header);
    TError DoCheckRequestCodecs(const NRpc::NProto::TRequestHeader& header);

    void OnRequestTimeout(TRequestId requestId, ERequestProcessingStage stage, bool aborted);
    void OnReplyBusTerminated(const NYT::TWeakPtr<NYT::NBus::IBus>& busWeak, const TError& error);

    void OnRequestAuthenticated(
        const NProfiling::TWallTimer& timer,
        TAcceptedRequest&& acceptedRequest,
        const TErrorOr<TAuthenticationResult>& authResultOrError);
    bool IsAuthenticationNeeded(const TAcceptedRequest& acceptedRequest);
    void HandleAuthenticatedRequest(TAcceptedRequest&& acceptedRequest);

    TRequestQueue* GetRequestQueue(
        TRuntimeMethodInfo* runtimeInfo,
        const NRpc::NProto::TRequestHeader& requestHeader);
    void RegisterRequestQueue(
        TRuntimeMethodInfo* runtimeInfo,
        TRequestQueue* requestQueue);
    void ConfigureRequestQueue(
        TRuntimeMethodInfo* runtimeInfo,
        TRequestQueue* requestQueue,
        const TMethodConfigPtr& config);

    TRequestBucket* GetRequestBucket(TRequestId requestId);
    TQueuedReplyBucket* GetQueuedReplyBucket(TRequestId requestId);
    TReplyBusBucket* GetReplyBusBucket(const NYT::NBus::IBusPtr& bus);

    void RegisterRequest(TServiceContext* context);
    void UnregisterRequest(TServiceContext* context);
    TServiceContextPtr FindRequest(TRequestId requestId);
    TServiceContextPtr DoFindRequest(TRequestBucket* bucket, TRequestId requestId);

    void RegisterQueuedReply(TRequestId requestId, TFuture<void> reply);
    void UnregisterQueuedReply(TRequestId requestId);
    bool TryCancelQueuedReply(TRequestId requestId);

    TPendingPayloadsEntry* DoGetOrCreatePendingPayloadsEntry(TRequestBucket* bucket, TRequestId requestId);
    std::vector<TStreamingPayload> GetAndErasePendingPayloads(TRequestId requestId);
    void OnPendingPayloadsLeaseExpired(TRequestId requestId);

    TMethodPerformanceCountersPtr CreateMethodPerformanceCounters(
        TRuntimeMethodInfo* runtimeInfo,
        const TRuntimeMethodInfo::TNonowningPerformanceCountersKey& key);
    TMethodPerformanceCounters* GetMethodPerformanceCounters(
        TRuntimeMethodInfo* runtimeInfo,
        const TRuntimeMethodInfo::TNonowningPerformanceCountersKey& key);

    TPerformanceCounters* GetPerformanceCounters();

    void SetActive();
    void ValidateInactive();

    bool IsStopped();
    void IncrementActiveRequestCount();
    void DecrementActiveRequestCount();

    void RegisterDiscoverRequest(const TCtxDiscoverPtr& context);
    void ReplyDiscoverRequest(const TCtxDiscoverPtr& context, bool isUp);

    void OnDiscoverRequestReplyDelayReached(TCtxDiscoverPtr context);

    static TString GetDiscoverRequestPayload(const TCtxDiscoverPtr& context);

    void OnServiceLivenessCheck();
};

DEFINE_REFCOUNTED_TYPE(TServiceBase)

////////////////////////////////////////////////////////////////////////////////

class TRequestQueue
    : public TRefCounted
{
public:
    TRequestQueue(const std::string& name, NProfiling::TProfiler profiler);

    bool Register(TServiceBase* service, TServiceBase::TRuntimeMethodInfo* runtimeInfo);
    void Configure(const TMethodConfigPtr& config);

    bool IsQueueSizeLimitExceeded() const;
    bool IsQueueByteSizeLimitExceeded() const;

    int GetQueueSize() const;
    i64 GetQueueByteSize() const;
    int GetConcurrency() const;
    i64 GetConcurrencyByte() const;

    void OnRequestArrived(const TServiceBase::TServiceContextPtr& context);
    void OnRequestFinished(i64 requestTotalSize);

    void ConfigureWeightThrottler(const NConcurrency::TThroughputThrottlerConfigPtr& config);
    void ConfigureBytesThrottler(const NConcurrency::TThroughputThrottlerConfigPtr& config);

    const std::string& GetName() const;

private:
    const std::string Name_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, RegisterLock_);
    std::atomic<bool> Registered_ = false;
    TServiceBase* Service_;
    TServiceBase::TRuntimeMethodInfo* RuntimeInfo_ = nullptr;

    std::atomic<int> Concurrency_ = 0;
    std::atomic<i64> ConcurrencyByte_ = 0;

    struct TRequestThrottler
    {
        const NConcurrency::IReconfigurableThroughputThrottlerPtr Throttler;
        std::atomic<bool> Specified = false;

        void Reconfigure(const NConcurrency::TThroughputThrottlerConfigPtr& config);
    };

    TRequestThrottler BytesThrottler_;
    TRequestThrottler WeightThrottler_;
    std::atomic<bool> Throttled_ = false;

    std::atomic<int> QueueSize_ = 0;
    std::atomic<i64> QueueByteSize_ = 0;
    moodycamel::ConcurrentQueue<TServiceBase::TServiceContextPtr> Queue_;


    void ScheduleRequestsFromQueue();
    void RunRequest(TServiceBase::TServiceContextPtr context);

    i64 GetTotalRequestSize(const TServiceBase::TServiceContextPtr& context);

    void IncrementQueueSize(const TServiceBase::TServiceContextPtr& context);
    void DecrementQueueSize(const TServiceBase::TServiceContextPtr& context);

    bool IncrementConcurrency(const TServiceBase::TServiceContextPtr& context);
    void DecrementConcurrency(i64 requestTotalSize);

    bool AreThrottlersOverdrafted() const;
    void AcquireThrottlers(const TServiceBase::TServiceContextPtr& context);
    void SubscribeToThrottlers();

    void OnConcurrencyLimitChanged();
    void OnConcurrencyByteLimitChanged();
};

DEFINE_REFCOUNTED_TYPE(TRequestQueue)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TRequestMessage, class T>
struct TPooledObjectTraits<
    class NRpc::TTypedServiceRequest<TRequestMessage>,
    T
>
    : public TPooledObjectTraitsBase<
        typename NRpc::TTypedServiceRequest<TRequestMessage>
    >
{
    static void Clean(NRpc::TTypedServiceRequest<TRequestMessage>* message)
    {
        message->Clear();
        message->Attachments().clear();
    }
};

template <class TResponseMessage, class T>
struct TPooledObjectTraits<
    class NRpc::TTypedServiceResponse<TResponseMessage>,
    T
>
    : public TPooledObjectTraitsBase<
        typename NRpc::TTypedServiceRequest<TResponseMessage>
    >
{
    static void Clean(NRpc::TTypedServiceRequest<TResponseMessage>* message)
    {
        message->Clear();
        message->Attachments().clear();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define SERVICE_DETAIL_INL_H_
#include "service_detail-inl.h"
#undef SERVICE_DETAIL_INL_H_

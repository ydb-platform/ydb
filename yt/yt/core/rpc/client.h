#pragma once

#include "public.h"
#include "channel.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/bus/client.h>

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/misc/memory_usage_tracker.h>

#include <yt/yt/core/rpc/helpers.h>
#include <yt/yt/core/rpc/message.h>
#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <atomic>
#include <optional>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  \note
 *  Thread affinity: single-threaded.
 *  Notable exceptions are IClientRequest::Serialize and IClientRequest::GetHash.
 *  Once the request is fully configured (from a single thread), these could be
 *  invoked from arbitrary threads concurrently.
 */
struct IClientRequest
    : public virtual TRefCounted
{
    virtual TSharedRefArray Serialize() = 0;

    virtual const NProto::TRequestHeader& Header() const = 0;
    virtual NProto::TRequestHeader& Header() = 0;

    virtual bool IsStreamingEnabled() const = 0;

    virtual const TStreamingParameters& ClientAttachmentsStreamingParameters() const = 0;
    virtual TStreamingParameters& ClientAttachmentsStreamingParameters() = 0;

    virtual const TStreamingParameters& ServerAttachmentsStreamingParameters() const = 0;
    virtual TStreamingParameters& ServerAttachmentsStreamingParameters() = 0;

    virtual NConcurrency::IAsyncZeroCopyOutputStreamPtr GetRequestAttachmentsStream() const = 0;
    virtual NConcurrency::IAsyncZeroCopyInputStreamPtr GetResponseAttachmentsStream() const = 0;

    virtual TRequestId GetRequestId() const = 0;
    virtual TRealmId GetRealmId() const = 0;
    virtual std::string GetService() const = 0;
    virtual std::string GetMethod() const = 0;

    virtual void DeclareClientFeature(int featureId) = 0;
    virtual void RequireServerFeature(int featureId) = 0;

    virtual const TString& GetUser() const = 0;
    virtual void SetUser(const TString& user) = 0;

    virtual const TString& GetUserTag() const = 0;
    virtual void SetUserTag(const TString& tag) = 0;

    virtual void SetUserAgent(const TString& userAgent) = 0;

    virtual bool GetRetry() const = 0;
    virtual void SetRetry(bool value) = 0;

    virtual TMutationId GetMutationId() const = 0;
    virtual void SetMutationId(TMutationId id) = 0;

    virtual bool IsLegacyRpcCodecsEnabled() = 0;

    virtual size_t GetHash() const = 0;

    // Extension methods.
    template <class E>
    void DeclareClientFeature(E featureId);
    template <class E>
    void RequireServerFeature(E featureId);
};

DEFINE_REFCOUNTED_TYPE(IClientRequest)

bool IsRequestSticky(const IClientRequestPtr& request);

////////////////////////////////////////////////////////////////////////////////

class TClientContext
    : public TRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TRequestId, RequestId);
    DEFINE_BYVAL_RO_PROPERTY(NTracing::TTraceContextPtr, TraceContext);
    DEFINE_BYVAL_RO_PROPERTY(std::string, Service);
    DEFINE_BYVAL_RO_PROPERTY(std::string, Method);
    DEFINE_BYVAL_RO_PROPERTY(TFeatureIdFormatter, FeatureIdFormatter);
    DEFINE_BYVAL_RO_PROPERTY(bool, ResponseHeavy);
    DEFINE_BYVAL_RO_PROPERTY(TAttachmentsOutputStreamPtr, RequestAttachmentsStream);
    DEFINE_BYVAL_RO_PROPERTY(TAttachmentsInputStreamPtr, ResponseAttachmentsStream);
    DEFINE_BYVAL_RO_PROPERTY(IMemoryUsageTrackerPtr, MemoryUsageTracker);

public:
    TClientContext(
        TRequestId requestId,
        NTracing::TTraceContextPtr traceContext,
        std::string service,
        std::string method,
        TFeatureIdFormatter featureIdFormatter,
        bool heavy,
        TAttachmentsOutputStreamPtr requestAttachmentsStream,
        TAttachmentsInputStreamPtr responseAttachmentsStream,
        IMemoryUsageTrackerPtr memoryUsageTracker);
};

DEFINE_REFCOUNTED_TYPE(TClientContext)

////////////////////////////////////////////////////////////////////////////////

class TClientRequest
    : public IClientRequest
{
public:
    DEFINE_BYREF_RW_PROPERTY(std::vector<TSharedRef>, Attachments);
    DEFINE_BYVAL_RW_PROPERTY(std::optional<TDuration>, Timeout);
    DEFINE_BYVAL_RW_PROPERTY(std::optional<TDuration>, AcknowledgementTimeout);
    //! If |true| then the request will be serialized in RPC heavy thread pool.
    DEFINE_BYVAL_RW_PROPERTY(bool, RequestHeavy);
    //! If |true| then the response will be deserialized and the response handler will
    //! be invoked in RPC heavy thread pool.
    DEFINE_BYVAL_RW_PROPERTY(bool, ResponseHeavy);
    DEFINE_BYVAL_RW_PROPERTY(NCompression::ECodec, RequestCodec, NCompression::ECodec::None);
    DEFINE_BYVAL_RW_PROPERTY(NCompression::ECodec, ResponseCodec, NCompression::ECodec::None);
    DEFINE_BYVAL_RW_PROPERTY(bool, EnableLegacyRpcCodecs, true);
    DEFINE_BYVAL_RW_PROPERTY(bool, GenerateAttachmentChecksums, true);
    DEFINE_BYVAL_RW_PROPERTY(IMemoryUsageTrackerPtr, MemoryUsageTracker);
    // Field is used on client side only. So it is never serialized.
    DEFINE_BYREF_RW_PROPERTY(NTracing::TTraceContext::TTagList, TracingTags);
    // For testing purposes only.
    DEFINE_BYVAL_RW_PROPERTY(std::optional<TDuration>, SendDelay);

public:
    TSharedRefArray Serialize() override;

    NProto::TRequestHeader& Header() override;
    const NProto::TRequestHeader& Header() const override;

    bool IsStreamingEnabled() const override;

    const TStreamingParameters& ClientAttachmentsStreamingParameters() const override;
    TStreamingParameters& ClientAttachmentsStreamingParameters() override;

    const TStreamingParameters& ServerAttachmentsStreamingParameters() const override;
    TStreamingParameters& ServerAttachmentsStreamingParameters() override;

    NConcurrency::IAsyncZeroCopyOutputStreamPtr GetRequestAttachmentsStream() const override;
    NConcurrency::IAsyncZeroCopyInputStreamPtr GetResponseAttachmentsStream() const override;

    TRequestId GetRequestId() const override;
    TRealmId GetRealmId() const override;
    std::string GetService() const override;
    std::string GetMethod() const override;

    using NRpc::IClientRequest::DeclareClientFeature;
    using NRpc::IClientRequest::RequireServerFeature;

    void DeclareClientFeature(int featureId) override;
    void RequireServerFeature(int featureId) override;

    const TString& GetUser() const override;
    void SetUser(const TString& user) override;

    const TString& GetUserTag() const override;
    void SetUserTag(const TString& tag) override;

    void SetUserAgent(const TString& userAgent) override;

    bool GetRetry() const override;
    void SetRetry(bool value) override;

    TMutationId GetMutationId() const override;
    void SetMutationId(TMutationId id) override;

    size_t GetHash() const override;

    bool IsLegacyRpcCodecsEnabled() override;

    EMultiplexingBand GetMultiplexingBand() const;
    void SetMultiplexingBand(EMultiplexingBand band);

    int GetMultiplexingParallelism() const;
    void SetMultiplexingParallelism(int parallelism);

protected:
    const IChannelPtr Channel_;
    const bool StreamingEnabled_;
    const bool SendBaggage_;
    const TFeatureIdFormatter FeatureIdFormatter_;

    TClientRequest(
        IChannelPtr channel,
        const TServiceDescriptor& serviceDescriptor,
        const TMethodDescriptor& methodDescriptor);
    TClientRequest(const TClientRequest& other);

    virtual TSharedRefArray SerializeHeaderless() const = 0;
    virtual size_t ComputeHash() const;

    TClientContextPtr CreateClientContext();

    IClientRequestControlPtr Send(IClientResponseHandlerPtr responseHandler);

private:
    std::atomic<bool> Serialized_ = false;

    std::atomic<bool> HeaderPrepared_ = false;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, HeaderPreparationLock_);
    NProto::TRequestHeader Header_;

    mutable TSharedRefArray SerializedHeaderlessMessage_;
    mutable std::atomic<bool> SerializedHeaderlessMessageLatch_ = false;
    mutable std::atomic<bool> SerializedHeaderlessMessageSet_ = false;

    static constexpr auto UnknownHash = static_cast<size_t>(-1);
    mutable std::atomic<size_t> Hash_ = UnknownHash;

    EMultiplexingBand MultiplexingBand_ = EMultiplexingBand::Default;
    int MultiplexingParallelism_ = 1;

    TStreamingParameters ClientAttachmentsStreamingParameters_;
    TStreamingParameters ServerAttachmentsStreamingParameters_;

    TAttachmentsOutputStreamPtr RequestAttachmentsStream_;
    TAttachmentsInputStreamPtr ResponseAttachmentsStream_;

    TString User_;
    TString UserTag_;

    TWeakPtr<IClientRequestControl> RequestControl_;

    void OnPullRequestAttachmentsStream();
    void OnRequestStreamingPayloadAcked(int sequenceNumber, const TError& error);
    void OnResponseAttachmentsStreamRead();
    void OnResponseStreamingFeedbackAcked(const TStreamingFeedback& feedback, const TError& error);

    void TraceRequest(const NTracing::TTraceContextPtr& traceContext);

    void PrepareHeader();
    TSharedRefArray GetHeaderlessMessage() const;
};

DEFINE_REFCOUNTED_TYPE(TClientRequest)

////////////////////////////////////////////////////////////////////////////////

template <class TRequestMessage, class TResponse>
class TTypedClientRequest
    : public TClientRequest
    , public TRequestMessage
{
public:
    using TThisPtr = TIntrusivePtr<TTypedClientRequest>;

    TTypedClientRequest(
        IChannelPtr channel,
        const TServiceDescriptor& serviceDescriptor,
        const TMethodDescriptor& methodDescriptor);

    TFuture<typename TResponse::TResult> Invoke();

private:
    TSharedRefArray SerializeHeaderless() const override;
};

////////////////////////////////////////////////////////////////////////////////

//! Handles the outcome of a single RPC request.
struct IClientResponseHandler
    : public virtual TRefCounted
{
    //! Called when request delivery is acknowledged.
    virtual void HandleAcknowledgement() = 0;

    //! Called if the request is replied with #EErrorCode::OK.
    /*!
     *  \param message A message containing the response.
     *  \param address Address of the response sender. Empty if it is not supported by the underlying RPC stack.
     */
    virtual void HandleResponse(TSharedRefArray message, const std::string& address) = 0;

    //! Called if the request fails.
    /*!
     *  \param error An error that has occurred.
     */
    virtual void HandleError(TError error) = 0;

    //! Enables passing streaming data from the service to clients.
    virtual void HandleStreamingPayload(const TStreamingPayload& payload) = 0;

    //! Enables the service to notify clients about its progress in receiving streaming data.
    virtual void HandleStreamingFeedback(const TStreamingFeedback& feedback) = 0;
};

DEFINE_REFCOUNTED_TYPE(IClientResponseHandler)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EClientResponseState,
    (Sent)
    (Ack)
    (Done)
);

//! Provides a common base for both one-way and two-way responses.
class TClientResponse
    : public IClientResponseHandler
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TInstant, StartTime);
    DEFINE_BYREF_RW_PROPERTY(std::vector<TSharedRef>, Attachments);

    //! Returns address of the response sender, as it was provided by the channel configuration (FQDN, IP address, etc).
    //! Empty if it is not supported by the underlying RPC stack or the OK response has not been received yet.
    //! Note: complex channels choose destination dynamically (hedging, roaming), so the address is not known beforehand.
    const TString& GetAddress() const;

    const NProto::TResponseHeader& Header() const;

    TSharedRefArray GetResponseMessage() const;

    //! Returns total size: response message size plus attachments.
    size_t GetTotalSize() const;

protected:
    const TClientContextPtr ClientContext_;

    using EState = EClientResponseState;
    std::atomic<EState> State_ = {EState::Sent};


    explicit TClientResponse(TClientContextPtr clientContext);

    virtual bool TryDeserializeBody(TRef data, std::optional<NCompression::ECodec> codecId = {}) = 0;

    // IClientResponseHandler implementation.
    void HandleError(TError error) override;
    void HandleAcknowledgement() override;
    void HandleResponse(TSharedRefArray message, const std::string& address) override;
    void HandleStreamingPayload(const TStreamingPayload& payload) override;
    void HandleStreamingFeedback(const TStreamingFeedback& feedback) override;

    void Finish(const TError& error);

    virtual void SetPromise(const TError& error) = 0;

    const IInvokerPtr& GetInvoker();

private:
    TString Address_;
    NProto::TResponseHeader Header_;
    TSharedRefArray ResponseMessage_;

    void TraceResponse();
    void DoHandleError(TError error);

    void DoHandleResponse(TSharedRefArray message, const std::string& address);
    void Deserialize(TSharedRefArray responseMessage);
};

DEFINE_REFCOUNTED_TYPE(TClientResponse)

////////////////////////////////////////////////////////////////////////////////

template <class TResponseMessage>
class TTypedClientResponse
    : public TClientResponse
    , public TResponseMessage
{
public:
    using TResult = TIntrusivePtr<TTypedClientResponse>;

    explicit TTypedClientResponse(TClientContextPtr clientContext);

    TPromise<TResult> GetPromise();

private:
    TPromise<TResult> Promise_ = NewPromise<TResult>();


    void SetPromise(const TError& error) override;
    bool TryDeserializeBody(TRef data, std::optional<NCompression::ECodec> codecId = {}) override;
};

////////////////////////////////////////////////////////////////////////////////

struct TServiceDescriptor
{
    std::string ServiceName;
    std::string FullServiceName;
    std::string Namespace;
    TProtocolVersion ProtocolVersion = DefaultProtocolVersion;
    TFeatureIdFormatter FeatureIdFormatter = nullptr;
    bool AcceptsBaggage = true;

    explicit TServiceDescriptor(std::string serviceName);

    TServiceDescriptor& SetProtocolVersion(int majorVersion);
    TServiceDescriptor& SetProtocolVersion(TProtocolVersion version);
    TServiceDescriptor& SetNamespace(std::string value);
    TServiceDescriptor& SetAcceptsBaggage(bool value);
    template <class E>
    TServiceDescriptor& SetFeaturesType();
};

#define DEFINE_RPC_PROXY(type, name, ...) \
    static const ::NYT::NRpc::TServiceDescriptor& GetDescriptor() \
    { \
        static const auto Descriptor = ::NYT::NRpc::TServiceDescriptor(#name) __VA_ARGS__; \
        return Descriptor; \
    } \
    \
    explicit type(::NYT::NRpc::IChannelPtr channel) \
        : ::NYT::NRpc::TProxyBase(std::move(channel), GetDescriptor()) \
    { } \
    static_assert(true)

////////////////////////////////////////////////////////////////////////////////

struct TMethodDescriptor
{
    TString MethodName;
    EMultiplexingBand MultiplexingBand = EMultiplexingBand::Default;
    bool StreamingEnabled = false;

    explicit TMethodDescriptor(const TString& methodName);

    TMethodDescriptor& SetMultiplexingBand(EMultiplexingBand value);
    TMethodDescriptor& SetStreamingEnabled(bool value);
};

#define DEFINE_RPC_PROXY_METHOD_GENERIC(method, request, response, ...) \
    using TRsp##method = ::NYT::NRpc::TTypedClientResponse<response>; \
    using TReq##method = ::NYT::NRpc::TTypedClientRequest<request, TRsp##method>; \
    using TRsp##method##Ptr = ::NYT::TIntrusivePtr<TRsp##method>; \
    using TReq##method##Ptr = ::NYT::TIntrusivePtr<TReq##method>; \
    using TErrorOrRsp##method##Ptr = ::NYT::TErrorOr<TRsp##method##Ptr>; \
    \
    TReq##method##Ptr method() const \
    { \
        static const auto Descriptor = ::NYT::NRpc::TMethodDescriptor(#method) __VA_ARGS__; \
        return CreateRequest<TReq##method>(Descriptor); \
    } \
    static_assert(true)

#define DEFINE_RPC_PROXY_METHOD(ns, method, ...) \
    DEFINE_RPC_PROXY_METHOD_GENERIC(method, ns::TReq##method, ns::TRsp##method, __VA_ARGS__)

////////////////////////////////////////////////////////////////////////////////

class TProxyBase
{
public:
    DEFINE_BYVAL_RW_PROPERTY(std::optional<TDuration>, DefaultTimeout);
    DEFINE_BYVAL_RW_PROPERTY(std::optional<TDuration>, DefaultAcknowledgementTimeout);
    DEFINE_BYVAL_RW_PROPERTY(NCompression::ECodec, DefaultRequestCodec, NCompression::ECodec::None);
    DEFINE_BYVAL_RW_PROPERTY(NCompression::ECodec, DefaultResponseCodec, NCompression::ECodec::None);
    DEFINE_BYVAL_RW_PROPERTY(IMemoryUsageTrackerPtr, DefaultMemoryUsageTracker);
    DEFINE_BYVAL_RW_PROPERTY(bool, DefaultEnableLegacyRpcCodecs, true);

    DEFINE_BYREF_RW_PROPERTY(TStreamingParameters, DefaultClientAttachmentsStreamingParameters);
    DEFINE_BYREF_RW_PROPERTY(TStreamingParameters, DefaultServerAttachmentsStreamingParameters);

protected:
    const IChannelPtr Channel_;
    const TServiceDescriptor ServiceDescriptor_;

    TProxyBase(
        IChannelPtr channel,
        const TServiceDescriptor& descriptor);

    template <class T>
    TIntrusivePtr<T> CreateRequest(const TMethodDescriptor& methodDescriptor) const;
};

////////////////////////////////////////////////////////////////////////////////

class TGenericProxy
    : public TProxyBase
{
public:
    TGenericProxy(
        IChannelPtr channel,
        const TServiceDescriptor& descriptor);

    DEFINE_RPC_PROXY_METHOD(NProto, Discover);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

#define CLIENT_INL_H_
#include "client-inl.h"
#undef CLIENT_INL_H_

#pragma once

#include "public.h"

#include "protocol_version.h"

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/bus/public.h>

#include <yt/yt/core/net/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/concurrency/async_stream.h>

#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>

#include <yt/yt/core/compression/public.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

//! Represents an RPC request at server-side.
/*!
 *  \note
 *  Implementations are not thread-safe.
 */
struct IServiceContext
    : public virtual TRefCounted
{
    //! Returns raw header of the request being handled.
    virtual const NProto::TRequestHeader& GetRequestHeader() const = 0;

    //! Returns the message that contains the request being handled.
    virtual TSharedRefArray GetRequestMessage() const = 0;

    //! Returns the id of the request.
    /*!
     *  These ids are assigned by the client to distinguish between responses.
     *  The server should not rely on their uniqueness.
     *
     *  #NullRequestId is a possible value.
     */
    virtual TRequestId GetRequestId() const = 0;

    //! Returns the statistics from the underlying bus.
    /*!
     *  For implementations not using bus, returns all zeroes.
     */
    virtual NYT::NBus::TBusNetworkStatistics GetBusNetworkStatistics() const = 0;

    //! Returns the attributes of the connected endpoint.
    virtual const NYTree::IAttributeDictionary& GetEndpointAttributes() const = 0;

    //! Returns the description of the connected endpoint.
    virtual const TString& GetEndpointDescription() const = 0;

    //! Returns the instant when the current retry of request was issued by the client, if known.
    virtual std::optional<TInstant> GetStartTime() const = 0;

    //! Returns the client-specified request timeout, if any.
    virtual std::optional<TDuration> GetTimeout() const = 0;

    //! Returns the instant request has arrived.
    virtual TInstant GetArriveInstant() const = 0;

    //! Returns the instant request become executing (if it already has started executing).
    virtual std::optional<TInstant> GetRunInstant() const = 0;

    //! Returns the instant request finished, i.e. either replied or canceled (if it already has finished).
    virtual std::optional<TInstant> GetFinishInstant() const = 0;

    //! Returns time between request arrival and execution start (if it already has started executing).
    virtual std::optional<TDuration> GetWaitDuration() const = 0;

    //! Returns time between request execution start and the moment of reply or cancellation (if it already happened).
    virtual std::optional<TDuration> GetExecutionDuration() const = 0;

    //! Returns trace context associated with request.
    virtual NTracing::TTraceContextPtr GetTraceContext() const = 0;

    //! If trace context is present, return total CPU time accounted to this trace context.
    virtual std::optional<TDuration> GetTraceContextTime() const = 0;

    //! Returns |true| if this is a duplicate copy of a previously sent (and possibly served) request.
    virtual bool IsRetry() const = 0;

    //! Returns the mutation id for this request, i.e. a unique id used to distinguish copies of the
    //! (semantically) same request. If no mutation id is assigned then returns null id.
    virtual TMutationId GetMutationId() const = 0;

    //! Returns request service name.
    // NB: Service name is supposed to be short, so SSO should work.
    virtual std::string GetService() const = 0;

    //! Returns request method name.
    // NB: Method name is supposed to be short, so SSO should work.
    virtual std::string GetMethod() const = 0;

    //! Returns request realm id.
    virtual TRealmId GetRealmId() const = 0;

    //! Returns the authentication identity passed from the client (and possibly validated by the infrastructure).
    //! Could be used for authentication and authorization.
    virtual const TAuthenticationIdentity& GetAuthenticationIdentity() const = 0;

    //! Returns |true| if the request was already replied.
    virtual bool IsReplied() const = 0;

    //! Signals that the request processing is complete and sends reply to the client.
    virtual void Reply(const TError& error) = 0;

    //! Parses the message and forwards to the client.
    virtual void Reply(const TSharedRefArray& message) = 0;

    //! Called by the service request handler (prior to calling #Reply or #ReplyFrom) to indicate
    //! that the bulk of the request processing is complete.
    /*!
     *  Both calling and handling this method is completely optional.
     *
     *  Upon receiving this call, the current RPC infrastructure decrements the queue size counters and
     *  starts pumping more requests from the queue.
     */
    virtual void SetComplete() = 0;

    //! Returns |true| is the request was canceled.
    virtual bool IsCanceled() const = 0;

    //! Raised when request processing is canceled.
    DECLARE_INTERFACE_SIGNAL(void(const TError&), Canceled);

    //! Raised when Reply() was called. Allows doing some post-request stuff like extended structured logging.
    DECLARE_INTERFACE_SIGNAL(void(), Replied);

    //! Cancels request processing.
    /*!
     *  Implementations are free to ignore this call.
     */
    virtual void Cancel() = 0;

    //! Returns a future representing the response message.
    /*!
     *  \note
     *  Can only be called before the request handling is started.
     */
    virtual TFuture<TSharedRefArray> GetAsyncResponseMessage() const = 0;

    //! Returns the serialized response message.
    /*!
     *  \note
     *  Can only be called after the context is replied.
     */
    virtual const TSharedRefArray& GetResponseMessage() const = 0;

    //! Returns the error that was previously set by #Reply.
    /*!
     *  Can only be called after the context is replied.
     */
    virtual const TError& GetError() const = 0;

    //! Returns the request body.
    virtual TSharedRef GetRequestBody() const = 0;

    //! Returns the response body.
    virtual TSharedRef GetResponseBody() = 0;

    //! Sets the response body.
    virtual void SetResponseBody(const TSharedRef& responseBody) = 0;

    //! Returns a vector of request attachments.
    virtual std::vector<TSharedRef>& RequestAttachments() = 0;

    //! Returns the stream of asynchronous request attachments.
    virtual NConcurrency::IAsyncZeroCopyInputStreamPtr GetRequestAttachmentsStream() = 0;

    //! Returns a vector of response attachments.
    virtual std::vector<TSharedRef>& ResponseAttachments() = 0;

    //! Returns the stream of asynchronous response attachments.
    virtual NConcurrency::IAsyncZeroCopyOutputStreamPtr GetResponseAttachmentsStream() = 0;

    //! Returns immutable request header.
    virtual const NProto::TRequestHeader& RequestHeader() const = 0;

    //! Returns mutable request header.
    virtual NProto::TRequestHeader& RequestHeader() = 0;

    //! Returns true if request/response info logging is enabled.
    virtual bool IsLoggingEnabled() const = 0;

    //! Registers a portion of request logging info.
    /*!
     *  \param incremental If true then \p info is just remembered but no logging happens.
     *  If false then all remembered infos are logged (comma-separated).
     *  This must be the last call to #SetRawRequestInfo for this context.
     *
     *  Passing empty \p info in incremental mode is no-op.
     *  Passing empty \p info in non-incremental mode flushes the logging message.
     */
    virtual void SetRawRequestInfo(TString info, bool incremental) = 0;

    //! After this call there is no obligation to set request info for this request.
    virtual void SuppressMissingRequestInfoCheck() = 0;

    //! Registers a portion of response logging info.
    /*!
     *  \param incremental If false then \p info overrides all previously remembered infos.
     *  These infos are logged (comma-separated) when the context is replied.
     *
     *  Passing empty \p info in incremental mode is no-op.
     *  Passing empty \p info in non-incremental mode clears the logging infos.
     */
    virtual void SetRawResponseInfo(TString info, bool incremental) = 0;

    //! Returns the logger for request/response messages.
    virtual const NLogging::TLogger& GetLogger() const = 0;

    //! Returns the logging level for request/response messages.
    virtual NLogging::ELogLevel GetLogLevel() const = 0;

    //! Returns |true| if requests and responses are pooled.
    virtual bool IsPooled() const = 0;

    //! Returns the currently configured response codec.
    virtual NCompression::ECodec GetResponseCodec() const = 0;

    //! Changes the response codec.
    virtual void SetResponseCodec(NCompression::ECodec codec) = 0;

    // COPMAT(danilalexeev)
    //! Returnes true if response body has been serialized with compression.
    virtual bool IsResponseBodySerializedWithCompression() const = 0;
    virtual void SetResponseBodySerializedWithCompression() = 0;

    // Extension methods.

    void SetRequestInfo();
    void SetResponseInfo();

    template <class... TArgs>
    void SetRequestInfo(const char* format, TArgs&&... args);

    template <class... TArgs>
    void SetIncrementalRequestInfo(const char* format, TArgs&&... args);

    template <class... TArgs>
    void SetResponseInfo(const char* format, TArgs&&... args);

    template <class... TArgs>
    void SetIncrementalResponseInfo(const char* format, TArgs&&... args);

    //! Replies with a given message when the latter is set.
    void ReplyFrom(TFuture<TSharedRefArray> asyncMessage);

    //! Replies with a given error when the latter is set.
    void ReplyFrom(TFuture<void> asyncError);

    //! Replies with a given error when the latter is set via the #invoker.
    void ReplyFrom(TFuture<void> asyncError, const IInvokerPtr& invoker);

    template <class E>
    bool IsClientFeatureSupported(E featureId) const;
    template <class E>
    void ValidateClientFeature(E featureId) const;
};

DEFINE_REFCOUNTED_TYPE(IServiceContext)

////////////////////////////////////////////////////////////////////////////////

struct TServiceId
{
    TServiceId() = default;
    TServiceId(std::string serviceName, TRealmId realmId = NullRealmId);

    bool operator==(const TServiceId& other) const = default;

    std::string ServiceName;
    TRealmId RealmId;
};

TString ToString(const TServiceId& serviceId);

////////////////////////////////////////////////////////////////////////////////

//! Represents an abstract service registered within IServer.
/*!
 *  \note
 *  Implementations must be fully thread-safe.
 */
struct IService
    : public virtual TRefCounted
{
    //! Applies a new configuration with a given defaults.
    virtual void Configure(
        const TServiceCommonConfigPtr& configDefaults,
        const NYTree::INodePtr& config) = 0;

    //! Stops the service forbidding new requests to be served
    //! and returns the future that is set when all currently
    //! executing requests are finished.
    virtual TFuture<void> Stop() = 0;

    //! Returns the service id.
    virtual const TServiceId& GetServiceId() const = 0;

    //! Handles incoming request.
    virtual void HandleRequest(
        std::unique_ptr<NProto::TRequestHeader> header,
        TSharedRefArray message,
        NYT::NBus::IBusPtr replyBus) = 0;

    //! Handles request cancelation.
    virtual void HandleRequestCancellation(
        TRequestId requestId) = 0;

    //! Enables passing streaming data from clients to the service.
    virtual void HandleStreamingPayload(
        TRequestId requestId,
        const TStreamingPayload& payload) = 0;

    //! Enables clients to notify the service about their progress in receiving streaming data.
    virtual void HandleStreamingFeedback(
        TRequestId requestId,
        const TStreamingFeedback& feedback) = 0;
};

DEFINE_REFCOUNTED_TYPE(IService)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

//! A hasher for TServiceId.
template <>
struct THash<NYT::NRpc::TServiceId>
{
    inline size_t operator()(const NYT::NRpc::TServiceId& id) const
    {
        return
            THash<TString>()(id.ServiceName) * 497 +
            THash<NYT::NRpc::TRealmId>()(id.RealmId);
    }
};

////////////////////////////////////////////////////////////////////////////////

#define SERVICE_INL_H_
#include "service-inl.h"
#undef SERVICE_INL_H_

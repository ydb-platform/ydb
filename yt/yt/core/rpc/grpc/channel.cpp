#include "channel.h"
#include "config.h"
#include "dispatcher.h"
#include "helpers.h"

#include <yt/yt/core/misc/singleton.h>
#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/rpc/channel.h>
#include <yt/yt/core/rpc/channel_detail.h>
#include <yt/yt/core/rpc/message.h>
#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>

#include <yt/yt/core/ytree/fluent.h>

#include <contrib/libs/grpc/include/grpc/grpc.h>
#include <contrib/libs/grpc/src/core/lib/channel/call_tracer.h>
#include <contrib/libs/grpc/src/core/lib/surface/call.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>
#include <library/cpp/yt/threading/spin_lock.h>

#include <array>

namespace NYT::NRpc::NGrpc {
namespace {

using namespace NRpc;
using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;
using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

class TGrpcCallTracer final
    : public grpc_core::ClientCallTracer::CallAttemptTracer
{
public:
    void RecordAnnotation(y_absl::string_view /*annotation*/) override
    { }

    TString TraceId() override
    {
        return {};
    }

    TString SpanId() override
    {
        return {};
    }

    bool IsSampled() override
    {
        return false;
    }

    void RecordSendInitialMetadata(
        grpc_metadata_batch* /*send_initial_metadata*/) override
    { }

    void RecordSendTrailingMetadata(grpc_metadata_batch* /*send_trailing_metadata*/) override
    { }

    void RecordSendMessage(const grpc_core::SliceBuffer& /*send_message*/) override
    { }

    void RecordReceivedInitialMetadata(grpc_metadata_batch* /*recv_initial_metadata*/) override
    { }

    void RecordSendCompressedMessage(const grpc_core::SliceBuffer& /*send_compressed_message*/) override
    { }

    void RecordReceivedMessage(const grpc_core::SliceBuffer& /*recv_message*/) override
    { }

    void RecordReceivedDecompressedMessage(const grpc_core::SliceBuffer& /*recv_decompressed_message*/) override
    { }

    void RecordCancel(grpc_error_handle cancelError) override
    {
        auto current = Error_.get();
        Error_.set(cancelError);
    }

    TError GetError()
    {
        auto error = Error_.get();
        intptr_t statusCode;
        if (!grpc_error_get_int(error, grpc_core::StatusIntProperty::kRpcStatus, &statusCode)) {
            statusCode = GRPC_STATUS_UNKNOWN;
        }
        TString statusDetail;
        if (!grpc_error_get_str(error, grpc_core::StatusStrProperty::kDescription, &statusDetail)) {
            statusDetail = "Unknown error";
        }

        return TError(StatusCodeToErrorCode(static_cast<grpc_status_code>(statusCode)), statusDetail)
            << TErrorAttribute("status_code", statusCode);
    }

    void RecordReceivedTrailingMetadata(
        y_absl::Status /*status*/,
        grpc_metadata_batch* /*recv_trailing_metadata*/,
        const grpc_transport_stream_stats* /*transport_stream_stats*/) override
    { }

    void RecordEnd(const gpr_timespec& /*latency*/) override
    { }

private:
    AtomicError Error_;
};

DECLARE_REFCOUNTED_TYPE(TGrpcCallTracer)
DEFINE_REFCOUNTED_TYPE(TGrpcCallTracer)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TChannel)

DEFINE_ENUM(EClientCallStage,
    (SendingRequest)
    (ReceivingInitialMetadata)
    (ReceivingResponse)
);

class TChannel
    : public IChannel
{
public:
    explicit TChannel(TChannelConfigPtr config)
        : Config_(std::move(config))
        , EndpointAddress_(Config_->Address)
        , EndpointAttributes_(ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Item("address").Value(EndpointAddress_)
            .EndMap()))
        , Credentials_(
            Config_->Credentials ?
                LoadChannelCredentials(Config_->Credentials) :
                TGrpcChannelCredentialsPtr(grpc_insecure_credentials_create()))
    {
        TGrpcChannelArgs args(Config_->GrpcArguments);
        Channel_ = TGrpcChannelPtr(grpc_channel_create(
            Config_->Address.c_str(),
            Credentials_.Unwrap(),
            args.Unwrap()));
    }

    // IChannel implementation.
    const TString& GetEndpointDescription() const override
    {
        return EndpointAddress_;
    }

    const IAttributeDictionary& GetEndpointAttributes() const override
    {
        return *EndpointAttributes_;
    }

    IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) override
    {
        auto guard = ReaderGuard(SpinLock_);
        if (!TerminationError_.IsOK()) {
            auto error = TerminationError_;
            guard.Release();
            responseHandler->HandleError(error);
            return nullptr;
        }
        return New<TCallHandler>(
            this,
            options,
            std::move(request),
            std::move(responseHandler));
    }

    void Terminate(const TError& error) override
    {
        {
            auto guard = WriterGuard(SpinLock_);

            if (!TerminationError_.IsOK()) {
                return;
            }

            TerminationError_ = error;
            LibraryLock_.Reset();
            Channel_.Reset();
        }

        Terminated_.Fire(TerminationError_);
    }

    void SubscribeTerminated(const TCallback<void(const TError&)>& callback) override
    {
        Terminated_.Subscribe(callback);
    }

    void UnsubscribeTerminated(const TCallback<void(const TError&)>& callback) override
    {
        Terminated_.Unsubscribe(callback);
    }

    // Custom methods.
    const TString& GetEndpointAddress() const
    {
        return EndpointAddress_;
    }

    int GetInflightRequestCount() override
    {
        YT_UNIMPLEMENTED();
    }

private:
    const TChannelConfigPtr Config_;
    const TString EndpointAddress_;
    const IAttributeDictionaryPtr EndpointAttributes_;

    TSingleShotCallbackList<void(const TError&)> Terminated_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    TError TerminationError_;
    TGrpcLibraryLockPtr LibraryLock_ = TDispatcher::Get()->GetLibraryLock();
    TGrpcChannelPtr Channel_;
    TGrpcChannelCredentialsPtr Credentials_;


    class TCallHandler
        : public TCompletionQueueTag
        , public TClientRequestPerformanceProfiler
    {
    public:
        TCallHandler(
            TChannelPtr owner,
            const TSendOptions& options,
            IClientRequestPtr request,
            IClientResponseHandlerPtr responseHandler)
            : TClientRequestPerformanceProfiler(request->GetService(), request->GetMethod())
            , Owner_(std::move(owner))
            , Options_(options)
            , Request_(std::move(request))
            , ResponseHandler_(std::move(responseHandler))
            , GuardedCompletionQueue_(TDispatcher::Get()->PickRandomGuardedCompletionQueue())
            , Logger(GrpcLogger)
        {
            YT_LOG_DEBUG("Sending request (RequestId: %v, Method: %v.%v, Timeout: %v)",
                Request_->GetRequestId(),
                Request_->GetService(),
                Request_->GetMethod(),
                Options_.Timeout);

            {
                auto completionQueueGuard = GuardedCompletionQueue_->TryLock();
                if (!completionQueueGuard) {
                    NotifyError("Failed to initialize request call", TError{"Completion queue is shut down"});
                    return;
                }

                auto methodSlice = BuildGrpcMethodString();
                Call_ = TGrpcCallPtr(grpc_channel_create_call(
                    Owner_->Channel_.Unwrap(),
                    nullptr,
                    0,
                    completionQueueGuard->Unwrap(),
                    methodSlice,
                    nullptr,
                    GetDeadline(),
                    nullptr));
                grpc_slice_unref(methodSlice);

                Tracer_ = New<TGrpcCallTracer>();
                grpc_call_context_set(Call_.Unwrap(), GRPC_CONTEXT_CALL_TRACER, Tracer_.Get(), [] (void* ptr) {
                    NYT::Unref(static_cast<TGrpcCallTracer*>(ptr));
                });
                NYT::Ref(Tracer_.Get());
            }
            InitialMetadataBuilder_.Add(RequestIdMetadataKey, ToString(Request_->GetRequestId()));
            InitialMetadataBuilder_.Add(UserMetadataKey, Request_->GetUser());
            if (Request_->GetUserTag()) {
                InitialMetadataBuilder_.Add(UserTagMetadataKey, Request_->GetUserTag());
            }

            TProtocolVersion protocolVersion{
                Request_->Header().protocol_version_major(),
                Request_->Header().protocol_version_minor()
            };

            InitialMetadataBuilder_.Add(ProtocolVersionMetadataKey, ToString(protocolVersion));

            if (Request_->Header().HasExtension(NRpc::NProto::TCredentialsExt::credentials_ext)) {
                const auto& credentialsExt = Request_->Header().GetExtension(NRpc::NProto::TCredentialsExt::credentials_ext);
                if (credentialsExt.has_token()) {
                    InitialMetadataBuilder_.Add(AuthTokenMetadataKey, credentialsExt.token());
                }
                if (credentialsExt.has_session_id()) {
                    InitialMetadataBuilder_.Add(AuthSessionIdMetadataKey, credentialsExt.session_id());
                }
                if (credentialsExt.has_ssl_session_id()) {
                    InitialMetadataBuilder_.Add(AuthSslSessionIdMetadataKey, credentialsExt.ssl_session_id());
                }
                if (credentialsExt.has_user_ticket()) {
                    InitialMetadataBuilder_.Add(AuthUserTicketMetadataKey, credentialsExt.user_ticket());
                }
                if (credentialsExt.has_service_ticket()) {
                    InitialMetadataBuilder_.Add(AuthServiceTicketMetadataKey, credentialsExt.service_ticket());
                }
            }

            if (const auto traceContext = NTracing::TryGetCurrentTraceContext()) {
                InitialMetadataBuilder_.Add(TracingTraceIdMetadataKey, ToString(traceContext->GetTraceId()));
                InitialMetadataBuilder_.Add(TracingSpanIdMetadataKey, ToString(traceContext->GetSpanId()));
                if (traceContext->IsSampled()) {
                    InitialMetadataBuilder_.Add(TracingSampledMetadataKey, "1");
                }
                if (traceContext->IsDebug()) {
                    InitialMetadataBuilder_.Add(TracingDebugMetadataKey, "1");
                }
            }

            if (Request_->Header().HasExtension(NRpc::NProto::TCustomMetadataExt::custom_metadata_ext)) {
                const auto& customMetadataExt = Request_->Header().GetExtension(NRpc::NProto::TCustomMetadataExt::custom_metadata_ext);
                for (const auto& [key, value] : customMetadataExt.entries()) {
                    InitialMetadataBuilder_.Add(key.c_str(), value);
                }
            }

            try {
                RequestBody_ = Request_->Serialize();
            } catch (const std::exception& ex) {
                auto responseHandler = TryAcquireResponseHandler();
                YT_VERIFY(responseHandler);
                responseHandler->HandleError(TError(NRpc::EErrorCode::TransportError, "Request serialization failed")
                    << ex);
                return;
            }

            if (Request_->Header().has_request_codec()) {
                InitialMetadataBuilder_.Add(RequestCodecKey, ToString(Request_->Header().request_codec()));
            }
            if (Request_->Header().has_response_codec()) {
                InitialMetadataBuilder_.Add(ResponseCodecKey, ToString(Request_->Header().response_codec()));
            }

            YT_VERIFY(RequestBody_.Size() >= 2);
            TMessageWithAttachments messageWithAttachments;
            if (Request_->IsLegacyRpcCodecsEnabled()) {
                messageWithAttachments.Message = ExtractMessageFromEnvelopedMessage(RequestBody_[1]);
            } else {
                messageWithAttachments.Message = RequestBody_[1];
            }

            for (int index = 2; index < std::ssize(RequestBody_); ++index) {
                messageWithAttachments.Attachments.push_back(RequestBody_[index]);
            }

            RequestBodyBuffer_ = MessageWithAttachmentsToByteBuffer(messageWithAttachments);
            if (!messageWithAttachments.Attachments.empty()) {
                InitialMetadataBuilder_.Add(MessageBodySizeMetadataKey, ToString(messageWithAttachments.Message.Size()));
            }

            Ref();
            Stage_ = EClientCallStage::SendingRequest;

            std::array<grpc_op, 3> ops;

            ops[0].op = GRPC_OP_SEND_INITIAL_METADATA;
            ops[0].flags = 0;
            ops[0].reserved = nullptr;
            ops[0].data.send_initial_metadata.maybe_compression_level.is_set = false;
            ops[0].data.send_initial_metadata.metadata = InitialMetadataBuilder_.Unwrap();
            ops[0].data.send_initial_metadata.count = InitialMetadataBuilder_.GetSize();

            ops[1].op = GRPC_OP_SEND_MESSAGE;
            ops[1].data.send_message.send_message = RequestBodyBuffer_.Unwrap();
            ops[1].flags = 0;
            ops[1].reserved = nullptr;

            ops[2].op = GRPC_OP_SEND_CLOSE_FROM_CLIENT;
            ops[2].flags = 0;
            ops[2].reserved = nullptr;

            if (!TryStartBatch(ops)) {
                Unref();
            }
        }

        // TCompletionQueueTag overrides
        void Run(bool success, int /*cookie*/) override
        {
            const auto guard = TraceContext_.MakeTraceContextGuard();

            auto error = success ? TError() : Tracer_->GetError();

            switch (Stage_) {
                case EClientCallStage::SendingRequest:
                    OnRequestSent(error);
                    break;

                case EClientCallStage::ReceivingInitialMetadata:
                    OnInitialMetadataReceived(error);
                    break;

                case EClientCallStage::ReceivingResponse:
                    OnResponseReceived(error);
                    break;

                default:
                    YT_ABORT();
            }
        }

        // IClientRequestControl overrides
        void Cancel() override
        {
            auto result = grpc_call_cancel(Call_.Unwrap(), nullptr);
            YT_VERIFY(result == GRPC_CALL_OK);

            YT_LOG_DEBUG("Request canceled (RequestId: %v)", Request_->GetRequestId());

            NotifyError(
                TStringBuf("Request canceled"),
                TError(NYT::EErrorCode::Canceled, "Request canceled"));
        }

        TFuture<void> SendStreamingPayload(const TStreamingPayload& /*payload*/) override
        {
            YT_UNIMPLEMENTED();
        }

        TFuture<void> SendStreamingFeedback(const TStreamingFeedback& /*feedback*/) override
        {
            YT_UNIMPLEMENTED();
        }

    private:
        const TChannelPtr Owner_;
        const TSendOptions Options_;
        const IClientRequestPtr Request_;

        YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ResponseHandlerLock_);
        IClientResponseHandlerPtr ResponseHandler_;

        // Completion queue must be accessed under read lock
        // in order to prohibit creating new requests after shutting completion queue down.
        TGuardedGrpcCompletionQueue* GuardedCompletionQueue_;
        const NLogging::TLogger& Logger;

        NYT::NTracing::TTraceContextHandler TraceContext_;

        TGrpcCallPtr Call_;
        TGrpcCallTracerPtr Tracer_;
        TSharedRefArray RequestBody_;
        TGrpcByteBufferPtr RequestBodyBuffer_;
        TGrpcMetadataArray ResponseInitialMetadata_;
        TGrpcByteBufferPtr ResponseBodyBuffer_;
        TGrpcMetadataArray ResponseFinalMetadata_;
        grpc_status_code ResponseStatusCode_ = GRPC_STATUS_UNKNOWN;
        TGrpcSlice ResponseStatusDetails_;

        EClientCallStage Stage_;

        TGrpcMetadataArrayBuilder InitialMetadataBuilder_;


        IClientResponseHandlerPtr TryAcquireResponseHandler()
        {
            IClientResponseHandlerPtr result;

            auto guard = Guard(ResponseHandlerLock_);

            // NB! Reset response handler explicitly.
            // Implicit destruction in ~TCallHandler is impossible
            // because of the following cycle dependency:
            // futureState -> TCallHandler -> responseHandler -> futureState.
            result.Swap(ResponseHandler_);

            return result;
        }

        //! Builds /<service>/<method> string.
        grpc_slice BuildGrpcMethodString()
        {
            auto length =
                1 + // slash
                Request_->GetService().length() +
                1 + // slash
                Request_->GetMethod().length();
            auto slice = grpc_slice_malloc(length);
            auto* ptr = GRPC_SLICE_START_PTR(slice);
            *ptr++ = '/';
            ::memcpy(ptr, Request_->GetService().c_str(), Request_->GetService().length());
            ptr += Request_->GetService().length();
            *ptr++ = '/';
            ::memcpy(ptr, Request_->GetMethod().c_str(), Request_->GetMethod().length());
            ptr += Request_->GetMethod().length();
            YT_ASSERT(ptr == GRPC_SLICE_END_PTR(slice));
            return slice;
        }

        gpr_timespec GetDeadline() const
        {
            return Options_.Timeout
                ? gpr_time_add(
                    gpr_now(GPR_CLOCK_REALTIME),
                    gpr_time_from_micros(Options_.Timeout->MicroSeconds(), GPR_TIMESPAN))
                : gpr_inf_future(GPR_CLOCK_REALTIME);
        }

        void OnRequestSent(const TError& error)
        {
            if (!error.IsOK()) {
                NotifyError(TStringBuf("Failed to send request"), error);
                Unref();
                return;
            }

            YT_LOG_DEBUG("Request sent (RequestId: %v, Method: %v.%v)",
                Request_->GetRequestId(),
                Request_->GetService(),
                Request_->GetMethod());

            ProfileRequest(RequestBody_);

            Stage_ = EClientCallStage::ReceivingInitialMetadata;

            std::array<grpc_op, 1> ops;

            ops[0].op = GRPC_OP_RECV_INITIAL_METADATA;
            ops[0].flags = 0;
            ops[0].reserved = nullptr;
            ops[0].data.recv_initial_metadata.recv_initial_metadata = ResponseInitialMetadata_.Unwrap();

            if (!TryStartBatch(ops)) {
                Unref();
            }
        }

        void OnInitialMetadataReceived(const TError& error)
        {
            if (!error.IsOK()) {
                NotifyError(TStringBuf("Failed to receive initial response metadata"), error);
                Unref();
                return;
            }

            ProfileAcknowledgement();
            YT_LOG_DEBUG("Initial response metadata received (RequestId: %v)",
                Request_->GetRequestId());

            Stage_ = EClientCallStage::ReceivingResponse;

            std::array<grpc_op, 2> ops;

            ops[0].op = GRPC_OP_RECV_MESSAGE;
            ops[0].flags = 0;
            ops[0].reserved = nullptr;
            ops[0].data.recv_message.recv_message = ResponseBodyBuffer_.GetPtr();

            ops[1].op = GRPC_OP_RECV_STATUS_ON_CLIENT;
            ops[1].flags = 0;
            ops[1].reserved = nullptr;
            ops[1].data.recv_status_on_client.trailing_metadata = ResponseFinalMetadata_.Unwrap();
            ops[1].data.recv_status_on_client.status = &ResponseStatusCode_;
            ops[1].data.recv_status_on_client.status_details = ResponseStatusDetails_.Unwrap();
            ops[1].data.recv_status_on_client.error_string = nullptr;

            if (!TryStartBatch(ops)) {
                Unref();
            }
        }

        void OnResponseReceived(const TError& error)
        {
            auto guard = Finally([this] { Unref(); });

            if (!error.IsOK()) {
                NotifyError(TStringBuf("Failed to receive response"), error);
                return;
            }

            if (ResponseStatusCode_ != GRPC_STATUS_OK) {
                TError error;
                auto serializedError = ResponseFinalMetadata_.Find(ErrorMetadataKey);
                if (serializedError) {
                    error = DeserializeError(serializedError);
                } else {
                    error = TError(StatusCodeToErrorCode(ResponseStatusCode_), ResponseStatusDetails_.AsString())
                        << TErrorAttribute("status_code", ResponseStatusCode_);
                }
                NotifyError(TStringBuf("Request failed"), error);
                return;
            }

            if (!ResponseBodyBuffer_) {
                auto error = TError(NRpc::EErrorCode::ProtocolError, "Empty response body");
                NotifyError(TStringBuf("Request failed"), error);
                return;
            }

            std::optional<ui32> messageBodySize;

            auto messageBodySizeString = ResponseFinalMetadata_.Find(MessageBodySizeMetadataKey);
            if (messageBodySizeString) {
                try {
                    messageBodySize = FromString<ui32>(messageBodySizeString);
                } catch (const std::exception& ex) {
                    auto error = TError(NRpc::EErrorCode::TransportError, "Failed to parse response message body size")
                        << ex;
                    NotifyError(TStringBuf("Failed to parse response message body size"), error);
                    return;
                }
            }

            NRpc::NProto::TResponseHeader responseHeader;
            ToProto(responseHeader.mutable_request_id(), Request_->GetRequestId());
            if (Request_->Header().has_response_codec()) {
                responseHeader.set_codec(Request_->Header().response_codec());
            }

            TMessageWithAttachments messageWithAttachments;
            try {
                messageWithAttachments = ByteBufferToMessageWithAttachments(
                    ResponseBodyBuffer_.Unwrap(),
                    messageBodySize,
                    !responseHeader.has_codec());
            } catch (const std::exception& ex) {
                auto error = TError(NRpc::EErrorCode::TransportError, "Failed to receive request body") << ex;
                NotifyError(TStringBuf("Failed to receive request body"), error);
                return;
            }

            auto responseMessage = CreateResponseMessage(
                responseHeader,
                messageWithAttachments.Message,
                messageWithAttachments.Attachments);

            ProfileReply(responseMessage);
            NotifyResponse(std::move(responseMessage));
        }

        template <class TOps>
        bool TryStartBatch(const TOps& ops)
        {

            auto completionQueueGuard = GuardedCompletionQueue_->TryLock();
            if (!completionQueueGuard) {
                NotifyError("Failed to enqueue request operations batch", TError{"Completion queue is shut down"});
                return false;
            }

            auto result = grpc_call_start_batch(
                Call_.Unwrap(),
                ops.data(),
                ops.size(),
                GetTag(),
                nullptr);
            YT_VERIFY(result == GRPC_CALL_OK);
            return true;
        }

        void NotifyError(TStringBuf reason, const TError& error)
        {
            auto responseHandler = TryAcquireResponseHandler();
            if (!responseHandler) {
                return;
            }

            auto detailedError = error
                << TErrorAttribute("realm_id", Request_->GetRealmId())
                << TErrorAttribute("service", Request_->GetService())
                << TErrorAttribute("method", Request_->GetMethod())
                << TErrorAttribute("request_id", Request_->GetRequestId())
                << Owner_->GetEndpointAttributes();
            if (Options_.Timeout) {
                detailedError = detailedError
                    << TErrorAttribute("timeout", Options_.Timeout);
            }

            ProfileError(error);
            YT_LOG_DEBUG(detailedError, "%v (RequestId: %v)",
                reason,
                Request_->GetRequestId());

            responseHandler->HandleError(detailedError);
        }

        void NotifyResponse(TSharedRefArray message)
        {
            auto responseHandler = TryAcquireResponseHandler();
            if (!responseHandler) {
                return;
            }

            auto elapsed = ProfileComplete();
            YT_LOG_DEBUG("Response received (RequestId: %v, Method: %v.%v, TotalTime: %v)",
                Request_->GetRequestId(),
                Request_->GetService(),
                Request_->GetMethod(),
                elapsed);

            responseHandler->HandleResponse(
                std::move(message),
                /*address*/ Owner_->GetEndpointAddress());
        }
    };
};

DEFINE_REFCOUNTED_TYPE(TChannel)

////////////////////////////////////////////////////////////////////////////////

class TChannelFactory
    : public IChannelFactory
{
public:
    IChannelPtr CreateChannel(const TString& address) override
    {
        auto config = New<TChannelConfig>();
        config->Address = address;
        return CreateGrpcChannel(config);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

IChannelPtr CreateGrpcChannel(TChannelConfigPtr config)
{
    return New<TChannel>(std::move(config));
}

IChannelFactoryPtr GetGrpcChannelFactory()
{
    return LeakyRefCountedSingleton<TChannelFactory>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NGrpc

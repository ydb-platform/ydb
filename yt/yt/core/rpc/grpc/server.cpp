#include "server.h"
#include "dispatcher.h"
#include "config.h"
#include "helpers.h"

#include <yt/yt/core/rpc/grpc/proto/grpc.pb.h>

#include <yt/yt/core/misc/shutdown_priorities.h>

#include <yt/yt/core/rpc/dispatcher.h>
#include <yt/yt/core/rpc/message.h>
#include <yt/yt/core/rpc/server_detail.h>

#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>

#include <yt/yt/core/bus/bus.h>

#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/shutdown.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

#include <library/cpp/string_utils/quote/quote.h>

#include <contrib/libs/grpc/include/grpc/grpc.h>
#include <contrib/libs/grpc/include/grpc/grpc_security.h>

#include <array>

namespace NYT::NRpc::NGrpc {

using namespace NRpc;
using namespace NBus;
using namespace NYTree;
using namespace NConcurrency;
using namespace NNet;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TServer)

DEFINE_ENUM(EServerCallStage,
    (Accept)
    (ReceivingRequest)
    (SendingInitialMetadata)
    (WaitingForService)
    (SendingResponse)
    (WaitingForClose)
    (Done)
);

DEFINE_ENUM(EServerCallCookie,
    (Normal)
    (Close)
);

class TServer
    : public TServerBase
{
public:
    explicit TServer(TServerConfigPtr config)
        : TServerBase(GrpcLogger().WithTag("GrpcServerId: %v", TGuid::Create()))
        , Config_(std::move(config))
        , ShutdownCookie_(RegisterShutdownCallback(
            "GrpcServer",
            BIND_NO_PROPAGATE(&TServer::Shutdown, MakeWeak(this), /*graceful*/ true),
            /*priority*/ GrpcServerShutdownPriority))
        , LibraryLock_(TDispatcher::Get()->GetLibraryLock())
        , Profiler_(GrpcServerProfiler.WithTag("name", Config_->ProfilingName))
        , CompletionQueue_(TDispatcher::Get()->PickRandomGuardedCompletionQueue()->UnwrapUnsafe())
    {
        Profiler_.AddFuncGauge("/call_handler_count", MakeStrong(this), [this] {
            return CallHandlerCount_.load();
        });
    }

private:
    const TServerConfigPtr Config_;
    const TShutdownCookie ShutdownCookie_;

    const TGrpcLibraryLockPtr LibraryLock_;

    NProfiling::TProfiler Profiler_;

    // This CompletionQueue_ does not have to be guarded in contrast to Channels completion queue,
    // because we require server to be shut down before completion queues.
    grpc_completion_queue* const CompletionQueue_;

    TGrpcServerPtr Native_;
    std::vector<TGrpcServerCredentialsPtr> Credentials_;

    std::atomic<int> CallHandlerCount_ = {0};
    std::atomic<bool> ShutdownStarted_ = {false};
    TPromise<void> ShutdownPromise_ = NewPromise<void>();


    void OnCallHandlerConstructed()
    {
        ++CallHandlerCount_;
    }

    void OnCallHandlerDestroyed()
    {
        if (--CallHandlerCount_ > 0) {
            return;
        }

        Cleanup();
        ShutdownPromise_.SetFrom(TServerBase::DoStop(true));
        Unref();
    }


    void DoStart() override
    {
        TGrpcChannelArgs args(Config_->GrpcArguments);

        Native_ = TGrpcServerPtr(grpc_server_create(
            args.Unwrap(),
            nullptr));

        grpc_server_register_completion_queue(
            Native_.Unwrap(),
            CompletionQueue_,
            nullptr);

        try {
            for (const auto& addressConfig : Config_->Addresses) {
                int result;
                if (addressConfig->Credentials) {
                    Credentials_.push_back(LoadServerCredentials(addressConfig->Credentials));
                } else {
                    Credentials_.emplace_back(grpc_insecure_server_credentials_create());
                }
                result = grpc_server_add_http2_port(
                    Native_.Unwrap(),
                    addressConfig->Address.c_str(),
                    Credentials_.back().Unwrap());
                if (result == 0) {
                    THROW_ERROR_EXCEPTION("Error configuring server to listen at %Qv",
                        addressConfig->Address);
                }
                YT_LOG_DEBUG("Server address configured (Address: %v)", addressConfig->Address);
            }
        } catch (const std::exception& ex) {
            Cleanup();
            throw;
        }

        grpc_server_start(Native_.Unwrap());

        Ref();

        // This instance is fake; see DoStop.
        OnCallHandlerConstructed();

        TServerBase::DoStart();

        New<TCallHandler>(this);
    }

    void Shutdown(bool graceful)
    {
        try {
            DoStop(graceful).Get().ThrowOnError();
        } catch (...) {
            if (auto* logFile = TryGetShutdownLogFile()) {
                ::fprintf(logFile, "%s\tGRPC server shutdown failed: %s (ThreadId: %" PRISZT ")\n",
                    GetInstant().ToString().c_str(),
                    CurrentExceptionMessage().c_str(),
                    GetCurrentThreadId());
            }
        }
    }

    TFuture<void> DoStop(bool graceful) override
    {
        class TStopTag
            : public TCompletionQueueTag
        {
        public:
            explicit TStopTag(TServerPtr owner)
                : Owner_(std::move(owner))
            { }

            void Run(bool success, int /*cookie*/) override
            {
                YT_VERIFY(success);
                Owner_->OnCallHandlerDestroyed();
                delete this;
            }

        private:
            const TServerPtr Owner_;
        };

        if (!ShutdownStarted_.exchange(true)) {
            YT_VERIFY(Native_);
            auto* shutdownTag = new TStopTag(this);

            grpc_server_shutdown_and_notify(Native_.Unwrap(), CompletionQueue_, shutdownTag->GetTag());

            if (!graceful) {
                grpc_server_cancel_all_calls(Native_.Unwrap());
            }
        }
        return ShutdownPromise_;
    }

    void Cleanup()
    {
        Native_.Reset();
    }

    class TCallHandler;

    class TReplyBus
        : public IBus
    {
    public:
        explicit TReplyBus(TCallHandler* handler)
            : Handler_(MakeWeak(handler))
            , PeerAddress_(handler->PeerAddress_)
            , PeerAddressString_(handler->PeerAddressString_)
            , EndpointAttributes_(ConvertToAttributes(BuildYsonStringFluently()
                .BeginMap()
                    .Item("address").Value(PeerAddressString_)
                .EndMap()))
        { }

        // IBus overrides.
        const TString& GetEndpointDescription() const override
        {
            return PeerAddressString_;
        }

        const NYTree::IAttributeDictionary& GetEndpointAttributes() const override
        {
            return *EndpointAttributes_;
        }

        TBusNetworkStatistics GetNetworkStatistics() const override
        {
            return {};
        }

        const std::string& GetEndpointAddress() const override
        {
            return PeerAddressString_;
        }

        const NNet::TNetworkAddress& GetEndpointNetworkAddress() const override
        {
            return PeerAddress_;
        }

        bool IsEndpointLocal() const override
        {
            return false;
        }

        bool IsEncrypted() const override
        {
            return false;
        }

        TFuture<void> GetReadyFuture() const override
        {
            return VoidFuture;
        }

        TFuture<void> Send(TSharedRefArray message, const NBus::TSendOptions& /*options*/) override
        {
            if (auto handler = Handler_.Lock()) {
                handler->OnResponseMessage(std::move(message));
            }
            return {};
        }

        void SetTosLevel(TTosLevel /*tosLevel*/) override
        { }

        void Terminate(const TError& error) override
        {
            TerminatedList_.Fire(error);
        }

        void SubscribeTerminated(const TCallback<void(const TError&)>& callback) override
        {
            TerminatedList_.Subscribe(callback);
        }

        void UnsubscribeTerminated(const TCallback<void(const TError&)>& callback) override
        {
            TerminatedList_.Unsubscribe(callback);
        }

    private:
        const TWeakPtr<TCallHandler> Handler_;
        const TNetworkAddress PeerAddress_;
        const TString PeerAddressString_;
        const IAttributeDictionaryPtr EndpointAttributes_;

        TSingleShotCallbackList<void(const TError&)> TerminatedList_;
    };

    class TCallHandler
        : public TCompletionQueueTag
        , public TRefCounted
    {
    public:
        explicit TCallHandler(TServerPtr owner)
            : Owner_(std::move(owner))
            , CompletionQueue_(TDispatcher::Get()->PickRandomGuardedCompletionQueue()->UnwrapUnsafe())
            , Logger(Owner_->Logger)
        {
            auto result = grpc_server_request_call(
                Owner_->Native_.Unwrap(),
                Call_.GetPtr(),
                CallDetails_.Unwrap(),
                CallMetadata_.Unwrap(),
                CompletionQueue_,
                Owner_->CompletionQueue_,
                GetTag());
            YT_VERIFY(result == GRPC_CALL_OK);

            Ref();
            Owner_->OnCallHandlerConstructed();
        }

        ~TCallHandler()
        {
            Owner_->OnCallHandlerDestroyed();
        }

        // TCompletionQueueTag overrides
        void Run(bool success, int cookie_) override
        {
            const auto traceContextGuard = [&] {
                auto guard = Guard(TraceContextSpinLock_);
                return TraceContextHandler_.MakeTraceContextGuard();
            }();

            auto cookie = static_cast<EServerCallCookie>(cookie_);
            switch (cookie) {
                case EServerCallCookie::Normal:
                {
                    const auto stage = [&] {
                        auto guard = Guard(SpinLock_);
                        return Stage_;
                    }();
                    switch (stage) {
                        case EServerCallStage::Accept:
                            OnAccepted(success);
                            break;

                        case EServerCallStage::ReceivingRequest:
                            OnRequestReceived(success);
                            break;

                        case EServerCallStage::SendingInitialMetadata:
                            OnInitialMetadataSent(success);
                            break;

                        case EServerCallStage::SendingResponse:
                            OnResponseSent(success);
                            break;

                        default:
                            YT_ABORT();
                    }
                    break;
                }
                case EServerCallCookie::Close:
                    OnCloseReceived(success);
                    break;

                default:
                    YT_ABORT();
            }
        }

    private:
        friend class TReplyBus;

        const TServerPtr Owner_;

        grpc_completion_queue* const CompletionQueue_;
        const NLogging::TLogger& Logger;

        YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
        EServerCallStage Stage_ = EServerCallStage::Accept;
        bool CancelRequested_ = false;
        TSharedRefArray ResponseMessage_;

        TString PeerAddressString_;
        TNetworkAddress PeerAddress_;

        TRequestId RequestId_;
        std::optional<TString> User_;
        std::optional<TString> UserTag_;
        std::optional<TString> UserAgent_;
        std::optional<NGrpc::NProto::TSslCredentialsExt> SslCredentialsExt_;
        std::optional<NRpc::NProto::TCredentialsExt> RpcCredentialsExt_;
        std::optional<NRpc::NProto::TCustomMetadataExt> CustomMetadataExt_;
        std::optional<NTracing::NProto::TTracingExt> TraceContext_;
        TString ServiceName_;
        TString MethodName_;
        std::optional<TDuration> Timeout_;
        NCompression::ECodec RequestCodec_ = NCompression::ECodec::None;
        NCompression::ECodec ResponseCodec_ = NCompression::ECodec::None;
        IServicePtr Service_;

        TGrpcMetadataArrayBuilder InitialMetadataBuilder_;
        TGrpcMetadataArrayBuilder TrailingMetadataBuilder_;

        TGrpcCallDetails CallDetails_;
        TGrpcMetadataArray CallMetadata_;
        TGrpcCallPtr Call_;
        TGrpcByteBufferPtr RequestBodyBuffer_;
        std::optional<ui32> RequestMessageBodySize_;
        TProtocolVersion ProtocolVersion_ = DefaultProtocolVersion;
        TGrpcByteBufferPtr ResponseBodyBuffer_;
        TString ErrorMessage_;
        TGrpcSlice ErrorMessageSlice_;
        int RawCanceled_ = 0;

        IBusPtr ReplyBus_;

        YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, TraceContextSpinLock_);
        NTracing::TTraceContextHandler TraceContextHandler_;

        template <class TOps>
        void StartBatch(const TOps& ops, EServerCallCookie cookie)
        {
            auto result = grpc_call_start_batch(
                Call_.Unwrap(),
                ops.data(),
                ops.size(),
                GetTag(static_cast<int>(cookie)),
                nullptr);
            YT_VERIFY(result == GRPC_CALL_OK);
        }

        void OnAccepted(bool success)
        {
            if (!success) {
                // This normally happens on server shutdown.
                YT_LOG_DEBUG("Server accept failed");
                Unref();
                return;
            }

            New<TCallHandler>(Owner_);

            ParseRequestId();

            if (!TryParsePeerAddress()) {
                YT_LOG_WARNING("Malformed peer address (PeerAddress: %v, RequestId: %v)",
                    PeerAddressString_,
                    RequestId_);
                Unref();
                return;
            }

            ParseTraceContext();
            ParseUser();
            ParseUserTag();
            ParseUserAgent();
            ParseRpcCredentials();
            ParseCustomMetadata();
            ParseTimeout();
            ParseRequestCodec();
            ParseResponseCodec();

            try {
                SslCredentialsExt_ = WaitFor(ParseSslCredentials())
                    .ValueOrThrow();
            } catch (const std::exception& ex) {
                YT_LOG_DEBUG(ex, "Failed to parse ssl credentials (RequestId: %v)",
                    RequestId_);
                Unref();
                return;
            }

            if (!TryParseRoutingParameters()) {
                YT_LOG_DEBUG("Malformed request routing parameters (RawMethod: %v, RequestId: %v)",
                    ToStringBuf(CallDetails_->method),
                    RequestId_);
                Unref();
                return;
            }

            if (!TryParseMessageBodySize()) {
                Unref();
                return;
            }

            if (!TryParseProtocolVersion()) {
                Unref();
                return;
            }

            YT_LOG_DEBUG("Request accepted (RequestId: %v, Host: %v, Method: %v.%v, %v%vPeerAddress: %v, Timeout: %v, ProtocolVersion: %v)",
                RequestId_,
                ToStringBuf(CallDetails_->host),
                ServiceName_,
                MethodName_,
                MakeFormatterWrapper([&] (auto* builder) {
                    if (User_) {
                        builder->AppendFormat("User: %v, ", *User_);
                    }
                }),
                MakeFormatterWrapper([&] (auto* builder) {
                    if (User_ && UserTag_ && *UserTag_ != *User_) {
                        builder->AppendFormat("UserTag: %v, ", *UserTag_);
                    }
                }),
                PeerAddressString_,
                Timeout_,
                ProtocolVersion_);

            Service_ = Owner_->FindService(TServiceId(ServiceName_));

            {
                auto guard = Guard(SpinLock_);
                Stage_ = EServerCallStage::ReceivingRequest;
            }

            std::array<grpc_op, 1> ops;

            ops[0].op = GRPC_OP_RECV_MESSAGE;
            ops[0].flags = 0;
            ops[0].reserved = nullptr;
            ops[0].data.recv_message.recv_message = RequestBodyBuffer_.GetPtr();

            StartBatch(ops, EServerCallCookie::Normal);
        }

        bool TryParsePeerAddress()
        {
            auto addressString = MakeGprString(grpc_call_get_peer(Call_.Unwrap()));
            PeerAddressString_ = TString(addressString.get());

            // Drop ipvN: prefix.
            if (PeerAddressString_.StartsWith("ipv6:") || PeerAddressString_.StartsWith("ipv4:")) {
                PeerAddressString_ = PeerAddressString_.substr(5);
            }

            if (PeerAddressString_.StartsWith("unix:")) {
                PeerAddress_ = NNet::TNetworkAddress::CreateUnixDomainSocketAddress(PeerAddressString_.substr(5));
                return true;
            }

            // Decode URL-encoded square brackets.
            CGIUnescape(PeerAddressString_);

            auto address = NNet::TNetworkAddress::TryParse(PeerAddressString_);
            if (!address.IsOK()) {
                return false;
            }

            PeerAddress_ = address.Value();
            return true;
        }

        void ParseTraceContext()
        {
            const auto traceIdString = CallMetadata_.Find(TracingTraceIdMetadataKey);
            const auto spanIdString = CallMetadata_.Find(TracingSpanIdMetadataKey);
            const auto sampledString = CallMetadata_.Find(TracingSampledMetadataKey);
            const auto debugString = CallMetadata_.Find(TracingDebugMetadataKey);

            if (!traceIdString &&
                !spanIdString &&
                !sampledString &&
                !debugString)
            {
                return;
            }

            NTracing::NProto::TTracingExt traceContext{};
            if (traceIdString) {
                TGuid traceId;
                if (!TGuid::FromString(traceIdString, &traceId)) {
                    return;
                }
                ToProto(traceContext.mutable_trace_id(), traceId);
            }
            if (spanIdString) {
                NTracing::TSpanId spanId;
                if (!TryFromString(spanIdString, spanId)) {
                    return;
                }
                traceContext.set_span_id(spanId);
            }
            if (sampledString) {
                bool sampled;
                if (!TryFromString(sampledString, sampled)) {
                    return;
                }
                traceContext.set_sampled(sampled);
            }
            if (debugString) {
                bool debug;
                if (!TryFromString(debugString, debug)) {
                    return;
                }
                traceContext.set_debug(debug);
            }
            TraceContext_.emplace(traceContext);
        }

        void ParseRequestId()
        {
            auto idString = CallMetadata_.Find(RequestIdMetadataKey);
            if (!idString) {
                RequestId_ = TRequestId::Create();
                return;
            }

            if (!TRequestId::FromString(idString, &RequestId_)) {
                RequestId_ = TRequestId::Create();
                YT_LOG_WARNING("Malformed request id, using a random one (MalformedRequestId: %v, RequestId: %v)",
                    idString,
                    RequestId_);
            }
        }

        void ParseUser()
        {
            auto userString = CallMetadata_.Find(UserMetadataKey);
            if (!userString) {
                return;
            }

            User_ = TString(userString);
        }

        void ParseUserTag()
        {
            auto userTagString = CallMetadata_.Find(UserTagMetadataKey);
            if (!userTagString) {
                return;
            }

            UserTag_ = TString(userTagString);
        }

        void ParseUserAgent()
        {
            auto userAgentString = CallMetadata_.Find(UserAgentMetadataKey);
            if (!userAgentString) {
                return;
            }

            UserAgent_ = TString(userAgentString);
        }

        void ParseRequestCodec()
        {
            auto requestCodecString = CallMetadata_.Find(RequestCodecKey);
            if (!requestCodecString) {
                return;
            }

            NCompression::ECodec codecId;
            int intCodecId;
            if (!TryFromString(requestCodecString, intCodecId)) {
                YT_LOG_WARNING("Failed to parse request codec from request metadata (RequestId: %v)",
                    RequestId_);
                return;
            }
            if (!TryEnumCast(intCodecId, &codecId)) {
                YT_LOG_WARNING("Request codec %v is not supported (RequestId: %v)",
                    intCodecId,
                    RequestId_);
                return;
            }

            RequestCodec_ = codecId;
        }

        void ParseResponseCodec()
        {
            auto responseCodecString = CallMetadata_.Find(ResponseCodecKey);
            if (!responseCodecString) {
                return;
            }

            NCompression::ECodec codecId;
            int intCodecId;
            if (!TryFromString(responseCodecString, intCodecId)) {
                YT_LOG_WARNING("Failed to parse response codec from request metadata (RequestId: %v)",
                    RequestId_);
                return;
            }
            if (!TryEnumCast(intCodecId, &codecId)) {
                YT_LOG_WARNING("Response codec %v is not supported (RequestId: %v)",
                    intCodecId,
                    RequestId_);
                return;
            }

            ResponseCodec_ = codecId;
        }

        void ParseRpcCredentials()
        {
            auto tokenString = CallMetadata_.Find(AuthTokenMetadataKey);
            auto sessionIdString = CallMetadata_.Find(AuthSessionIdMetadataKey);
            auto sslSessionIdString = CallMetadata_.Find(AuthSslSessionIdMetadataKey);
            auto userTicketString = CallMetadata_.Find(AuthUserTicketMetadataKey);
            auto serviceTicketString = CallMetadata_.Find(AuthServiceTicketMetadataKey);

            if (!tokenString &&
                !sessionIdString &&
                !sslSessionIdString &&
                !userTicketString &&
                !serviceTicketString)
            {
                return;
            }

            RpcCredentialsExt_.emplace();

            if (tokenString) {
                RpcCredentialsExt_->set_token(TString(tokenString));
            }
            if (sessionIdString) {
                RpcCredentialsExt_->set_session_id(TString(sessionIdString));
            }
            if (sslSessionIdString) {
                RpcCredentialsExt_->set_ssl_session_id(TString(sslSessionIdString));
            }
            if (userTicketString) {
                RpcCredentialsExt_->set_user_ticket(TString(userTicketString));
            }
            if (serviceTicketString) {
                RpcCredentialsExt_->set_service_ticket(TString(serviceTicketString));
            }
        }

        void ParseCustomMetadata()
        {
            for (const auto& [key, value] : CallMetadata_.ToMap()) {
                if (!GetNativeMetadataKeys().contains(key)) {
                    if (!CustomMetadataExt_) {
                        CustomMetadataExt_.emplace();
                    }
                    (*CustomMetadataExt_->mutable_entries())[key] = value;
                }
            }
        }

        TFuture<std::optional<NGrpc::NProto::TSslCredentialsExt>> ParseSslCredentials()
        {
            auto authContext = TGrpcAuthContextPtr(grpc_call_auth_context(Call_.Unwrap()));
            return BIND(&DoParseSslCredentials, Passed(std::move(authContext)))
                .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker())
                .Run();
        }

        static std::optional<NGrpc::NProto::TSslCredentialsExt> DoParseSslCredentials(TGrpcAuthContextPtr authContext)
        {
            if (!authContext) {
                return std::nullopt;
            }

            std::optional<NGrpc::NProto::TSslCredentialsExt> sslCredentialsExtension;

            ParsePeerIdentity(authContext, &sslCredentialsExtension);
            ParseIssuer(authContext, &sslCredentialsExtension);

            return sslCredentialsExtension;
        }

        static void ParsePeerIdentity(const TGrpcAuthContextPtr& authContext, std::optional<NGrpc::NProto::TSslCredentialsExt>* sslCredentialsExtension)
        {
            const char* peerIdentityPropertyName = grpc_auth_context_peer_identity_property_name(authContext.Unwrap());
            if (!peerIdentityPropertyName) {
                return;
            }

            auto peerIdentityPropertyIt = grpc_auth_context_find_properties_by_name(authContext.Unwrap(), peerIdentityPropertyName);
            auto* peerIdentityProperty = grpc_auth_property_iterator_next(&peerIdentityPropertyIt);
            if (!peerIdentityProperty) {
                return;
            }

            if (!sslCredentialsExtension->has_value()) {
                sslCredentialsExtension->emplace();
            }
            (*sslCredentialsExtension)->set_peer_identity(TString(peerIdentityProperty->value, peerIdentityProperty->value_length));
        }

        static void ParseIssuer(const TGrpcAuthContextPtr& authContext, std::optional<NGrpc::NProto::TSslCredentialsExt>* sslCredentialsExtension)
        {
            const char* peerIdentityPropertyName = grpc_auth_context_peer_identity_property_name(authContext.Unwrap());
            if (!peerIdentityPropertyName) {
                return;
            }

            auto pemCertPropertyIt = grpc_auth_context_find_properties_by_name(authContext.Unwrap(), GRPC_X509_PEM_CERT_PROPERTY_NAME);
            auto* pemCertProperty = grpc_auth_property_iterator_next(&pemCertPropertyIt);
            if (!pemCertProperty) {
                return;
            }

            auto issuer = ParseIssuerFromX509(TStringBuf(pemCertProperty->value, pemCertProperty->value_length));
            if (!issuer) {
                return;
            }

            if (!sslCredentialsExtension->has_value()) {
                sslCredentialsExtension->emplace();
            }
            (*sslCredentialsExtension)->set_issuer(std::move(*issuer));
        }

        void ParseTimeout()
        {
            auto deadline = CallDetails_->deadline;
            deadline = gpr_convert_clock_type(deadline, GPR_CLOCK_REALTIME);
            auto now = gpr_now(GPR_CLOCK_REALTIME);
            if (gpr_time_cmp(now, deadline) >= 0) {
                Timeout_ = TDuration::Zero();
                return;
            }

            auto micros = gpr_timespec_to_micros(gpr_time_sub(deadline, now));
            if (micros > static_cast<double>(std::numeric_limits<ui64>::max() / 2)) {
                return;
            }

            Timeout_ = TDuration::MicroSeconds(static_cast<ui64>(micros));
        }

        bool TryParseRoutingParameters()
        {
            const size_t methodLength = GRPC_SLICE_LENGTH(CallDetails_->method);
            if (methodLength == 0) {
                return false;
            }

            if (*GRPC_SLICE_START_PTR(CallDetails_->method) != '/') {
                return false;
            }

            auto methodWithoutLeadingSlash = grpc_slice_sub_no_ref(CallDetails_->method, 1, methodLength);
            const int secondSlashIndex = grpc_slice_chr(methodWithoutLeadingSlash, '/');
            if (secondSlashIndex < 0) {
                return false;
            }

            const char *serviceNameStart = reinterpret_cast<const char *>(GRPC_SLICE_START_PTR(methodWithoutLeadingSlash));
            ServiceName_.assign(serviceNameStart, secondSlashIndex);
            MethodName_.assign(serviceNameStart + secondSlashIndex + 1, methodLength - 1 - (secondSlashIndex + 1));
            return true;
        }

        bool TryParseMessageBodySize()
        {
            auto messageBodySizeString = CallMetadata_.Find(MessageBodySizeMetadataKey);
            if (!messageBodySizeString) {
                return true;
            }

            try {
                RequestMessageBodySize_ = FromString<ui32>(messageBodySizeString);
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Failed to parse message body size from request metadata (RequestId: %v)",
                    RequestId_);
                return false;
            }

            return true;
        }

        bool TryParseProtocolVersion()
        {
            auto protocolVersionString = CallMetadata_.Find(ProtocolVersionMetadataKey);
            if (!protocolVersionString) {
                return true;
            }

            try {
                ProtocolVersion_ = TProtocolVersion::FromString(protocolVersionString);
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Failed to parse protocol version from string (RequestId: %v)",
                    RequestId_);
                return false;
            }

            return true;
        }

        void OnRequestReceived(bool success)
        {
            if (!success) {
                YT_LOG_DEBUG("Failed to receive request body (RequestId: %v)",
                    RequestId_);
                Unref();
                return;
            }

            if (!RequestBodyBuffer_) {
                YT_LOG_DEBUG("Empty request body received (RequestId: %v)",
                    RequestId_);
                Unref();
                return;
            }

            auto header = std::make_unique<NRpc::NProto::TRequestHeader>();
            ToProto(header->mutable_request_id(), RequestId_);
            if (User_) {
                header->set_user(*User_);
            }
            if (UserTag_) {
                header->set_user_tag(*UserTag_);
            }
            if (UserAgent_) {
                header->set_user_agent(*UserAgent_);
            }
            if (TraceContext_) {
                *header->MutableExtension(NRpc::NProto::TRequestHeader::tracing_ext) = std::move(*TraceContext_);
            }
            header->set_service(ServiceName_);
            header->set_method(MethodName_);
            header->set_protocol_version_major(ProtocolVersion_.Major);
            header->set_protocol_version_minor(ProtocolVersion_.Minor);
            header->set_request_codec(ToProto<int>(RequestCodec_));
            header->set_response_codec(ToProto<int>(ResponseCodec_));

            if (Timeout_) {
                header->set_timeout(ToProto<i64>(*Timeout_));
            }
            if (SslCredentialsExt_) {
                *header->MutableExtension(NGrpc::NProto::TSslCredentialsExt::ssl_credentials_ext) = std::move(*SslCredentialsExt_);
            }
            if (RpcCredentialsExt_) {
                *header->MutableExtension(NRpc::NProto::TCredentialsExt::credentials_ext) = std::move(*RpcCredentialsExt_);
            }
            if (CustomMetadataExt_) {
                *header->MutableExtension(NRpc::NProto::TCustomMetadataExt::custom_metadata_ext) = std::move(*CustomMetadataExt_);
            }

            TMessageWithAttachments messageWithAttachments;
            try {
                messageWithAttachments = ByteBufferToMessageWithAttachments(
                    RequestBodyBuffer_.Unwrap(),
                    RequestMessageBodySize_,
                    !header->has_request_codec());
            } catch (const std::exception& ex) {
                YT_LOG_DEBUG(ex, "Failed to receive request body (RequestId: %v)",
                    RequestId_);
                Unref();
                return;
            }

            {
                auto guard = Guard(SpinLock_);
                Stage_ = EServerCallStage::SendingInitialMetadata;
            }

            YT_LOG_DEBUG("Request received (RequestId: %v)",
                RequestId_);

            InitialMetadataBuilder_.Add(RequestIdMetadataKey, ToString(RequestId_));

            {
                std::array<grpc_op, 1> ops;

                ops[0].op = GRPC_OP_SEND_INITIAL_METADATA;
                ops[0].flags = 0;
                ops[0].reserved = nullptr;
                ops[0].data.send_initial_metadata.maybe_compression_level.is_set = false;
                ops[0].data.send_initial_metadata.metadata = InitialMetadataBuilder_.Unwrap();
                ops[0].data.send_initial_metadata.count = InitialMetadataBuilder_.GetSize();

                StartBatch(ops, EServerCallCookie::Normal);
            }

            {
                Ref();

                std::array<grpc_op, 1> ops;

                ops[0].op = GRPC_OP_RECV_CLOSE_ON_SERVER;
                ops[0].flags = 0;
                ops[0].reserved = nullptr;
                ops[0].data.recv_close_on_server.cancelled = &RawCanceled_;

                StartBatch(ops, EServerCallCookie::Close);
            }

            YT_VERIFY(!ReplyBus_);
            ReplyBus_ = New<TReplyBus>(this);

            if (Service_) {
                auto requestMessage = CreateRequestMessage(
                    *header,
                    messageWithAttachments.Message,
                    messageWithAttachments.Attachments);

                Service_->HandleRequest(std::move(header), std::move(requestMessage), ReplyBus_);
            } else {
                auto error = TError(
                    NRpc::EErrorCode::NoSuchService,
                    "Service is not registered")
                    << TErrorAttribute("service", ServiceName_);
                YT_LOG_WARNING(error);

                auto responseMessage = CreateErrorResponseMessage(RequestId_, error);
                YT_UNUSED_FUTURE(ReplyBus_->Send(std::move(responseMessage)));
            }
        }

        void OnInitialMetadataSent(bool success)
        {
            if (!success) {
                YT_LOG_DEBUG("Failed to send initial metadata (RequestId: %v)",
                    RequestId_);
                Unref();
                return;
            }

            {
                auto guard = Guard(SpinLock_);
                if (ResponseMessage_) {
                    SendResponse(guard);
                } else {
                    Stage_ = EServerCallStage::WaitingForService;
                    CheckCanceled(guard);
                }
            }
        }

        void OnResponseMessage(TSharedRefArray message)
        {
            auto guard = Guard(SpinLock_);

            YT_VERIFY(!ResponseMessage_);
            ResponseMessage_ = std::move(message);

            if (Stage_ == EServerCallStage::WaitingForService) {
                SendResponse(guard);
            }
        }

        void SendResponse(TGuard<NThreading::TSpinLock>& guard)
        {
            Stage_ = EServerCallStage::SendingResponse;
            guard.Release();

            YT_LOG_DEBUG("Sending response (RequestId: %v)",
                RequestId_);

            {
                auto guard = Guard(TraceContextSpinLock_);
                TraceContextHandler_.UpdateTraceContext();
            }

            NRpc::NProto::TResponseHeader responseHeader;
            YT_VERIFY(TryParseResponseHeader(ResponseMessage_, &responseHeader));

            TCompactVector<grpc_op, 2> ops;

            TError error;
            if (responseHeader.has_error() && responseHeader.error().code() != static_cast<int>(NYT::EErrorCode::OK)) {
                FromProto(&error, responseHeader.error());
                ErrorMessage_ = ToString(error);
                ErrorMessageSlice_.Reset(grpc_slice_from_static_string(ErrorMessage_.c_str()));
                TrailingMetadataBuilder_.Add(ErrorMetadataKey, SerializeError(error));
            } else {
                YT_VERIFY(ResponseMessage_.Size() >= 2);

                TMessageWithAttachments messageWithAttachments;
                messageWithAttachments.Message = ResponseMessage_[1];
                for (int index = 2; index < std::ssize(ResponseMessage_); ++index) {
                    messageWithAttachments.Attachments.push_back(ResponseMessage_[index]);
                }

                ResponseBodyBuffer_ = MessageWithAttachmentsToByteBuffer(messageWithAttachments);

                if (!messageWithAttachments.Attachments.empty()) {
                    TrailingMetadataBuilder_.Add(MessageBodySizeMetadataKey, ToString(messageWithAttachments.Message.Size()));
                }

                ops.emplace_back();
                ops.back().op = GRPC_OP_SEND_MESSAGE;
                ops.back().data.send_message.send_message = ResponseBodyBuffer_.Unwrap();
                ops.back().flags = 0;
                ops.back().reserved = nullptr;
            }

            ops.emplace_back();
            ops.back().op = GRPC_OP_SEND_STATUS_FROM_SERVER;
            ops.back().flags = 0;
            ops.back().reserved = nullptr;
            ops.back().data.send_status_from_server.status = error.IsOK() ? GRPC_STATUS_OK : grpc_status_code(GenericErrorStatusCode);
            ops.back().data.send_status_from_server.status_details = error.IsOK() ? nullptr : ErrorMessageSlice_.Unwrap();
            ops.back().data.send_status_from_server.trailing_metadata_count = TrailingMetadataBuilder_.GetSize();
            ops.back().data.send_status_from_server.trailing_metadata = TrailingMetadataBuilder_.Unwrap();

            StartBatch(ops, EServerCallCookie::Normal);
        }


        void OnResponseSent(bool success)
        {
            {
                auto guard = Guard(SpinLock_);
                Stage_ = EServerCallStage::Done;
            }

            if (success) {
                YT_LOG_DEBUG("Response sent (RequestId: %v)",
                    RequestId_);
            } else {
                YT_LOG_DEBUG("Failed to send response (RequestId: %v)",
                    RequestId_);
            }

            Unref();
        }

        void OnCloseReceived(bool success)
        {
            if (success) {
                if (RawCanceled_) {
                    OnCanceled();
                } else {
                    YT_LOG_DEBUG("Request closed (RequestId: %v)",
                        RequestId_);
                }
            } else {
                YT_LOG_DEBUG("Failed to close request (RequestId: %v)",
                    RequestId_);
            }

            Unref();
        }

        void OnCanceled()
        {
            YT_LOG_DEBUG("Request cancelation received (RequestId: %v)",
                RequestId_);

            if (Service_) {
                Service_->HandleRequestCancellation(RequestId_);
            }

            {
                auto guard = Guard(SpinLock_);
                CancelRequested_ = true;
                CheckCanceled(guard);
            }
        }

        void CheckCanceled(TGuard<NThreading::TSpinLock>& guard)
        {
            if (CancelRequested_ && Stage_ == EServerCallStage::WaitingForService) {
                Stage_ = EServerCallStage::Done;
                guard.Release();
                Unref();
            }
        }
    };
};

DEFINE_REFCOUNTED_TYPE(TServer)

IServerPtr CreateServer(TServerConfigPtr config)
{
    return New<TServer>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NGrpc

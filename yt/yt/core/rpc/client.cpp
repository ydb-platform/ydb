#include "client.h"
#include "private.h"
#include "dispatcher.h"
#include "message.h"
#include "stream.h"

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/misc/checksum.h>
#include <yt/yt/core/misc/memory_usage_tracker.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/bus/tcp/dispatcher.h>

#include <yt/yt/build/ya_version.h>

#include <library/cpp/yt/misc/cast.h>

namespace NYT::NRpc {

using namespace NBus;
using namespace NYTree;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = RpcClientLogger;
static const auto LightInvokerDurationWarningThreshold = TDuration::MilliSeconds(10);

////////////////////////////////////////////////////////////////////////////////

TClientContext::TClientContext(
    TRequestId requestId,
    NTracing::TTraceContextPtr traceContext,
    std::string service,
    std::string method,
    TFeatureIdFormatter featureIdFormatter,
    bool responseIsHeavy,
    TAttachmentsOutputStreamPtr requestAttachmentsStream,
    TAttachmentsInputStreamPtr responseAttachmentsStream,
    IMemoryUsageTrackerPtr memoryUsageTracker)
    : RequestId_(requestId)
    , TraceContext_(std::move(traceContext))
    , Service_(std::move(service))
    , Method_(std::move(method))
    , FeatureIdFormatter_(featureIdFormatter)
    , ResponseHeavy_(responseIsHeavy)
    , RequestAttachmentsStream_(std::move(requestAttachmentsStream))
    , ResponseAttachmentsStream_(std::move(responseAttachmentsStream))
    , MemoryUsageTracker_(std::move(memoryUsageTracker))
{ }

////////////////////////////////////////////////////////////////////////////////

TClientRequest::TClientRequest(
    IChannelPtr channel,
    const TServiceDescriptor& serviceDescriptor,
    const TMethodDescriptor& methodDescriptor)
    : Channel_(std::move(channel))
    , StreamingEnabled_(methodDescriptor.StreamingEnabled)
    , SendBaggage_(serviceDescriptor.AcceptsBaggage)
    , FeatureIdFormatter_(serviceDescriptor.FeatureIdFormatter)
{
    YT_ASSERT(Channel_);

    ToProto(Header_.mutable_service(), serviceDescriptor.FullServiceName);
    ToProto(Header_.mutable_method(), methodDescriptor.MethodName);
    Header_.set_protocol_version_major(serviceDescriptor.ProtocolVersion.Major);
    Header_.set_protocol_version_minor(serviceDescriptor.ProtocolVersion.Minor);

    ToProto(Header_.mutable_request_id(), TRequestId::Create());
}

TClientRequest::TClientRequest(const TClientRequest& other)
    : Attachments_(other.Attachments_)
    , Timeout_(other.Timeout_)
    , AcknowledgementTimeout_(other.AcknowledgementTimeout_)
    , RequestHeavy_(other.RequestHeavy_)
    , ResponseHeavy_(other.ResponseHeavy_)
    , RequestCodec_(other.RequestCodec_)
    , ResponseCodec_(other.ResponseCodec_)
    , GenerateAttachmentChecksums_(other.GenerateAttachmentChecksums_)
    , MemoryUsageTracker_(other.MemoryUsageTracker_)
    , Channel_(other.Channel_)
    , StreamingEnabled_(other.StreamingEnabled_)
    , SendBaggage_(other.SendBaggage_)
    , FeatureIdFormatter_(other.FeatureIdFormatter_)
    , Header_(other.Header_)
    , MultiplexingBand_(other.MultiplexingBand_)
    , ClientAttachmentsStreamingParameters_(other.ClientAttachmentsStreamingParameters_)
    , ServerAttachmentsStreamingParameters_(other.ServerAttachmentsStreamingParameters_)
    , User_(other.User_)
    , UserTag_(other.UserTag_)
{ }

TSharedRefArray TClientRequest::Serialize()
{
    bool retry = Serialized_.exchange(true);

    PrepareHeader();

    auto headerlessMessage = GetHeaderlessMessage();

    if (!retry) {
        auto output = CreateRequestMessage(Header_, headerlessMessage);
        return std::move(output);
    }

    if (StreamingEnabled_) {
        THROW_ERROR_EXCEPTION("Retries are not supported for requests with streaming");
    }

    auto patchedHeader = Header_;
    patchedHeader.set_retry(true);

    auto output = CreateRequestMessage(patchedHeader, headerlessMessage);
    return std::move(output);
}

IClientRequestControlPtr TClientRequest::Send(IClientResponseHandlerPtr responseHandler)
{
    TSendOptions options{
        .Timeout = Timeout_,
        .AcknowledgementTimeout = AcknowledgementTimeout_,
        .GenerateAttachmentChecksums = GenerateAttachmentChecksums_,
        .RequestHeavy = RequestHeavy_,
        .MultiplexingBand = MultiplexingBand_,
        .MultiplexingParallelism = MultiplexingParallelism_,
        .SendDelay = SendDelay_
    };
    auto control = Channel_->Send(
        this,
        std::move(responseHandler),
        options);
    RequestControl_ = control;
    return control;
}

NProto::TRequestHeader& TClientRequest::Header()
{
    return Header_;
}

const NProto::TRequestHeader& TClientRequest::Header() const
{
    return Header_;
}

bool TClientRequest::IsStreamingEnabled() const
{
    return StreamingEnabled_;
}

const TStreamingParameters& TClientRequest::ClientAttachmentsStreamingParameters() const
{
    return ClientAttachmentsStreamingParameters_;
}

TStreamingParameters& TClientRequest::ClientAttachmentsStreamingParameters()
{
    return ClientAttachmentsStreamingParameters_;
}

const TStreamingParameters& TClientRequest::ServerAttachmentsStreamingParameters() const
{
    return ServerAttachmentsStreamingParameters_;
}

TStreamingParameters& TClientRequest::ServerAttachmentsStreamingParameters()
{
    return ServerAttachmentsStreamingParameters_;
}

NConcurrency::IAsyncZeroCopyOutputStreamPtr TClientRequest::GetRequestAttachmentsStream() const
{
    if (!RequestAttachmentsStream_) {
        THROW_ERROR_EXCEPTION(NRpc::EErrorCode::StreamingNotSupported, "Streaming is not supported");
    }
    return RequestAttachmentsStream_;
}

NConcurrency::IAsyncZeroCopyInputStreamPtr TClientRequest::GetResponseAttachmentsStream() const
{
    if (!ResponseAttachmentsStream_) {
        THROW_ERROR_EXCEPTION(NRpc::EErrorCode::StreamingNotSupported, "Streaming is not supported");
    }
    return ResponseAttachmentsStream_;
}

TRequestId TClientRequest::GetRequestId() const
{
    return FromProto<TRequestId>(Header_.request_id());
}

TRealmId TClientRequest::GetRealmId() const
{
    return FromProto<TRealmId>(Header_.realm_id());
}

std::string TClientRequest::GetService() const
{
    return FromProto<std::string>(Header_.service());
}

std::string TClientRequest::GetMethod() const
{
    return FromProto<std::string>(Header_.method());
}

void TClientRequest::DeclareClientFeature(int featureId)
{
    Header_.add_declared_client_feature_ids(featureId);
}

void TClientRequest::RequireServerFeature(int featureId)
{
    Header_.add_required_server_feature_ids(featureId);
}

const TString& TClientRequest::GetUser() const
{
    return User_;
}

void TClientRequest::SetUser(const TString& user)
{
    User_ = user;
}

const TString& TClientRequest::GetUserTag() const
{
    return UserTag_;
}

void TClientRequest::SetUserTag(const TString& tag)
{
    UserTag_ = tag;
}

void TClientRequest::SetUserAgent(const TString& userAgent)
{
    Header_.set_user_agent(userAgent);
}

bool TClientRequest::GetRetry() const
{
    return Header_.retry();
}

void TClientRequest::SetRetry(bool value)
{
    Header_.set_retry(value);
}

TMutationId TClientRequest::GetMutationId() const
{
    return NRpc::GetMutationId(Header_);
}

void TClientRequest::SetMutationId(TMutationId id)
{
    if (id) {
        ToProto(Header_.mutable_mutation_id(), id);
    } else {
        Header_.clear_mutation_id();
    }
}

size_t TClientRequest::GetHash() const
{
    auto hash = Hash_.load(std::memory_order::relaxed);
    if (hash == UnknownHash) {
        hash = ComputeHash();
        auto oldHash = Hash_.exchange(hash);
        YT_VERIFY(oldHash == UnknownHash || oldHash == hash);
    }
    return hash;
}

EMultiplexingBand TClientRequest::GetMultiplexingBand() const
{
    return MultiplexingBand_;
}

void TClientRequest::SetMultiplexingBand(EMultiplexingBand band)
{
    MultiplexingBand_ = band;
    Header_.set_tos_level(TTcpDispatcher::Get()->GetTosLevelForBand(band));
}

int TClientRequest::GetMultiplexingParallelism() const
{
    return MultiplexingParallelism_;
}

void TClientRequest::SetMultiplexingParallelism(int parallelism)
{
    MultiplexingParallelism_ = parallelism;
}

size_t TClientRequest::ComputeHash() const
{
    size_t hash = 0;
    for (const auto& part : GetHeaderlessMessage()) {
        HashCombine(hash, GetChecksum(part));
    }
    return hash;
}

TClientContextPtr TClientRequest::CreateClientContext()
{
    auto traceContext = CreateCallTraceContext(GetService(), GetMethod());
    if (traceContext) {
        auto* tracingExt = Header().MutableExtension(NRpc::NProto::TRequestHeader::tracing_ext);
        ToProto(tracingExt, traceContext);
        if (!SendBaggage_) {
            tracingExt->clear_baggage();
        }
        if (traceContext->IsSampled()) {
            TraceRequest(traceContext);
        }
    }

    // If user-agent was not specified explicitly, generate it from build information.
    if (!Header().has_user_agent()) {
        Header().set_user_agent(GetRpcUserAgent());
    }

    if (StreamingEnabled_) {
        RequestAttachmentsStream_ = New<TAttachmentsOutputStream>(
            RequestCodec_,
            TDispatcher::Get()->GetCompressionPoolInvoker(),
            BIND(&TClientRequest::OnPullRequestAttachmentsStream, MakeWeak(this)),
            ClientAttachmentsStreamingParameters_.WindowSize,
            ClientAttachmentsStreamingParameters_.WriteTimeout);
        ResponseAttachmentsStream_ = New<TAttachmentsInputStream>(
            BIND(&TClientRequest::OnResponseAttachmentsStreamRead, MakeWeak(this)),
            TDispatcher::Get()->GetCompressionPoolInvoker(),
            ClientAttachmentsStreamingParameters_.ReadTimeout);
    }

    return New<TClientContext>(
        GetRequestId(),
        std::move(traceContext),
        GetService(),
        GetMethod(),
        FeatureIdFormatter_,
        ResponseHeavy_,
        RequestAttachmentsStream_,
        ResponseAttachmentsStream_,
        MemoryUsageTracker_ ? MemoryUsageTracker_ : Channel_->GetChannelMemoryTracker());
}

void TClientRequest::OnPullRequestAttachmentsStream()
{
    auto payload = RequestAttachmentsStream_->TryPull();
    if (!payload) {
        return;
    }

    auto control = RequestControl_.Lock();
    if (!control) {
        RequestAttachmentsStream_->Abort(TError("Client request control is finalized")
            << TErrorAttribute("request_id", GetRequestId()));
        return;
    }

    YT_LOG_DEBUG("Request streaming attachments pulled (RequestId: %v, SequenceNumber: %v, Sizes: %v, Closed: %v)",
        GetRequestId(),
        payload->SequenceNumber,
        MakeFormattableView(payload->Attachments, [] (auto* builder, const auto& attachment) {
            builder->AppendFormat("%v", GetStreamingAttachmentSize(attachment));
        }),
        !payload->Attachments.back());

    control->SendStreamingPayload(*payload).Subscribe(
        BIND(&TClientRequest::OnRequestStreamingPayloadAcked, MakeStrong(this), payload->SequenceNumber));
}

void TClientRequest::OnRequestStreamingPayloadAcked(int sequenceNumber, const TError& error)
{
    if (error.IsOK()) {
        YT_LOG_DEBUG("Request streaming payload delivery acknowledged (RequestId: %v, SequenceNumber: %v)",
            GetRequestId(),
            sequenceNumber);
    } else {
        YT_LOG_DEBUG(error, "Response streaming payload delivery failed (RequestId: %v, SequenceNumber: %v)",
            GetRequestId(),
            sequenceNumber);
        RequestAttachmentsStream_->Abort(error);
    }
}

void TClientRequest::OnResponseAttachmentsStreamRead()
{
    auto feedback = ResponseAttachmentsStream_->GetFeedback();

    auto control = RequestControl_.Lock();
    if (!control) {
        RequestAttachmentsStream_->Abort(TError("Client request control is finalized")
            << TErrorAttribute("request_id", GetRequestId()));
        return;
    }

    YT_LOG_DEBUG("Response streaming attachments read (RequestId: %v, ReadPosition: %v)",
        GetRequestId(),
        feedback.ReadPosition);

    control->SendStreamingFeedback(feedback).Subscribe(
        BIND(&TClientRequest::OnResponseStreamingFeedbackAcked, MakeStrong(this), feedback));
}

void TClientRequest::OnResponseStreamingFeedbackAcked(const TStreamingFeedback& feedback, const TError& error)
{
    if (error.IsOK()) {
        YT_LOG_DEBUG("Response streaming feedback delivery acknowledged (RequestId: %v, ReadPosition: %v)",
            GetRequestId(),
            feedback.ReadPosition);
    } else {
        YT_LOG_DEBUG(error, "Response streaming feedback delivery failed (RequestId: %v)",
            GetRequestId());
        ResponseAttachmentsStream_->Abort(error);
    }
}

void TClientRequest::TraceRequest(const NTracing::TTraceContextPtr& traceContext)
{
    traceContext->AddTag(RequestIdAnnotation, GetRequestId());
    traceContext->AddTag(EndpointAnnotation, Channel_->GetEndpointDescription());
    for (const auto& [tagKey, tagValue] : TracingTags_) {
        traceContext->AddTag(tagKey, tagValue);
    }
}

void TClientRequest::PrepareHeader()
{
    if (HeaderPrepared_.load()) {
        return;
    }

    auto guard = Guard(HeaderPreparationLock_);

    if (HeaderPrepared_.load()) {
        return;
    }

    // COMPAT(danilalexeev): legacy RPC codecs
    if (!EnableLegacyRpcCodecs_) {
        Header_.set_request_codec(ToProto<int>(RequestCodec_));
        Header_.set_response_codec(ToProto<int>(ResponseCodec_));
    }

    if (StreamingEnabled_) {
        ToProto(Header_.mutable_server_attachments_streaming_parameters(), ServerAttachmentsStreamingParameters_);
    }

    if (User_ && User_ != RootUserName) {
        Header_.set_user(User_);
    }

    if (UserTag_ && UserTag_ != Header_.user()) {
        Header_.set_user_tag(UserTag_);
    }

    HeaderPrepared_.store(true);
}

bool TClientRequest::IsLegacyRpcCodecsEnabled()
{
    return EnableLegacyRpcCodecs_;
}

TSharedRefArray TClientRequest::GetHeaderlessMessage() const
{
    if (SerializedHeaderlessMessageSet_.load()) {
        return SerializedHeaderlessMessage_;
    }
    auto message = SerializeHeaderless();
    if (!SerializedHeaderlessMessageLatch_.exchange(true)) {
        SerializedHeaderlessMessage_ = message;
        SerializedHeaderlessMessageSet_.store(true);
    }
    return message;
}

bool IsRequestSticky(const IClientRequestPtr& request)
{
    if (!request) {
        return false;
    }
    const auto& balancingExt = request->Header().GetExtension(NProto::TBalancingExt::balancing_ext);
    return balancingExt.enable_stickiness();
}

////////////////////////////////////////////////////////////////////////////////

TClientResponse::TClientResponse(TClientContextPtr clientContext)
    : StartTime_(NProfiling::GetInstant())
    , ClientContext_(std::move(clientContext))
{ }

const TString& TClientResponse::GetAddress() const
{
    return Address_;
}

const NProto::TResponseHeader& TClientResponse::Header() const
{
    return Header_;
}

TSharedRefArray TClientResponse::GetResponseMessage() const
{
    YT_ASSERT(ResponseMessage_);
    return ResponseMessage_;
}

size_t TClientResponse::GetTotalSize() const
{
    YT_ASSERT(ResponseMessage_);
    return ResponseMessage_.ByteSize();
}

void TClientResponse::HandleError(TError error)
{
    auto prevState = State_.exchange(EState::Done);
    if (prevState == EState::Done) {
        // Ignore the error.
        // Most probably this is a late timeout.
        return;
    }

    EnrichClientRequestError(
        &error,
        ClientContext_->GetFeatureIdFormatter());

    GetInvoker()->Invoke(
        BIND(&TClientResponse::DoHandleError, MakeStrong(this), std::move(error)));
}

void TClientResponse::DoHandleError(TError error)
{
    NProfiling::TWallTimer timer;

    Finish(error);

    if (!ClientContext_->GetResponseHeavy() && timer.GetElapsedTime() > LightInvokerDurationWarningThreshold) {
        YT_LOG_DEBUG("Handling light request error took too long (RequestId: %v, Duration: %v)",
            ClientContext_->GetRequestId(),
            timer.GetElapsedTime());
    }
}

void TClientResponse::Finish(const TError& error)
{
    TraceResponse();

    if (const auto& requestAttachmentsStream = ClientContext_->GetRequestAttachmentsStream()) {
        // Abort the request stream unconditionally since there is no chance
        // the server will receive any of newly written data.
        requestAttachmentsStream->AbortUnlessClosed(error, false);
    }

    if (const auto& responseAttachmentsStream = ClientContext_->GetResponseAttachmentsStream()) {
        // Only abort the response stream in case of an error.
        // When request finishes successfully the client may still be reading the output stream.
        if (!error.IsOK()) {
            responseAttachmentsStream->AbortUnlessClosed(error, false);
        }
    }

    SetPromise(error);
}

void TClientResponse::TraceResponse()
{
    if (const auto& traceContext = ClientContext_->GetTraceContext()) {
        traceContext->Finish();
    }
}

const IInvokerPtr& TClientResponse::GetInvoker()
{
    return ClientContext_->GetResponseHeavy()
        ? TDispatcher::Get()->GetHeavyInvoker()
        : TDispatcher::Get()->GetLightInvoker();
}

void TClientResponse::Deserialize(TSharedRefArray responseMessage)
{
    YT_ASSERT(responseMessage);
    YT_ASSERT(!ResponseMessage_);

    ResponseMessage_ = std::move(responseMessage);

    if (ResponseMessage_.Size() < 2) {
        THROW_ERROR_EXCEPTION(NRpc::EErrorCode::ProtocolError, "Too few response message parts: %v < 2",
            ResponseMessage_.Size());
    }

    if (!TryParseResponseHeader(ResponseMessage_, &Header_)) {
        THROW_ERROR_EXCEPTION(NRpc::EErrorCode::ProtocolError, "Error deserializing response header");
    }

    // COMPAT(danilalexeev): legacy RPC codecs
    std::optional<NCompression::ECodec> bodyCodecId;
    NCompression::ECodec attachmentCodecId;
    if (Header_.has_codec()) {
        bodyCodecId = attachmentCodecId = CheckedEnumCast<NCompression::ECodec>(Header_.codec());
    } else {
        bodyCodecId = std::nullopt;
        attachmentCodecId = NCompression::ECodec::None;
    }

    if (!TryDeserializeBody(ResponseMessage_[1], bodyCodecId)) {
        THROW_ERROR_EXCEPTION(NRpc::EErrorCode::ProtocolError, "Error deserializing response body");
    }

    auto compressedAttachments = TRange(ResponseMessage_.Begin() + 2, ResponseMessage_.End());
    auto memoryUsageTracker = ClientContext_->GetMemoryUsageTracker();

    if (attachmentCodecId == NCompression::ECodec::None) {
        Attachments_ = compressedAttachments.ToVector();
    } else {
        Attachments_ = DecompressAttachments(compressedAttachments, attachmentCodecId);
    }

    for (auto& attachment : Attachments_) {
        attachment = TrackMemory(memoryUsageTracker, attachment);
    }
}

void TClientResponse::HandleAcknowledgement()
{
    // NB: Handle without switching to another invoker.
    auto expected = EState::Sent;
    State_.compare_exchange_strong(expected, EState::Ack);
}

void TClientResponse::HandleResponse(TSharedRefArray message, const std::string& address)
{
    auto prevState = State_.exchange(EState::Done);
    YT_ASSERT(prevState == EState::Sent || prevState == EState::Ack);

    GetInvoker()->Invoke(BIND(&TClientResponse::DoHandleResponse,
        MakeStrong(this),
        Passed(std::move(message)),
        address));
}

void TClientResponse::DoHandleResponse(TSharedRefArray message, const std::string& address)
{
    NProfiling::TWallTimer timer;

    Address_ = address;

    try {
        Deserialize(std::move(message));
        Finish({});
    } catch (const std::exception& ex) {
        Finish(ex);
    }

    if (!ClientContext_->GetResponseHeavy() && timer.GetElapsedTime() > LightInvokerDurationWarningThreshold) {
        YT_LOG_DEBUG("Handling light response took too long (RequestId: %v, Duration: %v)",
            ClientContext_->GetRequestId(),
            timer.GetElapsedTime());
    }
}

void TClientResponse::HandleStreamingPayload(const TStreamingPayload& payload)
{
    const auto& stream = ClientContext_->GetResponseAttachmentsStream();
    if (!stream) {
        YT_LOG_DEBUG("Received streaming attachments payload for request with disabled streaming; ignored (RequestId: %v)",
            ClientContext_->GetRequestId());
        return;
    }

    stream->EnqueuePayload(payload);
}

void TClientResponse::HandleStreamingFeedback(const TStreamingFeedback& feedback)
{
    const auto& stream = ClientContext_->GetRequestAttachmentsStream();
    if (!stream) {
        YT_LOG_DEBUG("Received streaming attachments feedback for request with disabled streaming; ignored (RequestId: %v)",
            ClientContext_->GetRequestId());
        return;
    }

    stream->HandleFeedback(feedback);
}

////////////////////////////////////////////////////////////////////////////////

TServiceDescriptor::TServiceDescriptor(std::string serviceName)
    : ServiceName(std::move(serviceName))
    , FullServiceName(ServiceName)
{ }

TServiceDescriptor& TServiceDescriptor::SetProtocolVersion(int majorVersion)
{
    auto version = DefaultProtocolVersion;
    version.Major = majorVersion;
    ProtocolVersion = version;
    return *this;
}

TServiceDescriptor& TServiceDescriptor::SetProtocolVersion(TProtocolVersion version)
{
    ProtocolVersion = version;
    return *this;
}

TServiceDescriptor& TServiceDescriptor::SetNamespace(std::string value)
{
    Namespace = std::move(value);
    FullServiceName = Namespace + "." + ServiceName;
    return *this;
}

TServiceDescriptor& TServiceDescriptor::SetAcceptsBaggage(bool value)
{
    AcceptsBaggage = value;
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

TMethodDescriptor::TMethodDescriptor(const TString& methodName)
    : MethodName(methodName)
{ }

TMethodDescriptor& TMethodDescriptor::SetMultiplexingBand(EMultiplexingBand value)
{
    MultiplexingBand = value;
    return *this;
}

TMethodDescriptor& TMethodDescriptor::SetStreamingEnabled(bool value)
{
    StreamingEnabled = value;
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

TProxyBase::TProxyBase(
    IChannelPtr channel,
    const TServiceDescriptor& descriptor)
    : Channel_(std::move(channel))
    , ServiceDescriptor_(descriptor)
{
    YT_VERIFY(Channel_);
}

////////////////////////////////////////////////////////////////////////////////

TGenericProxy::TGenericProxy(
    IChannelPtr channel,
    const TServiceDescriptor& descriptor)
    : TProxyBase(std::move(channel), descriptor)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

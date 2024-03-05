#include "server_detail.h"

#include "authentication_identity.h"
#include "config.h"
#include "dispatcher.h"
#include "message.h"
#include "private.h"

#include <yt/yt/core/bus/bus.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NRpc {

using namespace NConcurrency;
using namespace NBus;
using namespace NYTree;
using namespace NRpc::NProto;
using namespace NTracing;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TServiceContextBase::TServiceContextBase(
    std::unique_ptr<TRequestHeader> header,
    TSharedRefArray requestMessage,
    NLogging::TLogger logger,
    NLogging::ELogLevel logLevel)
    : RequestHeader_(std::move(header))
    , RequestMessage_(std::move(requestMessage))
    , Logger(std::move(logger))
    , LogLevel_(logLevel)
{
    Initialize();
}

TServiceContextBase::TServiceContextBase(
    TSharedRefArray requestMessage,
    NLogging::TLogger logger,
    NLogging::ELogLevel logLevel)
    : RequestHeader_(new TRequestHeader())
    , RequestMessage_(std::move(requestMessage))
    , Logger(std::move(logger))
    , LogLevel_(logLevel)
{
    YT_VERIFY(ParseRequestHeader(RequestMessage_, RequestHeader_.get()));
    Initialize();
}

void TServiceContextBase::DoFlush()
{ }

void TServiceContextBase::Initialize()
{
    LoggingEnabled_ = Logger.IsLevelEnabled(LogLevel_);

    RequestId_ = FromProto<TRequestId>(RequestHeader_->request_id());
    RealmId_ = FromProto<TRealmId>(RequestHeader_->realm_id());
    AuthenticationIdentity_.User = RequestHeader_->has_user() ? RequestHeader_->user() : RootUserName;
    AuthenticationIdentity_.UserTag = RequestHeader_->has_user_tag() ? RequestHeader_->user_tag() : AuthenticationIdentity_.User;

    YT_ASSERT(RequestMessage_.Size() >= 2);
    RequestBody_ = RequestMessage_[1];
    RequestAttachments_ = std::vector<TSharedRef>(
        RequestMessage_.Begin() + 2,
        RequestMessage_.End());
}

void TServiceContextBase::Reply(const TError& error)
{
    YT_ASSERT(!Replied_);

    Error_ = error;

    ReplyEpilogue();
}

void TServiceContextBase::Reply(const TSharedRefArray& responseMessage)
{
    YT_ASSERT(!Replied_);
    YT_ASSERT(responseMessage.Size() >= 1);

    // NB: One must parse responseMessage and only use its content since,
    // e.g., responseMessage may contain invalid request id.
    TResponseHeader header;
    YT_VERIFY(TryParseResponseHeader(responseMessage, &header));

    if (header.has_error()) {
        Error_ = FromProto<TError>(header.error());
    }
    if (Error_.IsOK()) {
        YT_ASSERT(responseMessage.Size() >= 2);
        ResponseBody_ = responseMessage[1];
        ResponseAttachments_ = std::vector<TSharedRef>(
            responseMessage.Begin() + 2,
            responseMessage.End());

        if (header.has_codec()) {
            YT_VERIFY(TryEnumCast(header.codec(), &ResponseCodec_));
            SetResponseBodySerializedWithCompression();
        }
        if (header.has_format()) {
            RequestHeader_->set_response_format(header.format());
        }
    } else {
        ResponseBody_.Reset();
        ResponseAttachments_.clear();
    }

    ReplyEpilogue();
}

void TServiceContextBase::ReplyEpilogue()
{
    if (!RequestInfoSet_ &&
        Error_.IsOK() &&
        LoggingEnabled_ &&
        TDispatcher::Get()->ShouldAlertOnMissingRequestInfo())
    {
        static const auto& Logger = RpcServerLogger;
        YT_LOG_ALERT("Missing request info (RequestId: %v, Method: %v.%v)",
            RequestId_,
            RequestHeader_->service(),
            RequestHeader_->method());
    }

    auto responseMessage = BuildResponseMessage();

    TPromise<TSharedRefArray> asyncResponseMessage;
    {
        auto responseGuard = Guard(ResponseLock_);
        YT_ASSERT(!ResponseMessage_);
        ResponseMessage_ = responseMessage;
        asyncResponseMessage = AsyncResponseMessage_;
        Replied_.store(true);
    }

    DoReply();

    if (LoggingEnabled_) {
        LogResponse();
    }

    DoFlush();

    if (asyncResponseMessage) {
        asyncResponseMessage.Set(std::move(responseMessage));
    }

    RepliedList_.Fire();
}

void TServiceContextBase::SetComplete()
{ }

TFuture<TSharedRefArray> TServiceContextBase::GetAsyncResponseMessage() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard  = Guard(ResponseLock_);

    if (!AsyncResponseMessage_) {
        AsyncResponseMessage_ = NewPromise<TSharedRefArray>();
        if (ResponseMessage_) {
            guard.Release();
            AsyncResponseMessage_.Set(ResponseMessage_);
        }
    }

    return AsyncResponseMessage_;
}

const TSharedRefArray& TServiceContextBase::GetResponseMessage() const
{
    YT_ASSERT(ResponseMessage_);
    return ResponseMessage_;
}

TSharedRefArray TServiceContextBase::BuildResponseMessage()
{
    NProto::TResponseHeader header;
    ToProto(header.mutable_request_id(), RequestId_);
    ToProto(header.mutable_error(), Error_);

    if (RequestHeader_->has_response_format()) {
        header.set_format(RequestHeader_->response_format());
    }

    // COMPAT(danilalexeev): legacy RPC codecs.
    if (IsResponseBodySerializedWithCompression()) {
        if (RequestHeader_->has_response_codec()) {
            header.set_codec(static_cast<int>(ResponseCodec_));
        } else {
            ResponseBody_ = PushEnvelope(ResponseBody_, ResponseCodec_);
            ResponseAttachments_ = DecompressAttachments(ResponseAttachments_, ResponseCodec_);
        }
    }

    auto message = Error_.IsOK()
        ? CreateResponseMessage(
            header,
            ResponseBody_,
            ResponseAttachments_)
        : CreateErrorResponseMessage(header);

    auto responseMessageError = CheckBusMessageLimits(ResponseMessage_);
    if (!responseMessageError.IsOK()) {
        return CreateErrorResponseMessage(responseMessageError);
    }

    return message;
}

bool TServiceContextBase::IsReplied() const
{
    VERIFY_THREAD_AFFINITY_ANY();
    return Replied_.load();
}

void TServiceContextBase::SubscribeCanceled(const TCanceledCallback& /*callback*/)
{ }

void TServiceContextBase::UnsubscribeCanceled(const TCanceledCallback& /*callback*/)
{ }

void TServiceContextBase::SubscribeReplied(const TClosure& /*callback*/)
{ }

void TServiceContextBase::UnsubscribeReplied(const TClosure& /*callback*/)
{ }

bool TServiceContextBase::IsCanceled() const
{
    return false;
}

void TServiceContextBase::Cancel()
{ }

const TError& TServiceContextBase::GetError() const
{
    YT_ASSERT(Replied_);

    return Error_;
}

TSharedRef TServiceContextBase::GetRequestBody() const
{
    return RequestBody_;
}

std::vector<TSharedRef>& TServiceContextBase::RequestAttachments()
{
    return RequestAttachments_;
}

IAsyncZeroCopyInputStreamPtr TServiceContextBase::GetRequestAttachmentsStream()
{
    return nullptr;
}

TSharedRef TServiceContextBase::GetResponseBody()
{
    return ResponseBody_;
}

void TServiceContextBase::SetResponseBody(const TSharedRef& responseBody)
{
    YT_ASSERT(!Replied_);

    ResponseBody_ = responseBody;
}

std::vector<TSharedRef>& TServiceContextBase::ResponseAttachments()
{
    return ResponseAttachments_;
}

IAsyncZeroCopyOutputStreamPtr TServiceContextBase::GetResponseAttachmentsStream()
{
    return nullptr;
}

const NProto::TRequestHeader& TServiceContextBase::GetRequestHeader() const
{
    return *RequestHeader_;
}

TSharedRefArray TServiceContextBase::GetRequestMessage() const
{
    return RequestMessage_;
}

TRequestId TServiceContextBase::GetRequestId() const
{
    return RequestId_;
}

TBusNetworkStatistics TServiceContextBase::GetBusNetworkStatistics() const
{
    return {};
}

const IAttributeDictionary& TServiceContextBase::GetEndpointAttributes() const
{
    return EmptyAttributes();
}

const TString& TServiceContextBase::GetEndpointDescription() const
{
    static const TString EmptyEndpointDescription;
    return EmptyEndpointDescription;
}

std::optional<TInstant> TServiceContextBase::GetStartTime() const
{
    return RequestHeader_->has_start_time()
        ? std::make_optional(FromProto<TInstant>(RequestHeader_->start_time()))
        : std::nullopt;
}

std::optional<TDuration> TServiceContextBase::GetTimeout() const
{
    return RequestHeader_->has_timeout()
        ? std::make_optional(FromProto<TDuration>(RequestHeader_->timeout()))
        : std::nullopt;
}

TInstant TServiceContextBase::GetArriveInstant() const
{
    return TInstant::Zero();
}

std::optional<TInstant> TServiceContextBase::GetRunInstant() const
{
    return std::nullopt;
}

std::optional<TInstant> TServiceContextBase::GetFinishInstant() const
{
    return std::nullopt;
}

std::optional<TDuration> TServiceContextBase::GetWaitDuration() const
{
    return std::nullopt;
}

std::optional<TDuration> TServiceContextBase::GetExecutionDuration() const
{
    return std::nullopt;
}

TTraceContextPtr TServiceContextBase::GetTraceContext() const
{
    return nullptr;
}

std::optional<TDuration> TServiceContextBase::GetTraceContextTime() const
{
    return std::nullopt;
}

bool TServiceContextBase::IsRetry() const
{
    return RequestHeader_->retry();
}

TMutationId TServiceContextBase::GetMutationId() const
{
    return FromProto<TMutationId>(RequestHeader_->mutation_id());
}

std::string TServiceContextBase::GetService() const
{
    return FromProto<std::string>(RequestHeader_->service());
}

std::string TServiceContextBase::GetMethod() const
{
    return FromProto<std::string>(RequestHeader_->method());
}

TRealmId TServiceContextBase::GetRealmId() const
{
    return RealmId_;
}

const TAuthenticationIdentity& TServiceContextBase::GetAuthenticationIdentity() const
{
    return AuthenticationIdentity_;
}

const TRequestHeader& TServiceContextBase::RequestHeader() const
{
    return *RequestHeader_;
}

TRequestHeader& TServiceContextBase::RequestHeader()
{
    return *RequestHeader_;
}

bool TServiceContextBase::IsLoggingEnabled() const
{
    return LoggingEnabled_;
}

void TServiceContextBase::SetRawRequestInfo(TString info, bool incremental)
{
    YT_ASSERT(!Replied_);

    RequestInfoSet_ = true;

    if (!LoggingEnabled_) {
        return;
    }

    if (!info.empty()) {
        RequestInfos_.push_back(std::move(info));
    }
    if (!incremental) {
        LogRequest();
    }
}

void TServiceContextBase::SuppressMissingRequestInfoCheck()
{
    YT_ASSERT(!Replied_);

    RequestInfoSet_ = true;
}

void TServiceContextBase::SetRawResponseInfo(TString info, bool incremental)
{
    YT_ASSERT(!Replied_);

    if (!LoggingEnabled_) {
        return;
    }

    if (!incremental) {
        ResponseInfos_.clear();
    }
    if (!info.empty()) {
        ResponseInfos_.push_back(std::move(info));
    }
}

const NLogging::TLogger& TServiceContextBase::GetLogger() const
{
    return Logger;
}

NLogging::ELogLevel TServiceContextBase::GetLogLevel() const
{
    return LogLevel_;
}

bool TServiceContextBase::IsPooled() const
{
    return false;
}

NCompression::ECodec TServiceContextBase::GetResponseCodec() const
{
    return ResponseCodec_;
}

void TServiceContextBase::SetResponseCodec(NCompression::ECodec codec)
{
    ResponseCodec_ = codec;
}

bool TServiceContextBase::IsResponseBodySerializedWithCompression() const
{
    return ResponseBodySerializedWithCompression_;
}

void TServiceContextBase::SetResponseBodySerializedWithCompression()
{
    ResponseBodySerializedWithCompression_ = true;
}

////////////////////////////////////////////////////////////////////////////////

TServiceContextWrapper::TServiceContextWrapper(IServiceContextPtr underlyingContext)
    : UnderlyingContext_(std::move(underlyingContext))
{ }

const NProto::TRequestHeader& TServiceContextWrapper::GetRequestHeader() const
{
    return UnderlyingContext_->GetRequestHeader();
}

TBusNetworkStatistics TServiceContextWrapper::GetBusNetworkStatistics() const
{
    return UnderlyingContext_->GetBusNetworkStatistics();
}

const NYTree::IAttributeDictionary& TServiceContextWrapper::GetEndpointAttributes() const
{
    return UnderlyingContext_->GetEndpointAttributes();
}

const TString& TServiceContextWrapper::GetEndpointDescription() const
{
    return UnderlyingContext_->GetEndpointDescription();
}

TSharedRefArray TServiceContextWrapper::GetRequestMessage() const
{
    return UnderlyingContext_->GetRequestMessage();
}

TRequestId TServiceContextWrapper::GetRequestId() const
{
    return UnderlyingContext_->GetRequestId();
}

std::optional<TInstant> TServiceContextWrapper::GetStartTime() const
{
    return UnderlyingContext_->GetStartTime();
}

std::optional<TDuration> TServiceContextWrapper::GetTimeout() const
{
    return UnderlyingContext_->GetTimeout();
}

TInstant TServiceContextWrapper::GetArriveInstant() const
{
    return UnderlyingContext_->GetArriveInstant();
}

std::optional<TInstant> TServiceContextWrapper::GetRunInstant() const
{
    return UnderlyingContext_->GetRunInstant();
}

std::optional<TInstant> TServiceContextWrapper::GetFinishInstant() const
{
    return UnderlyingContext_->GetFinishInstant();
}

std::optional<TDuration> TServiceContextWrapper::GetWaitDuration() const
{
    return UnderlyingContext_->GetWaitDuration();
}

std::optional<TDuration> TServiceContextWrapper::GetExecutionDuration() const
{
    return UnderlyingContext_->GetExecutionDuration();
}

TTraceContextPtr TServiceContextWrapper::GetTraceContext() const
{
    return UnderlyingContext_->GetTraceContext();
}

std::optional<TDuration> TServiceContextWrapper::GetTraceContextTime() const
{
    return UnderlyingContext_->GetTraceContextTime();
}

bool TServiceContextWrapper::IsRetry() const
{
    return UnderlyingContext_->IsRetry();
}

TMutationId TServiceContextWrapper::GetMutationId() const
{
    return UnderlyingContext_->GetMutationId();
}

std::string TServiceContextWrapper::GetService() const
{
    return UnderlyingContext_->GetService();
}

std::string TServiceContextWrapper::GetMethod() const
{
    return UnderlyingContext_->GetMethod();
}

TRealmId TServiceContextWrapper::GetRealmId() const
{
    return UnderlyingContext_->GetRealmId();
}

const TAuthenticationIdentity& TServiceContextWrapper::GetAuthenticationIdentity() const
{
    return UnderlyingContext_->GetAuthenticationIdentity();
}

bool TServiceContextWrapper::IsReplied() const
{
    return UnderlyingContext_->IsReplied();
}

void TServiceContextWrapper::Reply(const TError& error)
{
    UnderlyingContext_->Reply(error);
}

void TServiceContextWrapper::Reply(const TSharedRefArray& responseMessage)
{
    UnderlyingContext_->Reply(responseMessage);
}

void TServiceContextWrapper::SetComplete()
{
    UnderlyingContext_->SetComplete();
}

void TServiceContextWrapper::SubscribeCanceled(const TCanceledCallback& callback)
{
    UnderlyingContext_->SubscribeCanceled(callback);
}

void TServiceContextWrapper::UnsubscribeCanceled(const TCanceledCallback& callback)
{
    UnderlyingContext_->UnsubscribeCanceled(callback);
}

void TServiceContextWrapper::SubscribeReplied(const TClosure& callback)
{
    UnderlyingContext_->SubscribeReplied(callback);
}

void TServiceContextWrapper::UnsubscribeReplied(const TClosure& callback)
{
    UnderlyingContext_->UnsubscribeReplied(callback);
}

bool TServiceContextWrapper::IsCanceled() const
{
    return UnderlyingContext_->IsCanceled();
}

void TServiceContextWrapper::Cancel()
{ }

TFuture<TSharedRefArray> TServiceContextWrapper::GetAsyncResponseMessage() const
{
    return UnderlyingContext_->GetAsyncResponseMessage();
}

const TSharedRefArray& TServiceContextWrapper::GetResponseMessage() const
{
    return UnderlyingContext_->GetResponseMessage();
}

const TError& TServiceContextWrapper::GetError() const
{
    return UnderlyingContext_->GetError();
}

TSharedRef TServiceContextWrapper::GetRequestBody() const
{
    return UnderlyingContext_->GetRequestBody();
}

TSharedRef TServiceContextWrapper::GetResponseBody()
{
    return UnderlyingContext_->GetResponseBody();
}

void TServiceContextWrapper::SetResponseBody(const TSharedRef& responseBody)
{
    UnderlyingContext_->SetResponseBody(responseBody);
}

std::vector<TSharedRef>& TServiceContextWrapper::RequestAttachments()
{
    return UnderlyingContext_->RequestAttachments();
}

IAsyncZeroCopyInputStreamPtr TServiceContextWrapper::GetRequestAttachmentsStream()
{
    return UnderlyingContext_->GetRequestAttachmentsStream();
}

std::vector<TSharedRef>& TServiceContextWrapper::ResponseAttachments()
{
    return UnderlyingContext_->ResponseAttachments();
}

const NProto::TRequestHeader& TServiceContextWrapper::RequestHeader() const
{
    return UnderlyingContext_->RequestHeader();
}

IAsyncZeroCopyOutputStreamPtr TServiceContextWrapper::GetResponseAttachmentsStream()
{
    return UnderlyingContext_->GetResponseAttachmentsStream();
}

NProto::TRequestHeader& TServiceContextWrapper::RequestHeader()
{
    return UnderlyingContext_->RequestHeader();
}

bool TServiceContextWrapper::IsLoggingEnabled() const
{
    return UnderlyingContext_->IsLoggingEnabled();
}

void TServiceContextWrapper::SetRawRequestInfo(TString info, bool incremental)
{
    UnderlyingContext_->SetRawRequestInfo(std::move(info), incremental);
}

void TServiceContextWrapper::SuppressMissingRequestInfoCheck()
{
    UnderlyingContext_->SuppressMissingRequestInfoCheck();
}

void TServiceContextWrapper::SetRawResponseInfo(TString info, bool incremental)
{
    UnderlyingContext_->SetRawResponseInfo(std::move(info), incremental);
}

const NLogging::TLogger& TServiceContextWrapper::GetLogger() const
{
    return UnderlyingContext_->GetLogger();
}

NLogging::ELogLevel TServiceContextWrapper::GetLogLevel() const
{
    return UnderlyingContext_->GetLogLevel();
}

bool TServiceContextWrapper::IsPooled() const
{
    return UnderlyingContext_->IsPooled();
}

NCompression::ECodec TServiceContextWrapper::GetResponseCodec() const
{
    return UnderlyingContext_->GetResponseCodec();
}

void TServiceContextWrapper::SetResponseCodec(NCompression::ECodec codec)
{
    UnderlyingContext_->SetResponseCodec(codec);
}

bool TServiceContextWrapper::IsResponseBodySerializedWithCompression() const
{
    return UnderlyingContext_->IsResponseBodySerializedWithCompression();
}

void TServiceContextWrapper::SetResponseBodySerializedWithCompression()
{
    UnderlyingContext_->SetResponseBodySerializedWithCompression();
}

const IServiceContextPtr& TServiceContextWrapper::GetUnderlyingContext() const
{
    return UnderlyingContext_;
}

////////////////////////////////////////////////////////////////////////////////

void TServerBase::RegisterService(IServicePtr service)
{
    YT_VERIFY(service);

    auto serviceId = service->GetServiceId();

    {
        auto guard = WriterGuard(ServicesLock_);
        auto& serviceMap = RealmIdToServiceMap_[serviceId.RealmId];
        YT_VERIFY(serviceMap.emplace(serviceId.ServiceName, service).second);
        if (AppliedConfig_) {
            auto it = AppliedConfig_->Services.find(serviceId.ServiceName);
            if (it != AppliedConfig_->Services.end()) {
                service->Configure(AppliedConfig_, it->second);
            } else {
                service->Configure(AppliedConfig_, nullptr);
            }
        }
        DoRegisterService(service);
    }

    YT_LOG_INFO("RPC service registered (ServiceName: %v, RealmId: %v)",
        serviceId.ServiceName,
        serviceId.RealmId);
}

bool TServerBase::UnregisterService(IServicePtr service)
{
    YT_VERIFY(service);

    auto serviceId = service->GetServiceId();

    {
        auto guard = WriterGuard(ServicesLock_);

        auto serviceMapIt = RealmIdToServiceMap_.find(serviceId.RealmId);
        if (serviceMapIt == RealmIdToServiceMap_.end()) {
            return false;
        }
        auto& serviceMap = serviceMapIt->second;
        auto serviceIt = serviceMap.find(serviceId.ServiceName);
        if (serviceIt == serviceMap.end() || serviceIt->second != service) {
            return false;
        }
        serviceMap.erase(serviceIt);
        if (serviceMap.empty()) {
            YT_VERIFY(RealmIdToServiceMap_.erase(serviceId.RealmId));
        }

        DoUnregisterService(service);
    }

    YT_LOG_INFO("RPC service unregistered (ServiceName: %v, RealmId: %v)",
        serviceId.ServiceName,
        serviceId.RealmId);
    return true;
}

IServicePtr TServerBase::FindService(const TServiceId& serviceId) const
{
    auto guard = ReaderGuard(ServicesLock_);
    auto serviceMapIt = RealmIdToServiceMap_.find(serviceId.RealmId);
    if (serviceMapIt == RealmIdToServiceMap_.end()) {
        return nullptr;
    }
    auto& serviceMap = serviceMapIt->second;
    auto serviceIt = serviceMap.find(serviceId.ServiceName);
    return serviceIt == serviceMap.end() ? nullptr : serviceIt->second;
}

IServicePtr TServerBase::GetServiceOrThrow(const TServiceId& serviceId) const
{
    auto guard = ReaderGuard(ServicesLock_);

    const auto& realmId = serviceId.RealmId;
    const auto& serviceName = serviceId.ServiceName;
    auto serviceMapIt = RealmIdToServiceMap_.find(realmId);
    if (serviceMapIt == RealmIdToServiceMap_.end()) {
        if (realmId) {
            // TODO(gritukan): Stop wrapping error one day.
            auto innerError = TError(EErrorCode::NoSuchRealm, "Request realm is unknown")
                << TErrorAttribute("service", serviceName)
                << TErrorAttribute("realm_id", realmId);
            THROW_ERROR_EXCEPTION(
                EErrorCode::NoSuchService,
                "Service is not registered")
                << innerError;
        } else {
            THROW_ERROR_EXCEPTION(
                EErrorCode::NoSuchService,
                "Service is not registered")
                << TErrorAttribute("service", serviceName)
                << TErrorAttribute("realm_id", realmId);
        }
    }
    auto& serviceMap = serviceMapIt->second;
    auto serviceIt = serviceMap.find(serviceName);
    if (serviceIt == serviceMap.end()) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::NoSuchService,
            "Service is not registered")
            << TErrorAttribute("service", serviceName)
            << TErrorAttribute("realm_id", realmId);
    }

    return serviceIt->second;
}

void TServerBase::ApplyConfig()
{
    VERIFY_SPINLOCK_AFFINITY(ServicesLock_);

    auto newAppliedConfig = New<TServerConfig>();
    newAppliedConfig->EnableErrorCodeCounting = DynamicConfig_->EnableErrorCodeCounting.value_or(StaticConfig_->EnableErrorCodeCounting);
    newAppliedConfig->EnablePerUserProfiling = DynamicConfig_->EnablePerUserProfiling.value_or(StaticConfig_->EnablePerUserProfiling);
    newAppliedConfig->HistogramTimerProfiling = DynamicConfig_->HistogramTimerProfiling.value_or(StaticConfig_->HistogramTimerProfiling);
    newAppliedConfig->TracingMode = DynamicConfig_->TracingMode.value_or(StaticConfig_->TracingMode);
    newAppliedConfig->Services = StaticConfig_->Services;

    for (const auto& [name, node] : DynamicConfig_->Services) {
        newAppliedConfig->Services[name] = node;
    }

    AppliedConfig_ = newAppliedConfig;

    // Apply configuration to all existing services.
    for (const auto& [realmId, serviceMap] : RealmIdToServiceMap_) {
        for (const auto& [serviceName, service] : serviceMap) {
            auto it = AppliedConfig_->Services.find(serviceName);
            if (it != AppliedConfig_->Services.end()) {
                service->Configure(AppliedConfig_, it->second);
            } else {
                service->Configure(AppliedConfig_, nullptr);
            }
        }
    }
}

void TServerBase::Configure(const TServerConfigPtr& config)
{
    auto guard = WriterGuard(ServicesLock_);

    // Future services will be configured appropriately.
    StaticConfig_ = config;

    ApplyConfig();
}

void TServerBase::OnDynamicConfigChanged(const TServerDynamicConfigPtr& config)
{
    auto guard = WriterGuard(ServicesLock_);

    DynamicConfig_ = config;

    ApplyConfig();
}

void TServerBase::Start()
{
    YT_VERIFY(!Started_);

    DoStart();

    YT_LOG_INFO("RPC server started");
}

TFuture<void> TServerBase::Stop(bool graceful)
{
    if (!Started_) {
        return VoidFuture;
    }

    YT_LOG_INFO("Stopping RPC server (Graceful: %v)",
        graceful);

    return DoStop(graceful).Apply(BIND([this, this_ = MakeStrong(this)] () {
        YT_LOG_INFO("RPC server stopped");
    }));
}

TServerBase::TServerBase(NLogging::TLogger logger)
    : Logger(std::move(logger))
{ }

void TServerBase::DoStart()
{
    Started_ = true;
}

TFuture<void> TServerBase::DoStop(bool graceful)
{
    Started_ = false;

    std::vector<TFuture<void>> asyncResults;

    if (graceful) {
        std::vector<IServicePtr> services;
        {
            auto guard = ReaderGuard(ServicesLock_);
            for (const auto& [realmId, serviceMap] : RealmIdToServiceMap_) {
                for (const auto& [serviceName, service] : serviceMap) {
                    services.push_back(service);
                }
            }
        }

        for (const auto& service : services) {
            asyncResults.push_back(service->Stop());
        }
    }

    return AllSucceeded(asyncResults);
}

void TServerBase::DoRegisterService(const IServicePtr& /*service*/)
{ }

void TServerBase::DoUnregisterService(const IServicePtr& /*service*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

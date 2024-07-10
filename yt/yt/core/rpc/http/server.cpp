#include "server.h"
#include "helpers.h"

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/http/http.h>
#include <yt/yt/core/http/private.h>
#include <yt/yt/core/http/helpers.h>
#include <yt/yt/core/http/server.h>

#include <yt/yt/core/rpc/message.h>
#include <yt/yt/core/rpc/service.h>
#include <yt/yt/core/rpc/server_detail.h>

#include <yt/yt/core/bus/bus.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NRpc::NHttp {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NYTree;
using namespace NNet;
using namespace NYT::NHttp;
using namespace NYT::NBus;
using namespace NYT::NRpc;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(THttpReplyBus)
DECLARE_REFCOUNTED_CLASS(THttpHandler)

////////////////////////////////////////////////////////////////////////////////

class THttpReplyBus
    : public IBus
{
public:
    THttpReplyBus(
        IResponseWriterPtr rsp,
        TString endpointAddress,
        IAttributeDictionaryPtr endpointAttributes,
        TNetworkAddress endpointNetworkAddress,
        const NLogging::TLogger& logger)
        : Rsp_(std::move(rsp))
        , EndpointAddress_(std::move(endpointAddress))
        , EndpointAttributes_(std::move(endpointAttributes))
        , EndpointNetworkAddress_(std::move(endpointNetworkAddress))
        , Logger(logger)
    { }

    const TString& GetEndpointDescription() const override
    {
        return EndpointAddress_;
    }

    const IAttributeDictionary& GetEndpointAttributes() const override
    {
        return *EndpointAttributes_;
    }

    const TString& GetEndpointAddress() const override
    {
        return EndpointAddress_;
    }

    const NNet::TNetworkAddress& GetEndpointNetworkAddress() const override
    {
        return EndpointNetworkAddress_;
    }

    bool IsEndpointLocal() const override
    {
        return false;
    }

    bool IsEncrypted() const override
    {
        return false;
    }

    TBusNetworkStatistics GetNetworkStatistics() const override
    {
        return {};
    }

    TFuture<void> GetReadyFuture() const override
    {
        return VoidFuture;
    }

    TFuture<void> Send(TSharedRefArray message, const NBus::TSendOptions& /*options*/) override
    {
        if (message.Size() > 2) {
            THROW_ERROR_EXCEPTION("Attachments are not supported in HTTP transport");
        }

        YT_VERIFY(message.Size() >= 1);
        NRpc::NProto::TResponseHeader responseHeader;
        YT_VERIFY(TryParseResponseHeader(message, &responseHeader));

        if (responseHeader.has_error() && responseHeader.error().code() != 0) {
            FillYTErrorHeaders(Rsp_, FromProto<TError>(responseHeader.error()));
            Rsp_->SetStatus(EStatusCode::BadRequest);
            auto replySent = Rsp_->Close();
            replySent.Subscribe(BIND([this, this_ = MakeStrong(this)] (const TError& result){
                ReplySent_.Set(result);
            }));
            return replySent;
        }

        YT_VERIFY(message.Size() >= 2);
        if (responseHeader.has_format()) {
            auto format = CheckedEnumCast<EMessageFormat>(responseHeader.format());
            Rsp_->GetHeaders()->Add("Content-Type", ToHttpContentType(format));
        }

        FillYTErrorHeaders(Rsp_, TError{});
        Rsp_->SetStatus(EStatusCode::OK);
        auto bodySent = Rsp_->WriteBody(message[1]);
        bodySent.Subscribe(BIND([this, this_ = MakeStrong(this)] (const TError& result){
            ReplySent_.Set(result);
        }));
        return bodySent;
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

    TFuture<void> ReplySent(IServicePtr underlying, TRequestId requestId)
    {
        ReplySent_.OnCanceled(BIND([underlying, requestId, Logger = this->Logger] (const TError&) {
            YT_LOG_INFO("Request cancelled (RequestId: %v)", requestId);
            underlying->HandleRequestCancellation(requestId);
        }));
        return ReplySent_.ToFuture();
    }

private:
    const IResponseWriterPtr Rsp_;
    const TString EndpointAddress_;
    const IAttributeDictionaryPtr EndpointAttributes_;
    const TNetworkAddress EndpointNetworkAddress_;
    const NLogging::TLogger Logger;

    TPromise<void> ReplySent_ = NewPromise<void>();
    TSingleShotCallbackList<void(const TError&)> TerminatedList_;
};

DEFINE_REFCOUNTED_TYPE(THttpReplyBus)

////////////////////////////////////////////////////////////////////////////////

class THttpHandler
    : public IHttpHandler
{
public:
    THttpHandler(IServicePtr underlying, const TString& baseUrl, const NLogging::TLogger& logger)
        : Underlying_(std::move(underlying))
        , BaseUrl_(baseUrl)
        , Logger(logger)
    { }

    void HandleRequest(
        const IRequestPtr& req,
        const IResponseWriterPtr& rsp) override
    {
        if (MaybeHandleCors(req, rsp)) {
            return;
        }

        TRequestId requestId;
        auto header = std::make_unique<NRpc::NProto::TRequestHeader>();
        auto error = TranslateRequest(req, header.get(), &requestId);
        if (!error.IsOK()) {
            FillYTErrorHeaders(rsp, error);
            rsp->SetStatus(EStatusCode::BadRequest);
            WaitFor(rsp->Close())
                .ThrowOnError();
            return;
        }

        auto body = req->ReadAll();
        auto endpointNetworkAddress = req->GetRemoteAddress();
        auto endpointAddress = ToString(endpointNetworkAddress);
        auto endpointAttributes = ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Item("address").Value(endpointAddress)
            .EndMap());
        auto replyBus = New<THttpReplyBus>(
            rsp,
            std::move(endpointAddress),
            std::move(endpointAttributes),
            std::move(endpointNetworkAddress),
            Logger);

        auto replySent = replyBus->ReplySent(Underlying_, requestId);

        auto requestMessage = CreateRequestMessage(*header, body, {});
        Underlying_->HandleRequest(std::move(header), std::move(requestMessage), std::move(replyBus));

        WaitFor(replySent)
            .ThrowOnError();
    }

private:
    const IServicePtr Underlying_;
    const TString BaseUrl_;
    const NLogging::TLogger Logger;


    TError TranslateRequest(const IRequestPtr& req, NRpc::NProto::TRequestHeader* rpcHeader, TRequestId* requestId)
    {
        using namespace NYT::NHttp::NHeaders;

        if (req->GetMethod() != EMethod::Post) {
            return TError("Invalid method; POST expected");
        }

        const auto& url = req->GetUrl();
        if (url.Path.Size() <= BaseUrl_.Size()) {
            return TError("Invalid URL");
        }

        ToProto(rpcHeader->mutable_service(), Underlying_->GetServiceId().ServiceName);
        ToProto(rpcHeader->mutable_method(), url.Path.substr(BaseUrl_.Size()));

        const auto& httpHeaders = req->GetHeaders();

        auto contentTypeString = httpHeaders->Find(ContentTypeHeaderName);
        if (contentTypeString) {
            auto decodedType = FromHttpContentType(*contentTypeString);
            if (!decodedType) {
                return TError("Invalid \"Content-Type\" header value")
                    << TErrorAttribute("value", *contentTypeString);
            }
            rpcHeader->set_request_format(static_cast<i32>(*decodedType));
        }

        auto requestFormatOptionsYson = httpHeaders->Find(RequestFormatOptionsHeaderName);
        if (requestFormatOptionsYson) {
            rpcHeader->set_request_format_options(*requestFormatOptionsYson);
        }

        auto acceptString = httpHeaders->Find(AcceptHeaderName);
        if (acceptString) {
            auto decodedType = FromHttpContentType(*acceptString);
            if (!decodedType) {
                return TError("Invalid \"Accept\" header value")
                    << TErrorAttribute("value", *acceptString);
            }
            rpcHeader->set_response_format(static_cast<i32>(*decodedType));
        }

        auto responseFormatOptionsYson = httpHeaders->Find(ResponseFormatOptionsHeaderName);
        if (responseFormatOptionsYson) {
            rpcHeader->set_response_format_options(*responseFormatOptionsYson);
        }

        auto requestIdString = httpHeaders->Find(RequestIdHeaderName);
        if (requestIdString) {
            if (!TRequestId::FromString(*requestIdString, requestId)) {
                return TError("Invalid %Qv header value", RequestIdHeaderName)
                    << TErrorAttribute("value", *requestIdString);
            }
        } else {
            *requestId = TRequestId::Create();
        }
        ToProto(rpcHeader->mutable_request_id(), *requestId);

        auto getCredentialsExt = [&] {
            return rpcHeader->MutableExtension(NRpc::NProto::TCredentialsExt::credentials_ext);
        };

        auto authorizationString = httpHeaders->Find(AuthorizationHeaderName);
        if (authorizationString) {
            const TStringBuf Prefix = "OAuth ";
            if (!authorizationString->StartsWith(Prefix)) {
                return TError("Invalid \"Authorization\" header value");
            }
            getCredentialsExt()->set_token(TrimLeadingWhitespaces(authorizationString->substr(Prefix.length())));
        }

        auto userTicketString = httpHeaders->Find(UserTicketHeaderName);
        if (userTicketString) {
            getCredentialsExt()->set_user_ticket(TrimLeadingWhitespaces(*userTicketString));
        }

        auto cookieString = httpHeaders->Find(CookieHeaderName);
        if (cookieString) {
            auto cookieMap = ParseCookies(*cookieString);

            static const TString SessionIdCookieName("Session_id");
            auto sessionIdIt = cookieMap.find(SessionIdCookieName);
            if (sessionIdIt != cookieMap.end()) {
                getCredentialsExt()->set_session_id(sessionIdIt->second);
            }

            static const TString SessionId2CookieName("sessionid2");
            auto sslSessionIdIt = cookieMap.find(SessionId2CookieName);
            if (sslSessionIdIt != cookieMap.end()) {
                getCredentialsExt()->set_ssl_session_id(sslSessionIdIt->second);
            }
        }

        auto userAgent = httpHeaders->Find(UserAgentHeaderName);
        if (userAgent) {
            rpcHeader->set_user_agent(*userAgent);
        }

        auto user = httpHeaders->Find(UserNameHeaderName);
        if (user) {
            rpcHeader->set_user(*user);
        }

        auto userTag = httpHeaders->Find(UserTagHeaderName);
        if (userTag) {
            rpcHeader->set_user_tag(*userTag);
        }

        if (auto requestTimeout = httpHeaders->Find(RequestTimeoutHeaderName)) {
            rpcHeader->set_timeout(ToProto<i64>(TDuration::Seconds(FromString<i64>(*requestTimeout))));
        } else if (auto xRequestTimeout = httpHeaders->Find(XRequestTimeoutHeaderName)) {
            rpcHeader->set_timeout(ToProto<i64>(TDuration::MilliSeconds(FromString<i64>(*xRequestTimeout))));
        }

        auto protocolMajor = httpHeaders->Find(ProtocolVersionMajor);
        if (protocolMajor) {
            rpcHeader->set_protocol_version_major(FromString<i64>(*protocolMajor));
        }

        auto protocolMinor = httpHeaders->Find(ProtocolVersionMinor);
        if (protocolMinor) {
            rpcHeader->set_protocol_version_minor(FromString<i64>(*protocolMinor));
        }

        NRpc::NProto::TCustomMetadataExt customMetadataExt;
        bool hasCustomHeaders = false;
        for (const auto& [header, value] : DumpUnknownHeaders(httpHeaders)) {
            if (!header.StartsWith("X-") && !header.StartsWith("x-")) {
                continue;
            }
            TString key = header.substr(2);
            (*customMetadataExt.mutable_entries())[key] = value;
            hasCustomHeaders = true;
        }
        if (hasCustomHeaders) {
            *rpcHeader->MutableExtension(NRpc::NProto::TCustomMetadataExt::custom_metadata_ext) = std::move(customMetadataExt);
        }

        rpcHeader->set_request_codec(ToProto<int>(NCompression::ECodec::None));
        rpcHeader->set_response_codec(ToProto<int>(NCompression::ECodec::None));

        return {};
    }
};

DEFINE_REFCOUNTED_TYPE(THttpHandler)

////////////////////////////////////////////////////////////////////////////////

class TServer
    : public NRpc::TServerBase
{
public:
    explicit TServer(NYT::NHttp::IServerPtr httpServer)
        : TServerBase(HttpLogger().WithTag("ServerId: %v", TGuid::Create()))
        , HttpServer_(std::move(httpServer))
    { }

private:
    NYT::NHttp::IServerPtr HttpServer_;

    void DoStart() override
    {
        HttpServer_->Start();
        TServerBase::DoStart();
    }

    TFuture<void> DoStop(bool graceful) override
    {
        HttpServer_->Stop();
        HttpServer_.Reset();
        return TServerBase::DoStop(graceful);
    }

    void DoRegisterService(const IServicePtr& service) override
    {
        auto baseUrl = Format("/%v/", service->GetServiceId().ServiceName);
        HttpServer_->AddHandler(baseUrl, New<THttpHandler>(std::move(service), baseUrl, Logger));

        // COMPAT(babenko): remove this once fully migrated to new url scheme
        auto index = service->GetServiceId().ServiceName.find_last_of('.');
        if (index != TString::npos) {
            auto anotherBaseUrl = Format("/%v/", service->GetServiceId().ServiceName.substr(index + 1));
            HttpServer_->AddHandler(anotherBaseUrl, New<THttpHandler>(std::move(service), anotherBaseUrl, Logger));
        }
    }

    void DoUnregisterService(const IServicePtr& /*service*/) override
    {
        YT_ABORT();
    }
};

NRpc::IServerPtr CreateServer(NYT::NHttp::IServerPtr httpServer)
{
    return New<TServer>(std::move(httpServer));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NHttp

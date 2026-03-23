#include "oidc_protected_page.h"

#include "openid_connect.h"
#include <ydb/mvp/core/mvp_log.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/http/http.h>

namespace NMVP::NOIDC {

THandlerSessionServiceCheck::THandlerSessionServiceCheck(const NActors::TActorId& sender,
                                                         const NHttp::THttpIncomingRequestPtr& request,
                                                         const NActors::TActorId& httpProxyId,
                                                         const TOpenIdConnectSettings& settings)
    : Sender(sender)
    , Request(request)
    , HttpProxyId(httpProxyId)
    , Settings(settings)
    , ProtectedPage(Request)
{}

void THandlerSessionServiceCheck::Bootstrap(const NActors::TActorContext& ctx) {
    Send(Sender, new NHttp::TEvHttpProxy::TEvSubscribeForCancel(), NActors::IEventHandle::FlagTrackDelivery);
    if (!ProtectedPage.IsHttpSchemeAllowed() || !ProtectedPage.IsRequestedHostAllowed(Settings.AllowedProxyHosts)) {
        BLOG_D("THandlerSessionServiceCheck::Bootstrap rid=" << GetRequestIdForLogs(Request)
            << " method=" << Request->Method
            << " url=" << Request->GetURI()
            << " forbidden_host_or_scheme=1");
        return ReplyAndPassAway(CreateResponseForbiddenHost(Request, ProtectedPage));
    }
    NHttp::THeaders headers(Request->Headers);
    TStringBuf authHeader = headers.Get(AUTHORIZATION_HEADER);
    const bool isAuthorized = IsAuthorizedRequest(authHeader);
    BLOG_D("THandlerSessionServiceCheck::Bootstrap rid=" << GetRequestIdForLogs(Request)
        << " method=" << Request->Method
        << " url=" << Request->GetURI()
        << " is_authorized=" << (isAuthorized ? 1 : 0));
    if (Request->Method == "OPTIONS" || isAuthorized) {
        ForwardUserRequest(authHeader);
    } else {
        StartOidcProcess(ctx);
    }
}

void THandlerSessionServiceCheck::HandleProxy(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event) {
    if (event->Get()->Response != nullptr) {
        const bool needSecureRetry = NeedSendSecureHttpRequest(event->Get()->Response);
        BLOG_D("THandlerSessionServiceCheck::HandleProxy rid=" << GetRequestIdForLogs(Request)
            << " method=" << Request->Method
            << " url=" << Request->GetURI()
            << " has_response=1"
            << " status=" << event->Get()->Response->Status
            << " message=" << event->Get()->Response->Message
            << " error=" << event->Get()->GetError()
            << " need_secure_retry=" << (needSecureRetry ? 1 : 0)
            << " is_done=" << (event->Get()->Response->IsDone() ? 1 : 0));
        if (needSecureRetry) {
            NHttp::THttpIncomingResponsePtr response = std::move(event->Get()->Response);
            return SendSecureHttpRequest(response);
        }
    } else {
        BLOG_D("THandlerSessionServiceCheck::HandleProxy rid=" << GetRequestIdForLogs(Request)
            << " method=" << Request->Method
            << " url=" << Request->GetURI()
            << " has_response=0"
            << " error=" << event->Get()->GetError());
    }
    ExtensionManager->StartExtensionProcess(std::move(Request), std::move(event));
    PassAway();
}

void THandlerSessionServiceCheck::HandleIncompleteProxy(NHttp::TEvHttpProxy::TEvHttpIncompleteIncomingResponse::TPtr event) {
    if (event->Get()->Response != nullptr) {
        NHttp::THttpIncomingResponsePtr response = std::move(event->Get()->Response);
        BLOG_D("THandlerSessionServiceCheck::HandleIncompleteProxy rid=" << GetRequestIdForLogs(Request)
            << " method=" << Request->Method
            << " url=" << Request->GetURI()
            << " status=" << response->Status
            << " message=" << response->Message
            << " is_done=" << (response->IsDone() ? 1 : 0));

        StreamResponse = Request->CreateResponseString(response->AsString());
        StreamConnection = event->Sender;

        Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(StreamResponse));
    } else {
        BLOG_D("THandlerSessionServiceCheck::HandleIncompleteProxy rid=" << GetRequestIdForLogs(Request)
            << " method=" << Request->Method
            << " url=" << Request->GetURI()
            << " has_response=0 error=failed_to_process_streaming_response");
        ReplyAndPassAway(CreateResponseForNotExistingResponseFromProtectedResource(Request, "Failed to process streaming response"));
    }
}

void THandlerSessionServiceCheck::HandleDataChunk(NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk::TPtr event) {
    if (event->Get()->Error) {
        BLOG_D("THandlerSessionServiceCheck::HandleDataChunk rid=" << GetRequestIdForLogs(Request)
            << " method=" << Request->Method
            << " url=" << Request->GetURI()
            << " error=" << event->Get()->Error
            << " end_of_data=" << (event->Get()->IsEndOfData() ? 1 : 0));
        BLOG_D("rid=" << GetRequestIdForLogs(Request)
            << " Incoming data chunk for protected resource error: " << event->Get()->Error);
        Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(event->Get()->Error));
        return PassAway();
    }

    BLOG_D("THandlerSessionServiceCheck::HandleDataChunk rid=" << GetRequestIdForLogs(Request)
        << " method=" << Request->Method
        << " url=" << Request->GetURI()
        << " chunk_size=" << event->Get()->Data.size()
        << " end_of_data=" << (event->Get()->IsEndOfData() ? 1 : 0));
    BLOG_D("rid=" << GetRequestIdForLogs(Request)
        << " Incoming data chunk for protected resource: " << event->Get()->Data.size() << " bytes");
    NHttp::THttpOutgoingDataChunkPtr dataChunk;
    if (event->Get()->IsEndOfData()) {
        dataChunk = StreamResponse->CreateIncompleteDataChunk();
        dataChunk->SetEndOfData();
    } else {
        dataChunk = StreamResponse->CreateDataChunk(event->Get()->Data);
    }

    Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(dataChunk));

    if (dataChunk->IsEndOfData()) {
        PassAway();
    }
}

void THandlerSessionServiceCheck::HandleCancelled() {
    BLOG_D("THandlerSessionServiceCheck::HandleCancelled rid=" << GetRequestIdForLogs(Request)
        << " method=" << Request->Method
        << " url=" << Request->GetURI()
        << " has_stream_connection=" << (StreamConnection ? 1 : 0));
    BLOG_D("rid=" << GetRequestIdForLogs(Request) << " Connection closed");
    if (StreamConnection) {
        Send(StreamConnection, new NActors::TEvents::TEvPoisonPill());
    }
    PassAway();
}

void THandlerSessionServiceCheck::HandleUndelivered(NActors::TEvents::TEvUndelivered::TPtr event) {
    if (event->Get()->SourceType == NHttp::TEvHttpProxy::EvSubscribeForCancel) {
        HandleCancelled();
    }
}

bool THandlerSessionServiceCheck::IsAuthorizedRequest(TStringBuf authHeader) {
    if (authHeader.empty()) {
        return false;
    }
    return to_lower(ToString(authHeader)).StartsWith(IAM_TOKEN_SCHEME_LOWER);
}

TDuration THandlerSessionServiceCheck::GetRequestTimeout() const {
    auto path = TStringBuf(ProtectedPage.Url).Before('?');
    for (const auto& [suffix, timeout] : Settings.RequestTimeoutsByPath) {
        if (path.EndsWith(suffix)) {
            return timeout;
        }
    }
    return Settings.DefaultRequestTimeout;
}

void THandlerSessionServiceCheck::ForwardUserRequest(TStringBuf authHeader, bool secure) {
    auto timeout = GetRequestTimeout();
    BLOG_D("THandlerSessionServiceCheck::ForwardUserRequest rid=" << GetRequestIdForLogs(Request)
        << " method=" << Request->Method
        << " url=" << Request->GetURI()
        << " secure=" << (secure ? 1 : 0)
        << " timeout_ms=" << timeout.MilliSeconds());
    BLOG_D("rid=" << GetRequestIdForLogs(Request) << " Forward user request bypass OIDC");

    ExtensionManager = MakeHolder<TExtensionManager>(Sender, Settings, ProtectedPage, TString(authHeader));
    ExtensionManager->SetExtensionTimeout(timeout);
    ExtensionManager->ArrangeExtensions(Request);

    TProxiedRequestParams params(Request, authHeader, secure, ProtectedPage, Settings);
    auto httpRequest = CreateProxiedRequest(params);

    auto requestEvent = std::make_unique<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(httpRequest);
    requestEvent->Timeout = timeout;
    requestEvent->AllowConnectionReuse = !Request->IsConnectionClose();
    requestEvent->StreamContentTypes = {
        "multipart/x-mixed-replace",
        "multipart/form-data",
        "text/event-stream",
    };

    Send(HttpProxyId, requestEvent.release());
}

void THandlerSessionServiceCheck::SendSecureHttpRequest(const NHttp::THttpIncomingResponsePtr& response) {
    NHttp::THttpOutgoingRequestPtr request = response->GetRequest();
    BLOG_D("THandlerSessionServiceCheck::SendSecureHttpRequest rid=" << GetRequestIdForLogs(Request)
        << " method=" << Request->Method
        << " url=" << Request->GetURI()
        << " from_status=" << response->Status
        << " from_message=" << response->Message);
    BLOG_D("rid=" << GetRequestIdForLogs(Request) << " Try to send request to HTTPS port");
    NHttp::THeadersBuilder headers {request->Headers};
    ForwardUserRequest(headers.Get(AUTHORIZATION_HEADER), true);
}

void THandlerSessionServiceCheck::ReplyAndPassAway(NHttp::THttpOutgoingResponsePtr httpResponse) {
    if (httpResponse) {
        BLOG_D("THandlerSessionServiceCheck::ReplyAndPassAway rid=" << GetRequestIdForLogs(Request)
            << " method=" << Request->Method
            << " url=" << Request->GetURI()
            << " status=" << httpResponse->Status
            << " message=" << httpResponse->Message
            << " is_done=" << (httpResponse->IsDone() ? 1 : 0)
            << " is_need_body=" << (httpResponse->IsNeedBody() ? 1 : 0));
    } else {
        BLOG_D("THandlerSessionServiceCheck::ReplyAndPassAway rid=" << GetRequestIdForLogs(Request)
            << " method=" << Request->Method
            << " url=" << Request->GetURI()
            << " response=null");
    }
    Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(std::move(httpResponse)));
    PassAway();
}

} // NMVP::NOIDC

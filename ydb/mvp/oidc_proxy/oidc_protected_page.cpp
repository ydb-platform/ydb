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
    if (!ProtectedPage.CheckRequestedHost(Settings)) {
        return ReplyAndPassAway(CreateResponseForbiddenHost(Request, ProtectedPage));
    }
    NHttp::THeaders headers(Request->Headers);
    TStringBuf authHeader = headers.Get(AUTHORIZATION_HEADER);
    if (Request->Method == "OPTIONS" || IsAuthorizedRequest(authHeader)) {
        ForwardUserRequest(authHeader);
    } else {
        StartOidcProcess(ctx);
    }
}

void THandlerSessionServiceCheck::HandleProxy(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event) {
    if (event->Get()->Response != nullptr) {
        BLOG_D("Incoming response for protected resource: " << event->Get()->Response->Status);
        if (NeedSendSecureHttpRequest(event->Get()->Response)) {
            NHttp::THttpIncomingResponsePtr response = std::move(event->Get()->Response);
            return SendSecureHttpRequest(response);
        }
    }
    ExtensionManager->StartExtensionProcess(std::move(Request), std::move(event));
    PassAway();
}

void THandlerSessionServiceCheck::HandleEnrichmentTimeout() {
    ExtensionManager->StartExtensionProcess(std::move(Request));
    PassAway();
}

void THandlerSessionServiceCheck::HandleIncompleteProxy(NHttp::TEvHttpProxy::TEvHttpIncompleteIncomingResponse::TPtr event) {
    if (event->Get()->Response != nullptr) {
        NHttp::THttpIncomingResponsePtr response = std::move(event->Get()->Response);
        BLOG_D("Incoming incomplete response for protected resource: " << response->Status);

        StreamResponse = Request->CreateResponseString(response->AsString());
        StreamConnection = event->Sender;

        Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(StreamResponse));
    } else {
        static constexpr size_t MAX_LOGGED_SIZE = 1024;
        BLOG_D("Can not process incomplete request to protected resource:\n" << event->Get()->Request->GetObfuscatedData().substr(0, MAX_LOGGED_SIZE));
        ReplyAndPassAway(CreateResponseForNotExistingResponseFromProtectedResource(Request, "Failed to process streaming response"));
    }
}

void THandlerSessionServiceCheck::HandleDataChunk(NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk::TPtr event) {
    if (event->Get()->Error) {
        BLOG_D("Incoming data chunk for protected resource error: " << event->Get()->Error);
        Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(event->Get()->Error));
        return PassAway();
    }

    BLOG_D("Incoming data chunk for protected resource: " << event->Get()->Data.size() << " bytes");
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
    BLOG_D("Connection closed");
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

void THandlerSessionServiceCheck::ForwardUserRequest(TStringBuf authHeader, bool secure) {
    BLOG_D("Forward user request bypass OIDC");

    TProxiedRequestParams params(Request, authHeader, secure, ProtectedPage, Settings);
    auto httpRequest = CreateProxiedRequest(params);

    auto requestEvent = std::make_unique<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(httpRequest);
    requestEvent->Timeout = TDuration::Seconds(120);
    requestEvent->AllowConnectionReuse = !Request->IsConnectionClose();
    requestEvent->StreamContentTypes = {
        "multipart/x-mixed-replace",
        "multipart/form-data",
        "text/event-stream",
    };

    Send(HttpProxyId, requestEvent.release());

    ExtensionManager = MakeHolder<TExtensionManager>(Sender, Settings, ProtectedPage, TString(authHeader));
    ExtensionManager->ArrangeExtensions(Request);
    if (ExtensionManager->HasEnrichmentExtension()) {
        Schedule(TDuration::MilliSeconds(Settings.EnrichmentProcessTimeoutMs), new NActors::TEvents::TEvWakeup());
    }
}

void THandlerSessionServiceCheck::SendSecureHttpRequest(const NHttp::THttpIncomingResponsePtr& response) {
    NHttp::THttpOutgoingRequestPtr request = response->GetRequest();
    BLOG_D("Try to send request to HTTPS port");
    NHttp::THeadersBuilder headers {request->Headers};
    ForwardUserRequest(headers.Get(AUTHORIZATION_HEADER), true);
}

void THandlerSessionServiceCheck::ReplyAndPassAway(NHttp::THttpOutgoingResponsePtr httpResponse) {
    Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(std::move(httpResponse)));
    PassAway();
}

} // NMVP::NOIDC

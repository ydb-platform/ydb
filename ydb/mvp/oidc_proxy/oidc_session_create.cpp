#include "oidc_session_create.h"
#include "oidc_settings.h"
#include "openid_connect.h"

#include <ydb/mvp/core/mvp_log.h>

#include <ydb/library/actors/http/http.h>
#include <ydb/library/security/util.h>

#include <library/cpp/json/json_reader.h>

namespace NMVP::NOIDC {

THandlerSessionCreate::THandlerSessionCreate(const NActors::TActorId& sender,
                                             const NHttp::THttpIncomingRequestPtr& request,
                                             const NActors::TActorId& httpProxyId,
                                             const TOpenIdConnectSettings& settings)
    : TMvpLogContextProvider(CreateMvpLogContext(request))
    , Sender(sender)
    , Request(request)
    , HttpProxyId(httpProxyId)
    , Settings(settings)
{}

void THandlerSessionCreate::Bootstrap() {
    BLOG_D("Restore oidc session");
    NHttp::TUrlParameters urlParameters(Request->URL);
    Code = urlParameters["code"];

    const TString state = urlParameters["state"];
    const TDecodeStateResult decodedState = DecodeState(state);
    const TCheckStateResult checkStateResult = decodedState.Check(Settings.ClientSecret);
    NHttp::THeaders headers(Request->Headers);
    NHttp::TCookies cookies(headers.Get("cookie"));
    CookieContextResult = RestoreOidcContext(cookies, Settings.ClientSecret, checkStateResult.CookieSuffix);
    const TString flowId = decodedState.Payload.FlowId;
    const TString forwardUrl = decodedState.Payload.ForwardUrl;

    if (!checkStateResult.Ok) {
        BLOG_D(checkStateResult.ErrorMessage);
        if (CookieContextResult.IsSuccess() || CookieContextResult.Status.IsErrorRetryable) {
            Context = CookieContextResult.Context;
            RetryRequestToProtectedResourceAndDie();
        } else {
            SendUnknownErrorResponseAndDie();
        }
        return;
    }

    if (forwardUrl.empty() || forwardUrl == Settings.LocalEndpoint) {
        BLOG_D("Restore oidc context from local store"
            << " (flow_id: " << NKikimr::MaskTicket(flowId) << ")");
        ContinueAfterContextRestore(RestoreOidcContextFromStore(Settings.AuthFlowContextStore, flowId));
        return;
    }

    BLOG_D("Request oidc auth callback context from owner"
        << " (flow_id: " << NKikimr::MaskTicket(flowId)
        << ", forward_url: " << forwardUrl
        << ", timeout: " << TOpenIdConnectSettings::DEFAULT_AUTH_CALLBACK_CONTEXT_TIMEOUT << ")");
    NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet(
        CreateAuthCallbackContextRequestUrl(forwardUrl, state));
    httpRequest->Set(REQUEST_ID_HEADER, GetRequestId());
    auto requestEvent = std::make_unique<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(httpRequest);
    requestEvent->Timeout = TOpenIdConnectSettings::DEFAULT_AUTH_CALLBACK_CONTEXT_TIMEOUT;
    Send(HttpProxyId, requestEvent.release());
    Become(&THandlerSessionCreate::StateWaitAuthCallbackContextResponse);
}

void THandlerSessionCreate::ReplyBadRequestAndPassAway(TString errorMessage) {
    NHttp::THeadersBuilder responseHeaders;
    SetCORS(Request, &responseHeaders);
    SetRequestIdHeader(responseHeaders, GetRequestId());
    responseHeaders.Set("Content-Type", "text/plain");
    return ReplyAndPassAway(Request->CreateResponse("400", "Bad Request", responseHeaders, errorMessage));
}

void THandlerSessionCreate::Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event) {
    if (event->Get()->Error.empty() && event->Get()->Response) {
        NHttp::THttpIncomingResponsePtr response = std::move(event->Get()->Response);
        BLOG_D("Incoming response from authorization server: " << response->Status);
        if (response->Status == "200") {
            NJson::TJsonValue jsonValue;
            NJson::TJsonReaderConfig jsonConfig;
            if (NJson::ReadJsonTree(response->Body, &jsonConfig, &jsonValue)) {
                return ProcessSessionToken(jsonValue);
            }
            return ReplyBadRequestAndPassAway("Wrong OIDC response");
        } else {
            NHttp::THeadersBuilder responseHeaders;
            responseHeaders.Parse(response->Headers);
            SetRequestIdHeader(responseHeaders, GetRequestId());
            return ReplyAndPassAway(Request->CreateResponse(response->Status, response->Message, responseHeaders, response->Body));
        }
    }

    return ReplyBadRequestAndPassAway(event->Get()->Error);
}

void THandlerSessionCreate::HandleAuthCallbackContextResponse(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event) {
    if (!event->Get()->Error.empty() || !event->Get()->Response) {
        BLOG_D("Request oidc auth callback context failed: " << event->Get()->Error);
        ContinueAfterContextRestore(TRestoreOidcContextResult({
            .IsSuccess = false,
            .IsErrorRetryable = false,
            .ErrorMessage = TStringBuilder() << "Restore oidc context from owner failed: " << event->Get()->Error,
        }));
        return;
    }

    NHttp::THttpIncomingResponsePtr response = std::move(event->Get()->Response);
    BLOG_D("Incoming response from oidc auth callback context: " << response->Status);
    if (response->Status != "200") {
        ContinueAfterContextRestore(TRestoreOidcContextResult({
            .IsSuccess = false,
            .IsErrorRetryable = false,
            .ErrorMessage = TStringBuilder() << "Restore oidc context from owner failed with status " << response->Status,
        }));
        return;
    }

    TRestoreOidcContextResult restoreContextResult = RestoreOidcContextFromResponseBody(response->Body);
    if (restoreContextResult.IsSuccess()) {
        BLOG_D("Incoming oidc auth callback context successfully restored");
    }
    ContinueAfterContextRestore(restoreContextResult);
}

TString THandlerSessionCreate::ChangeSameSiteFieldInSessionCookie(const TString& cookie) {
    const static TStringBuf SameSiteParameter {"SameSite=Lax"};
    size_t n = cookie.find(SameSiteParameter);
    if (n == TString::npos) {
        return cookie;
    }
    TStringBuilder cookieBuilder;
    cookieBuilder << cookie.substr(0, n);
    cookieBuilder << "SameSite=None";
    cookieBuilder << cookie.substr(n + SameSiteParameter.size());
    return cookieBuilder;
}

void THandlerSessionCreate::RetryRequestToProtectedResourceAndDie() {
    NHttp::THeadersBuilder responseHeaders;
    RetryRequestToProtectedResourceAndDie(&responseHeaders);
}

void THandlerSessionCreate::RetryRequestToProtectedResourceAndDie(NHttp::THeadersBuilder* responseHeaders) {
    SetCORS(Request, responseHeaders);
    SetRequestIdHeader(*responseHeaders, GetRequestId());
    responseHeaders->Set("Location", Context.GetRequestedAddress());
    ReplyAndPassAway(Request->CreateResponse("302", "Found", *responseHeaders));
}

void THandlerSessionCreate::ContinueAfterResolvedContext(const TContext& context) {
    Context = context;
    if (Code.empty()) {
        BLOG_D("Restore oidc session failed: receive empty 'code' parameter");
        RetryRequestToProtectedResourceAndDie();
    } else {
        RequestSessionToken(Code);
    }
}

void THandlerSessionCreate::ContinueAfterContextRestore(const TRestoreOidcContextResult& restoreContextResult) {
    if (restoreContextResult.IsSuccess()) {
        ContinueAfterResolvedContext(restoreContextResult.Context);
        return;
    }

    BLOG_D(restoreContextResult.Status.ErrorMessage);
    if (CookieContextResult.IsSuccess()) {
        BLOG_D("Falling back to oidc context from cookie");
        ContinueAfterResolvedContext(CookieContextResult.Context);
        return;
    }

    if (restoreContextResult.Status.IsErrorRetryable) {
        RetryRequestToProtectedResourceAndDie();
    } else {
        SendUnknownErrorResponseAndDie();
    }
}

void THandlerSessionCreate::SendUnknownErrorResponseAndDie() {
    NHttp::THeadersBuilder responseHeaders;
    responseHeaders.Set("Content-Type", "text/html");
    SetCORS(Request, &responseHeaders);
    SetRequestIdHeader(responseHeaders, GetRequestId());
    const static TStringBuf BAD_REQUEST_HTML_PAGE = "<html>"
                                                        "<head>"
                                                            "<title>"
                                                                "400 Bad Request"
                                                            "</title>"
                                                        "</head>"
                                                        "<body bgcolor=\"white\">"
                                                            "<center>"
                                                                "<h1>"
                                                                    "Unknown error has occurred. Please open the page again"
                                                                "</h1>"
                                                            "</center>"
                                                        "</body>"
                                                    "</html>";
    ReplyAndPassAway(Request->CreateResponse("400", "Bad Request", responseHeaders, BAD_REQUEST_HTML_PAGE));
}

void THandlerSessionCreate::ReplyAndPassAway(NHttp::THttpOutgoingResponsePtr httpResponse) {
    Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(std::move(httpResponse)));
    PassAway();
}

} // NMVP::NOIDC

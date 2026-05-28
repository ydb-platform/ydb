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

    const TString signedState = urlParameters["state"];
    const TDecodeStateResult decodedState = DecodeState(signedState);
    const TCheckStateResult checkStateResult = decodedState.Check(Settings.ClientSecret);
    RestoreCookieContextFromRequest(checkStateResult.CookieSuffix);
    const TString flowId = decodedState.Payload.FlowId;
    const TString ownerEndpoint = decodedState.Payload.OwnerEndpoint;

    if (!checkStateResult.Ok) {
        HandleInvalidState(checkStateResult.ErrorMessage);
        return;
    }

    if (ownerEndpoint.empty() || ownerEndpoint == Settings.LocalEndpoint) {
        RestoreContextFromLocalStore(flowId);
        return;
    }

    RequestAuthCallbackContextFromOwner(flowId, ownerEndpoint, signedState);
}

void THandlerSessionCreate::RestoreCookieContextFromRequest(TStringBuf cookieSuffix) {
    NHttp::THeaders headers(Request->Headers);
    NHttp::TCookies cookies(headers.Get("cookie"));
    CookieContextResult = RestoreOidcContext(cookies, Settings.ClientSecret, cookieSuffix);
}

bool THandlerSessionCreate::CanRetryRequestWithCookieContext() const {
    return CookieContextResult.IsSuccess() || CookieContextResult.Status.IsErrorRetryable;
}

void THandlerSessionCreate::HandleInvalidState(TStringBuf errorMessage) {
    BLOG_D(errorMessage);
    if (!CanRetryRequestWithCookieContext()) {
        SendUnknownErrorResponseAndDie();
        return;
    }

    Context = CookieContextResult.Context;
    RetryRequestToProtectedResourceAndDie();
}

TRestoreOidcContextResult THandlerSessionCreate::CreateOwnerContextRestoreErrorResult(TString errorMessage) {
    return TRestoreOidcContextResult({
        .IsSuccess = false,
        .IsErrorRetryable = false,
        .ErrorMessage = TStringBuilder() << "Restore oidc context from owner failed: " << errorMessage,
    });
}

void THandlerSessionCreate::RestoreContextFromLocalStore(TStringBuf flowId) {
    BLOG_D("Restore oidc context from local store" << " (flow_id: " << NKikimr::MaskTicket(flowId) << ")");
    const TRestoreOidcContextResult restoreContextResult = RestoreOidcContextFromStore(Settings.AuthCallbackContextStore, flowId);
    ProcessContextRestoreResult(restoreContextResult);
}

void THandlerSessionCreate::RequestAuthCallbackContextFromOwner(TStringBuf flowId, TStringBuf ownerEndpoint, TStringBuf signedState) {
    const TDuration timeout = TOpenIdConnectSettings::DEFAULT_AUTH_CALLBACK_CONTEXT_TIMEOUT;
    const TString requestUrl = CreateAuthCallbackContextRequestUrl(ownerEndpoint, signedState);

    BLOG_D("Request oidc auth callback context from owner"
        << " (flow_id: " << NKikimr::MaskTicket(flowId)
        << ", owner_endpoint: " << ownerEndpoint
        << ", timeout: " << timeout << ")");
    NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet(requestUrl);
    httpRequest->Set(REQUEST_ID_HEADER, GetRequestId());
    auto requestEvent = std::make_unique<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(httpRequest);
    requestEvent->Timeout = timeout;
    Send(HttpProxyId, requestEvent.release());
    Become(&THandlerSessionCreate::StateWaitAuthCallbackContextResponse);
}

void THandlerSessionCreate::HandleAuthCallbackContextResponse(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event) {
    if (!event->Get()->Error.empty() || !event->Get()->Response) {
        BLOG_D("Request oidc auth callback context failed: " << event->Get()->Error);
        ProcessContextRestoreResult(CreateOwnerContextRestoreErrorResult(event->Get()->Error));
        return;
    }

    NHttp::THttpIncomingResponsePtr response = std::move(event->Get()->Response);
    BLOG_D("Incoming response from oidc auth callback context: " << response->Status);
    if (response->Status != "200") {
        ProcessContextRestoreResult(CreateOwnerContextRestoreErrorResult(
            TStringBuilder() << "status " << response->Status));
        return;
    }

    HandleOwnerContextResponseBody(response->Body);
}

void THandlerSessionCreate::HandleOwnerContextResponseBody(TStringBuf responseBody) {
    TRestoreOidcContextResult restoreContextResult = RestoreOidcContextFromResponseBody(responseBody);
    if (restoreContextResult.IsSuccess()) {
        BLOG_D("Incoming oidc auth callback context successfully restored");
    }
    ProcessContextRestoreResult(restoreContextResult);
}

void THandlerSessionCreate::ReplyBadRequestAndPassAway(TString errorMessage) {
    NHttp::THeadersBuilder responseHeaders;
    SetCORS(Request, &responseHeaders);
    SetRequestIdHeader(responseHeaders, GetRequestId());
    responseHeaders.Set("Content-Type", "text/plain");
    return ReplyAndPassAway(Request->CreateResponse("400", "Bad Request", responseHeaders, errorMessage));
}

void THandlerSessionCreate::Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event) {
    if (!event->Get()->Error.empty() || !event->Get()->Response) {
        ReplyBadRequestAndPassAway(event->Get()->Error);
        return;
    }

    NHttp::THttpIncomingResponsePtr response = std::move(event->Get()->Response);
    BLOG_D("Incoming response from authorization server: " << response->Status);
    if (response->Status != "200") {
        NHttp::THeadersBuilder responseHeaders;
        responseHeaders.Parse(response->Headers);
        SetRequestIdHeader(responseHeaders, GetRequestId());
        ReplyAndPassAway(Request->CreateResponse(response->Status, response->Message, responseHeaders, response->Body));
        return;
    }

    NJson::TJsonValue jsonValue;
    NJson::TJsonReaderConfig jsonConfig;
    if (!NJson::ReadJsonTree(response->Body, &jsonConfig, &jsonValue)) {
        ReplyBadRequestAndPassAway("Wrong OIDC response");
        return;
    }

    ProcessSessionToken(jsonValue);
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

void THandlerSessionCreate::RestoreSessionWithContext(const TContext& context) {
    Context = context;
    if (Code.empty()) {
        BLOG_D("Restore oidc session failed: receive empty 'code' parameter");
        RetryRequestToProtectedResourceAndDie();
    } else {
        RequestSessionToken(Code);
    }
}

void THandlerSessionCreate::HandleContextRestoreFailure(const TRestoreOidcContextResult& restoreContextResult) {
    BLOG_D(restoreContextResult.Status.ErrorMessage);
    if (CookieContextResult.IsSuccess()) {
        BLOG_D("Falling back to oidc context from cookie");
        RestoreSessionWithContext(CookieContextResult.Context);
        return;
    }

    if (restoreContextResult.Status.IsErrorRetryable) {
        RetryRequestToProtectedResourceAndDie();
    } else {
        SendUnknownErrorResponseAndDie();
    }
}

void THandlerSessionCreate::ProcessContextRestoreResult(const TRestoreOidcContextResult& restoreContextResult) {
    if (restoreContextResult.IsSuccess()) {
        RestoreSessionWithContext(restoreContextResult.Context);
        return;
    }

    HandleContextRestoreFailure(restoreContextResult);
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

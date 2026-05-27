#include "oidc_session_create.h"
#include "oidc_cookie.h"
#include "oidc_settings.h"
#include "openid_connect.h"

#include <ydb/mvp/core/mvp_log.h>

#include <ydb/library/actors/http/http.h>

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
    const TString code = urlParameters["code"];
    const TString signedState = urlParameters["state"];
    const TDecodeStateResult decodedState = DecodeState(signedState);
    const TCheckStateResult checkStateResult = decodedState.Check(Settings.ClientSecret);

    NHttp::THeaders headers(Request->Headers);
    NHttp::TCookies cookies(headers.Get("cookie"));
    const TString cookieName = CreateNameYdbOidcCookie();
    const TStringBuf authFlowCookieValue = GetCookie(cookies, cookieName);

    TString requestedAddress = decodedState.Payload.RequestedAddress;
    if (requestedAddress.empty()) {
        requestedAddress = RestoreRequestedAddressFromAuthFlowCookie(authFlowCookieValue, Settings.ClientSecret);
    }

    if (requestedAddress.empty()) {
        BLOG_D("Restore oidc session failed: requested address is missing in state and auth flow cookie");
        SendUnknownErrorResponseAndDie();
        return;
    }
    Context = TContext({.RequestedAddress = requestedAddress});

    const bool isSameBrowser = HasAuthFlowCookieNonce(
        authFlowCookieValue,
        Settings.ClientSecret,
        decodedState.Payload.AntiForgeryToken
    );

    if (!checkStateResult.Ok) {
        BLOG_D(checkStateResult.ErrorMessage);
        if (checkStateResult.PayloadTrusted) {
            RetryRequestToProtectedResourceAndDie();
        } else {
            SendUnknownErrorResponseAndDie();
        }
        return;
    }

    if (!isSameBrowser) {
        BLOG_D("Restore oidc session failed: auth flow cookie does not match state");
        RetryRequestToProtectedResourceAndDie();
        return;
    }

    MatchedAuthFlowNonce = decodedState.Payload.AntiForgeryToken;

    if (code.empty()) {
        BLOG_D("Restore oidc session failed: receive empty 'code' parameter");
        RetryRequestToProtectedResourceAndDie();
        return;
    }

    RequestSessionToken(code);
}

void THandlerSessionCreate::ReplyBadRequestAndPassAway(TString errorMessage) {
    NHttp::THeadersBuilder responseHeaders;
    SetCORS(Request, &responseHeaders);
    SetRequestIdHeader(responseHeaders, GetRequestId());
    responseHeaders.Set("Content-Type", "text/plain");
    AddAuthFlowCookieCleanupHeader(&responseHeaders);
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
            AddAuthFlowCookieCleanupHeader(&responseHeaders);
            return ReplyAndPassAway(Request->CreateResponse(response->Status, response->Message, responseHeaders, response->Body));
        }
    }

    return ReplyBadRequestAndPassAway(event->Get()->Error);
}

void THandlerSessionCreate::AddAuthFlowCookieCleanupHeader(NHttp::THeadersBuilder* responseHeaders) {
    if (MatchedAuthFlowNonce.empty()) {
        return;
    }

    NHttp::THeaders headers(Request->Headers);
    NHttp::TCookies cookies(headers.Get("cookie"));
    const TString updatedCookieValue = RemoveAuthFlowCookieNonce(
        GetCookie(cookies, CreateNameYdbOidcCookie()),
        Settings.ClientSecret,
        MatchedAuthFlowNonce
    );

    if (updatedCookieValue.empty()) {
        responseHeaders->Add("Set-Cookie", ClearAuthFlowCookie());
        return;
    }

    responseHeaders->Add("Set-Cookie", CreateAuthFlowCookie(updatedCookieValue));
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
    AddAuthFlowCookieCleanupHeader(responseHeaders);
    ReplyAndPassAway(Request->CreateResponse("302", "Found", *responseHeaders));
}

void THandlerSessionCreate::SendUnknownErrorResponseAndDie() {
    NHttp::THeadersBuilder responseHeaders;
    responseHeaders.Set("Content-Type", "text/html");
    SetCORS(Request, &responseHeaders);
    SetRequestIdHeader(responseHeaders, GetRequestId());
    AddAuthFlowCookieCleanupHeader(&responseHeaders);
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

#include <library/cpp/json/json_reader.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/mvp/core/mvp_log.h>
#include "openid_connect.h"
#include "oidc_session_create.h"
#include "oidc_settings.h"

namespace NMVP::NOIDC {

THandlerSessionCreate::THandlerSessionCreate(const NActors::TActorId& sender,
                                             const NHttp::THttpIncomingRequestPtr& request,
                                             const NActors::TActorId& httpProxyId,
                                             const TOpenIdConnectSettings& settings)
    : Sender(sender)
    , Request(request)
    , HttpProxyId(httpProxyId)
    , Settings(settings)
{}

void THandlerSessionCreate::Bootstrap() {
    BLOG_D("Restore oidc session");
    NHttp::TUrlParameters urlParameters(Request->URL);
    TString code = urlParameters["code"];
    TString state = urlParameters["state"];

    TCheckStateResult checkStateResult = CheckState(state, Settings.ClientSecret);

    NHttp::THeaders headers(Request->Headers);
    NHttp::TCookies cookies(headers.Get("cookie"));
    TRestoreOidcContextResult restoreContextResult = RestoreOidcContext(cookies, Settings.ClientSecret);
    Context = restoreContextResult.Context;

    if (checkStateResult.IsSuccess()) {
        if (restoreContextResult.IsSuccess()) {
            if (code.empty()) {
                BLOG_D("Restore oidc session failed: receive empty 'code' parameter");
                RetryRequestToProtectedResourceAndDie();
            } else {
                RequestSessionToken(code);
            }
        } else {
            const auto& restoreSessionStatus = restoreContextResult.Status;
            BLOG_D(restoreSessionStatus.ErrorMessage);
            if (restoreSessionStatus.IsErrorRetryable) {
                RetryRequestToProtectedResourceAndDie();
            } else {
                SendUnknownErrorResponseAndDie();
            }
        }
    } else {
        BLOG_D(checkStateResult.ErrorMessage);
        if (restoreContextResult.IsSuccess() || restoreContextResult.Status.IsErrorRetryable) {
            RetryRequestToProtectedResourceAndDie();
        } else {
            SendUnknownErrorResponseAndDie();
        }
    }

}

void THandlerSessionCreate::ReplyBadRequestAndPassAway(TString errorMessage) {
    NHttp::THeadersBuilder responseHeaders;
    SetCORS(Request, &responseHeaders);
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
            return ReplyAndPassAway(Request->CreateResponse(response->Status, response->Message, responseHeaders, response->Body));
        }
    }

    return ReplyBadRequestAndPassAway(event->Get()->Error);
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
    responseHeaders->Set("Location", Context.GetRequestedAddress());
    ReplyAndPassAway(Request->CreateResponse("302", "Found", *responseHeaders));
}

void THandlerSessionCreate::SendUnknownErrorResponseAndDie() {
    NHttp::THeadersBuilder responseHeaders;
    responseHeaders.Set("Content-Type", "text/html");
    SetCORS(Request, &responseHeaders);
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

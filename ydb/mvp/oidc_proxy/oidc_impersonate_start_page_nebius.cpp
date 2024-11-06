#include <library/cpp/json/json_reader.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/mvp/core/mvp_log.h>
#include "openid_connect.h"
#include "oidc_session_create.h"
#include "oidc_settings.h"

namespace NMVP {
namespace NOIDC {

THandlerImpersonateStart::THandlerImpersonateStart(const NActors::TActorId& sender,
                                             const NHttp::THttpIncomingRequestPtr& request,
                                             const NActors::TActorId& httpProxyId,
                                             const TOpenIdConnectSettings& settings)
    : Sender(sender)
    , Request(request)
    , HttpProxyId(httpProxyId)
    , Settings(settings)
{}

void THandlerImpersonateStart::Bootstrap(const NActors::TActorContext& ctx) {
    NHttp::TUrlParameters urlParameters(Request->URL);
    TString serviceAccountId = urlParameters["service_accound_id"];

    NHttp::THeaders headers(Request->Headers);
    LOG_DEBUG_S(ctx, EService::MVP, "Start impersonation process");
    NHttp::TCookies cookies(headers.Get("Cookie"));
    TString sessionToken = DecodeToken(cookies, CreateNameSessionCookie(Settings.ClientId));

    if (sessionToken && serviceAccountId) {
        RequestImpersonatedToken(sessionToken, serviceAccountId, ctx);
    } else {
        NHttp::THeadersBuilder responseHeaders;
        responseHeaders.Set("Content-Type", "text/plain");
        httpResponse = Request->CreateResponse("400", "Bad Request", responseHeaders, event->Get()->Error);
    }
}

void THandlerImpersonateStart::RequestImpersonatedToken(const TString sessionToken, const TString serviceAccountId, const NActors::TActorContext& ctx) {
    LOG_DEBUG_S(ctx, EService::MVP, "Request impersonated token");
    NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestPost(Settings.GetExchangeEndpointURL());
    httpRequest->Set<&NHttp::THttpRequest::ContentType>("application/x-www-form-urlencoded");

    TMvpTokenator* tokenator = MVPAppData()->Tokenator;
    TString token = "";
    if (tokenator) {
        token = tokenator->GetToken(Settings.SessionServiceTokenName);
    }
    httpRequest->Set("Authorization", token); // Bearer included
    TStringBuilder body;
    body << "grant_type=urn:ietf:params:oauth:grant-type:token-exchange"
            << "&requested_token_type=urn:ietf:params:oauth:token-type:access_token"
            << "&subject_token_type=urn:ietf:params:oauth:token-type:session_token"
            << "&subject_token=" << sessionToken;
    httpRequest->Set<&NHttp::THttpRequest::Body>(body);

    ctx.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));

    Become(&THandlerSessionServiceCheckNebius::StateExchange);
}

void THandlerImpersonateStart::Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event, const NActors::TActorContext& ctx) {
    NHttp::THttpOutgoingResponsePtr httpResponse;
    if (event->Get()->Error.empty() && event->Get()->Response) {
        NHttp::THttpIncomingResponsePtr response = event->Get()->Response;
        LOG_DEBUG_S(ctx, EService::MVP, "Incoming response from authorization server: " << response->Status);
        if (response->Status == "200") {
            TStringBuf jsonError;
            NJson::TJsonValue jsonValue;
            NJson::TJsonReaderConfig jsonConfig;
            if (NJson::ReadJsonTree(response->Body, &jsonConfig, &jsonValue)) {
                const NJson::TJsonValue* jsonAccessToken;
                if (jsonValue.GetValuePointer("access_token", &jsonAccessToken)) {
                    TString sessionToken = jsonAccessToken->GetStringRobust();
                    ProcessSessionToken(sessionToken, ctx);
                    return;
                } else {
                    jsonError = "Wrong OIDC provider response: access_token not found";
                }
            } else {
                jsonError =  "Wrong OIDC response";
            }
            NHttp::THeadersBuilder responseHeaders;
            responseHeaders.Set("Content-Type", "text/plain");
            httpResponse = Request->CreateResponse("400", "Bad Request", responseHeaders, jsonError);
        } else {
            NHttp::THeadersBuilder responseHeaders;
            responseHeaders.Parse(response->Headers);
            httpResponse = Request->CreateResponse(response->Status, response->Message, responseHeaders, response->Body);
        }
    } else {
        NHttp::THeadersBuilder responseHeaders;
        responseHeaders.Set("Content-Type", "text/plain");
        httpResponse = Request->CreateResponse("400", "Bad Request", responseHeaders, event->Get()->Error);
    }
    ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse));
    Die(ctx);
}

TString THandlerImpersonateStart::ChangeSameSiteFieldInSessionCookie(const TString& cookie) {
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

void THandlerImpersonateStart::RetryRequestToProtectedResourceAndDie(const NActors::TActorContext& ctx) {
    NHttp::THeadersBuilder responseHeaders;
    RetryRequestToProtectedResourceAndDie(&responseHeaders, ctx);
}

void THandlerImpersonateStart::RetryRequestToProtectedResourceAndDie(NHttp::THeadersBuilder* responseHeaders, const NActors::TActorContext& ctx) {
    SetCORS(Request, responseHeaders);
    responseHeaders->Set("Location", Context.GetRequestedAddress());
    ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request->CreateResponse("302", "Found", *responseHeaders)));
    Die(ctx);
}

void THandlerImpersonateStart::SendUnknownErrorResponseAndDie(const NActors::TActorContext& ctx) {
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
    ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request->CreateResponse("400", "Bad Request", responseHeaders, BAD_REQUEST_HTML_PAGE)));
    Die(ctx);
}

} // NOIDC
} // NMVP

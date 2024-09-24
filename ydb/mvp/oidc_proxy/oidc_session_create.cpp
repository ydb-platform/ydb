#include <library/cpp/json/json_reader.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/mvp/core/mvp_log.h>
#include "openid_connect.h"
#include "oidc_session_create.h"
#include "oidc_settings.h"

namespace NMVP {
namespace NOIDC {

THandlerSessionCreate::THandlerSessionCreate(const NActors::TActorId& sender,
                                             const NHttp::THttpIncomingRequestPtr& request,
                                             const NActors::TActorId& httpProxyId,
                                             const TOpenIdConnectSettings& settings)
    : Sender(sender)
    , Request(request)
    , HttpProxyId(httpProxyId)
    , Settings(settings)
{}

void THandlerSessionCreate::Bootstrap(const NActors::TActorContext& ctx) {
    TryRestoreOidcSessionFromCookie(ctx);
}

void THandlerSessionCreate::Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event, const NActors::TActorContext& ctx) {
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

void THandlerSessionCreate::RetryRequestToProtectedResource(const NActors::TActorContext& ctx, const TString& responseMessage) const {
    NHttp::THeadersBuilder responseHeaders;
    RetryRequestToProtectedResource(&responseHeaders, ctx, responseMessage);
}

void THandlerSessionCreate::RetryRequestToProtectedResource(NHttp::THeadersBuilder* responseHeaders, const NActors::TActorContext& ctx, const TString& responseMessage) const {
    SetCORS(Request, responseHeaders);
    responseHeaders->Set("Location", Context.GetRequestedAddress());
    ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request->CreateResponse("302", responseMessage, *responseHeaders)));
}

void THandlerSessionCreate::TryRestoreOidcSessionFromCookie(const NActors::TActorContext& ctx) {
    NHttp::TUrlParameters urlParameters(Request->URL);
    TString code = urlParameters["code"];
    TString state = urlParameters["state"];

    NHttp::THeaders headers(Request->Headers);
    NHttp::TCookies cookies(headers.Get("cookie"));
    TRestoreOidcContextResult restoreSessionResult = RestoreSessionStoredOnClientSide(state, cookies, Settings.ClientSecret);
    Context = restoreSessionResult.Context;
    if (restoreSessionResult.IsSuccess()) {
        if (code.Empty()) {
            LOG_DEBUG_S(ctx, NMVP::EService::MVP, "Restore oidc session failed: receive empty 'code' parameter");
            RetryRequestToProtectedResource(ctx, "Empty code");
            Die(ctx);
        } else {
            RequestSessionToken(code, ctx);
        }
    } else {
        const auto& restoreSessionStatus = restoreSessionResult.Status;
        LOG_DEBUG_S(ctx, NMVP::EService::MVP, restoreSessionStatus.ErrorMessage);
        if (restoreSessionStatus.IsErrorRetryable) {
            RetryRequestToProtectedResource(ctx, "Cannot restore oidc context");
        } else {
            NHttp::THeadersBuilder responseHeaders;
            responseHeaders.Set("Content-Type", "text/html");
            SetCORS(Request, &responseHeaders);
            const static TStringBuf BAD_REQUEST_HTML_PAGE = "<html><head><title>400 Bad Request</title></head><body bgcolor=\"white\"><center><h1>Unknown error has occurred. Please open the page again</h1></center></body></html>";
            ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request->CreateResponse("400", "Bad Request", responseHeaders, BAD_REQUEST_HTML_PAGE)));
        }
        Die(ctx);
    }
}

} // NOIDC
} // NMVP

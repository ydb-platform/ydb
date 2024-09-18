#include <library/cpp/json/json_reader.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/mvp/core/mvp_log.h>
#include "openid_connect.h"
#include "oidc_session_create.h"
#include "oidc_settings.h"
#include "context.h"
#include "context_storage.h"

namespace NMVP {
namespace NOIDC {

THandlerSessionCreate::THandlerSessionCreate(const NActors::TActorId& sender,
                                             const NHttp::THttpIncomingRequestPtr& request,
                                             const NActors::TActorId& httpProxyId,
                                             const TOpenIdConnectSettings& settings,
                                             TContextStorage* const contextStorage)
    : Sender(sender)
    , Request(request)
    , HttpProxyId(httpProxyId)
    , Settings(settings)
    , ContextStorage(contextStorage)
{}

void THandlerSessionCreate::Bootstrap(const NActors::TActorContext& ctx) {
    if (Settings.StoreContextOnHost) {
        TryRestoreContextFromHostStorage(ctx);
    } else {
        TryRestoreContextFromCookie(ctx);
    }
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

void THandlerSessionCreate::RetryRequestToProtectedResourceAndDie(const NActors::TActorContext& ctx, const TString& responseMessage) {
    NHttp::THeadersBuilder responseHeaders;
    RetryRequestToProtectedResourceAndDie(&responseHeaders, ctx, responseMessage);
}

void THandlerSessionCreate::RetryRequestToProtectedResourceAndDie(NHttp::THeadersBuilder* responseHeaders, const NActors::TActorContext& ctx, const TString& responseMessage) {
    SetCORS(Request, responseHeaders);
    responseHeaders->Set("Location", RestoredContext.GetRequestedAddress());
    ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request->CreateResponse("302", responseMessage, *responseHeaders)));
    Die(ctx);
}

void THandlerSessionCreate::TryRestoreContextFromCookie(const NActors::TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NMVP::EService::MVP, "Try restore context from cookie");
    NHttp::TUrlParameters urlParameters(Request->URL);
    TString code = urlParameters["code"];
    TString state = urlParameters["state"];

    NHttp::THeaders headers(Request->Headers);
    NHttp::TCookies cookies(headers.Get("cookie"));
    TRestoreOidcContextResult restoreSessionResult = RestoreContextFromCookie(state, cookies, Settings.ClientSecret);
    RestoredContext = restoreSessionResult.Context;
    if (restoreSessionResult.IsSuccess()) {
        if (code.empty()) {
            LOG_DEBUG_S(ctx, NMVP::EService::MVP, "Restore context from cookie failed: receive empty 'code' parameter");
            RetryRequestToProtectedResourceAndDie(ctx, "Empty code");
        } else {
            RequestSessionToken(code, ctx);
        }
    } else {
        const auto& restoreSessionStatus = restoreSessionResult.Status;
        LOG_DEBUG_S(ctx, NMVP::EService::MVP, restoreSessionStatus.ErrorMessage);
        if (restoreSessionStatus.IsErrorRetryable) {
            RetryRequestToProtectedResourceAndDie(ctx, "Cannot restore oidc context from cookie");
        } else {
            SendUnknownErrorResponseAndDie(ctx);
        }
    }
}

void THandlerSessionCreate::TryRestoreContextFromHostStorage(const NActors::TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NMVP::EService::MVP, "Try restore context from host storage");
    NHttp::TUrlParameters urlParameters(Request->URL);
    TString code = urlParameters["code"];
    TString state = urlParameters["state"];
    static const TString ERROR_RESTORE_CONTEXT_FROM_HOST = "Restore context from host failed: ";

    std::pair<bool, TContextRecord> restoreContextResult = ContextStorage->Find(state);
    if (restoreContextResult.first) {
        RestoredContext = restoreContextResult.second.GetContext();
        if (code.empty()) {
            LOG_DEBUG_S(ctx, NMVP::EService::MVP, ERROR_RESTORE_CONTEXT_FROM_HOST << "Receive empty 'code' parameter");
            RetryRequestToProtectedResourceAndDie(ctx, "Empty code");
        } else if (TInstant::Now() > restoreContextResult.second.GetExpirationTime()) {
            LOG_DEBUG_S(ctx, NMVP::EService::MVP, ERROR_RESTORE_CONTEXT_FROM_HOST << "State life time expired");
            RetryRequestToProtectedResourceAndDie(ctx, "Found");
        } else {
            RequestSessionToken(code, ctx);
        }
    } else {
        LOG_DEBUG_S(ctx, NMVP::EService::MVP, "Did not find context on host.");
        SendUnknownErrorResponseAndDie(ctx);
    }
}

void THandlerSessionCreate::SendUnknownErrorResponseAndDie(const NActors::TActorContext& ctx) {
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

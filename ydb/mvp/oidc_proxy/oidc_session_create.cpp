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

void THandlerSessionCreate::RetryRequestToProtectedResource(const NActors::TActorContext& ctx, const TString& responseMessage) const {
    NHttp::THeadersBuilder responseHeaders;
    RetryRequestToProtectedResource(&responseHeaders, ctx, responseMessage);
}

void THandlerSessionCreate::RetryRequestToProtectedResource(NHttp::THeadersBuilder* responseHeaders, const NActors::TActorContext& ctx, const TString& responseMessage) const {
    SetCORS(Request, responseHeaders);
    responseHeaders->Set("Location", RestoredContext.GetRequestedAddress());
    ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request->CreateResponse("302", responseMessage, *responseHeaders)));
}

void THandlerSessionCreate::TryRestoreContextFromCookie(const NActors::TActorContext& ctx) {
    NHttp::TUrlParameters urlParameters(Request->URL);
    Code = urlParameters["code"];
    TString state = urlParameters["state"];

    NHttp::THeaders headers(Request->Headers);
    NHttp::TCookies cookies(headers.Get("cookie"));
    TRestoreOidcContextResult restoreSessionResult = RestoreSessionStoredOnClientSide(state, cookies, Settings.ClientSecret);
    RestoredContext = restoreSessionResult.Context;
    if (restoreSessionResult.IsSuccess()) {
        if (Code.empty()) {
            LOG_DEBUG_S(ctx, NMVP::EService::MVP, "Restore oidc session failed: receive empty 'code' parameter");
            RetryRequestToProtectedResource(ctx, "Empty code");
            Die(ctx);
        } else {
            RequestSessionToken(Code, ctx);
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

void THandlerSessionCreate::TryRestoreContextFromHostStorage(const NActors::TActorContext& ctx) {
    NHttp::TUrlParameters urlParameters(Request->URL);
    Code = urlParameters["code"];
    TString stateParam = urlParameters["state"];

    TString decodedStateParam = Base64DecodeUneven(stateParam);
    TString stateContainer;
    TString expectedDigest;
    NJson::TJsonValue jsonValue;
    NJson::TJsonReaderConfig jsonConfig;
    if (NJson::ReadJsonTree(decodedStateParam, &jsonConfig, &jsonValue)) {
        const NJson::TJsonValue* jsonStateContainer = nullptr;
        if (jsonValue.GetValuePointer("container", &jsonStateContainer)) {
            stateContainer = jsonStateContainer->GetStringRobust();
            stateContainer = Base64Decode(stateContainer);
        }
        const NJson::TJsonValue* jsonDigest = nullptr;
        if (jsonValue.GetValuePointer("digest", &jsonDigest)) {
            expectedDigest = jsonDigest->GetStringRobust();
            expectedDigest = Base64Decode(expectedDigest);
        }
    }

    if (stateContainer.empty() || expectedDigest.empty()) {
        LOG_DEBUG_S(ctx, NMVP::EService::MVP, "Restore context from host failed: Receive empty digest or state container");
        ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(GetUnknownErrorResponse()));
        Die(ctx);
        return;
    }

    TString digest = HmacSHA1(Settings.ClientSecret, stateContainer);
    if (expectedDigest != digest) {
        LOG_DEBUG_S(ctx, NMVP::EService::MVP, "Restore context from host failed: Receive unknown state");
        ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(GetUnknownErrorResponse()));
        Die(ctx);
        return;
    }

    TString state;
    TString host;
    if (NJson::ReadJsonTree(stateContainer, &jsonConfig, &jsonValue)) {
        const NJson::TJsonValue* jsonState = nullptr;
        if (jsonValue.GetValuePointer("state", &jsonState)) {
            state = jsonState->GetStringRobust();
        }

        const NJson::TJsonValue* jsonHost = nullptr;
        if (jsonValue.GetValuePointer("host", &jsonHost)) {
            host = jsonHost->GetStringRobust();
        }
    }

    std::pair<bool, TContextRecord> restoreContextResult = ContextStorage->Find(state);
    if (restoreContextResult.first) {
        RestoredContext = restoreContextResult.second.GetContext();
        if (Code.empty()) {
            LOG_DEBUG_S(ctx, NMVP::EService::MVP, "Restore context from host failed: Receive empty 'code' parameter");
            RetryRequestToProtectedResource(ctx, "Empty code");
            Die(ctx);
        } else if (TInstant::Now() > restoreContextResult.second.GetExpirationTime()) {
            LOG_DEBUG_S(ctx, NMVP::EService::MVP, "Restore context from host failed: State life time expired");
            RetryRequestToProtectedResource(ctx, "Found");
            Die(ctx);
        } else {
            RequestSessionToken(Code, ctx);
        }
    } else {
        TryRestoreContextFromOtherHost(host, state, ctx);
    }
}

void THandlerSessionCreate::TryRestoreContextFromOtherHost(const TString& host, const TString& state, const NActors::TActorContext& ctx) {
    const TString restoreContextUrl = TStringBuilder()
                                << (Request->Endpoint->Secure ? "https://" : "http://")
                                << host << "/contex?state=" << state;
    ctx.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(NHttp::THttpOutgoingRequest::CreateRequestGet(restoreContextUrl)));
    Become(&THandlerSessionCreate::StateWork);
}

void THandlerSessionCreate::HandleRestoreContext(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event, const NActors::TActorContext& ctx) {
    NHttp::THttpOutgoingResponsePtr httpResponse;
    if (event->Get()->Error.empty() && event->Get()->Response) {
        NHttp::THttpIncomingResponsePtr response = event->Get()->Response;
        LOG_DEBUG_S(ctx, EService::MVP, "Incoming response from other oidc host: " << response->Status);
        if (response->Status == "200") {
            TString requestedAddress;
            bool isAjaxRequest = false;
            TInstant expirationTime;
            NJson::TJsonValue jsonValue;
            NJson::TJsonReaderConfig jsonConfig;
            if (NJson::ReadJsonTree(response->Body, &jsonConfig, &jsonValue)) {
                const NJson::TJsonValue* jsonRequestedAddress = nullptr;
                if (jsonValue.GetValuePointer("requested_address", &jsonRequestedAddress)) {
                    requestedAddress = jsonRequestedAddress->GetStringRobust();
                } else {
                    // Bad request. Cannot restore requested address
                }
                const NJson::TJsonValue* jsonAjaxRequest = nullptr;
                if (jsonValue.GetValuePointer("ajax_request", &jsonAjaxRequest)) {
                    isAjaxRequest = jsonAjaxRequest->GetBooleanRobust();
                } else {
                    // Bad request. Cannot restore requested address
                }
                const NJson::TJsonValue* jsonExpirationTime = nullptr;
                if (jsonValue.GetValuePointer("expiration_time", &jsonExpirationTime)) {
                    timeval timeVal {
                        .tv_sec = jsonExpirationTime->GetIntegerRobust(),
                        .tv_usec = 0
                    };
                    expirationTime = TInstant(timeVal);
                }
                RestoredContext = TContext("", requestedAddress, isAjaxRequest);
                if (Code.empty()) {
                    LOG_DEBUG_S(ctx, NMVP::EService::MVP, "Restore context from host failed: Receive empty 'code' parameter");
                    RetryRequestToProtectedResource(ctx, "Empty code");
                    Die(ctx);
                } else if (TInstant::Now() > expirationTime) {
                    LOG_DEBUG_S(ctx, NMVP::EService::MVP, "Restore context from host failed: State life time expired");
                    RetryRequestToProtectedResource(ctx, "Found");
                    Die(ctx);
                } else {
                    RequestSessionToken(Code, ctx);
                }
            } else {
                LOG_DEBUG_S(ctx, NMVP::EService::MVP, "Restore context from host failed: Can not read response from other oidc host");
                httpResponse = GetUnknownErrorResponse();
            }
        } else {
             LOG_DEBUG_S(ctx, NMVP::EService::MVP, "Restore context from host failed: response from other oidc host " << response->Status);
             httpResponse = GetUnknownErrorResponse();
        }
    } else {
        LOG_DEBUG_S(ctx, NMVP::EService::MVP, "Restore context from host failed: Unknown response from other oidc host");
        httpResponse = GetUnknownErrorResponse();
    }
    ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse));
    Die(ctx);
}

NHttp::THttpOutgoingResponsePtr THandlerSessionCreate::GetUnknownErrorResponse() {
    NHttp::THeadersBuilder responseHeaders;
    responseHeaders.Set("Content-Type", "text/html");
    SetCORS(Request, &responseHeaders);
    const static TStringBuf BAD_REQUEST_HTML_PAGE = "<html><head><title>400 Bad Request</title></head><body bgcolor=\"white\"><center><h1>Unknown error has occurred. Please open the page again</h1></center></body></html>";
    return Request->CreateResponse("400", "Bad Request", responseHeaders, BAD_REQUEST_HTML_PAGE);
}

} // NOIDC
} // NMVP

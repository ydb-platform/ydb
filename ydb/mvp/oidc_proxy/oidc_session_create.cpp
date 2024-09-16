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
    NHttp::TUrlParameters urlParameters(Request->URL);
    TString code = urlParameters["code"];
    TString state = urlParameters["state"];

    NHttp::THeaders headers(Request->Headers);
    NHttp::TCookies cookies(headers.Get("cookie"));

    if (IsStateValid(state, cookies, ctx) && !code.Empty()) {
        RequestSessionToken(code, ctx);
    } else {
        NHttp::THttpOutgoingResponsePtr response = GetHttpOutgoingResponsePtr(TStringBuf(), Request, Settings, ResponseHeaders, IsAjaxRequest);
        ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        TBase::Die(ctx);
        return;
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
            ResponseHeaders.Set("Content-Type", "text/plain");
            httpResponse = Request->CreateResponse("400", "Bad Request", ResponseHeaders, jsonError);
        } else {
            ResponseHeaders.Parse(response->Headers);
            httpResponse = Request->CreateResponse(response->Status, response->Message, ResponseHeaders, response->Body);
        }
    } else {
        ResponseHeaders.Set("Content-Type", "text/plain");
        httpResponse = Request->CreateResponse("400", "Bad Request", ResponseHeaders, event->Get()->Error);
    }
    ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse));
    Die(ctx);
}

void THandlerSessionCreate::RemoveAppliedCookie(const TString& cookieName) {
    ResponseHeaders.Set("Set-Cookie", TStringBuilder() << cookieName << "=; Path=" << GetAuthCallbackUrl() << "; Max-Age=0");
}

bool THandlerSessionCreate::IsStateValid(const TString& state, const NHttp::TCookies& cookies, const NActors::TActorContext& ctx) {
    const TString cookieName {CreateNameYdbOidcCookie(Settings.ClientSecret, state)};
    if (!cookies.Has(cookieName)) {
        LOG_DEBUG_S(ctx, EService::MVP, "Check state: Cannot find cookie " << cookieName);
        return false;
    }
    RemoveAppliedCookie(cookieName);
    TString cookieStruct = Base64Decode(cookies.Get(cookieName));
    TString stateStruct;
    TString expectedDigest;
    NJson::TJsonValue jsonValue;
    NJson::TJsonReaderConfig jsonConfig;
    if (NJson::ReadJsonTree(cookieStruct, &jsonConfig, &jsonValue)) {
        const NJson::TJsonValue* jsonStateStruct = nullptr;
        if (jsonValue.GetValuePointer("state_struct", &jsonStateStruct)) {
            stateStruct = jsonStateStruct->GetStringRobust();
            stateStruct = Base64Decode(stateStruct);
        }
        const NJson::TJsonValue* jsonDigest = nullptr;
        if (jsonValue.GetValuePointer("digest", &jsonDigest)) {
            expectedDigest = jsonDigest->GetStringRobust();
            expectedDigest = Base64Decode(expectedDigest);
        }
    }
    if (stateStruct.Empty() || expectedDigest.Empty()) {
        LOG_DEBUG_S(ctx, EService::MVP, "Check state: Struct with state and expected digest are empty");
        return false;
    }
    TString digest = HmacSHA256(Settings.ClientSecret, stateStruct);
    if (expectedDigest != digest) {
        LOG_DEBUG_S(ctx, EService::MVP, "Check state: Calculated digest is not equal expected digest");
        return false;
    }
    TString expectedState;
    if (NJson::ReadJsonTree(stateStruct, &jsonConfig, &jsonValue)) {
        const NJson::TJsonValue* jsonState = nullptr;
        if (jsonValue.GetValuePointer("state", &jsonState)) {
            expectedState = jsonState->GetStringRobust();
        }
        const NJson::TJsonValue* jsonRedirectUrl = nullptr;
        if (jsonValue.GetValuePointer("redirect_url", &jsonRedirectUrl)) {
            RedirectUrl = jsonRedirectUrl->GetStringRobust();
        } else {
            LOG_DEBUG_S(ctx, EService::MVP, "Check state: Redirect url not found in json");
            return false;
        }
        const NJson::TJsonValue* jsonExpirationTime = nullptr;
        if (jsonValue.GetValuePointer("expiration_time", &jsonExpirationTime)) {
            timeval timeVal {
                .tv_sec = jsonExpirationTime->GetIntegerRobust(),
                .tv_usec = 0
            };
            if (TInstant::Now() > TInstant(timeVal)) {
                LOG_DEBUG_S(ctx, EService::MVP, "Check state: State life time expired");
                return false;
            }
        } else {
            LOG_DEBUG_S(ctx, EService::MVP, "Check state: Expiration time not found in json");
            return false;
        }
        const NJson::TJsonValue* jsonAjaxRequest = nullptr;
        if (jsonValue.GetValuePointer("ajax_request", &jsonAjaxRequest)) {
            IsAjaxRequest = jsonAjaxRequest->GetBooleanRobust();
        } else {
            LOG_DEBUG_S(ctx, EService::MVP, "Check state: Can not detect ajax request");
            return false;
        }
    }
    return (!expectedState.Empty() && expectedState == state);
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

} // NOIDC
} // NMVP

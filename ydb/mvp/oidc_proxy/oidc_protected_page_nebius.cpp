#include <library/cpp/json/json_reader.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/security/util.h>
#include <ydb/mvp/core/appdata.h>
#include <ydb/mvp/core/mvp_tokens.h>
#include <ydb/mvp/core/mvp_log.h>
#include "openid_connect.h"
#include "context.h"
#include "oidc_protected_page_nebius.h"
namespace NMVP {
namespace NOIDC {

THandlerSessionServiceCheckNebius::THandlerSessionServiceCheckNebius(const NActors::TActorId& sender,
                                                                     const NHttp::THttpIncomingRequestPtr& request,
                                                                     const NActors::TActorId& httpProxyId,
                                                                     const TOpenIdConnectSettings& settings)
    : THandlerSessionServiceCheck(sender, request, httpProxyId, settings)
{}

TStringBuf THandlerSessionServiceCheckNebius::GetCookie(const NHttp::TCookies& cookies, const TString& cookieName, const NActors::TActorContext& ctx) {
    TStringBuf cookieValue = cookies.Get(cookieName);
    if (!cookieValue.Empty()) {
        LOG_DEBUG_S(ctx, EService::MVP, "Using cookie (" << cookieName << ": " << NKikimr::MaskTicket(cookieValue) << ")");
    }
    return cookieValue;
}

TString THandlerSessionServiceCheckNebius::DecodeToken(const TStringBuf& cookie, const NActors::TActorContext& ctx) {
    TString token;
    try {
        Base64StrictDecode(cookie, token);
    } catch (std::exception& e) {
        LOG_DEBUG_S(ctx, EService::MVP, "Base64Decode " << cookie << " cookie: " << e.what());
        token.clear();
    }
    return token;
}

void THandlerSessionServiceCheckNebius::StartOidcProcess(const NActors::TActorContext& ctx) {
    NHttp::THeaders headers(Request->Headers);
    LOG_DEBUG_S(ctx, EService::MVP, "Start OIDC process");

    NHttp::TCookies cookies(headers.Get("Cookie"));
    TStringBuf sessionCookieValue = GetCookie(cookies, CreateNameSessionCookie(Settings.ClientId), ctx);
    TStringBuf impersonatedCookieValue = GetCookie(cookies, CreateNameImpersonatedCookie(Settings.ClientId), ctx);

    TString sessionToken = DecodeToken(sessionCookieValue, ctx);
    if (sessionToken) {
        TString impersonatedToken = DecodeToken(impersonatedCookieValue, ctx);
        if (impersonatedToken) {
            ExchangeImpersonatedToken(sessionToken, impersonatedToken, ctx);
        } else {
            ExchangeSessionToken(sessionToken, ctx);
        }
    } else{
        RequestAuthorizationCode(ctx);
    }
}

void THandlerSessionServiceCheckNebius:: HandleExchange(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event, const NActors::TActorContext& ctx) {
    if (!event->Get()->Response) {
        LOG_DEBUG_S(ctx, EService::MVP, "Getting access token: Bad Request");
        NHttp::THeadersBuilder responseHeaders;
        responseHeaders.Set("Content-Type", "text/plain");
        NHttp::THttpOutgoingResponsePtr httpResponse = Request->CreateResponse("400", "Bad Request", responseHeaders, event->Get()->Error);
        ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse));
        Die(ctx);
    } else {
        NHttp::THttpIncomingResponsePtr response = event->Get()->Response;
        LOG_DEBUG_S(ctx, EService::MVP, "Getting access token: " << response->Status << " " << response->Message);
        if (response->Status == "200") {
            TString iamToken;
            static const NJson::TJsonReaderConfig JsonConfig;
            NJson::TJsonValue requestData;
            bool success = NJson::ReadJsonTree(response->Body, &JsonConfig, &requestData);
            if (success) {
                iamToken = requestData["access_token"].GetStringSafe({});
                const TString authHeader = IAM_TOKEN_SCHEME + iamToken;
                ForwardUserRequest(authHeader, ctx);
                return;
            }
        } else if (response->Status == "400" || response->Status == "401") {
            LOG_DEBUG_S(ctx, EService::MVP, "Getting access token: " << response->Body);
            if (tokenExchangeType == ETokenExchangeType::ImpersonatedToken) {
                ClearImpersonatedCookie(ctx);
            } else {
                RequestAuthorizationCode(ctx);
            }
            return;
        }
        // don't know what to do, just forward response
        NHttp::THttpOutgoingResponsePtr httpResponse;
        NHttp::THeadersBuilder responseHeaders;
        responseHeaders.Parse(response->Headers);
        httpResponse = Request->CreateResponse(response->Status, response->Message, responseHeaders, response->Body);
        ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse));
        Die(ctx);
    }
}

void THandlerSessionServiceCheckNebius::ExchangeSessionToken(const TString sessionToken, const NActors::TActorContext& ctx) {
    LOG_DEBUG_S(ctx, EService::MVP, "Exchange session token");
    tokenExchangeType = ETokenExchangeType::SessionToken;
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

void THandlerSessionServiceCheckNebius::ExchangeImpersonatedToken(const TString sessionToken, const TString impersonatedToken, const NActors::TActorContext& ctx) {
    LOG_DEBUG_S(ctx, EService::MVP, "Exchange impersonated token");
    tokenExchangeType = ETokenExchangeType::ImpersonatedToken;
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
            << "&subject_token_type=urn:ietf:params:oauth:token-type:jwt"
            << "&subject_token=" << impersonatedToken
            << "&actor_token=" << sessionToken
            << "&actor_token_type=urn:ietf:params:oauth:token-type:session_token";
    httpRequest->Set<&NHttp::THttpRequest::Body>(body);

    ctx.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));

    Become(&THandlerSessionServiceCheckNebius::StateExchange);
}

void THandlerSessionServiceCheckNebius::ClearImpersonatedCookie(const NActors::TActorContext& ctx) {
    TString impersonatedCookieName = CreateNameImpersonatedCookie(Settings.ClientId);
    LOG_DEBUG_S(ctx, EService::MVP, "Clear impersonated cookie (" << impersonatedCookieName << ") and retry");
    NHttp::THeadersBuilder responseHeaders;
    SetCORS(Request, &responseHeaders);
    responseHeaders.Set("Set-Cookie", ClearSecureCookie(impersonatedCookieName));
    responseHeaders.Set("Location", Request->URL);
    ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request->CreateResponse("307", "Temporary Redirect", responseHeaders)));
    Die(ctx);
}

void THandlerSessionServiceCheckNebius::RequestAuthorizationCode(const NActors::TActorContext& ctx) {
    LOG_DEBUG_S(ctx, EService::MVP, "Request authorization code");
    NHttp::THttpOutgoingResponsePtr httpResponse = GetHttpOutgoingResponsePtr(Request, Settings);
    ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse));
    Die(ctx);
}

void THandlerSessionServiceCheckNebius::ForwardUserRequest(TStringBuf authHeader, const NActors::TActorContext& ctx, bool secure) {
    THandlerSessionServiceCheck::ForwardUserRequest(authHeader, ctx, secure);
    Become(&THandlerSessionServiceCheckNebius::StateWork);
}

bool THandlerSessionServiceCheckNebius::NeedSendSecureHttpRequest(const NHttp::THttpIncomingResponsePtr& response) const {
    if ((response->Status == "400" || response->Status.empty()) && RequestedPageScheme.empty()) {
        return !response->GetRequest()->Secure;
    }
    return false;
}

} // NOIDC
} // NMVP

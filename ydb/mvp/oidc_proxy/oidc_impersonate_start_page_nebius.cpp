#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/string_utils/quote/quote.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/security/util.h>
#include <ydb/mvp/core/mvp_log.h>
#include <ydb/mvp/core/mvp_tokens.h>
#include "openid_connect.h"
#include "oidc_session_create.h"
#include "oidc_impersonate_start_page_nebius.h"

namespace NMVP::NOIDC {

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
    BLOG_D("Start impersonation process");

    NHttp::TUrlParameters urlParameters(Request->URL);
    TString serviceAccountId = urlParameters["service_account_id"];

    NHttp::THeaders headers(Request->Headers);
    NHttp::TCookies cookies(headers.Get("Cookie"));

    TString sessionCookieName = CreateNameSessionCookie(Settings.ClientId);
    TStringBuf sessionCookieValue = cookies.Get(sessionCookieName);
    if (!sessionCookieValue.Empty()) {
        BLOG_D("Using session cookie (" << sessionCookieName << ": " << NKikimr::MaskTicket(sessionCookieValue) << ")");
    }
    TString sessionToken = DecodeToken(sessionCookieValue);
    TStringBuf impersonatedCookieValue = GetCookie(cookies, CreateNameImpersonatedCookie(Settings.ClientId));

    if (sessionToken.empty()) {
        return ReplyBadRequestAndDie("Wrong impersonate parameter: session cookie not found", ctx);
    }
    if (!impersonatedCookieValue.empty()) {
        return ReplyBadRequestAndDie("Wrong impersonate parameter: impersonated cookie already exists", ctx);
    }
    if (serviceAccountId.empty()) {
        return ReplyBadRequestAndDie("Wrong impersonate parameter: service_account_id not found", ctx);
    }

    RequestImpersonatedToken(sessionToken, serviceAccountId, ctx);
}

void THandlerImpersonateStart::RequestImpersonatedToken(const TString& sessionToken, const TString& serviceAccountId, const NActors::TActorContext& ctx) {
    BLOG_D("Request impersonated token");
    NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestPost(Settings.GetImpersonateEndpointURL());
    httpRequest->Set<&NHttp::THttpRequest::ContentType>("application/x-www-form-urlencoded");

    TMvpTokenator* tokenator = MVPAppData()->Tokenator;
    TString token = "";
    if (tokenator) {
        token = tokenator->GetToken(Settings.SessionServiceTokenName);
    }
    httpRequest->Set("Authorization", token); // Bearer included

    TStringBuilder body;
    body << "session=" << sessionToken
         << "&service_account_id=" << serviceAccountId;
    TString bodyStr = body;
    CGIEscape(bodyStr);
    httpRequest->Set<&NHttp::THttpRequest::Body>(bodyStr);

    ctx.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));
    Become(&THandlerImpersonateStart::StateWork);
}

void THandlerImpersonateStart::ProcessImpersonatedToken(const TString& impersonatedToken, const NActors::TActorContext& ctx) {
    TString impersonatedCookieName = CreateNameImpersonatedCookie(Settings.ClientId);
    TString impersonatedCookieValue = Base64Encode(impersonatedToken);
    BLOG_D("Set impersonated cookie: (" << impersonatedCookieName << ": " << NKikimr::MaskTicket(impersonatedCookieValue) << ")");

    NHttp::THeadersBuilder responseHeaders;
    responseHeaders.Set("Set-Cookie", CreateSecureCookie(impersonatedCookieName, impersonatedCookieValue));
    SetCORS(Request, &responseHeaders);
    NHttp::THttpOutgoingResponsePtr httpResponse = Request->CreateResponse("200", "OK", responseHeaders);
    ReplyAndDie(httpResponse, ctx);
}

void THandlerImpersonateStart::Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event, const NActors::TActorContext& ctx) {
    NHttp::THttpOutgoingResponsePtr httpResponse;
    if (event->Get()->Error.empty() && event->Get()->Response) {
        NHttp::THttpIncomingResponsePtr response = event->Get()->Response;
        BLOG_D("Incoming response from authorization server: " << response->Status);
        if (response->Status == "200") {
            TStringBuf errorMessage;
            NJson::TJsonValue jsonValue;
            NJson::TJsonReaderConfig jsonConfig;
            if (NJson::ReadJsonTree(response->Body, &jsonConfig, &jsonValue)) {
                const NJson::TJsonValue* jsonImpersonatedToken;
                if (jsonValue.GetValuePointer("impersonation", &jsonImpersonatedToken)) {
                    TString impersonatedToken = jsonImpersonatedToken->GetStringRobust();
                    ProcessImpersonatedToken(impersonatedToken, ctx);
                    return;
                } else {
                    errorMessage = "Wrong OIDC provider response: impersonated token not found";
                }
            } else {
                errorMessage =  "Wrong OIDC response";
            }
            NHttp::THeadersBuilder responseHeaders;
            responseHeaders.Set("Content-Type", "text/plain");
            SetCORS(Request, &responseHeaders);
            return ReplyAndDie(Request->CreateResponse("400", "Bad Request", responseHeaders, errorMessage), ctx);
        } else {
            NHttp::THeadersBuilder responseHeaders;
            NHttp::THeaders headers(response->Headers);
            if (headers.Has("Content-Type")) {
                responseHeaders.Set("Content-Type", headers.Get("Content-Type"));
            }
            SetCORS(Request, &responseHeaders);
            return ReplyAndDie(Request->CreateResponse(response->Status, response->Message, responseHeaders, response->Body), ctx);
        }
    } else {
        NHttp::THeadersBuilder responseHeaders;
        responseHeaders.Set("Content-Type", "text/plain");
        SetCORS(Request, &responseHeaders);
        return ReplyAndDie(Request->CreateResponse("400", "Bad Request", responseHeaders, event->Get()->Error), ctx);
    }
}

void THandlerImpersonateStart::ReplyAndDie(NHttp::THttpOutgoingResponsePtr httpResponse, const NActors::TActorContext& ctx) {
    ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse));
    Die(ctx);
}

void THandlerImpersonateStart::ReplyBadRequestAndDie(const TString& errorMessage, const NActors::TActorContext& ctx) {
    NHttp::THeadersBuilder responseHeaders;
    responseHeaders.Set("Content-Type", "text/plain");
    SetCORS(Request, &responseHeaders);
    NHttp::THttpOutgoingResponsePtr httpResponse = Request->CreateResponse("400", "Bad Request", responseHeaders, errorMessage);
    ReplyAndDie(httpResponse, ctx);
}

TImpersonateStartPageHandler::TImpersonateStartPageHandler(const NActors::TActorId& httpProxyId, const TOpenIdConnectSettings& settings)
    : TBase(&TImpersonateStartPageHandler::StateWork)
    , HttpProxyId(httpProxyId)
    , Settings(settings)
{}

void TImpersonateStartPageHandler::Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
    ctx.Register(new THandlerImpersonateStart(event->Sender, event->Get()->Request, HttpProxyId, Settings));
}

} // NMVP::NOIDC

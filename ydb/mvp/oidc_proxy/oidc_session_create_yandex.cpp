#include <ydb/library/actors/http/http.h>
#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>
#include <ydb/library/security/util.h>
#include <ydb/mvp/core/mvp_tokens.h>
#include <ydb/mvp/core/appdata.h>
#include <ydb/mvp/core/mvp_log.h>
#include "oidc_session_create_yandex.h"

namespace NMVP {
namespace NOIDC {

using namespace NActors;

THandlerSessionCreateYandex::THandlerSessionCreateYandex(const NActors::TActorId& sender,
                                                         const NHttp::THttpIncomingRequestPtr& request,
                                                         const NActors::TActorId& httpProxyId,
                                                         const TOpenIdConnectSettings& settings)
    : THandlerSessionCreate(sender, request, httpProxyId, settings)
{}

void THandlerSessionCreateYandex::RequestSessionToken(const TString& code) {
    NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestPost(Settings.GetTokenEndpointURL());
    httpRequest->Set<&NHttp::THttpRequest::ContentType>("application/x-www-form-urlencoded");
    httpRequest->Set("Authorization", Settings.GetAuthorizationString());

    TCgiParameters params;
    params.emplace("grant_type", "authorization_code");
    params.emplace("code", code);
    httpRequest->Set<&NHttp::THttpRequest::Body>(params());

    Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));
    Become(&THandlerSessionCreateYandex::StateWork);
}

void THandlerSessionCreateYandex::ProcessSessionToken(const NJson::TJsonValue& jsonValue) {
    const NJson::TJsonValue* jsonAccessToken, jsonExpiresIn;
    if (!jsonValue.GetValuePointer("access_token", &jsonAccessToken)) {
        return ReplyBadRequestAndPassAway("Wrong OIDC provider response: access_token not found");
    }
    TString sessionToken = jsonAccessToken->GetStringRobust();
    std::unique_ptr<NYdbGrpc::TServiceConnection<TSessionService>> connection = CreateGRpcServiceConnection<TSessionService>(Settings.SessionServiceEndpoint);

    yandex::cloud::priv::oauth::v1::CreateSessionRequest requestCreate;
    requestCreate.Setaccess_token(sessionToken);

    TMvpTokenator* tokenator = MVPAppData()->Tokenator;
    TString token = "";
    if (tokenator) {
        token = tokenator->GetToken(Settings.SessionServiceTokenName);
    }
    NYdbGrpc::TCallMeta meta;
    SetHeader(meta, "authorization", token);
    meta.Timeout = std::chrono::seconds(10);

    NActors::TActorSystem* actorSystem = TActivationContext::ActorSystem();
    NActors::TActorId actorId = SelfId();
    NYdbGrpc::TResponseCallback<yandex::cloud::priv::oauth::v1::CreateSessionResponse> responseCb =
        [actorId, actorSystem](NYdbGrpc::TGrpcStatus&& status, yandex::cloud::priv::oauth::v1::CreateSessionResponse&& response) -> void {
        if (status.Ok()) {
            actorSystem->Send(actorId, new TEvPrivate::TEvCreateSessionResponse(std::move(response)));
        } else {
            actorSystem->Send(actorId, new TEvPrivate::TEvErrorResponse(status));
        }
    };
    connection->DoRequest(requestCreate, std::move(responseCb), &yandex::cloud::priv::oauth::v1::SessionService::Stub::AsyncCreate, meta);
}

void THandlerSessionCreateYandex::HandleCreateSession(TEvPrivate::TEvCreateSessionResponse::TPtr event) {
    BLOG_D("SessionService.Create(): OK");
    auto response = event->Get()->Response;
    NHttp::THeadersBuilder responseHeaders;
    for (const auto& cookie : response.Getset_cookie_header()) {
        responseHeaders.Set("Set-Cookie", ChangeSameSiteFieldInSessionCookie(cookie));
    }
    RetryRequestToProtectedResourceAndDie(&responseHeaders);
}

void THandlerSessionCreateYandex::HandleError(TEvPrivate::TEvErrorResponse::TPtr event) {
    BLOG_D("SessionService.Create(): " << event->Get()->Status);
    if (event->Get()->Status == "400") {
        RetryRequestToProtectedResourceAndDie();
    } else {
        NHttp::THeadersBuilder responseHeaders;
        responseHeaders.Set("Content-Type", "text/plain");
        SetCORS(Request, &responseHeaders);
        ReplyAndPassAway(Request->CreateResponse( event->Get()->Status, event->Get()->Message, responseHeaders, event->Get()->Details));
    }
}

} // NMVP

} // NMVP

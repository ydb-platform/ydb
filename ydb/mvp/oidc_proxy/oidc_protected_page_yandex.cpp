#include <ydb/library/actors/http/http.h>
#include <ydb/mvp/core/mvp_tokens.h>
#include <ydb/mvp/core/appdata.h>
#include <ydb/mvp/core/mvp_log.h>
#include "oidc_protected_page_yandex.h"

namespace NMVP {
namespace NOIDC {

THandlerSessionServiceCheckYandex::THandlerSessionServiceCheckYandex(const NActors::TActorId& sender,
                                                                     const NHttp::THttpIncomingRequestPtr& request,
                                                                     const NActors::TActorId& httpProxyId,
                                                                     const TOpenIdConnectSettings& settings)
    : THandlerSessionServiceCheck(sender, request, httpProxyId, settings)
{}

void THandlerSessionServiceCheckYandex::Bootstrap(const NActors::TActorContext& ctx) {
    THandlerSessionServiceCheck::Bootstrap(ctx);
    Become(&THandlerSessionServiceCheckYandex::StateWork);
}

void THandlerSessionServiceCheckYandex::Handle(TEvPrivate::TEvCheckSessionResponse::TPtr event, const NActors::TActorContext& ctx) {
    LOG_DEBUG_S(ctx, EService::MVP, "SessionService.Check(): OK");
    auto response = event->Get()->Response;
    const auto& iamToken = response.iam_token();
    const TString authHeader = IAM_TOKEN_SCHEME + iamToken.iam_token();
    ForwardUserRequest(authHeader, ctx);
}

void THandlerSessionServiceCheckYandex::Handle(TEvPrivate::TEvErrorResponse::TPtr event, const NActors::TActorContext& ctx) {
    LOG_DEBUG_S(ctx, EService::MVP, "SessionService.Check(): " << event->Get()->Status);
    NHttp::THttpOutgoingResponsePtr httpResponse;
    if (event->Get()->Status == "400") {
        httpResponse = GetHttpOutgoingResponsePtr(Request, Settings, IsAjaxRequest);
    } else {
        httpResponse = Request->CreateResponse( event->Get()->Status, event->Get()->Message, "text/plain", event->Get()->Details);
    }
    ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse));
    Die(ctx);
}

void THandlerSessionServiceCheckYandex::StartOidcProcess(const NActors::TActorContext& ctx) {
    NHttp::THeaders headers(Request->Headers);
    TStringBuf cookie = headers.Get("cookie");
    yandex::cloud::priv::oauth::v1::CheckSessionRequest request;
    request.Setcookie_header(TString(cookie));

    std::unique_ptr<NYdbGrpc::TServiceConnection<TSessionService>> connection = CreateGRpcServiceConnection<TSessionService>(Settings.SessionServiceEndpoint);

    NActors::TActorSystem* actorSystem = ctx.ActorSystem();
    NActors::TActorId actorId = ctx.SelfID;
    NYdbGrpc::TResponseCallback<yandex::cloud::priv::oauth::v1::CheckSessionResponse> responseCb =
        [actorId, actorSystem](NYdbGrpc::TGrpcStatus&& status, yandex::cloud::priv::oauth::v1::CheckSessionResponse&& response) -> void {
        if (status.Ok()) {
            actorSystem->Send(actorId, new TEvPrivate::TEvCheckSessionResponse(std::move(response)));
        } else {
            actorSystem->Send(actorId, new TEvPrivate::TEvErrorResponse(status));
        }
    };

    NMVP::TMvpTokenator* tokenator = MVPAppData()->Tokenator;
    TString token = "";
    if (tokenator) {
        token = tokenator->GetToken(Settings.SessionServiceTokenName);
    }
    NYdbGrpc::TCallMeta meta;
    SetHeader(meta, "authorization", token);
    meta.Timeout = TDuration::Seconds(10);
    connection->DoRequest(request, std::move(responseCb), &yandex::cloud::priv::oauth::v1::SessionService::Stub::AsyncCheck, meta);
}

bool THandlerSessionServiceCheckYandex::NeedSendSecureHttpRequest(const NHttp::THttpIncomingResponsePtr& response) const {
    if ((response->Status == "400" || response->Status.empty()) && RequestedPageScheme.empty()) {
        NHttp::THttpOutgoingRequestPtr request = response->GetRequest();
        if (!request->Secure) {
            static const TStringBuf bodyContent = "The plain HTTP request was sent to HTTPS port";
            NHttp::THeadersBuilder headers(response->Headers);
            TStringBuf contentType = headers.Get("Content-Type").NextTok(';');
            TStringBuf body = response->Body;
            return contentType == "text/html" && body.find(bodyContent) != TStringBuf::npos;
        }
    }
    return false;
}

}  // NOIDC
}  // NMVP

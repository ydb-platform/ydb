#include <library/cpp/json/json_reader.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/mvp/core/appdata.h>
#include <ydb/mvp/core/mvp_tokens.h>
#include <ydb/mvp/core/mvp_log.h>
#include "openid_connect.h"
#include "context.h"
#include "oidc_whoami_extend_nebius.h"

namespace NMVP::NOIDC {

THandlerWhoamiExtendNebius::THandlerWhoamiExtendNebius(const NActors::TActorId& sender,
                                                       const NHttp::THttpIncomingRequestPtr& request,
                                                       const NActors::TActorId& httpProxyId,
                                                       const TOpenIdConnectSettings& settings,
                                                       TStringBuf authHeader)
    : Sender(sender)
    , Request(request)
    , HttpProxyId(httpProxyId)
    , Settings(settings)
    , ProtectedPage(Request->URL.SubStr(1))
    , AuthHeader(authHeader)
{}

void THandlerWhoamiExtendNebius::Bootstrap(const NActors::TActorContext& ) {
    if (!ProtectedPage.CheckRequestedHost(Settings)) {
        return ReplyAndPassAway(CreateResponseForbiddenHost(Request, ProtectedPage));
    }

    ForwardUserRequest(AuthHeader, Secure);
    RequestWhoamiExtendedInfo();
    Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup());
    Become(&THandlerWhoamiExtendNebius::StateWork);
}


void THandlerWhoamiExtendNebius::RequestWhoamiExtendedInfo() {
    auto connection = CreateGRpcServiceConnection<TProfileService>(Settings.WhoamiExtendedInfoEndpoint);

    nebius::iam::v1::GetProfileRequest request;
    NActors::TActorSystem* actorSystem = NActors::TActivationContext::ActorSystem();
    NActors::TActorId actorId = SelfId();
    NYdbGrpc::TResponseCallback<nebius::iam::v1::GetProfileResponse> responseCb =
        [actorId, actorSystem](NYdbGrpc::TGrpcStatus&& status, nebius::iam::v1::GetProfileResponse&& response) -> void {
        if (status.Ok()) {
            actorSystem->Send(actorId, new TEvPrivate::TEvGetProfileResponse(std::move(response)));
        } else {
            actorSystem->Send(actorId, new TEvPrivate::TEvErrorResponse(status));
        }
    };

    NYdbGrpc::TCallMeta meta;
    SetHeader(meta, "authorization", AuthHeader);
    meta.Timeout = TDuration::Seconds(10);

    connection->DoRequest(request, std::move(responseCb), &nebius::iam::v1::ProfileService::Stub::AsyncGet, meta);
    DataRequests++;
}

void THandlerWhoamiExtendNebius::RequestDone() {
    DataRequests--;
    if (DataRequests == 0) {
        ReplyAndPassAway();
    }
}

void THandlerWhoamiExtendNebius::Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event) {
    WhoamiResponse = std::move(event);
    RequestDone();
}

void THandlerWhoamiExtendNebius::Handle(TEvPrivate::TEvGetProfileResponse::TPtr event) {
    BLOG_D("SessionService Extention Info: OK");
    WhoamiExtendedInfoResponse = std::move(event);
    RequestDone();
}

void THandlerWhoamiExtendNebius::Handle(TEvPrivate::TEvErrorResponse::TPtr event) {
    BLOG_D("SessionService Extention Info: " << event->Get()->Status);
    RequestDone();
}

void THandlerWhoamiExtendNebius::ForwardUserRequest(TStringBuf authHeader, bool secure) {
    BLOG_D("Forward user request bypass OIDC");

    TProxiedRequestParams params(Request, authHeader, secure, ProtectedPage, Settings);
    auto httpRequest = CreateProxiedRequest(params);

    auto requestEvent = std::make_unique<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(httpRequest);
    Send(HttpProxyId, requestEvent.release());
    DataRequests++;
}

bool THandlerWhoamiExtendNebius::NeedSendSecureHttpRequest(const NHttp::THttpIncomingResponsePtr& response) const {
    if ((response->Status == "400" || response->Status.empty()) && RequestedPageScheme.empty()) {
        return !response->GetRequest()->Secure;
    }
    return false;
}

void THandlerWhoamiExtendNebius::ReplyAndPassAway(NHttp::THttpOutgoingResponsePtr httpResponse) {
    Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(std::move(httpResponse)));
    PassAway();
}

void THandlerWhoamiExtendNebius::ReplyAndPassAway(TProxiedResponseParams& params) {
    if (params.Response) {
        return ReplyAndPassAway(CreateProxiedResponse(params));
    }

    NHttp::THeadersBuilder responseHeaders;
    responseHeaders.Set("Content-Type", "text/plain");
    SetCORS(Request, &responseHeaders);
    ReplyAndPassAway(Request->CreateResponse(params.OutStatus, params.OutMessage, responseHeaders));
}

constexpr TStringBuf USER_SID = "UserSID";
constexpr TStringBuf ORIGINAL_USER_TOKEN = "OriginalUserToken";
constexpr TStringBuf EXTENDED_INFO = "ExtendedInfo";

void THandlerWhoamiExtendNebius::SetResponseStatus(const NJson::TJsonValue& json, TProxiedResponseParams& params) const {
    bool hasUserSID = json.Has(USER_SID) ||
        (json.Has(EXTENDED_INFO) &&
         json[EXTENDED_INFO]["userProfile"].Has("id"));
    bool hasOriginalUserToken = json.Has(ORIGINAL_USER_TOKEN);

    if (hasUserSID && hasOriginalUserToken) {
        TStringStream content;
        NJson::WriteJson(&content, &json, {
            .FloatToStringMode = EFloatToStringMode::PREC_NDIGITS,
            .ValidateUtf8 = false,
            .WriteNanAsString = true,
        });
        params.OutStatus = "200";
        params.OutMessage = "OK";
        params.OutBody = content.Str();
        return;
    }

    if (!WhoamiResponse.has_value() ||
        (!(*WhoamiResponse)->Get()->Response &&
         (*WhoamiResponse)->Get()->Error.find("timeout") != TString::npos))
    {
        params.OutStatus = "504";
        params.OutMessage = "Gateway Timeout";
        return;
    }

    params.OutStatus = "502";
    params.OutMessage = "Bad Gateway";
    return;
}

void THandlerWhoamiExtendNebius::ReplyAndPassAway() {
    NJson::TJsonValue json;
    NHttp::THttpIncomingResponsePtr response;
    if (WhoamiResponse.has_value()) {
        if ((*WhoamiResponse)->Get()->Response != nullptr) {
            response = std::move((*WhoamiResponse)->Get()->Response);
            NJson::ReadJsonTree(response->Message, &json);
            if (response->Status != "200") {
                json["ViewerRequestStatus"] = response->Status;
                json["ViewerRequestBody"] = response->Message;
            }
        } else {
            BLOG_D("Incoming response for protected resource: " << (*WhoamiResponse)->Get()->Error);
            json["ViewerRequestBody"] = (*WhoamiResponse)->Get()->Error;
        }
    } else {
        json["ViewerRequestBody"] = "Timeout while waiting for Whoami info";
    }
    if (WhoamiExtendedInfoResponse.has_value()) {
        auto& iamResponse = (*WhoamiExtendedInfoResponse)->Get()->Response;
        NJson::TJsonValue extendedJson;
        TJsonSettings jsonSettings;
        TStringStream jsonStream;
        TProtoToJson::ProtoToJson(jsonStream, iamResponse, jsonSettings);

        NJson::ReadJsonTree(jsonStream.Str(), &json[EXTENDED_INFO]);
    }

    TProxiedResponseParams params {
        .Request = Request,
        .Response = response,
        .ProtectedPage = ProtectedPage,
        .Settings = Settings
    };
    SetResponseStatus(json, params);

    return ReplyAndPassAway(params);
}

} // NMVP::NOIDC

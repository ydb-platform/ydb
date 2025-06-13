#include "oidc_whoami_extended_nebius.h"
#include "context.h"

#include <ydb/mvp/core/appdata.h>
#include <ydb/mvp/core/mvp_log.h>
#include <ydb/mvp/core/mvp_tokens.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/protobuf/json/proto2json.h>

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

    ForwardUserRequest(AuthHeader);
    RequestWhoamiExtendedInfo();
    Schedule(TDuration::Seconds(10), new NActors::TEvents::TEvWakeup());
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
    if (event->Get()->Response != nullptr) {
        NHttp::THttpIncomingResponsePtr response = event->Get()->Response;
        BLOG_D("Incoming response for protected resource: " << response->Status);
        if (NeedSendSecureHttpRequest(response)) {
            BLOG_D("Try to send request to HTTPS port");
            ForwardUserRequest(AuthHeader, true);
        } else if (response->Status.StartsWith("3")) {
            TProxiedResponseParams params(Request, response, ProtectedPage, Settings);
            return ReplyAndPassAway(CreateProxiedResponse(params));
        } else {
            YdbResponse = std::move(event);
        }
    }
    RequestDone();
}

void THandlerWhoamiExtendNebius::Handle(TEvPrivate::TEvGetProfileResponse::TPtr event) {
    BLOG_D("Whoami Extention Info: OK");
    IamResponse = std::move(event);
    RequestDone();
}

void THandlerWhoamiExtendNebius::Handle(TEvPrivate::TEvErrorResponse::TPtr event) {
    BLOG_D("Whoami Extention Info " << event->Get()->Status << ": " << event->Get()->Message << ", " << event->Get()->Details);
    IamError = std::move(event);
    RequestDone();
}

void THandlerWhoamiExtendNebius::HandleTimeout() {
    BLOG_D("Timeout while waiting for whoami info");
    ReplyAndPassAway();
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
    if ((response->Status == "400" || response->Status.empty()) && ProtectedPage.Scheme.empty()) {
        return !response->GetRequest()->Secure;
    }
    return false;
}

void THandlerWhoamiExtendNebius::ReplyAndPassAway(NHttp::THttpOutgoingResponsePtr httpResponse) {
    Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(std::move(httpResponse)));
    PassAway();
}

void THandlerWhoamiExtendNebius::ReplyAndPassAway(TProxiedResponseParams params) {
    if (params.Response) {
        return ReplyAndPassAway(CreateProxiedResponse(params));
    }

    NHttp::THeadersBuilder responseHeaders;
    responseHeaders.Set("Content-Type", "text/plain");
    SetCORS(Request, &responseHeaders);
    ReplyAndPassAway(Request->CreateResponse(params.OutStatus, params.OutMessage, responseHeaders, params.OutBody));
}

TProxiedResponseParams THandlerWhoamiExtendNebius::CreateResponseParams(NHttp::THttpIncomingResponsePtr response,
                                                                        NJson::TJsonValue& json,
                                                                        NJson::TJsonValue& errorJson) const {
    TString OutStatus;
    TString OutMessage;
    NJson::TJsonValue* outJson = nullptr;

    if (json.Has(USER_SID) && json.Has(ORIGINAL_USER_TOKEN)) {
        OutStatus = "200";
        OutMessage = "OK";
        if (errorJson.Has(EXTENDED_ERRORS)) {
            json[EXTENDED_ERRORS] = errorJson[EXTENDED_ERRORS];
        }
        outJson = &json;
    } else {
        if (!YdbResponse.has_value() && !IamResponse.has_value() && !IamError.has_value()) {
            OutStatus = "504";
            OutMessage = "Gateway Timeout";
        } else {
            OutStatus = "500";
            OutMessage = "Internal Server Error";
        }
        outJson = &errorJson;
    }

    TStringStream content;
    NJson::WriteJson(&content, outJson, {
        .FloatToStringMode = EFloatToStringMode::PREC_NDIGITS,
        .ValidateUtf8 = false,
        .WriteNanAsString = true,
    });

    TProxiedResponseParams params {
        .Request = Request,
        .Response = response,
        .ProtectedPage = ProtectedPage,
        .Settings = Settings,
        .OutStatus = OutStatus,
        .OutMessage = OutMessage,
        .OutBody = content.Str()
    };
    return params;
}

void THandlerWhoamiExtendNebius::SetExtendedError(NJson::TJsonValue& root, const TStringBuf section, const TStringBuf key, const TStringBuf value) {
    if (!value.empty()) {
        root[EXTENDED_ERRORS][section][key] = value;
    }
}

void THandlerWhoamiExtendNebius::ReplyAndPassAway() {
    NJson::TJsonValue json;
    NJson::TJsonValue errorJson;
    NHttp::THttpIncomingResponsePtr response;

    if (YdbResponse.has_value()) {
        if ((*YdbResponse)->Get()->Response != nullptr) {
            response = (*YdbResponse)->Get()->Response;
            NJson::ReadJsonTree(response->Body, &json);
            if (!response->Status.StartsWith("2")) {
                SetExtendedError(errorJson, "Ydb", "ResponseStatus", response->Status);
                SetExtendedError(errorJson, "Ydb", "ResponseMessage", response->Message);
                SetExtendedError(errorJson, "Ydb", "ResponseBody", response->Body);
            }
        } else {
            BLOG_D("Incoming response for protected resource: " << (*YdbResponse)->Get()->Error);
            SetExtendedError(errorJson, "Ydb", "ClientError", (*YdbResponse)->Get()->Error);
        }
    } else {
        SetExtendedError(json, "Ydb", "ClientError", "Timeout while waiting for whoami info");
    }

    if (IamResponse.has_value()) {
        auto& iamResponse = (*IamResponse)->Get()->Response;
        TJsonSettings jsonSettings;
        TStringStream jsonStream;
        TProtoToJson::ProtoToJson(jsonStream, iamResponse, jsonSettings);

        NJson::TJsonValue extendedJson;
        if (NJson::ReadJsonTree(jsonStream.Str(), &extendedJson)) {
            json[EXTENDED_INFO] = extendedJson;
            if (!json.Has(USER_SID)) {
                if (extendedJson.Has("user_profile") && extendedJson["user_profile"].Has("id")) {
                    json[USER_SID] = extendedJson["user_profile"]["id"];
                }
            }
        }
    } else if (IamError.has_value()) {
        const auto& error = *IamError;
        SetExtendedError(errorJson, "Iam", "ResponseStatus", error->Get()->Status);
        SetExtendedError(errorJson, "Iam", "ResponseMessage", error->Get()->Message);
        SetExtendedError(errorJson, "Iam", "ResponseDetails", error->Get()->Details);
    } else {
        SetExtendedError(errorJson, "Iam", "ClientError", "Timeout while waiting for whoami extended info");
    }

    if (!json.Has(ORIGINAL_USER_TOKEN)) {
        TStringBuf tail;
        if (TStringBuf(AuthHeader).AfterPrefix(IAM_TOKEN_SCHEME, tail)) {
            json[ORIGINAL_USER_TOKEN] = tail;
        }
    }

    return ReplyAndPassAway(CreateResponseParams(response, json, errorJson));
}

} // NMVP::NOIDC

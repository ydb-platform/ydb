#include "extension_whoami.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/protobuf/json/proto2json.h>

namespace NMVP::NOIDC {

void TExtensionWhoami::Bootstrap() {
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
    meta.Timeout = TDuration::MilliSeconds(Settings.EnrichmentProcessTimeoutMs);

    connection->DoRequest(request, std::move(responseCb), &nebius::iam::v1::ProfileService::Stub::AsyncGet, meta);
    Become(&TExtensionWhoami::StateWork);
}

void TExtensionWhoami::Handle(TEvPrivate::TEvGetProfileResponse::TPtr event) {
    BLOG_D("Whoami Extention Info: OK");
    IamResponse = std::move(event);
    ApplyIfReady();
}

void TExtensionWhoami::Handle(TEvPrivate::TEvErrorResponse::TPtr event) {
    BLOG_D("Whoami Extention Info " << event->Get()->Status << ": " << event->Get()->Message << ", " << event->Get()->Details);
    IamError = std::move(event);
    ApplyIfReady();
}

void TExtensionWhoami::PatchResponse(NJson::TJsonValue& json, NJson::TJsonValue& errorJson) {
    TString statusOverride;
    TString messageOverride;
    NJson::TJsonValue* outJson = nullptr;

    SetCORS(Context->Params->Request, Context->Params->HeadersOverride.Get());
    Context->Params->HeadersOverride->Set("Content-Type", "application/json; charset=utf-8");

    if (json.Has(USER_SID) && json.Has(ORIGINAL_USER_TOKEN)) {
        statusOverride = "200";
        messageOverride = "OK";
        if (errorJson.Has(EXTENDED_ERRORS)) {
            json[EXTENDED_ERRORS] = errorJson[EXTENDED_ERRORS];
        }
        outJson = &json;
    } else {
        if (!IamResponse.has_value() && !IamError.has_value()) {
            statusOverride = "504";
            messageOverride = "Gateway Timeout";
        } else {
            statusOverride = "500";
            messageOverride = "Internal Server Error";
        }
        outJson = &errorJson;
    }

    TStringStream content;
    NJson::WriteJson(&content, outJson, {
        .FloatToStringMode = EFloatToStringMode::PREC_NDIGITS,
        .ValidateUtf8 = false,
        .WriteNanAsString = true,
    });

    auto& params = Context->Params;
    params->StatusOverride = statusOverride;
    params->MessageOverride = messageOverride;
    params->BodyOverride = content.Str();
}

void TExtensionWhoami::Handle(TEvPrivate::TEvExtensionRequest::TPtr ev) {
    Context = std::move(ev->Get()->Context);
    if (Context->Params->StatusOverride.StartsWith("3") || Context->Params->StatusOverride == "404") {
        ContinueAndPassAway();
    }
    ApplyIfReady();
}

void TExtensionWhoami::SetExtendedError(NJson::TJsonValue& root, const TStringBuf section, const TStringBuf key, const TStringBuf value) {
    if (!value.empty()) {
        root[EXTENDED_ERRORS][section][key] = value;
    }
}

void TExtensionWhoami::ApplyIfReady() {
    if (!Context) {
        return;
    }
    if (IamResponse.has_value() || IamError.has_value() || Timeout) {
        ApplyExtension();
    }
}

void TExtensionWhoami::ApplyExtension() {
    NJson::TJsonValue json;
    NJson::TJsonValue errorJson;
    NHttp::THttpIncomingResponsePtr response;
    auto& params = Context->Params;

    if (params->StatusOverride) {
        NJson::ReadJsonTree(params->BodyOverride, &json);
        if (!params->StatusOverride.StartsWith("2")) {
            SetExtendedError(errorJson, "Ydb", "ResponseStatus", params->StatusOverride);
            SetExtendedError(errorJson, "Ydb", "ResponseMessage", params->MessageOverride);
            SetExtendedError(errorJson, "Ydb", "ResponseBody", params->BodyOverride);
        }
    } else {
        TString& error = params->ResponseError;
        BLOG_D("Incoming client error for protected resource: " << error);
        SetExtendedError(errorJson, "Ydb", "ClientError", error);
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
                } else if (extendedJson.Has("service_account_profile") && extendedJson["service_account_profile"].Has("info") &&
                           extendedJson["service_account_profile"]["info"].Has("metadata") &&
                           extendedJson["service_account_profile"]["info"]["metadata"].Has("id")) {
                    json[USER_SID] = extendedJson["service_account_profile"]["info"]["metadata"]["id"];
                }
            }
        }
    } else if (IamError.has_value()) {
        const auto& error = *IamError;
        SetExtendedError(errorJson, "Iam", "ResponseStatus", error->Get()->Status);
        SetExtendedError(errorJson, "Iam", "ResponseMessage", error->Get()->Message);
        SetExtendedError(errorJson, "Iam", "ResponseDetails", error->Get()->Details);
    }

    if (!json.Has(ORIGINAL_USER_TOKEN)) {
        TStringBuf tail;
        if (TStringBuf(AuthHeader).AfterPrefix(IAM_TOKEN_SCHEME, tail)) {
            json[ORIGINAL_USER_TOKEN] = tail;
        }
    }

    PatchResponse(json, errorJson);
    ContinueAndPassAway();
}

} // NMVP::NOIDC

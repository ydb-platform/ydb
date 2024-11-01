#include "object.h"

#include <ydb/services/metadata/manager/ydb_value_operator.h>
#include <ydb/services/metadata/secret/fetcher.h>

#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/uri/uri.h>

namespace NKikimr::NColumnShard::NTiers {

NKikimrSchemeOp::TS3Settings TTierConfig::GetPatchedConfig(
    std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets) const {
    auto config = ProtoConfig;
    if (secrets) {
        if (!secrets->GetSecretValue(GetAccessKey(), *config.MutableAccessKey())) {
            AFL_ERROR(NKikimrServices::TX_TIERING)("error", "cannot_read_access_key")("secret", GetAccessKey().DebugString());
        }
        if (!secrets->GetSecretValue(GetSecretKey(), *config.MutableSecretKey())) {
            AFL_ERROR(NKikimrServices::TX_TIERING)("error", "cannot_read_secret_key")("secret", GetSecretKey().DebugString());
        }
    }
    return config;
}

bool TTierConfig::DeserializeFromProto(const NKikimrSchemeOp::TExternalDataSourceDescription& proto) {
    if (!proto.GetAuth().HasAws()) {
        return false;
    }

    // TODO fix secret owner
    {
        auto makeSecretId = [](const TStringBuf& secret) -> TString {
            return NMetadata::NSecret::TSecretId("a", secret).SerializeToString();   // ... and here
        };
        ProtoConfig.SetSecretKey(makeSecretId(proto.GetAuth().GetAws().GetAwsAccessKeyIdSecretName()));
        ProtoConfig.SetAccessKey(makeSecretId(proto.GetAuth().GetAws().GetAwsSecretAccessKeySecretName()));
    }

    NUri::TUri url;
    if (url.Parse(proto.GetLocation()) != NUri::TState::EParsed::ParsedOK) {
        return false;
    }

    switch (url.GetScheme()) {
        case NUri::TScheme::SchemeEmpty:
            break;
        case NUri::TScheme::SchemeHTTP:
            ProtoConfig.SetScheme(::NKikimrSchemeOp::TS3Settings_EScheme_HTTP);
            break;
        case NUri::TScheme::SchemeHTTPS:
            ProtoConfig.SetScheme(::NKikimrSchemeOp::TS3Settings_EScheme_HTTPS);
            break;
        default:
            return false;
    }

    {
        TStringBuf endpoint;
        TStringBuf bucket;

        TStringBuf host = url.GetHost();
        TStringBuf path = url.GetField(NUri::TField::FieldPath);
        if (path.StartsWith("/") && path.Size() > 1) {
            bucket = path.Skip(1);
            if (bucket.EndsWith("/")) {
                bucket = bucket.Chop(1);
            }
            if (bucket.Contains("/")) {
                return false;
            }
            endpoint = host;
        } else {
            if (!path.TrySplit('.', endpoint, bucket)) {
                return false;
            }
        }

        ProtoConfig.SetEndpoint(TString(endpoint));
        ProtoConfig.SetBucket(TString(bucket));
    }

    return true;
}

NJson::TJsonValue TTierConfig::SerializeConfigToJson() const {
    NJson::TJsonValue result;
    NProtobufJson::Proto2Json(ProtoConfig, result);
    return result;
}

bool TTierConfig::IsSame(const TTierConfig& item) const {
    return ProtoConfig.SerializeAsString() == item.ProtoConfig.SerializeAsString();
}

}

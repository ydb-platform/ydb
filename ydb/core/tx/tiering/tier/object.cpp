#include "object.h"

#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/uri/uri.h>

namespace NKikimr::NColumnShard::NTiers {

TConclusion<NKikimrSchemeOp::TS3Settings> TTierConfig::GetPatchedConfig(
    const std::shared_ptr<NMetadata::NSecret::ISecretAccessor>& secrets) const {
    auto config = ProtoConfig;
    if (secrets) {
        {
            auto secretIdOrValue = NMetadata::NSecret::TSecretIdOrValue::DeserializeFromString(config.GetAccessKey());
            AFL_VERIFY(secretIdOrValue);
            auto value = secrets->GetSecretValue(*secretIdOrValue);
            if (value.IsFail()) {
                return TConclusionStatus::Fail(TStringBuilder() << "Can't read access key: " << value.GetErrorMessage());
            }
            config.SetAccessKey(value.DetachResult());
        }
        {
            auto secretIdOrValue = NMetadata::NSecret::TSecretIdOrValue::DeserializeFromString(config.GetSecretKey());
            AFL_VERIFY(secretIdOrValue);
            auto value = secrets->GetSecretValue(*secretIdOrValue);
            if (value.IsFail()) {
                return TConclusionStatus::Fail(TStringBuilder() << "Can't read secret key: " << value.GetErrorMessage());
            }
            config.SetSecretKey(value.DetachResult());
        }
    }
    return config;
}

TConclusionStatus TTierConfig::DeserializeFromProto(const NKikimrSchemeOp::TExternalDataSourceDescription& proto) {
    if (!proto.GetAuth().HasAws()) {
        return TConclusionStatus::Fail("AWS auth is not defined for storage tier");
    }

    ProtoConfig.SetAccessKey(NMetadata::NSecret::TSecretName(proto.GetAuth().GetAws().GetAwsAccessKeyIdSecretName()).SerializeToString());
    ProtoConfig.SetSecretKey(
        NMetadata::NSecret::TSecretName(proto.GetAuth().GetAws().GetAwsSecretAccessKeySecretName()).SerializeToString());

    NUri::TUri url;
    if (url.Parse(proto.GetLocation(), NUri::TFeature::FeaturesAll) != NUri::TState::EParsed::ParsedOK) {
        return TConclusionStatus::Fail("Cannot parse url: " + proto.GetLocation());
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
            return TConclusionStatus::Fail("Unknown schema in url");
    }

    {
        TStringBuf endpoint;
        TStringBuf bucket;

        TStringBuf host = url.GetHost();
        TStringBuf path = url.GetField(NUri::TField::FieldPath);
        if (!path.Empty()) {
            endpoint = host;
            bucket = path;
            bucket.SkipPrefix("/");
            if (bucket.Contains("/")) {
                return TConclusionStatus::Fail(TStringBuilder() << "Not a bucket (contains directories): " << bucket);
            }
        } else {
            if (!path.TrySplit('.', endpoint, bucket)) {
                return TConclusionStatus::Fail(TStringBuilder() << "Bucket is not specified in URL: " << path);
            }
        }

        ProtoConfig.SetEndpoint(TString(endpoint));
        ProtoConfig.SetBucket(TString(bucket));
    }

    return TConclusionStatus::Success();
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

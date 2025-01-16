#include "object.h"
#include "s3_uri.h"

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

    {
        const auto aws = proto.GetAuth().GetAws();
        ProtoConfig.SetAccessKey(NMetadata::NSecret::TSecretName(aws.GetAwsAccessKeyIdSecretName()).SerializeToString());
        ProtoConfig.SetSecretKey(NMetadata::NSecret::TSecretName(aws.GetAwsSecretAccessKeySecretName()).SerializeToString());
        if (aws.HasAwsRegion()) {
            ProtoConfig.SetRegion(aws.GetAwsRegion());
        }
    }

    auto parsedUri = TS3Uri::ParseUri(proto.GetLocation());
    if (parsedUri.IsFail()) {
        return parsedUri;
    }
    parsedUri->FillSettings(ProtoConfig);

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

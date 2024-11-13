#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/services/metadata/secret/secret.h>

#include <library/cpp/json/writer/json_value.h>

namespace NKikimr::NMetadata::NSecret {
class TSnapshot;
}

namespace NKikimr::NColumnShard::NTiers {

class TTierConfig {
private:
    using TTierProto = NKikimrSchemeOp::TS3Settings;
    YDB_READONLY_DEF(TTierProto, ProtoConfig);
    YDB_READONLY_DEF(NKikimrSchemeOp::TCompressionOptions, Compression);

public:
    TTierConfig() = default;
    TTierConfig(const TTierProto& config, const NKikimrSchemeOp::TCompressionOptions& compression)
        : ProtoConfig(config)
        , Compression(compression) {
    }

    TConclusionStatus DeserializeFromProto(const NKikimrSchemeOp::TExternalDataSourceDescription& proto);

    NMetadata::NSecret::TSecretIdOrValue GetAccessKey() const {
        auto accessKey =
            NMetadata::NSecret::TSecretIdOrValue::DeserializeFromOptional(ProtoConfig.GetSecretableAccessKey(), ProtoConfig.GetAccessKey());
        if (!accessKey) {
            return NMetadata::NSecret::TSecretIdOrValue::BuildEmpty();
        }
        return *accessKey;
    }

    NMetadata::NSecret::TSecretIdOrValue GetSecretKey() const {
        auto secretKey =
            NMetadata::NSecret::TSecretIdOrValue::DeserializeFromOptional(ProtoConfig.GetSecretableSecretKey(), ProtoConfig.GetSecretKey());
        if (!secretKey) {
            return NMetadata::NSecret::TSecretIdOrValue::BuildEmpty();
        }
        return *secretKey;
    }

    NJson::TJsonValue SerializeConfigToJson() const;

    NKikimrSchemeOp::TS3Settings GetPatchedConfig(std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets) const;

    bool IsSame(const TTierConfig& item) const;
    NJson::TJsonValue GetDebugJson() const;
};
}

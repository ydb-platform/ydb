#pragma once
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/status.h>
#include <ydb/services/metadata/secret/accessor/secret_id.h>
#include <ydb/services/metadata/secret/accessor/snapshot.h>

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

    NJson::TJsonValue SerializeConfigToJson() const;

    TConclusion<NKikimrSchemeOp::TS3Settings> GetPatchedConfig(const std::shared_ptr<NMetadata::NSecret::ISecretAccessor>& secrets) const;

    bool IsSame(const TTierConfig& item) const;
    NJson::TJsonValue GetDebugJson() const;
};
}

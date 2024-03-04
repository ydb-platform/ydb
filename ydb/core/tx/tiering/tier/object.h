#pragma once
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/services/metadata/abstract/decoder.h>
#include <ydb/services/metadata/manager/preparation_controller.h>
#include <ydb/services/metadata/manager/table_record.h>
#include <ydb/services/metadata/manager/object.h>
#include <ydb/services/metadata/service.h>
#include <ydb/services/metadata/secret/secret.h>

#include <library/cpp/json/writer/json_value.h>

namespace NKikimr::NMetadata::NSecret {
class TSnapshot;
}

namespace NKikimr::NColumnShard::NTiers {

class TTierConfig: public NMetadata::NModifications::TObject<TTierConfig> {
private:
    using TTierProto = NKikimrSchemeOp::TStorageTierConfig;
    YDB_ACCESSOR_DEF(TString, TierName);
    TTierProto ProtoConfig;
public:

    TTierConfig() = default;
    TTierConfig(const TString& tierName)
        : TierName(tierName) {

    }

    TTierConfig(const TString& tierName, const TTierProto& config)
        : TierName(tierName)
        , ProtoConfig(config)
    {

    }

    const NKikimrSchemeOp::TCompressionOptions& GetCompression() const {
        return ProtoConfig.GetCompression();
    }

    NMetadata::NSecret::TSecretIdOrValue GetAccessKey() const {
        auto accessKey = NMetadata::NSecret::TSecretIdOrValue::DeserializeFromOptional(ProtoConfig.GetObjectStorage().GetSecretableAccessKey(), ProtoConfig.GetObjectStorage().GetAccessKey());
        if (!accessKey) {
            return NMetadata::NSecret::TSecretIdOrValue::BuildEmpty();
        }
        return *accessKey;
    }

    NMetadata::NSecret::TSecretIdOrValue GetSecretKey() const {
        auto secretKey = NMetadata::NSecret::TSecretIdOrValue::DeserializeFromOptional(ProtoConfig.GetObjectStorage().GetSecretableSecretKey(), ProtoConfig.GetObjectStorage().GetSecretKey());
        if (!secretKey) {
            return NMetadata::NSecret::TSecretIdOrValue::BuildEmpty();
        }
        return *secretKey;
    }

    NJson::TJsonValue SerializeConfigToJson() const;


    static NMetadata::IClassBehaviour::TPtr GetBehaviour();
    NKikimrSchemeOp::TS3Settings GetPatchedConfig(std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets) const;

    class TDecoder: public NMetadata::NInternal::TDecoderBase {
    private:
        YDB_READONLY(i32, TierNameIdx, -1);
        YDB_READONLY(i32, TierConfigIdx, -1);
    public:
        static inline const TString TierName = "tierName";
        static inline const TString TierConfig = "tierConfig";
        TDecoder(const Ydb::ResultSet& rawData) {
            TierNameIdx = GetFieldIndex(rawData, TierName);
            TierConfigIdx = GetFieldIndex(rawData, TierConfig);
        }
    };
    bool DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& r);
    NMetadata::NInternal::TTableRecord SerializeToRecord() const;

    bool IsSame(const TTierConfig& item) const;
    NJson::TJsonValue GetDebugJson() const;
    static TString GetTypeId() {
        return "TIER";
    }
};

}

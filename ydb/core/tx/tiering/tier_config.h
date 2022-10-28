#pragma once
#include "decoder.h"
#include <ydb/services/metadata/service.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <library/cpp/json/writer/json_value.h>

namespace NKikimr::NColumnShard::NTiers {

class TTierConfig {
private:
    using TTierProto = NKikimrSchemeOp::TStorageTierConfig;
    using TS3SettingsProto = NKikimrSchemeOp::TS3Settings;
    YDB_ACCESSOR_DEF(TString, OwnerPath);
    YDB_ACCESSOR_DEF(TString, TierName);
    YDB_ACCESSOR_DEF(TTierProto, ProtoConfig);

public:
    TTierConfig() = default;
    TTierConfig(const TString& ownerPath, const TString& tierName)
        : OwnerPath(ownerPath)
        , TierName(tierName)
    {

    }

    class TDecoder: public NInternal::TDecoderBase {
    private:
        YDB_READONLY(i32, OwnerPathIdx, -1);
        YDB_READONLY(i32, TierNameIdx, -1);
        YDB_READONLY(i32, TierConfigIdx, -1);
    public:
        static inline const TString OwnerPath = "ownerPath";
        static inline const TString TierName = "tierName";
        static inline const TString TierConfig = "tierConfig";
        TDecoder(const Ydb::ResultSet& rawData) {
            OwnerPathIdx = GetFieldIndex(rawData, OwnerPath);
            TierNameIdx = GetFieldIndex(rawData, TierName);
            TierConfigIdx = GetFieldIndex(rawData, TierConfig);
        }
    };
    bool DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& r);

    TString GetConfigId() const {
        return OwnerPath + "." + TierName;
    }

    bool NeedExport() const {
        return ProtoConfig.HasObjectStorage();
    }
    bool IsSame(const TTierConfig& item) const;
    NJson::TJsonValue GetDebugJson() const;
};

}

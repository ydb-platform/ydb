#pragma once
#include <ydb/services/metadata/service.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <library/cpp/json/writer/json_value.h>

namespace NKikimr::NColumnShard::NTiers {

class TConfig {
private:
    using TTierProto = NKikimrSchemeOp::TStorageTierConfig;
    using TS3SettingsProto = NKikimrSchemeOp::TS3Settings;
    YDB_READONLY_DEF(TString, OwnerPath);
    YDB_READONLY_DEF(TString, TierName);
    YDB_READONLY_DEF(TTierProto, ProtoConfig);
public:
    TConfig() = default;
    TConfig(const TString& ownerPath, const TString& tierName)
        : OwnerPath(ownerPath)
        , TierName(tierName)
    {

    }

    TString GetConfigId() const {
        return OwnerPath + "." + TierName;
    }

    bool NeedExport() const {
        return ProtoConfig.HasObjectStorage();
    }
    bool IsSame(const TConfig& item) const;
    bool DeserializeFromString(const TString& configProtoStr);
    NJson::TJsonValue SerializeToJson() const;
};

class TConfigsSnapshot: public NMetadataProvider::ISnapshot {
private:
    using TBase = NMetadataProvider::ISnapshot;
    using TConfigsMap = TMap<TString, TConfig>;
    YDB_READONLY_DEF(TConfigsMap, Data);
protected:
    virtual bool DoDeserializeFromResultSet(const Ydb::ResultSet& rawData) override;
    virtual TString DoSerializeToString() const override;
public:
    std::optional<TConfig> GetValue(const TString& key) const;
    using TBase::TBase;
};

class TSnapshotConstructor: public NMetadataProvider::TGenericSnapshotParser<TConfigsSnapshot> {
private:
    const TString TablePath;
protected:
    virtual TVector<NMetadataProvider::ITableModifier::TPtr> DoGetTableSchema() const override;
    virtual const TString& DoGetTablePath() const override {
        return TablePath;
    }
public:
    TSnapshotConstructor(const TString& tablePath)
        : TablePath(tablePath)
    {

    }
};

}

#include "external_data.h"
#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/protobuf/json/proto2json.h>

namespace NKikimr::NColumnShard::NTiers {

bool TConfigsSnapshot::DoDeserializeFromResultSet(const Ydb::ResultSet& rawData) {
    const i32 ownerPathIdx = GetFieldIndex(rawData, "ownerPath");
    const i32 tierNameIdx = GetFieldIndex(rawData, "tierName");
    const i32 tierConfigIdx = GetFieldIndex(rawData, "tierConfig");
    if (tierNameIdx < 0 || tierConfigIdx < 0 || ownerPathIdx < 0) {
        ALS_ERROR(NKikimrServices::TX_COLUMNSHARD) << "incorrect tiers config table structure";
        return false;
    }
    for (auto&& r : rawData.rows()) {
        TConfig config(r.items()[ownerPathIdx].bytes_value(), r.items()[tierNameIdx].bytes_value());
        if (!config.DeserializeFromString(r.items()[tierConfigIdx].bytes_value())) {
            ALS_ERROR(NKikimrServices::TX_COLUMNSHARD) << "cannot parse tier config from snapshot";
            return false;
        }
        if (!Data.emplace(config.GetConfigId(), config).second) {
            ALS_ERROR(NKikimrServices::TX_COLUMNSHARD) << "tier names duplication: " << config.GetTierName();
            return false;
        }
    }
    return true;
}

std::optional<TConfig> TConfigsSnapshot::GetValue(const TString& key) const {
    auto it = Data.find(key);
    if (it == Data.end()) {
        return {};
    } else {
        return it->second;
    }
}

TVector<NMetadataProvider::ITableModifier::TPtr> TSnapshotConstructor::DoGetTableSchema() const {
    Ydb::Table::CreateTableRequest request;
    request.set_session_id("");
    request.set_path(TablePath);
    request.add_primary_key("ownerPath");
    request.add_primary_key("tierName");
    {
        auto& column = *request.add_columns();
        column.set_name("tierName");
        column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
    }
    {
        auto& column = *request.add_columns();
        column.set_name("ownerPath");
        column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
    }
    {
        auto& column = *request.add_columns();
        column.set_name("tierConfig");
        column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
    }
    NMetadataProvider::ITableModifier::TPtr result(
        new NMetadataProvider::TGenericTableModifier<NInternal::NRequest::TDialogCreateTable>(request));
    return { result };
}

TString NTiers::TConfigsSnapshot::DoSerializeToString() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    for (auto&& i : Data) {
        result.InsertValue(i.first, i.second.SerializeToJson());
    }
    return result.GetStringRobust();
}

bool NTiers::TConfig::DeserializeFromString(const TString& configProtoStr) {
    if (!::google::protobuf::TextFormat::ParseFromString(configProtoStr, &ProtoConfig)) {
        ALS_ERROR(NKikimrServices::TX_COLUMNSHARD) << "cannot parse proto string: " << configProtoStr;
        return false;
    }
    return true;
}

NJson::TJsonValue TConfig::SerializeToJson() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    result.InsertValue("tierName", TierName);
    NProtobufJson::Proto2Json(ProtoConfig, result.InsertValue("tierConfig", NJson::JSON_MAP));
    return result;
}

bool TConfig::IsSame(const TConfig& item) const {
    return TierName == item.TierName && ProtoConfig.SerializeAsString() == item.ProtoConfig.SerializeAsString();
}

}

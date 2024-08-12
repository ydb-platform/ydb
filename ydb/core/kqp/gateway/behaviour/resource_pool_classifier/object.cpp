#include "object.h"
#include "behaviour.h"

#include <library/cpp/json/json_reader.h>


namespace NKikimr::NKqp {

using namespace NResourcePool;


//// TResourcePoolClassifierConfig::TDecoder

TResourcePoolClassifierConfig::TDecoder::TDecoder(const Ydb::ResultSet& rawData)
    : DatabaseIdx(GetFieldIndex(rawData, Database))
    , NameIdx(GetFieldIndex(rawData, Name))
    , RankIdx(GetFieldIndex(rawData, Rank))
    , ConfigJsonIdx(GetFieldIndex(rawData, ConfigJson))
{}

//// TResourcePoolClassifierConfig

bool TResourcePoolClassifierConfig::DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& rawData) {
    if (!decoder.Read(decoder.GetDatabaseIdx(), Database, rawData)) {
        return false;
    }
    if (!decoder.Read(decoder.GetNameIdx(), Name, rawData)) {
        return false;
    }
    if (!decoder.Read(decoder.GetRankIdx(), Rank, rawData)) {
        Rank = -1;
    }

    TString configJsonString;
    if (!decoder.Read(decoder.GetConfigJsonIdx(), configJsonString, rawData)) {
        return false;
    }
    if (!NJson::ReadJsonTree(configJsonString, &ConfigJson)) {
        return false;
    }

    return true;
}

NMetadata::NInternal::TTableRecord TResourcePoolClassifierConfig::SerializeToRecord() const {
    NMetadata::NInternal::TTableRecord result;
    result.SetColumn(TDecoder::Database, NMetadata::NInternal::TYDBValue::Utf8(Database));
    result.SetColumn(TDecoder::Name, NMetadata::NInternal::TYDBValue::Utf8(Name));
    result.SetColumn(TDecoder::Rank, NMetadata::NInternal::TYDBValue::Int64(Rank));

    NJsonWriter::TBuf writer;
    writer.WriteJsonValue(&ConfigJson);
    result.SetColumn(TDecoder::ConfigJson, NMetadata::NInternal::TYDBValue::Utf8(writer.Str()));

    return result;
}

TClassifierSettings TResourcePoolClassifierConfig::GetClassifierSettings() const {
    TClassifierSettings resourcePoolClassifierSettings;

    resourcePoolClassifierSettings.Rank = Rank;

    const auto& properties = resourcePoolClassifierSettings.GetPropertiesMap();
    for (const auto& [propery, value] : ConfigJson.GetMap()) {
        const auto it = properties.find(propery);
        if (it == properties.end()) {
            continue;
        }
        try {
            std::visit(TClassifierSettings::TParser{value.GetString()}, it->second);
        } catch (...) {
            continue;
        }
    }

    return resourcePoolClassifierSettings;
}

NJson::TJsonValue TResourcePoolClassifierConfig::GetDebugJson() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    result.InsertValue(TDecoder::Database, Database);
    result.InsertValue(TDecoder::Name, Name);
    result.InsertValue(TDecoder::Rank, Rank);
    result.InsertValue(TDecoder::ConfigJson, ConfigJson);
    return result;
}

bool TResourcePoolClassifierConfig::operator==(const TResourcePoolClassifierConfig& other) const {
    return std::tie(Database, Name, Rank, ConfigJson) != std::tie(other.Database, other.Name, other.Rank, other.ConfigJson);
}

NMetadata::IClassBehaviour::TPtr TResourcePoolClassifierConfig::GetBehaviour() {
    return TResourcePoolClassifierBehaviour::GetInstance();
}

TString TResourcePoolClassifierConfig::GetTypeId() {
    return "RESOURCE_POOL_CLASSIFIER";
}

NMetadata::NModifications::NColumnMerger::TMerger TResourcePoolClassifierConfig::MergerFactory(const TString& columnName) {
    if (columnName == TDecoder::ConfigJson) {
        return &JsonConfigsMerger;
    }
    return TBase::MergerFactory(columnName);
}

bool TResourcePoolClassifierConfig::JsonConfigsMerger(Ydb::Value& self, const Ydb::Value& other) {
    NJson::TJsonValue selfConfigJson;
    if (!NJson::ReadJsonTree(self.text_value(), &selfConfigJson)) {
        return false;
    }

    NJson::TJsonValue otherConfigJson;
    if (!NJson::ReadJsonTree(other.text_value(), &otherConfigJson)) {
        return false;
    }

    for (const auto& [key, value] : otherConfigJson.GetMap()) {
        selfConfigJson.InsertValue(key, value);
    }

    NJsonWriter::TBuf writer;
    writer.WriteJsonValue(&selfConfigJson);
    *self.mutable_text_value() = writer.Str();

    return true;
}

}  // namespace NKikimr::NKqp

#include "object.h"
#include "behaviour.h"

#include <library/cpp/json/json_reader.h>


namespace NKikimr::NKqp {

namespace {

using namespace NResourcePool;


class TJsonConfigsMerger : public NMetadata::NModifications::IColumnValuesMerger {
public:
    virtual TConclusionStatus Merge(Ydb::Value& value, const Ydb::Value& patch) const override {
        NJson::TJsonValue selfConfigJson;
        if (!NJson::ReadJsonTree(value.text_value(), &selfConfigJson)) {
            return TConclusionStatus::Fail("Failed to parse object json config");
        }

        NJson::TJsonValue otherConfigJson;
        if (!NJson::ReadJsonTree(patch.text_value(), &otherConfigJson)) {
            return TConclusionStatus::Fail("Failed to parse patch json config");
        }

        for (const auto& [key, value] : otherConfigJson.GetMap()) {
            selfConfigJson.InsertValue(key, value);
        }

        NJsonWriter::TBuf writer;
        writer.WriteJsonValue(&selfConfigJson);
        *value.mutable_text_value() = writer.Str();

        return TConclusionStatus::Success();
    }
};

}  // anonymous namespace


//// TResourcePoolClassifierConfig::TDecoder

TResourcePoolClassifierConfig::TDecoder::TDecoder(const Ydb::ResultSet& rawData)
    : DatabaseIdx(GetFieldIndex(rawData, Database))
    , NameIdx(GetFieldIndex(rawData, Name))
    , RankIdx(GetFieldIndex(rawData, Rank))
    , ConfigJsonIdx(GetFieldIndex(rawData, ConfigJson))
{}

//// TResourcePoolClassifierConfig

NMetadata::NModifications::IColumnValuesMerger::TPtr TResourcePoolClassifierConfig::BuildMerger(const TString& columnName) const {
    if (columnName == TDecoder::ConfigJson) {
        return std::make_shared<TJsonConfigsMerger>();
    }
    return TBase::BuildMerger(columnName);
}

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
    return std::tie(Database, Name, Rank, ConfigJson) == std::tie(other.Database, other.Name, other.Rank, other.ConfigJson);
}

NMetadata::IClassBehaviour::TPtr TResourcePoolClassifierConfig::GetBehaviour() {
    return TResourcePoolClassifierBehaviour::GetInstance();
}

TString TResourcePoolClassifierConfig::GetTypeId() {
    return "RESOURCE_POOL_CLASSIFIER";
}

}  // namespace NKikimr::NKqp

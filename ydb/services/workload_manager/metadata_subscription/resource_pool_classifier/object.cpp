#include "object.h"
#include "behaviour.h"

#include <library/cpp/json/json_reader.h>

#include <ydb/services/metadata/abstract/request_features.h>

#include <util/generic/serialized_enum.h>
#include <util/generic/yexception.h>
#include <util/string/cast.h>


namespace NKikimr::NWorkloadManager {

namespace {

using namespace NResourcePool;

void ResetOptionalConfigField(
    NJson::TJsonValue& configPatch,
    const TString& key,
    bool& hasConfigPatch)
{
    configPatch.InsertValue(key, NJson::TJsonValue{NJson::JSON_NULL});
    hasConfigPatch = true;
}

template <typename T, typename TSetNonEmpty>
std::optional<TString> ParseOptionalFeature(
    NYql::TFeaturesExtractor& featuresExtractor,
    const TString& key,
    std::optional<T>& field,
    NJson::TJsonValue& configPatch,
    bool& hasConfigPatch,
    TSetNonEmpty&& setNonEmpty)
{
    if (std::optional<TString> value = featuresExtractor.Extract(key)) {
        if (!value->empty()) {
            try {
                setNonEmpty(*value, field, configPatch);
                hasConfigPatch = true;
            } catch (const yexception& error) {
                return TStringBuilder() << "Failed to parse property " << key << ": " << error.what();
            }
        } else {
            field.reset();
            ResetOptionalConfigField(configPatch, key, hasConfigPatch);
        }
        return std::nullopt;
    }
    if (featuresExtractor.ExtractResetFeature(key)) {
        field.reset();
        ResetOptionalConfigField(configPatch, key, hasConfigPatch);
    }
    return std::nullopt;
}

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

        for (const auto& [key, patchValue] : otherConfigJson.GetMap()) {
            if (patchValue.GetType() == NJson::JSON_NULL) {
                selfConfigJson.EraseValue(key);
            } else {
                selfConfigJson.InsertValue(key, patchValue);
            }
        }

        if (selfConfigJson.Has("action")) {
            EClassifierAction action;
            if (TryFromString(to_lower(selfConfigJson["action"].GetStringRobust()), action)
                && action == EClassifierAction::Reject)
            {
                selfConfigJson.EraseValue("resource_pool");
            }
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

//// Serialize/Deserialize helpers

NJson::TJsonValue TResourcePoolClassifierConfig::SerializeToJson(const NResourcePool::TClassifierSettings& settings) {
    NJson::TJsonValue json = NJson::JSON_MAP;
    const bool isReject = settings.Action
        && *settings.Action == NResourcePool::EClassifierAction::Reject;
    if (settings.ResourcePool && !isReject) {
        json.InsertValue(JSON_KEY_RESOURCE_POOL, *settings.ResourcePool);
    }
    if (settings.MemberName) {
        json.InsertValue(JSON_KEY_MEMBER_NAME, *settings.MemberName);
    }
    if (settings.HasAppName) {
        json.InsertValue(JSON_KEY_HAS_APP_NAME, *settings.HasAppName);
    }
    if (settings.HasFullScan) {
        json.InsertValue(JSON_KEY_HAS_FULL_SCAN, settings.HasFullScan->Pattern);
    }
    if (settings.HasPath) {
        json.InsertValue(JSON_KEY_HAS_PATH, settings.HasPath->Pattern);
    }
    if (settings.HasStream) {
        json.InsertValue(JSON_KEY_HAS_STREAM, *settings.HasStream);
    }
    if (isReject) {
        json.InsertValue(JSON_KEY_ACTION, ToString(NResourcePool::EClassifierAction::Reject));
    }
    return json;
}

NResourcePool::TClassifierSettings TResourcePoolClassifierConfig::DeserializeFromJson(const NJson::TJsonValue& json, i64 rank) {
    NResourcePool::TClassifierSettings settings;
    settings.Rank = rank;
    const auto& properties = json.GetMap();

    // Empty strings are treated as absent for compatibility with legacy configs
    // that stored "" instead of omitting the key / writing JSON null.
    // Malformed individual fields are skipped (same as the old EnsureSettings path)
    // so a single bad property does not drop the whole classifier from the snapshot.
    auto readNonEmptyString = [&](const TString& key) -> std::optional<TString> {
        if (!properties.contains(key)) {
            return std::nullopt;
        }
        const TString value = properties.at(key).GetStringRobust();
        if (value.empty()) {
            return std::nullopt;
        }
        return value;
    };

    if (auto value = readNonEmptyString(JSON_KEY_RESOURCE_POOL)) {
        settings.ResourcePool = *value;
    }
    if (auto value = readNonEmptyString(JSON_KEY_MEMBER_NAME)) {
        settings.MemberName = *value;
    }
    if (auto value = readNonEmptyString(JSON_KEY_HAS_APP_NAME)) {
        settings.HasAppName = *value;
    }
    if (auto value = readNonEmptyString(JSON_KEY_HAS_FULL_SCAN)) {
        try {
            settings.HasFullScan = TRegexPredicate::FromGlob(*value);
        } catch (const yexception&) {
            // skip malformed glob
        }
    }
    if (auto value = readNonEmptyString(JSON_KEY_HAS_PATH)) {
        try {
            settings.HasPath = TRegexPredicate::FromGlob(*value);
        } catch (const yexception&) {
            // skip malformed glob
        }
    }
    if (properties.contains(JSON_KEY_HAS_STREAM)) {
        // Accept both JSON boolean and string forms ("true"/"false") for robustness.
        const TString value = properties.at(JSON_KEY_HAS_STREAM).GetStringRobust();
        if (!value.empty()) {
            try {
                settings.HasStream = FromString<bool>(value);
            } catch (const yexception&) {
                // skip malformed value
            }
        }
    }
    if (auto value = readNonEmptyString(JSON_KEY_ACTION)) {
        NResourcePool::EClassifierAction parsed;
        if (TryFromString(to_lower(*value), parsed)) {
            settings.Action = parsed;
        }
        // Invalid action values are skipped rather than failing the whole object.
    }
    if (settings.Action == NResourcePool::EClassifierAction::Reject) {
        settings.ResourcePool.reset();
    }
    return settings;
}

//// ParseFromFeaturesExtractor

std::optional<TString> TResourcePoolClassifierConfig::ParseFromFeaturesExtractor(
    NYql::TFeaturesExtractor& featuresExtractor,
    TParseResult* result)
{
    auto& settings = result->Settings;
    auto& configPatch = result->ConfigPatch;

    // Parse rank
    if (std::optional<TString> value = featuresExtractor.Extract(JSON_KEY_RANK)) {
        try {
            settings.Rank = FromString<i64>(*value);
            if (settings.Rank < -1) {
                return TStringBuilder() << "Failed to parse property " << JSON_KEY_RANK
                    << ": Invalid integer value " << settings.Rank << ", it is should be greater or equal -1";
            }
            result->HasRank = true;
        } catch (const yexception& error) {
            return TStringBuilder() << "Failed to parse property " << JSON_KEY_RANK << ": " << error.what();
        }
    } else if (featuresExtractor.ExtractResetFeature(JSON_KEY_RANK)) {
        return "Cannot reset property rank";
    }

    // Parse resource_pool
    if (std::optional<TString> value = featuresExtractor.Extract(JSON_KEY_RESOURCE_POOL)) {
        if (value->empty()) {
            return TStringBuilder() << "Failed to parse property " << JSON_KEY_RESOURCE_POOL
                << ": resource pool name must not be empty";
        }
        settings.ResourcePool = *value;
        configPatch.InsertValue(JSON_KEY_RESOURCE_POOL, *value);
        result->HasResourcePool = true;
        result->HasConfigPatch = true;
    } else if (featuresExtractor.ExtractResetFeature(JSON_KEY_RESOURCE_POOL)) {
        return "Cannot reset required property resource_pool";
    }

    // Parse member_name
    if (auto error = ParseOptionalFeature<TString>(
            featuresExtractor, JSON_KEY_MEMBER_NAME, settings.MemberName, configPatch, result->HasConfigPatch,
            [&](const TString& value, std::optional<TString>& field, NJson::TJsonValue& patch) {
                field = value;
                patch.InsertValue(JSON_KEY_MEMBER_NAME, value);
            }))
    {
        return error;
    }

    // Parse has_app_name
    if (auto error = ParseOptionalFeature<TString>(
            featuresExtractor, JSON_KEY_HAS_APP_NAME, settings.HasAppName, configPatch, result->HasConfigPatch,
            [&](const TString& value, std::optional<TString>& field, NJson::TJsonValue& patch) {
                field = value;
                patch.InsertValue(JSON_KEY_HAS_APP_NAME, value);
            }))
    {
        return error;
    }

    // Parse has_full_scan
    if (auto error = ParseOptionalFeature<TRegexPredicate>(
            featuresExtractor, JSON_KEY_HAS_FULL_SCAN, settings.HasFullScan, configPatch, result->HasConfigPatch,
            [&](const TString& value, std::optional<TRegexPredicate>& field, NJson::TJsonValue& patch) {
                field = TRegexPredicate::FromGlob(value);
                patch.InsertValue(JSON_KEY_HAS_FULL_SCAN, value);
            }))
    {
        return error;
    }

    // Parse has_path
    if (auto error = ParseOptionalFeature<TRegexPredicate>(
            featuresExtractor, JSON_KEY_HAS_PATH, settings.HasPath, configPatch, result->HasConfigPatch,
            [&](const TString& value, std::optional<TRegexPredicate>& field, NJson::TJsonValue& patch) {
                field = TRegexPredicate::FromGlob(value);
                patch.InsertValue(JSON_KEY_HAS_PATH, value);
            }))
    {
        return error;
    }

    // Parse has_stream
    if (auto error = ParseOptionalFeature<bool>(
            featuresExtractor, JSON_KEY_HAS_STREAM, settings.HasStream, configPatch, result->HasConfigPatch,
            [&](const TString& value, std::optional<bool>& field, NJson::TJsonValue& patch) {
                field = FromString<bool>(value);
                patch.InsertValue(JSON_KEY_HAS_STREAM, *field);
            }))
    {
        return error;
    }

    // Parse action
    if (std::optional<TString> value = featuresExtractor.Extract(JSON_KEY_ACTION)) {
        if (!value->empty()) {
            NResourcePool::EClassifierAction parsed;
            if (!TryFromString(to_lower(*value), parsed)) {
                return TStringBuilder() << "Failed to parse property " << JSON_KEY_ACTION
                    << ": Invalid action '" << *value
                    << "', supported values: " << GetEnumAllNames<NResourcePool::EClassifierAction>();
            }
            settings.Action = parsed;
            configPatch.InsertValue(JSON_KEY_ACTION, ToString(parsed));
            result->HasConfigPatch = true;
        } else {
            settings.Action.reset();
            ResetOptionalConfigField(configPatch, JSON_KEY_ACTION, result->HasConfigPatch);
        }
    } else if (featuresExtractor.ExtractResetFeature(JSON_KEY_ACTION)) {
        settings.Action.reset();
        ResetOptionalConfigField(configPatch, JSON_KEY_ACTION, result->HasConfigPatch);
    }

    return std::nullopt;
}

//// TResourcePoolClassifierConfig

bool TResourcePoolClassifierConfig::DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& rawData) {
    if (!decoder.Read(decoder.GetDatabaseIdx(), Database, rawData)) {
        return false;
    }
    if (!decoder.Read(decoder.GetNameIdx(), Name, rawData)) {
        return false;
    }
    i64 rank;
    if (!decoder.Read(decoder.GetRankIdx(), rank, rawData)) {
        rank = -1;
    }

    TString configJsonString;
    if (decoder.Read(decoder.GetConfigJsonIdx(), configJsonString, rawData)) {
        NJson::TJsonValue configJson;
        if (!NJson::ReadJsonTree(configJsonString, &configJson)) {
            return false;
        }
        Settings = DeserializeFromJson(configJson, rank);
    } else {
        // Config column is optional (e.g., for ALTER that only changes RANK).
        // Use default settings with the rank from the record.
        Settings.Rank = rank;
    }
    return true;
}

NMetadata::NInternal::TTableRecord TResourcePoolClassifierConfig::SerializeToRecord() const {
    NMetadata::NInternal::TTableRecord result;
    result.SetColumn(TDecoder::Database, NMetadata::NInternal::TYDBValue::Utf8(Database));
    result.SetColumn(TDecoder::Name, NMetadata::NInternal::TYDBValue::Utf8(Name));
    result.SetColumn(TDecoder::Rank, NMetadata::NInternal::TYDBValue::Int64(Settings.Rank));

    NJson::TJsonValue json = SerializeToJson(Settings);
    NJsonWriter::TBuf writer;
    writer.WriteJsonValue(&json);
    result.SetColumn(TDecoder::ConfigJson, NMetadata::NInternal::TYDBValue::Utf8(writer.Str()));

    return result;
}

NJson::TJsonValue TResourcePoolClassifierConfig::GetDebugJson() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    result.InsertValue(TDecoder::Database, Database);
    result.InsertValue(TDecoder::Name, Name);
    result.InsertValue(TDecoder::Rank, Settings.Rank);
    result.InsertValue(TDecoder::ConfigJson, SerializeToJson(Settings));
    return result;
}

NMetadata::IClassBehaviour::TPtr TResourcePoolClassifierConfig::GetBehaviour() {
    return TResourcePoolClassifierBehaviour::GetInstance();
}

TString TResourcePoolClassifierConfig::GetTypeId() {
    return "RESOURCE_POOL_CLASSIFIER";
}

}  // namespace NKikimr::NWorkloadManager

#include "object.h"
#include <ydb/services/metadata/manager/ydb_value_operator.h>

namespace NKikimr::NColumnShard::NTiers {

NJson::TJsonValue TTieringRule::GetDebugJson() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    result.InsertValue(TDecoder::TieringRuleId, TieringRuleId);
    result.InsertValue(TDecoder::DefaultColumn, DefaultColumn);
    result.InsertValue(TDecoder::Description, SerializeDescriptionToJson());
    return result;
}

TString TTieringRule::GetStorageTablePath() {
    return "/" + AppData()->TenantName + "/.metadata/tiering/rules";
}

void TTieringRule::AlteringPreparation(std::vector<TTieringRule>&& objects,
    NMetadataManager::IAlterPreparationController<TTieringRule>::TPtr controller,
    const NMetadata::IOperationsManager::TModificationContext& /*context*/)
{
    controller->PreparationFinished(std::move(objects));
}

NJson::TJsonValue TTieringRule::SerializeDescriptionToJson() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    auto& jsonRules = result.InsertValue("rules", NJson::JSON_ARRAY);
    for (auto&& i : Intervals) {
        jsonRules.AppendValue(i.SerializeToJson());
    }
    return result;
}

bool TTieringRule::DeserializeDescriptionFromJson(const NJson::TJsonValue & jsonInfo) {
    const NJson::TJsonValue::TArray* rules;
    if (!jsonInfo["rules"].GetArrayPointer(&rules)) {
        return false;
    }
    for (auto&& i : *rules) {
        TTieringInterval interval;
        if (!interval.DeserializeFromJson(i)) {
            return false;
        }
        Intervals.emplace_back(std::move(interval));
    }
    std::sort(Intervals.begin(), Intervals.end());
    return true;
}

NKikimr::NMetadataManager::TTableRecord TTieringRule::SerializeToRecord() const {
    NMetadataManager::TTableRecord result;
    result.SetColumn(TDecoder::TieringRuleId, NMetadataManager::TYDBValue::Bytes(TieringRuleId));
    result.SetColumn(TDecoder::DefaultColumn, NMetadataManager::TYDBValue::Bytes(DefaultColumn));
    {
        auto jsonDescription = SerializeDescriptionToJson();
        NJsonWriter::TBuf sout;
        sout.WriteJsonValue(&jsonDescription, true);
        result.SetColumn(TDecoder::Description, NMetadataManager::TYDBValue::Bytes(sout.Str()));
    }
    return result;
}

bool TTieringRule::DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& r) {
    if (!decoder.Read(decoder.GetTieringRuleIdIdx(), TieringRuleId, r)) {
        return false;
    }
    if (!decoder.Read(decoder.GetDefaultColumnIdx(), DefaultColumn, r)) {
        return false;
    }
    NJson::TJsonValue jsonDescription;
    if (!decoder.ReadJson(decoder.GetDescriptionIdx(), jsonDescription, r)) {
        return false;
    }
    if (!DeserializeDescriptionFromJson(jsonDescription)) {
        return false;
    }
    return true;
}

NMetadata::TOperationParsingResult TTieringRule::BuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings,
    const NMetadata::IOperationsManager::TModificationContext& /*context*/) {
    NKikimr::NMetadataManager::TTableRecord result;
    result.SetColumn(TDecoder::TieringRuleId, NMetadataManager::TYDBValue::Bytes(settings.GetObjectId()));
    {
        auto it = settings.GetFeatures().find(TDecoder::DefaultColumn);
        if (it != settings.GetFeatures().end()) {
            result.SetColumn(TDecoder::DefaultColumn, NMetadataManager::TYDBValue::Bytes(it->second));
        }
    }
    {
        auto it = settings.GetFeatures().find(TDecoder::Description);
        if (it != settings.GetFeatures().end()) {
            result.SetColumn(TDecoder::Description, NMetadataManager::TYDBValue::Bytes(it->second));
        }
    }
    return result;
}

NKikimr::NOlap::TTiersInfo TTieringRule::BuildTiersInfo() const {
    NOlap::TTiersInfo result(GetDefaultColumn());
    for (auto&& r : Intervals) {
        result.AddTier(r.GetTierName(), Now() - r.GetDurationForEvict());
    }
    return result;
}

std::vector<Ydb::Column> TTieringRule::TDecoder::GetPKColumns() {
    return
    {
        NMetadataManager::TYDBColumn::Bytes(TieringRuleId)
    };
}

std::vector<Ydb::Column> TTieringRule::TDecoder::GetColumns() {
    return
    {
        NMetadataManager::TYDBColumn::Bytes(TieringRuleId),
        NMetadataManager::TYDBColumn::Bytes(DefaultColumn),
        NMetadataManager::TYDBColumn::Bytes(Description)
    };
}

std::vector<TString> TTieringRule::TDecoder::GetPKColumnIds() {
    return { TieringRuleId };
}

}

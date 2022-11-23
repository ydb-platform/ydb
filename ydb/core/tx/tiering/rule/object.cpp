#include "object.h"
#include <ydb/services/metadata/manager/ydb_value_operator.h>

namespace NKikimr::NColumnShard::NTiers {
NJson::TJsonValue TTieringRule::GetDebugJson() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    result.InsertValue(TDecoder::TieringRuleId, TieringRuleId);
    result.InsertValue(TDecoder::TierName, TierName);
    result.InsertValue(TDecoder::TablePath, TablePath);
    result.InsertValue("tablePathId", TablePathId);
    result.InsertValue(TDecoder::Column, Column);
    result.InsertValue(TDecoder::DurationForEvict, DurationForEvict.ToString());
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

NKikimr::NMetadataManager::TTableRecord TTieringRule::SerializeToRecord() const {
    NMetadataManager::TTableRecord result;
    result.SetColumn(TDecoder::TieringRuleId, NMetadataManager::TYDBValue::Bytes(TieringRuleId));
    result.SetColumn(TDecoder::OwnerPath, NMetadataManager::TYDBValue::Bytes(OwnerPath));
    result.SetColumn(TDecoder::TierName, NMetadataManager::TYDBValue::Bytes(TierName));
    result.SetColumn(TDecoder::TablePath, NMetadataManager::TYDBValue::Bytes(TablePath));
    result.SetColumn(TDecoder::DurationForEvict, NMetadataManager::TYDBValue::Bytes(DurationForEvict.ToString()));
    result.SetColumn(TDecoder::Column, NMetadataManager::TYDBValue::Bytes(Column));
    return result;
}

bool TTieringRule::DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& r) {
    if (!decoder.Read(decoder.GetTieringRuleIdIdx(), TieringRuleId, r)) {
        return false;
    }
    if (!decoder.Read(decoder.GetOwnerPathIdx(), OwnerPath, r)) {
        return false;
    }
    OwnerPath = TFsPath(OwnerPath).Fix().GetPath();
    if (!decoder.Read(decoder.GetTierNameIdx(), TierName, r)) {
        return false;
    }
    if (!decoder.Read(decoder.GetTablePathIdx(), TablePath, r)) {
        return false;
    }
    TablePath = TFsPath(TablePath).Fix().GetPath();
    if (!decoder.Read(decoder.GetDurationForEvictIdx(), DurationForEvict, r)) {
        return false;
    }
    if (!decoder.Read(decoder.GetColumnIdx(), Column, r)) {
        return false;
    }
    return true;
}

NMetadata::TOperationParsingResult TTieringRule::BuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings,
    const NMetadata::IOperationsManager::TModificationContext& /*context*/) {
    NKikimr::NMetadataManager::TTableRecord result;
    result.SetColumn(TDecoder::TieringRuleId, NMetadataManager::TYDBValue::Bytes(settings.GetObjectId()));
    {
        auto it = settings.GetFeatures().find(TDecoder::TablePath);
        if (it != settings.GetFeatures().end()) {
            result.SetColumn(TDecoder::TablePath, NMetadataManager::TYDBValue::Bytes(it->second));
        }
    }
    {
        auto it = settings.GetFeatures().find(TDecoder::TierName);
        if (it != settings.GetFeatures().end()) {
            result.SetColumn(TDecoder::TierName, NMetadataManager::TYDBValue::Bytes(it->second));
        }
    }
    {
        auto it = settings.GetFeatures().find(TDecoder::OwnerPath);
        if (it != settings.GetFeatures().end()) {
            result.SetColumn(TDecoder::OwnerPath, NMetadataManager::TYDBValue::Bytes(it->second));
        }
    }
    {
        auto it = settings.GetFeatures().find(TDecoder::DurationForEvict);
        if (it != settings.GetFeatures().end()) {
            result.SetColumn(TDecoder::DurationForEvict, NMetadataManager::TYDBValue::Bytes(it->second));
        }
    }
    {
        auto it = settings.GetFeatures().find(TDecoder::Column);
        if (it != settings.GetFeatures().end()) {
            result.SetColumn(TDecoder::Column, NMetadataManager::TYDBValue::Bytes(it->second));
        }
    }
    return result;
}

NJson::TJsonValue TTableTiering::GetDebugJson() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    result.InsertValue(TTieringRule::TDecoder::TablePath, TablePath);
    result.InsertValue("tablePathId", TablePathId);
    result.InsertValue(TTieringRule::TDecoder::Column, Column);
    auto&& jsonRules = result.InsertValue("rules", NJson::JSON_ARRAY);
    for (auto&& i : Rules) {
        jsonRules.AppendValue(i.GetDebugJson());
    }
    return result;
}

void TTableTiering::AddRule(TTieringRule&& tr) {
    if (Rules.size()) {
        Y_VERIFY(Rules.back().GetDurationForEvict() <= tr.GetDurationForEvict());
        if (Column != tr.GetColumn()) {
            ALS_ERROR(NKikimrServices::TX_TIERING) << "inconsistency rule column: " <<
                TablePath << "/" << Column << " != " << tr.GetColumn();
            return;
        }
    } else {
        Column = tr.GetColumn();
        TablePath = tr.GetTablePath();
        TablePathId = tr.GetTablePathId();
    }
    Rules.emplace_back(std::move(tr));
}

NKikimr::NOlap::TTiersInfo TTableTiering::BuildTiersInfo() const {
    NOlap::TTiersInfo result(GetColumn());
    for (auto&& r : Rules) {
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
        NMetadataManager::TYDBColumn::Bytes(OwnerPath),
        NMetadataManager::TYDBColumn::Bytes(TierName),
        NMetadataManager::TYDBColumn::Bytes(TablePath),
        NMetadataManager::TYDBColumn::Bytes(Column),
        NMetadataManager::TYDBColumn::Bytes(DurationForEvict)
    };
}

std::vector<TString> TTieringRule::TDecoder::GetPKColumnIds() {
    return { TieringRuleId };
}

}

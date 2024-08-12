#include "constructor.h"
#include "meta.h"

#include <ydb/core/tx/schemeshard/olap/schema/schema.h>

namespace NKikimr::NOlap::NIndexes {

std::shared_ptr<NKikimr::NOlap::NIndexes::IIndexMeta> TBloomIndexConstructor::DoCreateIndexMeta(const ui32 indexId, const TString& indexName, const NSchemeShard::TOlapSchema& currentSchema, NSchemeShard::IErrorCollector& errors) const {
    std::set<ui32> columnIds;
    for (auto&& i : ColumnNames) {
        auto* columnInfo = currentSchema.GetColumns().GetByName(i);
        if (!columnInfo) {
            errors.AddError("no column with name " + i);
            return nullptr;
        }
        AFL_VERIFY(columnIds.emplace(columnInfo->GetId()).second);
    }
    return std::make_shared<TBloomIndexMeta>(indexId, indexName, GetStorageId().value_or(NBlobOperations::TGlobal::DefaultStorageId), columnIds, FalsePositiveProbability);
}

NKikimr::TConclusionStatus TBloomIndexConstructor::DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) {
    if (!jsonInfo.Has("column_names")) {
        return TConclusionStatus::Fail("column_names have to be in bloom filter features");
    }
    const NJson::TJsonValue::TArray* columnNamesArray;
    if (!jsonInfo["column_names"].GetArrayPointer(&columnNamesArray)) {
        return TConclusionStatus::Fail("column_names have to be in bloom filter features as array ['column_name_1', ... , 'column_name_N']");
    }
    for (auto&& i : *columnNamesArray) {
        if (!i.IsString()) {
            return TConclusionStatus::Fail("column_names have to be in bloom filter features as array of strings ['column_name_1', ... , 'column_name_N']");
        }
        ColumnNames.emplace(i.GetString());
    }
    if (!jsonInfo["false_positive_probability"].IsDouble()) {
        return TConclusionStatus::Fail("false_positive_probability have to be in bloom filter features as double field");
    }
    FalsePositiveProbability = jsonInfo["false_positive_probability"].GetDouble();
    if (FalsePositiveProbability < 0.01 || FalsePositiveProbability >= 1) {
        return TConclusionStatus::Fail("false_positive_probability have to be in bloom filter features as double field in interval [0.01, 1)");
    }
    return TConclusionStatus::Success();
}

NKikimr::TConclusionStatus TBloomIndexConstructor::DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexRequested& proto) {
    if (!proto.HasBloomFilter()) {
        const TString errorMessage = "not found BloobFilter section in proto: \"" + proto.DebugString() + "\"";
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("problem", errorMessage);
        return TConclusionStatus::Fail(errorMessage);
    }
    auto& bFilter = proto.GetBloomFilter();
    FalsePositiveProbability = bFilter.GetFalsePositiveProbability();
    if (FalsePositiveProbability < 0.01 || FalsePositiveProbability >= 1) {
        const TString errorMessage = "FalsePositiveProbability have to be in interval[0.01, 1)";
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("problem", errorMessage);
        return TConclusionStatus::Fail(errorMessage);
    }
    for (auto&& i : bFilter.GetColumnNames()) {
        ColumnNames.emplace(i);
    }
    return TConclusionStatus::Success();
}

void TBloomIndexConstructor::DoSerializeToProto(NKikimrSchemeOp::TOlapIndexRequested& proto) const {
    auto* filterProto = proto.MutableBloomFilter();
    filterProto->SetFalsePositiveProbability(FalsePositiveProbability);
    for (auto&& i : ColumnNames) {
        filterProto->AddColumnNames(i);
    }
}

}   // namespace NKikimr::NOlap::NIndexes
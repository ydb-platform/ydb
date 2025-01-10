#include "constructor.h"
#include "meta.h"

#include <ydb/core/tx/schemeshard/olap/schema/schema.h>

namespace NKikimr::NOlap::NIndexes::NCountMinSketch {

std::shared_ptr<NKikimr::NOlap::NIndexes::IIndexMeta> TCountMinSketchConstructor::DoCreateIndexMeta(const ui32 indexId, const TString& indexName, const NSchemeShard::TOlapSchema& currentSchema, NSchemeShard::IErrorCollector& errors) const {
    std::set<ui32> columnIds;
    if (ColumnNames.empty()) {
        for (const auto& [id, _] : currentSchema.GetColumns().GetColumns()) {
            AFL_VERIFY(columnIds.emplace(id).second);
        }
    }
    for (auto&& i : ColumnNames) {
        auto* columnInfo = currentSchema.GetColumns().GetByName(i);
        if (!columnInfo) {
            errors.AddError("no column with name " + i);
            return nullptr;
        }
        AFL_VERIFY(columnIds.emplace(columnInfo->GetId()).second);
    }
    return std::make_shared<TIndexMeta>(indexId, indexName, GetStorageId().value_or(NBlobOperations::TGlobal::DefaultStorageId), columnIds);
}

NKikimr::TConclusionStatus TCountMinSketchConstructor::DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) {
    if (!jsonInfo.Has("column_names")) {
        return TConclusionStatus::Fail("column_names have to be in count min sketch features");
    }
    const NJson::TJsonValue::TArray* columnNamesArray;
    if (!jsonInfo["column_names"].GetArrayPointer(&columnNamesArray)) {
        return TConclusionStatus::Fail("column_names have to be in count min sketch features as array ['column_name_1', ... , 'column_name_N']");
    }
    for (auto&& i : *columnNamesArray) {
        if (!i.IsString()) {
            return TConclusionStatus::Fail("column_names have to be in count min sketch features as array of strings ['column_name_1', ... , 'column_name_N']");
        }
        ColumnNames.emplace(i.GetString());
    }
    return TConclusionStatus::Success();
}

NKikimr::TConclusionStatus TCountMinSketchConstructor::DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexRequested& proto) {
    if (!proto.HasCountMinSketch()) {
        const TString errorMessage = "not found CountMinSketch section in proto: \"" + proto.DebugString() + "\"";
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("problem", errorMessage);
        return TConclusionStatus::Fail(errorMessage);
    }
    auto& sketch = proto.GetCountMinSketch();
    for (auto&& i : sketch.GetColumnNames()) {
        ColumnNames.emplace(i);
    }
    return TConclusionStatus::Success();
}

void TCountMinSketchConstructor::DoSerializeToProto(NKikimrSchemeOp::TOlapIndexRequested& proto) const {
    auto* sketchProto = proto.MutableCountMinSketch();
    for (auto&& i : ColumnNames) {
        sketchProto->AddColumnNames(i);
    }
}

}   // namespace NKikimr::NOlap::NIndexes

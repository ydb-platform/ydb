#include "constructor.h"
#include "meta.h"

#include <ydb/core/tx/schemeshard/olap/schema/schema.h>

namespace NKikimr::NOlap::NIndexes::NMax {

std::shared_ptr<NKikimr::NOlap::NIndexes::IIndexMeta> TIndexConstructor::DoCreateIndexMeta(
    const ui32 indexId, const TString& indexName, const NSchemeShard::TOlapSchema& currentSchema, NSchemeShard::IErrorCollector& errors) const {
    ui32 columnId;
    {
        auto* columnInfo = currentSchema.GetColumns().GetByName(ColumnName);
        if (!columnInfo) {
            errors.AddError("no column with name " + ColumnName);
            return nullptr;
        }
        if (!TIndexMeta::IsAvailableType(columnInfo->GetType())) {
            errors.AddError("inappropriate type for max index");
            return nullptr;
        }
        columnId = columnInfo->GetId();
    }
    return std::make_shared<TIndexMeta>(indexId, indexName, GetStorageId().value_or(NBlobOperations::TGlobal::LocalMetadataStorageId), columnId);
}

NKikimr::TConclusionStatus TIndexConstructor::DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) {
    if (!jsonInfo.Has("column_name")) {
        return TConclusionStatus::Fail("column_name have to be in max index features");
    }
    if (!jsonInfo["column_name"].GetString(&ColumnName)) {
        return TConclusionStatus::Fail("column_name have to be in max index features as string");
    }
    return TConclusionStatus::Success();
}

NKikimr::TConclusionStatus TIndexConstructor::DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexRequested& proto) {
    if (!proto.HasMaxIndex()) {
        const TString errorMessage = "Not found MaxIndex section in proto: \"" + proto.DebugString() + "\"";
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("problem", errorMessage);
        return TConclusionStatus::Fail(errorMessage);
    }
    auto& bIndex = proto.GetMaxIndex();
    ColumnName = bIndex.GetColumnName();
    if (!ColumnName) {
        return TConclusionStatus::Fail("Empty column name in MaxIndex proto");
    }
    return TConclusionStatus::Success();
}

void TIndexConstructor::DoSerializeToProto(NKikimrSchemeOp::TOlapIndexRequested& proto) const {
    auto* filterProto = proto.MutableMaxIndex();
    AFL_VERIFY(!!ColumnName)("problem", "not initialized max index info trying to serialize");
    filterProto->SetColumnName(ColumnName);
}

}   // namespace NKikimr::NOlap::NIndexes::NMax

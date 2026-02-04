#include "constructor.h"
#include "meta.h"

#include <ydb/core/tx/schemeshard/olap/schema/schema.h>

namespace NKikimr::NOlap::NIndexes::NMinMax {

std::shared_ptr<NKikimr::NOlap::NIndexes::IIndexMeta> TIndexConstructor::DoCreateIndexMeta(
    const ui32 indexId, const TString& indexName, const NSchemeShard::TOlapSchema& currentSchema, NSchemeShard::IErrorCollector& errors) const {
    ui32 columnId = 0;
    {
        auto* columnInfo = currentSchema.GetColumns().GetByName(ColumnName);
        if (!columnInfo) {
            errors.AddError(Sprintf("tried to create minmax index for column %s, but this column doesn't exist in target table",ColumnName.c_str()));
            return nullptr;
        }
        if (!TIndexMeta::IsAvailableType(columnInfo->GetType())) {
            errors.AddError(Sprintf("inappropriate column type for minmax index: %s", columnInfo->GetTypeName().c_str()));
            return nullptr;
        }
        columnId = columnInfo->GetId();
    }
    return std::make_shared<NMinMax::TIndexMeta>(indexId, indexName, GetStorageId().value_or(NBlobOperations::TGlobal::LocalMetadataStorageId),
        GetInheritPortionStorage().value_or(false), columnId, DataExtractor);
}

NKikimr::TConclusionStatus TIndexConstructor::DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) {
    if (auto conc = DataExtractor.DeserializeFromJson(jsonInfo["data_extractor"]); conc.IsFail()) {
        return conc;
    }
    if (!jsonInfo.Has("column_name")) {
        return TConclusionStatus::Fail("column_name have to be in minmax index features");
    }
    if (!jsonInfo["column_name"].GetString(&ColumnName)) {
        return TConclusionStatus::Fail("column_name have to be in minmax index features as string");
    }
    return TConclusionStatus::Success();
}

NKikimr::TConclusionStatus TIndexConstructor::DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexRequested& proto) {
    if (!proto.HasMinMaxIndex()) {
        const TString errorMessage = "Not found MinMaxIndex section in proto: \"" + proto.DebugString() + "\"";
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("problem", errorMessage);
        return TConclusionStatus::Fail(errorMessage);
    }
    auto& bIndex = proto.GetMinMaxIndex();
    ColumnName = bIndex.GetColumnName();
    if (!DataExtractor.DeserializeFromProto(bIndex.GetDataExtractor())) {
        return TConclusionStatus::Fail("cannot parse data extractor: " + bIndex.GetDataExtractor().DebugString());
    }
    AFL_VERIFY(DataExtractor.HasObject());
    return TConclusionStatus::Success();
}

void TIndexConstructor::DoSerializeToProto(NKikimrSchemeOp::TOlapIndexRequested& proto) const {
    auto* filterProto = proto.MutableMinMaxIndex();
    AFL_VERIFY(!!ColumnName)("problem", "not initialized max index info trying to serialize");
    filterProto->SetColumnName(ColumnName);
    *filterProto->MutableDataExtractor() = DataExtractor.SerializeToProto();
}

}   // namespace NKikimr::NOlap::NIndexes::NMax

#include "constructor.h"

#include <ydb/core/tx/schemeshard/olap/schema/schema.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

namespace NKikimr::NOlap::NIndexes {

TConclusionStatus TColumnIndexConstructor::DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) {
    if (!jsonInfo.Has("column_name")) {
        return TConclusionStatus::Fail("column_name have to be in bloom filter features");
    }
    if (!jsonInfo["column_name"].GetString(&ColumnName)) {
        return TConclusionStatus::Fail("column_name have to be string");
    }
    if (!ColumnName) {
        return TConclusionStatus::Fail("column_name cannot contains empty strings");
    }
    {
        auto conclusion = DataExtractor.DeserializeFromJson(jsonInfo["data_extractor"]);
        if (conclusion.IsFail()) {
            return conclusion;
        }
    }
    return TConclusionStatus::Success();
}

TConclusion<TString> TColumnIndexConstructor::ResolveColumnNameForAlterIndex(
    const NSchemeShard::TOlapSchema& currentSchema,
    const IIndexMeta& existingMeta) const {
    const auto colId = existingMeta.GetSingleColumnId();
    if (!colId) {
        return TConclusionStatus::Fail("existing index has no single column; cannot determine column for ALTER INDEX");
    }

    const auto* col = currentSchema.GetColumns().GetById(*colId);
    if (!col) {
        return TConclusionStatus::Fail(TStringBuilder() << "column id " << *colId << " not found in schema for ALTER INDEX");
    }

    return col->GetName();
}

bool TColumnIndexConstructor::IsSupportedTypeForEquals(const NScheme::TTypeId typeId) {
    if (!NScheme::NTypeIds::IsYqlType(typeId)) {
        return false;
    }

    switch (typeId) {
        case NScheme::NTypeIds::Yson:
        case NScheme::NTypeIds::Json:
        case NScheme::NTypeIds::JsonDocument:
            return false;
        default:
            return true;
    }
}

}   // namespace NKikimr::NOlap::NIndexes

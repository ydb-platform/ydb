#include "scheme.h"
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <util/string/join.h>

namespace NKikimr::NFormats {

arrow::Result<TArrowCSV> TArrowCSVScheme::Create(const TVector<std::pair<TString, NScheme::TTypeInfo>>& columns, bool header, const std::set<std::string>& notNullColumns) {
    TVector<TString> errors;
    TColummns convertedColumns;
    convertedColumns.reserve(columns.size());
    for (auto& [name, type] : columns) {
        const auto arrowType = NArrow::GetArrowType(type);
        if (!arrowType.ok()) {
            errors.emplace_back("column " + name + ": " + arrowType.status().ToString());
            continue;
        }
        const auto csvArrowType = NArrow::GetCSVArrowType(type);
        if (!csvArrowType.ok()) {
            errors.emplace_back("column " + name + ": " + csvArrowType.status().ToString());
            continue;
        }
        convertedColumns.emplace_back(TColumnInfo{name, *arrowType, *csvArrowType});
    }
    if (!errors.empty()) {
        return arrow::Status::TypeError(ErrorPrefix() + "columns errors: " + JoinSeq("; ", errors));
    }
    return TArrowCSVScheme(convertedColumns, header, notNullColumns);
}

}
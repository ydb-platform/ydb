#include "arrow_helpers_minikql.h"

#include <ydb/core/kqp/common/result_set_format/kqp_result_set_arrow.h>
#include <util/string/join.h>

namespace NKikimr::NArrow {

arrow::Result<arrow::FieldVector> MakeArrowFields(
    const std::vector<std::pair<TString, NKikimr::NMiniKQL::TType*>>& yqlColumns, const std::set<std::string>& notNullColumns) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(yqlColumns.size());
    TVector<TString> errors;
    for (auto& [name, mkqlType] : yqlColumns) {
        std::string colName(name.data(), name.size());
        std::shared_ptr<arrow::DataType> arrowType;

        try {
            arrowType = NKqp::NFormats::GetArrowType(mkqlType);
        } catch (const yexception& e) {
            errors.emplace_back(colName + " error: " + e.what());
        }

        if (arrowType) {
            fields.emplace_back(std::make_shared<arrow::Field>(colName, arrowType, !notNullColumns.contains(colName)));
        }
    }
    if (errors.empty()) {
        return fields;
    }
    return arrow::Status::TypeError(JoinSeq(", ", errors));
}

arrow::Result<std::shared_ptr<arrow::Schema>> MakeArrowSchema(
    const std::vector<std::pair<TString, NKikimr::NMiniKQL::TType*>>& yqlColumns, const std::set<std::string>& notNullColumns) {
    const auto fields = MakeArrowFields(yqlColumns, notNullColumns);
    if (fields.ok()) {
        return std::make_shared<arrow::Schema>(fields.ValueUnsafe());
    }
    return fields.status();
}

} // namespace NKikimr::NArrow

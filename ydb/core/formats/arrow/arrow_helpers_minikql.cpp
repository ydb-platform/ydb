#include "arrow_helpers_minikql.h"

#include <ydb/core/kqp/common/result_set_format/kqp_formats_arrow.h>
#include <util/string/join.h>

namespace NKikimr::NArrow {

arrow20::Result<arrow20::FieldVector> MakeArrowFields(
    const std::vector<std::pair<TString, NKikimr::NMiniKQL::TType*>>& yqlColumns, const std::set<std::string>& notNullColumns) {
    std::vector<std::shared_ptr<arrow20::Field>> fields;
    fields.reserve(yqlColumns.size());
    TVector<TString> errors;
    for (auto& [name, mkqlType] : yqlColumns) {
        std::string colName(name.data(), name.size());
        std::shared_ptr<arrow20::DataType> arrowType;

        try {
            arrowType = NKqp::NFormats::GetArrowType(mkqlType);
        } catch (const yexception& e) {
            errors.emplace_back(colName + " error: " + e.what());
        }

        if (arrowType) {
            fields.emplace_back(std::make_shared<arrow20::Field>(colName, arrowType, !notNullColumns.contains(colName)));
        }
    }
    if (errors.empty()) {
        return fields;
    }
    return arrow20::Status::TypeError(JoinSeq(", ", errors));
}

arrow20::Result<std::shared_ptr<arrow20::Schema>> MakeArrowSchema(
    const std::vector<std::pair<TString, NKikimr::NMiniKQL::TType*>>& yqlColumns, const std::set<std::string>& notNullColumns) {
    const auto fields = MakeArrowFields(yqlColumns, notNullColumns);
    if (fields.ok()) {
        return std::make_shared<arrow20::Schema>(fields.ValueUnsafe());
    }
    return fields.status();
}

} // namespace NKikimr::NArrow

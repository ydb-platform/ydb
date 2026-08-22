#pragma once

#include <yql/essentials/minikql/mkql_node.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <util/generic/string.h>

namespace NKikimr::NMiniKQL {
class TType;
}

namespace NKikimr::NArrow {

arrow::Result<arrow::FieldVector> MakeArrowFields(const std::vector<std::pair<TString, NKikimr::NMiniKQL::TType*>>& yqlColumns, const std::set<std::string>& notNullColumns = {});
arrow::Result<std::shared_ptr<arrow::Schema>> MakeArrowSchema(const std::vector<std::pair<TString, NKikimr::NMiniKQL::TType*>>& yqlColumns, const std::set<std::string>& notNullColumns = {});

} // namespace NKikimr::NArrow

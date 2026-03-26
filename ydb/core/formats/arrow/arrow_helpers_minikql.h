#pragma once

#include <yql/essentials/minikql/mkql_node.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/api.h>
#include <util/generic/string.h>

namespace NKikimr::NMiniKQL {
class TType;
}

namespace NKikimr::NArrow {

arrow20::Result<arrow20::FieldVector> MakeArrowFields(const std::vector<std::pair<TString, NKikimr::NMiniKQL::TType*>>& yqlColumns, const std::set<std::string>& notNullColumns = {});
arrow20::Result<std::shared_ptr<arrow20::Schema>> MakeArrowSchema(const std::vector<std::pair<TString, NKikimr::NMiniKQL::TType*>>& yqlColumns, const std::set<std::string>& notNullColumns = {});

} // namespace NKikimr::NArrow

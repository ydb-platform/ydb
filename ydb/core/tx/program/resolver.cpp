#include "resolver.h"

#include <ydb/core/tx/columnshard/common/portion.h>

namespace NKikimr::NArrow::NSSA {

TString TSchemaResolverColumnsOnly::GetColumnName(ui32 id, bool required /*= true*/) const {
    auto* column = Schema->GetColumns().GetById(id);
    AFL_VERIFY(!required || !!column);
    if (column) {
        return column->GetName();
    } else {
        return "";
    }
}

std::optional<ui32> TSchemaResolverColumnsOnly::GetColumnIdOptional(const TString& name) const {
    auto* column = Schema->GetColumns().GetByName(name);
    if (!column) {
        return {};
    } else {
        return column->GetId();
    }
}

TColumnInfo TSchemaResolverColumnsOnly::GetDefaultColumn() const {
    return TColumnInfo::Original(
        (ui32)NOlap::NPortion::TSpecialColumns::SPEC_COL_PLAN_STEP_INDEX, NOlap::NPortion::TSpecialColumns::SPEC_COL_PLAN_STEP);
}

}   // namespace NKikimr::NArrow::NSSA

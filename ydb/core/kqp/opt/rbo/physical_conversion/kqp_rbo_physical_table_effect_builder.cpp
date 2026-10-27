#include "kqp_rbo_physical_table_effect_builder.h"
#include <yql/essentials/core/yql_expr_optimize.h>
using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

TExprNode::TPtr TPhysicalTableEffectBuilder::BuildPhysicalOp(TExprNode::TPtr input) {
    TVector<std::pair<TString, TString>> columns;
    for (const auto& [id, label] : TableEffect.GetColumns().Items()) {
        columns.emplace_back(Names.Get(id), label);
    }
    return NPhysicalConvertionUtils::BuildRenameMap(input, columns, Ctx, /*ordered=*/true);
}

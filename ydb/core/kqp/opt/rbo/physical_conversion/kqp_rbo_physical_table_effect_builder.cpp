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
    if (TableEffect.GetPgConstraintColumns().Empty()) {
        return NPhysicalConvertionUtils::BuildRenameMap(input, columns, Ctx, /*ordered=*/true);
    }

    auto row = Build<TCoArgument>(Ctx, Pos).Name("write_row").Done();
    auto checked = row.Ptr();
    // PG types need explicit runtime NOT NULL checks. Check the IU-named input
    // before the sink rename; destination names are only used in diagnostics.
    for (const auto& [id, column] : TableEffect.GetColumns().Items()) {
        if (!TableEffect.GetPgConstraintColumns().Contains(id)) {
            continue;
        }
        // Match KqpRewriteWriteConstraint: each predicate consumes the previous
        // checked row so no earlier check can be pruned when several columns
        // are constrained. The value passed to the sink is the original row.
        checked = Build<TKqpEnsure>(Ctx, Pos)
            .Value(row)
            .Predicate<TCoExists>()
                .Optional<TCoMember>()
                    .Struct(checked)
                    .Name().Build(Names.Get(id))
                .Build()
            .Build()
            .IssueCode().Build(ToString(ui32(NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE)))
            .Message<TCoUtf8>()
                .Literal().Build(TStringBuilder() << "Tried to insert NULL value into NOT NULL column: " << column)
            .Build()
            .Done().Ptr();
    }
    auto rows = Build<TCoOrderedMap>(Ctx, Pos)
        .Input(input)
        .Lambda().Args({row}).Body(checked).Build()
        .Done().Ptr();
    return NPhysicalConvertionUtils::BuildRenameMap(rows, columns, Ctx, /*ordered=*/true);
}

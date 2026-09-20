#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_utils.h>

#include <yql/essentials/core/yql_expr_type_annotation.h>

namespace NKikimr::NKqp {

namespace {

using namespace NYql;
using namespace NYql::NNodes;

// Makes a null column.
TMapElement BuildNullColumn(const TInfoUnit& column, const TTypeAnnotationNode* columnType, TPositionHandle pos,
                            TExprContext& ctx, TPlanProps& props) {
    Y_ENSURE(columnType, "No type for grouping column " << column.GetFullName());

    if (columnType->IsOptionalOrNull()) {
        columnType = columnType->Cast<TOptionalExprType>()->GetItemType();
    }

    // clang-format off
    auto nullColumn = Build<TCoLambda>(ctx, pos)
        .Args({"null_arg"})
        .Body<TCoNothing>()
            .OptionalType<TCoOptionalType>()
                .ItemType(ExpandType(pos, *columnType, ctx))
            .Build()
        .Build()
    .Done().Ptr();
    // clang-format on

    return TMapElement(column, TExpression(nullColumn, &ctx, &props));
}

// Makes an optional column.
TMapElement BuildOptionalColumn(const TInfoUnit& column, const TInfoUnit& sourceIU, TPositionHandle pos,
                                TExprContext& ctx, TPlanProps& props) {
    auto argument = ctx.NewArgument(pos, "optional_arg");

    // clang-format off
    auto optionalColumn = Build<TCoLambda>(ctx, pos)
        .Args({argument})
        .Body<TCoJust>()
            .Input<TCoMember>()
                .Struct(argument)
                .Name<TCoAtom>()
                    .Value(sourceIU.GetFullName())
                .Build()
            .Build()
        .Build()
    .Done().Ptr();
    // clang-format on

    return TMapElement(column, TExpression(optionalColumn, &ctx, &props));
}

} // anonymous namespace

bool TExpandGroupingSetsRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::GroupingSets;
}

TIntrusivePtr<IOperator> TExpandGroupingSetsRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& rboCtx,
                                                                      TPlanProps& props) {
    const auto groupingSetsOp = CastOperator<TOpGroupingSets>(input);
    const auto aggregate = CastOperator<TOpAggregate>(groupingSetsOp->GetInput());
    const auto& groupByKeys = aggregate->GetKeyColumns();
    const auto* aggregateInputStructType = aggregate->GetInput()->Type->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    const auto* aggregateStructType = aggregate->Type->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    const auto* groupingSetsStructType = groupingSetsOp->Type->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();

    THashSet<TInfoUnit, TInfoUnit::THashFunction> groupByKeySet(groupByKeys.begin(), groupByKeys.end());
    THashSet<TInfoUnit, TInfoUnit::THashFunction> commonKeys = groupByKeySet;
    // Find keys which are present in each grouping sets, other keys may become null.
    for (const auto& groupKeys : groupingSetsOp->GetGroupingSets()) {
        THashSet<TInfoUnit, TInfoUnit::THashFunction> keySet(groupKeys.begin(), groupKeys.end());
        THashSet<TInfoUnit, TInfoUnit::THashFunction> intersectionKeys;
        for (const auto& key : commonKeys) {
            if (keySet.contains(key)) {
                intersectionKeys.insert(key);
            }
        }
        commonKeys = std::move(intersectionKeys);
    }

    TVector<TIntrusivePtr<IOperator>> logicalBranches;
    logicalBranches.reserve(groupingSetsOp->GetGroupingSets().size());

    for (const auto& groupKeys : groupingSetsOp->GetGroupingSets()) {
        THashSet<TInfoUnit, TInfoUnit::THashFunction> keySet;
        for (const auto& key : groupKeys) {
            Y_ENSURE(keySet.insert(key).second, "Duplicate grouping key: " << key.GetFullName());
        }

        // Aggregate for group by keys specified in grouping sets.
        TIntrusivePtr<IOperator> logicalBranch = MakeIntrusive<TOpAggregate>(
            aggregate->GetInput(), aggregate->GetAggregationTraits(), groupKeys, aggregate->GetAggregationPhase(), aggregate->IsDistinctAll(), aggregate->Pos);

        TVector<TMapElement> renames;
        TVector<std::pair<TInfoUnit, TInfoUnit>> optionalColumns;
        for (const auto& key : groupKeys) {
            const auto* keyType = aggregateInputStructType->FindItemType(key.GetFullName());
            Y_ENSURE(keyType, "No type for grouping key " << key.GetFullName());
            // If column is not present in each grouping set and its non optional by default make it optional.
            if (!commonKeys.contains(key) && !keyType->IsOptionalOrNull()) {
                const auto source = MakeGeneratedIgnoreIU(props);
                renames.emplace_back(source, key, aggregate->Pos, &rboCtx.ExprCtx, &props, true);
                optionalColumns.emplace_back(key, source);
            }
        }

        if (!groupKeys.empty()) {
            for (const auto& traits : aggregate->GetAggregationTraits()) {
                const auto& resultColumn = traits.ResultColName;
                const auto* sourceColumnType = aggregateStructType->FindItemType(resultColumn.GetFullName());
                const auto* targetColumnType = groupingSetsStructType->FindItemType(resultColumn.GetFullName());
                Y_ENSURE(sourceColumnType && targetColumnType, "No type for aggregation result column" << resultColumn.GetFullName());
                if (!sourceColumnType->IsOptionalOrNull() && targetColumnType->IsOptionalOrNull()) {
                    const auto sourceColumn = MakeGeneratedIgnoreIU(props);
                    renames.emplace_back(sourceColumn, resultColumn, aggregate->Pos, &rboCtx.ExprCtx, &props, true);
                    optionalColumns.emplace_back(resultColumn, sourceColumn);
                }
            }
        }

        if (!renames.empty()) {
            logicalBranch = MakeIntrusive<TOpMap>(logicalBranch, aggregate->Pos, renames);
        }

        TVector<TMapElement> originalNames;
        for (const auto& [column, source] : optionalColumns) {
            originalNames.emplace_back(BuildOptionalColumn(column, source, aggregate->Pos, rboCtx.ExprCtx, props));
        }

        for (const auto& key : groupByKeys) {
            if (!keySet.contains(key)) {
                originalNames.emplace_back(BuildNullColumn(key, aggregateInputStructType->FindItemType(key.GetFullName()), aggregate->Pos,
                                                           rboCtx.ExprCtx, props));
            }
        }
        if (!originalNames.empty()) {
            logicalBranch = MakeIntrusive<TOpMap>(logicalBranch, aggregate->Pos, originalNames);
        }
        logicalBranches.emplace_back(std::move(logicalBranch));
    }

    Y_ENSURE(!logicalBranches.empty(), "Grouping sets list must not be empty");
    if (logicalBranches.size() == 1) {
        return logicalBranches.front();
    }

    return MakeIntrusive<TOpUnionAll>(std::move(logicalBranches), groupingSetsOp->Pos, aggregate->GetOutputIUs());
}

} // namespace NKikimr::NKqp

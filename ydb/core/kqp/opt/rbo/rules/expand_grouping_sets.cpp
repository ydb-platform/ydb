#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_utils.h>

#include <yql/essentials/core/yql_expr_type_annotation.h>

namespace NKikimr::NKqp {

namespace {

using namespace NYql;
using namespace NYql::NNodes;

// Makes a null column.
TExpression BuildNullColumn(const TTypeAnnotationNode* columnType, TPositionHandle pos,
                            TExprContext& ctx, TPlanProps& props) {
    Y_ENSURE(columnType, "No type for grouping column");

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

    return TExpression(nullColumn, &ctx, &props);
}

// Makes an optional column.
TExpression BuildOptionalColumn(TInfoUnitId sourceIU, TPositionHandle pos,
                                TExprContext& ctx, TPlanProps& props) {
    auto argument = ctx.NewArgument(pos, "optional_arg");

    // clang-format off
    auto optionalColumn = Build<TCoLambda>(ctx, pos)
        .Args({argument})
        .Body<TCoJust>()
            .Input<TCoMember>()
                .Struct(argument)
                .Name<TCoAtom>()
                    .Value(ctx.GetIndexAsString(sourceIU))
                .Build()
            .Build()
        .Build()
    .Done().Ptr();
    // clang-format on

    return TExpression(optionalColumn, &ctx, &props);
}

TExpression BuildGroupingIndicatorColumn(bool aggregatedAway, TPositionHandle pos, TExprContext& ctx, TPlanProps& props) {
    // clang-format off
    auto indicatorColumn = Build<TCoLambda>(ctx, pos)
        .Args({"grouping_indicator_arg"})
        .Body<TCoUint64>()
            .Literal().Build(aggregatedAway ? "1" : "0")
        .Build()
    .Done().Ptr();
    // clang-format on

    return TExpression(indicatorColumn, &ctx, &props);
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
    const auto& groupingSets = groupingSetsOp->GetGroupingSets();
    auto& ctx = rboCtx.ExprCtx;
    auto& registry = props.InfoUnitRegistry;
    Y_ENSURE(!groupingSets.empty(), "Grouping sets list must not be empty");

    // Find keys which are present in each grouping sets, other keys may become null.
    auto commonKeys = groupByKeys.Unordered();
    for (const auto& keys : groupingSets) {
        Y_ENSURE(keys.IsSubsetOf(groupByKeys.Unordered()), "Unknown grouping key");
        commonKeys &= keys;
    }

    TVector<TIntrusivePtr<IOperator>> branches;
    TMappedIUs<TUnionInputRow> inputsByOutput;
    for (const auto output : groupingSetsOp->GetOutputIUs()) {
        inputsByOutput.Add(output).Inputs.reserve(groupingSets.size());
    }

    const auto sourceInput = aggregate->GetInput();
    auto hub = groupingSets.size() > 1
        ? TReplicate::Create(sourceInput, aggregate->Pos, registry)
        : TIntrusivePtr<TReplicate>{};
    for (const auto& groupKeys : groupingSets) {
        auto port = hub ? hub->AddOutput() : nullptr;
        const auto* rebindings = port && !port->IsPrimary() ? &port->GetRebindings() : nullptr;
        const auto rebind = [&](TInfoUnitId id) {
            return rebindings ? *rebindings->Find(id) : id;
        };
        // Each branch defines its own aggregate results. Labels can coincide;
        // the UnionAll rows connect their values. `values` is the branch's
        // current binding for each original ID.
        TMappedIUs<TInfoUnitId> values;
        TAggregationIUs aggregations;
        for (const auto& [output, traits] : aggregate->GetAggregationTraits().Items()) {
            const auto value = registry.AddCopy(output);
            auto reboundTraits = traits;
            reboundTraits.Input = rebind(traits.Input);
            aggregations.Add(value, std::move(reboundTraits));
            values.Add(output, value);
        }
        TOrderedIUs<> keys;
        for (const auto key : groupByKeys.Items()) {
            if (groupKeys.Contains(key)) {
                keys.Append(rebind(key));
                if (!values.Keys().Contains(key)) {
                    values.Add(key, rebind(key));
                }
            }
        }
        TIntrusivePtr<IOperator> branchInput = port ? port : sourceInput;
        TIntrusivePtr<IOperator> branch = MakeIntrusive<TOpAggregate>(std::move(branchInput),
            std::move(aggregations), std::move(keys), aggregate->GetAggregationPhase(), aggregate->IsDistinctAll(), aggregate->Pos);

        TMapIUs computed;
        // A computed value replaces the branch's binding for its original ID.
        auto addValue = [&](TInfoUnitId output, TExpression expression) {
            const auto value = registry.AddCopy(output);
            computed.Add(value, std::move(expression));
            if (values.Keys().Contains(output)) {
                values.Replace(output, value);
            } else {
                values.Add(output, value);
            }
        };
        for (const auto key : groupByKeys.Items()) {
            const auto* type = sourceInput->GetIUType(key, ctx);
            Y_ENSURE(type, "No type for grouping key " << key);
            if (!groupKeys.Contains(key)) {
                addValue(key, BuildNullColumn(type, aggregate->Pos, ctx, props));
            } else if (!commonKeys.Contains(key) && !type->IsOptionalOrNull()) {
                addValue(key, BuildOptionalColumn(values.At(key), aggregate->Pos, ctx, props));
            }
        }
        if (!groupKeys.Empty()) {
            for (const auto& [output, source] : groupingSetsOp->GetColumns().Items()) {
                if (!aggregate->GetAggregationTraits().Keys().Contains(source)) {
                    continue;
                }
                const auto* sourceType = aggregate->GetIUType(source, ctx);
                const auto* targetType = groupingSetsOp->GetIUType(output, ctx);
                Y_ENSURE(sourceType && targetType, "No type for aggregation result " << output);
                if (!sourceType->IsOptionalOrNull() && targetType->IsOptionalOrNull()) {
                    addValue(source, BuildOptionalColumn(values.At(source), aggregate->Pos, ctx, props));
                }
            }
        }
        for (const auto& [output, key] : groupingSetsOp->GetGroupingIndicators().Items()) {
            const auto value = registry.AddCopy(output);
            computed.Add(value, BuildGroupingIndicatorColumn(!groupKeys.Contains(key), aggregate->Pos, ctx, props));
            inputsByOutput.At(output).Inputs.push_back(value);
        }
        if (!computed.Keys().Empty()) {
            branch = MakeIntrusive<TOpMap>(std::move(branch), aggregate->Pos, std::move(computed));
        }
        for (const auto& [output, source] : groupingSetsOp->GetColumns().Items()) {
            inputsByOutput.At(output).Inputs.push_back(values.At(source));
        }
        branches.push_back(std::move(branch));
    }

    if (branches.size() == 1) {
        // Preserve the external binding IDs without introducing a one-way Union.
        TMapIUs bindings;
        for (const auto& [output, row] : inputsByOutput.Items()) {
            if (output != row.Inputs.front()) {
                bindings.Add(output, MakeColumnAccess(row.Inputs.front(), aggregate->Pos, &ctx, &props));
            }
        }
        if (bindings.Keys().Empty()) {
            return std::move(branches.front());
        }
        return MakeIntrusive<TOpMap>(std::move(branches.front()), aggregate->Pos, std::move(bindings));
    }

    TUnionAllIUs columns(TUnionInputPolicy{branches.size()});
    for (const auto output : inputsByOutput.Keys()) {
        columns.Add(output, std::move(inputsByOutput.At(output)));
    }
    return MakeIntrusive<TOpUnionAll>(std::move(branches), groupingSetsOp->Pos, std::move(columns));
}

} // namespace NKikimr::NKqp

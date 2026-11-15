#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_utils.h>

namespace NKikimr::NKqp {

namespace {

TExpression BuildGroupingIndicatorColumn(bool aggregatedAway, NYql::TPositionHandle pos, TPlanProps& props) {
    return MakeConstant("Uint64", aggregatedAway ? "1" : "0", pos, &props);
}

} // anonymous namespace

bool TExpandGroupingSetsRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::GroupingSets;
}

TIntrusivePtr<IOperator> TExpandGroupingSetsRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext&,
                                                                      TPlanProps& props) {
    const auto groupingSetsOp = CastOperator<TOpGroupingSets>(input);
    const auto aggregate = CastOperator<TOpAggregate>(groupingSetsOp->GetInput());
    const auto& groupByKeys = aggregate->GetKeyColumns();
    const auto& groupingSets = groupingSetsOp->GetGroupingSets();
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
        const auto nativeOptional = [&](TInfoUnitId source, NFrontend::TType type) {
            const auto& arena = props.NativeExpressions;
            auto value = arena->Column(source, type);
            return TExpression::FromNative(arena, arena->Convert(value, type.Optional()), &props, aggregate->Pos);
        };
        for (const auto key : groupByKeys.Items()) {
            const auto type = sourceInput->GetNativeIUType(key);
            if (!groupKeys.Contains(key)) {
                const auto& arena = props.NativeExpressions;
                addValue(key, TExpression::FromNative(arena, arena->Literal(type.Optional(), std::monostate{}), &props, aggregate->Pos));
            } else if (!commonKeys.Contains(key) && !type.OptionalDepth) {
                addValue(key, nativeOptional(values.At(key), type));
            }
        }
        if (!groupKeys.Empty()) {
            for (const auto& [output, source] : groupingSetsOp->GetColumns().Items()) {
                if (!aggregate->GetAggregationTraits().Keys().Contains(source)) {
                    continue;
                }
                const auto sourceType = aggregate->GetNativeIUType(source);
                if (!sourceType.OptionalDepth && groupingSetsOp->GetNativeIUType(output).OptionalDepth) {
                    addValue(source, nativeOptional(values.At(source), sourceType));
                }
            }
        }
        for (const auto& [output, key] : groupingSetsOp->GetGroupingIndicators().Items()) {
            const auto value = registry.AddCopy(output);
            computed.Add(value, BuildGroupingIndicatorColumn(!groupKeys.Contains(key), aggregate->Pos, props));
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
                bindings.Add(output, MakeColumnAccess(row.Inputs.front(), *branches.front(), aggregate->Pos, &props));
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

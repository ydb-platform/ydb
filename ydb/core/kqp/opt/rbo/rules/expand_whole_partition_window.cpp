#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/opt/rbo/rules/decorrelation/dependent_join_pushdown.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>

namespace NKikimr::NKqp {

namespace {

bool HasWholePartitionAggregates(const IOperator& input) {
    if (input.GetKind() != EOperator::Window) {
        return false;
    }

    const auto& window = CastOperator<TOpWindow>(input);
    if (!window.GetFrame().IsWholePartition()) {
        return false;
    }

    const auto funcs = window.GetWindowFuncs().Items() | std::views::values;
    return std::any_of(funcs.begin(), funcs.end(), [](const TOpWindowFunc& func) { return func.Kind == EWindowFuncKind::Aggregate; });
}

} // anonymous namespace

bool TExpandWholePartitionWindowRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return HasWholePartitionAggregates(*input);
}

// f(x) OVER (PARTITION BY p) becomes input JOIN (SELECT p, f(x) FROM input GROUP BY p) USING (p),
// since aggregation and join can spill and the window cannot.
TIntrusivePtr<IOperator> TExpandWholePartitionWindowRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& rboCtx,
                                                                              TPlanProps& props) {
    if (!rboCtx.KqpCtx.Config->GetWindowFunctionsV2() || !HasWholePartitionAggregates(*input)) {
        return input;
    }

    const auto window = CastOperator<TOpWindow>(input);
    const auto windowInput = window->GetInput();
    const auto pos = window->Pos;
    auto& ctx = rboCtx.ExprCtx;
    auto& registry = props.InfoUnitRegistry;

    const auto hub = TReplicate::Create(windowInput, pos, registry);
    TIntrusivePtr<IOperator> rows = hub->AddOutput();
    const auto groupsPort = hub->AddOutput();
    const auto& rebindings = groupsPort->GetRebindings();

    const bool grouped = !window->GetPartitionKeys().Items().empty();
    TOrderedIUs<> groupKeys;
    TJoinIUs joinKeys;
    TJoinIUs nullableJoinKeys;
    for (const auto key : window->GetPartitionKeys().Items()) {
        const auto groupKey = rebindings.At(key);
        groupKeys.Append(groupKey);
        if (IsNullableIU(windowInput, key, ctx)) {
            nullableJoinKeys.Add(key, groupKey);
        } else {
            joinKeys.Add(key, groupKey);
        }
    }

    TAggregationIUs aggregations;
    TMapIUs results;
    TWindowIUs natives;
    for (const auto& [output, func] : window->GetWindowFuncs().Items()) {
        if (func.Kind == EWindowFuncKind::Native) {
            natives.Add(output, func);
            continue;
        }

        Y_ENSURE(func.Arguments.Items().size() == 1, "Window aggregate " << func.Function << " expects a single argument");
        const auto argument = func.Arguments.Items().front();
        const auto aggregated = registry.AddCopy(output);
        aggregations.Add(aggregated, TOpAggregationTraits{rebindings.At(argument), func.Function});

        // The New RBO types window aggregates other than count as optional even over a never empty frame,
        // unlike YQL (see the disabled not null test). A grouped aggregate over a non-optional input is not
        // optional, so wrap it to keep the window type. Replace with Unwrap of scalar aggregates once that is fixed.
        auto value = MakeColumnAccess(aggregated, pos, &ctx, &props);
        if (func.Function != "count" && grouped && !IsNullableIU(windowInput, argument, ctx)) {
            value = MakeUnaryCallable("Just", value);
        }
        results.Add(output, std::move(value));
    }

    TIntrusivePtr<IOperator> groups =
        MakeIntrusive<TOpAggregate>(groupsPort, std::move(aggregations), std::move(groupKeys), EOpPhase::Undefined, /*distinctAll=*/false, pos);

    // A window puts NULL keys into one partition. Both join inputs are new and
    // untyped here, so every key passed below gets the null-safe semantics.
    if (!nullableJoinKeys.Items().empty()) {
        for (const auto& key : MakeNullSafeJoinKeys(rows, groups, nullableJoinKeys, pos, rboCtx, props).Items()) {
            joinKeys.Add(key);
        }
    }

    TIntrusivePtr<IOperator> result = MakeIntrusive<TOpJoin>(rows, groups, pos, grouped ? "Inner" : "Cross", std::move(joinKeys));
    result = MakeIntrusive<TOpMap>(result, pos, std::move(results));
    if (!natives.Keys().Empty()) {
        result = MakeIntrusive<TOpWindow>(result, pos, std::move(natives), window->GetPartitionKeys(), window->GetSortElements(), window->GetFrame());
    }
    return result;
}

} // namespace NKikimr::NKqp

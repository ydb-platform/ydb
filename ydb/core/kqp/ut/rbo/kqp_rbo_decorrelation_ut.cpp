#include "kqp_rbo_test_helpers.h"

#include <ydb/core/kqp/opt/rbo/rules/decorrelation/dependent_join_pushdown.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {
namespace {

struct TPlan {
    NTests::TIdTestContext Test;
    TExprContext& Ctx = Test.ExprCtx;
    const TPositionHandle Pos;
    TOpRoot Root;

    explicit TPlan(TOrderedIUs<TString> outputs = {})
        : Root(MakeIntrusive<TOpEmptySource>(Pos), Pos, std::move(outputs))
    {}

    TInfoUnitId Id() { return Root.PlanProps.InfoUnitRegistry.AddGenerated(); }

    TExpression Column(TInfoUnitId id) {
        return MakeColumnAccess(id, Pos, &Ctx, &Root.PlanProps);
    }

    TIntrusivePtr<IOperator> Source(TUnorderedIUs ids) {
        TMapIUs values;
        for (const auto id : ids) {
            values.Add(id, MakeConstant("Uint64", "1", Pos, &Ctx));
        }
        return MakeIntrusive<TOpMap>(MakeIntrusive<TOpEmptySource>(Pos), Pos, std::move(values));
    }

    TIntrusivePtr<TOpAddDependencies> Capture(TIntrusivePtr<IOperator> input,
        std::initializer_list<std::pair<TInfoUnitId, TInfoUnitId>> bindings)
    {
        TDependencyIUs captures;
        for (const auto& [local, outer] : bindings) {
            captures.Add(local, TCapturedIU{outer, Ctx.MakeType<TDataExprType>(EDataSlot::Uint64)});
        }
        return MakeIntrusive<TOpAddDependencies>(std::move(input), Pos, std::move(captures));
    }

    TIntrusivePtr<IOperator> Producer(TInfoUnitId parameter, TInfoUnitId local, TInfoUnitId value,
        const TString& comparison = ">")
    {
        return MakeIntrusive<TOpFilter>(Capture(Source({value}), {{local, parameter}}), Pos,
            MakeBinaryPredicate(comparison, Column(value), Column(local)));
    }

    TIntrusivePtr<TOpAggregate> Count(TIntrusivePtr<IOperator> input, TInfoUnitId value, TInfoUnitId output) {
        TAggregationIUs functions;
        functions.Add(output, TOpAggregationTraits{value, "count"});
        return MakeIntrusive<TOpAggregate>(std::move(input), std::move(functions), TOrderedIUs<>{},
            EOpPhase::Undefined, false, Pos);
    }

    TIntrusivePtr<IOperator> DependentJoin(TIntrusivePtr<IOperator> body, TUnorderedIUs parameters) {
        return MakeIntrusive<TOpDependentJoin>(MakeDomainProjection(Source(parameters), parameters, Pos),
            std::move(body), std::move(parameters), Pos);
    }

    void Attach(TIntrusivePtr<IOperator> body, TUnorderedIUs parameters) {
        Root.SetInput(DependentJoin(std::move(body), std::move(parameters)));
    }

    void Decorrelate() {
        TVector<std::unique_ptr<IRule>> rules;
        rules.emplace_back(std::make_unique<TRewriteDependentJoinToCrossJoinNoFreeVarsRule>());
        rules.emplace_back(std::make_unique<TRewriteDependentJoinToCrossJoinRule>());
        rules.emplace_back(std::make_unique<TEliminateDependentJoinDomainRule>());
        rules.emplace_back(std::make_unique<TPushDependentJoinThroughFilterRule>());
        rules.emplace_back(std::make_unique<TPushDependentJoinThroughMapRule>());
        rules.emplace_back(std::make_unique<TPushDependentJoinThroughAggregateRule>());
        rules.emplace_back(std::make_unique<TPushDependentJoinThroughUnionAllRule>());
        rules.emplace_back(std::make_unique<TPushDependentJoinThroughJoinRule>());
        rules.emplace_back(std::make_unique<TPushDependentJoinThroughReplicateRule>());
        rules.emplace_back(std::make_unique<TDependentJoinNotSupportedRule>());
        TRuleBasedStage("Decorrelation", std::move(rules)).RunStage(Root, Test.RboCtx);
    }

    void Rewrite() {
        Decorrelate();
        Root.ComputeParents();
        Root.RecomputeOutputIUsSubtree();
        UNIT_ASSERT_VALUES_EQUAL(CountKind(EOperator::DependentJoin), 0);
        NTests::AssertIdInvariants(Root, Root.PlanProps);
    }

    size_t CountKind(EOperator kind) {
        size_t count = 0;
        for (const auto& item : IterateSubtree(&Root)) {
            count += item.Current->Kind == kind;
        }
        return count;
    }
};

TInfoUnitId PortId(TOpReplicate& port, TInfoUnitId source) {
    return port.IsPrimary() ? source : *port.GetRebindings().Find(source);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(KqpRboDecorrelation) {
    Y_UNIT_TEST(DomainKeysPreserveKnownNullabilityBeforeRetyping) {
        for (const bool annotated : {false, true}) {
            for (const bool nullable : {false, true}) {
                NTests::TIdTestContext f;
                const auto parameter = f.Id();
                TIntrusivePtr<IOperator> caller = f.Read({parameter});
                if (annotated) {
                    f.SetType(*caller, nullable ? TUnorderedIUs{parameter} : TUnorderedIUs{});
                }
                auto domain = MakeSubplanDomain(caller, {parameter}, f.Pos, f.Props);
                TJoinIUs keys;
                keys.Add(parameter, domain.Keys.Items()[0].second);
                auto result = MakeNullSafeJoinKeys(caller, domain.Input, keys, f.Pos, f.RboCtx, f.Props);
                const bool encoded = result.Left() != keys.Left();
                UNIT_ASSERT_VALUES_EQUAL(encoded || HasEqualNullsKey(result), !annotated || nullable);
                if (annotated && !nullable) {
                    UNIT_ASSERT(result.Right() == keys.Right());
                    UNIT_ASSERT(caller->Kind == EOperator::Replicate);
                    UNIT_ASSERT(domain.Input->Kind == EOperator::Aggregate);
                }
            }
        }
    }

    Y_UNIT_TEST(SubplanDomainRebindsSourcesNotCaptureDefinitions) {
        TPlan plan;
        const auto parameter = plan.Id(), payload = plan.Id(), local = plan.Id(), other = plan.Id(), outer = plan.Id();
        auto caller = plan.Source({parameter, payload});
        auto body = plan.Capture(plan.Source({}), {{local, parameter}, {other, outer}});
        auto domain = MakeSubplanDomain(caller, {parameter}, plan.Pos, plan.Root.PlanProps);
        const auto key = domain.Keys.Items()[0].second;
        UNIT_ASSERT(key != parameter);
        UNIT_ASSERT(caller->GetOutputIUs() == (TUnorderedIUs{parameter, payload}));
        UNIT_ASSERT(domain.Input->GetOutputIUs() == TUnorderedIUs{key});
        auto bound = std::move(domain).Bind(std::move(body), plan.Pos);
        auto& join = CastOperator<TOpDependentJoin>(*bound);
        auto& rebound = CastOperator<TOpAddDependencies>(*join.GetInput());
        UNIT_ASSERT(rebound.GetDependencies().Keys() == (TUnorderedIUs{local, other}));
        UNIT_ASSERT_VALUES_EQUAL(rebound.GetDependencies().Find(local)->Outer, key);
        UNIT_ASSERT_VALUES_EQUAL(rebound.GetDependencies().Find(other)->Outer, outer);
        UNIT_ASSERT(join.Dependencies == TUnorderedIUs{key});
        auto& port = CastOperator<TOpReplicate>(*CastOperator<TOpAggregate>(join.GetDomain())->GetInput());
        UNIT_ASSERT(&port.GetReplicate() == &CastOperator<TOpReplicate>(*caller).GetReplicate());
    }

    // Each branch reads its own copy of the domain and of the shared producer,
    // as when the rules rebuilt every shared operator they pushed through.
    Y_UNIT_TEST(UnionBranchesGetDomainCopiesAndPrivateProducers) {
        for (const bool reverse : {false, true}) {
            TPlan plan({{0, "parameter"}, {0, "again"}});
            const auto parameter = plan.Id(), local = plan.Id(), value = plan.Id(), output = plan.Id();
            auto hub = TReplicate::Create(plan.Producer(parameter, local, value),
                plan.Pos, plan.Root.PlanProps.InfoUnitRegistry);
            auto first = hub->AddOutput(), second = hub->AddOutput();
            if (reverse) {
                first.Swap(second);
            }
            TUnionAllIUs columns(TUnionInputPolicy{2});
            columns.Add(output, TUnionInputRow{{PortId(*first, value), PortId(*second, value)}});
            plan.Attach(MakeIntrusive<TOpUnionAll>(std::move(first), std::move(second), plan.Pos, std::move(columns)),
                {parameter});

            plan.Rewrite();

            auto& merge = CastOperator<TOpUnionAll>((*plan.Root.GetInput()));
            const auto domain = plan.Root.GetColumns().Items()[0].first;
            UNIT_ASSERT(domain != parameter);
            UNIT_ASSERT_VALUES_EQUAL(plan.Root.GetColumns().Items()[1].first, domain);
            UNIT_ASSERT_VALUES_EQUAL(plan.Root.GetColumns().Items()[0].second, "parameter");
            UNIT_ASSERT_VALUES_EQUAL(plan.Root.GetColumns().Items()[1].second, "again");
            UNIT_ASSERT_VALUES_EQUAL(plan.CountKind(EOperator::AddDependencies), 0);
            UNIT_ASSERT_VALUES_EQUAL(plan.CountKind(EOperator::Join), 2);
            UNIT_ASSERT_VALUES_EQUAL(plan.CountKind(EOperator::Filter), 2);
            const auto* row = merge.GetColumns().Find(domain);
            UNIT_ASSERT(row && row->Inputs[0] != row->Inputs[1]);
            UNIT_ASSERT(!HasFreeCorrelation(plan.Root.GetInput(), {parameter}));
        }
    }

    Y_UNIT_TEST(JoinSidesGetNullSafeDomainPairs) {
        TPlan plan;
        const auto parameter = plan.Id(), local = plan.Id(), value = plan.Id();
        auto hub = TReplicate::Create(plan.Producer(parameter, local, value),
            plan.Pos, plan.Root.PlanProps.InfoUnitRegistry);
        auto left = hub->AddOutput(), right = hub->AddOutput();
        const auto rightValue = PortId(*right, value);
        auto predicate = MakeBinaryPredicate(">", plan.Column(value), plan.Column(rightValue));
        plan.Attach(MakeIntrusive<TOpJoin>(std::move(left), std::move(right), plan.Pos, "Cross",
            TPairedIUs{}, TVector<TExpression>{predicate}), {parameter});
        plan.Rewrite();
        auto& join = CastOperator<TOpJoin>((*plan.Root.GetInput()));
        UNIT_ASSERT_VALUES_EQUAL(join.JoinKind, "Inner");
        UNIT_ASSERT_VALUES_EQUAL(join.JoinKeys.Items().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(join.JoinFilters.size(), 1);
        UNIT_ASSERT(join.JoinFilters.front().GetExpressionBody()->IsCallable(">"));
        for (auto* side : join.GetChildren()) {
            const auto& map = CastOperator<TOpMap>(*side);
            UNIT_ASSERT_VALUES_EQUAL(map.GetMapElements().Keys().Size(), 1);
            const auto& entry = *map.GetMapElements().Items().begin();
            UNIT_ASSERT(entry.second.GetExpression().GetExpressionBody()->IsCallable("StablePickle"));
        }
        UNIT_ASSERT(!HasFreeCorrelation(plan.Root.GetInput(), {parameter}));
    }

    Y_UNIT_TEST(ScalarCountsRestoreZeroAndKeepOneResultDefinition) {
        TPlan plan;
        const auto parameter = plan.Id(), local = plan.Id(), value = plan.Id();
        const auto firstCount = plan.Id(), secondCount = plan.Id(), output = plan.Id();
        auto hub = TReplicate::Create(plan.Producer(parameter, local, value),
            plan.Pos, plan.Root.PlanProps.InfoUnitRegistry);
        auto first = hub->AddOutput(), second = hub->AddOutput();
        const auto secondValue = PortId(*second, value);
        TUnionAllIUs columns(TUnionInputPolicy{2});
        columns.Add(output, TUnionInputRow{{firstCount, secondCount}});
        auto left = plan.Count(std::move(first), value, firstCount);
        auto right = plan.Count(std::move(second), secondValue, secondCount);
        plan.Attach(MakeIntrusive<TOpUnionAll>(std::move(left), std::move(right), plan.Pos, std::move(columns)),
            {parameter});
        plan.Rewrite();
        TUnorderedIUs restored, partials;
        for (const auto& item : IterateSubtree(&plan.Root)) {
            if (item.Current->Kind == EOperator::Map) {
                for (const auto& [id, definition] : CastOperator<TOpMap>(*item.Current).GetMapElements().Items()) {
                    if (definition.GetExpression().GetExpressionBody()->IsCallable("Coalesce")) {
                        restored.Add(id);
                    }
                }
            } else if (item.Current->Kind == EOperator::Aggregate) {
                for (const auto& [id, traits] : CastOperator<TOpAggregate>(*item.Current).GetAggregationTraits().Items()) {
                    if (traits.AggFunction == "count") {
                        partials.Add(id);
                    }
                }
            }
        }
        UNIT_ASSERT(partials == (TUnorderedIUs{firstCount, secondCount}));
        UNIT_ASSERT_VALUES_EQUAL(restored.Size(), 2);
        UNIT_ASSERT(!partials.HasAny(restored));
        const auto& row = *CastOperator<TOpUnionAll>(*plan.Root.GetInput()).GetColumns().Find(output);
        UNIT_ASSERT(restored == (TUnorderedIUs{row.Inputs[0], row.Inputs[1]}));
        UNIT_ASSERT_VALUES_EQUAL(plan.CountKind(EOperator::Filter), 2);
        UNIT_ASSERT_VALUES_EQUAL(plan.CountKind(EOperator::Join), 4); // Per branch: domain cross join + restoration.
    }

    Y_UNIT_TEST(DistinctAllDefinesFreshDomainResults) {
        TPlan plan({{0, "parameter"}});
        const auto parameter = plan.Id(), local = plan.Id(), value = plan.Id(), output = plan.Id();
        TAggregationIUs functions;
        functions.Add(output, TOpAggregationTraits{value, "distinct"});
        auto aggregate = MakeIntrusive<TOpAggregate>(plan.Producer(parameter, local, value),
            std::move(functions), TOrderedIUs<>{value}, EOpPhase::Undefined, true, plan.Pos);
        plan.Attach(std::move(aggregate), {parameter});
        plan.Rewrite();
        auto& result = CastOperator<TOpAggregate>((*plan.Root.GetInput()));
        const auto domain = plan.Root.GetColumns().Items()[0].first;
        UNIT_ASSERT(result.IsDistinctAll());
        UNIT_ASSERT(domain != parameter);
        UNIT_ASSERT(result.GetAggregationTraits().Find(output));
        UNIT_ASSERT(result.GetAggregationTraits().Find(domain));
        UNIT_ASSERT_VALUES_EQUAL(result.GetAggregationTraits().Find(domain)->Input, parameter);
    }

    Y_UNIT_TEST(EqualityBoundDomainEliminatesOnlyBoundKeys) {
        for (const bool partial : {false, true}) {
            TPlan plan(partial ? TOrderedIUs<TString>{{0, "first"}, {1, "second"}}
                               : TOrderedIUs<TString>{{0, "first"}});
            const auto first = plan.Id(), second = plan.Id(), local = plan.Id(), value = plan.Id();
            plan.Attach(plan.Producer(first, local, value, "=="),
                partial ? TUnorderedIUs{first, second} : TUnorderedIUs{first});
            plan.Rewrite();
            UNIT_ASSERT_VALUES_EQUAL(plan.Root.GetColumns().Items()[0].first, local);
            if (partial) {
                auto& join = CastOperator<TOpJoin>(*plan.Root.GetInput());
                auto& domain = CastOperator<TOpAggregate>(*join.GetLeftInput());
                UNIT_ASSERT_VALUES_EQUAL(domain.GetKeyColumns().Items().size(), 1);
                UNIT_ASSERT(domain.GetOutputIUs().Contains(plan.Root.GetColumns().Items()[1].first));
            } else {
                UNIT_ASSERT_VALUES_EQUAL(plan.CountKind(EOperator::Join), 0);
                UNIT_ASSERT_VALUES_EQUAL(plan.CountKind(EOperator::Replicate), 0);
                auto& filter = CastOperator<TOpFilter>(*plan.Root.GetInput());
                UNIT_ASSERT(filter.GetFilterExpression().GetExpressionBody()->IsCallable("Exists"));
                auto& bindings = CastOperator<TOpMap>(*filter.GetInput());
                UNIT_ASSERT_VALUES_EQUAL(bindings.FindOutputElement(local)->GetColumnAccess(), value);
            }
        }
    }

    Y_UNIT_TEST(RepeatedCapturesReadTheDomainColumn) {
        TPlan plan({{1, "one"}, {2, "two"}});
        const auto parameter = plan.Id(), one = plan.Id(), two = plan.Id();
        plan.Attach(plan.Capture(plan.Source({}), {{one, parameter}, {two, parameter}}), {parameter});
        plan.Rewrite();
        UNIT_ASSERT(!HasFreeCorrelation(plan.Root.GetInput(), {parameter}));
        UNIT_ASSERT_VALUES_EQUAL(CastOperator<TOpJoin>(*plan.Root.GetInput()).JoinKind, "Cross");
        UNIT_ASSERT_VALUES_EQUAL(plan.Root.GetColumns().Items()[0].first, parameter);
        UNIT_ASSERT_VALUES_EQUAL(plan.Root.GetColumns().Items()[1].first, parameter);
    }

    Y_UNIT_TEST(NestedScopesLowerInsideOut) {
        TPlan plan;
        const auto parameter = plan.Id(), innerParameter = plan.Id(), innerLocal = plan.Id();
        auto domain = MakeDomainProjection(plan.Capture(plan.Source({}), {{innerParameter, parameter}}),
            {innerParameter}, plan.Pos);
        auto inner = MakeIntrusive<TOpDependentJoin>(std::move(domain),
            plan.Capture(plan.Source({}), {{innerLocal, innerParameter}}), TUnorderedIUs{innerParameter}, plan.Pos);
        UNIT_ASSERT(HasFreeCorrelation(inner, {parameter}));
        plan.Attach(std::move(inner), {parameter});
        plan.Rewrite();
        UNIT_ASSERT_VALUES_EQUAL(plan.CountKind(EOperator::AddDependencies), 0);
    }

    Y_UNIT_TEST(UnsupportedCorrelatedOperatorFails) {
        TPlan plan;
        const auto parameter = plan.Id(), local = plan.Id(), value = plan.Id();
        auto limit = MakeIntrusive<TOpLimit>(plan.Producer(parameter, local, value), plan.Pos,
            MakeConstant("Uint64", "1", plan.Pos, &plan.Ctx), EOpPhase::Undefined);
        plan.Attach(std::move(limit), {parameter});
        auto* old = plan.Root.GetInput().Get();
        UNIT_ASSERT_EXCEPTION_CONTAINS(plan.Decorrelate(), yexception, "Cannot decorrelate");
        UNIT_ASSERT(plan.Root.GetInput().Get() == old);
        UNIT_ASSERT(HasFreeCorrelation(CastOperator<TOpDependentJoin>(*old).GetInput(), {parameter}));
    }

    // Consumers outside the dependent join keep reading the original producer.
    Y_UNIT_TEST(CorrelatedReplicateIsCopiedForItsConsumer) {
        TPlan plan;
        const auto parameter = plan.Id(), local = plan.Id(), value = plan.Id();
        auto hub = TReplicate::Create(plan.Producer(parameter, local, value),
            plan.Pos, plan.Root.PlanProps.InfoUnitRegistry);
        auto inside = hub->AddOutput(), outside = hub->AddOutput();
        auto dependent = plan.DependentJoin(std::move(inside), {parameter});
        plan.Root.SetInput(MakeIntrusive<TOpJoin>(std::move(dependent), std::move(outside), plan.Pos, "Cross", TPairedIUs{}));
        plan.Decorrelate();
        UNIT_ASSERT_VALUES_EQUAL(plan.CountKind(EOperator::DependentJoin), 0);
        auto& join = CastOperator<TOpJoin>(*plan.Root.GetInput());
        UNIT_ASSERT(!HasFreeCorrelation(join.GetLeftInput(), {parameter}));
        UNIT_ASSERT(HasFreeCorrelation(join.GetRightInput(), {parameter}));
        UNIT_ASSERT(&CastOperator<TOpReplicate>(*join.GetRightInput()).GetReplicate() == hub.Get());
        UNIT_ASSERT(HasFreeCorrelation(hub->GetInput(), {parameter}));
    }

    Y_UNIT_TEST(SimpleInLeavesRepeatedCallsForTheMarkJoinPath) {
        for (const bool repeated : {false, true}) {
            NTests::TIdTestContext f;
            const auto lookup = f.Id(), value = f.Id(), call = f.Id();
            auto left = f.Read({lookup});
            auto right = f.Read({value});
            f.SetType(*left);
            f.SetType(*right);
            f.Props.Subplans.Add(call, std::move(right), ESubplanType::IN_SUBPLAN, {lookup}, value);
            auto predicate = f.Column(call);
            if (repeated) {
                predicate = MakeBinaryPredicate("And", predicate, MakeBinaryPredicate("==", predicate,
                    MakeConstant("Bool", "true", f.Pos, &f.ExprCtx)));
            }
            auto filter = MakeIntrusive<TOpFilter>(std::move(left), f.Pos, predicate);
            const auto* original = filter.get();
            TInlineSimpleInExistsSubplanRule simple;
            auto result = simple.SimpleMatchAndApply(std::move(filter), f.RboCtx, f.Props);
            if (repeated) {
                UNIT_ASSERT(result.get() == original);
                UNIT_ASSERT(f.Props.Subplans.Contains(call));
                TInlineGenericInExistsSubplanRule generic;
                result = generic.SimpleMatchAndApply(std::move(result), f.RboCtx, f.Props);
                UNIT_ASSERT(result->Kind == EOperator::Filter);
                UNIT_ASSERT(result->GetOutputIUs().Contains(call));
                UNIT_ASSERT(CastOperator<TOpFilter>(*result).GetFilterExpression().GetRawInputIUs().Contains(call));
            } else {
                UNIT_ASSERT_VALUES_EQUAL(CastOperator<TOpJoin>(*result).JoinKind, "LeftSemi");
                UNIT_ASSERT(result->GetOutputIUs() == TUnorderedIUs{lookup});
            }
            UNIT_ASSERT(!f.Props.Subplans.Contains(call));
            NTests::AssertIdInvariants(*result, f.Props);
        }
    }

    Y_UNIT_TEST(NullableNotInUsesDisjointMatchAndNullSummaryBranches) {
        for (const bool correlated : {false, true}) {
            NTests::TIdTestContext f;
            const auto lookup = f.Id(), value = f.Id(), call = f.Id();
            auto left = f.Read({lookup});
            f.SetType(*left, {lookup});
            TIntrusivePtr<IOperator> right = f.Read({value});
            if (correlated) {
                TDependencyIUs captures;
                captures.Add(f.Id(), TCapturedIU{lookup, left->GetIUType(lookup, f.ExprCtx)});
                right = MakeIntrusive<TOpAddDependencies>(std::move(right), f.Pos, std::move(captures));
            }
            f.SetType(*right, {value});
            f.Props.Subplans.Add(call, std::move(right), ESubplanType::IN_SUBPLAN, {lookup}, value);
            f.Props.Subplans.RefreshDependencies(call);
            auto predicate = MakeNegation(f.Column(call));
            auto filter = MakeIntrusive<TOpFilter>(std::move(left), f.Pos, predicate);
            const auto* original = filter.get();
            TInlineSimpleInExistsSubplanRule simple;
            auto result = simple.SimpleMatchAndApply(std::move(filter), f.RboCtx, f.Props);
            UNIT_ASSERT(result.get() == original);
            UNIT_ASSERT(f.Props.Subplans.Contains(call));
            TInlineGenericInExistsSubplanRule generic;
            result = generic.SimpleMatchAndApply(std::move(result), f.RboCtx, f.Props);
            UNIT_ASSERT(!f.Props.Subplans.Contains(call));
            auto& output = CastOperator<TOpMap>((*CastOperator<TOpFilter>(*result).GetInput()));
            UNIT_ASSERT(output.GetMapElements().Keys() == TUnorderedIUs{call});
            UNIT_ASSERT(output.GetOutputIUs().Contains(lookup));
            THashSet<const TReplicate*> hubs;
            for (const auto& item : IterateSubtree(result.get())) {
                if (item.Current->Kind == EOperator::Replicate) {
                    hubs.insert(&CastOperator<TOpReplicate>(*item.Current).GetReplicate());
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(hubs.size(), 2);
            NTests::AssertIdInvariants(*result, f.Props);
        }
    }
}

} // namespace NKikimr::NKqp

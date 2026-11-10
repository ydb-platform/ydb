#include <ydb/core/kqp/ut/rbo/kqp_rbo_test_helpers.h>

#include <ydb/core/kqp/opt/rbo/copy_logical_subtree.h>
#include <ydb/core/kqp/opt/rbo/rules/decorrelation/dependent_join_pushdown.h>

#include <yql/essentials/core/yql_expr_optimize.h>

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
        rules.emplace_back(std::make_unique<TPushDependentJoinThroughSortRule>());
        rules.emplace_back(std::make_unique<TPushDependentJoinThroughLimitRule>());
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

// A caller and a subplan that reads the caller's Outer column through Capture.
struct TPullUpFixture : NTests::TIdTestContext {
    const bool Nullable;
    const TInfoUnitId Outer = Id();
    const TInfoUnitId Inner = Id();
    const TInfoUnitId Capture = Id();
    const TInfoUnitId Lookup = Id();
    const TInfoUnitId Result = Id();
    const TInfoUnitId Call = Id();
    const TIntrusivePtr<IOperator> Left = Read({Outer, Lookup});
    const TIntrusivePtr<IOperator> Input = Read({Inner, Result});

    explicit TPullUpFixture(bool nullable = false)
        : Nullable(nullable)
    {
        SetTypes(*Left);
    }

    void SetTypes(IOperator& op) {
        SetType(op, Nullable ? op.GetOutputIUs() : TUnorderedIUs{});
    }

    TExpression Compare(const TString& comparison) {
        return MakeBinaryPredicate(comparison, Column(Inner), Column(Capture));
    }

    TIntrusivePtr<IOperator> Captured(TIntrusivePtr<IOperator> input, TInfoUnitId capture) {
        TDependencyIUs captures;
        captures.Add(capture, TCapturedIU{Outer, Left->GetIUType(Outer, ExprCtx)});
        return MakeIntrusive<TOpAddDependencies>(std::move(input), Pos, std::move(captures));
    }

    TIntrusivePtr<TOpFilter> Correlated(const TExpression& predicate, TIntrusivePtr<IOperator> input) {
        return MakeIntrusive<TOpFilter>(Captured(std::move(input), Capture), Pos, predicate);
    }

    TIntrusivePtr<TOpFilter> Correlated(const TExpression& predicate) {
        return Correlated(predicate, Input);
    }

    void AddExists(TIntrusivePtr<IOperator> body, TInfoUnitId call) {
        SetTypes(*body);
        Props.Subplans.Add(call, std::move(body), ESubplanType::EXISTS);
        Props.Subplans.RefreshDependencies(call);
    }

    void AddExists(TIntrusivePtr<IOperator> body) {
        AddExists(std::move(body), Call);
    }

    void AddIn(TIntrusivePtr<IOperator> body, TInfoUnitId result) {
        SetTypes(*body);
        Props.Subplans.Add(Call, std::move(body), ESubplanType::IN_SUBPLAN, {Lookup}, result);
        Props.Subplans.RefreshDependencies(Call);
    }

    void AddIn(TIntrusivePtr<IOperator> body) {
        AddIn(std::move(body), Result);
    }

    TIntrusivePtr<IOperator> PullUp(const TExpression& predicate) {
        auto result = TPullUpCorrelatedPredicateRule().SimpleMatchAndApply(MakeIntrusive<TOpFilter>(Left, Pos, predicate), RboCtx, Props);
        NTests::AssertIdInvariants(*result, Props);
        return result;
    }

    void AssertDeclined(const TExpression& predicate) {
        auto filter = MakeIntrusive<TOpFilter>(Left, Pos, predicate);
        const auto* original = filter.get();
        auto result = TPullUpCorrelatedPredicateRule().SimpleMatchAndApply(filter, RboCtx, Props);
        UNIT_ASSERT(result.get() == original);
        UNIT_ASSERT(Props.Subplans.Contains(Call));
    }
};

void AssertOnlyKey(const TOpJoin& join, TInfoUnitId caller, TInfoUnitId subplan) {
    UNIT_ASSERT_VALUES_EQUAL(join.JoinKeys.Items().size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(join.JoinKeys.Items()[0].first, caller);
    UNIT_ASSERT_VALUES_EQUAL(join.JoinKeys.Items()[0].second, subplan);
}

bool HasKey(const TOpJoin& join, TInfoUnitId caller, TInfoUnitId subplan) {
    return AnyOf(join.JoinKeys.Items(), [&](const auto& key) {
        return key.first == caller && key.second == subplan;
    });
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
        // The count reads a parameter: there is no common bound for the row numbers.
        auto limit = MakeIntrusive<TOpLimit>(plan.Producer(parameter, local, value), plan.Pos,
            plan.Column(local), EOpPhase::Undefined);
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

    Y_UNIT_TEST(CorrelatedReplicateRejectsUnsafeCopiesBeforeChangingInputs) {
        enum class EBlocker { Average, Random, SubplanCall };
        for (const auto blocker : {EBlocker::Average, EBlocker::Random, EBlocker::SubplanCall}) {
            TPlan plan;
            const auto parameter = plan.Id(), local = plan.Id(), value = plan.Id(), result = plan.Id();
            TIntrusivePtr<IOperator> producer = plan.Producer(parameter, local, value);
            if (blocker == EBlocker::Average) {
                TAggregationIUs functions;
                functions.Add(result, TOpAggregationTraits{value, "avg"});
                producer = MakeIntrusive<TOpAggregate>(producer, std::move(functions), TOrderedIUs<>{},
                    EOpPhase::Undefined, false, plan.Pos);
            } else if (blocker == EBlocker::Random) {
                producer = MakeIntrusive<TOpFilter>(producer, plan.Pos,
                    MakeBinaryPredicate(">", MakeUnaryCallable("RandomNumber", plan.Column(value)), plan.Column(local)));
            } else {
                const auto call = plan.Id(), inner = plan.Id();
                plan.Root.PlanProps.Subplans.Add(call, plan.Source({inner}), ESubplanType::EXPR, {}, inner);
                TMapIUs definitions;
                definitions.Add(result, plan.Column(call));
                producer = MakeIntrusive<TOpMap>(producer, plan.Pos, std::move(definitions));
            }
            const auto originalInput = producer->GetChild(0);
            auto hub = TReplicate::Create(producer, plan.Pos, plan.Root.PlanProps.InfoUnitRegistry);
            auto inside = hub->AddOutput(), outside = hub->AddOutput();
            auto dependent = plan.DependentJoin(inside, {parameter});
            plan.Root.SetInput(MakeIntrusive<TOpJoin>(dependent, outside, plan.Pos, "Cross", TJoinIUs{}));
            plan.Root.ComputeParents();
            const auto previousSize = plan.Root.PlanProps.InfoUnitRegistry.Size();
            TPushDependentJoinThroughReplicateRule rule;

            auto rewritten = rule.SimpleMatchAndApply(dependent, plan.Test.RboCtx, plan.Root.PlanProps);

            UNIT_ASSERT(rewritten == dependent);
            UNIT_ASSERT(hub->GetInput() == producer);
            UNIT_ASSERT(producer->GetChild(0) == originalInput);
            UNIT_ASSERT_VALUES_EQUAL(plan.Root.PlanProps.InfoUnitRegistry.Size(), previousSize);
            UNIT_ASSERT_VALUES_EQUAL(hub->GetOutputs().size(), 2);
        }
    }

    Y_UNIT_TEST(CorrelatedReplicateCopiesSumWithoutTypes) {
        TPlan plan;
        const auto parameter = plan.Id(), local = plan.Id(), value = plan.Id(), result = plan.Id();
        auto input = plan.Producer(parameter, local, value);
        TAggregationIUs functions;
        functions.Add(result, TOpAggregationTraits{value, "sum"});
        auto producer = MakeIntrusive<TOpAggregate>(input, std::move(functions), TOrderedIUs<>{},
            EOpPhase::Undefined, false, plan.Pos);
        auto hub = TReplicate::Create(producer, plan.Pos, plan.Root.PlanProps.InfoUnitRegistry);
        auto inside = hub->AddOutput(), outside = hub->AddOutput();
        auto dependent = plan.DependentJoin(inside, {parameter});
        plan.Root.SetInput(MakeIntrusive<TOpJoin>(dependent, outside, plan.Pos, "Cross", TJoinIUs{}));
        plan.Root.ComputeParents();
        UNIT_ASSERT(!input->Type);

        auto rewritten = TPushDependentJoinThroughReplicateRule().SimpleMatchAndApply(
            dependent, plan.Test.RboCtx, plan.Root.PlanProps);

        UNIT_ASSERT(rewritten != dependent);
        auto& copy = CastOperator<TOpAggregate>(*CastOperator<TOpDependentJoin>(*rewritten).GetInput());
        UNIT_ASSERT(&copy != producer.Get());
        UNIT_ASSERT(!copy.GetAggregationTraits().Keys().Contains(result));
        auto& originalPort = CastOperator<TOpReplicate>(*producer->GetInput());
        auto& copiedPort = CastOperator<TOpReplicate>(*copy.GetInput());
        UNIT_ASSERT(&originalPort.GetReplicate() == &copiedPort.GetReplicate());
        UNIT_ASSERT(originalPort.GetInput() == input);
    }

    Y_UNIT_TEST(ExistsPullUpTurnsPredicatesIntoJoinConditions) {
        for (const bool negated : {false, true}) {
            TPullUpFixture f;
            f.AddExists(f.Correlated(MakeConjunction({f.Compare("=="), f.Compare("!=")})));

            auto result = f.PullUp(negated ? MakeNegation(f.Column(f.Call)) : f.Column(f.Call));

            auto& join = CastOperator<TOpJoin>(*result);
            UNIT_ASSERT_VALUES_EQUAL(join.JoinKind, negated ? "LeftOnly" : "LeftSemi");
            UNIT_ASSERT(join.GetLeftInput() == f.Left);
            UNIT_ASSERT(join.GetRightInput() == f.Input);
            AssertOnlyKey(join, f.Outer, f.Inner);
            UNIT_ASSERT_VALUES_EQUAL(join.JoinFilters.size(), 1);
            UNIT_ASSERT(join.JoinFilters[0].GetRawInputIUs() == (TUnorderedIUs{f.Inner, f.Outer}));
            UNIT_ASSERT(!f.Props.Subplans.Contains(f.Call));
        }
    }

    Y_UNIT_TEST(ExistsPullUpOrientsJoinKeys) {
        TPullUpFixture f;
        f.AddExists(f.Correlated(MakeBinaryPredicate("==", f.Column(f.Capture), f.Column(f.Inner))));

        auto result = f.PullUp(f.Column(f.Call));

        AssertOnlyKey(CastOperator<TOpJoin>(*result), f.Outer, f.Inner);
    }

    Y_UNIT_TEST(ExistsPullUpKeepsLocalPredicatesOnTheInput) {
        TPullUpFixture f;
        f.AddExists(f.Correlated(MakeConjunction({f.Compare("=="), MakeBinaryPredicate(">", f.Column(f.Inner), f.Constant())})));

        auto result = f.PullUp(f.Column(f.Call));

        auto& join = CastOperator<TOpJoin>(*result);
        UNIT_ASSERT(join.JoinFilters.empty());
        auto& local = CastOperator<TOpFilter>(*join.GetRightInput());
        UNIT_ASSERT(local.GetInput() == f.Input);
        UNIT_ASSERT(local.GetFilterExpression().GetRawInputIUs() == TUnorderedIUs{f.Inner});
    }

    Y_UNIT_TEST(ExistsPullUpKeepsCallerOnlyPredicatesInTheJoin) {
        TPullUpFixture f;
        f.AddExists(f.Correlated(MakeConjunction({f.Compare("=="), MakeBinaryPredicate(">", f.Column(f.Capture), f.Constant())})));

        auto result = f.PullUp(f.Column(f.Call));

        auto& join = CastOperator<TOpJoin>(*result);
        UNIT_ASSERT(join.GetLeftInput() == f.Left);
        UNIT_ASSERT_VALUES_EQUAL(join.JoinFilters.size(), 1);
        UNIT_ASSERT(join.JoinFilters[0].GetRawInputIUs() == TUnorderedIUs{f.Outer});
    }

    Y_UNIT_TEST(ExistsPullUpLooksThroughMaps) {
        TPullUpFixture f;
        const auto alias = f.Id();
        auto map = f.Copies(f.Correlated(f.Compare("==")), {{alias, f.Inner}});
        f.AddExists(MakeIntrusive<TOpFilter>(map, f.Pos, MakeBinaryPredicate("!=", f.Column(alias), f.Column(f.Capture))));

        auto result = f.PullUp(f.Column(f.Call));

        auto& join = CastOperator<TOpJoin>(*result);
        AssertOnlyKey(join, f.Outer, f.Inner);
        UNIT_ASSERT_VALUES_EQUAL(join.JoinFilters.size(), 1);
        UNIT_ASSERT(join.JoinFilters[0].GetRawInputIUs() == (TUnorderedIUs{alias, f.Outer}));
        UNIT_ASSERT(CastOperator<TOpMap>(*join.GetRightInput()).GetInput() == f.Input);
    }

    Y_UNIT_TEST(ExistsPullUpUsesPlainKeysForNullableColumns) {
        TPullUpFixture f(/*nullable=*/true);
        f.AddExists(f.Correlated(MakeConjunction({f.Compare("=="), f.Compare("!=")})));

        auto result = f.PullUp(f.Column(f.Call));

        auto& join = CastOperator<TOpJoin>(*result);
        AssertOnlyKey(join, f.Outer, f.Inner);
        UNIT_ASSERT(!HasEqualNullsKey(join.JoinKeys));
    }

    Y_UNIT_TEST(ExistsPullUpKeepsTheUncorrelatedInputWhole) {
        TPullUpFixture f;
        const auto value = f.Id();
        TAggregationIUs functions;
        functions.Add(f.Id(), TOpAggregationTraits{value, "avg"});
        TOrderedIUs<> groupKeys;
        groupKeys.Append(f.Inner);
        TIntrusivePtr<IOperator> input = MakeIntrusive<TOpAggregate>(f.Read({f.Inner, value}), std::move(functions), std::move(groupKeys),
            EOpPhase::Undefined, false, f.Pos);
        // Nothing below the correlation is copied, so this input is not a barrier.
        UNIT_ASSERT(!CanDuplicateOperator(*input));
        f.AddExists(f.Correlated(f.Compare("=="), input));

        auto result = f.PullUp(f.Column(f.Call));

        UNIT_ASSERT(CastOperator<TOpJoin>(*result).GetRightInput() == input);
    }

    Y_UNIT_TEST(ExistsPullUpSkipsConjunctsItCannotRewrite) {
        TPullUpFixture f;
        const auto key = f.Id(), capture = f.Id(), rewritable = f.Id();
        f.AddExists(f.Correlated(f.Compare(">")));
        f.AddExists(MakeIntrusive<TOpFilter>(f.Captured(f.Read({key}), capture), f.Pos,
            MakeBinaryPredicate("==", f.Column(key), f.Column(capture))), rewritable);

        auto result = f.PullUp(MakeConjunction({f.Column(f.Call), f.Column(rewritable)}));

        auto& remaining = CastOperator<TOpFilter>(*result);
        UNIT_ASSERT(remaining.GetFilterExpression().GetRawInputIUs() == TUnorderedIUs{f.Call});
        AssertOnlyKey(CastOperator<TOpJoin>(*remaining.GetInput()), f.Outer, key);
        UNIT_ASSERT(f.Props.Subplans.Contains(f.Call));
        UNIT_ASSERT(!f.Props.Subplans.Contains(rewritable));
    }

    Y_UNIT_TEST(ExistsPullUpDeclinesWithoutEquality) {
        TPullUpFixture f;
        f.AddExists(f.Correlated(f.Compare(">")));
        f.AssertDeclined(f.Column(f.Call));
    }

    Y_UNIT_TEST(ExistsPullUpDeclinesCorrelationBelowLimit) {
        TPullUpFixture f;
        f.AddExists(MakeIntrusive<TOpLimit>(f.Correlated(f.Compare("==")), f.Pos, f.Constant(), EOpPhase::Undefined));
        f.AssertDeclined(f.Column(f.Call));
    }

    Y_UNIT_TEST(ExistsPullUpDeclinesCorrelationBelowAggregate) {
        TPullUpFixture f;
        TAggregationIUs functions;
        functions.Add(f.Id(), TOpAggregationTraits{f.Inner, "count"});
        f.AddExists(MakeIntrusive<TOpAggregate>(f.Correlated(f.Compare("==")), std::move(functions), TOrderedIUs<>{},
            EOpPhase::Undefined, false, f.Pos));
        f.AssertDeclined(f.Column(f.Call));
    }

    Y_UNIT_TEST(ExistsPullUpDeclinesVolatilePredicates) {
        TPullUpFixture f;
        f.AddExists(f.Correlated(MakeConjunction({f.Compare("=="),
            MakeBinaryPredicate(">", MakeUnaryCallable("Random", f.Column(f.Inner)), f.Constant())})));
        f.AssertDeclined(f.Column(f.Call));
    }

    Y_UNIT_TEST(ExistsPullUpDeclinesFiltersWithoutBlockHashJoin) {
        TPullUpFixture f;
        f.Config->SetUseBlockHashJoin(false);
        f.AddExists(f.Correlated(MakeConjunction({f.Compare("=="), f.Compare("!=")})));
        f.AssertDeclined(f.Column(f.Call));
    }

    Y_UNIT_TEST(ExistsPullUpDeclinesFiltersOnNonDataKeys) {
        TPullUpFixture f;
        TIntrusivePtr<IOperator> body = f.Correlated(MakeConjunction({f.Compare("=="), f.Compare("!=")}));
        f.AddExists(body);
        const auto* listType = f.ExprCtx.MakeType<TListExprType>(f.ExprCtx.MakeType<TDataExprType>(EDataSlot::Int32));
        TVector<const TItemExprType*> fields{f.ExprCtx.MakeType<TItemExprType>(f.ExprCtx.GetIndexAsString(f.Inner), listType)};
        body->Type = f.ExprCtx.MakeType<TListExprType>(f.ExprCtx.MakeType<TStructExprType>(fields));
        f.AssertDeclined(f.Column(f.Call));
    }

    Y_UNIT_TEST(ExistsPullUpDeclinesRepeatedCalls) {
        TPullUpFixture f;
        f.AddExists(f.Correlated(f.Compare("==")));
        f.AssertDeclined(MakeConjunction({f.Column(f.Call),
            MakeBinaryPredicate("==", f.Column(f.Call), MakeConstant("Bool", "true", f.Pos, &f.ExprCtx))}));
    }

    Y_UNIT_TEST(ExistsPullUpDeclinesUncorrelatedExists) {
        TPullUpFixture f;
        f.Props.Subplans.Add(f.Call, f.Input, ESubplanType::EXISTS);
        f.AssertDeclined(f.Column(f.Call));
    }

    Y_UNIT_TEST(InPullUpAddsTheLookupKey) {
        for (const bool negated : {false, true}) {
            TPullUpFixture f;
            f.AddIn(f.Correlated(f.Compare("==")));

            auto result = f.PullUp(negated ? MakeNegation(f.Column(f.Call)) : f.Column(f.Call));

            auto& join = CastOperator<TOpJoin>(*result);
            UNIT_ASSERT_VALUES_EQUAL(join.JoinKind, negated ? "LeftOnly" : "LeftSemi");
            UNIT_ASSERT(join.GetRightInput() == f.Input);
            UNIT_ASSERT_VALUES_EQUAL(join.JoinKeys.Items().size(), 2);
            UNIT_ASSERT(HasKey(join, f.Outer, f.Inner));
            UNIT_ASSERT(HasKey(join, f.Lookup, f.Result));
            UNIT_ASSERT(join.JoinFilters.empty());
            UNIT_ASSERT(!f.Props.Subplans.Contains(f.Call));
        }
    }

    Y_UNIT_TEST(InPullUpNeedsNoEqualityInTheSubplan) {
        TPullUpFixture f;
        f.AddIn(f.Correlated(f.Compare(">")));

        auto result = f.PullUp(f.Column(f.Call));

        auto& join = CastOperator<TOpJoin>(*result);
        AssertOnlyKey(join, f.Lookup, f.Result);
        UNIT_ASSERT_VALUES_EQUAL(join.JoinFilters.size(), 1);
        UNIT_ASSERT(join.JoinFilters[0].GetRawInputIUs() == (TUnorderedIUs{f.Inner, f.Outer}));
    }

    Y_UNIT_TEST(InPullUpAllowsNullableColumns) {
        TPullUpFixture f(/*nullable=*/true);
        f.AddIn(f.Correlated(f.Compare("==")));

        auto result = f.PullUp(f.Column(f.Call));

        auto& join = CastOperator<TOpJoin>(*result);
        UNIT_ASSERT_VALUES_EQUAL(join.JoinKind, "LeftSemi");
        UNIT_ASSERT(!HasEqualNullsKey(join.JoinKeys));
    }

    Y_UNIT_TEST(InPullUpDeclinesUncorrelatedSubplans) {
        TPullUpFixture f;
        f.Props.Subplans.Add(f.Call, f.Input, ESubplanType::IN_SUBPLAN, {f.Lookup}, f.Result);
        f.AssertDeclined(f.Column(f.Call));
    }

    Y_UNIT_TEST(InPullUpDeclinesResultReadingTheCaller) {
        TPullUpFixture f;
        const auto result = f.Id();
        f.AddIn(f.Copies(f.Correlated(f.Compare("==")), {{result, f.Capture}}), result);
        f.AssertDeclined(f.Column(f.Call));
    }

    Y_UNIT_TEST(NotInPullUpAllowsNullableCorrelationColumns) {
        TPullUpFixture f;
        f.SetType(*f.Left, {f.Outer});
        auto body = f.Correlated(f.Compare("=="));
        f.AddIn(body);
        f.SetType(*body, {f.Inner, f.Capture});

        auto result = f.PullUp(MakeNegation(f.Column(f.Call)));

        UNIT_ASSERT_VALUES_EQUAL(CastOperator<TOpJoin>(*result).JoinKind, "LeftOnly");
    }

    Y_UNIT_TEST(NotInPullUpDeclinesNullableLookup) {
        TPullUpFixture f;
        f.SetType(*f.Left, {f.Lookup});
        f.AddIn(f.Correlated(f.Compare("==")));
        f.AssertDeclined(MakeNegation(f.Column(f.Call)));
    }

    Y_UNIT_TEST(NotInPullUpDeclinesNullableResult) {
        TPullUpFixture f;
        auto body = f.Correlated(f.Compare("=="));
        f.AddIn(body);
        f.SetType(*body, {f.Result});
        f.AssertDeclined(MakeNegation(f.Column(f.Call)));
    }

    Y_UNIT_TEST(ExistsPullUpLeavesTheSubplanIntactForTheDependentJoin) {
        TPullUpFixture f;
        auto correlated = f.Correlated(f.Compare("=="));
        const auto captured = correlated->GetInput();
        // The map reads the capture, so the rule declines only after reaching the filter below it.
        f.AddExists(f.Copies(correlated, {{f.Id(), f.Capture}}));

        f.AssertDeclined(f.Column(f.Call));

        UNIT_ASSERT(correlated->GetInput() == captured);
        UNIT_ASSERT(correlated->GetFilterExpression().GetRawInputIUs() == (TUnorderedIUs{f.Inner, f.Capture}));

        auto result = TInlineSimpleInExistsSubplanRule().SimpleMatchAndApply(MakeIntrusive<TOpFilter>(f.Left, f.Pos, f.Column(f.Call)), f.RboCtx, f.Props);

        size_t dependentJoins = 0;
        for (const auto& item : IterateSubtree(result.get())) {
            dependentJoins += item.Current->Kind == EOperator::DependentJoin;
        }
        UNIT_ASSERT_VALUES_EQUAL(dependentJoins, 1);
        UNIT_ASSERT(!f.Props.Subplans.Contains(f.Call));
        NTests::AssertIdInvariants(*result, f.Props);
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

    Y_UNIT_TEST(GenericInExistsInlinesIntoMap) {
        for (const bool exists : {false, true}) {
            for (const bool correlated : {false, true}) {
                for (const bool nullable : {false, true}) {
                    NTests::TIdTestContext f;
                    const auto lookup = f.Id(), value = f.Id(), call = f.Id(), output = f.Id();
                    auto left = f.Read({lookup});
                    f.SetType(*left, nullable ? TUnorderedIUs{lookup} : TUnorderedIUs{});
                    TIntrusivePtr<IOperator> right = f.Read({value});
                    if (correlated) {
                        TDependencyIUs captures;
                        captures.Add(f.Id(), TCapturedIU{lookup, left->GetIUType(lookup, f.ExprCtx)});
                        right = MakeIntrusive<TOpAddDependencies>(std::move(right), f.Pos, std::move(captures));
                    }
                    f.SetType(*right, nullable ? TUnorderedIUs{value} : TUnorderedIUs{});
                    if (exists) {
                        f.Props.Subplans.Add(call, std::move(right), ESubplanType::EXISTS);
                    } else {
                        // A nullable lookup makes the query type the IN result Bool?.
                        f.Props.Subplans.Add(call, std::move(right), ESubplanType::IN_SUBPLAN, {lookup}, value, nullable);
                    }
                    f.Props.Subplans.RefreshDependencies(call);

                    // The projection returns the subplan result as a column.
                    TMapIUs elements;
                    elements.Add(output, f.Column(call));
                    TIntrusivePtr<IOperator> map = MakeIntrusive<TOpMap>(std::move(left), f.Pos, std::move(elements));
                    const auto* original = map.get();

                    TInlineGenericInExistsSubplanRule generic;
                    UNIT_ASSERT(generic.QuickMatch(map, f.Props));
                    auto result = generic.SimpleMatchAndApply(map, f.RboCtx, f.Props);

                    UNIT_ASSERT(result.get() != original);
                    UNIT_ASSERT(result->Kind == EOperator::Map);
                    const auto& newMap = CastOperator<TOpMap>(*result);
                    UNIT_ASSERT(newMap.GetMapElements().Keys() == TUnorderedIUs{output});
                    UNIT_ASSERT(newMap.GetInput()->GetOutputIUs().Contains(call));
                    UNIT_ASSERT(!f.Props.Subplans.Contains(call));

                    size_t dependentJoins = 0;
                    for (const auto& item : IterateSubtree(result.get())) {
                        dependentJoins += item.Current->Kind == EOperator::DependentJoin;
                    }
                    UNIT_ASSERT_VALUES_EQUAL(dependentJoins, correlated ? 1 : 0);

                    // Only a Bool? IN result can be NULL.
                    const TMapElement* definition = nullptr;
                    for (const auto& item : IterateSubtree(result.get())) {
                        if (item.Current != result.get() && item.Current->Kind == EOperator::Map) {
                            if (const auto* element = CastOperator<TOpMap>(*item.Current).FindOutputElement(call)) {
                                definition = element;
                            }
                        }
                    }
                    UNIT_ASSERT(definition);
                    const bool canBeNull = FindNode(definition->GetExpression().GetExpressionBody(),
                        [](const TExprNode::TPtr& node) { return node->IsCallable("Nothing"); }) != nullptr;
                    UNIT_ASSERT_VALUES_EQUAL(canBeNull, !exists && nullable);
                    NTests::AssertIdInvariants(*result, f.Props);
                }
            }
        }
    }
}

} // namespace NKikimr::NKqp

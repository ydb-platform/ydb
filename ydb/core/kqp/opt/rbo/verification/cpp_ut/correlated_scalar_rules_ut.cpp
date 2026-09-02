#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/opt/rbo/kqp_operator.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>

#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/time_provider/time_provider.h>

#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>

namespace {

using namespace NKikimr;
using namespace NKikimr::NKqp;
using namespace NYql;

struct TRuleTestContext {
    TRuleTestContext()
        : FuncRegistry(NKikimr::NMiniKQL::CreateFunctionRegistry(
              NKikimr::NMiniKQL::CreateBuiltinRegistry()))
        , Config(MakeIntrusive<TKikimrConfiguration>())
        , QueryCtx(MakeIntrusive<TKikimrQueryContext>(
              FuncRegistry.Get(),
              CreateDefaultTimeProvider(),
              CreateDefaultRandomProvider()))
        , Tables(MakeIntrusive<TKikimrTablesData>())
        , UserRequestContext(MakeIntrusive<TUserRequestContext>())
        , KqpCtx("ut", Config, QueryCtx, Tables, UserRequestContext)
        , RboCtx(KqpCtx, ExprCtx, TypeCtx, TypeAnnTransformer, *FuncRegistry)
    {
    }

    TExprContext ExprCtx;
    TTypeAnnotationContext TypeCtx;
    TNullTransformer TypeAnnTransformer;
    TIntrusivePtr<NKikimr::NMiniKQL::IFunctionRegistry> FuncRegistry;
    TIntrusivePtr<TKikimrConfiguration> Config;
    TIntrusivePtr<TKikimrQueryContext> QueryCtx;
    TIntrusivePtr<TKikimrTablesData> Tables;
    TIntrusivePtr<TUserRequestContext> UserRequestContext;
    NOpt::TKqpOptimizeContext KqpCtx;
    TRBOContext RboCtx;
    TPlanProps PlanProps;
};

const TTypeAnnotationNode* DataType(
    TRuleTestContext& ctx,
    NUdf::EDataSlot slot)
{
    return ctx.ExprCtx.MakeType<TDataExprType>(slot);
}

void SetOutputType(
    TRuleTestContext& ctx,
    IOperator& op,
    const TVector<std::pair<TInfoUnit, const TTypeAnnotationNode*>>& columns)
{
    TVector<const TItemExprType*> items;
    for (const auto& [column, type] : columns) {
        items.push_back(ctx.ExprCtx.MakeType<TItemExprType>(
            column.GetFullName(),
            type));
    }
    op.Type = ctx.ExprCtx.MakeType<TListExprType>(
        ctx.ExprCtx.MakeType<TStructExprType>(std::move(items)));
}

TIntrusivePtr<TOpRead> MakeRead(
    const TVector<TInfoUnit>& columns,
    TPositionHandle pos)
{
    TVector<TString> names;
    for (const auto& column : columns) {
        names.push_back(column.GetFullName());
    }
    return MakeIntrusive<TOpRead>(
        "",
        names,
        columns,
        NYql::EStorageType::RowStorage,
        nullptr,
        nullptr,
        nullptr,
        std::nullopt,
        std::nullopt,
        ESortDir::None,
        TPhysicalOpProps{},
        pos);
}

void ComputeParents(const TIntrusivePtr<IOperator>& input, TPositionHandle pos) {
    TOpRoot root(input, pos, {});
    root.ComputeParents();
}

TExpression MakeWindowColumnAccess(
    const TInfoUnit& column,
    TPositionHandle pos,
    TExprContext& exprCtx,
    TPlanProps& planProps)
{
    const auto access = MakeColumnAccess(
        column,
        pos,
        &exprCtx,
        &planProps);
    return TExpression(
        access.GetLambda(),
        &exprCtx,
        &planProps,
        exprCtx.NewAtom(pos, "window_definition"));
}

TExpression MakeDisjunction(
    TRuleTestContext& ctx,
    TPositionHandle pos,
    const TVector<TExpression>& disjuncts)
{
    Y_ENSURE(!disjuncts.empty());
    TExprNode::TListType bodies;
    bodies.reserve(disjuncts.size());
    for (const auto& disjunct : disjuncts) {
        bodies.push_back(disjunct.GetExpressionBody());
    }
    return TExpression(
        ctx.ExprCtx.NewCallable(pos, "Or", std::move(bodies)),
        &ctx.ExprCtx,
        &ctx.PlanProps);
}

struct TCorrelatedCountFixture {
    explicit TCorrelatedCountFixture(
        bool grouped = false,
        bool computedResult = false)
        : Int32(DataType(Ctx, NUdf::EDataSlot::Int32))
        , Uint64(DataType(Ctx, NUdf::EDataSlot::Uint64))
        , OuterRead(MakeRead({OuterKey}, Pos))
        , InnerRead(MakeRead({InnerKey, InnerValue, InnerGroup}, Pos))
    {
        SetOutputType(Ctx, *OuterRead, {{OuterKey, Int32}});
        SetOutputType(Ctx, *InnerRead, {
            {InnerKey, Int32},
            {InnerValue, Int32},
            {InnerGroup, Int32},
        });

        AddDependencies = MakeIntrusive<TOpAddDependencies>(
            InnerRead,
            Pos,
            TVector<std::pair<TInfoUnit, const TTypeAnnotationNode*>>{{
                OuterKey,
                Int32,
            }});
        auto equality = MakeBinaryPredicate(
            "==",
            MakeColumnAccess(
                InnerKey,
                Pos,
                &Ctx.ExprCtx,
                &Ctx.PlanProps),
            MakeColumnAccess(
                OuterKey,
                Pos,
                &Ctx.ExprCtx,
                &Ctx.PlanProps));
        CorrelationFilter = MakeIntrusive<TOpFilter>(
            AddDependencies,
            Pos,
            equality);

        Aggregate = MakeIntrusive<TOpAggregate>(
            CorrelationFilter,
            TVector<TOpAggregationTraits>{TOpAggregationTraits(
                InnerValue,
                "count",
                CountResult)},
            grouped ? TVector<TInfoUnit>{InnerGroup} : TVector<TInfoUnit>{},
            EOpPhase::Undefined,
            false,
            Pos);

        TExpression resultExpression;
        if (computedResult) {
            const auto count = MakeColumnAccess(
                CountResult,
                Pos,
                &Ctx.ExprCtx,
                &Ctx.PlanProps);
            const auto one = MakeConstant(
                "Uint64",
                "1",
                Pos,
                &Ctx.ExprCtx);
            resultExpression = TExpression(
                Ctx.ExprCtx.NewCallable(
                    Pos,
                    "Add",
                    {count.GetExpressionBody(), one.GetExpressionBody()}),
                &Ctx.ExprCtx,
                &Ctx.PlanProps);
        } else {
            resultExpression = MakeColumnAccess(
                CountResult,
                Pos,
                &Ctx.ExprCtx,
                &Ctx.PlanProps);
        }
        ResultMap = MakeIntrusive<TOpMap>(
            Aggregate,
            Pos,
            TVector<TMapElement>{TMapElement(
                ScalarResult,
                resultExpression,
                !computedResult)});
        Subplan = ResultMap;
    }

    void PullUpCorrelation() {
        ComputeParents(Subplan, Pos);

        TIntrusivePtr<IOperator> aggregate = Aggregate;
        TPullUpCorrelatedFilterRule rule;
        UNIT_ASSERT(rule.MatchAndApply(
            aggregate,
            Ctx.RboCtx,
            Ctx.PlanProps));
        ResultMap->SetInput(aggregate);

        Subplan = ResultMap;
        ComputeParents(Subplan, Pos);
        UNIT_ASSERT(rule.MatchAndApply(
            Subplan,
            Ctx.RboCtx,
            Ctx.PlanProps));

        RefreshTypes();
    }

    TIntrusivePtr<TOpMap> MakeProjectionConsumer() {
        return MakeIntrusive<TOpMap>(
            OuterRead,
            Pos,
            TVector<TMapElement>{TMapElement(
                Output,
                MakeColumnAccess(
                    Binding,
                    Pos,
                    &Ctx.ExprCtx,
                    &Ctx.PlanProps))});
    }

    TIntrusivePtr<TOpFilter> MakeFilterConsumer() {
        return MakeIntrusive<TOpFilter>(
            OuterRead,
            Pos,
            MakeBinaryPredicate(
                "==",
                MakeColumnAccess(
                    Binding,
                    Pos,
                    &Ctx.ExprCtx,
                    &Ctx.PlanProps),
                MakeConstant("Uint64", "0", Pos, &Ctx.ExprCtx)));
    }

    void RegisterSubplan() {
        Ctx.PlanProps.Subplans.Add(
            Binding,
            TSubplanEntry{
                Subplan,
                {},
                ESubplanType::EXPR,
                Binding,
                {OuterKey}});
    }

    void RefreshTypes() {
        TVector<std::pair<TInfoUnit, const TTypeAnnotationNode*>>
            aggregateColumns;
        for (const auto& key : Aggregate->KeyColumns) {
            aggregateColumns.emplace_back(key, Int32);
        }
        aggregateColumns.emplace_back(CountResult, Uint64);
        SetOutputType(Ctx, *Aggregate, aggregateColumns);

        auto mapColumns = aggregateColumns;
        if (ResultMap->MapElements.front().IsRename()) {
            std::erase_if(
                mapColumns,
                [&](const auto& column) {
                    return column.first == CountResult;
                });
        }
        mapColumns.emplace_back(ScalarResult, Uint64);
        SetOutputType(Ctx, *ResultMap, mapColumns);

        auto dependentColumns = mapColumns;
        dependentColumns.emplace_back(OuterKey, Int32);
        SetOutputType(Ctx, *AddDependencies, dependentColumns);
        SetOutputType(Ctx, *CorrelationFilter, dependentColumns);
    }

    TRuleTestContext Ctx;
    const TPositionHandle Pos;
    const TInfoUnit OuterKey{"outer.k"};
    const TInfoUnit InnerKey{"inner.k"};
    const TInfoUnit InnerValue{"inner.value"};
    const TInfoUnit InnerGroup{"inner.group"};
    const TInfoUnit CountResult{"count.value"};
    const TInfoUnit ScalarResult{"scalar.value"};
    const TInfoUnit Binding{"_rbo_scalar", true};
    const TInfoUnit Output{"result"};
    const TTypeAnnotationNode* const Int32;
    const TTypeAnnotationNode* const Uint64;
    TIntrusivePtr<TOpRead> OuterRead;
    TIntrusivePtr<TOpRead> InnerRead;
    TIntrusivePtr<TOpAddDependencies> AddDependencies;
    TIntrusivePtr<TOpFilter> CorrelationFilter;
    TIntrusivePtr<TOpAggregate> Aggregate;
    TIntrusivePtr<TOpMap> ResultMap;
    TIntrusivePtr<IOperator> Subplan;
};

TExpression MakeColumnComparison(
    TCorrelatedCountFixture& fixture,
    TStringBuf callable,
    const TInfoUnit& left,
    const TInfoUnit& right)
{
    return MakeBinaryPredicate(
        TString(callable),
        MakeColumnAccess(
            left,
            fixture.Pos,
            &fixture.Ctx.ExprCtx,
            &fixture.Ctx.PlanProps),
        MakeColumnAccess(
            right,
            fixture.Pos,
            &fixture.Ctx.ExprCtx,
            &fixture.Ctx.PlanProps));
}

TExpression MakeInt32Comparison(
    TCorrelatedCountFixture& fixture,
    TStringBuf callable,
    const TInfoUnit& column,
    TStringBuf value)
{
    return MakeBinaryPredicate(
        TString(callable),
        MakeColumnAccess(
            column,
            fixture.Pos,
            &fixture.Ctx.ExprCtx,
            &fixture.Ctx.PlanProps),
        MakeConstant(
            "Int32",
            TString(value),
            fixture.Pos,
            &fixture.Ctx.ExprCtx));
}

void AssertAggregatePullupRejectedWithoutMutation(
    TCorrelatedCountFixture& fixture)
{
    TIntrusivePtr<IOperator> aggregate = fixture.Aggregate;
    ComputeParents(aggregate, fixture.Pos);

    TPullUpCorrelatedFilterRule rule;
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        rule.MatchAndApply(
            aggregate,
            fixture.Ctx.RboCtx,
            fixture.Ctx.PlanProps),
        yexception,
        "Only equi-join conditions");

    UNIT_ASSERT_VALUES_EQUAL(aggregate.Get(), fixture.Aggregate.Get());
    UNIT_ASSERT_VALUES_EQUAL(
        fixture.Aggregate->GetInput().Get(),
        fixture.CorrelationFilter.Get());
    UNIT_ASSERT_VALUES_EQUAL(
        fixture.CorrelationFilter->GetInput().Get(),
        fixture.AddDependencies.Get());
    UNIT_ASSERT_VALUES_EQUAL(
        fixture.AddDependencies->GetInput().Get(),
        fixture.InnerRead.Get());
    UNIT_ASSERT(
        fixture.CorrelationFilter->FilterExpr
            .GetExpressionBody()
            ->IsCallable("Or"));
    UNIT_ASSERT(fixture.Aggregate->KeyColumns.empty());
    UNIT_ASSERT(!fixture.Aggregate->WasKeylessBeforeCorrelation);
}

TIntrusivePtr<TOpMap> ApplyProjectionInline(
    TCorrelatedCountFixture& fixture)
{
    fixture.RegisterSubplan();
    auto consumer = fixture.MakeProjectionConsumer();
    TIntrusivePtr<IOperator> input = consumer;

    TInlineScalarSubplanRule rule;
    UNIT_ASSERT(rule.MatchAndApply(
        input,
        fixture.Ctx.RboCtx,
        fixture.Ctx.PlanProps));
    UNIT_ASSERT_VALUES_EQUAL(input.Get(), consumer.Get());
    UNIT_ASSERT(!fixture.Ctx.PlanProps.Subplans.PlanMap.contains(
        fixture.Binding));
    return consumer;
}

const TExprNode* AssertOptionalCountRepair(const TMapElement& element) {
    const auto body = element.GetExpression().GetExpressionBody();
    UNIT_ASSERT(body->IsCallable("Just"));
    UNIT_ASSERT_VALUES_EQUAL(body->ChildrenSize(), 1);

    const auto* coalesce = body->Child(0);
    UNIT_ASSERT(coalesce->IsCallable("Coalesce"));
    UNIT_ASSERT_VALUES_EQUAL(coalesce->ChildrenSize(), 2);
    UNIT_ASSERT(coalesce->Child(0)->IsCallable("Member"));
    UNIT_ASSERT_VALUES_EQUAL(coalesce->Child(0)->ChildrenSize(), 2);
    UNIT_ASSERT(coalesce->Child(0)->Child(1)->IsAtom());
    UNIT_ASSERT(coalesce->Child(1)->IsCallable("Uint64"));
    UNIT_ASSERT_VALUES_EQUAL(coalesce->Child(1)->ChildrenSize(), 1);
    UNIT_ASSERT_VALUES_EQUAL(coalesce->Child(1)->Child(0)->Content(), "0");
    return coalesce->Child(0);
}

Y_UNIT_TEST_SUITE(KqpRboCorrelatedScalarRules) {
    Y_UNIT_TEST(DoesNotPullCorrelatedFilterAcrossWindowProjection) {
        TRuleTestContext ctx;
        const auto pos = TPositionHandle();
        const TInfoUnit outerKey("outer.k");
        const TInfoUnit innerKey("inner.k");
        const TInfoUnit innerValue("inner.value");
        const TInfoUnit windowValue("window.value");
        const auto* int32 = DataType(ctx, NUdf::EDataSlot::Int32);

        auto read = MakeRead({innerKey, innerValue}, pos);
        auto dependencies = MakeIntrusive<TOpAddDependencies>(
            read,
            pos,
            TVector<std::pair<TInfoUnit, const TTypeAnnotationNode*>>{{
                outerKey,
                int32,
            }});
        auto equality = MakeBinaryPredicate(
            "==",
            MakeColumnAccess(
                innerKey,
                pos,
                &ctx.ExprCtx,
                &ctx.PlanProps),
            MakeColumnAccess(
                outerKey,
                pos,
                &ctx.ExprCtx,
                &ctx.PlanProps));
        auto filter = MakeIntrusive<TOpFilter>(
            dependencies,
            pos,
            equality);
        auto map = MakeIntrusive<TOpMap>(
            filter,
            pos,
            TVector<TMapElement>{TMapElement(
                windowValue,
                MakeWindowColumnAccess(
                    innerValue,
                    pos,
                    ctx.ExprCtx,
                    ctx.PlanProps))});
        TIntrusivePtr<IOperator> input = map;
        ComputeParents(input, pos);

        TPullUpCorrelatedFilterRule rule;
        UNIT_ASSERT(!rule.MatchAndApply(
            input,
            ctx.RboCtx,
            ctx.PlanProps));

        UNIT_ASSERT_VALUES_EQUAL(input.Get(), map.Get());
        UNIT_ASSERT_VALUES_EQUAL(map->GetInput().Get(), filter.Get());
        UNIT_ASSERT_VALUES_EQUAL(
            filter->GetInput().Get(),
            dependencies.Get());
        UNIT_ASSERT_VALUES_EQUAL(dependencies->GetInput().Get(), read.Get());
    }

    Y_UNIT_TEST(PullupFactorsCommonAggregateCorrelationFromOrBranches) {
        TCorrelatedCountFixture fixture;

        auto firstEquality = MakeColumnComparison(
            fixture, "==", fixture.InnerKey, fixture.OuterKey);
        auto secondEquality = MakeColumnComparison(
            fixture, "==", fixture.InnerKey, fixture.OuterKey);
        auto firstResidual = MakeInt32Comparison(
            fixture, ">", fixture.InnerValue, "0");
        auto secondResidual = MakeInt32Comparison(
            fixture, "<", fixture.InnerGroup, "10");
        const auto originalPredicate = MakeDisjunction(
            fixture.Ctx,
            fixture.Pos,
            {
                MakeConjunction({firstEquality, firstResidual}),
                MakeConjunction({secondEquality, secondResidual}),
            });
        fixture.CorrelationFilter->FilterExpr = originalPredicate;

        TIntrusivePtr<IOperator> aggregate = fixture.Aggregate;
        ComputeParents(aggregate, fixture.Pos);

        TPullUpCorrelatedFilterRule rule;
        UNIT_ASSERT(rule.MatchAndApply(
            aggregate,
            fixture.Ctx.RboCtx,
            fixture.Ctx.PlanProps));

        UNIT_ASSERT_VALUES_EQUAL(
            aggregate.Get(),
            fixture.CorrelationFilter.Get());
        UNIT_ASSERT_VALUES_EQUAL(
            fixture.CorrelationFilter->GetInput().Get(),
            fixture.AddDependencies.Get());
        UNIT_ASSERT_VALUES_EQUAL(
            fixture.AddDependencies->GetInput().Get(),
            fixture.Aggregate.Get());
        UNIT_ASSERT(fixture.Aggregate->GetInput()->Kind == EOperator::Filter);
        auto residualFilter = CastOperator<TOpFilter>(
            fixture.Aggregate->GetInput());
        UNIT_ASSERT_VALUES_EQUAL(
            residualFilter->GetInput().Get(),
            fixture.InnerRead.Get());

        UNIT_ASSERT(
            fixture.CorrelationFilter->FilterExpr
                .GetExpressionBody()
                ->IsCallable("=="));
        UNIT_ASSERT(
            residualFilter->FilterExpr
                .GetExpressionBody()
                ->IsCallable("Or"));
        UNIT_ASSERT(
            originalPredicate.GetExpressionBody()->IsCallable("Or"));

        const auto& pulledIUs =
            fixture.CorrelationFilter->FilterExpr.GetInputIUs(true, true);
        UNIT_ASSERT_VALUES_EQUAL(pulledIUs.size(), 2);
        UNIT_ASSERT(std::find(
            pulledIUs.begin(),
            pulledIUs.end(),
            fixture.InnerKey) != pulledIUs.end());
        UNIT_ASSERT(std::find(
            pulledIUs.begin(),
            pulledIUs.end(),
            fixture.OuterKey) != pulledIUs.end());

        const auto& residualIUs =
            residualFilter->FilterExpr.GetInputIUs(true, true);
        UNIT_ASSERT_VALUES_EQUAL(residualIUs.size(), 2);
        UNIT_ASSERT(std::find(
            residualIUs.begin(),
            residualIUs.end(),
            fixture.InnerValue) != residualIUs.end());
        UNIT_ASSERT(std::find(
            residualIUs.begin(),
            residualIUs.end(),
            fixture.InnerGroup) != residualIUs.end());
        UNIT_ASSERT(std::find(
            residualIUs.begin(),
            residualIUs.end(),
            fixture.OuterKey) == residualIUs.end());

        UNIT_ASSERT(fixture.Aggregate->WasKeylessBeforeCorrelation);
        UNIT_ASSERT(
            fixture.Aggregate->KeyColumns ==
            TVector<TInfoUnit>{fixture.InnerKey});
    }

    Y_UNIT_TEST(PullupRejectsOrBranchesWithDifferentCorrelationsWithoutMutation) {
        TCorrelatedCountFixture fixture;

        auto firstEquality = MakeColumnComparison(
            fixture, "==", fixture.InnerKey, fixture.OuterKey);
        auto secondEquality = MakeColumnComparison(
            fixture, "==", fixture.InnerGroup, fixture.OuterKey);
        auto firstResidual = MakeInt32Comparison(
            fixture, ">", fixture.InnerValue, "0");
        auto secondResidual = MakeInt32Comparison(
            fixture, "<", fixture.InnerValue, "10");
        fixture.CorrelationFilter->FilterExpr = MakeDisjunction(
            fixture.Ctx,
            fixture.Pos,
            {
                MakeConjunction({firstEquality, firstResidual}),
                MakeConjunction({secondEquality, secondResidual}),
            });
        AssertAggregatePullupRejectedWithoutMutation(fixture);
    }

    Y_UNIT_TEST(PullupRejectsFactoredDependencyInResidualWithoutMutation) {
        TCorrelatedCountFixture fixture;

        auto firstEquality = MakeColumnComparison(
            fixture, "==", fixture.InnerKey, fixture.OuterKey);
        auto secondEquality = MakeColumnComparison(
            fixture, "==", fixture.InnerKey, fixture.OuterKey);
        auto localResidual = MakeInt32Comparison(
            fixture, ">", fixture.InnerValue, "0");
        auto dependentResidual = MakeColumnComparison(
            fixture, ">", fixture.InnerGroup, fixture.OuterKey);
        fixture.CorrelationFilter->FilterExpr = MakeDisjunction(
            fixture.Ctx,
            fixture.Pos,
            {
                MakeConjunction({firstEquality, localResidual}),
                MakeConjunction({secondEquality, dependentResidual}),
            });
        AssertAggregatePullupRejectedWithoutMutation(fixture);
    }

    Y_UNIT_TEST(PullupMarksOriginallyKeylessAggregate) {
        TCorrelatedCountFixture fixture;
        fixture.PullUpCorrelation();

        UNIT_ASSERT(fixture.Aggregate->WasKeylessBeforeCorrelation);
        UNIT_ASSERT(
            fixture.Aggregate->KeyColumns ==
            TVector<TInfoUnit>{fixture.InnerKey});
    }

    Y_UNIT_TEST(PullupDoesNotMarkOriginallyGroupedAggregate) {
        TCorrelatedCountFixture fixture(/* grouped */ true);
        fixture.PullUpCorrelation();

        UNIT_ASSERT(!fixture.Aggregate->WasKeylessBeforeCorrelation);
        UNIT_ASSERT(
            fixture.Aggregate->KeyColumns ==
            (TVector<TInfoUnit>{
                fixture.InnerGroup,
                fixture.InnerKey,
            }));
    }

    Y_UNIT_TEST(InlineDirectCountAddsExactOptionalZeroRepair) {
        TCorrelatedCountFixture fixture;
        fixture.PullUpCorrelation();
        auto consumer = ApplyProjectionInline(fixture);

        UNIT_ASSERT(consumer->GetInput()->Kind == EOperator::Map);
        auto scalarRename = CastOperator<TOpMap>(consumer->GetInput());
        UNIT_ASSERT_VALUES_EQUAL(scalarRename->MapElements.size(), 1);
        UNIT_ASSERT(scalarRename->MapElements.front().IsRename());

        UNIT_ASSERT(scalarRename->GetInput()->Kind == EOperator::Map);
        auto repair = CastOperator<TOpMap>(scalarRename->GetInput());
        UNIT_ASSERT_VALUES_EQUAL(repair->MapElements.size(), 1);
        const auto* countMember =
            AssertOptionalCountRepair(repair->MapElements.front());
        UNIT_ASSERT_VALUES_EQUAL(
            countMember->Child(1)->Content(),
            fixture.ScalarResult.GetFullName());

        UNIT_ASSERT(repair->GetInput()->Kind == EOperator::Join);
        auto join = CastOperator<TOpJoin>(repair->GetInput());
        UNIT_ASSERT_VALUES_EQUAL(join->JoinKind, "Left");
        UNIT_ASSERT(
            join->JoinKeys ==
            (TVector<std::pair<TInfoUnit, TInfoUnit>>{{
                fixture.OuterKey,
                fixture.InnerKey,
            }}));
    }

    Y_UNIT_TEST(InlineCountRepairsFilterConsumer) {
        TCorrelatedCountFixture fixture;
        fixture.PullUpCorrelation();
        fixture.RegisterSubplan();
        auto consumer = fixture.MakeFilterConsumer();
        TIntrusivePtr<IOperator> input = consumer;

        TInlineScalarSubplanRule rule;
        UNIT_ASSERT(rule.MatchAndApply(
            input,
            fixture.Ctx.RboCtx,
            fixture.Ctx.PlanProps));
        UNIT_ASSERT(!fixture.Ctx.PlanProps.Subplans.PlanMap.contains(
            fixture.Binding));

        UNIT_ASSERT(consumer->GetInput()->Kind == EOperator::Map);
        auto repair = CastOperator<TOpMap>(consumer->GetInput());
        UNIT_ASSERT_VALUES_EQUAL(repair->MapElements.size(), 1);
        AssertOptionalCountRepair(repair->MapElements.front());
        UNIT_ASSERT(repair->GetInput()->Kind == EOperator::Join);

        const auto repairedIU =
            repair->MapElements.front().GetElementName();
        const auto usedIUs =
            consumer->FilterExpr.GetInputIUs(true, true);
        UNIT_ASSERT_VALUES_EQUAL(usedIUs.size(), 1);
        UNIT_ASSERT(usedIUs.front() == repairedIU);
        UNIT_ASSERT(!(usedIUs.front() == fixture.Binding));
    }

    Y_UNIT_TEST(ComputedCountProjectionFailsBeforeRemovingSubplan) {
        TCorrelatedCountFixture fixture(
            /* grouped */ false,
            /* computedResult */ true);
        fixture.PullUpCorrelation();
        fixture.RegisterSubplan();
        auto consumer = fixture.MakeProjectionConsumer();
        const auto originalChild = consumer->GetInput();
        TIntrusivePtr<IOperator> input = consumer;

        TInlineScalarSubplanRule rule;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            rule.MatchAndApply(
                input,
                fixture.Ctx.RboCtx,
                fixture.Ctx.PlanProps),
            yexception,
            "Computed correlated scalar aggregate results");
        UNIT_ASSERT_VALUES_EQUAL(consumer->GetInput().Get(), originalChild.Get());
        UNIT_ASSERT(fixture.Ctx.PlanProps.Subplans.PlanMap.contains(
            fixture.Binding));
        UNIT_ASSERT_VALUES_EQUAL(
            fixture.Ctx.PlanProps.Subplans.PlanMap.at(fixture.Binding).Plan.Get(),
            fixture.Subplan.Get());
    }

    Y_UNIT_TEST(OriginallyGroupedCountKeepsOrdinaryLeftJoin) {
        TCorrelatedCountFixture fixture(/* grouped */ true);
        fixture.PullUpCorrelation();
        auto consumer = ApplyProjectionInline(fixture);

        UNIT_ASSERT(consumer->GetInput()->Kind == EOperator::Map);
        auto scalarRename = CastOperator<TOpMap>(consumer->GetInput());
        UNIT_ASSERT_VALUES_EQUAL(scalarRename->MapElements.size(), 1);
        UNIT_ASSERT(scalarRename->GetInput()->Kind == EOperator::Join);

        auto join = CastOperator<TOpJoin>(scalarRename->GetInput());
        UNIT_ASSERT_VALUES_EQUAL(join->JoinKind, "Left");
        UNIT_ASSERT(
            scalarRename->MapElements.front().GetRename() ==
            fixture.ScalarResult);
    }
}

} // namespace

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
              FuncRegistry.Get(), CreateDefaultTimeProvider(), CreateDefaultRandomProvider()))
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

TIntrusivePtr<TOpRead> MakeRead(
    NYql::EStorageType storage,
    TPositionHandle pos,
    const TPhysicalOpProps& props = {},
    const TString& column = "a")
{
    return MakeIntrusive<TOpRead>(
        "",
        TVector<TString>{column},
        TVector<TInfoUnit>{TInfoUnit(column)},
        storage,
        nullptr,
        nullptr,
        nullptr,
        std::nullopt,
        std::nullopt,
        ESortDir::None,
        props,
        pos);
}

void ComputeParents(
    const TIntrusivePtr<IOperator>& left,
    const TIntrusivePtr<IOperator>& right,
    TPositionHandle pos)
{
    auto unionAll = MakeIntrusive<TOpUnionAll>(
        left,
        right,
        pos,
        TVector<TInfoUnit>{TInfoUnit("a")});
    TOpRoot root(unionAll, pos, {"a"});
    root.ComputeParents();
}

Y_UNIT_TEST_SUITE(KqpRboLimitPushdownRules) {
    Y_UNIT_TEST(DoesNotPushIntermediateLimitIntoSharedRead) {
        TRuleTestContext ctx;
        const auto pos = TPositionHandle();

        TPhysicalOpProps stageProps;
        stageProps.StageId = 1;
        auto read = MakeRead(NYql::EStorageType::ColumnStorage, pos, stageProps);
        auto limit = MakeIntrusive<TOpLimit>(
            read,
            pos,
            stageProps,
            MakeConstant("Uint64", "1", pos, &ctx.ExprCtx),
            EOpPhase::Intermediate);
        auto otherConsumer = MakeIntrusive<TOpLimit>(
            read,
            pos,
            MakeConstant("Uint64", "2", pos, &ctx.ExprCtx),
            EOpPhase::Final);
        ComputeParents(limit, otherConsumer, pos);

        UNIT_ASSERT_VALUES_EQUAL(read->Parents.size(), 2);
        UNIT_ASSERT(!read->Limit);

        TPropagateLimitThroughStageRule rule;
        const auto result =
            rule.SimpleMatchAndApply(limit, ctx.RboCtx, ctx.PlanProps);

        UNIT_ASSERT_VALUES_EQUAL(result.Get(), limit.Get());
        UNIT_ASSERT(!read->Limit);
        UNIT_ASSERT_VALUES_EQUAL(read->Parents.size(), 2);
    }

    Y_UNIT_TEST(DoesNotPushLimitIntoSharedSort) {
        TRuleTestContext ctx;
        const auto pos = TPositionHandle();

        auto read = MakeRead(NYql::EStorageType::RowStorage, pos);
        auto sort = MakeIntrusive<TOpSort>(
            read,
            pos,
            TVector<TSortElement>{TSortElement(TInfoUnit("a"), true, true)});
        auto limit = MakeIntrusive<TOpLimit>(
            sort,
            pos,
            MakeConstant("Uint64", "1", pos, &ctx.ExprCtx),
            EOpPhase::Undefined);
        auto otherConsumer = MakeIntrusive<TOpLimit>(
            sort,
            pos,
            MakeConstant("Uint64", "2", pos, &ctx.ExprCtx),
            EOpPhase::Final);
        ComputeParents(limit, otherConsumer, pos);

        UNIT_ASSERT_VALUES_EQUAL(sort->Parents.size(), 2);
        UNIT_ASSERT(!sort->LimitCond);

        TPushLimitIntoSortRule rule;
        const auto result =
            rule.SimpleMatchAndApply(limit, ctx.RboCtx, ctx.PlanProps);

        UNIT_ASSERT_VALUES_EQUAL(result.Get(), limit.Get());
        UNIT_ASSERT(!sort->LimitCond);
        UNIT_ASSERT_VALUES_EQUAL(sort->Parents.size(), 2);
    }
}

Y_UNIT_TEST_SUITE(KqpRboOrderSensitiveJoinRules) {
    Y_UNIT_TEST(KeepsMarkedJoinOutsideCboWhileOptimizingBothSides) {
        TRuleTestContext ctx;
        const auto pos = TPositionHandle();
        TBuildInitialCBOTreeRule buildCboTree;

        auto markedJoin = MakeIntrusive<TOpJoin>(
            MakeRead(NYql::EStorageType::RowStorage, pos),
            MakeRead(NYql::EStorageType::RowStorage, pos),
            pos,
            "Cross",
            TVector<std::pair<TInfoUnit, TInfoUnit>>{});
        markedJoin->PreserveInputOrder = true;

        const auto markedResult =
            buildCboTree.SimpleMatchAndApply(markedJoin, ctx.RboCtx, ctx.PlanProps);
        UNIT_ASSERT_VALUES_EQUAL(markedResult.Get(), markedJoin.Get());

        auto makeCboSide = [&]() {
            auto join = MakeIntrusive<TOpJoin>(
                MakeRead(NYql::EStorageType::RowStorage, pos),
                MakeRead(NYql::EStorageType::RowStorage, pos),
                pos,
                "Cross",
                TVector<std::pair<TInfoUnit, TInfoUnit>>{});
            auto result =
                buildCboTree.SimpleMatchAndApply(join, ctx.RboCtx, ctx.PlanProps);
            UNIT_ASSERT_VALUES_UNEQUAL(result.Get(), join.Get());
            UNIT_ASSERT(result->Kind == EOperator::CBOTree);
            return result;
        };

        auto leftCboTree = makeCboSide();
        auto rightCboTree = makeCboSide();
        auto markedJoinWithCboSides = MakeIntrusive<TOpJoin>(
            leftCboTree,
            rightCboTree,
            pos,
            "Cross",
            TVector<std::pair<TInfoUnit, TInfoUnit>>{});
        markedJoinWithCboSides->PreserveInputOrder = true;

        TExpandCBOTreeRule expandCboTree;
        const auto expandedResult = expandCboTree.SimpleMatchAndApply(
            markedJoinWithCboSides,
            ctx.RboCtx,
            ctx.PlanProps);

        UNIT_ASSERT_VALUES_EQUAL(
            expandedResult.Get(),
            markedJoinWithCboSides.Get());
        UNIT_ASSERT_VALUES_EQUAL(
            markedJoinWithCboSides->GetLeftInput().Get(),
            leftCboTree.Get());
        UNIT_ASSERT_VALUES_EQUAL(
            markedJoinWithCboSides->GetRightInput().Get(),
            rightCboTree.Get());
    }

    Y_UNIT_TEST(DoesNotConvertMarkedCrossJoinThroughFilterPushdown) {
        TRuleTestContext ctx;
        const auto pos = TPositionHandle();
        const TInfoUnit leftColumn("left");
        const TInfoUnit rightColumn("right");

        auto markedJoin = MakeIntrusive<TOpJoin>(
            MakeRead(NYql::EStorageType::RowStorage, pos, {}, leftColumn.GetFullName()),
            MakeRead(NYql::EStorageType::RowStorage, pos, {}, rightColumn.GetFullName()),
            pos,
            "Cross",
            TVector<std::pair<TInfoUnit, TInfoUnit>>{});
        markedJoin->PreserveInputOrder = true;
        auto equality = MakeBinaryPredicate(
            "==",
            MakeColumnAccess(leftColumn, pos, &ctx.ExprCtx, &ctx.PlanProps),
            MakeColumnAccess(rightColumn, pos, &ctx.ExprCtx, &ctx.PlanProps));
        auto filter = MakeIntrusive<TOpFilter>(
            markedJoin,
            pos,
            equality);
        TOpRoot root(filter, pos, {"left", "right"});
        root.ComputeParents();
        UNIT_ASSERT(markedJoin->IsSingleConsumer());

        TPushFilterIntoJoinRule pushFilter;
        const auto result =
            pushFilter.SimpleMatchAndApply(filter, ctx.RboCtx, ctx.PlanProps);

        UNIT_ASSERT_VALUES_EQUAL(result.Get(), filter.Get());
        UNIT_ASSERT_VALUES_EQUAL(markedJoin->JoinKind, "Cross");
        UNIT_ASSERT(markedJoin->JoinKeys.empty());
    }
}

} // namespace

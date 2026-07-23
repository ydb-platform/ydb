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
    const TPhysicalOpProps& props = {})
{
    return MakeIntrusive<TOpRead>(
        "",
        TVector<TString>{"a"},
        TVector<TInfoUnit>{TInfoUnit("a")},
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

} // namespace

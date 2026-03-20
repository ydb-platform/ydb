#include "helpers/local.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/expr_nodes_gen/yql_expr_nodes_gen.h>
#include <yql/essentials/core/yql_type_annotation.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp::NOpt {
NYql::NNodes::TExprBase KqpEliminateWideMapPackUnpack(
    const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx,
    NYql::TTypeAnnotationContext& typesCtx);
}

namespace NKikimr::NKqp {

using namespace NYdb;

namespace {

NYql::TExprNode::TPtr BuildPackUnpackWideMap(NYql::TExprContext& ctx, bool withComputation) {
    auto pos = NYql::TPositionHandle();

    auto packArgA = ctx.NewArgument(pos, "a");
    auto packArgB = ctx.NewArgument(pos, "b");
    auto packArgCount = ctx.NewArgument(pos, "count");

    NYql::TExprNode::TPtr valA = packArgA;
    NYql::TExprNode::TPtr valB = packArgB;
    if (withComputation) {
        valA = ctx.NewCallable(pos, "Increment", {packArgA});
        valB = ctx.NewCallable(pos, "Increment", {packArgB});
    }

    auto bas = ctx.NewCallable(pos, "BlockAsStruct", {
        ctx.NewList(pos, {ctx.NewAtom(pos, "c1"), valA}),
        ctx.NewList(pos, {ctx.NewAtom(pos, "c2"), valB}),
    });

    auto packLambda = ctx.NewLambda(pos,
        ctx.NewArguments(pos, {packArgA, packArgB, packArgCount}),
        {bas, packArgCount});

    auto input = ctx.NewCallable(pos, "TestInput", {});
    auto innerWideMap = ctx.NewCallable(pos, "WideMap", {input, packLambda});

    auto limit = ctx.NewCallable(pos, "Uint64", {ctx.NewAtom(pos, "1")});
    auto wideTake = ctx.NewCallable(pos, "WideTakeBlocks", {innerWideMap, limit});

    auto unpackArgStruct = ctx.NewArgument(pos, "struct");
    auto unpackArgCount = ctx.NewArgument(pos, "ucount");

    auto unpackLambda = ctx.NewLambda(pos,
        ctx.NewArguments(pos, {unpackArgStruct, unpackArgCount}),
        {
            ctx.NewCallable(pos, "BlockMember", {unpackArgStruct, ctx.NewAtom(pos, "c1")}),
            ctx.NewCallable(pos, "BlockMember", {unpackArgStruct, ctx.NewAtom(pos, "c2")}),
            unpackArgCount,
        });

    return ctx.NewCallable(pos, "WideMap", {wideTake, unpackLambda});
}

TString ExplainOlapQuery(const TString& createTableSql, const TString& querySql) {
    auto settings = TKikimrSettings().SetWithSampleTables(false);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
    auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
    TKikimrRunner kikimr(settings);

    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    {
        auto result = session.ExecuteSchemeQuery(createTableSql).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
    }

    auto client = kikimr.GetQueryClient();
    NQuery::TExecuteQuerySettings explainSettings;
    explainSettings.ExecMode(NQuery::EExecMode::Explain);
    auto it = client.StreamExecuteQuery(
        querySql, NQuery::TTxControl::BeginTx().CommitTx(), explainSettings).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
    auto plan = CollectStreamResult(it);
    UNIT_ASSERT(plan.QueryStats.Defined());
    return plan.QueryStats->Getquery_ast();
}

const TString CreateTestTable = R"(
    CREATE TABLE `/Root/TestTable` (
        id Uint64 NOT NULL, c1 String, c2 String, c3 String,
        PRIMARY KEY (id)
    )
    PARTITION BY HASH(id)
    WITH (STORE = COLUMN, PARTITION_COUNT = 1)
)";

} // namespace

Y_UNIT_TEST_SUITE(KqpOlapPeephole) {

    Y_UNIT_TEST(EliminateWideMapPackUnpackOnSelectStarLimit) {
        const auto ast = ExplainOlapQuery(CreateTestTable,
            "SELECT * FROM `/Root/TestTable` LIMIT 1");

        UNIT_ASSERT_C(ast.Contains("(WideTakeBlocks (FromFlow"),
            "Scan stage: expected WideTakeBlocks directly on FromFlow "
            "(WideMap pack/unpack roundtrip should be eliminated). AST: " + ast);

        UNIT_ASSERT_C(!ast.Contains("(WideMap (WideTakeBlocks (WideMap (FromFlow"),
            "Scan stage: WideMap pack/unpack roundtrip should not be present. AST: " + ast);
    }

    Y_UNIT_TEST(PreserveWideMapInComputeStage) {
        const auto ast = ExplainOlapQuery(CreateTestTable,
            "SELECT * FROM `/Root/TestTable` LIMIT 1");

        UNIT_ASSERT_C(ast.Contains("(WideMap (WideTakeBlocks"),
            "Compute stage: WideMap around WideTakeBlocks should be preserved "
            "(our optimization only eliminates scan stage pack/unpack roundtrip). AST: " + ast);
    }

    Y_UNIT_TEST(EliminatePackUnpackWithColumnSubset) {
        const auto ast = ExplainOlapQuery(CreateTestTable,
            "SELECT id, c1 FROM `/Root/TestTable` LIMIT 1");

        UNIT_ASSERT_C(ast.Contains("(WideTakeBlocks (FromFlow"),
            "Scan stage: expected WideTakeBlocks directly on FromFlow "
            "even with column subset (pack/unpack roundtrip should be eliminated). AST: " + ast);
    }

    Y_UNIT_TEST(UnitIdentityPackUnpackIsEliminated) {
        NYql::TExprContext ctx;
        NYql::TTypeAnnotationContext typesCtx;

        auto node = BuildPackUnpackWideMap(ctx, false);
        auto result = NOpt::KqpEliminateWideMapPackUnpack(NYql::NNodes::TExprBase(node), ctx, typesCtx);

        UNIT_ASSERT_C(result.Ptr() != node,
            "Identity pack/unpack roundtrip should be eliminated");
        UNIT_ASSERT_C(result.Ref().IsCallable("WideTakeBlocks"),
            "Result should be WideTakeBlocks(input)");
        UNIT_ASSERT_C(result.Ref().Head().IsCallable("TestInput"),
            "WideTakeBlocks input should be the original TestInput");
    }

    Y_UNIT_TEST(UnitPackWithComputationIsPreserved) {
        NYql::TExprContext ctx;
        NYql::TTypeAnnotationContext typesCtx;

        auto node = BuildPackUnpackWideMap(ctx, true);
        auto result = NOpt::KqpEliminateWideMapPackUnpack(NYql::NNodes::TExprBase(node), ctx, typesCtx);

        UNIT_ASSERT_C(result.Ptr() == node,
            "Pack with computation (a+1) should NOT be eliminated");
    }
}

} // namespace NKikimr::NKqp

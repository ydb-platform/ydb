#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

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

namespace {

enum class EPackKind {
    Identity,       // (a, b, count) -> BlockAsStruct('c1':a, 'c2':b), count
    Computation,    // (a, b, count) -> BlockAsStruct('c1':Inc(a), 'c2':Inc(b)), count
    Reorder,        // (a, b, count) -> BlockAsStruct('c1':b, 'c2':a), count -- swapped
    Projection,     // (a, b, c, count) -> BlockAsStruct('c1':a, 'c2':b), count -- c dropped
};

NYql::TExprNode::TPtr BuildPackUnpackWideMap(NYql::TExprContext& ctx, EPackKind kind) {
    auto pos = NYql::TPositionHandle();

    auto packArgA = ctx.NewArgument(pos, "a");
    auto packArgB = ctx.NewArgument(pos, "b");
    auto packArgCount = ctx.NewArgument(pos, "count");

    NYql::TExprNode::TListType packArgs = {packArgA, packArgB, packArgCount};

    NYql::TExprNode::TPtr valA, valB;

    switch (kind) {
        case EPackKind::Identity:
            valA = packArgA;
            valB = packArgB;
            break;
        case EPackKind::Computation:
            valA = ctx.NewCallable(pos, "Increment", {packArgA});
            valB = ctx.NewCallable(pos, "Increment", {packArgB});
            break;
        case EPackKind::Reorder:
            valA = packArgB;  // swapped: c1 gets b
            valB = packArgA;  // swapped: c2 gets a
            break;
        case EPackKind::Projection: {
            auto packArgC = ctx.NewArgument(pos, "c");
            packArgs = {packArgA, packArgB, packArgC, packArgCount};
            valA = packArgA;
            valB = packArgB;
            break;
        }
    }

    auto bas = ctx.NewCallable(pos, "BlockAsStruct", {
        ctx.NewList(pos, {ctx.NewAtom(pos, "c1"), valA}),
        ctx.NewList(pos, {ctx.NewAtom(pos, "c2"), valB}),
    });

    auto packLambda = ctx.NewLambda(pos,
        ctx.NewArguments(pos, std::move(packArgs)),
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

} // namespace

Y_UNIT_TEST_SUITE(KqpPeepholeRules) {

    Y_UNIT_TEST_QUAD(TimestampBucketAggregation, ConstraintsEnabled, BlockChannels) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.FeatureFlags.SetEnableKqpConstraintsTransformer(ConstraintsEnabled);
        auto* tableService = settings.AppConfig.MutableTableServiceConfig();
        tableService->SetEnableNewRBO(false);
        tableService->SetBlockChannelsMode(BlockChannels
            ? NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_AUTO
            : NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_SCALAR);
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetQueryClient();
        auto create = db.ExecuteQuery(R"(
            CREATE TABLE `events` (
                time Timestamp NOT NULL,
                uuid Uuid NOT NULL,
                hostid Utf8,
                source Utf8,
                PRIMARY KEY (time, uuid)
            ) PARTITION BY HASH(time)
            WITH (STORE = COLUMN, PARTITION_COUNT = 2);
        )", NYdb::NQuery::TTxControl::NoTx())
                          .ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(create.GetStatus(), NYdb::EStatus::SUCCESS, create.GetIssues().ToString());
        auto insert = db.ExecuteQuery(R"(
            UPSERT INTO `events` (hostid, time, source, uuid) VALUES
                ('target', Timestamp('2026-01-16T15:27:22Z'), 'a', Uuid('00000000-0000-0000-0000-000000000001')),
                ('target', Timestamp('2026-01-16T15:27:23Z'), 'a', Uuid('00000000-0000-0000-0000-000000000002')),
                ('target', Timestamp('2026-01-16T15:29:59Z'), 'a', Uuid('00000000-0000-0000-0000-000000000003')),
                ('target', Timestamp('2026-01-16T15:28:00Z'), 'b', Uuid('00000000-0000-0000-0000-000000000004')),
                ('target', Timestamp('2026-01-16T15:30:00Z'), 'a', Uuid('00000000-0000-0000-0000-000000000005')),
                ('target', Timestamp('2026-01-16T17:27:23Z'), 'a', Uuid('00000000-0000-0000-0000-000000000006')),
                ('target', Timestamp('2026-01-16T17:27:24Z'), 'a', Uuid('00000000-0000-0000-0000-000000000007')),
                ('other', Timestamp('2026-01-16T15:28:00Z'), 'a', Uuid('00000000-0000-0000-0000-000000000008'));
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx())
                          .ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(insert.GetStatus(), NYdb::EStatus::SUCCESS, insert.GetIssues().ToString());
        auto result = db.ExecuteQuery(R"(
            $start_time = Timestamp('2026-01-16T15:27:23+00:00');
            $end_time = Timestamp('2026-01-16T17:27:23+00:00');
            $interval_us = 600000000;

            SELECT
                MIN(CAST(((CAST(time AS Uint64) / $interval_us) * $interval_us) AS Timestamp)) AS interval_start,
                source,
                COUNT(*) AS event_count
            FROM `events`
            WHERE hostid = 'target' AND time >= $start_time AND time <= $end_time
            GROUP BY (CAST(time AS Uint64) / $interval_us) AS bucket, source
            ORDER BY interval_start ASC, source ASC;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx())
                          .ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [[1768576800000000u];["a"];2u];
            [[1768576800000000u];["b"];1u];
            [[1768577400000000u];["a"];1u];
            [[1768584000000000u];["a"];1u]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(/* resultIndex */ 0)));
    }

    Y_UNIT_TEST(NonDeterministicProducerKeepsTypes) {
        TKikimrSettings settings;
        auto* tableService = settings.AppConfig.MutableTableServiceConfig();
        tableService->SetEnableNewRBO(false);
        tableService->SetBlockChannelsMode(NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_AUTO);
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetQueryClient();
        auto result = db.ExecuteQuery(R"(
            SELECT COUNT(*) FROM EightShard
            WHERE CAST(Key AS Timestamp) < CurrentUtcTimestamp();
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson("[[24u]]", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(IdentityPackUnpackIsEliminated) {
        NYql::TExprContext ctx;
        NYql::TTypeAnnotationContext typesCtx;

        auto node = BuildPackUnpackWideMap(ctx, EPackKind::Identity);
        auto result = NOpt::KqpEliminateWideMapPackUnpack(NYql::NNodes::TExprBase(node), ctx, typesCtx);

        UNIT_ASSERT_C(result.Ptr() != node,
            "Identity pack/unpack roundtrip should be eliminated");
        UNIT_ASSERT_C(result.Ref().IsCallable("WideTakeBlocks"),
            "Result should be WideTakeBlocks(input)");
        UNIT_ASSERT_C(result.Ref().Head().IsCallable("TestInput"),
            "WideTakeBlocks input should be the original TestInput");
    }

    Y_UNIT_TEST(PackWithComputationIsPreserved) {
        NYql::TExprContext ctx;
        NYql::TTypeAnnotationContext typesCtx;

        // (a, b) -> BlockAsStruct('c1': Increment(a), 'c2': Increment(b))
        auto node = BuildPackUnpackWideMap(ctx, EPackKind::Computation);
        auto result = NOpt::KqpEliminateWideMapPackUnpack(NYql::NNodes::TExprBase(node), ctx, typesCtx);

        UNIT_ASSERT_C(result.Ptr() == node,
            "Pack with computation (a+1) should NOT be eliminated");
    }

    Y_UNIT_TEST(PackWithReorderIsPreserved) {
        NYql::TExprContext ctx;
        NYql::TTypeAnnotationContext typesCtx;

        // (a, b) -> BlockAsStruct('c1': b, 'c2': a) -- columns swapped
        auto node = BuildPackUnpackWideMap(ctx, EPackKind::Reorder);
        auto result = NOpt::KqpEliminateWideMapPackUnpack(NYql::NNodes::TExprBase(node), ctx, typesCtx);

        UNIT_ASSERT_C(result.Ptr() == node,
            "Pack with reordered columns should NOT be eliminated");
    }

    Y_UNIT_TEST(PackWithProjectionIsPreserved) {
        NYql::TExprContext ctx;
        NYql::TTypeAnnotationContext typesCtx;

        // (a, b, c) -> BlockAsStruct('c1': a, 'c2': b) -- column c dropped
        auto node = BuildPackUnpackWideMap(ctx, EPackKind::Projection);
        auto result = NOpt::KqpEliminateWideMapPackUnpack(NYql::NNodes::TExprBase(node), ctx, typesCtx);

        UNIT_ASSERT_C(result.Ptr() == node,
            "Pack with projection (dropped column) should NOT be eliminated");
    }
}

} // namespace NKikimr::NKqp

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/hook/hook.h>
#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>

#include "dq_opt_log.h"
#include "dq_opt_join.h"

using namespace NYql;
using namespace NNodes;
using namespace NYql::NDq;

Y_UNIT_TEST_SUITE(DQCBO) {

Y_UNIT_TEST(Empty) {
	IOptimizer::TInput input;
    std::unique_ptr<IOptimizer> optimizer = std::unique_ptr<IOptimizer>(NDq::MakeNativeOptimizer(input, {}));
}

Y_UNIT_TEST(RelCollector) {
    TExprContext ctx;
    auto pos = ctx.AppendPosition({});
    TVector<TExprBase> joinArgs;
    TVector<TExprBase> tables;
    tables.emplace_back(Build<TCoEquiJoinInput>(ctx, pos).List(Build<TCoAtomList>(ctx, pos).Done().Ptr()).Scope(ctx.NewAtom(pos, "orders")).Done());
    tables.emplace_back(Build<TCoEquiJoinInput>(ctx, pos).List(Build<TCoAtomList>(ctx, pos).Done().Ptr()).Scope(ctx.NewAtom(pos, "customer")).Done());
    tables.emplace_back(Build<TCoEquiJoinInput>(ctx, pos).List(Build<TCoAtomList>(ctx, pos).Done().Ptr()).Scope(ctx.NewAtom(pos, "nation")).Done());

    auto joinTree = Build<TCoAtomList>(ctx, pos).Done().Ptr();
    auto settings = Build<TCoAtomList>(ctx, pos).Done().Ptr();

    joinArgs.insert(joinArgs.end(), tables.begin(), tables.end());
    joinArgs.emplace_back(joinTree);
    joinArgs.emplace_back(settings);

    TCoEquiJoin equiJoin = Build<TCoEquiJoin>(ctx, pos)
        .Add(joinArgs)
        .Done();

    TTypeAnnotationContext typeCtx;
    UNIT_ASSERT(DqCollectJoinRelationsWithStats(typeCtx, equiJoin, [&](auto, auto) {}) == false);

    typeCtx.StatisticsMap[tables[1].Ptr()->Child(0)] = std::make_shared<TOptimizerStatistics>(1, 1, 1);
    UNIT_ASSERT(DqCollectJoinRelationsWithStats(typeCtx, equiJoin, [&](auto, auto) {}) == false);

    typeCtx.StatisticsMap[tables[0].Ptr()->Child(0)] = std::make_shared<TOptimizerStatistics>(1, 1, 1);
    typeCtx.StatisticsMap[tables[2].Ptr()->Child(0)] = std::make_shared<TOptimizerStatistics>(1, 1, 1);

    TVector<TString> labels;
    UNIT_ASSERT(DqCollectJoinRelationsWithStats(typeCtx, equiJoin, [&](auto label, auto) { labels.emplace_back(label); }) == true);
    UNIT_ASSERT(labels.size() == 3);
    UNIT_ASSERT_STRINGS_EQUAL(labels[0], "orders");
    UNIT_ASSERT_STRINGS_EQUAL(labels[1], "customer");
    UNIT_ASSERT_STRINGS_EQUAL(labels[2], "nation");
}

Y_UNIT_TEST(RelCollectorBrokenEquiJoin) {
    TExprContext ctx;
    auto pos = ctx.AppendPosition({});
    TVector<TExprBase> joinArgs;
    auto joinTree = Build<TCoAtomList>(ctx, pos).Done().Ptr();
    auto settings = Build<TCoAtomList>(ctx, pos).Done().Ptr();
    TCoEquiJoin equiJoin = Build<TCoEquiJoin>(ctx, pos)
        .Add(joinArgs)
        .Done();

    TTypeAnnotationContext typeCtx;
    UNIT_ASSERT(DqCollectJoinRelationsWithStats(typeCtx, equiJoin, [&](auto, auto) {}) == false);
}

} // DQCBO


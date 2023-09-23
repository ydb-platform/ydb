#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/hook/hook.h>
#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/optimizer.h>

#include "dq_opt_log.h"
#include "dq_opt_join.h"

using namespace NYql;
using namespace NNodes;
using namespace NYql::NDq;

namespace {

TExprNode::TPtr MakeLabel(TExprContext& ctx, const std::vector<TStringBuf>& vars) {
    TVector<TExprNodePtr> label; label.reserve(vars.size());

    auto pos = ctx.AppendPosition({});
    for (auto var : vars) {
        label.emplace_back(ctx.NewAtom(pos, var));
    }

    return Build<TCoAtomList>(ctx, pos)
            .Add(label)
            .Done()
        .Ptr();
}

} // namespace

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

void _DqOptimizeEquiJoinWithCosts(const std::function<IOptimizer*(IOptimizer::TInput&&)>& optFactory) {
    TTypeAnnotationContext typeCtx;
    TExprContext ctx;
    auto pos = ctx.AppendPosition({});
    TVector<TExprBase> joinArgs;
    TVector<TExprBase> tables;
    tables.emplace_back(Build<TCoEquiJoinInput>(ctx, pos).List(Build<TCoAtomList>(ctx, pos).Done().Ptr()).Scope(ctx.NewAtom(pos, "orders")).Done());
    tables.emplace_back(Build<TCoEquiJoinInput>(ctx, pos).List(Build<TCoAtomList>(ctx, pos).Done().Ptr()).Scope(ctx.NewAtom(pos, "customer")).Done());

    auto settings = Build<TCoAtomList>(ctx, pos).Done().Ptr();

    auto joinTree = Build<TCoEquiJoinTuple>(ctx, pos)
        .Type(ctx.NewAtom(pos, "Inner"))
        .LeftScope(ctx.NewAtom(pos, "orders"))
        .RightScope(ctx.NewAtom(pos, "customer"))
        .LeftKeys(MakeLabel(ctx, {"orders", "a"}))
        .RightKeys(MakeLabel(ctx, {"customer", "b"}))
        .Options(settings)
        .Done().Ptr();

    joinArgs.insert(joinArgs.end(), tables.begin(), tables.end());
    joinArgs.emplace_back(joinTree);
    joinArgs.emplace_back(settings);

    typeCtx.StatisticsMap[tables[0].Ptr()->Child(0)] = std::make_shared<TOptimizerStatistics>(1, 1, 1);
    typeCtx.StatisticsMap[tables[1].Ptr()->Child(0)] = std::make_shared<TOptimizerStatistics>(1, 1, 1);

    TCoEquiJoin equiJoin = Build<TCoEquiJoin>(ctx, pos)
        .Add(joinArgs)
        .Done();

    auto res = DqOptimizeEquiJoinWithCosts(equiJoin, ctx, typeCtx, optFactory, true);
    UNIT_ASSERT(equiJoin.Ptr() != res.Ptr());
    UNIT_ASSERT(equiJoin.Ptr()->ChildrenSize() == res.Ptr()->ChildrenSize());
    UNIT_ASSERT(equiJoin.Maybe<TCoEquiJoin>());
}

Y_UNIT_TEST(DqOptimizeEquiJoinWithCostsNative) {
    std::function<void(const TString&)> log = [&](auto str) {
        Cerr << str;
    };
    std::function<IOptimizer*(IOptimizer::TInput&&)> optFactory = [&](auto input) {
        return MakeNativeOptimizer(input, log);
    };
    _DqOptimizeEquiJoinWithCosts(optFactory);
}

Y_UNIT_TEST(DqOptimizeEquiJoinWithCostsPG) {
    std::function<void(const TString&)> log = [&](auto str) {
        Cerr << str;
    };
    std::function<IOptimizer*(IOptimizer::TInput&&)> optFactory = [&](auto input) {
        return MakePgOptimizer(input, log);
    };
    _DqOptimizeEquiJoinWithCosts(optFactory);
}

} // DQCBO


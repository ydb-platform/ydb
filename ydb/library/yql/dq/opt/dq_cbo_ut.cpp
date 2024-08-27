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
    TBaseProviderContext pctx;
    std::unique_ptr<IOptimizerNew> optimizer = std::unique_ptr<IOptimizerNew>(MakeNativeOptimizerNew(pctx, 100000));
}

Y_UNIT_TEST(JoinSearch2Rels) {
    TBaseProviderContext pctx;
    std::unique_ptr<IOptimizerNew> optimizer = std::unique_ptr<IOptimizerNew>(MakeNativeOptimizerNew(pctx, 100000));

    auto rel1 = std::make_shared<TRelOptimizerNode>("a",
        std::make_shared<TOptimizerStatistics>(BaseTable, 100000, 1, 0, 1000000));
    auto rel2 = std::make_shared<TRelOptimizerNode>("b",
        std::make_shared<TOptimizerStatistics>(BaseTable, 1000000, 1, 0, 9000009));

    std::set<std::pair<NDq::TJoinColumn, NDq::TJoinColumn>> joinConditions;
    joinConditions.insert({
        NDq::TJoinColumn("a", "1"),
        NDq::TJoinColumn("b", "1")
    });
    auto op = std::make_shared<TJoinOptimizerNode>(
        std::static_pointer_cast<IBaseOptimizerNode>(rel1),
        std::static_pointer_cast<IBaseOptimizerNode>(rel2),
        joinConditions,
        InnerJoin,
        EJoinAlgoType::GraceJoin
        );

    auto res = optimizer->JoinSearch(op);
    std::stringstream ss;
    res->Print(ss);
    TString expected = R"__(Join: (InnerJoin,MapJoin) b.1=a.1,
Type: ManyManyJoin, Nrows: 2e+10, Ncols: 2, ByteSize: 0, Cost: 2.00112e+10, Storage: NA
    Rel: b
    Type: BaseTable, Nrows: 1e+06, Ncols: 1, ByteSize: 0, Cost: 9.00001e+06, Storage: NA
    Rel: a
    Type: BaseTable, Nrows: 100000, Ncols: 1, ByteSize: 0, Cost: 1e+06, Storage: NA
)__";

    UNIT_ASSERT_STRINGS_EQUAL(expected, ss.str());
}

Y_UNIT_TEST(JoinSearch3Rels) {
    TBaseProviderContext pctx;
    std::unique_ptr<IOptimizerNew> optimizer = std::unique_ptr<IOptimizerNew>(MakeNativeOptimizerNew(pctx, 100000));

    auto rel1 = std::make_shared<TRelOptimizerNode>("a",
        std::make_shared<TOptimizerStatistics>(BaseTable, 100000, 1, 0, 1000000));
    auto rel2 = std::make_shared<TRelOptimizerNode>("b",
        std::make_shared<TOptimizerStatistics>(BaseTable, 1000000, 1, 0, 9000009));
    auto rel3 = std::make_shared<TRelOptimizerNode>("c",
        std::make_shared<TOptimizerStatistics>(BaseTable, 10000, 1, 0, 9009));

    std::set<std::pair<NDq::TJoinColumn, NDq::TJoinColumn>> joinConditions;
    joinConditions.insert({
        NDq::TJoinColumn("a", "1"),
        NDq::TJoinColumn("b", "1")
    });
    auto op1 = std::make_shared<TJoinOptimizerNode>(
        std::static_pointer_cast<IBaseOptimizerNode>(rel1),
        std::static_pointer_cast<IBaseOptimizerNode>(rel2),
        joinConditions,
        InnerJoin,
        EJoinAlgoType::GraceJoin
        );

    joinConditions.insert({
        NDq::TJoinColumn("a", "1"),
        NDq::TJoinColumn("c", "1")
    });

    auto op2 = std::make_shared<TJoinOptimizerNode>(
        std::static_pointer_cast<IBaseOptimizerNode>(op1),
        std::static_pointer_cast<IBaseOptimizerNode>(rel3),
        joinConditions,
        InnerJoin,
        EJoinAlgoType::GraceJoin
        );

    auto res = optimizer->JoinSearch(op2);
    std::stringstream ss;
    res->Print(ss);

    TString expected = R"__(Join: (InnerJoin,MapJoin) a.1=b.1,a.1=c.1,
Type: ManyManyJoin, Nrows: 4e+13, Ncols: 3, ByteSize: 0, Cost: 4.004e+13, Storage: NA
    Join: (InnerJoin,MapJoin) b.1=a.1,
    Type: ManyManyJoin, Nrows: 2e+10, Ncols: 2, ByteSize: 0, Cost: 2.00112e+10, Storage: NA
        Rel: b
        Type: BaseTable, Nrows: 1e+06, Ncols: 1, ByteSize: 0, Cost: 9.00001e+06, Storage: NA
        Rel: a
        Type: BaseTable, Nrows: 100000, Ncols: 1, ByteSize: 0, Cost: 1e+06, Storage: NA
    Rel: c
    Type: BaseTable, Nrows: 10000, Ncols: 1, ByteSize: 0, Cost: 9009, Storage: NA
)__";

    UNIT_ASSERT_STRINGS_EQUAL(expected, ss.str());
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
    TVector<std::shared_ptr<TRelOptimizerNode>> rels;
    UNIT_ASSERT(DqCollectJoinRelationsWithStats(rels, typeCtx, equiJoin, [&](auto, auto, auto, auto) {}) == false);

    typeCtx.StatisticsMap[tables[1].Ptr()->Child(0)] =
        std::make_shared<TOptimizerStatistics>(BaseTable, 1, 1, 1);
    UNIT_ASSERT(DqCollectJoinRelationsWithStats(rels, typeCtx, equiJoin, [&](auto, auto, auto, auto) {}) == false);

    typeCtx.StatisticsMap[tables[0].Ptr()->Child(0)] =
        std::make_shared<TOptimizerStatistics>(BaseTable, 1, 1, 1);
    typeCtx.StatisticsMap[tables[2].Ptr()->Child(0)] =
        std::make_shared<TOptimizerStatistics>(BaseTable, 1, 1, 1);

    TVector<TString> labels;
    UNIT_ASSERT(DqCollectJoinRelationsWithStats(rels, typeCtx, equiJoin, [&](auto, auto label, auto, auto) { labels.emplace_back(label); }) == true);
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
    TVector<std::shared_ptr<TRelOptimizerNode>> rels;
    UNIT_ASSERT(DqCollectJoinRelationsWithStats(rels, typeCtx, equiJoin, [&](auto, auto, auto, auto) {}) == false);
}

void _DqOptimizeEquiJoinWithCosts(const std::function<IOptimizerNew*()>& optFactory, TExprContext& ctx) {
    TTypeAnnotationContext typeCtx;
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

    typeCtx.StatisticsMap[tables[0].Ptr()->Child(0)] =
        std::make_shared<TOptimizerStatistics>(BaseTable, 1, 1, 1);
    typeCtx.StatisticsMap[tables[1].Ptr()->Child(0)] =
        std::make_shared<TOptimizerStatistics>(BaseTable, 1, 1, 1);

    TCoEquiJoin equiJoin = Build<TCoEquiJoin>(ctx, pos)
        .Add(joinArgs)
        .Done();

    auto opt = std::unique_ptr<IOptimizerNew>(optFactory());
    std::function<void(TVector<std::shared_ptr<TRelOptimizerNode>>&, TStringBuf, const TExprNode::TPtr, const std::shared_ptr<TOptimizerStatistics>&)> providerCollect = [](auto& rels, auto label, auto node, auto stats) {
        Y_UNUSED(node);
        auto rel = std::make_shared<TRelOptimizerNode>(TString(label), stats);
        rels.push_back(rel);
    };
    auto res = DqOptimizeEquiJoinWithCosts(equiJoin, ctx, typeCtx, 2, *opt, providerCollect);
    UNIT_ASSERT(equiJoin.Ptr() != res.Ptr());
    UNIT_ASSERT(equiJoin.Ptr()->ChildrenSize() == res.Ptr()->ChildrenSize());
    UNIT_ASSERT(equiJoin.Maybe<TCoEquiJoin>());
    auto resStr = NCommon::ExprToPrettyString(ctx, *res.Ptr());
    auto expected = R"__((
(let $1 '('"Inner" '"orders" '"customer" '('"orders" '"a") '('"customer" '"b") '('('"join_algo" '"MapJoin"))))
(return (EquiJoin '('() '"orders") '('() '"customer") $1 '()))
)
)__";
    UNIT_ASSERT_STRINGS_EQUAL(expected, resStr);
}

Y_UNIT_TEST(DqOptimizeEquiJoinWithCostsNative) {
    TExprContext ctx;
    TBaseProviderContext pctx;
    std::function<IOptimizerNew*()> optFactory = [&]() {
        return MakeNativeOptimizerNew(pctx, 100000);
    };
    _DqOptimizeEquiJoinWithCosts(optFactory, ctx);
}

Y_UNIT_TEST(DqOptimizeEquiJoinWithCostsPG) {
    TExprContext ctx;
    TBaseProviderContext pctx;
    std::function<void(const TString&)> log = [&](auto str) {
        Cerr << str;
    };
    std::function<IOptimizerNew*()> optFactory = [&]() {
        return MakePgOptimizerNew(pctx, ctx, log);
    };
    _DqOptimizeEquiJoinWithCosts(optFactory, ctx);
}

} // DQCBO

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/hook/hook.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>

#include "kqp_opt_join_cost_based.h"
#include "kqp_opt_join.h"

using namespace NKikimr::NKqp;
using namespace NYql::NNodes;

namespace {

NYql::TExprNode::TPtr MakeLabel(NYql::TExprContext& ctx, const std::vector<TStringBuf>& vars) {
    TVector<NYql::TExprNodePtr> label; label.reserve(vars.size());

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

Y_UNIT_TEST_SUITE(KqpCBO) {

Y_UNIT_TEST(Empty) {
    TBaseProviderContext pctx;
    NYql::TExprContext dummyCtx;

    TCBOSettings settings{};
    std::unique_ptr<IOptimizerNew> optimizer = std::unique_ptr<IOptimizerNew>(MakeNativeOptimizerNew(pctx, settings, dummyCtx, false));
}

Y_UNIT_TEST(JoinSearch2Rels) {
    TBaseProviderContext pctx;
    NYql::TExprContext dummyCtx;

    TCBOSettings settings{};
    std::unique_ptr<IOptimizerNew> optimizer = std::unique_ptr<IOptimizerNew>(MakeNativeOptimizerNew(pctx, settings, dummyCtx, false));

    auto rel1 = std::make_shared<TRelOptimizerNode>(
        "a",
        TOptimizerStatistics(BaseTable, 100000, 1, 0, 1000000)
    );
    auto rel2 = std::make_shared<TRelOptimizerNode>(
        "b",
        TOptimizerStatistics(BaseTable, 1000000, 1, 0, 9000009)
    );

    TVector<TJoinColumn> leftKeys = {TJoinColumn("a", "1")};
    TVector<TJoinColumn> rightKeys ={TJoinColumn("b", "1")};

    auto op = std::make_shared<TJoinOptimizerNode>(
        std::static_pointer_cast<IBaseOptimizerNode>(rel1),
        std::static_pointer_cast<IBaseOptimizerNode>(rel2),
        leftKeys,
        rightKeys,
        InnerJoin,
        EJoinAlgoType::GraceJoin,
        true,
        false
        );

    auto res = optimizer->JoinSearch(op);
    std::stringstream ss;
    res->Print(ss);
    Cout << ss.str() << '\n';
}

Y_UNIT_TEST(JoinSearch3Rels) {
    TBaseProviderContext pctx;
    NYql::TExprContext dummyCtx;

    TCBOSettings settings{};
    std::unique_ptr<IOptimizerNew> optimizer = std::unique_ptr<IOptimizerNew>(MakeNativeOptimizerNew(pctx, settings, dummyCtx, false));

    auto rel1 = std::make_shared<TRelOptimizerNode>("a",
        TOptimizerStatistics(BaseTable, 100000, 1, 0, 1000000));
    auto rel2 = std::make_shared<TRelOptimizerNode>("b",
        TOptimizerStatistics(BaseTable, 1000000, 1, 0, 9000009));
    auto rel3 = std::make_shared<TRelOptimizerNode>("c",
        TOptimizerStatistics(BaseTable, 10000, 1, 0, 9009));

    TVector<TJoinColumn> leftKeys = {TJoinColumn("a", "1")};
    TVector<TJoinColumn> rightKeys ={TJoinColumn("b", "1")};

    auto op1 = std::make_shared<TJoinOptimizerNode>(
        std::static_pointer_cast<IBaseOptimizerNode>(rel1),
        std::static_pointer_cast<IBaseOptimizerNode>(rel2),
        leftKeys,
        rightKeys,
        InnerJoin,
        EJoinAlgoType::GraceJoin,
        false,
        false
    );

    leftKeys.push_back(TJoinColumn("a", "1"));
    rightKeys.push_back(TJoinColumn("c", "1"));

    auto op2 = std::make_shared<TJoinOptimizerNode>(
        std::static_pointer_cast<IBaseOptimizerNode>(op1),
        std::static_pointer_cast<IBaseOptimizerNode>(rel3),
        leftKeys,
        rightKeys,
        InnerJoin,
        EJoinAlgoType::GraceJoin,
        true,
        false
    );

    auto res = optimizer->JoinSearch(op2);
    std::stringstream ss;
    res->Print(ss);
    Cout << ss.str() << '\n';
}

Y_UNIT_TEST(JoinSearchYQL19363) {
    // Verify that JoinSearch() correctly handles dot and comma characters.
    TBaseProviderContext pctx;
    NYql::TExprContext dummyCtx;

    TCBOSettings settings{};
    std::unique_ptr<IOptimizerNew> optimizer = std::unique_ptr<IOptimizerNew>(MakeNativeOptimizerNew(pctx, settings, dummyCtx, false));

    TString relName1 = "a,b.c";
    TString colName1 = "a.x";
    TString relName2 = "b,d.e";
    TString colName2 = "b.y";

    auto rel1 = std::make_shared<TRelOptimizerNode>(relName1,
        TOptimizerStatistics(BaseTable, 1, 1, 0, 1));
    auto rel2 = std::make_shared<TRelOptimizerNode>(relName2,
        TOptimizerStatistics(BaseTable, 1, 1, 0, 1));

    TVector<TJoinColumn> leftKeys = {TJoinColumn(relName1, colName1)};
    TVector<TJoinColumn> rightKeys ={TJoinColumn(relName2, colName2)};

    auto op = std::make_shared<TJoinOptimizerNode>(
        std::static_pointer_cast<IBaseOptimizerNode>(rel1),
        std::static_pointer_cast<IBaseOptimizerNode>(rel2),
        leftKeys,
        rightKeys,
        InnerJoin,
        EJoinAlgoType::GraceJoin,
        false,
        false
    );

    auto res = optimizer->JoinSearch(op);

    UNIT_ASSERT_STRINGS_EQUAL(res->LeftJoinKeys[0].RelName, relName1);
    UNIT_ASSERT_STRINGS_EQUAL(res->LeftJoinKeys[0].AttributeName, colName1);
    UNIT_ASSERT_STRINGS_EQUAL(res->RightJoinKeys[0].RelName, relName2);
    UNIT_ASSERT_STRINGS_EQUAL(res->RightJoinKeys[0].AttributeName, colName2);

    auto generateSpecialCharacters = []() -> TString {
        TString result;
        for (int i = 1; i <= 255; ++i) {
            result += char(i);
        }
        return result;
    };

    relName1 = generateSpecialCharacters() + ".a";
    relName2 = generateSpecialCharacters() + ".b";

    // Verify that arbitrary characters are correctly handled and preserved
    rel1 = std::make_shared<TRelOptimizerNode>(relName1,
        TOptimizerStatistics(BaseTable, 1, 1, 0, 1));
    rel2 = std::make_shared<TRelOptimizerNode>(relName2,
        TOptimizerStatistics(BaseTable, 1, 1, 0, 1));

    colName1 = colName2 = generateSpecialCharacters();

    leftKeys = TVector<TJoinColumn>{TJoinColumn(relName1, colName1)};
    rightKeys = TVector<TJoinColumn>{TJoinColumn(relName2, colName2)};

    op = std::make_shared<TJoinOptimizerNode>(
        std::static_pointer_cast<IBaseOptimizerNode>(rel1),
        std::static_pointer_cast<IBaseOptimizerNode>(rel2),
        leftKeys,
        rightKeys,
        InnerJoin,
        EJoinAlgoType::GraceJoin,
        false,
        false
    );

    res = optimizer->JoinSearch(op);

    UNIT_ASSERT_STRINGS_EQUAL(res->LeftJoinKeys[0].RelName, relName1);
    UNIT_ASSERT_STRINGS_EQUAL(res->LeftJoinKeys[0].AttributeName, colName1);
    UNIT_ASSERT_STRINGS_EQUAL(res->RightJoinKeys[0].RelName, relName2);
    UNIT_ASSERT_STRINGS_EQUAL(res->RightJoinKeys[0].AttributeName, colName2);
}

struct TMockProviderContextYT24403 : public TBaseProviderContext {
    bool IsJoinApplicable(
        const std::shared_ptr<IBaseOptimizerNode>&,
        const std::shared_ptr<IBaseOptimizerNode>&,
        const TVector<TJoinColumn>&,
        const TVector<TJoinColumn>&,
        EJoinAlgoType joinAlgo,
        EJoinKind
    ) override {
        CalledIsJoinApplicable.insert(joinAlgo);
        return true;
    }

    TOptimizerStatistics ComputeJoinStats(
        const TOptimizerStatistics& leftStats,
        const TOptimizerStatistics& rightStats,
        const TVector<TJoinColumn>& leftJoinKeys,
        const TVector<TJoinColumn>& rightJoinKeys,
        EJoinAlgoType joinAlgo,
        EJoinKind joinKind,
        TCardinalityHints::TCardinalityHint* maybeHint
    ) const override {
        CalledComputeJoinStats.insert(joinAlgo);
        return TBaseProviderContext::ComputeJoinStats(leftStats, rightStats, leftJoinKeys, rightJoinKeys, joinAlgo, joinKind, maybeHint);
    }

    THashSet<EJoinAlgoType> CalledIsJoinApplicable;
    mutable THashSet<EJoinAlgoType> CalledComputeJoinStats;
};

Y_UNIT_TEST(JoinSearchYT24403) {
    TMockProviderContextYT24403 pctx;
    NYql::TExprContext dummyCtx;

    TCBOSettings settings{};
    std::unique_ptr<IOptimizerNew> optimizer = std::unique_ptr<IOptimizerNew>(MakeNativeOptimizerNew(pctx, settings, dummyCtx, false));

    const TString relName1 = "a";
    const TString relName2 = "b";
    const TString colName1 = "x";
    const TString colName2 = "x";

    auto rel1 = std::make_shared<TRelOptimizerNode>(relName1,
        TOptimizerStatistics(BaseTable, 1, 1, 0, 1));
    auto rel2 = std::make_shared<TRelOptimizerNode>(relName2,
        TOptimizerStatistics(BaseTable, 1, 1, 0, 1));

    TVector<TJoinColumn> leftKeys = {TJoinColumn(relName1, colName1)};
    TVector<TJoinColumn> rightKeys ={TJoinColumn(relName2, colName2)};

    auto op = std::make_shared<TJoinOptimizerNode>(
        std::static_pointer_cast<IBaseOptimizerNode>(rel1),
        std::static_pointer_cast<IBaseOptimizerNode>(rel2),
        leftKeys,
        rightKeys,
        InnerJoin,
        EJoinAlgoType::GraceJoin,
        false,
        false
    );

    auto res = optimizer->JoinSearch(op);

    for (auto joinAlgo : AllJoinAlgos) {
        UNIT_ASSERT(pctx.CalledIsJoinApplicable.count(joinAlgo) > 0);
        UNIT_ASSERT(pctx.CalledComputeJoinStats.count(joinAlgo) > 0);
    }
}

Y_UNIT_TEST(RelCollector) {
    NYql::TExprContext ctx;
    auto pos = ctx.AppendPosition({});
    TVector<NYql::NNodes::TExprBase> joinArgs;
    TVector<NYql::NNodes::TExprBase> tables;
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

    TKqpStatsStore kqpStats;
    TVector<std::shared_ptr<TRelOptimizerNode>> rels;
    UNIT_ASSERT(KqpCollectJoinRelationsWithStats(rels, kqpStats, equiJoin, [&](auto, auto, auto, auto) {}) == false);

    kqpStats.SetStats(tables[1].Ptr()->Child(0), std::make_shared<TOptimizerStatistics>(BaseTable, 1, 1, 1));
    UNIT_ASSERT(KqpCollectJoinRelationsWithStats(rels, kqpStats, equiJoin, [&](auto, auto, auto, auto) {}) == false);

    kqpStats.SetStats(tables[0].Ptr()->Child(0), std::make_shared<TOptimizerStatistics>(BaseTable, 1, 1, 1));
    kqpStats.SetStats(tables[2].Ptr()->Child(0), std::make_shared<TOptimizerStatistics>(BaseTable, 1, 1, 1));

    TVector<TString> labels;
    UNIT_ASSERT(KqpCollectJoinRelationsWithStats(rels, kqpStats, equiJoin, [&](auto, auto label, auto, auto) { labels.emplace_back(label); }) == true);
    UNIT_ASSERT(labels.size() == 3);
    UNIT_ASSERT_STRINGS_EQUAL(labels[0], "orders");
    UNIT_ASSERT_STRINGS_EQUAL(labels[1], "customer");
    UNIT_ASSERT_STRINGS_EQUAL(labels[2], "nation");
}

Y_UNIT_TEST(RelCollectorBrokenEquiJoin) {
    NYql::TExprContext ctx;
    auto pos = ctx.AppendPosition({});
    TVector<NYql::NNodes::TExprBase> joinArgs;
    auto joinTree = Build<TCoAtomList>(ctx, pos).Done().Ptr();
    auto settings = Build<TCoAtomList>(ctx, pos).Done().Ptr();
    TCoEquiJoin equiJoin = Build<TCoEquiJoin>(ctx, pos)
        .Add(joinArgs)
        .Done();

    TKqpStatsStore kqpStats;
    TVector<std::shared_ptr<TRelOptimizerNode>> rels;
    UNIT_ASSERT(KqpCollectJoinRelationsWithStats(rels, kqpStats, equiJoin, [&](auto, auto, auto, auto) {}) == false);
}

void _KqpOptimizeEquiJoinWithCosts(const std::function<IOptimizerNew*()>& optFactory, NYql::TExprContext& ctx) {
    NYql::TTypeAnnotationContext typeCtx;
    TKqpStatsStore kqpStats;
    auto pos = ctx.AppendPosition({});
    TVector<NYql::NNodes::TExprBase> joinArgs;
    TVector<NYql::NNodes::TExprBase> tables;
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

    kqpStats.SetStats(tables[0].Ptr()->Child(0), std::make_shared<TOptimizerStatistics>(BaseTable, 1, 1, 1));
    kqpStats.SetStats(tables[1].Ptr()->Child(0), std::make_shared<TOptimizerStatistics>(BaseTable, 1, 1, 1));

    TCoEquiJoin equiJoin = Build<TCoEquiJoin>(ctx, pos)
        .Add(joinArgs)
        .Done();

    auto opt = std::unique_ptr<IOptimizerNew>(optFactory());
    std::function<void(TVector<std::shared_ptr<TRelOptimizerNode>>&, TStringBuf, const NYql::TExprNode::TPtr, const std::shared_ptr<TOptimizerStatistics>&)> providerCollect = [](auto& rels, auto label, auto node, auto stats) {
        Y_UNUSED(node);
        auto rel = std::make_shared<TRelOptimizerNode>(TString(label), *stats);
        rels.push_back(rel);
    };
    auto res = KqpOptimizeEquiJoinWithCosts(equiJoin, ctx, typeCtx, kqpStats, 2, *opt, providerCollect);
    UNIT_ASSERT(equiJoin.Ptr() != res.Ptr());
    UNIT_ASSERT(equiJoin.Ptr()->ChildrenSize() == res.Ptr()->ChildrenSize());
    UNIT_ASSERT(equiJoin.Maybe<TCoEquiJoin>());
    auto resStr = NYql::NCommon::ExprToPrettyString(ctx, *res.Ptr());
    auto expected = R"__((
(let $1 '('"Inner" '"orders" '"customer" '('"orders" '"a") '('"customer" '"b") '('('join_algo 'MapJoin))))
(return (EquiJoin '('() '"orders") '('() '"customer") $1 '()))
)
)__";
    UNIT_ASSERT_STRINGS_EQUAL(expected, resStr);
}

Y_UNIT_TEST(KqpOptimizeEquiJoinWithCostsNative) {
    NYql::TExprContext ctx;
    TBaseProviderContext pctx;
    std::function<IOptimizerNew*()> optFactory = [&]() {
        TCBOSettings settings{};
        return MakeNativeOptimizerNew(pctx, settings, ctx, false);
    };
    _KqpOptimizeEquiJoinWithCosts(optFactory, ctx);
}

} // KqpCBO

#include <ydb/core/kqp/opt/rbo/kqp_rbo.h>
#include <ydb/core/kqp/opt/rbo/physical_conversion/kqp_rbo_physical_window_builder.h>

#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/core/type_ann/type_ann_expr.h>

namespace NKikimr::NKqp {
namespace {

using namespace NYql;

void CheckRankingWindow(const TString& function, bool partitioned, bool ordered) {
    TExprContext ctx;
    TTypeAnnotationContext types;
    const auto pos = TPositionHandle();
    TInfoUnitRegistry registry;
    const auto key = registry.Add(TInfoUnit("key"));
    const auto partition = registry.Add(TInfoUnit("partition"));
    const auto result = registry.Add(TInfoUnit("result"));
    auto read = MakeIntrusive<TOpRead>("", TUnorderedIUs{key, partition}, NYql::EStorageType::RowStorage,
        nullptr, nullptr, nullptr, std::nullopt, std::nullopt, ESortDir::None, TPhysicalOpProps{}, pos);
    const auto* keyType = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
    const auto* resultType = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Uint64);
    const auto* keyItem = ctx.MakeType<TItemExprType>(ToString(key), keyType);
    read->Type = ctx.MakeType<TListExprType>(ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{
        keyItem, ctx.MakeType<TItemExprType>(ToString(partition), keyType)}));
    TSortIUs order;
    if (ordered) {
        order.Append(key, TSortOrder{true, true});
    }
    auto window = MakeIntrusive<TOpWindow>(read, pos,
        TWindowIUs{{result, {function, EWindowFuncKind::Native, {}}}},
        partitioned ? TOrderedIUs<>{partition} : TOrderedIUs<>{}, order, TOpWindowFrame{});
    window->Type = ctx.MakeType<TListExprType>(ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{
        keyItem, ctx.MakeType<TItemExprType>(ToString(partition), keyType),
        ctx.MakeType<TItemExprType>(ToString(result), resultType)}));
    TOpRoot root(window, pos, {{key, "key"}, {result, "result"}});
    root.PlanProps.InfoUnitRegistry = registry;
    root.RecomputeOutputIUsSubtree();
    root.ComputeParents();
    ComputePlanLiveness(root);
    const TPhysicalNames names(registry);
    TExprNode::TListType rows;
    for (const auto value : {"5", "2", "5"}) {
        rows.push_back(ctx.NewCallable(pos, "AsStruct", {ctx.NewList(pos, {
            ctx.NewAtom(pos, names.Get(key)), ctx.NewCallable(pos, "Int32", {ctx.NewAtom(pos, value)})}),
            ctx.NewList(pos, {ctx.NewAtom(pos, names.Get(partition)),
                ctx.NewCallable(pos, "Int32", {ctx.NewAtom(pos, "1")})})}));
    }
    UNIT_ASSERT(TPhysicalWindowBuilder::CanBuildWindow(*window));
    auto physical = TPhysicalWindowBuilder(*window, ctx, pos, names).BuildPhysicalOp(
        ctx.NewCallable(pos, "AsList", std::move(rows)));
    UNIT_ASSERT_C(NYql::InstantAnnotateTypes(physical, ctx, false, types), ctx.IssueManager.GetIssues().ToString());
    const auto* item = physical->GetTypeAnn()->Cast<TStreamExprType>()->GetItemType()->Cast<TStructExprType>();
    UNIT_ASSERT(item->FindItemType(names.Get(result)) == resultType);
}

} // namespace

Y_UNIT_TEST_SUITE(KqpPhysicalRankingWindow) {
    Y_UNIT_TEST(UnorderedRank) {
        for (bool partitioned : {false, true}) {
            CheckRankingWindow("rank", partitioned, false);
        }
    }

    Y_UNIT_TEST(UnorderedDenseRank) {
        for (bool partitioned : {false, true}) {
            CheckRankingWindow("denserank", partitioned, false);
        }
    }

    Y_UNIT_TEST(OrderedRankingAndRowNumber) {
        for (const auto& function : {TString("rank"), TString("denserank"), TString("rownumber")}) {
            for (bool partitioned : {false, true}) {
                CheckRankingWindow(function, partitioned, true);
            }
        }
        CheckRankingWindow("rownumber", false, false);
    }
}

} // namespace NKikimr::NKqp

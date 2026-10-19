#include <ydb/core/kqp/opt/rbo/kqp_rbo.h>
#include <ydb/core/kqp/opt/rbo/physical_conversion/kqp_rbo_physical_window_builder.h>

#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/core/type_ann/type_ann_expr.h>

namespace NKikimr::NKqp {
namespace {

using namespace NYql;

void CheckWindowSort(const TString& function, bool extraSortKey, bool ascending) {
    TExprContext ctx;
    TTypeAnnotationContext types;
    const auto pos = TPositionHandle();
    TInfoUnitRegistry registry;
    const auto key = registry.Add(TInfoUnit("key"));
    const auto value = registry.Add(TInfoUnit("value"));
    const auto result = registry.Add(TInfoUnit("result"));
    auto read = MakeIntrusive<TOpRead>("", TUnorderedIUs{key, value}, NYql::EStorageType::RowStorage,
        nullptr, nullptr, nullptr, std::nullopt, std::nullopt, ESortDir::None, TPhysicalOpProps{}, pos);
    const auto* keyType = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
    const auto* resultType = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Uint64);
    const auto* keyItem = ctx.MakeType<TItemExprType>(ToString(key), keyType);
    read->Type = ctx.MakeType<TListExprType>(ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{
        keyItem, ctx.MakeType<TItemExprType>(ToString(value), keyType)}));
    TSortIUs order;
    order.Append(key, TSortOrder{ascending, true});
    if (extraSortKey) {
        order.Append(value, TSortOrder{ascending, true});
    }
    auto window = MakeIntrusive<TOpWindow>(read, pos,
        TWindowIUs{{result, {function, EWindowFuncKind::Native, {}}}},
        TOrderedIUs<>{key}, order, TOpWindowFrame{});
    window->Type = ctx.MakeType<TListExprType>(ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{
        keyItem, ctx.MakeType<TItemExprType>(ToString(value), keyType),
        ctx.MakeType<TItemExprType>(ToString(result), resultType)}));
    TOpRoot root(window, pos, {{key, "key"}, {result, "result"}});
    root.PlanProps.InfoUnitRegistry = registry;
    root.RecomputeOutputIUsSubtree();
    root.ComputeParents();
    ComputePlanLiveness(root);
    const TPhysicalNames names(registry);
    TExprNode::TListType rows;
    for (const auto literal : {"5", "2", "5"}) {
        rows.push_back(ctx.NewCallable(pos, "AsStruct", {ctx.NewList(pos, {
            ctx.NewAtom(pos, names.Get(key)), ctx.NewCallable(pos, "Int32", {ctx.NewAtom(pos, "1")})}),
            ctx.NewList(pos, {ctx.NewAtom(pos, names.Get(value)),
                ctx.NewCallable(pos, "Int32", {ctx.NewAtom(pos, literal)})})}));
    }
    UNIT_ASSERT(TPhysicalWindowBuilder::CanBuildWindow(*window));
    auto physical = TPhysicalWindowBuilder(*window, ctx, pos, names).BuildPhysicalOp(
        ctx.NewCallable(pos, "AsList", std::move(rows)));
    UNIT_ASSERT_C(NYql::InstantAnnotateTypes(physical, ctx, false, types), ctx.IssueManager.GetIssues().ToString());
    const auto* item = physical->GetTypeAnn()->Cast<TStreamExprType>()->GetItemType()->Cast<TStructExprType>();
    UNIT_ASSERT(item->FindItemType(names.Get(result)) == resultType);
}

} // namespace

Y_UNIT_TEST_SUITE(KqpPhysicalWindowSort) {
    Y_UNIT_TEST(SharedPartitionAndSortKey) {
        for (const auto& function : {TString("rank"), TString("denserank"), TString("rownumber")}) {
            for (bool ascending : {false, true}) {
                CheckWindowSort(function, false, ascending);
            }
        }
    }

    Y_UNIT_TEST(SharedPartitionAndSortKeyWithTrailingKey) {
        for (const auto& function : {TString("rank"), TString("denserank"), TString("rownumber")}) {
            for (bool ascending : {false, true}) {
                CheckWindowSort(function, true, ascending);
            }
        }
    }
}

} // namespace NKikimr::NKqp

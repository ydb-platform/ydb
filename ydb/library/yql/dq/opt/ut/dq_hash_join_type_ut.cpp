#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/providers/common/transform/yql_visit.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NDq {
namespace {

TExprNode::TPtr WideFlowNode(TExprContext& ctx, TPositionHandle pos,
    std::initializer_list<const TTypeAnnotationNode*> items)
{
    auto flow = ctx.NewArgument(pos, "flow");
    flow->SetTypeAnn(ctx.MakeType<TFlowExprType>(
        ctx.MakeType<TMultiExprType>(TTypeAnnotationNode::TListType(items))));
    return flow;
}

TExprNode::TPtr WideBlockStream(TExprContext& ctx, TPositionHandle pos,
    std::initializer_list<const TTypeAnnotationNode*> items)
{
    TTypeAnnotationNode::TListType blockItems;
    for (const auto* item : items) {
        blockItems.push_back(ctx.MakeType<TBlockExprType>(item));
    }
    blockItems.push_back(ctx.MakeType<TScalarExprType>(ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Uint64)));
    auto stream = ctx.NewArgument(pos, "stream");
    stream->SetTypeAnn(ctx.MakeType<TStreamExprType>(ctx.MakeType<TMultiExprType>(blockItems)));
    return stream;
}

TExprNode::TPtr ScalarHashJoin(TExprContext& ctx, TPositionHandle pos, TStringBuf joinKind,
    TExprNode::TPtr left, TExprNode::TPtr right)
{
    return ctx.NewCallable(pos, "ScalarHashJoinCore", {
        std::move(left),
        std::move(right),
        ctx.NewAtom(pos, joinKind),
        ctx.NewList(pos, {ctx.NewAtom(pos, "0")}),
        ctx.NewList(pos, {ctx.NewAtom(pos, "0")}),
        ctx.NewList(pos, {ctx.NewAtom(pos, "a.key")}),
        ctx.NewList(pos, {ctx.NewAtom(pos, "b.key")}),
    });
}

TExprNode::TPtr BlockHashJoin(TExprContext& ctx, TPositionHandle pos, TStringBuf joinKind,
    TExprNode::TPtr left, TExprNode::TPtr right)
{
    return ctx.NewCallable(pos, "BlockHashJoinCore", {
        std::move(left),
        std::move(right),
        ctx.NewAtom(pos, joinKind),
        ctx.NewList(pos, {ctx.NewAtom(pos, "0")}),
        ctx.NewList(pos, {ctx.NewAtom(pos, "0")}),
        ctx.NewList(pos, {ctx.NewAtom(pos, "a.key")}),
        ctx.NewList(pos, {ctx.NewAtom(pos, "b.key")}),
        ctx.NewList(pos, {}),
    });
}

const TTypeAnnotationNode::TListType& AnnotateItems(TExprContext& ctx, const TExprNode::TPtr& join) {
    auto transformer = CreateDqTypeAnnotationTransformer();
    TExprNode::TPtr output;
    const auto status = transformer->Transform(join, output, ctx);
    UNIT_ASSERT_C(status == IGraphTransformer::TStatus::Ok, ctx.IssueManager.GetIssues().ToString());
    const TTypeAnnotationNode* sequence = output->GetTypeAnn();
    UNIT_ASSERT(sequence);
    const auto* itemType = sequence->GetKind() == ETypeAnnotationKind::Flow
        ? sequence->Cast<TFlowExprType>()->GetItemType()
        : sequence->Cast<TStreamExprType>()->GetItemType();
    return itemType->Cast<TMultiExprType>()->GetItems();
}

} // namespace

Y_UNIT_TEST_SUITE(DqHashJoinNullColumnType) {
    Y_UNIT_TEST(ScalarLeftJoinKeepsNull) {
        TExprContext ctx;
        const auto pos = ctx.AppendPosition({});
        const auto* keyType = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::String);
        const auto* payloadType = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
        const auto* nullType = ctx.MakeType<TNullExprType>();
        const auto* optionalPayload = ctx.MakeType<TOptionalExprType>(payloadType);

        const auto join = ScalarHashJoin(ctx, pos, "Left",
            WideFlowNode(ctx, pos, {keyType, payloadType}),
            WideFlowNode(ctx, pos, {keyType, nullType, optionalPayload, payloadType}));
        const auto& items = AnnotateItems(ctx, join);

        UNIT_ASSERT_VALUES_EQUAL(items.size(), 6U);
        UNIT_ASSERT(items[0] == keyType);
        UNIT_ASSERT(items[1] == payloadType);
        UNIT_ASSERT(items[2] == ctx.MakeType<TOptionalExprType>(keyType));
        UNIT_ASSERT(items[3] == nullType);
        UNIT_ASSERT(items[4] == optionalPayload);
        UNIT_ASSERT(items[5] == optionalPayload);
    }

    Y_UNIT_TEST(ScalarInnerJoinDoesNotWrap) {
        TExprContext ctx;
        const auto pos = ctx.AppendPosition({});
        const auto* keyType = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::String);
        const auto* nullType = ctx.MakeType<TNullExprType>();

        const auto join = ScalarHashJoin(ctx, pos, "Inner",
            WideFlowNode(ctx, pos, {keyType}),
            WideFlowNode(ctx, pos, {keyType, nullType}));
        const auto& items = AnnotateItems(ctx, join);

        UNIT_ASSERT_VALUES_EQUAL(items.size(), 3U);
        UNIT_ASSERT(items[2] == nullType);
    }

    Y_UNIT_TEST(BlockLeftJoinKeepsNull) {
        TExprContext ctx;
        const auto pos = ctx.AppendPosition({});
        const auto* keyType = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::String);
        const auto* payloadType = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
        const auto* nullType = ctx.MakeType<TNullExprType>();

        const auto join = BlockHashJoin(ctx, pos, "Left",
            WideBlockStream(ctx, pos, {keyType, payloadType}),
            WideBlockStream(ctx, pos, {keyType, nullType, payloadType}));
        const auto& items = AnnotateItems(ctx, join);

        UNIT_ASSERT_VALUES_EQUAL(items.size(), 6U);
        UNIT_ASSERT(items[2]->Cast<TBlockExprType>()->GetItemType() == ctx.MakeType<TOptionalExprType>(keyType));
        UNIT_ASSERT(items[3]->Cast<TBlockExprType>()->GetItemType() == nullType);
        UNIT_ASSERT(items[4]->Cast<TBlockExprType>()->GetItemType() == ctx.MakeType<TOptionalExprType>(payloadType));
        UNIT_ASSERT(items[5]->GetKind() == ETypeAnnotationKind::Scalar);
    }
}

} // namespace NYql::NDq

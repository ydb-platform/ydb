#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/providers/common/transform/yql_visit.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NDq {
namespace {

const TTypeAnnotationNode::TListType& AnnotateLeftJoin(TExprContext& ctx, TStringBuf callable,
    const TTypeAnnotationNode* leftType, const TTypeAnnotationNode* rightType)
{
    const auto pos = ctx.AppendPosition({});
    auto left = ctx.NewArgument(pos, "left");
    left->SetTypeAnn(leftType);
    auto right = ctx.NewArgument(pos, "right");
    right->SetTypeAnn(rightType);
    const auto join = ctx.NewCallable(pos, callable, {
        std::move(left),
        std::move(right),
        ctx.NewAtom(pos, "Left"),
        ctx.NewList(pos, {ctx.NewAtom(pos, "0")}),
        ctx.NewList(pos, {ctx.NewAtom(pos, "0")}),
        ctx.NewList(pos, {ctx.NewAtom(pos, "a.key")}),
        ctx.NewList(pos, {ctx.NewAtom(pos, "b.key")}),
        ctx.NewList(pos, {}),
    });

    TExprNode::TPtr output;
    const auto status = CreateDqTypeAnnotationTransformer()->Transform(join, output, ctx);
    UNIT_ASSERT_C(status == IGraphTransformer::TStatus::Ok, ctx.IssueManager.GetIssues().ToString());
    return GetSeqItemType(*output->GetTypeAnn()).Cast<TMultiExprType>()->GetItems();
}

} // namespace

Y_UNIT_TEST_SUITE(DqHashJoinNullColumnType) {
    Y_UNIT_TEST(ScalarLeftJoinKeepsNull) {
        TExprContext ctx;
        const auto* keyType = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Uint64);
        const auto* nullType = ctx.MakeType<TNullExprType>();
        const auto flow = [&](TTypeAnnotationNode::TListType items) {
            return ctx.MakeType<TFlowExprType>(ctx.MakeType<TMultiExprType>(items));
        };

        const auto& items = AnnotateLeftJoin(ctx, "ScalarHashJoinCore",
            flow({keyType}), flow({keyType, nullType}));

        UNIT_ASSERT_VALUES_EQUAL(items.size(), 3U);
        UNIT_ASSERT(items[1] == ctx.MakeType<TOptionalExprType>(keyType));
        UNIT_ASSERT(items[2] == nullType);
    }

    Y_UNIT_TEST(BlockLeftJoinKeepsNull) {
        TExprContext ctx;
        const auto* keyType = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Uint64);
        const auto* nullType = ctx.MakeType<TNullExprType>();
        const auto stream = [&](TTypeAnnotationNode::TListType items) {
            for (auto& item : items) {
                item = ctx.MakeType<TBlockExprType>(item);
            }
            items.push_back(ctx.MakeType<TScalarExprType>(keyType));
            return ctx.MakeType<TStreamExprType>(ctx.MakeType<TMultiExprType>(items));
        };

        const auto& items = AnnotateLeftJoin(ctx, "BlockHashJoinCore",
            stream({keyType}), stream({keyType, nullType}));

        UNIT_ASSERT_VALUES_EQUAL(items.size(), 4U);
        UNIT_ASSERT(items[1] == ctx.MakeType<TBlockExprType>(ctx.MakeType<TOptionalExprType>(keyType)));
        UNIT_ASSERT(items[2] == ctx.MakeType<TBlockExprType>(nullType));
    }
}

} // namespace NYql::NDq

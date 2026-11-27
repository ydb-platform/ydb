#include <yql/essentials/core/yql_expr_csee.h>
#include <yql/essentials/core/yql_expr_constraint.h>
#include <yql/essentials/core/type_ann/type_ann_expr.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {
namespace {

TExprNode::TPtr MakeInnerMap(TExprContext& ctx, const TExprNode::TPtr& list,
                             const TExprNode::TPtr& captured, ui32 depth) {
    const TPositionHandle pos;
    auto inner = ctx.NewArgument(pos, "inner");
    auto body = ctx.NewCallable(pos, "Add", {captured, inner});
    for (ui32 i = 0; i < depth; ++i) {
        body = ctx.NewCallable(pos, "Add", {body, body});
    }
    auto innerLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {inner}), std::move(body));
    return ctx.NewCallable(pos, "Map", {list, innerLambda});
}

TExprNode::TPtr MakeInput(TExprContext& ctx) {
    const TPositionHandle pos;
    return ctx.NewCallable(pos, "AsList", {ctx.NewCallable(pos, "Uint64", {ctx.NewAtom(pos, "1")})});
}

TExprNode::TPtr MakeMixedDag(TExprContext& ctx, ui32 depth) {
    const TPositionHandle pos;
    auto outer = ctx.NewArgument(pos, "outer");
    auto list = MakeInput(ctx);
    auto innerMap = MakeInnerMap(ctx, list, outer, depth);
    auto outerLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {outer}), std::move(innerMap));
    return ctx.NewCallable(pos, "Map", {list, outerLambda});
}

void RunCsee(TExprNode::TPtr& root, TExprContext& ctx) {
    TTypeAnnotationContext types;
    UNIT_ASSERT_C(InstantAnnotateTypes(root, ctx, false, types), ctx.IssueManager.GetIssues().ToString());
    auto constraints = CreateConstraintTransformer(types);
    UNIT_ASSERT_EQUAL(InstantTransform(*constraints, root, ctx).Level, IGraphTransformer::TStatus::Ok);
    UNIT_ASSERT_EQUAL(UpdateCompletness(root, root, ctx).Level, IGraphTransformer::TStatus::Ok);
    UNIT_ASSERT_EQUAL(EliminateCommonSubExpressions(root, root, ctx, false, *types.ColumnOrderStorage).Level,
                      IGraphTransformer::TStatus::Ok);
}

} // namespace

Y_UNIT_TEST_SUITE(TCsee) {
Y_UNIT_TEST(SharedMixedDependencyDag) {
    // Thirty-two shared nodes have billions of paths if hashing expands the DAG.
    for (ui32 depth : {0U, 1U, 32U}) {
        TExprContext ctx;
        auto root = MakeMixedDag(ctx, depth);
        RunCsee(root, ctx);
    }
}

Y_UNIT_TEST(EquivalentMixedLambdas) {
    TExprContext ctx;
    auto root = ctx.NewCallable(TPositionHandle(), "AsList", {MakeMixedDag(ctx, 8), MakeMixedDag(ctx, 8)});
    RunCsee(root, ctx);
    UNIT_ASSERT_EQUAL(root->Child(0), root->Child(1));
}

Y_UNIT_TEST(DistinctCapturedArguments) {
    TExprContext ctx;
    const TPositionHandle pos;
    auto list = MakeInput(ctx);
    auto outer = ctx.NewArgument(pos, "outer");
    auto middle = ctx.NewArgument(pos, "middle");
    auto maps = ctx.NewCallable(pos, "AsList", {MakeInnerMap(ctx, list, outer, 8), MakeInnerMap(ctx, list, outer, 8), MakeInnerMap(ctx, list, middle, 8)});
    auto middleLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {middle}), std::move(maps));
    auto middleMap = ctx.NewCallable(pos, "Map", {list, middleLambda});
    auto outerLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {outer}), std::move(middleMap));
    auto root = ctx.NewCallable(pos, "Map", {list, outerLambda});
    RunCsee(root, ctx);
    const auto& optimizedMaps = root->Tail().Tail().Tail().Tail();
    UNIT_ASSERT_EQUAL(optimizedMaps.Child(0), optimizedMaps.Child(1));
    UNIT_ASSERT_UNEQUAL(optimizedMaps.Child(0), optimizedMaps.Child(2));
}
} // Y_UNIT_TEST_SUITE(TCsee)

} // namespace NYql

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/expr_nodes_gen/yql_expr_nodes_gen.h>
#include <yql/essentials/core/yql_type_annotation.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp::NOpt {
NYql::NNodes::TExprBase KqpEliminateWideMapPackUnpack(
    const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx,
    NYql::TTypeAnnotationContext& typesCtx);
NYql::NNodes::TExprBase KqpEliminateBlockMemberOverBlockAsStruct(
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

    Y_UNIT_TEST(BlockMemberOverBlockAsStructIsFolded) {
        NYql::TExprContext ctx;
        NYql::TTypeAnnotationContext typesCtx;
        auto pos = NYql::TPositionHandle();

        auto valA = ctx.NewArgument(pos, "a");
        auto valB = ctx.NewArgument(pos, "b");
        auto bas = ctx.NewCallable(pos, "BlockAsStruct", {
            ctx.NewList(pos, {ctx.NewAtom(pos, "c1"), valA}),
            ctx.NewList(pos, {ctx.NewAtom(pos, "c2"), valB}),
        });

        auto member = ctx.NewCallable(pos, "BlockMember", {bas, ctx.NewAtom(pos, "c2")});
        auto result = NOpt::KqpEliminateBlockMemberOverBlockAsStruct(NYql::NNodes::TExprBase(member), ctx, typesCtx);

        UNIT_ASSERT_C(result.Ptr() == valB,
            "BlockMember over freshly built BlockAsStruct should fold to the field value");
    }

    Y_UNIT_TEST(BlockMemberOverNonStructIsPreserved) {
        NYql::TExprContext ctx;
        NYql::TTypeAnnotationContext typesCtx;
        auto pos = NYql::TPositionHandle();

        auto opaque = ctx.NewCallable(pos, "SomeOtherCallable", {});
        auto member = ctx.NewCallable(pos, "BlockMember", {opaque, ctx.NewAtom(pos, "c1")});
        auto result = NOpt::KqpEliminateBlockMemberOverBlockAsStruct(NYql::NNodes::TExprBase(member), ctx, typesCtx);

        UNIT_ASSERT_C(result.Ptr() == member,
            "BlockMember over a non-BlockAsStruct input must be preserved");
    }

    Y_UNIT_TEST(BlockMemberPicksCorrectField) {
        NYql::TExprContext ctx;
        NYql::TTypeAnnotationContext typesCtx;
        auto pos = NYql::TPositionHandle();

        auto valA = ctx.NewArgument(pos, "a");
        auto valB = ctx.NewArgument(pos, "b");
        auto valC = ctx.NewArgument(pos, "c");
        auto bas = ctx.NewCallable(pos, "BlockAsStruct", {
            ctx.NewList(pos, {ctx.NewAtom(pos, "alpha"), valA}),
            ctx.NewList(pos, {ctx.NewAtom(pos, "beta"), valB}),
            ctx.NewList(pos, {ctx.NewAtom(pos, "gamma"), valC}),
        });

        auto memberA = ctx.NewCallable(pos, "BlockMember", {bas, ctx.NewAtom(pos, "alpha")});
        auto memberB = ctx.NewCallable(pos, "BlockMember", {bas, ctx.NewAtom(pos, "beta")});
        auto memberC = ctx.NewCallable(pos, "BlockMember", {bas, ctx.NewAtom(pos, "gamma")});

        UNIT_ASSERT_C(
            NOpt::KqpEliminateBlockMemberOverBlockAsStruct(NYql::NNodes::TExprBase(memberA), ctx, typesCtx).Ptr() == valA,
            "BlockMember 'alpha' should fold to valA");
        UNIT_ASSERT_C(
            NOpt::KqpEliminateBlockMemberOverBlockAsStruct(NYql::NNodes::TExprBase(memberB), ctx, typesCtx).Ptr() == valB,
            "BlockMember 'beta' should fold to valB");
        UNIT_ASSERT_C(
            NOpt::KqpEliminateBlockMemberOverBlockAsStruct(NYql::NNodes::TExprBase(memberC), ctx, typesCtx).Ptr() == valC,
            "BlockMember 'gamma' should fold to valC");
    }
}

} // namespace NKikimr::NKqp

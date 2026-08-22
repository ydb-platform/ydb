#include <ydb/core/kqp/opt/peephole/kqp_opt_peephole_rules.h>

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_type_annotation.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NKqp;
using namespace NKikimr::NKqp::NOpt;
using namespace NYql;
using namespace NYql::NNodes;

namespace {

constexpr TStringBuf ResidentCallable = "KqpWasmResidentString";

TExprNode::TPtr MakeUdf(TExprContext& ctx, TPositionHandle pos) {
    return ctx.NewCallable(pos, "Udf", {ctx.NewAtom(pos, "WasmMod.Func")});
}

//! A loop-invariant string value: a parameter member, no lambda arguments in its
//! subtree. Mimics the scalar `$dict` precompute the rule targets.
TExprNode::TPtr MakeParamString(TExprContext& ctx, TPositionHandle pos) {
    auto param = ctx.NewCallable(pos, "Parameter", {ctx.NewAtom(pos, "%kqp%tx_result_binding_0_0")});
    param->SetTypeAnn(ctx.MakeType<TDataExprType>(NUdf::EDataSlot::String));
    return param;
}

TExprNode::TPtr MakeMember(TExprContext& ctx, TPositionHandle pos, const TExprNode::TPtr& row, TStringBuf name) {
    auto member = ctx.NewCallable(pos, "Member", {row, ctx.NewAtom(pos, name)});
    member->SetTypeAnn(ctx.MakeType<TDataExprType>(NUdf::EDataSlot::String));
    return member;
}

TExprNode::TPtr Rewrite(TExprContext& ctx, const TExprNode::TPtr& apply) {
    return KqpRewriteWasmResidentConstArgs(TExprBase(apply), ctx).Ptr();
}

} // namespace

Y_UNIT_TEST_SUITE(KqpWasmResidentConstArgs) {

Y_UNIT_TEST(WrapsLoopInvariantStringArg) {
    TExprContext ctx;
    const auto pos = ctx.AppendPosition({});

    auto apply = ctx.NewCallable(pos, "Apply", {MakeUdf(ctx, pos), MakeParamString(ctx, pos)});
    auto result = Rewrite(ctx, apply);

    UNIT_ASSERT_VALUES_EQUAL(result->ChildrenSize(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(result->Child(1)->Content(), ResidentCallable);
    UNIT_ASSERT_VALUES_EQUAL(result->Child(1)->Child(0)->Content(), "Parameter");
}

Y_UNIT_TEST(DoesNotWrapPerRowArg) {
    TExprContext ctx;
    const auto pos = ctx.AppendPosition({});

    auto row = ctx.NewArgument(pos, "row");
    auto apply = ctx.NewCallable(pos, "Apply", {MakeUdf(ctx, pos), MakeMember(ctx, pos, row, "addr")});
    auto result = Rewrite(ctx, apply);

    // Same node back, argument untouched.
    UNIT_ASSERT(result == apply);
    UNIT_ASSERT_VALUES_EQUAL(result->Child(1)->Content(), "Member");
}

Y_UNIT_TEST(IsIdempotent) {
    TExprContext ctx;
    const auto pos = ctx.AppendPosition({});

    auto apply = ctx.NewCallable(pos, "Apply", {MakeUdf(ctx, pos), MakeParamString(ctx, pos)});
    auto once = Rewrite(ctx, apply);
    auto twice = Rewrite(ctx, once);

    // Second pass sees an already wrapped arg and returns the node unchanged.
    UNIT_ASSERT(twice == once);
    UNIT_ASSERT_VALUES_EQUAL(twice->Child(1)->Content(), ResidentCallable);
    UNIT_ASSERT_VALUES_EQUAL(twice->Child(1)->Child(0)->Content(), "Parameter");
}

Y_UNIT_TEST(DoesNotWrapNonStringArg) {
    TExprContext ctx;
    const auto pos = ctx.AppendPosition({});

    auto number = ctx.NewCallable(pos, "Int64", {ctx.NewAtom(pos, "5")});
    number->SetTypeAnn(ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Int64));
    auto apply = ctx.NewCallable(pos, "Apply", {MakeUdf(ctx, pos), std::move(number)});
    auto result = Rewrite(ctx, apply);

    UNIT_ASSERT(result == apply);
    UNIT_ASSERT_VALUES_EQUAL(result->Child(1)->Content(), "Int64");
}

Y_UNIT_TEST(DoesNotWrapNonUdfCallable) {
    TExprContext ctx;
    const auto pos = ctx.AppendPosition({});

    // Callee is a captured lambda argument, not a Udf: we cannot claim it is wasm.
    auto callable = ctx.NewArgument(pos, "callable");
    auto apply = ctx.NewCallable(pos, "Apply", {callable, MakeParamString(ctx, pos)});
    auto result = Rewrite(ctx, apply);

    UNIT_ASSERT(result == apply);
    UNIT_ASSERT_VALUES_EQUAL(result->Child(1)->Content(), "Parameter");
}

Y_UNIT_TEST(WrapsOnlyInvariantOfSeveralArgs) {
    TExprContext ctx;
    const auto pos = ctx.AppendPosition({});

    auto row = ctx.NewArgument(pos, "row");
    auto apply = ctx.NewCallable(pos, "Apply", {
        MakeUdf(ctx, pos),
        MakeMember(ctx, pos, row, "addr"),   // per-row, stays
        MakeParamString(ctx, pos),           // invariant, wrapped
    });
    auto result = Rewrite(ctx, apply);

    UNIT_ASSERT_VALUES_EQUAL(result->ChildrenSize(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(result->Child(1)->Content(), "Member");
    UNIT_ASSERT_VALUES_EQUAL(result->Child(2)->Content(), ResidentCallable);
}

} // Y_UNIT_TEST_SUITE

#include <yt/yql/providers/yt/gateway/lib/yt_helpers.h>

#include <yql/essentials/ast/yql_expr.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {
namespace {

TExprNode::TPtr MakeBool(TExprContext& ctx, TPositionHandle pos, TStringBuf value) {
    return ctx.NewCallable(pos, "Bool", {ctx.NewAtom(pos, value)});
}

TExprNode::TPtr MakeQLFilter(TExprContext& ctx, TPositionHandle pos, TExprNode::TPtr predicate) {
    auto arguments = ctx.NewArguments(pos, {ctx.NewArgument(pos, "row")});
    auto lambda = ctx.NewLambda(pos, std::move(arguments), std::move(predicate));
    return ctx.NewCallable(pos, "YtQLFilter", {ctx.NewAtom(pos, "rowType"), std::move(lambda)});
}

TExprNode::TPtr MakeQLFilter(TExprContext& ctx, TPositionHandle pos, TStringBuf op, TExprNode::TListType operands) {
    auto predicate = ctx.NewCallable(pos, op, std::move(operands));
    return MakeQLFilter(ctx, pos, std::move(predicate));
}

Y_UNIT_TEST_SUITE(TGenerateInputQueryTest) {
    Y_UNIT_TEST(BalancesVariadicOrForDepthLimit) {
        TExprContext ctx;
        const auto pos = ctx.AppendPosition({});
        auto qlFilter = MakeQLFilter(ctx, pos, "Or", {
            MakeBool(ctx, pos, "true"),
            MakeBool(ctx, pos, "false"),
            MakeBool(ctx, pos, "true"),
            MakeBool(ctx, pos, "false"),
        });

        const auto tooShallowQuery = GenerateInputQuery(qlFilter, 2);
        const auto query = GenerateInputQuery(qlFilter, 3);

        UNIT_ASSERT(!tooShallowQuery);
        UNIT_ASSERT(query);
        UNIT_ASSERT_VALUES_EQUAL(
            "* WHERE ((true) OR (false)) OR ((true) OR (false))",
            *query);
    }

    Y_UNIT_TEST(BalancesOddNumberOfAndOperands) {
        TExprContext ctx;
        const auto pos = ctx.AppendPosition({});
        auto qlFilter = MakeQLFilter(ctx, pos, "And", {
            MakeBool(ctx, pos, "true"),
            MakeBool(ctx, pos, "false"),
            MakeBool(ctx, pos, "true"),
            MakeBool(ctx, pos, "false"),
            MakeBool(ctx, pos, "true"),
        });

        const auto query = GenerateInputQuery(qlFilter, 4);

        UNIT_ASSERT(query);
        UNIT_ASSERT_VALUES_EQUAL(
            "* WHERE ((true) AND (false)) AND ((true) AND ((false) AND (true)))",
            *query);
    }

    Y_UNIT_TEST(DoesNotIncreaseDepthForUnevenLogicalOperands) {
        TExprContext ctx;
        const auto pos = ctx.AppendPosition({});
        auto deepOperand = ctx.NewCallable(pos, "And", {
            MakeBool(ctx, pos, "true"),
            ctx.NewCallable(pos, "Or", {
                MakeBool(ctx, pos, "false"),
                MakeBool(ctx, pos, "true"),
            }),
        });
        auto qlFilter = MakeQLFilter(ctx, pos, "Or", {
            MakeBool(ctx, pos, "true"),
            MakeBool(ctx, pos, "false"),
            MakeBool(ctx, pos, "true"),
            std::move(deepOperand),
        });

        const auto tooShallowQuery = GenerateInputQuery(qlFilter, 3);
        const auto query = GenerateInputQuery(qlFilter, 4);

        UNIT_ASSERT(!tooShallowQuery);
        UNIT_ASSERT(query);
        UNIT_ASSERT_VALUES_EQUAL(
            "* WHERE (true) OR (false) OR (true) OR ((true) AND ((false) OR (true)))",
            *query);
    }
}

} // namespace
} // namespace NYql

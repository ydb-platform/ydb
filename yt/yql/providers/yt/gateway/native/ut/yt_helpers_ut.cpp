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
    Y_UNIT_TEST(UsesNullComparisonsForExists) {
        for (bool negated : {false, true}) {
            TExprContext ctx;
            const auto pos = ctx.AppendPosition({});
            auto member = ctx.NewCallable(pos, "Member", {
                ctx.NewArgument(pos, "row"),
                ctx.NewAtom(pos, "value"),
            });
            auto predicate = ctx.NewCallable(pos, "Exists", {std::move(member)});
            if (negated) {
                predicate = ctx.NewCallable(pos, "Not", {std::move(predicate)});
            }
            auto qlFilter = MakeQLFilter(ctx, pos, std::move(predicate));

            UNIT_ASSERT(!GenerateInputQuery(qlFilter, 1));
            const auto query = GenerateInputQuery(qlFilter, 2);
            UNIT_ASSERT(query);
            UNIT_ASSERT_VALUES_EQUAL(
                negated ? "* WHERE (`value`) = NULL" : "* WHERE (`value`) != NULL",
                *query);
        }
    }

    Y_UNIT_TEST(UsesNullComparisonsForOptionalComparisons) {
        struct TCase {
            TStringBuf Op;
            TStringBuf Value;
            bool NullValue;
            TStringBuf Expected;
            ui32 Depth;
        };
        for (const auto& test : {
            TCase{"<", "1", false, "* WHERE `value` != NULL AND `value` < 1", 3},
            TCase{">", "1", true, "* WHERE `value` = NULL OR `value` > 1", 3},
            TCase{"<", "-1", false, "* WHERE `value` != NULL AND `value` < -1", 4},
            TCase{"<", "300", false, "* WHERE `value` != NULL", 2},
            TCase{">", "300", true, "* WHERE `value` = NULL", 2},
        }) {
            TExprContext ctx;
            const auto pos = ctx.AppendPosition({});
            auto member = ctx.NewCallable(pos, "Member", {
                ctx.NewArgument(pos, "row"),
                ctx.NewAtom(pos, "value"),
            });
            member->SetTypeAnn(ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Int8)));
            auto comparison = ctx.NewCallable(pos, test.Op, {
                std::move(member),
                ctx.NewCallable(pos, "Int32", {ctx.NewAtom(pos, test.Value)}),
            });
            auto predicate = ctx.NewCallable(pos, "Coalesce", {
                std::move(comparison),
                MakeBool(ctx, pos, test.NullValue ? "true" : "false"),
            });
            auto qlFilter = MakeQLFilter(ctx, pos, std::move(predicate));

            UNIT_ASSERT(!GenerateInputQuery(qlFilter, test.Depth - 1));
            const auto query = GenerateInputQuery(qlFilter, test.Depth);
            UNIT_ASSERT(query);
            UNIT_ASSERT_VALUES_EQUAL(test.Expected, *query);
        }
    }

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

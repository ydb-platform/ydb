#include <yt/yql/providers/yt/lib/qlfilter/qlfilter.h>

#include <yql/essentials/ast/yql_expr.h>

#include <yql/essentials/core/yql_expr_type_annotation.h>

#include <yql/essentials/utils/parse_double.h>

#include <library/cpp/testing/unittest/registar.h>

#include <cmath>
#include <limits>

namespace NYql {
namespace {

Y_UNIT_TEST_SUITE(TQLFilterTest) {
    Y_UNIT_TEST(SerializesExtractedConjunct) {
        TExprContext ctx;
        const auto pos = ctx.AppendPosition({});
        const auto* boolType = ctx.MakeType<TDataExprType>(EDataSlot::Bool);
        const auto* rowType = ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{
            ctx.MakeType<TItemExprType>("flag", boolType),
            ctx.MakeType<TItemExprType>("otherFlag", boolType),
        });
        const auto row = ctx.NewArgument(pos, "row");
        row->SetTypeAnn(rowType);
        const auto filterRow = ctx.NewArgument(pos, row->Content());

        const auto makeFlag = [&] (TStringBuf name) {
            auto member = ctx.NewCallable(pos, "Member", {row, ctx.NewAtom(pos, name)});
            member->SetTypeAnn(boolType);
            return member;
        };
        const auto flag = makeFlag("flag");
        const auto notFlag = ctx.NewCallable(pos, "Not", {flag});
        const auto rowComparison = ctx.NewCallable(pos, "==", {flag, makeFlag("otherFlag")});

        const auto disjunction = ctx.NewCallable(pos, "Or", {notFlag, rowComparison});
        UNIT_ASSERT(!ExtractQLFilterPredicate(disjunction, row, filterRow, pos, ctx));

        const auto conjunction = ctx.NewCallable(pos, "And", {notFlag, rowComparison});
        auto selected = ExtractQLFilterPredicate(conjunction, row, filterRow, pos, ctx);
        UNIT_ASSERT(selected);
        UNIT_ASSERT(selected->IsCallable("Not"));
        UNIT_ASSERT(selected->Head().IsCallable("Member"));
        UNIT_ASSERT(selected->Head().HeadPtr() == filterRow);

        auto args = ctx.NewArguments(pos, {filterRow});
        auto lambda = ctx.NewLambda(pos, std::move(args), std::move(selected));
        const auto filter = ctx.NewCallable(pos, "YtQLFilter", {ExpandType(pos, *rowType, ctx), std::move(lambda)});

        UNIT_ASSERT(!GenerateInputQuery(filter, /*depthLimit*/ 1));
        const auto query = GenerateInputQuery(filter, /*depthLimit*/ 2);
        UNIT_ASSERT(query);
        UNIT_ASSERT_VALUES_EQUAL(*query, "* WHERE NOT (`flag`)");
    }
}

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

Y_UNIT_TEST_SUITE(TConvertFloatingValueForQLTest) {
    Y_UNIT_TEST(PreservesValuesAndFloatingPointSyntax) {
        struct TCase {
            TStringBuf Value;
            bool IsFloat;
            double ExpectedValue;
            TStringBuf ExpectedLiteral;
        };
        const double infinity = std::numeric_limits<double>::infinity();
        constexpr double base = 87423.203125;
        for (const auto& test : {
            TCase{"1E2", false, 100.0, "100.0"},
            TCase{"-1e-2", false, -0.01, "-0.01"},
            TCase{"1E100", false, 1e100, "1e+100"},
            TCase{"0xAA", false, 170.0, "170.0"},
            // 2^63 is exact in Double but exceeds Int64.
            TCase{"9223372036854775808", false, 0x1p63, {}},
            // Float(2^27 + 1) rounds to 2^27; QL must recover that exact value as Double.
            TCase{"134217729", true, 0x1p27, {}},
            // Each pair straddles the midpoint between base and its next representable value.
            TCase{"87423.2031250000001", false, base, {}},
            TCase{"87423.20312500001", false, std::nextafter(base, infinity), {}},
            TCase{"87423.207", true, base, {}},
            TCase{"87423.208", true, std::nextafter(static_cast<float>(base), std::numeric_limits<float>::infinity()), {}},
            TCase{"+inf", false, infinity, "inf"},
            TCase{"-inf", false, -infinity, "-inf"},
        }) {
            const auto literal = ConvertFloatingValueForQL(test.Value, test.IsFloat);
            UNIT_ASSERT_C(literal, test.Value);
            UNIT_ASSERT_VALUES_EQUAL_C(DoubleFromString(*literal), test.ExpectedValue, test.Value);
            if (test.ExpectedLiteral) {
                UNIT_ASSERT_VALUES_EQUAL(*literal, test.ExpectedLiteral);
            } else {
                UNIT_ASSERT_C(literal->Contains('.') || literal->Contains('e'), *literal);
            }
        }
        for (bool isFloat : {false, true}) {
            UNIT_ASSERT(!ConvertFloatingValueForQL("nan", isFloat));
        }
        UNIT_ASSERT(!ConvertFloatingValueForQL("1e+", false));
    }
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

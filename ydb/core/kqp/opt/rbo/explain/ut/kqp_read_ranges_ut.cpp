#include <ydb/core/kqp/opt/rbo/explain/kqp_read_ranges_explain.h>

#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/ast/yql_expr.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>
#include <util/string/subst.h>

namespace NKikimr::NKqp::NOpt {
namespace {

NYql::TExprNode::TPtr Compile(TStringBuf expression, NYql::TExprContext& ctx) {
    auto ast = NYql::ParseAst(TStringBuilder() << "((return " << expression << "))");
    UNIT_ASSERT_C(ast.IsOk(), ast.Issues.ToString());
    NYql::TExprNode::TPtr node;
    UNIT_ASSERT_C(NYql::CompileExpr(*ast.Root, node, ctx, nullptr, nullptr), ctx.IssueManager.GetIssues().ToString());
    return node;
}

std::optional<TVector<TString>> Describe(TStringBuf expression, const TVector<TString>& keys, size_t maxRanges = MaxExplainedReadRanges) {
    NYql::TExprContext ctx;
    auto ranges = Compile(expression, ctx);
    // The extractor finalizes its internal range representation before handing it to the read source.
    if (!ranges->IsCallable("RangeFinalize")) {
        ranges = ctx.NewCallable(ranges->Pos(), "RangeFinalize", {ranges});
    }
    auto description = DescribeReadRanges(nullptr, ranges, keys, MaxEvaluatedRangeInputs);
    if (description.Ranges && description.Ranges->size() > maxRanges) {
        return std::nullopt;
    }
    return description.Ranges;
}

TString DescribeLiteral(TStringBuf expression, const TVector<TString>& keys) {
    NYql::TExprContext ctx;
    const auto description = DescribeReadRanges(Compile(expression, ctx), nullptr, keys);
    UNIT_ASSERT(description.Ranges);
    UNIT_ASSERT_VALUES_EQUAL(description.Ranges->size(), 1);
    return description.Ranges->front();
}

void AssertExpression(TStringBuf expression, const TVector<TString>& keys, TStringBuf expected) {
    NYql::TExprContext ctx;
    const auto actual = DescribeReadRanges(nullptr, Compile(expression, ctx), keys).Expression;
    UNIT_ASSERT(actual);
    UNIT_ASSERT_VALUES_EQUAL(*actual, expected);
}

// IN over a list as the predicate extractor builds it, with the list and the limit of RangeMultiply.
TString SqlIn(TStringBuf list, ui64 limit) {
    return TStringBuilder() << R"(
        (IfPresent )" << list << R"(
            (lambda '(collection) (block '(
                (let points (Collect (Take (FlatMap collection (lambda '(item)
                    (RangeMultiply (Uint64 ')" << limit << R"() (RangeFor '== item (DataType 'Int64))))) (Uint64 ')" << limit + 1 << R"())))
                (let inf '((Nothing (OptionalType (DataType 'Int64))) (Int32 '0)))
                (return (IfStrict (> (Length points) (Uint64 ')" << limit << R"()) (AsRange '(inf inf)) (RangeUnion points))))))
            (RangeEmpty (DataType 'Int64)))
    )";
}

// IN over (b, a) tuples on the key (a, b), as the predicate extractor builds it: the tuples are put in key order first.
TString TupleSqlIn(TStringBuf list, ui64 limit) {
    return TStringBuilder() << R"(
        (IfPresent )" << list << R"(
            (lambda '(collection) (block '(
                (let ordered (Map
                    (FlatMap collection (lambda '(x) (StrictCast x (OptionalType (TupleType (DataType 'Int32) (DataType 'Int32))))))
                    (lambda '(t) '((Nth t '1) (Nth t '0)))))
                (let points (Collect (Take (FlatMap ordered (lambda '(item) (RangeMultiply (Uint64 ')" << limit << R"()
                    (RangeFor '== (Nth item '0) (DataType 'Int64))
                    (RangeFor '=== (Nth item '1) (DataType 'Int64))))) (Uint64 ')" << limit + 1 << R"())))
                (let inf '((Nothing (OptionalType (DataType 'Int64))) (Nothing (OptionalType (DataType 'Int64))) (Int32 '0)))
                (return (IfStrict (> (Length points) (Uint64 ')" << limit << R"()) (AsRange '(inf inf)) (RangeUnion points))))))
            (RangeEmpty (TupleType (DataType 'Int64) (DataType 'Int64))))
    )";
}

constexpr TStringBuf PairList = "(Map (Just '('((Int32 '10) (Int32 '1)) '((Int32 '20) (Int32 '2)))) (lambda '(t) (AsList (Nth t '0) (Nth t '1))))";

constexpr TStringBuf TupleList = "(Map (Just '((Int32 '1) (Int32 '2))) (lambda '(t) (AsList (Nth t '0) (Nth t '1))))";

TString PointsUnion(size_t count) {
    TStringBuilder result;
    result << "(RangeUnion";
    for (size_t i = 0; i < count; ++i) {
        result << " (RangeFor '== (Int64 '" << i << ") (DataType 'Int64))";
    }
    return result << ')';
}

void AssertRanges(TStringBuf expression, const TVector<TString>& keys, const TVector<TString>& expected) {
    const auto actual = Describe(expression, keys);
    UNIT_ASSERT(actual);
    UNIT_ASSERT_VALUES_EQUAL(*actual, expected);
}

} // namespace

Y_UNIT_TEST_SUITE(KqpReadRangeDescriptions) {
    Y_UNIT_TEST(CompositeTupleWithExcludedUpperPrefix) {
        AssertRanges(R"(
            (AsRange '(
                '((Just (Timestamp '1776309661000000)) (Just (String 'a1)) (Just (String 'b1)) (Int32 '0))
                '((Just (Timestamp '1776310561000000))
                  (Nothing (OptionalType (DataType 'String)))
                  (Nothing (OptionalType (DataType 'String))) (Int32 '0))
            ))
        )", {"timestamp", "resource_type", "resource_id"}, {
            "({timestamp: 2026-04-16T03:21:01.000000Z, resource_type: \"a1\", resource_id: \"b1\"}, "
            "{timestamp: 2026-04-16T03:36:01.000000Z, resource_type: -∞, resource_id: -∞})"
        });
    }

    Y_UNIT_TEST(PrefixBoundsPreserveInclusionAndKeyOrder) {
        AssertRanges(R"(
            (RangeIntersect
                (RangeFor '> (Uint64 '5) (DataType 'Uint64))
                (RangeFor '< (Uint64 '10) (DataType 'Uint64)))
        )", {"z", "a"}, {"({z: 5, a: +∞}, {z: 10, a: -∞})"});
        AssertRanges(R"(
            (RangeIntersect
                (RangeFor '>= (Uint64 '5) (DataType 'Uint64))
                (RangeFor '<= (Uint64 '10) (DataType 'Uint64)))
        )", {"z", "a"}, {"({z: 5, a: -∞}, {z: 10, a: +∞})"});
    }

    Y_UNIT_TEST(DisjointCompositeRangesStayGrouped) {
        AssertRanges(R"(
            (RangeFinalize (RangeMultiply (Uint64 '100)
                (RangeUnion
                    (RangeFor '== (Uint64 '1) (DataType 'Uint64))
                    (RangeFor '== (Uint64 '3) (DataType 'Uint64)))
                (RangeIntersect
                    (RangeFor '>= (Uint64 '10) (DataType 'Uint64))
                    (RangeFor '< (Uint64 '20) (DataType 'Uint64)))))
        )", {"a", "b"}, {
            "[{a: 1, b: 10}, {a: 1, b: 20})",
            "[{a: 3, b: 10}, {a: 3, b: 20})"
        });
    }

    Y_UNIT_TEST(NonPointPrefixDoesNotConstrainSuffix) {
        AssertRanges(R"(
            (RangeMultiply (Uint64 '100)
                (RangeFor '> (Uint64 '1) (DataType 'Uint64))
                (RangeFor '== (Uint64 '10) (DataType 'Uint64)))
        )", {"a", "b"}, {"({a: 1, b: +∞}, {a: +∞, b: +∞})"});
    }

    Y_UNIT_TEST(PointPrefixWithUnboundedSuffixKeepsPrefixInclusion) {
        AssertRanges(R"(
            (RangeMultiply (Uint64 '100)
                (RangeFor '== (Uint64 '1) (DataType 'Uint64))
                (RangeFor '> (Uint64 '10) (DataType 'Uint64)))
        )", {"a", "b"}, {"({a: 1, b: 10}, {a: 1, b: +∞})"});
    }

    Y_UNIT_TEST(IntersectionChoosesWholeTupleBounds) {
        AssertRanges(R"(
            (RangeIntersect
                (AsRange '(
                    '((Just (Uint64 '1)) (Nothing (OptionalType (DataType 'Uint64))) (Int32 '0))
                    '((Just (Uint64 '3)) (Nothing (OptionalType (DataType 'Uint64))) (Int32 '1))))
                (AsRange '(
                    '((Just (Uint64 '1)) (Just (Uint64 '10)) (Int32 '1))
                    '((Just (Uint64 '3)) (Just (Uint64 '20)) (Int32 '0)))))
        )", {"a", "b"}, {"({a: 1, b: +∞}, {a: 3, b: 20})"});
    }

    Y_UNIT_TEST(NullAndStringValuesAreNotInfinities) {
        AssertRanges(R"(
            (AsRange '(
                '((Just (Nothing (OptionalType (DataType 'String)))) (Int32 '1))
                '((Just (Just (String 'a1))) (Int32 '0))))
        )", {"key"}, {"[{key: NULL}, {key: \"a1\"})"});
        AssertRanges(R"(
            (RangeFor '== (String '"a,\"b") (DataType 'String))
        )", {"key"}, {"[{key: \"a,\\\"b\"}, {key: \"a,\\\"b\"}]"});
    }

    Y_UNIT_TEST(UnknownAndEmptyRangesAreDifferent) {
        UNIT_ASSERT(!Describe(R"(
            (RangeFor '> (Parameter 'p (DataType 'Uint64)) (DataType 'Uint64))
        )", {"a"}));
        AssertRanges("(RangeEmpty (DataType 'Uint64))", {"a"}, {});
        AssertRanges(R"(
            (RangeIntersect
                (RangeFor '> (Uint64 '5) (DataType 'Uint64))
                (RangeFor '<= (Uint64 '5) (DataType 'Uint64)))
        )", {"a"}, {});
    }

    Y_UNIT_TEST(RangeLimitUsesStructuralDescription) {
        const TString first = R"((RangeMultiply (Uint64 '1)
            (RangeUnion (RangeFor '== (Int64 '1) (DataType 'Int64))
                        (RangeFor '== (Int64 '3) (DataType 'Int64)))))";
        UNIT_ASSERT(!Describe(first, {"a"}));
        AssertExpression(first, {"a"}, "range_product(a ∈ {1, 3}, limit=1)");
        const TString product = R"((RangeMultiply (Uint64 '1)
            (RangeUnion (RangeFor '== (Int64 '1) (DataType 'Int64))
                        (RangeFor '== (Int64 '3) (DataType 'Int64)))
            (RangeFor '== (Int64 '10) (DataType 'Int64))))";
        UNIT_ASSERT(!Describe(product, {"a", "b"}));
        AssertExpression(product, {"a", "b"}, "range_product(a ∈ {1, 3}, b ∈ {10}, limit=1)");
    }

    Y_UNIT_TEST(UnionMergesOverlappingRanges) {
        AssertRanges(R"(
            (RangeUnion
                (RangeFor '!= (Int64 '5) (DataType 'Int64))
                (RangeFor '== (Int64 '5) (DataType 'Int64)))
        )", {"a"}, {"({a: -∞}, {a: +∞})"});
        AssertRanges(R"(
            (RangeUnion
                (RangeFor '>= (Int64 '-3) (DataType 'Int64))
                (RangeFor '< (Int64 '-10) (DataType 'Int64))
                (RangeFor '== (Int64 '-20) (DataType 'Int64)))
        )", {"a"}, {"({a: -∞}, {a: -10})", "[{a: -3}, {a: +∞})"});
    }

    Y_UNIT_TEST(MergedUnionIsNotPointPrefix) {
        AssertRanges(R"(
            (RangeMultiply (Uint64 '100)
                (RangeUnion
                    (RangeFor '== (Uint64 '1) (DataType 'Uint64))
                    (RangeFor '>= (Uint64 '1) (DataType 'Uint64)))
                (RangeFor '== (Uint64 '10) (DataType 'Uint64)))
        )", {"a", "b"}, {"({a: 1, b: -∞}, {a: +∞, b: +∞})"});
    }

    Y_UNIT_TEST(WidePointBecomesPointOnFinalize) {
        AssertRanges(R"(
            (RangeFinalize (RangeMultiply (Uint64 '100)
                (RangeFor '== (Int32 '1) (DataType 'Int64))
                (RangeFor '=== (Int32 '2) (DataType 'Int64))))
        )", {"a", "b"}, {"[{a: 1, b: 2}, {a: 1, b: 2}]"});
        AssertRanges(R"(
            (RangeMultiply (Uint64 '100)
                (RangeFor '=== (Int64 '1) (DataType 'Int64))
                (RangeFor '== (Int64 '2) (DataType 'Int64)))
        )", {"a", "b"}, {"({a: 1, b: -∞}, {a: 1, b: +∞})"});
    }

    Y_UNIT_TEST(NullableKeyRangesStartAfterNull) {
        AssertRanges(R"(
            (RangeFor '< (Int64 '5) (OptionalType (DataType 'Int64)))
        )", {"a"}, {"({a: NULL}, {a: 5})"});
        AssertRanges(R"(
            (RangeFor 'NotExists (Void) (OptionalType (DataType 'Int64)))
        )", {"a"}, {"[{a: NULL}, {a: NULL}]"});
        AssertRanges(R"(
            (RangeFor '== (Nothing (OptionalType (DataType 'Int64))) (OptionalType (DataType 'Int64)))
        )", {"a"}, {});
    }

    Y_UNIT_TEST(EqualInfinitePrefixesAreEqual) {
        AssertRanges(R"(
            (RangeIntersect
                (AsRange '('((Nothing (OptionalType (DataType 'Uint64))) (Nothing (OptionalType (DataType 'Uint64))) (Int32 '1))
                             '((Just (Uint64 '7)) (Nothing (OptionalType (DataType 'Uint64))) (Int32 '1))))
                (AsRange '('((Nothing (OptionalType (DataType 'Uint64))) (Nothing (OptionalType (DataType 'Uint64))) (Int32 '0))
                             '((Just (Uint64 '5)) (Nothing (OptionalType (DataType 'Uint64))) (Int32 '1)))))
        )", {"a", "b"}, {"({a: -∞, b: -∞}, {a: 5, b: +∞})"});
    }

    Y_UNIT_TEST(TextValuesStayReadable) {
        AssertRanges(R"(
            (RangeFor '== (Utf8 '"Привет\n") (DataType 'Utf8))
        )", {"name"}, {"[{name: \"Привет\\n\"}, {name: \"Привет\\n\"}]"});
    }

    Y_UNIT_TEST(PgOrderingIsLeftToTheStructuralDescription) {
        const TString text = "(RangeFor '== (PgConst 'NULL (PgType 'text)) (PgType 'text))";
        UNIT_ASSERT(!Describe(text, {"name"}));
        AssertExpression(text, {"name"}, "name ∈ {\"NULL\"}");
        const TString bounds = "(RangeIntersect (RangeFor '> (PgConst 'a (PgType 'text)) (PgType 'text)) "
                               "(RangeFor '< (PgConst 'b (PgType 'text)) (PgType 'text)))";
        UNIT_ASSERT(!Describe(bounds, {"name"}));
        AssertExpression(bounds, {"name"}, "name ∈ (\"a\", \"b\")");
    }

    Y_UNIT_TEST(LiteralRangePadsMissingColumns) {
        UNIT_ASSERT_VALUES_EQUAL(DescribeLiteral(R"(
            '( (KqlKeyInc (Int64 '1)) (KqlKeyInc (Int64 '1)))
        )", {"a", "b"}), "({a: 1, b: -∞}, {a: 1, b: +∞})");
        UNIT_ASSERT_VALUES_EQUAL(DescribeLiteral(R"(
            '( (KqlKeyExc (Int64 '5)) (KqlKeyInc))
        )", {"a"}), "({a: 5}, {a: +∞})");
        UNIT_ASSERT_VALUES_EQUAL(DescribeLiteral(R"(
            '( (KqlKeyExc) (KqlKeyExc))
        )", {"a"}), "({a: -∞}, {a: +∞})");
    }

    Y_UNIT_TEST(LiteralRangeShowsParametersAndQuotesStrings) {
        UNIT_ASSERT_VALUES_EQUAL(DescribeLiteral(R"(
            '(
                (KqlKeyInc (Int64 '1) (Int64 '10))
                (KqlKeyExc (Int64 '1) (Parameter '$hi (DataType 'Int64))))
        )", {"a", "b"}), "[{a: 1, b: 10}, {a: 1, b: $hi})");
        UNIT_ASSERT_VALUES_EQUAL(DescribeLiteral(R"(
            '( (KqlKeyInc (String '"x, b: 1")) (KqlKeyInc (String '"x, b: 1")))
        )", {"a", "b"}), "({a: \"x, b: 1\", b: -∞}, {a: \"x, b: 1\", b: +∞})");
        UNIT_ASSERT_VALUES_EQUAL(DescribeLiteral(R"(
            '( (KqlKeyInc (Nothing (OptionalType (DataType 'Int64)))) (KqlKeyInc (Nothing (OptionalType (DataType 'Int64)))))
        )", {"a"}), "[{a: NULL}, {a: NULL}]");
    }

    Y_UNIT_TEST(EvaluationStopsAtDisplayLimit) {
        UNIT_ASSERT(Describe(PointsUnion(MaxExplainedReadRanges), {"a"}));
        UNIT_ASSERT(!Describe(PointsUnion(MaxExplainedReadRanges + 1), {"a"}));
        UNIT_ASSERT(Describe(PointsUnion(3), {"a"}, 3));
        UNIT_ASSERT(!Describe(PointsUnion(3), {"a"}, 2));
    }

    Y_UNIT_TEST(EvaluationBoundsInputsBeforeComputing) {
        NYql::TExprContext ctx;
        const auto point = Compile("(RangeFor '== (Int64 '1) (DataType 'Int64))", ctx);
        UNIT_ASSERT(DescribeReadRanges(nullptr, point, {"a"}, MaxEvaluatedRangeInputs).Ranges);
        UNIT_ASSERT(!DescribeReadRanges(nullptr, point, {"a"}, MaxEvaluatedRangeInputs + 1).Ranges);
        UNIT_ASSERT(!DescribeReadRanges(nullptr, point, {"a"}, {}).Ranges);
        const auto manyNodes = Compile(PointsUnion(MaxRangeEvaluationNodes), ctx);
        UNIT_ASSERT(!DescribeReadRanges(nullptr, manyNodes, {"a"}, 1).Ranges);
        const auto largeValue = Compile(TStringBuilder() << "(RangeFor '== (String '\""
            << TString(MaxRangeEvaluationBytes + 1, 'x') << "\") (DataType 'String))", ctx);
        UNIT_ASSERT(!DescribeReadRanges(nullptr, largeValue, {"a"}, 1).Ranges);

        // Every shared value is also reachable by a short path; visiting it once must not hide the deep path.
        const auto one = Compile("(Int64 '1)", ctx);
        const auto condition = Compile("(Bool 'true)", ctx);
        NYql::TExprNode::TListType values{one};
        for (size_t i = 0; i < 70; ++i) {
            values.push_back(ctx.NewCallable(one->Pos(), "If", {condition, values.back(), one}));
        }
        Reverse(values.begin(), values.end());
        const auto list = ctx.NewCallable(one->Pos(), "AsList", std::move(values));
        auto first = ctx.NewCallable(one->Pos(), "Nth", {list, ctx.NewAtom(one->Pos(), "0")});
        const auto deepRange = ctx.ChangeChild(*point, 1, std::move(first));
        UNIT_ASSERT(!DescribeReadRanges(nullptr, deepRange, {"a"}, 1).Ranges);
    }

    Y_UNIT_TEST(EvaluationRejectsRuntimeAndUnboundedExpressions) {
        for (const auto value : {"(RandomNumber)", "(CurrentUtcTimestamp)",
                                 "(Parameter '$p (DataType 'Int64))", "(Apply (Udf 'Random.Next))"}) {
            UNIT_ASSERT(!Describe(TStringBuilder() << "(RangeFor '== " << value << " (DataType 'Int64))", {"a"}));
        }
        UNIT_ASSERT(!Describe(SqlIn("(Just (ListFromRange (Int64 '0) (Int64 '1000000000)))", 10000), {"a"}));
    }

    Y_UNIT_TEST(EvaluationOnlyInterpretsSelectedBranch) {
        const TStringBuf point = "(RangeFor '== (Int64 '1) (DataType 'Int64))";
        const TStringBuf unknown = "(Apply (Udf 'Range.Build) (Parameter '$p (DataType 'Int64)))";
        for (const auto op : {"If", "IfStrict"}) {
            AssertRanges(TStringBuilder() << '(' << op << " (Bool 'true) " << point << ' ' << unknown << ')',
                {"a"}, {"[{a: 1}, {a: 1}]"});
            AssertRanges(TStringBuilder() << '(' << op << " (Bool 'false) " << unknown << ' ' << point << ')',
                {"a"}, {"[{a: 1}, {a: 1}]"});
            const TString unsupported = TStringBuilder() << '(' << op << " (Bool 'true) " << unknown << ' ' << point << ')';
            UNIT_ASSERT(!Describe(unsupported, {"a"}));
            AssertExpression(unsupported, {"a"}, "f1($p)");
        }
    }

    Y_UNIT_TEST(EvaluationFailureDoesNotChangeQueryContext) {
        NYql::TExprContext ctx;
        const auto point = Compile("(RangeFor '== (Int64 '1) (DataType 'Int64))", ctx);
        const auto failing = ctx.NewCallable(point->Pos(), "Ensure", {
            point, ctx.NewCallable(point->Pos(), "Bool", {ctx.NewAtom(point->Pos(), "false")}),
            ctx.NewCallable(point->Pos(), "String", {ctx.NewAtom(point->Pos(), "range evaluation failure")})});
        ctx.AddError(NYql::TIssue({}, "existing query issue"));
        const auto issues = ctx.IssueManager.GetIssues().ToString();
        ctx.Step.Done(NYql::TExprStep::ExprEval);
        UNIT_ASSERT(!DescribeReadRanges(nullptr, failing, {"a"}, 1).Ranges);
        UNIT_ASSERT_VALUES_EQUAL(ctx.IssueManager.GetIssues().ToString(), issues);
        UNIT_ASSERT(ctx.Step.IsDone(NYql::TExprStep::ExprEval));
        UNIT_ASSERT(!point->GetTypeAnn());
        const auto invalid = Compile("(RangeFor 'invalid (Int64 '1) (DataType 'Int64))", ctx);
        UNIT_ASSERT(!DescribeReadRanges(nullptr, invalid, {"a"}, 1).Ranges);
        UNIT_ASSERT_VALUES_EQUAL(ctx.IssueManager.GetIssues().ToString(), issues);
        UNIT_ASSERT(ctx.Step.IsDone(NYql::TExprStep::ExprEval));
        UNIT_ASSERT(DescribeReadRanges(nullptr, point, {"a"}, 1).Ranges);
        UNIT_ASSERT_VALUES_EQUAL(ctx.IssueManager.GetIssues().ToString(), issues);
        UNIT_ASSERT(ctx.Step.IsDone(NYql::TExprStep::ExprEval));
        UNIT_ASSERT(!point->GetTypeAnn());
        const auto fallback = DescribeReadRanges(nullptr, point, {"a"});
        UNIT_ASSERT(!fallback.Ranges);
        UNIT_ASSERT_VALUES_EQUAL(*fallback.Expression, "a ∈ {1}");
    }

    Y_UNIT_TEST(ExpressionShowsUnknownValues) {
        const TStringBuf expression = R"(
            (RangeFinalize (RangeMultiply (Uint64 '10000)
                (RangeUnion
                    (RangeFor '== (Int64 '1) (DataType 'Int64))
                    (RangeFor '== (Int64 '2) (DataType 'Int64)))
                (RangeIntersect
                    (RangeFor '>= (Int64 '10) (DataType 'Int64))
                    (RangeFor '< (Parameter '$hi (DataType 'Int64)) (DataType 'Int64)))))
        )";
        UNIT_ASSERT(!Describe(expression, {"a", "b"}));
        AssertExpression(expression, {"a", "b"}, "a ∈ {1, 2} × b ∈ [10, $hi)");
    }

    Y_UNIT_TEST(ExpressionGroupsOperands) {
        AssertExpression(R"(
            (RangeMultiply (Uint64 '100)
                (RangeUnion
                    (RangeFor '< (Parameter '$p (DataType 'Int64)) (DataType 'Int64))
                    (RangeFor '>= (Int64 '10) (DataType 'Int64)))
                (RangeFor '== (Int64 '1) (DataType 'Int64)))
        )", {"a", "b"}, "range_product(a ∈ (-∞, $p) ∪ [10, +∞), b ∈ {1}, limit=100)");
        AssertExpression(R"(
            (RangeUnion
                (RangeMultiply (Uint64 '100)
                    (RangeFor '== (Parameter '$x (DataType 'Int64)) (DataType 'Int64))
                    (RangeFor '> (Int64 '5) (DataType 'Int64)))
                (RangeMultiply (Uint64 '100)
                    (RangeFor '== (Int64 '2) (DataType 'Int64))
                    (RangeFor '< (Int64 '3) (DataType 'Int64))))
        )", {"a", "b"}, "(a ∈ {$x} × b ∈ (5, +∞)) ∪ (a ∈ {2} × b ∈ (-∞, 3))");
        AssertExpression(R"(
            (RangeFor '!= (Parameter '$p (DataType 'Int64)) (DataType 'Int64))
        )", {"a"}, "a ∈ (-∞, $p) ∪ ($p, +∞)");
    }

    Y_UNIT_TEST(ExpressionShortensLargeSets) {
        AssertExpression(PointsUnion(12), {"a"}, "a ∈ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, … 2 more}");
    }

    Y_UNIT_TEST(SqlInIsEvaluated) {
        const TString expression = TStringBuilder() << "(RangeFinalize (RangeMultiply (Uint64 '10000) "
            << SqlIn(TupleList, 10000) << " (RangeFor '=== (Int32 '7) (DataType 'Int64))))";
        AssertRanges(expression, {"a", "b"}, {"[{a: 1, b: 7}, {a: 1, b: 7}]", "[{a: 2, b: 7}, {a: 2, b: 7}]"});
        AssertExpression(expression, {"a", "b"}, "a ∈ {1, 2} × b ∈ {7}");
        AssertRanges(SqlIn("(Just (AsList (Int64 '3) (Int64 '3)))", 10000), {"a"}, {"[{a: 3}, {a: 3}]"});
        AssertExpression(SqlIn("(Just (AsList (Int64 '3) (Int64 '3)))", 10000), {"a"}, "a ∈ {3}");
    }

    Y_UNIT_TEST(SqlInLimitsPreserveDuplicateInputs) {
        const TStringBuf duplicates = "(Just (AsList (Int64 '3) (Int64 '3)))";
        AssertExpression(SqlIn(duplicates, 1), {"a"}, "in_ranges(a, [3, 3], limit=1)");
        AssertExpression(SqlIn(duplicates, 2), {"a"}, "a ∈ {3}");
        AssertExpression(TupleSqlIn(PairList, 1), {"a", "b"}, "in_ranges((a, b), [(1, 10), (2, 20)], limit=1)");
    }

    Y_UNIT_TEST(SqlInMembershipKeepsTypeAndNullSemantics) {
        AssertExpression(SqlIn("(Just (AsList (Nothing (OptionalType (DataType 'Int64))) (Just (Int64 '1))))", 10000),
            {"a"}, "in_ranges(a, [NULL, 1], limit=10000)");
        auto outside = SqlIn("(Just (AsList (Int64 '300)))", 10000);
        SubstGlobal(outside, "(DataType 'Int64)", "(DataType 'Int8)");
        AssertExpression(outside, {"a"}, "in_ranges(a, [300], limit=10000)");
    }

    Y_UNIT_TEST(ExpansionLimitsStayOnTheirOperations) {
        const auto first = SqlIn(TupleList, 1);
        const auto second = SqlIn("(Just (Parameter '$list (ListType (DataType 'Int64))))", 2);
        const TString ranges = TStringBuilder() << "(RangeUnion " << first << ' ' << second << ')';
        AssertExpression(ranges, {"a"}, "in_ranges(a, [1, 2], limit=1) ∪ in_ranges(a, $list, limit=2)");
        AssertExpression(TStringBuilder() << "(RangeMultiply (Uint64 '2) " << ranges << ')', {"a"},
            "range_product(in_ranges(a, [1, 2], limit=1) ∪ in_ranges(a, $list, limit=2), limit=2)");
        AssertExpression(TStringBuilder() << "(RangeMultiply (Uint64 '2) " << second << ')', {"a"},
            "in_ranges(a, $list, limit=2)");
    }

    Y_UNIT_TEST(RangeProductPreservesPrefixBehavior) {
        AssertExpression(R"((RangeMultiply (Uint64 '100)
            (RangeFor '> (Parameter '$p (DataType 'Int64)) (DataType 'Int64))
            (RangeFor '== (Int64 '7) (DataType 'Int64))))", {"a", "b"},
            "range_product(a ∈ ($p, +∞), b ∈ {7}, limit=100)");
        // An overflowing prefix stops multiplication before the empty suffix is reached.
        AssertExpression(R"((RangeMultiply (Uint64 '1)
            (RangeUnion (RangeFor '== (Int64 '1) (DataType 'Int64)) (RangeFor '== (Int64 '3) (DataType 'Int64)))
            (RangeFor '== (Int64 '10) (DataType 'Int64))
            (RangeEmpty (DataType 'Int64))))", {"a", "b", "c"},
            "range_product(a ∈ {1, 3}, b ∈ {10}, ∅, limit=1)");
        const auto in = SqlIn("(Just (Parameter '$list (ListType (DataType 'Int64))))", 10000);
        AssertExpression(TStringBuilder() << "(RangeMultiply (Uint64 '10000) " << in
            << " (RangeFor '== (Int64 '7) (DataType 'Int64)))", {"a", "b"},
            "range_product(in_ranges(a, $list, limit=10000), b ∈ {7}, limit=10000)");
    }

    Y_UNIT_TEST(ConditionalRangeOperationsStayInTheirBranches) {
        AssertExpression(R"((If (Parameter '$flag (DataType 'Bool))
            (RangeFor '== (Int64 '1) (DataType 'Int64)) (RangeEmpty)))", {"a"},
            "if($flag, a ∈ {1}, ∅)");
        const auto limited = SqlIn(TupleList, 1);
        for (const TStringBuf callable : {"If", "IfStrict"}) {
            AssertExpression(TStringBuilder() << '(' << callable << " (Parameter '$flag (DataType 'Bool)) " << limited
                << " (RangeFor '== (Int64 '9) (DataType 'Int64)))", {"a"},
                "if($flag, in_ranges(a, [1, 2], limit=1), a ∈ {9})");
        }
        AssertExpression(R"((If (Parameter '$flag (DataType 'Bool))
            (Unknown (Parameter '$p (DataType 'Int64))) (RangeFor '== (Int64 '9) (DataType 'Int64))))", {"a"},
            "if($flag, f1($p), a ∈ {9})");
    }

    Y_UNIT_TEST(SqlInOverLimitIsSymbolic) {
        UNIT_ASSERT(!Describe(SqlIn(TupleList, 1), {"a"}));
        AssertExpression(SqlIn(TupleList, 1), {"a"}, "in_ranges(a, [1, 2], limit=1)");
    }

    Y_UNIT_TEST(SqlInInputCountDoesNotProveRangeLimitOverflow) {
        const auto withNull = SqlIn(R"(
            (Just (AsList (Nothing (OptionalType (DataType 'Int64))) (Just (Int64 '1))))
        )", 1);
        // Even if NULL filtering could bring the count below the limit, do not guess a coarser scan.
        UNIT_ASSERT(!Describe(withNull, {"a"}));
        AssertExpression(withNull, {"a"}, "in_ranges(a, [NULL, 1], limit=1)");

        const auto withParameter = SqlIn(R"(
            (Just (AsList (Nothing (OptionalType (DataType 'Int64)))
                (Just (Parameter '$p (DataType 'Int64)))))
        )", 1);
        UNIT_ASSERT(!Describe(withParameter, {"a"}));
        AssertExpression(withParameter, {"a"}, "in_ranges(a, [NULL, $p], limit=1)");
    }

    Y_UNIT_TEST(ExpressionUnionPreservesPaddedKeyWidth) {
        AssertExpression(R"(
            (RangeMultiply (Uint64 '100)
                (RangeUnion
                    (RangeMultiply (Uint64 '100)
                        (RangeFor '== (Int64 '1) (DataType 'Int64))
                        (AsRange '('((Nothing (OptionalType (DataType 'Int64))) (Int32 '0))
                                  '((Nothing (OptionalType (DataType 'Int64))) (Int32 '0)))))
                    (RangeMultiply (Uint64 '100)
                        (RangeFor '== (Int64 '2) (DataType 'Int64))
                        (AsRange '('((Nothing (OptionalType (DataType 'Int64))) (Int32 '0))
                                  '((Nothing (OptionalType (DataType 'Int64))) (Int32 '0))))))
                (RangeFor '== (Parameter '$c (DataType 'Int64)) (DataType 'Int64)))
        )", {"a", "b", "c"}, "range_product((a ∈ {1} × b ∈ (-∞, +∞)) ∪ (a ∈ {2} × b ∈ (-∞, +∞)), c ∈ {$c}, limit=100)");
    }

    Y_UNIT_TEST(ExpressionLeavesOutUnconstrainedColumns) {
        const TString expression = TStringBuilder() << "(RangeMultiply (Uint64 '10000) "
            << SqlIn("(Just (Parameter '$list (ListType (DataType 'Int64))))", 10000)
            << " (AsRange '('((Nothing (OptionalType (DataType 'Int64))) (Int32 '0)) '((Nothing (OptionalType (DataType 'Int64))) (Int32 '0)))))";
        AssertExpression(expression, {"a", "b"}, "in_ranges(a, $list, limit=10000)");
    }

    Y_UNIT_TEST(SqlInOverParameterIsDescribed) {
        const auto expression = SqlIn("(Just (Parameter '$list (ListType (DataType 'Int64))))", 10000);
        UNIT_ASSERT(!Describe(expression, {"a"}));
        AssertExpression(expression, {"a"}, "in_ranges(a, $list, limit=10000)");
    }

    Y_UNIT_TEST(SqlInDoesNotDiscardCollectionOperations) {
        AssertExpression(SqlIn(R"(
            (Map (Just (Parameter '$list (ListType (DataType 'Int64)))) (lambda '(x) x))
        )", 10000), {"a"}, "in_ranges(a, $list, limit=10000)");
        AssertExpression(SqlIn(R"(
            (Just (Take (Parameter '$list (ListType (DataType 'Int64))) (Uint64 '1)))
        )", 10000), {"a"}, "in_ranges(a, f1($list), limit=10000)");
        AssertExpression(SqlIn(R"(
            (Just (Map (Parameter '$list (ListType (DataType 'Int64))) (lambda '(x) (+ x (Int64 '1)))))
        )", 10000), {"a"}, "in_ranges(a, f1($list), limit=10000)");
    }

    Y_UNIT_TEST(TupleSqlInIsEvaluatedInKeyOrder) {
        const TString expression = TStringBuilder() << "(RangeFinalize " << TupleSqlIn(PairList, 10000) << ")";
        AssertRanges(expression, {"a", "b"}, {"[{a: 1, b: 10}, {a: 1, b: 10}]", "[{a: 2, b: 20}, {a: 2, b: 20}]"});
        AssertExpression(expression, {"a", "b"}, "(a, b) ∈ {(1, 10), (2, 20)}");
    }

    Y_UNIT_TEST(TupleSqlInOverParameterOrLimit) {
        const auto parameter = TupleSqlIn("(Map (Just (Parameter '$pairs (ListType (TupleType (DataType 'Int32) (DataType 'Int32))))) (lambda '(x) x))", 10000);
        UNIT_ASSERT(!Describe(parameter, {"a", "b"}));
        AssertExpression(parameter, {"a", "b"}, "in_ranges((b, a), $pairs, limit=10000)");
        UNIT_ASSERT(!Describe(TupleSqlIn(PairList, 1), {"a", "b"}));
    }

    Y_UNIT_TEST(TupleSqlInProjectionIsNotPlainMembership) {
        const auto parameter = TupleSqlIn("(Just (Parameter '$pairs (ListType (TupleType (DataType 'Int32) (DataType 'Int32)))))", 10000);
        auto projected = parameter;
        SubstGlobal(projected, "(TupleType (DataType 'Int32) (DataType 'Int32))",
            "(TupleType (DataType 'Int32) (DataType 'Int32) (DataType 'Int32))");
        auto duplicated = parameter;
        SubstGlobal(duplicated, "(Nth t '1)", "(Nth t '0)");
        AssertExpression(projected, {"a", "b"}, "in_ranges((a, b), project($pairs, 1, 0), limit=10000)");
        AssertExpression(duplicated, {"a", "b"}, "in_ranges((a, b), project($pairs, 0, 0), limit=10000)");
    }

    Y_UNIT_TEST(OpaqueExpressionsKeepDependenciesAndKnownStructure) {
        AssertExpression(R"((RangeFor '> (+ (Parameter '$p (DataType 'Int64)) (Int64 '1)) (DataType 'Int64)))",
            {"a"}, "a ∈ (f1($p), +∞)");
        AssertExpression(R"((RangeUnion (RangeFor '== (Int64 '1) (DataType 'Int64))
            (UnknownRange (Parameter '$p (DataType 'Int64)))))", {"a"}, "a ∈ {1} ∪ f1($p)");
        AssertExpression(R"((RangeMultiply (Uint64 '100) (UnknownRange (Parameter '$p (DataType 'Int64)))
            (RangeFor '== (Int64 '7) (DataType 'Int64))))", {"a", "b"}, "f2($p)");
        AssertExpression(R"((If (Parameter '$flag (DataType 'Bool))
            (RangeFor '== (Int64 '1) (DataType 'Int64))
            (RangeFor '== (Int64 '3) (DataType 'Int64))))", {"a"}, "if($flag, a ∈ {1}, a ∈ {3})");
    }

    Y_UNIT_TEST(UnknownNamesAreSharedWithinOneExplain) {
        NYql::TExprContext firstCtx, secondCtx;
        const auto first = Compile("(Unknown (Parameter '$b (DataType 'Int64)) (Parameter '$a (DataType 'Int64)))", firstCtx);
        const auto second = Compile("(Unknown (Parameter '$a (DataType 'Int64)))", secondCtx);
        TExplainContext explain;
        UNIT_ASSERT_VALUES_EQUAL(explain.UnknownExpressionName(first), "f1");
        UNIT_ASSERT_VALUES_EQUAL(explain.DescribeUnknownExpression(first), "f1($a, $b)");
        UNIT_ASSERT_VALUES_EQUAL(explain.DescribeUnknownExpression(second), "f2($a)");
        UNIT_ASSERT_VALUES_EQUAL(*DescribeReadRanges(nullptr, first, {"a"}, {}, &explain).Expression, "f1($a, $b)");
        UNIT_ASSERT_VALUES_EQUAL(*DescribeReadRanges(nullptr, second, {"b"}, {}, &explain).Expression, "f2($a)");
        UNIT_ASSERT_VALUES_EQUAL(*DescribeReadRanges(nullptr, second, {"b"}).Expression, "f1($a)");
    }

    Y_UNIT_TEST(UnknownDependencyCollectionIsBoundedAndRepeatable) {
        NYql::TExprContext ctx;
        auto node = Compile("(Unknown (Parameter '$p (DataType 'Int64)))", ctx);
        for (size_t i = 0; i < 20; ++i) node = ctx.NewCallable(node->Pos(), "Unknown", {node, node});
        TExplainContext explain;
        UNIT_ASSERT_VALUES_EQUAL(explain.DescribeUnknownExpression(node), "f1($p)");
        NYql::TExprNode::TListType inputs;
        for (size_t i = 0; i < 20; ++i) {
            inputs.push_back(Compile(TStringBuilder() << "(Parameter '$p" << i << " (DataType 'Int64))", ctx));
        }
        const auto wide = ctx.NewCallable(node->Pos(), "Unknown", std::move(inputs));
        const auto text = explain.DescribeUnknownExpression(wide);
        UNIT_ASSERT_VALUES_EQUAL(text, "f2($p0, $p1, $p2, $p3, $p4, $p5, $p6, $p7, …)");
        UNIT_ASSERT_VALUES_EQUAL(explain.DescribeUnknownExpression(wide), text);
        auto deep = Compile("(Unknown)", ctx);
        for (size_t i = 0; i < 4200; ++i) deep = ctx.NewCallable(deep->Pos(), "Unknown", {deep});
        UNIT_ASSERT_VALUES_EQUAL(*DescribeReadRanges(nullptr, deep, {"a"}, {}, &explain).Expression, "f3(…)");
    }

    Y_UNIT_TEST(FloatRangesRemainSymbolic) {
        for (const TStringBuf op : {">", "==", "!=", ">="}) {
            UNIT_ASSERT(!Describe(TStringBuilder() << "(RangeFor '" << op << " (Double '5) (DataType 'Double))", {"x"}));
        }
        UNIT_ASSERT(!Describe("(RangeFor '== (Double 'nan) (DataType 'Double))", {"x"}));
        AssertExpression("(RangeFor '> (Double '5) (DataType 'Double))", {"x"}, "x ∈ (5, +∞)");
    }

    Y_UNIT_TEST(DecimalAndDateValues) {
        UNIT_ASSERT(!Describe("(RangeFor '< (Decimal '\"1.5\" '10 '2) (DataType 'Decimal '10 '2))", {"d"}));
        AssertRanges("(RangeFor '>= (Date '19723) (DataType 'Date))", {"d"}, {"[{d: 2024-01-01}, {d: +∞})"});
    }

    Y_UNIT_TEST(ValuesOutsideTheKeyTypeAreRounded) {
        // 300 does not fit Int8: == finds nothing, != everything, and < or > take the nearest Int8 bound.
        AssertRanges("(RangeFor '< (Int64 '300) (DataType 'Int8))", {"a"}, {"({a: -∞}, {a: 127}]"});
        AssertRanges("(RangeFor '> (Int64 '300) (DataType 'Int8))", {"a"}, {});
        AssertRanges("(RangeFor '> (Int64 '-300) (DataType 'Int8))", {"a"}, {"[{a: -128}, {a: +∞})"});
        AssertRanges("(RangeFor '== (Int64 '300) (DataType 'Int8))", {"a"}, {});
        AssertRanges("(RangeFor '!= (Int64 '300) (DataType 'Int8))", {"a"}, {"({a: -∞}, {a: +∞})"});
        // Float-to-integer conversions are deliberately not interpreted.
        UNIT_ASSERT(!Describe("(RangeFor '== (Double '1.5) (DataType 'Int64))", {"a"}));
        UNIT_ASSERT(!Describe("(RangeFor '== (Double '2) (DataType 'Int64))", {"a"}));
    }

    Y_UNIT_TEST(StartsWithIsAPrefixRange) {
        AssertRanges("(RangeFor 'StartsWith (String 'ab) (DataType 'String))", {"s"}, {"[{s: \"ab\"}, {s: \"ac\"})"});
        AssertRanges("(RangeFor 'NotStartsWith (String 'ab) (DataType 'String))", {"s"},
                     {"({s: -∞}, {s: \"ab\"})", "[{s: \"ac\"}, {s: +∞})"});
        AssertRanges("(RangeFor 'StartsWith (Utf8 'ab) (DataType 'Utf8))", {"s"}, {"[{s: \"ab\"}, {s: \"ac\"})"});
        AssertExpression("(RangeFor 'StartsWith (Parameter '$p (DataType 'String)) (DataType 'String))", {"s"}, "s ∈ StartsWith($p)");
    }

    Y_UNIT_TEST(ConstantConditions) {
        const TStringBuf ranges = "(RangeFor '== (Int64 '1) (DataType 'Int64)) (RangeEmpty (DataType 'Int64))";
        AssertRanges(TStringBuilder() << "(If (Bool 'true) " << ranges << ")", {"a"}, {"[{a: 1}, {a: 1}]"});
        AssertRanges(TStringBuilder() << "(If (Bool 'false) " << ranges << ")", {"a"}, {});
        const TString parameter = TStringBuilder() << "(If (Parameter '$flag (DataType 'Bool)) " << ranges << ")";
        UNIT_ASSERT(!Describe(parameter, {"a"}));
        AssertExpression(parameter, {"a"}, "if($flag, a ∈ {1}, ∅)");
        const TString full = "(If (Parameter '$flag (DataType 'Bool)) (AsRange '('((Nothing (OptionalType (DataType 'Int64))) (Int32 '0)) "
                             "'((Nothing (OptionalType (DataType 'Int64))) (Int32 '0)))) (RangeEmpty (DataType 'Int64)))";
        AssertExpression(full, {"a"}, "if($flag, a ∈ (-∞, +∞), ∅)");
    }

    Y_UNIT_TEST(WidePointsMergeAndFinalizeWithSuffixInfinities) {
        const TString points = "(RangeUnion (RangeFor '=== (Int32 '1) (DataType 'Int64)) "
                               "(RangeFor '=== (Int32 '2) (DataType 'Int64)))";
        AssertRanges(points, {"a"}, {"[{a: 1}, {a: 3})"});
        const TString suffix = "(AsRange '('((Nothing (OptionalType (DataType 'Int64))) (Int32 '0)) "
                               "'((Nothing (OptionalType (DataType 'Int64))) (Int32 '0))))";
        AssertRanges(TStringBuilder() << "(RangeMultiply (Uint64 '100) " << points << ' ' << suffix << ')',
                     {"a", "b"}, {"({a: 1, b: -∞}, {a: 3, b: -∞})"});
        AssertRanges(TStringBuilder() << "(RangeMultiply (Uint64 '100) (RangeFor '=== (Int32 '1) (DataType 'Int64)) " << suffix << ')',
                     {"a", "b"}, {"({a: 1, b: -∞}, {a: 1, b: +∞})"});
    }

    Y_UNIT_TEST(IntegerExtremesDoNotOverflow) {
        AssertRanges("(RangeFor '=== (Uint64 '18446744073709551615) (DataType 'Uint64))", {"a"},
                     {"[{a: 18446744073709551615}, {a: 18446744073709551615}]"});
        AssertRanges("(RangeFor '== (Int64 '-1) (DataType 'Uint64))", {"a"}, {});
        AssertRanges("(RangeFor '< (Uint64 '18446744073709551615) (DataType 'Int64))", {"a"},
                     {"({a: -∞}, {a: 9223372036854775807}]"});
        AssertRanges("(RangeFor '=== (Int64 '-9223372036854775808) (DataType 'Int64))", {"a"},
                     {"[{a: -9223372036854775808}, {a: -9223372036854775808}]"});
    }

    Y_UNIT_TEST(UnionKeepsAnExcludedPointGap) {
        AssertRanges("(RangeUnion (RangeFor '< (Int64 '5) (DataType 'Int64)) "
                     "(RangeFor '> (Int64 '5) (DataType 'Int64)))", {"a"},
                     {"({a: -∞}, {a: 5})", "({a: 5}, {a: +∞})"});
        AssertRanges("(RangeUnion (RangeFor '< (Int64 '5) (DataType 'Int64)) "
                     "(RangeFor '>= (Int64 '5) (DataType 'Int64)))", {"a"}, {"({a: -∞}, {a: +∞})"});
        AssertRanges("(RangeIntersect (RangeFor '>= (Int64 '5) (DataType 'Int64)) "
                     "(RangeFor '< (Int64 '5) (DataType 'Int64)))", {"a"}, {});
    }

    Y_UNIT_TEST(Utf8PrefixesUseTheExistingSuccessorFunction) {
        AssertRanges("(RangeFor 'StartsWith (String '\"при\") (DataType 'Utf8))", {"s"},
                     {"[{s: \"при\"}, {s: \"прй\"})"});
        AssertRanges("(RangeFor 'StartsWith (String '\"\\xFF\") (DataType 'String))", {"s"},
                     {"[{s: \"\\xFF\"}, {s: +∞})"});
    }

    Y_UNIT_TEST(SqlInRecognizesTheWholeTemplate) {
        auto changed = SqlIn(TupleList, 10000);
        SubstGlobal(changed, "(Uint64 '10001)", "(Uint64 '1)");
        UNIT_ASSERT(!Describe(changed, {"a"}));
        AssertExpression(changed, {"a"}, "f1()");
        changed = SqlIn(TupleList, 10000);
        SubstGlobal(changed, "(RangeFor '== item", "(RangeFor '> item");
        UNIT_ASSERT(!Describe(changed, {"a"}));
        AssertExpression(changed, {"a"}, "f1()");
        // A narrowing tuple cast may discard a whole row. It is not plain projection.
        changed = TupleSqlIn(PairList, 10000);
        SubstGlobal(changed, "(TupleType (DataType 'Int32) (DataType 'Int32))",
                    "(TupleType (DataType 'Int8) (DataType 'Int8))");
        UNIT_ASSERT(!Describe(changed, {"a", "b"}));
        AssertExpression(changed, {"a", "b"}, "f1()");
    }

    Y_UNIT_TEST(MalformedValuesAndBoundsFallBack) {
        const TStringBuf expressions[] = {
            "(RangeFor '== (Uuid 'short) (DataType 'Uuid))",
            "(RangeFor '== (Decimal '0 '0 '0) (DataType 'Decimal '0 '0))",
            "(RangeFor '== (Decimal 'inf '255 '0) (DataType 'Decimal '255 '0))",
            "(RangeFor '== (Decimal '1 '3 '4) (DataType 'Decimal '3 '4))",
            "(RangeFor '== (Parameter '$p) (DataType 'Int64))",
            "(RangeFor '== (PgConst '1) (PgType 'int8))",
            "(RangeFor '== (PgConst '1 (PgType)) (PgType 'int8))",
            "(AsRange '('((Just (Int64 '1)) (Int32)) '((Just (Int64 '2)) (Int32 '1))))",
            "(AsRange '('((Just (Int64 '1)) (Int32 '2)) '((Just (Int64 '2)) (Int32 '1))))",
            "(RangeEmpty)",
        };
        for (const auto expression : expressions) {
            NYql::TExprContext ctx;
            const auto ranges = Compile(expression, ctx);
            for (const auto estimate : {TMaybe<size_t>{}, TMaybe<size_t>{1}}) {
                const auto result = DescribeReadRanges(nullptr, ranges, {"a"}, estimate);
                UNIT_ASSERT_C(!result.Ranges && result.Expression, expression);
            }
        }
    }

    Y_UNIT_TEST(MalformedLiteralRangeCanUseComputedRanges) {
        const TStringBuf literals[] = {
            "(Unknown)", "'()", "'((KqlKeyInc))",
            "'((Unknown (Int64 '1)) (KqlKeyInc (Int64 '1)))",
            "'((KqlKeyInc (Uuid 'short)) (KqlKeyInc (Uuid 'short)))",
            "'((KqlKeyInc (Unknown)) (KqlKeyInc (Unknown)))",
            "'((KqlKeyInc (Int64 '1) (Int64 '2)) (KqlKeyInc (Int64 '1)))",
            "'((KqlKeyInc (Parameter '$p)) (KqlKeyInc (Int64 '1)))",
        };
        for (const auto expression : literals) {
            NYql::TExprContext ctx;
            const auto literal = Compile(expression, ctx);
            const auto unknown = DescribeReadRanges(literal, nullptr, {"a"});
            UNIT_ASSERT_C(!unknown.Ranges && unknown.Expression, expression);
            const auto point = Compile("(RangeFor '== (Int64 '1) (DataType 'Int64))", ctx);
            const auto known = DescribeReadRanges(literal, point, {"a"}, 1);
            UNIT_ASSERT_C(known.Ranges, expression);
            UNIT_ASSERT_VALUES_EQUAL(known.Ranges->front(), "[{a: 1}, {a: 1}]");
        }
    }

    Y_UNIT_TEST(LiteralRangesAndColumnNamesHaveBudgets) {
        NYql::TExprContext ctx;
        auto value = Compile("(Int64 '1)", ctx);
        for (size_t i = 0; i < 1000; ++i) value = ctx.NewCallable(value->Pos(), "Just", {value});
        const auto describe = [&](const NYql::TExprNode::TPtr& item, size_t width) {
            const auto bound = ctx.NewCallable(item->Pos(), "KqlKeyInc", NYql::TExprNode::TListType(width, item));
            return DescribeReadRanges(ctx.NewList(item->Pos(), {bound, bound}), nullptr, TVector<TString>(width, "key"));
        };
        UNIT_ASSERT(describe(value, 1).Expression);
        value = Compile(TStringBuilder() << "(String '\"" << TString(1024, 'x') << "\")", ctx);
        UNIT_ASSERT(describe(value, 40).Expression);
        value = Compile("(Int64 '1)", ctx);
        UNIT_ASSERT(describe(value, 65).Expression);
        const auto point = Compile("(RangeFor '== (Int64 '1) (DataType 'Int64))", ctx);
        const auto result = DescribeReadRanges(nullptr, point, {TString(MaxRangeEvaluationBytes + 1, 'x')}, 1);
        UNIT_ASSERT(!result.Ranges && result.Expression);
        UNIT_ASSERT_VALUES_EQUAL(*result.Expression, "f1()");
    }

    Y_UNIT_TEST(InvalidColumnAliasesFallBack) {
        NYql::TExprContext ctx;
        const auto point = Compile("(RangeFor '== (Int64 '1) (DataType 'Int64))", ctx);
        const auto literal = Compile("'((KqlKeyInc (Int64 '1)) (KqlKeyInc (Int64 '1)))", ctx);
        for (const auto estimate : {TMaybe<size_t>{}, TMaybe<size_t>{1}}) {
            const auto result = DescribeReadRanges(nullptr, point, {"_alias_missing_separator"}, estimate);
            UNIT_ASSERT(!result.Ranges && result.Expression);
            UNIT_ASSERT_VALUES_EQUAL(*result.Expression, "f1()");
        }
        const auto result = DescribeReadRanges(literal, nullptr, {"_alias_missing_separator"});
        UNIT_ASSERT(!result.Ranges && result.Expression);
        UNIT_ASSERT_VALUES_EQUAL(*result.Expression, "f1()");
    }

    Y_UNIT_TEST(MutatedExpressionShapesAlwaysHaveADescription) {
        NYql::TExprContext ctx;
        const TVector<NYql::TExprNode::TPtr> roots{
            Compile("(RangeFinalize (RangeMultiply (Uint64 '100) (RangeFor '== (Int64 '1) (DataType 'Int64)) "
                    "(RangeEmpty (DataType 'Int64))))", ctx),
            Compile("(AsRange '('((Just (Int64 '1)) (Int32 '1)) '((Just (Int64 '2)) (Int32 '0))))", ctx),
            Compile(SqlIn(TupleList, 10000), ctx),
            Compile(TupleSqlIn(PairList, 10000), ctx),
            Compile("'((KqlKeyInc (Int64 '1)) (KqlKeyExc (Parameter '$p (DataType 'Int64))))", ctx),
        };
        size_t mutations = 0;
        for (const auto& root : roots) {
            THashSet<const NYql::TExprNode*> visited;
            TVector<NYql::TExprNode::TPtr> pending{root};
            while (!pending.empty()) {
                const auto node = pending.back();
                pending.pop_back();
                if (!visited.insert(node.Get()).second) continue;
                for (const auto& child : node->Children()) pending.push_back(child);
                if (!node->ChildrenSize()) continue;
                const auto check = [&](NYql::TExprNode::TListType children) {
                    auto changed = ctx.ChangeChildren(*node, std::move(children));
                    auto mutated = ctx.ReplaceNode(NYql::TExprNode::TPtr(root), *node, std::move(changed));
                    for (const bool literal : {false, true}) {
                        const auto result = DescribeReadRanges(literal ? mutated : nullptr, literal ? nullptr : mutated, {"a", "b"}, 256);
                        UNIT_ASSERT(result.Ranges.has_value() != result.Expression.has_value());
                        if (result.Expression) UNIT_ASSERT(result.Expression->size() <= MaxRangeEvaluationBytes);
                    }
                    ++mutations;
                };
                for (size_t count = 0; count < node->ChildrenSize(); ++count) {
                    auto children = node->ChildrenList();
                    children.resize(count);
                    check(std::move(children));
                }
                auto children = node->ChildrenList();
                children.push_back(children.front());
                check(std::move(children));
                children.assign(node->ChildrenSize(), ctx.NewCallable(node->Pos(), "Unknown", {}));
                check(std::move(children));
            }
        }
        UNIT_ASSERT_C(mutations > 200, mutations);
    }

    Y_UNIT_TEST(SharedLiteralBoundsHaveAnExpandedSizeLimit) {
        NYql::TExprContext ctx;
        const auto value = Compile(TStringBuilder() << "(Just (String '\"" << TString(1024, 'x') << "\"))", ctx);
        NYql::TExprNode::TListType components(40, value);
        components.push_back(Compile("(Int32 '1)", ctx));
        const auto bound = ctx.NewList(value->Pos(), std::move(components));
        const auto pair = ctx.NewList(value->Pos(), {bound, bound});
        const auto ranges = ctx.NewCallable(value->Pos(), "AsRange", {pair});
        const auto description = DescribeReadRanges(nullptr, ranges, TVector<TString>(40, "key"), 1);
        UNIT_ASSERT(!description.Ranges);
        UNIT_ASSERT(description.Expression);
        UNIT_ASSERT_VALUES_EQUAL(*description.Expression, "f1()");

        const auto smallBound = ctx.NewList(value->Pos(), {value, Compile("(Int32 '1)", ctx)});
        const auto smallPair = ctx.NewList(value->Pos(), {smallBound, smallBound});
        auto shared = ctx.NewCallable(value->Pos(), "AsRange", {smallPair});
        for (size_t i = 0; i < 6; ++i) shared = ctx.NewCallable(value->Pos(), "RangeUnion", {shared, shared});
        UNIT_ASSERT(!DescribeReadRanges(nullptr, shared, {"key"}, 1).Ranges);
    }

    Y_UNIT_TEST(SharedExpressionAndCompositeWidthAreBounded) {
        NYql::TExprContext ctx;
        auto node = Compile("(RangeFor '== (Int64 '1) (DataType 'Int64))", ctx);
        for (size_t i = 0; i < 20; ++i) node = ctx.NewCallable(node->Pos(), "RangeUnion", {node, node});
        const auto description = DescribeReadRanges(nullptr, node, {"a"}, 1);
        UNIT_ASSERT(!description.Ranges);
        UNIT_ASSERT(description.Expression);
        UNIT_ASSERT_VALUES_EQUAL(*description.Expression, "f1()");
        const auto point = Compile("(RangeFor '== (Int64 '1) (DataType 'Int64))", ctx);
        NYql::TExprNode::TListType arguments{Compile("(Uint64 '1000)", ctx)};
        TVector<TString> keys;
        for (size_t i = 0; i < 65; ++i) {
            arguments.push_back(point);
            keys.push_back(TStringBuilder() << 'k' << i);
        }
        node = ctx.NewCallable(point->Pos(), "RangeMultiply", std::move(arguments));
        UNIT_ASSERT(!DescribeReadRanges(nullptr, node, keys, 1).Ranges);
    }
}

} // namespace NKikimr::NKqp::NOpt

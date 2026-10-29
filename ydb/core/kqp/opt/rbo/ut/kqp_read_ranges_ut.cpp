#include <ydb/core/kqp/opt/rbo/kqp_read_ranges_calculator.h>
#include <ydb/core/kqp/opt/rbo/kqp_read_ranges_explain.h>
#include <ydb/core/kqp/opt/rbo/kqp_read_ranges_structure.h>

#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/ast/yql_ast.h>

namespace NKikimr::NKqp::NOpt {
namespace {

NYql::TExprNode::TPtr Compile(TStringBuf expression, NYql::TExprContext& ctx) {
    auto ast = NYql::ParseAst(TStringBuilder() << "((return " << expression << "))");
    UNIT_ASSERT_C(ast.IsOk(), ast.Issues.ToString());
    NYql::TExprNode::TPtr node;
    UNIT_ASSERT_C(NYql::CompileExpr(*ast.Root, node, ctx, nullptr, nullptr), ctx.IssueManager.GetIssues().ToString());
    return node;
}

std::optional<TVector<TString>> Describe(TStringBuf expression, const TVector<TString>& keys, size_t maxRanges = Max<size_t>()) {
    NYql::TExprContext ctx;
    const auto list = NReadRanges::CalculateRanges(Compile(expression, ctx));
    if (!list || list->Width > keys.size() || list->Ranges.size() > maxRanges) {
        return std::nullopt;
    }
    TVector<TString> result;
    for (const auto& range : list->Ranges) {
        result.push_back(NReadRanges::FormatRange(range, keys));
    }
    return result;
}

TString DescribeLiteral(TStringBuf expression, const TVector<TString>& keys) {
    NYql::TExprContext ctx;
    return DescribeLiteralKeyRange(Compile(expression, ctx), keys);
}

void AssertExpression(TStringBuf expression, const TVector<TString>& keys, TStringBuf expected) {
    NYql::TExprContext ctx;
    const auto actual = NReadRanges::DescribeRangesStructure(Compile(expression, ctx), keys);
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
                '((Just (String 'a1)) (Int32 '0))))
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

    Y_UNIT_TEST(RangeLimitFallsBackToFullRange) {
        AssertRanges(R"(
            (RangeMultiply (Uint64 '1)
                (RangeUnion
                    (RangeFor '== (Uint64 '1) (DataType 'Uint64))
                    (RangeFor '== (Uint64 '3) (DataType 'Uint64))))
        )", {"a"}, {"({a: -∞}, {a: +∞})"});
    }

    Y_UNIT_TEST(RangeLimitPadsComputedPrefix) {
        AssertRanges(R"(
            (RangeMultiply (Uint64 '1)
                (RangeUnion
                    (RangeFor '== (Uint64 '1) (DataType 'Uint64))
                    (RangeFor '== (Uint64 '3) (DataType 'Uint64)))
                (RangeFor '== (Uint64 '10) (DataType 'Uint64)))
        )", {"a", "b"}, {
            "({a: 1, b: -∞}, {a: 1, b: +∞})",
            "({a: 3, b: -∞}, {a: 3, b: +∞})"
        });
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
        )", {"a", "b"}, {"({a: 1, b: -∞}, {a: 2, b: -∞})"});
    }

    Y_UNIT_TEST(NullableKeyRangesStartAfterNull) {
        AssertRanges(R"(
            (RangeFor '< (Int64 '5) (OptionalType (DataType 'Int64)))
        )", {"a"}, {"({a: NULL}, {a: 5})"});
        AssertRanges(R"(
            (RangeFor 'NotExists (Nothing (OptionalType (DataType 'Int64))) (OptionalType (DataType 'Int64)))
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
        AssertRanges(R"(
            (RangeFor '== (PgConst 'NULL (PgType 'text)) (PgType 'text))
        )", {"name"}, {"[{name: \"NULL\"}, {name: \"NULL\"}]"});
        AssertRanges(R"(
            (RangeFor '> (PgConst '5 (PgType 'int4)) (PgType 'int4))
        )", {"id"}, {"({id: 5}, {id: +∞})"});
    }

    Y_UNIT_TEST(UnorderedLiteralsAreUnknown) {
        UNIT_ASSERT(!Describe(R"(
            (RangeIntersect
                (RangeFor '> (PgConst 'a (PgType 'text)) (PgType 'text))
                (RangeFor '< (PgConst 'b (PgType 'text)) (PgType 'text)))
        )", {"name"}));
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

    Y_UNIT_TEST(CalculationStopsAtBudgetAndDisplayLimit) {
        UNIT_ASSERT(Describe(PointsUnion(1000), {"a"}));
        UNIT_ASSERT(!Describe(PointsUnion(1001), {"a"}));
        UNIT_ASSERT(Describe(PointsUnion(3), {"a"}, 3));
        UNIT_ASSERT(!Describe(PointsUnion(3), {"a"}, 2));
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
        )", {"a", "b"}, "(a ∈ (-∞, $p) ∪ [10, +∞)) × b ∈ {1}");
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

    Y_UNIT_TEST(SqlInIsCalculated) {
        const TString expression = TStringBuilder() << "(RangeFinalize (RangeMultiply (Uint64 '10000) "
            << SqlIn(TupleList, 10000) << " (RangeFor '=== (Int32 '7) (DataType 'Int64))))";
        AssertRanges(expression, {"a", "b"}, {"[{a: 1, b: 7}, {a: 1, b: 7}]", "[{a: 2, b: 7}, {a: 2, b: 7}]"});
        AssertExpression(expression, {"a", "b"}, "a ∈ {1, 2} × b ∈ {7}");
        AssertRanges(SqlIn("(Just (AsList (Int64 '3) (Int64 '3)))", 10000), {"a"}, {"[{a: 3}, {a: 3}]"});
    }

    Y_UNIT_TEST(SqlInOverLimitReadsEverything) {
        AssertRanges(SqlIn(TupleList, 1), {"a"}, {"({a: -∞}, {a: +∞})"});
        AssertExpression(SqlIn(TupleList, 1), {"a"}, "a ∈ (-∞, +∞)");
    }

    Y_UNIT_TEST(ExpressionLeavesOutUnconstrainedColumns) {
        const TString expression = TStringBuilder() << "(RangeMultiply (Uint64 '10000) "
            << SqlIn("(Just (Parameter '$list (ListType (DataType 'Int64))))", 10000)
            << " (AsRange '('((Nothing (OptionalType (DataType 'Int64))) (Int32 '0)) '((Nothing (OptionalType (DataType 'Int64))) (Int32 '0)))))";
        AssertExpression(expression, {"a", "b"}, "a ∈ $list");
    }

    Y_UNIT_TEST(SqlInOverParameterIsDescribed) {
        const auto expression = SqlIn("(Just (Parameter '$list (ListType (DataType 'Int64))))", 10000);
        UNIT_ASSERT(!Describe(expression, {"a"}));
        AssertExpression(expression, {"a"}, "a ∈ $list");
    }

    Y_UNIT_TEST(TupleSqlInIsCalculatedInKeyOrder) {
        const TString expression = TStringBuilder() << "(RangeFinalize " << TupleSqlIn(PairList, 10000) << ")";
        AssertRanges(expression, {"a", "b"}, {"[{a: 1, b: 10}, {a: 1, b: 10}]", "[{a: 2, b: 20}, {a: 2, b: 20}]"});
        AssertExpression(expression, {"a", "b"}, "(a, b) ∈ {(1, 10), (2, 20)}");
    }

    Y_UNIT_TEST(TupleSqlInOverParameterOrLimit) {
        const auto parameter = TupleSqlIn("(Map (Just (Parameter '$pairs (ListType (TupleType (DataType 'Int32) (DataType 'Int32))))) (lambda '(x) x))", 10000);
        UNIT_ASSERT(!Describe(parameter, {"a", "b"}));
        AssertExpression(parameter, {"a", "b"}, "(a, b) ∈ $pairs");
        AssertRanges(TupleSqlIn(PairList, 1), {"a", "b"}, {"({a: -∞, b: -∞}, {a: +∞, b: +∞})"});
    }

    Y_UNIT_TEST(FloatRangesStopBeforeNaN) {
        // NaN is ordered after every number and equals nothing, so ranges up to infinity end before it.
        AssertRanges("(RangeFor '> (Double '5) (DataType 'Double))", {"x"}, {"({x: 5}, {x: nan})"});
        AssertRanges("(RangeFor '== (Double '1.5) (DataType 'Double))", {"x"}, {"[{x: 1.5}, {x: 1.5}]"});
        AssertRanges("(RangeFor '== (Double 'nan) (DataType 'Double))", {"x"}, {});
        AssertRanges("(RangeFor '!= (Double '5) (DataType 'Double))", {"x"}, {"({x: -∞}, {x: 5})", "({x: 5}, {x: +∞})"});
        AssertRanges("(RangeFor '>= (Int32 '5) (DataType 'Double))", {"x"}, {"[{x: 5}, {x: nan})"});
    }

    Y_UNIT_TEST(DecimalAndDateValues) {
        AssertRanges("(RangeFor '< (Decimal '\"1.5\" '10 '2) (DataType 'Decimal '10 '2))", {"d"}, {"({d: -∞}, {d: 1.5})"});
        AssertRanges("(RangeFor '>= (Date '19723) (DataType 'Date))", {"d"}, {"[{d: 2024-01-01}, {d: +∞})"});
    }

    Y_UNIT_TEST(ValuesOutsideTheKeyTypeAreRounded) {
        // 300 does not fit Int8: == finds nothing, != everything, and < or > take the nearest Int8 bound.
        AssertRanges("(RangeFor '< (Int64 '300) (DataType 'Int8))", {"a"}, {"({a: -∞}, {a: 127}]"});
        AssertRanges("(RangeFor '> (Int64 '300) (DataType 'Int8))", {"a"}, {});
        AssertRanges("(RangeFor '> (Int64 '-300) (DataType 'Int8))", {"a"}, {"[{a: -128}, {a: +∞})"});
        AssertRanges("(RangeFor '== (Int64 '300) (DataType 'Int8))", {"a"}, {});
        AssertRanges("(RangeFor '!= (Int64 '300) (DataType 'Int8))", {"a"}, {"({a: -∞}, {a: +∞})"});
        // A fraction has no Int64 counterpart, a whole number does.
        AssertRanges("(RangeFor '== (Double '1.5) (DataType 'Int64))", {"a"}, {});
        AssertRanges("(RangeFor '== (Double '2) (DataType 'Int64))", {"a"}, {"[{a: 2}, {a: 2}]"});
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
        AssertExpression(parameter, {"a"}, "a ∈ {1} if $flag");
        const TString full = "(If (Parameter '$flag (DataType 'Bool)) (AsRange '('((Nothing (OptionalType (DataType 'Int64))) (Int32 '0)) "
                             "'((Nothing (OptionalType (DataType 'Int64))) (Int32 '0)))) (RangeEmpty (DataType 'Int64)))";
        AssertExpression(full, {"a"}, "a ∈ (-∞, +∞) if $flag");
    }
}

} // namespace NKikimr::NKqp::NOpt

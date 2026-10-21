#include <ydb/core/kqp/opt/rbo/kqp_read_ranges_explain.h>

#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/ast/yql_ast.h>

namespace NKikimr::NKqp::NOpt {
namespace {

std::optional<TVector<TString>> Describe(TStringBuf expression, const TVector<TString>& keys) {
    auto ast = NYql::ParseAst(TStringBuilder() << "((return " << expression << "))");
    UNIT_ASSERT_C(ast.IsOk(), ast.Issues.ToString());
    NYql::TExprContext ctx;
    NYql::TExprNode::TPtr node;
    UNIT_ASSERT_C(NYql::CompileExpr(*ast.Root, node, ctx, nullptr, nullptr), ctx.IssueManager.GetIssues().ToString());
    return BuildReadRangeDescriptions(node, keys);
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
            "({timestamp: 1776309661000000, resource_type: \"a1\", resource_id: \"b1\"}, "
            "{timestamp: 1776310561000000, resource_type: -∞, resource_id: -∞})"
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
        )", {"z", "a"}, {"[{z: 5, a: -∞}, {z: 10, a: +∞}]"});
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
        )", {"a", "b"}, {"({a: 1, b: 10}, {a: 1, b: +∞}]"});
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
            "[{a: 1, b: -∞}, {a: 1, b: +∞}]",
            "[{a: 3, b: -∞}, {a: 3, b: +∞}]"
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
        )", {"a", "b"}, {"[{a: 1, b: -∞}, {a: +∞, b: +∞})"});
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
        )", {"a", "b"}, {"[{a: 1, b: -∞}, {a: 2, b: -∞})"});
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
        )", {"a", "b"}, {"({a: -∞, b: -∞}, {a: 5, b: +∞}]"});
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
        UNIT_ASSERT(!Describe(R"(
            (RangeFor '== (Double '1.5) (DataType 'Double))
        )", {"x"}));
    }
}

} // namespace NKikimr::NKqp::NOpt

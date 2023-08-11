#include "sql_ut.h"
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/sql/sql.h>
#include <util/generic/map.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/split.h>

using namespace NSQLTranslation;

NYql::TAstParseResult MatchRecognizeSqlToYql(const TString& query) {
    TString enablingPragma = R"(
pragma FeatureR010="prototype";
)";
    return SqlToYql(enablingPragma + query);
}

const NYql::TAstNode* FindMatchRecognizeParam(const NYql::TAstNode* root, TString name) {
    auto matchRecognizeBlock = FindNodeByChildAtomContent(root, 1, "match_recognize");
    UNIT_ASSERT(matchRecognizeBlock);
    auto paramNode = FindNodeByChildAtomContent(matchRecognizeBlock, 1, name);
    return paramNode->GetChild(2);
}


bool IsQuotedListOfSize(const NYql::TAstNode* node, ui32 size) {
    UNIT_ASSERT(node->IsListOfSize(2));
    if (!node->IsListOfSize(2))
        return false;
    UNIT_ASSERT_EQUAL(node->GetChild(0)->GetContent(), "quote");
    if (node->GetChild(0)->GetContent() != "quote")
        return false;
    UNIT_ASSERT_EQUAL(node->GetChild(1)->GetChildrenCount(), size);
    return node->GetChild(1)->IsListOfSize(size);
}

Y_UNIT_TEST_SUITE(MatchRecognize) {
    auto minValidMatchRecognizeSql = R"(
USE plato;
SELECT *
FROM Input MATCH_RECOGNIZE(
    PATTERN ( A )
    DEFINE A as A
    )
)";
    Y_UNIT_TEST(EnabledWithPragma) {
        UNIT_ASSERT(not SqlToYql(minValidMatchRecognizeSql).IsOk());
        UNIT_ASSERT(MatchRecognizeSqlToYql(minValidMatchRecognizeSql).IsOk());
    }

    Y_UNIT_TEST(InputTableName) {
        auto r = MatchRecognizeSqlToYql(minValidMatchRecognizeSql);
        UNIT_ASSERT(r.IsOk());
        auto input = FindMatchRecognizeParam(r.Root, "input");
        UNIT_ASSERT(input->IsAtom() && input->GetContent() == "core");
    }

    Y_UNIT_TEST(MatchRecognizeAndSample) {
        auto matchRecognizeAndSample = R"(
USE plato;
SELECT *
FROM Input  MATCH_RECOGNIZE(
    PATTERN ( A )
    DEFINE A as A
    ) TABLESAMPLE BERNOULLI(1.0)
)";
        UNIT_ASSERT(not MatchRecognizeSqlToYql(matchRecognizeAndSample).IsOk());
    }

    Y_UNIT_TEST(NoPartitionBy) {
        auto r = MatchRecognizeSqlToYql(minValidMatchRecognizeSql);
        UNIT_ASSERT(r.IsOk());
        auto partitionKeySelector = FindMatchRecognizeParam(r.Root, "partitionKeySelector");
        UNIT_ASSERT(IsQuotedListOfSize(partitionKeySelector->GetChild(2), 0)); //empty tuple
        auto partitionColumns = FindMatchRecognizeParam(r.Root, "partitionColumns");
        UNIT_ASSERT(IsQuotedListOfSize(partitionColumns, 0)); //empty tuple
    }

    Y_UNIT_TEST(PartitionBy) {
        auto stmt = R"(
USE plato;
SELECT *
FROM Input MATCH_RECOGNIZE(
    PARTITION BY col1 as c1, ~CAST(col1 as Int32) as invertedC1, c2
    PATTERN ( A )
    DEFINE A as A
    )
)";
        auto r = MatchRecognizeSqlToYql(stmt);
        UNIT_ASSERT(r.IsOk());
        auto partitionKeySelector = FindMatchRecognizeParam(r.Root, "partitionKeySelector");
        UNIT_ASSERT(IsQuotedListOfSize(partitionKeySelector->GetChild(2), 3));
        auto partitionColumns = FindMatchRecognizeParam(r.Root, "partitionColumns");
        UNIT_ASSERT(IsQuotedListOfSize(partitionColumns, 3));
        //TODO check partitioner lambdas(alias/no alias)
    }

    Y_UNIT_TEST(NoOrderBy) {
        auto r = MatchRecognizeSqlToYql(minValidMatchRecognizeSql);
        UNIT_ASSERT(r.IsOk());
        auto sortTraits = FindMatchRecognizeParam(r.Root, "sortTraits");
        UNIT_ASSERT(sortTraits && sortTraits->IsListOfSize(1));
        UNIT_ASSERT(sortTraits->GetChild(0)->GetContent() == "Void");
    }

    Y_UNIT_TEST(OrderBy) {
        auto stmt = R"(
USE plato;
SELECT *
FROM Input MATCH_RECOGNIZE(
    ORDER BY col1, ~CAST(col1 as Int32), c2
    PATTERN ( A )
    DEFINE A as A
    )
)";
        auto r = MatchRecognizeSqlToYql(stmt);
        UNIT_ASSERT(r.IsOk());
        auto sortTraits = FindMatchRecognizeParam(r.Root, "sortTraits");
        UNIT_ASSERT(sortTraits && sortTraits->IsListOfSize(4));
        UNIT_ASSERT(sortTraits->GetChild(0)->GetContent() == "SortTraits");
        UNIT_ASSERT(IsQuotedListOfSize(sortTraits->GetChild(2), 3));
        UNIT_ASSERT(IsQuotedListOfSize(sortTraits->GetChild(3)->GetChild(2), 3));
    }
    Y_UNIT_TEST(Measures) {
        //TODO https://st.yandex-team.ru/YQL-16186
    }
    Y_UNIT_TEST(RowsPerMatch) {
        //TODO https://st.yandex-team.ru/YQL-16186
    }
    Y_UNIT_TEST(SkipAfterMatch) {
        //TODO https://st.yandex-team.ru/YQL-16186
    }
    Y_UNIT_TEST(row_pattern_initial_or_seek) {
        //TODO https://st.yandex-team.ru/YQL-16186
    }
    Y_UNIT_TEST(PatternSimple) {
        const auto stmt = R"(
USE plato;
SELECT *
FROM Input MATCH_RECOGNIZE(
    PATTERN (A+  B* C?)
    DEFINE A as A
    )
)";
        const auto& r = MatchRecognizeSqlToYql(stmt);
        UNIT_ASSERT(r.IsOk());
        const auto& patternCallable = FindMatchRecognizeParam(r.Root, "pattern");
        UNIT_ASSERT_EQUAL(patternCallable->GetChild(0)->GetContent(), "MatchRecognizePattern");
        UNIT_ASSERT_EQUAL(patternCallable->GetChildrenCount(), 1 + 1);
        const auto& term = patternCallable->GetChild(1);
        UNIT_ASSERT(IsQuotedListOfSize(term, 3));
    }

    Y_UNIT_TEST(PatternMultiTerm) {
        const auto stmt = R"(
USE plato;
SELECT *
FROM Input MATCH_RECOGNIZE(
    PATTERN ($ A+ B{1,3} | C{3} D{1,4} E? | F?? | G{3,}? H*? I J ^)
    DEFINE A as A
    )
)";
        const auto& r = MatchRecognizeSqlToYql(stmt);
        UNIT_ASSERT(r.IsOk());
        const auto& patternCallable = FindMatchRecognizeParam(r.Root, "pattern");
        UNIT_ASSERT_EQUAL(patternCallable->GetChild(0)->GetContent(), "MatchRecognizePattern");
        UNIT_ASSERT_EQUAL(patternCallable->GetChildrenCount(), 1 + 4);
        const auto& lastTerm = patternCallable->GetChild(4);
        UNIT_ASSERT(IsQuotedListOfSize(lastTerm, 5));
    }

    Y_UNIT_TEST(PatternWithParanthesis) {
        const auto stmt = R"(
USE plato;
SELECT *
FROM Input MATCH_RECOGNIZE(
    PATTERN (
        A | ($ B)+ C D
    )
    DEFINE A as A
    )
)";
        const auto& r = MatchRecognizeSqlToYql(stmt);
        UNIT_ASSERT(r.IsOk());
        const auto& patternCallable = FindMatchRecognizeParam(r.Root, "pattern");
        UNIT_ASSERT_EQUAL(patternCallable->GetChild(0)->GetContent(), "MatchRecognizePattern");
        UNIT_ASSERT_EQUAL(patternCallable->GetChildrenCount(), 1 + 2);
        const auto& firstTerm = patternCallable->GetChild(1);
        UNIT_ASSERT(IsQuotedListOfSize(firstTerm, 1));
        const auto& lastTerm = patternCallable->GetChild(2);
        UNIT_ASSERT(IsQuotedListOfSize(lastTerm, 3));
        const auto& firstFactorOfLastTerm = lastTerm->GetChild(1)->GetChild(0);
        UNIT_ASSERT(IsQuotedListOfSize(firstFactorOfLastTerm, 5));
        const auto nestedPattern = firstFactorOfLastTerm->GetChild(1)->GetChild(0);
        UNIT_ASSERT_EQUAL(nestedPattern->GetChildrenCount(), 1 + 1);
        UNIT_ASSERT_EQUAL(nestedPattern->GetChild(0)->GetContent(), "MatchRecognizePattern");
        UNIT_ASSERT(IsQuotedListOfSize(nestedPattern->GetChild(1), 2));
    }

    Y_UNIT_TEST(PatternLimietedNesting) {
        const size_t MaxNesting = 20;
        for (size_t extraNesting = 0; extraNesting <= 1; ++extraNesting) {
            std::string pattern;
            for (size_t i = 0; i != MaxNesting + extraNesting; ++i)
                pattern.push_back('(');
            pattern.push_back('A');
            for (size_t i = 0; i != MaxNesting + extraNesting; ++i)
                pattern.push_back(')');
            const auto stmt = TString(R"(
USE plato;
SELECT *
FROM Input MATCH_RECOGNIZE(
        PATTERN(
)") + pattern + R"(
            )
    DEFINE A as A
    )
)";
            const auto &r = MatchRecognizeSqlToYql(stmt);
            if (not extraNesting) {
                UNIT_ASSERT(r.IsOk());
            } else {
                UNIT_ASSERT(not r.IsOk());
            }
        }
    }


    //TODO add tests for factors, quantifiers and greediness https://st.yandex-team.ru/YQL-16186

   Y_UNIT_TEST(row_pattern_subset_clause) {
        //TODO https://st.yandex-team.ru/YQL-16186
    }

    Y_UNIT_TEST(Defines) {
        //TODO https://st.yandex-team.ru/YQL-16186
    }
}

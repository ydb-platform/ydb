#include "sql_ut.h"
#include "match_recognize.h"
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/core/sql_types/match_recognize.h>
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

bool IsLambda(const NYql::TAstNode* node, ui32 numberOfArgs) {
    if (!node->IsListOfSize(3)) {
        return false;
    }
    if (!node->GetChild(0)->IsAtom() || node->GetChild(0)->GetContent() != "lambda") {
        return  false;
    }
    return IsQuotedListOfSize(node->GetChild(1), numberOfArgs);
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
        auto stmt = R"(
USE plato;
SELECT *
FROM Input MATCH_RECOGNIZE(
    MEASURES
        Last(Q.dt) as T,
        First(Y.key) as Key
    PATTERN ( Y Q )
    DEFINE Y as true
)
)";
        auto r = MatchRecognizeSqlToYql(stmt);
        UNIT_ASSERT(r.IsOk());
        const auto measures = FindMatchRecognizeParam(r.Root, "measures");
        UNIT_ASSERT_VALUES_EQUAL(6, measures->GetChildrenCount());
        const auto columnNames = measures->GetChild(3);
        UNIT_ASSERT(IsQuotedListOfSize(columnNames, 2));
        UNIT_ASSERT_VALUES_EQUAL("T", columnNames->GetChild(1)->GetChild(0)->GetChild(1)->GetContent());
        UNIT_ASSERT_VALUES_EQUAL("Key", columnNames->GetChild(1)->GetChild(1)->GetChild(1)->GetContent());
        UNIT_ASSERT(IsLambda(measures->GetChild(4), 2));
        UNIT_ASSERT(IsLambda(measures->GetChild(5), 2));
    }
    Y_UNIT_TEST(RowsPerMatch) {
        {
            const auto stmt = R"(
USE plato;
SELECT *
FROM Input MATCH_RECOGNIZE(
    ONE ROW PER MATCH
    PATTERN (A)
    DEFINE A as A
)
)";
            auto r = MatchRecognizeSqlToYql(stmt);
            UNIT_ASSERT(r.IsOk());
            auto rowsPerMatch = FindMatchRecognizeParam(r.Root, "rowsPerMatch");
            UNIT_ASSERT_VALUES_EQUAL("RowsPerMatch_OneRow", rowsPerMatch->GetChild(1)->GetContent());
        }
        {
            const auto stmt = R"(
USE plato;
SELECT *
FROM Input MATCH_RECOGNIZE(
    ALL ROWS PER MATCH
    PATTERN (A)
    DEFINE A as A
)
)";
            auto r = MatchRecognizeSqlToYql(stmt);
            UNIT_ASSERT(not r.IsOk()); ///https://st.yandex-team.ru/YQL-16213
        }
        { //default
            const auto stmt = R"(
USE plato;
SELECT *
FROM Input MATCH_RECOGNIZE(
    PATTERN (A)
    DEFINE A as A
)
)";
            auto r = MatchRecognizeSqlToYql(stmt);
            UNIT_ASSERT(r.IsOk());
            auto rowsPerMatch = FindMatchRecognizeParam(r.Root, "rowsPerMatch");
            UNIT_ASSERT_VALUES_EQUAL("RowsPerMatch_OneRow", rowsPerMatch->GetChild(1)->GetContent());
        }

    }
    Y_UNIT_TEST(SkipAfterMatch) {
        {
            const auto stmt = R"(
USE plato;
SELECT *
FROM Input MATCH_RECOGNIZE(
    AFTER MATCH SKIP TO NEXT ROW
    PATTERN (A)
    DEFINE A as A
)
)";
            auto r = MatchRecognizeSqlToYql(stmt);
            UNIT_ASSERT(r.IsOk());
            auto skipTo = FindMatchRecognizeParam(r.Root, "skipTo");
            UNIT_ASSERT_VALUES_EQUAL("AfterMatchSkip_NextRow", skipTo->GetChild(1)->GetChild(0)->GetChild(1)->GetContent());
        }
        {
            const auto stmt = R"(
USE plato;
SELECT *
FROM Input MATCH_RECOGNIZE(
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (A)
    DEFINE A as A
)
)";
            auto r = MatchRecognizeSqlToYql(stmt);
            UNIT_ASSERT(r.IsOk());
            auto skipTo = FindMatchRecognizeParam(r.Root, "skipTo");
            UNIT_ASSERT_VALUES_EQUAL("AfterMatchSkip_PastLastRow", skipTo->GetChild(1)->GetChild(0)->GetChild(1)->GetContent());
        }
        {
            const auto stmt = R"(
USE plato;
SELECT *
FROM Input MATCH_RECOGNIZE(
    AFTER MATCH SKIP TO FIRST Y
    PATTERN (A | (U | (Q | Y)) | ($ B)+ C D)
    DEFINE A as A
)
)";
            auto r = MatchRecognizeSqlToYql(stmt);
            UNIT_ASSERT(r.IsOk());
            auto skipTo = FindMatchRecognizeParam(r.Root, "skipTo");
            UNIT_ASSERT_VALUES_EQUAL("AfterMatchSkip_ToFirst", skipTo->GetChild(1)->GetChild(0)->GetChild(1)->GetContent());
            UNIT_ASSERT_VALUES_EQUAL("Y", skipTo->GetChild(1)->GetChild(1)->GetChild(1)->GetContent());
        }
        {
            const auto stmt = R"(
USE plato;
SELECT *
FROM Input MATCH_RECOGNIZE(
    AFTER MATCH SKIP TO FIRST T -- unknown pattern var
    PATTERN (A | (U | (Q | Y)) | ($ B)+ C D)
    DEFINE A as A
)
)";
            auto r = MatchRecognizeSqlToYql(stmt);
            UNIT_ASSERT(not r.IsOk());
        }
        {
            const auto stmt = R"(
USE plato;
SELECT *
FROM Input MATCH_RECOGNIZE(
    AFTER MATCH SKIP TO LAST Y
    PATTERN (A | (U | (Q | Y)) | ($ B)+ C D)
    DEFINE A as A
)
)";
            auto r = MatchRecognizeSqlToYql(stmt);
            UNIT_ASSERT(r.IsOk());
            auto skipTo = FindMatchRecognizeParam(r.Root, "skipTo");
            UNIT_ASSERT_VALUES_EQUAL("AfterMatchSkip_ToLast", skipTo->GetChild(1)->GetChild(0)->GetChild(1)->GetContent());
            UNIT_ASSERT_VALUES_EQUAL("Y", skipTo->GetChild(1)->GetChild(1)->GetChild(1)->GetContent());
        }
        {
            const auto stmt = R"(
USE plato;
SELECT *
FROM Input MATCH_RECOGNIZE(
    AFTER MATCH SKIP TO LAST T -- unknown pattern var
    PATTERN (A | (U | (Q | Y)) | ($ B)+ C D)
    DEFINE A as A
)
)";
            auto r = MatchRecognizeSqlToYql(stmt);
            UNIT_ASSERT(not r.IsOk());
        }
        {
            const auto stmt = R"(
USE plato;
SELECT *
FROM Input MATCH_RECOGNIZE(
    AFTER MATCH SKIP TO Y
    PATTERN (A | (U | (Q | Y)) | ($ B)+ C D)
    DEFINE A as A
)
)";
            auto r = MatchRecognizeSqlToYql(stmt);
            UNIT_ASSERT(r.IsOk());
            auto skipTo = FindMatchRecognizeParam(r.Root, "skipTo");
            UNIT_ASSERT_VALUES_EQUAL("AfterMatchSkip_To", skipTo->GetChild(1)->GetChild(0)->GetChild(1)->GetContent());
            UNIT_ASSERT_VALUES_EQUAL("Y", skipTo->GetChild(1)->GetChild(1)->GetChild(1)->GetContent());
        }
        {
            const auto stmt = R"(
USE plato;
SELECT *
FROM Input MATCH_RECOGNIZE(
    AFTER MATCH SKIP TO T -- unknown pattern var
    PATTERN (A | (U | (Q | Y)) | ($ B)+ C D)
    DEFINE A as A
)
)";
            auto r = MatchRecognizeSqlToYql(stmt);
            UNIT_ASSERT(not r.IsOk());
        }
    }
    Y_UNIT_TEST(row_pattern_initial) {
        const auto stmt = R"(
USE plato;
SELECT *
FROM Input MATCH_RECOGNIZE(
    INITIAL
    PATTERN (A+  B* C?)
    DEFINE A as A
    )
)";
        auto r = MatchRecognizeSqlToYql(stmt);
        UNIT_ASSERT(not r.IsOk());
    }

    Y_UNIT_TEST(row_pattern_seek) {
        const auto stmt = R"(
USE plato;
SELECT *
FROM Input MATCH_RECOGNIZE(
    SEEK
    PATTERN (A+  B* C?)
    DEFINE A as A
    )
)";
        auto r = MatchRecognizeSqlToYql(stmt);
        UNIT_ASSERT(not r.IsOk());
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
        UNIT_ASSERT(IsQuotedListOfSize(firstFactorOfLastTerm, 6));
        const auto nestedPattern = firstFactorOfLastTerm->GetChild(1)->GetChild(0);
        UNIT_ASSERT_EQUAL(nestedPattern->GetChildrenCount(), 1 + 1);
        UNIT_ASSERT_EQUAL(nestedPattern->GetChild(0)->GetContent(), "MatchRecognizePattern");
        UNIT_ASSERT(IsQuotedListOfSize(nestedPattern->GetChild(1), 2));
    }

    Y_UNIT_TEST(PatternManyAlternatives) {
        const auto stmt = R"(
USE plato;
SELECT *
FROM Input MATCH_RECOGNIZE(
PATTERN (
        (A B C D ) | (B A C D ) | (C B A D ) | (B C A D ) | (C A B D ) | (A C B D ) | (D A B C ) | (A D B C ) | (B A D C ) | (A B D C ) | (B D A C ) | (D B A C ) | (C D A B ) | (D C A B ) | (A D C B ) | (D A C B ) | (A C D B ) | (C A D B ) | (B C D A ) | (C B D A ) | (D C B A ) | (C D B A ) | (D B C A ) | (B D C A )
    )
    DEFINE A as A
)
)";
        UNIT_ASSERT(MatchRecognizeSqlToYql(stmt).IsOk());
    }

    Y_UNIT_TEST(PatternLimitedNesting) {
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

    Y_UNIT_TEST(PatternFactorQuantifiers) {
        auto makeRequest = [](const TString& factor) {
           return TString(R"(
USE plato;
SELECT *
FROM Input MATCH_RECOGNIZE(
        PATTERN(
)") + factor + R"(
            )
    DEFINE A as A
    )
)";
        };
        auto getTheFactor = [](const NYql::TAstNode* root) {
            const auto& patternCallable = FindMatchRecognizeParam(root, "pattern");
            const auto& factor =  patternCallable->GetChild(1)->GetChild(1)->GetChild(0)->GetChild(1);
            return NYql::NMatchRecognize::TRowPatternFactor{
                    TString(), //primary var or subexpression, not used in this test
                    FromString<uint64_t>(factor->GetChild(1)->GetChild(1)->GetContent()), //QuantityMin
                    FromString<uint64_t>(factor->GetChild(2)->GetChild(1)->GetContent()), //QuantityMax
                    FromString<bool>(factor->GetChild(3)->GetChild(1)->GetContent()), //Greedy
                    false, //Output, not used in this test
                    false, // Flag "Unused", not used in this test
            };
        };
        {
            //no quantifiers
            const auto stmt = makeRequest("A");
            const auto &r = MatchRecognizeSqlToYql(stmt);
            UNIT_ASSERT(r.IsOk());
            const auto& factor = getTheFactor(r.Root);
            UNIT_ASSERT_EQUAL(1, factor.QuantityMin);
            UNIT_ASSERT_EQUAL(1, factor.QuantityMax);
            UNIT_ASSERT(factor.Greedy);
        }
        {
            //optional greedy(default)
            const auto stmt = makeRequest("A?");
            const auto &r = MatchRecognizeSqlToYql(stmt);
            UNIT_ASSERT(r.IsOk());
            const auto& factor = getTheFactor(r.Root);
            UNIT_ASSERT_EQUAL(0, factor.QuantityMin);
            UNIT_ASSERT_EQUAL(1, factor.QuantityMax);
            UNIT_ASSERT(factor.Greedy);
        }
        {
            //optional reluctant
            const auto stmt = makeRequest("A??");
            const auto &r = MatchRecognizeSqlToYql(stmt);
            UNIT_ASSERT(r.IsOk());
            const auto& factor = getTheFactor(r.Root);
            UNIT_ASSERT_EQUAL(0, factor.QuantityMin);
            UNIT_ASSERT_EQUAL(1, factor.QuantityMax);
            UNIT_ASSERT(!factor.Greedy);
        }
        {
            //+ greedy(default)
            const auto stmt = makeRequest("A+");
            const auto &r = MatchRecognizeSqlToYql(stmt);
            UNIT_ASSERT(r.IsOk());
            const auto& factor = getTheFactor(r.Root);
            UNIT_ASSERT_EQUAL(1, factor.QuantityMin);
            UNIT_ASSERT_EQUAL(std::numeric_limits<uint64_t>::max(), factor.QuantityMax);
            UNIT_ASSERT(factor.Greedy);
        }
        {
            //+ reluctant
            const auto stmt = makeRequest("A+?");
            const auto &r = MatchRecognizeSqlToYql(stmt);
            UNIT_ASSERT(r.IsOk());
            const auto& factor = getTheFactor(r.Root);
            UNIT_ASSERT_EQUAL(1, factor.QuantityMin);
            UNIT_ASSERT_EQUAL(std::numeric_limits<uint64_t>::max(), factor.QuantityMax);
            UNIT_ASSERT(!factor.Greedy);
        }
        {
            //* greedy(default)
            const auto stmt = makeRequest("A*");
            const auto &r = MatchRecognizeSqlToYql(stmt);
            UNIT_ASSERT(r.IsOk());
            const auto& factor = getTheFactor(r.Root);
            UNIT_ASSERT_EQUAL(0, factor.QuantityMin);
            UNIT_ASSERT_EQUAL(std::numeric_limits<uint64_t>::max(), factor.QuantityMax);
            UNIT_ASSERT(factor.Greedy);
        }
        {
            //* reluctant
            const auto stmt = makeRequest("A*?");
            const auto &r = MatchRecognizeSqlToYql(stmt);
            UNIT_ASSERT(r.IsOk());
            const auto& factor = getTheFactor(r.Root);
            UNIT_ASSERT_EQUAL(0, factor.QuantityMin);
            UNIT_ASSERT_EQUAL(std::numeric_limits<uint64_t>::max(), factor.QuantityMax);
            UNIT_ASSERT(!factor.Greedy);
        }
        {
            //exact n
            const auto stmt = makeRequest("A{4}");
            const auto &r = MatchRecognizeSqlToYql(stmt);
            UNIT_ASSERT(r.IsOk());
            const auto& factor = getTheFactor(r.Root);
            UNIT_ASSERT_EQUAL(4, factor.QuantityMin);
            UNIT_ASSERT_EQUAL(4, factor.QuantityMax);
        }
        {
            //from n to m greedy(default
            const auto stmt = makeRequest("A{4, 7}");
            const auto &r = MatchRecognizeSqlToYql(stmt);
            UNIT_ASSERT(r.IsOk());
            const auto& factor = getTheFactor(r.Root);
            UNIT_ASSERT_EQUAL(4, factor.QuantityMin);
            UNIT_ASSERT_EQUAL(7, factor.QuantityMax);
            UNIT_ASSERT(factor.Greedy);
        }
        {
            //from n to m reluctant
            const auto stmt = makeRequest("A{4,7}?");
            const auto &r = MatchRecognizeSqlToYql(stmt);
            UNIT_ASSERT(r.IsOk());
            const auto& factor = getTheFactor(r.Root);
            UNIT_ASSERT_EQUAL(4, factor.QuantityMin);
            UNIT_ASSERT_EQUAL(7, factor.QuantityMax);
            UNIT_ASSERT(!factor.Greedy);
        }
        {
            //at least n greedy(default)
            const auto stmt = makeRequest("A{4,}");
            const auto &r = MatchRecognizeSqlToYql(stmt);
            UNIT_ASSERT(r.IsOk());
            const auto& factor = getTheFactor(r.Root);
            UNIT_ASSERT_EQUAL(4, factor.QuantityMin);
            UNIT_ASSERT_EQUAL(std::numeric_limits<uint64_t>::max(), factor.QuantityMax);
            UNIT_ASSERT(factor.Greedy);
        }
        {
            //at least n reluctant
            const auto stmt = makeRequest("A{4,}?");
            const auto &r = MatchRecognizeSqlToYql(stmt);
            UNIT_ASSERT(r.IsOk());
            const auto& factor = getTheFactor(r.Root);
            UNIT_ASSERT_EQUAL(4, factor.QuantityMin);
            UNIT_ASSERT_EQUAL(std::numeric_limits<uint64_t>::max(), factor.QuantityMax);
            UNIT_ASSERT(!factor.Greedy);
        }
        {
            //at most m greedy(default)
            const auto stmt = makeRequest("A{,7}");
            const auto &r = MatchRecognizeSqlToYql(stmt);
            UNIT_ASSERT(r.IsOk());
            const auto& factor = getTheFactor(r.Root);
            UNIT_ASSERT_EQUAL(0, factor.QuantityMin);
            UNIT_ASSERT_EQUAL(7, factor.QuantityMax);
            UNIT_ASSERT(factor.Greedy);
        }
        {
            //at least n reluctant
            const auto stmt = makeRequest("A{,7}?");
            const auto &r = MatchRecognizeSqlToYql(stmt);
            UNIT_ASSERT(r.IsOk());
            const auto& factor = getTheFactor(r.Root);
            UNIT_ASSERT_EQUAL(0, factor.QuantityMin);
            UNIT_ASSERT_EQUAL(7, factor.QuantityMax);
            UNIT_ASSERT(!factor.Greedy);
        }

        {
            //quantifiers on subexpression
            const auto stmt = makeRequest("(A B+ C | D | ^){4,7}?");
            const auto &r = MatchRecognizeSqlToYql(stmt);
            UNIT_ASSERT(r.IsOk());
            const auto& factor = getTheFactor(r.Root);
            UNIT_ASSERT_EQUAL(4, factor.QuantityMin);
            UNIT_ASSERT_EQUAL(7, factor.QuantityMax);
            UNIT_ASSERT(!factor.Greedy);
        }
    }

    Y_UNIT_TEST(Permute) {
        const auto stmt = R"(
USE plato;
SELECT *
FROM Input MATCH_RECOGNIZE(
    PATTERN (
        PERMUTE(A, B, C, D, E) --5 variables produce 5! permutations
    )
    DEFINE A as A
)
)";
        const auto& r = MatchRecognizeSqlToYql(stmt);
        UNIT_ASSERT(r.IsOk());

        const auto& patternCallable = FindMatchRecognizeParam(r.Root, "pattern");
        const auto permutePattern = patternCallable->GetChild(1)->GetChild(1)->GetChild(0)->GetChild(1)->GetChild(0);
        UNIT_ASSERT(permutePattern->IsListOfSize(1 + 120)); //CallableName + 5!
    }

    Y_UNIT_TEST(PermuteTooMuch) {
        for (size_t n = 1; n <= NYql::NMatchRecognize::MaxPermutedItems + 1; ++n) {
            std::vector<std::string> vars(n);
            std::generate(begin(vars), end(vars), [n = 0] () mutable { return "A" + std::to_string(n++);});
            const auto stmt = TString(R"(
USE plato;
SELECT *
FROM Input MATCH_RECOGNIZE(
    PATTERN (
        PERMUTE( )" + std::accumulate(cbegin(vars) + 1, cend(vars), vars.front(),
            [](const std::string& acc, const std::string& v) {
                return acc + ", " + v;
            }) +
    R"(
        )
    )
    DEFINE A0 as A0
)
)"
            );
            const auto &r = MatchRecognizeSqlToYql(stmt);
            if (n <= NYql::NMatchRecognize::MaxPermutedItems) {
                UNIT_ASSERT(r.IsOk());
            } else {
                UNIT_ASSERT(!r.IsOk());
            }
        }
    }


    Y_UNIT_TEST(row_pattern_subset_clause) {
        //TODO https://st.yandex-team.ru/YQL-16186
    }

    Y_UNIT_TEST(Defines) {
        auto stmt = R"(
USE plato;
SELECT *
FROM Input MATCH_RECOGNIZE(
    PATTERN ( Y Q L )
    DEFINE
        Y as true,
        Q as Q.V = "value",
        L as L.V = LAST(Q.T)
)
)";
        auto r = MatchRecognizeSqlToYql(stmt);
        UNIT_ASSERT(r.IsOk());
        const auto defines = FindMatchRecognizeParam(r.Root, "define");
        UNIT_ASSERT_VALUES_EQUAL(7, defines->GetChildrenCount());
        const auto varNames = defines->GetChild(3);
        UNIT_ASSERT(IsQuotedListOfSize(varNames, 3));
        UNIT_ASSERT_VALUES_EQUAL("Y", varNames->GetChild(1)->GetChild(0)->GetChild(1)->GetContent());
        UNIT_ASSERT_VALUES_EQUAL("Q", varNames->GetChild(1)->GetChild(1)->GetChild(1)->GetContent());
        UNIT_ASSERT_VALUES_EQUAL("L", varNames->GetChild(1)->GetChild(2)->GetChild(1)->GetContent());

        UNIT_ASSERT(IsLambda(defines->GetChild(4), 3));
        UNIT_ASSERT(IsLambda(defines->GetChild(5), 3));
        UNIT_ASSERT(IsLambda(defines->GetChild(6), 3));
    }

    Y_UNIT_TEST(AbsentRowPatternVariableInDefines) {
        auto getStatement = [](const TString &var) {
            return TStringBuilder() << R"(
USE plato;
SELECT *
FROM Input MATCH_RECOGNIZE(
PATTERN ( Q )
DEFINE
)" << var << " AS TRUE )";
        };
        UNIT_ASSERT(MatchRecognizeSqlToYql(getStatement("Q")).IsOk());
        UNIT_ASSERT(!MatchRecognizeSqlToYql(getStatement("Y")).IsOk());
    }

    Y_UNIT_TEST(CheckRequiredNavigationFunction) {
        TString stmtPrefix = R"(
USE plato;
SELECT *
FROM Input MATCH_RECOGNIZE(
    PATTERN ( Y Q L )
    DEFINE
        L as L.V =
)";
        //Be aware that right parenthesis is added at the end of the query as required
        UNIT_ASSERT(MatchRecognizeSqlToYql(stmtPrefix + "LAST(Q.dt) )").IsOk());
        UNIT_ASSERT(!MatchRecognizeSqlToYql(stmtPrefix + "Q.dt )").IsOk());
    }

}

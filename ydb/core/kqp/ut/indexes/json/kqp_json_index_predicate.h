#pragma once

#include <ydb/core/kqp/ut/indexes/json/kqp_json_index_corpus.h>

namespace NKikimr::NKqp {

// Controls which predicate families TPredicateBuilder generates
struct TPredicateBuilderOptions {
    // JSON function families
    bool EnableJsonExists = false;
    bool EnableJsonValue = false;
    bool EnableNonJsonFilters = false;

    // JsonPath extensions
    bool EnableJsonPathMethods = false;
    // JsonPath filter predicates
    bool EnableJsonPathPredicates = false;

    // Syntax extensions
    bool EnablePassingVariables = false;
    bool EnableSqlParameters = false;

    // Comparison operators
    bool EnableRangeComparisons = false;
    bool EnableBetween = false;
    bool EnableInList = false;

    // Compound predicate shapes
    bool EnableAndCombinations = false;
    bool EnableOrCombinations = false;

    // Predicates that check whether the root JSON value IS a specific scalar literal
    bool EnableJsonIsLiteral = false;

    // Arithmetic operators: inside JsonPath and at the SQL level
    bool EnableArithmeticOperators = false;

    // IS [NOT] NULL applied to JSON_* function results
    bool EnableSqlNullChecks = false;

    // IS [NOT] DISTINCT FROM applied to JSON_* function results
    bool EnableDistinctFrom = false;

    // Complex JsonPath filter patterns
    bool EnableComplexJsonPathFilters = false;
};

// A single SQL predicate for the WHERE clause
struct TBuiltPredicate {
    std::string Sql;

    // SQL parameters referenced in Sql
    std::optional<NYdb::TParams> Params;

    // If true, the VIEW json_idx query must fail with an extract error.
    bool ExpectExtractError = false;
    std::string ExpectedErrorSubstr = "Failed to extract search terms from predicate";
};

// Produces a batch of SQL JSON_* predicates
class TPredicateBuilder {
public:
    std::vector<TBuiltPredicate> BuildBatch(const TJsonCorpus& corpus, bool isStrict,
        size_t maxCount, ui64 seed,
        const TPredicateBuilderOptions& opts = {}) const;
};

} // namespace NKikimr::NKqp

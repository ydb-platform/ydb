#pragma once

#include <ydb/core/kqp/ut/indexes/json/kqp_json_index_corpus.h>

namespace NKikimr::NKqp {

// Controls which predicate families TPredicateBuilder generates
struct TPredicateBuilderOptions {
    // JSON function families
    bool EnableJsonExists = true;
    bool EnableJsonValue = true;
    bool EnableNonJsonFilters = true;

    // JsonPath extensions
    bool EnableJsonPathMethods = true;

    // Syntax extensions
    bool EnablePassingVariables = true;
    bool EnableSqlParameters = true;

    // Comparison operators
    bool EnableRangeComparisons = true;
    bool EnableBetween = true;
    bool EnableInList = true;

    // Compound predicate shapes
    bool EnableAndCombinations = true;
    bool EnableOrCombinations = true;

    // Predicates that check whether the root JSON value IS a specific scalar literal
    bool EnableJsonIsLiteral = true;
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

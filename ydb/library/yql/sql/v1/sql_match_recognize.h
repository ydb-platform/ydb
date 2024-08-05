#pragma once

#include "sql_translation.h"
#include "match_recognize.h"

namespace NSQLTranslationV1 {

using namespace NSQLv1Generated;

class TSqlMatchRecognizeClause: public TSqlTranslation {
public:
    TSqlMatchRecognizeClause(TContext& ctx, NSQLTranslation::ESqlMode mode)
        : TSqlTranslation(ctx, mode)
    {}
    TMatchRecognizeBuilderPtr CreateBuilder(const TRule_row_pattern_recognition_clause& node);
private:
    TVector<TNamedFunction> ParsePartitionBy(const TRule_window_partition_clause& partitionClause);
    TNamedFunction ParseOneMeasure(const TRule_row_pattern_measure_definition& node);
    TVector<TNamedFunction> ParseMeasures(const TRule_row_pattern_measure_list& node);
    std::pair<TPosition, ERowsPerMatch> ParseRowsPerMatch(const TRule_row_pattern_rows_per_match& rowsPerMatchClause);
    std::pair<TPosition, TAfterMatchSkipTo> ParseAfterMatchSkipTo(const TRule_row_pattern_skip_to& skipToClause);
    NYql::NMatchRecognize::TRowPatternTerm ParsePatternTerm(const TRule_row_pattern_term& node);
    NYql::NMatchRecognize::TRowPattern ParsePattern(const TRule_row_pattern& node);
    TNamedFunction ParseOneDefinition(const TRule_row_pattern_definition& node);
    TVector<TNamedFunction> ParseDefinitions(const TRule_row_pattern_definition_list& node);
private:
    size_t PatternNestingLevel = 0;
};

} // namespace NSQLTranslationV1

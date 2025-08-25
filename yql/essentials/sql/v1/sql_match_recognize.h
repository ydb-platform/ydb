#pragma once

#include "match_recognize.h"
#include "node.h"
#include "sql_translation.h"

namespace NSQLTranslationV1 {

class TSqlMatchRecognizeClause final : public TSqlTranslation {
public:
    TSqlMatchRecognizeClause(TContext& ctx, NSQLTranslation::ESqlMode mode);
    TMatchRecognizeBuilderPtr CreateBuilder(const TRule_row_pattern_recognition_clause& node);
    static constexpr size_t MaxPatternNesting = 20;
    static constexpr size_t MaxPermutedItems = 6;

private:
    std::tuple<TNodePtr, TNodePtr> ParsePartitionBy(TPosition pos, const TRule_window_partition_clause* node);
    TMaybe<TVector<TSortSpecificationPtr>> ParseOrderBy(const TRule_order_by_clause* node);
    TNamedFunction ParseOneMeasure(const TRule_row_pattern_measure_definition& node);
    TVector<TNamedFunction> ParseMeasures(const TRule_row_pattern_measure_list* node);
    TNodePtr ParseRowsPerMatch(TPosition pos, const TRule_row_pattern_rows_per_match* node);
    TNodePtr ParseAfterMatchSkipTo(TPosition pos, const TRule_row_pattern_skip_to* node);
    TNodePtr BuildPatternFactor(TPosition pos, TNodePtr primary, std::tuple<ui64, ui64, bool, bool, bool> quantifier);
    TNodePtr ParsePatternFactor(TPosition pos, const TRule_row_pattern_factor& node, size_t nestingLevel, bool output);
    TNodePtr BuildPatternTerm(TPosition pos, std::vector<TNodePtr> term);
    TNodePtr ParsePatternTerm(TPosition pos, const TRule_row_pattern_term& node, size_t nestingLevel, bool output);
    TNodePtr BuildPattern(TPosition pos, std::vector<TNodePtr> pattern);
    TNodePtr ParsePattern(TPosition pos, const TRule_row_pattern& node, size_t nestingLevel, bool output);
    TMaybe<TNodePtr> ParseSubset(TPosition pos, const TRule_row_pattern_subset_clause* node);
    TNamedFunction ParseOneDefinition(const TRule_row_pattern_definition& node);
    TVector<TNamedFunction> ParseDefinitions(const TRule_row_pattern_definition_list& node);

private:
    THashSet<TString> PatternVarNames_;
    TNodePtr PatternVars_;
};

} // namespace NSQLTranslationV1

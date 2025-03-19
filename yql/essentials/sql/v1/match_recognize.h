#pragma once

#include "node.h"

#include <yql/essentials/core/sql_types/match_recognize.h>
#include <util/generic/ptr.h>

namespace NSQLTranslationV1 {

struct TNamedFunction {
    TNodePtr Callable;
    TString Name;
};

class TMatchRecognizeBuilder: public TSimpleRefCount<TMatchRecognizeBuilder> {
public:
    TMatchRecognizeBuilder(
        TPosition pos,
        TNodePtr partitionKeySelector,
        TNodePtr partitionColumns,
        TVector<TSortSpecificationPtr> sortSpecs,
        TVector<TNamedFunction> measures,
        TNodePtr rowsPerMatch,
        TNodePtr skipTo,
        TNodePtr pattern,
        TNodePtr patternVars,
        TNodePtr subset,
        TVector<TNamedFunction> definitions)
    : Pos(pos)
    , PartitionKeySelector(std::move(partitionKeySelector))
    , PartitionColumns(std::move(partitionColumns))
    , SortSpecs(std::move(sortSpecs))
    , Measures(std::move(measures))
    , RowsPerMatch(std::move(rowsPerMatch))
    , SkipTo(std::move(skipTo))
    , Pattern(std::move(pattern))
    , PatternVars(std::move(patternVars))
    , Subset(std::move(subset))
    , Definitions(std::move(definitions))
    {}

    TNodePtr Build(TContext& ctx, TString label, ISource* source);

private:
    TPosition Pos;
    TNodePtr PartitionKeySelector;
    TNodePtr PartitionColumns;
    TVector<TSortSpecificationPtr> SortSpecs;
    TVector<TNamedFunction> Measures;
    TNodePtr RowsPerMatch;
    TNodePtr SkipTo;
    TNodePtr Pattern;
    TNodePtr PatternVars;
    TNodePtr Subset;
    TVector<TNamedFunction> Definitions;
};

using TMatchRecognizeBuilderPtr = TIntrusivePtr<TMatchRecognizeBuilder>;

TNodePtr BuildMatchRecognizeColumnAccess(TPosition pos, TString var, TString column);
TNodePtr BuildMatchRecognizeDefineAggregate(TPosition pos, TString name, TVector<TNodePtr> args);
TNodePtr BuildMatchRecognizeVarAccess(TPosition pos, TNodePtr extractor);

} // namespace NSQLTranslationV1

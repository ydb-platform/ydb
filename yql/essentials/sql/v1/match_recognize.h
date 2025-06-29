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
    : Pos_(pos)
    , PartitionKeySelector_(std::move(partitionKeySelector))
    , PartitionColumns_(std::move(partitionColumns))
    , SortSpecs_(std::move(sortSpecs))
    , Measures_(std::move(measures))
    , RowsPerMatch_(std::move(rowsPerMatch))
    , SkipTo_(std::move(skipTo))
    , Pattern_(std::move(pattern))
    , PatternVars_(std::move(patternVars))
    , Subset_(std::move(subset))
    , Definitions_(std::move(definitions))
    {}

    TNodePtr Build(TContext& ctx, TString label, ISource* source);

private:
    TPosition Pos_;
    TNodePtr PartitionKeySelector_;
    TNodePtr PartitionColumns_;
    TVector<TSortSpecificationPtr> SortSpecs_;
    TVector<TNamedFunction> Measures_;
    TNodePtr RowsPerMatch_;
    TNodePtr SkipTo_;
    TNodePtr Pattern_;
    TNodePtr PatternVars_;
    TNodePtr Subset_;
    TVector<TNamedFunction> Definitions_;
};

using TMatchRecognizeBuilderPtr = TIntrusivePtr<TMatchRecognizeBuilder>;

TNodePtr BuildMatchRecognizeColumnAccess(TPosition pos, TString var, TString column);
TNodePtr BuildMatchRecognizeDefineAggregate(TPosition pos, TString name, TVector<TNodePtr> args);
TNodePtr BuildMatchRecognizeVarAccess(TPosition pos, TNodePtr extractor);

} // namespace NSQLTranslationV1

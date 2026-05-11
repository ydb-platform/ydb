#pragma once

#include "aggregation.h"
#include "node.h"

namespace NSQLTranslationV1 {

struct TYqlAggregationArgs {
    TString FunctionName;
    TString FactoryName;
    EAggregationType Type;
    EAggregateMode Mode;
    TVector<TNodePtr> Args;
};

TNodeResult BuildYqlAggregation(TPosition position, TYqlAggregationArgs&& args);

TNodePtr BuildYqlGrouping(TPosition position, TVector<TNodePtr> args);

} // namespace NSQLTranslationV1

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

} // namespace NSQLTranslationV1

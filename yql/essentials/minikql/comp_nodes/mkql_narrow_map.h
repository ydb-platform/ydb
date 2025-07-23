#pragma once

#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr::NMiniKQL {

IComputationNode* WrapNarrowMap(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}

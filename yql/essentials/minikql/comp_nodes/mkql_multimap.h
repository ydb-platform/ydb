#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr::NMiniKQL {

IComputationNode* WrapMultiMap(TCallable& callable, const TComputationNodeFactoryContext& ctx);

IComputationNode* WrapNarrowMultiMap(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // namespace NKikimr::NMiniKQL

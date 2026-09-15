#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr::NMiniKQL {

IComputationNode* WrapFlatMap(TCallable& callable, const TComputationNodeFactoryContext& ctx);

IComputationNode* WrapNarrowFlatMap(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // namespace NKikimr::NMiniKQL

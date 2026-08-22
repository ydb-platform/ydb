#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr::NMiniKQL {

IComputationNode* WrapPickle(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapStablePickle(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapUnpickle(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapAscending(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapDescending(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // namespace NKikimr::NMiniKQL

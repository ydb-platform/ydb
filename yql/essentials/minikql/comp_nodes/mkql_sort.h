#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr::NMiniKQL {

IComputationNode* WrapUnstableSort(TCallable& callable, const TComputationNodeFactoryContext& ctx);

IComputationNode* WrapSort(TCallable& callable, const TComputationNodeFactoryContext& ctx);

IComputationNode* WrapTop(TCallable& callable, const TComputationNodeFactoryContext& ctx);

IComputationNode* WrapTopSort(TCallable& callable, const TComputationNodeFactoryContext& ctx);

IComputationNode* WrapKeepTop(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // namespace NKikimr::NMiniKQL

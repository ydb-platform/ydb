#pragma once

#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr::NMiniKQL {

IComputationNode* WrapWideTop(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapWideTopSort(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapWideSort(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // namespace NKikimr::NMiniKQL

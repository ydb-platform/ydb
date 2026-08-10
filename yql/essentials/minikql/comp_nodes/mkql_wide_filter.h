#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr::NMiniKQL {

IComputationNode* WrapWideFilter(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapWideTakeWhile(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapWideSkipWhile(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapWideTakeWhileInclusive(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapWideSkipWhileInclusive(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // namespace NKikimr::NMiniKQL

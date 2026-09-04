#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr::NMiniKQL {

IComputationNode* WrapTakeWhile(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapSkipWhile(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapTakeWhileInclusive(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapSkipWhileInclusive(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // namespace NKikimr::NMiniKQL

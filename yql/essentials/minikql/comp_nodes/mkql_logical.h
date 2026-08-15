#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr::NMiniKQL {

IComputationNode* WrapAnd(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapOr(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapXor(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapNot(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // namespace NKikimr::NMiniKQL

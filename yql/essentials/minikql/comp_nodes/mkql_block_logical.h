#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr::NMiniKQL {

IComputationNode* WrapBlockAnd(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapBlockOr(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapBlockXor(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapBlockNot(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // namespace NKikimr::NMiniKQL

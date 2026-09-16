#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr::NMiniKQL {

IComputationNode* WrapBlockDecimalMul(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapBlockDecimalDiv(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapBlockDecimalMod(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // namespace NKikimr::NMiniKQL

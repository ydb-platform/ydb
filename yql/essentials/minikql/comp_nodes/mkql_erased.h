#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr::NMiniKQL {

IComputationNode* WrapAsErased(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapPeekErased(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // namespace NKikimr::NMiniKQL

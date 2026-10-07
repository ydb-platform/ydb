#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr::NMiniKQL {

IComputationNode* WrapFromString(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapStrictFromString(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // namespace NKikimr::NMiniKQL

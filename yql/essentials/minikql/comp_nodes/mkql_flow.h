#pragma once

#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr::NMiniKQL {

IComputationNode* WrapToFlow(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapFromFlow(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // namespace NKikimr::NMiniKQL

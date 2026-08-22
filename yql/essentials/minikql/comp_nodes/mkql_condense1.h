#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr::NMiniKQL {

IComputationNode* WrapCondense1(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapSqueeze1(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // namespace NKikimr::NMiniKQL

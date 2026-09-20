#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr::NMiniKQL {

IComputationNode* WrapBlockMember(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapBlockNth(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // namespace NKikimr::NMiniKQL

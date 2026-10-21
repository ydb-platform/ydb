#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>

namespace NKikimr::NMiniKQL {

IComputationNode* WrapHoppingCore(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // namespace NKikimr::NMiniKQL

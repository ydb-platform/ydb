#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr::NMiniKQL {

IComputationNode* WrapTimezoneId(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapTimezoneName(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapAddTimezone(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // namespace NKikimr::NMiniKQL

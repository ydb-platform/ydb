#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapToDynamicLinear(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapFromDynamicLinear(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // namespace NMiniKQL
} // namespace NKikimr

#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NYql {
namespace NDq {

using namespace NKikimr::NMiniKQL;

IComputationNode* WrapDqBlockHashJoin(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // namespace NDq
} // namespace NYql

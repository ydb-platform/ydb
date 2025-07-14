#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapBlockGraceJoinCore(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // NKikimr
} // NMiniKQL

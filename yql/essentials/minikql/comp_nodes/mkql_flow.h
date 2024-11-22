#pragma once

#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapToFlow(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapFromFlow(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}

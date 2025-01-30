#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapSourceOf(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapSource(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}

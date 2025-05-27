#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapFromString(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapStrictFromString(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}

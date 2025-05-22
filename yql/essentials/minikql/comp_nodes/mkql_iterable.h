#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapIterable(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}

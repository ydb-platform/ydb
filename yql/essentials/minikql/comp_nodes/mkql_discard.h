#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapDiscard(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}

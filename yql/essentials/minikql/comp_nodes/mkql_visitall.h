#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapVisitAll(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}

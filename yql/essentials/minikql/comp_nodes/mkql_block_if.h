#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapBlockIf(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}

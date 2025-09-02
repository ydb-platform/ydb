#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapWithContext(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}

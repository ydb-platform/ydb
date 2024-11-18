#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapSeq(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}

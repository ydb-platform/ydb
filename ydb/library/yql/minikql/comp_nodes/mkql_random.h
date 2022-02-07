#pragma once
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapNewMTRand(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapNextMTRand(TCallable& callable, const TComputationNodeFactoryContext& ctx);

enum class ERandom {
   Double,
   Number,
   Uuid
};

template <ERandom Rnd>
IComputationNode* WrapRandom(TCallable& callable, const TComputationNodeFactoryContext& ctx);


}
}

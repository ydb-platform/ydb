#pragma once
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapGraceJoin(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapGraceSelfJoin(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapGraceJoinWithSpilling(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapGraceSelfJoinWithSpilling(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}

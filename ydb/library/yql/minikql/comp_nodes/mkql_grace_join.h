#pragma once
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapGraceJoin(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapSelfJoin(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}

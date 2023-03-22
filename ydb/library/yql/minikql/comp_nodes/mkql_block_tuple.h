#pragma once
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapBlockAsTuple(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapBlockNth(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}

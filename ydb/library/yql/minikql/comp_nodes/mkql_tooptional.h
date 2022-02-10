#pragma once
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapHead(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapLast(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}

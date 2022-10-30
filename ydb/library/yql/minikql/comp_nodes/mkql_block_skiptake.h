#pragma once
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapWideSkipBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapWideTakeBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}

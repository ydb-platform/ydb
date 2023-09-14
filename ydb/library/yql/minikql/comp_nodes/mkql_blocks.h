#pragma once

#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapToBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapWideToBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapFromBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapWideFromBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapAsScalar(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapReplicateScalar(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapBlockExpandChunked(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}

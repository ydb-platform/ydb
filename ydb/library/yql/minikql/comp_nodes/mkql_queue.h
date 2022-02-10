#pragma once
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include "mkql_safe_circular_buffer.h"

#include <ydb/library/yql/public/udf/udf_value.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapQueueCreate(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapQueuePush(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapQueuePop(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapQueuePeek(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapQueueRange(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapPreserveStream(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}

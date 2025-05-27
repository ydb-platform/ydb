#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include "mkql_safe_circular_buffer.h"

#include <yql/essentials/public/udf/udf_value.h>

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

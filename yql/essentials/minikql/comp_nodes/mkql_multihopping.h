#pragma once

#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/mkql_watermark.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapMultiHoppingCore(TCallable& callable, const TComputationNodeFactoryContext& ctx, TWatermark& watermark);

}
}

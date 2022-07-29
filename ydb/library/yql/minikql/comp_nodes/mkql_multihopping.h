#pragma once

#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {


struct TWatermark {
    TInstant WatermarkIn;
};

IComputationNode* WrapMultiHoppingCore(TCallable& callable, const TComputationNodeFactoryContext& ctx, TWatermark& watermark, bool watermarkMode = false);

}
}

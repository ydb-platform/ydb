#pragma once

#include <yt/yql/providers/yt/codec/yt_codec_io.h>

#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/mkql_node.h>

namespace NYql {

NKikimr::NMiniKQL::IComputationNode* WrapYtOutput(NKikimr::NMiniKQL::TCallable& callable,
    const NKikimr::NMiniKQL::TComputationNodeFactoryContext& ctx, TMkqlWriterImpl& writer);

} // NYql

#pragma once

#include <ydb/library/yql/providers/yt/codec/yt_codec_io.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/mkql_node.h>

namespace NYql {

NKikimr::NMiniKQL::IComputationNode* WrapYtOutput(NKikimr::NMiniKQL::TCallable& callable,
    const NKikimr::NMiniKQL::TComputationNodeFactoryContext& ctx, TMkqlWriterImpl& writer);

} // NYql

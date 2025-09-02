#pragma once

#include <yt/yql/providers/yt/codec/yt_codec.h>

#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/mkql_node.h>

#include <yt/cpp/mapreduce/interface/io.h>

namespace NYql {

NKikimr::NMiniKQL::IComputationNode* WrapYtInput(NKikimr::NMiniKQL::TCallable& callable, const NKikimr::NMiniKQL::TComputationNodeFactoryContext& ctx,
    const TMkqlIOSpecs& specs, NYT::IReaderImplBase* input);

} // NYql

#pragma once

#include <yt/yql/providers/yt/codec/yt_codec_job.h>
#include <yql/essentials/providers/common/codec/yql_codec.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

#include <yt/cpp/mapreduce/interface/io.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYql {

NKikimr::NMiniKQL::TComputationNodeFactory GetJobFactory(NYql::NCommon::TCodecContext& codecCtx,
    const TString& optLLVM, const TMkqlIOSpecs* specs, NYT::IReaderImplBase* reader, TMkqlWriterImpl* writer, const TString& prefix = "Yt");

} // NYql

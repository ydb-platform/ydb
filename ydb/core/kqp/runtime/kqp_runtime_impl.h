#pragma once

#include <ydb/core/scheme/scheme_tabledefs.h>

#include <ydb/library/yql/dq/runtime/dq_output_consumer.h>
#include <ydb/library/yql/minikql/mkql_node.h>

namespace NKikimr {
namespace NKqp {

struct TKqpRangePartition {
    TKeyDesc::TPartitionRangeInfo Range;
    ui64 ChannelId = std::numeric_limits<ui64>::max();
};

TTableId ParseTableId(const NMiniKQL::TRuntimeNode& node);

NScheme::TTypeInfo UnwrapTypeInfoFromStruct(const NMiniKQL::TStructType& structType, ui32 index);

NYql::NDq::IDqOutputConsumer::TPtr CreateOutputRangePartitionConsumer(
    TVector<NYql::NDq::IDqOutput::TPtr>&& outputs, TVector<TKqpRangePartition>&& partitions,
    TVector<NScheme::TTypeInfo>&& keyColumnTypes, TVector<ui32>&& keyColumnIndices,
    const NMiniKQL::TTypeEnvironment& typeEnv);

NYql::NDq::IDqOutputConsumer::TPtr CreateKqpApplyEffectsConsumer(NUdf::IApplyContext* applyCtx);

} // namespace NKqp
} // namespace NKikimr

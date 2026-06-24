#pragma once

#include "dq_columns_resolve.h"
#include "dq_output.h"

#include <yql/essentials/public/udf/udf_value_builder.h>

#include <yql/essentials/minikql/mkql_alloc.h>

namespace NKikimr::NMiniKQL {
    class TTypeEnvironment;
    class THolderFactory;
} // namespace NKikimr::NMiniKQL

namespace NYql::NDq {

// Per-scatter-consumer telemetry. Surfaces what the adaptive router actually did
// (vs. what the plan promised): how many of the dst-channels got used, how often
// backpressure forced widening the active set, and what fill-level routing
// decisions looked like. Aggregated per task; KQP rolls up per stage.
struct TDqScatterStats {
    // The DstStageId of the channels this scatter writes into, used as the
    // aggregation key on the KQP side. Zero if unknown (sinks).
    ui32 DstStageId = 0;
    // Total number of destination channels the plan allocated.
    ui32 OutputsCount = 0;
    // High-water mark of active channels at any single point in time.
    // ActiveCountMax / OutputsCount is the underutilization signal.
    ui32 ActiveCountMax = 0;
    // How many times a new channel was promoted from inactive to active.
    // Equals (ActiveCountMax - 1) in a single-task scatter run.
    ui64 ActivationsCount = 0;
    // Of those activations, how many were triggered by the active set hitting
    // HardLimit on the current channel vs. SoftLimit. HardLimit dominance means
    // downstream cannot keep up at all; Soft means it almost can.
    ui64 TriggersHard = 0;
    ui64 TriggersSoft = 0;
    // Routing decisions by the fill level of the chosen channel (sanity check:
    // PicksHardLimit should stay 0 — the router fails over to a fresh channel
    // before returning a Hard one).
    ui64 PicksNoLimit = 0;
    ui64 PicksSoftLimit = 0;
    ui64 PicksHardLimit = 0;
};

class IDqOutputConsumer : public TSimpleRefCount<IDqOutputConsumer>,
    public NKikimr::NMiniKQL::TWithDefaultMiniKQLAlloc {
public:
    using TPtr = TIntrusivePtr<IDqOutputConsumer>;

public:
    virtual ~IDqOutputConsumer() = default;

    virtual EDqFillLevel GetFillLevel() const = 0;
    virtual void Consume(NKikimr::NUdf::TUnboxedValue&& value) = 0;
    virtual void WideConsume(NKikimr::NUdf::TUnboxedValue values[], ui32 count) = 0;
    virtual void Consume(NDqProto::TCheckpoint&& checkpoint) = 0;
    virtual void Consume(NDqProto::TWatermark&& watermark) = 0;
    virtual void Finish() = 0;
    virtual void Flush() = 0;
    virtual bool IsFinished() const = 0;
    virtual bool IsEarlyFinished() const = 0;
    virtual TString DebugString() {
        return "";
    }
    // Default no-op; only scatter consumers (and multi-consumers that contain
    // scatter children) append their stats. Called once per task at finish.
    virtual void CollectScatterStats(TVector<TDqScatterStats>& /*out*/) const {}
};

IDqOutputConsumer::TPtr CreateOutputMultiConsumer(TVector<IDqOutputConsumer::TPtr>&& consumers);

IDqOutputConsumer::TPtr CreateOutputMapConsumer(IDqOutput::TPtr output);

IDqOutputConsumer::TPtr CreateOutputHashPartitionConsumer(
    TVector<IDqOutput::TPtr>&& outputs,
    TVector<TColumnInfo>&& keyColumns, const  NKikimr::NMiniKQL::TType* outputType,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    TMaybe<ui8> minFillPercentage,
    const NDqProto::TTaskOutputHashPartition& hashPartition,
    NUdf::IPgBuilder* pgBuilder
);

IDqOutputConsumer::TPtr CreateOutputBroadcastConsumer(TVector<IDqOutput::TPtr>&& outputs, TMaybe<ui32> outputWidth);

IDqOutputConsumer::TPtr CreateOutputScatterConsumer(
    TVector<IDqOutput::TPtr>&& outputs, TMaybe<ui32> outputWidth,
    ui32 primaryChannelIdx = 0, ui32 dstStageId = 0);

} // namespace NYql::NDq

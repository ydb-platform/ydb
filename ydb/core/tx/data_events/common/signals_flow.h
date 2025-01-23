#pragma once
#include <ydb/core/tx/columnshard/counters/common/owner.h>

namespace NKikimr::NEvWrite {

enum class EWriteStage {
    Created = 0,
    Queued,
    Started,
    BuildBatch,
    WaitFlush,
    BuildSlices,
    BuildSlicesPack,
    Result,
    Finished,
    Aborted
};

class TWriteFlowCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;

    std::vector<NMonitoring::TDynamicCounters::TCounterPtr> CountByWriteStage;
    std::vector<NMonitoring::TDynamicCounters::TCounterPtr> WriteStageAdd;
    std::vector<std::vector<NMonitoring::TDynamicCounters::TCounterPtr>> CountByStageMoving;
    std::vector<std::vector<NMonitoring::TDynamicCounters::TCounterPtr>> CountByStageDuration;
    std::vector<NMonitoring::THistogramPtr> DurationToStage;
    NMonitoring::TDynamicCounters::TCounterPtr DurationToFinish;
    NMonitoring::TDynamicCounters::TCounterPtr DurationToAbort;

public:
    void OnStageMove(const EWriteStage fromStage, const EWriteStage toStage, const TDuration d) const {
        CountByWriteStage[(ui32)fromStage]->Sub(1);
        CountByWriteStage[(ui32)toStage]->Add(1);
        WriteStageAdd[(ui32)toStage]->Add(1);
        DurationToStage[(ui32)toStage]->Collect(d.MilliSeconds());
        CountByStageMoving[(ui32)fromStage][(ui32)toStage]->Add(1);
        CountByStageDuration[(ui32)fromStage][(ui32)toStage]->Add(d.MilliSeconds());
    }

    void OnWritingStart(const EWriteStage stage) const {
        WriteStageAdd[(ui32)stage]->Add(1);
        CountByWriteStage[(ui32)stage]->Add(1);
    }

    void OnWriteFinished(const TDuration d) const {
        DurationToFinish->Add(d.MicroSeconds());
    }

    void OnWriteAborted(const TDuration d) const {
        DurationToAbort->Add(d.MicroSeconds());
    }

    TWriteFlowCounters();
};

}   // namespace NKikimr::NEvWrite

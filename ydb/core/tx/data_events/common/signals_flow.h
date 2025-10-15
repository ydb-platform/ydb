#pragma once
#include <ydb/library/signals/owner.h>
#include <ydb/library/signals/states.h>

namespace NKikimr::NEvWrite {

enum class EWriteStage {
    Created = 0,
    Queued,
    Started,
    BuildBatch,
    WaitFlush,
    SlicesConstruction,
    SlicesReady,
    SlicesError,
    PackSlicesConstruction,
    PackSlicesReady,
    PackSlicesError,
    SuccessWritingToLocalDB,
    FailWritingToLocalDB,
    Replied,
    Aborted,
    Finished
};

class TWriteFlowCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;

    NMonitoring::TDynamicCounters::TCounterPtr DurationToFinish;
    NMonitoring::TDynamicCounters::TCounterPtr DurationToAbort;
    NOlap::NCounters::TStateSignalsOperator<EWriteStage> Tracing;

public:
    NOlap::NCounters::TStateSignalsOperator<EWriteStage>& MutableTracing() {
        return Tracing;
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

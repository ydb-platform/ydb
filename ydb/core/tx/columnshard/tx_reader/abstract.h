#pragma once
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/columnshard/counters/common_data.h>

#include <ydb/library/actors/core/actor.h>

namespace NLWTrace {
class TOrbit;
}

namespace NKikimr {

class ITxReader {
private:
    YDB_READONLY_DEF(TString, StageName);
    TString NamePrefix;
    NLWTrace::TOrbit* Orbit = nullptr;
    bool IsReady = false;
    bool IsStarted = false;
    TMonotonic FirstAttemptInstant;
    ui32 Attempts = 0;
    TDuration ExecutionDuration;
    std::shared_ptr<ITxReader> NextReaderAfterLoad;
    NColumnShard::TLoadTimeSignals PrechargeCounters;
    NColumnShard::TLoadTimeSignals ReaderCounters;
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) = 0;
    virtual bool DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) = 0;

    virtual std::shared_ptr<ITxReader> BuildNextReaderAfterLoad() {
        return nullptr;
    }

    bool ExecuteAttempt(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx);

    virtual void DoAddNamePrefix(const TString& /*prefix*/) {
    }

    virtual void DoSetOrbit(NLWTrace::TOrbit& /*orbit*/) {
    }

public:
    virtual ~ITxReader() = default;

    void AddNamePrefix(const TString& prefix) {
        NamePrefix = prefix + NamePrefix;
        StageName = prefix + StageName;
        DoAddNamePrefix(prefix);
    }

    void SetOrbit(NLWTrace::TOrbit& orbit) {
        Orbit = &orbit;
        DoSetOrbit(orbit);
    }

    // the name in the counters stays as constructed, so a suffix never multiplies them
    void AddNameSuffix(const TString& suffix) {
        StageName = StageName + suffix;
    }

    ITxReader(const TString& stageName)
        : StageName(stageName)
        , PrechargeCounters(NColumnShard::TLoadTimeSignals::TSignalsRegistry::GetSignal("PRECHARGE:" + stageName))
        , ReaderCounters(NColumnShard::TLoadTimeSignals::TSignalsRegistry::GetSignal("EXECUTE:" + stageName))
    {
        AFL_VERIFY(StageName);
    }

    bool GetIsStarted() const {
        return IsStarted;
    }

    bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx);
};

}   // namespace NKikimr

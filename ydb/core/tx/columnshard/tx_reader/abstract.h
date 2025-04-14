#pragma once
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/columnshard/counters/common_data.h>

#include <ydb/library/actors/core/actor.h>

namespace NKikimr {

class ITxReader {
private:
    const bool IsAsync;
    YDB_READONLY_DEF(TString, StageName);
    bool IsStarted = false;
    bool IsFinishedItself = false;
    std::shared_ptr<ITxReader> NextReader;
    NColumnShard::TLoadTimeSignals PrechargeCounters;
    NColumnShard::TLoadTimeSignals ReaderCounters;
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) = 0;
    virtual bool DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) = 0;

    virtual std::shared_ptr<ITxReader> BuildNextReader() {
        return nullptr;
    }

public:
    virtual ~ITxReader() = default;
    void AddNamePrefix(const TString& prefix) {
        StageName = prefix + StageName;
    }

    ITxReader(const TString& stageName, const bool isAsync = false)
        : IsAsync(isAsync)
        , StageName(stageName)
        , PrechargeCounters(NColumnShard::TLoadTimeSignals::TSignalsRegistry::GetSignal("PRECHARGE:" + stageName))
        , ReaderCounters(NColumnShard::TLoadTimeSignals::TSignalsRegistry::GetSignal("EXECUTE:" + stageName)) {
        AFL_VERIFY(StageName);
    }

    bool GetIsAsync() const {
        return IsAsync;
    }

    bool GetIsStarted() const {
        return IsStarted;
    }

    virtual bool GetIsFinished() const {
        return IsFinishedItself && (!NextReader || NextReader->GetIsFinished());
    }

    virtual bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx);
};

}   // namespace NKikimr

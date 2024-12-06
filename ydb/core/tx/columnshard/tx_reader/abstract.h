#pragma once
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/columnshard/counters/common_data.h>

#include <ydb/library/actors/core/actor.h>

namespace NKikimr {

class ITxReader {
private:
    YDB_READONLY_DEF(TString, StageName);
    bool IsReady = false;
    bool IsStarted = false;
    std::shared_ptr<ITxReader> NextReaderAfterLoad;
    NColumnShard::TLoadTimeSignals PrechargeCounters;
    NColumnShard::TLoadTimeSignals ReaderCounters;
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) = 0;
    virtual bool DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) = 0;

    virtual std::shared_ptr<ITxReader> BuildNextReaderAfterLoad() {
        return nullptr;
    }

public:
    virtual ~ITxReader() = default;
    void AddNamePrefix(const TString& prefix) {
        StageName = prefix + StageName;
    }

    ITxReader(const TString& stageName)
        : StageName(stageName)
        , PrechargeCounters(NColumnShard::TLoadTimeSignals::TSignalsRegistry::GetSignal("PRECHARGE:" + stageName))
        , ReaderCounters(NColumnShard::TLoadTimeSignals::TSignalsRegistry::GetSignal("EXECUTE:" + stageName)) {
        AFL_VERIFY(StageName);
    }

    bool GetIsStarted() const {
        return IsStarted;
    }

    bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx);
};

}   // namespace NKikimr

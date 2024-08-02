#pragma once
#include "common/owner.h"

#include <ydb/core/tx/columnshard/counters/tablet_counters.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/hash_set.h>

namespace NKikimr::NColumnShard {

enum class EWriteFailReason {
    Disabled /* "disabled" */,
    PutBlob /* "put_blob" */,
    LongTxDuplication /* "long_tx_duplication" */,
    NoTable /* "no_table" */,
    IncorrectSchema /* "incorrect_schema" */,
    Overload /* "overload" */
};

class TCSInitialization: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;

    const NMonitoring::THistogramPtr HistogramTabletInitializationMs;
    const NMonitoring::THistogramPtr HistogramTxInitDurationMs;
    const NMonitoring::THistogramPtr HistogramTxUpdateSchemaDurationMs;
    const NMonitoring::THistogramPtr HistogramTxInitSchemaDurationMs;
    const NMonitoring::THistogramPtr HistogramActivateExecutorFromActivationDurationMs;
    const NMonitoring::THistogramPtr HistogramSwitchToWorkFromActivationDurationMs;
    const NMonitoring::THistogramPtr HistogramSwitchToWorkFromCreateDurationMs;

public:
    void OnTxInitFinished(const TDuration d) const {
        HistogramTxInitDurationMs->Collect(d.MilliSeconds());
    }

    void OnTxUpdateSchemaFinished(const TDuration d) const {
        HistogramTxUpdateSchemaDurationMs->Collect(d.MilliSeconds());
    }

    void OnTxInitSchemaFinished(const TDuration d) const {
        HistogramTxInitSchemaDurationMs->Collect(d.MilliSeconds());
    }

    void OnActivateExecutor(const TDuration fromCreate) const {
        HistogramActivateExecutorFromActivationDurationMs->Collect(fromCreate.MilliSeconds());
    }
    void OnSwitchToWork(const TDuration fromStart, const TDuration fromCreate) const {
        HistogramSwitchToWorkFromActivationDurationMs->Collect(fromStart.MilliSeconds());
        HistogramSwitchToWorkFromCreateDurationMs->Collect(fromCreate.MilliSeconds());
    }

    TCSInitialization(TCommonCountersOwner& owner)
        : TBase(owner, "stage", "initialization")
        , HistogramTabletInitializationMs(TBase::GetHistogram("TabletInitializationMs", NMonitoring::ExponentialHistogram(15, 2, 32)))
        , HistogramTxInitDurationMs(TBase::GetHistogram("TxInitDurationMs", NMonitoring::ExponentialHistogram(15, 2, 32)))
        , HistogramTxUpdateSchemaDurationMs(TBase::GetHistogram("TxInitDurationMs", NMonitoring::ExponentialHistogram(15, 2, 32)))
        , HistogramTxInitSchemaDurationMs(TBase::GetHistogram("TxInitSchemaDurationMs", NMonitoring::ExponentialHistogram(15, 2, 32)))
        , HistogramActivateExecutorFromActivationDurationMs(
              TBase::GetHistogram("ActivateExecutorFromActivationDurationMs", NMonitoring::ExponentialHistogram(15, 2, 32)))
        , HistogramSwitchToWorkFromActivationDurationMs(
              TBase::GetHistogram("SwitchToWorkFromActivationDurationMs", NMonitoring::ExponentialHistogram(15, 2, 32)))
        , HistogramSwitchToWorkFromCreateDurationMs(
              TBase::GetHistogram("SwitchToWorkFromCreateDurationMs", NMonitoring::ExponentialHistogram(15, 2, 32))) {
    }
};

class TTxProgressCounters: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    using TOpType = TString;

    class TProgressCounters: public TCommonCountersOwner {
    private:
        using TBase = TCommonCountersOwner;

    public:
        NMonitoring::TDynamicCounters::TCounterPtr RegisterTx;
        NMonitoring::TDynamicCounters::TCounterPtr RegisterTxWithDeadline;
        NMonitoring::TDynamicCounters::TCounterPtr StartProposeOnExecute;
        NMonitoring::TDynamicCounters::TCounterPtr StartProposeOnComplete;
        NMonitoring::TDynamicCounters::TCounterPtr FinishProposeOnExecute;
        NMonitoring::TDynamicCounters::TCounterPtr FinishProposeOnComplete;
        NMonitoring::TDynamicCounters::TCounterPtr FinishPlannedTx;
        NMonitoring::TDynamicCounters::TCounterPtr AbortTx;

        TProgressCounters(TBase& owner)
            : TBase(owner)
            , RegisterTx(owner.GetDeriviative("RegisterTx"))
            , RegisterTxWithDeadline(owner.GetDeriviative("RegisterTxWithDeadline"))
            , StartProposeOnExecute(owner.GetDeriviative("StartProposeOnExecute"))
            , StartProposeOnComplete(owner.GetDeriviative("StartProposeOnComplete"))
            , FinishProposeOnExecute(owner.GetDeriviative("FinishProposeOnExecute"))
            , FinishProposeOnComplete(owner.GetDeriviative("FinishProposeOnComplete"))
            , FinishPlannedTx(owner.GetDeriviative("FinishPlannedTx"))
            , AbortTx(owner.GetDeriviative("AbortTx")) {
        }
    };

    THashMap<TOpType, TProgressCounters> SubGroups;

public:
    void OnRegisterTx(const TOpType& opType) {
        GetSubGroup(opType).RegisterTx->Add(1);
    }

    void OnRegisterTxWithDeadline(const TOpType& opType) {
        GetSubGroup(opType).RegisterTxWithDeadline->Add(1);
    }

    void OnStartProposeOnExecute(const TOpType& opType) {
        GetSubGroup(opType).StartProposeOnExecute->Add(1);
    }

    void OnStartProposeOnComplete(const TOpType& opType) {
        GetSubGroup(opType).StartProposeOnComplete->Add(1);
    }

    void OnFinishProposeOnExecute(const TOpType& opType) {
        GetSubGroup(opType).FinishProposeOnExecute->Add(1);
    }

    void OnFinishProposeOnComplete(const TOpType& opType) {
        GetSubGroup(opType).FinishProposeOnComplete->Add(1);
    }

    void OnFinishPlannedTx(const TOpType& opType) {
        GetSubGroup(opType).FinishPlannedTx->Add(1);
    }

    void OnAbortTx(const TOpType& opType) {
        GetSubGroup(opType).AbortTx->Add(1);
    }

    TTxProgressCounters(TCommonCountersOwner& owner)
        : TBase(owner, "txProgress") {  // TODO: fix parameter name?
    }

private:
    TProgressCounters& GetSubGroup(const TOpType& opType) {
        return SubGroups.try_emplace(opType, *this).first->second;
    }
};

class TCSCounters: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;

    std::shared_ptr<const TTabletCountersHandle> TabletCounters;

    NMonitoring::TDynamicCounters::TCounterPtr StartBackgroundCount;
    NMonitoring::TDynamicCounters::TCounterPtr TooEarlyBackgroundCount;
    NMonitoring::TDynamicCounters::TCounterPtr SetupCompactionCount;
    NMonitoring::TDynamicCounters::TCounterPtr SetupIndexationCount;
    NMonitoring::TDynamicCounters::TCounterPtr SetupTtlCount;
    NMonitoring::TDynamicCounters::TCounterPtr SetupCleanupCount;

    NMonitoring::TDynamicCounters::TCounterPtr SkipIndexationInputDueToGranuleOverloadBytes;
    NMonitoring::TDynamicCounters::TCounterPtr SkipIndexationInputDueToGranuleOverloadCount;
    NMonitoring::TDynamicCounters::TCounterPtr SkipIndexationInputDueToSplitCompactionBytes;
    NMonitoring::TDynamicCounters::TCounterPtr SkipIndexationInputDueToSplitCompactionCount;
    NMonitoring::TDynamicCounters::TCounterPtr FutureIndexationInputBytes;
    NMonitoring::TDynamicCounters::TCounterPtr IndexationInputBytes;

    NMonitoring::TDynamicCounters::TCounterPtr IndexMetadataLimitBytes;

    NMonitoring::TDynamicCounters::TCounterPtr OverloadInsertTableBytes;
    NMonitoring::TDynamicCounters::TCounterPtr OverloadInsertTableCount;
    NMonitoring::TDynamicCounters::TCounterPtr OverloadMetadataBytes;
    NMonitoring::TDynamicCounters::TCounterPtr OverloadMetadataCount;
    NMonitoring::TDynamicCounters::TCounterPtr OverloadShardTxBytes;
    NMonitoring::TDynamicCounters::TCounterPtr OverloadShardTxCount;
    NMonitoring::TDynamicCounters::TCounterPtr OverloadShardWritesBytes;
    NMonitoring::TDynamicCounters::TCounterPtr OverloadShardWritesCount;
    NMonitoring::TDynamicCounters::TCounterPtr OverloadShardWritesSizeBytes;
    NMonitoring::TDynamicCounters::TCounterPtr OverloadShardWritesSizeCount;

    std::shared_ptr<TValueAggregationClient> InternalCompactionGranuleBytes;
    std::shared_ptr<TValueAggregationClient> InternalCompactionGranulePortionsCount;

    std::shared_ptr<TValueAggregationClient> SplitCompactionGranuleBytes;
    std::shared_ptr<TValueAggregationClient> SplitCompactionGranulePortionsCount;

    NMonitoring::THistogramPtr HistogramSuccessWritePutBlobsDurationMs;
    NMonitoring::THistogramPtr HistogramSuccessWriteMiddle1PutBlobsDurationMs;
    NMonitoring::THistogramPtr HistogramSuccessWriteMiddle2PutBlobsDurationMs;
    NMonitoring::THistogramPtr HistogramSuccessWriteMiddle3PutBlobsDurationMs;
    NMonitoring::THistogramPtr HistogramSuccessWriteMiddle4PutBlobsDurationMs;
    NMonitoring::THistogramPtr HistogramSuccessWriteMiddle5PutBlobsDurationMs;
    NMonitoring::THistogramPtr HistogramSuccessWriteMiddle6PutBlobsDurationMs;
    NMonitoring::THistogramPtr HistogramFailedWritePutBlobsDurationMs;
    NMonitoring::THistogramPtr HistogramWriteTxCompleteDurationMs;

    NMonitoring::TDynamicCounters::TCounterPtr WritePutBlobsCount;
    NMonitoring::TDynamicCounters::TCounterPtr WriteRequests;
    THashMap<EWriteFailReason, NMonitoring::TDynamicCounters::TCounterPtr> FailedWriteRequests;
    NMonitoring::TDynamicCounters::TCounterPtr SuccessWriteRequests;

public:
    const TCSInitialization Initialization;
    TTxProgressCounters TxProgress;

    void OnStartWriteRequest() const {
        WriteRequests->Add(1);
    }

    void OnFailedWriteResponse(const EWriteFailReason reason) const;

    void OnSuccessWriteResponse() const {
        WriteRequests->Sub(1);
        SuccessWriteRequests->Add(1);
    }

    void OnWritePutBlobsSuccess(const TDuration d) const {
        HistogramSuccessWritePutBlobsDurationMs->Collect(d.MilliSeconds());
        WritePutBlobsCount->Sub(1);
    }

    void OnWriteMiddle1PutBlobsSuccess(const TDuration d) const {
        HistogramSuccessWriteMiddle1PutBlobsDurationMs->Collect(d.MilliSeconds());
    }

    void OnWriteMiddle2PutBlobsSuccess(const TDuration d) const {
        HistogramSuccessWriteMiddle2PutBlobsDurationMs->Collect(d.MilliSeconds());
    }

    void OnWriteMiddle3PutBlobsSuccess(const TDuration d) const {
        HistogramSuccessWriteMiddle3PutBlobsDurationMs->Collect(d.MilliSeconds());
    }

    void OnWriteMiddle4PutBlobsSuccess(const TDuration d) const {
        HistogramSuccessWriteMiddle4PutBlobsDurationMs->Collect(d.MilliSeconds());
    }

    void OnWriteMiddle5PutBlobsSuccess(const TDuration d) const {
        HistogramSuccessWriteMiddle5PutBlobsDurationMs->Collect(d.MilliSeconds());
    }

    void OnWriteMiddle6PutBlobsSuccess(const TDuration d) const {
        HistogramSuccessWriteMiddle6PutBlobsDurationMs->Collect(d.MilliSeconds());
    }

    void OnWritePutBlobsFail(const TDuration d) const {
        HistogramFailedWritePutBlobsDurationMs->Collect(d.MilliSeconds());
        WritePutBlobsCount->Sub(1);
    }

    void OnWritePutBlobsStart() const {
        WritePutBlobsCount->Add(1);
    }

    void OnWriteTxComplete(const TDuration d) const {
        HistogramWriteTxCompleteDurationMs->Collect(d.MilliSeconds());
    }

    void OnInternalCompactionInfo(const ui64 bytes, const ui32 portionsCount) const {
        InternalCompactionGranuleBytes->SetValue(bytes);
        InternalCompactionGranulePortionsCount->SetValue(portionsCount);
    }

    void OnSplitCompactionInfo(const ui64 bytes, const ui32 portionsCount) const {
        SplitCompactionGranuleBytes->SetValue(bytes);
        SplitCompactionGranulePortionsCount->SetValue(portionsCount);
    }

    void OnWriteOverloadDisk() const {
        TabletCounters->IncCounter(COUNTER_OUT_OF_SPACE);
    }

    void OnWriteOverloadInsertTable(const ui64 size) const {
        TabletCounters->IncCounter(COUNTER_WRITE_OVERLOAD);
        OverloadInsertTableBytes->Add(size);
        OverloadInsertTableCount->Add(1);
    }

    void OnWriteOverloadMetadata(const ui64 size) const {
        TabletCounters->IncCounter(COUNTER_WRITE_OVERLOAD);
        OverloadMetadataBytes->Add(size);
        OverloadMetadataCount->Add(1);
    }

    void OnWriteOverloadShardTx(const ui64 size) const {
        TabletCounters->IncCounter(COUNTER_WRITE_OVERLOAD);
        OverloadShardTxBytes->Add(size);
        OverloadShardTxCount->Add(1);
    }

    void OnWriteOverloadShardWrites(const ui64 size) const {
        TabletCounters->IncCounter(COUNTER_WRITE_OVERLOAD);
        OverloadShardWritesBytes->Add(size);
        OverloadShardWritesCount->Add(1);
    }

    void OnWriteOverloadShardWritesSize(const ui64 size) const {
        TabletCounters->IncCounter(COUNTER_WRITE_OVERLOAD);
        OverloadShardWritesSizeBytes->Add(size);
        OverloadShardWritesSizeCount->Add(1);
    }

    void SkipIndexationInputDueToSplitCompaction(const ui64 size) const {
        SkipIndexationInputDueToSplitCompactionBytes->Add(size);
        SkipIndexationInputDueToSplitCompactionCount->Add(1);
    }

    void SkipIndexationInputDueToGranuleOverload(const ui64 size) const {
        SkipIndexationInputDueToGranuleOverloadBytes->Add(size);
        SkipIndexationInputDueToGranuleOverloadCount->Add(1);
    }

    void FutureIndexationInput(const ui64 size) const {
        FutureIndexationInputBytes->Add(size);
    }

    void IndexationInput(const ui64 size) const {
        IndexationInputBytes->Add(size);
    }

    void OnIndexMetadataLimit(const ui64 limit) const {
        IndexMetadataLimitBytes->Set(limit);
    }

    void OnStartBackground() const {
        StartBackgroundCount->Add(1);
    }

    void OnTooEarly() const {
        TooEarlyBackgroundCount->Add(1);
    }

    void OnSetupCompaction() const {
        SetupCompactionCount->Add(1);
    }

    void OnSetupIndexation() const {
        SetupIndexationCount->Add(1);
    }

    void OnSetupTtl() const {
        SetupTtlCount->Add(1);
    }

    void OnSetupCleanup() const {
        SetupCleanupCount->Add(1);
    }

    TCSCounters(std::shared_ptr<const TTabletCountersHandle> tabletCounters);
};

}

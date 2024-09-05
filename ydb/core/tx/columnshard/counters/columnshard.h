#pragma once
#include "common/owner.h"
#include "initialization.h"
#include "tx_progress.h"

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
    Overload /* "overload" */,
    OverlimitReadRawMemory /* "overlimit_read_raw_memory" */,
    OverlimitReadBlobMemory /* "overlimit_read_blob_memory" */
};

class TCSCounters: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;

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

    void OnWriteOverloadInsertTable(const ui64 size) const {
        OverloadInsertTableBytes->Add(size);
        OverloadInsertTableCount->Add(1);
    }

    void OnWriteOverloadMetadata(const ui64 size) const {
        OverloadMetadataBytes->Add(size);
        OverloadMetadataCount->Add(1);
    }

    void OnWriteOverloadShardTx(const ui64 size) const {
        OverloadShardTxBytes->Add(size);
        OverloadShardTxCount->Add(1);
    }

    void OnWriteOverloadShardWrites(const ui64 size) const {
        OverloadShardWritesBytes->Add(size);
        OverloadShardWritesCount->Add(1);
    }

    void OnWriteOverloadShardWritesSize(const ui64 size) const {
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

    TCSCounters();
};

}

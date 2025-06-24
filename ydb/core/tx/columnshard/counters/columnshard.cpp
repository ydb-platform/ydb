#include "columnshard.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NColumnShard {

TCSCounters::TCSCounters()
    : TBase("CS")
    , WritingCounters(std::make_shared<TWriteCounters>(*this))
    , Initialization(*this)
    , TxProgress(*this) {
    for (auto&& i : GetEnumAllValues<EOverloadStatus>()) {
        AFL_VERIFY((ui32)i == WaitingOverloads.size());
        auto overloadCounters = CreateSubGroup("overload_type", ::ToString(i));
        WaitingOverloads.emplace_back(overloadCounters.GetDeriviative("Overload/Waiting/Count"));
        WriteOverloadCount.emplace_back(overloadCounters.GetDeriviative("Overload/Write/Count"));
        WriteOverloadBytes.emplace_back(overloadCounters.GetDeriviative("Overload/Write/Bytes"));
    }
    StartBackgroundCount = TBase::GetDeriviative("StartBackground/Count");
    TooEarlyBackgroundCount = TBase::GetDeriviative("TooEarlyBackground/Count");
    SetupCompactionCount = TBase::GetDeriviative("SetupCompaction/Count");
    SetupIndexationCount = TBase::GetDeriviative("SetupIndexation/Count");
    SetupTtlCount = TBase::GetDeriviative("SetupTtl/Count");
    SetupCleanupCount = TBase::GetDeriviative("SetupCleanup/Count");

    SkipIndexationInputDueToGranuleOverloadBytes = TBase::GetDeriviative("SkipIndexationInput/GranuleOverload/Bytes");
    SkipIndexationInputDueToGranuleOverloadCount = TBase::GetDeriviative("SkipIndexationInput/GranuleOverload/Count");
    SkipIndexationInputDueToSplitCompactionBytes = TBase::GetDeriviative("SkipIndexationInput/SplitCompaction/Bytes");
    SkipIndexationInputDueToSplitCompactionCount = TBase::GetDeriviative("SkipIndexationInput/SplitCompaction/Count");

    FutureIndexationInputBytes = TBase::GetDeriviative("FutureIndexationInput/Bytes");
    IndexationInputBytes = TBase::GetDeriviative("IndexationInput/Bytes");

    IndexMetadataLimitBytes = TBase::GetValue("IndexMetadata/Limit/Bytes");

    OverloadMetadataBytes = TBase::GetDeriviative("Overload/Metadata/Bytes");
    OverloadMetadataCount = TBase::GetDeriviative("Overload/Metadata/Count");
    OverloadCompactionBytes = TBase::GetDeriviative("Overload/Compaction/Bytes");
    OverloadCompactionCount = TBase::GetDeriviative("Overload/Compaction/Count");
    OverloadShardTxBytes = TBase::GetDeriviative("Overload/Shard/Tx/Bytes");
    OverloadShardTxCount = TBase::GetDeriviative("Overload/Shard/Tx/Count");
    OverloadShardWritesBytes = TBase::GetDeriviative("Overload/Shard/Writes/Bytes");
    OverloadShardWritesCount = TBase::GetDeriviative("Overload/Shard/Writes/Count");
    OverloadShardWritesSizeBytes = TBase::GetDeriviative("Overload/Shard/WritesSize/Bytes");
    OverloadShardWritesSizeCount = TBase::GetDeriviative("Overload/Shard/WritesSize/Count");

    InternalCompactionGranuleBytes = TBase::GetValueAutoAggregationsClient("InternalCompaction/Bytes");
    InternalCompactionGranulePortionsCount = TBase::GetValueAutoAggregationsClient("InternalCompaction/PortionsCount");

    SplitCompactionGranuleBytes = TBase::GetValueAutoAggregationsClient("SplitCompaction/Bytes");
    SplitCompactionGranulePortionsCount = TBase::GetValueAutoAggregationsClient("SplitCompaction/PortionsCount");

    HistogramSuccessWritePutBlobsDurationMs = TBase::GetHistogram("SuccessWritePutBlobsDurationMs", NMonitoring::ExponentialHistogram(18, 2, 5));
    HistogramSuccessWriteMiddle1PutBlobsDurationMs = TBase::GetHistogram("SuccessWriteMiddle1PutBlobsDurationMs", NMonitoring::ExponentialHistogram(18, 2, 5));
    HistogramSuccessWriteMiddle2PutBlobsDurationMs = TBase::GetHistogram("SuccessWriteMiddle2PutBlobsDurationMs", NMonitoring::ExponentialHistogram(18, 2, 5));
    HistogramSuccessWriteMiddle3PutBlobsDurationMs = TBase::GetHistogram("SuccessWriteMiddle3PutBlobsDurationMs", NMonitoring::ExponentialHistogram(18, 2, 5));
    HistogramSuccessWriteMiddle4PutBlobsDurationMs = TBase::GetHistogram("SuccessWriteMiddle4PutBlobsDurationMs", NMonitoring::ExponentialHistogram(18, 2, 5));
    HistogramSuccessWriteMiddle5PutBlobsDurationMs = TBase::GetHistogram("SuccessWriteMiddle5PutBlobsDurationMs", NMonitoring::ExponentialHistogram(18, 2, 5));
    HistogramSuccessWriteMiddle6PutBlobsDurationMs = TBase::GetHistogram("SuccessWriteMiddle6PutBlobsDurationMs", NMonitoring::ExponentialHistogram(18, 2, 5));
    HistogramFailedWritePutBlobsDurationMs = TBase::GetHistogram("FailedWritePutBlobsDurationMs", NMonitoring::ExponentialHistogram(18, 2, 5));
    HistogramWriteTxCompleteDurationMs = TBase::GetHistogram("WriteTxCompleteDurationMs", NMonitoring::ExponentialHistogram(18, 2, 5));

    WritePutBlobsCount = TBase::GetValue("WritePutBlobs");
    WriteRequests = TBase::GetValue("WriteRequests");

    for (auto&& i : GetEnumAllValues<EWriteFailReason>()) {
        auto sub = CreateSubGroup("reason", ::ToString(i));
        FailedWriteRequests.emplace(i, sub.GetDeriviative("FailedWriteRequests"));
    }

    SuccessWriteRequests = TBase::GetDeriviative("SuccessWriteRequests");
}

void TCSCounters::OnFailedWriteResponse(const EWriteFailReason reason) const {
    WriteRequests->Sub(1);
    auto it = FailedWriteRequests.find(reason);
    AFL_VERIFY(it != FailedWriteRequests.end());
    it->second->Add(1);
}

void TCSCounters::OnWaitingOverload(const EOverloadStatus status) const {
    AFL_VERIFY((ui64)status < WaitingOverloads.size());
    WaitingOverloads[(ui64)status]->Inc();
}

void TCSCounters::OnWriteOverload(const EOverloadStatus status, const ui32 size) const {
    AFL_VERIFY((ui64)status < WriteOverloadCount.size());
    WriteOverloadCount[(ui64)status]->Inc();
    WriteOverloadBytes[(ui64)status]->Add(size);
}

}

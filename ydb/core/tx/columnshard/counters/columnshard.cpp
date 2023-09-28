#include "columnshard.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>

namespace NKikimr::NColumnShard {

TCSCounters::TCSCounters()
    : TBase("CS")
{
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

    OverloadInsertTableBytes = TBase::GetDeriviative("OverloadInsertTable/Bytes");
    OverloadInsertTableCount = TBase::GetDeriviative("OverloadInsertTable/Count");
    OverloadShardBytes = TBase::GetDeriviative("OverloadShard/Bytes");
    OverloadShardCount = TBase::GetDeriviative("OverloadShard/Count");

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
    HistogramFailedWritePutBlobsDurationMs = TBase::GetHistogram("FailedWritePutBlobsDurationMs", NMonitoring::ExponentialHistogram(18, 2, 5));
    HistogramWriteTxCompleteDurationMs = TBase::GetHistogram("WriteTxCompleteDurationMs", NMonitoring::ExponentialHistogram(18, 2, 5));
    WritePutBlobsCount = TBase::GetValue("WritePutBlobs");
    WriteRequests = TBase::GetValue("WriteRequests");
    FailedWriteRequests = TBase::GetDeriviative("FailedWriteRequests");
    SuccessWriteRequests = TBase::GetDeriviative("SuccessWriteRequests");
}

}

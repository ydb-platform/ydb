#include "write.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>

namespace NKikimr::NColumnShard {

TWriteCounters::TWriteCounters()
    : TBase("Write")
{
    ReadBytes = TBase::GetDeriviative("Read/Bytes");
    ReadErrors = TBase::GetDeriviative("Read/Errors/Count");
    AnalizeInsertedPortions = TBase::GetDeriviative("AnalizeInsertion/Portions");
    AnalizeInsertedBytes = TBase::GetDeriviative("AnalizeInsertion/Bytes");
    RepackedInsertedPortions = TBase::GetDeriviative("RepackedInsertion/Portions");
    RepackedInsertedPortionBytes = TBase::GetDeriviative("RepackedInsertion/Bytes");

    AnalizeCompactedPortions = TBase::GetDeriviative("AnalizeCompaction/Portions");
    AnalizeCompactedBytes = TBase::GetDeriviative("AnalizeCompaction/Bytes");
    SkipPortionsMoveThroughIntersection = TBase::GetDeriviative("SkipMoveThroughIntersection/Portions");
    SkipPortionBytesMoveThroughIntersection = TBase::GetDeriviative("SkipMoveThroughIntersection/Bytes");
    RepackedCompactedPortions = TBase::GetDeriviative("RepackedCompaction/Portions");
    MovedPortions = TBase::GetDeriviative("Moved/Portions");
    MovedPortionBytes = TBase::GetDeriviative("Moved/Bytes");

    CompactionDuration = TBase::GetHistogram("CompactionDuration", NMonitoring::ExponentialHistogram(18, 2, 20));
    HistogramCompactionInputBytes = TBase::GetHistogram("CompactionInput/Bytes", NMonitoring::ExponentialHistogram(18, 2, 1024));
    CompactionInputBytes = TBase::GetDeriviative("CompactionInput/Bytes");
    CompactionExceptions = TBase::GetDeriviative("Exceptions/Count");
    CompactionFails = TBase::GetDeriviative("CompactionFails/Count");

    SplittedPortionLargestColumnSize = TBase::GetHistogram("SplittedPortionLargestColumnSize", NMonitoring::ExponentialHistogram(15, 2, 1024));
    SplittedPortionColumnSize = TBase::GetHistogram("SplittedPortionColumnSize", NMonitoring::ExponentialHistogram(15, 2, 1024));
    SimpleSplitPortionLargestColumnSize = TBase::GetHistogram("SimpleSplitPortionLargestColumnSize", NMonitoring::ExponentialHistogram(15, 2, 1024));
    TooSmallBlob = TBase::GetDeriviative("TooSmallBlob/Count");
    TooSmallBlobFinish = TBase::GetDeriviative("TooSmallBlobFinish/Count");
    TooSmallBlobStart = TBase::GetDeriviative("TooSmallBlobStart/Count");

    SplitterCounters = std::make_shared<TSplitterCounters>(*this);
}

}

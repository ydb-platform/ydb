#pragma once
#include <ydb/core/tx/columnshard/counters/common/owner.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/hash.h>

namespace NKikimr::NOlap::NChanges {

class TGeneralCompactionCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr FullBlobsAppendCount;
    NMonitoring::TDynamicCounters::TCounterPtr FullBlobsAppendBytes;
    NMonitoring::TDynamicCounters::TCounterPtr SplittedBlobsAppendCount;
    NMonitoring::TDynamicCounters::TCounterPtr SplittedBlobsAppendBytes;
    NMonitoring::TDynamicCounters::TCounterPtr RepackPortionsCount;
    NMonitoring::TDynamicCounters::TCounterPtr RepackPortionsBytes;
    NMonitoring::TDynamicCounters::TCounterPtr RepackInsertedPortionsBytes;
    NMonitoring::TDynamicCounters::TCounterPtr RepackCompactedPortionsBytes;
    NMonitoring::TDynamicCounters::TCounterPtr RepackOtherPortionsBytes;
    NMonitoring::THistogramPtr HistogramRepackPortionsBytes;
    NMonitoring::THistogramPtr HistogramRepackPortionsCount;
public:
    TGeneralCompactionCounters()
        : TBase("GeneralCompaction")
    {
        FullBlobsAppendCount = TBase::GetDeriviative("FullBlobsAppend/Count");
        FullBlobsAppendBytes = TBase::GetDeriviative("FullBlobsAppend/Bytes");
        SplittedBlobsAppendCount = TBase::GetDeriviative("SplittedBlobsAppend/Count");
        SplittedBlobsAppendBytes = TBase::GetDeriviative("SplittedBlobsAppend/Bytes");
        RepackPortionsCount = TBase::GetDeriviative("RepackPortions/Count");
        RepackPortionsBytes = TBase::GetDeriviative("RepackPortions/Bytes");
        HistogramRepackPortionsBytes = TBase::GetHistogram("RepackPortions/Bytes", NMonitoring::ExponentialHistogram(18, 2, 256 * 1024));
        HistogramRepackPortionsCount = TBase::GetHistogram("RepackPortions/Count", NMonitoring::ExponentialHistogram(15, 2, 4));

        RepackInsertedPortionsBytes = TBase::GetDeriviative("RepackInsertedPortions/Bytes");
        RepackCompactedPortionsBytes = TBase::GetDeriviative("RepackCompactedPortions/Bytes");
        RepackOtherPortionsBytes = TBase::GetDeriviative("RepackOtherPortions/Bytes");
    }

    static void OnRepackPortions(const i64 portionsCount, const i64 portionBytes) {
        Singleton<TGeneralCompactionCounters>()->RepackPortionsCount->Add(portionsCount);
        Singleton<TGeneralCompactionCounters>()->RepackPortionsBytes->Add(portionBytes);
        Singleton<TGeneralCompactionCounters>()->HistogramRepackPortionsCount->Collect(portionsCount);
        Singleton<TGeneralCompactionCounters>()->HistogramRepackPortionsBytes->Collect(portionBytes);
    }

    static void OnPortionsKind(const i64 insertedBytes, const i64 compactedBytes, const i64 otherBytes) {
        Singleton<TGeneralCompactionCounters>()->RepackInsertedPortionsBytes->Add(insertedBytes);
        Singleton<TGeneralCompactionCounters>()->RepackCompactedPortionsBytes->Add(compactedBytes);
        Singleton<TGeneralCompactionCounters>()->RepackOtherPortionsBytes->Add(otherBytes);
    }

    static void OnSplittedBlobAppend(const i64 bytes) {
        Singleton<TGeneralCompactionCounters>()->SplittedBlobsAppendCount->Add(1);
        Singleton<TGeneralCompactionCounters>()->SplittedBlobsAppendBytes->Add(bytes);
    }

    static void OnFullBlobAppend(const i64 bytes) {
        Singleton<TGeneralCompactionCounters>()->FullBlobsAppendCount->Add(1);
        Singleton<TGeneralCompactionCounters>()->FullBlobsAppendBytes->Add(bytes);
    }
};

}

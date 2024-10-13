#pragma once
#include <ydb/core/tx/columnshard/counters/common/owner.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/hash.h>

namespace NKikimr::NOlap::NChanges {

class TPortionKindCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr PortionsCount;
    NMonitoring::TDynamicCounters::TCounterPtr PortionsRawBytes;
    NMonitoring::TDynamicCounters::TCounterPtr PortionsBlobBytes;

public:
    TPortionKindCounters(const TString& kind, NColumnShard::TCommonCountersOwner& baseOwner)
        : TBase(baseOwner, "kind", kind) {
        PortionsCount = TBase::GetDeriviative("RepackPortions/Count");
        PortionsRawBytes = TBase::GetDeriviative("RepackPortions/Raw/Bytes");
        PortionsBlobBytes = TBase::GetDeriviative("RepackPortions/Blob/Bytes");
    }

    void OnData(const i64 portionsCount, const i64 portionBlobBytes, const i64 portionRawBytes) {
        PortionsCount->Add(portionsCount);
        PortionsRawBytes->Add(portionRawBytes);
        PortionsBlobBytes->Add(portionBlobBytes);
    }
};

class TGeneralCompactionCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr FullBlobsAppendCount;
    NMonitoring::TDynamicCounters::TCounterPtr FullBlobsAppendBytes;
    NMonitoring::TDynamicCounters::TCounterPtr SplittedBlobsAppendCount;
    NMonitoring::TDynamicCounters::TCounterPtr SplittedBlobsAppendBytes;

    TPortionKindCounters RepackPortions;
    TPortionKindCounters RepackInsertedPortions;
    TPortionKindCounters RepackCompactedPortions;
    NMonitoring::THistogramPtr HistogramRepackPortionsRawBytes;
    NMonitoring::THistogramPtr HistogramRepackPortionsBlobBytes;
    NMonitoring::THistogramPtr HistogramRepackPortionsCount;

public:
    TGeneralCompactionCounters()
        : TBase("GeneralCompaction")
        , RepackPortions("ALL", *this)
        , RepackInsertedPortions("INSERTED", *this)
        , RepackCompactedPortions("COMPACTED", *this)
    {
        FullBlobsAppendCount = TBase::GetDeriviative("FullBlobsAppend/Count");
        FullBlobsAppendBytes = TBase::GetDeriviative("FullBlobsAppend/Bytes");
        SplittedBlobsAppendCount = TBase::GetDeriviative("SplittedBlobsAppend/Count");
        SplittedBlobsAppendBytes = TBase::GetDeriviative("SplittedBlobsAppend/Bytes");
        HistogramRepackPortionsRawBytes = TBase::GetHistogram("RepackPortions/Raw/Bytes", NMonitoring::ExponentialHistogram(18, 2, 256 * 1024));
        HistogramRepackPortionsBlobBytes = TBase::GetHistogram("RepackPortions/Blob/Bytes", NMonitoring::ExponentialHistogram(18, 2, 256 * 1024));
        HistogramRepackPortionsCount = TBase::GetHistogram("RepackPortions/Count", NMonitoring::LinearHistogram(15, 10, 16));
    }

    static void OnRepackPortions(const i64 portionsCount, const i64 portionBlobBytes, const i64 portionRawBytes) {
        Singleton<TGeneralCompactionCounters>()->RepackPortions->OnData(portionsCount, portionBlobBytes, portionRawBytes);
        Singleton<TGeneralCompactionCounters>()->HistogramRepackPortionsCount->Collect(portionsCount);
        Singleton<TGeneralCompactionCounters>()->HistogramRepackPortionsBlobBytes->Collect(portionBlobBytes);
        Singleton<TGeneralCompactionCounters>()->HistogramRepackPortionsRawBytes->Collect(portionRawBytes);
    }

    static void OnRepackInsertedPortions(const i64 portionsCount, const i64 portionBlobBytes, const i64 portionRawBytes) {
        Singleton<TGeneralCompactionCounters>()->RepackInsertedPortions->OnData(portionsCount, portionBlobBytes, portionRawBytes);
    }

    static void OnRepackCompactedPortions(const i64 portionsCount, const i64 portionBlobBytes, const i64 portionRawBytes) {
        Singleton<TGeneralCompactionCounters>()->RepackCompactedPortions->OnData(portionsCount, portionBlobBytes, portionRawBytes);
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

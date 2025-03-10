#pragma once
#include <ydb/core/tx/columnshard/counters/common/owner.h>
#include <ydb/core/tx/columnshard/counters/portions.h>

#include <ydb/library/actors/core/log.h>

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

    TPortionGroupCounters RepackPortions;
    TPortionGroupCounters RepackInsertedPortions;
    TPortionGroupCounters RepackCompactedPortions;
    THashMap<ui32, TPortionGroupCounters> RepackPortionsFromLevel;
    THashMap<ui32, TPortionGroupCounters> RepackPortionsToLevel;
    THashMap<ui32, TPortionGroupCounters> MovePortionsFromLevel;
    THashMap<ui32, TPortionGroupCounters> MovePortionsToLevel;
    NMonitoring::THistogramPtr HistogramRepackPortionsRawBytes;
    NMonitoring::THistogramPtr HistogramRepackPortionsBlobBytes;
    NMonitoring::THistogramPtr HistogramRepackPortionsCount;

public:
    TGeneralCompactionCounters()
        : TBase("GeneralCompaction")
        , RepackPortions("ALL", CreateSubGroup("action", "repack"))
        , RepackInsertedPortions("INSERTED", CreateSubGroup("action", "repack"))
        , RepackCompactedPortions("COMPACTED", CreateSubGroup("action", "repack")) {
        for (ui32 i = 0; i < 10; ++i) {
            RepackPortionsFromLevel.emplace(
                i, TPortionGroupCounters("level=" + ::ToString(i), CreateSubGroup("action", "repack").CreateSubGroup("direction", "from")));
            RepackPortionsToLevel.emplace(
                i, TPortionGroupCounters("level=" + ::ToString(i), CreateSubGroup("action", "repack").CreateSubGroup("direction", "to")));
            MovePortionsFromLevel.emplace(
                i, TPortionGroupCounters("level=" + ::ToString(i), CreateSubGroup("action", "move").CreateSubGroup("direction", "from")));
            MovePortionsToLevel.emplace(
                i, TPortionGroupCounters("level=" + ::ToString(i), CreateSubGroup("action", "move").CreateSubGroup("direction", "to")));
        }
        FullBlobsAppendCount = TBase::GetDeriviative("FullBlobsAppend/Count");
        FullBlobsAppendBytes = TBase::GetDeriviative("FullBlobsAppend/Bytes");
        SplittedBlobsAppendCount = TBase::GetDeriviative("SplittedBlobsAppend/Count");
        SplittedBlobsAppendBytes = TBase::GetDeriviative("SplittedBlobsAppend/Bytes");
        HistogramRepackPortionsRawBytes = TBase::GetHistogram("RepackPortions/Raw/Bytes", NMonitoring::ExponentialHistogram(18, 2, 256 * 1024));
        HistogramRepackPortionsBlobBytes =
            TBase::GetHistogram("RepackPortions/Blob/Bytes", NMonitoring::ExponentialHistogram(18, 2, 256 * 1024));
        HistogramRepackPortionsCount = TBase::GetHistogram("RepackPortions/Count", NMonitoring::LinearHistogram(15, 10, 16));
    }

    static void OnRepackPortions(const TSimplePortionsGroupInfo& portions) {
        Singleton<TGeneralCompactionCounters>()->RepackPortions.OnData(portions);
        Singleton<TGeneralCompactionCounters>()->HistogramRepackPortionsCount->Collect(portions.GetCount());
        Singleton<TGeneralCompactionCounters>()->HistogramRepackPortionsBlobBytes->Collect(portions.GetBlobBytes());
        Singleton<TGeneralCompactionCounters>()->HistogramRepackPortionsRawBytes->Collect(portions.GetRawBytes());
    }

    static void OnRepackPortionsByLevel(const THashMap<ui32, TSimplePortionsGroupInfo>& portions, const ui32 targetLevelIdx) {
        for (auto&& i : portions) {
            auto& counters = (i.first == targetLevelIdx) ? Singleton<TGeneralCompactionCounters>()->RepackPortionsToLevel
                                                         : Singleton<TGeneralCompactionCounters>()->RepackPortionsFromLevel;
            auto it = counters.find(i.first);
            AFL_VERIFY(it != counters.end());
            it->second.OnData(i.second);
        }
    }

    static void OnMovePortionsByLevel(const THashMap<ui32, TSimplePortionsGroupInfo>& portions, const ui32 targetLevelIdx) {
        for (auto&& i : portions) {
            auto& counters = (i.first == targetLevelIdx) ? Singleton<TGeneralCompactionCounters>()->MovePortionsToLevel
                                                         : Singleton<TGeneralCompactionCounters>()->MovePortionsFromLevel;
            auto it = counters.find(i.first);
            AFL_VERIFY(it != counters.end());
            it->second.OnData(i.second);
        }
    }

    static void OnRepackInsertedPortions(const TSimplePortionsGroupInfo& portions) {
        Singleton<TGeneralCompactionCounters>()->RepackInsertedPortions.OnData(portions);
    }

    static void OnRepackCompactedPortions(const TSimplePortionsGroupInfo& portions) {
        Singleton<TGeneralCompactionCounters>()->RepackCompactedPortions.OnData(portions);
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

}   // namespace NKikimr::NOlap::NChanges

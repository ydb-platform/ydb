#pragma once
#include <ydb/library/signals/owner.h>
#include <ydb/core/tx/columnshard/counters/portions.h>
#include <ydb/core/tx/columnshard/counters/histogram_borders.h>

#include <ydb/library/actors/core/log.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/hash.h>

namespace NKikimr::NOlap::NChanges {

class TGeneralCompactionCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    TPortionGroupCounters RepackPortions;
    TPortionGroupCounters RepackInsertedPortions;
    TPortionGroupCounters RepackCompactedPortions;
    THashMap<ui32, TPortionGroupCounters> RepackPortionsFromLevel;
    THashMap<ui32, TPortionGroupCounters> RepackPortionsToLevel;
    THashMap<ui32, TPortionGroupCounters> MovePortionsFromLevel;
    THashMap<ui32, TPortionGroupCounters> MovePortionsToLevel;
    NColumnShard::TDeriviativeHistogram HistogramRepackPortionsRawBytes;
    NColumnShard::TDeriviativeHistogram HistogramRepackPortionsBlobBytes;
    NMonitoring::THistogramPtr HistogramRepackPortionsCount;
    NMonitoring::THistogramPtr HistogramRepackPortionsRows;
    NMonitoring::THistogramPtr HistogramBlobsWrittenCount;
    NColumnShard::TDeriviativeHistogram HistogramBlobsWrittenBytes;
    NColumnShard::TDeriviativeHistogram HistogramCompactionDuration;
    NColumnShard::TDeriviativeHistogram HistogramTaskGenerationDuration;
    NMonitoring::THistogramPtr HistogramTaskGenerationCount;

public:
    TGeneralCompactionCounters()
        : TBase("GeneralCompaction")
        , RepackPortions("ALL", CreateSubGroup("action", "repack"))
        , RepackInsertedPortions("INSERTED", CreateSubGroup("action", "repack"))
        , RepackCompactedPortions("COMPACTED", CreateSubGroup("action", "repack"))
        , HistogramRepackPortionsRawBytes("GeneralCompaction", "RepackPortions/Raw/Bytes", "", NColumnShard::THistorgamBorders::BytesBorders)
        , HistogramRepackPortionsBlobBytes("GeneralCompaction", "RepackPortions/Blob/Bytes", "", NColumnShard::THistorgamBorders::BytesBorders)
        , HistogramBlobsWrittenBytes("GeneralCompaction", "BlobsWritten/Bytes", "", NColumnShard::THistorgamBorders::BytesBorders)
        , HistogramCompactionDuration("GeneralCompaction", "Compaction/Duration", "", NColumnShard::THistorgamBorders::TimeBordersMicroseconds)
        , HistogramTaskGenerationDuration("GeneralCompaction", "TaskGeneration/Duration", "", NColumnShard::THistorgamBorders::TimeBordersMicroseconds) {
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
        HistogramRepackPortionsCount = TBase::GetHistogram("RepackPortions/Count", NMonitoring::ExponentialHistogram(15, 2));
        HistogramRepackPortionsRows = TBase::GetHistogram("RepackPortions/Rows", NMonitoring::ExponentialHistogram(25, 2));
        HistogramBlobsWrittenCount = TBase::GetHistogram("BlobsWritten/Count", NMonitoring::ExponentialHistogram(15, 2));
        HistogramTaskGenerationCount = TBase::GetHistogram("TaskGeneration/Count", NMonitoring::LinearHistogram(20, 0, 1));
    }

    static void OnRepackPortions(const TSimplePortionsGroupInfo& portions) {
        Singleton<TGeneralCompactionCounters>()->RepackPortions.OnData(portions);
        Singleton<TGeneralCompactionCounters>()->HistogramRepackPortionsCount->Collect(portions.GetCount());
        Singleton<TGeneralCompactionCounters>()->HistogramRepackPortionsBlobBytes.Collect(portions.GetBlobBytes());
        Singleton<TGeneralCompactionCounters>()->HistogramRepackPortionsRawBytes.Collect(portions.GetRawBytes());
        Singleton<TGeneralCompactionCounters>()->HistogramRepackPortionsRows->Collect(portions.GetRecordsCount());
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

    static void OnCompactionWriteIndexCompleted( const ui64 blobsWritten, const ui64 bytesWritten) {
        Singleton<TGeneralCompactionCounters>()->HistogramBlobsWrittenCount->Collect(blobsWritten);
        Singleton<TGeneralCompactionCounters>()->HistogramBlobsWrittenBytes.Collect(bytesWritten);
    }

    static void OnCompactionFinished(const ui64 durationMicroSeconds) {
        Singleton<TGeneralCompactionCounters>()->HistogramCompactionDuration.Collect(durationMicroSeconds);
    }

    static void OnTasksGeneratred(const ui64 durationMicroSeconds, const ui64 count) {
        Singleton<TGeneralCompactionCounters>()->HistogramTaskGenerationDuration.Collect(durationMicroSeconds);
        Singleton<TGeneralCompactionCounters>()->HistogramTaskGenerationCount->Collect(count);
    }
};

}   // namespace NKikimr::NOlap::NChanges

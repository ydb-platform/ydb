#pragma once

#include "common_data.h"

#include <ydb/core/tx/columnshard/common/portion.h>

#include <ydb/library/signals/histogram.h>
#include <ydb/library/signals/owner.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/string/builder.h>

#include <set>

namespace NKikimr::NOlap {
class TPortionInfo;
class TColumnEngineForLogs;
}   // namespace NKikimr::NOlap

namespace NKikimr::NColumnShard {

class TBaseGranuleDataClassSummary {
protected:
    i64 ColumnPortionsSize = 0;
    i64 TotalPortionsSize = 0;
    i64 PortionsCount = 0;
    i64 RecordsCount = 0;
    i64 MetadataMemoryPortionsSize = 0;

public:
    i64 GetColumnPortionsSize() const {
        return ColumnPortionsSize;
    }

    i64 GetTotalPortionsSize() const {
        return TotalPortionsSize;
    }

    i64 GetRecordsCount() const {
        return RecordsCount;
    }

    i64 GetPortionsCount() const {
        return PortionsCount;
    }

    i64 GetMetadataMemoryPortionsSize() const {
        return MetadataMemoryPortionsSize;
    }

    TString DebugString() const {
        return TStringBuilder() << "columns_size:" << ColumnPortionsSize << ";total_size:" << TotalPortionsSize << ";count:" << PortionsCount
                                << ";metadata_portions_size:" << MetadataMemoryPortionsSize << ";records_count:" << RecordsCount << ";";
    }

    TBaseGranuleDataClassSummary operator+(const TBaseGranuleDataClassSummary& item) const;
};

class TDataClassCounters {
private:
    std::shared_ptr<TValueAggregationClient> PortionsSize;
    std::shared_ptr<TValueAggregationClient> PortionsCount;

public:
    TDataClassCounters(
        const std::shared_ptr<TValueAggregationClient>& portionsSize, const std::shared_ptr<TValueAggregationClient>& portionsCount)
        : PortionsSize(portionsSize)
        , PortionsCount(portionsCount)
    {
    }

    void OnPortionsInfo(const TBaseGranuleDataClassSummary& dataInfo) const {
        PortionsSize->SetValue(dataInfo.GetTotalPortionsSize());
        PortionsCount->SetValue(dataInfo.GetPortionsCount());
    }
};

class TAgentDataClassCounters: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    std::shared_ptr<TValueAggregationAgent> PortionsSize;
    std::shared_ptr<TValueAggregationAgent> PortionsCount;

public:
    TAgentDataClassCounters(const TString& baseName, const TString& signalId)
        : TBase(baseName)
    {
        PortionsSize = TBase::GetValueAutoAggregations(signalId + "/Bytes");
        PortionsCount = TBase::GetValueAutoAggregations(signalId + "/Chunks");
    }

    TDataClassCounters RegisterClient() const {
        return TDataClassCounters(PortionsSize->GetClient(), PortionsCount->GetClient());
    }
};

class TGranuleDataCounters {
private:
    const TDataClassCounters InsertedData;
    const TDataClassCounters CompactedData;
    const TDataClassCounters FullData;

public:
    TGranuleDataCounters(const TDataClassCounters& insertedData, const TDataClassCounters& compactedData, const TDataClassCounters& fullData)
        : InsertedData(insertedData)
        , CompactedData(compactedData)
        , FullData(fullData)
    {
    }

    void OnPortionsDataRefresh(const TBaseGranuleDataClassSummary& inserted, const TBaseGranuleDataClassSummary& compacted) const {
        FullData.OnPortionsInfo(inserted + compacted);
        InsertedData.OnPortionsInfo(inserted);
        CompactedData.OnPortionsInfo(compacted);
    }
};

class TAgentGranuleDataCounters {
private:
    TAgentDataClassCounters InsertedData;
    TAgentDataClassCounters CompactedData;
    TAgentDataClassCounters FullData;

public:
    TAgentGranuleDataCounters(const TString& ownerId)
        : InsertedData(ownerId, "ByGranule/Inserted")
        , CompactedData(ownerId, "ByGranule/Compacted")
        , FullData(ownerId, "ByGranule/Full")
    {
    }

    TGranuleDataCounters RegisterClient() const {
        return TGranuleDataCounters(InsertedData.RegisterClient(), CompactedData.RegisterClient(), FullData.RegisterClient());
    }
};

class TEngineLogsCounters: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr PortionToDropCount;
    NMonitoring::TDynamicCounters::TCounterPtr PortionToDropBytes;
    NMonitoring::THistogramPtr PortionToDropLag;
    NMonitoring::THistogramPtr SkipDeleteWithProcessMemory;
    NMonitoring::THistogramPtr SkipDeleteWithTxLimit;

    NMonitoring::TDynamicCounters::TCounterPtr PortionToEvictCount;
    NMonitoring::TDynamicCounters::TCounterPtr PortionToEvictBytes;
    NMonitoring::THistogramPtr PortionToEvictLag;
    NMonitoring::THistogramPtr SkipEvictionWithProcessMemory;
    NMonitoring::THistogramPtr SkipEvictionWithTxLimit;

    NMonitoring::THistogramPtr ActualizationTaskSizeRemove;
    NMonitoring::THistogramPtr ActualizationTaskSizeEvict;

    NMonitoring::TDynamicCounters::TCounterPtr IndexMetadataUsageBytes;

    NMonitoring::TDynamicCounters::TCounterPtr BadPortionsCount;

    NMonitoring::TDynamicCounters::TCounterPtr CleanupPortionsSkippedByLock;
    NMonitoring::TDynamicCounters::TCounterPtr CleanupPortionsLimitExceeded;

    NMonitoring::THistogramPtr HistogramAddPortionDurationMs;

    TAgentGranuleDataCounters GranuleDataAgent;
    std::vector<std::shared_ptr<TIncrementalHistogram>> PortionSizeDistribution;
    std::vector<std::shared_ptr<TIncrementalHistogram>> PortionRecordsDistribution;

public:
    friend class NKikimr::NOlap::TColumnEngineForLogs;

    class TPortionsInfoGuard {
    private:
        std::vector<std::shared_ptr<TIncrementalHistogram::TGuard>> PortionRecordCountGuards;
        std::vector<std::shared_ptr<TIncrementalHistogram::TGuard>> PortionSizeGuards;
        TValueGuard BadPortionsCount;

    public:
        TPortionsInfoGuard(const std::vector<std::shared_ptr<TIncrementalHistogram>>& distrPortionSize,
            const std::vector<std::shared_ptr<TIncrementalHistogram>>& distrRecordsCount,
            NMonitoring::TDynamicCounters::TCounterPtr badPortionsCount)
            : BadPortionsCount(badPortionsCount)
        {
            for (auto&& i : distrPortionSize) {
                PortionSizeGuards.emplace_back(i->BuildGuard());
            }
            for (auto&& i : distrRecordsCount) {
                PortionRecordCountGuards.emplace_back(i->BuildGuard());
            }
        }

        void OnNewPortion(const std::shared_ptr<NOlap::TPortionInfo>& portion);
        void OnDropPortion(const std::shared_ptr<NOlap::TPortionInfo>& portion);
    };

    void OnActualizationTask(const ui32 evictCount, const ui32 removeCount) const;

    TPortionsInfoGuard BuildPortionBlobsGuard() const {
        return TPortionsInfoGuard(PortionSizeDistribution, PortionRecordsDistribution, BadPortionsCount);
    }

    TGranuleDataCounters RegisterGranuleDataCounters() const {
        return GranuleDataAgent.RegisterClient();
    }

    void OnSkipDeleteWithProcessMemory(const TDuration lag) const {
        SkipDeleteWithProcessMemory->Collect(lag.Seconds());
    }

    void OnSkipDeleteWithTxLimit(const TDuration lag) const {
        SkipDeleteWithTxLimit->Collect(lag.Seconds());
    }

    void OnSkipEvictionWithProcessMemory(const TDuration lag) const {
        SkipEvictionWithProcessMemory->Collect(lag.Seconds());
    }

    void OnSkipEvictionWithTxLimit(const TDuration lag) const {
        SkipEvictionWithTxLimit->Collect(lag.Seconds());
    }

    void OnPortionToEvict(const ui64 size, const TDuration lag) const {
        PortionToEvictCount->Add(1);
        PortionToEvictBytes->Add(size);
        PortionToEvictLag->Collect(lag.Seconds());
    }

    void OnPortionToDrop(const ui64 size, const TDuration lag) const {
        PortionToDropCount->Add(1);
        PortionToDropBytes->Add(size);
        PortionToDropLag->Collect(lag.Seconds());
    }

    void OnIndexMetadataUsageBytes(const ui64 size) const {
        IndexMetadataUsageBytes->Set(size);
    }

    void OnCleanupPortionSkippedByLock(ui32 count) const {
        CleanupPortionsSkippedByLock->Add(count);
    }

    void OnCleanupPortionsLimitExceed() const {
        CleanupPortionsLimitExceeded->Add(1);
    }

    void OnPortionAdded(const TDuration d) const {
        HistogramAddPortionDurationMs->Collect(d.MilliSeconds());
    }

    TEngineLogsCounters();
};

}   // namespace NKikimr::NColumnShard

#pragma once
#include "common/owner.h"
#include <ydb/core/tx/columnshard/common/portion.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/string/builder.h>
#include <set>

namespace NKikimr::NOlap {
class TPortionInfo;
}

namespace NKikimr::NColumnShard {

class TBaseGranuleDataClassSummary {
protected:
    i64 ColumnPortionsSize = 0;
    i64 TotalPortionsSize = 0;
    i64 PortionsCount = 0;
    i64 RecordsCount = 0;
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

    TString DebugString() const {
        return TStringBuilder() << "columns_size:" << ColumnPortionsSize << ";total_size:" << TotalPortionsSize << ";count:" << PortionsCount << ";";
    }

    TBaseGranuleDataClassSummary operator+(const TBaseGranuleDataClassSummary& item) const;
};

class TDataClassCounters {
private:
    std::shared_ptr<TValueAggregationClient> PortionsSize;
    std::shared_ptr<TValueAggregationClient> PortionsCount;
public:
    TDataClassCounters(const std::shared_ptr<TValueAggregationClient>& portionsSize, const std::shared_ptr<TValueAggregationClient>& portionsCount)
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
        , FullData(ownerId, "ByGranule/Full") {
    }

    TGranuleDataCounters RegisterClient() const {
        return TGranuleDataCounters(InsertedData.RegisterClient(), CompactedData.RegisterClient(), FullData.RegisterClient());
    }
};

class TIncrementalHistogram: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    std::map<i64, NMonitoring::TDynamicCounters::TCounterPtr> Counters;
    NMonitoring::TDynamicCounters::TCounterPtr PlusInf;

    NMonitoring::TDynamicCounters::TCounterPtr GetQuantile(const i64 value) const {
        auto it = Counters.lower_bound(value);
        if (it == Counters.end()) {
            return PlusInf;
        } else {
            return it->second;
        }
    }
public:

    class TGuard {
    private:
        class TLineGuard {
        private:
            NMonitoring::TDynamicCounters::TCounterPtr Counter;
            i64 Value = 0;
        public:
            TLineGuard(NMonitoring::TDynamicCounters::TCounterPtr counter)
                : Counter(counter)
            {

            }

            ~TLineGuard() {
                Sub(Value);
            }

            void Add(const i64 value) {
                Counter->Add(value);
                Value += value;
            }

            void Sub(const i64 value) {
                Counter->Sub(value);
                Value -= value;
                Y_ABORT_UNLESS(Value >= 0);
            }
        };

        std::map<i64, TLineGuard> Counters;
        TLineGuard PlusInf;

        TLineGuard& GetLineGuard(const i64 value) {
            auto it = Counters.lower_bound(value);
            if (it == Counters.end()) {
                return PlusInf;
            } else {
                return it->second;
            }
        }
    public:
        TGuard(const TIncrementalHistogram& owner)
            : PlusInf(owner.PlusInf)
        {
            for (auto&& i : owner.Counters) {
                Counters.emplace(i.first, TLineGuard(i.second));
            }
        }
        void Add(const i64 value, const i64 count) {
            GetLineGuard(value).Add(count);
        }

        void Sub(const i64 value, const i64 count) {
            GetLineGuard(value).Sub(count);
        }
    };

    std::shared_ptr<TGuard> BuildGuard() const {
        return std::make_shared<TGuard>(*this);
    }

    TIncrementalHistogram(const TString& moduleId, const TString& metricId, const TString& category, const std::set<i64>& values)
        : TBase(moduleId) {
        DeepSubGroup("metric", metricId);
        if (category) {
            DeepSubGroup("category", category);
        }
        std::optional<TString> predName;
        for (auto&& i : values) {
            if (!predName) {
                Counters.emplace(i, TBase::GetValue("(-Inf," + ::ToString(i) + "]"));
            } else {
                Counters.emplace(i, TBase::GetValue("(" + *predName + "," + ::ToString(i) + "]"));
            }
            predName = ::ToString(i);
        }
        Y_ABORT_UNLESS(predName);
        PlusInf = TBase::GetValue("(" + *predName + ",+Inf)");
    }

    TIncrementalHistogram(const TString& moduleId, const TString& metricId, const TString& category, const std::map<i64, TString>& values)
        : TBase(moduleId)
    {
        DeepSubGroup("metric", metricId);
        if (category) {
            DeepSubGroup("category", category);
        }
        std::optional<TString> predName;
        for (auto&& i : values) {
            if (!predName) {
                Counters.emplace(i.first, TBase::GetValue("(-Inf," + i.second + "]"));
            } else {
                Counters.emplace(i.first, TBase::GetValue("(" + *predName + "," + i.second + "]"));
            }
            predName = i.second;
        }
        Y_ABORT_UNLESS(predName);
        PlusInf = TBase::GetValue("(" + *predName + ",+Inf)");
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

    NMonitoring::TDynamicCounters::TCounterPtr ActualizationSkipRWProgressCount;
    NMonitoring::THistogramPtr ActualizationSkipTooFreshPortion;

    NMonitoring::TDynamicCounters::TCounterPtr PortionNoTtlColumnCount;
    NMonitoring::TDynamicCounters::TCounterPtr PortionNoTtlColumnBytes;

    NMonitoring::TDynamicCounters::TCounterPtr StatUsageForTTLCount;
    NMonitoring::TDynamicCounters::TCounterPtr ChunkUsageForTTLCount;

    NMonitoring::TDynamicCounters::TCounterPtr PortionNoBorderCount;
    NMonitoring::TDynamicCounters::TCounterPtr PortionNoBorderBytes;

    NMonitoring::TDynamicCounters::TCounterPtr GranuleOptimizerLocked;

    TAgentGranuleDataCounters GranuleDataAgent;
    std::vector<std::shared_ptr<TIncrementalHistogram>> BlobSizeDistribution;
    std::vector<std::shared_ptr<TIncrementalHistogram>> PortionSizeDistribution;
    std::vector<std::shared_ptr<TIncrementalHistogram>> PortionRecordsDistribution;

public:

    class TPortionsInfoGuard {
    private:
        std::vector<std::shared_ptr<TIncrementalHistogram::TGuard>> BlobGuards;
        std::vector<std::shared_ptr<TIncrementalHistogram::TGuard>> PortionRecordCountGuards;
        std::vector<std::shared_ptr<TIncrementalHistogram::TGuard>> PortionSizeGuards;
    public:
        TPortionsInfoGuard(const std::vector<std::shared_ptr<TIncrementalHistogram>>& distrBlobs,
            const std::vector<std::shared_ptr<TIncrementalHistogram>>& distrPortionSize,
            const std::vector<std::shared_ptr<TIncrementalHistogram>>& distrRecordsCount)
        {
            for (auto&& i : distrBlobs) {
                BlobGuards.emplace_back(i->BuildGuard());
            }
            for (auto&& i : distrPortionSize) {
                PortionSizeGuards.emplace_back(i->BuildGuard());
            }
            for (auto&& i : distrRecordsCount) {
                PortionRecordCountGuards.emplace_back(i->BuildGuard());
            }
        }


        void OnNewPortion(const std::shared_ptr<NOlap::TPortionInfo>& portion) const;
        void OnDropPortion(const std::shared_ptr<NOlap::TPortionInfo>& portion) const;

    };

    void OnActualizationTask(const ui32 evictCount, const ui32 removeCount) const;

    TPortionsInfoGuard BuildPortionBlobsGuard() const {
        return TPortionsInfoGuard(BlobSizeDistribution, PortionSizeDistribution, PortionRecordsDistribution);
    }

    TGranuleDataCounters RegisterGranuleDataCounters() const {
        return GranuleDataAgent.RegisterClient();
    }

    void OnActualizationSkipRWProgress() const {
        ActualizationSkipRWProgressCount->Add(1);
    }

    void OnActualizationSkipTooFreshPortion(const TDuration dWait) const {
        ActualizationSkipTooFreshPortion->Collect(dWait.Seconds());
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

    void OnPortionNoTtlColumn(const ui64 size) const {
        PortionNoTtlColumnCount->Add(1);
        PortionNoTtlColumnBytes->Add(size);
    }

    void OnChunkUsageForTTL() const {
        ChunkUsageForTTLCount->Add(1);
    }

    void OnStatUsageForTTL() const {
        StatUsageForTTLCount->Add(1);
    }

    void OnPortionNoBorder(const ui64 size) const {
        PortionNoBorderCount->Add(1);
        PortionNoBorderBytes->Add(size);
    }

    void OnGranuleOptimizerLocked() const {
        GranuleOptimizerLocked->Add(1);
    }

    TEngineLogsCounters();
};

}

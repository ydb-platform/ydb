#pragma once
#include "common/owner.h"
#include <ydb/core/tx/columnshard/common/portion.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/string/builder.h>
#include <set>

namespace NKikimr::NColumnShard {

class TBaseGranuleDataClassSummary {
protected:
    i64 PortionsSize = 0;
    i64 PortionsCount = 0;
    i64 RecordsCount = 0;
public:
    i64 GetPortionsSize() const {
        return PortionsSize;
    }
    i64 GetRecordsCount() const {
        return RecordsCount;
    }
    i64 GetPortionsCount() const {
        return PortionsCount;
    }

    TString DebugString() const {
        return TStringBuilder() << "size:" << PortionsSize << ";count:" << PortionsCount << ";";
    }

    TBaseGranuleDataClassSummary operator+(const TBaseGranuleDataClassSummary& item) const {
        TBaseGranuleDataClassSummary result;
        result.PortionsSize = PortionsSize + item.PortionsSize;
        result.PortionsCount = PortionsCount + item.PortionsCount;
        result.RecordsCount = RecordsCount + item.RecordsCount;
        return result;
    }
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
        PortionsSize->SetValue(dataInfo.GetPortionsSize());
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
                Y_VERIFY(Value >= 0);
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
        Y_VERIFY(predName);
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
        Y_VERIFY(predName);
        PlusInf = TBase::GetValue("(" + *predName + ",+Inf)");
    }

};

class TEngineLogsCounters: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr PortionToDropCount;
    NMonitoring::TDynamicCounters::TCounterPtr PortionToDropBytes;

    NMonitoring::TDynamicCounters::TCounterPtr PortionToEvictCount;
    NMonitoring::TDynamicCounters::TCounterPtr PortionToEvictBytes;

    NMonitoring::TDynamicCounters::TCounterPtr PortionNoTtlColumnCount;
    NMonitoring::TDynamicCounters::TCounterPtr PortionNoTtlColumnBytes;

    NMonitoring::TDynamicCounters::TCounterPtr PortionNoBorderCount;
    NMonitoring::TDynamicCounters::TCounterPtr PortionNoBorderBytes;

    TAgentGranuleDataCounters GranuleDataAgent;
    std::vector<std::shared_ptr<TIncrementalHistogram>> BlobSizeDistribution;
public:
    NMonitoring::TDynamicCounters::TCounterPtr OverloadGranules;
    NMonitoring::TDynamicCounters::TCounterPtr CompactOverloadGranulesSelection;
    NMonitoring::TDynamicCounters::TCounterPtr NoCompactGranulesSelection;
    NMonitoring::TDynamicCounters::TCounterPtr SplitCompactGranulesSelection;
    NMonitoring::TDynamicCounters::TCounterPtr InternalCompactGranulesSelection;

    class TPortionsInfoGuard {
    private:
        std::vector<std::shared_ptr<TIncrementalHistogram::TGuard>> Guards;
    public:
        TPortionsInfoGuard(const std::vector<std::shared_ptr<TIncrementalHistogram>>& distr)
        {
            for (auto&& i : distr) {
                Guards.emplace_back(i->BuildGuard());
            }
        }

        void OnNewBlob(const NOlap::NPortion::EProduced produced, const ui64 size) const {
            Y_VERIFY((ui32)produced < Guards.size());
            Guards[(ui32)produced]->Add(size, size);
        }

        void OnDropBlob(const NOlap::NPortion::EProduced produced, const ui64 size) const {
            Y_VERIFY((ui32)produced < Guards.size());
            Guards[(ui32)produced]->Sub(size, size);
        }

    };

    TPortionsInfoGuard BuildPortionBlobsGuard() const {
        return TPortionsInfoGuard(BlobSizeDistribution);
    }

    TGranuleDataCounters RegisterGranuleDataCounters() const {
        return GranuleDataAgent.RegisterClient();
    }

    void OnPortionToEvict(const ui64 size) const {
        PortionToEvictCount->Add(1);
        PortionToEvictBytes->Add(size);
    }

    void OnPortionToDrop(const ui64 size) const {
        PortionToDropCount->Add(1);
        PortionToDropBytes->Add(size);
    }

    void OnPortionNoTtlColumn(const ui64 size) const {
        PortionNoTtlColumnCount->Add(1);
        PortionNoTtlColumnBytes->Add(size);
    }

    void OnPortionNoBorder(const ui64 size) const {
        PortionNoBorderCount->Add(1);
        PortionNoBorderBytes->Add(size);
    }

    TEngineLogsCounters();
};

}

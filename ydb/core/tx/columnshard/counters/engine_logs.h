#pragma once
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include "common/owner.h"

namespace NKikimr::NColumnShard {

class TBaseGranuleDataClassSummary {
protected:
    i64 PortionsSize = 0;
    i64 MaxColumnsSize = 0;
    i64 PortionsCount = 0;
    i64 RecordsCount = 0;
public:
    i64 GetPortionsSize() const {
        return PortionsSize;
    }
    i64 GetRecordsCount() const {
        return RecordsCount;
    }
    i64 GetMaxColumnsSize() const {
        return MaxColumnsSize;
    }
    i64 GetPortionsCount() const {
        return PortionsCount;
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
        PortionsSize->Set(dataInfo.GetPortionsSize());
        PortionsCount->Set(dataInfo.GetPortionsCount());
    }

    void OnPortionsInfo(const ui64 size, const ui32 chunks) const {
        PortionsSize->Set(size);
        PortionsCount->Set(chunks);
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
        return TDataClassCounters(PortionsSize->GetClient(PortionsSize), PortionsCount->GetClient(PortionsCount));
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

    void OnFullData(const ui64 size, const ui32 chunksCount) const {
        FullData.OnPortionsInfo(size, chunksCount);
    }

    void OnInsertedData(const TBaseGranuleDataClassSummary& dataInfo) const {
        InsertedData.OnPortionsInfo(dataInfo);
    }

    void OnCompactedData(const TBaseGranuleDataClassSummary& dataInfo) const {
        CompactedData.OnPortionsInfo(dataInfo);
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
public:
    NMonitoring::TDynamicCounters::TCounterPtr OverloadGranules;
    NMonitoring::TDynamicCounters::TCounterPtr CompactOverloadGranulesSelection;
    NMonitoring::TDynamicCounters::TCounterPtr NoCompactGranulesSelection;
    NMonitoring::TDynamicCounters::TCounterPtr SplitCompactGranulesSelection;
    NMonitoring::TDynamicCounters::TCounterPtr InternalCompactGranulesSelection;

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

#pragma once
#include <ydb/core/tx/columnshard/counters/common/owner.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

namespace NKikimr::NOlap::NActualizer {

class TPortionCategoryCounterAgents: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
public:
    const std::shared_ptr<NColumnShard::TValueAggregationAgent> RecordsCount;
    const std::shared_ptr<NColumnShard::TValueAggregationAgent> Count;
    const std::shared_ptr<NColumnShard::TValueAggregationAgent> Bytes;
    TPortionCategoryCounterAgents(NColumnShard::TCommonCountersOwner& base, const TString& categoryName)
        : TBase(base, "category", categoryName)
        , RecordsCount(TBase::GetValueAutoAggregations("ByGranule/Portions/RecordsCount"))
        , Count(TBase::GetValueAutoAggregations("ByGranule/Portions/Count"))
        , Bytes(TBase::GetValueAutoAggregations("ByGranule/Portions/Bytes"))
    {
    }
};

class TPortionCategoryCounters {
private:
    std::shared_ptr<NColumnShard::TValueAggregationClient> RecordsCount;
    std::shared_ptr<NColumnShard::TValueAggregationClient> Count;
    std::shared_ptr<NColumnShard::TValueAggregationClient> Bytes;
public:
    TPortionCategoryCounters(TPortionCategoryCounterAgents& agents)
    {
        RecordsCount = agents.RecordsCount->GetClient();
        Count = agents.Count->GetClient();
        Bytes = agents.Bytes->GetClient();
    }

    void AddPortion(const std::shared_ptr<TPortionInfo>& p) {
        RecordsCount->Add(p->NumRows());
        Count->Add(1);
        Bytes->Add(p->GetTotalBlobBytes());
    }

    void RemovePortion(const std::shared_ptr<TPortionInfo>& p) {
        RecordsCount->Remove(p->NumRows());
        Count->Remove(1);
        Bytes->Remove(p->GetTotalBlobBytes());
    }
};

class TGlobalCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    std::shared_ptr<TPortionCategoryCounterAgents> PortionsWaitingEviction;
    std::shared_ptr<TPortionCategoryCounterAgents> PortionsWaitingDelete;
    std::shared_ptr<TPortionCategoryCounterAgents> PortionsLatenessEviction;
    std::shared_ptr<TPortionCategoryCounterAgents> PortionsLatenessDelete;
    std::shared_ptr<TPortionCategoryCounterAgents> PortionsToSyncSchema;
public:

    TGlobalCounters()
        : TBase("Actualizer")
    {
        PortionsWaitingEviction = std::make_shared<TPortionCategoryCounterAgents>(*this, "eviction_waiting");
        PortionsWaitingDelete = std::make_shared<TPortionCategoryCounterAgents>(*this, "delete_waiting");
        PortionsLatenessEviction = std::make_shared<TPortionCategoryCounterAgents>(*this, "eviction_lateness");
        PortionsLatenessDelete = std::make_shared<TPortionCategoryCounterAgents>(*this, "delete_lateness");
        PortionsToSyncSchema = std::make_shared<TPortionCategoryCounterAgents>(*this, "sync_schema");
    }

    static std::shared_ptr<TPortionCategoryCounters> BuildPortionsWaitingEvictionAggregation() {
        return std::make_shared<TPortionCategoryCounters>(*Singleton<TGlobalCounters>()->PortionsWaitingEviction);
    }

    static std::shared_ptr<TPortionCategoryCounters> BuildPortionsWaitingDeleteAggregation() {
        return std::make_shared<TPortionCategoryCounters>(*Singleton<TGlobalCounters>()->PortionsWaitingDelete);
    }

    static std::shared_ptr<TPortionCategoryCounters> BuildPortionsLatenessEvictionAggregation() {
        return std::make_shared<TPortionCategoryCounters>(*Singleton<TGlobalCounters>()->PortionsLatenessEviction);
    }

    static std::shared_ptr<TPortionCategoryCounters> BuildPortionsLatenessDeleteAggregation() {
        return std::make_shared<TPortionCategoryCounters>(*Singleton<TGlobalCounters>()->PortionsLatenessDelete);
    }

    static std::shared_ptr<TPortionCategoryCounters> BuildPortionsToSyncSchemaAggregation() {
        return std::make_shared<TPortionCategoryCounters>(*Singleton<TGlobalCounters>()->PortionsToSyncSchema);
    }
};

class TCounters {
public:
    const std::shared_ptr<TPortionCategoryCounters> PortionsWaitingEviction;
    const std::shared_ptr<TPortionCategoryCounters> PortionsWaitingDelete;
    const std::shared_ptr<TPortionCategoryCounters> PortionsLatenessEviction;
    const std::shared_ptr<TPortionCategoryCounters> PortionsLatenessDelete;
    const std::shared_ptr<TPortionCategoryCounters> PortionsToSyncSchema;

    TCounters()
        : PortionsWaitingEviction(TGlobalCounters::BuildPortionsWaitingEvictionAggregation())
        , PortionsWaitingDelete(TGlobalCounters::BuildPortionsWaitingDeleteAggregation())
        , PortionsLatenessEviction(TGlobalCounters::BuildPortionsLatenessEvictionAggregation())
        , PortionsLatenessDelete(TGlobalCounters::BuildPortionsLatenessDeleteAggregation())
        , PortionsToSyncSchema(TGlobalCounters::BuildPortionsToSyncSchemaAggregation())
    {
    }

};

}

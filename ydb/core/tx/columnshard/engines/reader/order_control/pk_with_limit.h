#pragma once
#include "abstract.h"
#include <ydb/core/tx/columnshard/engines/reader/read_filter_merger.h>

namespace NKikimr::NOlap::NIndexedReader {

class TGranuleOrdered {
private:
    bool StartedFlag = false;
    std::deque<TGranule::TBatchForMerge> OrderedBatches;
    TGranule::TPtr Granule;
public:
    bool Start() {
        if (!StartedFlag) {
            StartedFlag = true;
            return true;
        } else {
            return false;
        }

    }

    TGranuleOrdered(std::deque<TGranule::TBatchForMerge>&& orderedBatches, TGranule::TPtr granule)
        : OrderedBatches(std::move(orderedBatches))
        , Granule(granule)
    {
    }

    std::deque<TGranule::TBatchForMerge>& GetOrderedBatches() noexcept {
        return OrderedBatches;
    }

    TGranule::TPtr GetGranule() const noexcept {
        return Granule;
    }
};

class TPKSortingWithLimit: public IOrderPolicy {
private:
    using TBase = IOrderPolicy;
    std::deque<TGranule::TPtr> GranulesOutOrder;
    std::deque<TGranuleOrdered> GranulesOutOrderForPortions;
    ui32 CurrentItemsLimit = 0;
    THashMap<ui32, ui32> CountBatchesByPools;
    ui32 CountProcessedGranules = 0;
    ui32 CountSkippedBatches = 0;
    ui32 CountProcessedBatches = 0;
    ui32 CountNotSortedPortions = 0;
    ui32 CountSkippedGranules = 0;
    TMergePartialStream MergeStream;
protected:
    virtual bool DoWakeup(const TGranule& granule, TGranulesFillingContext& context) override;
    virtual void DoFill(TGranulesFillingContext& context) override;
    virtual std::vector<TGranule::TPtr> DoDetachReadyGranules(THashMap<ui64, NIndexedReader::TGranule::TPtr>& granulesToOut) override;
    virtual bool DoOnFilterReady(TBatch& batchInfo, const TGranule& granule, TGranulesFillingContext& context) override;
    virtual TFeatures DoGetFeatures() const override {
        return (TFeatures)EFeatures::CanInterrupt & (TFeatures)EFeatures::NeedNotAppliedEarlyFilter;
    }

    virtual TString DoDebugString() const override {
        return TStringBuilder() << "type=PKSortingWithLimit;granules_count=" << GranulesOutOrder.size() << ";limit=" << CurrentItemsLimit << ";";
    }
public:
    virtual std::set<ui32> GetFilterStageColumns() override {
        std::set<ui32> result = ReadMetadata->GetEarlyFilterColumnIds();
        for (auto&& i : ReadMetadata->GetPKColumnIds()) {
            result.emplace(i);
        }
        return result;
    }

    TPKSortingWithLimit(TReadMetadata::TConstPtr readMetadata);
    virtual bool ReadyForAddNotIndexedToEnd() const override {
        return ReadMetadata->IsDescSorted() && GranulesOutOrder.empty();
    }
};

}

#pragma once
#include "abstract.h"
#include <ydb/core/tx/columnshard/engines/reader/read_filter_merger.h>

namespace NKikimr::NOlap::NIndexedReader {

class TGranuleOrdered {
private:
    bool StartedFlag = false;
    std::deque<TBatch*> OrderedBatches;
    TGranule* Granule = nullptr;
public:
    bool Start() {
        if (!StartedFlag) {
            StartedFlag = true;
            return true;
        } else {
            return false;
        }

    }

    TGranuleOrdered(std::deque<TBatch*>&& orderedBatches, TGranule* granule)
        : OrderedBatches(std::move(orderedBatches))
        , Granule(granule)
    {
    }

    std::deque<TBatch*>& GetOrderedBatches() noexcept {
        return OrderedBatches;
    }

    TGranule* GetGranule() const noexcept {
        return Granule;
    }
};

class TPKSortingWithLimit: public IOrderPolicy {
private:
    using TBase = IOrderPolicy;
    std::deque<TGranule*> GranulesOutOrder;
    std::deque<TGranuleOrdered> GranulesOutOrderForPortions;
    ui32 CurrentItemsLimit = 0;
    ui32 CountProcessedGranules = 0;
    ui32 CountNotSorted = 0;
    ui32 CountSorted = 0;
    TMergePartialStream MergeStream;
protected:
    virtual bool DoWakeup(const TGranule& granule, TGranulesFillingContext& context) override;
    virtual void DoFill(TGranulesFillingContext& context) override;
    virtual std::vector<TGranule*> DoDetachReadyGranules(THashMap<ui64, NIndexedReader::TGranule*>& granulesToOut) override;
    virtual bool DoOnFilterReady(TBatch& batchInfo, const TGranule& granule, TGranulesFillingContext& context) override;
    virtual TFeatures DoGetFeatures() const override {
        return (TFeatures)EFeatures::CanInterrupt & (TFeatures)EFeatures::NeedNotAppliedEarlyFilter;
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

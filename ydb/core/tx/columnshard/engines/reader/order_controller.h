#pragma once
#include "granule.h"
#include "read_metadata.h"

namespace NKikimr::NOlap::NIndexedReader {

class TGranulesFillingContext;

class IOrderPolicy {
protected:
    TReadMetadata::TConstPtr ReadMetadata;
    virtual void DoFill(TGranulesFillingContext& context) = 0;
    virtual std::vector<TGranule*> DoDetachReadyGranules(THashMap<ui64, NIndexedReader::TGranule*>& granulesToOut) = 0;
    virtual bool DoOnFilterReady(TBatch& batchInfo, const TGranule& /*granule*/, TGranulesFillingContext& context) {
        OnBatchFilterInitialized(batchInfo, context);
        return true;
    }

    void OnBatchFilterInitialized(TBatch& batch, TGranulesFillingContext& context);
public:
    using TPtr = std::shared_ptr<IOrderPolicy>;
    virtual ~IOrderPolicy() = default;

    IOrderPolicy(TReadMetadata::TConstPtr readMetadata)
        : ReadMetadata(readMetadata)
    {

    }

    virtual bool CanInterrupt() const {
        return false;
    }

    bool OnFilterReady(TBatch& batchInfo, const TGranule& granule, TGranulesFillingContext& context) {
        return DoOnFilterReady(batchInfo, granule, context);
    }


    virtual bool ReadyForAddNotIndexedToEnd() const = 0;

    std::vector<TGranule*> DetachReadyGranules(THashMap<ui64, NIndexedReader::TGranule*>& granulesToOut) {
        return DoDetachReadyGranules(granulesToOut);
    }

    void Fill(TGranulesFillingContext& context) {
        DoFill(context);
    }
};

class TNonSorting: public IOrderPolicy {
private:
    using TBase = IOrderPolicy;
protected:
    virtual void DoFill(TGranulesFillingContext& /*context*/) override {
    }

    virtual std::vector<TGranule*> DoDetachReadyGranules(THashMap<ui64, NIndexedReader::TGranule*>& granulesToOut) override;
public:
    TNonSorting(TReadMetadata::TConstPtr readMetadata)
        :TBase(readMetadata)
    {

    }

    virtual bool ReadyForAddNotIndexedToEnd() const override {
        return true;
    }
};

class TAnySorting: public IOrderPolicy {
private:
    using TBase = IOrderPolicy;
    std::deque<TGranule*> GranulesOutOrder;
protected:
    virtual void DoFill(TGranulesFillingContext& context) override;
    virtual std::vector<TGranule*> DoDetachReadyGranules(THashMap<ui64, NIndexedReader::TGranule*>& granulesToOut) override;
public:
    TAnySorting(TReadMetadata::TConstPtr readMetadata)
        :TBase(readMetadata) {

    }
    virtual bool ReadyForAddNotIndexedToEnd() const override {
        return ReadMetadata->IsDescSorted() && GranulesOutOrder.empty();
    }
};

class TPKSortingWithLimit: public IOrderPolicy {
private:
    using TBase = IOrderPolicy;
    std::deque<TGranule*> GranulesOutOrder;
    std::deque<TGranule*> GranulesOutOrderForPortions;
    THashMap<ui64, std::deque<TBatch*>> OrderedBatches;
    ui32 CurrentItemsLimit = 0;
protected:
    virtual void DoFill(TGranulesFillingContext& context) override;
    virtual std::vector<TGranule*> DoDetachReadyGranules(THashMap<ui64, NIndexedReader::TGranule*>& granulesToOut) override;
    virtual bool DoOnFilterReady(TBatch& batchInfo, const TGranule& granule, TGranulesFillingContext& context) override;
public:
    virtual bool CanInterrupt() const override {
        return true;
    }

    TPKSortingWithLimit(TReadMetadata::TConstPtr readMetadata)
        :TBase(readMetadata) {
        CurrentItemsLimit = ReadMetadata->Limit;
    }
    virtual bool ReadyForAddNotIndexedToEnd() const override {
        return ReadMetadata->IsDescSorted() && GranulesOutOrder.empty();
    }
};

}

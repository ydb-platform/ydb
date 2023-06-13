#pragma once
#include <ydb/core/tx/columnshard/engines/reader/granule.h>
#include <ydb/core/tx/columnshard/engines/reader/read_metadata.h>

namespace NKikimr::NOlap::NIndexedReader {

class TGranulesFillingContext;

class IOrderPolicy {
public:
    enum class EFeatures: ui32 {
        CanInterrupt = 1,
        NeedNotAppliedEarlyFilter = 1 << 1
    };
    using TFeatures = ui32;
private:
    mutable std::optional<TFeatures> Features;
protected:
    TReadMetadata::TConstPtr ReadMetadata;
    virtual TString DoDebugString() const = 0;
    virtual void DoFill(TGranulesFillingContext& context) = 0;
    virtual bool DoWakeup(const TGranule& /*granule*/, TGranulesFillingContext& /*context*/) {
        return true;
    }
    virtual std::vector<TGranule*> DoDetachReadyGranules(THashMap<ui64, NIndexedReader::TGranule*>& granulesToOut) = 0;
    virtual bool DoOnFilterReady(TBatch& batchInfo, const TGranule& /*granule*/, TGranulesFillingContext& context) {
        OnBatchFilterInitialized(batchInfo, context);
        return true;
    }

    void OnBatchFilterInitialized(TBatch& batch, TGranulesFillingContext& context);
    virtual TFeatures DoGetFeatures() const {
        return 0;
    }
    TFeatures GetFeatures() const {
        if (!Features) {
            Features = DoGetFeatures();
        }
        return *Features;
    }
public:
    using TPtr = std::shared_ptr<IOrderPolicy>;
    virtual ~IOrderPolicy() = default;

    virtual std::set<ui32> GetFilterStageColumns() {
        return ReadMetadata->GetEarlyFilterColumnIds();
    }

    IOrderPolicy(TReadMetadata::TConstPtr readMetadata)
        : ReadMetadata(readMetadata)
    {

    }

    bool CanInterrupt() const {
        return GetFeatures() & (TFeatures)EFeatures::CanInterrupt;
    }

    bool NeedNotAppliedEarlyFilter() const {
        return GetFeatures() & (TFeatures)EFeatures::NeedNotAppliedEarlyFilter;
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

    bool Wakeup(const TGranule& granule, TGranulesFillingContext& context) {
        return DoWakeup(granule, context);
    }

    TString DebugString() const {
        return DoDebugString();
    }
};

}

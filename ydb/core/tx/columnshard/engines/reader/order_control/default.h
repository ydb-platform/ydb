#pragma once
#include "abstract.h"

namespace NKikimr::NOlap::NIndexedReader {

class TAnySorting: public IOrderPolicy {
private:
    using TBase = IOrderPolicy;
    std::deque<TGranule::TPtr> GranulesOutOrder;
protected:
    virtual void DoFill(TGranulesFillingContext& context) override;
    virtual std::vector<TGranule::TPtr> DoDetachReadyGranules(TResultController& granulesToOut) override;
    virtual TString DoDebugString() const override {
        return TStringBuilder() << "type=AnySorting;granules_count=" << GranulesOutOrder.size() << ";";
    }

public:
    TAnySorting(TReadMetadata::TConstPtr readMetadata)
        :TBase(readMetadata) {

    }
    virtual bool ReadyForAddNotIndexedToEnd() const override {
        return ReadMetadata->IsDescSorted() && GranulesOutOrder.empty();
    }
};

}

#pragma once
#include "abstract.h"

namespace NKikimr::NOlap::NIndexedReader {

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

}

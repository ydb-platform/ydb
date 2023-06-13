#pragma once
#include "abstract.h"

namespace NKikimr::NOlap::NIndexedReader {

class TNonSorting: public IOrderPolicy {
private:
    using TBase = IOrderPolicy;
protected:
    virtual TString DoDebugString() const override {
        return TStringBuilder() << "type=NonSorting;";
    }

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

}

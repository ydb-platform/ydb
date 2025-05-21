#pragma once
#include "abstract.h"

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

class TTransparentPortionsSelector: public IPortionsSelector {
private:
    using TBase = IPortionsSelector;
    virtual bool DoIsAppropriate(const TPortionInfo::TPtr& /*portionInfo*/) const override {
        return true;
    }

public:
    using TBase::TBase;
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets

#pragma once
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

#include <ydb/services/bg_tasks/abstract/interface.h>

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

class IPortionsSelector {
private:
    YDB_READONLY_DEF(TString, Name);
    virtual bool DoIsAppropriate(const TPortionInfo::TPtr& portionInfo) const = 0;

public:
    virtual ~IPortionsSelector() = default;

    IPortionsSelector(const TString& name)
        : Name(name) {
    }

    bool IsAppropriate(const TPortionInfo::TPtr& portionInfo) const {
        return DoIsAppropriate(portionInfo);
    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets

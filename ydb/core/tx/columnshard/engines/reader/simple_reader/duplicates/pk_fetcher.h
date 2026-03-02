#pragma once

#include "common.h"
#include "context.h"

#include <ydb/core/formats/arrow/reader/merger.h>
#include <ydb/core/tx/conveyor/usage/abstract.h>
#include <ydb/core/tx/general_cache/usage/abstract.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

class TFetchPKKeysTask: public NConveyor::ITask {
private:
    using TColumnData = THashMap<NGeneralCache::TGlobalColumnAddress, std::shared_ptr<NArrow::NAccessor::IChunkedArray>>;
    
    TBuildFilterContext Context;
    TColumnData PKData;
    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> AllocationGuard;

private:
    virtual void DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) override;
    virtual void DoOnCannotExecute(const TString& reason) override;

    virtual TString GetTaskClassIdentifier() const override {
        return "FETCH_PK_KEYS";
    }

public:
    TFetchPKKeysTask(TBuildFilterContext&& context,
        TColumnData&& pkData,
        const std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>& allocationGuard)
        : Context(std::move(context))
        , PKData(std::move(pkData))
        , AllocationGuard(allocationGuard)
    {
    }

    TString DebugString() const {
        return TStringBuilder() << "FetchPKKeysTask";
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
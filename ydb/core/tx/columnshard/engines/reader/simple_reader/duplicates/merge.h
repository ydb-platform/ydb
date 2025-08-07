#pragma once

#include "common.h"

#include <ydb/core/formats/arrow/reader/merger.h>
#include <ydb/core/formats/arrow/rows/view.h>
#include <ydb/core/tx/columnshard/counters/duplicate_filtering.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates/context.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract/index_info.h>
#include <ydb/core/tx/conveyor/usage/abstract.h>

#include <ydb/library/actors/interconnect/types.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering  {

class TBuildDuplicateFilters: public NConveyor::ITask {
private:
    TDuplicateSourceCacheResult ColumnData;
    std::shared_ptr<TInternalFilterConstructor> Context;
    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> AllocationGuard;

private:
    virtual void DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) override;
    virtual void DoOnCannotExecute(const TString& reason) override;

    virtual TString GetTaskClassIdentifier() const override {
        return "BUILD_DUPLICATE_FILTERS";
    }

public:
    TBuildDuplicateFilters(TDuplicateSourceCacheResult&& columnData, const std::shared_ptr<TInternalFilterConstructor>& context, const std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>& guard)
        : ColumnData(columnData), Context(context), AllocationGuard(guard){
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple

#pragma once

#include "context.h"
#include "filters.h"

#include <ydb/core/tx/columnshard/column_fetching/cache_policy.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates/common.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering::NPrivate {

class TEvFilterRequestResourcesAllocated
    : public NActors::TEventLocal<TEvFilterRequestResourcesAllocated, NColumnShard::TEvPrivate::EvFilterRequestResourcesAllocated> {
private:
    YDB_READONLY_DEF(std::shared_ptr<TFilterAccumulator>, Request);
    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> AllocationGuard;
    std::unique_ptr<TFilterBuildingGuard> RequestGuard;

public:
    TEvFilterRequestResourcesAllocated(const std::shared_ptr<TFilterAccumulator>& request,
        const std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>& guard, std::unique_ptr<TFilterBuildingGuard>&& requestGuard);

    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& ExtractAllocationGuard();
    std::unique_ptr<TFilterBuildingGuard>&& ExtractRequestGuard();
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering::NPrivate

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

class TBuildFilterTaskExecutor;
class TBuildFilterTaskContext {
private:
    TBuildFilterContext Context;
    YDB_READONLY_DEF(std::shared_ptr<TBuildFilterTaskExecutor>, Executor);
    YDB_READONLY_DEF(TBordersBatch, Batch);

public:
    TBuildFilterTaskContext(
        TBuildFilterContext&& context, const std::shared_ptr<TBuildFilterTaskExecutor>& executor, TBordersBatch&& batch);

    const TBuildFilterContext& GetGlobalContext() const;
    TBuildFilterContext&& ExtractGlobalContext();
};

class TDuplicateSourceCacheResult {
private:
    using TColumnData = THashMap<NGeneralCache::TGlobalColumnAddress, std::shared_ptr<NArrow::NAccessor::IChunkedArray>>;
    TColumnData DataByAddress;

public:
    TDuplicateSourceCacheResult(TColumnData&& data);

    THashMap<ui64, std::shared_ptr<NArrow::TGeneralContainer>> ExtractDataByPortion(
        const std::map<ui32, std::shared_ptr<arrow::Field>>& fieldByColumn);
};

class TEvBordersConstructionResult
    : public NActors::TEventLocal<TEvBordersConstructionResult, NColumnShard::TEvPrivate::EvBordersConstructionResult> {
public:
    TBuildFilterTaskContext Context;
    TConclusion<TDuplicateSourceCacheResult> Result;
    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> AllocationGuard;

public:
    TEvBordersConstructionResult(TBuildFilterTaskContext&& context,
        THashMap<NGeneralCache::TGlobalColumnAddress, std::shared_ptr<NArrow::NAccessor::IChunkedArray>>&& columns,
        const std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>& allocationGuard);

    TEvBordersConstructionResult(TBuildFilterTaskContext&& context,
        TConclusion<TDuplicateSourceCacheResult>&& error);
};

class TEvMergeBordersResult
    : public NActors::TEventLocal<TEvMergeBordersResult, NColumnShard::TEvPrivate::EvMergeBordersResult> {
public:
    TBuildFilterTaskContext Context;
    THashMap<ui64, NArrow::TColumnFilter> ReadyFilters;
    TConclusionStatus Result;

public:
    TEvMergeBordersResult(TBuildFilterTaskContext&& context, THashMap<ui64, NArrow::TColumnFilter>&& readyFilters, TConclusionStatus&& conclusion);
};

}
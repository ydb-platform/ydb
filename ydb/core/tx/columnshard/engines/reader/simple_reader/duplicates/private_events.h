#pragma once

#include "context.h"

#include <ydb/core/tx/columnshard/column_fetching/cache_policy.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates/common.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering::NPrivate {

class TEvFilterRequestResourcesAllocated
    : public NActors::TEventLocal<TEvFilterRequestResourcesAllocated, NColumnShard::TEvPrivate::EvFilterRequestResourcesAllocated> {
private:
    YDB_READONLY_DEF(std::shared_ptr<TFilterAccumulator>, Request);
    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> AllocationGuard;

public:
    TEvFilterRequestResourcesAllocated(
        const std::shared_ptr<TFilterAccumulator>& request, const std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>& guard)
        : Request(request)
        , AllocationGuard(guard)
    {
    }

    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& ExtractAllocationGuard() {
        return std::move(AllocationGuard);
    }
};

class TEvFilterConstructionResult
    : public NActors::TEventLocal<TEvFilterConstructionResult, NColumnShard::TEvPrivate::EvFilterConstructionResult> {
private:
    using TFilters = THashMap<TDuplicateMapInfo, NArrow::TColumnFilter>;
    TConclusion<TFilters> Result;

public:
    TEvFilterConstructionResult(TConclusion<TFilters>&& result)
        : Result(std::move(result))
    {
    }

    const TConclusion<TFilters>& GetConclusion() const {
        return Result;
    }

    TFilters&& ExtractResult() {
        return Result.DetachResult();
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering::NPrivate

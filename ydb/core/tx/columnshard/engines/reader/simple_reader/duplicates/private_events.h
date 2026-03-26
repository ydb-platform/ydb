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
        const std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>& guard, std::unique_ptr<TFilterBuildingGuard>&& requestGuard)
        : Request(request)
        , AllocationGuard(guard)
        , RequestGuard(std::move(requestGuard))
    {
        AFL_VERIFY(RequestGuard);
    }

    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& ExtractAllocationGuard() {
        return std::move(AllocationGuard);
    }
    std::unique_ptr<TFilterBuildingGuard>&& ExtractRequestGuard() {
        AFL_VERIFY(RequestGuard);
        return std::move(RequestGuard);
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering::NPrivate

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {
    
class TBuildFilterTaskExecutor;
class TBuildFilterTaskContext {
private:
    TBuildFilterContext Context;
    YDB_READONLY_DEF(std::shared_ptr<TBuildFilterTaskExecutor>, Executor);
    YDB_READONLY_DEF(std::vector<TIntervalInfo>, Intervals);
    YDB_READONLY_DEF(THashSet<ui64>, RequiredPortions);

public:
    TBuildFilterTaskContext(
        TBuildFilterContext&& context, const std::shared_ptr<TBuildFilterTaskExecutor>& executor, std::vector<TIntervalInfo>&& intervals, THashSet<ui64>&& portions)
        : Context(std::move(context))
        , Executor(executor)
        , Intervals(std::move(intervals))
        , RequiredPortions(std::move(portions))
    {
    }

    const TBuildFilterContext& GetGlobalContext() const {
        return Context;
    }

    TBuildFilterContext&& ExtractGlobalContext() {
        return std::move(Context);
    }
};

class TDuplicateSourceCacheResult {
private:
    using TColumnData = THashMap<NGeneralCache::TGlobalColumnAddress, std::shared_ptr<NArrow::NAccessor::IChunkedArray>>;
    TColumnData DataByAddress;

public:
    TDuplicateSourceCacheResult(TColumnData&& data)
        : DataByAddress(std::move(data))
    {
    }

    THashMap<ui64, std::shared_ptr<NArrow::TGeneralContainer>> ExtractDataByPortion(
        const std::map<ui32, std::shared_ptr<arrow::Field>>& fieldByColumn) {
        THashMap<ui64, std::shared_ptr<NArrow::TGeneralContainer>> dataByPortion;
        std::vector<std::shared_ptr<arrow::Field>> fields;
        for (const auto& [_, field] : fieldByColumn) {
            fields.emplace_back(field);
        }

        THashMap<ui64, THashMap<ui32, std::shared_ptr<NArrow::NAccessor::IChunkedArray>>> columnsByPortion;
        for (auto&& [address, data] : DataByAddress) {
            AFL_VERIFY(columnsByPortion[address.GetPortionId()].emplace(address.GetColumnId(), data).second);
        }

        for (auto& [portion, columns] : columnsByPortion) {
            std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> sortedColumns;
            for (const auto& [columnId, _] : fieldByColumn) {
                auto column = columns.FindPtr(columnId);
                AFL_VERIFY(column);
                sortedColumns.emplace_back(*column);
            }
            std::shared_ptr<NArrow::TGeneralContainer> container =
                std::make_shared<NArrow::TGeneralContainer>(fields, std::move(sortedColumns));
            AFL_VERIFY(dataByPortion.emplace(portion, std::move(container)).second);
        }

        return dataByPortion;
    }
};

class TEvIntervalConstructionResult
    : public NActors::TEventLocal<TEvIntervalConstructionResult, NColumnShard::TEvPrivate::EvIntervalConstructionResult> {
public:
    TBuildFilterTaskContext Context;
    TConclusion<TDuplicateSourceCacheResult> Result;
    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> AllocationGuard;
    std::optional<TJobStatus::TResultInFlightGuard> ResultGuard;

public:
    TEvIntervalConstructionResult(TBuildFilterTaskContext&& context,
        THashMap<NGeneralCache::TGlobalColumnAddress, std::shared_ptr<NArrow::NAccessor::IChunkedArray>>&& columns,
        const std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>& allocationGuard)
        : Context(std::move(context))
        , Result(std::move(columns))
        , AllocationGuard(allocationGuard)
    {}
    
    TEvIntervalConstructionResult(TBuildFilterTaskContext&& context,
        TConclusion<TDuplicateSourceCacheResult>&& error,
        std::optional<TJobStatus::TResultInFlightGuard>&& resultGuard)
        : Context(std::move(context))
        , Result(std::move(error))
        , ResultGuard(std::move(resultGuard))
    {}
};

}
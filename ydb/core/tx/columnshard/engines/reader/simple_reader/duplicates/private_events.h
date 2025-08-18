#pragma once

#include "context.h"

#include <ydb/core/tx/columnshard/column_fetching/cache_policy.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates/common.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering::NPrivate {

class TEvFilterRequestResourcesAllocated
    : public NActors::TEventLocal<TEvFilterRequestResourcesAllocated, NColumnShard::TEvPrivate::EvFilterRequestResourcesAllocated> {
private:
    YDB_READONLY_DEF(std::shared_ptr<TInternalFilterConstructor>, Request);
    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> AllocationGuard;

public:
    TEvFilterRequestResourcesAllocated(
        const std::shared_ptr<TInternalFilterConstructor>& request, const std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>& guard)
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
        if (Result.IsSuccess()) {
            for (const auto& [info, filter] : *Result) {
                AFL_VERIFY(!!filter.GetRecordsCount() && filter.GetRecordsCountVerified() == info.GetRows().NumRows())(
                                                                                             "filter", filter.GetRecordsCount().value_or(0))(
                                                                                             "info", info.DebugString());
            }
        }
    }

    const TConclusion<TFilters>& GetConclusion() const {
        return Result;
    }

    TFilters&& ExtractResult() {
        return Result.DetachResult();
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
            for (const auto& [columnId, field] : fieldByColumn) {
                auto column = columns.FindPtr(columnId);
                AFL_VERIFY(column);
                sortedColumns.emplace_back(*column);
            }
            std::shared_ptr<NArrow::TGeneralContainer> container = std::make_shared<NArrow::TGeneralContainer>(fields, std::move(sortedColumns));
            AFL_VERIFY(dataByPortion.emplace(portion, std::move(container)).second);
        }

        return dataByPortion;
    }
};

class TEvDuplicateSourceCacheResult
    : public NActors::TEventLocal<TEvDuplicateSourceCacheResult, NColumnShard::TEvPrivate::EvDuplicateSourceCacheResult> {
private:
    TConclusion<TDuplicateSourceCacheResult> Result;
    YDB_READONLY_DEF(std::shared_ptr<TInternalFilterConstructor>, Context);
    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> AllocationGuard;

public:
    TEvDuplicateSourceCacheResult(const std::shared_ptr<TInternalFilterConstructor>& context, TDuplicateSourceCacheResult&& data,
        std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& allocationGuard)
        : Result(std::move(data))
        , Context(context)
        , AllocationGuard(std::move(allocationGuard))
    {
        AFL_VERIFY(!!AllocationGuard);
    }

    TEvDuplicateSourceCacheResult(const std::shared_ptr<TInternalFilterConstructor>& context, const TString& errorMessage)
        : Result(TConclusionStatus::Fail(errorMessage))
        , Context(context)
    {
    }

    const TConclusion<TDuplicateSourceCacheResult>& GetConclusion() const {
        return Result;
    }

    TDuplicateSourceCacheResult&& ExtractResult() {
        return Result.DetachResult();
    }

    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> ExtractAllocationGuard() {
        AFL_VERIFY(!!AllocationGuard);
        auto result = std::move(AllocationGuard);
        AllocationGuard.reset();
        return result;
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering::NPrivate

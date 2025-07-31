#pragma once

#include "context.h"

#include <ydb/core/tx/columnshard/column_fetching/cache_policy.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates/common.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering::NPrivate {

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

class TEvDuplicateSourceCacheResult
    : public NActors::TEventLocal<TEvDuplicateSourceCacheResult, NColumnShard::TEvPrivate::EvDuplicateSourceCacheResult> {
private:
    using TColumnData = THashMap<NGeneralCache::TGlobalColumnAddress, std::shared_ptr<NArrow::NAccessor::IChunkedArray>>;
    TConclusion<TColumnData> Result;
    YDB_READONLY_DEF(std::shared_ptr<TInternalFilterConstructor>, Context);
    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> AllocationGuard;

public:
    TEvDuplicateSourceCacheResult(const std::shared_ptr<TInternalFilterConstructor>& context, TColumnData&& data,
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

    const TConclusion<TColumnData>& GetConclusion() const {
        return Result;
    }

    TColumnData&& ExtractResult() {
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

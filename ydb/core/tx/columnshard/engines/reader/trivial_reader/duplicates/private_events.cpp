#include "private_events.h"

namespace NKikimr::NOlap::NReader::NTrivial::NDuplicateFiltering::NPrivate {

TEvFilterRequestResourcesAllocated::TEvFilterRequestResourcesAllocated(const std::shared_ptr<TFilterAccumulator>& request,
    const std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>& guard, std::unique_ptr<TFilterBuildingGuard>&& requestGuard)
    : Request(request)
    , AllocationGuard(guard)
    , RequestGuard(std::move(requestGuard))
{
    AFL_VERIFY(RequestGuard);
}

std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& TEvFilterRequestResourcesAllocated::ExtractAllocationGuard() {
    return std::move(AllocationGuard);
}

std::unique_ptr<TFilterBuildingGuard>&& TEvFilterRequestResourcesAllocated::ExtractRequestGuard() {
    AFL_VERIFY(RequestGuard);
    return std::move(RequestGuard);
}

}   // namespace NKikimr::NOlap::NReader::NTrivial::NDuplicateFiltering::NPrivate

namespace NKikimr::NOlap::NReader::NTrivial::NDuplicateFiltering {

TBuildFilterTaskContext::TBuildFilterTaskContext(
    TBuildFilterContext&& context, const std::shared_ptr<TBuildFilterTaskExecutor>& executor, TBordersBatch&& batch)
    : Context(std::move(context))
    , Executor(executor)
    , Batch(std::move(batch))
{
}

const TBuildFilterContext& TBuildFilterTaskContext::GetGlobalContext() const {
    return Context;
}

TBuildFilterContext&& TBuildFilterTaskContext::ExtractGlobalContext() {
    return std::move(Context);
}

TDuplicateSourceCacheResult::TDuplicateSourceCacheResult(TColumnData&& data)
    : DataByAddress(std::move(data))
{
}

THashMap<ui64, std::shared_ptr<NArrow::TGeneralContainer>> TDuplicateSourceCacheResult::ExtractDataByPortion(
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

TEvBordersConstructionResult::TEvBordersConstructionResult(TBuildFilterTaskContext&& context,
    THashMap<NGeneralCache::TGlobalColumnAddress, std::shared_ptr<NArrow::NAccessor::IChunkedArray>>&& columns,
    const std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>& allocationGuard)
    : Context(std::move(context))
    , Result(std::move(columns))
    , AllocationGuard(allocationGuard)
{}

TEvBordersConstructionResult::TEvBordersConstructionResult(TBuildFilterTaskContext&& context,
    TConclusion<TDuplicateSourceCacheResult>&& error)
    : Context(std::move(context))
    , Result(std::move(error))
{}

TEvMergeBordersResult::TEvMergeBordersResult(TBuildFilterTaskContext&& context, THashMap<ui64, NArrow::TColumnFilter>&& readyFilters, TConclusionStatus&& conclusion)
    : Context(std::move(context))
    , ReadyFilters(std::move(readyFilters))
    , Result(std::move(conclusion)) {
}

}   // namespace NKikimr::NOlap::NReader::NTrivial::NDuplicateFiltering

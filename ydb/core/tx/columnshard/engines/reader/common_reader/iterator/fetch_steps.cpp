#include "fetch_steps.h"
#include "source.h"

#include <ydb/core/formats/arrow/common/container.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract/index_info.h>
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

#include <ydb/library/formats/arrow/simple_arrays_cache.h>

#include <ranges>

namespace NKikimr::NOlap::NReader::NCommon {

TConclusion<bool> TColumnBlobsFetchingStep::DoExecuteInplace(
    const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const {
    return !source->StartFetchingColumns(source, step, Columns);
}

ui64 TColumnBlobsFetchingStep::GetProcessingDataSize(const std::shared_ptr<IDataSource>& source) const {
    return source->GetColumnBlobBytes(Columns.GetColumnIds());
}

TConclusion<bool> TAssemblerStep::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    source->AssembleColumns(Columns);
    return true;
}

ui64 TAssemblerStep::GetProcessingDataSize(const std::shared_ptr<IDataSource>& source) const {
    return source->GetColumnRawBytes(Columns->GetColumnIds());
}

TConclusion<bool> TOptionalAssemblerStep::DoExecuteInplace(
    const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    source->AssembleColumns(Columns, !source->IsSourceInMemory());
    return true;
}

ui64 TOptionalAssemblerStep::GetProcessingDataSize(const std::shared_ptr<IDataSource>& source) const {
    return source->GetColumnsVolume(Columns->GetColumnIds(), EMemType::RawSequential);
}

bool TAllocateMemoryStep::TFetchingStepAllocation::DoOnAllocated(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard,
    const std::shared_ptr<NGroupedMemoryManager::IAllocation>& /*allocation*/) {
    auto data = Source.lock();
    if (!data || data->GetContext()->IsAborted()) {
        guard->Release();
        return false;
    }
    if (StageIndex == EStageFeaturesIndexes::Accessors) {
        data->MutableStageData().SetAccessorsGuard(std::move(guard));
    } else {
        data->RegisterAllocationGuard(std::move(guard));
    }
    Step.Next();
    auto task = std::make_shared<TStepAction>(data, std::move(Step), data->GetContext()->GetCommonContext()->GetScanActorId());
    NConveyor::TScanServiceOperator::SendTaskToExecute(task, data->GetContext()->GetCommonContext()->GetConveyorProcessId());
    return true;
}

TAllocateMemoryStep::TFetchingStepAllocation::TFetchingStepAllocation(
    const std::shared_ptr<IDataSource>& source, const ui64 mem, const TFetchingScriptCursor& step, const EStageFeaturesIndexes stageIndex)
    : TBase(mem)
    , Source(source)
    , Step(step)
    , TasksGuard(source->GetContext()->GetCommonContext()->GetCounters().GetResourcesAllocationTasksGuard())
    , StageIndex(stageIndex) {
}

void TAllocateMemoryStep::TFetchingStepAllocation::DoOnAllocationImpossible(const TString& errorMessage) {
    auto sourcePtr = Source.lock();
    if (sourcePtr) {
        sourcePtr->GetContext()->GetCommonContext()->AbortWithError(
            "cannot allocate memory for step " + Step.GetName() + ": '" + errorMessage + "'");
    }
}

TConclusion<bool> TAllocateMemoryStep::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const {
    ui64 size = PredefinedSize.value_or(0);
    for (auto&& i : Packs) {
        ui32 sizeLocal = source->GetColumnsVolume(i.GetColumns().GetColumnIds(), i.GetMemType());
        if (source->GetStageData().GetUseFilter() && i.GetMemType() != EMemType::Blob && source->GetContext()->GetReadMetadata()->HasLimit() && 
            (HasAppData() && !AppDataVerified().ColumnShardConfig.GetUseSlicesFilter())) {
            const ui32 filtered =
                source->GetStageData().GetFilteredCount(source->GetRecordsCount(), source->GetContext()->GetReadMetadata()->GetLimitRobust());
            if (filtered < source->GetRecordsCount()) {
                sizeLocal = sizeLocal * 1.0 * filtered / source->GetRecordsCount();
            }
        }
        size += sizeLocal;
    }

    auto allocation = std::make_shared<TFetchingStepAllocation>(source, size, step, StageIndex);
    NGroupedMemoryManager::TScanMemoryLimiterOperator::SendToAllocation(source->GetContext()->GetProcessMemoryControlId(),
        source->GetContext()->GetCommonContext()->GetScanId(), source->GetMemoryGroupId(), { allocation }, (ui32)StageIndex);
    return false;
}

ui64 TAllocateMemoryStep::GetProcessingDataSize(const std::shared_ptr<IDataSource>& /*source*/) const {
    return 0;
}

NKikimr::TConclusion<bool> TBuildStageResultStep::DoExecuteInplace(
    const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    source->BuildStageResult(source);
    return true;
}

TConclusion<bool> TTtlFilter::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const {
    AFL_VERIFY(source->GetContext()->GetReadMetadata()->GetTtlBound());
    const auto& bound = *source->GetContext()->GetReadMetadata()->GetTtlBound();
    const auto& columnName = source->GetContext()->GetReadMetadata()->GetColumnNameDef(bound.GetColumnId());
    AFL_VERIFY(columnName)("column_id", bound.GetColumnId());
    auto column = source->GetStageData().GetTable()->GetAccessorByNameVerified(*columnName);
    if (source->GetContext()->GetReadMetadata()->GetIndexInfo().GetPKFirstColumnId() == bound.GetColumnId()) {
        if (NArrow::ScalarLess(bound.GetLargestExpiredScalar(), column->GetScalar(0))) {
            return true;
        }
        if (!NArrow::ScalarLess(bound.GetLargestExpiredScalar(), column->GetScalar(source->GetRecordsCount() - 1))) {
            source->MutableStageData().CutFilter(source->GetRecordsCount(), 0, false);
            return true;
        }

        const auto range = std::ranges::iota_view((ui64)0u, column->GetRecordsCount());
        auto findBound = std::upper_bound(range.begin(), range.end(), bound.GetLargestExpiredScalar(),
            [&column](const std::shared_ptr<arrow::Scalar>& bound, const ui64 index) {
                return NArrow::ScalarLess(bound, column->GetScalar(index));
            });

        ui64 expiredCount;
        if (findBound == range.end()) {
            expiredCount = range.size();
        } else {
            expiredCount = *findBound;
        }

        source->MutableStageData().CutFilter(source->GetRecordsCount(), source->GetRecordsCount() - expiredCount, true);
    } else {
        // Not implemented
        AFL_VERIFY(false)("first_pk", source->GetContext()->GetReadMetadata()->GetIndexInfo().GetPKFirstColumnId())("ttl", bound.GetColumnId());
    }
    return true;
}

}   // namespace NKikimr::NOlap::NReader::NCommon

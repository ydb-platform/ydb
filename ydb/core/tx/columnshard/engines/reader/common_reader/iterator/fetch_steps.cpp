#include "fetch_steps.h"
#include "source.h"

#include <ydb/core/formats/arrow/common/container.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract/index_info.h>
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

#include <ydb/library/formats/arrow/simple_arrays_cache.h>

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
        if (data) {
            FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, data->AddEvent("fail_malloc"));
        }
        guard->Release();
        return false;
    }
    if (StageIndex == EStageFeaturesIndexes::Accessors) {
        data->MutableStageData().SetAccessorsGuard(std::move(guard));
    } else {
        data->RegisterAllocationGuard(std::move(guard));
    }
    Step.Next();
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, data->AddEvent("fmalloc"));
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
        FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, sourcePtr->AddEvent("fail_malloc"));
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
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, source->AddEvent("smalloc"));
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

}   // namespace NKikimr::NOlap::NReader::NCommon

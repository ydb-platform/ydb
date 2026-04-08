#include "fetch_steps.h"
#include "source.h"

#include <ydb/core/formats/arrow/common/container.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract/index_info.h>
#include <ydb/core/tx/columnshard/engines/reader/tracing/data_source_probes.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

#include <ydb/library/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NOlap::NReader::NCommon {

LWTRACE_USING(YDB_CS_DATA_SOURCE);

TConclusion<bool> TColumnBlobsFetchingStep::DoExecuteInplace(
    const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const {
    const TDuration durationMs = source->GetAndResetWaitDuration();
    
    LWTRACK(ColumnBlobsFetchingStart, source->GetDataSourceOrbit(), source->GetRawPathId(), source->GetTabletId(),
            source->GetTxId(), source->GetSourceIdx(), step.GetStepIndex(),
            step.GetTracingName(), durationMs, Columns.GetColumnsCount(), source->GetRecordsCount());
    
    const TMonotonic start = TMonotonic::Now();
    auto result = !source->StartFetchingColumns(source, step, Columns);
    const TDuration executionDurationMs = TMonotonic::Now() - start;
    source->AddExecutionDuration(executionDurationMs);
    
    ui64 bytesRead = source->GetColumnBlobBytes(Columns.GetColumnIds());
    source->AddBytesRead(bytesRead);
    
    const TDuration finishDurationMs = source->GetAndResetWaitDuration();
    LWTRACK(ColumnBlobsFetchingFinish, source->GetDataSourceOrbit(), source->GetRawPathId(), source->GetTabletId(),
            source->GetTxId(), source->GetSourceIdx(), step.GetStepIndex(),
            step.GetTracingName(), finishDurationMs, executionDurationMs, Columns.GetColumnsCount(), bytesRead, source->GetRecordsCount());
    
    return result;
}

ui64 TColumnBlobsFetchingStep::GetProcessingDataSize(const std::shared_ptr<IDataSource>& source) const {
    return source->GetColumnBlobBytes(Columns.GetColumnIds());
}

TConclusion<bool> TAssemblerStep::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const {
    const TDuration durationMs = source->GetAndResetWaitDuration();
    
    LWTRACK(AssemblerStepStart, source->GetDataSourceOrbit(), source->GetRawPathId(), source->GetTabletId(),
            source->GetTxId(), source->GetSourceIdx(), step.GetStepIndex(),
            step.GetTracingName(), durationMs, Columns->GetColumnsCount(), source->GetRecordsCount());
    
    const TMonotonic start = TMonotonic::Now();
    source->AssembleColumns(Columns);
    const TDuration executionDurationMs = TMonotonic::Now() - start;
    source->AddExecutionDuration(executionDurationMs);
    
    ui64 bytesAssembled = source->GetColumnRawBytes(Columns->GetColumnIds());
    
    const TDuration finishDurationMs = source->GetAndResetWaitDuration();
    LWTRACK(AssemblerStepFinish, source->GetDataSourceOrbit(), source->GetRawPathId(), source->GetTabletId(),
            source->GetTxId(), source->GetSourceIdx(), step.GetStepIndex(),
            step.GetTracingName(), finishDurationMs, executionDurationMs, Columns->GetColumnsCount(), bytesAssembled, source->GetRecordsCount());
    
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
    if (StageIndex == NArrow::NSSA::IMemoryCalculationPolicy::EStage::Accessors) {
//        data->SetAccessorsGuard( std::move(guard));
    } else {
        data->RegisterAllocationGuard(std::move(guard));
    }
    if (NeedNextStep) {
        Step.Next();
    }
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, data->AddEvent("fmalloc"));
    auto convProcId = data->GetContext()->GetCommonContext()->GetConveyorProcessId();
    auto task = std::make_shared<TStepAction>(std::move(data), std::move(Step), data->GetContext()->GetCommonContext()->GetScanActorId(), false);
    NConveyorComposite::TScanServiceOperator::SendTaskToExecute(task, convProcId);
    return true;
}

TAllocateMemoryStep::TFetchingStepAllocation::TFetchingStepAllocation(const std::shared_ptr<IDataSource>& source, const ui64 mem,
    const TFetchingScriptCursor& step, const NArrow::NSSA::IMemoryCalculationPolicy::EStage stageIndex, const bool needNextStep)
    : TBase(mem)
    , Source(source)
    , Step(step)
    , TasksGuard(source->GetContext()->GetCommonContext()->GetCounters().GetResourcesAllocationTasksGuard())
    , StageIndex(stageIndex)
    , NeedNextStep(needNextStep) {
}

void TAllocateMemoryStep::TFetchingStepAllocation::DoOnAllocationImpossible(const TString& errorMessage) {
    auto sourcePtr = Source.lock();
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "allocation_impossible")("error", errorMessage);
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

    const TDuration durationMs = source->GetAndResetWaitDuration();
    LWTRACK(MemoryAllocationStart, source->GetDataSourceOrbit(), source->GetRawPathId(), source->GetTabletId(),
            source->GetTxId(), source->GetSourceIdx(), step.GetStepIndex(), step.GetTracingName(), durationMs, size);
    
    const TMonotonic start = TMonotonic::Now();
    auto allocation = std::make_shared<TFetchingStepAllocation>(source, size, step, StageIndex);
    const TDuration executionDurationMs = TMonotonic::Now() - start;
    
    const TDuration finishDurationMs = source->GetAndResetWaitDuration();
    LWTRACK(MemoryAllocationFinish, source->GetDataSourceOrbit(), source->GetRawPathId(), source->GetTabletId(),
            source->GetTxId(), source->GetSourceIdx(), step.GetStepIndex(),
            step.GetTracingName(), finishDurationMs, executionDurationMs, size, true);
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

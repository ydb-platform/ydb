#include "fetch_steps.h"
#include "source.h"

#include <ydb/core/formats/arrow/container/container.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/tracing/data_source_probes.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract/index_info.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

#include <ydb/library/formats/arrow/simple_arrays_cache.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_SCAN

namespace NKikimr::NOlap::NReader::NCommon {

LWTRACE_USING(YDB_CS_DATA_SOURCE);

void TColumnBlobsFetchingStep::ReportTracing(IDataSource& source, const TFetchingScriptCursor& step, const TDuration executionDurationMs,
    const ui64 blobBytes, const ui64 rawBytes) const {
    LWTRACK(ColumnBlobsFetching, source.GetDataSourceOrbit(), source.GetRawPathId(), source.GetTabletId(), source.GetTxId(),
        source.GetSourceId(), step.GetStepIndex(), step.GetTracingName(), source.GetAndResetWaitDuration(), executionDurationMs,
        Columns.GetColumnsCount(), blobBytes, rawBytes, source.GetRecordsCount(), source.GetReservedMemory());
}

TConclusion<TExecutionResult> TColumnBlobsFetchingStep::DoExecuteInplace(IDataSource& source, const TFetchingScriptCursor& step) const {
    const TMonotonic start = TMonotonic::Now();
    auto result = source.StartFetchingColumns(step, Columns);
    const TDuration executionDurationMs = TMonotonic::Now() - start;
    source.AddExecutionDuration(executionDurationMs);

    ui64 blobBytes = source.GetColumnBlobBytes(Columns.GetColumnIds());
    ui64 rawBytes = source.GetColumnRawBytes(Columns.GetColumnIds());
    source.AddBytesRead(blobBytes);
    ReportTracing(source, step, executionDurationMs, blobBytes, rawBytes);

    return result;
}

ui64 TColumnBlobsFetchingStep::GetProcessingDataSize(const IDataSource& source) const {
    return source.GetColumnBlobBytes(Columns.GetColumnIds());
}

void TAssemblerStep::ReportTracing(
    IDataSource& source, const TFetchingScriptCursor& step, const TDuration executionDurationMs, const ui64 bytesAssembled) const {
    const TDuration finishDurationMs = source.GetAndResetWaitDuration();
    LWTRACK(AssemblerStep, source.GetDataSourceOrbit(), source.GetRawPathId(), source.GetTabletId(), source.GetTxId(), source.GetSourceId(),
        step.GetStepIndex(), step.GetTracingName(), finishDurationMs, executionDurationMs, Columns->GetColumnsCount(), bytesAssembled,
        source.GetRecordsCount(), source.GetReservedMemory());
}

TConclusion<TExecutionResult> TAssemblerStep::DoExecuteInplace(IDataSource& source, const TFetchingScriptCursor& step) const {
    const TMonotonic start = TMonotonic::Now();
    source.AssembleColumns(Columns);
    const TDuration executionDurationMs = TMonotonic::Now() - start;
    source.AddExecutionDuration(executionDurationMs);

    ui64 bytesAssembled = source.GetColumnRawBytes(Columns->GetColumnIds());
    ReportTracing(source, step, executionDurationMs, bytesAssembled);

    return TExecutionResult::Done();
}

ui64 TAssemblerStep::GetProcessingDataSize(const IDataSource& source) const {
    return source.GetColumnRawBytes(Columns->GetColumnIds());
}

TConclusion<TExecutionResult> TOptionalAssemblerStep::DoExecuteInplace(IDataSource& source, const TFetchingScriptCursor& /*step*/) const {
    source.AssembleColumns(Columns, !source.IsSourceInMemory());
    return TExecutionResult::Done();
}

ui64 TOptionalAssemblerStep::GetProcessingDataSize(const IDataSource& source) const {
    return source.GetColumnsVolume(Columns->GetColumnIds(), EMemType::RawSequential);
}

void TAllocateMemoryStep::TFetchingStepAllocation::TStartJob::Start(std::unique_ptr<TDataSourceLease> sourceLease) {
    const auto context = sourceLease->GetSource().GetContext();
    const ui64 groupId = sourceLease->GetSource().GetMemoryGroupId();
    auto allocation = std::make_shared<TFetchingStepAllocation>(std::move(sourceLease), Memory, Step, StageIndex, NeedNextStep);
    context->SendToGroupedMemoryAllocation(groupId, { allocation }, (ui32)StageIndex);
}

bool TAllocateMemoryStep::TFetchingStepAllocation::DoOnAllocated(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard,
    const std::shared_ptr<NGroupedMemoryManager::IAllocation>& /*allocation*/) {
    auto& data = SourceLease->GetSource();
    if (data.GetContext()->IsAborted()) {
        FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, data.AddEvent("fail_malloc"));
        guard->Release();
        return false;
    }
    if (StageIndex == NArrow::NSSA::IMemoryCalculationPolicy::EStage::Accessors) {
        //        data->SetAccessorsGuard( std::move(guard));
    } else {
        data.RegisterAllocationGuard(std::move(guard));
    }
    if (NeedNextStep) {
        Step.Next();
    }
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, data.AddEvent("fmalloc"));
    const auto& commonContext = *data.GetContext()->GetCommonContext();
    auto task = std::make_shared<TStepAction>(std::move(SourceLease), std::move(Step), commonContext.GetScanActorId(), false);
    commonContext.SendTaskToExecute(task);
    return true;
}

TAllocateMemoryStep::TFetchingStepAllocation::TFetchingStepAllocation(std::unique_ptr<TDataSourceLease> sourceLease, const ui64 mem,
    const TFetchingScriptCursor& step, const NArrow::NSSA::IMemoryCalculationPolicy::EStage stageIndex, const bool needNextStep)
    : TBase(mem)
    , SourceLease(std::move(sourceLease))
    , Step(step)
    , TasksGuard(SourceLease->GetSource().GetContext()->GetCommonContext()->GetCounters().GetResourcesAllocationTasksGuard())
    , StageIndex(stageIndex)
    , NeedNextStep(needNextStep)
{
}

void TAllocateMemoryStep::TFetchingStepAllocation::DoOnAllocationImpossible(const TString& errorMessage) {
    auto& source = SourceLease->GetSource();
    YDB_LOG_WARN("",
        {"event", "allocation_impossible"},
        {"error", errorMessage});
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, source.AddEvent("fail_malloc"));
    source.GetContext()->GetCommonContext()->AbortWithError("cannot allocate memory for step " + Step.GetName() + ": '" + errorMessage + "'");
}

void TAllocateMemoryStep::ReportTracing(
    IDataSource& source, const TFetchingScriptCursor& step, const TDuration executionDurationMs, const ui64 size) const {
    LWTRACK(MemoryAllocation, source.GetDataSourceOrbit(), source.GetRawPathId(), source.GetTabletId(), source.GetTxId(), source.GetSourceId(),
        step.GetStepIndex(), step.GetTracingName(), source.GetAndResetWaitDuration(), executionDurationMs, size, true,
        source.GetReservedMemory());
}

TConclusion<TExecutionResult> TAllocateMemoryStep::DoExecuteInplace(IDataSource& source, const TFetchingScriptCursor& step) const {
    const TMonotonic start = TMonotonic::Now();
    ui64 size = PredefinedSize.value_or(0);
    for (auto&& i : Packs) {
        ui32 sizeLocal = source.GetColumnsVolume(i.GetColumns().GetColumnIds(), i.GetMemType());
        if (source.GetStageData().GetUseFilter() && i.GetMemType() != EMemType::Blob && source.GetContext()->GetReadMetadata()->HasLimit() &&
            (HasAppData() && !AppDataVerified().ColumnShardConfig.GetUseSlicesFilter())) {
            const ui32 filtered =
                source.GetStageData().GetFilteredCount(source.GetRecordsCount(), source.GetContext()->GetReadMetadata()->GetLimitRobust());
            if (filtered < source.GetRecordsCount()) {
                sizeLocal = sizeLocal * 1.0 * filtered / source.GetRecordsCount();
            }
        }
        size += sizeLocal;
    }
    ReportTracing(source, step, TMonotonic::Now() - start, size);
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, source.AddEvent("smalloc"));
    return TExecutionResult::Pending(std::make_shared<TFetchingStepAllocation::TStartJob>(size, step, StageIndex, true));
}

ui64 TAllocateMemoryStep::GetProcessingDataSize(const IDataSource& /*source*/) const {
    return 0;
}

TConclusion<TExecutionResult> TBuildStageResultStep::DoExecuteInplace(IDataSource& source, const TFetchingScriptCursor& /*step*/) const {
    source.BuildStageResult();
    return TExecutionResult::Done();
}

TExecutionResult StartProgramStepReserveMemory(
    const IDataSource& source, const ui64 sizeToReserve, const NArrow::NSSA::IMemoryCalculationPolicy::EStage stage) {
    return TExecutionResult::Pending(std::make_shared<TAllocateMemoryStep::TFetchingStepAllocation::TStartJob>(
        sizeToReserve, source.GetExecutionContext().GetCursorStep(), stage, false));
}

}   // namespace NKikimr::NOlap::NReader::NCommon

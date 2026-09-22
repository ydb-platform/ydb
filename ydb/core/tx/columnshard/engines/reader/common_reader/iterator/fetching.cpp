#include "constructor.h"
#include "default_fetching.h"
#include "fetch_steps.h"
#include "fetching.h"
#include "source.h"
#include "sub_columns_fetching.h"

#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>
#include <ydb/core/formats/arrow/program/index.h>
#include <ydb/core/formats/arrow/program/original.h>
#include <ydb/core/tx/columnshard/blobs_reader/actor.h>
#include <ydb/core/tx/columnshard/engines/reader/tracing/data_source_probes.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/skip_index/meta.h>

#include <util/string/builder.h>
#include <yql/essentials/minikql/mkql_terminator.h>

namespace NKikimr::NOlap::NReader::NCommon {

LWTRACE_USING(YDB_CS_DATA_SOURCE);

bool TStepAction::DoApply(IDataReader& owner) {
    AFL_VERIFY(FinishedFlag);
    YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_SCAN, "",
        {"event", "apply"});
    auto& source = SourceLease->GetSource();
    source.StartSyncSection();
    source.OnSourceFetchingFinishedSafe(owner, std::move(SourceLease));
    return true;
}

TConclusion<bool> TStepAction::DoExecuteImpl() {
    auto& source = SourceLease->GetSource();
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, source.AddEvent("step_action"));
    if (source.GetContext()->IsAborted()) {
        OnFinished();
        return true;
    }
    auto result = Cursor.Execute(source);
    if (result.IsFail()) {
        OnFinished();
        return result.GetError();
    }
    if (result->IsPending()) {
        auto job = std::dynamic_pointer_cast<IAsyncJob>(result->ExtractPendingJob());
        AFL_VERIFY(job);
        job->Start(std::move(SourceLease));
        return false;
    }
    OnFinished();
    return true;
}

void TStepAction::OnFinished() {
    AFL_VERIFY(!FinishedFlag);
    FinishedFlag = true;
    CacheSourceStats();
}

void TStepAction::CacheSourceStats() {
    auto& source = SourceLease->GetSource();
    CachedBlobBytes = source.ExtractTotalBytesRead();
    CachedRawBytes = source.GetUsedRawBytesOptional();
    CachedFilteredRows = source.GetFilteredRowsCount();
    CachedTotalRows = source.GetRecordsCountOptional().value_or(0);
    CachedTotalReservedBytes = source.GetReservedMemory();
}

TStepAction::TStepAction(std::unique_ptr<TDataSourceLease> sourceLease, TFetchingScriptCursor&& cursor, const NActors::TActorId& ownerActorId,
    const bool changeSyncSection)
    : TBase(ownerActorId, sourceLease->GetSource().GetContext()->GetCommonContext()->GetCounters().GetAssembleTasksGuard())
    , SourceLease(std::move(sourceLease))
    , Cursor(std::move(cursor))
    , CachedSourceId(SourceLease->GetSource().GetSourceId())
{
    if (changeSyncSection) {
        SourceLease->GetSource().StartAsyncSection();
    } else {
        SourceLease->GetSource().CheckAsyncSection();
    }
}

void TProgramStep::ReportTracing(IDataSource& source, const NArrow::NAccessor::TAccessorsCollection& resources,
    const TDuration executionDurationMs, const TString& currentExecutionResult, const ui32 nodeId, const TString& currentCategoryName,
    const std::shared_ptr<NArrow::NSSA::IResourceProcessor>& processor) const {
    const auto& scanOrbit = source.GetContext()->GetCommonContext()->GetScanOrbit();
    if (!NLWTrace::HasShuttles(source.GetDataSourceOrbit()) && !(scanOrbit && NLWTrace::HasShuttles(*scanOrbit)) &&
        !LWPROBE_ENABLED(ProgramConst) && !LWPROBE_ENABLED(ProgramCalculation) && !LWPROBE_ENABLED(ProgramProjection) &&
        !LWPROBE_ENABLED(ProgramFilter) && !LWPROBE_ENABLED(ProgramAggregation) && !LWPROBE_ENABLED(ProgramFetchOriginalData) &&
        !LWPROBE_ENABLED(ProgramAssembleOriginalData) && !LWPROBE_ENABLED(ProgramCheckIndexData) && !LWPROBE_ENABLED(ProgramCheckHeaderData) &&
        !LWPROBE_ENABLED(ProgramStreamLogic) && !LWPROBE_ENABLED(ProgramReserveMemory)) {
        return;
    }
    const auto& step = source.GetExecutionContext().GetCursorStep();
    const auto& prevTracing = source.GetExecutionContext().GetPrevNodeTracing();
    const TString tracingName = prevTracing.CategoryName + " - " + currentCategoryName;
    const TString tracingExecutionResult = prevTracing.ExecutionResult + " - " + currentExecutionResult;
    const TDuration finishDurationMs = source.GetAndResetWaitDuration();
    const ui64 reservedMemory = source.GetReservedMemory();
    const auto processorType = processor->GetProcessorType();
    const TString details = processor->DebugJson().GetStringRobust();

    const ui32 filteredRows = resources.GetRecordsCountActualOptional().value_or(source.GetRecordsCount());
    TString indexStatus = "Unknown";
    ui32 indexFilteredRows = source.GetRecordsCount();
    if (processorType == NArrow::NSSA::EProcessorType::CheckIndexData) {
        auto* indexProcessor = dynamic_cast<const NArrow::NSSA::TIndexCheckerProcessor*>(processor.get());
        if (indexProcessor && source.GetSourceSchemaOptional()) {
            const auto& idxCtx = indexProcessor->GetIndexContext();
            NIndexes::NRequest::TOriginalDataAddress addr(idxCtx.GetColumnId(), idxCtx.GetSubColumnName());
            auto skipIndexes = source.GetSourceSchemaOptional()->GetIndexInfo().FindSkipIndexes(addr, idxCtx.GetOperation());
            bool hasActualIndexData = false;
            if (!skipIndexes.empty() && source.HasPortionAccessor()) {
                std::set<ui32> indexEntityIds;
                for (auto&& skipIdx : skipIndexes) {
                    indexEntityIds.insert(skipIdx->GetIndexId());
                }
                hasActualIndexData = source.GetPortionAccessor().GetIndexBlobBytes(indexEntityIds, false) > 0;
            }
            if (skipIndexes.empty() || !hasActualIndexData) {
                indexStatus = "NoIndex";
                indexFilteredRows = source.GetRecordsCount();
            } else {
                const ui32 outputColumnId = indexProcessor->GetOutputColumnIdOnce();
                const auto& outputAccessor = resources.GetAccessorOptional(outputColumnId);
                if (outputAccessor) {
                    auto* sparsed = dynamic_cast<const NArrow::NAccessor::TSparsedArray*>(outputAccessor.get());
                    if (sparsed && sparsed->GetDefaultValue() && sparsed->GetDefaultValue()->is_valid) {
                        auto* uint8Scalar = dynamic_cast<const arrow::UInt8Scalar*>(sparsed->GetDefaultValue().get());
                        if (uint8Scalar && uint8Scalar->value == 0) {
                            indexStatus = "AllDenied";
                            indexFilteredRows = 0;
                        } else {
                            indexStatus = "AllAccepted";
                            indexFilteredRows = source.GetRecordsCount();
                        }
                    } else {
                        indexStatus = "AllAccepted";
                        indexFilteredRows = source.GetRecordsCount();
                    }
                } else {
                    indexStatus = "Partial";
                    indexFilteredRows = resources.GetFilter().GetFilteredCount().value_or(source.GetRecordsCount());
                }
            }
        }
    }

#define PROGRAM_PROBE_ARGS                                                                                                                 \
    source.GetDataSourceOrbit(), source.GetRawPathId(), source.GetTabletId(), source.GetTxId(), source.GetSourceId(), step.GetStepIndex(), \
        tracingName, nodeId, finishDurationMs, executionDurationMs, filteredRows
#define PROGRAM_PROBE_RESERVED reservedMemory
#define PROGRAM_PROBE_TAIL tracingExecutionResult, details
    switch (processorType) {
        case NArrow::NSSA::EProcessorType::Const:
            LWTRACK(ProgramConst, PROGRAM_PROBE_ARGS, PROGRAM_PROBE_RESERVED, PROGRAM_PROBE_TAIL);
            break;
        case NArrow::NSSA::EProcessorType::Calculation:
            LWTRACK(ProgramCalculation, PROGRAM_PROBE_ARGS, PROGRAM_PROBE_RESERVED, PROGRAM_PROBE_TAIL);
            break;
        case NArrow::NSSA::EProcessorType::Projection:
            LWTRACK(ProgramProjection, PROGRAM_PROBE_ARGS, PROGRAM_PROBE_RESERVED, PROGRAM_PROBE_TAIL);
            break;
        case NArrow::NSSA::EProcessorType::Filter:
        case NArrow::NSSA::EProcessorType::DistinctMarker:
            LWTRACK(ProgramFilter, PROGRAM_PROBE_ARGS, PROGRAM_PROBE_RESERVED, PROGRAM_PROBE_TAIL);
            break;
        case NArrow::NSSA::EProcessorType::Aggregation:
            LWTRACK(ProgramAggregation, PROGRAM_PROBE_ARGS, PROGRAM_PROBE_RESERVED, PROGRAM_PROBE_TAIL);
            break;
        case NArrow::NSSA::EProcessorType::FetchOriginalData: {
            ui64 blobBytes = 0;
            ui64 rawBytes = 0;
            auto* fetchProcessor = dynamic_cast<const NArrow::NSSA::TOriginalColumnDataProcessor*>(processor.get());
            if (fetchProcessor) {
                std::set<ui32> dataColumnIds;
                for (auto&& [colId, addr] : fetchProcessor->GetDataAddresses()) {
                    dataColumnIds.insert(colId);
                }
                if (!dataColumnIds.empty()) {
                    blobBytes += source.GetColumnBlobBytes(dataColumnIds);
                    rawBytes += source.GetColumnRawBytes(dataColumnIds);
                }
                if (!fetchProcessor->GetIndexContext().empty() && source.HasPortionAccessor() && source.GetSourceSchemaOptional()) {
                    const auto& accessor = source.GetPortionAccessor();
                    std::set<ui32> indexEntityIds;
                    const auto& indexInfo = source.GetSourceSchemaOptional()->GetIndexInfo();
                    for (auto&& [colId, idxCtx] : fetchProcessor->GetIndexContext()) {
                        for (auto&& [subCol, ops] : idxCtx.GetOperationsBySubColumn().GetData()) {
                            NIndexes::NRequest::TOriginalDataAddress addr(colId, subCol);
                            for (auto&& op : ops) {
                                for (auto&& skipIdx : indexInfo.FindSkipIndexes(addr, op)) {
                                    indexEntityIds.insert(skipIdx->GetIndexId());
                                }
                            }
                        }
                    }
                    if (!indexEntityIds.empty()) {
                        blobBytes += accessor.GetIndexBlobBytes(indexEntityIds, false);
                        rawBytes += accessor.GetIndexRawBytes(indexEntityIds, false);
                    }
                }
            }
            bool hasSubColumns = false;
            // After ADD COLUMN the portion source schema may not contain the column yet.
            if (fetchProcessor && source.GetSourceSchemaOptional()) {
                for (auto&& [colId, addr] : fetchProcessor->GetDataAddresses()) {
                    if (auto loader = source.GetSourceSchemaOptional()->GetColumnLoaderOptional(colId)) {
                        if (loader->GetAccessorConstructor()->GetType() == NArrow::NAccessor::IChunkedArray::EType::SubColumnsArray) {
                            hasSubColumns = true;
                            break;
                        }
                    }
                }
                if (!hasSubColumns) {
                    source.AddBytesRead(blobBytes);
                }
            }
            LWTRACK(ProgramFetchOriginalData, PROGRAM_PROBE_ARGS, blobBytes, rawBytes, PROGRAM_PROBE_RESERVED, PROGRAM_PROBE_TAIL);
        } break;
        case NArrow::NSSA::EProcessorType::AssembleOriginalData:
            LWTRACK(ProgramAssembleOriginalData, PROGRAM_PROBE_ARGS, PROGRAM_PROBE_RESERVED, PROGRAM_PROBE_TAIL);
            break;
        case NArrow::NSSA::EProcessorType::CheckIndexData:
            LWTRACK(ProgramCheckIndexData, PROGRAM_PROBE_ARGS, indexFilteredRows, indexStatus, PROGRAM_PROBE_RESERVED, PROGRAM_PROBE_TAIL);
            break;
        case NArrow::NSSA::EProcessorType::CheckHeaderData:
            LWTRACK(ProgramCheckHeaderData, PROGRAM_PROBE_ARGS, PROGRAM_PROBE_RESERVED, PROGRAM_PROBE_TAIL);
            break;
        case NArrow::NSSA::EProcessorType::StreamLogic:
            LWTRACK(ProgramStreamLogic, PROGRAM_PROBE_ARGS, PROGRAM_PROBE_RESERVED, PROGRAM_PROBE_TAIL);
            break;
        case NArrow::NSSA::EProcessorType::ReserveMemory:
            LWTRACK(ProgramReserveMemory, PROGRAM_PROBE_ARGS, reservedMemory, PROGRAM_PROBE_RESERVED, PROGRAM_PROBE_TAIL);
            break;
        case NArrow::NSSA::EProcessorType::Unknown:
            break;
    }
#undef PROGRAM_PROBE_ARGS
#undef PROGRAM_PROBE_RESERVED
#undef PROGRAM_PROBE_TAIL
}

TConclusion<TExecutionResult> TProgramStep::DoExecuteInplace(IDataSource& source, const TFetchingScriptCursor& step) const {
    auto& executionContext = source.MutableExecutionContext();
    if (executionContext.HasProgramIterator()) {
        executionContext.GetProgramIteratorVerified()->Next();
        executionContext.OnFinishProgramStepExecution();
    } else {
        executionContext.Start(source, Program, step);
    }
    const auto iterator = executionContext.GetProgramIteratorVerified();
    const auto visitor = executionContext.GetExecutionVisitorVerified();
    const auto& counters = source.GetContext()->GetCommonContext()->GetCounters();
    while (iterator->IsValid()) {
        {
            auto conclusion = iterator->Next();
            if (conclusion.IsFail()) {
                return conclusion;
            }
        }
        if (!visitor->GetExecutionNode()) {
            if (iterator->IsValid()) {
                GetSignals(iterator->GetCurrentNodeId())->OnSkipGraphNode(source.GetRecordsCount());
                counters.OnSkipGraphNode(iterator->GetCurrentNode().GetIdentifier());
            }
            continue;
        }
        const ui32 nodeId = iterator->GetCurrentNodeId();
        AFL_VERIFY(visitor->GetExecutionNode()->GetIdentifier() == nodeId);
        const TString categoryName = iterator->GetCurrentNode().GetSignalCategoryName();
        const auto processor = iterator->GetProcessorVerified();
        const auto& signals = GetSignals(nodeId);
        executionContext.OnStartProgramStepExecution(nodeId, signals);

        const TMonotonic start = TMonotonic::Now();
        auto conclusion = visitor->Execute();
        const TDuration executionDuration = TMonotonic::Now() - start;
        counters.AddExecutionDuration(executionDuration);
        signals->AddExecutionDuration(executionDuration);
        source.AddExecutionDuration(executionDuration);

        const TString executionResult = conclusion.IsFail() ? "Fail" : conclusion->DebugString();
        ReportTracing(source, visitor->MutableContext().GetResources(), executionDuration, executionResult, nodeId, categoryName, processor);
        executionContext.SetPrevNodeTracing(categoryName, executionResult);
        if (conclusion.IsFail()) {
            executionContext.OnFailedProgramStepExecution();
            return conclusion;
        }
        if (conclusion->IsPending()) {
            return conclusion;
        }
        executionContext.OnFinishProgramStepExecution();
        signals->OnExecuteGraphNode(source.GetRecordsCount());
        counters.OnExecuteGraphNode(iterator->GetCurrentNode().GetIdentifier());
        if (visitor->MutableContext().GetResources().GetRecordsCountActualOptional() == 0) {
            visitor->MutableContext().MutableResources().Clear();
            break;
        }
    }
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, source.AddEvent("fgraph"));
    YDB_LOG_DEBUG_COMP(NKikimrServices::SSA_GRAPH_EXECUTION, "",
        {"graphConstructed", Program->DebugDOT(visitor->GetExecutedIds())});
    source.MutableStageData().ReturnTable(visitor->MutableContext().ExtractResources());
    return TExecutionResult::Done();
}

const std::shared_ptr<TFetchingStepSignals>& TProgramStep::GetSignals(const ui32 nodeId) const {
    auto it = Signals.find(nodeId);
    AFL_VERIFY(it != Signals.end())("node_id", nodeId);
    return it->second;
}

}   // namespace NKikimr::NOlap::NReader::NCommon

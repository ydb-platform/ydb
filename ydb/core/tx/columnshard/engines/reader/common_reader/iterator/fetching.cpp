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
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "apply");
    Source->StartSyncSection();
    Source->OnSourceFetchingFinishedSafe(owner, Source);
    return true;
}

TConclusion<bool> TStepAction::DoExecuteImpl() {
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, Source->AddEvent("step_action"));
    if (Source->GetContext()->IsAborted()) {
        AFL_VERIFY(!FinishedFlag);
        FinishedFlag = true;
        CacheSourceStats();
        return true;
    }
    auto executeResult = Cursor.Execute(Source);
    if (executeResult.IsFail()) {
        AFL_VERIFY(!FinishedFlag);
        FinishedFlag = true;
        CacheSourceStats();
        return executeResult;
    }
    if (*executeResult) {
        AFL_VERIFY(!FinishedFlag);
        FinishedFlag = true;
        CacheSourceStats();
    }
    return FinishedFlag;
}

void TStepAction::CacheSourceStats() {
    CachedBlobBytes = Source->ExtractTotalBytesRead();
    CachedRawBytes = Source->GetUsedRawBytesOptional();
    CachedFilteredRows = Source->GetFilteredRowsCount();
    CachedTotalRows = Source->GetRecordsCount();
    CachedTotalReservedBytes = Source->GetReservedMemory();
}

TStepAction::TStepAction(
    std::shared_ptr<IDataSource>&& source, TFetchingScriptCursor&& cursor, const NActors::TActorId& ownerActorId, const bool changeSyncSection)
    : TBase(ownerActorId, source->GetContext()->GetCommonContext()->GetCounters().GetAssembleTasksGuard())
    , Source(std::move(source))
    , Cursor(std::move(cursor))
    , CachedSourceId(Source->GetDeprecatedPortionId())
{
    if (changeSyncSection) {
        Source->StartAsyncSection();
    } else {
        Source->CheckAsyncSection();
    }
}

void TProgramStep::ReportTracing(const std::shared_ptr<IDataSource>& source, const TDuration executionDurationMs,
    const TString& currentExecutionResult, const ui32 nodeId, const TString& currentCategoryName,
    const std::shared_ptr<NArrow::NSSA::IResourceProcessor>& processor, const ui64 reservedMemory) const {
    if (!processor) {
        return;
    }
    const auto& scanOrbit = source->GetContext()->GetCommonContext()->GetScanOrbit();
    if (!NLWTrace::HasShuttles(source->GetDataSourceOrbit()) && !(scanOrbit && NLWTrace::HasShuttles(*scanOrbit)) &&
        !LWPROBE_ENABLED(ProgramConst) && !LWPROBE_ENABLED(ProgramCalculation) && !LWPROBE_ENABLED(ProgramProjection) &&
        !LWPROBE_ENABLED(ProgramFilter) && !LWPROBE_ENABLED(ProgramAggregation) && !LWPROBE_ENABLED(ProgramFetchOriginalData) &&
        !LWPROBE_ENABLED(ProgramAssembleOriginalData) && !LWPROBE_ENABLED(ProgramCheckIndexData) && !LWPROBE_ENABLED(ProgramCheckHeaderData) &&
        !LWPROBE_ENABLED(ProgramStreamLogic) && !LWPROBE_ENABLED(ProgramReserveMemory)) {
        return;
    }
    const auto& step = source->GetExecutionContext().GetCursorStep();
    const auto prevTracing = source->GetExecutionContext().GetPrevNodeTracing();
    const TString tracingName = prevTracing.CategoryName + " - " + currentCategoryName;
    const TString tracingExecutionResult = prevTracing.ExecutionResult + " - " + currentExecutionResult;
    const TDuration finishDurationMs = source->GetAndResetWaitDuration();
    const auto processorType = processor->GetProcessorType();
    const TString details = processor->DebugJson().GetStringRobust();

    // Pin the visitor for the duration of this function so Stop() on ExecutionContext cannot free it under us.
    // Snapshot scalars only — do not keep references into Resources across further work.
    // Never call GetReservedMemory() here: Execute() may have started a concurrent continuation that
    // mutates ResourceGuards (RegisterAllocationGuard / ClearMemoryGuards) on another worker.
    const auto visitor = source->GetExecutionContext().GetExecutionVisitorOptional();
    ui32 filteredRows = source->GetRecordsCount();
    TString indexStatus = "Unknown";
    ui32 indexFilteredRows = source->GetRecordsCount();
    if (const auto* resources = visitor ? visitor->MutableContext().GetResourcesOptional() : nullptr) {
        filteredRows = resources->GetRecordsCountActualOptional().value_or(source->GetRecordsCount());
    }
    if (processorType == NArrow::NSSA::EProcessorType::CheckIndexData) {
        auto* indexProcessor = dynamic_cast<const NArrow::NSSA::TIndexCheckerProcessor*>(processor.get());
        if (indexProcessor && source->GetSourceSchemaOptional()) {
            const auto& idxCtx = indexProcessor->GetIndexContext();
            NIndexes::NRequest::TOriginalDataAddress addr(idxCtx.GetColumnId(), idxCtx.GetSubColumnName());
            auto skipIndexes = source->GetSourceSchemaOptional()->GetIndexInfo().FindSkipIndexes(addr, idxCtx.GetOperation());
            bool hasActualIndexData = false;
            if (!skipIndexes.empty() && source->HasPortionAccessor()) {
                std::set<ui32> indexEntityIds;
                for (auto&& skipIdx : skipIndexes) {
                    indexEntityIds.insert(skipIdx->GetIndexId());
                }
                hasActualIndexData = source->GetPortionAccessor().GetIndexBlobBytes(indexEntityIds, false) > 0;
            }
            if (skipIndexes.empty() || !hasActualIndexData) {
                indexStatus = "NoIndex";
                indexFilteredRows = source->GetRecordsCount();
            } else if (const auto* resources = visitor ? visitor->MutableContext().GetResourcesOptional() : nullptr) {
                // Re-read resources after non-resource work — concurrent ExtractResources may have cleared them.
                const ui32 outputColumnId = indexProcessor->GetOutputColumnIdOnce();
                const auto& outputAccessor = resources->GetAccessorOptional(outputColumnId);
                if (outputAccessor) {
                    auto* sparsed = dynamic_cast<const NArrow::NAccessor::TSparsedArray*>(outputAccessor.get());
                    if (sparsed && sparsed->GetDefaultValue() && sparsed->GetDefaultValue()->is_valid) {
                        auto* uint8Scalar = dynamic_cast<const arrow::UInt8Scalar*>(sparsed->GetDefaultValue().get());
                        if (uint8Scalar && uint8Scalar->value == 0) {
                            indexStatus = "AllDenied";
                            indexFilteredRows = 0;
                        } else {
                            indexStatus = "AllAccepted";
                            indexFilteredRows = source->GetRecordsCount();
                        }
                    } else {
                        indexStatus = "AllAccepted";
                        indexFilteredRows = source->GetRecordsCount();
                    }
                } else {
                    indexStatus = "Partial";
                    indexFilteredRows = resources->GetFilter().GetFilteredCount().value_or(source->GetRecordsCount());
                }
            }
        }
    }

#define PROGRAM_PROBE_ARGS                                                                                                            \
    source->GetDataSourceOrbit(), source->GetRawPathId(), source->GetTabletId(), source->GetTxId(), source->GetDeprecatedPortionId(), \
        step.GetStepIndex(), tracingName, nodeId, finishDurationMs, executionDurationMs, filteredRows
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
                    blobBytes += source->GetColumnBlobBytes(dataColumnIds);
                    rawBytes += source->GetColumnRawBytes(dataColumnIds);
                }
                if (!fetchProcessor->GetIndexContext().empty() && source->HasPortionAccessor() && source->GetSourceSchemaOptional()) {
                    const auto& accessor = source->GetPortionAccessor();
                    std::set<ui32> indexEntityIds;
                    const auto& indexInfo = source->GetSourceSchemaOptional()->GetIndexInfo();
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
                bool hasSubColumns = false;
                if (source->GetSourceSchemaOptional()) {
                    for (auto&& [colId, addr] : fetchProcessor->GetDataAddresses()) {
                        if (source->GetSourceSchemaOptional()->GetColumnLoaderVerified(colId)->GetAccessorConstructor()->GetType() ==
                            NArrow::NAccessor::IChunkedArray::EType::SubColumnsArray) {
                            hasSubColumns = true;
                            break;
                        }
                    }
                }
                if (!hasSubColumns) {
                    source->AddBytesRead(blobBytes);
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

NO_SANITIZE_THREAD
TConclusion<bool> TProgramStep::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const {
    const bool started = !source->GetExecutionContext().HasProgramIterator();
    if (!source->GetExecutionContext().HasProgramIterator()) {
        source->MutableExecutionContext().Start(source, Program, step);
    }
    auto iterator = source->GetExecutionContext().GetProgramIteratorVerified();
    if (!started) {
        iterator->Next();
        source->MutableExecutionContext().OnFinishProgramStepExecution();
    }
    while (iterator->IsValid()) {
        {
            auto conclusion = iterator->Next();
            if (conclusion.IsFail()) {
                return conclusion;
            }
        }
        if (!source->GetExecutionContext().GetExecutionVisitorVerified()->GetExecutionNode()) {
            if (iterator->IsValid()) {
                GetSignals(iterator->GetCurrentNodeId())->OnSkipGraphNode(source->GetRecordsCount());
                source->GetContext()->GetCommonContext()->GetCounters().OnSkipGraphNode(iterator->GetCurrentNode().GetIdentifier());
            }
            continue;
        }
        AFL_VERIFY(source->GetExecutionContext().GetExecutionVisitorVerified()->GetExecutionNode()->GetIdentifier() == iterator->GetCurrentNodeId());
        const ui32 tracingNodeId = iterator->GetCurrentNodeId();
        const TString tracingCategoryName = iterator->GetCurrentNode().GetSignalCategoryName();
        const auto tracingProcessor = iterator->GetProcessorVerified();
        source->MutableExecutionContext().OnStartProgramStepExecution(tracingNodeId, GetSignals(tracingNodeId));
        auto signals = GetSignals(tracingNodeId);

        // Snapshot before Execute(): allocation callbacks may mutate ResourceGuards concurrently afterwards.
        const ui64 reservedMemoryBeforeExecute = source->GetReservedMemory();
        const TMonotonic start = TMonotonic::Now();
        auto conclusion = source->GetExecutionContext().GetExecutionVisitorVerified()->Execute();
        const TDuration executionDurationMs = TMonotonic::Now() - start;
        source->GetContext()->GetCommonContext()->GetCounters().AddExecutionDuration(executionDurationMs);
        signals->AddExecutionDuration(executionDurationMs);
        source->AddExecutionDuration(executionDurationMs);

        const TString currentExecutionResult = conclusion.IsFail() ? "Fail" : ToString(*conclusion);
        ReportTracing(source, executionDurationMs, currentExecutionResult, tracingNodeId, tracingCategoryName, tracingProcessor,
            reservedMemoryBeforeExecute);
        source->MutableExecutionContext().SetPrevNodeTracing(tracingNodeId, conclusion);
        if (conclusion.IsFail()) {
            source->MutableExecutionContext().OnFailedProgramStepExecution();
            return conclusion;
        }

        // A nested continuation may have finished the shared program (extracted resources / stopped visitor)
        // while Execute() was in progress. Do not keep mutating that shared state from this frame.
        // Pin visitor once — HasExecutionVisitor + GetExecutionVisitorVerified is a TOCTOU with Stop().
        const auto visitor = source->GetExecutionContext().GetExecutionVisitorOptional();
        if (!visitor || !visitor->MutableContext().HasResources()) {
            return false;
        }

        if (*conclusion == NArrow::NSSA::IResourceProcessor::EExecutionResult::InBackground) {
            return false;
        }
        source->MutableExecutionContext().OnFinishProgramStepExecution();
        GetSignals(iterator->GetCurrentNodeId())->OnExecuteGraphNode(source->GetRecordsCount());
        source->GetContext()->GetCommonContext()->GetCounters().OnExecuteGraphNode(iterator->GetCurrentNode().GetIdentifier());
        if (const auto* resources = visitor->MutableContext().GetResourcesOptional();
            resources && resources->GetRecordsCountActualOptional() == 0) {
            visitor->MutableContext().MutableResources().Clear();
            break;
        }
    }
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, source->AddEvent("fgraph"));
    const auto visitor = source->GetExecutionContext().GetExecutionVisitorOptional();
    if (!visitor || !visitor->MutableContext().HasResources()) {
        // Nested continuation already took ownership of progress — do not advance this cursor.
        return false;
    }
    AFL_DEBUG(NKikimrServices::SSA_GRAPH_EXECUTION)("graph_constructed", Program->DebugDOT(visitor->GetExecutedIds()));
    source->MutableStageData().ReturnTable(visitor->MutableContext().ExtractResources());

    return true;
}

const std::shared_ptr<TFetchingStepSignals>& TProgramStep::GetSignals(const ui32 nodeId) const {
    auto it = Signals.find(nodeId);
    AFL_VERIFY(it != Signals.end())("node_id", nodeId);
    return it->second;
}

}   // namespace NKikimr::NOlap::NReader::NCommon

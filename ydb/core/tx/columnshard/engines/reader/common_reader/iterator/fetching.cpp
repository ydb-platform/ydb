#include "constructor.h"
#include "default_fetching.h"
#include "fetch_steps.h"
#include "fetching.h"
#include "source.h"
#include "sub_columns_fetching.h"

#include <ydb/core/tx/columnshard/blobs_reader/actor.h>
#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>
#include <ydb/core/formats/arrow/program/index.h>
#include <ydb/core/formats/arrow/program/original.h>
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
    , CachedSourceId(Source->GetDeprecatedPortionId()) {
    if (changeSyncSection) {
        Source->StartAsyncSection();
    } else {
        Source->CheckAsyncSection();
    }
}

void TProgramStep::ReportTracing(const std::shared_ptr<IDataSource>& source, const TDuration executionDurationMs, const TString& currentExecutionResult) const {
    if (!source->GetExecutionContext().HasProgramIterator()) {
        return;
    }
    auto iterator = source->GetExecutionContext().GetProgramIteratorVerified();
    if (!iterator->IsValid()) {
        return;
    }
    const auto& currentCategoryName = iterator->GetCurrentNode().GetSignalCategoryName();
    const auto& scanOrbit = source->GetContext()->GetCommonContext()->GetScanOrbit();
    if (!NLWTrace::HasShuttles(source->GetDataSourceOrbit())
        && !(scanOrbit && NLWTrace::HasShuttles(*scanOrbit))
        && !LWPROBE_ENABLED(ProgramConst)
        && !LWPROBE_ENABLED(ProgramCalculation)
        && !LWPROBE_ENABLED(ProgramProjection)
        && !LWPROBE_ENABLED(ProgramFilter)
        && !LWPROBE_ENABLED(ProgramAggregation)
        && !LWPROBE_ENABLED(ProgramFetchOriginalData)
        && !LWPROBE_ENABLED(ProgramAssembleOriginalData)
        && !LWPROBE_ENABLED(ProgramCheckIndexData)
        && !LWPROBE_ENABLED(ProgramCheckHeaderData)
        && !LWPROBE_ENABLED(ProgramStreamLogic)
        && !LWPROBE_ENABLED(ProgramReserveMemory)) {
        source->MutableExecutionContext().SetPrevCategoryName(currentCategoryName);
        source->MutableExecutionContext().SetPrevExecutionResult(currentExecutionResult);
        return;
    }
    const auto& step = source->GetExecutionContext().GetCursorStep();
    const TString tracingName = source->GetExecutionContext().GetPrevCategoryName() + " - " + currentCategoryName;
    const TString tracingExecutionResult = source->GetExecutionContext().GetPrevExecutionResult() + " - " + currentExecutionResult;
    const TDuration finishDurationMs = source->GetAndResetWaitDuration();
    const auto& processor = iterator->GetProcessorVerified();
    const auto processorType = processor->GetProcessorType();
    const TString details = processor->DebugJson().GetStringRobust();
    const auto& resources = source->GetExecutionContext().GetExecutionVisitorVerified()->MutableContext().GetResources();
    const ui32 filteredRows = resources.GetRecordsCountActualOptional().value_or(source->GetRecordsCount());
#define PROGRAM_PROBE_ARGS source->GetDataSourceOrbit(), source->GetRawPathId(), source->GetTabletId(), \
                    source->GetTxId(), source->GetDeprecatedPortionId(), step.GetStepIndex(), \
                    tracingName, iterator->GetCurrentNodeId(), finishDurationMs, \
                    executionDurationMs, filteredRows
#define PROGRAM_PROBE_RESERVED source->GetReservedMemory()
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
        case NArrow::NSSA::EProcessorType::FetchOriginalData:
            {
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
                LWTRACK(ProgramFetchOriginalData, PROGRAM_PROBE_ARGS, blobBytes, rawBytes, PROGRAM_PROBE_RESERVED, PROGRAM_PROBE_TAIL);
            }
            break;
        case NArrow::NSSA::EProcessorType::AssembleOriginalData:
            LWTRACK(ProgramAssembleOriginalData, PROGRAM_PROBE_ARGS, PROGRAM_PROBE_RESERVED, PROGRAM_PROBE_TAIL);
            break;
        case NArrow::NSSA::EProcessorType::CheckIndexData:
            {
                TString indexStatus = "Unknown";
                ui32 indexFilteredRows = source->GetRecordsCount();
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
                    } else {
                        // After DoExecute in index.cpp:
                        // - AllDenied/AllAccepted: output column stored, filter NOT modified
                        // - Partial: no output column, filter modified via AddFilter
                        const ui32 outputColumnId = indexProcessor->GetOutputColumnIdOnce();
                        const auto& outputAccessor = resources.GetAccessorOptional(outputColumnId);
                        if (outputAccessor) {
                            // Output column exists → AllDenied or AllAccepted
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
                            // No output column → Partial (filter was applied via AddFilter)
                            indexStatus = "Partial";
                            indexFilteredRows = resources.GetFilter().GetFilteredCount().value_or(source->GetRecordsCount());
                        }
                    }
                }
                LWTRACK(ProgramCheckIndexData, PROGRAM_PROBE_ARGS, indexFilteredRows, indexStatus, PROGRAM_PROBE_RESERVED, PROGRAM_PROBE_TAIL);
            }
            break;
        case NArrow::NSSA::EProcessorType::CheckHeaderData:
            LWTRACK(ProgramCheckHeaderData, PROGRAM_PROBE_ARGS, PROGRAM_PROBE_RESERVED, PROGRAM_PROBE_TAIL);
            break;
        case NArrow::NSSA::EProcessorType::StreamLogic:
            LWTRACK(ProgramStreamLogic, PROGRAM_PROBE_ARGS, PROGRAM_PROBE_RESERVED, PROGRAM_PROBE_TAIL);
            break;
        case NArrow::NSSA::EProcessorType::ReserveMemory:
            LWTRACK(ProgramReserveMemory, PROGRAM_PROBE_ARGS, source->GetReservedMemory(), PROGRAM_PROBE_RESERVED, PROGRAM_PROBE_TAIL);
            break;
        case NArrow::NSSA::EProcessorType::Unknown:
            break;
    }
#undef PROGRAM_PROBE_ARGS
#undef PROGRAM_PROBE_RESERVED
#undef PROGRAM_PROBE_TAIL
    source->MutableExecutionContext().SetPrevCategoryName(currentCategoryName);
    source->MutableExecutionContext().SetPrevExecutionResult(currentExecutionResult);
}

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
        source->MutableExecutionContext().OnStartProgramStepExecution(iterator->GetCurrentNodeId(), GetSignals(iterator->GetCurrentNodeId()));
        auto signals = GetSignals(iterator->GetCurrentNodeId());

        const TMonotonic start = TMonotonic::Now();
        auto conclusion = source->GetExecutionContext().GetExecutionVisitorVerified()->Execute();
        const TDuration executionDurationMs = TMonotonic::Now() - start;
        source->GetContext()->GetCommonContext()->GetCounters().AddExecutionDuration(executionDurationMs);
        signals->AddExecutionDuration(executionDurationMs);
        source->AddExecutionDuration(executionDurationMs);

        const TString currentExecutionResult = conclusion.IsFail() ? "Fail" : ToString(*conclusion);
        ReportTracing(source, executionDurationMs, currentExecutionResult);
        if (conclusion.IsFail()) {
            source->MutableExecutionContext().OnFailedProgramStepExecution();
            return conclusion;
        } else if (*conclusion == NArrow::NSSA::IResourceProcessor::EExecutionResult::InBackground) {
            return false;
        }
        source->MutableExecutionContext().OnFinishProgramStepExecution();
        GetSignals(iterator->GetCurrentNodeId())->OnExecuteGraphNode(source->GetRecordsCount());
        source->GetContext()->GetCommonContext()->GetCounters().OnExecuteGraphNode(iterator->GetCurrentNode().GetIdentifier());
        if (source->GetExecutionContext().GetExecutionVisitorVerified()->MutableContext().GetResources().GetRecordsCountActualOptional() == 0) {
            source->GetExecutionContext().GetExecutionVisitorVerified()->MutableContext().MutableResources().Clear();
            break;
        }
    }
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, source->AddEvent("fgraph"));
    AFL_DEBUG(NKikimrServices::SSA_GRAPH_EXECUTION)(
        "graph_constructed", Program->DebugDOT(source->GetExecutionContext().GetExecutionVisitorVerified()->GetExecutedIds()));
    source->MutableStageData().ReturnTable(source->GetExecutionContext().GetExecutionVisitorVerified()->MutableContext().ExtractResources());

    return true;
}

const std::shared_ptr<TFetchingStepSignals>& TProgramStep::GetSignals(const ui32 nodeId) const {
    auto it = Signals.find(nodeId);
    AFL_VERIFY(it != Signals.end())("node_id", nodeId);
    return it->second;
}

}   // namespace NKikimr::NOlap::NReader::NCommon

#include "constructor.h"
#include "source.h"
#include <ydb/core/formats/arrow/program/aggr_keys.h>
#include <ydb/core/formats/arrow/program/assign_internal.h>
#include <ydb/core/formats/arrow/program/assign_const.h>
#include <ydb/core/formats/arrow/program/header.h>
#include <ydb/core/formats/arrow/program/index.h>
#include <ydb/core/formats/arrow/program/original.h>
#include <ydb/core/formats/arrow/program/stats.h>
#include <ydb/core/formats/arrow/program/stream_logic.h>

#include <set>

#include <ydb/core/tx/columnshard/engines/reader/tracing/data_source_probes.h>
#include <ydb/core/tx/columnshard/engines/reader/tracing/probes.h>

namespace NKikimr::NOlap::NReader::NCommon {

LWTRACE_USING(YDB_CS_DATA_SOURCE);
LWTRACE_USING(YDB_CS_SCAN);

TString TExecutionContext::GetStatsColumnName(IDataSource& source, ui32 columnId) const {
    if (auto it = StatsExpressions.find(columnId); it != StatsExpressions.end()) { return it->second; }
    auto name = source.GetContext()->GetCommonContext()->GetResolver()->GetColumnName(columnId, false);
    if (name) { return name; }
    return "intermediate result";
}

TString TExecutionContext::BuildStatsDescription(IDataSource& source, ui32 nodeId,
    const std::shared_ptr<NArrow::NSSA::IResourceProcessor>& processor, std::vector<TString> columns) {
    TString function = processor->GetSignalCategoryName();
    if (const auto* calculation = dynamic_cast<const NArrow::NSSA::TCalculationProcessor*>(processor.get())) {
        function = calculation->GetStatsFunctionDescription();
    } else if (const auto* stream = dynamic_cast<const NArrow::NSSA::TStreamLogicProcessor*>(processor.get())) {
        function = ::ToString(stream->GetOperation());
    } else if (const auto* index = dynamic_cast<const NArrow::NSSA::TIndexCheckerProcessor*>(processor.get())) {
        const auto context = index->GetIndexContext();
        auto column = context.GetColumnName();
        if (!column) { column = source.GetContext()->GetCommonContext()->GetResolver()->GetColumnName(context.GetColumnId(), false); }
        if (context.GetSubColumnName()) { column += "[" + context.GetSubColumnName().GetValue() + "]"; }
        columns = {column ? column : TString("unknown column"), columns.back()};
        function = TStringBuilder() << "Index " << context.GetOperation().GetOperation();
        if (!context.GetOperation().GetCaseSensitive()) { function += " (case insensitive)"; }
    } else if (const auto* header = dynamic_cast<const NArrow::NSSA::THeaderCheckerProcessor*>(processor.get())) {
        const auto context = header->GetHeaderContext();
        auto column = source.GetContext()->GetCommonContext()->GetResolver()->GetColumnName(context.GetColumnId(), false);
        if (context.GetSubColumnName()) { column += "[" + context.GetSubColumnName().GetValue() + "]"; }
        columns = {column ? column : TString("unknown column")};
    }
    TString description = NArrow::NSSA::BuildStatsExpression(function, columns, nodeId);
    if (const auto* constant = dynamic_cast<const NArrow::NSSA::TConstProcessor*>(processor.get())) {
        const auto value = constant->GetScalarConstant()->ToString();
        description = NArrow::NSSA::BoundStatsDescription(TStringBuf(value.data(), value.size()), nodeId);
    }
    const auto type = processor->GetProcessorType();
    if (type == NArrow::NSSA::EProcessorType::Calculation || type == NArrow::NSSA::EProcessorType::Const ||
        type == NArrow::NSSA::EProcessorType::StreamLogic || type == NArrow::NSSA::EProcessorType::CheckIndexData ||
        type == NArrow::NSSA::EProcessorType::CheckHeaderData) {
        for (const auto& output : processor->GetOutput()) { StatsExpressions[output.GetColumnId()] = description; }
    }
    return description;
}

void TExecutionContext::OnStartProgramStepExecution(const ui32 nodeId, const std::shared_ptr<TFetchingStepSignals>& signals,
    IDataSource& source, const std::shared_ptr<NArrow::NSSA::IResourceProcessor>& processor, const TString& metaStep) {
    if (!CurrentProgramNodeId) {
        const auto& collector = source.GetContext()->GetCommonContext()->GetRuntimeStats();
        if (collector) {
            const auto kind = processor->GetStatsKind();
            std::vector<TString> columns;
            if (const auto* fetch = dynamic_cast<const NArrow::NSSA::TOriginalColumnDataProcessor*>(processor.get())) {
                std::set<TString> names;
                for (const auto& [_, address] : fetch->GetDataAddresses()) { names.insert(address.GetColumnName()); }
                const auto addSchemaColumn = [&](ui32 id) {
                    auto name = source.GetContext()->GetCommonContext()->GetResolver()->GetColumnName(id, false);
                    names.insert(name ? name : TString("unknown column"));
                };
                for (const auto& [_, context] : fetch->GetIndexContext()) { addSchemaColumn(context.GetColumnId()); }
                for (const auto& [_, context] : fetch->GetHeaderContext()) { addSchemaColumn(context.GetColumnId()); }
                columns.assign(names.begin(), names.end());
            } else if (const auto* assemble = dynamic_cast<const NArrow::NSSA::TOriginalColumnAccessorProcessor*>(processor.get())) {
                columns.push_back(assemble->GetDataAddress().GetColumnName());
            } else {
                for (const auto& input : processor->GetInput()) { columns.push_back(GetStatsColumnName(source, input.GetColumnId())); }
            }
            const TString description = BuildStatsDescription(source, nodeId, processor, columns);
            RuntimeStats = collector->Start(kind, processor->GetSignalCategoryName(), description,
                CursorStep->GetRuntimeStats(), source.GetSourceId());
            if (RuntimeStats) {
                RuntimeStats->Update([&](NScanStats::TStep& record) {
                    record.SetMetaStep(metaStep);
                    record.SetCanonicalPath("");
                    if (kind == NScanStats::TStep::FetchColumns || kind == NScanStats::TStep::Assemble) {
                        auto* names = kind == NScanStats::TStep::FetchColumns
                            ? record.MutableDetails()->MutableFetchColumns()->MutableColumns()
                            : record.MutableDetails()->MutableAssemble()->MutableColumns();
                        for (const auto& name : columns) { names->AddNames(name); }
                    }
                    if (const auto* keyed = dynamic_cast<const NArrow::NSSA::NAggregation::TWithKeysAggregationProcessor*>(processor.get())) {
                        auto* details = record.MutableDetails()->MutableAggregation();
                        for (const auto& key : keyed->GetAggregationKeys()) { details->MutableKeys()->AddNames(GetStatsColumnName(source, key.GetColumnId())); }
                        std::vector<TString> functions;
                        for (const auto& aggregate : keyed->GetAggregations()) {
                            std::vector<TString> inputs;
                            for (const auto& input : aggregate.GetInputs()) { inputs.push_back(GetStatsColumnName(source, input.GetColumnId())); }
                            TString function = TStringBuilder() << aggregate.GetAggregationId() << "(" << JoinSeq(", ", inputs) << ")";
                            details->MutableFunctions()->AddNames(function);
                            functions.push_back(function);
                        }
                        record.SetOperation(TStringBuilder() << "GROUP BY " << JoinSeq(", ", details->GetKeys().GetNames()) << ": " << JoinSeq(", ", functions));
                    }
                    if (kind == NScanStats::TStep::Projection) {
                        auto* details = record.MutableDetails()->MutableProjection();
                        for (ui32 id : ExecutionVisitor->MutableContext().GetResources().GetStatsColumnIds()) {
                            details->MutableInputColumns()->AddNames(GetStatsColumnName(source, id));
                        }
                        for (const auto& name : columns) { details->MutableOutputColumns()->AddNames(name); }
                    }
                });
            }
        }
        CurrentNodeStart = TMonotonic::Now();
        CurrentProgramNodeId = nodeId;
        AFL_VERIFY(!CurrentStepSignals);
        CurrentStepSignals = signals;
    } else {
        AFL_VERIFY(CurrentProgramNodeId == nodeId);
        AFL_VERIFY(!!CurrentStepSignals);
        AFL_VERIFY(!!CurrentNodeStart);
    }
}

void TExecutionContext::OnFinishProgramStepExecution(NScanStats::TStep::EOutcome outcome) {
    if (RuntimeStats) { RuntimeStats->Finish(outcome); RuntimeStats.reset(); }
    AFL_VERIFY(!!CurrentProgramNodeId);
    AFL_VERIFY(!!CurrentStepSignals);
    AFL_VERIFY(!!CurrentNodeStart);
    CurrentStepSignals->AddTotalDuration(TMonotonic::Now() - *CurrentNodeStart);
    CurrentProgramNodeId.reset();
    CurrentStepSignals = nullptr;
    CurrentNodeStart.reset();
}

void TExecutionContext::Stop() {
    ProgramIterator.reset();
    ExecutionVisitor.reset();
}

void TExecutionContext::Start(
    IDataSource& source, const std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TCompiledGraph>& program, const TFetchingScriptCursor& step) {
    auto readMeta = source.GetContext()->GetCommonContext()->GetReadMetadata();
    // ItemsLimit is a distinct-key cap when DistinctMarker is present (reader sync point). SSA CutFilter
    // would otherwise keep only that many physical rows and hide later keys in the same source.
    const std::optional<i64> ssaLimit =
        readMeta->GetProgram().GetDistinctKeyColumnIdOptional() ? std::nullopt : readMeta->GetLimitRobustOptional();
    NArrow::NSSA::TProcessorContext context(source, source.MutableStageData().ExtractTable(), ssaLimit, readMeta->IsDescSorted());
    auto visitor = std::make_shared<NArrow::NSSA::NGraph::NExecution::TExecutionVisitor>(std::move(context));
    SetProgramIterator(program->BuildIterator(visitor), visitor);
    SetCursorStep(step);
    PrevNode = TPrevNodeTracing{ .CategoryName = step.GetPrevName() };
}

const TFetchingStepSignals& TExecutionContext::GetCurrentStepSignalsVerified() const {
    AFL_VERIFY(!!CurrentStepSignals);
    return *CurrentStepSignals;
}

const TFetchingStepSignals* TExecutionContext::GetCurrentStepSignalsOptional() const {
    if (!CurrentStepSignals) {
        return nullptr;
    }
    return &*CurrentStepSignals;
}

void TExecutionContext::SetProgramIterator(const std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TCompiledGraph::TIterator>& it,
    const std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TExecutionVisitor>& visitor) {
    AFL_VERIFY(!ProgramIterator);
    ProgramIterator = it;
    ExecutionVisitor = visitor;
}

void TExecutionContext::SetCursorStep(const TFetchingScriptCursor& step) {
    AFL_VERIFY(!CursorStep);
    CursorStep = step;
}

const TFetchingScriptCursor& TExecutionContext::GetCursorStep() const {
    AFL_VERIFY(!!CursorStep);
    return *CursorStep;
}

const std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TCompiledGraph::TIterator>& TExecutionContext::GetProgramIteratorVerified() const {
    AFL_VERIFY(!!ProgramIterator);
    return ProgramIterator;
}

const std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TExecutionVisitor>& TExecutionContext::GetExecutionVisitorVerified() const {
    AFL_VERIFY(!!ExecutionVisitor);
    return ExecutionVisitor;
}

ui64 IDataSource::DoGetSourceRecordsCount() const {
    if (RecordsCountImpl) {
        return *RecordsCountImpl;
    } else {
        AFL_VERIFY(!!GetRecordsCountVirtual());
        return GetRecordsCountVirtual();
    }
}

TConclusionStatus IDataSource::DoApplyPendingFetcher(const NArrow::NSSA::TProcessorContext& context, const ui32 entityId) {
    if (!HasStageData()) {
        return TConclusionStatus::Success();
    }
    auto fetcher = MutableStageData().ExtractFetcherOptional(entityId);
    if (!fetcher) {
        return TConclusionStatus::Success();
    }
    TFetchingResultContext fetchContext(context.MutableResources(), *GetStageData().GetIndexes(), *this);
    return fetcher->OnDataCollected(fetchContext);
}

TConclusion<TExecutionResult> IDataSource::DoStartFetch(
    const NArrow::NSSA::TProcessorContext& context, const std::vector<std::shared_ptr<NArrow::NSSA::IFetchLogic>>& fetchersExt) {
    std::vector<std::shared_ptr<IKernelFetchLogic>> fetchers;
    for (auto&& i : fetchersExt) {
        fetchers.emplace_back(std::static_pointer_cast<IKernelFetchLogic>(i));
    }
    if (fetchers.empty()) {
        return TExecutionResult::Done();
    }
    return DoStartFetchImpl(context, fetchers);
}

ui32 IDataSource::GetRecordsCountVirtual() const {
    AFL_VERIFY(false);
    return 0;
}

ui64 IDataSource::GetReservedMemory() const {
    ui64 result = 0;
    for (auto&& i : ResourceGuards) {
        result += i->GetMemory();
    }
    return result;
}

const TPortionDataAccessor& IDataSource::GetPortionAccessor() const {
    AFL_VERIFY(!!Accessor);
    return *Accessor;
}

std::shared_ptr<TPortionDataAccessor> IDataSource::ExtractPortionAccessor() {
    AFL_VERIFY(!!Accessor);
    auto result = std::move(Accessor);
    Accessor.reset();
    return result;
}

void IDataSource::SetPortionAccessor(std::shared_ptr<TPortionDataAccessor>&& acc) {
    AFL_VERIFY(!!acc);
    AFL_VERIFY(!Accessor);
    Accessor = std::move(acc);
}

void IDataSource::InitRecordsCount(const ui32 recordsCount) {
    AFL_VERIFY(!RecordsCountImpl);
    RecordsCountImpl = recordsCount;
    AFL_VERIFY(StageData);
    StageData->InitRecordsCount(recordsCount);
}

ui32 IDataSource::GetRecordsCount() const {
    if (RecordsCountImpl) {
        return *RecordsCountImpl;
    } else {
        AFL_VERIFY(!!GetRecordsCountVirtual());
        return GetRecordsCountVirtual();
    }
}

void IDataSource::OnStartProcessing() {
    AFL_VERIFY(!SourceCreatedTimestamp);
    SourceCreatedTimestamp = TMonotonic::Now();
    GetContext()->GetCommonContext()->GetCounters().OnSourceStartProcessing(IsConflicting());
    if (!NLWTrace::HasShuttles(DataSourceOrbit) && !NLWTrace::HasShuttles(*GetContext()->GetCommonContext()->GetScanOrbit()) &&
        !LWPROBE_ENABLED(StartSourceProcessing) && !LWPROBE_ENABLED(ScanStartSource)) {
        return;
    }
    const ui64 portionBlobBytes = HasPortionAccessor() ? GetPortionAccessor().GetPortionInfo().GetTotalBlobBytes() : 0;
    const ui64 portionRawBytes = HasPortionAccessor() ? GetPortionAccessor().GetPortionInfo().GetTotalRawBytes() : 0;
    TString minPk = HasPortionAccessor() ? GetPortionAccessor().GetPortionInfo().IndexKeyStart().DebugString() : TString{};
    TString maxPk = HasPortionAccessor() ? GetPortionAccessor().GetPortionInfo().IndexKeyEnd().DebugString() : TString{};
    const TString minSnapshot = TStringBuilder() << GetRecordSnapshotMin();
    const TString maxSnapshot = TStringBuilder() << GetRecordSnapshotMax();
    LWTRACK(StartSourceProcessing, DataSourceOrbit, GetRawPathId(), GetTabletId(), GetTxId(), GetSourceId(), portionBlobBytes, portionRawBytes,
        GetReservedMemory(), minPk, maxPk, minSnapshot, maxSnapshot);
    LWTRACK(ScanStartSource, *GetContext()->GetCommonContext()->GetScanOrbit(), GetRawPathId(), GetTabletId(), GetTxId(),
        GetContext()->GetCommonContext()->GetScanId(), GetSourceId(), portionBlobBytes, portionRawBytes, minPk, maxPk, minSnapshot, maxSnapshot);
}

void IDataSource::StartAsyncSection() {
    AFL_VERIFY(AtomicCas(&SyncSectionFlag, 0, 1));
}

void IDataSource::CheckAsyncSection() {
    AFL_VERIFY(AtomicGet(SyncSectionFlag) == 0);
}

void IDataSource::StartSyncSection() {
    AFL_VERIFY(AtomicCas(&SyncSectionFlag, 1, 0));
}

void IDataSource::AddEvent(const TString& evDescription) {
    AFL_VERIFY(!!Events);
    Events->AddEvent(evDescription);
}

TString IDataSource::GetEventsReport() const {
    return Events ? Events->DebugString() : Default<TString>();
}

const std::shared_ptr<ISnapshotSchema>& IDataSource::GetSourceSchema() const {
    AFL_VERIFY(false);
    return Default<std::shared_ptr<ISnapshotSchema>>();
}

TString IDataSource::GetColumnStorageId(const ui32 /*columnId*/) const {
    AFL_VERIFY(false);
    return "";
}

TString IDataSource::GetEntityStorageId(const ui32 /*entityId*/) const {
    AFL_VERIFY(false);
    return "";
}

TString IDataSource::GetIndexStorageId(const ui32 /*indexId*/) const {
    AFL_VERIFY(false);
    return "";
}

TBlobRange IDataSource::RestoreBlobRange(const TBlobRangeLink16& /*rangeLink*/) const {
    AFL_VERIFY(false);
    return TBlobRange();
}

IDataSource::IDataSource(const EType type, const ui32 sourceIdx, const std::shared_ptr<TSpecialReadContext>& context, const bool isConflicting,
    const TSnapshot& recordSnapshotMin, const TSnapshot& recordSnapshotMax, const std::optional<ui32> recordsCount,
    const std::optional<ui64> shardingVersion, const bool hasDeletions, const ui64 deprecatedPortionId)
    : Type(type)
    , SourceIdx(sourceIdx)
    , SourceId(deprecatedPortionId)
    , RecordSnapshotMin(recordSnapshotMin)
    , RecordSnapshotMax(recordSnapshotMax)
    , Context(context)
    , RecordsCountImpl(recordsCount)
    , ShardingVersionOptional(shardingVersion)
    , HasDeletions(hasDeletions)
    , ConflictingFlag(isConflicting)
{
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, Events.emplace(NEvLog::TLogsThread()));
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, AddEvent("c"));
}

std::vector<std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>> IDataSource::ExtractResourceGuards() {
    auto result = std::move(ResourceGuards);
    ResourceGuards.clear();
    return std::move(result);
}

bool IDataSource::IsSourceInMemory() const {
    AFL_VERIFY(IsSourceInMemoryFlag);
    return *IsSourceInMemoryFlag;
}

void IDataSource::SetSourceInMemory(const bool value) {
    AFL_VERIFY(!IsSourceInMemoryFlag);
    IsSourceInMemoryFlag = value;
    if (!value) {
        AFL_VERIFY(StageData);
        StageData->SetUseFilter(value);
    }
}

void IDataSource::SetMemoryGroupId(const ui64 groupId) {
    AFL_VERIFY(!MemoryGroupId);
    MemoryGroupId = groupId;
}

ui64 IDataSource::GetMemoryGroupId() const {
    AFL_VERIFY(!!MemoryGroupId);
    return *MemoryGroupId;
}

ui64 IDataSource::GetResourceGuardsMemory() const {
    ui64 result = 0;
    for (auto&& i : ResourceGuards) {
        result += i->GetMemory();
    }
    return result;
}

void IDataSource::AssembleColumns(const std::shared_ptr<TColumnsSet>& columns, const bool sequential) {
    if (columns->IsEmpty()) {
        return;
    }
    DoAssembleColumns(columns, sequential);
}

void IDataSource::ResetSourceFinishedFlag() {
    AFL_VERIFY(AtomicCas(&SourceFinishedSafeFlag, 0, 1));
}

void IDataSource::OnSourceFetchingFinishedSafe(IDataReader& owner, std::unique_ptr<TDataSourceLease> self) {
    AFL_VERIFY(AtomicCas(&SourceFinishedSafeFlag, 1, 0));
    AFL_VERIFY(self && &self->GetSource() == this);
    DoOnSourceFetchingFinishedSafe(owner, std::move(self));
}

void IDataSource::OnEmptyStageData() {
    AFL_VERIFY(AtomicCas(&StageResultBuiltFlag, 1, 0));
    AFL_VERIFY(!StageResult);
    AFL_VERIFY(StageData);
    DoOnEmptyStageData();
    AFL_VERIFY(StageResult);
    AFL_VERIFY(!StageData);

    const TDuration durationMs = GetAndResetWaitDuration();
    LWTRACK(SourceFinished, DataSourceOrbit, GetRawPathId(), GetTabletId(), GetTxId(), GetSourceId(), 0,
        ExecutionContext.GetPrevCategoryName() + " - " + "SourceFinished(Empty)", durationMs, GetTotalDuration(), GetTotalBytesRead(),
        GetTotalExecutionDuration(), GetReservedMemory());
}

void IDataSource::BuildStageResult() {
    TMemoryProfileGuard mpg("SCAN_PROFILE::STAGE_RESULT", IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD_SCAN_MEMORY));
    AFL_VERIFY(AtomicCas(&StageResultBuiltFlag, 1, 0));
    AFL_VERIFY(!StageResult);
    AFL_VERIFY(StageData);
    DoBuildStageResult();
    AFL_VERIFY(StageResult);
    AFL_VERIFY(!StageData);

    const TDuration durationMs = GetAndResetWaitDuration();
    LWTRACK(SourceFinished, DataSourceOrbit, GetRawPathId(), GetTabletId(), GetTxId(), GetSourceId(), 0,
        ExecutionContext.GetPrevCategoryName() + " - " + "SourceFinished", durationMs, GetTotalDuration(), GetTotalBytesRead(),
        GetTotalExecutionDuration(), GetReservedMemory());
}

bool IDataSource::AddTxConflict() {
    if (!Context->GetCommonContext()->HasLock()) {
        return false;
    }
    if (DoAddTxConflict()) {
        StageData->Clear();
        StageData->Abort();
        return true;
    }
    return false;
}

void IDataSource::InitStageData(std::unique_ptr<TFetchedData>&& data) {
    AFL_VERIFY(!StageData);
    StageData = std::move(data);
}

std::unique_ptr<TFetchedData> IDataSource::ExtractStageData() {
    AFL_VERIFY(StageData)("source_idx", SourceIdx);
    auto result = std::move(StageData);
    StageData.reset();
    return std::move(result);
}

const TFetchedData& IDataSource::GetStageData() const {
    AFL_VERIFY(StageData)("source_idx", SourceIdx);
    return *StageData;
}

TFetchedData& IDataSource::MutableStageData() {
    AFL_VERIFY(StageData);
    return *StageData;
}

const TFetchedResult& IDataSource::GetStageResult() const {
    AFL_VERIFY(!!StageResult);
    return *StageResult;
}

TFetchedResult& IDataSource::MutableStageResult() {
    AFL_VERIFY(!!StageResult);
    return *StageResult;
}

}   // namespace NKikimr::NOlap::NReader::NCommon

#include "constructor.h"
#include "source.h"

#include <ydb/core/tx/columnshard/engines/reader/tracing/data_source_probes.h>
#include <ydb/core/tx/columnshard/engines/reader/tracing/probes.h>

namespace NKikimr::NOlap::NReader::NCommon {

LWTRACE_USING(YDB_CS_DATA_SOURCE);
LWTRACE_USING(YDB_CS_SCAN);

void TExecutionContext::OnStartProgramStepExecution(const ui32 nodeId, const std::shared_ptr<TFetchingStepSignals>& signals) {
    if (!CurrentProgramNodeId) {
        CurrentNodeStart = TMonotonic::Now();
        CurrentProgramNodeId = nodeId;
        YDB_LOG_VERIFY(!CurrentStepSignals);
        CurrentStepSignals = signals;
    } else {
        YDB_LOG_VERIFY(CurrentProgramNodeId == nodeId);
        YDB_LOG_VERIFY(!!CurrentStepSignals);
        YDB_LOG_VERIFY(!!CurrentNodeStart);
    }
}

void TExecutionContext::OnFinishProgramStepExecution() {
    YDB_LOG_VERIFY(!!CurrentProgramNodeId);
    YDB_LOG_VERIFY(!!CurrentStepSignals);
    YDB_LOG_VERIFY(!!CurrentNodeStart);
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
    YDB_LOG_VERIFY(!!CurrentStepSignals);
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
    YDB_LOG_VERIFY(!ProgramIterator);
    ProgramIterator = it;
    ExecutionVisitor = visitor;
}

void TExecutionContext::SetCursorStep(const TFetchingScriptCursor& step) {
    YDB_LOG_VERIFY(!CursorStep);
    CursorStep = step;
}

const TFetchingScriptCursor& TExecutionContext::GetCursorStep() const {
    YDB_LOG_VERIFY(!!CursorStep);
    return *CursorStep;
}

const std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TCompiledGraph::TIterator>& TExecutionContext::GetProgramIteratorVerified() const {
    YDB_LOG_VERIFY(!!ProgramIterator);
    return ProgramIterator;
}

const std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TExecutionVisitor>& TExecutionContext::GetExecutionVisitorVerified() const {
    YDB_LOG_VERIFY(!!ExecutionVisitor);
    return ExecutionVisitor;
}

ui64 IDataSource::DoGetSourceRecordsCount() const {
    if (RecordsCountImpl) {
        return *RecordsCountImpl;
    } else {
        YDB_LOG_VERIFY(!!GetRecordsCountVirtual());
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
    YDB_LOG_VERIFY(false);
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
    YDB_LOG_VERIFY(!!Accessor);
    return *Accessor;
}

std::shared_ptr<TPortionDataAccessor> IDataSource::ExtractPortionAccessor() {
    YDB_LOG_VERIFY(!!Accessor);
    auto result = std::move(Accessor);
    Accessor.reset();
    return result;
}

void IDataSource::SetPortionAccessor(std::shared_ptr<TPortionDataAccessor>&& acc) {
    YDB_LOG_VERIFY(!!acc);
    YDB_LOG_VERIFY(!Accessor);
    Accessor = std::move(acc);
}

void IDataSource::InitRecordsCount(const ui32 recordsCount) {
    YDB_LOG_VERIFY(!RecordsCountImpl);
    RecordsCountImpl = recordsCount;
    YDB_LOG_VERIFY(StageData);
    StageData->InitRecordsCount(recordsCount);
}

ui32 IDataSource::GetRecordsCount() const {
    if (RecordsCountImpl) {
        return *RecordsCountImpl;
    } else {
        YDB_LOG_VERIFY(!!GetRecordsCountVirtual());
        return GetRecordsCountVirtual();
    }
}

void IDataSource::OnStartProcessing() {
    YDB_LOG_VERIFY(!SourceCreatedTimestamp);
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
    YDB_LOG_VERIFY(AtomicCas(&SyncSectionFlag, 0, 1));
}

void IDataSource::CheckAsyncSection() {
    YDB_LOG_VERIFY(AtomicGet(SyncSectionFlag) == 0);
}

void IDataSource::StartSyncSection() {
    YDB_LOG_VERIFY(AtomicCas(&SyncSectionFlag, 1, 0));
}

void IDataSource::AddEvent(const TString& evDescription) {
    YDB_LOG_VERIFY(!!Events);
    Events->AddEvent(evDescription);
}

TString IDataSource::GetEventsReport() const {
    return Events ? Events->DebugString() : Default<TString>();
}

const std::shared_ptr<ISnapshotSchema>& IDataSource::GetSourceSchema() const {
    YDB_LOG_VERIFY(false);
    return Default<std::shared_ptr<ISnapshotSchema>>();
}

TString IDataSource::GetColumnStorageId(const ui32 /*columnId*/) const {
    YDB_LOG_VERIFY(false);
    return "";
}

TString IDataSource::GetEntityStorageId(const ui32 /*entityId*/) const {
    YDB_LOG_VERIFY(false);
    return "";
}

TString IDataSource::GetIndexStorageId(const ui32 /*indexId*/) const {
    YDB_LOG_VERIFY(false);
    return "";
}

TBlobRange IDataSource::RestoreBlobRange(const TBlobRangeLink16& /*rangeLink*/) const {
    YDB_LOG_VERIFY(false);
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
    YDB_LOG_VERIFY(IsSourceInMemoryFlag);
    return *IsSourceInMemoryFlag;
}

void IDataSource::SetSourceInMemory(const bool value) {
    YDB_LOG_VERIFY(!IsSourceInMemoryFlag);
    IsSourceInMemoryFlag = value;
    if (!value) {
        YDB_LOG_VERIFY(StageData);
        StageData->SetUseFilter(value);
    }
}

void IDataSource::SetMemoryGroupId(const ui64 groupId) {
    YDB_LOG_VERIFY(!MemoryGroupId);
    MemoryGroupId = groupId;
}

ui64 IDataSource::GetMemoryGroupId() const {
    YDB_LOG_VERIFY(!!MemoryGroupId);
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
    YDB_LOG_VERIFY(AtomicCas(&SourceFinishedSafeFlag, 0, 1));
}

void IDataSource::OnSourceFetchingFinishedSafe(IDataReader& owner, std::unique_ptr<TDataSourceLease> self) {
    YDB_LOG_VERIFY(AtomicCas(&SourceFinishedSafeFlag, 1, 0));
    YDB_LOG_VERIFY(self && &self->GetSource() == this);
    DoOnSourceFetchingFinishedSafe(owner, std::move(self));
}

void IDataSource::OnEmptyStageData() {
    YDB_LOG_VERIFY(AtomicCas(&StageResultBuiltFlag, 1, 0));
    YDB_LOG_VERIFY(!StageResult);
    YDB_LOG_VERIFY(StageData);
    DoOnEmptyStageData();
    YDB_LOG_VERIFY(StageResult);
    YDB_LOG_VERIFY(!StageData);

    const TDuration durationMs = GetAndResetWaitDuration();
    LWTRACK(SourceFinished, DataSourceOrbit, GetRawPathId(), GetTabletId(), GetTxId(), GetSourceId(), 0,
        ExecutionContext.GetPrevCategoryName() + " - " + "SourceFinished(Empty)", durationMs, GetTotalDuration(), GetTotalBytesRead(),
        GetTotalExecutionDuration(), GetReservedMemory());
}

void IDataSource::BuildStageResult() {
    TMemoryProfileGuard mpg("SCAN_PROFILE::STAGE_RESULT", IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD_SCAN_MEMORY));
    YDB_LOG_VERIFY(AtomicCas(&StageResultBuiltFlag, 1, 0));
    YDB_LOG_VERIFY(!StageResult);
    YDB_LOG_VERIFY(StageData);
    DoBuildStageResult();
    YDB_LOG_VERIFY(StageResult);
    YDB_LOG_VERIFY(!StageData);

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
    YDB_LOG_VERIFY(!StageData);
    StageData = std::move(data);
}

std::unique_ptr<TFetchedData> IDataSource::ExtractStageData() {
    YDB_LOG_VERIFY(StageData,
        {"sourceIdx", SourceIdx});
    auto result = std::move(StageData);
    StageData.reset();
    return std::move(result);
}

const TFetchedData& IDataSource::GetStageData() const {
    YDB_LOG_VERIFY(StageData,
        {"sourceIdx", SourceIdx});
    return *StageData;
}

TFetchedData& IDataSource::MutableStageData() {
    YDB_LOG_VERIFY(StageData);
    return *StageData;
}

const TFetchedResult& IDataSource::GetStageResult() const {
    YDB_LOG_VERIFY(!!StageResult);
    return *StageResult;
}

TFetchedResult& IDataSource::MutableStageResult() {
    YDB_LOG_VERIFY(!!StageResult);
    return *StageResult;
}

}   // namespace NKikimr::NOlap::NReader::NCommon

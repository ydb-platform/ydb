#include "constructor.h"
#include "source.h"

namespace NKikimr::NOlap::NReader::NCommon {

void TExecutionContext::OnStartProgramStepExecution(const ui32 nodeId, const std::shared_ptr<TFetchingStepSignals>& signals) {
    if (!CurrentProgramNodeId) {
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

void TExecutionContext::OnFinishProgramStepExecution() {
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

void TExecutionContext::Start(const std::shared_ptr<IDataSource>& source,
    const std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TCompiledGraph>& program, const TFetchingScriptCursor& step) {
    auto readMeta = source->GetContext()->GetCommonContext()->GetReadMetadata();
    NArrow::NSSA::TProcessorContext context(
        source, source->MutableStageData().ExtractTable(), readMeta->GetLimitRobustOptional(), readMeta->IsDescSorted());
    auto visitor = std::make_shared<NArrow::NSSA::NGraph::NExecution::TExecutionVisitor>(std::move(context));
    SetProgramIterator(program->BuildIterator(visitor), visitor);
    SetCursorStep(step);
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

ui64 IDataSource::DoGetEntityRecordsCount() const {
    if (RecordsCountImpl) {
        return *RecordsCountImpl;
    } else {
        AFL_VERIFY(!!GetRecordsCountVirtual());
        return GetRecordsCountVirtual();
    }
}

TConclusion<bool> IDataSource::DoStartFetch(
    const NArrow::NSSA::TProcessorContext& context, const std::vector<std::shared_ptr<NArrow::NSSA::IFetchLogic>>& fetchersExt) {
    std::vector<std::shared_ptr<IKernelFetchLogic>> fetchers;
    for (auto&& i : fetchersExt) {
        fetchers.emplace_back(std::static_pointer_cast<IKernelFetchLogic>(i));
    }
    if (fetchers.empty()) {
        return false;
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

TBlobRange IDataSource::RestoreBlobRange(const TBlobRangeLink16& /*rangeLink*/) const {
    AFL_VERIFY(false);
    return TBlobRange();
}

IDataSource::IDataSource(const EType type, const ui64 sourceId, const ui32 sourceIdx, const std::shared_ptr<TSpecialReadContext>& context,
    const TSnapshot& recordSnapshotMin, const TSnapshot& recordSnapshotMax, const std::optional<ui32> recordsCount,
    const std::optional<ui64> shardingVersion, const bool hasDeletions)
    : Type(type)
    , SourceId(sourceId)
    , SourceIdx(sourceIdx)
    , RecordSnapshotMin(recordSnapshotMin)
    , RecordSnapshotMax(recordSnapshotMax)
    , Context(context)
    , RecordsCountImpl(recordsCount)
    , ShardingVersionOptional(shardingVersion)
    , HasDeletions(hasDeletions) {
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

void IDataSource::OnSourceFetchingFinishedSafe(IDataReader& owner, const std::shared_ptr<IDataSource>& sourcePtr) {
    AFL_VERIFY(AtomicCas(&SourceFinishedSafeFlag, 1, 0));
    AFL_VERIFY(sourcePtr);
    DoOnSourceFetchingFinishedSafe(owner, sourcePtr);
}

void IDataSource::OnEmptyStageData(const std::shared_ptr<NCommon::IDataSource>& sourcePtr) {
    AFL_VERIFY(AtomicCas(&StageResultBuiltFlag, 1, 0));
    AFL_VERIFY(sourcePtr);
    AFL_VERIFY(!StageResult);
    AFL_VERIFY(StageData);
    DoOnEmptyStageData(sourcePtr);
    AFL_VERIFY(StageResult);
    AFL_VERIFY(!StageData);
}

void IDataSource::BuildStageResult(const std::shared_ptr<IDataSource>& sourcePtr) {
    TMemoryProfileGuard mpg("SCAN_PROFILE::STAGE_RESULT", IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD_SCAN_MEMORY));
    AFL_VERIFY(AtomicCas(&StageResultBuiltFlag, 1, 0));
    AFL_VERIFY(sourcePtr);
    AFL_VERIFY(!StageResult);
    AFL_VERIFY(StageData);
    DoBuildStageResult(sourcePtr);
    AFL_VERIFY(StageResult);
    AFL_VERIFY(!StageData);
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
    AFL_VERIFY(StageData)("source_id", SourceId);
    auto result = std::move(StageData);
    StageData.reset();
    return std::move(result);
}

const TFetchedData& IDataSource::GetStageData() const {
    AFL_VERIFY(StageData)("source_id", SourceId);
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

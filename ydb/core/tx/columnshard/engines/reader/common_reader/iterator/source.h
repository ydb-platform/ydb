#pragma once
#include "context.h"
#include "fetched_data.h"
#include "fetching.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/program/execution.h>
#include <ydb/core/formats/arrow/program/visitor.h>
#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/action.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/predicate/range.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/common/columns_set.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/filtered_scheme.h>
#include <ydb/core/tx/columnshard/resource_subscriber/task.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>
#include <ydb/core/util/evlog/log.h>

#include <util/string/join.h>

namespace NKikimr::NOlap {
class IDataReader;
}

namespace NKikimr::NOlap::NReader::NCommon {

class TFetchingScriptCursor;

class TExecutionContext {
private:
    std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TCompiledGraph::TIterator> ProgramIterator;
    std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TExecutionVisitor> ExecutionVisitor;

    std::optional<ui32> CurrentProgramNodeId;
    std::shared_ptr<TFetchingStepSignals> CurrentStepSignals;
    std::optional<TMonotonic> CurrentNodeStart;

    std::optional<TFetchingScriptCursor> CursorStep;

public:
    void OnStartProgramStepExecution(const ui32 nodeId, const std::shared_ptr<TFetchingStepSignals>& signals) {
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

    void OnFinishProgramStepExecution() {
        AFL_VERIFY(!!CurrentProgramNodeId);
        AFL_VERIFY(!!CurrentStepSignals);
        AFL_VERIFY(!!CurrentNodeStart);
        CurrentStepSignals->AddTotalDuration(TMonotonic::Now() - *CurrentNodeStart);
        CurrentProgramNodeId.reset();
        CurrentStepSignals = nullptr;
        CurrentNodeStart.reset();
    }

    void OnFailedProgramStepExecution() {
        OnFinishProgramStepExecution();
    }

    void Start(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TCompiledGraph>& program,
        const TFetchingScriptCursor& step);

    void Stop();

    const TFetchingStepSignals& GetCurrentStepSignalsVerified() const {
        AFL_VERIFY(!!CurrentStepSignals);
        return *CurrentStepSignals;
    }

    const TFetchingStepSignals* GetCurrentStepSignalsOptional() const {
        if (!CurrentStepSignals) {
            return nullptr;
        }
        return &*CurrentStepSignals;
    }

    bool HasProgramIterator() const {
        return !!ProgramIterator;
    }

    void SetProgramIterator(const std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TCompiledGraph::TIterator>& it,
        const std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TExecutionVisitor>& visitor) {
        AFL_VERIFY(!ProgramIterator);
        ProgramIterator = it;
        ExecutionVisitor = visitor;
    }

    void SetCursorStep(const TFetchingScriptCursor& step) {
        AFL_VERIFY(!CursorStep);
        CursorStep = step;
    }

    const TFetchingScriptCursor& GetCursorStep() const {
        AFL_VERIFY(!!CursorStep);
        return *CursorStep;
    }

    const std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TCompiledGraph::TIterator>& GetProgramIteratorVerified() const {
        AFL_VERIFY(!!ProgramIterator);
        return ProgramIterator;
    }

    const std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TExecutionVisitor>& GetExecutionVisitorVerified() const {
        AFL_VERIFY(!!ExecutionVisitor);
        return ExecutionVisitor;
    }
};

class IDataSource: public ICursorEntity, public NArrow::NSSA::IDataSource {
public:
    enum class EType {
        Undefined,
        SimpleSysInfo,
        SimplePortion,
        SimpleAggregation,
        PlainPortion
    };

private:
    TAtomic SyncSectionFlag = 1;
    YDB_READONLY(EType, Type, EType::Undefined);
    YDB_READONLY(ui64, SourceId, 0);
    YDB_READONLY(ui32, SourceIdx, 0);
    static inline TAtomicCounter MemoryGroupCounter = 0;
    YDB_READONLY(ui64, SequentialMemoryGroupIdx, MemoryGroupCounter.Inc());
    YDB_READONLY(TSnapshot, RecordSnapshotMin, TSnapshot::Zero());
    YDB_READONLY(TSnapshot, RecordSnapshotMax, TSnapshot::Zero());
    YDB_READONLY_DEF(std::shared_ptr<TSpecialReadContext>, Context);
    std::optional<ui32> RecordsCountImpl;
    YDB_READONLY_DEF(std::optional<ui64>, ShardingVersionOptional);
    YDB_READONLY(bool, HasDeletions, false);
    std::optional<ui64> MemoryGroupId;
    TExecutionContext ExecutionContext;
    virtual bool DoAddTxConflict() = 0;

    virtual ui64 DoGetEntityId() const override {
        return SourceId;
    }

    virtual ui64 DoGetEntityRecordsCount() const override {
        if (RecordsCountImpl) {
            return *RecordsCountImpl;
        } else {
            AFL_VERIFY(!!GetRecordsCountVirtual());
            return GetRecordsCountVirtual();
        }
    }

    std::optional<bool> IsSourceInMemoryFlag;
    TAtomic SourceFinishedSafeFlag = 0;
    TAtomic StageResultBuiltFlag = 0;
    virtual void DoOnSourceFetchingFinishedSafe(IDataReader& owner, const std::shared_ptr<IDataSource>& sourcePtr) = 0;
    virtual void DoBuildStageResult(const std::shared_ptr<IDataSource>& sourcePtr) = 0;
    virtual void DoOnEmptyStageData(const std::shared_ptr<NCommon::IDataSource>& sourcePtr) = 0;

    virtual TConclusion<bool> DoStartFetchImpl(
        const NArrow::NSSA::TProcessorContext& context, const std::vector<std::shared_ptr<IKernelFetchLogic>>& fetchersExt) = 0;

    virtual TConclusion<bool> DoStartFetch(const NArrow::NSSA::TProcessorContext& context,
        const std::vector<std::shared_ptr<NArrow::NSSA::IFetchLogic>>& fetchersExt) override final;

    virtual bool DoStartFetchingColumns(
        const std::shared_ptr<IDataSource>& sourcePtr, const TFetchingScriptCursor& step, const TColumnsSetIds& columns) = 0;
    virtual void DoAssembleColumns(const std::shared_ptr<TColumnsSet>& columns, const bool sequential) = 0;

    std::optional<NEvLog::TLogsThread> Events;
    std::unique_ptr<TFetchedData> StageData;
    std::shared_ptr<TPortionDataAccessor> Accessor;

protected:
    std::vector<std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>> ResourceGuards;
    std::unique_ptr<TFetchedResult> StageResult;
    virtual ui32 GetRecordsCountVirtual() const {
        AFL_VERIFY(false);
        return 0;
    }

public:

    const TPortionDataAccessor& GetPortionAccessor() const {
        AFL_VERIFY(!!Accessor);
        return *Accessor;
    }

    std::shared_ptr<TPortionDataAccessor> ExtractPortionAccessor() {
        AFL_VERIFY(!!Accessor);
        auto result = std::move(Accessor);
        Accessor.reset();
        return result;
    }

    bool HasPortionAccessor() const {
        return !!Accessor;
    }

    void SetPortionAccessor(std::shared_ptr<TPortionDataAccessor>&& acc) {
        AFL_VERIFY(!!acc);
        AFL_VERIFY(!Accessor);
        Accessor = std::move(acc);
    }

    template <class T>
    const T* GetAs() const {
        AFL_VERIFY(T::CheckTypeCast(Type))("type", Type);
        return static_cast<const T*>(this);
    }

    template <class T>
    T* MutableAs() {
        AFL_VERIFY(T::CheckTypeCast(Type))("type", Type);
        return static_cast<T*>(this);
    }

    virtual bool NeedPortionData() const {
        return true;
    }

    std::optional<ui32> GetRecordsCountOptional() const {
        return RecordsCountImpl;
    }

    virtual void InitRecordsCount(const ui32 recordsCount) {
        AFL_VERIFY(!RecordsCountImpl);
        RecordsCountImpl = recordsCount;
        AFL_VERIFY(StageData);
        StageData->InitRecordsCount(recordsCount);
    }

    ui32 GetRecordsCount() const {
        if (RecordsCountImpl) {
            return *RecordsCountImpl;
        } else {
            AFL_VERIFY(!!GetRecordsCountVirtual());
            return GetRecordsCountVirtual();
        }
    }

    void StartAsyncSection() {
        AFL_VERIFY(AtomicCas(&SyncSectionFlag, 0, 1));
    }

    void CheckAsyncSection() {
        AFL_VERIFY(AtomicGet(SyncSectionFlag) == 0);
    }

    void StartSyncSection() {
        AFL_VERIFY(AtomicCas(&SyncSectionFlag, 1, 0));
    }

    bool IsSyncSection() const {
        return AtomicGet(SyncSectionFlag) == 1;
    }

    void AddEvent(const TString& evDescription) {
        AFL_VERIFY(!!Events);
        Events->AddEvent(evDescription);
    }

    TString GetEventsReport() const {
        return Events ? Events->DebugString() : Default<TString>();
    }

    TExecutionContext& MutableExecutionContext() {
        return ExecutionContext;
    }

    const TExecutionContext& GetExecutionContext() const {
        return ExecutionContext;
    }

    virtual const std::shared_ptr<ISnapshotSchema>& GetSourceSchema() const {
        AFL_VERIFY(false);
        return Default<std::shared_ptr<ISnapshotSchema>>();
    }

    virtual TString GetColumnStorageId(const ui32 /*columnId*/) const {
        AFL_VERIFY(false);
        return "";
    }

    virtual TString GetEntityStorageId(const ui32 /*entityId*/) const {
        AFL_VERIFY(false);
        return "";
    }

    virtual TBlobRange RestoreBlobRange(const TBlobRangeLink16& /*rangeLink*/) const {
        AFL_VERIFY(false);
        return TBlobRange();
    }

    IDataSource(const EType type, const ui64 sourceId, const ui32 sourceIdx, const std::shared_ptr<TSpecialReadContext>& context,
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

    virtual ~IDataSource() = default;

    const std::vector<std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>>& GetResourceGuards() const {
        return ResourceGuards;
    }

    std::vector<std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>> ExtractResourceGuards() {
        auto result = std::move(ResourceGuards);
        ResourceGuards.clear();
        return std::move(result);
    }

    virtual THashMap<TChunkAddress, TString> DecodeBlobAddresses(NBlobOperations::NRead::TCompositeReadBlobs&& blobsOriginal) const = 0;

    bool IsSourceInMemory() const {
        AFL_VERIFY(IsSourceInMemoryFlag);
        return *IsSourceInMemoryFlag;
    }

    bool HasSourceInMemoryFlag() const {
        return !!IsSourceInMemoryFlag;
    }

    void SetSourceInMemory(const bool value) {
        AFL_VERIFY(!IsSourceInMemoryFlag);
        IsSourceInMemoryFlag = value;
        if (!value) {
            AFL_VERIFY(StageData);
            StageData->SetUseFilter(value);
        }
    }

    void SetMemoryGroupId(const ui64 groupId) {
        AFL_VERIFY(!MemoryGroupId);
        MemoryGroupId = groupId;
    }

    ui64 GetMemoryGroupId() const {
        AFL_VERIFY(!!MemoryGroupId);
        return *MemoryGroupId;
    }

    virtual ui64 GetColumnsVolume(const std::set<ui32>& columnIds, const EMemType type) const = 0;

    ui64 GetResourceGuardsMemory() const {
        ui64 result = 0;
        for (auto&& i : ResourceGuards) {
            result += i->GetMemory();
        }
        return result;
    }
    void RegisterAllocationGuard(const std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>& guard) {
        ResourceGuards.emplace_back(guard);
    }
    virtual ui64 GetColumnRawBytes(const std::set<ui32>& columnIds) const = 0;
    virtual ui64 GetColumnBlobBytes(const std::set<ui32>& columnsIds) const = 0;

    void AssembleColumns(const std::shared_ptr<TColumnsSet>& columns, const bool sequential = false) {
        if (columns->IsEmpty()) {
            return;
        }
        DoAssembleColumns(columns, sequential);
    }

    bool StartFetchingColumns(const std::shared_ptr<IDataSource>& sourcePtr, const TFetchingScriptCursor& step, const TColumnsSetIds& columns) {
        return DoStartFetchingColumns(sourcePtr, step, columns);
    }

    void ResetSourceFinishedFlag() {
        AFL_VERIFY(AtomicCas(&SourceFinishedSafeFlag, 0, 1));
    }

    void OnSourceFetchingFinishedSafe(IDataReader& owner, const std::shared_ptr<IDataSource>& sourcePtr) {
        AFL_VERIFY(AtomicCas(&SourceFinishedSafeFlag, 1, 0));
        AFL_VERIFY(sourcePtr);
        DoOnSourceFetchingFinishedSafe(owner, sourcePtr);
    }

    void OnEmptyStageData(const std::shared_ptr<NCommon::IDataSource>& sourcePtr) {
        AFL_VERIFY(AtomicCas(&StageResultBuiltFlag, 1, 0));
        AFL_VERIFY(sourcePtr);
        AFL_VERIFY(!StageResult);
        AFL_VERIFY(StageData);
        DoOnEmptyStageData(sourcePtr);
        AFL_VERIFY(StageResult);
        AFL_VERIFY(!StageData);
    }

    template <class T>
    void BuildStageResult(const std::shared_ptr<T>& sourcePtr) {
        BuildStageResult(std::static_pointer_cast<IDataSource>(sourcePtr));
    }

    void BuildStageResult(const std::shared_ptr<IDataSource>& sourcePtr) {
        TMemoryProfileGuard mpg("SCAN_PROFILE::STAGE_RESULT", IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD_SCAN_MEMORY));
        AFL_VERIFY(AtomicCas(&StageResultBuiltFlag, 1, 0));
        AFL_VERIFY(sourcePtr);
        AFL_VERIFY(!StageResult);
        AFL_VERIFY(StageData);
        DoBuildStageResult(sourcePtr);
        AFL_VERIFY(StageResult);
        AFL_VERIFY(!StageData);
    }

    bool AddTxConflict() {
        if (!Context->GetCommonContext()->HasLock()) {
            return false;
        }
        if (DoAddTxConflict()) {
            StageData->Abort();
            return true;
        }
        return false;
    }

    void InitStageData(std::unique_ptr<TFetchedData>&& data) {
        AFL_VERIFY(!StageData);
        StageData = std::move(data);
    }

    std::unique_ptr<TFetchedData> ExtractStageData() {
        AFL_VERIFY(StageData)("source_id", SourceId);
        auto result = std::move(StageData);
        StageData.reset();
        return std::move(result);
    }

    void ClearStageData() {
        StageData.reset();
    }

    const TFetchedData& GetStageData() const {
        AFL_VERIFY(StageData)("source_id", SourceId);
        return *StageData;
    }

    bool HasStageData() const {
        return !!StageData;
    }

    TFetchedData& MutableStageData() {
        AFL_VERIFY(StageData);
        return *StageData;
    }

    bool HasStageResult() const {
        return !!StageResult;
    }

    const TFetchedResult& GetStageResult() const {
        AFL_VERIFY(!!StageResult);
        return *StageResult;
    }

    TFetchedResult& MutableStageResult() {
        AFL_VERIFY(!!StageResult);
        return *StageResult;
    }
};

}   // namespace NKikimr::NOlap::NReader::NCommon

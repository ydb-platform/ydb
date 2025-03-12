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
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/columns_set.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/filtered_scheme.h>
#include <ydb/core/tx/columnshard/resource_subscriber/task.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>

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
private:
    YDB_READONLY(ui64, SourceId, 0);
    YDB_READONLY(ui32, SourceIdx, 0);
    YDB_READONLY(TSnapshot, RecordSnapshotMin, TSnapshot::Zero());
    YDB_READONLY(TSnapshot, RecordSnapshotMax, TSnapshot::Zero());
    YDB_READONLY_DEF(std::shared_ptr<TSpecialReadContext>, Context);
    YDB_READONLY(ui32, RecordsCount, 0);
    YDB_READONLY_DEF(std::optional<ui64>, ShardingVersionOptional);
    YDB_READONLY(bool, HasDeletions, false);
    std::optional<ui64> MemoryGroupId;
    TExecutionContext ExecutionContext;
    virtual bool DoAddTxConflict() = 0;

    virtual ui64 DoGetEntityId() const override {
        return SourceId;
    }

    virtual ui64 DoGetEntityRecordsCount() const override {
        return RecordsCount;
    }

    std::optional<bool> IsSourceInMemoryFlag;
    TAtomic SourceFinishedSafeFlag = 0;
    TAtomic StageResultBuiltFlag = 0;
    virtual void DoOnSourceFetchingFinishedSafe(IDataReader& owner, const std::shared_ptr<IDataSource>& sourcePtr) = 0;
    virtual void DoBuildStageResult(const std::shared_ptr<IDataSource>& sourcePtr) = 0;
    virtual void DoOnEmptyStageData(const std::shared_ptr<NCommon::IDataSource>& sourcePtr) = 0;

    virtual bool DoStartFetchingColumns(
        const std::shared_ptr<IDataSource>& sourcePtr, const TFetchingScriptCursor& step, const TColumnsSetIds& columns) = 0;
    virtual void DoAssembleColumns(const std::shared_ptr<TColumnsSet>& columns, const bool sequential) = 0;

protected:
    std::vector<std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>> ResourceGuards;
    std::unique_ptr<TFetchedData> StageData;
    std::unique_ptr<TFetchedResult> StageResult;

public:
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

    virtual std::optional<TSnapshot> GetDataSnapshot() const {
        return std::nullopt;
    }

    IDataSource(const ui64 sourceId, const ui32 sourceIdx, const std::shared_ptr<TSpecialReadContext>& context,
        const TSnapshot& recordSnapshotMin, const TSnapshot& recordSnapshotMax, const ui32 recordsCount,
        const std::optional<ui64> shardingVersion, const bool hasDeletions)
        : SourceId(sourceId)
        , SourceIdx(sourceIdx)
        , RecordSnapshotMin(recordSnapshotMin)
        , RecordSnapshotMax(recordSnapshotMax)
        , Context(context)
        , RecordsCount(recordsCount)
        , ShardingVersionOptional(shardingVersion)
        , HasDeletions(hasDeletions) {
    }

    virtual ~IDataSource() = default;

    const std::vector<std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>>& GetResourceGuards() const {
        return ResourceGuards;
    }

    virtual THashMap<TChunkAddress, TString> DecodeBlobAddresses(NBlobOperations::NRead::TCompositeReadBlobs&& blobsOriginal) const = 0;

    bool IsSourceInMemory() const {
        AFL_VERIFY(IsSourceInMemoryFlag);
        return *IsSourceInMemoryFlag;
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

    bool HasStageData() const {
        return !!StageData;
    }

    const TFetchedData& GetStageData() const {
        AFL_VERIFY(StageData);
        return *StageData;
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

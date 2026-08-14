#pragma once
#include "context.h"
#include "fetched_data.h"

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
#include <ydb/core/tx/columnshard/engines/reader/common_reader/common/script_counters.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/common/script_cursor.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/filtered_scheme.h>
#include <ydb/core/tx/columnshard/resource_subscriber/task.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>
#include <ydb/core/util/evlog/log.h>

#include <library/cpp/lwtrace/shuttle.h>
#include <util/string/join.h>

#include <atomic>

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

private:
    struct TPrevNodeState {
        ui32 NodeId = 0;
        NArrow::NSSA::IResourceProcessor::EExecutionResult Result = NArrow::NSSA::IResourceProcessor::EExecutionResult::Success;
        bool Failed = false;
        bool Defined = false;
    };

    static_assert(std::atomic<TPrevNodeState>::is_always_lock_free);

    // Prev-node tracing state is written by every finished program-step frame; a continuation frame may
    // still be unwinding concurrently (issue #49169), so mutable TStrings are not allowed here. The
    // whole state fits into one lock-free atomic struct; the category name is resolved on read through
    // the immutable compiled graph.
    TString StartCategoryName;
    std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TCompiledGraph> Program;
    std::atomic<TPrevNodeState> PrevNode = {};

    struct TSourceOwnershipState {
        std::shared_ptr<IDataSource> Source;
    };

    // Logical ownership of the source while a program node Execute() is on the stack. Async work must
    // extract it before arming a callback, leaving the caller with no source to touch while unwinding.
    std::shared_ptr<TSourceOwnershipState> SourceOwnership;

    TString RenderCategoryName(const TPrevNodeState& state) const {
        if (!state.Defined) {
            return StartCategoryName;
        }
        AFL_VERIFY(Program);
        auto it = Program->GetNodes().find(state.NodeId);
        AFL_VERIFY(it != Program->GetNodes().end())("node_id", state.NodeId);
        return it->second->GetProcessor()->GetSignalCategoryName();
    }

public:
    class TSourceOwnershipGuard: TNonCopyable {
    private:
        TExecutionContext& Context;
        const std::shared_ptr<TSourceOwnershipState> State;
        std::shared_ptr<IDataSource>& RestoreTarget;

    public:
        TSourceOwnershipGuard(
            TExecutionContext& context, const std::shared_ptr<TSourceOwnershipState>& state, std::shared_ptr<IDataSource>& restoreTarget)
            : Context(context)
            , State(state)
            , RestoreTarget(restoreTarget)
        {
            AFL_VERIFY(State);
            AFL_VERIFY(State->Source);
            AFL_VERIFY(!RestoreTarget);
        }

        void Restore() {
            AFL_VERIFY(State->Source);
            AFL_VERIFY(Context.SourceOwnership == State);
            AFL_VERIFY(!RestoreTarget);
            RestoreTarget = std::move(State->Source);
            Context.SourceOwnership.reset();
        }

        ~TSourceOwnershipGuard() {
            // If async work extracted the token, State is empty and Context may already be gone.
            if (State->Source) {
                Restore();
            }
        }
    };

    [[nodiscard]] TSourceOwnershipGuard GuardSourceOwnership(
        std::shared_ptr<IDataSource>&& ownership, std::shared_ptr<IDataSource>& restoreTarget) {
        AFL_VERIFY(!SourceOwnership);
        AFL_VERIFY(ownership);
        SourceOwnership = std::make_shared<TSourceOwnershipState>(TSourceOwnershipState{ .Source = std::move(ownership) });
        return TSourceOwnershipGuard(*this, SourceOwnership, restoreTarget);
    }

    std::shared_ptr<IDataSource> ExtractSourceOwnership() {
        AFL_VERIFY(SourceOwnership);
        AFL_VERIFY(SourceOwnership->Source);
        auto state = std::move(SourceOwnership);
        return std::move(state->Source);
    }

    bool HasSourceOwnership() const {
        return SourceOwnership && !!SourceOwnership->Source;
    }

    void SetStartCategoryName(TString&& name) {
        StartCategoryName = std::move(name);
    }

    void SetPrevNodeTracing(const ui32 nodeId, const TConclusion<NArrow::NSSA::IResourceProcessor::EExecutionResult>& conclusion) {
        PrevNode.store(TPrevNodeState{ .NodeId = nodeId,
                           .Result = conclusion.IsFail() ? NArrow::NSSA::IResourceProcessor::EExecutionResult::Success : *conclusion,
                           .Failed = conclusion.IsFail(),
                           .Defined = true }, std::memory_order_release);
    }

    TString GetPrevCategoryName() const {
        return RenderCategoryName(PrevNode.load(std::memory_order_acquire));
    }

    struct TPrevNodeTracing {
        TString CategoryName;
        TString ExecutionResult;
    };

    // CategoryName/ExecutionResult are coupled only in the program-step transition tracing; a single
    // load keeps the pair consistent.
    TPrevNodeTracing GetPrevNodeTracing() const {
        const TPrevNodeState state = PrevNode.load(std::memory_order_acquire);
        TString executionResult;
        if (state.Defined) {
            executionResult = state.Failed ? "Fail" : ::ToString(state.Result);
        }
        return TPrevNodeTracing{ .CategoryName = RenderCategoryName(state), .ExecutionResult = std::move(executionResult) };
    }

    void OnStartProgramStepExecution(const ui32 nodeId, const std::shared_ptr<TFetchingStepSignals>& signals);

    void OnFinishProgramStepExecution();

    void OnFailedProgramStepExecution() {
        OnFinishProgramStepExecution();
    }

    void Start(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TCompiledGraph>& program,
        const TFetchingScriptCursor& step);

    void Stop();

    const TFetchingStepSignals& GetCurrentStepSignalsVerified() const;

    const TFetchingStepSignals* GetCurrentStepSignalsOptional() const;

    bool HasProgramIterator() const {
        return !!ProgramIterator;
    }

    bool HasExecutionVisitor() const {
        return !!ExecutionVisitor;
    }

    std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TExecutionVisitor> GetExecutionVisitorOptional() const {
        return ExecutionVisitor;
    }

    void SetProgramIterator(const std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TCompiledGraph::TIterator>& it,
        const std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TExecutionVisitor>& visitor);

    void SetCursorStep(const TFetchingScriptCursor& step);

    const TFetchingScriptCursor& GetCursorStep() const;

    const std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TCompiledGraph::TIterator>& GetProgramIteratorVerified() const;

    const std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TExecutionVisitor>& GetExecutionVisitorVerified() const;
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
    YDB_READONLY(ui32, SourceIdx, 0);
    YDB_READONLY_DEF(ui64, DeprecatedPortionId);
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
        return SourceIdx;
    }

    virtual ui64 DoGetDeprecatedPortionId() const override {
        return DeprecatedPortionId;
    }

    virtual ui64 DoGetEntityRecordsCount() const override;

    std::optional<bool> IsSourceInMemoryFlag;
    bool InFlightReleasedFlag = false;
    TAtomic SourceFinishedSafeFlag = 0;
    TAtomic StageResultBuiltFlag = 0;
    virtual void DoOnSourceFetchingFinishedSafe(IDataReader& owner, std::shared_ptr<IDataSource>&& sourcePtr) = 0;
    virtual void DoBuildStageResult(const std::shared_ptr<IDataSource>& sourcePtr) = 0;
    virtual void DoOnEmptyStageData(const std::shared_ptr<NCommon::IDataSource>& sourcePtr) = 0;

    virtual TConclusion<bool> DoStartFetchImpl(
        const NArrow::NSSA::TProcessorContext& context, const std::vector<std::shared_ptr<IKernelFetchLogic>>& fetchersExt) = 0;

    virtual TConclusion<bool> DoStartFetch(const NArrow::NSSA::TProcessorContext& context,
        const std::vector<std::shared_ptr<NArrow::NSSA::IFetchLogic>>& fetchersExt) override final;

    virtual bool DoStartFetchingColumns(
        std::shared_ptr<IDataSource>&& sourcePtr, const TFetchingScriptCursor& step, const TColumnsSetIds& columns) = 0;
    virtual void DoAssembleColumns(const std::shared_ptr<TColumnsSet>& columns, const bool sequential) = 0;

    std::optional<NEvLog::TLogsThread> Events;
    std::unique_ptr<TFetchedData> StageData;
    std::shared_ptr<TPortionDataAccessor> Accessor;

protected:
    std::vector<std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>> ResourceGuards;
    NLWTrace::TOrbit DataSourceOrbit;
    TMonotonic LastProbeTimestamp;
    TMonotonic SourcesAheadQueueEnterTime;
    ui32 SourcesAhead = 0;
    TMonotonic SourceCreatedTimestamp;
    TDuration TotalExecutionDuration;
    ui64 TotalBytesRead = 0;
    std::unique_ptr<TFetchedResult> StageResult;
    virtual ui32 GetRecordsCountVirtual() const;

public:
    ui64 GetReservedMemory() const;

    TDuration GetAndResetWaitDuration() {
        const TMonotonic now = TMonotonic::Now();
        const TDuration result = LastProbeTimestamp ? (now - LastProbeTimestamp) : TDuration::Zero();
        LastProbeTimestamp = now;
        return result;
    }

    void SetSourcesAheadQueueEnterTime(const TMonotonic t) {
        SourcesAheadQueueEnterTime = t;
    }

    TDuration GetSourcesAheadQueueWaitDuration() const {
        if (!SourcesAhead || !SourcesAheadQueueEnterTime) {
            return TDuration::Zero();
        }
        return TMonotonic::Now() - SourcesAheadQueueEnterTime;
    }

    void SetSourcesAhead(const ui32 count) {
        SourcesAhead = count;
    }

    ui32 GetSourcesAhead() const {
        return SourcesAhead;
    }

    ui32 GetFilteredRowsCount() const {
        if (!HasStageResult() || GetStageResult().IsEmpty()) {
            return 0;
        }
        const auto& notAppliedFilter = GetStageResult().GetNotAppliedFilter();
        return notAppliedFilter ? notAppliedFilter->GetFilteredCount().value_or(GetStageResult().GetBatch()->num_rows())
                                : GetStageResult().GetBatch()->num_rows();
    }

    void AddExecutionDuration(const TDuration d) {
        TotalExecutionDuration += d;
    }

    void AddBytesRead(const ui64 bytes) {
        TotalBytesRead += bytes;
    }

    void OnStartProcessing();

    TDuration GetTotalDuration() const {
        return SourceCreatedTimestamp ? (TMonotonic::Now() - SourceCreatedTimestamp) : TDuration::Zero();
    }

    TDuration GetTotalExecutionDuration() const {
        return TotalExecutionDuration;
    }

    ui64 GetTotalBytesRead() const {
        return TotalBytesRead;
    }

    ui64 ExtractTotalBytesRead() {
        const ui64 result = TotalBytesRead;
        TotalBytesRead = 0;
        return result;
    }

    NLWTrace::TOrbit& GetDataSourceOrbit() {
        return DataSourceOrbit;
    }

    const NLWTrace::TOrbit& GetDataSourceOrbit() const {
        return DataSourceOrbit;
    }

    const TPortionDataAccessor& GetPortionAccessor() const;

    std::shared_ptr<TPortionDataAccessor> ExtractPortionAccessor();

    bool HasPortionAccessor() const {
        return !!Accessor;
    }

    void SetPortionAccessor(std::shared_ptr<TPortionDataAccessor>&& acc);

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

    template <class T>
    const T* GetOptionalAs() const {
        if (!T::CheckTypeCast(Type)) {
            return nullptr;
        }
        return static_cast<const T*>(this);
    }

    template <class T>
    T* MutableOptionalAs() {
        if (!T::CheckTypeCast(Type)) {
            return nullptr;
        }
        return static_cast<T*>(this);
    }

    virtual bool NeedPortionData() const {
        return true;
    }

    std::optional<ui32> GetRecordsCountOptional() const {
        return RecordsCountImpl;
    }

    virtual void InitRecordsCount(const ui32 recordsCount);

    ui32 GetRecordsCount() const;

    void StartAsyncSection();

    void CheckAsyncSection();

    void StartSyncSection();

    bool IsSyncSection() const {
        return AtomicGet(SyncSectionFlag) == 1;
    }

    void AddEvent(const TString& evDescription);

    TString GetEventsReport() const;

    TExecutionContext& MutableExecutionContext() {
        return ExecutionContext;
    }

    const TExecutionContext& GetExecutionContext() const {
        return ExecutionContext;
    }

    virtual const std::shared_ptr<ISnapshotSchema>& GetSourceSchema() const;

    virtual const std::shared_ptr<ISnapshotSchema>& GetSourceSchemaOptional() const {
        static std::shared_ptr<ISnapshotSchema> defaultValue;
        return defaultValue;
    }

    virtual ui64 GetUsedRawBytesOptional() const {
        return 0;
    }

    virtual TString GetColumnStorageId(const ui32 /*columnId*/) const;

    virtual TString GetEntityStorageId(const ui32 /*entityId*/) const;

    virtual TBlobRange RestoreBlobRange(const TBlobRangeLink16& /*rangeLink*/) const;

    IDataSource(const EType type, const ui32 sourceIdx, const std::shared_ptr<TSpecialReadContext>& context, const TSnapshot& recordSnapshotMin,
        const TSnapshot& recordSnapshotMax, const std::optional<ui32> recordsCount, const std::optional<ui64> shardingVersion,
        const bool hasDeletions, const ui64 deprecatedPortionId);

    virtual ~IDataSource() = default;

    const std::vector<std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>>& GetResourceGuards() const {
        return ResourceGuards;
    }

    std::vector<std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>> ExtractResourceGuards();

    virtual THashMap<TChunkAddress, TString> DecodeBlobAddresses(NBlobOperations::NRead::TCompositeReadBlobs&& blobsOriginal) const = 0;

    bool IsSourceInMemory() const;

    bool HasSourceInMemoryFlag() const {
        return !!IsSourceInMemoryFlag;
    }

    void SetSourceInMemory(const bool value);

    void SetMemoryGroupId(const ui64 groupId);

    ui64 GetMemoryGroupId() const;

    virtual ui64 GetColumnsVolume(const std::set<ui32>& columnIds, const EMemType type) const = 0;

    ui64 GetResourceGuardsMemory() const;

    void RegisterAllocationGuard(const std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>& guard) {
        ResourceGuards.emplace_back(guard);
    }

    virtual ui64 GetColumnRawBytes(const std::set<ui32>& columnIds) const = 0;
    virtual ui64 GetColumnBlobBytes(const std::set<ui32>& columnsIds) const = 0;

    void AssembleColumns(const std::shared_ptr<TColumnsSet>& columns, const bool sequential = false);

    bool StartFetchingColumns(std::shared_ptr<IDataSource>&& sourcePtr, const TFetchingScriptCursor& step, const TColumnsSetIds& columns) {
        return DoStartFetchingColumns(std::move(sourcePtr), step, columns);
    }

    bool IsInFlightReleased() const {
        return InFlightReleasedFlag;
    }

    void SetInFlightReleased() {
        AFL_VERIFY(!InFlightReleasedFlag);
        InFlightReleasedFlag = true;
    }

    void ResetSourceFinishedFlag();

    void OnSourceFetchingFinishedSafe(IDataReader& owner, std::shared_ptr<IDataSource>&& sourcePtr);

    void OnEmptyStageData(const std::shared_ptr<NCommon::IDataSource>& sourcePtr);

    template <class T>
    void BuildStageResult(const std::shared_ptr<T>& sourcePtr) {
        BuildStageResult(std::static_pointer_cast<IDataSource>(sourcePtr));
    }

    void BuildStageResult(const std::shared_ptr<IDataSource>& sourcePtr);

    bool AddTxConflict();

    void InitStageData(std::unique_ptr<TFetchedData>&& data);

    std::unique_ptr<TFetchedData> ExtractStageData();

    void ClearStageData() {
        StageData.reset();
    }

    const TFetchedData& GetStageData() const;

    bool HasStageData() const {
        return !!StageData;
    }

    TFetchedData& MutableStageData();

    bool HasStageResult() const {
        return !!StageResult;
    }

    const TFetchedResult& GetStageResult() const;

    TFetchedResult& MutableStageResult();

    virtual std::optional<ui64> GetPortionIdOptional() const = 0;

    virtual NColumnShard::TInternalPathId GetPathId() const = 0;

    ui64 GetRawPathId() const {
        return GetPathId().GetRawValue();
    }

    ui64 GetTabletId() const {
        return GetContext()->GetCommonContext()->GetReadMetadata()->GetTabletId();
    }

    ui64 GetTxId() const {
        return GetContext()->GetCommonContext()->GetReadMetadata()->GetTxId();
    }
};

}   // namespace NKikimr::NOlap::NReader::NCommon

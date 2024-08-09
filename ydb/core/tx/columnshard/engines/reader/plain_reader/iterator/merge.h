#pragma once
#include "context.h"

#include <ydb/core/formats/arrow/reader/merger.h>
#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/tx/columnshard/counters/scan.h>
#include <ydb/core/tx/columnshard/engines/reader/common/conveyor_task.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>

namespace NKikimr::NOlap::NReader::NPlain {

class TMergingContext {
protected:
    YDB_READONLY_DEF(NArrow::NMerger::TSortableBatchPosition, Start);
    YDB_READONLY_DEF(NArrow::NMerger::TSortableBatchPosition, Finish);
    YDB_READONLY(bool, IncludeFinish, true);
    YDB_READONLY(bool, IncludeStart, false);
    YDB_READONLY(ui32, IntervalIdx, 0);
    bool IsExclusiveIntervalFlag = false;
    std::optional<ui64> IntervalChunkMemory;

public:
    TMergingContext(const NArrow::NMerger::TSortableBatchPosition& start, const NArrow::NMerger::TSortableBatchPosition& finish,
        const ui32 intervalIdx, const bool includeFinish, const bool includeStart, const bool isExclusiveInterval)
        : Start(start)
        , Finish(finish)
        , IncludeFinish(includeFinish)
        , IncludeStart(includeStart)
        , IntervalIdx(intervalIdx)
        , IsExclusiveIntervalFlag(isExclusiveInterval) {
    }

    void SetIntervalChunkMemory(const ui64 value) {
        IntervalChunkMemory = value;
    }

    ui64 GetIntervalChunkMemory() const {
        AFL_VERIFY(IntervalChunkMemory);
        return *IntervalChunkMemory;
    }

    bool IsExclusiveInterval() const {
        return IsExclusiveIntervalFlag;
    }

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("start", Start.DebugJson());
        result.InsertValue("idx", IntervalIdx);
        result.InsertValue("finish", Finish.DebugJson());
        result.InsertValue("include_finish", IncludeFinish);
        result.InsertValue("exclusive", IsExclusiveIntervalFlag);
        return result;
    }
};

class TBaseMergeTask: public IDataTasksProcessor::ITask, public NGroupedMemoryManager::IAllocation {
private:
    using TBase = IDataTasksProcessor::ITask;

protected:
    std::shared_ptr<arrow::Table> ResultBatch;
    std::shared_ptr<arrow::RecordBatch> LastPK;
    const NColumnShard::TCounterGuard Guard;
    std::shared_ptr<TSpecialReadContext> Context;
    mutable std::unique_ptr<NArrow::NMerger::TMergePartialStream> Merger;
    std::shared_ptr<TMergingContext> MergingContext;
    const ui32 IntervalIdx;
    std::optional<NArrow::TShardedRecordBatch> ShardedBatch;
    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> AllocationGuard;

    [[nodiscard]] std::optional<NArrow::NMerger::TCursor> DrainMergerLinearScan(const std::optional<ui32> resultBufferLimit);

    void PrepareResultBatch();

private:
    virtual bool DoApply(IDataReader& indexedDataRead) const override;
    virtual bool DoOnAllocated(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard,
        const std::shared_ptr<NGroupedMemoryManager::IAllocation>& allocation) override;

public:
    TBaseMergeTask(const std::shared_ptr<TMergingContext>& mergingContext, const std::shared_ptr<TSpecialReadContext>& readContext)
        : TBase(readContext->GetCommonContext()->GetScanActorId())
        , Guard(readContext->GetCommonContext()->GetCounters().GetMergeTasksGuard())
        , Context(readContext)
        , MergingContext(mergingContext)
        , IntervalIdx(MergingContext->GetIntervalIdx()) {
    }
};

class TStartMergeTask: public TBaseMergeTask {
private:
    using TBase = TBaseMergeTask;
    bool OnlyEmptySources = true;
    THashMap<ui32, std::shared_ptr<IDataSource>> Sources;

protected:
    virtual TConclusionStatus DoExecuteImpl() override;

public:
    virtual TString GetTaskClassIdentifier() const override {
        return "CS::MERGE_START";
    }

    TStartMergeTask(const std::shared_ptr<TMergingContext>& mergingContext, const std::shared_ptr<TSpecialReadContext>& readContext,
        THashMap<ui32, std::shared_ptr<IDataSource>>&& sources);
};

class TContinueMergeTask: public TBaseMergeTask {
private:
    using TBase = TBaseMergeTask;

protected:
    virtual TConclusionStatus DoExecuteImpl() override;

public:
    virtual TString GetTaskClassIdentifier() const override {
        return "CS::MERGE_CONTINUE";
    }

    TContinueMergeTask(const std::shared_ptr<TMergingContext>& mergingContext, const std::shared_ptr<TSpecialReadContext>& readContext,
        std::unique_ptr<NArrow::NMerger::TMergePartialStream>&& merger)
        : TBase(mergingContext, readContext) {
        AFL_VERIFY(merger);
        Merger = std::move(merger);
    }
};

}   // namespace NKikimr::NOlap::NReader::NPlain

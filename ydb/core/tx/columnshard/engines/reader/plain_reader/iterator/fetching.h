#pragma once
#include "columns_set.h"

#include <ydb/core/tx/columnshard/counters/scan.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/common/conveyor_task.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract_scheme.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap::NReader::NPlain {
class IDataSource;
class TFetchingScriptCursor;
class TSpecialReadContext;
class IFetchingStep {
private:
    YDB_READONLY_DEF(TString, Name);
    YDB_READONLY(TDuration, SumDuration, TDuration::Zero());
    YDB_READONLY(ui64, SumSize, 0);

protected:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const = 0;
    virtual TString DoDebugString() const {
        return "";
    }

public:
    void AddDuration(const TDuration d) {
        SumDuration += d;
    }
    void AddDataSize(const ui64 size) {
        SumSize += size;
    }
    virtual ui64 DoPredictRawBytes(const std::shared_ptr<IDataSource>& /*source*/) const {
        return 0;
    }
    virtual bool DoInitSourceSeqColumnIds(const std::shared_ptr<IDataSource>& /*source*/) const {
        return false;
    }

    virtual ~IFetchingStep() = default;

    [[nodiscard]] TConclusion<bool> ExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const {
        return DoExecuteInplace(source, step);
    }

    virtual ui64 GetProcessingDataSize(const std::shared_ptr<IDataSource>& /*source*/) const {
        return 0;
    }

    IFetchingStep(const TString& name)
        : Name(name) {
    }

    TString DebugString() const {
        TStringBuilder sb;
        sb << "name=" << Name << ";duration=" << SumDuration << ";"
           << "size=" << 1e-9 * SumSize << ";details={" << DoDebugString() << "};";
        return sb;
    }
};

class TFetchingScript {
private:
    YDB_ACCESSOR(TString, BranchName, "UNDEFINED");
    std::vector<std::shared_ptr<IFetchingStep>> Steps;
    std::optional<TMonotonic> StartInstant;
    std::optional<TMonotonic> FinishInstant;
    const ui32 Limit;

public:
    TFetchingScript(const TSpecialReadContext& context);

    void AddStepDataSize(const ui32 index, const ui64 size) {
        GetStep(index)->AddDataSize(size);
    }

    void AddStepDuration(const ui32 index, const TDuration d) {
        FinishInstant = TMonotonic::Now();
        GetStep(index)->AddDuration(d);
    }

    void OnExecute() {
        if (!StartInstant) {
            StartInstant = TMonotonic::Now();
        }
    }

    TString DebugString() const;

    const std::shared_ptr<IFetchingStep>& GetStep(const ui32 index) const {
        AFL_VERIFY(index < Steps.size());
        return Steps[index];
    }

    ui64 PredictRawBytes(const std::shared_ptr<IDataSource>& source) const {
        ui64 result = 0;
        for (auto&& current : Steps) {
            result += current->DoPredictRawBytes(source);
        }
        return result;
    }

    void AddStep(const std::shared_ptr<IFetchingStep>& step) {
        AFL_VERIFY(step);
        Steps.emplace_back(step);
    }

    bool InitSourceSeqColumnIds(const std::shared_ptr<IDataSource>& source) const {
        for (auto it = Steps.rbegin(); it != Steps.rend(); ++it) {
            if ((*it)->DoInitSourceSeqColumnIds(source)) {
                return true;
            }
        }
        return false;
    }

    bool IsFinished(const ui32 currentStepIdx) const {
        AFL_VERIFY(currentStepIdx <= Steps.size());
        return currentStepIdx == Steps.size();
    }

    ui32 Execute(const ui32 startStepIdx, const std::shared_ptr<IDataSource>& source) const;
};

class TFetchingScriptCursor {
private:
    std::optional<TMonotonic> CurrentStartInstant;
    std::optional<ui64> CurrentStartDataSize;
    ui32 CurrentStepIdx = 0;
    std::shared_ptr<TFetchingScript> Script;
    void FlushDuration() {
        AFL_VERIFY(CurrentStartInstant);
        AFL_VERIFY(CurrentStartDataSize);
        Script->AddStepDuration(CurrentStepIdx, TMonotonic::Now() - *CurrentStartInstant);
        Script->AddStepDataSize(CurrentStepIdx, *CurrentStartDataSize);
        CurrentStartInstant.reset();
        CurrentStartDataSize.reset();
    }

public:
    TFetchingScriptCursor(const std::shared_ptr<TFetchingScript>& script, const ui32 index)
        : CurrentStepIdx(index)
        , Script(script) {
    }

    const TString& GetName() const {
        return Script->GetStep(CurrentStepIdx)->GetName();
    }

    TString DebugString() const {
        return Script->GetStep(CurrentStepIdx)->DebugString();
    }

    bool Next() {
        FlushDuration();
        return !Script->IsFinished(++CurrentStepIdx);
    }

    TConclusion<bool> Execute(const std::shared_ptr<IDataSource>& source);
};

class TStepAction: public IDataTasksProcessor::ITask {
private:
    using TBase = IDataTasksProcessor::ITask;
    std::shared_ptr<IDataSource> Source;
    TFetchingScriptCursor Cursor;
    bool FinishedFlag = false;

protected:
    virtual bool DoApply(IDataReader& owner) const override;
    virtual TConclusionStatus DoExecuteImpl() override;

public:
    virtual TString GetTaskClassIdentifier() const override {
        return "STEP_ACTION";
    }

    TStepAction(const std::shared_ptr<IDataSource>& source, TFetchingScriptCursor&& cursor, const NActors::TActorId& ownerActorId)
        : TBase(ownerActorId)
        , Source(source)
        , Cursor(std::move(cursor)) {
    }
};

class TBuildFakeSpec: public IFetchingStep {
private:
    using TBase = IFetchingStep;
    const ui32 Count = 0;

protected:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
    virtual ui64 DoPredictRawBytes(const std::shared_ptr<IDataSource>& /*source*/) const override {
        return TIndexInfo::GetSpecialColumnsRecordSize() * Count;
    }

public:
    TBuildFakeSpec(const ui32 count)
        : TBase("FAKE_SPEC")
        , Count(count) {
        AFL_VERIFY(Count);
    }
};

class TApplyIndexStep: public IFetchingStep {
private:
    using TBase = IFetchingStep;
    const NIndexes::TIndexCheckerContainer IndexChecker;

protected:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;

public:
    TApplyIndexStep(const NIndexes::TIndexCheckerContainer& indexChecker)
        : TBase("APPLY_INDEX")
        , IndexChecker(indexChecker) {
    }
};

class TAllocateMemoryStep: public IFetchingStep {
private:
    using TBase = IFetchingStep;
    std::shared_ptr<TColumnsSet> Columns;
    const EStageFeaturesIndexes StageIndex;

protected:
    class TFetchingStepAllocation: public NGroupedMemoryManager::IAllocation {
    private:
        using TBase = NGroupedMemoryManager::IAllocation;
        std::weak_ptr<IDataSource> Source;
        TFetchingScriptCursor Step;
        NColumnShard::TCounterGuard TasksGuard;
        virtual bool DoOnAllocated(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard,
            const std::shared_ptr<NGroupedMemoryManager::IAllocation>& allocation) override;

    public:
        TFetchingStepAllocation(const std::shared_ptr<IDataSource>& source, const ui64 mem, const TFetchingScriptCursor& step);
    };

    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
    virtual ui64 GetProcessingDataSize(const std::shared_ptr<IDataSource>& source) const override;
    virtual ui64 DoPredictRawBytes(const std::shared_ptr<IDataSource>& /*source*/) const override {
        return 0;
    }
    virtual TString DoDebugString() const override {
        return TStringBuilder() << "columns=" << Columns->DebugString() << ";stage=" << StageIndex << ";";
    }

public:
    TAllocateMemoryStep(const std::shared_ptr<TColumnsSet>& columns, const EStageFeaturesIndexes stageIndex)
        : TBase("ALLOCATE_MEMORY::" + ::ToString(stageIndex))
        , Columns(columns)
        , StageIndex(stageIndex) {
        AFL_VERIFY(Columns);
        AFL_VERIFY(Columns->GetColumnsCount());
    }
};

class TColumnBlobsFetchingStep: public IFetchingStep {
private:
    using TBase = IFetchingStep;
    std::shared_ptr<TColumnsSet> Columns;

protected:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
    virtual ui64 DoPredictRawBytes(const std::shared_ptr<IDataSource>& source) const override;
    virtual TString DoDebugString() const override {
        return TStringBuilder() << "columns=" << Columns->DebugString() << ";";
    }

public:
    virtual ui64 GetProcessingDataSize(const std::shared_ptr<IDataSource>& source) const override;
    TColumnBlobsFetchingStep(const std::shared_ptr<TColumnsSet>& columns)
        : TBase("FETCHING_COLUMNS")
        , Columns(columns) {
        AFL_VERIFY(Columns);
        AFL_VERIFY(Columns->GetColumnsCount());
    }
};

class TIndexBlobsFetchingStep: public IFetchingStep {
private:
    using TBase = IFetchingStep;
    std::shared_ptr<TIndexesSet> Indexes;

protected:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
    virtual ui64 DoPredictRawBytes(const std::shared_ptr<IDataSource>& source) const override;
    virtual TString DoDebugString() const override {
        return TStringBuilder() << "indexes=" << Indexes->DebugString() << ";";
    }

public:
    TIndexBlobsFetchingStep(const std::shared_ptr<TIndexesSet>& indexes)
        : TBase("FETCHING_INDEXES")
        , Indexes(indexes) {
        AFL_VERIFY(Indexes);
        AFL_VERIFY(Indexes->GetIndexesCount());
    }
};

class TAssemblerStep: public IFetchingStep {
private:
    using TBase = IFetchingStep;
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, Columns);
    virtual TString DoDebugString() const override {
        return TStringBuilder() << "columns=" << Columns->DebugString() << ";";
    }

public:
    virtual ui64 GetProcessingDataSize(const std::shared_ptr<IDataSource>& source) const override;
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
    TAssemblerStep(const std::shared_ptr<TColumnsSet>& columns, const TString& specName = Default<TString>())
        : TBase("ASSEMBLER" + (specName ? "::" + specName : ""))
        , Columns(columns) {
        AFL_VERIFY(Columns);
        AFL_VERIFY(Columns->GetColumnsCount());
    }
};

class TOptionalAssemblerStep: public IFetchingStep {
private:
    using TBase = IFetchingStep;
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, Columns);
    virtual TString DoDebugString() const override {
        return TStringBuilder() << "columns=" << Columns->DebugString() << ";";
    }

protected:
    virtual bool DoInitSourceSeqColumnIds(const std::shared_ptr<IDataSource>& source) const override;

public:
    virtual ui64 GetProcessingDataSize(const std::shared_ptr<IDataSource>& source) const override;

    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
    TOptionalAssemblerStep(const std::shared_ptr<TColumnsSet>& columns, const TString& specName = Default<TString>())
        : TBase("OPTIONAL_ASSEMBLER" + (specName ? "::" + specName : ""))
        , Columns(columns) {
        AFL_VERIFY(Columns);
        AFL_VERIFY(Columns->GetColumnsCount());
    }
};

class TFilterProgramStep: public IFetchingStep {
private:
    using TBase = IFetchingStep;
    std::shared_ptr<NSsa::TProgramStep> Step;

protected:
    virtual ui64 DoPredictRawBytes(const std::shared_ptr<IDataSource>& source) const override;

public:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
    TFilterProgramStep(const std::shared_ptr<NSsa::TProgramStep>& step)
        : TBase("PROGRAM")
        , Step(step) {
    }
};

class TFilterCutLimit: public IFetchingStep {
private:
    using TBase = IFetchingStep;
    const ui32 Limit;
    const bool Reverse;

protected:
    virtual ui64 DoPredictRawBytes(const std::shared_ptr<IDataSource>& /*source*/) const override {
        return 0;
    }

public:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
    TFilterCutLimit(const ui32 limit, const bool reverse)
        : TBase("LIMIT")
        , Limit(limit)
        , Reverse(reverse)
    {
    }
};

class TPredicateFilter: public IFetchingStep {
private:
    using TBase = IFetchingStep;

public:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
    TPredicateFilter()
        : TBase("PREDICATE") {
    }
};

class TSnapshotFilter: public IFetchingStep {
private:
    using TBase = IFetchingStep;

public:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
    TSnapshotFilter()
        : TBase("SNAPSHOT") {
    }
};

class TDeletionFilter: public IFetchingStep {
private:
    using TBase = IFetchingStep;

public:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
    TDeletionFilter()
        : TBase("DELETION") {
    }
};

class TShardingFilter: public IFetchingStep {
private:
    using TBase = IFetchingStep;

public:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
    TShardingFilter()
        : TBase("SHARDING") {
    }
};

}   // namespace NKikimr::NOlap::NReader::NPlain

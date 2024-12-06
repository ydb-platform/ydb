#pragma once
#include <ydb/core/tx/columnshard/counters/scan.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/common/conveyor_task.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/columns_set.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract_scheme.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap::NReader::NSimple {

using TColumnsSet = NCommon::TColumnsSet;
using TIndexesSet = NCommon::TIndexesSet;
using EStageFeaturesIndexes = NCommon::EStageFeaturesIndexes;
using TColumnsSetIds = NCommon::TColumnsSetIds;
using EMemType = NCommon::EMemType;

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

    void Allocation(const std::set<ui32>& entityIds, const EStageFeaturesIndexes stage, const EMemType mType);

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

    template <class T, typename... Args>
    std::shared_ptr<T> AddStep(Args... args) {
        auto result = std::make_shared<T>(args...);
        Steps.emplace_back(result);
        return result;
    }

    template <class T, typename... Args>
    std::shared_ptr<T> InsertStep(const ui32 index, Args... args) {
        AFL_VERIFY(index <= Steps.size())("index", index)("size", Steps.size());
        auto result = std::make_shared<T>(args...);
        Steps.insert(Steps.begin() + index, result);
        return result;
    }

    void AddStep(const std::shared_ptr<IFetchingStep>& step) {
        AFL_VERIFY(step);
        Steps.emplace_back(step);
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
        AFL_VERIFY(!Script->IsFinished(CurrentStepIdx));
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
    const NColumnShard::TCounterGuard CountersGuard;

protected:
    virtual bool DoApply(IDataReader& owner) const override;
    virtual TConclusionStatus DoExecuteImpl() override;

public:
    virtual TString GetTaskClassIdentifier() const override {
        return "STEP_ACTION";
    }

    TStepAction(const std::shared_ptr<IDataSource>& source, TFetchingScriptCursor&& cursor, const NActors::TActorId& ownerActorId);
};

class TBuildFakeSpec: public IFetchingStep {
private:
    using TBase = IFetchingStep;
    const ui32 Count = 0;

protected:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;

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
    class TColumnsPack {
    private:
        YDB_READONLY_DEF(TColumnsSetIds, Columns);
        YDB_READONLY(EMemType, MemType, EMemType::Blob);

    public:
        TColumnsPack(const TColumnsSetIds& columns, const EMemType memType)
            : Columns(columns)
            , MemType(memType) {
        }
    };
    std::vector<TColumnsPack> Packs;
    THashMap<ui32, THashSet<EMemType>> Control;
    const EStageFeaturesIndexes StageIndex;
    const std::optional<ui64> PredefinedSize;

protected:
    class TFetchingStepAllocation: public NGroupedMemoryManager::IAllocation {
    private:
        using TBase = NGroupedMemoryManager::IAllocation;
        std::weak_ptr<IDataSource> Source;
        TFetchingScriptCursor Step;
        NColumnShard::TCounterGuard TasksGuard;
        const EStageFeaturesIndexes StageIndex;
        virtual bool DoOnAllocated(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard,
            const std::shared_ptr<NGroupedMemoryManager::IAllocation>& allocation) override;
        virtual void DoOnAllocationImpossible(const TString& errorMessage) override;

    public:
        TFetchingStepAllocation(const std::shared_ptr<IDataSource>& source, const ui64 mem, const TFetchingScriptCursor& step,
            const EStageFeaturesIndexes stageIndex);
    };
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
    virtual ui64 GetProcessingDataSize(const std::shared_ptr<IDataSource>& source) const override;
    virtual TString DoDebugString() const override {
        return TStringBuilder() << "stage=" << StageIndex << ";";
    }

public:
    void AddAllocation(const TColumnsSetIds& ids, const EMemType memType) {
        if (!ids.GetColumnsCount()) {
            return;
        }
        for (auto&& i : ids.GetColumnIds()) {
            AFL_VERIFY(Control[i].emplace(memType).second);
        }
        Packs.emplace_back(ids, memType);
    }
    EStageFeaturesIndexes GetStage() const {
        return StageIndex;
    }

    TAllocateMemoryStep(const TColumnsSetIds& columns, const EMemType memType, const EStageFeaturesIndexes stageIndex)
        : TBase("ALLOCATE_MEMORY::" + ::ToString(stageIndex))
        , StageIndex(stageIndex) {
        AddAllocation(columns, memType);
    }

    TAllocateMemoryStep(const ui64 memSize, const EStageFeaturesIndexes stageIndex)
        : TBase("ALLOCATE_MEMORY::" + ::ToString(stageIndex))
        , StageIndex(stageIndex)
        , PredefinedSize(memSize) {
    }
};

class TDetectInMemStep: public IFetchingStep {
private:
    using TBase = IFetchingStep;
    const TColumnsSetIds Columns;

protected:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
    virtual TString DoDebugString() const override {
        return TStringBuilder() << "columns=" << Columns.DebugString() << ";";
    }

public:
    virtual ui64 GetProcessingDataSize(const std::shared_ptr<IDataSource>& source) const override;
    TDetectInMemStep(const TColumnsSetIds& columns)
        : TBase("FETCHING_COLUMNS")
        , Columns(columns) {
        AFL_VERIFY(Columns.GetColumnsCount());
    }
};

class TPrepareResultStep: public IFetchingStep {
private:
    using TBase = IFetchingStep;

protected:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
    virtual TString DoDebugString() const override {
        return TStringBuilder();
    }

public:
    virtual ui64 GetProcessingDataSize(const std::shared_ptr<IDataSource>& /*source*/) const override {
        return 0;
    }
    TPrepareResultStep()
        : TBase("PREPARE_RESULT") {
    }
};

class TBuildResultStep: public IFetchingStep {
private:
    using TBase = IFetchingStep;
    const ui32 StartIndex;
    const ui32 RecordsCount;

protected:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
    virtual TString DoDebugString() const override {
        return TStringBuilder();
    }

public:
    virtual ui64 GetProcessingDataSize(const std::shared_ptr<IDataSource>& /*source*/) const override {
        return 0;
    }
    TBuildResultStep(const ui32 startIndex, const ui32 recordsCount)
        : TBase("BUILD_RESULT")
        , StartIndex(startIndex)
        , RecordsCount(recordsCount) {
    }
};

class TColumnBlobsFetchingStep: public IFetchingStep {
private:
    using TBase = IFetchingStep;
    TColumnsSetIds Columns;

protected:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
    virtual TString DoDebugString() const override {
        return TStringBuilder() << "columns=" << Columns.DebugString() << ";";
    }

public:
    virtual ui64 GetProcessingDataSize(const std::shared_ptr<IDataSource>& source) const override;
    TColumnBlobsFetchingStep(const TColumnsSetIds& columns)
        : TBase("FETCHING_COLUMNS")
        , Columns(columns) {
        AFL_VERIFY(Columns.GetColumnsCount());
    }
};

class TPortionAccessorFetchingStep: public IFetchingStep {
private:
    using TBase = IFetchingStep;

protected:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
    virtual TString DoDebugString() const override {
        return TStringBuilder();
    }

public:
    TPortionAccessorFetchingStep()
        : TBase("FETCHING_ACCESSOR") {
    }
};

class TIndexBlobsFetchingStep: public IFetchingStep {
private:
    using TBase = IFetchingStep;
    std::shared_ptr<TIndexesSet> Indexes;

protected:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
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

public:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
    TFilterCutLimit(const ui32 limit, const bool reverse)
        : TBase("LIMIT")
        , Limit(limit)
        , Reverse(reverse) {
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

class TDetectInMem: public IFetchingStep {
private:
    using TBase = IFetchingStep;
    TColumnsSetIds Columns;

public:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
    TDetectInMem(const TColumnsSetIds& columns)
        : TBase("DETECT_IN_MEM")
        , Columns(columns) {
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

}   // namespace NKikimr::NOlap::NReader::NSimple

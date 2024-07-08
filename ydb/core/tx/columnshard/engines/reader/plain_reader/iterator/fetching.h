#pragma once
#include "columns_set.h"
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract_scheme.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/common/conveyor_task.h>

namespace NKikimr::NOlap::NReader::NPlain {
class IDataSource;
class TFetchingScriptCursor;
class IFetchingStep {
private:
    YDB_READONLY_DEF(TString, Name);
protected:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const = 0;
    virtual TString DoDebugString() const {
        return "";
    }
public:
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

    IFetchingStep(const TString& name)
        : Name(name)
    {

    }

    TString DebugString() const {
        TStringBuilder sb;
        sb << "name=" << Name << ";details={" << DoDebugString() << "};";
        return sb;
    }
};

class TFetchingScript {
private:
    YDB_ACCESSOR(TString, BranchName, "UNDEFINED");
    std::vector<std::shared_ptr<IFetchingStep>> Steps;
public:
    TFetchingScript() = default;

    TString DebugString() const {
        TStringBuilder sb;
        sb << "[";
        for (auto&& i : Steps) {
            sb << "{" << i->DebugString() << "};";
        }
        sb << "]";
        return sb;
    }

    const std::shared_ptr<IFetchingStep>& GetStep(const ui32 index) const {
        AFL_VERIFY(index < Steps.size());
        return Steps[index];
    }

    ui64 PredictRawBytes(const std::shared_ptr<IDataSource>& source) const {
        ui64 result = 0;
        for (auto&& current: Steps) {
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
    ui32 CurrentStepIdx = 0;
    std::shared_ptr<TFetchingScript> Script;
public:
    TFetchingScriptCursor(const std::shared_ptr<TFetchingScript>& script, const ui32 index)
        : CurrentStepIdx(index)
        , Script(script)
    {

    }

    const TString& GetName() const {
        return Script->GetStep(CurrentStepIdx)->GetName();
    }

    TString DebugString() const {
        return Script->GetStep(CurrentStepIdx)->DebugString();
    }

    bool Next() {
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
        , Cursor(std::move(cursor))
    {

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
        , Count(count)
    {
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
        , IndexChecker(indexChecker)
    {

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
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
    TAssemblerStep(const std::shared_ptr<TColumnsSet>& columns, const TString& specName = Default<TString>())
        : TBase("ASSEMBLER" + (specName ? "::" + specName : ""))
        , Columns(columns)
    {
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
        , Step(step)
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

class TSnapshotFilter : public IFetchingStep {
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

class TShardingFilter : public IFetchingStep {
private:
    using TBase = IFetchingStep;

public:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
    TShardingFilter()
        : TBase("SHARDING") {
    }
};


}

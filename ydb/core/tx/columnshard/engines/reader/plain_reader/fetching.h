#pragma once
#include "columns_set.h"
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract_scheme.h>
#include <ydb/core/tx/columnshard/engines/reader/conveyor_task.h>
#include <ydb/core/tx/columnshard/engines/reader/read_metadata.h>

namespace NKikimr::NOlap::NPlainReader {
class IDataSource;

class IFetchingStep {
private:
    std::shared_ptr<IFetchingStep> NextStep;
    YDB_READONLY_DEF(TString, Name);
    YDB_READONLY(ui32, Index, 0);
    YDB_READONLY_DEF(TString, BranchName);
protected:
    virtual bool DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFetchingStep>& step) const = 0;
    virtual TString DoDebugString() const {
        return "";
    }
public:
    virtual ~IFetchingStep() = default;

    std::shared_ptr<IFetchingStep> AttachNext(const std::shared_ptr<IFetchingStep>& nextStep) {
        AFL_VERIFY(nextStep);
        NextStep = nextStep;
        nextStep->Index = Index + 1;
        nextStep->BranchName = BranchName;
        return nextStep;
    }

    virtual ui64 PredictRawBytes(const std::shared_ptr<IDataSource>& /*source*/) const {
        return 0;
    }

    bool ExecuteInplace(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFetchingStep>& step) const {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("scan_step", DebugString())("scan_step_idx", GetIndex());
        return DoExecuteInplace(source, step);
    }

    const std::shared_ptr<IFetchingStep>& GetNextStep() const {
        return NextStep;
    }

    IFetchingStep(const TString& name, const TString& branchName = Default<TString>())
        : Name(name)
        , BranchName(branchName)
    {

    }

    TString DebugString() const {
        TStringBuilder sb;
        sb << "name=" << Name << ";" << DoDebugString() << ";branch=" << BranchName << ";";
        if (NextStep) {
            sb << "next=" << NextStep->DebugString() << ";";
        }
        return sb;
    }
};

class TStepAction: public NColumnShard::IDataTasksProcessor::ITask {
private:
    using TBase = NColumnShard::IDataTasksProcessor::ITask;
    std::shared_ptr<IDataSource> Source;
    std::shared_ptr<IFetchingStep> Step;
    bool FinishedFlag = false;
protected:
    virtual bool DoApply(IDataReader& /*owner*/) const override;
    virtual bool DoExecute() override;
public:
    virtual TString GetTaskClassIdentifier() const override {
        return "STEP_ACTION";
    }

    TStepAction(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFetchingStep>& step, const NActors::TActorId& ownerActorId)
        : TBase(ownerActorId)
        , Source(source)
        , Step(step)
    {

    }
};

class TBuildFakeSpec: public IFetchingStep {
private:
    using TBase = IFetchingStep;
    const ui32 Count = 0;
protected:
    virtual bool DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFetchingStep>& /*step*/) const override;
public:
    TBuildFakeSpec(const ui32 count, const TString& nameBranch = "")
        : TBase("FAKE_SPEC", nameBranch)
        , Count(count)
    {
        AFL_VERIFY(Count);
    }
};

class TBlobsFetchingStep: public IFetchingStep {
private:
    using TBase = IFetchingStep;
    std::shared_ptr<TColumnsSet> Columns;
protected:
    virtual bool DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFetchingStep>& step) const override;
    virtual ui64 PredictRawBytes(const std::shared_ptr<IDataSource>& source) const override;
    virtual TString DoDebugString() const override {
        return TStringBuilder() << "columns=" << Columns->DebugString() << ";";
    }
public:
    TBlobsFetchingStep(const std::shared_ptr<TColumnsSet>& columns, const TString& nameBranch = "")
        : TBase("FETCHING", nameBranch)
        , Columns(columns)
    {
        AFL_VERIFY(Columns);
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
    virtual bool DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFetchingStep>& /*step*/) const override;
    TAssemblerStep(const std::shared_ptr<TColumnsSet>& columns)
        : TBase("ASSEMBLER")
        , Columns(columns)
    {
        AFL_VERIFY(Columns);
    }
};

class TFilterProgramStep: public IFetchingStep {
private:
    using TBase = IFetchingStep;
    std::shared_ptr<NSsa::TProgramStep> Step;
public:
    virtual bool DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFetchingStep>& step) const override;
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
    virtual bool DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFetchingStep>& step) const override;
    TPredicateFilter()
        : TBase("PREDICATE") {

    }
};

class TSnapshotFilter: public IFetchingStep {
private:
    using TBase = IFetchingStep;
public:
    virtual bool DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFetchingStep>& step) const override;
    TSnapshotFilter()
        : TBase("SNAPSHOT") {

    }
};

}

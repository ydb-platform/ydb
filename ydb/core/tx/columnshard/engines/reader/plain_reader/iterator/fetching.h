#pragma once
#include "columns_set.h"
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract_scheme.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/common/conveyor_task.h>

namespace NKikimr::NOlap::NReader::NPlain {
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
    virtual ui64 DoPredictRawBytes(const std::shared_ptr<IDataSource>& /*source*/) const {
        return 0;
    }
    virtual bool DoInitSourceSeqColumnIds(const std::shared_ptr<IDataSource>& /*source*/) const {
        return false;
    }

public:
    virtual ~IFetchingStep() = default;

    bool InitSourceSeqColumnIds(const std::shared_ptr<IDataSource>& source) const {
        auto* current = this;
        while (current) {
            if (current->DoInitSourceSeqColumnIds(source)) {
                return true;
            } else {
                current = current->NextStep.get();
            }
        }
        return false;
    }

    std::shared_ptr<IFetchingStep> AttachNext(const std::shared_ptr<IFetchingStep>& nextStep) {
        AFL_VERIFY(nextStep);
        NextStep = nextStep;
        nextStep->Index = Index + 1;
        if (!nextStep->BranchName) {
            nextStep->BranchName = BranchName;
        }
        return nextStep;
    }

    ui64 PredictRawBytes(const std::shared_ptr<IDataSource>& source) const {
        ui64 result = 0;
        auto* current = this;
        while (current) {
            result += current->DoPredictRawBytes(source);
            current = current->NextStep.get();
        }
        return result;
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

class TStepAction: public IDataTasksProcessor::ITask {
private:
    using TBase = IDataTasksProcessor::ITask;
    std::shared_ptr<IDataSource> Source;
    std::shared_ptr<IFetchingStep> Step;
    bool FinishedFlag = false;
protected:
    virtual bool DoApply(IDataReader& owner) const override;
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
    virtual ui64 DoPredictRawBytes(const std::shared_ptr<IDataSource>& /*source*/) const override {
        return TIndexInfo::GetSpecialColumnsRecordSize() * Count;
    }
public:
    TBuildFakeSpec(const ui32 count, const TString& nameBranch = "")
        : TBase("FAKE_SPEC", nameBranch)
        , Count(count)
    {
        AFL_VERIFY(Count);
    }
};

class TFakeStep: public IFetchingStep {
private:
    using TBase = IFetchingStep;
protected:
    virtual bool DoExecuteInplace(const std::shared_ptr<IDataSource>& /*source*/, const std::shared_ptr<IFetchingStep>& /*step*/) const override {
        return true;
    }

public:
    TFakeStep()
        : TBase("FAKE")
    {

    }
};

class TApplyIndexStep: public IFetchingStep {
private:
    using TBase = IFetchingStep;
    const NIndexes::TIndexCheckerContainer IndexChecker;
protected:
    virtual bool DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFetchingStep>& /*step*/) const override;
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
    virtual bool DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFetchingStep>& step) const override;
    virtual ui64 DoPredictRawBytes(const std::shared_ptr<IDataSource>& source) const override;
    virtual TString DoDebugString() const override {
        return TStringBuilder() << "columns=" << Columns->DebugString() << ";";
    }
public:
    TColumnBlobsFetchingStep(const std::shared_ptr<TColumnsSet>& columns, const TString& nameBranch = "")
        : TBase("FETCHING_COLUMNS", nameBranch)
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
    virtual bool DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFetchingStep>& step) const override;
    virtual ui64 DoPredictRawBytes(const std::shared_ptr<IDataSource>& source) const override;
    virtual TString DoDebugString() const override {
        return TStringBuilder() << "indexes=" << Indexes->DebugString() << ";";
    }
public:
    TIndexBlobsFetchingStep(const std::shared_ptr<TIndexesSet>& indexes, const TString& nameBranch = "")
        : TBase("FETCHING_INDEXES", nameBranch)
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
    virtual bool DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFetchingStep>& /*step*/) const override;
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
    virtual bool DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFetchingStep>& /*step*/) const override;
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

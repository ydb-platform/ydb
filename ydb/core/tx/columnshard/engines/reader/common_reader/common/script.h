#pragma once
#include "columns_set.h"
#include "script_counters.h"

#include <ydb/core/formats/arrow/program/abstract.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/conclusion/result.h>

namespace NKikimr::NOlap::NReader::NCommon {
class IDataSource;
class TFetchingScriptCursor;

class IFetchingStep: public TNonCopyable {
private:
    YDB_READONLY_DEF(TString, Name);
    TAtomicCounter SumDuration;
    TAtomicCounter SumSize;
    std::shared_ptr<TFetchingStepSignals> Signals;

protected:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const = 0;
    virtual TString DoDebugString() const {
        return "";
    }

public:
    TDuration GetSumDuration() const {
        return TDuration::MicroSeconds(SumDuration.Val());
    }

    ui64 GetSumSize() const {
        return SumSize.Val();
    }

    void AddExecutionDuration(const TDuration dLocal, const TDuration dGlobal) {
        SumDuration.Add(dLocal.MicroSeconds());
        Signals->AddExecutionDuration(dLocal);
        Signals->AddTotalDuration(dGlobal);
    }

    virtual ~IFetchingStep() = default;

    [[nodiscard]] TConclusion<bool> ExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const {
        return DoExecuteInplace(source, step);
    }

    virtual ui64 GetProcessingDataSize(const std::shared_ptr<IDataSource>& /*source*/) const {
        return 0;
    }

    IFetchingStep(const TString& name)
        : Name(name)
        , Signals(TFetchingStepsSignalsCollection::GetSignals(name)) {
    }

    TString DebugString(const bool stats = false) const;
};

class TFetchingScript {
private:
    YDB_READONLY_DEF(TString, BranchName);
    std::vector<std::shared_ptr<IFetchingStep>> Steps;
    TAtomic StartInstant;
    TAtomic FinishInstant;
    bool Started = false;

public:
    TFetchingScript(const TString& branchName, std::vector<std::shared_ptr<IFetchingStep>>&& steps)
        : BranchName(branchName)
        , Steps(std::move(steps)) {
    }

    void AddStepDuration(const ui32 index, const TDuration dLocal, const TDuration dGlobal) {
        AtomicSet(FinishInstant, TMonotonic::Now().MicroSeconds());
        GetStep(index)->AddExecutionDuration(dLocal, dGlobal);
    }

    void OnExecute() {
        if (!Started) {
            AtomicSet(StartInstant, TMonotonic::Now().MicroSeconds());
            Started = true;
        }
    }

    TString DebugString() const;
    TString ProfileDebugString() const;

    const std::shared_ptr<IFetchingStep>& GetStep(const ui32 index) const {
        AFL_VERIFY(index < Steps.size());
        return Steps[index];
    }

    bool IsFinished(const ui32 currentStepIdx) const {
        AFL_VERIFY(currentStepIdx <= Steps.size());
        return currentStepIdx == Steps.size();
    }

    ui32 Execute(const ui32 startStepIdx, const std::shared_ptr<IDataSource>& source) const;
};

class TFetchingScriptOwner: TNonCopyable {
private:
    TAtomic InitializationDetector = 0;
    std::shared_ptr<TFetchingScript> Script;

    void FinishInitialization(std::shared_ptr<TFetchingScript>&& script) {
        Script = std::move(script);
        AFL_VERIFY(AtomicCas(&InitializationDetector, 1, 2));
    }

public:
    const std::shared_ptr<TFetchingScript>& GetScriptVerified() const {
        AFL_VERIFY(Script);
        return Script;
    }

    TString ProfileDebugString() const {
        if (Script) {
            return TStringBuilder() << Script->ProfileDebugString() << Endl;
        } else {
            return TStringBuilder() << "NO_SCRIPT" << Endl;
        }
    }

    bool HasScript() const {
        return !!Script;
    }

    bool NeedInitialization() const {
        return AtomicGet(InitializationDetector) != 1;
    }

    class TInitializationGuard: TNonCopyable {
    private:
        TFetchingScriptOwner& Owner;

    public:
        TInitializationGuard(TFetchingScriptOwner& owner)
            : Owner(owner) {
            Owner.StartInitialization();
        }
        void InitializationFinished(std::shared_ptr<TFetchingScript>&& script) {
            Owner.FinishInitialization(std::move(script));
        }
        ~TInitializationGuard() {
            AFL_VERIFY(!Owner.NeedInitialization());
        }
    };

    std::optional<TInitializationGuard> StartInitialization() {
        if (AtomicCas(&InitializationDetector, 2, 0)) {
            return std::optional<TInitializationGuard>(*this);
        } else {
            return std::nullopt;
        }
    }
};

class TFetchingScriptBuilder {
private:
    std::shared_ptr<TColumnsSetIds> GuaranteeNotOptional;
    ISnapshotSchema::TPtr FullSchema;

    YDB_ACCESSOR(TString, BranchName, "UNDEFINED");
    std::vector<std::shared_ptr<IFetchingStep>> Steps;
    YDB_READONLY_DEF(TColumnsSetIds, AddedFetchingColumns);
    YDB_READONLY_DEF(TColumnsSetIds, AddedAssembleColumns);

    TFetchingScriptBuilder(const ISnapshotSchema::TPtr& schema, const std::shared_ptr<TColumnsSetIds>& guaranteeNotOptional)
        : GuaranteeNotOptional(guaranteeNotOptional)
        , FullSchema(schema) {
    }

private:
    void AddAllocation(const std::set<ui32>& entityIds, const NArrow::NSSA::IMemoryCalculationPolicy::EStage stage, const EMemType mType);

    template <class T, typename... Args>
    std::shared_ptr<T> InsertStep(const ui32 index, Args... args) {
        AFL_VERIFY(index <= Steps.size())("index", index)("size", Steps.size());
        auto result = std::make_shared<T>(args...);
        Steps.insert(Steps.begin() + index, result);
        return result;
    }

public:
    TFetchingScriptBuilder(const TSpecialReadContext& context);

    std::shared_ptr<TFetchingScript> Build()&& {
        return std::make_shared<TFetchingScript>(BranchName, std::move(Steps));
    }

    void AddStep(const std::shared_ptr<IFetchingStep>& step) {
        AFL_VERIFY(step);
        Steps.emplace_back(step);
    }

    void AddFetchingStep(const TColumnsSetIds& columns, const NArrow::NSSA::IMemoryCalculationPolicy::EStage stage);
    void AddAssembleStep(const TColumnsSetIds& columns, const TString& purposeId, const NArrow::NSSA::IMemoryCalculationPolicy::EStage stage,
        const bool sequential);

    static TFetchingScriptBuilder MakeForTests(ISnapshotSchema::TPtr schema, std::shared_ptr<TColumnsSetIds> guaranteeNotOptional = nullptr) {
        return TFetchingScriptBuilder(schema, guaranteeNotOptional ? guaranteeNotOptional : std::make_shared<TColumnsSetIds>());
    }
};

}   // namespace NKikimr::NOlap::NReader::NCommon

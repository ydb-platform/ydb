#pragma once
#include "columns_set.h"

#include <ydb/core/tx/columnshard/counters/common/owner.h>
#include <ydb/core/tx/columnshard/counters/scan.h>
#include <ydb/core/tx/columnshard/engines/reader/common/conveyor_task.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/monotonic.h>
#include <ydb/library/conclusion/result.h>

#include <util/datetime/base.h>

namespace NKikimr::NOlap::NReader::NCommon {

class IDataSource;
class TSpecialReadContext;
class TFetchingScriptCursor;

class TFetchingStepSignals: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr ExecutionDurationCounter;
    NMonitoring::TDynamicCounters::TCounterPtr TotalDurationCounter;
    NMonitoring::TDynamicCounters::TCounterPtr BytesCounter;
    NMonitoring::TDynamicCounters::TCounterPtr SkipGraphNode;
    NMonitoring::TDynamicCounters::TCounterPtr SkipGraphNodeRecords;
    NMonitoring::TDynamicCounters::TCounterPtr ExecuteGraphNode;
    NMonitoring::TDynamicCounters::TCounterPtr ExecuteGraphNodeRecords;

public:
    TFetchingStepSignals(NColumnShard::TCommonCountersOwner&& owner)
        : TBase(std::move(owner))
        , ExecutionDurationCounter(TBase::GetDeriviative("Duration/Execution/Us"))
        , TotalDurationCounter(TBase::GetDeriviative("Duration/Total/Us"))
        , BytesCounter(TBase::GetDeriviative("Bytes/Count"))
        , SkipGraphNode(TBase::GetDeriviative("Skips/Count"))
        , SkipGraphNodeRecords(TBase::GetDeriviative("Skips/Records/Count"))
        , ExecuteGraphNode(TBase::GetDeriviative("Executions/Count"))
        , ExecuteGraphNodeRecords(TBase::GetDeriviative("Executions/Records/Count")) {
    }

    void OnSkipGraphNode(const ui32 recordsCount) {
        SkipGraphNode->Add(1);
        SkipGraphNodeRecords->Add(recordsCount);
    }

    void OnExecuteGraphNode(const ui32 recordsCount) {
        ExecuteGraphNode->Add(1);
        ExecuteGraphNodeRecords->Add(recordsCount);
    }

    void AddExecutionDuration(const TDuration d) const {
        ExecutionDurationCounter->Add(d.MicroSeconds());
    }

    void AddTotalDuration(const TDuration d) const {
        TotalDurationCounter->Add(d.MicroSeconds());
    }

    void AddBytes(const ui32 v) const {
        BytesCounter->Add(v);
    }
};

class TFetchingStepsSignalsCollection: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    TMutex Mutex;
    THashMap<TString, TFetchingStepSignals> Collection;
    TFetchingStepSignals GetSignalsImpl(const TString& name) {
        TGuard<TMutex> g(Mutex);
        auto it = Collection.find(name);
        if (it == Collection.end()) {
            it = Collection.emplace(name, TFetchingStepSignals(CreateSubGroup("step_name", name))).first;
        }
        return it->second;
    }

public:
    TFetchingStepsSignalsCollection()
        : TBase("ScanSteps") {
    }

    static TFetchingStepSignals GetSignals(const TString& name) {
        return Singleton<TFetchingStepsSignalsCollection>()->GetSignalsImpl(name);
    }
};

class IFetchingStep: public TNonCopyable {
private:
    YDB_READONLY_DEF(TString, Name);
    TAtomicCounter SumDuration;
    TAtomicCounter SumSize;
    TFetchingStepSignals Signals;

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

    void AddExecutionDuration(const TDuration d) {
        SumDuration.Add(d.MicroSeconds());
        Signals.AddExecutionDuration(d);
    }
    void AddDataSize(const ui64 size) {
        SumSize.Add(size);
        Signals.AddBytes(size);
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

public:
    TFetchingScript(const TString& branchName, std::vector<std::shared_ptr<IFetchingStep>>&& steps)
        : BranchName(branchName)
        , Steps(std::move(steps)) {
    }

    void AddStepDataSize(const ui32 index, const ui64 size) {
        GetStep(index)->AddDataSize(size);
    }

    void AddStepDuration(const ui32 index, const TDuration d) {
        AtomicSet(FinishInstant, TMonotonic::Now().MicroSeconds());
        GetStep(index)->AddExecutionDuration(d);
    }

    void OnExecute() {
        AtomicCas(&StartInstant, TMonotonic::Now().MicroSeconds(), 0);
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
    void AddAllocation(const std::set<ui32>& entityIds, const EStageFeaturesIndexes stage, const EMemType mType);

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

    void AddFetchingStep(const TColumnsSetIds& columns, const EStageFeaturesIndexes stage);
    void AddAssembleStep(const TColumnsSetIds& columns, const TString& purposeId, const EStageFeaturesIndexes stage, const bool sequential);

    static TFetchingScriptBuilder MakeForTests(ISnapshotSchema::TPtr schema, std::shared_ptr<TColumnsSetIds> guaranteeNotOptional = nullptr) {
        return TFetchingScriptBuilder(schema, guaranteeNotOptional ? guaranteeNotOptional : std::make_shared<TColumnsSetIds>());
    }
};

class TFetchingScriptCursor {
private:
    ui32 CurrentStepIdx = 0;
    std::shared_ptr<TFetchingScript> Script;
    void FlushDuration(const TDuration d) {
        Script->AddStepDuration(CurrentStepIdx, d);
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

    template <class T>
    TStepAction(const std::shared_ptr<T>& source, TFetchingScriptCursor&& cursor, const NActors::TActorId& ownerActorId)
        : TStepAction(std::static_pointer_cast<IDataSource>(source), std::move(cursor), ownerActorId) {
    }
    TStepAction(const std::shared_ptr<IDataSource>& source, TFetchingScriptCursor&& cursor, const NActors::TActorId& ownerActorId);
};

class TProgramStep: public IFetchingStep {
private:
    using TBase = IFetchingStep;
    const std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TCompiledGraph> Program;
    THashMap<ui32, std::shared_ptr<TFetchingStepSignals>> Signals;
    const std::shared_ptr<TFetchingStepSignals>& GetSignals(const ui32 nodeId) const;

public:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;

    TProgramStep(const std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TCompiledGraph>& program)
        : TBase("PROGRAM_EXECUTION")
        , Program(program) {
        for (auto&& i : Program->GetNodes()) {
            Signals.emplace(
                i.first, std::make_shared<TFetchingStepSignals>(TFetchingStepsSignalsCollection::GetSignals(i.second->GetSignalCategoryName())));
        }
    }
};

}   // namespace NKikimr::NOlap::NReader::NCommon

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
    NMonitoring::TDynamicCounters::TCounterPtr DurationCounter;
    NMonitoring::TDynamicCounters::TCounterPtr BytesCounter;

public:
    TFetchingStepSignals(NColumnShard::TCommonCountersOwner&& owner)
        : TBase(std::move(owner))
        , DurationCounter(TBase::GetDeriviative("duration_ms"))
        , BytesCounter(TBase::GetDeriviative("bytes_ms")) {
    }

    void AddDuration(const TDuration d) const {
        DurationCounter->Add(d.MilliSeconds());
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
        : TBase("scan_steps") {
    }

    static TFetchingStepSignals GetSignals(const TString& name) {
        return Singleton<TFetchingStepsSignalsCollection>()->GetSignalsImpl(name);
    }
};

class IFetchingStep: public TNonCopyable {
private:
    YDB_READONLY_DEF(TString, Name);
    YDB_READONLY(TDuration, SumDuration, TDuration::Zero());
    YDB_READONLY(ui64, SumSize, 0);
    TFetchingStepSignals Signals;

protected:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const = 0;
    virtual TString DoDebugString() const {
        return "";
    }

public:
    void AddDuration(const TDuration d) {
        SumDuration += d;
        Signals.AddDuration(d);
    }
    void AddDataSize(const ui64 size) {
        SumSize += size;
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

    TString DebugString() const;
};

class TFetchingScript {
private:
    YDB_ACCESSOR(TString, BranchName, "UNDEFINED");
    std::vector<std::shared_ptr<IFetchingStep>> Steps;
    std::optional<TMonotonic> StartInstant;
    std::optional<TMonotonic> FinishInstant;

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

class TColumnsAccumulator {
private:
    TColumnsSetIds FetchingReadyColumns;
    TColumnsSetIds AssemblerReadyColumns;
    ISnapshotSchema::TPtr FullSchema;
    std::shared_ptr<TColumnsSetIds> GuaranteeNotOptional;

public:
    TColumnsAccumulator(const std::shared_ptr<TColumnsSetIds>& guaranteeNotOptional, const ISnapshotSchema::TPtr& fullSchema)
        : FullSchema(fullSchema)
        , GuaranteeNotOptional(guaranteeNotOptional) {
    }

    TColumnsSetIds GetNotFetchedAlready(const TColumnsSetIds& columns) const {
        return columns - FetchingReadyColumns;
    }

    bool AddFetchingStep(TFetchingScript& script, const TColumnsSetIds& columns, const EStageFeaturesIndexes stage);
    bool AddAssembleStep(TFetchingScript& script, const TColumnsSetIds& columns, const TString& purposeId, const EStageFeaturesIndexes stage,
        const bool sequential);
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

    template <class T>
    TStepAction(const std::shared_ptr<T>& source, TFetchingScriptCursor&& cursor, const NActors::TActorId& ownerActorId)
        : TStepAction(std::static_pointer_cast<IDataSource>(source), std::move(cursor), ownerActorId) {
    }
    TStepAction(const std::shared_ptr<IDataSource>& source, TFetchingScriptCursor&& cursor, const NActors::TActorId& ownerActorId);
};

}   // namespace NKikimr::NOlap::NReader::NCommon

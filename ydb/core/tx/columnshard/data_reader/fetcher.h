#pragma once
#include "contexts.h"
#include "fetching_executor.h"

#include <ydb/core/tx/columnshard/blobs_reader/task.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/columns_set.h>
#include <ydb/core/tx/conveyor_composite/usage/common.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/signals/states.h>

namespace NKikimr::NOlap::NDataFetcher {

class TClassCounters {
private:
    std::shared_ptr<NCounters::TStateSignalsOperator<EFetchingStage>> StateSignals;

public:
    TClassCounters(NColumnShard::TCommonCountersOwner& owner) {
        StateSignals = std::make_shared<NCounters::TStateSignalsOperator<EFetchingStage>>(owner, "fetching_stage");
    }

    NCounters::TStateSignalsOperator<EFetchingStage>::TGuard GetGuard(const std::optional<EFetchingStage> start) {
        return StateSignals->BuildGuard(start);
    }
};

class TCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    TMutex Mutex;
    THashMap<TString, std::shared_ptr<TClassCounters>> ClassCounters;

public:
    TCounters()
        : TBase("data_fetcher") {
    }

    std::shared_ptr<TClassCounters> GetClassCounters(const TString& className) {
        TGuard<TMutex> g(Mutex);
        auto it = ClassCounters.find(className);
        if (it == ClassCounters.end()) {
            it = ClassCounters.emplace(className, std::make_shared<TClassCounters>(*this)).first;
        }
        return it->second;
    }
};

class TPortionsDataFetcher: TNonCopyable {
private:
    const TRequestInput Input;
    const std::shared_ptr<IFetchCallback> Callback;
    std::shared_ptr<TClassCounters> ClassCounters;
    NCounters::TStateSignalsOperator<EFetchingStage>::TGuard Guard;
    TScriptExecution Script;
    TCurrentContext CurrentContext;
    std::shared_ptr<TEnvironment> Environment;
    const NConveyorComposite::ESpecialTaskCategory ConveyorCategory;
    bool IsFinishedFlag = false;

public:
    void AskMemoryAllocation(const std::shared_ptr<NGroupedMemoryManager::IAllocation>& task) {
        NGroupedMemoryManager::TCompMemoryLimiterOperator::SendToAllocation(
            CurrentContext.GetMemoryProcessId(), CurrentContext.GetMemoryScopeId(), CurrentContext.GetMemoryGroupId(), { task }, 0);
    }

    ui64 GetNecessaryDataMemory(
        const std::shared_ptr<NReader::NCommon::TColumnsSetIds>& columnIds, const std::vector<TPortionDataAccessor>& acc) const {
        return Callback->GetNecessaryDataMemory(columnIds, acc);
    }

    TPortionsDataFetcher(TRequestInput&& input, std::shared_ptr<IFetchCallback>&& callback, const std::shared_ptr<TEnvironment>& environment,
        const std::shared_ptr<TScript>& script, const NConveyorComposite::ESpecialTaskCategory conveyorCategory)
        : Input(std::move(input))
        , Callback(std::move(callback))
        , ClassCounters(Singleton<TCounters>()->GetClassCounters(Callback->GetClassName()))
        , Guard(ClassCounters->GetGuard(EFetchingStage::Created))
        , Script(script)
        , Environment(environment)
        , ConveyorCategory(conveyorCategory) {
        AFL_VERIFY(Environment);
        AFL_VERIFY(Callback);
    }

    ~TPortionsDataFetcher() {
        AFL_VERIFY(NActors::TActorSystem::IsStopped() || IsFinishedFlag || Guard.GetStage() == EFetchingStage::Created
            || Callback->IsAborted())("stage", Guard.GetStage())("class_name", Callback->GetClassName());
    }

    static void StartAccessorPortionsFetching(TRequestInput&& input, std::shared_ptr<IFetchCallback>&& callback,
        const std::shared_ptr<TEnvironment>& environment, const NConveyorComposite::ESpecialTaskCategory conveyorCategory);

    static void StartFullPortionsFetching(TRequestInput&& input, std::shared_ptr<IFetchCallback>&& callback,
        const std::shared_ptr<TEnvironment>& environment, const NConveyorComposite::ESpecialTaskCategory conveyorCategory);

    static void StartColumnsFetching(TRequestInput&& input, std::shared_ptr<NReader::NCommon::TColumnsSetIds>& entityIds,
        std::shared_ptr<IFetchCallback>&& callback, const std::shared_ptr<TEnvironment>& environment,
        const NConveyorComposite::ESpecialTaskCategory conveyorCategory);

    TScriptExecution& MutableScript() {
        return Script;
    }

    const TRequestInput& GetInput() const {
        return Input;
    }

    const TEnvironment& GetEnvironment() const {
        return *Environment;
    }

    TCurrentContext& MutableCurrentContext() {
        return CurrentContext;
    }

    const TCurrentContext& GetCurrentContext() const {
        return CurrentContext;
    }

    void SetStage(const EFetchingStage stage) {
        Callback->OnStageStarting(stage);
        Guard.SetState(stage);
    }

    void OnError(const TString& errMessage) {
        NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("event", "on_error")("consumer", Input.GetConsumer())(
            "task_id", Input.GetExternalTaskId())("script", Script.GetScriptClassName());
        AFL_VERIFY(!IsFinishedFlag);
        IsFinishedFlag = true;
        SetStage(EFetchingStage::Error);
        Callback->OnError(errMessage);
    }

    void OnFinished() {
        NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("event", "on_finished")("consumer", Input.GetConsumer())(
            "task_id", Input.GetExternalTaskId())("script", Script.GetScriptClassName());
        AFL_VERIFY(!IsFinishedFlag);
        IsFinishedFlag = true;
        SetStage(EFetchingStage::Finished);
        Callback->OnFinished(std::move(CurrentContext));
    }

    bool IsAborted() const {
        return Callback->IsAborted();
    }

    bool Resume(std::shared_ptr<TPortionsDataFetcher>& selfPtr) {
        if (IsFinishedFlag) {
            return false;
        }
        NConveyorComposite::TServiceOperator::SendTaskToExecute(std::make_shared<TFetchingExecutor>(selfPtr), ConveyorCategory, 0);
        return true;
    }
};

}   // namespace NKikimr::NOlap::NDataFetcher

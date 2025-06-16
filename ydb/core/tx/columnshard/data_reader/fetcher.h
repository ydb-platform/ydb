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

namespace NKikimr::NOlap::NDataFetcher {

class TPortionsDataFetcher: TNonCopyable {
private:
    const TRequestInput Input;
    const std::shared_ptr<IFetchCallback> Callback;
    TScriptExecution Script;
    TCurrentContext CurrentContext;
    std::shared_ptr<TEnvironment> Environment;
    const NConveyorComposite::ESpecialTaskCategory ConveyorCategory;
    bool IsFinishedFlag = false;
    EFetchingStage Stage = EFetchingStage::Created;

public:
    void AskMemoryAllocation(const std::shared_ptr<NGroupedMemoryManager::IAllocation>& task) {
        NGroupedMemoryManager::TCompMemoryLimiterOperator::SendToAllocation(
            CurrentContext.GetMemoryProcessId(), CurrentContext.GetMemoryScopeId(), CurrentContext.GetMemoryGroupId(), { task }, 0);
    }

    TPortionsDataFetcher(TRequestInput&& input, std::shared_ptr<IFetchCallback>&& callback, const std::shared_ptr<TEnvironment>& environment,
        const std::shared_ptr<TScript>& script, const NConveyorComposite::ESpecialTaskCategory conveyorCategory)
        : Input(std::move(input))
        , Callback(std::move(callback))
        , Script(script)
        , Environment(environment)
        , ConveyorCategory(conveyorCategory) {
        AFL_VERIFY(Environment);
        AFL_VERIFY(Callback);
    }

    bool IsFinished() {
        return IsFinishedFlag;
    }

    ~TPortionsDataFetcher() {
        AFL_VERIFY(IsFinishedFlag || Stage == EFetchingStage::Created || Callback->IsAborted())("stage", Stage);
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
        Stage = stage;
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

    bool Resume(std::shared_ptr<TPortionsDataFetcher>& selfPtr) {
        if (IsFinishedFlag) {
            return false;
        }
        if (Callback->IsAborted()) {
            OnError("aborted");
            return false;
        }
        NConveyorComposite::TServiceOperator::SendTaskToExecute(std::make_shared<TFetchingExecutor>(selfPtr), ConveyorCategory, 0);
        return true;
    }
};

}   // namespace NKikimr::NOlap::NDataFetcher

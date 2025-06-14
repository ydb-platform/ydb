#pragma once
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap::NDataFetcher {

class TCurrentContext {
private:
    std::optional<std::vector<TPortionDataAccessor>> Accessors;
    std::vector<std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>> ResourceGuards;
    std::shared_ptr<NGroupedMemoryManager::TProcessGuard> MemoryProcessGuard;
    std::shared_ptr<NGroupedMemoryManager::TScopeGuard> MemoryProcessScopeGuard;
    std::shared_ptr<NGroupedMemoryManager::TGroupGuard> MemoryProcessGroupGuard;
    static inline TAtomicCounter Counter = 0;
    const ui64 MemoryProcessId = Counter.Inc();
    std::optional<TCompositeReadBlobs> Blobs;

public:
    ui64 GetMemoryProcessId() const {
        return MemoryProcessId;
    }

    void SetBlobs(TCompositeReadBlobs&& blobs) {
        AFL_VERIFY(!Blobs);
        Blobs = std::move(blobs);
    }

    TCompositeReadBlobs& MutableBlobs() {
        AFL_VERIFY(!!Blobs);
        return *Blobs;
    }

    void ResetBlobs() {
        Blobs.reset();
    }

    TCurrentContext() {
        MemoryProcessGuard = TCompMemoryLimiterOperator::BuildProcessGuard(
            MemoryProcessId, { std::make_shared<TStageFeatures>("DEFAULT", 1000000000, 5000000000, nullptr, nullptr) });
        MemoryProcessScopeGuard = TCompMemoryLimiterOperator::BuildScopeGuard(processId, 1);
        MemoryProcessGroupGuard = TCompMemoryLimiterOperator::BuildGroupGuard(processId, 1);
    }

    void SetPortionAccessors(std::vector<TPortionDataAccessor>&& acc) {
        AFL_VERIFY(!Accessors);
        Accessors = std::move(acc);
    }

    const std::vector<TPortionDataAccessor> GetPortionAccessors() const {
        AFL_VERIFY(Accessors);
        return *Accessors;
    }

    void RegisterResourcesGuard(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& g) {
        ResourceGuards.emplace_back(g);
    }
};

class IFetchCallback {
private:
    virtual void DoOnFinished(TCurrentContext&& context) = 0;
    virtual void DoOnError(const TString& errorMessage) = 0;
    bool IsFinished = false;

public:
    void OnFinished(TCurrentContext&& context) {
        AFL_VERIFY(!IsFinished);
        IsFinished = true;
        return DoOnFinished(std::move(context));
    }

    void OnError(const TString& errorMessage) {
        AFL_VERIFY(!IsFinished);
        IsFinished = true;
        return DoOnError(errorMessage);
    }
};

enum class EFetchingStage : ui32 {
    Created = 0,
    AskAccessorResources,
    AskDataResources,
    AskAccessors,
    ReadBlobs,
    Finished,
    Error
};

class IFetchingStep {
public:
    enum class EStepResult {
        Continue,
        Detached,
        Error
    };

private:
    virtual EStepResult DoExecute(const std::shared_ptr<TPortionsDataFetcher>& fetchingContext) const = 0;

public:
    [[nodiscard]] EStepResult Execute(const std::shared_ptr<TPortionsDataFetcher>& fetchingContext) const {
        return DoExecute(fetchingContext);
    }
};

class TFetchingExecutor: public NConveyorComposite::ITask {
private:
    std::shared_ptr<TPortionsDataFetcher> Fetcher;
    virtual void DoExecute(const std::shared_ptr<ITask>& taskPtr) override {
        NActors::TLogContextGuard lGuard =
            NActors::TLogContextBuilder::Build()("event", "on_execution")("consumer", Fetcher->GetInput().GetConsumer())(
                "task_id", Fetcher->GetInput().GetExternalTaskId())("script", Fetcher->MutableScript().GetScriptClassName());
        while (!Fetcher->MutableScript().IsFinished()) {
            switch (Fetcher->MutableScript().GetCurrentStep()->Execute(Fetcher)) {
                case EStepResult::Continue:
                    Fetcher->MutableScript().Next();
                    break;
                case EStepResult::Detached:
                    return;
                case EStepResult::Error:
                    return;
            }
        }
        Fetcher->OnFinished();
    }

public:
    virtual TString GetTaskClassIdentifier() const override {
        return Fetcher->MutableScript().GetClassName();
    }
};

class TAskAccessorResourcesStep: public IFetchingStep {
private:
    class TSubscriber: public NGroupedMemoryManager::IAllocation {
    private:
        using TBase = NOlap::NResourceBroker::NSubscribe::ITask;
        std::shared_ptr<TPortionsDataFetcher> FetchingContext;

        virtual void DoOnAllocationImpossible(const TString& errorMessage) override {
            FetchingContext->OnError(errorMessage);
        }
        virtual bool DoOnAllocated(
            std::shared_ptr<TAllocationGuard>&& guard, const std::shared_ptr<NGroupedMemoryManager::IAllocation>& /*allocation*/) override {
            FetchingContext->MutableCurrentContext().RegisterResourcesGuard(std::move(guard));
            FetchingContext->Resume();
        }

    public:
        TSubscriber(const ui64 memory, const std::shared_ptr<TPortionsDataFetcher>& fetchingContext)
            : TBase(memory)
            , FetchingContext(fetchingContext) {
        }
    };

    virtual IFetchingStep::EStepResult DoExecute(const std::shared_ptr<TPortionsDataFetcher>& fetchingContext) const override {
        fetchingContext->SetStage(EFetchingStage::AskAccessorResources);
        auto request = std::make_shared<TDataAccessorsRequest>(::ToString(fetchingContext->GetInput()->GetConsumer()));
        for (auto&& i : fetchingContext->GetInput()->GetPortions()) {
            request->AddPortion(i);
        }
        const ui64 memory = request->PredictAccessorsMemory(fetchingContext->GetInput()->GetActualSchema());
        if (!memory) {
            return IFetchingStep::EStepResult::Continue;
        }
        TCompMemoryLimiterOperator::SendToAllocation(
            FetchingContext->GetCurrentContext().GetMemoryProcessId(), 1, 1, { std::make_shared<TSubscriber>(memory, fetchingContext) }, 0);
        return IFetchingStep::EStepResult::Detached;
    }

public:
};

class TAskAccessorsStep: public IFetchingStep {
private:
    class TSubscriber: public IDataAccessorRequestsSubscriber {
    private:
        std::shared_ptr<TPortionsDataFetcher> Fetcher;

    protected:
        virtual void DoOnRequestsFinished(TDataAccessorsResult&& result) override {
            if (result.HasErrors()) {
                Fetcher->OnError("cannot fetch accessors");
            } else {
                auto accessors = result.ExtractPortionsVector();
                AFL_VERIFY(accessors.size() == Fetcher->GetInput().GetPortions().size());
                for (ui32 idx = 0; idx < accessors.size(); ++idx) {
                    AFL_VERIFY(accessors[idx].GetPortionInfo()->GetPortionId() == Fetcher->GetInput().GetPortions()[idx].GetPortionInfo()->GetPortionId());
                }
                Fetcher->MutableCurrentContext().SetPortionAccessors(std::move(accessors));
                Fetcher->Resume();
            }
        }
        virtual const std::shared_ptr<const TAtomicCounter>& DoGetAbortionFlag() const override {
            return Default<std::shared_ptr<const TAtomicCounter>>();
        }

    public:
    };

    virtual IFetchingStep::EStepResult DoExecute(const std::shared_ptr<TPortionsDataFetcher>& fetchingContext) const override {
        fetchingContext->SetStage(EFetchingStage::AskAccessors);
        std::shared_ptr<TDataAccessorsRequest> request = std::make_shared<TDataAccessorsRequest>(fetchingContext->GetConsumer());
        for (auto&& i : fetchingContext->GetPortions()) {
            request->AddPortion(i);
        }
        fetchingContext->GetEnvironment()->GetDataAccessorsManager()->AskData(request);
        return IFetchingStep::EStepResult::Detached;
    }

public:
};

class TAskDataResourceStep: public IFetchingStep {
private:
    std::shared_ptr<TColumnsSetIds> ColumnIds;

    class TSubscriber: public NGroupedMemoryManager::IAllocation {
    private:
        using TBase = NOlap::NResourceBroker::NSubscribe::ITask;
        std::shared_ptr<TPortionsDataFetcher> FetchingContext;

        virtual void DoOnAllocationImpossible(const TString& errorMessage) override {
            FetchingContext->OnError(errorMessage);
        }
        virtual bool DoOnAllocated(
            std::shared_ptr<TAllocationGuard>&& guard, const std::shared_ptr<NGroupedMemoryManager::IAllocation>& /*allocation*/) override {
            FetchingContext->MutableCurrentContext().RegisterResourcesGuard(std::move(guard));
            FetchingContext->Resume();
        }

    public:
        TSubscriber(const ui64 memory, const std::shared_ptr<TPortionsDataFetcher>& fetchingContext)
            : TBase(memory)
            , FetchingContext(fetchingContext) {
        }
    };

    virtual IFetchingStep::EStepResult DoExecute(const std::shared_ptr<TPortionsDataFetcher>& fetchingContext) const override {
        fetchingContext->SetStage(EFetchingStage::AskDataResources);
        const std::vector<TPortionDataAccessor>& accessors = fetchingContext->GetCurrentContext()->GetPortionAccessors();
        ui64 memory = 0;
        for (auto&& i : accessors) {
            if (ColumnIds) {
                memory += i.GetColumnBlobBytes(ColumnIds->GetColumnIds());
            } else {
                memory += i.GetColumnBlobBytes();
            }
        }
        if (!memory) {
            return IFetchingStep::EStepResult::Continue;
        }
        TCompMemoryLimiterOperator::SendToAllocation(
            FetchingContext->GetCurrentContext().GetMemoryProcessId(), 1, 1, { std::make_shared<TSubscriber>(memory, fetchingContext) }, 0);
        return IFetchingStep::EStepResult::Detached;
    }

public:
    TAskDataResourceStep(const std::shared_ptr<TColumnsSetIds>& columnIds)
        : ColumnIds(columnIds) {
    }
};

class TAskDataStep: public IFetchingStep {
private:
    std::shared_ptr<TColumnsSetIds> ColumnIds;

    class TSubscriber: public NOlap::NBlobOperations::NRead::ITask {
    private:
        using TBase = NOlap::NBlobOperations::NRead::ITask;
        std::shared_ptr<TPortionsDataFetcher> FetchingContext;

    protected:
        virtual void DoOnDataReady(const std::shared_ptr<NOlap::NResourceBroker::NSubscribe::TResourcesGuard>& /*resourcesGuard*/) override {
            FetchingContext->MutableCurrentContext().SetBlobs(ExtractBlobsData());
            FetchingContext->Resume();
        }
        virtual bool DoOnError(
            const TString& storageId, const NOlap::TBlobRange& range, const NOlap::IBlobsReadingAction::TErrorStatus& status) override {
            FetchingContext->OnError("cannot read blob range: " + storageId + "::" + range.ToString() + "::" + status.GetErrorMessage());
            return false;
        }

    public:
        TSubscriber(
            const std::shared_ptr<TPortionsDataFetcher>& fetchingContext, std::vector<std::shared_ptr<IBlobsReadingAction>>&& readActions)
            : TBase(readActions, fetchingContext->GetConsumer(), fetchingContext->GetExternalTaskId())
            , FetchingContext(fetchingContext) {
        }
    };

    virtual bool DoExecute(const std::shared_ptr<TPortionsDataFetcher>& fetchingContext) const override {
        fetchingContext->SetStage(EFetchingStage::ReadBlobs);
        const std::vector<TPortionDataAccessor>& accessors = fetchingContext->GetPortionAccessors();
        THashMap<TString, THashSet<TBlobRange>> ranges;
        ui32 idx = 0;
        for (ui32 idx = 0; idx < accessors.size(); ++idx) {
            if (ColumnIds) {
                i.FillBlobRangesByStorage(ranges, fetchingContext->GetInput().GetPortions()[idx]->GetSchema());
            } else {
                i.FillBlobRangesByStorage(
                    ranges, fetchingContext->GetInput().GetPortions()[idx]->GetSchema()->GetIndexInfo(), &ColumnIds->GetColumnIds());
            }
        }
        std::vector<std::shared_ptr<IBlobsReadingAction>> readActions;
        for (auto&& i : ranges) {
            auto blobsOperator = fetchingContext->GetEnvironment()->GetStoragesManager()->GetOperatorVerified(i.first);
            auto readBlobs = blobsOperator->StartReadingAction(fetchingContext->GetConsumer());
            for (auto&& br : i.second) {
                readBlobs->AddRange(br);
            }
            readActions.emplace_back(readBlobs);
        }
        if (readActions.size()) {
            TActorContext::AsActorContext().Register(
                new NOlap::NBlobOperations::NRead::TActor(std::make_shared<TSubscriber>(fetchingContext, std::move(readActions))));
            return true;
        } else {
            return false;
        }
    }

public:
    TAskDataResourceStep(const std::shared_ptr<TColumnsSetIds>& columnIds)
        : ColumnIds(columnIds) {
    }
};

class TEnvironment {
private:
    std::shared_ptr<NDataAccessorControl::IDataAccessorsManager> DataAccessorsManager;
    std::shared_ptr<IStoragesManager> StoragesManager;

public:
};

class TScript {
private:
    const TString ClassName;
    std::vector<std::shared_ptr<IFetchingStep>> Steps;

public:
    TScript(std::vector<std::shared_ptr<IFetchingStep>>&& steps, const TString& className)
        : ClassName(className)
        , Steps(std::move(steps)) {
    }

    const std::shared_ptr<IFetchingStep>& GetStep(const ui32 index) const {
        AFL_VERIFY(index < Steps.size());
        return Steps[index];
    }

    ui32 GetStepsCount() const {
        return Steps.size();
    }
};

class TScriptExecution {
private:
    std::shared_ptr<TScript> Script;
    ui32 StepIndex = 0;

public:
    TScriptExecution(const std::shared_ptr<TScript>& script)
        : Script(script) {
        AFL_VERIFY(Script);
    }

    const TString& GetScriptClassName() const {
        return Script->GetClassName();
    }

    const std::shared_ptr<IFetchingStep>& GetCurrentStep() const {
        return Script->GetStep(StepIndex);
    }

    bool IsFinished() const {
        return StepIndex == Script.GetStepsCount();
    }

    void Next() {
        AFL_VERIFY(!IsFinished());
        ++StepIndex;
    }
};

class TFullPortionInfo {
private:
    YDB_READONLY_DEF(std::shared_ptr<TPortionInfo>, PortionInfo);
    YDB_READONLY_DEF(ISnapshotSchema::TPtr, Schema);

public:
    TFullPortionInfo(const std::shared_ptr<TPortionInfo>& portionInfo, const ISnapshotSchema::TPtr& schema)
        : PortionInfo(portionInfo)
        , Schema(schema) {
    }
};

class TRequestInput {
private:
    YDB_READONLY_DEF(std::vector<std::shared_ptr<TFullPortionInfo>>, Portions);
    YDB_READONLY(NBlobOperations::EConsumer, Consumer, NBlobOperations::EConsumer::UNDEFINED);
    YDB_READONLY_DEF(TString, ExternalTaskId);

public:
    TRequestInput(const std::vector<std::shared_ptr<TPortionInfo>>& portions, const std::shared_ptr<TVersionedIndex>& versions,
        const NBlobOperations::EConsumer consumer, const TString& externalTaskId)
        : Consumer(consumer)
        , ExternalTaskId(externalTaskId) {
        for (auto&& i : portions) {
            Portions.emplace_back(TFullPortionInfo(i, versions->GetSchemaVerified(i->GetSchemaVersionVerified())));
        }
    }
};

class TPortionsDataFetcher: TNonCopyable {
private:
    const TRequestInput Input;
    const std::shared_ptr<IFetchCallback> Callback;
    const TScriptExecution Script;
    TCurrentContext CurrentContext;
    std::shared_ptr<TEnvironment> Environment;
    const NConveyorComposite::ESpecialTaskCategory ConveyorCategory;
    bool IsFinished = false;

public:
    TPortionsDataFetcher(TRequestInput&& input, std::shared_ptr<IFetchCallback>&& callback, const std::shared_ptr<TEnvironment>& environment,
        const std::shared_ptr<TScript>& script, const NConveyorComposite::ESpecialTaskCategory conveyorCategory)
        : Input(std::move(input))
        , Callback(std::move(callback))
        , Script(script)
        , Environment(environment) {
    }

    static void StartFullPortionsFetching(TRequestInput&& input, std::shared_ptr<IFetchCallback>&& callback,
        const std::shared_ptr<TEnvironment>& environment, const NConveyorComposite::ESpecialTaskCategory conveyorCategory) {
        static const std::shared_ptr<TScript> script = [&]() {
            std::vector<std::shared_ptr<IFetchingStep>> steps;
            steps.emplace_back(std::make_shared<TAskAccessorResourcesStep>());
            steps.emplace_back(std::make_shared<TAskAccessorsStep>());
            steps.emplace_back(std::make_shared<TAskDataResourceStep>());
            steps.emplace_back(std::make_shared<TAskDataStep>());
            return std::make_shared<TScript>(std::move(steps), "FULL_PORTIONS_FETCHING::" + ::ToString(input.GetConsumer()));
        }();
        auto fetcher = std::make_shared<TPortionsDataFetcher>(std::move(input), std::move(callback), environment, script, conveyorCategory);
        fetcher->Resume();
    }

    static void StartColumnsFetching(TRequestInput&& input, std::shared_ptr<TColumnsSetIds>& entityIds, std::shared_ptr<IFetchCallback>&& callback,
        const std::shared_ptr<TEnvironment>& environment, const NConveyorComposite::ESpecialTaskCategory conveyorCategory) {
        std::shared_ptr<TScript> script = []() {
            std::vector<std::shared_ptr<IFetchingStep>> steps;
            steps.emplace_back(std::make_shared<TAskAccessorResourcesStep>());
            steps.emplace_back(std::make_shared<TAskAccessorsStep>());
            steps.emplace_back(std::make_shared<TAskDataResourceStep>(entityIds));
            steps.emplace_back(std::make_shared<TAskDataStep>(entityIds));
            return std::make_shared<TScript>(std::move(steps), "PARTIAL_PORTIONS_FETCHING::" + ::ToString(input.GetConsumer()));
        }();
        auto fetcher = std::make_shared<TPortionsDataFetcher>(std::move(input), std::move(callback), environment, script, conveyorCategory);
        fetcher->Resume();
    }

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

    void OnError(const TString& errMessage) {
        NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("event", "on_error")("consumer", Input.GetConsumer())(
            "task_id", Input.GetExternalTaskId())("script", Script->GetScriptClassName());
        AFL_VERIFY(!IsFinished);
        IsFinished = true;
        SetStage(EFetchingStage::Error);
        Callback->OnError(errMessage);
    }

    void OnFinished() {
        NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("event", "on_finished")("consumer", Input.GetConsumer())(
            "task_id", Input.GetExternalTaskId())("script", Script->GetScriptClassName());
        AFL_VERIFY(!IsFinished);
        IsFinished = true;
        SetStage(EFetchingStage::Finished);
        Callback->OnFinished(std::move(CurrentContext));
    }

    void Resume(std::shared_ptr<TPortionsDataFetcher>& selfPtr) {
        NConveyorComposite::TServiceOperator::SendTaskToExecute(std::make_shared<TFetchingExecutor>(selfPtr), ConveyorCategory, 0);
    }
};

}   // namespace NKikimr::NOlap::NDataFetcher

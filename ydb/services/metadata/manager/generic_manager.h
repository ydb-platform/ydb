#pragma once
#include <ydb/services/metadata/manager/common.h>
#include <ydb/services/metadata/service.h>

namespace NKikimr::NMetadata::NModifications {

class TOperationsController: public IAlterController {
private:
    YDB_READONLY_DEF(NThreading::TPromise<IOperationsManager::TYqlConclusionStatus>, Promise);
public:
    TOperationsController(NThreading::TPromise<IOperationsManager::TYqlConclusionStatus>&& p)
        : Promise(std::move(p))
    {

    }

    virtual void OnAlteringProblem(const TString& errorMessage) override {
        Promise.SetValue(IOperationsManager::TYqlConclusionStatus::Fail(errorMessage));
    }
    virtual void OnAlteringFinished() override {
        Promise.SetValue(IOperationsManager::TYqlConclusionStatus::Success());
    }

};

template <class T>
class TGenericOperationsManager: public IObjectOperationsManager<T> {
private:
    using TBase = IObjectOperationsManager<T>;
    using IOperationsManager::TYqlConclusionStatus;
public:
    using TInternalModificationContext = typename TBase::TInternalModificationContext;
protected:
    virtual NThreading::TFuture<TYqlConclusionStatus> DoModify(
        const NYql::TObjectSettingsImpl& settings, const ui32 nodeId,
        IClassBehaviour::TPtr manager, TInternalModificationContext& context) const override
    {
        if (!manager) {
            return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail("modification object behaviour not initialized"));
        }
        if (!manager->GetOperationsManager()) {
            return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail("modification is unavailable for " + manager->GetTypeId()));
        }
        auto promise = NThreading::NewPromise<TYqlConclusionStatus>();
        auto result = promise.GetFuture();
        {
            TOperationParsingResult patch(TBase::BuildPatchFromSettings(settings, context));
            if (!patch.IsSuccess()) {
                return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail(patch.GetErrorMessage()));
            }
            auto controller = std::make_shared<TOperationsController>(std::move(promise));
            IAlterCommand::TPtr alterCommand;
            switch (context.GetActivityType()) {
                case IOperationsManager::EActivityType::Create:
                    alterCommand = std::make_shared<TCreateCommand<T>>(patch.GetResult(), manager, controller, context);
                    break;
                case IOperationsManager::EActivityType::Alter:
                    alterCommand = std::make_shared<TAlterCommand<T>>(patch.GetResult(), manager, controller, context);
                    break;
                case IOperationsManager::EActivityType::Drop:
                    alterCommand = std::make_shared<TDropCommand<T>>(patch.GetResult(), manager, controller, context);
                    break;
                case IOperationsManager::EActivityType::Undefined:
                    return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail("undefined action type"));
            }
            TActivationContext::Send(new IEventHandle(NProvider::MakeServiceId(nodeId), {}, new NProvider::TEvObjectsOperation(alterCommand)));
        }
        return result;
    }
public:
};

}

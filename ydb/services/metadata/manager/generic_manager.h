#pragma once
#include <ydb/services/metadata/manager/common.h>
#include <ydb/services/metadata/service.h>

namespace NKikimr::NMetadata::NModifications {

class TOperationsController: public IAlterController {
private:
    YDB_READONLY_DEF(NThreading::TPromise<TObjectOperatorResult>, Promise);
public:
    TOperationsController(NThreading::TPromise<TObjectOperatorResult>&& p)
        : Promise(std::move(p))
    {

    }

    virtual void OnAlteringProblem(const TString& errorMessage) override {
        Promise.SetValue(TObjectOperatorResult(false).SetErrorMessage(errorMessage));
    }
    virtual void OnAlteringFinished() override {
        Promise.SetValue(TObjectOperatorResult(true));
    }

};

template <class T>
class TGenericOperationsManager: public IObjectOperationsManager<T> {
private:
    using TBase = IObjectOperationsManager<T>;
public:
    using TInternalModificationContext = typename TBase::TInternalModificationContext;
protected:
    virtual NThreading::TFuture<TObjectOperatorResult> DoModify(
        const NYql::TObjectSettingsImpl& settings, const ui32 nodeId,
        IClassBehaviour::TPtr manager, TInternalModificationContext& context) const override
    {
        if (!manager) {
            TObjectOperatorResult result("modification object behaviour not initialized");
            return NThreading::MakeFuture<TObjectOperatorResult>(result);
        }
        if (!manager->GetOperationsManager()) {
            TObjectOperatorResult result("modification is unavailable for " + manager->GetTypeId());
            return NThreading::MakeFuture<TObjectOperatorResult>(result);
        }
        auto promise = NThreading::NewPromise<TObjectOperatorResult>();
        auto result = promise.GetFuture();
        {
            TOperationParsingResult patch(TBase::BuildPatchFromSettings(settings, context));
            if (!patch.IsSuccess()) {
                TObjectOperatorResult result(patch.GetErrorMessage());
                return NThreading::MakeFuture<TObjectOperatorResult>(result);
            }
            auto c = std::make_shared<TOperationsController>(std::move(promise));
            IAlterCommand::TPtr alterCommand;
            switch (context.GetActivityType()) {
                case IOperationsManager::EActivityType::Create:
                    alterCommand = std::make_shared<TCreateCommand<T>>(patch.GetRecord(), manager, c, context);
                    break;
                case IOperationsManager::EActivityType::Alter:
                    alterCommand = std::make_shared<TAlterCommand<T>>(patch.GetRecord(), manager, c, context);
                    break;
                case IOperationsManager::EActivityType::Drop:
                    alterCommand = std::make_shared<TDropCommand<T>>(patch.GetRecord(), manager, c, context);
                    break;
                case IOperationsManager::EActivityType::Undefined:
                    return NThreading::MakeFuture<TObjectOperatorResult>(TObjectOperatorResult("undefined action type"));
            }
            TActivationContext::Send(new IEventHandle(NProvider::MakeServiceId(nodeId), {}, new NProvider::TEvObjectsOperation(alterCommand)));
        }
        return result;
    }
public:
};

}

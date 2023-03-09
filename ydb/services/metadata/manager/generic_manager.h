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
    using TModificationContext = typename TBase::TModificationContext;
private:
    template <class TCommand, class TSettings>
    NThreading::TFuture<TObjectOperatorResult> DoModifyObject(
        const TSettings& settings, const ui32 nodeId,
        IClassBehaviour::TPtr manager, const TModificationContext& context) const
    {
        if (!manager) {
            TObjectOperatorResult result("modification object behaviour not initialized");
            return NThreading::MakeFuture<TObjectOperatorResult>(result);
        }
        if (!manager->GetOperationsManager()) {
            TObjectOperatorResult result("modification is unavailable for " + manager->GetTypeId());
            return NThreading::MakeFuture<TObjectOperatorResult>(result);
        }
        TOperationParsingResult patch(TBase::BuildPatchFromSettings(settings, context));
        if (!patch.IsSuccess()) {
            TObjectOperatorResult result(patch.GetErrorMessage());
            return NThreading::MakeFuture<TObjectOperatorResult>(result);
        }
        auto promise = NThreading::NewPromise<TObjectOperatorResult>();
        auto result = promise.GetFuture();
        auto c = std::make_shared<TOperationsController>(std::move(promise));
        auto command = std::make_shared<TCommand>(patch.GetRecord(), manager, c, context);
        TActivationContext::Send(new IEventHandleFat(NProvider::MakeServiceId(nodeId), {},
            new NProvider::TEvObjectsOperation(command)));
        return result;
    }
protected:
    virtual NThreading::TFuture<TObjectOperatorResult> DoCreateObject(
        const NYql::TCreateObjectSettings& settings, const ui32 nodeId,
        IClassBehaviour::TPtr manager, const TModificationContext& context) const override
    {
        return DoModifyObject<TCreateCommand<T>>(settings, nodeId, manager, context);
    }
    virtual NThreading::TFuture<TObjectOperatorResult> DoAlterObject(
        const NYql::TAlterObjectSettings& settings, const ui32 nodeId,
        IClassBehaviour::TPtr manager, const TModificationContext& context) const override
    {
        return DoModifyObject<TAlterCommand<T>>(settings, nodeId, manager, context);
    }
    virtual NThreading::TFuture<TObjectOperatorResult> DoDropObject(
        const NYql::TDropObjectSettings& settings, const ui32 nodeId,
        IClassBehaviour::TPtr manager, const TModificationContext& context) const override
    {
        return DoModifyObject<TDropCommand<T>>(settings, nodeId, manager, context);
    }
public:
};

}

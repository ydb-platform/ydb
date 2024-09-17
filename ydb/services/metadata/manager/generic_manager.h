#pragma once
#include <ydb/services/metadata/manager/alter.h>
#include <ydb/services/metadata/manager/common.h>
#include <ydb/services/metadata/service.h>

namespace NKikimr::NMetadata::NModifications {

class TOperationsController: public IAlterController {
private:
    YDB_READONLY_DEF(NThreading::TPromise<IOperationsManager::TYqlConclusionStatus>, Promise);
public:
    TOperationsController(NThreading::TPromise<IOperationsManager::TYqlConclusionStatus> p)
        : Promise(std::move(p))
    {

    }

    virtual void OnAlteringProblem(const TString& errorMessage) override {
        Promise.SetValue(IOperationsManager::TYqlConclusionStatus::Fail(errorMessage));
    }
    virtual void OnAlteringFinished(TInstant /*historyInstant*/) override {
        Promise.SetValue(IOperationsManager::TYqlConclusionStatus::Success());
    }

};

template <class T>
class TGenericOperationsManager: public IObjectOperationsManager<T> {
private:
    using TSelf = TGenericOperationsManager<T>;
    using TBase = IObjectOperationsManager<T>;
    using IOperationsManager::TYqlConclusionStatus;

    class TPatchBuilder : public TPatchBuilderBase {
    private:
        const TSelf& Owner;
        IOperationsManager::TInternalModificationContext& Context;

    public:
        TPatchBuilder(const TSelf& owner, IOperationsManager::TInternalModificationContext& context) : Owner(owner), Context(context) {}

    protected:
        TOperationParsingResult DoBuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings) const override {
            return Owner.DoBuildPatchFromSettings(settings, Context);
        }
    };

public:
    using TInternalModificationContext = typename TBase::TInternalModificationContext;
    using TExternalModificationContext = typename TBase::TExternalModificationContext;
    using EActivityType = typename IOperationsManager::EActivityType;

protected:
    virtual TOperationParsingResult DoBuildPatchFromSettings(
        const NYql::TObjectSettingsImpl& settings, TInternalModificationContext& context) const = 0;

protected:
    virtual NThreading::TFuture<TYqlConclusionStatus> DoModify(
        const NYql::TObjectSettingsImpl& settings, const ui32 nodeId,
        const IClassBehaviour::TPtr& manager, TInternalModificationContext& context) const override
    {
        if (!manager) {
            return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail("modification object behaviour not initialized"));
        }
        if (!manager->GetOperationsManager()) {
            return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail("modification is unavailable for " + manager->GetTypeId()));
        }
        auto promise = NThreading::NewPromise<TYqlConclusionStatus>();
        {
            const TPatchBuilder patchBuilder(*this, context);
            TOperationParsingResult patch = patchBuilder.BuildPatchFromSettings(settings);
            if (!patch.IsSuccess()) {
                return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail(patch.GetErrorMessage()));
            }
            auto controller = std::make_shared<TOperationsController>(promise);
            IObjectModificationCommand::TPtr modifyObjectCommand;
            switch (context.GetActivityType()) {
                case EActivityType::Upsert:
                    modifyObjectCommand = std::make_shared<TUpsertObjectCommand<T>>(patch.GetResult(), manager, std::move(controller), context);
                    break;
                case EActivityType::Create:
                    modifyObjectCommand = std::make_shared<TCreateObjectCommand<T>>(patch.GetResult(), manager, std::move(controller), context, settings.GetExistingOk());
                    break;
                case EActivityType::Alter:
                    modifyObjectCommand = std::make_shared<TUpdateObjectCommand<T>>(patch.GetResult(), manager, std::move(controller), context);
                    break;
                case EActivityType::Drop:
                    modifyObjectCommand = std::make_shared<TDeleteObjectCommand<T>>(patch.GetResult(), manager, std::move(controller), context, settings.GetMissingOk());
                    break;
                case EActivityType::Undefined:
                    return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail("undefined action type"));
            }
            TActivationContext::Send(new IEventHandle(NProvider::MakeServiceId(nodeId), {}, new NProvider::TEvObjectsOperation(modifyObjectCommand)));
        }
        return promise;
    }

    virtual TYqlConclusionStatus DoPrepare(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TObjectSettingsImpl& settings,
        const IClassBehaviour::TPtr& manager, TInternalModificationContext& context) const override {
    if (!manager) {
            return TYqlConclusionStatus::Fail("modification object behaviour not initialized");
        }
        if (!manager->GetOperationsManager()) {
            return TYqlConclusionStatus::Fail("modification is unavailable for " + manager->GetTypeId());
        }

        const TPatchBuilder patchBuilder(*this, context);
        TOperationParsingResult patch = patchBuilder.BuildPatchFromSettings(settings);
        if (!patch.IsSuccess()) {
            return TYqlConclusionStatus::Fail(patch.GetErrorMessage());
        }

        NKqpProto::TKqpPhyMetadataOperation* operationProto = nullptr;
        switch (context.GetActivityType()) {
            case EActivityType::Upsert:
                operationProto = schemeOperation.MutableUpsertObject();
                break;
            case EActivityType::Create:
                operationProto = schemeOperation.MutableCreateObject();
                operationProto->SetSuccessOnAlreadyExists(settings.GetExistingOk());
                break;
            case EActivityType::Alter:
                operationProto = schemeOperation.MutableAlterObject();
                break;
            case EActivityType::Drop:
                operationProto = schemeOperation.MutableDropObject();
                operationProto->SetSuccessOnNotExist(settings.GetMissingOk());
                break;
            case EActivityType::Undefined:
                return TYqlConclusionStatus::Fail("undefined action type");
        }
        Y_ENSURE(operationProto);

        for (const auto& [col, value] : patch.GetResult().GetValues()) {
            auto* proto = operationProto->AddColumnValues();
            proto->SetColumn(col);
            proto->MutableValue()->CopyFrom(value);
        }

        return TYqlConclusionStatus::Success();
    }

    virtual NThreading::TFuture<TYqlConclusionStatus> ExecutePrepared(const NKqpProto::TKqpSchemeOperation& schemeOperation,
        const ui32 nodeId, const IClassBehaviour::TPtr& manager, const TExternalModificationContext& context) const override {
        if (!manager) {
            return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail("modification object behaviour not initialized"));
        }
        if (!manager->GetOperationsManager()) {
            return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail("modification is unavailable for " + manager->GetTypeId()));
        }

        auto promise = NThreading::NewPromise<TYqlConclusionStatus>();
        auto controller = std::make_shared<TOperationsController>(promise);
        IObjectModificationCommand::TPtr modifyObjectCommand;
        TInternalModificationContext internalContext(context);
        switch (schemeOperation.GetOperationCase()) {
            case NKqpProto::TKqpSchemeOperation::kUpsertObject:
                internalContext.SetActivityType(EActivityType::Upsert);
                modifyObjectCommand = std::make_shared<TUpsertObjectCommand<T>>(BuildPatchFromProto(schemeOperation.GetUpsertObject()), manager, std::move(controller), internalContext);
                break;
            case NKqpProto::TKqpSchemeOperation::kCreateObject:
                internalContext.SetActivityType(EActivityType::Create);
                modifyObjectCommand = std::make_shared<TCreateObjectCommand<T>>(BuildPatchFromProto(schemeOperation.GetCreateObject()), manager, std::move(controller), internalContext, schemeOperation.GetCreateObject().GetSuccessOnAlreadyExists());
                break;
            case NKqpProto::TKqpSchemeOperation::kAlterObject:
                internalContext.SetActivityType(EActivityType::Alter);
                modifyObjectCommand = std::make_shared<TUpdateObjectCommand<T>>(BuildPatchFromProto(schemeOperation.GetAlterObject()), manager, std::move(controller), internalContext);
                break;
            case NKqpProto::TKqpSchemeOperation::kDropObject:
                internalContext.SetActivityType(EActivityType::Drop);
                modifyObjectCommand = std::make_shared<TDeleteObjectCommand<T>>(BuildPatchFromProto(schemeOperation.GetDropObject()), manager, std::move(controller), internalContext, schemeOperation.GetDropObject().GetSuccessOnNotExist());
                break;
            default:
                return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail("undefined operation type"));
        }
        TActivationContext::Send(new IEventHandle(NProvider::MakeServiceId(nodeId), {}, new NProvider::TEvObjectsOperation(modifyObjectCommand)));

        return promise;
    }
private:
    static NInternal::TTableRecord BuildPatchFromProto(const NKqpProto::TKqpPhyMetadataOperation& op) {
        NInternal::TTableRecord result;
        for (const auto& val : op.GetColumnValues()) {
            result.SetColumn(val.GetColumn(), val.GetValue());
        }
        return result;
    }
};

}

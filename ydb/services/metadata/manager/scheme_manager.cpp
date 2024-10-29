#include "scheme_manager.h"

#include <ydb/core/kqp/gateway/actors/scheme.h>

namespace NKikimr::NMetadata::NModifications {

namespace {

class TOperationControllerBase: public IBuildRequestController {
private:
    using TYqlConclusionStatus = IOperationsManager::TYqlConclusionStatus;

    NThreading::TPromise<TYqlConclusionStatus> Promise;
    TActorSystem* ActorSystem;

protected:
    const NYql::TObjectSettingsImpl OriginalRequest;
    const TString StorageDirectory;
    const IOperationsManager::TInternalModificationContext Context;

protected:
    static NKikimrSchemeOp::TModifyACL MakeModifyACL(const TString& objectId, const std::optional<NACLib::TUserToken>& owner) {
        NACLib::TDiffACL diffAcl;
        for (const TString& usedSid : AppData()->AdministrationAllowedSIDs) {
            diffAcl.AddAccess(NACLib::EAccessType::Allow, NACLib::EAccessRights::GenericFull, usedSid);
        }

        auto useAccess = NACLib::EAccessRights::SelectRow | NACLib::EAccessRights::DescribeSchema;
        for (const auto& userSID : AppData()->DefaultUserSIDs) {
            diffAcl.AddAccess(NACLib::EAccessType::Allow, useAccess, userSID);
        }
        diffAcl.AddAccess(NACLib::EAccessType::Allow, useAccess, AppData()->AllAuthenticatedUsers);
        diffAcl.AddAccess(NACLib::EAccessType::Allow, useAccess, BUILTIN_ACL_ROOT);

        auto token = MakeIntrusive<NACLib::TUserToken>(BUILTIN_ACL_METADATA, TVector<NACLib::TSID>{});
        ::NKikimrSchemeOp::TModifyACL modifyACL;

        modifyACL.SetName(objectId);
        modifyACL.SetDiffACL(diffAcl.SerializeAsString());
        if (owner) {
            modifyACL.SetNewOwner(owner->GetUserSID());
        }

        return modifyACL;
    }

private:
    virtual std::optional<TString> GetUserToken() const {
        if (Context.GetExternalData().GetUserToken()) {
            return Context.GetExternalData().GetUserToken()->GetSerializedToken();
        }
        return std::nullopt;
    }

    virtual NKikimrSchemeOp::EOperationType GetOperationType() const = 0;
    virtual void FillSchemeOperation(
        NKikimrSchemeOp::TModifyScheme& request, const NKikimrSchemeOp::TMetadataObjectProperties& properties) const = 0;

private:
    static IClassBehaviour::TPtr GetBehaviourVerified(const TString& typeId) {
        NMetadata::IClassBehaviour::TPtr cBehaviour(NMetadata::IClassBehaviour::TPtr(NMetadata::IClassBehaviour::TFactory::Construct(typeId)));
        AFL_VERIFY(cBehaviour);
        return cBehaviour;
    }

    TConclusion<NKikimrSchemeOp::TModifyScheme> MakeRequest(const NKikimrSchemeOp::TMetadataObjectProperties& properties) const {
        NKikimrSchemeOp::TModifyScheme modifyScheme;
        modifyScheme.SetWorkingDir(StorageDirectory);
        modifyScheme.SetFailedOnAlreadyExists(!OriginalRequest.GetExistingOk());
        modifyScheme.SetSuccessOnNotExist(OriginalRequest.GetMissingOk());
        modifyScheme.SetFailOnExist(!OriginalRequest.GetReplaceIfExists());
        modifyScheme.SetOperationType(GetOperationType());
        FillSchemeOperation(modifyScheme, properties);
        return modifyScheme;
    }

    NThreading::TFuture<TYqlConclusionStatus> SendSchemeRequest(TEvTxUserProxy::TEvProposeTransaction* request) const {
        AFL_DEBUG(NKikimrServices::KQP_GATEWAY)("event", "propose_modify_metadata_object")("request", request->Record.DebugString());
        auto schemePromise = NThreading::NewPromise<NKqp::TSchemeOpRequestHandler::TResult>();
        const bool failOnExist = request->Record.GetTransaction().GetModifyScheme().GetFailOnExist();
        const bool successOnNotExist = request->Record.GetTransaction().GetModifyScheme().GetSuccessOnNotExist();
        ActorSystem->Register(new NKqp::TSchemeOpRequestHandler(request, schemePromise, failOnExist, successOnNotExist));
        return schemePromise.GetFuture().Apply([](const NThreading::TFuture<NKqp::TSchemeOpRequestHandler::TResult>& f) {
            if (f.GetValue().Success()) {
                return TYqlConclusionStatus::Success();
            } else {
                return TYqlConclusionStatus::Fail(f.GetValue().Status(), f.GetValue().Issues().ToString());
            }
        });
    }

public:
    void OnBuildFinished(NKikimrSchemeOp::TMetadataObjectProperties properties) override {
        auto ev = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        ev->Record.SetDatabaseName(Context.GetExternalData().GetDatabase());
        if (const std::optional<TString>& userToken = GetUserToken()) {
            ev->Record.SetUserToken(*userToken);
        }
        {
            auto conclusion = MakeRequest(properties);
            if (conclusion.IsFail()) {
                Promise.SetValue(IOperationsManager::TYqlConclusionStatus::Fail(conclusion.GetErrorMessage()));
            }
            *ev->Record.MutableTransaction()->MutableModifyScheme() = conclusion.DetachResult();
        }

        auto future = SendSchemeRequest(ev.Release());
        future.Subscribe([promise = Promise](NThreading::TFuture<TYqlConclusionStatus> f) mutable {
            promise.SetValue(f.GetValueSync());
        });
    }

    virtual void OnBuildProblem(const TString& errorMessage) override {
        Promise.SetValue(IOperationsManager::TYqlConclusionStatus::Fail(errorMessage));
    }

    TOperationControllerBase(NThreading::TPromise<TYqlConclusionStatus> promise, NYql::TObjectSettingsImpl settings,
        const TString& storageDirectory, IOperationsManager::TInternalModificationContext context)
        : Promise(std::move(promise))
        , ActorSystem(context.GetExternalData().GetActorSystem())
        , OriginalRequest(std::move(settings))
        , StorageDirectory(storageDirectory)
        , Context(std::move(context)) {
        AFL_VERIFY(ActorSystem);
    }
};

class TCreateOperationController: public TOperationControllerBase {
private:
    std::optional<TString> GetUserToken() const override {
        return NACLib::TSystemUsers::Metadata().GetSerializedToken();
    }

    NKikimrSchemeOp::EOperationType GetOperationType() const override {
        return NKikimrSchemeOp::ESchemeOpCreateMetadataObject;
    }

    void FillSchemeOperation(
        NKikimrSchemeOp::TModifyScheme& request, const NKikimrSchemeOp::TMetadataObjectProperties& properties) const override {
        request.MutableCreateMetadataObject()->SetName(OriginalRequest.GetObjectId());
        request.MutableCreateMetadataObject()->MutableProperties()->CopyFrom(properties);
        *request.MutableModifyACL() = MakeModifyACL(OriginalRequest.GetObjectId(), Context.GetExternalData().GetUserToken());
    }

public:
    using TOperationControllerBase::TOperationControllerBase;
};

class TAlterOperationController: public TOperationControllerBase {
private:
    NKikimrSchemeOp::EOperationType GetOperationType() const override {
        return NKikimrSchemeOp::ESchemeOpAlterMetadataObject;
    }

    void FillSchemeOperation(
        NKikimrSchemeOp::TModifyScheme& request, const NKikimrSchemeOp::TMetadataObjectProperties& properties) const override {
        request.MutableCreateMetadataObject()->SetName(OriginalRequest.GetObjectId());
        request.MutableCreateMetadataObject()->MutableProperties()->CopyFrom(properties);
    }

public:
    using TOperationControllerBase::TOperationControllerBase;
};

class TDropOperationController: public TOperationControllerBase {
private:
    NKikimrSchemeOp::EOperationType GetOperationType() const override {
        return NKikimrSchemeOp::ESchemeOpDropMetadataObject;
    }

    void FillSchemeOperation(
        NKikimrSchemeOp::TModifyScheme& request, const NKikimrSchemeOp::TMetadataObjectProperties& /*properties*/) const override {
        request.MutableDrop()->SetName(OriginalRequest.GetObjectId());
    }

public:
    using TOperationControllerBase::TOperationControllerBase;
};

class TOperationControllerBuilderBase {
public:
    using TPtr = std::shared_ptr<TOperationControllerBuilderBase>;

private:
    YDB_READONLY(NThreading::TPromise<IOperationsManager::TYqlConclusionStatus>, Promise,
        NThreading::NewPromise<IOperationsManager::TYqlConclusionStatus>());
    YDB_ACCESSOR_DEF(std::optional<NYql::TObjectSettingsImpl>, Settings);
    YDB_ACCESSOR_DEF(std::optional<TString>, StorageDirectory);
    YDB_ACCESSOR_DEF(std::optional<IOperationsManager::TInternalModificationContext>, Context);

protected:
    virtual TOperationControllerBase::TPtr DoMakeController() const = 0;
    virtual IOperationsManager::EActivityType GetActivityType() const = 0;
    virtual NKqpProto::TKqpPhyMetadataObjectSettings GetSettingsProto(const NKqpProto::TKqpSchemeOperation& operation) const = 0;

public:
    bool IsInitialized() const {
        return Settings && StorageDirectory && Context;
    }

    void SetContext(IOperationsManager::TExternalModificationContext externalContext) {
        Context = IOperationsManager::TInternalModificationContext(std::move(externalContext));
        Context->SetActivityType(GetActivityType());
    }

    IOperationsManager::TYqlConclusionStatus SetSettingsFromKqpOperation(const NKqpProto::TKqpSchemeOperation& operation) {
        Settings = NYql::TObjectSettingsImpl();
        if (!Settings->ParseFromProto(GetSettingsProto(operation))) {
            return IOperationsManager::TYqlConclusionStatus::Fail("Cannot parse operation description");
        }
        return IOperationsManager::TYqlConclusionStatus::Success();
    }

    TOperationControllerBase::TPtr MakeControllerVerified() const {
        AFL_VERIFY(IsInitialized())("settings", Settings.has_value())("storage_directory", StorageDirectory.has_value())(
            "context", Context.has_value());
        return DoMakeController();
    }

    NThreading::TFuture<IOperationsManager::TYqlConclusionStatus> GetResultFuture() const {
        return Promise.GetFuture();
    }

    virtual ~TOperationControllerBuilderBase() = default;
};

class TCreateControllerBuilder : public TOperationControllerBuilderBase {
private:
    TOperationControllerBase::TPtr DoMakeController() const override {
        return std::make_shared<TCreateOperationController>(GetPromise(), GetSettings().value(), GetStorageDirectory().value(), GetContext().value());
    }

    IOperationsManager::EActivityType GetActivityType() const override {
        return IOperationsManager::EActivityType::Create;
    }

    NKqpProto::TKqpPhyMetadataObjectSettings GetSettingsProto(const NKqpProto::TKqpSchemeOperation& operation) const override {
        return operation.GetCreateSchemeObject();
    }
};

class TAlterControllerBuilder : public TOperationControllerBuilderBase {
private:
    TOperationControllerBase::TPtr DoMakeController() const override {
        return std::make_shared<TAlterOperationController>(GetPromise(), GetSettings().value(), GetStorageDirectory().value(), GetContext().value());
    }

    IOperationsManager::EActivityType GetActivityType() const override {
        return IOperationsManager::EActivityType::Alter;
    }

    NKqpProto::TKqpPhyMetadataObjectSettings GetSettingsProto(const NKqpProto::TKqpSchemeOperation& operation) const override {
        return operation.GetAlterSchemeObject();
    }
};

class TDropControllerBuilder : public TOperationControllerBuilderBase {
private:
    TOperationControllerBase::TPtr DoMakeController() const override {
        return std::make_shared<TDropOperationController>(GetPromise(), GetSettings().value(), GetStorageDirectory().value(), GetContext().value());
    }

    IOperationsManager::EActivityType GetActivityType() const override {
        return IOperationsManager::EActivityType::Drop;
    }

    NKqpProto::TKqpPhyMetadataObjectSettings GetSettingsProto(const NKqpProto::TKqpSchemeOperation& operation) const override {
        return operation.GetDropSchemeObject();
    }
};

}   // namespace

NThreading::TFuture<TSchemeObjectOperationsManager::TYqlConclusionStatus> TSchemeObjectOperationsManager::DoModify(
    const NYql::TObjectSettingsImpl& settings, const ui32 /*nodeId*/, const IClassBehaviour::TPtr& /*manager*/,
    TInternalModificationContext& context) const {
    AFL_VERIFY(context.GetExternalData().GetActorSystem())("type_id", settings.GetTypeId());
    AFL_DEBUG(NKikimrServices::KQP_GATEWAY)("event", "handle_modify_abstract_object")("type", settings.GetTypeId())("object", settings.GetObjectId());

    TOperationControllerBuilderBase::TPtr controllerBuilder;
    switch (context.GetActivityType()) {
        case IOperationsManager::EActivityType::Undefined:
            return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail("Operation type is undefined"));
        case IOperationsManager::EActivityType::Upsert:
            return NThreading::MakeFuture<TYqlConclusionStatus>(
                TYqlConclusionStatus::Fail("Upsert operations are not supported for metadata objects"));
        case IOperationsManager::EActivityType::Create:
            controllerBuilder = std::make_shared<TCreateControllerBuilder>();
            break;
        case IOperationsManager::EActivityType::Alter:
            controllerBuilder = std::make_shared<TAlterControllerBuilder>();
            break;
        case IOperationsManager::EActivityType::Drop:
            controllerBuilder = std::make_shared<TDropControllerBuilder>();
            break;
    }

    AFL_VERIFY(controllerBuilder);
    controllerBuilder->SetSettings(settings);
    controllerBuilder->SetStorageDirectory(GetStorageDirectory());
    controllerBuilder->SetContext(context);

    DoBuildRequestFromSettings(settings, context, controllerBuilder->MakeControllerVerified());
    return controllerBuilder->GetResultFuture();
}

NThreading::TFuture<TSchemeObjectOperationsManager::TYqlConclusionStatus> TSchemeObjectOperationsManager::ExecutePrepared(
    const NKqpProto::TKqpSchemeOperation& schemeOperation, const ui32 /*nodeId*/, const IClassBehaviour::TPtr& manager,
    const TExternalModificationContext& context) const {
    AFL_VERIFY(schemeOperation.GetObjectType() == manager->GetTypeId())("operation", schemeOperation.GetObjectType())(
                                                      "manager", manager->GetTypeId());

    TOperationControllerBuilderBase::TPtr controllerBuilder;
    switch (schemeOperation.GetOperationCase()) {
        case NKqpProto::TKqpSchemeOperation::kCreateSchemeObject:
            controllerBuilder = std::make_shared<TCreateControllerBuilder>();
            break;
        case NKqpProto::TKqpSchemeOperation::kAlterSchemeObject:
            controllerBuilder = std::make_shared<TAlterControllerBuilder>();
            break;
        case NKqpProto::TKqpSchemeOperation::kDropSchemeObject:
            controllerBuilder = std::make_shared<TDropControllerBuilder>();
            break;
        default:
            return NThreading::MakeFuture(TYqlConclusionStatus::Fail("Invalid operation type"));
    }

    AFL_VERIFY(controllerBuilder);
    controllerBuilder->SetSettingsFromKqpOperation(schemeOperation);
    controllerBuilder->SetStorageDirectory(GetStorageDirectory());
    controllerBuilder->SetContext(context);
    AFL_VERIFY(controllerBuilder->IsInitialized());

    DoBuildRequestFromSettings(
        controllerBuilder->GetSettings().value(), controllerBuilder->MutableContext().value(), controllerBuilder->MakeControllerVerified());
    return controllerBuilder->GetResultFuture();
}

}   // namespace NKikimr::NMetadata::NModifications

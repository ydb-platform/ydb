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

    TConclusion<NKikimrSchemeOp::TModifyScheme> MakeRequest(const NKikimrSchemeOp::TMetadataObjectProperties& properties) {
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
        AFL_DEBUG(NKikimrServices::KQP_GATEWAY)("event", "propose_modify_abstract_object")("request", request->Record.DebugString());
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
        const TString& storageDirector, IOperationsManager::TInternalModificationContext context, TActorSystem* actorSystem)
        : Promise(std::move(promise))
        , ActorSystem(actorSystem)
        , OriginalRequest(std::move(settings))
        , StorageDirectory(storageDirector)
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

}   // namespace

NThreading::TFuture<TSchemeObjectOperationsManager::TYqlConclusionStatus> TSchemeObjectOperationsManager::DoModify(
    const NYql::TObjectSettingsImpl& settings, const ui32 /*nodeId*/, const IClassBehaviour::TPtr& /*manager*/,
    TInternalModificationContext& context) const {
    AFL_VERIFY(context.GetExternalData().GetActorSystem())("type_id", settings.GetTypeId());
    NThreading::TPromise<TYqlConclusionStatus> promise = NThreading::NewPromise<TYqlConclusionStatus>();

    std::shared_ptr<IBuildRequestController> controller;
    switch (context.GetActivityType()) {
        case IOperationsManager::EActivityType::Undefined:
            return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail("Operation type is undefined"));
        case IOperationsManager::EActivityType::Upsert:
            return NThreading::MakeFuture<TYqlConclusionStatus>(
                TYqlConclusionStatus::Fail("Upsert operations are not supported for metadata objects"));
        case IOperationsManager::EActivityType::Create:
            controller = std::make_shared<TCreateOperationController>(
                promise, settings, GetStorageDirectory(), context, context.GetExternalData().GetActorSystem());
            break;
        case IOperationsManager::EActivityType::Alter:
            controller = std::make_shared<TAlterOperationController>(
                promise, settings, GetStorageDirectory(), context, context.GetExternalData().GetActorSystem());
            break;
        case IOperationsManager::EActivityType::Drop:
            controller = std::make_shared<TDropOperationController>(
                promise, settings, GetStorageDirectory(), context, context.GetExternalData().GetActorSystem());
            break;
    }

    DoBuildRequestFromSettings(settings, context, controller);
    return promise.GetFuture();
}

}   // namespace NKikimr::NMetadata::NModifications

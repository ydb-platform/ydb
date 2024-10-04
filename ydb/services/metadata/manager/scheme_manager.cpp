#include "scheme_manager.h"

#include <ydb/core/kqp/gateway/actors/scheme.h>

namespace NKikimr::NMetadata::NModifications {

namespace {

class TOperationController: public IBuildRequestController {
private:
    using TYqlConclusionStatus = IOperationsManager::TYqlConclusionStatus;

    NThreading::TPromise<TYqlConclusionStatus> Promise;
    const NYql::TObjectSettingsImpl OriginalRequest;
    IOperationsManager::TInternalModificationContext Context;
    TActorSystem* ActorSystem;

private:
    static IClassBehaviour::TPtr GetBehaviourVerified(const TString& typeId) {
        NMetadata::IClassBehaviour::TPtr cBehaviour(NMetadata::IClassBehaviour::TPtr(NMetadata::IClassBehaviour::TFactory::Construct(typeId)));
        AFL_VERIFY(cBehaviour);
        return cBehaviour;
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
    void OnBuildFinished(NKikimrSchemeOp::TModifyScheme request, std::optional<NACLib::TUserToken> userToken) override {
        auto ev = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        ev->Record.SetDatabaseName(Context.GetExternalData().GetDatabase());
        if (userToken) {
            ev->Record.SetUserToken(userToken->GetSerializedToken());
        }
        *ev->Record.MutableTransaction()->MutableModifyScheme() = std::move(request);

        auto future = SendSchemeRequest(ev.Release());
        future.Subscribe([promise = Promise](NThreading::TFuture<TYqlConclusionStatus> f) mutable {
            promise.SetValue(f.GetValueSync());
        });
    }

    virtual void OnBuildProblem(const TString& errorMessage) override {
        Promise.SetValue(IOperationsManager::TYqlConclusionStatus::Fail(errorMessage));
    }

    TOperationController(NThreading::TPromise<TYqlConclusionStatus> promise, NYql::TObjectSettingsImpl request,
        IOperationsManager::TInternalModificationContext context, TActorSystem* actorSystem)
        : Promise(std::move(promise))
        , OriginalRequest(std::move(request))
        , Context(std::move(context))
        , ActorSystem(actorSystem) {
        AFL_VERIFY(ActorSystem);
    }
};

}   // namespace

NThreading::TFuture<TSchemeObjectOperationsManager::TYqlConclusionStatus> TSchemeObjectOperationsManager::DoModify(
    const NYql::TObjectSettingsImpl& settings, const ui32 /*nodeId*/, const IClassBehaviour::TPtr& /*manager*/,
    TInternalModificationContext& context) const {
    AFL_VERIFY(context.GetExternalData().GetActorSystem())("type_id", settings.GetTypeId());
    NThreading::TPromise<TYqlConclusionStatus> promise = NThreading::NewPromise<TYqlConclusionStatus>();
    DoBuildRequestFromSettings(
        settings, context, std::make_shared<TOperationController>(promise, context, context.GetExternalData().GetActorSystem()));
    return promise.GetFuture();
}

}   // namespace NKikimr::NMetadata::NModifications

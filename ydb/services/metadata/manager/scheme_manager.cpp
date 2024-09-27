#include "scheme_manager.h"

#include <ydb/core/kqp/gateway/actors/scheme.h>

namespace NKikimr::NMetadata::NModifications {

namespace {

class TPreprocessingController: public IPreprocessingController {
private:
    using TYqlConclusionStatus = IOperationsManager::TYqlConclusionStatus;

    NThreading::TPromise<TYqlConclusionStatus> Promise;
    IOperationsManager::TInternalModificationContext Context;
    TActorSystem* ActorSystem;

private:
    NThreading::TFuture<TYqlConclusionStatus> SendSchemeRequest(TEvTxUserProxy::TEvProposeTransaction* request) const {
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

    TConclusion<THolder<TEvTxUserProxy::TEvProposeTransaction>> MakeRequest(const NYql::TObjectSettingsImpl& settings) const {
        auto ev = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        ev->Record.SetDatabaseName(Context.GetExternalData().GetDatabase());
        if (Context.GetExternalData().GetUserToken()) {
            ev->Record.SetUserToken(Context.GetExternalData().GetUserToken()->GetSerializedToken());
        }

        std::pair<TString, TString> dbLocalPathPair;
        {
            TString error;
            if (!TrySplitPathByDb(settings.GetObjectId(), Context.GetExternalData().GetDatabase(), dbLocalPathPair, error)) {
                return TConclusionStatus::Fail(error);
            }
        }

        auto& modifyScheme = *ev->Record.MutableTransaction()->MutableModifyScheme();
        modifyScheme.SetWorkingDir(dbLocalPathPair.first);
        modifyScheme.SetFailedOnAlreadyExists(!settings.GetExistingOk());
        modifyScheme.SetSuccessOnNotExist(settings.GetExistingOk());
        switch (Context.GetActivityType()) {
            case IOperationsManager::EActivityType::Create:
                modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateAbstractObject);
                break;
            case IOperationsManager::EActivityType::Alter:
                modifyScheme.SetSuccessOnNotExist(NKikimrSchemeOp::ESchemeOpAlterAbstractObject);
                break;
            case IOperationsManager::EActivityType::Drop:
                modifyScheme.SetSuccessOnNotExist(NKikimrSchemeOp::ESchemeOpDropAbstractObject);
                break;
            case IOperationsManager::EActivityType::Upsert:
                return TConclusionStatus::Fail("Upsert operations are not supported for " + settings.GetTypeId() + " objects");
            case IOperationsManager::EActivityType::Undefined:
                return TConclusionStatus::Fail("Operation type is undefined");
        }

        return ev;
    }

public:
    void OnPreprocessingFinished(NYql::TObjectSettingsImpl settings) override {
        auto request = MakeRequest(settings);
        if (request.IsFail()) {
            Promise.SetValue(TYqlConclusionStatus::Fail(request.GetErrorMessage()));
        }

        auto future = SendSchemeRequest(request.MutableResult().Release());
        future.Subscribe([promise = Promise](NThreading::TFuture<TYqlConclusionStatus> f) mutable {
            promise.SetValue(f.GetValueSync());
        });
    }

    virtual void OnPreprocessingProblem(const TString& errorMessage) override {
        Promise.SetValue(IOperationsManager::TYqlConclusionStatus::Fail(errorMessage));
    }

    TPreprocessingController(
        NThreading::TPromise<TYqlConclusionStatus> promise, IOperationsManager::TInternalModificationContext context, TActorSystem* actorSystem)
        : Promise(std::move(promise))
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
    NThreading::TPromise<TYqlConclusionStatus> promise;
    DoPreprocessSettings(
        settings, context, std::make_shared<TPreprocessingController>(promise, context, context.GetExternalData().GetActorSystem()));
    return promise.GetFuture();
}

}   // namespace NKikimr::NMetadata::NModifications

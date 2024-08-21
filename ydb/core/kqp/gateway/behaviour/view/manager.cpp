#include "manager.h"

#include <ydb/core/base/path.h>
#include <ydb/core/kqp/gateway/actors/scheme.h>
#include <ydb/core/kqp/gateway/utils/scheme_helpers.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

namespace NKikimr::NKqp {

namespace {

using TYqlConclusionStatus = TViewManager::TYqlConclusionStatus;
using TInternalModificationContext = TViewManager::TInternalModificationContext;

TString GetByKeyOrDefault(const NYql::TCreateObjectSettings& container, const TString& key) {
    const auto value = container.GetFeaturesExtractor().Extract(key);
    return value ? *value : TString{};
}

TYqlConclusionStatus CheckFeatureFlag(TInternalModificationContext& context) {
    auto* const actorSystem = context.GetExternalData().GetActorSystem();
    if (!actorSystem) {
        ythrow yexception() << "This place needs an actor system. Please contact internal support";
    }
    return AppData(actorSystem)->FeatureFlags.GetEnableViews()
        ? TYqlConclusionStatus::Success()
        : TYqlConclusionStatus::Fail("Views are disabled. Please contact your system administrator to enable the feature");
}

std::pair<TString, TString> SplitPathByDb(const TString& objectId,
                                          const TString& database) {
    std::pair<TString, TString> pathPair;
    TString error;
    if (!TrySplitPathByDb(objectId, database, pathPair, error)) {
        ythrow TBadArgumentException() << error;
    }
    return pathPair;
}

std::pair<TString, TString> SplitPathByObjectId(const TString& objectId) {
    std::pair<TString, TString> pathPair;
    TString error;
    if (!NSchemeHelpers::TrySplitTablePath(objectId, pathPair, error)) {
        ythrow TBadArgumentException() << error;
    }
    return pathPair;
}

void FillCreateViewProposal(NKikimrSchemeOp::TModifyScheme& modifyScheme,
                            const NYql::TCreateObjectSettings& settings,
                            const TString& database) {

    const auto pathPair = SplitPathByDb(settings.GetObjectId(), database);
    modifyScheme.SetWorkingDir(pathPair.first);
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateView);

    auto& viewDesc = *modifyScheme.MutableCreateView();
    viewDesc.SetName(pathPair.second);
    viewDesc.SetQueryText(GetByKeyOrDefault(settings, "query_text"));

    if (!settings.GetFeaturesExtractor().IsFinished()) {
        ythrow TBadArgumentException() << "Unknown property: " << settings.GetFeaturesExtractor().GetRemainedParamsString();
    }
}

void FillDropViewProposal(NKikimrSchemeOp::TModifyScheme& modifyScheme,
                         const NYql::TDropObjectSettings& settings) {

    const auto pathPair = SplitPathByObjectId(settings.GetObjectId());
    modifyScheme.SetWorkingDir(pathPair.first);
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpDropView);

    auto& drop = *modifyScheme.MutableDrop();
    drop.SetName(pathPair.second);
}

NThreading::TFuture<TYqlConclusionStatus> SendSchemeRequest(TEvTxUserProxy::TEvProposeTransaction* request,
                                                            TActorSystem* actorSystem,
                                                            bool failOnAlreadyExists) {
    const auto promiseScheme = NThreading::NewPromise<NKqp::TSchemeOpRequestHandler::TResult>();
    IActor* const requestHandler = new TSchemeOpRequestHandler(request, promiseScheme, failOnAlreadyExists);
    actorSystem->Register(requestHandler);
    return promiseScheme.GetFuture().Apply([](const NThreading::TFuture<NKqp::TSchemeOpRequestHandler::TResult>& opResult) {
        if (opResult.HasValue()) {
            if (!opResult.HasException() && opResult.GetValue().Success()) {
                return TYqlConclusionStatus::Success();
            }
            return TYqlConclusionStatus::Fail(opResult.GetValue().Status(), opResult.GetValue().Issues().ToString());
        }
        return TYqlConclusionStatus::Fail("no value in result");
    });
}

NThreading::TFuture<TYqlConclusionStatus> CreateView(const NYql::TCreateObjectSettings& settings,
                                                     TInternalModificationContext& context) {
    auto proposal = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
    proposal->Record.SetDatabaseName(context.GetExternalData().GetDatabase());
    if (context.GetExternalData().GetUserToken()) {
        proposal->Record.SetUserToken(context.GetExternalData().GetUserToken()->GetSerializedToken());
    }
    auto& schemeTx = *proposal->Record.MutableTransaction()->MutableModifyScheme();
    FillCreateViewProposal(schemeTx, settings, context.GetExternalData().GetDatabase());

    return SendSchemeRequest(proposal.Release(), context.GetExternalData().GetActorSystem(), true);
}

NThreading::TFuture<TYqlConclusionStatus> DropView(const NYql::TDropObjectSettings& settings,
                                                   TInternalModificationContext& context) {
    auto proposal = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
    proposal->Record.SetDatabaseName(context.GetExternalData().GetDatabase());
    if (context.GetExternalData().GetUserToken()) {
        proposal->Record.SetUserToken(context.GetExternalData().GetUserToken()->GetSerializedToken());
    }
    auto& schemeTx = *proposal->Record.MutableTransaction()->MutableModifyScheme();
    FillDropViewProposal(schemeTx, settings);

    return SendSchemeRequest(proposal.Release(), context.GetExternalData().GetActorSystem(), false);
}

void PrepareCreateView(NKqpProto::TKqpSchemeOperation& schemeOperation,
                       const NYql::TObjectSettingsImpl& settings,
                       TInternalModificationContext& context) {
    FillCreateViewProposal(*schemeOperation.MutableCreateView(), settings, context.GetExternalData().GetDatabase());
}

void PrepareDropView(NKqpProto::TKqpSchemeOperation& schemeOperation,
                     const NYql::TObjectSettingsImpl& settings) {
    FillDropViewProposal(*schemeOperation.MutableDropView(), settings);
}

}

NThreading::TFuture<TYqlConclusionStatus> TViewManager::DoModify(const NYql::TObjectSettingsImpl& settings,
                                                                 const ui32 nodeId,
                                                                 const NMetadata::IClassBehaviour::TPtr& manager,
                                                                 TInternalModificationContext& context) const {
    Y_UNUSED(nodeId, manager);
    const auto makeFuture = [](const TYqlConclusionStatus& status) {
        return NThreading::MakeFuture<TYqlConclusionStatus>(status);
    };

    try {
        if (const auto status = CheckFeatureFlag(context); status.IsFail()) {
            return makeFuture(status);
        }
        switch (context.GetActivityType()) {
            case EActivityType::Alter:
                return makeFuture(TYqlConclusionStatus::Fail("Alter operation for VIEW objects is not implemented"));
            case EActivityType::Upsert:
                return makeFuture(TYqlConclusionStatus::Fail("Upsert operation for VIEW objects is not implemented"));
            case EActivityType::Undefined:
                return makeFuture(TYqlConclusionStatus::Fail("Undefined operation for a VIEW object"));
            case EActivityType::Create:
                return CreateView(settings, context);
            case EActivityType::Drop:
                return DropView(settings, context);
        }
    } catch (...) {
        return makeFuture(TYqlConclusionStatus::Fail(CurrentExceptionMessage()));
    }
}

TViewManager::TYqlConclusionStatus TViewManager::DoPrepare(NKqpProto::TKqpSchemeOperation& schemeOperation,
                                                           const NYql::TObjectSettingsImpl& settings,
                                                           const NMetadata::IClassBehaviour::TPtr& manager,
                                                           TInternalModificationContext& context) const {
    Y_UNUSED(manager);

    try {
        if (const auto status = CheckFeatureFlag(context); status.IsFail()) {
            return status;
        }
        switch (context.GetActivityType()) {
            case EActivityType::Undefined:
                return TYqlConclusionStatus::Fail("Undefined operation for a VIEW object");
            case EActivityType::Upsert:
                return TYqlConclusionStatus::Fail("Upsert operation for VIEW objects is not implemented");
            case EActivityType::Alter:
                return TYqlConclusionStatus::Fail("Alter operation for VIEW objects is not implemented");
            case EActivityType::Create:
                PrepareCreateView(schemeOperation, settings, context);
                break;
            case EActivityType::Drop:
                PrepareDropView(schemeOperation, settings);
                break;
        }
    } catch (...) {
        return TYqlConclusionStatus::Fail(CurrentExceptionMessage());
    }
    return TYqlConclusionStatus::Success();
}

NThreading::TFuture<TYqlConclusionStatus> TViewManager::ExecutePrepared(const NKqpProto::TKqpSchemeOperation& schemeOperation,
                                                                        const ui32 nodeId,
                                                                        const NMetadata::IClassBehaviour::TPtr& manager,
                                                                        const TExternalModificationContext& context) const {
    Y_UNUSED(manager, nodeId);

    auto proposal = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
    proposal->Record.SetDatabaseName(context.GetDatabase());
    if (context.GetUserToken()) {
        proposal->Record.SetUserToken(context.GetUserToken()->GetSerializedToken());
    }

    auto& schemeTx = *proposal->Record.MutableTransaction()->MutableModifyScheme();
    switch (schemeOperation.GetOperationCase()) {
        case NKqpProto::TKqpSchemeOperation::kCreateView:
            schemeTx.CopyFrom(schemeOperation.GetCreateView());
            return SendSchemeRequest(proposal.Release(), context.GetActorSystem(), true);
        case NKqpProto::TKqpSchemeOperation::kDropView:
            schemeTx.CopyFrom(schemeOperation.GetDropView());
            return SendSchemeRequest(proposal.Release(), context.GetActorSystem(), false);
        default:
            return NThreading::MakeFuture(TYqlConclusionStatus::Fail(
                    TStringBuilder()
                        << "Execution of prepare operation for a VIEW object, unsupported operation: "
                        << int(schemeOperation.GetOperationCase())
                )
            );
    }
}

}

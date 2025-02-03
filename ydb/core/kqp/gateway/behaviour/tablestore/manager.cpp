#include "behaviour.h"
#include "manager.h"

#include "operations/abstract.h"

#include <ydb/core/base/path.h>
#include <ydb/core/kqp/gateway/actors/scheme.h>
#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>

#include <util/string/type.h>

namespace NKikimr::NKqp {

TConclusion<ITableStoreOperation::TPtr> TTableStoreManager::BuildOperation(
    const NYql::TObjectSettingsImpl& settings, NMetadata::NModifications::IOperationsManager::TInternalModificationContext& context) const {
    if (context.GetActivityType() != NMetadata::NModifications::IOperationsManager::EActivityType::Alter) {
        return TConclusionStatus::Fail("not implemented");
    }
    if (IsStandalone && !AppData()->ColumnShardConfig.GetAlterObjectEnabled()) {
        return TConclusionStatus::Fail("ALTER OBJECT is disabled for column tables");
    }
    auto actionName = settings.GetFeaturesExtractor().Extract("ACTION");
    if (!actionName) {
        return TConclusionStatus::Fail("can't find ACTION parameter");
    }
    ITableStoreOperation::TPtr operation(ITableStoreOperation::TFactory::Construct(*actionName));
    if (!operation) {
        return TConclusionStatus::Fail("invalid ACTION: " + *actionName);
    }
    {
        auto parsingResult = operation->Deserialize(settings);
        if (!parsingResult) {
            return TConclusionStatus::Fail(parsingResult.GetErrorMessage());
        }
    }
    return operation;
}

NThreading::TFuture<TTableStoreManager::TYqlConclusionStatus> TTableStoreManager::SendSchemeTx(
    THolder<TEvTxUserProxy::TEvProposeTransaction>&& request,
    const NMetadata::NModifications::IOperationsManager::TExternalModificationContext& context) const {
    auto* actorSystem = context.GetActorSystem();
    if (!actorSystem) {
        return NThreading::MakeFuture<TYqlConclusionStatus>(
            TYqlConclusionStatus::Fail("This place needs an actor system. Please contact internal support"));
    }

    auto promiseScheme = NThreading::NewPromise<NKqp::TSchemeOpRequestHandler::TResult>();
    actorSystem->Register(new NKqp::TSchemeOpRequestHandler(request.Release(), promiseScheme, false));
    return promiseScheme.GetFuture().Apply([](const NThreading::TFuture<NKqp::TSchemeOpRequestHandler::TResult>& f) {
        if (f.HasValue() && !f.HasException() && f.GetValue().Success()) {
            return TYqlConclusionStatus::Success();
        } else if (f.HasValue()) {
            return TYqlConclusionStatus::Fail(f.GetValue().Status(), f.GetValue().Issues().ToString());
        }
        return TYqlConclusionStatus::Fail("no value in result");
    });
}

NThreading::TFuture<TTableStoreManager::TYqlConclusionStatus> TTableStoreManager::DoModify(const NYql::TObjectSettingsImpl& settings,
    const ui32 nodeId, const NMetadata::IClassBehaviour::TPtr& manager, TInternalModificationContext& context) const {
    Y_UNUSED(nodeId);
    Y_UNUSED(manager);
    auto operation = BuildOperation(settings, context);
    if (operation.IsFail()) {
        return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail(operation.GetErrorMessage()));
    }

    auto ev = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
    ev->Record.SetDatabaseName(context.GetExternalData().GetDatabase());
    if (context.GetExternalData().GetUserToken()) {
        ev->Record.SetUserToken(context.GetExternalData().GetUserToken()->GetSerializedToken());
    }
    auto& schemeTx = *ev->Record.MutableTransaction()->MutableModifyScheme();
    (*operation)->SerializeScheme(schemeTx, IsStandalone);

    return SendSchemeTx(std::move(ev), context.GetExternalData());
}

NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus TTableStoreManager::DoPrepare(
    NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TObjectSettingsImpl& settings, const NMetadata::IClassBehaviour::TPtr& manager,
    NMetadata::NModifications::IOperationsManager::TInternalModificationContext& context) const {
    schemeOperation.SetObjectType(manager->GetTypeId());

    auto operation = BuildOperation(settings, context);
    if (operation.IsFail()) {
        return TYqlConclusionStatus::Fail(operation.GetErrorMessage());
    }
    auto* serializedOperation = [this, &schemeOperation]() {
        if (IsStandalone) {
            return schemeOperation.MutableAlterColumnTable();
        } else {
            return schemeOperation.MutableAlterTableStore();
        }
    }();
    (*operation)->SerializeScheme(*serializedOperation, IsStandalone);

    return TYqlConclusionStatus::Success();
}

NThreading::TFuture<NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus> TTableStoreManager::ExecutePrepared(
    const NKqpProto::TKqpSchemeOperation& schemeOperation, const ui32 /*nodeId*/, const NMetadata::IClassBehaviour::TPtr& /*manager*/,
    const IOperationsManager::TExternalModificationContext& context) const {
    const NKikimrSchemeOp::TModifyScheme& schemeTx = [this, &schemeOperation]() {
        if (IsStandalone) {
            return schemeOperation.GetAlterColumnTable();
        } else {
            return schemeOperation.GetAlterTableStore();
        }
    }();

    auto ev = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
    ev->Record.SetDatabaseName(context.GetDatabase());
    if (context.GetUserToken()) {
        ev->Record.SetUserToken(context.GetUserToken()->GetSerializedToken());
    }
    ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(schemeTx);

    return SendSchemeTx(std::move(ev), context);
}
}

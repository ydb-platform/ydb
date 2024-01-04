#include "behaviour.h"
#include "manager.h"
#include "operations/abstract.h"

#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/kqp/gateway/actors/scheme.h>
#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>
#include <ydb/core/base/path.h>

#include <util/string/type.h>

namespace NKikimr::NKqp {

NThreading::TFuture<TTableStoreManager::TYqlConclusionStatus> TTableStoreManager::DoModify(const NYql::TObjectSettingsImpl& settings, const ui32 nodeId,
        const NMetadata::IClassBehaviour::TPtr& manager, TInternalModificationContext& context) const {
    Y_UNUSED(nodeId);
    Y_UNUSED(manager);
    auto promise = NThreading::NewPromise<TYqlConclusionStatus>();
    auto result = promise.GetFuture();

    auto* actorSystem = context.GetExternalData().GetActorSystem();
    if (!actorSystem) {
        return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail("This place needs an actor system. Please contact internal support"));
    }

    switch (context.GetActivityType()) {
        case EActivityType::Create:
        case EActivityType::Upsert:
        case EActivityType::Drop:
        case EActivityType::Undefined:
            return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail("not implemented"));
        case EActivityType::Alter:
        try {
            auto actionName = settings.GetFeaturesExtractor().Extract("ACTION");
            if (!actionName) {
                return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail("can't find ACTION parameter"));
            }
            ITableStoreOperation::TPtr operation(ITableStoreOperation::TFactory::Construct(*actionName));
            if (!operation) {
                return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail("invalid ACTION: " + *actionName));
            }
            {
                auto parsingResult = operation->Deserialize(settings);
                if (!parsingResult) {
                    return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail(parsingResult.GetErrorMessage()));
                }
            }
            auto ev = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
            ev->Record.SetDatabaseName(context.GetExternalData().GetDatabase());
            if (context.GetExternalData().GetUserToken()) {
                ev->Record.SetUserToken(context.GetExternalData().GetUserToken()->GetSerializedToken());
            }
            auto& schemeTx = *ev->Record.MutableTransaction()->MutableModifyScheme();
            operation->SerializeScheme(schemeTx, IsStandalone);

            auto promiseScheme = NThreading::NewPromise<NKqp::TSchemeOpRequestHandler::TResult>();
            actorSystem->Register(new NKqp::TSchemeOpRequestHandler(ev.Release(), promiseScheme, false));
            return promiseScheme.GetFuture().Apply([](const NThreading::TFuture<NKqp::TSchemeOpRequestHandler::TResult>& f) {
                if (f.HasValue() && !f.HasException() && f.GetValue().Success()) {
                    return TYqlConclusionStatus::Success();
                } else if (f.HasValue()) {
                    return TYqlConclusionStatus::Fail(f.GetValue().Status(), f.GetValue().Issues().ToString());
                }
                return TYqlConclusionStatus::Fail("no value in result");
            });
        } catch (yexception& e) {
            return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail(e.what()));
        }
    }
    return result;
}

NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus TTableStoreManager::DoPrepare(NKqpProto::TKqpSchemeOperation& /*schemeOperation*/, const NYql::TObjectSettingsImpl& /*settings*/,
    const NMetadata::IClassBehaviour::TPtr& /*manager*/, NMetadata::NModifications::IOperationsManager::TInternalModificationContext& /*context*/) const {
    return NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus::Fail(
        "Prepare operations for TABLE objects are not supported");
}

NThreading::TFuture<NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus> TTableStoreManager::ExecutePrepared(const NKqpProto::TKqpSchemeOperation& /*schemeOperation*/,
        const ui32 /*nodeId*/, const NMetadata::IClassBehaviour::TPtr& /*manager*/, const IOperationsManager::TExternalModificationContext& /*context*/) const {
    return NThreading::MakeFuture(NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus::Fail(
        "Execution of prepare operations for TABLE objects is not supported"));
}

}

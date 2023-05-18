#include "behaviour.h"
#include "manager.h"
#include "operations/abstract.h"

#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/kqp/gateway/actors/scheme.h>
#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>
#include <ydb/core/base/path.h>

#include <util/string/type.h>

namespace NKikimr::NKqp {

NThreading::TFuture<NMetadata::NModifications::TObjectOperatorResult> TTableStoreManager::DoModify(const NYql::TObjectSettingsImpl& settings, const ui32 nodeId,
        NMetadata::IClassBehaviour::TPtr manager, TInternalModificationContext& context) const {
            Y_UNUSED(nodeId);
            Y_UNUSED(manager);
        auto promise = NThreading::NewPromise<NMetadata::NModifications::TObjectOperatorResult>();
        auto result = promise.GetFuture();

        switch (context.GetActivityType()) {
            case EActivityType::Create:
            case EActivityType::Drop:
            case EActivityType::Undefined:
                return NThreading::MakeFuture<NMetadata::NModifications::TObjectOperatorResult>(NMetadata::NModifications::TObjectOperatorResult("not impelemented"));
            case EActivityType::Alter:
            try {
                auto actionIt = settings.GetFeatures().find("ACTION");
                if (actionIt == settings.GetFeatures().end()) {
                    return NThreading::MakeFuture<NMetadata::NModifications::TObjectOperatorResult>(NMetadata::NModifications::TObjectOperatorResult("can't find ACTION"));
                }
                ITableStoreOperation::TPtr operation(ITableStoreOperation::TFactory::Construct(actionIt->second));
                if (!operation) {
                    return NThreading::MakeFuture<NMetadata::NModifications::TObjectOperatorResult>(NMetadata::NModifications::TObjectOperatorResult("invalid ACTION: " + actionIt->second));
                }
                {
                    auto parsingResult = operation->Deserialize(settings);
                    if (!parsingResult.IsSuccess()) {
                        return NThreading::MakeFuture<NMetadata::NModifications::TObjectOperatorResult>(parsingResult);
                    }
                }
                auto ev = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
                ev->Record.SetDatabaseName(context.GetExternalData().GetDatabase());
                if (context.GetExternalData().GetUserToken()) {
                    ev->Record.SetUserToken(context.GetExternalData().GetUserToken()->GetSerializedToken());
                }
                auto& schemeTx = *ev->Record.MutableTransaction()->MutableModifyScheme();
                operation->SerializeScheme(schemeTx);

                auto promiseScheme = NThreading::NewPromise<NKqp::TSchemeOpRequestHandler::TResult>(); 
                TActivationContext::AsActorContext().Register(new NKqp::TSchemeOpRequestHandler(ev.Release(), promiseScheme, false));
                return promiseScheme.GetFuture().Apply([](const NThreading::TFuture<NKqp::TSchemeOpRequestHandler::TResult>& f) {
                    if (f.HasValue() && !f.HasException() && f.GetValue().Success()) {
                        NMetadata::NModifications::TObjectOperatorResult localResult(true);
                        return localResult;
                    } else if (f.HasValue()) {
                        NMetadata::NModifications::TObjectOperatorResult localResult(f.GetValue().Issues().ToString());
                        return localResult;
                    }
                    NMetadata::NModifications::TObjectOperatorResult localResult(false);
                    return localResult;
                    });
            } catch (yexception& e) {
                return NThreading::MakeFuture<NMetadata::NModifications::TObjectOperatorResult>(NMetadata::NModifications::TObjectOperatorResult(e.what()));
            }
        }
        return result;
}

}


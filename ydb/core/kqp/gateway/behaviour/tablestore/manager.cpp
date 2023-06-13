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
        NMetadata::IClassBehaviour::TPtr manager, TInternalModificationContext& context) const {
            Y_UNUSED(nodeId);
            Y_UNUSED(manager);
        auto promise = NThreading::NewPromise<TYqlConclusionStatus>();
        auto result = promise.GetFuture();

        switch (context.GetActivityType()) {
            case EActivityType::Create:
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
                operation->SerializeScheme(schemeTx);

                auto promiseScheme = NThreading::NewPromise<NKqp::TSchemeOpRequestHandler::TResult>(); 
                TActivationContext::AsActorContext().Register(new NKqp::TSchemeOpRequestHandler(ev.Release(), promiseScheme, false));
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

}


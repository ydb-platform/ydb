#include "yql_generic_provider.h"

#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>

namespace NYql {

    TDataProviderInitializer GetGenericDataProviderInitializer(NConnector::IClient::TPtr genericClient,
                                                               const std::shared_ptr<IDatabaseAsyncResolver> dbResolver)
    {
        return [genericClient, dbResolver](const TString& userName, const TString& sessionId, const TGatewaysConfig* gatewaysConfig,
                                           const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
                                           TIntrusivePtr<IRandomProvider> randomProvider, TIntrusivePtr<TTypeAnnotationContext> typeCtx,
                                           const TOperationProgressWriter& progressWriter, const TYqlOperationOptions& operationOptions,
                                           THiddenQueryAborter)
        {
            Y_UNUSED(sessionId);
            Y_UNUSED(userName);
            Y_UNUSED(functionRegistry);
            Y_UNUSED(randomProvider);
            Y_UNUSED(progressWriter);
            Y_UNUSED(operationOptions);

            auto state = MakeIntrusive<TGenericState>();

            state->Types = typeCtx.Get();
            state->FunctionRegistry = functionRegistry;
            state->DbResolver = dbResolver;
            if (gatewaysConfig) {
                state->Configuration->Init(gatewaysConfig->GetGeneric(), state->DbResolver, state->DatabaseIds, typeCtx->Credentials);
            }

            TDataProviderInfo info;

            info.Names.insert({TString{GenericProviderName}});
            info.Source = CreateGenericDataSource(state, genericClient);
            info.Sink = CreateGenericDataSink(state);

            return info;
        };
    }

} // namespace NYql

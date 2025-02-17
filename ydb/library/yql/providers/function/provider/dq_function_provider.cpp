#include "dq_function_provider.h"

#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <yql/essentials/providers/common/schema/expr/yql_expr_schema.h>
#include <yql/essentials/core/yql_data_provider.h>
#include <yql/essentials/core/yql_type_annotation.h>

namespace NYql {

TDataProviderInitializer GetDqFunctionDataProviderInitializer(
        ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
        TDqFunctionGatewayFactory::TPtr gatewayFactory,
        // TRunActorParams.TScope
        const TString& scopeFolderId,
        const THashMap<TString, TString>& secureParams) {

    return [credentialsFactory, gatewayFactory, scopeFolderId, secureParams] (
                const TString& userName,
                const TString& sessionId,
                const TGatewaysConfig* gatewaysConfig,
                const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
                TIntrusivePtr<IRandomProvider> randomProvider,
                TIntrusivePtr<TTypeAnnotationContext> typeCtx,
                const TOperationProgressWriter& progressWriter,
                const TYqlOperationOptions& operationOptions,
                THiddenQueryAborter hiddenAborter,
                const TQContext& qContext)
        {

            Y_UNUSED(userName);
            Y_UNUSED(gatewaysConfig);
            Y_UNUSED(functionRegistry);
            Y_UNUSED(randomProvider);
            Y_UNUSED(progressWriter);
            Y_UNUSED(operationOptions);
            Y_UNUSED(hiddenAborter);
            Y_UNUSED(qContext);

            auto state = MakeIntrusive<TDqFunctionState>();
            state->SessionId = sessionId;
            state->Types = typeCtx.Get();
            state->GatewayFactory = gatewayFactory;
            state->ScopeFolderId = scopeFolderId;
            state->SecureParams = secureParams;

            TDataProviderInfo provider;
            provider.Names.insert({TString{FunctionProviderName}});
            provider.Source = CreateDqFunctionDataSource(state);
            provider.Sink = CreateDqFunctionDataSink(state);
            return provider;
        };
}

}
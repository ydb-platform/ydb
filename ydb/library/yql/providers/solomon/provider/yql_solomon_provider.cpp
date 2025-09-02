#include "yql_solomon_provider.h"

#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/solomon/provider/yql_solomon_dq_integration.h>

namespace NYql {

TDataProviderInitializer GetSolomonDataProviderInitializer(ISolomonGateway::TPtr gateway, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory, bool supportRtmrMode) {
    return [gateway, credentialsFactory, supportRtmrMode] (
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
        Y_UNUSED(sessionId);
        Y_UNUSED(userName);
        Y_UNUSED(functionRegistry);
        Y_UNUSED(randomProvider);
        Y_UNUSED(progressWriter);
        Y_UNUSED(operationOptions);
        Y_UNUSED(hiddenAborter);
        Y_UNUSED(qContext);

        auto solomonState = MakeIntrusive<TSolomonState>();

        solomonState->SupportRtmrMode = supportRtmrMode;
        solomonState->Types = typeCtx.Get();
        solomonState->Gateway = gateway;
        solomonState->CredentialsFactory = credentialsFactory;
        solomonState->DqIntegration = CreateSolomonDqIntegration(solomonState);
        if (gatewaysConfig) {
            solomonState->Configuration->Init(gatewaysConfig->GetSolomon(), typeCtx);
        }

        TDataProviderInfo info;

        info.Names.insert({TString{SolomonProviderName}});
        info.Source = CreateSolomonDataSource(solomonState);
        info.Sink = CreateSolomonDataSink(solomonState);

        return info;
    };
}

} // namespace NYql

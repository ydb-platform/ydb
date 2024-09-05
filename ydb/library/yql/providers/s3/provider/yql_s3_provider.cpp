#include "yql_s3_provider.h"
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>

namespace NYql {

TDataProviderInitializer GetS3DataProviderInitializer(IHTTPGateway::TPtr gateway, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory, bool allowLocalFiles, NActors::TActorSystem* actorSystem) {
    return [gateway, credentialsFactory, allowLocalFiles, actorSystem] (
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

        auto state = MakeIntrusive<TS3State>();

        state->Types = typeCtx.Get();
        state->FunctionRegistry = functionRegistry;
        state->CredentialsFactory = credentialsFactory;
        state->ActorSystem = actorSystem;
        if (gatewaysConfig) {
            state->Configuration->Init(gatewaysConfig->GetS3(), typeCtx);
        }
        state->Configuration->AllowLocalFiles = allowLocalFiles;
        state->Gateway = gateway;

        TDataProviderInfo info;

        info.Names.insert({TString{S3ProviderName}});
        info.Source = CreateS3DataSource(state);
        info.Sink = CreateS3DataSink(state);

        return info;
    };
}

} // namespace NYql

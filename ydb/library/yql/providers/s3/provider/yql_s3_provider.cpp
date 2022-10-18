#include "yql_s3_provider.h"
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>

namespace NYql {

TDataProviderInitializer GetS3DataProviderInitializer(IHTTPGateway::TPtr gateway, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory, bool allowLocalFiles) {
    return [gateway, credentialsFactory, allowLocalFiles] (
        const TString& userName,
        const TString& sessionId,
        const TGatewaysConfig* gatewaysConfig,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        TIntrusivePtr<IRandomProvider> randomProvider,
        TIntrusivePtr<TTypeAnnotationContext> typeCtx,
        const TOperationProgressWriter& progressWriter,
        const TYqlOperationOptions& operationOptions,
        THiddenQueryAborter)
    {
        Y_UNUSED(sessionId);
        Y_UNUSED(userName);
        Y_UNUSED(functionRegistry);
        Y_UNUSED(randomProvider);
        Y_UNUSED(progressWriter);
        Y_UNUSED(operationOptions);

        auto state = MakeIntrusive<TS3State>();

        state->Types = typeCtx.Get();
        state->FunctionRegistry = functionRegistry;
        state->CredentialsFactory = credentialsFactory;
        if (gatewaysConfig) {
            state->Configuration->Init(gatewaysConfig->GetS3(), typeCtx);
        }

        state->Configuration->AllowLocalFiles = allowLocalFiles;

        TDataProviderInfo info;

        info.Names.insert({TString{S3ProviderName}});
        info.Source = CreateS3DataSource(state, gateway);
        info.Sink = CreateS3DataSink(state, gateway);

        return info;
    };
}

} // namespace NYql

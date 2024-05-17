#include "yql_generic_provider.h"

#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>

namespace NYql {

    TDataProviderInitializer GetGenericDataProviderInitializer(NConnector::IClient::TPtr genericClient,
                                                               const IDatabaseAsyncResolver::TPtr& dbResolver,
                                                               const ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory)
    {
        return [genericClient, dbResolver, credentialsFactory](const TString& userName, const TString& sessionId, const TGatewaysConfig* gatewaysConfig,
                                                               const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
                                                               TIntrusivePtr<IRandomProvider> randomProvider, TIntrusivePtr<TTypeAnnotationContext> typeCtx,
                                                               const TOperationProgressWriter& progressWriter, const TYqlOperationOptions& operationOptions,
                                                               THiddenQueryAborter hiddenAborter, const TQContext& qContext)
        {
            Y_UNUSED(sessionId);
            Y_UNUSED(userName);
            Y_UNUSED(functionRegistry);
            Y_UNUSED(randomProvider);
            Y_UNUSED(progressWriter);
            Y_UNUSED(operationOptions);
            Y_UNUSED(hiddenAborter);
            Y_UNUSED(qContext);

            auto state = MakeIntrusive<TGenericState>(
                typeCtx.Get(),
                functionRegistry,
                dbResolver,
                credentialsFactory,
                genericClient,
                gatewaysConfig->GetGeneric());

            TDataProviderInfo info;

            info.Names.insert({TString{GenericProviderName}});
            info.Source = CreateGenericDataSource(state);
            info.Sink = CreateGenericDataSink(state);

            return info;
        };
    }

} // namespace NYql

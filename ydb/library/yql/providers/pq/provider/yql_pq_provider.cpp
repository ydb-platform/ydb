#include "yql_pq_provider.h"
#include "yql_pq_provider_impl.h"
#include "yql_pq_dq_integration.h"

#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/utils/log/context.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>

namespace NYql {

TDataProviderInitializer GetPqDataProviderInitializer(
    IPqGateway::TPtr gateway,
    bool supportRtmrMode,
    std::shared_ptr<NYql::IDatabaseAsyncResolver> dbResolver) {
    return [gateway, supportRtmrMode, dbResolver] (
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
            Y_UNUSED(functionRegistry);
            Y_UNUSED(randomProvider);
            Y_UNUSED(progressWriter);
            Y_UNUSED(operationOptions);
            Y_UNUSED(hiddenAborter);
            Y_UNUSED(qContext);

            auto state = MakeIntrusive<TPqState>(sessionId);
            state->SupportRtmrMode = supportRtmrMode;
            state->Types = typeCtx.Get();
            state->FunctionRegistry = functionRegistry;
            state->DbResolver = dbResolver;
            if (gatewaysConfig) {
                state->Configuration->Init(gatewaysConfig->GetPq(), typeCtx, dbResolver, state->DatabaseIds);
            }
            state->Gateway = gateway;
            state->DqIntegration = CreatePqDqIntegration(state);

            TDataProviderInfo info;

            info.Names.insert({TString{PqProviderName}});
            info.Source = CreatePqDataSource(state, gateway);
            info.Sink = CreatePqDataSink(state, gateway);

            info.OpenSession = [gateway](const TString& sessionId, const TString& username,
                                                  const TOperationProgressWriter& progressWriter, const TYqlOperationOptions& operationOptions,
                                                  TIntrusivePtr<IRandomProvider> randomProvider, TIntrusivePtr<ITimeProvider> timeProvider) {
                Y_UNUSED(progressWriter);
                Y_UNUSED(operationOptions);
                Y_UNUSED(randomProvider);
                Y_UNUSED(timeProvider);

                return gateway->OpenSession(sessionId, username);
            };

            info.CloseSessionAsync = [gateway](const TString& sessionId) {
                return gateway->CloseSession(sessionId);
            };

            return info;
        };
}

const TPqState::TTopicMeta* TPqState::FindTopicMeta(const TString& cluster, const TString& topicPath) const {
    const auto topicKey = std::make_pair(cluster, topicPath);
    return Topics.FindPtr(topicKey);
}

} // namespace NYql

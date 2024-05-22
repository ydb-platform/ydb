#include "yql_clickhouse_provider.h"
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>

namespace NYql {

TDataProviderInitializer GetClickHouseDataProviderInitializer(
    IHTTPGateway::TPtr gateway,
    const std::shared_ptr<NYql::IDatabaseAsyncResolver> dbResolver)
{
    return [gateway, dbResolver] (
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

        auto state = MakeIntrusive<TClickHouseState>();

        state->Types = typeCtx.Get();
        state->FunctionRegistry = functionRegistry;
        state->DbResolver = dbResolver;
        if (gatewaysConfig) {
            state->Configuration->Init(gatewaysConfig->GetClickHouse(), state->DbResolver, state->DatabaseIds);
        }

        TDataProviderInfo info;

        info.Names.insert({TString{ClickHouseProviderName}});
        info.Source = CreateClickHouseDataSource(state, gateway);
        info.Sink = CreateClickHouseDataSink(state);

        return info;
    };
}

} // namespace NYql

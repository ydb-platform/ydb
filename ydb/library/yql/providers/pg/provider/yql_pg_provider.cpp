#include "yql_pg_provider.h"
#include "yql_pg_provider_impl.h"

#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>

namespace NYql {

TDataProviderInitializer GetPgDataProviderInitializer() {
    return [] (
        const TString& userName,
        const TString& sessionId,
        const TGatewaysConfig* gatewaysConfig,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        TIntrusivePtr<IRandomProvider> randomProvider,
        TIntrusivePtr<TTypeAnnotationContext> typeCtx,
        const TOperationProgressWriter& progressWriter,
        const TYqlOperationOptions& operationOptions,
        THiddenQueryAborter hiddenAborter,
        const TQContext& qContext
    ) {
        Y_UNUSED(userName);
        Y_UNUSED(sessionId);
        Y_UNUSED(gatewaysConfig);
        Y_UNUSED(functionRegistry);
        Y_UNUSED(randomProvider);
        Y_UNUSED(typeCtx);
        Y_UNUSED(progressWriter);
        Y_UNUSED(operationOptions);
        Y_UNUSED(hiddenAborter);
        Y_UNUSED(qContext);

        auto state = MakeIntrusive<TPgState>();
        state->Types = typeCtx.Get();
        TDataProviderInfo info;
        info.Names.insert(TString{PgProviderName});

        info.Source = CreatePgDataSource(state);
        info.Sink = CreatePgDataSink(state);
        info.OpenSession = [](
            const TString& sessionId,
            const TString& username,
            const TOperationProgressWriter& progressWriter,
            const TYqlOperationOptions& operationOptions,
            TIntrusivePtr<IRandomProvider> randomProvider,
            TIntrusivePtr<ITimeProvider> timeProvider) {
            Y_UNUSED(sessionId);
            Y_UNUSED(username);
            Y_UNUSED(progressWriter);
            Y_UNUSED(operationOptions);
            Y_UNUSED(randomProvider);
            Y_UNUSED(timeProvider);
            return NThreading::MakeFuture();
        };

        info.CloseSessionAsync = [](const TString& sessionId) {
            Y_UNUSED(sessionId);
            return NThreading::MakeFuture();
        };

        return info;
    };
}

}

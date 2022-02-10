#include "yql_dq_provider.h"
#include "yql_dq_state.h"

#include "yql_dq_datasink.h"
#include "yql_dq_datasource.h"

#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h> 
#include <ydb/library/yql/providers/common/provider/yql_provider.h> 
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h> 

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

TDataProviderInitializer GetDqDataProviderInitializer(
    TExecTransformerFactory execTransformerFactory, 
    const IDqGateway::TPtr& dqGateway,
    NKikimr::NMiniKQL::TComputationNodeFactory compFactory,
    const IMetricsRegistryPtr& metrics,
    const TFileStoragePtr& fileStorage,
    bool externalUser)
{
    return [execTransformerFactory, dqGateway, compFactory, metrics, fileStorage, externalUser] ( 
        const TString& userName,
        const TString& sessionId,
        const TGatewaysConfig* gatewaysConfig,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        TIntrusivePtr<IRandomProvider> randomProvider,
        TIntrusivePtr<TTypeAnnotationContext> typeCtx,
        const TOperationProgressWriter& progressWriter,
        const TYqlOperationOptions& operationOptions
    ) {
        Y_UNUSED(userName);

        TDqStatePtr state = MakeIntrusive<TDqState>(
            dqGateway, // nullptr for yqlrun
            gatewaysConfig,
            functionRegistry,
            compFactory,
            randomProvider,
            typeCtx.Get(),
            progressWriter,
            operationOptions,
            sessionId,
            metrics, // nullptr for yqlrun
            fileStorage,
            dqGateway ? dqGateway->GetVanillaJobPath() : "",
            dqGateway ? dqGateway->GetVanillaJobMd5() : "",
            externalUser
        );

        TDataProviderInfo info;
        info.Names.insert(TString{DqProviderName});

        info.Source = CreateDqDataSource(state, execTransformerFactory); 
        info.Sink = CreateDqDataSink(state);
        info.OpenSession = [dqGateway, metrics, gatewaysConfig, state](
            const TString& sessionId,
            const TString& username,
            const TOperationProgressWriter& progressWriter,
            const TYqlOperationOptions& operationOptions,
            TIntrusivePtr<IRandomProvider> randomProvider,
            TIntrusivePtr<ITimeProvider> timeProvider) {

            if (metrics) {
                metrics->IncCounter("dq", "OpenSession");
            }

            if (gatewaysConfig) {
                state->Settings->Init(gatewaysConfig->GetDq(), username);
            }

            Y_UNUSED(progressWriter);
            Y_UNUSED(operationOptions);
            Y_UNUSED(randomProvider);
            Y_UNUSED(timeProvider);
            if (dqGateway) { // nullptr in yqlrun
                auto t = TInstant::Now();
                YQL_LOG(DEBUG) << "OpenSession " << sessionId;
                auto future = dqGateway->OpenSession(sessionId, username);
                future.Subscribe([sessionId, t] (const auto& ) {
                    YQL_LOG(DEBUG) << "OpenSession " << sessionId << " complete in " << (TInstant::Now()-t).MilliSeconds();
                });
                return future;
            } else {
                return NThreading::MakeFuture();
            }
        };

        info.CloseSession = [dqGateway, metrics](const TString& sessionId) {
            if (metrics) {
                metrics->IncCounter("dq", "CloseSession");
            }

            if (dqGateway) { // nullptr in yqlrun
                YQL_LOG(DEBUG) << "CloseSession " << sessionId;
                dqGateway->CloseSession(sessionId);
            }
        };

        info.TokenResolver = [](const TString& url) -> TMaybe<TString> {
            Y_UNUSED(url);

            return Nothing();
        };

        return info;
    };
}

} // namespace NYql

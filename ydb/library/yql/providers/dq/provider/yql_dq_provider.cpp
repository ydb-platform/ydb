#include "yql_dq_provider.h"
#include "yql_dq_state.h"

#include "yql_dq_datasink.h"
#include "yql_dq_datasource.h"

#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/common/activation/yql_activation.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>

#include <ydb/library/yql/dq/integration/transform/yql_dq_task_transform.h>
#include <ydb/library/yql/dq/transform/yql_common_dq_transform.h>

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
        const TYqlOperationOptions& operationOptions,
        THiddenQueryAborter hiddenAborter,
        const TQContext& qContext
    ) {
        Y_UNUSED(userName);
        Y_UNUSED(qContext);

        auto dqTaskTransformFactory = NYql::CreateCompositeTaskTransformFactory({
            NYql::CreateCommonDqTaskTransformFactory()
        });

        TDqStatePtr state = MakeIntrusive<TDqState>(
            dqGateway, // nullptr for yqlrun
            gatewaysConfig,
            functionRegistry,
            compFactory,
            dqTaskTransformFactory,
            randomProvider,
            typeCtx.Get(),
            progressWriter,
            operationOptions,
            sessionId,
            metrics, // nullptr for yqlrun
            fileStorage,
            dqGateway ? dqGateway->GetVanillaJobPath() : "",
            dqGateway ? dqGateway->GetVanillaJobMd5() : "",
            externalUser,
            std::move(hiddenAborter)
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
                std::unordered_set<std::string_view> groups;
                if (state->TypeCtx->Credentials != nullptr) {
                    groups.insert(state->TypeCtx->Credentials->GetGroups().begin(), state->TypeCtx->Credentials->GetGroups().end());
                }
                auto filter = [username, state, groups = std::move(groups)](const NYql::TAttr& attr) -> bool {
                    if (!attr.HasActivation()) {
                        return true;
                    }
                    if (NConfig::Allow(attr.GetActivation(), username, groups)) {
                        with_lock(state->Mutex) {
                            state->Statistics[Max<ui32>()].Entries.emplace_back(TStringBuilder() << "Activation:" << attr.GetName(), 0, 0, 0, 0, 1);
                        }
                        return true;
                    }
                    return false;
                };

                state->Settings->Init(gatewaysConfig->GetDq(), filter);
            }

            Y_UNUSED(progressWriter);
            Y_UNUSED(operationOptions);
            Y_UNUSED(randomProvider);
            Y_UNUSED(timeProvider);
            if (dqGateway) { // nullptr in yqlrun
                auto t = TInstant::Now();
                YQL_CLOG(DEBUG, ProviderDq) << "OpenSession " << sessionId;
                auto future = dqGateway->OpenSession(sessionId, username);
                future.Subscribe([sessionId, t] (const auto& ) {
                    YQL_CLOG(DEBUG, ProviderDq) << "OpenSession " << sessionId << " complete in " << (TInstant::Now()-t).MilliSeconds();
                });
                return future;
            } else {
                return NThreading::MakeFuture();
            }
        };

        info.CloseSessionAsync = [dqGateway, metrics](const TString& sessionId) {
            if (metrics) {
                metrics->IncCounter("dq", "CloseSession");
            }

            if (dqGateway) { // nullptr in yqlrun
                YQL_CLOG(DEBUG, ProviderDq) << "CloseSession " << sessionId;
                dqGateway->CloseSession(sessionId);

                return dqGateway->CloseSessionAsync(sessionId);
            }

            return NThreading::MakeFuture();
        };

        return info;
    };
}

} // namespace NYql

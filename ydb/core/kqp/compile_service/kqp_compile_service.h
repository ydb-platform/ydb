#pragma once

#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/common/simple/temp_tables.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>

namespace NKikimr {
namespace NKqp {

enum class ECompileActorAction {
    COMPILE,
    PARSE,
    SPLIT,
};

IActor* CreateKqpCompileService(const NKikimrConfig::TTableServiceConfig& tableServiceConfig,
    const NKikimrConfig::TQueryServiceConfig& queryServiceConfig,
    const TKqpSettings::TConstPtr& kqpSettings, TIntrusivePtr<TModuleResolverState> moduleResolverState,
    TIntrusivePtr<TKqpCounters> counters, std::shared_ptr<IQueryReplayBackendFactory> queryReplayFactory,
    std::optional<TKqpFederatedQuerySetup> federatedQuerySetup
    );

IActor* CreateKqpCompileComputationPatternService(const NKikimrConfig::TTableServiceConfig& serviceConfig,
    TIntrusivePtr<TKqpCounters> counters);

IActor* CreateKqpCompileActor(const TActorId& owner, const TKqpSettings::TConstPtr& kqpSettings,
    const NKikimrConfig::TTableServiceConfig& tableServiceConfig,
    const NKikimrConfig::TQueryServiceConfig& queryServiceConfig,
    TIntrusivePtr<TModuleResolverState> moduleResolverState, TIntrusivePtr<TKqpCounters> counters,
    const TString& uid, const TKqpQueryId& query,
    const TIntrusiveConstPtr<NACLib::TUserToken>& userToken,
    std::optional<TKqpFederatedQuerySetup> federatedQuerySetup,
    TKqpDbCountersPtr dbCounters, const TGUCSettings::TPtr& gUCSettings, const TMaybe<TString>& applicationName,
    const TIntrusivePtr<TUserRequestContext>& userRequestContext, NWilson::TTraceId traceId = {},
    TKqpTempTablesState::TConstPtr tempTablesState = nullptr,
    ECompileActorAction compileAction = ECompileActorAction::COMPILE,
    TMaybe<TQueryAst> queryAst = {},
    bool collectFullDiagnostics = false,
    bool PerStatementResult = false,
    NYql::TExprContext* ctx = nullptr,
    NYql::TExprNode::TPtr expr = nullptr);

IActor* CreateKqpCompileRequestActor(const TActorId& owner, const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, const TMaybe<TString>& uid,
    TMaybe<TKqpQueryId>&& query, bool keepInCache, const TInstant& deadline, TKqpDbCountersPtr dbCounters,
    ui64 cookie, NLWTrace::TOrbit orbit = {}, NWilson::TTraceId = {});

} // namespace NKqp
} // namespace NKikimr

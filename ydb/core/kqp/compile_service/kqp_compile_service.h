#pragma once

#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/common/simple/temp_tables.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>

namespace NKikimr {
namespace NKqp {

IActor* CreateKqpCompileService(const NKikimrConfig::TTableServiceConfig& serviceConfig,
    const NKikimrConfig::TMetadataProviderConfig& metadataProviderConfig,
    const TKqpSettings::TConstPtr& kqpSettings, TIntrusivePtr<TModuleResolverState> moduleResolverState,
    TIntrusivePtr<TKqpCounters> counters, std::shared_ptr<IQueryReplayBackendFactory> queryReplayFactory,
    std::optional<TKqpFederatedQuerySetup> federatedQuerySetup
    );

IActor* CreateKqpCompileComputationPatternService(const NKikimrConfig::TTableServiceConfig& serviceConfig,
    TIntrusivePtr<TKqpCounters> counters);

IActor* CreateKqpCompileActor(const TActorId& owner, const TKqpSettings::TConstPtr& kqpSettings,
    const NKikimrConfig::TTableServiceConfig& serviceConfig, 
    const NKikimrConfig::TMetadataProviderConfig& metadataProviderConfig,
    TIntrusivePtr<TModuleResolverState> moduleResolverState, TIntrusivePtr<TKqpCounters> counters,
    const TString& uid, const TKqpQueryId& query,
    const TIntrusiveConstPtr<NACLib::TUserToken>& userToken,
    std::optional<TKqpFederatedQuerySetup> federatedQuerySetup,
    TKqpDbCountersPtr dbCounters, const TIntrusivePtr<TUserRequestContext>& userRequestContext,
    NWilson::TTraceId traceId = {},
    TKqpTempTablesState::TConstPtr tempTablesState = nullptr);

IActor* CreateKqpCompileRequestActor(const TActorId& owner, const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, const TMaybe<TString>& uid,
    TMaybe<TKqpQueryId>&& query, bool keepInCache, const TInstant& deadline, TKqpDbCountersPtr dbCounters,
    ui64 cookie, NLWTrace::TOrbit orbit = {}, NWilson::TTraceId = {});

} // namespace NKqp
} // namespace NKikimr

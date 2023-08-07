#pragma once

#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>

namespace NKikimr {
namespace NKqp {

IActor* CreateKqpCompileService(const NKikimrConfig::TTableServiceConfig& serviceConfig,
    const TKqpSettings::TConstPtr& kqpSettings, TIntrusivePtr<TModuleResolverState> moduleResolverState,
    TIntrusivePtr<TKqpCounters> counters, std::shared_ptr<IQueryReplayBackendFactory> queryReplayFactory,
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    NYql::IHTTPGateway::TPtr httpGateway);

IActor* CreateKqpCompileActor(const TActorId& owner, const TKqpSettings::TConstPtr& kqpSettings,
    const NKikimrConfig::TTableServiceConfig& serviceConfig, NYql::IHTTPGateway::TPtr httpGateway,
    TIntrusivePtr<TModuleResolverState> moduleResolverState, TIntrusivePtr<TKqpCounters> counters,
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    const TString& uid, const TKqpQueryId& query,
    const TIntrusiveConstPtr<NACLib::TUserToken>& userToken,
    TKqpDbCountersPtr dbCounters, NWilson::TTraceId traceId = {});

IActor* CreateKqpCompileRequestActor(const TActorId& owner, const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, const TMaybe<TString>& uid,
    TMaybe<TKqpQueryId>&& query, bool keepInCache, const TInstant& deadline, TKqpDbCountersPtr dbCounters,
    ui64 cookie, NLWTrace::TOrbit orbit = {}, NWilson::TTraceId = {});

} // namespace NKqp
} // namespace NKikimr

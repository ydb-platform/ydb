#pragma once

#include "kqp.h"

#include <ydb/core/kqp/common/kqp_gateway.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>

#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/log/tls_backend.h>

#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/protobuf/util/pb_io.h>

namespace NKikimr {
namespace NKqp {

struct TKqpSettings {
    using TConstPtr = std::shared_ptr<const TKqpSettings>;

    TKqpSettings(const TVector<NKikimrKqp::TKqpSetting>& settings)
        : Settings(settings)
    {
        auto defaultSettingsData = NResource::Find("kqp_default_settings.txt");
        TStringInput defaultSettingsStream(defaultSettingsData);
        Y_VERIFY(TryParseFromTextFormat(defaultSettingsStream, DefaultSettings));
    }

    TKqpSettings()
        : Settings()
    {
        auto defaultSettingsData = NResource::Find("kqp_default_settings.txt");
        TStringInput defaultSettingsStream(defaultSettingsData);
        Y_VERIFY(TryParseFromTextFormat(defaultSettingsStream, DefaultSettings));
    }

    NKikimrKqp::TKqpDefaultSettings DefaultSettings;
    TVector<NKikimrKqp::TKqpSetting> Settings;
};

struct TModuleResolverState : public TThrRefBase {
    NYql::TExprContext ExprCtx;
    NYql::IModuleResolver::TPtr ModuleResolver;
    THolder<NYql::TExprContext::TFreezeGuard> FreezeGuardHolder;
};

void ApplyServiceConfig(NYql::TKikimrConfiguration& kqpConfig, const NKikimrConfig::TTableServiceConfig& serviceConfig);

IActor* CreateKqpCompileService(const NKikimrConfig::TTableServiceConfig& serviceConfig,
    const TKqpSettings::TConstPtr& kqpSettings, TIntrusivePtr<TModuleResolverState> moduleResolverState,
    TIntrusivePtr<TKqpCounters> counters, std::shared_ptr<IQueryReplayBackendFactory> queryReplayFactory);

IActor* CreateKqpCompileActor(const TActorId& owner, const TKqpSettings::TConstPtr& kqpSettings,
    const NKikimrConfig::TTableServiceConfig& serviceConfig, TIntrusivePtr<TModuleResolverState> moduleResolverState,
    TIntrusivePtr<TKqpCounters> counters, const TString& uid, const TKqpQueryId& query, const TString& userToken,
    TKqpDbCountersPtr dbCounters, NWilson::TTraceId traceId = {});

IActor* CreateKqpCompileRequestActor(const TActorId& owner, const TString& userToken, const TMaybe<TString>& uid,
    TMaybe<TKqpQueryId>&& query, bool keepInCache, const TInstant& deadline, TKqpDbCountersPtr dbCounters,
    NLWTrace::TOrbit orbit = {}, NWilson::TTraceId = {});

struct TKqpWorkerSettings {
    TString Cluster;
    TString Database;
    bool LongSession = false;

    NKikimrConfig::TTableServiceConfig Service;

    TKqpDbCountersPtr DbCounters;

    TKqpWorkerSettings(const TString& cluster, const TString& database,
        const NKikimrConfig::TTableServiceConfig& serviceConfig, TKqpDbCountersPtr dbCounters)
        : Cluster(cluster)
        , Database(database)
        , Service(serviceConfig)
        , DbCounters(dbCounters) {}
};

IActor* CreateKqpWorkerActor(const TActorId& owner, const TString& sessionId,
    const TKqpSettings::TConstPtr& kqpSettings, const TKqpWorkerSettings& workerSettings,
    TIntrusivePtr<TModuleResolverState> moduleResolverState, TIntrusivePtr<TKqpCounters> counters);

IActor* CreateKqpSessionActor(const TActorId& owner, const TString& sessionId,
    const TKqpSettings::TConstPtr& kqpSettings, const TKqpWorkerSettings& workerSettings,
    TIntrusivePtr<TModuleResolverState> moduleResolverState, TIntrusivePtr<TKqpCounters> counters);

TIntrusivePtr<IKqpGateway> CreateKikimrIcGateway(const TString& cluster, const TString& database,
    std::shared_ptr<IKqpGateway::IKqpTableMetadataLoader>&& metadataLoader, NActors::TActorSystem* actorSystem,
    ui32 nodeId, TKqpRequestCounters::TPtr counters);

TMaybe<Ydb::StatusIds::StatusCode> GetYdbStatus(const NYql::TIssue& issue);
Ydb::StatusIds::StatusCode GetYdbStatus(const NYql::NCommon::TOperationResult& queryResult);
Ydb::StatusIds::StatusCode GetYdbStatus(const NYql::TIssues& issues);
void AddQueryIssues(NKikimrKqp::TQueryResponse& response, const NYql::TIssues& issues);
bool HasSchemeOrFatalIssues(const NYql::TIssues& issues);

// for tests only
void FailForcedNewEngineCompilationForTests(bool fail = true);

} // namespace NKqp
} // namespace NKikimr

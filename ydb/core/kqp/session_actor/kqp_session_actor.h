#pragma once

#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>

#include <library/cpp/actors/core/actorid.h>

namespace NKikimr::NKqp {

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

IActor* CreateKqpSessionActor(const TActorId& owner, const TString& sessionId,
    const TKqpSettings::TConstPtr& kqpSettings, const TKqpWorkerSettings& workerSettings,
    NYql::IHTTPGateway::TPtr httpGateway, NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    TIntrusivePtr<TModuleResolverState> moduleResolverState, TIntrusivePtr<TKqpCounters> counters);

}  // namespace NKikimr::NKqp

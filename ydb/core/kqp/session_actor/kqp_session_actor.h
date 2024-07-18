#pragma once

#include <ydb/core/kqp/common/simple/temp_tables.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/table_service_config.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>

#include <ydb/core/control/immediate_control_board_wrapper.h>
#include <ydb/library/actors/core/actorid.h>

namespace NKikimr::NKqp::NComputeActor {
    struct IKqpNodeComputeActorFactory;
}

namespace NKikimr::NKqp::NRm {
    class IKqpResourceManager;
}

namespace NKikimr::NKqp {

struct TKqpWorkerSettings {
    TString Cluster;
    TString Database;
    TMaybe<TString> ApplicationName;
    TMaybe<TString> UserName;
    bool LongSession = false;

    NKikimrConfig::TTableServiceConfig TableService;
    NKikimrConfig::TQueryServiceConfig QueryService;

    TControlWrapper MkqlInitialMemoryLimit;
    TControlWrapper MkqlMaxMemoryLimit;

    TKqpDbCountersPtr DbCounters;

    explicit TKqpWorkerSettings(const TString& cluster, const TString& database,
            const TMaybe<TString>& applicationName, const TMaybe<TString>& userName, const NKikimrConfig::TTableServiceConfig& tableServiceConfig,
            const  NKikimrConfig::TQueryServiceConfig& queryServiceConfig, TKqpDbCountersPtr dbCounters)
        : Cluster(cluster)
        , Database(database)
        , ApplicationName(applicationName)
        , UserName(userName)
        , TableService(tableServiceConfig)
        , QueryService(queryServiceConfig)
        , MkqlInitialMemoryLimit(2097152, 1, Max<i64>())
        , MkqlMaxMemoryLimit(1073741824, 1, Max<i64>())
        , DbCounters(dbCounters)
    {
        AppData()->Icb->RegisterSharedControl(
            MkqlInitialMemoryLimit, "KqpSession.MkqlInitialMemoryLimit");
        AppData()->Icb->RegisterSharedControl(
            MkqlMaxMemoryLimit, "KqpSession.MkqlMaxMemoryLimit");
    }
};

IActor* CreateKqpSessionActor(const TActorId& owner,
    std::shared_ptr<NKikimr::NKqp::NRm::IKqpResourceManager> resourceManager_,
    std::shared_ptr<NKikimr::NKqp::NComputeActor::IKqpNodeComputeActorFactory> caFactory_,
    const TString& sessionId,
    const TKqpSettings::TConstPtr& kqpSettings, const TKqpWorkerSettings& workerSettings,
    std::optional<TKqpFederatedQuerySetup> federatedQuerySetup,
    NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
    TIntrusivePtr<TModuleResolverState> moduleResolverState, TIntrusivePtr<TKqpCounters> counters,
    const NKikimrConfig::TQueryServiceConfig& queryServiceConfig,
    const TActorId& kqpTempTablesAgentActor);

IActor* CreateKqpTempTablesManager(
    TKqpTempTablesState tempTablesState, TIntrusiveConstPtr<NACLib::TUserToken> userToken, const TActorId& target, const TString& database);

}  // namespace NKikimr::NKqp

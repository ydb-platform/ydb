#pragma once

#include <ydb/core/kqp/common/simple/temp_tables.h>
#include <ydb/core/kqp/common/kqp_tx_manager.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/protos/table_service_config.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/dq/runtime/dq_channel_service.h>

#include <ydb/core/control/lib/immediate_control_board_wrapper.h>
#include <ydb/library/actors/core/actorid.h>

namespace NACLib {
    class TUserContext;
}

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

    TIntrusivePtr<TExecuterMutableConfig> MutableExecuterConfig;

private:
    std::shared_ptr<const NKikimrConfig::TTableServiceConfig> TableServicePtr;
    std::shared_ptr<const NKikimrConfig::TQueryServiceConfig> QueryServicePtr;
    std::shared_ptr<const NKikimrConfig::TTliConfig> TliConfigPtr;

public:
    const NKikimrConfig::TTableServiceConfig& TableService;
    const NKikimrConfig::TQueryServiceConfig& QueryService;
    const NKikimrConfig::TTliConfig& TliConfig;

    TControlWrapper MkqlInitialMemoryLimit;
    TControlWrapper MkqlMaxMemoryLimit;

    TKqpDbCountersPtr DbCounters;

    explicit TKqpWorkerSettings(const TString& cluster, const TString& database,
            const TMaybe<TString>& applicationName, const TMaybe<TString>& userName,
            const TIntrusivePtr<TExecuterMutableConfig> mutableExecuterConfig,
            std::shared_ptr<const NKikimrConfig::TTableServiceConfig> tableServiceConfig,
            std::shared_ptr<const NKikimrConfig::TQueryServiceConfig> queryServiceConfig,
            std::shared_ptr<const NKikimrConfig::TTliConfig> tliConfig,
            TControlWrapper mkqlInitialMemoryLimit,
            TControlWrapper mkqlMaxMemoryLimit,
            TKqpDbCountersPtr dbCounters)
        : Cluster(cluster)
        , Database(database)
        , ApplicationName(applicationName)
        , UserName(userName)
        , MutableExecuterConfig(mutableExecuterConfig)
        , TableServicePtr(std::move(tableServiceConfig))
        , QueryServicePtr(std::move(queryServiceConfig))
        , TliConfigPtr(std::move(tliConfig))
        , TableService(*TableServicePtr)
        , QueryService(*QueryServicePtr)
        , TliConfig(*TliConfigPtr)
        , MkqlInitialMemoryLimit(mkqlInitialMemoryLimit)
        , MkqlMaxMemoryLimit(mkqlMaxMemoryLimit)
        , DbCounters(dbCounters)
    {}

    TKqpWorkerSettings(const TKqpWorkerSettings& other)
        : Cluster(other.Cluster)
        , Database(other.Database)
        , ApplicationName(other.ApplicationName)
        , UserName(other.UserName)
        , LongSession(other.LongSession)
        , MutableExecuterConfig(other.MutableExecuterConfig)
        , TableServicePtr(other.TableServicePtr)
        , QueryServicePtr(other.QueryServicePtr)
        , TliConfigPtr(other.TliConfigPtr)
        , TableService(*TableServicePtr)
        , QueryService(*QueryServicePtr)
        , TliConfig(*TliConfigPtr)
        , MkqlInitialMemoryLimit(other.MkqlInitialMemoryLimit)
        , MkqlMaxMemoryLimit(other.MkqlMaxMemoryLimit)
        , DbCounters(other.DbCounters)
    {}
};

class TKqpQueryCache;

IActor* CreateKqpSessionActor(const TActorId& owner,
    TIntrusivePtr<TKqpQueryCache> queryCache,
    std::shared_ptr<NKikimr::NKqp::NRm::IKqpResourceManager> resourceManager_,
    std::shared_ptr<NKikimr::NKqp::NComputeActor::IKqpNodeComputeActorFactory> caFactory_,
    const TString& sessionId,
    TIntrusiveConstPtr<NYql::TKikimrConfiguration> kqpConfig,
    const TKqpSettings::TConstPtr& kqpSettings, const TKqpWorkerSettings& workerSettings,
    std::optional<TKqpFederatedQuerySetup> federatedQuerySetup,
    NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
    TIntrusivePtr<TModuleResolverState> moduleResolverState, TIntrusivePtr<TKqpCounters> counters,
    const TActorId& kqpTempTablesAgentActor,
    std::shared_ptr<NYql::NDq::IDqChannelService> channelService,
    TIntrusivePtr<NACLib::TUserContext> userCtx);

IActor* CreateKqpTempTablesManager(
    TKqpTempTablesState tempTablesState, TIntrusiveConstPtr<NACLib::TUserToken> userToken, const TActorId& target, const TString& database);

}  // namespace NKikimr::NKqp

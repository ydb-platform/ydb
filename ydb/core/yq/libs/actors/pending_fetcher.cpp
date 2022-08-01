#include <ydb/core/yq/libs/config/protos/pinger.pb.h>
#include <ydb/core/yq/libs/config/protos/yq_config.pb.h>
#include "proxy.h"
#include "nodes_manager.h"

#include "database_resolver.h"

#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/protobuf/interop/cast.h>
#include <ydb/core/protos/services.pb.h>

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/utils/actor_log/log.h>
#include <ydb/library/yql/core/services/mounts/yql_mounts.h>
#include <ydb/library/yql/core/facade/yql_facade.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/providers/common/udf_resolve/yql_simple_udf_resolver.h>
#include <ydb/library/yql/providers/common/comp_nodes/yql_factory.h>
#include <ydb/library/yql/providers/common/schema/mkql/yql_mkql_schema.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_provider.h>
#include <ydb/library/yql/providers/dq/interface/yql_dq_task_transform.h>
#include <ydb/library/yql/providers/ydb/provider/yql_ydb_provider.h>
#include <ydb/library/yql/providers/clickhouse/provider/yql_clickhouse_provider.h>
#include <ydb/library/yql/sql/settings/translation_settings.h>
#include <library/cpp/yson/node/node_io.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/mkql_proto/mkql_proto.h>
#include <ydb/library/yql/providers/common/codec/yql_codec.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/dq/worker_manager/interface/events.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/public/issue/protos/issue_message.pb.h>

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_value/value.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

#include <ydb/core/yq/libs/common/compression.h>
#include <ydb/core/yq/libs/common/entity_id.h>
#include <ydb/core/yq/libs/events/events.h>

#include <ydb/core/yq/libs/control_plane_storage/control_plane_storage.h>
#include <ydb/core/yq/libs/control_plane_storage/events/events.h>
#include <ydb/core/yq/libs/private_client/internal_service.h>

#include <library/cpp/actors/core/log.h>

#include <ydb/library/security/util.h>

#include <util/generic/deque.h>
#include <util/generic/guid.h>
#include <util/system/hostname.h>

#define LOG_E(stream) \
    LOG_ERROR_S(*TlsActivationContext, NKikimrServices::YQL_PROXY, "Fetcher: " << stream)
#define LOG_W(stream) \
    LOG_WARN_S(*TlsActivationContext, NKikimrServices::YQL_PROXY, "Fetcher: " << stream)
#define LOG_I(stream) \
    LOG_INFO_S(*TlsActivationContext, NKikimrServices::YQL_PROXY, "Fetcher: " << stream)
#define LOG_D(stream) \
    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::YQL_PROXY, "Fetcher: " << stream)
#define LOG_T(stream) \
    LOG_TRACE_S(*TlsActivationContext, NKikimrServices::YQL_PROXY, "Fetcher: " << stream)

namespace NYq {

using namespace NActors;
using namespace NYql;
using namespace NFq;

namespace {

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),

        EvCleanupCounters = EvBegin,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    // Events
    struct TEvCleanupCounters : public NActors::TEventLocal<TEvCleanupCounters, EvCleanupCounters> {
        TEvCleanupCounters(const TString& queryId, const NActors::TActorId& runActorId)
            : QueryId(queryId)
            , RunActorId(runActorId)
        {
        }

        const TString QueryId;
        const NActors::TActorId RunActorId;
    };
};

template <class TElement>
TVector<TElement> VectorFromProto(const ::google::protobuf::RepeatedPtrField<TElement>& field) {
    return { field.begin(), field.end() };
}

constexpr auto CLEANUP_PERIOD = TDuration::Seconds(60);

} // namespace

class TPendingFetcher : public NActors::TActorBootstrapped<TPendingFetcher> {
public:
    TPendingFetcher(
        const NYq::TYqSharedResources::TPtr& yqSharedResources,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        const ::NYq::NConfig::TCommonConfig& commonConfig,
        const ::NYq::NConfig::TCheckpointCoordinatorConfig& checkpointCoordinatorConfig,
        const ::NYq::NConfig::TPrivateApiConfig& privateApiConfig,
        const ::NYq::NConfig::TGatewaysConfig& gatewaysConfig,
        const ::NYq::NConfig::TPingerConfig& pingerConfig,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        TIntrusivePtr<ITimeProvider> timeProvider,
        TIntrusivePtr<IRandomProvider> randomProvider,
        NKikimr::NMiniKQL::TComputationNodeFactory dqCompFactory,
        const ::NYql::NCommon::TServiceCounters& serviceCounters,
        ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
        IHTTPGateway::TPtr s3Gateway,
        ::NPq::NConfigurationManager::IConnections::TPtr pqCmConnections,
        const ::NMonitoring::TDynamicCounterPtr& clientCounters,
        const TString& tenantName
        )
        : YqSharedResources(yqSharedResources)
        , CredentialsProviderFactory(credentialsProviderFactory)
        , CommonConfig(commonConfig)
        , CheckpointCoordinatorConfig(checkpointCoordinatorConfig)
        , PrivateApiConfig(privateApiConfig)
        , GatewaysConfig(gatewaysConfig)
        , PingerConfig(pingerConfig)
        , FunctionRegistry(functionRegistry)
        , TimeProvider(timeProvider)
        , RandomProvider(randomProvider)
        , DqCompFactory(dqCompFactory)
        , ServiceCounters(serviceCounters, "pending_fetcher")
        , CredentialsFactory(credentialsFactory)
        , S3Gateway(s3Gateway)
        , PqCmConnections(std::move(pqCmConnections))
        , FetcherGuid(CreateGuidAsString())
        , ClientCounters(clientCounters)
        , TenantName(tenantName)
        , InternalServiceId(MakeInternalServiceActorId())
    {
        Y_ENSURE(GetYqlDefaultModuleResolverWithContext(ModuleResolver));
    }

    static constexpr char ActorName[] = "YQ_PENDING_FETCHER";

    void PassAway() final {
        LOG_D("Stop Fetcher");
        Send(DatabaseResolver, new NActors::TEvents::TEvPoison());
        NActors::IActor::PassAway();
    }

    void Bootstrap() {
        Become(&TPendingFetcher::StateFunc);
        DatabaseResolver = Register(CreateDatabaseResolver(MakeYqlAnalyticsHttpProxyId(), CredentialsFactory));
        Send(SelfId(), new NActors::TEvents::TEvWakeup());

        LogScope.ConstructInPlace(NActors::TActivationContext::ActorSystem(), NKikimrServices::YQL_PROXY, FetcherGuid);
    }

private:
    void OnUndelivered(NActors::TEvents::TEvUndelivered::TPtr&, const NActors::TActorContext&) {
        LOG_E("TYqlPendingFetcher::OnUndelivered");

        HasRunningRequest = false;
    }

    void HandleWakeup(NActors::TEvents::TEvWakeup::TPtr&) {
        Schedule(PendingFetchPeriod, new NActors::TEvents::TEvWakeup());
        if (!HasRunningRequest) {
            HasRunningRequest = true;
            GetPendingTask();
        }
    }

    void HandleCleanupCounters(TEvPrivate::TEvCleanupCounters::TPtr& ev) {
        const TString& queryId = ev->Get()->QueryId;
        const auto countersIt = CountersMap.find(queryId);
        if (countersIt == CountersMap.end() || countersIt->second.RunActorId != ev->Get()->RunActorId) {
            return;
        }

        auto& counters = countersIt->second;
        if (counters.RootCountersParent) {
            counters.RootCountersParent->RemoveSubgroup("query_id", queryId);
        }
        if (counters.PublicCountersParent) {
            counters.PublicCountersParent->RemoveSubgroup("query_id", queryId);
        }
        CountersMap.erase(countersIt);
    }

    void Handle(TEvInternalService::TEvGetTaskResponse::TPtr& ev) {
        HasRunningRequest = false;
        LOG_T("Got GetTask response from PrivateApi");
        if (!ev->Get()->Status.IsSuccess()) {
            LOG_E("Error with GetTask: "<< ev->Get()->Status.GetIssues().ToString());
            return;
        }

        const auto& res = ev->Get()->Result;

        LOG_T("Tasks count: " << res.tasks().size());
        if (!res.tasks().empty()) {
            ProcessTask(res);
            HasRunningRequest = true;
            GetPendingTask();
        }
    }

    void HandlePoisonTaken(NActors::TEvents::TEvPoisonTaken::TPtr& ev) {
        auto runActorId = ev->Sender;

        auto itA = RunActorMap.find(runActorId);
        if (itA == RunActorMap.end()) {
            LOG_W("Unknown RunActor " << runActorId << " destroyed");
            return;
        }
        auto queryId = itA->second;
        RunActorMap.erase(itA);

        auto itC = CountersMap.find(queryId);
        if (itC != CountersMap.end()) {
            auto& info = itC->second;
            if (info.RunActorId == runActorId) {
                Schedule(CLEANUP_PERIOD, new TEvPrivate::TEvCleanupCounters(queryId, runActorId));
            }
        }
    }

    void GetPendingTask() {
        FetcherGeneration++;
        LOG_T("Request Private::GetTask" << ", Owner: " << GetOwnerId() << ", Host: " << HostName() << ", Tenant: " << TenantName);
        Fq::Private::GetTaskRequest request;
        request.set_owner_id(GetOwnerId());
        request.set_host(HostName());
        request.set_tenant(TenantName);
        Send(InternalServiceId, new TEvInternalService::TEvGetTaskRequest(request));
    }

    void ProcessTask(const Fq::Private::GetTaskResult& result) {
        for (const auto& task : result.tasks()) {
            RunTask(task);
        }

    }

    TString GetOwnerId() const {
        return FetcherGuid + ToString(FetcherGeneration);
    }

    void RunTask(const Fq::Private::GetTaskResult::Task& task) {
        LOG_D("NewTask:"
              << " Scope: " << task.scope()
              << " Id: " << task.query_id().value()
              << " UserId: " << task.user_id()
              << " AuthToken: " << NKikimr::MaskTicket(task.user_token()));

        THashMap<TString, TString> serviceAccounts;
        for (const auto& identity : task.service_accounts()) {
            serviceAccounts[identity.value()] = identity.signature();
        }

        NDq::SetYqlLogLevels(NActors::NLog::PRI_TRACE);

        const TVector<TString> path = StringSplitter(task.scope()).Split('/').SkipEmpty(); // yandexcloud://{folder_id}
        const TString folderId = path.size() == 2 && path.front().StartsWith(NYdb::NYq::TScope::YandexCloudScopeSchema)
                            ? path.back() : TString{};
        const TString cloudId = task.sensor_labels().at("cloud_id");
        const TString queryId = task.query_id().value();

        ::NYql::NCommon::TServiceCounters queryCounters(ServiceCounters);
        auto publicCountersParent = ServiceCounters.PublicCounters;

        if (cloudId && folderId) {
            publicCountersParent = publicCountersParent->GetSubgroup("cloud_id", cloudId)->GetSubgroup("folder_id", folderId);
        }
        queryCounters.PublicCounters = publicCountersParent->GetSubgroup("query_id",
            task.automatic() ? (task.query_name() ? task.query_name() : "automatic") : queryId);

        auto rootCountersParent = ServiceCounters.RootCounters;
        std::set<std::pair<TString, TString>> sensorLabels(task.sensor_labels().begin(), task.sensor_labels().end());
        for (const auto& [label, item]: sensorLabels) {
            rootCountersParent = rootCountersParent->GetSubgroup(label, item);
        }

        queryCounters.RootCounters = rootCountersParent->GetSubgroup("query_id",
            task.automatic() ? (folderId ? "automatic_" + folderId : "automatic") : queryId);
        queryCounters.Counters = queryCounters.RootCounters;

        queryCounters.InitUptimeCounter();
        const auto createdAt = TInstant::Now();
        TVector<TString> dqGraphs;
        if (!task.dq_graph_compressed().empty()) {
            dqGraphs.reserve(task.dq_graph_compressed().size());
            for (auto& g : task.dq_graph_compressed()) {
                TCompressor compressor(g.method());
                dqGraphs.emplace_back(compressor.Decompress(g.data()));
            }
        } else {
            // todo: remove after migration
            dqGraphs = VectorFromProto(task.dq_graph());
        }
        TRunActorParams params(
            YqSharedResources, CredentialsProviderFactory, S3Gateway,
            FunctionRegistry, RandomProvider,
            ModuleResolver, ModuleResolver->GetNextUniqueId(),
            DqCompFactory, PqCmConnections,
            CommonConfig, CheckpointCoordinatorConfig,
            PrivateApiConfig, GatewaysConfig, PingerConfig,
            task.text(), task.scope(), task.user_token(),
            DatabaseResolver, queryId,
            task.user_id(), GetOwnerId(), task.generation(),
            VectorFromProto(task.connection()),
            VectorFromProto(task.binding()),
            CredentialsFactory,
            serviceAccounts,
            task.query_type(),
            task.execute_mode(),
            GetEntityIdAsString(CommonConfig.GetIdsPrefix(), EEntityType::RESULT),
            task.state_load_mode(),
            task.disposition(),
            task.status(),
            cloudId,
            VectorFromProto(task.result_set_meta()),
            std::move(dqGraphs),
            task.dq_graph_index(),
            VectorFromProto(task.created_topic_consumers()),
            task.automatic(),
            task.query_name(),
            NProtoInterop::CastFromProto(task.deadline()),
            ClientCounters,
            createdAt,
            TenantName,
            task.result_limit(),
            NProtoInterop::CastFromProto(task.execution_limit()),
            NProtoInterop::CastFromProto(task.request_started_at())
            );

        auto runActorId = Register(CreateRunActor(SelfId(), queryCounters, std::move(params)));

        RunActorMap[runActorId] = queryId;
        if (!task.automatic()) {
            CountersMap[queryId] = { rootCountersParent, publicCountersParent, runActorId };
        }
    }

    STRICT_STFUNC(StateFunc,
        hFunc(NActors::TEvents::TEvWakeup, HandleWakeup)
        HFunc(NActors::TEvents::TEvUndelivered, OnUndelivered)
        hFunc(TEvInternalService::TEvGetTaskResponse, Handle)
        hFunc(NActors::TEvents::TEvPoisonTaken, HandlePoisonTaken)
        hFunc(TEvPrivate::TEvCleanupCounters, HandleCleanupCounters)
    );

    NYq::TYqSharedResources::TPtr YqSharedResources;
    NKikimr::TYdbCredentialsProviderFactory CredentialsProviderFactory;
    NYq::NConfig::TCommonConfig CommonConfig;
    NYq::NConfig::TCheckpointCoordinatorConfig CheckpointCoordinatorConfig;
    NYq::NConfig::TPrivateApiConfig PrivateApiConfig;
    NYq::NConfig::TGatewaysConfig GatewaysConfig;
    NYq::NConfig::TPingerConfig PingerConfig;

    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry;
    TIntrusivePtr<ITimeProvider> TimeProvider;
    TIntrusivePtr<IRandomProvider> RandomProvider;
    NKikimr::NMiniKQL::TComputationNodeFactory DqCompFactory;
    TIntrusivePtr<IDqGateway> DqGateway;
    ::NYql::NCommon::TServiceCounters ServiceCounters;

    IModuleResolver::TPtr ModuleResolver;

    bool HasRunningRequest = false;
    const TDuration PendingFetchPeriod = TDuration::Seconds(1);

    TActorId DatabaseResolver;

    ISecuredServiceAccountCredentialsFactory::TPtr CredentialsFactory;
    const IHTTPGateway::TPtr S3Gateway;
    const ::NPq::NConfigurationManager::IConnections::TPtr PqCmConnections;

    const TString FetcherGuid;
    uint64_t FetcherGeneration = 0;
    const ::NMonitoring::TDynamicCounterPtr ClientCounters;

    TMaybe<NYql::NLog::TScopedBackend<NYql::NDq::TYqlLogScope>> LogScope;

    struct TQueryCountersInfo {
        ::NMonitoring::TDynamicCounterPtr RootCountersParent;
        ::NMonitoring::TDynamicCounterPtr PublicCountersParent;
        TActorId RunActorId;
    };

    TMap<TString, TQueryCountersInfo> CountersMap;
    TMap<TActorId, TString> RunActorMap;
    TString TenantName;
    TActorId InternalServiceId;
};


NActors::IActor* CreatePendingFetcher(
    const NYq::TYqSharedResources::TPtr& yqSharedResources,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const ::NYq::NConfig::TCommonConfig& commonConfig,
    const ::NYq::NConfig::TCheckpointCoordinatorConfig& checkpointCoordinatorConfig,
    const ::NYq::NConfig::TPrivateApiConfig& privateApiConfig,
    const ::NYq::NConfig::TGatewaysConfig& gatewaysConfig,
    const ::NYq::NConfig::TPingerConfig& pingerConfig,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    TIntrusivePtr<ITimeProvider> timeProvider,
    TIntrusivePtr<IRandomProvider> randomProvider,
    NKikimr::NMiniKQL::TComputationNodeFactory dqCompFactory,
    const ::NYql::NCommon::TServiceCounters& serviceCounters,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    IHTTPGateway::TPtr s3Gateway,
    ::NPq::NConfigurationManager::IConnections::TPtr pqCmConnections,
    const ::NMonitoring::TDynamicCounterPtr& clientCounters,
    const TString& tenantName)
{
    return new TPendingFetcher(
        yqSharedResources,
        credentialsProviderFactory,
        commonConfig,
        checkpointCoordinatorConfig,
        privateApiConfig,
        gatewaysConfig,
        pingerConfig,
        functionRegistry,
        timeProvider,
        randomProvider,
        dqCompFactory,
        serviceCounters,
        credentialsFactory,
        s3Gateway,
        std::move(pqCmConnections),
        clientCounters,
        tenantName);
}

TActorId MakePendingFetcherId(ui32 nodeId) {
    constexpr TStringBuf name = "YQLFETCHER";
    return NActors::TActorId(nodeId, name);
}

} /* NYq */

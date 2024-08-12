#include "proxy.h"
#include "nodes_manager.h"

#include "database_resolver.h"

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/protobuf/interop/cast.h>

#include <ydb/core/mon/mon.h>
#include <ydb/library/services/services.pb.h>

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
#include <ydb/library/yql/providers/generic/connector/libcpp/client.h>
#include <ydb/library/yql/dq/integration/transform/yql_dq_task_transform.h>
#include <ydb/library/yql/providers/ydb/provider/yql_ydb_provider.h>
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

#include <ydb/core/fq/libs/common/compression.h>
#include <ydb/core/fq/libs/common/entity_id.h>
#include <ydb/core/fq/libs/common/util.h>
#include <ydb/core/fq/libs/compute/common/config.h>
#include <ydb/core/fq/libs/compute/ydb/actors_factory.h>
#include <ydb/core/fq/libs/compute/ydb/ydb_run_actor.h>
#include <ydb/core/fq/libs/config/protos/fq_config.pb.h>
#include <ydb/core/fq/libs/config/protos/pinger.pb.h>
#include <ydb/core/fq/libs/control_plane_storage/control_plane_storage.h>
#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/private_client/internal_service.h>

#include <ydb/library/actors/core/log.h>

#include <ydb/library/security/util.h>

#include <util/generic/deque.h>
#include <util/generic/guid.h>
#include <util/system/hostname.h>

#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FQ_PENDING_FETCHER, stream)
#define LOG_W(stream) LOG_WARN_S (*TlsActivationContext, NKikimrServices::FQ_PENDING_FETCHER, stream)
#define LOG_I(stream) LOG_INFO_S (*TlsActivationContext, NKikimrServices::FQ_PENDING_FETCHER, stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::FQ_PENDING_FETCHER, stream)
#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::FQ_PENDING_FETCHER, stream)

namespace NFq {

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

constexpr auto CLEANUP_PERIOD = TDuration::Seconds(60);

} // namespace

class TPendingFetcher : public NActors::TActorBootstrapped<TPendingFetcher> {
    struct TRequestCounters {
        const TString Name;

        ::NMonitoring::TDynamicCounterPtr Counters;
        ::NMonitoring::TDynamicCounters::TCounterPtr InFly;
        ::NMonitoring::TDynamicCounters::TCounterPtr Ok;
        ::NMonitoring::TDynamicCounters::TCounterPtr Error;
        ::NMonitoring::TDynamicCounters::TCounterPtr Retry;
        ::NMonitoring::THistogramPtr LatencyMs;

        explicit TRequestCounters(const TString& name, const ::NMonitoring::TDynamicCounterPtr& counters = nullptr)
            : Name(name)
            , Counters(counters)
        { 
            Register();
        }

        void Register() {
            ::NMonitoring::TDynamicCounterPtr subgroup = Counters->GetSubgroup("request", Name);
            InFly = subgroup->GetCounter("InFly", false);
            Ok = subgroup->GetCounter("Ok", true);
            Error = subgroup->GetCounter("Error", true);
            Retry = subgroup->GetCounter("Retry", true);
            LatencyMs = subgroup->GetHistogram("LatencyMs", GetLatencyHistogramBuckets());
        }

    private:
        static ::NMonitoring::IHistogramCollectorPtr GetLatencyHistogramBuckets() {
            return ::NMonitoring::ExplicitHistogram({0, 1, 2, 5, 10, 20, 50, 100, 500, 1000, 2000, 5000, 10000, 30000, 50000, 500000});
        }
    };

public:
    TPendingFetcher(
        const NFq::TYqSharedResources::TPtr& yqSharedResources,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        const ::NFq::NConfig::TConfig& config,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        TIntrusivePtr<ITimeProvider> timeProvider,
        TIntrusivePtr<IRandomProvider> randomProvider,
        NKikimr::NMiniKQL::TComputationNodeFactory dqCompFactory,
        const ::NYql::NCommon::TServiceCounters& serviceCounters,
        ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
        IHTTPGateway::TPtr s3Gateway,
        NYql::NConnector::IClient::TPtr connectorClient,
        ::NPq::NConfigurationManager::IConnections::TPtr pqCmConnections,
        const ::NMonitoring::TDynamicCounterPtr& clientCounters,
        const TString& tenantName,
        NActors::TMon* monitoring,
        std::shared_ptr<NYql::NDq::IS3ActorsFactory> s3ActorsFactory
        )
        : YqSharedResources(yqSharedResources)
        , CredentialsProviderFactory(credentialsProviderFactory)
        , Config(config)
        , FunctionRegistry(functionRegistry)
        , TimeProvider(timeProvider)
        , RandomProvider(randomProvider)
        , DqCompFactory(dqCompFactory)
        , ServiceCounters(serviceCounters, "pending_fetcher")
        , GetTaskCounters("GetTask", ServiceCounters.Counters)
        , FailedStatusCodeCounters(MakeIntrusive<TStatusCodeByScopeCounters>("IntermediateFailedStatusCode", ServiceCounters.RootCounters->GetSubgroup("component", "QueryDiagnostic")))
        , CredentialsFactory(credentialsFactory)
        , S3Gateway(s3Gateway)
        , ConnectorClient(connectorClient)
        , PqCmConnections(std::move(pqCmConnections))
        , FetcherGuid(CreateGuidAsString())
        , ClientCounters(clientCounters)
        , TenantName(tenantName)
        , InternalServiceId(MakeInternalServiceActorId())
        , Monitoring(monitoring)
        , ComputeConfig(config.GetCompute())
        , S3ActorsFactory(std::move(s3ActorsFactory))
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
        if (Monitoring) {
            Monitoring->RegisterActorPage(Monitoring->RegisterIndexPage("fq_diag", "Federated Query diagnostics"),
                "fetcher", "Pending Fetcher", false, TActivationContext::ActorSystem(), SelfId());
            Monitoring->RegisterActorPage(Monitoring->RegisterIndexPage("fq_diag", "Federated Query diagnostics"),
                "local_worker_manager", "Local Worker Manager", false, TActivationContext::ActorSystem(), NYql::NDqs::MakeWorkerManagerActorID(SelfId().NodeId()));
        }

        Become(&TPendingFetcher::StateFunc);
        DatabaseResolver = Register(CreateDatabaseResolver(MakeYqlAnalyticsHttpProxyId(), CredentialsFactory));
        Send(SelfId(), new NActors::TEvents::TEvWakeup());

        LogScope.ConstructInPlace(NActors::TActivationContext::ActorSystem(), NKikimrServices::YQL_PROXY, FetcherGuid);
    }

private:
    void OnUndelivered(NActors::TEvents::TEvUndelivered::TPtr&) {
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
        GetTaskCounters.LatencyMs->Collect((TInstant::Now() - StartGetTaskTime).MilliSeconds());
        GetTaskCounters.InFly->Dec();
        if (!ev->Get()->Status.IsSuccess()) {
            GetTaskCounters.Error->Inc();
            LOG_E("Error with GetTask: "<< ev->Get()->Status.GetIssues().ToString());
            return;
        }
        GetTaskCounters.Ok->Inc();

        const auto& res = ev->Get()->Result;

        LOG_T("Tasks count: " << res.tasks().size());
        if (!res.tasks().empty()) {
            ProcessTask(res);
            HasRunningRequest = true;
            GetPendingTask();
        }
    }

    void Handle(NMon::TEvHttpInfo::TPtr& ev) {
        const auto& params = ev->Get()->Request.GetParams();
        if (params.Has("query")) {
            TString queryId = params.Get("query");

            auto it = CountersMap.find(queryId);
            if (it != CountersMap.end()) {
                auto runActorId = it->second.RunActorId;
                if (RunActorMap.find(runActorId) != RunActorMap.end()) {
                    TActivationContext::Send(ev->Forward(runActorId));
                    return;
                }
            }
        }

        TStringStream html;
        html << "<table class='table simple-table1 table-hover table-condensed'>";
        html << "<thead><tr>";
        html << "<th>Query ID</th>";
        html << "<th>Query Name</th>";
        html << "</tr></thead><tbody>";
        for (const auto& pr : RunActorMap) {
            const auto& runActorInfo = pr.second;
            html << "<tr>";
            html << "<td><a href='fetcher?query=" << runActorInfo.QueryId << "'>" << runActorInfo.QueryId << "</a></td>";
            html << "<td>" << runActorInfo.QueryName << "</td>";
            html << "</tr>";
        }
        html << "</tbody></table>";

        Send(ev->Sender, new NMon::TEvHttpInfoRes(html.Str()));
    }

    void HandlePoisonTaken(NActors::TEvents::TEvPoisonTaken::TPtr& ev) {
        auto runActorId = ev->Sender;

        auto itA = RunActorMap.find(runActorId);
        if (itA == RunActorMap.end()) {
            LOG_W("Unknown RunActor " << runActorId << " destroyed");
            return;
        }
        auto queryId = itA->second.QueryId;
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
        GetTaskCounters.InFly->Inc();
        StartGetTaskTime = TInstant::Now();
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

    NActors::NLog::EPriority GetYqlLogLevel() const {
        ui8 currentLevel = TlsActivationContext->LoggerSettings()->GetComponentSettings(NKikimrServices::YQL_PROXY).Raw.X.Level;
        return static_cast<NActors::NLog::EPriority>(currentLevel);
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

        NDq::SetYqlLogLevels(GetYqlLogLevel());

        const TString folderId = NYdb::NFq::TScope(task.scope()).ParseFolder();
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

        Fq::Private::TaskResources resources(task.resources());
        if (task.created_topic_consumers_size()) {
            // todo: remove after migration
            *resources.mutable_topic_consumers() = task.created_topic_consumers();
        }

        NFq::NConfig::TYdbStorageConfig computeConnection = ComputeConfig.GetExecutionConnection(task.scope());
        computeConnection.set_endpoint(task.compute_connection().endpoint());
        computeConnection.set_database(task.compute_connection().database());
        computeConnection.set_usessl(task.compute_connection().usessl());

        TRunActorParams params(
            YqSharedResources, CredentialsProviderFactory, S3Gateway, ConnectorClient,
            FunctionRegistry, RandomProvider,
            ModuleResolver, ModuleResolver->GetNextUniqueId(),
            DqCompFactory, PqCmConnections,
            Config,
            task.text(), task.scope(), task.user_token(),
            DatabaseResolver, queryId,
            task.user_id(), GetOwnerId(), task.generation(),
            VectorFromProto(task.connection()),
            VectorFromProto(task.binding()),
            CredentialsFactory,
            serviceAccounts,
            task.query_type(),
            task.query_syntax(),
            task.execute_mode(),
            GetEntityIdAsString(Config.GetCommon().GetIdsPrefix(), EEntityType::RESULT),
            task.state_load_mode(),
            task.disposition(),
            task.status(),
            cloudId,
            VectorFromProto(task.result_set_meta()),
            std::move(dqGraphs),
            task.dq_graph_index(),
            task.automatic(),
            task.query_name(),
            NProtoInterop::CastFromProto(task.deadline()),
            ClientCounters,
            createdAt,
            TenantName,
            task.result_limit(),
            NProtoInterop::CastFromProto(task.execution_limit()),
            NProtoInterop::CastFromProto(task.request_started_at()),
            task.restart_count(),
            task.job_id().value(),
            resources,
            task.execution_id(),
            task.operation_id(),
            computeConnection,
            NProtoInterop::CastFromProto(task.result_ttl()),
            std::map<TString, Ydb::TypedValue>(task.parameters().begin(), task.parameters().end()),
            S3ActorsFactory,
            ComputeConfig.GetWorkloadManagerConfig(task.scope())
            );

        auto runActorId =
            ComputeConfig.GetComputeType(task.query_type(), task.scope()) == NConfig::EComputeType::YDB
                ? Register(CreateYdbRunActor(std::move(params), queryCounters))
                : Register(CreateRunActor(SelfId(), queryCounters, std::move(params)));

        RunActorMap[runActorId] = TRunActorInfo { .QueryId = queryId, .QueryName = task.query_name() };
        if (!task.automatic()) {
            CountersMap[queryId] = { rootCountersParent, publicCountersParent, runActorId };
        }
    }

    NActors::IActor* CreateYdbRunActor(TRunActorParams&& params, const ::NYql::NCommon::TServiceCounters& queryCounters) const {
        auto actorFactory = CreateActorFactory(params, queryCounters, FailedStatusCodeCounters);
        return ::NFq::CreateYdbRunActor(SelfId(), queryCounters, std::move(params), actorFactory);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(NActors::TEvents::TEvWakeup, HandleWakeup)
        hFunc(NActors::TEvents::TEvUndelivered, OnUndelivered)
        hFunc(TEvInternalService::TEvGetTaskResponse, Handle)
        hFunc(NActors::TEvents::TEvPoisonTaken, HandlePoisonTaken)
        hFunc(TEvPrivate::TEvCleanupCounters, HandleCleanupCounters)
        hFunc(NMon::TEvHttpInfo, Handle)
    );

    NFq::TYqSharedResources::TPtr YqSharedResources;
    NKikimr::TYdbCredentialsProviderFactory CredentialsProviderFactory;
    NFq::NConfig::TConfig Config;

    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry;
    TIntrusivePtr<ITimeProvider> TimeProvider;
    TIntrusivePtr<IRandomProvider> RandomProvider;
    NKikimr::NMiniKQL::TComputationNodeFactory DqCompFactory;
    TIntrusivePtr<IDqGateway> DqGateway;
    ::NYql::NCommon::TServiceCounters ServiceCounters;
    TRequestCounters GetTaskCounters;
    TInstant StartGetTaskTime;
    NFq::TStatusCodeByScopeCounters::TPtr FailedStatusCodeCounters;

    IModuleResolver::TPtr ModuleResolver;

    bool HasRunningRequest = false;
    const TDuration PendingFetchPeriod = TDuration::Seconds(1);

    TActorId DatabaseResolver;

    ISecuredServiceAccountCredentialsFactory::TPtr CredentialsFactory;
    const IHTTPGateway::TPtr S3Gateway;
    const NYql::NConnector::IClient::TPtr ConnectorClient;
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

    struct TRunActorInfo {
        TString QueryId;
        TString QueryName;
    };

    TMap<TString, TQueryCountersInfo> CountersMap;
    TMap<TActorId, TRunActorInfo> RunActorMap;
    TString TenantName;
    TActorId InternalServiceId;
    NActors::TMon* Monitoring;
    TComputeConfig ComputeConfig;
    std::shared_ptr<NYql::NDq::IS3ActorsFactory> S3ActorsFactory;
};


NActors::IActor* CreatePendingFetcher(
    const NFq::TYqSharedResources::TPtr& yqSharedResources,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const ::NFq::NConfig::TConfig& config,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    TIntrusivePtr<ITimeProvider> timeProvider,
    TIntrusivePtr<IRandomProvider> randomProvider,
    NKikimr::NMiniKQL::TComputationNodeFactory dqCompFactory,
    const ::NYql::NCommon::TServiceCounters& serviceCounters,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    IHTTPGateway::TPtr s3Gateway,
    NYql::NConnector::IClient::TPtr connectorClient,
    ::NPq::NConfigurationManager::IConnections::TPtr pqCmConnections,
    const ::NMonitoring::TDynamicCounterPtr& clientCounters,
    const TString& tenantName,
    NActors::TMon* monitoring,
    std::shared_ptr<NYql::NDq::IS3ActorsFactory> s3ActorsFactory)
{
    return new TPendingFetcher(
        yqSharedResources,
        credentialsProviderFactory,
        config,
        functionRegistry,
        timeProvider,
        randomProvider,
        dqCompFactory,
        serviceCounters,
        credentialsFactory,
        s3Gateway,
        connectorClient,
        std::move(pqCmConnections),
        clientCounters,
        tenantName,
        monitoring,
        std::move(s3ActorsFactory));
}

TActorId MakePendingFetcherId(ui32 nodeId) {
    constexpr TStringBuf name = "YQLFETCHER";
    return NActors::TActorId(nodeId, name);
}

} /* NFq */

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

#include <ydb/core/yq/libs/common/entity_id.h> 
#include <ydb/core/yq/libs/events/events.h> 

#include <ydb/core/yq/libs/control_plane_storage/control_plane_storage.h> 
#include <ydb/core/yq/libs/control_plane_storage/events/events.h> 
#include <ydb/core/yq/libs/private_client/private_client.h> 

#include <library/cpp/actors/core/log.h>

#include <ydb/library/security/util.h> 

#include <util/generic/deque.h>
#include <util/generic/guid.h>
#include <util/system/hostname.h>

#define LOG_E(stream) \
    LOG_ERROR_S(*TlsActivationContext, NKikimrServices::YQL_PROXY, "Fetcher: " << stream)
#define LOG_I(stream) \
    LOG_INFO_S(*TlsActivationContext, NKikimrServices::YQL_PROXY, "Fetcher: " << stream)
#define LOG_D(stream) \
    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::YQL_PROXY, "Fetcher: " << stream)

namespace NYq {

using namespace NActors;

namespace {

struct TEvGetTaskInternalResponse : public NActors::TEventLocal<TEvGetTaskInternalResponse, NActors::TEvents::TSystem::Completed> {
    bool Success = false;
    const TIssues Issues;
    const Yq::Private::GetTaskResult Result;

    TEvGetTaskInternalResponse(
        bool success,
        const TIssues& issues,
        const Yq::Private::GetTaskResult& result)
        : Success(success)
        , Issues(issues)
        , Result(result)
    { }
};

template <class TElement>
TVector<TElement> VectorFromProto(const ::google::protobuf::RepeatedPtrField<TElement>& field) {
    return { field.begin(), field.end() };
}

} // namespace

class TYqlPendingFetcher : public NActors::TActorBootstrapped<TYqlPendingFetcher> {
public:
    TYqlPendingFetcher(
        const NYq::TYqSharedResources::TPtr& yqSharedResources,
        const ::NYq::NConfig::TCommonConfig& commonConfig,
        const ::NYq::NConfig::TCheckpointCoordinatorConfig& checkpointCoordinatorConfig,
        const ::NYq::NConfig::TPrivateApiConfig& privateApiConfig,
        const ::NYq::NConfig::TGatewaysConfig& gatewaysConfig,
        const ::NYq::NConfig::TPingerConfig& pingerConfig,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        TIntrusivePtr<ITimeProvider> timeProvider,
        TIntrusivePtr<IRandomProvider> randomProvider,
        NKikimr::NMiniKQL::TComputationNodeFactory dqCompFactory,
        const ::NYq::NCommon::TServiceCounters& serviceCounters,
        ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
        IHTTPGateway::TPtr s3Gateway,
        ::NPq::NConfigurationManager::IConnections::TPtr pqCmConnections,
        const NMonitoring::TDynamicCounterPtr& clientCounters
        )
        : YqSharedResources(yqSharedResources)
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
        , Guid(CreateGuidAsString())
        , ClientCounters(clientCounters)
        , Client(
            YqSharedResources->YdbDriver,
            NYdb::TCommonClientSettings()
                .DiscoveryEndpoint(PrivateApiConfig.GetTaskServiceEndpoint())
                .Database(PrivateApiConfig.GetTaskServiceDatabase() ? PrivateApiConfig.GetTaskServiceDatabase() : TMaybe<TString>()),
            ClientCounters)
    {
        Y_ENSURE(GetYqlDefaultModuleResolverWithContext(ModuleResolver));
    }

    static constexpr char ActorName[] = "YQ_PENDING_FETCHER";

    void PassAway() final {
        LOG_D("Stop Fetcher");
        Send(DatabaseResolver, new NActors::TEvents::TEvPoison());
        NActors::IActor::PassAway();
    }

    void Bootstrap(const TActorContext& ctx) {
        Become(&TYqlPendingFetcher::StateFunc);

        Y_UNUSED(ctx);

        DatabaseResolver = Register(CreateDatabaseResolver(MakeYqlAnalyticsHttpProxyId(), CredentialsFactory));
        Send(SelfId(), new NActors::TEvents::TEvWakeup());

        LOG_I("STARTED");
        LogScope.ConstructInPlace(NActors::TActivationContext::ActorSystem(), NKikimrServices::YQL_PROXY, Guid);
    }

private:
    void OnUndelivered(NActors::TEvents::TEvUndelivered::TPtr&, const NActors::TActorContext&) {
        LOG_E("TYqlPendingFetcher::OnUndelivered");

        HasRunningRequest = false;
    }

    void HandleWakeup(NActors::TEvents::TEvWakeup::TPtr&, const NActors::TActorContext&) {
        Schedule(PendingFetchPeriod, new NActors::TEvents::TEvWakeup());

        if (!HasRunningRequest) {
            HasRunningRequest = true;
            GetPendingTask();
        }
    }

    void HandleResponse(TEvGetTaskInternalResponse::TPtr& ev) {
        HasRunningRequest = false;
        LOG_D("Got GetTask response from PrivateApi");
        if (!ev->Get()->Success) {
            LOG_E("Error with GetTask: "<< ev->Get()->Issues.ToString());
            return;
        }

        const auto& res = ev->Get()->Result;

        LOG_D("Tasks count: " << res.tasks().size());
        if (!res.tasks().empty()) {
            ProcessTask(res);
            HasRunningRequest = true;
            GetPendingTask();
        }
    }

    void GetPendingTask() {
        LOG_D("Request Private::GetTask" << ", Owner: " << Guid << ", Host: " << HostName());
        Yq::Private::GetTaskRequest request;
        request.set_owner_id(Guid);
        request.set_host(HostName());
        const auto actorSystem = NActors::TActivationContext::ActorSystem();
        const auto selfId = SelfId();
        Client
            .GetTask(std::move(request))
            .Subscribe([actorSystem, selfId](const NThreading::TFuture<TGetTaskResult>& future) {
                const auto& wrappedResult = future.GetValue();
                if (wrappedResult.IsResultSet()) {
                    actorSystem->Send(selfId, new TEvGetTaskInternalResponse(
                        wrappedResult.IsSuccess(), wrappedResult.GetIssues(), wrappedResult.GetResult())
                    );
                } else {
                    actorSystem->Send(selfId, new TEvGetTaskInternalResponse(
                        false, TIssues{{TIssue{"grpc private api result is not set for get task call"}}}, Yq::Private::GetTaskResult{})
                    );
                }
            });
    }

    void ProcessTask(const Yq::Private::GetTaskResult& result) {
        for (const auto& task : result.tasks()) {
            RunTask(task);
        }

    }

    void RunTask(const Yq::Private::GetTaskResult::Task& task) {
        LOG_D("NewTask:"
              << " Scope: " << task.scope()
              << " Id: " << task.query_id().value()
              << " UserId: " << task.user_id()
              << " AuthToken: " << NKikimr::MaskTicket(task.user_token()));

        THashMap<TString, TString> serviceAccounts;
        for (const auto& identity : task.service_accounts()) {
            serviceAccounts[identity.value()] = identity.signature();
        }

        TRunActorParams params(
            YqSharedResources->YdbDriver, S3Gateway,
            FunctionRegistry, RandomProvider,
            ModuleResolver, ModuleResolver->GetNextUniqueId(),
            DqCompFactory, PqCmConnections,
            CommonConfig, CheckpointCoordinatorConfig,
            PrivateApiConfig, GatewaysConfig, PingerConfig,
            task.text(), task.scope(), task.user_token(),
            DatabaseResolver, task.query_id().value(),
            task.user_id(), Guid, task.generation(),
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
            task.sensor_labels().at("cloud_id"),
            VectorFromProto(task.result_set_meta()),
            VectorFromProto(task.dq_graph()),
            task.dq_graph_index(),
            VectorFromProto(task.created_topic_consumers()),
            task.automatic(),
            task.query_name(),
            NProtoInterop::CastFromProto(task.deadline()),
            ClientCounters);

        NDq::SetYqlLogLevels(NActors::NLog::PRI_TRACE);
        Register(CreateRunActor(ServiceCounters, std::move(params)));
    }

    STRICT_STFUNC(
        StateFunc,

        HFunc(NActors::TEvents::TEvWakeup, HandleWakeup)
        HFunc(NActors::TEvents::TEvUndelivered, OnUndelivered)
        hFunc(TEvGetTaskInternalResponse, HandleResponse)
        );

    NYq::TYqSharedResources::TPtr YqSharedResources;
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
    ::NYq::NCommon::TServiceCounters ServiceCounters;

    IModuleResolver::TPtr ModuleResolver;

    bool HasRunningRequest = false;
    const TDuration PendingFetchPeriod = TDuration::Seconds(1);

    TActorId DatabaseResolver;

    ISecuredServiceAccountCredentialsFactory::TPtr CredentialsFactory;
    const IHTTPGateway::TPtr S3Gateway;
    const ::NPq::NConfigurationManager::IConnections::TPtr PqCmConnections;

    const TString Guid; //OwnerId
    const NMonitoring::TDynamicCounterPtr ClientCounters;
    TPrivateClient Client;

    TMaybe<NYql::NLog::TScopedBackend<NYql::NDq::TYqlLogScope>> LogScope;
};


NActors::IActor* CreatePendingFetcher(
    const NYq::TYqSharedResources::TPtr& yqSharedResources,
    const ::NYq::NConfig::TCommonConfig& commonConfig,
    const ::NYq::NConfig::TCheckpointCoordinatorConfig& checkpointCoordinatorConfig,
    const ::NYq::NConfig::TPrivateApiConfig& privateApiConfig,
    const ::NYq::NConfig::TGatewaysConfig& gatewaysConfig,
    const ::NYq::NConfig::TPingerConfig& pingerConfig,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    TIntrusivePtr<ITimeProvider> timeProvider,
    TIntrusivePtr<IRandomProvider> randomProvider,
    NKikimr::NMiniKQL::TComputationNodeFactory dqCompFactory,
    const ::NYq::NCommon::TServiceCounters& serviceCounters,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    IHTTPGateway::TPtr s3Gateway,
    ::NPq::NConfigurationManager::IConnections::TPtr pqCmConnections,
    const NMonitoring::TDynamicCounterPtr& clientCounters)
{
    return new TYqlPendingFetcher(
        yqSharedResources,
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
        clientCounters);
}

TActorId MakeYqlAnalyticsFetcherId(ui32 nodeId) {
    constexpr TStringBuf name = "YQLFETCHER";
    return NActors::TActorId(nodeId, name);
}

} /* NYq */

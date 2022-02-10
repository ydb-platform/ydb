#include "init.h"

#include <ydb/core/yq/libs/control_plane_storage/control_plane_storage.h>
#include <ydb/core/yq/libs/test_connection/test_connection.h>

#include <ydb/core/yq/libs/audit/yq_audit_service.h>
#include <ydb/core/yq/libs/common/service_counters.h>
#include <ydb/core/yq/libs/control_plane_proxy/control_plane_proxy.h>
#include <ydb/core/yq/libs/shared_resources/shared_resources.h>
#include <ydb/core/yq/libs/checkpoint_storage/storage_service.h>
#include <ydb/library/folder_service/folder_service.h>

#include <library/cpp/actors/http/http_proxy.h>
#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <ydb/library/yql/dq/actors/compute/dq_checkpoints.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_io_actors_factory.h>
#include <ydb/library/yql/dq/comp_nodes/yql_common_dq_factory.h>
#include <ydb/library/yql/dq/comp_nodes/yql_common_dq_transform.h>
#include <ydb/library/yql/utils/actor_log/log.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/providers/common/comp_nodes/yql_factory.h>
#include <ydb/library/yql/providers/dq/task_runner/tasks_runner_local.h>
#include <ydb/library/yql/providers/dq/worker_manager/local_worker_manager.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_source_factory.h>
#include <ydb/library/yql/providers/s3/proto/retry_config.pb.h>
#include <ydb/library/yql/providers/clickhouse/actors/yql_ch_source_factory.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_read_actor.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_write_actor.h>
#include <ydb/library/yql/providers/solomon/async_io/dq_solomon_write_actor.h>
#include <ydb/library/yql/providers/ydb/actors/yql_ydb_source_factory.h>
#include <ydb/library/yql/providers/ydb/comp_nodes/yql_ydb_factory.h>
#include <ydb/library/yql/providers/ydb/comp_nodes/yql_ydb_dq_transform.h>
#include <ydb/library/yql/providers/ydb/actors/yql_ydb_source_factory.h>

#include <util/stream/file.h>
#include <util/system/hostname.h>

namespace {

std::tuple<TString, TString> GetLocalAddress(const TString* overrideHostname = nullptr) {
    constexpr auto MaxLocalHostNameLength = 4096;
    std::array<char, MaxLocalHostNameLength> buffer;
    buffer.fill(0);
    TString hostName;
    TString localAddress;

    int result = gethostname(buffer.data(), buffer.size() - 1);
    if (result != 0) {
        Cerr << "gethostname failed " << strerror(errno) << Endl;
        return std::make_tuple(hostName, localAddress);
    }

    if (overrideHostname) {
        memcpy(&buffer[0], overrideHostname->c_str(), Min<int>(
            overrideHostname->size()+1, buffer.size()-1
        ));
    }

    hostName = &buffer[0];

    addrinfo request;
    memset(&request, 0, sizeof(request));
    request.ai_family = AF_INET6;
    request.ai_socktype = SOCK_STREAM;

    addrinfo* response = nullptr;
    result = getaddrinfo(buffer.data(), nullptr, &request, &response);
    if (result != 0) {
        Cerr << "getaddrinfo failed " << gai_strerror(result) << Endl;
        return std::make_tuple(hostName, localAddress);
    }

    std::unique_ptr<addrinfo, void (*)(addrinfo*)> holder(response, &freeaddrinfo);

    if (!response->ai_addr) {
        Cerr << "getaddrinfo failed: no ai_addr" << Endl;
        return std::make_tuple(hostName, localAddress);
    }

    auto* sa = response->ai_addr;
    Y_VERIFY(sa->sa_family == AF_INET6);
    inet_ntop(AF_INET6, &(((struct sockaddr_in6*)sa)->sin6_addr),
                &buffer[0], buffer.size() - 1);

    localAddress = &buffer[0];

    return std::make_tuple(hostName, localAddress);
}

}

namespace NYq {

using namespace NKikimr;

void Init(
    const NYq::NConfig::TConfig& protoConfig,
    ui32 nodeId,
    const TActorRegistrator& actorRegistrator,
    const TAppData* appData,
    const TString& tenant,
    ::NPq::NConfigurationManager::IConnections::TPtr pqCmConnections,
    const IYqSharedResources::TPtr& iyqSharedResources,
    const std::function<IActor*(const NKikimrProto::NFolderService::TFolderServiceConfig& authConfig)>& folderServiceFactory,
    const std::function<IActor*(const NYq::NConfig::TAuditConfig& auditConfig)>& auditServiceFactory,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const ui32& icPort
    )
{
    Y_VERIFY(iyqSharedResources, "No YQ shared resources created");
    TYqSharedResources::TPtr yqSharedResources = TYqSharedResources::Cast(iyqSharedResources);
    const auto clientCounters = appData->Counters->GetSubgroup("counters", "yq")->GetSubgroup("subsystem", "ClientMetrics");

    if (protoConfig.GetControlPlaneStorage().GetEnabled()) {
        auto controlPlaneStorage = protoConfig.GetControlPlaneStorage().GetUseInMemory()
            ? NYq::CreateInMemoryControlPlaneStorageServiceActor(protoConfig.GetControlPlaneStorage())
            : NYq::CreateYdbControlPlaneStorageServiceActor(
                protoConfig.GetControlPlaneStorage(),
                protoConfig.GetCommon(),
                appData->Counters->GetSubgroup("counters", "yq")->GetSubgroup("subsystem", "ControlPlaneStorage"),
                yqSharedResources,
                credentialsProviderFactory);
        actorRegistrator(NYq::ControlPlaneStorageServiceActorId(), controlPlaneStorage);
    }

    if (protoConfig.GetTestConnection().GetEnabled()) {
        auto testConnection = NYq::CreateTestConnectionActor(
                protoConfig.GetTestConnection(),
                appData->Counters->GetSubgroup("counters", "yq")->GetSubgroup("subsystem", "TestConnection"));
        actorRegistrator(NYq::TestConnectionActorId(), testConnection);
    }

    if (protoConfig.GetControlPlaneProxy().GetEnabled()) {
        auto controlPlaneProxy = NYq::CreateControlPlaneProxyActor(protoConfig.GetControlPlaneProxy(),
            appData->Counters->GetSubgroup("counters", "yq")->GetSubgroup("subsystem", "ControlPlaneProxy"));
        actorRegistrator(NYq::ControlPlaneProxyActorId(), controlPlaneProxy);
    }

    if (protoConfig.GetAudit().GetEnabled()) {
        auto* auditSerive = auditServiceFactory(protoConfig.GetAudit());
        actorRegistrator(NYq::YqAuditServiceActorId(), auditSerive);
    }

    // if not enabled then stub
    {
        auto folderService = folderServiceFactory(protoConfig.GetFolderService());
        actorRegistrator(NKikimr::NFolderService::FolderServiceActorId(), folderService);
    }

    if (protoConfig.GetCheckpointCoordinator().GetEnabled()) {
        auto checkpointStorage = NYq::NewCheckpointStorageService(protoConfig.GetCheckpointCoordinator(), protoConfig.GetCommon(), credentialsProviderFactory);
        actorRegistrator(NYql::NDq::MakeCheckpointStorageID(), checkpointStorage.release());
    }

    auto yqCounters = appData->Counters->GetSubgroup("counters", "yq");
    auto workerManagerCounters = NYql::NDqs::TWorkerManagerCounters(yqCounters->GetSubgroup("subsystem", "worker_manager"));

    NKikimr::NMiniKQL::TComputationNodeFactory dqCompFactory = NKikimr::NMiniKQL::GetCompositeWithBuiltinFactory({
        NYql::GetCommonDqFactory(),
        NYql::GetDqYdbFactory(yqSharedResources->YdbDriver),
        NKikimr::NMiniKQL::GetYqlFactory()
    });

    NYql::TTaskTransformFactory dqTaskTransformFactory = NYql::CreateCompositeTaskTransformFactory({
        NYql::CreateCommonDqTaskTransformFactory(),
        NYql::CreateYdbDqTaskTransformFactory()
    });

    auto sourceActorFactory = MakeIntrusive<NYql::NDq::TDqSourceFactory>();
    auto sinkActorFactory = MakeIntrusive<NYql::NDq::TDqSinkFactory>();

    NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory;
    const auto httpGateway = NYql::IHTTPGateway::Make(
        &protoConfig.GetGateways().GetHttpGateway(),
        yqCounters->GetSubgroup("subcomponent", "http_gateway"));

    if (protoConfig.GetTokenAccessor().GetEnabled()) {
        credentialsFactory = NYql::CreateSecuredServiceAccountCredentialsOverTokenAccessorFactory(protoConfig.GetTokenAccessor().GetEndpoint(), protoConfig.GetTokenAccessor().GetUseSsl());
        RegisterDqPqReadActorFactory(*sourceActorFactory, yqSharedResources->YdbDriver, credentialsFactory);
        RegisterYdbReadActorFactory(*sourceActorFactory, yqSharedResources->YdbDriver, credentialsFactory);
        RegisterS3ReadActorFactory(*sourceActorFactory, credentialsFactory,
            httpGateway, std::make_shared<NYql::NS3::TRetryConfig>(protoConfig.GetReadActorsFactoryConfig().GetS3ReadActorFactoryConfig().GetRetryConfig()));
        RegisterClickHouseReadActorFactory(*sourceActorFactory, credentialsFactory, httpGateway);

        RegisterDqPqWriteActorFactory(*sinkActorFactory, yqSharedResources->YdbDriver, credentialsFactory);
        RegisterDQSolomonWriteActorFactory(*sinkActorFactory, credentialsFactory);
    } 

    ui64 mkqlInitialMemoryLimit = 8_GB; 
 
    if (protoConfig.GetResourceManager().GetEnabled()) {
        mkqlInitialMemoryLimit = protoConfig.GetResourceManager().GetMkqlInitialMemoryLimit(); 
        if (!mkqlInitialMemoryLimit) {
            mkqlInitialMemoryLimit = 8_GB;
        }
        ui64 mkqlTotalMemoryLimit = protoConfig.GetResourceManager().GetMkqlTotalMemoryLimit();
        ui64 mkqlAllocSize = protoConfig.GetResourceManager().GetMkqlAllocSize();
        if (!mkqlAllocSize) {
            mkqlAllocSize = 30_MB;
        }
        NYql::NDqs::TLocalWorkerManagerOptions lwmOptions;
        lwmOptions.Counters = workerManagerCounters;
        lwmOptions.Factory = NYql::NTaskRunnerProxy::CreateFactory(appData->FunctionRegistry, dqCompFactory, dqTaskTransformFactory, false);
        lwmOptions.SourceActorFactory = sourceActorFactory;
        lwmOptions.SinkActorFactory = sinkActorFactory;
        lwmOptions.TaskRunnerInvokerFactory = new NYql::NDqs::TTaskRunnerInvokerFactory();
        lwmOptions.MkqlInitialMemoryLimit = mkqlInitialMemoryLimit;
        lwmOptions.MkqlTotalMemoryLimit = mkqlTotalMemoryLimit;
        lwmOptions.MkqlMinAllocSize = mkqlAllocSize;
        auto resman = NYql::NDqs::CreateLocalWorkerManager(lwmOptions);

        actorRegistrator(NYql::NDqs::MakeWorkerManagerActorID(nodeId), resman);
    }

    ::NYq::NCommon::TServiceCounters serviceCounters(appData->Counters);

    if (protoConfig.GetNodesManager().GetEnabled()) {
        const auto localAddr =  GetLocalAddress(&HostName());
        auto nodesManager = CreateYqlNodesManager(
            workerManagerCounters,
            TAppData::TimeProvider,
            TAppData::RandomProvider,
            serviceCounters,
            protoConfig.GetPrivateApi(),
            yqSharedResources,
            icPort,
            std::get<1>(localAddr),
            tenant, 
            mkqlInitialMemoryLimit,
            clientCounters);

        actorRegistrator(MakeYqlNodesManagerId(), nodesManager);
    }

    auto httpProxy = NHttp::CreateHttpProxy(*NMonitoring::TMetricRegistry::Instance());
    actorRegistrator(MakeYqlAnalyticsHttpProxyId(), httpProxy);

    if (protoConfig.GetPendingFetcher().GetEnabled()) {
        auto fetcher = CreatePendingFetcher(
            yqSharedResources,
            protoConfig.GetCommon(),
            protoConfig.GetCheckpointCoordinator(),
            protoConfig.GetPrivateApi(),
            protoConfig.GetGateways(),
            protoConfig.GetPinger(),
            appData->FunctionRegistry,
            TAppData::TimeProvider,
            TAppData::RandomProvider,
            dqCompFactory,
            serviceCounters,
            credentialsFactory,
            httpGateway,
            std::move(pqCmConnections),
            clientCounters
            );

        actorRegistrator(MakeYqlAnalyticsFetcherId(nodeId), fetcher);
    }

    if (protoConfig.GetPrivateProxy().GetEnabled()) {
        auto proxyPrivate = CreateYqlAnalyticsPrivateProxy(
            TAppData::TimeProvider,
            TAppData::RandomProvider,
            serviceCounters.Counters,
            protoConfig.GetTokenAccessor());

        actorRegistrator(MakeYqPrivateProxyId(), proxyPrivate);
    }
}

IYqSharedResources::TPtr CreateYqSharedResources(
    const NYq::NConfig::TConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const NMonitoring::TDynamicCounterPtr& counters)
{
    return CreateYqSharedResourcesImpl(config, credentialsProviderFactory, counters);
}

} // NYq

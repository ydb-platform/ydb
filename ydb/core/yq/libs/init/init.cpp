#include "init.h"

#include <ydb/core/yq/libs/control_plane_storage/control_plane_storage.h>
#include <ydb/core/yq/libs/test_connection/test_connection.h>

#include <ydb/core/yq/libs/audit/yq_audit_service.h>
#include <ydb/core/yq/libs/checkpoint_storage/storage_service.h>
#include <ydb/core/yq/libs/control_plane_config/control_plane_config.h>
#include <ydb/core/yq/libs/control_plane_proxy/control_plane_proxy.h>
#include <ydb/core/yq/libs/health/health.h>
#include <ydb/core/yq/libs/checkpoint_storage/storage_service.h>
#include <ydb/core/yq/libs/private_client/internal_service.h>
#include <ydb/core/yq/libs/private_client/loopback_service.h>
#include <ydb/core/yq/libs/quota_manager/quota_manager.h>
#include <ydb/core/yq/libs/quota_manager/quota_proxy.h>
#include <ydb/core/yq/libs/rate_limiter/control_plane_service/rate_limiter_control_plane_service.h>
#include <ydb/core/yq/libs/rate_limiter/events/control_plane_events.h>
#include <ydb/core/yq/libs/rate_limiter/events/data_plane.h>
#include <ydb/core/yq/libs/rate_limiter/quoter_service/quoter_service.h>
#include <ydb/core/yq/libs/shared_resources/shared_resources.h>
#include <ydb/library/folder_service/folder_service.h>
#include <ydb/library/yql/providers/common/metrics/service_counters.h>

#include <library/cpp/actors/http/http_proxy.h>
#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <ydb/library/yql/dq/actors/compute/dq_checkpoints.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/dq/comp_nodes/yql_common_dq_factory.h>
#include <ydb/library/yql/dq/transform/yql_common_dq_transform.h>
#include <ydb/library/yql/utils/actor_log/log.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/providers/common/comp_nodes/yql_factory.h>
#include <ydb/library/yql/providers/dq/task_runner/tasks_runner_local.h>
#include <ydb/library/yql/providers/dq/worker_manager/local_worker_manager.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_sink_factory.h>
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
#include <ydb/library/yql/providers/common/http_gateway/yql_http_default_retry_policy.h>

#include <util/stream/file.h>
#include <util/system/hostname.h>

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
    const std::function<IActor*(const NYq::NConfig::TAuditConfig& auditConfig, const ::NMonitoring::TDynamicCounterPtr& counters)>& auditServiceFactory,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    ui32 icPort,
    const std::vector<NKikimr::NMiniKQL::TComputationNodeFactory>& additionalCompNodeFactories
    )
{
    Y_VERIFY(iyqSharedResources, "No YQ shared resources created");
    TYqSharedResources::TPtr yqSharedResources = TYqSharedResources::Cast(iyqSharedResources);

    auto yqCounters = appData->Counters->GetSubgroup("counters", "yq");
    const auto clientCounters = yqCounters->GetSubgroup("subsystem", "ClientMetrics");

    if (protoConfig.GetControlPlaneStorage().GetEnabled()) {
        auto controlPlaneStorage = protoConfig.GetControlPlaneStorage().GetUseInMemory()
            ? NYq::CreateInMemoryControlPlaneStorageServiceActor(protoConfig.GetControlPlaneStorage())
            : NYq::CreateYdbControlPlaneStorageServiceActor(
                protoConfig.GetControlPlaneStorage(),
                protoConfig.GetCommon(),
                yqCounters->GetSubgroup("subsystem", "ControlPlaneStorage"),
                yqSharedResources,
                credentialsProviderFactory,
                tenant);
        actorRegistrator(NYq::ControlPlaneStorageServiceActorId(), controlPlaneStorage);

        actorRegistrator(NYq::ControlPlaneConfigActorId(),
            CreateControlPlaneConfigActor(yqSharedResources, credentialsProviderFactory, protoConfig.GetControlPlaneStorage(),
                yqCounters->GetSubgroup("subsystem", "ControlPlaneConfig"))
        );
    }

    if (protoConfig.GetControlPlaneProxy().GetEnabled()) {
        auto controlPlaneProxy = NYq::CreateControlPlaneProxyActor(protoConfig.GetControlPlaneProxy(),
            yqCounters->GetSubgroup("subsystem", "ControlPlaneProxy"), protoConfig.GetQuotasManager().GetEnabled());
        actorRegistrator(NYq::ControlPlaneProxyActorId(), controlPlaneProxy);
    }

    if (protoConfig.GetRateLimiter().GetControlPlaneEnabled()) {
        Y_VERIFY(protoConfig.GetQuotasManager().GetEnabled()); // Rate limiter resources want to know CPU quota on creation
        NActors::IActor* rateLimiterService = NYq::CreateRateLimiterControlPlaneService(protoConfig.GetRateLimiter(), yqSharedResources, credentialsProviderFactory);
        actorRegistrator(NYq::RateLimiterControlPlaneServiceId(), rateLimiterService);
    }

    if (protoConfig.GetRateLimiter().GetDataPlaneEnabled()) {
        actorRegistrator(NYq::YqQuoterServiceActorId(), NYq::CreateQuoterService(protoConfig.GetRateLimiter(), yqSharedResources, credentialsProviderFactory));
    }

    if (protoConfig.GetAudit().GetEnabled()) {
        auto* auditSerive = auditServiceFactory(
            protoConfig.GetAudit(),
            yqCounters->GetSubgroup("subsystem", "audit"));
        actorRegistrator(NYq::YqAuditServiceActorId(), auditSerive);
    }

    // if not enabled then stub
    {
        auto folderService = folderServiceFactory(protoConfig.GetFolderService());
        actorRegistrator(NKikimr::NFolderService::FolderServiceActorId(), folderService);
    }

    if (protoConfig.GetCheckpointCoordinator().GetEnabled()) {
        auto checkpointStorage = NYq::NewCheckpointStorageService(protoConfig.GetCheckpointCoordinator(), protoConfig.GetCommon(), credentialsProviderFactory, yqSharedResources);
        actorRegistrator(NYql::NDq::MakeCheckpointStorageID(), checkpointStorage.release());
    }

    auto workerManagerCounters = NYql::NDqs::TWorkerManagerCounters(yqCounters->GetSubgroup("subsystem", "worker_manager"));

    TVector<NKikimr::NMiniKQL::TComputationNodeFactory> compNodeFactories = {
        NYql::GetCommonDqFactory(),
        NYql::GetDqYdbFactory(yqSharedResources->UserSpaceYdbDriver),
        NKikimr::NMiniKQL::GetYqlFactory()
    };

    compNodeFactories.insert(compNodeFactories.end(), additionalCompNodeFactories.begin(), additionalCompNodeFactories.end());
    NKikimr::NMiniKQL::TComputationNodeFactory dqCompFactory = NKikimr::NMiniKQL::GetCompositeWithBuiltinFactory(std::move(compNodeFactories));

    NYql::TTaskTransformFactory dqTaskTransformFactory = NYql::CreateCompositeTaskTransformFactory({
        NYql::CreateCommonDqTaskTransformFactory(),
        NYql::CreateYdbDqTaskTransformFactory()
    });

    auto asyncIoFactory = MakeIntrusive<NYql::NDq::TDqAsyncIoFactory>();

    NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory;
    const auto httpGateway = NYql::IHTTPGateway::Make(
        &protoConfig.GetGateways().GetHttpGateway(),
        yqCounters->GetSubgroup("subcomponent", "http_gateway"));

    if (protoConfig.GetTokenAccessor().GetEnabled()) {
        const auto& tokenAccessorConfig = protoConfig.GetTokenAccessor();

        TString caContent;
        if (const auto& path = tokenAccessorConfig.GetSslCaCert()) {
            caContent = TUnbufferedFileInput(path).ReadAll();
        }

        credentialsFactory = NYql::CreateSecuredServiceAccountCredentialsOverTokenAccessorFactory(tokenAccessorConfig.GetEndpoint(), tokenAccessorConfig.GetUseSsl(), caContent, tokenAccessorConfig.GetConnectionPoolSize());
    }

    if (protoConfig.GetPrivateApi().GetEnabled()) {
        const auto& s3readConfig = protoConfig.GetReadActorsFactoryConfig().GetS3ReadActorFactoryConfig();
        auto s3HttpRetryPolicy = NYql::GetHTTPDefaultRetryPolicy(TDuration::MilliSeconds(s3readConfig.GetRetryConfig().GetMaxRetryTimeMs())); // if MaxRetryTimeMs is not set, default http gateway will use the default one
        NYql::NDq::TS3ReadActorFactoryConfig readActorFactoryCfg;
        if (const ui64 rowsInBatch = s3readConfig.GetRowsInBatch()) {
            readActorFactoryCfg.RowsInBatch = rowsInBatch;
        }
        if (const ui64 maxInflight = s3readConfig.GetMaxInflight()) {
            readActorFactoryCfg.MaxInflight = maxInflight;
        }
        if (const ui64 dataInflight = s3readConfig.GetDataInflight()) {
            readActorFactoryCfg.DataInflight = dataInflight;
        }
        RegisterDqPqReadActorFactory(*asyncIoFactory, yqSharedResources->UserSpaceYdbDriver, credentialsFactory, !protoConfig.GetReadActorsFactoryConfig().GetPqReadActorFactoryConfig().GetCookieCommitMode());
        RegisterYdbReadActorFactory(*asyncIoFactory, yqSharedResources->UserSpaceYdbDriver, credentialsFactory);
        RegisterS3ReadActorFactory(*asyncIoFactory, credentialsFactory, httpGateway, s3HttpRetryPolicy, readActorFactoryCfg,
            yqCounters->GetSubgroup("subsystem", "S3ReadActor"));
        RegisterS3WriteActorFactory(*asyncIoFactory, credentialsFactory,
            httpGateway, s3HttpRetryPolicy);
        RegisterClickHouseReadActorFactory(*asyncIoFactory, credentialsFactory, httpGateway);

        RegisterDqPqWriteActorFactory(*asyncIoFactory, yqSharedResources->UserSpaceYdbDriver, credentialsFactory);
        RegisterDQSolomonWriteActorFactory(*asyncIoFactory, credentialsFactory);
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
        lwmOptions.DqTaskCounters = protoConfig.GetEnableTaskCounters() ? appData->Counters->GetSubgroup("counters", "dq_tasks") : nullptr;
        lwmOptions.Factory = NYql::NTaskRunnerProxy::CreateFactory(appData->FunctionRegistry, dqCompFactory, dqTaskTransformFactory, nullptr, false);
        lwmOptions.AsyncIoFactory = asyncIoFactory;
        lwmOptions.FunctionRegistry = appData->FunctionRegistry;
        lwmOptions.TaskRunnerInvokerFactory = new NYql::NDqs::TTaskRunnerInvokerFactory();
        lwmOptions.MkqlInitialMemoryLimit = mkqlInitialMemoryLimit;
        lwmOptions.MkqlTotalMemoryLimit = mkqlTotalMemoryLimit;
        lwmOptions.MkqlProgramHardMemoryLimit = protoConfig.GetResourceManager().GetMkqlTaskHardMemoryLimit();
        lwmOptions.MkqlMinAllocSize = mkqlAllocSize;
        lwmOptions.TaskRunnerActorFactory = NYql::NDq::NTaskRunnerActor::CreateLocalTaskRunnerActorFactory(
            [=](const NYql::NDqProto::TDqTask& task, const NYql::NDq::TLogFunc&) {
                return lwmOptions.Factory->Get(task);
            });
        if (protoConfig.GetRateLimiter().GetDataPlaneEnabled()) {
            lwmOptions.QuoterServiceActorId = NYq::YqQuoterServiceActorId();
        }
        auto resman = NYql::NDqs::CreateLocalWorkerManager(lwmOptions);

        actorRegistrator(NYql::NDqs::MakeWorkerManagerActorID(nodeId), resman);
    }

    ::NYql::NCommon::TServiceCounters serviceCounters(appData->Counters);

    if (protoConfig.GetNodesManager().GetEnabled() || protoConfig.GetPendingFetcher().GetEnabled()) {
        auto internal = protoConfig.GetPrivateApi().GetLoopback()
            ? NFq::CreateLoopbackServiceActor(clientCounters)
            : NFq::CreateInternalServiceActor(
                yqSharedResources,
                credentialsProviderFactory,
                protoConfig.GetPrivateApi(),
                clientCounters
            );
        actorRegistrator(NFq::MakeInternalServiceActorId(), internal);
    }

    if (protoConfig.GetNodesManager().GetEnabled()) {
        auto nodesManager = CreateNodesManager(
            workerManagerCounters,
            TAppData::TimeProvider,
            TAppData::RandomProvider,
            serviceCounters,
            protoConfig.GetPrivateApi(),
            yqSharedResources,
            icPort,
            protoConfig.GetNodesManager().GetDataCenter(),
            protoConfig.GetNodesManager().GetUseDataCenter(),
            tenant,
            mkqlInitialMemoryLimit);

        actorRegistrator(MakeNodesManagerId(), nodesManager);
    }

    auto httpProxy = NHttp::CreateHttpProxy(NMonitoring::TMetricRegistry::SharedInstance());
    actorRegistrator(MakeYqlAnalyticsHttpProxyId(), httpProxy);

    if (protoConfig.GetTestConnection().GetEnabled()) {
        auto testConnection = NYq::CreateTestConnectionActor(
                protoConfig.GetTestConnection(),
                protoConfig.GetControlPlaneStorage(),
                protoConfig.GetCommon(),
                protoConfig.GetTokenAccessor(),
                yqSharedResources,
                credentialsFactory,
                pqCmConnections,
                appData->FunctionRegistry,
                httpGateway,
                yqCounters->GetSubgroup("subsystem", "TestConnection"));
        actorRegistrator(NYq::TestConnectionActorId(), testConnection);
    }

    if (protoConfig.GetPendingFetcher().GetEnabled()) {
        auto fetcher = CreatePendingFetcher(
            yqSharedResources,
            credentialsProviderFactory,
            protoConfig.GetCommon(),
            protoConfig.GetCheckpointCoordinator(),
            protoConfig.GetPrivateApi(),
            protoConfig.GetGateways(),
            protoConfig.GetPinger(),
            protoConfig.GetRateLimiter(),
            appData->FunctionRegistry,
            TAppData::TimeProvider,
            TAppData::RandomProvider,
            dqCompFactory,
            serviceCounters,
            credentialsFactory,
            httpGateway,
            std::move(pqCmConnections),
            clientCounters,
            tenant,
            appData->Mon
            );

        actorRegistrator(MakePendingFetcherId(nodeId), fetcher);
    }

    if (protoConfig.GetPrivateProxy().GetEnabled()) {
        auto proxyPrivate = CreateYqlAnalyticsPrivateProxy(
            protoConfig.GetPrivateProxy(),
            TAppData::TimeProvider,
            TAppData::RandomProvider,
            serviceCounters.Counters,
            protoConfig.GetTokenAccessor());

        actorRegistrator(MakeYqPrivateProxyId(), proxyPrivate);
    }

    if (protoConfig.GetHealth().GetEnabled()) {
        auto health = NYq::CreateHealthActor(
            protoConfig.GetHealth(),
            yqSharedResources,
            serviceCounters.Counters);
        actorRegistrator(NYq::HealthActorId(), health);
    }

    if (protoConfig.GetQuotasManager().GetEnabled()) {
        auto quotaService = NYq::CreateQuotaServiceActor(
            protoConfig.GetQuotasManager(),
            protoConfig.GetControlPlaneStorage().GetStorage(),
            yqSharedResources,
            credentialsProviderFactory,
            serviceCounters.Counters,
            {
                TQuotaDescription(SUBJECT_TYPE_CLOUD, QUOTA_ANALYTICS_COUNT_LIMIT, 100, 1000, NYq::ControlPlaneStorageServiceActorId()),
                TQuotaDescription(SUBJECT_TYPE_CLOUD, QUOTA_STREAMING_COUNT_LIMIT, 100, 1000, NYq::ControlPlaneStorageServiceActorId()),
                TQuotaDescription(SUBJECT_TYPE_CLOUD, QUOTA_CPU_PERCENT_LIMIT, 200, 3200, protoConfig.GetRateLimiter().GetControlPlaneEnabled() ? NYq::RateLimiterControlPlaneServiceId() : NActors::TActorId()),
                TQuotaDescription(SUBJECT_TYPE_CLOUD, QUOTA_MEMORY_LIMIT, 0),
                TQuotaDescription(SUBJECT_TYPE_CLOUD, QUOTA_RESULT_LIMIT, 0),
                TQuotaDescription(SUBJECT_TYPE_CLOUD, QUOTA_ANALYTICS_DURATION_LIMIT, 1440),
                TQuotaDescription(SUBJECT_TYPE_CLOUD, QUOTA_STREAMING_DURATION_LIMIT, 0),
                TQuotaDescription(SUBJECT_TYPE_CLOUD, QUOTA_QUERY_RESULT_LIMIT, 20_MB, 2_GB)
            },
            appData->Mon);
        actorRegistrator(NYq::MakeQuotaServiceActorId(nodeId), quotaService);

        auto quotaProxy = NYq::CreateQuotaProxyActor(
            protoConfig.GetQuotasManager(),
            serviceCounters.Counters);
        actorRegistrator(NYq::MakeQuotaProxyActorId(), quotaProxy);
    }
}

IYqSharedResources::TPtr CreateYqSharedResources(
    const NYq::NConfig::TConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const ::NMonitoring::TDynamicCounterPtr& counters)
{
    return CreateYqSharedResourcesImpl(config, credentialsProviderFactory, counters);
}

} // NYq

#include "init.h"

#include <ydb/core/fq/libs/audit/yq_audit_service.h>
#include <ydb/core/fq/libs/checkpoint_storage/storage_service.h>
#include <ydb/core/fq/libs/checkpoint_storage/storage_service.h>
#include <ydb/core/fq/libs/cloud_audit/yq_cloud_audit_service.h>
#include <ydb/core/fq/libs/compute/ydb/control_plane/compute_database_control_plane_service.h>
#include <ydb/core/fq/libs/control_plane_config/control_plane_config.h>
#include <ydb/core/fq/libs/control_plane_proxy/control_plane_proxy.h>
#include <ydb/core/fq/libs/control_plane_storage/control_plane_storage.h>
#include <ydb/core/fq/libs/health/health.h>
#include <ydb/core/fq/libs/private_client/internal_service.h>
#include <ydb/core/fq/libs/private_client/loopback_service.h>
#include <ydb/core/fq/libs/quota_manager/quota_manager.h>
#include <ydb/core/fq/libs/quota_manager/quota_proxy.h>
#include <ydb/core/fq/libs/rate_limiter/control_plane_service/rate_limiter_control_plane_service.h>
#include <ydb/core/fq/libs/rate_limiter/events/control_plane_events.h>
#include <ydb/core/fq/libs/rate_limiter/events/data_plane.h>
#include <ydb/core/fq/libs/rate_limiter/quoter_service/quoter_service.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>
#include <ydb/core/fq/libs/test_connection/test_connection.h>

#include <ydb/library/folder_service/folder_service.h>
#include <ydb/library/yql/providers/common/metrics/service_counters.h>

#include <ydb/library/actors/http/http_proxy.h>
#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/dq/actors/input_transforms/dq_input_transform_lookup_factory.h>
#include <ydb/library/yql/dq/comp_nodes/yql_common_dq_factory.h>
#include <ydb/library/yql/dq/transform/yql_common_dq_transform.h>
#include <ydb/library/yql/utils/actor_log/log.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/providers/common/comp_nodes/yql_factory.h>
#include <ydb/library/yql/providers/dq/task_runner/tasks_runner_local.h>
#include <ydb/library/yql/providers/dq/worker_manager/local_worker_manager.h>
#include <ydb/library/yql/providers/generic/actors/yql_generic_provider_factories.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_sink_factory.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_source_factory.h>
#include <ydb/library/yql/providers/s3/proto/retry_config.pb.h>
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

namespace NFq {

using namespace NKikimr;

void Init(
    const NFq::NConfig::TConfig& protoConfig,
    ui32 nodeId,
    const TActorRegistrator& actorRegistrator,
    const TAppData* appData,
    const TString& tenant,
    ::NPq::NConfigurationManager::IConnections::TPtr pqCmConnections,
    const IYqSharedResources::TPtr& iyqSharedResources,
    const std::function<IActor*(const NKikimrProto::NFolderService::TFolderServiceConfig& authConfig)>& folderServiceFactory,
    ui32 icPort,
    const std::vector<NKikimr::NMiniKQL::TComputationNodeFactory>& additionalCompNodeFactories
    )
{
    Y_ABORT_UNLESS(iyqSharedResources, "No YQ shared resources created");
    TYqSharedResources::TPtr yqSharedResources = TYqSharedResources::Cast(iyqSharedResources);

    auto yqCounters = appData->Counters->GetSubgroup("counters", "yq");
    const auto clientCounters = yqCounters->GetSubgroup("subsystem", "ClientMetrics");

    if (protoConfig.GetControlPlaneStorage().GetEnabled()) {
        auto controlPlaneStorage = protoConfig.GetControlPlaneStorage().GetUseInMemory()
            ? NFq::CreateInMemoryControlPlaneStorageServiceActor(protoConfig.GetControlPlaneStorage())
            : NFq::CreateYdbControlPlaneStorageServiceActor(
                protoConfig.GetControlPlaneStorage(),
                protoConfig.GetGateways().GetS3(),
                protoConfig.GetCommon(),
                protoConfig.GetCompute(),
                yqCounters->GetSubgroup("subsystem", "ControlPlaneStorage"),
                yqSharedResources,
                NKikimr::CreateYdbCredentialsProviderFactory,
                tenant);
        actorRegistrator(NFq::ControlPlaneStorageServiceActorId(), controlPlaneStorage);

        actorRegistrator(NFq::ControlPlaneConfigActorId(),
            CreateControlPlaneConfigActor(yqSharedResources, NKikimr::CreateYdbCredentialsProviderFactory, protoConfig.GetControlPlaneStorage(),
                protoConfig.GetCompute(), yqCounters->GetSubgroup("subsystem", "ControlPlaneConfig"))
        );
    }

    NFq::TSigner::TPtr signer;
    if (protoConfig.GetTokenAccessor().GetHmacSecretFile()) {
        signer = ::NFq::CreateSignerFromFile(protoConfig.GetTokenAccessor().GetHmacSecretFile());
    }

    if (protoConfig.GetControlPlaneProxy().GetEnabled()) {
        auto controlPlaneProxy = NFq::CreateControlPlaneProxyActor(
            protoConfig.GetControlPlaneProxy(),
            protoConfig.GetControlPlaneStorage(),
            protoConfig.GetCompute(),
            protoConfig.GetCommon(),
            protoConfig.GetGateways().GetS3(),
            signer,
            yqSharedResources,
            NKikimr::CreateYdbCredentialsProviderFactory,
            yqCounters->GetSubgroup("subsystem", "ControlPlaneProxy"),
            protoConfig.GetQuotasManager().GetEnabled());
        actorRegistrator(NFq::ControlPlaneProxyActorId(), controlPlaneProxy);
    }

    if (protoConfig.GetCompute().GetYdb().GetEnable() && protoConfig.GetCompute().GetYdb().GetControlPlane().GetEnable()) {
        auto computeDatabaseService = NFq::CreateComputeDatabaseControlPlaneServiceActor(protoConfig.GetCompute(), 
                                                                                         NKikimr::CreateYdbCredentialsProviderFactory, 
                                                                                         protoConfig.GetCommon(), 
                                                                                         signer, 
                                                                                         yqSharedResources, 
                                                                                         yqCounters->GetSubgroup("subsystem", "DatabaseControlPlane"));
        actorRegistrator(NFq::ComputeDatabaseControlPlaneServiceActorId(), computeDatabaseService.release());
    }

    if (protoConfig.GetRateLimiter().GetControlPlaneEnabled()) {
        Y_ABORT_UNLESS(protoConfig.GetQuotasManager().GetEnabled()); // Rate limiter resources want to know CPU quota on creation
        NActors::IActor* rateLimiterService = NFq::CreateRateLimiterControlPlaneService(protoConfig.GetRateLimiter(), yqSharedResources, NKikimr::CreateYdbCredentialsProviderFactory);
        actorRegistrator(NFq::RateLimiterControlPlaneServiceId(), rateLimiterService);
    }

    if (protoConfig.GetRateLimiter().GetDataPlaneEnabled()) {
        actorRegistrator(NFq::YqQuoterServiceActorId(), NFq::CreateQuoterService(protoConfig.GetRateLimiter(), yqSharedResources, NKikimr::CreateYdbCredentialsProviderFactory));
    }

    if (protoConfig.GetAudit().GetEnabled()) {
        auto* auditService = NFq::CreateYqCloudAuditServiceActor(
            protoConfig.GetAudit(),
            yqCounters->GetSubgroup("subsystem", "audit"));
        actorRegistrator(NFq::YqAuditServiceActorId(), auditService);
    }

    // if not enabled then stub
    {
        auto folderService = folderServiceFactory(protoConfig.GetFolderService());
        actorRegistrator(NKikimr::NFolderService::FolderServiceActorId(), folderService);
    }

    if (protoConfig.GetCheckpointCoordinator().GetEnabled()) {
        auto checkpointStorage = NFq::NewCheckpointStorageService(protoConfig.GetCheckpointCoordinator(), protoConfig.GetCommon(), NKikimr::CreateYdbCredentialsProviderFactory, yqSharedResources);
        actorRegistrator(NYql::NDq::MakeCheckpointStorageID(), checkpointStorage.release());
    }

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

    NYql::NConnector::IClient::TPtr connectorClient = nullptr;
    if (protoConfig.GetGateways().GetGeneric().HasConnector()) {
        connectorClient = NYql::NConnector::MakeClientGRPC(protoConfig.GetGateways().GetGeneric().GetConnector());
    }

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
        auto s3HttpRetryPolicy = NYql::GetHTTPDefaultRetryPolicy(NYql::THttpRetryPolicyOptions{.MaxTime = TDuration::Max(), .RetriedCurlCodes = NYql::FqRetriedCurlCodes()});
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
        for (auto& formatSizeLimit: protoConfig.GetGateways().GetS3().GetFormatSizeLimit()) {
            if (formatSizeLimit.GetName()) { // ignore unnamed limits
                readActorFactoryCfg.FormatSizeLimits.emplace(
                    formatSizeLimit.GetName(), formatSizeLimit.GetFileSizeLimit());
            }
        }
        if (protoConfig.GetGateways().GetS3().HasFileSizeLimit()) {
            readActorFactoryCfg.FileSizeLimit =
                protoConfig.GetGateways().GetS3().GetFileSizeLimit();
        }
        if (protoConfig.GetGateways().GetS3().HasBlockFileSizeLimit()) {
            readActorFactoryCfg.BlockFileSizeLimit =
                protoConfig.GetGateways().GetS3().GetBlockFileSizeLimit();
        }
        RegisterDqInputTransformLookupActorFactory(*asyncIoFactory);
        RegisterDqPqReadActorFactory(*asyncIoFactory, yqSharedResources->UserSpaceYdbDriver, credentialsFactory);
        RegisterYdbReadActorFactory(*asyncIoFactory, yqSharedResources->UserSpaceYdbDriver, credentialsFactory);
        RegisterS3ReadActorFactory(*asyncIoFactory, credentialsFactory, httpGateway, s3HttpRetryPolicy, readActorFactoryCfg,
            yqCounters->GetSubgroup("subsystem", "S3ReadActor"));
        RegisterS3WriteActorFactory(*asyncIoFactory, credentialsFactory,
            httpGateway, s3HttpRetryPolicy);
        RegisterGenericProviderFactories(*asyncIoFactory, credentialsFactory, connectorClient);

        RegisterDqPqWriteActorFactory(*asyncIoFactory, yqSharedResources->UserSpaceYdbDriver, credentialsFactory, yqCounters->GetSubgroup("subsystem", "DqSinkTracker"));
        RegisterDQSolomonWriteActorFactory(*asyncIoFactory, credentialsFactory);
    }

    ui64 mkqlInitialMemoryLimit = 8_GB;
    auto taskCounters = protoConfig.GetEnableTaskCounters() ? appData->Counters->GetSubgroup("counters", "dq_tasks") : nullptr;
    auto workerManagerCounters = NYql::NDqs::TWorkerManagerCounters(
        yqCounters->GetSubgroup("subsystem", "worker_manager"),
        taskCounters);

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
        Y_ABORT_UNLESS(appData->FunctionRegistry);
        NYql::NDqs::TLocalWorkerManagerOptions lwmOptions;
        lwmOptions.Counters = workerManagerCounters;
        lwmOptions.Factory = NYql::NTaskRunnerProxy::CreateFactory(appData->FunctionRegistry, dqCompFactory, dqTaskTransformFactory, nullptr, false);
        lwmOptions.AsyncIoFactory = asyncIoFactory;
        lwmOptions.FunctionRegistry = appData->FunctionRegistry;
        lwmOptions.TaskRunnerInvokerFactory = new NYql::NDqs::TTaskRunnerInvokerFactory();
        lwmOptions.MkqlInitialMemoryLimit = mkqlInitialMemoryLimit;
        lwmOptions.MkqlTotalMemoryLimit = mkqlTotalMemoryLimit;
        lwmOptions.MkqlProgramHardMemoryLimit = protoConfig.GetResourceManager().GetMkqlTaskHardMemoryLimit();
        lwmOptions.MkqlMinAllocSize = mkqlAllocSize;
        lwmOptions.TaskRunnerActorFactory = NYql::NDq::NTaskRunnerActor::CreateLocalTaskRunnerActorFactory(
            [=](std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc, const NYql::NDq::TDqTaskSettings& task, NYql::NDqProto::EDqStatsMode statsMode, const NYql::NDq::TLogFunc&) {
                return lwmOptions.Factory->Get(alloc, task, statsMode);
            });
        if (protoConfig.GetRateLimiter().GetDataPlaneEnabled()) {
            lwmOptions.QuoterServiceActorId = NFq::YqQuoterServiceActorId();
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
                NKikimr::CreateYdbCredentialsProviderFactory,
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
        auto testConnection = NFq::CreateTestConnectionActor(
                protoConfig.GetTestConnection(),
                protoConfig.GetControlPlaneStorage(),
                protoConfig.GetGateways().GetS3(),
                protoConfig.GetCommon(),
                signer,
                yqSharedResources,
                credentialsFactory,
                pqCmConnections,
                appData->FunctionRegistry,
                httpGateway,
                yqCounters->GetSubgroup("subsystem", "TestConnection"));
        actorRegistrator(NFq::TestConnectionActorId(), testConnection);
    }

    if (protoConfig.GetPendingFetcher().GetEnabled()) {
        auto fetcher = CreatePendingFetcher(
            yqSharedResources,
            NKikimr::CreateYdbCredentialsProviderFactory,
            protoConfig,
            appData->FunctionRegistry,
            TAppData::TimeProvider,
            TAppData::RandomProvider,
            dqCompFactory,
            serviceCounters,
            credentialsFactory,
            httpGateway,
            connectorClient,
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
            signer);

        actorRegistrator(MakeYqPrivateProxyId(), proxyPrivate);
    }

    if (protoConfig.GetHealth().GetEnabled()) {
        auto health = NFq::CreateHealthActor(
            protoConfig.GetHealth(),
            yqSharedResources,
            serviceCounters.Counters);
        actorRegistrator(NFq::HealthActorId(), health);
    }

    if (protoConfig.GetQuotasManager().GetEnabled()) {
        auto quotaService = NFq::CreateQuotaServiceActor(
            protoConfig.GetQuotasManager(),
            protoConfig.GetControlPlaneStorage().GetStorage(),
            yqSharedResources,
            NKikimr::CreateYdbCredentialsProviderFactory,
            serviceCounters.Counters,
            {
                TQuotaDescription(SUBJECT_TYPE_CLOUD, QUOTA_ANALYTICS_COUNT_LIMIT, 100, 1000, NFq::ControlPlaneStorageServiceActorId()),
                TQuotaDescription(SUBJECT_TYPE_CLOUD, QUOTA_STREAMING_COUNT_LIMIT, 100, 1000, NFq::ControlPlaneStorageServiceActorId()),
                TQuotaDescription(SUBJECT_TYPE_CLOUD, QUOTA_CPU_PERCENT_LIMIT, 200, 3200, protoConfig.GetRateLimiter().GetControlPlaneEnabled() ? NFq::RateLimiterControlPlaneServiceId() : NActors::TActorId()),
                TQuotaDescription(SUBJECT_TYPE_CLOUD, QUOTA_MEMORY_LIMIT, 0),
                TQuotaDescription(SUBJECT_TYPE_CLOUD, QUOTA_RESULT_LIMIT, 0),
                TQuotaDescription(SUBJECT_TYPE_CLOUD, QUOTA_ANALYTICS_DURATION_LIMIT, 1440),
                TQuotaDescription(SUBJECT_TYPE_CLOUD, QUOTA_STREAMING_DURATION_LIMIT, 0),
                TQuotaDescription(SUBJECT_TYPE_CLOUD, QUOTA_QUERY_RESULT_LIMIT, 20_MB, 2_GB)
            },
            appData->Mon);
        actorRegistrator(NFq::MakeQuotaServiceActorId(nodeId), quotaService);

        auto quotaProxy = NFq::CreateQuotaProxyActor(
            protoConfig.GetQuotasManager(),
            serviceCounters.Counters);
        actorRegistrator(NFq::MakeQuotaProxyActorId(), quotaProxy);
    }
}

IYqSharedResources::TPtr CreateYqSharedResources(
    const NFq::NConfig::TConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const ::NMonitoring::TDynamicCounterPtr& counters)
{
    return CreateYqSharedResourcesImpl(config, credentialsProviderFactory, counters);
}

} // NFq

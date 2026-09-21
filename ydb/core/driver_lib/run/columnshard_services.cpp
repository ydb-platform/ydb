#include "kikimr_services_initializers.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/blob_depot/blob_depot.h>
#include <ydb/core/kesus/tablet/tablet.h>
#include <ydb/core/keyvalue/keyvalue.h>
#include <ydb/core/mind/labels_maintainer.h>
#include <ydb/core/mind/tenant_pool.h>
#include <ydb/core/mind/hive/hive.h>
#include <ydb/core/persqueue/pq.h>
#include <ydb/core/statistics/aggregator/aggregator.h>
#include <ydb/core/sys_view/processor/processor.h>
#include <ydb/core/test_tablet/test_tablet.h>
#include <ydb/core/test_tablet/state_server_interface.h>
#include <ydb/core/tx/coordinator/coordinator.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/mediator/mediator.h>
#include <ydb/core/tx/replication/controller/controller.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/sequenceshard/sequenceshard.h>
#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/overload_manager/overload_manager_service.h>
#include <ydb/core/tx/columnshard/data_accessor/cache_policy/policy.h>
#include <ydb/core/tx/columnshard/column_fetching/cache_policy.h>
#include <ydb/core/tx/general_cache/service/service.h>
#include <ydb/core/tx/general_cache/usage/service.h>
#include <ydb/core/backup/controller/tablet.h>
#include <ydb/core/graph/api/shard.h>
#include <ydb/core/tx/columnshard/flow_control_manager/flow_control_manager_service.h>
#include <ydb/services/udf_store/compile_controller/compile_controller.h>

#if defined(YDB_EMBEDDED_NBS_ENABLED)
#include <ydb/core/nbs/cloud/blockstore/bootstrap/bootstrap.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/ss_proxy/ss_proxy.h>
#include <ydb/core/nbs/cloud/blockstore/config/protos/storage.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/volume/volume.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/partition_direct.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/dbs_controller/dbs_controller.h>
#endif
#include <ydb/core/load_test/nbs_dbg_like_load_tablet.h>

#include <ydb/core/tx/conveyor_composite/service/service.h>
#include <ydb/core/tx/conveyor_composite/usage/config.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>
#include <ydb/core/tx/priorities/usage/config.h>
#include <ydb/core/tx/priorities/service/service.h>
#include <ydb/core/tx/priorities/usage/service.h>

namespace NKikimr::NKikimrServicesInitializers {

TLocalServiceInitializer::TLocalServiceInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig)
{}

void TLocalServiceInitializer::InitializeServices(
        NActors::TActorSystemSetup* setup,
        const NKikimr::TAppData* appData) {
    // choose pool id for important tablets
    ui32 importantPoolId = appData->UserPoolId;
    if (Config.GetFeatureFlags().GetImportantTabletsUseSystemPool()) {
        importantPoolId = appData->SystemPoolId;
    }

    // setup local
    TLocalConfig::TPtr localConfig(new TLocalConfig());

    std::unordered_map<TTabletTypes::EType, NKikimrLocal::TTabletAvailability> tabletAvailabilities;
    for (const auto& availability : Config.GetDynamicNodeConfig().GetTabletAvailability()) {
        tabletAvailabilities.emplace(availability.GetType(), availability);
    }

    auto addToLocalConfig = [&localConfig, &tabletAvailabilities, tabletPool = appData->SystemPoolId](TTabletTypes::EType tabletType,
                                                                                                      TTabletSetupInfo::TTabletCreationFunc op,
                                                                                                      NActors::TMailboxType::EType mailboxType,
                                                                                                      ui32 poolId) {
        auto availIt = tabletAvailabilities.find(tabletType);
        auto localIt = localConfig->TabletClassInfo.emplace(tabletType, new TTabletSetupInfo(op, mailboxType, poolId, TMailboxType::ReadAsFilled, tabletPool)).first;
        if (availIt != tabletAvailabilities.end()) {
            localIt->second.MaxCount = availIt->second.GetMaxCount();
            localIt->second.Priority = availIt->second.GetPriority();
        }
    };

    addToLocalConfig(TTabletTypes::SchemeShard, &CreateFlatTxSchemeShard, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::DataShard, &CreateDataShard, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::KeyValue, &CreateKeyValueFlat, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::PersQueue, &CreatePersQueue, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::PersQueueReadBalancer, &CreatePersQueueReadBalancer, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::Coordinator, &CreateFlatTxCoordinator, TMailboxType::Revolving, importantPoolId);
    addToLocalConfig(TTabletTypes::Mediator, &CreateTxMediator, TMailboxType::Revolving, importantPoolId);
    addToLocalConfig(TTabletTypes::Kesus, &NKesus::CreateKesusTablet, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::Hive, &CreateDefaultHive, TMailboxType::ReadAsFilled, importantPoolId);
    addToLocalConfig(TTabletTypes::SysViewProcessor, &NSysView::CreateSysViewProcessor, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::TestShard, &NTestShard::CreateTestShard, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::NbsLoadTablet, &NKikimr::NNbsDbgLike::CreateNbsDbgLikeLoadTablet, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::ColumnShard, &CreateColumnShard, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::SequenceShard, &NSequenceShard::CreateSequenceShard, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::ReplicationController, &NReplication::CreateController, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::BlobDepot, &NBlobDepot::CreateBlobDepot, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::StatisticsAggregator, &NStat::CreateStatisticsAggregator, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::GraphShard, &NGraph::CreateGraphShard, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::BackupController, &NBackup::CreateBackupController, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::WasmCompileController, &NUdfStore::CreateWasmCompileController, TMailboxType::ReadAsFilled, appData->UserPoolId);
#if defined(YDB_EMBEDDED_NBS_ENABLED)
    addToLocalConfig(TTabletTypes::BlockStoreVolumeDirect, &NYdb::NBS::NStorage::CreateVolumeTablet, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::BlockStorePartitionDirect, &NYdb::NBS::NBlockStore::NStorage::NPartitionDirect::CreatePartitionTablet, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::DbsController, &NYdb::NBS::NBlockStore::NStorage::NDbsController::CreateDbsControllerTablet, TMailboxType::ReadAsFilled, appData->UserPoolId);
#endif

    if (Config.GetShutdownConfig().HasDrainTimeoutSeconds()) {
        localConfig->DrainNodeTimeout = TDuration::Seconds(Config.GetShutdownConfig().GetDrainTimeoutSeconds());
    }

    TTenantPoolConfig::TPtr tenantPoolConfig = new TTenantPoolConfig(Config.GetTenantPoolConfig(), localConfig);
    if (!tenantPoolConfig->IsEnabled && !tenantPoolConfig->StaticSlots.empty())
        Y_ABORT("Tenant slots are not allowed in disabled pool");

    setup->LocalServices.push_back(std::make_pair(MakeTenantPoolRootID(),
        TActorSetupCmd(CreateTenantPool(tenantPoolConfig), TMailboxType::ReadAsFilled, 0)));

    setup->LocalServices.push_back(std::make_pair(
        TActorId(),
        TActorSetupCmd(CreateLabelsMaintainer(Config.GetMonitoringConfig()),
                       TMailboxType::ReadAsFilled, 0)));

    setup->LocalServices.emplace_back(NTestShard::MakeStateServerInterfaceActorId(), TActorSetupCmd(
        NTestShard::CreateStateServerInterfaceActor(nullptr), TMailboxType::ReadAsFilled, 0));

    NKesus::AddKesusProbesList();
}

TBlobCacheInitializer::TBlobCacheInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig)
{}

void TBlobCacheInitializer::InitializeServices(
        NActors::TActorSystemSetup* setup,
        const NKikimr::TAppData* appData) {

    TIntrusivePtr<::NMonitoring::TDynamicCounters> tabletGroup = GetServiceCounters(appData->Counters, "tablets");
    TIntrusivePtr<::NMonitoring::TDynamicCounters> blobCacheGroup = tabletGroup->GetSubgroup("type", "BLOB_CACHE");

    std::optional<ui64> maxCacheSize;
    if (Config.HasBlobCacheConfig()) {
        if (Config.GetBlobCacheConfig().HasMaxSizeBytes()) {
            maxCacheSize = Config.GetBlobCacheConfig().GetMaxSizeBytes();
        }
    }
    setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(NBlobCache::MakeBlobCacheServiceId(),
        TActorSetupCmd(NBlobCache::CreateBlobCache(maxCacheSize, blobCacheGroup), TMailboxType::ReadAsFilled, appData->UserPoolId)));
}

TGeneralCachePortionsMetadataInitializer::TGeneralCachePortionsMetadataInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TGeneralCachePortionsMetadataInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    auto serviceConfig = NGeneralCache::NPublic::TConfig::BuildFromProto(Config.GetPortionsMetadataCache());
    if (serviceConfig.IsFail()) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("error", "cannot parse portions metadata cache config")("action", "default_usage")(
            "error", serviceConfig.GetErrorMessage())("default", NGeneralCache::NPublic::TConfig::BuildDefault().DebugString());
        serviceConfig = NGeneralCache::NPublic::TConfig::BuildDefault();
    }
    AFL_VERIFY(!serviceConfig.IsFail());

    TIntrusivePtr<::NMonitoring::TDynamicCounters> tabletGroup = GetServiceCounters(appData->Counters, "tablets");
    TIntrusivePtr<::NMonitoring::TDynamicCounters> conveyorGroup = tabletGroup->GetSubgroup("type", "TX_GENERAL_CACHE_PORTIONS_METADATA");

    auto service = NGeneralCache::CreateService<NOlap::NGeneralCache::TPortionsMetadataCachePolicy>(*serviceConfig, conveyorGroup);

    setup->LocalServices.push_back(
        std::make_pair(NGeneralCache::TServiceOperator<NOlap::NGeneralCache::TPortionsMetadataCachePolicy>::MakeServiceId(NodeId),
            TActorSetupCmd(service, TMailboxType::HTSwap, appData->UserPoolId)));
}

TGeneralCacheColumnDataInitializer::TGeneralCacheColumnDataInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig)
{
}

void TGeneralCacheColumnDataInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    auto serviceConfig = NGeneralCache::NPublic::TConfig::BuildFromProto(Config.GetColumnDataCache());
    if (serviceConfig.IsFail()) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("error", "cannot parse column data cache config")("action", "default_usage")(
            "error", serviceConfig.GetErrorMessage())("default", NGeneralCache::NPublic::TConfig::BuildDefault().DebugString());
        serviceConfig = NGeneralCache::NPublic::TConfig::BuildDefault();
    }
    AFL_VERIFY(!serviceConfig.IsFail());

    TIntrusivePtr<::NMonitoring::TDynamicCounters> tabletGroup = GetServiceCounters(appData->Counters, "tablets");
    TIntrusivePtr<::NMonitoring::TDynamicCounters> conveyorGroup = tabletGroup->GetSubgroup("type", "TX_GENERAL_CACHE_COLUMN_DATA");

    auto service = NGeneralCache::CreateService<NOlap::NGeneralCache::TColumnDataCachePolicy>(*serviceConfig, conveyorGroup);

    setup->LocalServices.push_back(
        std::make_pair(NGeneralCache::TServiceOperator<NOlap::NGeneralCache::TColumnDataCachePolicy>::MakeServiceId(NodeId),
            TActorSetupCmd(service, TMailboxType::HTSwap, appData->UserPoolId)));
}

TOverloadManagerInitializer::TOverloadManagerInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TOverloadManagerInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    TIntrusivePtr<::NMonitoring::TDynamicCounters> tabletGroup = GetServiceCounters(appData->Counters, "tablets");
    TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup = tabletGroup->GetSubgroup("type", "CS_OVERLOAD_MANAGER");

    setup->LocalServices.push_back(std::make_pair(NColumnShard::NOverload::TOverloadManagerServiceOperator::MakeServiceId(),
        TActorSetupCmd(NColumnShard::NOverload::TOverloadManagerServiceOperator::CreateService(countersGroup), TMailboxType::HTSwap, appData->UserPoolId)));
}

TFlowControlManagerInitializer::TFlowControlManagerInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TFlowControlManagerInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup =
        NColumnShard::NFlowControl::TFlowControlManagerServiceOperator::BuildCountersGroup(appData->Counters);

    setup->LocalServices.push_back(std::make_pair(NColumnShard::NFlowControl::TFlowControlManagerServiceOperator::MakeServiceId(NodeId),
        TActorSetupCmd(NColumnShard::NFlowControl::TFlowControlManagerServiceOperator::CreateService(countersGroup), TMailboxType::HTSwap, appData->UserPoolId)));
}

TCompPrioritiesInitializer::TCompPrioritiesInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TCompPrioritiesInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    NPrioritiesQueue::TConfig serviceConfig;
    if (Config.HasCompPrioritiesConfig()) {
        Y_ABORT_UNLESS(serviceConfig.DeserializeFromProto(Config.GetCompPrioritiesConfig()));
    }

    if (serviceConfig.IsEnabled()) {
        TIntrusivePtr<::NMonitoring::TDynamicCounters> tabletGroup = GetServiceCounters(appData->Counters, "tablets");
        TIntrusivePtr<::NMonitoring::TDynamicCounters> conveyorGroup = tabletGroup->GetSubgroup("type", "TX_COMP_PRIORITIES");

        auto service = NPrioritiesQueue::CreateService<NPrioritiesQueue::TCompConveyorPolicy>(serviceConfig, conveyorGroup);

        setup->LocalServices.push_back(std::make_pair(
            NPrioritiesQueue::TCompServiceOperator::MakeServiceId(NodeId),
            TActorSetupCmd(service, TMailboxType::HTSwap, appData->UserPoolId)));
    }
}

TCompositeConveyorInitializer::TCompositeConveyorInitializer(const TKikimrRunConfig& runConfig)
	: IKikimrServicesInitializer(runConfig) {
}

void TCompositeConveyorInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    const NKikimrConfig::TCompositeConveyorConfig protoConfig = [&]() {
        if (Config.HasCompositeConveyorConfig()) {
            return Config.GetCompositeConveyorConfig();
        }
        NKikimrConfig::TCompositeConveyorConfig result;
        if (Config.HasCompConveyorConfig()) {
            NKikimrConfig::TCompositeConveyorConfig::TCategory& protoCategory = *result.AddCategories();
            protoCategory.SetName(::ToString(NConveyorComposite::ESpecialTaskCategory::Compaction));
            NKikimrConfig::TCompositeConveyorConfig::TWorkersPool& protoWorkersPool = *result.AddWorkerPools();
            NKikimrConfig::TCompositeConveyorConfig::TWorkerPoolCategoryLink& protoLink = *protoWorkersPool.AddLinks();
            protoLink.SetCategory(::ToString(NConveyorComposite::ESpecialTaskCategory::Compaction));
            protoLink.SetWeight(1);
            if (Config.GetCompConveyorConfig().HasWorkersCountDouble()) {
                protoWorkersPool.SetWorkersCount(Config.GetCompConveyorConfig().GetWorkersCountDouble());
            } else if (Config.GetCompConveyorConfig().HasWorkersCount()) {
                protoWorkersPool.SetWorkersCount(Config.GetCompConveyorConfig().GetWorkersCount());
            } else if (Config.GetCompConveyorConfig().HasDefaultFractionOfThreadsCount()) {
                protoWorkersPool.SetDefaultFractionOfThreadsCount(Config.GetCompConveyorConfig().GetDefaultFractionOfThreadsCount());
            } else {
                protoWorkersPool.SetDefaultFractionOfThreadsCount(0.33);
            }
        } else {
            NKikimrConfig::TCompositeConveyorConfig::TCategory& protoCategory = *result.AddCategories();
            protoCategory.SetName(::ToString(NConveyorComposite::ESpecialTaskCategory::Compaction));
            NKikimrConfig::TCompositeConveyorConfig::TWorkersPool& protoWorkersPool = *result.AddWorkerPools();
            NKikimrConfig::TCompositeConveyorConfig::TWorkerPoolCategoryLink& protoLink = *protoWorkersPool.AddLinks();
            protoLink.SetCategory(::ToString(NConveyorComposite::ESpecialTaskCategory::Compaction));
            protoLink.SetWeight(1);
            protoWorkersPool.SetDefaultFractionOfThreadsCount(0.33);
            protoWorkersPool.SetMaxBatchSize(1);
        }

        if (Config.HasInsertConveyorConfig()) {
            NKikimrConfig::TCompositeConveyorConfig::TCategory& protoCategory = *result.AddCategories();
            protoCategory.SetName(::ToString(NConveyorComposite::ESpecialTaskCategory::Insert));
            NKikimrConfig::TCompositeConveyorConfig::TWorkersPool& protoWorkersPool = *result.AddWorkerPools();
            NKikimrConfig::TCompositeConveyorConfig::TWorkerPoolCategoryLink& protoLink = *protoWorkersPool.AddLinks();
            protoLink.SetCategory(::ToString(NConveyorComposite::ESpecialTaskCategory::Insert));
            protoLink.SetWeight(1);
            if (Config.GetInsertConveyorConfig().HasWorkersCountDouble()) {
                protoWorkersPool.SetWorkersCount(Config.GetInsertConveyorConfig().GetWorkersCountDouble());
            } else if (Config.GetInsertConveyorConfig().HasWorkersCount()) {
                protoWorkersPool.SetWorkersCount(Config.GetInsertConveyorConfig().GetWorkersCount());
            } else if (Config.GetCompConveyorConfig().HasDefaultFractionOfThreadsCount()) {
                protoWorkersPool.SetDefaultFractionOfThreadsCount(Config.GetCompConveyorConfig().GetDefaultFractionOfThreadsCount());
            } else {
                protoWorkersPool.SetDefaultFractionOfThreadsCount(0.2);
            }
        } else {
            NKikimrConfig::TCompositeConveyorConfig::TCategory& protoCategory = *result.AddCategories();
            protoCategory.SetName(::ToString(NConveyorComposite::ESpecialTaskCategory::Insert));
            NKikimrConfig::TCompositeConveyorConfig::TWorkersPool& protoWorkersPool = *result.AddWorkerPools();
            NKikimrConfig::TCompositeConveyorConfig::TWorkerPoolCategoryLink& protoLink = *protoWorkersPool.AddLinks();
            protoLink.SetCategory(::ToString(NConveyorComposite::ESpecialTaskCategory::Insert));
            protoLink.SetWeight(1);
            protoWorkersPool.SetDefaultFractionOfThreadsCount(0.2);
            protoWorkersPool.SetMaxBatchSize(1);
        }
        if (Config.HasScanConveyorConfig()) {
            NKikimrConfig::TCompositeConveyorConfig::TCategory& protoCategory = *result.AddCategories();
            protoCategory.SetName(::ToString(NConveyorComposite::ESpecialTaskCategory::Scan));
            NKikimrConfig::TCompositeConveyorConfig::TWorkersPool& protoWorkersPool = *result.AddWorkerPools();
            NKikimrConfig::TCompositeConveyorConfig::TWorkerPoolCategoryLink& protoLink = *protoWorkersPool.AddLinks();
            protoLink.SetCategory(::ToString(NConveyorComposite::ESpecialTaskCategory::Scan));
            protoLink.SetWeight(1);
            if (Config.GetScanConveyorConfig().HasWorkersCountDouble()) {
                protoWorkersPool.SetWorkersCount(Config.GetScanConveyorConfig().GetWorkersCountDouble());
            } else if (Config.GetScanConveyorConfig().HasWorkersCount()) {
                protoWorkersPool.SetWorkersCount(Config.GetScanConveyorConfig().GetWorkersCount());
            } else if (Config.GetCompConveyorConfig().HasDefaultFractionOfThreadsCount()) {
                protoWorkersPool.SetDefaultFractionOfThreadsCount(Config.GetCompConveyorConfig().GetDefaultFractionOfThreadsCount());
            } else {
                protoWorkersPool.SetDefaultFractionOfThreadsCount(0.4);
            }
        } else {
            NKikimrConfig::TCompositeConveyorConfig::TCategory& protoCategory = *result.AddCategories();
            protoCategory.SetName(::ToString(NConveyorComposite::ESpecialTaskCategory::Scan));
            NKikimrConfig::TCompositeConveyorConfig::TWorkersPool& protoWorkersPool = *result.AddWorkerPools();
            NKikimrConfig::TCompositeConveyorConfig::TWorkerPoolCategoryLink& protoLink = *protoWorkersPool.AddLinks();
            protoLink.SetCategory(::ToString(NConveyorComposite::ESpecialTaskCategory::Scan));
            protoLink.SetWeight(1);
            protoWorkersPool.SetDefaultFractionOfThreadsCount(0.4);
        }

        NKikimrConfig::TCompositeConveyorConfig::TCategory& protoCategory = *result.AddCategories();
        protoCategory.SetName(::ToString(NConveyorComposite::ESpecialTaskCategory::Deduplication));
        NKikimrConfig::TCompositeConveyorConfig::TWorkersPool& protoWorkersPool = *result.AddWorkerPools();
        NKikimrConfig::TCompositeConveyorConfig::TWorkerPoolCategoryLink& protoLink = *protoWorkersPool.AddLinks();
        protoLink.SetCategory(::ToString(NConveyorComposite::ESpecialTaskCategory::Deduplication));
        protoLink.SetWeight(1);
        protoWorkersPool.SetDefaultFractionOfThreadsCount(0.3);

        return result;
    }();

    auto serviceConfig = NConveyorComposite::NConfig::TConfig::BuildFromProto(protoConfig);
    if (serviceConfig.IsFail()) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("error", "cannot parse composite conveyor config")("action", "default_usage")(
            "error", serviceConfig.GetErrorMessage())("default", NConveyorComposite::NConfig::TConfig::BuildDefault().DebugString());
        serviceConfig = NConveyorComposite::NConfig::TConfig::BuildDefault();
    }
    AFL_VERIFY(!serviceConfig.IsFail());

    if (serviceConfig->IsEnabled()) {
        TIntrusivePtr<::NMonitoring::TDynamicCounters> tabletGroup = GetServiceCounters(appData->Counters, "tablets");
        TIntrusivePtr<::NMonitoring::TDynamicCounters> conveyorGroup = tabletGroup->GetSubgroup("type", "TX_COMPOSITE_CONVEYOR");

        const auto registerService = [&](const ui32 poolId, bool useBatchPool) {
            auto poolConveyorGroup = conveyorGroup->GetSubgroup("actor_system_pool_id", ::ToString(poolId));
            auto service = NConveyorComposite::CreateService(*serviceConfig, poolConveyorGroup);
            setup->LocalServices.push_back(std::make_pair(
                NConveyorComposite::TServiceOperator::MakeServiceId(NodeId, useBatchPool),
                TActorSetupCmd(service, TMailboxType::HTSwap, poolId)));
        };

        registerService(appData->UserPoolId, false);
        registerService(appData->BatchPoolId, true);
    }
}

} // namespace NKikimr::NKikimrServicesInitializers

#include "kikimr_services_initializers.h"
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
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/backup/controller/tablet.h>
#include <ydb/core/graph/api/shard.h>
#include <ydb/services/udf_store/compile_controller/compile_controller.h>
#include <ydb/core/load_test/nbs_dbg_like_load_tablet.h>

#if defined(YDB_EMBEDDED_NBS_ENABLED)
#include <ydb/core/nbs/cloud/blockstore/bootstrap/bootstrap.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/ss_proxy/ss_proxy.h>
#include <ydb/core/nbs/cloud/blockstore/config/protos/storage.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/volume/volume.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/partition_direct.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/dbs_controller/dbs_controller.h>
#endif

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

} // namespace NKikimr::NKikimrServicesInitializers

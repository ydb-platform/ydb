#include "schemeshard_impl.h"
#include "schemeshard__local_index_migration.h"
#include "schemeshard_svp_migration.h"

#include "olap/bg_tasks/adapter/adapter.h"
#include "olap/bg_tasks/events/global.h"
#include "olap/operations/local_index_helpers.h"
#include "schemeshard.h"
#include "schemeshard__root_shred_manager.h"
#include "schemeshard__tenant_shred_manager.h"
#include "schemeshard_svp_migration.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tx_processing.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/protos/auth.pb.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/core/protos/fs_settings.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/protos/schemeshard_config.pb.h>
#include <ydb/core/protos/table_stats.pb.h>  // for TStoragePoolsStats
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/service/service.h>
#include <ydb/core/sys_view/common/path.h>
#include <ydb/core/sys_view/common/resolver.h>
#include <ydb/core/sys_view/partition_stats/partition_stats.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/core/tablet_flat/bloom_filter_defaults.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/test_tablet/events.h>
#include <ydb/core/tx/columnshard/bg_tasks/events/events.h>
#include <ydb/core/tx/scheme_board/events_schemeshard.h>
#include <ydb/core/tx/schemeshard/schemeshard_path.h>
#include <ydb/core/tx/schemeshard/schemeshard_sysviews_update.h>

#include <ydb/library/login/account_lockout/account_lockout.h>
#include <ydb/library/login/password_checker/password_checker.h>

#include <yql/essentials/minikql/mkql_type_ops.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>

#include <util/random/random.h>
#include <util/system/byteorder.h>
#include <util/system/unaligned_mem.h>

namespace NKikimr {
namespace NSchemeShard {
void TSchemeShard::OnDetach(const TActorContext &ctx) {
    Die(ctx);
}

void TSchemeShard::OnTabletDead(TEvTablet::TEvTabletDead::TPtr &ev, const TActorContext &ctx) {
    Y_UNUSED(ev);
    Die(ctx);
}

void TSchemeShard::OnActivateExecutor(const TActorContext &ctx) {
    const TTabletId selfTabletId = SelfTabletId();

    const auto& selfDomain = GetDomainDescription(ctx);
    IsDomainSchemeShard = selfTabletId == TTabletId(selfDomain.SchemeRoot);
    InitState = TTenantInitState::Uninitialized;

    auto appData = AppData(ctx);
    Y_ABORT_UNLESS(appData);

    EnableBackgroundCompaction = appData->FeatureFlags.GetEnableBackgroundCompaction();
    EnableBackgroundCompactionServerless = appData->FeatureFlags.GetEnableBackgroundCompactionServerless();
    EnableBorrowedSplitCompaction = appData->FeatureFlags.GetEnableBorrowedSplitCompaction();
    EnableMoveIndex = appData->FeatureFlags.GetEnableMoveIndex();
    EnableAlterDatabaseCreateHiveFirst = appData->FeatureFlags.GetEnableAlterDatabaseCreateHiveFirst();
    EnableStatistics = appData->FeatureFlags.GetEnableStatistics();
    EnableServerlessExclusiveDynamicNodes = appData->FeatureFlags.GetEnableServerlessExclusiveDynamicNodes();
    EnableAddColumsWithDefaults = appData->FeatureFlags.GetEnableAddColumsWithDefaults();
    EnableReplaceIfExistsForExternalEntities = appData->FeatureFlags.GetEnableReplaceIfExistsForExternalEntities();
    EnableTempTables = appData->FeatureFlags.GetEnableTempTables();
    EnableInitialUniqueIndex = appData->FeatureFlags.GetEnableUniqConstraint();
    EnableAddUniqueIndex = appData->FeatureFlags.GetEnableAddUniqueIndex();
    EnableOnlineAddUniqueIndex = appData->FeatureFlags.GetEnableOnlineAddUniqueIndex();
    EnableFulltextIndex = appData->FeatureFlags.GetEnableFulltextIndex();
    EnableCompactFulltextIndex = appData->FeatureFlags.GetEnableCompactFulltextIndex();
    EnableJsonIndex = appData->FeatureFlags.GetEnableJsonIndex();
    EnableResourcePoolsOnServerless = appData->FeatureFlags.GetEnableResourcePoolsOnServerless();
    EnableExternalDataSourcesOnServerless = appData->FeatureFlags.GetEnableExternalDataSourcesOnServerless();
    EnableShred = appData->FeatureFlags.GetEnableDataErasure();
    EnableExternalSourceSchemaInference = appData->FeatureFlags.GetEnableExternalSourceSchemaInference();

    ConfigureCompactionQueues(appData->CompactionConfig, ctx);
    ConfigureStatsBatching(appData->SchemeShardConfig, ctx);
    ConfigureStatsOperations(appData->SchemeShardConfig, ctx);
    MaxCdcInitialScanShardsInFlight = appData->SchemeShardConfig.GetMaxCdcInitialScanShardsInFlight();
    MaxRestoreBuildIndexShardsInFlight = appData->SchemeShardConfig.GetMaxRestoreBuildIndexShardsInFlight();
    MaxBuildIndexShardsInFlight = appData->SchemeShardConfig.GetMaxBuildIndexShardsInFlight();
    MaxStoredIndexBuilds = appData->SchemeShardConfig.GetMaxStoredIndexBuilds();
    ConfigureCondErase(appData->SchemeShardConfig, ctx);

    SendStatsIntervalSecondsDedicated = appData->StatisticsConfig.GetBaseStatsSendIntervalSecondsDedicated();
    SendStatsIntervalSecondsServerless = appData->StatisticsConfig.GetBaseStatsSendIntervalSecondsServerless();

    ConfigureBackgroundCleaningQueue(appData->BackgroundCleaningConfig, ctx);
    ConfigureShredManager(appData->ShredConfig);
    ConfigureExternalSources(appData->QueryServiceConfig, ctx);

    if (appData->ChannelProfiles) {
        ChannelProfiles = appData->ChannelProfiles;
    }
    auto& icb = *appData->Icb;

    TControlBoard::RegisterSharedControl(AllowConditionalEraseOperations, icb.SchemeShardControls.AllowConditionalEraseOperations);
    TControlBoard::RegisterSharedControl(DisablePublicationsOfDropping, icb.SchemeShardControls.DisablePublicationsOfDropping);
    TControlBoard::RegisterSharedControl(FillAllocatePQ, icb.SchemeShardControls.FillAllocatePQ);
    TControlBoard::RegisterSharedControl(TolerateOrphanedPaths, icb.SchemeShardControls.TolerateOrphanedPaths);

    TControlBoard::RegisterSharedControl(MaxCommitRedoMB, icb.TabletControls.MaxCommitRedoMB);

    AllowDataColumnForIndexTable = appData->FeatureFlags.GetEnableDataColumnForIndexTable();
    TControlBoard::RegisterSharedControl(AllowDataColumnForIndexTable, icb.SchemeShardControls.AllowDataColumnForIndexTable);

    for (const auto& sid : appData->MeteringConfig.GetSystemBackupSIDs()) {
        SystemBackupSIDs.insert(sid);
    }

    AllowServerlessStorageBilling = appData->FeatureFlags.GetAllowServerlessStorageBillingForSchemeShard();
    TControlBoard::RegisterSharedControl(AllowServerlessStorageBilling, icb.SchemeShardControls.AllowServerlessStorageBilling);

    TxAllocatorClient = RegisterWithSameMailbox(CreateTxAllocatorClient(appData));

    SysPartitionStatsCollector = Register(NSysView::CreatePartitionStatsCollector().Release());

    SplitSettings.Register(appData->Icb);

    BackupSettings.Register(appData->Icb);

    IncrementalRestoreSettings.Register(appData->Icb);

    Executor()->RegisterExternalTabletCounters(TabletCountersPtr);
    Execute(CreateTxInitSchema(), ctx);

    SubscribeConsoleConfigs(ctx);
}

// This is overriden as noop in order to activate the table only at the end of Init transaction
// when all the in-mem state has been populated
void TSchemeShard::DefaultSignalTabletActive(const TActorContext &ctx) {
    Y_UNUSED(ctx);
}

void TSchemeShard::Cleanup(const TActorContext &ctx) {
    Y_UNUSED(ctx);
}

void TSchemeShard::Enqueue(STFUNC_SIG) {
    Y_FAIL_S("No enqueue method implemented."
              << " unhandled event type: " << ev->GetTypeRewrite()
             << " event: " << ev->ToString());

}

void TSchemeShard::StateInit(STFUNC_SIG) {
    TRACE_EVENT(NKikimrServices::FLAT_TX_SCHEMESHARD);
    switch (ev->GetTypeRewrite()) {
        HFuncTraced(TEvents::TEvUndelivered, Handle);

        //console configs
        HFuncTraced(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, Handle);
        HFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);
        HFunc(TEvPrivate::TEvConsoleConfigsTimeout, Handle);

        // These may arrive during StateInit after a reboot; re-dispatch happens in TTxInit.
        IgnoreFunc(TEvDataShard::TEvIncrementalRestoreShardProgress);
        IgnoreFunc(TEvTabletPipe::TEvClientConnected);
        IgnoreFunc(TEvTabletPipe::TEvClientDestroyed);

    default:
        StateInitImpl(ev, SelfId());
    }
}

void TSchemeShard::StateConfigure(STFUNC_SIG) {
    SelfPinger->OnAnyEvent(this->ActorContext());

    TRACE_EVENT(NKikimrServices::FLAT_TX_SCHEMESHARD);
    switch (ev->GetTypeRewrite()) {
        HFuncTraced(TEvents::TEvUndelivered, Handle);

        HFuncTraced(NKikimr::NOlap::NBackground::TEvExecuteGeneralLocalTransaction, Handle);
        HFuncTraced(NKikimr::NOlap::NBackground::TEvRemoveSession, Handle);
        HFuncTraced(TEvSchemeShard::TEvInitRootShard, Handle);
        HFuncTraced(TEvSchemeShard::TEvInitTenantSchemeShard, Handle);
        HFuncTraced(TEvSchemeShard::TEvMigrateSchemeShard, Handle);
        HFuncTraced(TEvSchemeShard::TEvPublishTenantAsReadOnly, Handle);

        HFuncTraced(TEvSchemeShard::TEvMeasureSelfResponseTime, SelfPinger->Handle);
        HFuncTraced(TEvSchemeShard::TEvWakeupToMeasureSelfResponseTime, SelfPinger->Handle);

        //operation initiate msg, must return error
        HFuncTraced(TEvSchemeShard::TEvModifySchemeTransaction, Handle);
        HFuncTraced(TEvSchemeShard::TEvDescribeScheme, Handle);
        HFuncTraced(TEvSchemeShard::TEvNotifyTxCompletion, Handle);
        HFuncTraced(TEvSchemeShard::TEvCancelTx, Handle);

        //pipes mgs
        HFuncTraced(TEvTabletPipe::TEvClientConnected, Handle);
        HFuncTraced(TEvTabletPipe::TEvClientDestroyed, Handle);
        HFuncTraced(TEvTabletPipe::TEvServerConnected, Handle);
        HFuncTraced(TEvTabletPipe::TEvServerDisconnected, Handle);

        //console configs
        HFuncTraced(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, Handle);
        HFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);
        HFunc(TEvPrivate::TEvConsoleConfigsTimeout, Handle);

    default:
        if (!HandleDefaultEvents(ev, SelfId())) {
            ALOG_WARN(NKikimrServices::FLAT_TX_SCHEMESHARD,
                       "StateConfigure:"
                           << " unhandled event type: " << ev->GetTypeRewrite()
                           << " event: " << ev->ToString());
        }
    }
}

void TSchemeShard::StateWork(STFUNC_SIG) {
    SelfPinger->OnAnyEvent(this->ActorContext());

    TRACE_EVENT(NKikimrServices::FLAT_TX_SCHEMESHARD);
    switch (ev->GetTypeRewrite()) {
        HFuncTraced(TEvents::TEvUndelivered, Handle);
        HFuncTraced(TEvSchemeShard::TEvInitRootShard, Handle);
        HFuncTraced(NKikimr::NOlap::NBackground::TEvExecuteGeneralLocalTransaction, Handle);
        HFuncTraced(NKikimr::NOlap::NBackground::TEvRemoveSession, Handle);

        HFuncTraced(TEvSchemeShard::TEvMeasureSelfResponseTime, SelfPinger->Handle);
        HFuncTraced(TEvSchemeShard::TEvWakeupToMeasureSelfResponseTime, SelfPinger->Handle);

        //operation initiate msg
        HFuncTraced(TEvSchemeShard::TEvModifySchemeTransaction, Handle);
        HFuncTraced(TEvSchemeShard::TEvDescribeScheme, Handle);
        HFuncTraced(TEvSchemeShard::TEvNotifyTxCompletion, Handle);
        HFuncTraced(TEvSchemeShard::TEvCancelTx, Handle);

        //operation schedule msg
        HFuncTraced(TEvPrivate::TEvProgressOperation, Handle);

        //coordination distributed transactions msg
        HFuncTraced(TEvTxProcessing::TEvPlanStep, Handle);

        //operations managed msg
        HFuncTraced(TEvHive::TEvCreateTabletReply, Handle);
        IgnoreFunc(TEvHive::TEvTabletCreationResult);
        HFuncTraced(TEvHive::TEvAdoptTabletReply, Handle);
        HFuncTraced(TEvHive::TEvDeleteTabletReply, Handle);
        HFuncTraced(TEvHive::TEvDeleteOwnerTabletsReply, Handle);
        HFuncTraced(TEvHive::TEvUpdateTabletsObjectReply, Handle);
        HFuncTraced(TEvHive::TEvUpdateDomainReply, Handle);

        HFuncTraced(TEvDataShard::TEvProposeTransactionResult, Handle);
        HFuncTraced(TEvDataShard::TEvSchemaChanged, Handle);
        HFuncTraced(TEvDataShard::TEvStateChanged, Handle);
        HFuncTraced(TEvDataShard::TEvInitSplitMergeDestinationAck, Handle);
        HFuncTraced(TEvDataShard::TEvSplitAck, Handle);
        HFuncTraced(TEvDataShard::TEvSplitPartitioningChangedAck, Handle);
        HFuncTraced(TEvDataShard::TEvPeriodicTableStats, Handle);
        HFuncTraced(TEvPersQueue::TEvPeriodicTopicStats, Handle);
        HFuncTraced(TEvDataShard::TEvGetTableStatsResult, Handle);

        //
        HFuncTraced(TEvColumnShard::TEvProposeTransactionResult, Handle);
        HFuncTraced(TEvColumnShard::TEvNotifyTxCompletionResult, Handle);

        // sequence shard
        HFuncTraced(NSequenceShard::TEvSequenceShard::TEvCreateSequenceResult, Handle);
        HFuncTraced(NSequenceShard::TEvSequenceShard::TEvDropSequenceResult, Handle);
        HFuncTraced(NSequenceShard::TEvSequenceShard::TEvUpdateSequenceResult, Handle);
        HFuncTraced(NSequenceShard::TEvSequenceShard::TEvFreezeSequenceResult, Handle);
        HFuncTraced(NSequenceShard::TEvSequenceShard::TEvRestoreSequenceResult, Handle);
        HFuncTraced(NSequenceShard::TEvSequenceShard::TEvRedirectSequenceResult, Handle);
        HFuncTraced(NSequenceShard::TEvSequenceShard::TEvGetSequenceResult, Handle);

        // replication
        HFuncTraced(NReplication::TEvController::TEvCreateReplicationResult, Handle);
        HFuncTraced(NReplication::TEvController::TEvAlterReplicationResult, Handle);
        HFuncTraced(NReplication::TEvController::TEvDropReplicationResult, Handle);

        // conditional erase
        HFuncTraced(TEvPrivate::TEvRunConditionalErase, Handle);
        HFuncTraced(TEvPrivate::TEvFlushConditionalEraseBatch, Handle);
        HFuncTraced(TEvDataShard::TEvConditionalEraseRowsResponse, Handle);

        HFuncTraced(TEvPrivate::TEvServerlessStorageBilling, Handle);
        HFuncTraced(TEvPrivate::TEvProgressTablePartitionsFormatSweep, Handle);

        HFuncTraced(NSysView::TEvSysView::TEvGetPartitionStats, Handle);

        HFuncTraced(TEvSubDomain::TEvConfigureStatus, Handle);
        HFuncTraced(TEvSchemeShard::TEvInitTenantSchemeShard, Handle);
        HFuncTraced(TEvSchemeShard::TEvInitTenantSchemeShardResult, Handle);
        HFuncTraced(TEvSchemeShard::TEvPublishTenantAsReadOnly, Handle);
        HFuncTraced(TEvSchemeShard::TEvPublishTenantAsReadOnlyResult, Handle);
        HFuncTraced(TEvSchemeShard::TEvPublishTenant, Handle);
        HFuncTraced(TEvSchemeShard::TEvPublishTenantResult, Handle);
        HFuncTraced(TEvSchemeShard::TEvMigrateSchemeShardResult, Handle);
        HFuncTraced(TEvDataShard::TEvMigrateSchemeShardResponse, Handle);
        HFuncTraced(TEvDataShard::TEvCompactTableResult, Handle);
        HFuncTraced(TEvDataShard::TEvCompactBorrowedResult, Handle);

        HFuncTraced(TEvSchemeShard::TEvSyncTenantSchemeShard, Handle);
        HFuncTraced(TEvSchemeShard::TEvProcessingRequest, Handle);

        HFuncTraced(TEvSchemeShard::TEvUpdateTenantSchemeShard, Handle);

        HFuncTraced(NSchemeBoard::NSchemeshardEvents::TEvUpdateAck, Handle);

        HFuncTraced(TEvBlockStore::TEvUpdateVolumeConfigResponse, Handle);
        HFuncTraced(TEvFileStore::TEvUpdateConfigResponse, Handle);
        HFuncTraced(NKesus::TEvKesus::TEvSetConfigResult, Handle);
        HFuncTraced(TEvPersQueue::TEvDropTabletReply, Handle);
        HFuncTraced(TEvPersQueue::TEvUpdateConfigResponse, Handle);
        HFuncTraced(TEvPersQueue::TEvProposeTransactionResult, Handle);
        HFuncTraced(TEvBlobDepot::TEvApplyConfigResult, Handle);

        //pipes mgs
        HFuncTraced(TEvTabletPipe::TEvClientConnected, Handle);
        HFuncTraced(TEvTabletPipe::TEvClientDestroyed, Handle);
        HFuncTraced(TEvTabletPipe::TEvServerConnected, Handle);
        HFuncTraced(TEvTabletPipe::TEvServerDisconnected, Handle);

        // namespace NExport {
        HFuncTraced(TEvExport::TEvCreateExportRequest, Handle);
        HFuncTraced(TEvExport::TEvGetExportRequest, Handle);
        HFuncTraced(TEvExport::TEvCancelExportRequest, Handle);
        HFuncTraced(TEvExport::TEvForgetExportRequest, Handle);
        HFuncTraced(TEvExport::TEvListExportsRequest, Handle);
        // } // NExport
        HFuncTraced(NBackground::TEvListRequest, Handle);
        HFuncTraced(TEvPrivate::TEvExportSchemeUploadResult, Handle);
        HFuncTraced(TEvPrivate::TEvExportUploadMetadataResult, Handle);

        // namespace NImport {
        HFuncTraced(TEvImport::TEvCreateImportRequest, Handle);
        HFuncTraced(TEvImport::TEvGetImportRequest, Handle);
        HFuncTraced(TEvImport::TEvCancelImportRequest, Handle);
        HFuncTraced(TEvImport::TEvForgetImportRequest, Handle);
        HFuncTraced(TEvImport::TEvListImportsRequest, Handle);
        HFuncTraced(TEvImport::TEvListObjectsInS3ExportRequest, Handle);
        HFuncTraced(TEvPrivate::TEvImportSchemeReady, Handle);
        HFuncTraced(TEvPrivate::TEvImportSchemaMappingReady, Handle);
        HFuncTraced(TEvPrivate::TEvImportSchemeQueryResult, Handle);
        // } // NImport

        // namespace NBackup {
        HFuncTraced(TEvBackup::TEvFetchBackupCollectionsRequest, Handle);
        HFuncTraced(TEvBackup::TEvListBackupCollectionsRequest, Handle);
        HFuncTraced(TEvBackup::TEvCreateBackupCollectionRequest, Handle);
        HFuncTraced(TEvBackup::TEvReadBackupCollectionRequest, Handle);
        HFuncTraced(TEvBackup::TEvUpdateBackupCollectionRequest, Handle);
        HFuncTraced(TEvBackup::TEvDeleteBackupCollectionRequest, Handle);

        HFuncTraced(TEvBackup::TEvGetIncrementalBackupRequest, Handle);
        HFuncTraced(TEvBackup::TEvForgetIncrementalBackupRequest, Handle);
        HFuncTraced(TEvBackup::TEvListIncrementalBackupsRequest, Handle);

        HFuncTraced(TEvBackup::TEvGetBackupCollectionRestoreRequest, Handle);
        HFuncTraced(TEvBackup::TEvForgetBackupCollectionRestoreRequest, Handle);
        HFuncTraced(TEvBackup::TEvListBackupCollectionRestoresRequest, Handle);

        HFuncTraced(TEvBackup::TEvGetFullBackupRequest, Handle);
        HFuncTraced(TEvBackup::TEvForgetFullBackupRequest, Handle);
        HFuncTraced(TEvBackup::TEvListFullBackupsRequest, Handle);
        HFuncTraced(TEvPrivate::TEvFullBackupItemDone, Handle);
        // } // NBackup


        //namespace NIndexBuilder {
        HFuncTraced(TEvSetColumnConstraint::TEvCreateRequest, Handle);
        HFuncTraced(TEvSetColumnConstraint::TEvGetRequest, Handle);
        HFuncTraced(TEvSetColumnConstraint::TEvListRequest, Handle);
        HFuncTraced(TEvSetColumnConstraint::TEvForgetRequest, Handle);
        HFuncTraced(TEvSetColumnConstraint::TEvCancelRequest, Handle);
        HFuncTraced(TEvDataShard::TEvValidateRowConditionResponse, Handle);
        HFuncTraced(TEvIndexBuilder::TEvCreateRequest, Handle);
        HFuncTraced(TEvIndexBuilder::TEvGetRequest, Handle);
        HFuncTraced(TEvIndexBuilder::TEvCancelRequest, Handle);
        HFuncTraced(TEvIndexBuilder::TEvForgetRequest, Handle);
        HFuncTraced(TEvIndexBuilder::TEvListRequest, Handle);
        HFuncTraced(TEvDataShard::TEvBuildIndexProgressResponse, Handle);
        HFuncTraced(TEvPrivate::TEvIndexBuildingMakeABill, Handle);
        HFuncTraced(TEvDataShard::TEvSampleKResponse, Handle);
        HFuncTraced(TEvDataShard::TEvReshuffleKMeansResponse, Handle);
        HFuncTraced(TEvDataShard::TEvRecomputeKMeansResponse, Handle);
        HFuncTraced(TEvDataShard::TEvFilterKMeansResponse, Handle);
        HFuncTraced(TEvDataShard::TEvLocalKMeansResponse, Handle);
        HFuncTraced(TEvDataShard::TEvPrefixKMeansResponse, Handle);
        HFuncTraced(TEvIndexBuilder::TEvUploadSampleKResponse, Handle);
        HFuncTraced(TEvDataShard::TEvValidateUniqueIndexResponse, Handle);
        HFuncTraced(TEvDataShard::TEvBuildFulltextIndexResponse, Handle);
        HFuncTraced(TEvDataShard::TEvBuildFulltextDictResponse, Handle);
        // } // NIndexBuilder

        // namespace NForcedCompaction {
        HFuncTraced(TEvForcedCompaction::TEvCreateRequest, Handle);
        HFuncTraced(TEvForcedCompaction::TEvGetRequest, Handle);
        HFuncTraced(TEvForcedCompaction::TEvCancelRequest, Handle);
        HFuncTraced(TEvForcedCompaction::TEvForgetRequest, Handle);
        HFuncTraced(TEvForcedCompaction::TEvListRequest, Handle);
        HFuncTraced(TEvPrivate::TEvProgressForcedCompaction, Handle);
        // } // NForcedCompaction

        //namespace NCdcStreamScan {
        HFuncTraced(TEvPrivate::TEvRunCdcStreamScan, Handle);
        HFuncTraced(TEvDataShard::TEvCdcStreamScanResponse, Handle);
        // } // NCdcStreamScan

        //namespace NIncrementalRestore {
        HFuncTraced(TEvPrivate::TEvRunIncrementalRestore, Handle);
        HFuncTraced(TEvPrivate::TEvProgressIncrementalRestore, Handle);
        HFuncTraced(TEvDataShard::TEvIncrementalRestoreShardProgress, Handle);
        // } // NIncrementalRestore

        // namespace NLongRunningCommon {
        HFuncTraced(TEvTxAllocatorClient::TEvAllocateResult, Handle);
        HFuncTraced(TEvSchemeShard::TEvModifySchemeTransactionResult, Handle);
        HFuncTraced(TEvIndexBuilder::TEvCreateResponse, Handle);
        HFuncTraced(TEvSchemeShard::TEvNotifyTxCompletionRegistered, Handle);
        HFuncTraced(TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
        HFuncTraced(TEvSchemeShard::TEvCancelTxResult, Handle);
        HFuncTraced(TEvIndexBuilder::TEvCancelResponse, Handle);
        // } // NLongRunningCommon

        //console configs
        HFuncTraced(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, Handle);
        HFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);
        HFunc(TEvPrivate::TEvConsoleConfigsTimeout, Handle);

        HFuncTraced(TEvSchemeShard::TEvFindTabletSubDomainPathId, Handle);

        IgnoreFunc(TEvTxProxy::TEvProposeTransactionStatus);

        HFuncTraced(TEvPrivate::TEvCleanDroppedPaths, Handle);
        HFuncTraced(TEvPrivate::TEvCleanDroppedSubDomains, Handle);
        HFuncTraced(TEvPrivate::TEvSubscribeToShardDeletion, Handle);
        HFuncTraced(TEvPrivate::TEvMoveShardToStoragePool, Handle);

        // Test-only notification
        IgnoreFunc(TEvPrivate::TEvTestNotifySubdomainCleanup);

        HFuncTraced(TEvPrivate::TEvPersistTableStats, Handle);
        HFuncTraced(TEvPrivate::TEvPersistTopicStats, Handle);

        HFuncTraced(TEvSchemeShard::TEvLogin, Handle);
        HFuncTraced(TEvSchemeShard::TEvListUsers, Handle);

        HFuncTraced(TEvDataShard::TEvProposeTransactionAttachResult, Handle);

        HFuncTraced(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
        HFuncTraced(TEvPrivate::TEvSendBaseStatsToSA, Handle);

        // for subscriptions on owners
        HFuncTraced(TEvInterconnect::TEvNodeDisconnected, Handle);
        HFuncTraced(TEvPrivate::TEvRetryNodeSubscribe, Handle);

        // shred
        HFuncTraced(TEvSchemeShard::TEvWakeupToRunShred, Handle);
        HFuncTraced(TEvSchemeShard::TEvTenantShredRequest, Handle);
        HFuncTraced(TEvDataShard::TEvVacuumResult, Handle);
        HFuncTraced(TEvKeyValue::TEvVacuumResponse, Handle);
        HFuncTraced(TEvPrivate::TEvAddNewShardToShred, Handle);
        HFuncTraced(TEvSchemeShard::TEvTenantShredResponse, Handle);
        HFuncTraced(TEvSchemeShard::TEvShredInfoRequest, Handle);
        HFuncTraced(TEvSchemeShard::TEvShredManualStartupRequest, Handle);
        HFuncTraced(TEvBlobStorage::TEvControllerShredResponse, Handle);
        HFuncTraced(TEvSchemeShard::TEvWakeupToRunShredBSC, Handle);

        HFuncTraced(NKikimr::NTestShard::TEvControlResponse, Handle);

        HFuncTraced(TEvPersQueue::TEvOffloadStatus, Handle);
        HFuncTraced(TEvPrivate::TEvContinuousBackupCleanerResult, Handle);

    default:
        if (!HandleDefaultEvents(ev, SelfId())) {
            ALOG_WARN(NKikimrServices::FLAT_TX_SCHEMESHARD,
                       "StateWork:"
                           << " unhandled event type: " << ev->GetTypeRewrite()
                           << " event: " << ev->ToString());
        }
        break;
    }
}

void TSchemeShard::BrokenState(STFUNC_SIG) {
    TRACE_EVENT(NKikimrServices::FLAT_TX_SCHEMESHARD);
    switch (ev->GetTypeRewrite()) {
        HFuncTraced(TEvTablet::TEvTabletDead, HandleTabletDead);
    default:
        if (!HandleDefaultEvents(ev, SelfId())) {
            ALOG_WARN(NKikimrServices::FLAT_TX_SCHEMESHARD,
                       "BrokenState:"
                           << " unhandled event type: " << ev->GetTypeRewrite()
                           << " event: " << ev->ToString());
        }
        break;
    }
}

void TSchemeShard::DeleteSplitOp(TOperationId operationId, TTxState& txState) {
    Y_ABORT_UNLESS(txState.ShardsInProgress.empty(), "All shards should have already completed their steps");

    TTableInfo::TPtr tableInfo = *Tables.FindPtr(txState.TargetPathId);
    Y_ABORT_UNLESS(tableInfo);
    tableInfo->FinishSplitMergeOp(operationId);
}

bool TSchemeShard::ShardIsUnderSplitMergeOp(const TShardIdx& idx) const {
    const TShardInfo* shardInfo = ShardInfos.FindPtr(idx);
    if (!shardInfo) {
        return false;
    }

    TTxId lastTxId = shardInfo->CurrentTxId;
    if (!lastTxId) {
        return false;
    }

    TTableInfo::TCPtr table = Tables.at(shardInfo->PathId);

    TOperationId lastOpId = TOperationId(lastTxId, 0);
    if (!TxInFlight.contains(lastOpId)) {
        Y_VERIFY_S(!table->IsShardInSplitMergeOp(idx),
                   "shardIdx: " << idx
                   << " pathId: " << shardInfo->PathId);
        return false;
    }

    Y_VERIFY_S(table->IsShardInSplitMergeOp(idx),
               "shardIdx: " << idx
               << " pathId: " << shardInfo->PathId
               << " lastOpId: " << lastOpId);
    return true;
}

TTxState &TSchemeShard::CreateTx(TOperationId opId, TTxState::ETxType txType, TPathId targetPath, TPathId sourcePath) {
    Y_VERIFY_S(!TxInFlight.contains(opId),
               "Trying to create duplicate Tx " << opId);
    TTxState& txState = TxInFlight[opId];
    txState = TTxState(txType, targetPath, sourcePath);
    TabletCounters->Simple()[TxTypeInFlightCounter(txType)].Add(1);
    IncrementPathDbRefCount(targetPath, "transaction target path");
    if (sourcePath) {
        IncrementPathDbRefCount(sourcePath, "transaction source path");
    }
    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "CreateTx for txid " << opId
                    << " type: " << TTxState::TypeName(txType)
                    << " target path: " << targetPath
                    << " source path: " << sourcePath);
    return txState;
}

TTxState *TSchemeShard::FindTx(TOperationId opId) {
    TTxState* txState = TxInFlight.FindPtr(opId);
    return txState;
}

TTxState* TSchemeShard::FindTxSafe(TOperationId opId, const TTxState::ETxType& txType) {
    TTxState* txState = FindTx(opId);
    Y_ABORT_UNLESS(txState);
    Y_ABORT_UNLESS(txState->TxType == txType);
    return txState;
}

void TSchemeShard::RemoveTx(const TActorContext &ctx, NIceDb::TNiceDb &db, TOperationId opId, TTxState *txState) {
    if (!txState) {
        txState = TxInFlight.FindPtr(opId);
    }
    if (!txState) {
        return;
    }

    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "RemoveTx for txid " << opId);
    auto pathId = txState->TargetPathId;

    PersistRemoveTx(db, opId, *txState);
    TabletCounters->Simple()[TxTypeInFlightCounter(txState->TxType)].Sub(1);

    if (txState->IsItActuallyMerge()) {
        TabletCounters->Cumulative()[TxTypeFinishedCounter(TTxState::TxMergeTablePartition)].Increment(1);
    } else {
        TabletCounters->Cumulative()[TxTypeFinishedCounter(txState->TxType)].Increment(1);
    }

    DecrementPathDbRefCount(pathId, "remove txstate target path");
    if (txState->SourcePathId) {
        DecrementPathDbRefCount(txState->SourcePathId, "remove txstate source path");
    }

    // Check if this operation is part of an incremental restore and notify completion
    if (TxIdToIncrementalRestore.contains(opId.GetTxId())) {
        NotifyIncrementalRestoreOperationCompleted(opId, ctx);
    }

    TxInFlight.erase(opId); // must be called last, erases txState invalidating txState ptr
}

TMaybe<NKikimrSchemeOp::TPartitionConfig> TSchemeShard::GetTablePartitionConfigWithAlterData(TPathId pathId) const {
    Y_VERIFY_S(PathsById.contains(pathId), "Unknown pathId " << pathId);
    auto pTable = Tables.FindPtr(pathId);
    if (pTable) {
        TTableInfo::TPtr table = *pTable;
        if (table->AlterData) {
            return table->AlterData->PartitionConfigCompatible();
        }
        return table->PartitionConfig();
    }
    return Nothing();
}

void TSchemeShard::ExamineTreeVFS(TPathId nodeId, std::function<void (TPathElement::TPtr)> func, const TActorContext &ctx) {
    TPathElement::TPtr node = PathsById.at(nodeId);
    Y_ABORT_UNLESS(node);

    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "ExamineTreeVFS visit path id " << nodeId <<
                " name: " << node->Name <<
                " type: " << NKikimrSchemeOp::EPathType_Name(node->PathType) <<
                " state: " << NKikimrSchemeOp::EPathState_Name(node->PathState) <<
                " stepDropped: " << node->StepDropped <<
                " droppedTxId: " << node->DropTxId <<
                " parent: " << node->ParentPathId);

    // node dropped and no hidden tx is in fly
    if (node->Dropped() && !Operations.contains(node->DropTxId)) {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "ExamineTreeVFS skip path id " << nodeId);

        if (node->IsTable()) { //lets check indexes
            for (auto childrenIt: node->GetChildren()) {
                ExamineTreeVFS(childrenIt.second, func, ctx);
            }
        }
        return;
    }

    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "ExamineTreeVFS run path id: " << nodeId);

    func(node);

    for (auto childrenIt: node->GetChildren()) {
        ExamineTreeVFS(childrenIt.second, func, ctx);
    }
}

THashSet<TPathId> TSchemeShard::ListSubTree(TPathId subdomain_root, const TActorContext &ctx) {
    THashSet<TPathId> paths;

    auto savePath = [&] (TPathElement::TPtr node) {
        paths.insert(node->PathId);
    };

    ExamineTreeVFS(subdomain_root, savePath, ctx);

    return paths;
}

THashSet<TTxId> TSchemeShard::GetRelatedTransactions(const THashSet<TPathId> &paths, const TActorContext &ctx) {
    Y_UNUSED(ctx);
    THashSet<TTxId> transactions;

    for (const auto& txInFly: TxInFlight) {
        TOperationId opId = txInFly.first;
        const TTxState& state = txInFly.second;

        if (!paths.contains(state.TargetPathId)) {
            continue;
        }

        transactions.insert(opId.GetTxId());
    }

    return transactions;
}

THashSet<TShardIdx> TSchemeShard::CollectAllShards(const THashSet<TPathId> &paths) const {
    THashSet<TShardIdx> shards;

    for (const auto& shardItem: ShardInfos) {
        auto idx = shardItem.first;
        const TShardInfo& info = shardItem.second;

        if (!paths.contains(info.PathId)) {
            continue;
        }

        shards.insert(idx);
    }

    for (const auto& pathId: paths) {
        Y_ABORT_UNLESS(PathsById.contains(pathId));
        TPathElement::TPtr path = PathsById.at(pathId);
        if (!path->IsSubDomainRoot()) {
            continue;
        }
        TSubDomainInfo::TPtr domainInfo = SubDomains.at(pathId);
        const auto& domainShards = domainInfo->GetInternalShards();
        shards.insert(domainShards.begin(), domainShards.end());
    }

    return shards;
}

void TSchemeShard::UncountNode(TPathElement::TPtr node) {
    const auto isBackupTable = IsBackupTable(node->PathId);
    EPathCategory pathCategory;
    if (isBackupTable) {
        pathCategory = EPathCategory::Backup;
    } else if (node->IsSystemDirectory() || node->IsSysView()) {
        pathCategory = EPathCategory::System;
    } else {
        pathCategory = EPathCategory::Regular;
    }

    if (node->IsDomainRoot()) {
        ResolveDomainInfo(node->ParentPathId)->DecPathsInside(this, 1, pathCategory);
    } else {
        ResolveDomainInfo(node)->DecPathsInside(this, 1, pathCategory);
    }
    PathsById.at(node->ParentPathId)->DecAliveChildrenPrivate(isBackupTable);

    TabletCounters->Simple()[COUNTER_USER_ATTRIBUTES_COUNT].Sub(node->UserAttrs->Size());

    switch (node->PathType) {
    case TPathElement::EPathType::EPathTypeDir:
        TabletCounters->Simple()[COUNTER_DIR_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeRtmrVolume:
        TabletCounters->Simple()[COUNTER_RTMR_VOLUME_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeTable:
        TabletCounters->Simple()[COUNTER_TABLE_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypePersQueueGroup:
        TabletCounters->Simple()[COUNTER_PQ_GROUP_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeSubDomain:
        TabletCounters->Simple()[COUNTER_SUB_DOMAIN_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeExtSubDomain:
        TabletCounters->Simple()[COUNTER_EXTSUB_DOMAIN_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeBlockStoreVolume:
        TabletCounters->Simple()[COUNTER_BLOCKSTORE_VOLUME_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeFileStore:
        TabletCounters->Simple()[COUNTER_FILESTORE_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeKesus:
        TabletCounters->Simple()[COUNTER_KESUS_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeSolomonVolume:
        TabletCounters->Simple()[COUNTER_SOLOMON_VOLUME_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeTableIndex:
        TabletCounters->Simple()[COUNTER_TABLE_INDEXES_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeColumnStore:
    case TPathElement::EPathType::EPathTypeColumnTable:
        // TODO
        break;
    case TPathElement::EPathType::EPathTypeCdcStream:
        TabletCounters->Simple()[COUNTER_CDC_STREAMS_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeSequence:
        TabletCounters->Simple()[COUNTER_SEQUENCE_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeReplication:
        TabletCounters->Simple()[COUNTER_REPLICATION_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeTransfer:
        TabletCounters->Simple()[COUNTER_TRANSFER_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeBlobDepot:
        TabletCounters->Simple()[COUNTER_BLOB_DEPOT_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeExternalTable:
        TabletCounters->Simple()[COUNTER_EXTERNAL_TABLE_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeExternalDataSource:
        TabletCounters->Simple()[COUNTER_EXTERNAL_DATA_SOURCE_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeView:
        TabletCounters->Simple()[COUNTER_VIEW_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeResourcePool:
        TabletCounters->Simple()[COUNTER_RESOURCE_POOL_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeBackupCollection:
        TabletCounters->Simple()[COUNTER_BACKUP_COLLECTION_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeSysView:
        TabletCounters->Simple()[COUNTER_SYS_VIEW_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeSecret:
        TabletCounters->Simple()[COUNTER_SECRET_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeStreamingQuery:
        TabletCounters->Simple()[COUNTER_STREAMING_QUERY_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeTestShardSet:
        TabletCounters->Simple()[COUNTER_TEST_SHARD_SET_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeInvalid:
        Y_ABORT("impossible path type");
    }
}

void TSchemeShard::MarkAsMigrated(TPathElement::TPtr node, const TActorContext &ctx) {
    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "Mark as Migrated path id " << node->PathId);

    Y_ABORT_UNLESS(!node->Dropped());
    Y_VERIFY_S(PathsById.contains(ResolvePathIdForDomain(node)),
               "details:"
                   << " node->PathId: " << node->PathId
                   << ", node->DomainPathId: " << node->DomainPathId);

    Y_VERIFY_S(PathsById.at(ResolvePathIdForDomain(node))->IsExternalSubDomainRoot(),
               "details:"
                   << " pathId: " << ResolvePathIdForDomain(node)
                   << ", pathType: " << NKikimrSchemeOp::EPathType_Name(PathsById.at(ResolvePathIdForDomain(node))->PathType));

    node->PathState = TPathElement::EPathState::EPathStateMigrated;

    UncountNode(node);
}

void TSchemeShard::MarkAsDropping(TPathElement::TPtr node, TTxId txId, const TActorContext &ctx) {
    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Mark as Dropping path id " << node->PathId <<
                " by tx: " << txId);
    if (!node->Dropped()) {
        node->PathState = TPathElement::EPathState::EPathStateDrop;
        node->DropTxId = txId;
    }
}

void TSchemeShard::MarkAsDropping(const THashSet<TPathId> &paths, TTxId txId, const TActorContext &ctx) {
    for (auto id: paths) {
        MarkAsDropping(PathsById.at(id), txId, ctx);
    }
}

void TSchemeShard::DropNode(TPathElement::TPtr node, TStepId step, TTxId txId, NIceDb::TNiceDb &db, const TActorContext &ctx) {
    Y_VERIFY_S(node->PathState == TPathElement::EPathState::EPathStateDrop
               || node->IsMigrated(),
               "path id: " << node->PathId <<
                   " type: " << NKikimrSchemeOp::EPathType_Name(node->PathType) <<
                   " state: " << NKikimrSchemeOp::EPathState_Name(node->PathState) <<
                   " txid: " << txId);

    Y_ABORT_UNLESS(node->DropTxId == txId || node->IsMigrated());

    if (!node->IsMigrated()) {
        UncountNode(node);
    }

    node->SetDropped(step, txId);
    PersistDropStep(db, node->PathId, node->StepDropped, TOperationId(node->DropTxId, 0));

    switch (node->PathType) {
        case TPathElement::EPathType::EPathTypeTable:
            PersistRemoveTable(db, node->PathId, ctx);
            break;
        case TPathElement::EPathType::EPathTypeTableIndex:
            PersistRemoveTableIndex(db, node->PathId);
            break;
        case TPathElement::EPathType::EPathTypePersQueueGroup:
            PersistRemovePersQueueGroup(db, node->PathId);
            break;
        case TPathElement::EPathType::EPathTypeBlockStoreVolume:
            PersistRemoveBlockStoreVolume(db, node->PathId);
            break;
        case TPathElement::EPathType::EPathTypeFileStore:
            PersistRemoveFileStoreInfo(db, node->PathId);
            break;
        case TPathElement::EPathType::EPathTypeKesus:
            PersistRemoveKesusInfo(db, node->PathId);
            break;
        case TPathElement::EPathType::EPathTypeSolomonVolume:
            PersistRemoveSolomonVolume(db, node->PathId);
            break;
        case TPathElement::EPathType::EPathTypeColumnStore:
            PersistOlapStoreRemove(db, node->PathId);
            break;
        case TPathElement::EPathType::EPathTypeColumnTable:
            PersistColumnTableRemove(db, node->PathId, ctx);
            break;
        case TPathElement::EPathType::EPathTypeSubDomain:
        case TPathElement::EPathType::EPathTypeExtSubDomain:
            // N.B. we must not remove subdomains as part of a tree drop
            // SubDomains must be removed when there are no longer
            // any references to them, e.g. all shards have been deleted
            // and all operations have been completed.
            break;
        case TPathElement::EPathType::EPathTypeBlobDepot:
            Y_ABORT("not implemented");
        case TPathElement::EPathType::EPathTypeTestShardSet:
            PersistRemoveTestShardSet(db, node->PathId);
            break;
        default:
            // not all path types support removal
            break;
    }

    PersistUserAttributes(db, node->PathId, node->UserAttrs, nullptr);
}

void TSchemeShard::DropPaths(const THashSet<TPathId> &paths, TStepId step, TTxId txId, NIceDb::TNiceDb &db, const TActorContext &ctx) {
    for (auto id: paths) {
        DropNode(PathsById.at(id), step, txId, db, ctx);
    }
}

TString TSchemeShard::FillBackupTxBody(TPathId pathId, const NKikimrSchemeOp::TBackupTask& task, ui32 shardNum, TMessageSeqNo seqNo) const
{
    NKikimrTxDataShard::TFlatSchemeTransaction tx;
    FillSeqNo(tx, seqNo);
    auto backup = tx.MutableBackup();
    backup->CopyFrom(task);
    backup->SetTableId(pathId.LocalPathId);
    backup->SetShardNum(shardNum);

    TString txBody;
    Y_PROTOBUF_SUPPRESS_NODISCARD tx.SerializeToString(&txBody);
    return txBody;
}

void TSchemeShard::Handle(TEvDataShard::TEvCompactTableResult::TPtr &ev, const TActorContext &ctx) {
    switch (static_cast<ECompactionType>(ev->Cookie)) {
    case ECompactionType::Unspecified: // Backward compatibility for 0, handle like both Background and Forced.
        // For background compaction, it doesn't matter who started compaction.
        HandleBackgroundCompactionResult(ev, ctx);
        // For forced compaction, shards without a known compact operation will be ignored.
        HandleForcedCompactionResult(ev, ctx);
        break;
    case ECompactionType::Background:
        HandleBackgroundCompactionResult(ev, ctx);
        break;
    case ECompactionType::Forced:
        HandleForcedCompactionResult(ev, ctx);
        break;
    default:
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "Got TEvDataShard::TEvCompactTableResult with unknown cookie# " << ev->Cookie
            << ", tabletId# " << ev->Get()->Record.GetTabletId()
            << ", at schemeshard# " << TabletID());
        break;
    }
}

void TSchemeShard::Handle(TEvDataShard::TEvSchemaChanged::TPtr& ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvSchemaChanged"
                    << ", tabletId: " << TabletID()
                    << ", at schemeshard: " << TabletID()
                    << ", message: " << ev->Get()->Record.ShortDebugString());

    const auto txId = TTxId(ev->Get()->Record.GetTxId());
    const auto tableId = TTabletId(ev->Get()->Record.GetOrigin());

    TActorId ackTo = ev->Get()->GetSource();

    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvDataShard::TEvSchemaChanged"
                   << " for unknown txId " <<  txId
                   << " message# " << ev->Get()->Record.DebugString());

        auto event = MakeHolder<TEvDataShard::TEvSchemaChangedResult>(ui64(txId));
        ctx.Send(ackTo, event.Release());
        return;
    }

    auto partId = Operations.at(txId)->FindRelatedPartByTabletId(tableId, ctx);
    if (partId == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvDataShard::TEvSchemaChanged"
                   << " for unknown part in txId: " <<  txId
                   << " message# " << ev->Get()->Record.DebugString());

        auto event = MakeHolder<TEvDataShard::TEvSchemaChangedResult>(ui64(txId));
        ctx.Send(ackTo, event.Release());
        return;
    }

    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(TEvDataShard::TEvStateChanged::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvStateChanged"
                    << ", at schemeshard: " << TabletID()
                    << ", message: " << ev->Get()->Record.ShortDebugString());

    Execute(CreateTxShardStateChanged(ev), ctx);
}


void TSchemeShard::Handle(TEvDataShard::TEvInitSplitMergeDestinationAck::TPtr& ev, const TActorContext& ctx) {
    const auto txId = TTxId(ev->Get()->Record.GetOperationCookie());
    const auto tabletId = TTabletId(ev->Get()->Record.GetTabletId());
    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got InitSplitMergeDestinationAck"
                   << " for unknown txId " << txId
                   << " datashard " << tabletId);
        return;
    }

    Execute(CreateTxOperationReply(TOperationId(txId, 0), ev), ctx);
}

void TSchemeShard::Handle(TEvDataShard::TEvSplitAck::TPtr& ev, const TActorContext& ctx) {
    const auto txId = TTxId(ev->Get()->Record.GetOperationCookie());
    const auto tabletId = TTabletId(ev->Get()->Record.GetTabletId());
    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got SplitAck"
                   << " for unknown txId " << txId
                   << " datashard " << tabletId);
        return;
    }

    Execute(CreateTxOperationReply(TOperationId(txId, 0), ev), ctx);
}

void TSchemeShard::Handle(TEvDataShard::TEvSplitPartitioningChangedAck::TPtr& ev, const TActorContext& ctx) {
    const auto txId = TTxId(ev->Get()->Record.GetOperationCookie());
    const auto tabletId = TTabletId(ev->Get()->Record.GetTabletId());
    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvSplitPartitioningChangedAck"
                   << " for unknown txId " << txId
                   << " datashard " << tabletId);
        return;
    }

    Execute(CreateTxOperationReply(TOperationId(txId, 0), ev), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvDescribeScheme::TPtr &ev, const TActorContext &ctx) {
    Execute(CreateTxDescribeScheme(ev), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvNotifyTxCompletion::TPtr &ev, const TActorContext &ctx) {
    Execute(CreateTxNotifyTxCompletion(ev), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvInitRootShard::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxInitRootCompatibility(ev), ctx);
}

void TSchemeShard::Handle(NKikimr::NOlap::NBackground::TEvRemoveSession::TPtr& ev, const TActorContext& ctx) {
    auto txRemove = BackgroundSessionsManager->TxRemove(ev->Get()->GetClassName(), ev->Get()->GetIdentifier());
    AFL_VERIFY(!!txRemove);
    Execute(txRemove.release(), ctx);
}

void TSchemeShard::Handle(NKikimr::NOlap::NBackground::TEvExecuteGeneralLocalTransaction::TPtr& ev, const TActorContext& ctx) {
    Execute(ev->Get()->ExtractTransaction().release(), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvInitTenantSchemeShard::TPtr &ev, const TActorContext &ctx) {
    Execute(CreateTxInitTenantSchemeShard(ev), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvModifySchemeTransaction::TPtr &ev, const TActorContext &ctx) {
    if (IsReadOnlyMode) {
        ui64 txId = ev->Get()->Record.GetTxId();
        ui64 selfId = TabletID();
        auto result = MakeHolder<TEvSchemeShard::TEvModifySchemeTransactionResult>(
            NKikimrScheme::StatusReadOnly, txId, selfId, "Schema is in ReadOnly mode");

        ctx.Send(ev->Sender, result.Release());

        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Schema modification rejected because of ReadOnly mode"
                       << ", at tablet: " << selfId
                       << " txid: " << txId);
        return;
    }

    Execute(CreateTxOperationPropose(ev), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvProcessingRequest::TPtr& ev, const TActorContext& ctx) {
    const auto processor = ev->Get()->RestoreProcessor();
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "TSchemeShard::Handle"
        << ", at schemeshard: " << TabletID()
        << ", processor: " << (processor ? processor->DebugString() : "nullptr"));
    if (processor) {
        NKikimrScheme::TEvProcessingResponse result;
        processor->Process(*this, result);
        ctx.Send(ev->Sender, new TEvSchemeShard::TEvProcessingResponse(result));
    } else {
        ctx.Send(ev->Sender, new TEvSchemeShard::TEvProcessingResponse("cannot restore processor: " + ev->Get()->Record.GetClassName()));
    }
}

void TSchemeShard::Handle(TEvPrivate::TEvProgressOperation::TPtr &ev, const TActorContext &ctx) {
    const auto txId = TTxId(ev->Get()->TxId);
    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvPrivate::TEvProgressOperation"
                   << " for unknown txId " << txId);
        return;
    }

    Y_ABORT_UNLESS(ev->Get()->TxPartId != InvalidSubTxId);
    Execute(CreateTxOperationProgress(TOperationId(txId, ev->Get()->TxPartId)), ctx);
}

void TSchemeShard::Handle(TEvDataShard::TEvProposeTransactionAttachResult::TPtr& ev, const TActorContext& ctx)
{
    const auto txId = TTxId(ev->Get()->Record.GetTxId());
    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvDataShard::TEvProposeTransactionAttachResult"
                   << " for unknown txId: " << txId
                   << " message: " << ev->Get()->Record.ShortDebugString());
        return;
    }

    auto tabletId = TTabletId(ev->Get()->Record.GetTabletId());
    TSubTxId partId = Operations.at(txId)->FindRelatedPartByTabletId(tabletId, ctx);
    if (partId == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvDataShard::TEvProposeTransactionAttachResult but partId is unknown"
                       << ", for txId: " << txId
                       << ", tabletId: " << tabletId
                       << ", at schemeshard: " << TabletID());
        return;
    }

    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx) {
    const auto tabletId = TTabletId(ev->Get()->TabletId);
    const TActorId clientId = ev->Get()->ClientId;

    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvClientConnected"
                    << ", tabletId: " << tabletId
                    << ", status: " << NKikimrProto::EReplyStatus_Name(ev->Get()->Status)
                    << ", at schemeshard: " << TabletID());

    Y_ABORT_UNLESS(ev->Get()->Leader);

    if (PipeClientCache->OnConnect(ev)) {
        return; //all Ok
    }

    if (IndexBuildPipes.Has(clientId)) {
        Execute(CreatePipeRetry(IndexBuildPipes.GetOwnerId(clientId), IndexBuildPipes.GetTabletId(clientId)), ctx);
        return;
    }

    if (SetColumnConstraintPipes.Has(clientId)) {
        Execute(CreatePipeRetrySetColumnConstraint(SetColumnConstraintPipes.GetOwnerId(clientId), SetColumnConstraintPipes.GetTabletId(clientId)), ctx);
        return;
    }

    if (IncrementalRestorePipes.Has(clientId)) {
        RetryIncrementalRestorePipe(IncrementalRestorePipes.GetOwnerId(clientId),
                                    IncrementalRestorePipes.GetTabletId(clientId), ctx);
        return;
    }

    if (CdcStreamScanPipes.Has(clientId)) {
        Execute(CreatePipeRetry(CdcStreamScanPipes.GetOwnerId(clientId), CdcStreamScanPipes.GetTabletId(clientId)), ctx);
        return;
    }

    if (ShardDeleter.Has(tabletId, clientId)) {
        ShardDeleter.ResendDeleteRequests(TTabletId(ev->Get()->TabletId), ShardInfos, ctx);
        return;
    }

    if (ParentDomainLink.HasPipeTo(tabletId, clientId)) {
        ParentDomainLink.AtPipeError(ctx);
        return;
    }

    if (clientId == SAPipeClientId) {
        ConnectToSA();
        return;
    }

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "Failed to connect"
                   << ", to tablet: " << tabletId
                   << ", at schemeshard: " << TabletID());

    BorrowedCompactionHandleDisconnect(tabletId, clientId);
    ConditionalEraseHandleDisconnect(tabletId, clientId, ctx);
    if (IsDomainSchemeShard) {
        RootShredManager->HandleDisconnect(tabletId, clientId, ctx);
    }
    TenantShredManager->HandleDisconnect(tabletId, clientId, ctx);
    RestartPipeTx(tabletId, ctx);
}

void TSchemeShard::Handle(TEvTabletPipe::TEvServerConnected::TPtr &ev, const TActorContext &ctx) {
    Y_UNUSED(ev);
    Y_UNUSED(ctx);

    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Pipe server connected"
                    << ", at tablet: " << ev->Get()->TabletId);
}

void TSchemeShard::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx) {
    const auto tabletId = TTabletId(ev->Get()->TabletId);
    const TActorId clientId = ev->Get()->ClientId;

    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Client pipe"
                    << ", to tablet: " << tabletId
                    << ", from:" << TabletID() << " is reset");

    PipeClientCache->OnDisconnect(ev);

    if (IndexBuildPipes.Has(clientId)) {
        Execute(CreatePipeRetry(IndexBuildPipes.GetOwnerId(clientId), IndexBuildPipes.GetTabletId(clientId)), ctx);
        return;
    }

    if (SetColumnConstraintPipes.Has(clientId)) {
        Execute(CreatePipeRetrySetColumnConstraint(SetColumnConstraintPipes.GetOwnerId(clientId), SetColumnConstraintPipes.GetTabletId(clientId)), ctx);
        return;
    }

    if (IncrementalRestorePipes.Has(clientId)) {
        RetryIncrementalRestorePipe(IncrementalRestorePipes.GetOwnerId(clientId),
                                    IncrementalRestorePipes.GetTabletId(clientId), ctx);
        return;
    }

    if (CdcStreamScanPipes.Has(clientId)) {
        Execute(CreatePipeRetry(CdcStreamScanPipes.GetOwnerId(clientId), CdcStreamScanPipes.GetTabletId(clientId)), ctx);
        return;
    }

    if (ShardDeleter.Has(tabletId, clientId)) {
        ShardDeleter.ResendDeleteRequests(tabletId, ShardInfos, ctx);
        return;
    }

    if (ParentDomainLink.HasPipeTo(tabletId, clientId)) {
        ParentDomainLink.AtPipeError(ctx);
        return;
    }

    if (clientId == SAPipeClientId) {
        ConnectToSA();
        return;
    }

    BorrowedCompactionHandleDisconnect(tabletId, clientId);
    ConditionalEraseHandleDisconnect(tabletId, clientId, ctx);
    if (IsDomainSchemeShard) {
        RootShredManager->HandleDisconnect(tabletId, clientId, ctx);
    }
    TenantShredManager->HandleDisconnect(tabletId, clientId, ctx);
    RestartPipeTx(tabletId, ctx);
}

void TSchemeShard::Handle(TEvTabletPipe::TEvServerDisconnected::TPtr &, const TActorContext &ctx) {
    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Server pipe is reset"
                    << ", at schemeshard: " << TabletID());
}

void TSchemeShard::Handle(TEvSchemeShard::TEvSyncTenantSchemeShard::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Handle TEvSyncTenantSchemeShard, at schemeshard: " << TabletID()
        << ", msg: " << record.ShortDebugString()
    );
    Y_VERIFY_S(IsDomainSchemeShard, "unexpected message: schemeshard: " << TabletID() << " mgs: " << record.DebugString());

    const TPathId pathId(record.GetDomainSchemeShard(), record.GetDomainPathId());

    if (!SubDomains.contains(pathId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Handle TEvSyncTenantSchemeShard, at schemeshard: " << TabletID()
            << ", ignore spurious message from dropped subdomain's schemeshard (partial cleanup)" << pathId
        );
        return;
    }

    if (!PathsById.contains(pathId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Handle TEvSyncTenantSchemeShard, at schemeshard: " << TabletID()
            << ", ignore spurious message from dropped subdomain's schemeshard (full cleanup)" << pathId
        );
        return;
    }

    if (PathsById.at(pathId)->Dropped()) {
        // This could happen when root schemeshard reboots just after marking subdomain's path as dropped
        // but before being able to begin subdomain cleanup. Then, if tenant schemeshard tablet is still alive,
        // it will detect disconnect error in pipe-to-parent, re-establish connection and send TEvSyncTenantSchemeShard.
        // Root schemeshard should ignore it and should not register dropped subdomain in subdomain links again.
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Handle TEvSyncTenantSchemeShard, at schemeshard: " << TabletID()
            << ", ignore spurious message from dropped subdomain's schemeshard (pre cleanup)" << pathId
        );
        return;
    }

    if (SubDomainsLinks.Sync(ev, ctx)) {
        Execute(CreateTxSyncTenant(pathId), ctx);
    }
}

void TSchemeShard::Handle(TEvSchemeShard::TEvUpdateTenantSchemeShard::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "Handle TEvUpdateTenantSchemeShard"
                   << ", at schemeshard: " << TabletID()
                   << ", msg: " << record.ShortDebugString());
    Y_VERIFY_S(!IsDomainSchemeShard, "unexpected message: schemeshard: " << TabletID() << " mgs: " << record.DebugString());

    Execute(CreateTxUpdateTenant(ev), ctx);
}

void TSchemeShard::Handle(NSchemeBoard::NSchemeshardEvents::TEvUpdateAck::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "Handle TEvUpdateAck"
                   << ", at schemeshard: " << TabletID()
                   << ", msg: " << record.ShortDebugString()
                   << ", cookie: " << ev->Cookie);

    const auto pathId = TPathId(ev->Get()->Record.GetPathOwnerId(), ev->Get()->Record.GetLocalPathId());
    if (DelayedInitTenantReply && DelayedInitTenantDestination && pathId == RootPathId()) {
        ctx.Send(DelayedInitTenantDestination, DelayedInitTenantReply.Release());
        DelayedInitTenantDestination = {};
    }

    const auto txId = TTxId(ev->Cookie);
    if (!txId) {
        // There was no txId, so we are not waiting for an ack
        return;
    }

    if (!Operations.contains(txId) && !Publications.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvUpdateAck"
                   << " for unknown txId " << txId
                   << ", at schemeshard: " << TabletID());
        return;
    }

    Execute(CreateTxAckPublishToSchemeBoard(ev), ctx);
}

void TSchemeShard::Handle(TEvTxProcessing::TEvPlanStep::TPtr &ev, const TActorContext &ctx) {
    Execute(CreateTxOperationPlanStep(ev), ctx);
}

void TSchemeShard::Handle(TEvHive::TEvCreateTabletReply::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvCreateTabletReply"
                << " at schemeshard: " << TabletID()
                << " message: " << ev->Get()->Record.ShortDebugString());

    auto shardIdx = TShardIdx(ev->Get()->Record.GetOwner(),
                              TLocalShardIdx(ev->Get()->Record.GetOwnerIdx()));

    if (!ShardInfos.contains(shardIdx)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvCreateTabletReply"
                   << " for unknown shard idx " <<  shardIdx
                   << " tabletId " << ev->Get()->Record.GetTabletID());
        return;
    }

    TShardInfo& shardInfo = ShardInfos[shardIdx];
    const auto txId = shardInfo.CurrentTxId;

    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvCreateTabletReply"
                   << " for unknown txId: " << txId
                   << ", shardIdx: " << shardIdx
                   << ", tabletId: " << ev->Get()->Record.GetTabletID()
                   << ", at schemeshard: " << TabletID());
        return;
    }

    TSubTxId partId = Operations.at(txId)->FindRelatedPartByShardIdx(shardIdx, ctx);
    if (partId == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvCreateTabletReply but partId in unknown"
                       << ", for txId: " << txId
                       << ", shardIdx: " << shardIdx
                       << ", tabletId: " << ev->Get()->Record.GetTabletID()
                       << ", at schemeshard: " << TabletID());
        return;
    }

    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(TEvHive::TEvAdoptTabletReply::TPtr &ev, const TActorContext &ctx) {
    auto shardIdx = MakeLocalId(TLocalShardIdx(ev->Get()->Record.GetOwnerIdx()));      // internal id

    if (!ShardInfos.contains(shardIdx)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvAdoptTabletReply"
                   << " for unknown shard idx " <<  shardIdx
                   << " tabletId " << ev->Get()->Record.GetTabletID());
        return;
    }

    TShardInfo& shardInfo = ShardInfos[shardIdx];
    const auto txId = shardInfo.CurrentTxId;

    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvAdoptTabletReply"
                   << " for unknown txId " << txId
                   << " shardIdx " << shardIdx
                   << " tabletId " << ev->Get()->Record.GetTabletID());

        return;
    }

    Execute(CreateTxOperationReply(TOperationId(txId, 0), ev), ctx);
}

void TSchemeShard::Handle(TEvHive::TEvDeleteTabletReply::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Free tablet reply"
                    << ", message: " << ev->Get()->Record.ShortDebugString()
                    << ", at schemeshard: " << TabletID());

    Execute(CreateTxDeleteTabletReply(ev), ctx);
}

void TSchemeShard::Handle(TEvHive::TEvDeleteOwnerTabletsReply::TPtr &ev, const TActorContext &ctx) {
    auto& record = ev->Get()->Record;

    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Free owner tablets reply"
                    << ", message: " << record.ShortDebugString()
                    << ", at schemeshard: " << TabletID());

    const auto txId = TTxId(record.GetTxId());

    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvDeleteOwnerTabletsReply"
                       << " for unknown txId " << txId
                       << " ownerID " << record.GetOwner()
                       << " form hive " << record.GetOrigin()
                       << " at schemeshard " << TabletID());
        return;
    }

    Execute(CreateTxOperationReply(TOperationId(txId, 0), ev), ctx);
}

void TSchemeShard::Handle(TEvHive::TEvUpdateTabletsObjectReply::TPtr &ev, const TActorContext &ctx) {
    auto& record = ev->Get()->Record;

    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Update tablets object reply"
                    << ", message: " << record.ShortDebugString()
                    << ", at schemeshard: " << TabletID());

    const auto txId = TTxId(record.GetTxId());
    const auto partId = TSubTxId(record.GetTxPartId());

    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvUpdateTabletsObjectReply"
                       << " for unknown txId " << txId
                       << " at schemeshard " << TabletID());
        return;
    }

    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(TEvHive::TEvUpdateDomainReply::TPtr &ev, const TActorContext &ctx) {
    const auto& record = ev->Get()->Record;

    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Update domain reply"
                    << ", message: " << record.ShortDebugString()
                    << ", at schemeshard: " << TabletID());

    const auto txId = TTxId(record.GetTxId());
    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvUpdateDomainReply"
                       << " for unknown txId " << txId
                       << " at schemeshard " << TabletID());
        return;
    }

    const auto tabletId = TTabletId(record.GetOrigin());
    const auto partId = Operations.at(txId)->FindRelatedPartByTabletId(tabletId, ctx);
    if (partId == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvHive::TEvUpdateDomainReply but partId is unknown"
                       << ", for txId: " << txId
                       << ", tabletId: " << tabletId
                       << ", at schemeshard: " << TabletID());
        return;
    }

    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(TEvPersQueue::TEvDropTabletReply::TPtr &ev, const TActorContext &ctx) {

    const auto txId = TTxId(ev->Get()->Record.GetTxId());
    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvPersQueue::TEvDropTabletReply"
                   << " for unknown txId " << txId
                   << ", message: " << ev->Get()->Record.ShortDebugString());
        return;
    }

    auto tabletId = TTabletId(ev->Get()->Record.GetTabletId());
    TSubTxId partId = Operations.at(txId)->FindRelatedPartByTabletId(tabletId, ctx);
    if (partId == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvPersQueue::TEvDropTabletReply but partId is unknown"
                       << ", for txId: " << txId
                       << ", tabletId: " << tabletId
                       << ", at schemeshard: " << TabletID());
        return;
    }

    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(TEvPersQueue::TEvUpdateConfigResponse::TPtr& ev, const TActorContext& ctx)
{
    const TTxId txId(ev->Get()->Record.GetTxId());
    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvPersQueue::TEvUpdateConfigResponse"
                   << " for unknown txId " << txId
                   << " message " << ev->Get()->Record.ShortDebugString());
        return;
    }

    const TTabletId tabletId(ev->Get()->Record.GetOrigin());
    const TSubTxId partId = Operations.at(txId)->FindRelatedPartByTabletId(tabletId, ctx);
    if (partId == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvUpdateConfigResponse but partId is unknown"
                       << ", for txId: " << txId
                       << ", tabletId: " << tabletId
                       << ", at schemeshard: " << TabletID());
        return;
    }

    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(TEvPersQueue::TEvProposeTransactionResult::TPtr& ev, const TActorContext& ctx)
{
    const TTxId txId(ev->Get()->Record.GetTxId());
    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvPersQueue::TEvProposeTransactionResult"
                   << " for unknown txId " << txId
                   << " message " << ev->Get()->Record.ShortDebugString());
        return;
    }

    const TTabletId tabletId(ev->Get()->Record.GetOrigin());
    const TSubTxId partId = Operations.at(txId)->FindRelatedPartByTabletId(tabletId, ctx);
    if (partId == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvProposeTransactionResult but partId is unknown"
                       << ", for txId: " << txId
                       << ", tabletId: " << tabletId
                       << ", at schemeshard: " << TabletID());
        return;
    }

    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(TEvBlobDepot::TEvApplyConfigResult::TPtr& ev, const TActorContext& ctx) {
    const TTxId txId(ev->Get()->Record.GetTxId());
    const TTabletId tabletId(ev->Get()->Record.GetTabletId());
    if (const auto it = Operations.find(txId); it == Operations.end()) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
           "Got TEvBlobDepot::TEvApplyConfigResult"
           << " for unknown txId " << txId
           << " message " << ev->Get()->Record.ShortDebugString());
    } else if (const TSubTxId partId = it->second->FindRelatedPartByTabletId(tabletId, ctx); partId == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
           "Got TEvBlobDepot::TEvApplyConfigResult but partId is unknown"
               << ", for txId: " << txId
               << ", tabletId: " << tabletId
               << ", at schemeshard: " << TabletID());
    } else {
        Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
    }
}

void TSchemeShard::Handle(TEvColumnShard::TEvProposeTransactionResult::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvProposeTransactionResult"
                << ", at schemeshard: " << TabletID()
                << ", message: " << ev->Get()->Record.ShortDebugString());

    const auto txId = TTxId(ev->Get()->Record.GetTxId());
    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvColumnShard::TEvProposeTransactionResult for unknown txId, ignore it"
                       << ", txId: " << txId
                       << ", message: " << ev->Get()->Record.ShortDebugString()
                       << ", at schemeshard: " << TabletID());
        return;
    }

    auto tabletId = TTabletId(ev->Get()->Record.GetOrigin());
    TSubTxId partId = Operations.at(txId)->FindRelatedPartByTabletId(tabletId, ctx);
    if (partId == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvProposeTransactionResult but partId in unknown"
                       << ", for txId: " << txId
                       << ", tabletId: " << tabletId
                       << ", at schemeshard: " << TabletID());
        return;
    }
    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(TEvColumnShard::TEvNotifyTxCompletionResult::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvNotifyTxCompletionResult"
                << ", at schemeshard: " << TabletID()
                << ", message: " << ev->Get()->Record.ShortDebugString());

    const auto txId = TTxId(ev->Get()->Record.GetTxId());
    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvColumnShard::TEvNotifyTxCompletionResult for unknown txId, ignore it"
                       << ", txId: " << txId
                       << ", message: " << ev->Get()->Record.ShortDebugString()
                       << ", at schemeshard: " << TabletID());
        return;
    }

    auto tabletId = TTabletId(ev->Get()->Record.GetOrigin());
    TSubTxId partId = Operations.at(txId)->FindRelatedPartByTabletId(tabletId, ctx);
    if (partId == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvNotifyTxCompletionResult but partId in unknown"
                       << ", for txId: " << txId
                       << ", tabletId: " << tabletId
                       << ", at schemeshard: " << TabletID());
        return;
    }
    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(NSequenceShard::TEvSequenceShard::TEvCreateSequenceResult::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvCreateSequenceResult"
                << ", at schemeshard: " << TabletID()
                << ", message: " << ev->Get()->Record.ShortDebugString());

    TTxId txId = TTxId(ev->Get()->Record.GetTxId());
    TSubTxId partId = TSubTxId(ev->Get()->Record.GetTxPartId());
    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(NSequenceShard::TEvSequenceShard::TEvDropSequenceResult::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvDropSequenceResult"
                << ", at schemeshard: " << TabletID()
                << ", message: " << ev->Get()->Record.ShortDebugString());

    TTxId txId = TTxId(ev->Get()->Record.GetTxId());
    TSubTxId partId = TSubTxId(ev->Get()->Record.GetTxPartId());
    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(NSequenceShard::TEvSequenceShard::TEvUpdateSequenceResult::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvUpdateSequenceResult"
                << ", at schemeshard: " << TabletID()
                << ", message: " << ev->Get()->Record.ShortDebugString());

    TTxId txId = TTxId(ev->Get()->Record.GetTxId());
    TSubTxId partId = TSubTxId(ev->Get()->Record.GetTxPartId());
    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(NSequenceShard::TEvSequenceShard::TEvFreezeSequenceResult::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvFreezeSequenceResult"
                << ", at schemeshard: " << TabletID()
                << ", message: " << ev->Get()->Record.ShortDebugString());

    TTxId txId = TTxId(ev->Get()->Record.GetTxId());
    TSubTxId partId = TSubTxId(ev->Get()->Record.GetTxPartId());
    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(NSequenceShard::TEvSequenceShard::TEvRestoreSequenceResult::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvRestoreSequenceResult"
                << ", at schemeshard: " << TabletID()
                << ", message: " << ev->Get()->Record.ShortDebugString());

    TTxId txId = TTxId(ev->Get()->Record.GetTxId());
    TSubTxId partId = TSubTxId(ev->Get()->Record.GetTxPartId());
    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(NSequenceShard::TEvSequenceShard::TEvRedirectSequenceResult::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvRedirectSequenceResult"
                << ", at schemeshard: " << TabletID()
                << ", message: " << ev->Get()->Record.ShortDebugString());

    TTxId txId = TTxId(ev->Get()->Record.GetTxId());
    TSubTxId partId = TSubTxId(ev->Get()->Record.GetTxPartId());
    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(NSequenceShard::TEvSequenceShard::TEvGetSequenceResult::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvGetSequenceResult"
                << ", at schemeshard: " << TabletID()
                << ", message: " << ev->Get()->Record.ShortDebugString());

    TTxId txId = TTxId(ev->Get()->Record.GetTxId());
    TSubTxId partId = TSubTxId(ev->Get()->Record.GetTxPartId());
    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(NReplication::TEvController::TEvCreateReplicationResult::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvCreateReplicationResult"
                << ", at schemeshard: " << TabletID()
                << ", message: " << ev->Get()->Record.ShortDebugString());

    const auto txId = TTxId(ev->Get()->Record.GetOperationId().GetTxId());
    const auto partId = TSubTxId(ev->Get()->Record.GetOperationId().GetPartId());
    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(NReplication::TEvController::TEvAlterReplicationResult::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvAlterReplicationResult"
                << ", at schemeshard: " << TabletID()
                << ", message: " << ev->Get()->Record.ShortDebugString());

    const auto txId = TTxId(ev->Get()->Record.GetOperationId().GetTxId());
    const auto partId = TSubTxId(ev->Get()->Record.GetOperationId().GetPartId());
    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(NReplication::TEvController::TEvDropReplicationResult::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvDropReplicationResult"
                << ", at schemeshard: " << TabletID()
                << ", message: " << ev->Get()->Record.ShortDebugString());

    const auto txId = TTxId(ev->Get()->Record.GetOperationId().GetTxId());
    const auto partId = TSubTxId(ev->Get()->Record.GetOperationId().GetPartId());
    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(TEvDataShard::TEvProposeTransactionResult::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvProposeTransactionResult"
                << ", at schemeshard: " << TabletID()
                << ", message: " << ev->Get()->Record.ShortDebugString());

    const auto txId = TTxId(ev->Get()->Record.GetTxId());
    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvDataShard::TEvProposeTransactionResult for unknown txId, ignore it"
                       << ", txId: " << txId
                       << ", message: " << ev->Get()->Record.ShortDebugString()
                       << ", at schemeshard: " << TabletID());
        return;
    }

    auto tabletId = TTabletId(ev->Get()->Record.GetOrigin());
    TSubTxId partId = Operations.at(txId)->FindRelatedPartByTabletId(tabletId, ctx);
    if (partId == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvProposeTransactionResult but partId in unknown"
                       << ", for txId: " << txId
                       << ", tabletId: " << tabletId
                       << ", at schemeshard: " << TabletID());
        return;
    }
    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(TEvSubDomain::TEvConfigureStatus::TPtr &ev, const TActorContext &ctx) {
    const auto& record = ev->Get()->Record;
    auto tabletId = TTabletId(record.GetOnTabletId());

    TOperationId opId = RouteIncoming(tabletId, ctx);
    if (!opId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvSubDomain::TEvConfigureStatus,"
                       << " no route has found by tabletId " << tabletId
                       << " message " << ev->Get()->Record.ShortDebugString());
        return;
    }
    Y_ABORT_UNLESS(opId.GetTxId());

    if (opId.GetSubTxId() == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvSubDomain::TEvConfigureStatus but partId in unknown"
                       << ", for txId: " << opId.GetTxId()
                       << ", tabletId: " << tabletId
                       << ", at schemeshard: " << TabletID());
        return;
    }

    Execute(CreateTxOperationReply(opId, ev), ctx);
}

void TSchemeShard::Handle(TEvBlockStore::TEvUpdateVolumeConfigResponse::TPtr& ev, const TActorContext& ctx) {
    const auto txId = TTxId(ev->Get()->Record.GetTxId());
    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvBlockStore::TEvUpdateVolumeConfigResponse"
                   << " for unknown txId " << txId
                   << " tabletId " << ev->Get()->Record.GetOrigin());
        return;
    }

    auto tabletId = TTabletId(ev->Get()->Record.GetOrigin());
    auto partId = Operations.at(txId)->FindRelatedPartByTabletId(tabletId, ctx);
    if (partId == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvUpdateVolumeConfigResponse but partId in unknown"
                       << ", for txId: " << txId
                       << ", tabletId: " << tabletId
                       << ", at schemeshard: " << TabletID());
        return;
    }

    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(TEvFileStore::TEvUpdateConfigResponse::TPtr& ev, const TActorContext& ctx) {
    const auto txId = TTxId(ev->Get()->Record.GetTxId());
    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "Got TEvFileStore::TEvUpdateConfigResponse"
                << " for unknown txId " << txId
                << " tabletId " << ev->Get()->Record.GetOrigin());
        return;
    }

    auto tabletId = TTabletId(ev->Get()->Record.GetOrigin());
    auto partId = Operations.at(txId)->FindRelatedPartByTabletId(tabletId, ctx);
    if (partId == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "Got TEvUpdateVolumeConfigResponse but partId in unknown"
                << ", for txId: " << txId
                << ", tabletId: " << tabletId
                << ", at schemeshard: " << TabletID());
        return;
    }

    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvInitTenantSchemeShardResult::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    auto tabletId = TTabletId(record.GetTenantSchemeShard());

    TOperationId opId = RouteIncoming(tabletId, ctx);
    if (!opId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvSchemeShard::TEvInitTenantSchemeShardResult"
                       << " no route has found by tabletId " << tabletId
                       << " message " << ev->Get()->Record.ShortDebugString());
        return;
    }
    Y_ABORT_UNLESS(opId.GetTxId());

    if (opId.GetSubTxId() == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvSubDomain::TEvConfigureStatus but partId in unknown"
                       << ", for txId: " << opId.GetTxId()
                       << ", tabletId: " << tabletId
                       << ", at schemeshard: " << TabletID());
        return;
    }

    Execute(CreateTxOperationReply(opId, ev), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvPublishTenantAsReadOnlyResult::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    auto tabletId = TTabletId(record.GetTenantSchemeShard());

    TOperationId opId = RouteIncoming(tabletId, ctx);
    if (!opId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvSchemeShard::TEvPublishTenantAsReadOnlyResult"
                       << " no route has found by tabletId " << tabletId
                       << " message " << ev->Get()->Record.ShortDebugString());
        return;
    }
    Y_ABORT_UNLESS(opId.GetTxId());

    if (opId.GetSubTxId() == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvSchemeShard::TEvPublishTenantAsReadOnlyResult but partId in unknown"
                       << ", for txId: " << opId.GetTxId()
                       << ", tabletId: " << tabletId
                       << ", at schemeshard: " << TabletID());
        return;
    }

    Execute(CreateTxOperationReply(opId, ev), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvPublishTenantResult::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    auto tabletId = TTabletId(record.GetTenantSchemeShard());

    TOperationId opId = RouteIncoming(tabletId, ctx);
    if (!opId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvSchemeShard::TEvPublishTenantResult"
                       << " no route has found by tabletId " << tabletId
                       << " message " << ev->Get()->Record.ShortDebugString());
        return;
    }
    Y_ABORT_UNLESS(opId.GetTxId());

    if (opId.GetSubTxId() == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvSchemeShard::TEvPublishTenantResult but partId in unknown"
                       << ", for txId: " << opId.GetTxId()
                       << ", tabletId: " << tabletId
                       << ", at schemeshard: " << TabletID());
        return;
    }

    Execute(CreateTxOperationReply(opId, ev), ctx);
}


void TSchemeShard::Handle(NKesus::TEvKesus::TEvSetConfigResult::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    auto tabletId = TTabletId(record.GetTabletId());


    TOperationId opId = RouteIncoming(tabletId, ctx);
    if (!opId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got NKesus::TEvKesus::TEvSetConfigResult"
                       << " no route has found by tabletId " << tabletId
                       << " message " << ev->Get()->Record.ShortDebugString());
        return;
    }
    Y_ABORT_UNLESS(opId.GetTxId());

    if (opId.GetSubTxId() == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got NKesus::TEvKesus::TEvSetConfigResult but partId in unknown"
                       << ", for txId: " << opId.GetTxId()
                       << ", tabletId: " << tabletId
                       << ", at schemeshard: " << TabletID());
        return;
    }

    Execute(CreateTxOperationReply(opId, ev), ctx);
}

TOperationId TSchemeShard::RouteIncoming(TTabletId tabletId, const TActorContext& ctx) {
    auto transactionIds = PipeTracker.FindTx(ui64(tabletId));

    Y_ABORT_UNLESS(transactionIds.size() <= 1);

    if (transactionIds.empty()) {
        return InvalidOperationId;
    }

    auto txId = TTxId(*transactionIds.begin());
    Y_ABORT_UNLESS(txId);

    if (!Operations.contains(txId)) {
        return InvalidOperationId;
    }

    TOperation::TPtr operation = Operations.at(txId);
    auto subTxId = operation->FindRelatedPartByTabletId(tabletId, ctx);
    return TOperationId(txId, subTxId);
}

void TSchemeShard::RestartPipeTx(TTabletId tabletId, const TActorContext& ctx) {
    for (auto item : PipeTracker.FindTx(ui64(tabletId))) {
        auto txId = TTxId(item);
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "Transaction " << txId
                    << " reset current state at schemeshard " << TabletID()
                    << " because pipe to tablet " << tabletId
                    << " disconnected");

        if (!Operations.contains(txId)) {
            continue;
        }

        TOperation::TPtr operation = Operations.at(txId);

        if (!operation->PipeBindedMessages.contains(tabletId)) {
            for (ui64 pipeTrackerCookie : PipeTracker.FindCookies(ui64(txId), ui64(tabletId))) {
                LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                            "Pipe attached message is not found, ignore event"
                                << ", opId:" << TOperationId(txId, pipeTrackerCookie)
                                << ", tableId: " << tabletId
                                << ", at schemeshardId: " << TabletID());
            }
            continue;
        }

        for (auto& item: operation->PipeBindedMessages.at(tabletId)) {
            TPipeMessageId msgCookie = item.first;
            TOperation::TPreSerializedMessage& msg = item.second;

            LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "Pipe attached message is found and resent into the new pipe"
                            << ", opId:" << msg.OpId
                            << ", dst tableId: " << tabletId
                            << ", msg type: " << msg.Type
                            << ", msg cookie: " << msgCookie
                            << ", at schemeshardId: " << TabletID());

            PipeClientCache->Send(ctx, ui64(tabletId),  msg.Type, msg.Data, msgCookie.second);
        }
    }
}

void TSchemeShard::Handle(NMon::TEvRemoteHttpInfo::TPtr& ev, const TActorContext& ctx) {
    RenderHtmlPage(ev, ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvCancelTx::TPtr& ev, const TActorContext& ctx) {
    if (IsReadOnlyMode) {
        LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Ignoring message TEvSchemeShard::TEvCancelTx" <<
                   " reason# schemeshard in readonly" <<
                   " schemeshard# " << TabletID());
        return;
    }

    Execute(CreateTxOperationPropose(ev), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvPublishTenantAsReadOnly::TPtr &ev, const TActorContext &ctx) {
    Execute(CreateTxPublishTenantAsReadOnly(ev), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvPublishTenant::TPtr &ev, const TActorContext &ctx) {
    Execute(CreateTxPublishTenant(ev), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvMigrateSchemeShard::TPtr &ev, const TActorContext &ctx) {
    if (InitState != TTenantInitState::Inprogress) {
        LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "Ignoring message TEvSchemeShard::TEvMigrateSchemeShard:" <<
                        " reason# schemeshard not in TTenantInitState::Inprogress state" <<
                        " state is " << (ui64) InitState <<
                        " schemeshard# " << TabletID());
        return;
    }
    Execute(CreateTxMigrate(ev), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvMigrateSchemeShardResult::TPtr &ev, const TActorContext &ctx) {
    const auto& record = ev->Get()->Record;
    auto tabletId = TTabletId(record.GetTenantSchemeShard());

    auto opId = RouteIncoming(tabletId, ctx);
    if (!opId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "unable to resolve operation by tabletID: " << tabletId <<
                       " ignore TEvSubDomain::TEvMigrateSchemeShardResult " <<
                       ", at schemeshard: " << TabletID());
        return;
    }

    Y_ABORT_UNLESS(opId.GetSubTxId() == FirstSubTxId);

    Execute(CreateTxOperationReply(opId, ev), ctx);
}

void TSchemeShard::Handle(TEvDataShard::TEvMigrateSchemeShardResponse::TPtr &ev, const TActorContext &ctx) {
    const auto& record = ev->Get()->Record;
    auto tabletId = TTabletId(record.GetTabletId());

    auto opId = RouteIncoming(tabletId, ctx);
    if (!opId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "unable to resolve operation by tabletID: " << tabletId <<
                       " ignore TEvDataShard::TEvMigrateSchemeShardResponse " <<
                       ", at schemeshard: " << TabletID());
        return;
    }

    Y_ABORT_UNLESS(opId.GetSubTxId() == FirstSubTxId);

    Execute(CreateTxOperationReply(opId, ev), ctx);
}

void TSchemeShard::ScheduleConditionalEraseRun(const TActorContext& ctx) {
    ctx.Schedule(TDuration::Minutes(1), new TEvPrivate::TEvRunConditionalErase());
}

void TSchemeShard::Handle(TEvPrivate::TEvRunConditionalErase::TPtr& ev, const TActorContext& ctx) {
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Handle: TEvRunConditionalErase"
        << ", at schemeshard: " << TabletID());

    Execute(CreateTxRunConditionalErase(ev), ctx);
}

bool TSchemeShard::ProcessPendingConditionalEraseResponseBatch(const TInstant& now, const TActorContext& ctx) {
    const bool completeBySize = (PendingCondEraseResponses.size() >= CondEraseResponseBatchSize);
    const auto batchAge = (now - PendingCondEraseResponsesStartTime);
    const bool completeByTime = (batchAge >= CondEraseResponseBatchMaxTime);

    if (PendingCondEraseResponses.size() > 0 && (completeBySize || completeByTime)) {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Conditional erase flush pending response batch (by " << (completeBySize ? "size" : "time") << ")"
            << ", batch size " << PendingCondEraseResponses.size() << "/" << CondEraseResponseBatchSize
            << ", batch age " << batchAge << "/" << CondEraseResponseBatchMaxTime
            << ", at schemeshard: " << TabletID()
        );

        Execute(CreateTxScheduleConditionalErase(std::move(PendingCondEraseResponses), PendingCondEraseResponsesStartTime), ctx);
        PendingCondEraseResponses.clear();

        return true;
    }

    return false;
}

void TSchemeShard::Handle(TEvPrivate::TEvFlushConditionalEraseBatch::TPtr& ev, const TActorContext& ctx) {
    if (PendingCondEraseResponsesStartTime > ev->Get()->BatchStartTime) {
        // Current batch is a new one, old batch was already processed.
        return;
    }

    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Handle: TEvFlushConditionalEraseBatch"
        << ", at schemeshard: " << TabletID());

    // This ensures incomplete batches don't get stuck indefinitely
    // The batch's Complete() will also trigger TTxRunConditionalErase for affected tables
    ProcessPendingConditionalEraseResponseBatch(ctx.Now(), ctx);
}

void TSchemeShard::Handle(TEvDataShard::TEvConditionalEraseRowsResponse::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    const TTabletId tabletId(record.GetTabletID());
    const TShardIdx shardIdx = GetShardIdx(tabletId);

    if (!ShardInfos.contains(shardIdx)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Unable to resolve shard info"
            << ": tabletId: " << tabletId
            << ", at schemeshard: " << TabletID());
        return;
    }

    // ACCEPTED and PARTIAL are handled here, outside a local transaction, since
    // neither causes state changes and TTL response processing is a performance bottleneck.
    if (record.GetStatus() == NKikimrTxDataShard::TEvConditionalEraseRowsResponse::ACCEPTED) {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Conditional erase accepted"
            << ": tabletId: " << tabletId
            << ", at schemeshard: " << TabletID());
        return;
    }
    if (record.GetStatus() == NKikimrTxDataShard::TEvConditionalEraseRowsResponse::PARTIAL) {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Conditional erase already running"
            << ": tabletId: " << tabletId
            << ", at schemeshard: " << TabletID());
        return;
    }

    // Check if batching is enabled and batch size is configured
    const bool batchingEnabled = AppData(ctx)->FeatureFlags.GetEnableConditionalEraseResponseBatching()
        && CondEraseResponseBatchSize > 0;

    const auto now = ctx.Now();

    // Accumulate response for batch processing
    if (PendingCondEraseResponses.empty()) {
        PendingCondEraseResponsesStartTime = now;
    }
    PendingCondEraseResponses.push_back(ev);

    if (batchingEnabled) {
        if (!ProcessPendingConditionalEraseResponseBatch(now, ctx)) {
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Conditional erase finished"
                << ", tabletId: " << tabletId
                << ", status: " << record.GetStatus()
                << ", batch size " << PendingCondEraseResponses.size() << "/" << CondEraseResponseBatchSize
                << ", batch age " << (now - PendingCondEraseResponsesStartTime) << "/" << CondEraseResponseBatchMaxTime
                << ", enqueued"
                << ", at schemeshard: " << TabletID()
            );

            // for the new batch schedule end of time event
            if (PendingCondEraseResponsesStartTime == now) {
                ctx.Schedule(CondEraseResponseBatchMaxTime, new TEvPrivate::TEvFlushConditionalEraseBatch(PendingCondEraseResponsesStartTime));
            }
        }
    } else {
        // Process response immediately
        Execute(CreateTxScheduleConditionalErase(std::move(PendingCondEraseResponses), PendingCondEraseResponsesStartTime), ctx);
        PendingCondEraseResponses.clear();
    }
}

void TSchemeShard::ScheduleServerlessStorageBilling(const TActorContext &ctx) {
    ctx.Send(SelfId(), new TEvPrivate::TEvServerlessStorageBilling());
}

void TSchemeShard::Handle(TEvPrivate::TEvServerlessStorageBilling::TPtr &, const TActorContext &ctx) {
    Execute(CreateTxServerlessStorageBilling(), ctx);
}

void TSchemeShard::Handle(TEvTxAllocatorClient::TEvAllocateResult::TPtr& ev, const TActorContext& ctx) {
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Handle: TEvAllocateResult"
                   << ": Cookie# " << ev->Cookie
                   << ", at schemeshard: " << TabletID());

    const ui64 id = ev->Cookie;
    if (0 == id) {
        for (auto txId: ev->Get()->TxIds) {
            CachedTxIds.push_back(TTxId(txId));
        }

        if (AppData()->FeatureFlags.GetEnableRealSystemViewPaths() && !SysViewsRosterUpdateStarted) {
            SysViewsRosterUpdateStarted = true;
            CollectSysViewUpdates(ctx);
        }

        if (AppData()->FeatureFlags.GetEnableLocalIndexAsSchemeObject() && !LocalIndexMigrationStarted) {
            LocalIndexMigrationStarted = true;
            CollectLocalIndexMigrations(ctx);
        }

        return;
    } else if (Exports.contains(id)) {
        return Execute(CreateTxProgressExport(ev), ctx);
    } else if (Imports.contains(id)) {
        return Execute(CreateTxProgressImport(ev), ctx);
    } else if (IncrementalRestoreStates.contains(id)) {
        return Execute(CreateTxProgressIncrementalRestoreAllocateResult(ev), ctx);
    } else if (IndexBuilds.contains(TIndexBuildId(id))) {
        return Execute(CreateTxReply(ev), ctx);
    } else if (SetColumnConstraintOperations.contains(TIndexBuildId(id))) {
        return Execute(CreateTxReplyAllocateSetColumnConstraint(ev), ctx);
    }

    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "no able to determine destination for message TEvAllocateResult: "
                   << " Cookie: " << id
                   << ", at schemeshard: " << TabletID());
}

void TSchemeShard::Handle(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev, const TActorContext& ctx) {
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Handle: TEvModifySchemeTransactionResult"
                   << ": txId# " << ev->Get()->Record.GetTxId()
                   << ", status# " << ev->Get()->Record.GetStatus());
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Message:\n" << ev->Get()->Record.ShortDebugString());

    const auto txId = TTxId(ev->Get()->Record.GetTxId());

    if (TxIdToExport.contains(txId)) {
        return Execute(CreateTxProgressExport(ev), ctx);
    } else if (TxIdToImport.contains(txId)) {
        return Execute(CreateTxProgressImport(ev), ctx);
    } else if (TxIdToIncrementalRestore.contains(txId)) {
        return Execute(CreateTxProgressIncrementalRestore(ev, ctx), ctx);
    } else if (TxIdToIndexBuilds.contains(txId)) {
        return Execute(CreateTxReply(ev), ctx);
    } else if (TxIdToSetColumnConstraintOperations.contains(txId)) {
        return Execute(CreateTxReplyModifySetColumnConstraint(ev), ctx);
    } else if (BackgroundCleaningTxToDirPathId.contains(txId)) {
        return HandleBackgroundCleaningTransactionResult(ev);
    }

    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "no able to determine destination for message TEvModifySchemeTransactionResult: "
                   << " txId: " << txId
                   << ", at schemeshard: " << TabletID());
}


} // namespace NSchemeShard
} // namespace NKikimr

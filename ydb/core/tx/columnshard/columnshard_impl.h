#pragma once
#include "background_controller.h"
#include "columnshard.h"
#include "columnshard_private_events.h"
#include "columnshard_subdomain_path_id.h"
#include "counters.h"
#include "defs.h"
#include "inflight_request_tracker.h"
#include "tables_manager.h"

#include "bg_tasks/events/local.h"
#include "blobs_action/events/delete_blobs.h"
#include "common/path_id.h"
#include "counters/columnshard.h"
#include "counters/counters_manager.h"
#include "data_sharing/destination/events/control.h"
#include "data_sharing/destination/events/transfer.h"
#include "data_sharing/manager/sessions.h"
#include "data_sharing/manager/shared_blobs.h"
#include "data_sharing/modification/events/change_owning.h"
#include "data_sharing/source/events/control.h"
#include "data_sharing/source/events/transfer.h"
#include "normalizer/abstract/abstract.h"
#include "operations/events.h"
#include "operations/manager.h"
#include "resource_subscriber/counters.h"
#include "resource_subscriber/task.h"
#include "subscriber/abstract/manager/manager.h"
#include "tablet/ext_tx_base.h"
#include "transactions/tx_controller.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/tablet/tablet_counters.h>
#include <ydb/core/tablet/tablet_pipe_client_cache.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tx/columnshard/column_fetching/manager.h>
#include <ydb/core/tx/columnshard/overload_manager/overload_manager_service.h>
#include <ydb/core/tx/columnshard/overload_manager/overload_manager_events.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/locks/locks.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tiering/common.h>
#include <ydb/core/tx/time_cast/time_cast.h>
#include <ydb/core/tx/tx_processing.h>

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/service.h>

namespace NKikimr::NOlap {
class TCleanupPortionsColumnEngineChanges;
class TCleanupTablesColumnEngineChanges;
class TTTLColumnEngineChanges;
class TChangesWithAppend;
class TCompactColumnEngineChanges;
class TInsertColumnEngineChanges;
class TStoragesManager;
class TRemovePortionsChange;
class TMovePortionsChange;

namespace NReader {
class TTxScan;
class TTxInternalScan;
namespace NCommon {
class TReadMetadata;
}
namespace NPlain {
class TIndexScannerConstructor;
}
namespace NSimple {
class TIndexScannerConstructor;
}
}   // namespace NReader

namespace NDataSharing {
class TTxDataFromSource;
class TTxDataAckToSource;
class TTxFinishAckToSource;
class TTxFinishAckFromInitiator;
}   // namespace NDataSharing

namespace NBackground {
class TSessionsManager;
}

namespace NBlobOperations {
namespace NBlobStorage {
class TWriteAction;
class TOperator;
}   // namespace NBlobStorage
namespace NTier {
class TOperator;
}
}   // namespace NBlobOperations
namespace NCompaction {
class TGeneralCompactColumnEngineChanges;
}
}   // namespace NKikimr::NOlap

namespace NKikimr::NColumnShard {

class TArrowData;
class TEvWriteCommitPrimaryTransactionOperator;
class TEvWriteCommitSecondaryTransactionOperator;
class TTxFinishAsyncTransaction;
class TTxCleanupSchemasWithnoData;
class TTxRemoveSharedBlobs;
class TOperationsManager;
class TWaitEraseTablesTxSubscriber;
class TTxBlobsWritingFinished;
class TTxBlobsWritingFailed;
class TWriteTasksQueue;
class TWriteTask;
class TCommitOperation;

namespace NLoading {
class TTxControllerInitializer;
class TOperationsManagerInitializer;
class TStoragesManagerInitializer;
class TDBLocksInitializer;
class TBackgroundSessionsInitializer;
class TSharingSessionsInitializer;
class TInFlightReadsInitializer;
class TSpecialValuesInitializer;
class TTablesManagerInitializer;
class TTiersManagerInitializer;
}   // namespace NLoading

extern bool gAllowLogBatchingDefaultValue;

IActor* CreateWriteActor(ui64 tabletId, IWriteController::TPtr writeController, const TInstant deadline);
IActor* CreateColumnShardScan(const TActorId& scanComputeActor, ui32 scanId, ui64 txId);

struct TSettings {
    static constexpr ui32 MAX_INDEXATIONS_TO_SKIP = 16;
    static constexpr TDuration GuaranteeIndexationInterval = TDuration::Seconds(10);
    static constexpr TDuration DefaultStatsReportInterval = TDuration::Seconds(10);
    static constexpr i64 GuaranteeIndexationStartBytesLimit = (i64)5 * 1024 * 1024 * 1024;

    TControlWrapper BlobWriteGrouppingEnabled;
    TControlWrapper CacheDataAfterIndexing;
    TControlWrapper CacheDataAfterCompaction;
    static constexpr ui64 OverloadTxInFlight = 1000;

    TSettings()
        : BlobWriteGrouppingEnabled(1, 0, 1)
        , CacheDataAfterIndexing(1, 0, 1)
        , CacheDataAfterCompaction(1, 0, 1) {
    }

    void RegisterControls(TControlBoard& icb) {
        TControlBoard::RegisterSharedControl(BlobWriteGrouppingEnabled, icb.ColumnShardControls.BlobWriteGrouppingEnabled);
        TControlBoard::RegisterSharedControl(CacheDataAfterIndexing, icb.ColumnShardControls.CacheDataAfterIndexing);
        TControlBoard::RegisterSharedControl(CacheDataAfterCompaction, icb.ColumnShardControls.CacheDataAfterCompaction);
    }
};

using ITransaction = NTabletFlatExecutor::ITransaction;

template <typename T>
using TTransactionBase = NTabletFlatExecutor::TTransactionBase<T>;

class TColumnShard: public TActor<TColumnShard>, public NTabletFlatExecutor::TTabletExecutedFlat {
    friend class TEvWriteCommitSyncTransactionOperator;
    friend class TEvWriteCommitSecondaryTransactionOperator;
    friend class TEvWriteCommitPrimaryTransactionOperator;
    friend class TTxInit;
    friend class TTxCleanupSchemasWithnoData;
    friend class TTxInitSchema;
    friend class TTxUpdateSchema;
    friend class TTxProposeTransaction;
    friend class TTxNotifyTxCompletion;
    friend class TTxTxAbort;
    friend class TTxPlanStep;
    friend class TTxWrite;
    friend class TTxBlobsWritingFinished;
    friend class TTxBlobsWritingFailed;
    friend class TTxReadBase;
    friend class TTxRead;
    friend class TTxWriteIndex;
    friend class TTxExportFinish;
    friend class TTxRunGC;
    friend class TTxProcessGCResult;
    friend class TTxReadBlobRanges;
    friend class TTxApplyNormalizer;
    friend class TTxMonitoring;
    friend class TTxRemoveSharedBlobs;
    friend class TTxFinishAsyncTransaction;
    friend class TWaitOnProposeTxSubscriberBase;
    friend class TTxPersistSubDomainOutOfSpace;
    friend class TTxPersistSubDomainPathId;
    friend class TSpaceWatcher;

    friend class NOlap::TCleanupPortionsColumnEngineChanges;
    friend class NOlap::TCleanupTablesColumnEngineChanges;
    friend class NOlap::TTTLColumnEngineChanges;
    friend class NOlap::TChangesWithAppend;
    friend class NOlap::TCompactColumnEngineChanges;
    friend class NOlap::TInsertColumnEngineChanges;
    friend class NOlap::TColumnEngineChanges;
    friend class NOlap::NCompaction::TGeneralCompactColumnEngineChanges;
    friend class NOlap::NBlobOperations::NBlobStorage::TWriteAction;
    friend class NOlap::NBlobOperations::NBlobStorage::TOperator;
    friend class NOlap::NBlobOperations::NTier::TOperator;

    friend class NOlap::NDataSharing::TTxDataFromSource;
    friend class NOlap::NDataSharing::TTxDataAckToSource;
    friend class NOlap::NDataSharing::TTxFinishAckToSource;
    friend class NOlap::NDataSharing::TTxFinishAckFromInitiator;
    friend class NOlap::NDataSharing::TSourceSession;

    friend class NOlap::TStoragesManager;

    friend class NOlap::NReader::TTxScan;
    friend class NOlap::NReader::TTxInternalScan;
    friend class NOlap::NReader::NPlain::TIndexScannerConstructor;
    friend class NOlap::NReader::NSimple::TIndexScannerConstructor;
    friend class NOlap::NReader::NCommon::TReadMetadata;
    friend class NOlap::TRemovePortionsChange;
    friend class NOlap::TMovePortionsChange;

    class TStoragesManager;
    friend class TTxController;

    friend class TOperationsManager;
    friend class TWriteOperation;

    friend class TSchemaTransactionOperator;
    friend class TEvWriteTransactionOperator;
    friend class TBackupTransactionOperator;
    friend class TRestoreTransactionOperator;
    friend class IProposeTxOperator;
    friend class TSharingTransactionOperator;

    friend class NLoading::TTxControllerInitializer;
    friend class NLoading::TOperationsManagerInitializer;
    friend class NLoading::TStoragesManagerInitializer;
    friend class NLoading::TDBLocksInitializer;
    friend class NLoading::TBackgroundSessionsInitializer;
    friend class NLoading::TSharingSessionsInitializer;
    friend class NLoading::TInFlightReadsInitializer;
    friend class NLoading::TSpecialValuesInitializer;
    friend class NLoading::TTablesManagerInitializer;
    friend class NLoading::TTiersManagerInitializer;
    friend class TWriteTasksQueue;
    friend class TWriteTask;

    class TTxProgressTx;
    class TTxProposeCancel;
    // proto
    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvServerConnected::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTxProcessing::TEvReadSet::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTxProcessing::TEvReadSetAck::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvColumnShard::TEvProposeTransaction::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvColumnShard::TEvCheckPlannedTransaction::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvCancelTransactionProposal::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvColumnShard::TEvNotifyTxCompletion::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTxProcessing::TEvPlanStep::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvKqpScan::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvColumnShard::TEvInternalScan::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvMediatorTimecast::TEvRegisterTabletResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvMediatorTimecast::TEvNotifyPlanStep::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvWriteBlobsResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvStartCompaction::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvMetadataAccessorsInfo::TPtr& ev, const TActorContext& ctx);

    void Handle(NPrivateEvents::NWrite::TEvWritePortionResult::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvPrivate::TEvScanStats::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvReadFinished::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvReportBaseStatistics::TPtr&);
    void Handle(TEvPrivate::TEvReportExecutorStatistics::TPtr&);
    void Handle(TEvPrivate::TEvPeriodicWakeup::TPtr& ev, const TActorContext& ctx);
    void Handle(NActors::TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvPingSnapshotsUsage::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvPrivate::TEvWriteIndex::TPtr& ev, const TActorContext& ctx);
    void Handle(NEvents::TDataEvents::TEvWrite::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvWriteDraft::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvGarbageCollectionFinished::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvTieringModified::TPtr& ev, const TActorContext&);
    void Handle(TEvPrivate::TEvNormalizerResult::TPtr& ev, const TActorContext&);

    void Handle(NStat::TEvStatistics::TEvAnalyzeShard::TPtr& ev, const TActorContext& ctx);
    void Handle(NStat::TEvStatistics::TEvStatisticsRequest::TPtr& ev, const TActorContext& ctx);

    void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev, const TActorContext&);

    void Handle(NOlap::NBlobOperations::NEvents::TEvDeleteSharedBlobs::TPtr& ev, const TActorContext& ctx);
    void Handle(NOlap::NBackground::TEvExecuteGeneralLocalTransaction::TPtr& ev, const TActorContext& ctx);

    void Handle(NOlap::NDataSharing::NEvents::TEvApplyLinksModification::TPtr& ev, const TActorContext& ctx);
    void Handle(NOlap::NDataSharing::NEvents::TEvApplyLinksModificationFinished::TPtr& ev, const TActorContext& ctx);

    void Handle(NOlap::NDataSharing::NEvents::TEvProposeFromInitiator::TPtr& ev, const TActorContext& ctx);
    void Handle(NOlap::NDataSharing::NEvents::TEvConfirmFromInitiator::TPtr& ev, const TActorContext& ctx);
    void Handle(NOlap::NDataSharing::NEvents::TEvStartToSource::TPtr& ev, const TActorContext& ctx);
    void Handle(NOlap::NDataSharing::NEvents::TEvSendDataFromSource::TPtr& ev, const TActorContext& ctx);
    void Handle(NOlap::NDataSharing::NEvents::TEvAckDataToSource::TPtr& ev, const TActorContext& ctx);
    void Handle(NOlap::NDataSharing::NEvents::TEvFinishedFromSource::TPtr& ev, const TActorContext& ctx);
    void Handle(NOlap::NDataSharing::NEvents::TEvAckFinishToSource::TPtr& ev, const TActorContext& ctx);
    void Handle(NOlap::NDataSharing::NEvents::TEvAckFinishFromInitiator::TPtr& ev, const TActorContext& ctx);
    void Handle(NColumnShard::TEvPrivate::TEvAskTabletDataAccessors::TPtr& ev, const TActorContext& ctx);
    void Handle(NColumnShard::TEvPrivate::TEvAskColumnData::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvCancelBackup::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvCancelRestore::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvColumnShard::TEvOverloadUnsubscribe::TPtr& ev, const TActorContext& ctx);
    void Handle(NLongTxService::TEvLongTxService::TEvLockStatus::TPtr& ev, const TActorContext& ctx);
    void SubscribeLockIfNotAlready(const ui64 lockId, const ui32 lockNodeId) const;
    void ProposeTransaction(std::shared_ptr<TCommitOperation> op, const TActorId source, const ui64 cookie);
    void TransactionToAbort(const ui64 lockId);
    void MaybeAbortTransaction(const ui64 lockId);
    void CancelTransaction(const ui64 txId);

    void HandleInit(TEvPrivate::TEvTieringModified::TPtr& ev, const TActorContext&);

    ITransaction* CreateTxInitSchema();

    void OnActivateExecutor(const TActorContext& ctx) override;
    void OnDetach(const TActorContext& ctx) override;
    //void OnTabletStop(TEvTablet::TEvTabletStop::TPtr& ev, const TActorContext& ctx);
    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr& ev, const TActorContext& ctx) override;
    bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext& ctx) override;

    void DefaultSignalTabletActive(const TActorContext& ctx) override {
        Y_UNUSED(ctx);
    }

    const NTiers::TManager* GetTierManagerPointer(const TString& tierId) const;

    void Die(const TActorContext& ctx) override;

    void CleanupActors(const TActorContext& ctx);
    void BecomeBroken(const TActorContext& ctx);
    void TrySwitchToWork(const TActorContext& ctx);

    bool IsAnyChannelYellowStop() const {
        return Executor()->GetStats().IsAnyChannelYellowStop;
    }

    bool IsAnyChannelYellowMove() const {
        return Executor()->GetStats().IsAnyChannelYellowMove;
    }

    void OnYellowChannels(const TPutStatus& putStatus) {
        putStatus.OnYellowChannels(Executor());
    }

    void ActivateTiering(const TInternalPathId pathId, const THashSet<NTiers::TExternalStorageId>& tiers);
    void OnTieringModified(const std::optional<TInternalPathId> pathId = {});

    std::shared_ptr<TAtomicCounter> TabletActivityImpl = std::make_shared<TAtomicCounter>(0);

public:
    TAtomicCounter InitShardCounter;

    ui64 BuildEphemeralTxId() {
        static TAtomicCounter Counter = 0;
        static constexpr ui64 shift = (ui64)1 << 47;
        return shift | Counter.Inc();
    }

    using EOverloadStatus = EOverloadStatus;
    using TOverloadStatus = TOverloadStatus;

    // For syslocks
    void IncCounter(NDataShard::ECumulativeCounters counter, ui64 num = 1) const {
        Counters.GetTabletCounters()->IncCounter(counter, num);
    }

    void IncCounter(NDataShard::EPercentileCounters counter, ui64 num) const {
        Counters.GetTabletCounters()->IncCounter(counter, num);
    }

    void IncCounter(NDataShard::EPercentileCounters counter, const TDuration& latency) const {
        Counters.GetTabletCounters()->IncCounter(counter, latency);
    }

    inline TRowVersion LastCompleteTxVersion() const {
        return TRowVersion(LastCompletedTx.GetPlanStep(), LastCompletedTx.GetTxId());
    }

    ui32 Generation() const {
        return Executor()->Generation();
    }

    bool IsUserTable(const TTableId&) const {
        return true;
    }

private:
    void OverloadWriteFail(const EOverloadStatus overloadReason, const NEvWrite::TWriteMeta& writeMeta, const ui64 writeSize, const ui64 cookie,
        std::unique_ptr<NActors::IEventBase>&& event, const TActorContext& ctx);
    TOverloadStatus ResourcesStatusToOverloadStatus(const NOverload::EResourcesStatus status) const;
    TOverloadStatus CheckOverloadedImmediate(const TInternalPathId tableId) const;
    EOverloadStatus CheckOverloadedWait(const TInternalPathId tableId) const;

protected:
    STFUNC(StateInit) {
        TRACE_EVENT(NKikimrServices::TX_COLUMNSHARD);
        StateInitImpl(ev, SelfId());
    }

    STFUNC(StateBroken) {
        TRACE_EVENT(NKikimrServices::TX_COLUMNSHARD);
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTablet::TEvTabletDead, HandleTabletDead);
            default:
                LOG_S_WARN("TColumnShard.StateBroken at " << TabletID() << " unhandled event type: " << ev->GetTypeName()
                                                          << " event: " << ev->ToString());
                Send(IEventHandle::ForwardOnNondelivery(std::move(ev), NActors::TEvents::TEvUndelivered::ReasonActorUnknown));
                break;
        }
    }

    STFUNC(StateWork) {
        const TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletID())(
            "self_id", SelfId())("ev", ev->GetTypeName());
        TRACE_EVENT(NKikimrServices::TX_COLUMNSHARD);
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProcessing::TEvReadSet, Handle);
            HFunc(TEvTxProcessing::TEvReadSetAck, Handle);

            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvTabletPipe::TEvServerConnected, Handle);
            HFunc(TEvTabletPipe::TEvServerDisconnected, Handle);
            HFunc(TEvColumnShard::TEvProposeTransaction, Handle);
            HFunc(TEvColumnShard::TEvCheckPlannedTransaction, Handle);
            HFunc(TEvDataShard::TEvCancelTransactionProposal, Handle);
            HFunc(TEvColumnShard::TEvNotifyTxCompletion, Handle);
            HFunc(TEvDataShard::TEvKqpScan, Handle);
            HFunc(TEvColumnShard::TEvInternalScan, Handle);
            HFunc(TEvTxProcessing::TEvPlanStep, Handle);
            HFunc(TEvPrivate::TEvWriteBlobsResult, Handle);
            HFunc(TEvPrivate::TEvStartCompaction, Handle);
            HFunc(TEvPrivate::TEvMetadataAccessorsInfo, Handle);
            HFunc(NPrivateEvents::NWrite::TEvWritePortionResult, Handle);

            HFunc(TEvMediatorTimecast::TEvRegisterTabletResult, Handle);
            HFunc(TEvMediatorTimecast::TEvNotifyPlanStep, Handle);
            HFunc(TEvPrivate::TEvWriteIndex, Handle);
            HFunc(TEvPrivate::TEvScanStats, Handle);
            HFunc(TEvPrivate::TEvReadFinished, Handle);
            HFunc(TEvPrivate::TEvPeriodicWakeup, Handle);
            HFunc(NActors::TEvents::TEvWakeup, Handle);
            HFunc(TEvPrivate::TEvPingSnapshotsUsage, Handle);
            hFunc(TEvPrivate::TEvReportBaseStatistics, Handle);
            hFunc(TEvPrivate::TEvReportExecutorStatistics, Handle);
            HFunc(NEvents::TDataEvents::TEvWrite, Handle);
            HFunc(TEvPrivate::TEvWriteDraft, Handle);
            HFunc(TEvPrivate::TEvGarbageCollectionFinished, Handle);
            HFunc(TEvPrivate::TEvTieringModified, Handle);

            HFunc(NStat::TEvStatistics::TEvAnalyzeShard, Handle);
            HFunc(NStat::TEvStatistics::TEvStatisticsRequest, Handle);

            HFunc(NActors::TEvents::TEvUndelivered, Handle);

            HFunc(NOlap::NBlobOperations::NEvents::TEvDeleteSharedBlobs, Handle);
            HFunc(NOlap::NBackground::TEvExecuteGeneralLocalTransaction, Handle);
            HFunc(NOlap::NDataSharing::NEvents::TEvApplyLinksModification, Handle);
            HFunc(NOlap::NDataSharing::NEvents::TEvApplyLinksModificationFinished, Handle);

            HFunc(NOlap::NDataSharing::NEvents::TEvProposeFromInitiator, Handle);
            HFunc(NOlap::NDataSharing::NEvents::TEvConfirmFromInitiator, Handle);
            HFunc(NOlap::NDataSharing::NEvents::TEvStartToSource, Handle);
            HFunc(NOlap::NDataSharing::NEvents::TEvSendDataFromSource, Handle);
            HFunc(NOlap::NDataSharing::NEvents::TEvAckDataToSource, Handle);
            HFunc(NOlap::NDataSharing::NEvents::TEvFinishedFromSource, Handle);
            HFunc(NOlap::NDataSharing::NEvents::TEvAckFinishToSource, Handle);
            HFunc(NOlap::NDataSharing::NEvents::TEvAckFinishFromInitiator, Handle);
            HFunc(NColumnShard::TEvPrivate::TEvAskTabletDataAccessors, Handle);
            HFunc(NColumnShard::TEvPrivate::TEvAskColumnData, Handle);
            HFunc(TEvTxProxySchemeCache::TEvWatchNotifyUpdated, Handle);
            HFunc(TEvColumnShard::TEvOverloadUnsubscribe, Handle);
            HFunc(NLongTxService::TEvLongTxService::TEvLockStatus, Handle);
            HFunc(TEvDataShard::TEvCancelBackup, Handle);
            HFunc(TEvDataShard::TEvCancelRestore, Handle);

            default:
                if (!HandleDefaultEvents(ev, SelfId())) {
                    LOG_S_WARN("TColumnShard.StateWork at " << TabletID() << " unhandled event type: " << ev->GetTypeName()
                                                            << " event: " << ev->ToString());
                }
                break;
        }
    }

private:
    std::unique_ptr<TTabletCountersBase> TabletCountersHolder;
    TCountersManager Counters;
    std::unique_ptr<TWriteTasksQueue> WriteTasksQueue;

    std::unique_ptr<TTxController> ProgressTxController;
    std::unique_ptr<TOperationsManager> OperationsManager;
    std::shared_ptr<NOlap::NDataSharing::TSessionsManager> SharingSessionsManager;
    std::shared_ptr<NOlap::IStoragesManager> StoragesManager;
    std::shared_ptr<NOlap::NBackground::TSessionsManager> BackgroundSessionsManager;
    std::shared_ptr<NOlap::NDataLocks::TManager> DataLocksManager;

    ui64 PrioritizationClientId = 0;

    using TSchemaPreset = TSchemaPreset;
    using TTableInfo = TTableInfo;

    const TMonotonic CreateInstant = TMonotonic::Now();
    std::optional<TMonotonic> StartInstant;
    bool IsTxInitFinished = false;

    ui64 CurrentSchemeShardId = 0;
    TMessageSeqNo LastSchemaSeqNo;
    std::optional<NKikimrSubDomains::TProcessingParams> ProcessingParams;
    ui64 LastPlannedStep = 0;
    ui64 LastPlannedTxId = 0;
    NOlap::TSnapshot LastCompletedTx = NOlap::TSnapshot::Zero();
    ui64 LastExportNo = 0;
    NKikimrTxColumnShard::TCompletedBackupTransaction LastCompletedBackupTransaction;

    ui64 StatsReportRound = 0;
    TString OwnerPath;

    TMediatorTimecastEntry::TCPtr MediatorTimeCastEntry;
    bool MediatorTimeCastRegistered = false;
    TSet<ui64> MediatorTimeCastWaitingSteps;
    const TDuration PeriodicWakeupActivationPeriod;
    TDuration FailActivationDelay = TDuration::Seconds(1);
    const TDuration StatsReportInterval;
    TInstant LastStatsReport;

    TActorId ResourceSubscribeActor;
    TActorId BufferizationPortionsWriteActorId;
    NOlap::NDataAccessorControl::TDataAccessorsManagerContainer DataAccessorsManager;
    NBackgroundTasks::TControlInterfaceContainer<NOlap::NColumnFetching::TColumnDataManager> ColumnDataManager;

    std::vector<TActorId> ActorsToStop;

    TInFlightReadsTracker InFlightReadsTracker;
    TTablesManager TablesManager;
    std::shared_ptr<NSubscriber::TManager> Subscribers;
    std::shared_ptr<TTiersManager> Tiers;
    std::unique_ptr<NTabletPipe::IClientCache> PipeClientCache;
    NOlap::NResourceBroker::NSubscribe::TTaskContext CompactTaskSubscription;
    NOlap::NResourceBroker::NSubscribe::TTaskContext TTLTaskSubscription;

    std::optional<ui64> ProgressTxInFlight;
    THashMap<ui64, TInstant> ScanTxInFlight;
    TMultiMap<NOlap::TSnapshot, TEvDataShard::TEvKqpScan::TPtr> WaitingScans;
    TBackgroundController BackgroundController;
    TSettings Settings;
    TLimits Limits;
    NOlap::TNormalizationController NormalizerController;
    NDataShard::TSysLocks SysLocks;
    TSpaceWatcher* SpaceWatcher;
    TActorId SpaceWatcherId;
    THashMap<TActorId, TActorId> PipeServersInterconnectSessions;
    TActorId ScanDiagnosticsActorId;

    TActorId StatsReportPipe;
    std::unique_ptr<TEvDataShard::TEvPeriodicTableStats> LastStats;
    ui32 JitterIntervalMS = 200;
    ui32 BaseStatsEvInflight = 0;
    ui32 ExecutorStatsEvInflight = 0;
    void TryRegisterMediatorTimeCast();
    void UnregisterMediatorTimeCast();

    bool WaitPlanStep(ui64 step);
    void SendWaitPlanStep(ui64 step);
    void RescheduleWaitingReads();
    NOlap::TSnapshot GetMaxReadVersion() const;
    NOlap::TSnapshot GetMinReadSnapshot() const;
    ui64 GetOutdatedStep() const;
    TDuration GetTxCompleteLag() const {
        ui64 mediatorTime = MediatorTimeCastEntry ? MediatorTimeCastEntry->Get(TabletID()) : 0;
        return ProgressTxController->GetTxCompleteLag(mediatorTime);
    }

    void EnqueueBackgroundActivities(const bool periodic = false);
    virtual void Enqueue(STFUNC_SIG) override;

    void UpdateSchemaSeqNo(const TMessageSeqNo& seqNo, NTabletFlatExecutor::TTransactionContext& txc);
    void ProtectSchemaSeqNo(const NKikimrTxColumnShard::TSchemaSeqNo& seqNoProto, NTabletFlatExecutor::TTransactionContext& txc);

    void RunSchemaTx(
        const NKikimrTxColumnShard::TSchemaTxBody& body, const NOlap::TSnapshot& version, NTabletFlatExecutor::TTransactionContext& txc);
    void RunInit(const NKikimrTxColumnShard::TInitShard& body, const NOlap::TSnapshot& version, NTabletFlatExecutor::TTransactionContext& txc);
    void RunEnsureTable(
        const NKikimrTxColumnShard::TCreateTable& body, const NOlap::TSnapshot& version, NTabletFlatExecutor::TTransactionContext& txc);
    void RunAlterTable(
        const NKikimrTxColumnShard::TAlterTable& body, const NOlap::TSnapshot& version, NTabletFlatExecutor::TTransactionContext& txc);
    void RunDropTable(
        const NKikimrTxColumnShard::TDropTable& body, const NOlap::TSnapshot& version, NTabletFlatExecutor::TTransactionContext& txc);
    void RunAlterStore(
        const NKikimrTxColumnShard::TAlterStore& body, const NOlap::TSnapshot& version, NTabletFlatExecutor::TTransactionContext& txc);
    void RunMoveTable(
        const NKikimrTxColumnShard::TMoveTable& proto, const NOlap::TSnapshot& version, NTabletFlatExecutor::TTransactionContext& txc);
    void RunCopyTable(
        const NKikimrTxColumnShard::TCopyTable& proto, const NOlap::TSnapshot& version, NTabletFlatExecutor::TTransactionContext& txc);

    void SetupCompaction(const std::set<TInternalPathId>& pathIds);
    void StartCompaction(const std::shared_ptr<NPrioritiesQueue::TAllocationGuard>& guard);

    void SetupMetadata();
    bool SetupTtl();
    void SetupCleanupPortions();
    void SetupCleanupTables();
    void SetupCleanupSchemas();
    void SetupGC();

    void UpdateIndexCounters();
    void UpdateResourceMetrics(const TActorContext& ctx, const TUsage& usage);
    ui64 MemoryUsage() const;

    void SendPeriodicStats(bool);
    void ScheduleBaseStatistics();
    void ScheduleExecutorStatistics();

    void FillOlapStats(const TActorContext& ctx, std::unique_ptr<TEvDataShard::TEvPeriodicTableStats>& ev, IExecutor* executor);
    void FillColumnTableStats(const TActorContext& ctx, std::unique_ptr<TEvDataShard::TEvPeriodicTableStats>& ev, IExecutor* executor);

public:
    ui64 TabletTxCounter = 0;

    std::shared_ptr<const TAtomicCounter> GetTabletActivity() const {
        return TabletActivityImpl;
    }

    const TTablesManager& GetTablesManager() const {
        return TablesManager;
    }

    void EnqueueProgressTx(const TActorContext& ctx, const std::optional<ui64> continueTxId);
    NOlap::TSnapshot GetLastTxSnapshot() const {
        return NOlap::TSnapshot(LastPlannedStep, LastPlannedTxId);
    }

    NOlap::TSnapshot GetCurrentSnapshotForInternalModification() const {
        return NOlap::TSnapshot::MaxForPlanStep(GetOutdatedStep());
    }

    const std::shared_ptr<NOlap::NDataSharing::TSessionsManager>& GetSharingSessionsManager() const {
        return SharingSessionsManager;
    }

    template <class T>
    const T& GetIndexAs() const {
        return TablesManager.GetPrimaryIndexAsVerified<T>();
    }

    const NOlap::IColumnEngine* GetIndexOptional() const {
        return TablesManager.GetPrimaryIndex() ? TablesManager.GetPrimaryIndex().get() : nullptr;
    }

    NOlap::IColumnEngine* MutableIndexOptional() const {
        return TablesManager.GetPrimaryIndex() ? TablesManager.GetPrimaryIndex().get() : nullptr;
    }

    const NOlap::IColumnEngine& GetIndexVerified() const {
        AFL_VERIFY(TablesManager.GetPrimaryIndex());
        return *TablesManager.GetPrimaryIndex();
    }

    template <class T>
    T& MutableIndexAs() {
        return TablesManager.MutablePrimaryIndexAsVerified<T>();
    }

    TTxController& GetProgressTxController() const {
        AFL_VERIFY(ProgressTxController);
        return *ProgressTxController;
    }

    TOperationsManager& GetOperationsManager() const {
        AFL_VERIFY(OperationsManager);
        return *OperationsManager;
    }

    bool HasIndex() const {
        return !!TablesManager.GetPrimaryIndex();
    }

    NOlap::TSnapshot GetLastPlannedSnapshot() const {
        return NOlap::TSnapshot(LastPlannedStep, LastPlannedTxId);
    }

    NOlap::TSnapshot GetLastCompletedTx() const {
        return LastCompletedTx;
    }

    const std::shared_ptr<NOlap::NBackground::TSessionsManager>& GetBackgroundSessionsManager() const {
        return BackgroundSessionsManager;
    }

    const std::shared_ptr<NOlap::IStoragesManager>& GetStoragesManager() const {
        AFL_VERIFY(StoragesManager);
        return StoragesManager;
    }

    const std::shared_ptr<NOlap::NDataLocks::TManager>& GetDataLocksManager() const {
        AFL_VERIFY(DataLocksManager);
        return DataLocksManager;
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_COLUMNSHARD_ACTOR;
    }

    TColumnShard(TTabletStorageInfo* info, const TActorId& tablet);
};

}   // namespace NKikimr::NColumnShard

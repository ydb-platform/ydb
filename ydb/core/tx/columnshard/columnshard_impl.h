#pragma once
#include "defs.h"
#include "background_controller.h"
#include "counters.h"
#include "columnshard.h"
#include "columnshard_common.h"
#include "columnshard_ttl.h"
#include "columnshard_private_events.h"
#include "tables_manager.h"

#include "blobs_action/events/delete_blobs.h"
#include "bg_tasks/events/local.h"
#include "transactions/tx_controller.h"
#include "inflight_request_tracker.h"
#include "counters/columnshard.h"
#include "counters/counters_manager.h"
#include "resource_subscriber/counters.h"
#include "resource_subscriber/task.h"
#include "normalizer/abstract/abstract.h"

#include "export/events/events.h"

#include "data_sharing/destination/events/control.h"
#include "data_sharing/source/events/control.h"
#include "data_sharing/destination/events/transfer.h"
#include "data_sharing/source/events/transfer.h"
#include "data_sharing/manager/sessions.h"
#include "data_sharing/manager/shared_blobs.h"
#include "data_sharing/common/transactions/tx_extension.h"
#include "data_sharing/modification/events/change_owning.h"

#include "subscriber/abstract/manager/manager.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/tablet/tablet_counters.h>
#include <ydb/core/tablet/tablet_pipe_client_cache.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/tiering/common.h>
#include <ydb/core/tx/time_cast/time_cast.h>
#include <ydb/core/tx/tx_processing.h>
#include <ydb/core/tx/locks/locks.h>
#include <ydb/services/metadata/service.h>
#include <ydb/services/metadata/abstract/common.h>

namespace NKikimr::NOlap {
class TCleanupPortionsColumnEngineChanges;
class TCleanupTablesColumnEngineChanges;
class TTTLColumnEngineChanges;
class TChangesWithAppend;
class TCompactColumnEngineChanges;
class TInsertColumnEngineChanges;
class TStoragesManager;

namespace NReader {
class TTxScan;
class TTxInternalScan;
namespace NPlain {
class TIndexScannerConstructor;
}
}

namespace NDataSharing {
class TTxDataFromSource;
class TTxDataAckToSource;
class TTxFinishAckToSource;
class TTxFinishAckFromInitiator;
}

namespace NBackground {
class TSessionsManager;
}

namespace NBlobOperations {
namespace NBlobStorage {
class TWriteAction;
class TOperator;
}
namespace NTier {
class TOperator;
}
}
namespace NCompaction {
class TGeneralCompactColumnEngineChanges;
}
}

namespace NKikimr::NColumnShard {

class TTxFinishAsyncTransaction;
class TTxInsertTableCleanup;
class TTxRemoveSharedBlobs;
class TOperationsManager;
class TWaitEraseTablesTxSubscriber;

extern bool gAllowLogBatchingDefaultValue;

IActor* CreateWriteActor(ui64 tabletId, IWriteController::TPtr writeController, const TInstant deadline);
IActor* CreateColumnShardScan(const TActorId& scanComputeActor, ui32 scanId, ui64 txId);

struct TSettings {
    static constexpr ui32 MAX_ACTIVE_COMPACTIONS = 1;

    static constexpr ui32 MAX_INDEXATIONS_TO_SKIP = 16;
    static constexpr TDuration GuaranteeIndexationInterval = TDuration::Seconds(10);
    static constexpr TDuration DefaultPeriodicWakeupActivationPeriod = TDuration::Seconds(60);
    static constexpr TDuration DefaultStatsReportInterval = TDuration::Seconds(10);
    static constexpr i64 GuaranteeIndexationStartBytesLimit = (i64)5 * 1024 * 1024 * 1024;

    TControlWrapper BlobWriteGrouppingEnabled;
    TControlWrapper CacheDataAfterIndexing;
    TControlWrapper CacheDataAfterCompaction;
    static constexpr ui64 OverloadTxInFlight = 1000;
    static constexpr ui64 OverloadWritesInFlight = 1000;
    static constexpr ui64 OverloadWritesSizeInFlight = 128 * 1024 * 1024;

    TSettings()
        : BlobWriteGrouppingEnabled(1, 0, 1)
        , CacheDataAfterIndexing(1, 0, 1)
        , CacheDataAfterCompaction(1, 0, 1)
    {}

    void RegisterControls(TControlBoard& icb) {
        icb.RegisterSharedControl(BlobWriteGrouppingEnabled, "ColumnShardControls.BlobWriteGrouppingEnabled");
        icb.RegisterSharedControl(CacheDataAfterIndexing, "ColumnShardControls.CacheDataAfterIndexing");
        icb.RegisterSharedControl(CacheDataAfterCompaction, "ColumnShardControls.CacheDataAfterCompaction");
    }
};

using ITransaction = NTabletFlatExecutor::ITransaction;

template <typename T>
using TTransactionBase = NTabletFlatExecutor::TTransactionBase<T>;

class TColumnShard
    : public TActor<TColumnShard>
    , public NTabletFlatExecutor::TTabletExecutedFlat
{
    friend class TTxInsertTableCleanup;
    friend class TTxInit;
    friend class TTxInitSchema;
    friend class TTxUpdateSchema;
    friend class TTxProposeTransaction;
    friend class TTxNotifyTxCompletion;
    friend class TTxPlanStep;
    friend class TTxWrite;
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
    friend class TWaitEraseTablesTxSubscriber;

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

    friend class NOlap::TStoragesManager;

    friend class NOlap::NReader::TTxScan;
    friend class NOlap::NReader::TTxInternalScan;
    friend class NOlap::NReader::NPlain::TIndexScannerConstructor;

    class TStoragesManager;
    friend class TTxController;

    friend class TOperationsManager;
    friend class TWriteOperation;

    friend class TSchemaTransactionOperator;
    friend class TLongTxTransactionOperator;
    friend class TEvWriteTransactionOperator;
    friend class TBackupTransactionOperator;
    friend class IProposeTxOperator;
    friend class TSharingTransactionOperator;


    class TTxProgressTx;
    class TTxProposeCancel;
    // proto
    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvServerConnected::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvColumnShard::TEvProposeTransaction::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvColumnShard::TEvCheckPlannedTransaction::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvColumnShard::TEvCancelTransactionProposal::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvColumnShard::TEvNotifyTxCompletion::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTxProcessing::TEvPlanStep::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvColumnShard::TEvWrite::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvColumnShard::TEvScan::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvColumnShard::TEvInternalScan::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvMediatorTimecast::TEvRegisterTabletResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvMediatorTimecast::TEvNotifyPlanStep::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvWriteBlobsResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvScanStats::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvReadFinished::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvPeriodicWakeup::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvWriteIndex::TPtr& ev, const TActorContext& ctx);
    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev);
    void Handle(NEvents::TDataEvents::TEvWrite::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvWriteDraft::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvGarbageCollectionFinished::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvTieringModified::TPtr& ev, const TActorContext&);
    void Handle(TEvPrivate::TEvNormalizerResult::TPtr& ev, const TActorContext&);

    void Handle(NStat::TEvStatistics::TEvAnalyzeTable::TPtr& ev, const TActorContext& ctx);
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
    void SwitchToWork(const TActorContext& ctx);

    bool IsAnyChannelYellowStop() const {
        return Executor()->GetStats().IsAnyChannelYellowStop;
    }

    bool IsAnyChannelYellowMove() const {
        return Executor()->GetStats().IsAnyChannelYellowMove;
    }

    void OnYellowChannels(const TPutStatus& putStatus) {
        putStatus.OnYellowChannels(Executor());
    }

    void ActivateTiering(const ui64 pathId, const TString& useTiering);
    void OnTieringModified(const std::optional<ui64> pathId = {});
public:
    enum class EOverloadStatus {
        ShardTxInFly /* "shard_tx" */,
        ShardWritesInFly /* "shard_writes" */,
        ShardWritesSizeInFly /* "shard_writes_size" */,
        InsertTable /* "insert_table" */,
        OverloadMetadata /* "overload_metadata" */,
        Disk /* "disk" */,
        None /* "none" */
    };

    // For syslocks
    void IncCounter(NDataShard::ECumulativeCounters counter, ui64 num = 1) const {
        Counters.GetTabletCounters().IncCounter(counter, num);
    }

    void IncCounter(NDataShard::EPercentileCounters counter, ui64 num) const {
        Counters.GetTabletCounters().IncCounter(counter, num);
    }

    void IncCounter(NDataShard::EPercentileCounters counter, const TDuration& latency) const {
        Counters.GetTabletCounters().IncCounter(counter, latency);
    }

    inline TRowVersion LastCompleteTxVersion() const {
        return TRowVersion(LastCompletedTx.GetPlanStep(), LastCompletedTx.GetTxId());
    }

    ui32 Generation() const { return Executor()->Generation(); }

    bool IsUserTable(const TTableId&) const {
        return true;
    }

private:
    void OverloadWriteFail(const EOverloadStatus overloadReason, const NEvWrite::TWriteData& writeData, const ui64 cookie, std::unique_ptr<NActors::IEventBase>&& event, const TActorContext& ctx);
    EOverloadStatus CheckOverloaded(const ui64 tableId) const;

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
            LOG_S_WARN("TColumnShard.StateBroken at " << TabletID()
                       << " unhandled event type: " << ev->GetTypeRewrite()
                       << " event: " << ev->ToString());
            Send(IEventHandle::ForwardOnNondelivery(std::move(ev), NActors::TEvents::TEvUndelivered::ReasonActorUnknown));
            break;
        }
    }

    STFUNC(StateWork) {
        const TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletID())("self_id", SelfId());
        TRACE_EVENT(NKikimrServices::TX_COLUMNSHARD);
        switch (ev->GetTypeRewrite()) {
            hFunc(NMetadata::NProvider::TEvRefreshSubscriberData, Handle);

            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvTabletPipe::TEvServerConnected, Handle);
            HFunc(TEvTabletPipe::TEvServerDisconnected, Handle);
            HFunc(TEvColumnShard::TEvProposeTransaction, Handle);
            HFunc(TEvColumnShard::TEvCheckPlannedTransaction, Handle);
            HFunc(TEvColumnShard::TEvCancelTransactionProposal, Handle);
            HFunc(TEvColumnShard::TEvNotifyTxCompletion, Handle);
            HFunc(TEvColumnShard::TEvScan, Handle);
            HFunc(TEvColumnShard::TEvInternalScan, Handle);
            HFunc(TEvTxProcessing::TEvPlanStep, Handle);
            HFunc(TEvColumnShard::TEvWrite, Handle);
            HFunc(TEvPrivate::TEvWriteBlobsResult, Handle);
            HFunc(TEvMediatorTimecast::TEvRegisterTabletResult, Handle);
            HFunc(TEvMediatorTimecast::TEvNotifyPlanStep, Handle);
            HFunc(TEvPrivate::TEvWriteIndex, Handle);
            HFunc(TEvPrivate::TEvScanStats, Handle);
            HFunc(TEvPrivate::TEvReadFinished, Handle);
            HFunc(TEvPrivate::TEvPeriodicWakeup, Handle);
            HFunc(NEvents::TDataEvents::TEvWrite, Handle);
            HFunc(TEvPrivate::TEvWriteDraft, Handle);
            HFunc(TEvPrivate::TEvGarbageCollectionFinished, Handle);
            HFunc(TEvPrivate::TEvTieringModified, Handle);

            HFunc(NStat::TEvStatistics::TEvAnalyzeTable, Handle);
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
        default:
            if (!HandleDefaultEvents(ev, SelfId())) {
                LOG_S_WARN("TColumnShard.StateWork at " << TabletID()
                           << " unhandled event type: "<< ev->GetTypeRewrite()
                           << " event: " << ev->ToString());
            }
            break;
        }
    }

private:
    std::unique_ptr<TTxController> ProgressTxController;
    std::unique_ptr<TOperationsManager> OperationsManager;
    std::shared_ptr<NOlap::NDataSharing::TSessionsManager> SharingSessionsManager;
    std::shared_ptr<NOlap::IStoragesManager> StoragesManager;
    std::shared_ptr<NOlap::NBackground::TSessionsManager> BackgroundSessionsManager;
    std::shared_ptr<NOlap::NDataLocks::TManager> DataLocksManager;

    using TSchemaPreset = TSchemaPreset;
    using TTableInfo = TTableInfo;

    const TMonotonic CreateInstant = TMonotonic::Now();
    std::optional<TMonotonic> StartInstant;

    struct TLongTxWriteInfo {
        ui64 WriteId;
        ui32 WritePartId;
        NLongTxService::TLongTxId LongTxId;
        ui64 PreparedTxId = 0;
        std::optional<ui32> GranuleShardingVersionId;
    };

    ui64 CurrentSchemeShardId = 0;
    TMessageSeqNo LastSchemaSeqNo;
    std::optional<NKikimrSubDomains::TProcessingParams> ProcessingParams;
    TWriteId LastWriteId = TWriteId{0};
    ui64 LastPlannedStep = 0;
    ui64 LastPlannedTxId = 0;
    NOlap::TSnapshot LastCompletedTx = NOlap::TSnapshot::Zero();
    ui64 LastExportNo = 0;

    ui64 OwnerPathId = 0;
    ui64 StatsReportRound = 0;
    TString OwnerPath;

    TIntrusivePtr<TMediatorTimecastEntry> MediatorTimeCastEntry;
    bool MediatorTimeCastRegistered = false;
    TSet<ui64> MediatorTimeCastWaitingSteps;
    const TDuration PeriodicWakeupActivationPeriod;
    TDuration FailActivationDelay = TDuration::Seconds(1);
    const TDuration StatsReportInterval;
    TInstant LastStatsReport;

    TActorId ResourceSubscribeActor;
    TActorId BufferizationWriteActorId;
    TActorId StatsReportPipe;

    std::unique_ptr<TTabletCountersBase> TabletCountersHolder;
    TCountersManager Counters;

    TInFlightReadsTracker InFlightReadsTracker;
    TTablesManager TablesManager;
    std::shared_ptr<NSubscriber::TManager> Subscribers;
    std::shared_ptr<TTiersManager> Tiers;
    std::unique_ptr<NTabletPipe::IClientCache> PipeClientCache;
    std::unique_ptr<NOlap::TInsertTable> InsertTable;
    NOlap::NResourceBroker::NSubscribe::TTaskContext InsertTaskSubscription;
    NOlap::NResourceBroker::NSubscribe::TTaskContext CompactTaskSubscription;
    NOlap::NResourceBroker::NSubscribe::TTaskContext TTLTaskSubscription;

    bool ProgressTxInFlight = false;
    THashMap<ui64, TInstant> ScanTxInFlight;
    THashMap<TWriteId, TLongTxWriteInfo> LongTxWrites;
    using TPartsForLTXShard = THashMap<ui32, TLongTxWriteInfo*>;
    THashMap<TULID, TPartsForLTXShard> LongTxWritesByUniqueId;
    TMultiMap<NOlap::TSnapshot, TEvColumnShard::TEvScan::TPtr> WaitingScans;
    TBackgroundController BackgroundController;
    TSettings Settings;
    TLimits Limits;
    NOlap::TNormalizationController NormalizerController;
    NDataShard::TSysLocks SysLocks;
    const TDuration MaxReadStaleness;

    void TryRegisterMediatorTimeCast();
    void UnregisterMediatorTimeCast();
    void TryAbortWrites(NIceDb::TNiceDb& db, NOlap::TDbWrapper& dbTable, THashSet<TWriteId>&& writesToAbort);

    bool WaitPlanStep(ui64 step);
    void SendWaitPlanStep(ui64 step);
    void RescheduleWaitingReads();
    NOlap::TSnapshot GetMaxReadVersion() const;
    ui64 GetMinReadStep() const;
    ui64 GetOutdatedStep() const;
    TDuration GetTxCompleteLag() const {
        ui64 mediatorTime = MediatorTimeCastEntry ? MediatorTimeCastEntry->Get(TabletID()) : 0;
        return ProgressTxController->GetTxCompleteLag(mediatorTime);
    }

    TWriteId HasLongTxWrite(const NLongTxService::TLongTxId& longTxId, const ui32 partId) const;
    TWriteId GetLongTxWrite(NIceDb::TNiceDb& db, const NLongTxService::TLongTxId& longTxId, const ui32 partId, const std::optional<ui32> granuleShardingVersionId);
    void AddLongTxWrite(TWriteId writeId, ui64 txId);
    void LoadLongTxWrite(TWriteId writeId, const ui32 writePartId, const NLongTxService::TLongTxId& longTxId, const std::optional<ui32> granuleShardingVersion);
    bool RemoveLongTxWrite(NIceDb::TNiceDb& db, const TWriteId writeId, const ui64 txId);

    TWriteId BuildNextWriteId(NTabletFlatExecutor::TTransactionContext& txc);
    TWriteId BuildNextWriteId(NIceDb::TNiceDb& db);

    void EnqueueProgressTx(const TActorContext& ctx);
    void EnqueueBackgroundActivities(const bool periodic = false);
    virtual void Enqueue(STFUNC_SIG) override;

    void UpdateSchemaSeqNo(const TMessageSeqNo& seqNo, NTabletFlatExecutor::TTransactionContext& txc);
    void ProtectSchemaSeqNo(const NKikimrTxColumnShard::TSchemaSeqNo& seqNoProto, NTabletFlatExecutor::TTransactionContext& txc);

    void RunSchemaTx(const NKikimrTxColumnShard::TSchemaTxBody& body, const NOlap::TSnapshot& version, NTabletFlatExecutor::TTransactionContext& txc);
    void RunInit(const NKikimrTxColumnShard::TInitShard& body, const NOlap::TSnapshot& version, NTabletFlatExecutor::TTransactionContext& txc);
    void RunEnsureTable(const NKikimrTxColumnShard::TCreateTable& body, const NOlap::TSnapshot& version, NTabletFlatExecutor::TTransactionContext& txc);
    void RunAlterTable(const NKikimrTxColumnShard::TAlterTable& body, const NOlap::TSnapshot& version, NTabletFlatExecutor::TTransactionContext& txc);
    void RunDropTable(const NKikimrTxColumnShard::TDropTable& body, const NOlap::TSnapshot& version, NTabletFlatExecutor::TTransactionContext& txc);
    void RunAlterStore(const NKikimrTxColumnShard::TAlterStore& body, const NOlap::TSnapshot& version, NTabletFlatExecutor::TTransactionContext& txc);

    void StartIndexTask(std::vector<const NOlap::TInsertedData*>&& dataToIndex, const i64 bytesToIndex);
    void SetupIndexation();
    void SetupCompaction();
    bool SetupTtl(const THashMap<ui64, NOlap::TTiering>& pathTtls = {});
    void SetupCleanupPortions();
    void SetupCleanupTables();
    void SetupCleanupInsertTable();
    void SetupGC();

    void UpdateInsertTableCounters();
    void UpdateIndexCounters();
    void UpdateResourceMetrics(const TActorContext& ctx, const TUsage& usage);
    ui64 MemoryUsage() const;

    void SendPeriodicStats();
    void FillOlapStats(const TActorContext& ctx, std::unique_ptr<TEvDataShard::TEvPeriodicTableStats>& ev);
    void FillColumnTableStats(const TActorContext& ctx, std::unique_ptr<TEvDataShard::TEvPeriodicTableStats>& ev);

public:
    ui64 TabletTxCounter = 0;

    NOlap::TSnapshot GetLastTxSnapshot() const {
        return NOlap::TSnapshot(LastPlannedStep, LastPlannedTxId);
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

    template <class T>
    T& MutableIndexAs() {
        return TablesManager.MutablePrimaryIndexAsVerified<T>();
    }

    TTxController& GetProgressTxController() const {
        AFL_VERIFY(ProgressTxController);
        return *ProgressTxController;
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

    const NOlap::TInsertTable& GetInsertTable() const {
        AFL_VERIFY(!!InsertTable);
        return *InsertTable;
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_COLUMNSHARD_ACTOR;
    }

    TColumnShard(TTabletStorageInfo* info, const TActorId& tablet);
};

}

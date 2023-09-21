#pragma once
#include "defs.h"
#include "background_controller.h"
#include "counters.h"
#include "columnshard.h"
#include "columnshard_common.h"
#include "columnshard_ttl.h"
#include "columnshard_private_events.h"
#include "blob_manager.h"
#include "tables_manager.h"
#include "tx_controller.h"
#include "inflight_request_tracker.h"
#include "counters/columnshard.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tablet/tablet_counters.h>
#include <ydb/core/tablet/tablet_pipe_client_cache.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tx/ev_write/events.h>
#include <ydb/core/tx/tiering/common.h>
#include <ydb/core/tx/tiering/manager.h>
#include <ydb/core/tx/time_cast/time_cast.h>
#include <ydb/core/tx/tx_processing.h>
#include <ydb/services/metadata/service.h>

namespace NKikimr::NOlap {
class TCleanupColumnEngineChanges;
class TTTLColumnEngineChanges;
class TChangesWithAppend;
class TCompactColumnEngineChanges;
class TInsertColumnEngineChanges;
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

class TTxInsertTableCleanup;
class TOperationsManager;

extern bool gAllowLogBatchingDefaultValue;

IActor* CreateWriteActor(ui64 tabletId, IWriteController::TPtr writeController, const TInstant& deadline);
IActor* CreateReadActor(ui64 tabletId, const NActors::TActorId readBlobsActor,
                        const TActorId& dstActor, const std::shared_ptr<NOlap::IStoragesManager>& storages,
                        std::unique_ptr<TEvColumnShard::TEvReadResult>&& event,
                        NOlap::TReadMetadata::TConstPtr readMetadata,
                        const TInstant& deadline,
                        const TActorId& columnShardActorId,
                        ui64 requestCookie, const TConcreteScanCounters& counters);
IActor* CreateColumnShardScan(const TActorId& scanComputeActor, ui32 scanId, ui64 txId);
IActor* CreateExportActor(const ui64 tabletId, const TActorId& dstActor, TAutoPtr<TEvPrivate::TEvExport> ev);

struct TSettings {
    static constexpr ui32 MAX_ACTIVE_COMPACTIONS = 1;

    static constexpr ui32 MAX_INDEXATIONS_TO_SKIP = 16;

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
    friend class TTxScan;
    friend class TTxWriteIndex;
    friend class TTxExportFinish;
    friend class TTxRunGC;
    friend class TTxProcessGCResult;
    friend class TTxReadBlobRanges;
    friend class NOlap::TCleanupColumnEngineChanges;
    friend class NOlap::TTTLColumnEngineChanges;
    friend class NOlap::TChangesWithAppend;
    friend class NOlap::TCompactColumnEngineChanges;
    friend class NOlap::TInsertColumnEngineChanges;
    friend class NOlap::TColumnEngineChanges;
    friend class NOlap::NCompaction::TGeneralCompactColumnEngineChanges;
    friend class NOlap::NBlobOperations::NBlobStorage::TWriteAction;
    friend class NOlap::NBlobOperations::NBlobStorage::TOperator;
    friend class NOlap::NBlobOperations::NTier::TOperator;

    class TStoragesManager;
    friend class TTxController;

    friend class TOperationsManager;
    friend class TWriteOperation;

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
    void Handle(TEvColumnShard::TEvRead::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvColumnShard::TEvScan::TPtr& ev, const TActorContext& ctx);
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

    ITransaction* CreateTxInitSchema();

    void OnActivateExecutor(const TActorContext& ctx) override;
    void OnDetach(const TActorContext& ctx) override;
    //void OnTabletStop(TEvTablet::TEvTabletStop::TPtr& ev, const TActorContext& ctx);
    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr& ev, const TActorContext& ctx) override;
    //bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext& ctx);

    void DefaultSignalTabletActive(const TActorContext& ctx) override {
        Y_UNUSED(ctx);
    }

    const NTiers::TManager& GetTierManagerVerified(const TString& tierId) const {
        Y_VERIFY(!!Tiers);
        return Tiers->GetManagerVerified(tierId);
    }

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

    void SetCounter(NColumnShard::ESimpleCounters counter, ui64 num) const {
        TabletCounters->Simple()[counter].Set(num);
    }

    void IncCounter(NColumnShard::ECumulativeCounters counter, ui64 num = 1) const {
        TabletCounters->Cumulative()[counter].Increment(num);
    }

    void IncCounter(NColumnShard::EPercentileCounters counter, const TDuration& latency) const {
        TabletCounters->Percentile()[counter].IncrementFor(latency.MicroSeconds());
    }

    void ActivateTiering(const ui64 pathId, const TString& useTiering);
    void OnTieringModified();
public:
    enum class EOverloadStatus {
        Shard /* "shard" */,
        InsertTable /* "insert_table" */,
        Disk /* "disk" */,
        None /* "none" */
    };

private:
    void OverloadWriteFail(const EOverloadStatus overloadReason, const NEvWrite::TWriteData& writeData, std::unique_ptr<NActors::IEventBase>&& event, const TActorContext& ctx);
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
            Send(IEventHandle::ForwardOnNondelivery(ev, TEvents::TEvUndelivered::ReasonActorUnknown));
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
            HFunc(TEvTxProcessing::TEvPlanStep, Handle);
            HFunc(TEvColumnShard::TEvWrite, Handle);
            HFunc(TEvColumnShard::TEvRead, Handle);
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
    TTxController ProgressTxController;
    TOperationsManager OperationsManager;

    struct TAlterMeta {
        NKikimrTxColumnShard::TSchemaTxBody Body;
        THashSet<TActorId> NotifySubscribers;

        bool Validate(const NOlap::ISnapshotSchema::TPtr& schema) const;
    };

    struct TCommitMeta {
        THashSet<TWriteId> WriteIds;

        void AddWriteId(TWriteId id) {
            WriteIds.insert(id);
        }
    };

    using TSchemaPreset = TSchemaPreset;
    using TTableInfo = TTableInfo;

    struct TLongTxWriteInfo {
        ui64 WriteId;
        ui32 WritePartId;
        NLongTxService::TLongTxId LongTxId;
        ui64 PreparedTxId = 0;
    };

    class TWritesMonitor {
    private:
        TColumnShard& Owner;
        ui64 WritesInFlight = 0;
        ui64 WritesSizeInFlight = 0;

    public:
        class TGuard: public TNonCopyable {
            friend class TWritesMonitor;
        private:
            TWritesMonitor& Owner;

            explicit TGuard(TWritesMonitor& owner)
                : Owner(owner)
            {}

        public:
            ~TGuard() {
                Owner.UpdateCounters();
            }
        };

        TWritesMonitor(TColumnShard& owner)
            : Owner(owner)
        {}

        TGuard RegisterWrite(const ui64 dataSize) {
            ++WritesInFlight;
            WritesSizeInFlight += dataSize;
            return TGuard(*this);
        }

        TGuard FinishWrite(const ui64 dataSize) {
            Y_VERIFY(WritesInFlight > 0);
            Y_VERIFY(WritesSizeInFlight >= dataSize);
            --WritesInFlight;
            WritesSizeInFlight -= dataSize;
            return TGuard(*this);
        }

        bool ShardOverloaded() const {
            ui64 txLimit = Owner.Settings.OverloadTxInFlight;
            ui64 writesLimit = Owner.Settings.OverloadWritesInFlight;
            ui64 writesSizeLimit = Owner.Settings.OverloadWritesSizeInFlight;
            return  (txLimit && Owner.Executor()->GetStats().TxInFly > txLimit) ||
                    (writesLimit && WritesInFlight > writesLimit) ||
                    (writesSizeLimit && WritesSizeInFlight > writesSizeLimit);
        }

        TString DebugString() const {
            return TStringBuilder() << "TWritesMonitor: inflight " << WritesInFlight << " (" << WritesSizeInFlight << " bytes)";
        }

    private:
        void UpdateCounters() {
            Owner.SetCounter(COUNTER_WRITES_IN_FLY, WritesInFlight);
        }
    };

    ui64 CurrentSchemeShardId = 0;
    TMessageSeqNo LastSchemaSeqNo;
    std::optional<NKikimrSubDomains::TProcessingParams> ProcessingParams;
    TWriteId LastWriteId = TWriteId{0};
    ui64 LastPlannedStep = 0;
    ui64 LastPlannedTxId = 0;
    ui64 LastExportNo = 0;

    ui64 OwnerPathId = 0;
    ui64 TabletTxCounter = 0;
    ui64 StatsReportRound = 0;
    ui32 SkippedIndexations = TSettings::MAX_INDEXATIONS_TO_SKIP; // Force indexation on tablet init
    TString OwnerPath;

    TIntrusivePtr<TMediatorTimecastEntry> MediatorTimeCastEntry;
    bool MediatorTimeCastRegistered = false;
    TSet<ui64> MediatorTimeCastWaitingSteps;
    TDuration MaxReadStaleness = TDuration::Minutes(5); // TODO: Make configurable?
    TDuration ActivationPeriod = TDuration::Seconds(60);
    TDuration FailActivationDelay = TDuration::Seconds(1);
    TDuration StatsReportInterval = TDuration::Seconds(10);
    TInstant LastAccessTime;
    TInstant LastStatsReport;

    TActorId BlobsReadActor;
    TActorId StatsReportPipe;

    std::shared_ptr<NOlap::IStoragesManager> StoragesManager;
    TInFlightReadsTracker InFlightReadsTracker;
    TTablesManager TablesManager;
    bool TiersInitializedFlag = false;
    std::shared_ptr<TTiersManager> Tiers;
    std::unique_ptr<TTabletCountersBase> TabletCountersPtr;
    TTabletCountersBase* TabletCounters;
    std::unique_ptr<NTabletPipe::IClientCache> PipeClientCache;
    std::unique_ptr<NOlap::TInsertTable> InsertTable;
    const TScanCounters ReadCounters;
    const TScanCounters ScanCounters;
    const TIndexationCounters CompactionCounters = TIndexationCounters("GeneralCompaction");
    const TIndexationCounters IndexationCounters = TIndexationCounters("Indexation");
    const TIndexationCounters EvictionCounters = TIndexationCounters("Eviction");

    const TCSCounters CSCounters;
    TWritesMonitor WritesMonitor;

    bool ProgressTxInFlight = false;
    THashMap<ui64, TInstant> ScanTxInFlight;
    THashMap<ui64, TAlterMeta> AltersInFlight;
    THashMap<ui64, TCommitMeta> CommitsInFlight; // key is TxId from propose
    THashMap<TWriteId, TLongTxWriteInfo> LongTxWrites;
    using TPartsForLTXShard = THashMap<ui32, TLongTxWriteInfo*>;
    THashMap<TULID, TPartsForLTXShard> LongTxWritesByUniqueId;
    TMultiMap<TRowVersion, TEvColumnShard::TEvRead::TPtr> WaitingReads;
    TMultiMap<TRowVersion, TEvColumnShard::TEvScan::TPtr> WaitingScans;
    ui32 ActiveEvictions = 0;
    TBackgroundController BackgroundController;
    TSettings Settings;
    TLimits Limits;
    TCompactionLimits CompactionLimits;

    void TryRegisterMediatorTimeCast();
    void UnregisterMediatorTimeCast();

    bool WaitPlanStep(ui64 step);
    void SendWaitPlanStep(ui64 step);
    void RescheduleWaitingReads();
    TRowVersion GetMaxReadVersion() const;
    ui64 GetMinReadStep() const;
    ui64 GetOutdatedStep() const;

    TWriteId HasLongTxWrite(const NLongTxService::TLongTxId& longTxId, const ui32 partId);
    TWriteId GetLongTxWrite(NIceDb::TNiceDb& db, const NLongTxService::TLongTxId& longTxId, const ui32 partId);
    void AddLongTxWrite(TWriteId writeId, ui64 txId);
    void LoadLongTxWrite(TWriteId writeId, const ui32 writePartId, const NLongTxService::TLongTxId& longTxId);
    bool RemoveLongTxWrite(NIceDb::TNiceDb& db, TWriteId writeId, ui64 txId = 0);
    bool AbortTx(const ui64 txId, const NKikimrTxColumnShard::ETransactionKind& txKind, NTabletFlatExecutor::TTransactionContext& txc);
    bool LoadTx(const ui64 txId, const NKikimrTxColumnShard::ETransactionKind& txKind, const TString& txBody);
    void TryAbortWrites(NIceDb::TNiceDb& db, NOlap::TDbWrapper& dbTable, THashSet<TWriteId>&& writesToAbort);

    TWriteId BuildNextWriteId(NTabletFlatExecutor::TTransactionContext& txc);
    TWriteId BuildNextWriteId(NIceDb::TNiceDb& db);

    void EnqueueProgressTx(const TActorContext& ctx);
    void EnqueueBackgroundActivities(bool periodic = false, TBackgroundActivity activity = TBackgroundActivity::All());
    virtual void Enqueue(STFUNC_SIG) override;

    void UpdateSchemaSeqNo(const TMessageSeqNo& seqNo, NTabletFlatExecutor::TTransactionContext& txc);
    void ProtectSchemaSeqNo(const NKikimrTxColumnShard::TSchemaSeqNo& seqNoProto, NTabletFlatExecutor::TTransactionContext& txc);

    void RunSchemaTx(const NKikimrTxColumnShard::TSchemaTxBody& body, const TRowVersion& version, NTabletFlatExecutor::TTransactionContext& txc);
    void RunInit(const NKikimrTxColumnShard::TInitShard& body, const TRowVersion& version, NTabletFlatExecutor::TTransactionContext& txc);
    void RunEnsureTable(const NKikimrTxColumnShard::TCreateTable& body, const TRowVersion& version, NTabletFlatExecutor::TTransactionContext& txc);
    void RunAlterTable(const NKikimrTxColumnShard::TAlterTable& body, const TRowVersion& version, NTabletFlatExecutor::TTransactionContext& txc);
    void RunDropTable(const NKikimrTxColumnShard::TDropTable& body, const TRowVersion& version, NTabletFlatExecutor::TTransactionContext& txc);
    void RunAlterStore(const NKikimrTxColumnShard::TAlterStore& body, const TRowVersion& version, NTabletFlatExecutor::TTransactionContext& txc);

#ifndef KIKIMR_DISABLE_S3_OPS
    NWrappers::NExternalStorage::IExternalStorageOperator::TPtr GetTierStorageOperator(const TString& tierId) const;
#endif

    void SetupIndexation();
    void SetupCompaction();
    bool SetupTtl(const THashMap<ui64, NOlap::TTiering>& pathTtls = {}, const bool force = false);
    void SetupCleanup();
    void SetupCleanupInsertTable();
    void SetupGC();

    void UpdateInsertTableCounters();
    void UpdateIndexCounters();
    void UpdateResourceMetrics(const TActorContext& ctx, const TUsage& usage);
    ui64 MemoryUsage() const;
    void SendPeriodicStats();
public:
    const TActorId& GetBlobsReadActorId() const {
        return BlobsReadActor;
    }

    const std::shared_ptr<NOlap::IStoragesManager>& GetStoragesManager() const {
        return StoragesManager;
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_COLUMNSHARD_ACTOR;
    }

    TColumnShard(TTabletStorageInfo* info, const TActorId& tablet);
};

}

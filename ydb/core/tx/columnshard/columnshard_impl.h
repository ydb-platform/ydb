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
class TInGranuleCompactColumnEngineChanges;
class TSplitCompactColumnEngineChanges;
class TInsertColumnEngineChanges;
}

namespace NKikimr::NColumnShard {

class TOperationsManager;

extern bool gAllowLogBatchingDefaultValue;

IActor* CreateIndexingActor(ui64 tabletId, const TActorId& parent, const TIndexationCounters& counters);
IActor* CreateCompactionActor(ui64 tabletId, const TActorId& parent, const ui64 workers);
IActor* CreateEvictionActor(ui64 tabletId, const TActorId& parent, const TIndexationCounters& counters);
IActor* CreateWriteActor(ui64 tabletId, IWriteController::TPtr writeController, TBlobBatch&& blobBatch, const TInstant& deadline, const ui64 maxSmallBlobSize);
IActor* CreateReadActor(ui64 tabletId,
                        const TActorId& dstActor,
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
    TControlWrapper MaxSmallBlobSize;
    static constexpr ui64 OverloadTxInFlight = 1000;
    static constexpr ui64 OverloadWritesInFlight = 1000;
    static constexpr ui64 OverloadWritesSizeInFlight = 128 * 1024 * 1024;

    TSettings()
        : BlobWriteGrouppingEnabled(1, 0, 1)
        , CacheDataAfterIndexing(1, 0, 1)
        , CacheDataAfterCompaction(1, 0, 1)
        , MaxSmallBlobSize(0, 0, 8000000)
    {}

    void RegisterControls(TControlBoard& icb) {
        icb.RegisterSharedControl(BlobWriteGrouppingEnabled, "ColumnShardControls.BlobWriteGrouppingEnabled");
        icb.RegisterSharedControl(CacheDataAfterIndexing, "ColumnShardControls.CacheDataAfterIndexing");
        icb.RegisterSharedControl(CacheDataAfterCompaction, "ColumnShardControls.CacheDataAfterCompaction");
        icb.RegisterSharedControl(MaxSmallBlobSize, "ColumnShardControls.MaxSmallBlobSize");
    }
};

using ITransaction = NTabletFlatExecutor::ITransaction;

template <typename T>
using TTransactionBase = NTabletFlatExecutor::TTransactionBase<T>;

class TColumnShard
    : public TActor<TColumnShard>
    , public NTabletFlatExecutor::TTabletExecutedFlat
{
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
    friend class TTxForget;
    friend class TTxRunGC;
    friend class TTxProcessGCResult;
    friend class TTxReadBlobRanges;
    friend class NOlap::TCleanupColumnEngineChanges;
    friend class NOlap::TTTLColumnEngineChanges;
    friend class NOlap::TChangesWithAppend;
    friend class NOlap::TCompactColumnEngineChanges;
    friend class NOlap::TInGranuleCompactColumnEngineChanges;
    friend class NOlap::TSplitCompactColumnEngineChanges;
    friend class NOlap::TInsertColumnEngineChanges;
    friend class NOlap::TColumnEngineChanges;

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
    void Handle(TEvColumnShard::TEvReadBlobRanges::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvMediatorTimecast::TEvRegisterTabletResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvMediatorTimecast::TEvNotifyPlanStep::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvWriteBlobsResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvScanStats::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvReadFinished::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvPeriodicWakeup::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvWriteIndex::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvExport::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvForget::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr& ev, const TActorContext& ctx);
    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev);
    void Handle(NEvents::TDataEvents::TEvWrite::TPtr& ev, const TActorContext& ctx);

    ITransaction* CreateTxInitSchema();
    ITransaction* CreateTxRunGc();

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

public:
    enum class EOverloadStatus {
        Shard /* "shard" */,
        Granule /* "granule" */,
        InsertTable /* "insert_table" */,
        Disk /* "disk" */,
        None
    };

private:
    void OverloadWriteFail(const EOverloadStatus& overloadReason, const NEvWrite::TWriteData& writeData, std::unique_ptr<NActors::IEventBase>&& event, const TActorContext& ctx);
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
        TLogContextGuard gLogging(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletID()));
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
            HFunc(TEvColumnShard::TEvReadBlobRanges, Handle);
            HFunc(TEvPrivate::TEvWriteBlobsResult, Handle);
            HFunc(TEvMediatorTimecast::TEvRegisterTabletResult, Handle);
            HFunc(TEvMediatorTimecast::TEvNotifyPlanStep, Handle);
            HFunc(TEvBlobStorage::TEvCollectGarbageResult, Handle);
            HFunc(TEvPrivate::TEvWriteIndex, Handle);
            HFunc(TEvPrivate::TEvExport, Handle);
            HFunc(TEvPrivate::TEvForget, Handle);
            HFunc(TEvPrivate::TEvScanStats, Handle);
            HFunc(TEvPrivate::TEvReadFinished, Handle);
            HFunc(TEvPrivate::TEvPeriodicWakeup, Handle);
            HFunc(NEvents::TDataEvents::TEvWrite, Handle);
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
        ui64 MetaShard{};
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

    TTablesManager TablesManager;

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

    TActorId IndexingActor;     // It's logically bounded to 1: we move each portion of data to multiple indices.
    TActorId CompactionActor;   // It's memory bounded to 1: we have no memory for parallel compaction.
    TActorId EvictionActor;
    TActorId StatsReportPipe;

    std::shared_ptr<TTiersManager> Tiers;
    std::unique_ptr<TTabletCountersBase> TabletCountersPtr;
    TTabletCountersBase* TabletCounters;
    std::unique_ptr<NTabletPipe::IClientCache> PipeClientCache;
    std::unique_ptr<NOlap::TInsertTable> InsertTable;
    const TScanCounters ReadCounters;
    const TScanCounters ScanCounters;
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
    std::unique_ptr<TBlobManager> BlobManager;
    TInFlightReadsTracker InFlightReadsTracker;
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
    void CleanForgottenBlobs(const TActorContext& ctx, const THashSet<TUnifiedBlobId>& allowList = {});

    void UpdateSchemaSeqNo(const TMessageSeqNo& seqNo, NTabletFlatExecutor::TTransactionContext& txc);
    void ProtectSchemaSeqNo(const NKikimrTxColumnShard::TSchemaSeqNo& seqNoProto, NTabletFlatExecutor::TTransactionContext& txc);

    void RunSchemaTx(const NKikimrTxColumnShard::TSchemaTxBody& body, const TRowVersion& version, NTabletFlatExecutor::TTransactionContext& txc);
    void RunInit(const NKikimrTxColumnShard::TInitShard& body, const TRowVersion& version, NTabletFlatExecutor::TTransactionContext& txc);
    void RunEnsureTable(const NKikimrTxColumnShard::TCreateTable& body, const TRowVersion& version, NTabletFlatExecutor::TTransactionContext& txc);
    void RunAlterTable(const NKikimrTxColumnShard::TAlterTable& body, const TRowVersion& version, NTabletFlatExecutor::TTransactionContext& txc);
    void RunDropTable(const NKikimrTxColumnShard::TDropTable& body, const TRowVersion& version, NTabletFlatExecutor::TTransactionContext& txc);
    void RunAlterStore(const NKikimrTxColumnShard::TAlterStore& body, const TRowVersion& version, NTabletFlatExecutor::TTransactionContext& txc);

    void MapExternBlobs(const TActorContext& ctx, NOlap::TReadMetadata& metadata);
    TActorId GetS3ActorForTier(const TString& tierId) const;
    void Reexport(const TActorContext& ctx);
    void ExportBlobs(const TActorContext& ctx, std::unique_ptr<TEvPrivate::TEvExport>&& ev);
    void ForgetTierBlobs(const TActorContext& ctx, const TString& tierName, std::vector<NOlap::TEvictedBlob>&& blobs) const;
    void ForgetBlobs(const TActorContext& ctx, const THashMap<TString, THashSet<NOlap::TEvictedBlob>>& evictedBlobs);
    bool GetExportedBlob(const TActorContext& ctx, TActorId dst, ui64 cookie, const TString& tierName,
                         NOlap::TEvictedBlob&& evicted, std::vector<NOlap::TBlobRange>&& ranges);

    void ScheduleNextGC(const TActorContext& ctx, bool cleanupOnly = false);

    void SetupIndexation();
    void SetupCompaction();
    std::unique_ptr<TEvPrivate::TEvEviction> SetupTtl(const THashMap<ui64, NOlap::TTiering>& pathTtls = {},
                                                      bool force = false);
    std::unique_ptr<TEvPrivate::TEvWriteIndex> SetupCleanup();

    void UpdateBlobMangerCounters();
    void UpdateInsertTableCounters();
    void UpdateIndexCounters();
    void UpdateResourceMetrics(const TActorContext& ctx, const TUsage& usage);
    ui64 MemoryUsage() const;
    void SendPeriodicStats();
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_COLUMNSHARD_ACTOR;
    }

    TColumnShard(TTabletStorageInfo* info, const TActorId& tablet);
};

}

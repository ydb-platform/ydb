#pragma once
#include "defs.h"
#include "counters.h"
#include "columnshard.h"
#include "columnshard_common.h"
#include "columnshard_ttl.h"
#include "columnshard_private_events.h"
#include "blob_manager.h"
#include "tables_manager.h"
#include "inflight_request_tracker.h"

#include <ydb/core/tablet/tablet_counters.h>
#include <ydb/core/tablet/tablet_pipe_client_cache.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/tiering/common.h>
#include <ydb/core/tx/tiering/manager.h>
#include <ydb/core/tx/time_cast/time_cast.h>
#include <ydb/core/tx/tx_processing.h>
#include <ydb/services/metadata/service.h>

namespace NKikimr::NColumnShard {

extern bool gAllowLogBatchingDefaultValue;

IActor* CreateIndexingActor(ui64 tabletId, const TActorId& parent);
IActor* CreateCompactionActor(ui64 tabletId, const TActorId& parent);
IActor* CreateEvictionActor(ui64 tabletId, const TActorId& parent);
IActor* CreateWriteActor(ui64 tabletId, const NOlap::TIndexInfo& indexTable,
                         const TActorId& dstActor, TBlobBatch&& blobBatch, bool blobGrouppingEnabled,
                         TAutoPtr<TEvColumnShard::TEvWrite> ev, const TInstant& deadline = TInstant::Max());
IActor* CreateWriteActor(ui64 tabletId, const NOlap::TIndexInfo& indexTable,
                         const TActorId& dstActor, TBlobBatch&& blobBatch, bool blobGrouppingEnabled,
                         TAutoPtr<TEvPrivate::TEvWriteIndex> ev, const TInstant& deadline = TInstant::Max());
IActor* CreateReadActor(ui64 tabletId,
                        const TActorId& dstActor,
                        std::unique_ptr<TEvColumnShard::TEvReadResult>&& event,
                        NOlap::TReadMetadata::TConstPtr readMetadata,
                        const TInstant& deadline,
                        const TActorId& columnShardActorId,
                        ui64 requestCookie, const TScanCounters& counters);
IActor* CreateColumnShardScan(const TActorId& scanComputeActor, ui32 scanId, ui64 txId);
IActor* CreateExportActor(const ui64 tabletId, const TActorId& dstActor, TAutoPtr<TEvPrivate::TEvExport> ev);

struct TSettings {
    static constexpr ui32 MAX_INDEXATIONS_TO_SKIP = 16;

    TControlWrapper BlobWriteGrouppingEnabled;
    TControlWrapper CacheDataAfterIndexing;
    TControlWrapper CacheDataAfterCompaction;
    TControlWrapper MaxSmallBlobSize;
    TControlWrapper OverloadTxInFly;
    TControlWrapper OverloadWritesInFly;

    TSettings()
        : BlobWriteGrouppingEnabled(1, 0, 1)
        , CacheDataAfterIndexing(1, 0, 1)
        , CacheDataAfterCompaction(1, 0, 1)
        , MaxSmallBlobSize(0, 0, 8000000)
        , OverloadTxInFly(1000, 0, 10000)
        , OverloadWritesInFly(1000, 0, 10000)
    {}

    void RegisterControls(TControlBoard& icb) {
        icb.RegisterSharedControl(BlobWriteGrouppingEnabled, "ColumnShardControls.BlobWriteGrouppingEnabled");
        icb.RegisterSharedControl(CacheDataAfterIndexing, "ColumnShardControls.CacheDataAfterIndexing");
        icb.RegisterSharedControl(CacheDataAfterCompaction, "ColumnShardControls.CacheDataAfterCompaction");
        icb.RegisterSharedControl(MaxSmallBlobSize, "ColumnShardControls.MaxSmallBlobSize");
        icb.RegisterSharedControl(OverloadTxInFly, "ColumnShardControls.OverloadTxInFly");
        icb.RegisterSharedControl(OverloadWritesInFly, "ColumnShardControls.OverloadWritesInFly");
    }
};

class TBackgroundActivity {
public:
    enum EBackActivity : ui32 {
        NONE = 0x00,
        INDEX = 0x01,
        COMPACT = 0x02,
        CLEAN  = 0x04,
        TTL = 0x08,
        ALL = 0xffff
    };

    static TBackgroundActivity Indexation() { return TBackgroundActivity(INDEX); }
    static TBackgroundActivity Compaction() { return TBackgroundActivity(COMPACT); }
    static TBackgroundActivity Cleanup() { return TBackgroundActivity(CLEAN); }
    static TBackgroundActivity Ttl() { return TBackgroundActivity(TTL); }
    static TBackgroundActivity All() { return TBackgroundActivity(ALL); }
    static TBackgroundActivity None() { return TBackgroundActivity(NONE); }

    TBackgroundActivity() = default;

    bool HasIndexation() const { return Activity & INDEX; }
    bool HasCompaction() const { return Activity & COMPACT; }
    bool HasCleanup() const { return Activity & CLEAN; }
    bool HasTtl() const { return Activity & TTL; }
    bool HasAll() const { return Activity == ALL; }
    bool IndexationOnly() const { return Activity == INDEX; }

private:
    EBackActivity Activity = NONE;

    TBackgroundActivity(EBackActivity activity)
        : Activity(activity)
    {}
};

using ITransaction = NTabletFlatExecutor::ITransaction;

template <typename T>
using TTransactionBase = NTabletFlatExecutor::TTransactionBase<T>;

class TColumnShard
    : public TActor<TColumnShard>
    , public NTabletFlatExecutor::TTabletExecutedFlat
{
    friend class TIndexingActor;
    friend class TCompactionActor;
    friend class TEvictionActor;
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

    class TTxProgressTx;
    class TTxProposeCancel;

    // proto
    void Handle(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx);
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
    void Handle(TEvPrivate::TEvScanStats::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvReadFinished::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvPeriodicWakeup::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvWriteIndex::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvExport::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvForget::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr& ev, const TActorContext& ctx);
    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev);


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

    void BecomeBroken(const TActorContext& ctx);
    void SwitchToWork(const TActorContext& ctx);

    bool IsAnyChannelYellowStop() const {
        return Executor()->GetStats().IsAnyChannelYellowStop;
    }

    bool IsAnyChannelYellowMove() const {
        return Executor()->GetStats().IsAnyChannelYellowMove;
    }

    void OnYellowChannels(TVector<ui32>&& yellowMove, TVector<ui32>&& yellowStop) {
        if (yellowMove.size() || yellowStop.size()) {
            Executor()->OnYellowChannels(std::move(yellowMove), std::move(yellowStop));
        }
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

protected:
    STFUNC(StateInit) {
        TRACE_EVENT(NKikimrServices::TX_COLUMNSHARD);
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvPoisonPill, Handle);
        default:
            StateInitImpl(ev, SelfId());
        }
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
        TRACE_EVENT(NKikimrServices::TX_COLUMNSHARD);
        switch (ev->GetTypeRewrite()) {
            hFunc(NMetadata::NProvider::TEvRefreshSubscriberData, Handle);
            HFunc(TEvents::TEvPoisonPill, Handle);
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
            HFunc(TEvMediatorTimecast::TEvRegisterTabletResult, Handle);
            HFunc(TEvMediatorTimecast::TEvNotifyPlanStep, Handle);
            HFunc(TEvBlobStorage::TEvCollectGarbageResult, Handle);
            HFunc(TEvPrivate::TEvWriteIndex, Handle);
            HFunc(TEvPrivate::TEvExport, Handle);
            HFunc(TEvPrivate::TEvForget, Handle);
            HFunc(TEvPrivate::TEvScanStats, Handle);
            HFunc(TEvPrivate::TEvReadFinished, Handle);
            HFunc(TEvPrivate::TEvPeriodicWakeup, Handle);
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
    struct TBasicTxInfo {
        ui64 TxId;
        ui64 MaxStep = Max<ui64>();
        ui64 PlanStep = 0;
        TActorId Source;
        ui64 Cookie = 0;
        NKikimrTxColumnShard::ETransactionKind TxKind;
    };

    struct TDeadlineQueueItem {
        ui64 MaxStep;
        ui64 TxId;

        TDeadlineQueueItem() = default;
        TDeadlineQueueItem(ui64 maxStep, ui64 txId)
            : MaxStep(maxStep)
            , TxId(txId)
        { }

        inline bool operator<(const TDeadlineQueueItem& rhs) const {
            return MaxStep < rhs.MaxStep || (MaxStep == rhs.MaxStep && TxId < rhs.TxId);
        }
    };

    struct TPlanQueueItem {
        ui64 Step;
        ui64 TxId;

        TPlanQueueItem() = default;
        TPlanQueueItem(ui64 step, ui64 txId)
            : Step(step)
            , TxId(txId)
        { }

        inline bool operator<(const TPlanQueueItem& rhs) const {
            return Step < rhs.Step || (Step == rhs.Step && TxId < rhs.TxId);
        }
    };

    struct TAlterMeta {
        NKikimrTxColumnShard::TSchemaTxBody Body;
        THashSet<TActorId> NotifySubscribers;

        bool Validate() const;
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

    ui64 CurrentSchemeShardId = 0;
    TMessageSeqNo LastSchemaSeqNo;
    std::optional<NKikimrSubDomains::TProcessingParams> ProcessingParams;
    TWriteId LastWriteId = TWriteId{0};
    ui64 LastPlannedStep = 0;
    ui64 LastPlannedTxId = 0;
    ui64 LastCompactedGranule = 0;
    ui64 LastExportNo = 0;
    ui64 WritesInFly = 0;
    ui64 OwnerPathId = 0;
    ui64 StatsReportRound = 0;
    ui64 BackgroundActivation = 0;
    ui32 SkippedIndexations = TSettings::MAX_INDEXATIONS_TO_SKIP; // Force indexation on tablet init
    TString OwnerPath;

    TIntrusivePtr<TMediatorTimecastEntry> MediatorTimeCastEntry;
    bool MediatorTimeCastRegistered = false;
    TSet<ui64> MediatorTimeCastWaitingSteps;
    TDuration MaxReadStaleness = TDuration::Minutes(5); // TODO: Make configurable?
    TDuration MaxCommitTxDelay = TDuration::Seconds(30); // TODO: Make configurable?
    TDuration ActivationPeriod = TDuration::Seconds(60);
    TDuration FailActivationDelay = TDuration::Seconds(1);
    TDuration StatsReportInterval = TDuration::Seconds(10);
    TInstant LastAccessTime;
    TInstant LastPeriodicBackActivation;
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
    TBatchCache BatchCache;
    TScanCounters ReadCounters;
    TScanCounters ScanCounters;

    THashMap<ui64, TBasicTxInfo> BasicTxInfo;
    TSet<TDeadlineQueueItem> DeadlineQueue;
    std::set<TPlanQueueItem> PlanQueue;
    std::set<TPlanQueueItem> RunningQueue;
    bool ProgressTxInFlight = false;
    THashMap<ui64, TInstant> ScanTxInFlight;
    THashMap<ui64, TAlterMeta> AltersInFlight;
    THashMap<ui64, TCommitMeta> CommitsInFlight; // key is TxId from propose
    THashMap<TWriteId, TLongTxWriteInfo> LongTxWrites;
    using TPartsForLTXShard = THashMap<ui32, TLongTxWriteInfo*>;
    THashMap<TULID, TPartsForLTXShard> LongTxWritesByUniqueId;
    TMultiMap<TRowVersion, TEvColumnShard::TEvRead::TPtr> WaitingReads;
    TMultiMap<TRowVersion, TEvColumnShard::TEvScan::TPtr> WaitingScans;
    bool ActiveIndexingOrCompaction = false;
    bool ActiveCleanup = false;
    bool ActiveTtl = false;
    ui32 ActiveEvictions = 0;
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
    ui64 GetAllowedStep() const;
    bool HaveOutdatedTxs() const;

    bool ShardOverloaded() const {
        ui64 txLimit = Settings.OverloadTxInFly;
        ui64 writesLimit = Settings.OverloadWritesInFly;
        return (txLimit && Executor()->GetStats().TxInFly > txLimit) ||
           (writesLimit && WritesInFly > writesLimit);
    }

    bool InsertTableOverloaded() const {
        return InsertTable && InsertTable->HasOverloaded();
    }

    bool IndexOverloaded() const {
        return TablesManager.IndexOverloaded();
    }

    TWriteId HasLongTxWrite(const NLongTxService::TLongTxId& longTxId, const ui32 partId);
    TWriteId GetLongTxWrite(NIceDb::TNiceDb& db, const NLongTxService::TLongTxId& longTxId, const ui32 partId);
    void AddLongTxWrite(TWriteId writeId, ui64 txId);
    void LoadLongTxWrite(TWriteId writeId, const ui32 writePartId, const NLongTxService::TLongTxId& longTxId);
    bool RemoveLongTxWrite(NIceDb::TNiceDb& db, TWriteId writeId, ui64 txId = 0);
    bool RemoveTx(NTable::TDatabase& database, ui64 txId);
    void TryAbortWrites(NIceDb::TNiceDb& db, NOlap::TDbWrapper& dbTable, THashSet<TWriteId>&& writesToAbort);

    void EnqueueProgressTx(const TActorContext& ctx);
    void EnqueueBackgroundActivities(bool periodic = false, TBackgroundActivity activity = TBackgroundActivity::All());

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
    void ExportBlobs(const TActorContext& ctx, ui64 exportNo, const TString& tierName, ui64 pathId,
        TEvPrivate::TEvExport::TBlobDataMap&& blobsInfo) const;
    void ForgetTierBlobs(const TActorContext& ctx, const TString& tierName, std::vector<NOlap::TEvictedBlob>&& blobs) const;
    void ForgetBlobs(const TActorContext& ctx, const THashSet<NOlap::TEvictedBlob>& blobs);
    bool GetExportedBlob(const TActorContext& ctx, TActorId dst, ui64 cookie, const TString& tierName,
                         NOlap::TEvictedBlob&& evicted, std::vector<NOlap::TBlobRange>&& ranges);

    void ScheduleNextGC(const TActorContext& ctx, bool cleanupOnly = false);

    std::unique_ptr<TEvPrivate::TEvIndexing> SetupIndexation();
    std::unique_ptr<TEvPrivate::TEvCompaction> SetupCompaction();
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

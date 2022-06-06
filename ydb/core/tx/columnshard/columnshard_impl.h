#pragma once
#include "defs.h"
#include "columnshard.h"
#include "columnshard_common.h"
#include "columnshard_ttl.h"
#include "columnshard_txs.h"
#include "blob_manager.h"
#include "inflight_request_tracker.h"

#include <ydb/core/tx/tx_processing.h>
#include <ydb/core/tx/time_cast/time_cast.h>
#include <ydb/core/tablet/tablet_counters.h>
#include <ydb/core/tablet/tablet_pipe_client_cache.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NKikimr::NColumnShard {

extern bool gAllowLogBatchingDefaultValue;

struct TSettings {
    TControlWrapper BlobWriteGrouppingEnabled;
    TControlWrapper CacheDataAfterIndexing;
    TControlWrapper CacheDataAfterCompaction;
    TControlWrapper MaxSmallBlobSize;

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
    void Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr& ev, const TActorContext& ctx);

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

    void Die(const TActorContext& ctx) override {
        // TODO
        UnregisterMediatorTimeCast();
        return IActor::Die(ctx);
    }

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

protected:
    STFUNC(StateInit) {
        TRACE_EVENT(NKikimrServices::TX_COLUMNSHARD);
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvPoisonPill, Handle);
        default:
            StateInitImpl(ev, ctx);
        }
    }

    STFUNC(StateBroken) {
        TRACE_EVENT(NKikimrServices::TX_COLUMNSHARD);
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTablet::TEvTabletDead, HandleTabletDead);
        default:
            LOG_S_WARN("TColumnShard.StateBroken at " << TabletID()
                       << " unhandled event type: " << ev->GetTypeRewrite()
                       << " event: " << (ev->HasEvent() ? ev->GetBase()->ToString().data() : "serialized?"));
            ctx.Send(ev->ForwardOnNondelivery(TEvents::TEvUndelivered::ReasonActorUnknown));
            break;
        }
    }

    STFUNC(StateWork) {
        TRACE_EVENT(NKikimrServices::TX_COLUMNSHARD);
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvPoisonPill, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvTabletPipe::TEvServerConnected, Handle);
            HFunc(TEvTabletPipe::TEvServerDisconnected, Handle);
            HFunc(TEvColumnShard::TEvProposeTransaction, Handle);
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
            HFunc(TEvPrivate::TEvScanStats, Handle);
            HFunc(TEvPrivate::TEvReadFinished, Handle);
            HFunc(TEvPrivate::TEvPeriodicWakeup, Handle);
        default:
            if (!HandleDefaultEvents(ev, ctx)) {
                LOG_S_WARN("TColumnShard.StateWork at " << TabletID()
                           << " unhandled event type: "<< ev->GetTypeRewrite()
                           << " event: " << (ev->HasEvent() ? ev->GetBase()->ToString().data() : "serialized?"));
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
    };

    struct TCommitMeta {
        ui64 MetaShard{};
        THashSet<TWriteId> WriteIds;

        void AddWriteId(TWriteId id) {
            WriteIds.insert(id);
        }
    };

    struct TSchemaPreset {
        using TVerProto = NKikimrTxColumnShard::TSchemaPresetVersionInfo;

        ui32 Id;
        TString Name;
        TMap<TRowVersion, TVerProto> Versions;
        TRowVersion DropVersion = TRowVersion::Max();

        bool IsDropped() const {
            return DropVersion != TRowVersion::Max();
        }
    };
#if 0
    struct TTtlSettingsPreset {
        using TVerProto = NKikimrTxColumnShard::TTtlSettingsPresetVersionInfo;

        ui32 Id;
        TString Name;
        TMap<TRowVersion, TVerProto> Versions;
        TRowVersion DropVersion = TRowVersion::Max();

        bool IsDropped() const {
            return DropVersion != TRowVersion::Max();
        }
    };
#endif
    struct TTableInfo {
        using TVerProto = NKikimrTxColumnShard::TTableVersionInfo;

        ui64 PathId;
        std::map<TRowVersion, TVerProto> Versions;
        TRowVersion DropVersion = TRowVersion::Max();

        bool IsDropped() const {
            return DropVersion != TRowVersion::Max();
        }
    };

    struct TLongTxWriteInfo {
        ui64 WriteId;
        NLongTxService::TLongTxId LongTxId;
        ui64 PreparedTxId = 0;
    };

    ui64 CurrentSchemeShardId = 0;
    TMessageSeqNo LastSchemaSeqNo;
    std::optional<NKikimrSubDomains::TProcessingParams> ProcessingParams;
    TWriteId LastWriteId = TWriteId{0};
    ui64 LastPlannedStep = 0;
    ui64 LastPlannedTxId = 0;
    ui64 LastCompactedGranule = 0;

    TIntrusivePtr<TMediatorTimecastEntry> MediatorTimeCastEntry;
    bool MediatorTimeCastRegistered = false;
    TSet<ui64> MediatorTimeCastWaitingSteps;
    TDuration MaxReadStaleness = TDuration::Minutes(5); // TODO: Make configurable?
    TDuration MaxCommitTxDelay = TDuration::Seconds(30); // TODO: Make configurable?
    TDuration ActivationPeriod = TDuration::Seconds(60);
    TDuration FailActivationDelay = TDuration::Seconds(1);
    TInstant LastBackActivation;

    TActorId IndexingActor;     // It's logically bounded to 1: we move each portion of data to multiple indices.
    TActorId CompactionActor;   // It's memory bounded to 1: we have no memory for parallel compation.
    TActorId EvictionActor;
    std::unique_ptr<TTabletCountersBase> TabletCountersPtr;
    TTabletCountersBase* TabletCounters;
    std::unique_ptr<NTabletPipe::IClientCache> PipeClientCache;
    std::unique_ptr<NOlap::TInsertTable> InsertTable;
    std::unique_ptr<NOlap::IColumnEngine> PrimaryIndex;
    TTtl Ttl;

    THashMap<ui64, TBasicTxInfo> BasicTxInfo;
    TSet<TDeadlineQueueItem> DeadlineQueue;
    TSet<TPlanQueueItem> PlanQueue;
    bool ProgressTxInFlight = false;
    THashMap<ui64, TInstant> ScanTxInFlight;

    THashMap<ui64, TAlterMeta> AltersInFlight;
    THashMap<ui64, TCommitMeta> CommitsInFlight; // key is TxId from propose
    THashMap<ui32, TSchemaPreset> SchemaPresets;
    //THashMap<ui32, TTtlSettingsPreset> TtlSettingsPresets;
    THashMap<ui64, TTableInfo> Tables;
    THashMap<TWriteId, TLongTxWriteInfo> LongTxWrites;
    THashMap<TULID, TLongTxWriteInfo*> LongTxWritesByUniqueId;
    TMultiMap<TRowVersion, TEvColumnShard::TEvRead::TPtr> WaitingReads;
    TMultiMap<TRowVersion, TEvColumnShard::TEvScan::TPtr> WaitingScans;
    THashSet<ui64> PathsToDrop;
    bool ActiveIndexing = false;
    bool ActiveCompaction = false;
    bool ActiveCleanup = false;
    bool ActiveTtl = false;
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

    TWriteId GetLongTxWrite(NIceDb::TNiceDb& db, const NLongTxService::TLongTxId& longTxId);
    void AddLongTxWrite(TWriteId writeId, ui64 txId);
    void LoadLongTxWrite(TWriteId writeId, const NLongTxService::TLongTxId& longTxId);
    void RemoveLongTxWrite(NIceDb::TNiceDb& db, TWriteId writeId, ui64 txId = 0);
    bool RemoveTx(NTable::TDatabase& database, ui64 txId);

    void EnqueueProgressTx();
    void EnqueueBackgroundActivities(bool periodic = false, bool insertOnly = false);

    void UpdateSchemaSeqNo(const TMessageSeqNo& seqNo, NTabletFlatExecutor::TTransactionContext& txc);
    void ProtectSchemaSeqNo(const NKikimrTxColumnShard::TSchemaSeqNo& seqNoProto, NTabletFlatExecutor::TTransactionContext& txc);

    bool IsTableWritable(ui64 tableId) const;

    ui32 EnsureSchemaPreset(NIceDb::TNiceDb& db, const NKikimrSchemeOp::TColumnTableSchemaPreset& presetProto, const TRowVersion& version);
    //ui32 EnsureTtlSettingsPreset(NIceDb::TNiceDb& db, const NKikimrSchemeOp::TColumnTableTtlSettingsPreset& presetProto, const TRowVersion& version);

    void RunSchemaTx(const NKikimrTxColumnShard::TSchemaTxBody& body, const TRowVersion& version, NTabletFlatExecutor::TTransactionContext& txc);
    void RunEnsureTable(const NKikimrTxColumnShard::TCreateTable& body, const TRowVersion& version, NTabletFlatExecutor::TTransactionContext& txc);
    void RunAlterTable(const NKikimrTxColumnShard::TAlterTable& body, const TRowVersion& version, NTabletFlatExecutor::TTransactionContext& txc);
    void RunDropTable(const NKikimrTxColumnShard::TDropTable& body, const TRowVersion& version, NTabletFlatExecutor::TTransactionContext& txc);
    void RunAlterStore(const NKikimrTxColumnShard::TAlterStore& body, const TRowVersion& version, NTabletFlatExecutor::TTransactionContext& txc);
    void SetPrimaryIndex(TMap<NOlap::TSnapshot, NOlap::TIndexInfo>&& schemaVersions);

    NOlap::TIndexInfo ConvertSchema(const NKikimrSchemeOp::TColumnTableSchema& schema);

    std::unique_ptr<TEvPrivate::TEvIndexing> SetupIndexation();
    std::unique_ptr<TEvPrivate::TEvCompaction> SetupCompaction();
    std::unique_ptr<TEvPrivate::TEvEviction> SetupTtl(const THashMap<ui64, NOlap::TTiersInfo>& pathTtls = {},
                                                      bool force = false);
    std::unique_ptr<TEvPrivate::TEvWriteIndex> SetupCleanup();

    void UpdateBlobMangerCounters();
    void UpdateInsertTableCounters(bool updateCommitted = true);
    void UpdateIndexCounters();
    void UpdateResourceMetrics(const TUsage& usage);

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_COLUMNSHARD_ACTOR;
    }

    TColumnShard(TTabletStorageInfo* info, const TActorId& tablet);
};

}

#pragma once
#include "defs.h"
#include "tablet_flat_executor.h"
#include "flat_database.h"
#include "flat_dbase_change.h"
#include "flat_sausagecache.h"
#include "flat_part_store.h"
#include "flat_part_outset.h"
#include "flat_part_loader.h"
#include "flat_load_blob_queue.h"
#include "flat_comp.h"
#include "flat_scan_events.h"
#include "flat_scan_eggs.h"
#include "flat_exec_commit.h"
#include "flat_executor_misc.h"
#include "flat_executor_compaction_logic.h"
#include "flat_executor_gclogic.h"
#include "flat_executor_vacuum_logic.h"
#include "flat_executor_backup.h"
#include "flat_bio_events.h"
#include "flat_bio_stats.h"
#include "flat_fwd_sieve.h"
#include "flat_sausage_grind.h"
#include "shared_cache_events.h"
#include "util_fmt_logger.h"

#include <ydb/core/control/lib/immediate_control_board_wrapper.h>
#include <ydb/core/tablet/tablet_counters.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/core/tablet/tablet_metrics.h>
#include <ydb/core/util/queue_oneone_inplace.h>
#include <ydb/library/actors/wilson/wilson_span.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/system/hp_timer.h>
#include <util/thread/singleton.h>

#include <map>
#include <optional>
#include <variant>

namespace NKikimr {

namespace NTable {
    class TLoader;
}

namespace NTabletFlatExecutor {

class TLogicSnap;
class TExecutorBootLogic;
class TLogicRedo;
class TLogicAlter;
class TLogicSnap;
class TExecutorBorrowLogic;
class TExecutorCounters;
class TCommitManager;
class TScans;
class TMemory;
struct TIdEmitter;
struct TPageCollectionReadEnv;
struct TPageCollectionTxEnv;
struct TProdCompact;
struct TProdBackup;
struct TSeat;

struct TPendingPartSwitch {
    struct TLargeGlobLoader {
        size_t Index;
        NPageCollection::TLargeGlobId LargeGlobId;
        NPageCollection::TLargeGlobIdRestoreState State;

        explicit TLargeGlobLoader(size_t idx, const NPageCollection::TLargeGlobId& largeGlobId)
            : Index(idx)
            , LargeGlobId(largeGlobId)
            , State(LargeGlobId)
        { }

        bool Accept(const TLogoBlobID& id, TString body) {
            return State.Apply(id, body);
        }

        TSharedData Finish() {
            return State.ExtractSharedData();
        }
    };

    using TLargeGlobLoaders = TList<TLargeGlobLoader>;

    struct TMetaStage {
        NTable::TPartComponents PartComponents;
        TLargeGlobLoaders Loaders;

        explicit TMetaStage(NTable::TPartComponents&& pc)
            : PartComponents(std::move(pc))
        {
            for (size_t idx = 0; idx < PartComponents.PageCollectionComponents.size(); ++idx) {
                if (!PartComponents.PageCollectionComponents[idx].PageCollection) {
                    Loaders.emplace_back(idx, PartComponents.PageCollectionComponents[idx].LargeGlobId);
                }
            }
        }

        bool Finished() const {
            return !Loaders;
        }

        bool Accept(TLargeGlobLoaders::iterator it, const TLogoBlobID& id, TString body) {
            if (it->Accept(id, std::move(body))) {
                PartComponents.PageCollectionComponents[it->Index].ParsePageCollection(it->Finish());
                Loaders.erase(it);
                return !Loaders;
            }

            return false;
        }
    };

    struct TLoaderStage {
        NTable::TLoader Loader;
        const NPageCollection::IPageCollection* Fetching = nullptr;

        explicit TLoaderStage(NTable::TPartComponents&& pc)
            : Loader(std::move(pc))
        { }
    };

    struct TResultStage {
        NTable::TPartView PartView;

        explicit TResultStage(NTable::TPartView&& partView)
            : PartView(std::move(partView))
        { }
    };

    struct TNewBundle {
        std::variant<TMetaStage, TLoaderStage, TResultStage> Stage;

        explicit TNewBundle(NTable::TPartComponents pc)
            : Stage(std::in_place_type<TMetaStage>, std::move(pc))
        { }

        template<class T>
        inline T* GetStage() {
            return std::get_if<T>(&Stage);
        }
    };

    struct TTxStatusLoadStage {
        TIntrusiveConstPtr<NTable::TTxStatusPart> TxStatus;
        std::optional<TLargeGlobLoader> Loader;
        NTable::TEpoch Epoch;

        explicit TTxStatusLoadStage(const NPageCollection::TLargeGlobId& dataId, NTable::TEpoch epoch, const TString &data)
            : Epoch(epoch)
        {
            if (!data.empty()) {
                TxStatus = new NTable::TTxStatusPartStore(dataId, Epoch, TSharedData::Copy(data));
            } else {
                Loader.emplace(0, dataId);
            }
        }

        bool Finished() const {
            return !Loader;
        }

        bool Accept(const TLogoBlobID& id, TString body) {
            if (Loader->Accept(id, std::move(body))) {
                auto data = Loader->Finish();
                TxStatus = new NTable::TTxStatusPartStore(Loader->LargeGlobId, Epoch, std::move(data));
                Loader.reset();
                return !Loader;
            }

            return false;
        }
    };

    struct TTxStatusResultStage {
        TIntrusiveConstPtr<NTable::TTxStatusPart> TxStatus;

        explicit TTxStatusResultStage(TIntrusiveConstPtr<NTable::TTxStatusPart> txStatus)
            : TxStatus(std::move(txStatus))
        { }
    };

    struct TNewTxStatus {
        std::variant<TTxStatusLoadStage, TTxStatusResultStage> Stage;

        explicit TNewTxStatus(const NPageCollection::TLargeGlobId& dataId, NTable::TEpoch epoch, const TString& data)
            : Stage(std::in_place_type<TTxStatusLoadStage>, dataId, epoch, data)
        { }

        template<class T>
        inline T* GetStage() {
            return std::get_if<T>(&Stage);
        }
    };

    struct TChangedBundle {
        TLogoBlobID Label;
        TString Legacy;
        TString Opaque;
    };

    struct TBundleDelta {
        TLogoBlobID Label;
        TString Delta;
    };

    struct TBundleMove {
        TLogoBlobID Label;
        NTable::TEpoch RebasedEpoch = NTable::TEpoch::Max();
        ui32 SourceTable = Max<ui32>();
    };

    struct TNewBundleWaiter {
        TNewBundle* Bundle;
        TLargeGlobLoaders::iterator Loader;

        explicit TNewBundleWaiter(TNewBundle* bundle, TLargeGlobLoaders::iterator loader)
            : Bundle(bundle)
            , Loader(loader)
        { }
    };

    struct TNewTxStatusWaiter {
        TNewTxStatus* TxStatus;

        explicit TNewTxStatusWaiter(TNewTxStatus* txStatus)
            : TxStatus(txStatus)
        { }
    };

    struct TBlobWaiter : public std::variant<TNewBundleWaiter, TNewTxStatusWaiter> {
        using TBase = std::variant<TNewBundleWaiter, TNewTxStatusWaiter>;

        explicit TBlobWaiter(TNewBundle* bundle, TLargeGlobLoaders::iterator loader)
            : TBase(std::in_place_type<TNewBundleWaiter>, bundle, loader)
        { }

        explicit TBlobWaiter(TNewTxStatus* txStatus)
            : TBase(std::in_place_type<TNewTxStatusWaiter>, txStatus)
        { }

        template<class T>
        inline T* GetWaiter() {
            return std::get_if<T>(static_cast<TBase*>(this));
        }
    };

    using TPendingBlobs = THashMultiMap<TLogoBlobID, TBlobWaiter>;

    ui32 TableId = 0;
    ui32 Step = 0;

    TList<TNewBundle> NewBundles;
    TList<TNewTxStatus> NewTxStatus;
    TVector<TIntrusiveConstPtr<NTable::TColdPart>> NewColdParts;
    TPendingBlobs PendingBlobs;
    size_t PendingLoads = 0;

    TVector<TChangedBundle> Changed;
    TVector<TBundleDelta> Deltas;
    TVector<TLogoBlobID> Leaving;
    TVector<TLogoBlobID> LeavingTxStatus;
    TVector<TBundleMove> Moves;
    NTable::TEpoch Head = NTable::TEpoch::Zero();

    ui32 FollowerUpdateStep = 0;

    bool AddPendingBlob(const TLogoBlobID& id, TBlobWaiter waiter) {
        TPendingBlobs::insert_ctx ctx;
        bool newBlob = PendingBlobs.find(id, ctx) == PendingBlobs.end();
        PendingBlobs.emplace_direct(ctx, id, std::move(waiter));
        return newBlob;
    }
};

struct TExecutorStatsImpl : public TExecutorStats {
    TInstant YellowLastChecked;
    ui64 PacksMetaBytes = 0;    /* Memory occupied by NPageCollection::TMeta */
};

struct TTransactionWaitPad : public NPageCollection::TPagesWaitPad {
    TSeat* Seat;
    NWilson::TSpan WaitingSpan;

    TTransactionWaitPad(TSeat* seat);
    ~TTransactionWaitPad();

    NWilson::TTraceId GetWaitingTraceId() const;
};

struct TCompactionChangesCtx;

struct TExecutorCaches {
    THashMap<TLogoBlobID, TIntrusivePtr<TPrivatePageCache::TPageCollection>> PageCollections;
    THashMap<TLogoBlobID, TSharedData> TxStatusCaches;
};

class TExecutor
    : public TActor<TExecutor>
    , public NFlatExecutorSetup::IExecutor
    , public IActorExceptionHandler
    , private NTable::ICompactionBackend
    , private ILoadBlob
{
    using ELnLev = NUtil::ELnLev;

    friend class TExecutorCompactionLogic;
    class TTxExecutorDbMon;

    static constexpr ui64 PostponeTransactionMemThreshold = 250*1024*1024;

    struct TEvPrivate {
        enum EEv {
            EvActivateExecution = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
            EvUpdateCounters,
            EvCheckYellow,
            EvUpdateCompactions,
            EvActivateCompactionChanges,
            EvBrokenTransaction,
            EvLeaseExtend,
            EvActivateLowExecution,
            EvRetryGcRequest,

            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE), "enum range overrun");

        struct TEvActivateExecution : public TEventLocal<TEvActivateExecution, EvActivateExecution> {};
        struct TEvActivateLowExecution : public TEventLocal<TEvActivateLowExecution, EvActivateLowExecution> {};
        struct TEvUpdateCounters : public TEventLocal<TEvUpdateCounters, EvUpdateCounters> {};
        struct TEvCheckYellow : public TEventLocal<TEvCheckYellow, EvCheckYellow> {};
        struct TEvUpdateCompactions : public TEventLocal<TEvUpdateCompactions, EvUpdateCompactions> {};
        struct TEvActivateCompactionChanges : public TEventLocal<TEvActivateCompactionChanges, EvActivateCompactionChanges> {};
        struct TEvBrokenTransaction : public TEventLocal<TEvBrokenTransaction, EvBrokenTransaction> {};
        struct TEvLeaseExtend : public TEventLocal<TEvLeaseExtend, EvLeaseExtend> {};

        struct TEvRetryGcRequest : public TEventLocal<TEvRetryGcRequest, EvRetryGcRequest> {
            const ui32 Channel;

            explicit TEvRetryGcRequest(ui32 channel)
                : Channel(channel)
            {}
        };

    };

    enum class ETxMode {
        Execute,
        Enqueue,
        LowPriority,
    };

    enum class EBrokenReason {
        Storage,
        Exception,
        Transaction,
    };

    static const TString& BrokenAlertName(EBrokenReason);

    const TIntrusivePtr<ITimeProvider> Time = nullptr;
    NFlatExecutorSetup::ITablet * Owner;
    const TActorId OwnerActorId;
    TAutoPtr<NUtil::ILogger> Logger;

    ui32 FollowerId = 0;
    THashSet<ui32> PreloadTablesData;

    // This becomes true when executor enables the use of leases, e.g. starts persisting them
    // This may become false again when leases are not actively used for some time
    bool LeaseEnabled = false;
    // As soon as lease is persisted we may theoretically use read-only checks for lease prolongation
    bool LeasePersisted = false;
    // When lease is dropped we must stop accepting new lease-dependent requests
    bool LeaseDropped = false;
    // When lease is used in any given cycle this becomes true
    bool LeaseUsed = false;
    // This flag marks when TEvLeaseExtend message is already pending
    bool LeaseExtendPending = false;
    // This flag is enabled when LeaseDuration is changed and needs to be persisted again
    bool LeaseDurationUpdated = false;
    TDuration LeaseDuration;
    TMonotonic LeaseEnd;
    // Counts the number of times an unused lease has been extended
    size_t UnusedLeaseExtensions = 0;
    // Counts the number of times LeaseDuration was increased
    size_t LeaseDurationIncreases = 0;

    struct TLeaseCommit : public TIntrusiveListItem<TLeaseCommit> {
        using TByEndMap = std::multimap<TMonotonic, TLeaseCommit*>;

        // Note: for lightweight read-only confirmations Step == 0
        const ui32 Step;
        const TMonotonic Start;
        TMonotonic LeaseEnd;
        TVector<std::function<void()>> Callbacks;
        TByEndMap::iterator ByEndIterator;
        const ui64 Cookie;

        TLeaseCommit(ui32 step, TMonotonic start, TMonotonic leaseEnd, ui64 cookie)
            : Step(step)
            , Start(start)
            , LeaseEnd(leaseEnd)
            , Cookie(cookie)
        { }
    };

    TList<TLeaseCommit> LeaseCommits;
    TLeaseCommit::TByEndMap LeaseCommitsByEnd;
    TIntrusiveList<TLeaseCommit> LeaseCommitsByStep;
    ui64 LeaseCommitsCounter = 0;

    // TSeat's UniqID to an owned pointer
    absl::flat_hash_map<ui64, std::unique_ptr<TSeat>> Transactions;

    using TSeatList = TIntrusiveList<TSeat>;
    TSeatList ActivationQueue;
    TSeatList ActivationLowQueue;
    TSeatList PendingQueue;

    bool CompactionChangesActivating = false;

    TSeatList PostponedTransactions;
    THashMap<ui64, THolder<TScanSnapshot>> ScanSnapshots;
    ui64 ScanSnapshotId = 1;

    class TActiveTransactionZone;

    bool ActiveTransaction = false;
    bool BrokenTransaction = false;
    ui32 ActivateTransactionWaiting = 0;
    ui32 ActivateTransactionInFlight = 0;
    ui32 ActivateLowTransactionWaiting = 0;
    ui32 ActivateLowTransactionInFlight = 0;

    using TWaitingSnaps = THashMap<TTableSnapshotContext *, TIntrusivePtr<TTableSnapshotContext>>;

    const TIntrusivePtr<TIdEmitter> Emitter;
    TAutoPtr<TBroker> Broker;

    TWaitingSnaps WaitingSnapshots;

    ui64 BootAttempt = 0;
    THolder<TExecutorBootLogic> BootLogic;
    THolder<TPrivatePageCache> PrivatePageCache;
    THolder<TExecutorCounters> Counters;
    THolder<TTabletCountersBase> AppCounters;
    THolder<TTabletCountersBase> CountersBaseline;
    THolder<TTabletCountersBase> AppCountersBaseline;
    THolder<NMetrics::TResourceMetrics> ResourceMetrics;

    TAutoPtr<NTable::TDatabase> Database;

    TAutoPtr<TCommitManager> CommitManager;
    TAutoPtr<TScans> Scans;
    TAutoPtr<TMemory> Memory;
    TAutoPtr<NTable::IMemTableMemoryConsumersCollection> MemTableMemoryConsumersCollection;
    TAutoPtr<TLogicSnap> LogicSnap;
    TAutoPtr<TLogicRedo> LogicRedo;
    TAutoPtr<TLogicAlter> LogicAlter;
    THolder<TExecutorGCLogic> GcLogic;
    THolder<TCompactionLogic> CompactionLogic;
    THolder<TExecutorBorrowLogic> BorrowLogic;
    THolder<TVacuumLogic> VacuumLogic;

    TLoadBlobQueue PendingBlobQueue;

    // Used control number of in flight events to the counter aggregator
    TIntrusivePtr<TEvTabletCounters::TInFlightCookie> CounterEventsInFlight;

    TTabletCountersWithTxTypes* AppTxCounters = nullptr;

    TActorId Launcher;

    THashMap<NPageCollection::TPagesWaitPad*, TIntrusivePtr<TTransactionWaitPad>> TransactionWaitPads;

    ui64 TransactionUniqCounter = 0;

    bool LogBatchFlushScheduled = false;
    bool NeedLogSnapshot = false;

    mutable bool HadRejectProbabilityByTxInFly = false;
    mutable bool HadRejectProbabilityByOverload = false;

    THashMap<ui32, TIntrusivePtr<TBarrier>> InFlyCompactionGcBarriers;
    TDeque<THolder<TEvTablet::TFUpdateBody>> PostponedFollowerUpdates;
    THashMap<ui32, TVector<TIntrusivePtr<TBarrier>>> InFlySnapCollectionBarriers;

    THolder<TExecutorStatsImpl> Stats;
    bool HasYellowCheckInFly = false;

    TDeque<TPendingPartSwitch> PendingPartSwitches;
    size_t ReadyPartSwitches = 0;

    ui64 UsedTabletMemory = 0;
    ui64 TransactionPagesMemory = 0;

    TActorContext SelfCtx() const;
    TActorContext OwnerCtx() const;

    TControlWrapper LogFlushDelayOverrideUsec;
    TControlWrapper MaxCommitRedoMB;
    TControlWrapper MaxTxInFly;

    ui64 Stamp() const noexcept;
    void Registered(TActorSystem*, const TActorId&) override;
    void PassAway() override;
    void Broken(EBrokenReason reason);
    void Active(const TActorContext &ctx);
    void ActivateFollower(const TActorContext &ctx);
    void RecreatePrivateCache();
    void ReflectSchemeSettings();
    void OnYellowChannels(TVector<ui32> yellowMoveChannels, TVector<ui32> yellowStopChannels) override;
    void CheckYellow(TVector<ui32> &&yellowMoveChannels, TVector<ui32> &&yellowStopChannels, bool terminal = false);
    void SendReassignYellowChannels(const TVector<ui32> &yellowChannels);
    void CheckCollectionBarrier(TIntrusivePtr<TBarrier> &barrier);
    void UtilizeSubset(const NTable::TSubset&, const NTable::NFwd::TSeen&,
        THashSet<TLogoBlobID> reusedBundles, TLogCommit *commit);
    void UtilizeSubset(const NTable::TSubset&, TLogCommit *commit);
    bool PrepareExternalPart(TPendingPartSwitch &partSwitch, NTable::TPartComponents &&pc);
    bool PrepareExternalPart(TPendingPartSwitch &partSwitch, TPendingPartSwitch::TNewBundle &bundle);
    bool PrepareExternalTxStatus(TPendingPartSwitch &partSwitch, const NPageCollection::TLargeGlobId &dataId, NTable::TEpoch epoch, const TString &data);
    bool PrepareExternalTxStatus(TPendingPartSwitch &partSwitch, TPendingPartSwitch::TNewTxStatus &txStatus);
    void OnBlobLoaded(const TLogoBlobID& id, TString body, uintptr_t cookie) override;
    void AdvancePendingPartSwitches();
    bool ApplyReadyPartSwitches();

    TExecutorCaches CleanupState();
    bool CanExecuteTransaction() const;

    void TranscriptBootOpResult(ui32 res, const TActorContext &ctx);
    void TranscriptFollowerBootOpResult(ui32 res, const TActorContext &ctx);
    std::unique_ptr<TSeat> RemoveTransaction(ui64 id);
    void FinishCancellation(TSeat* seat, bool activateMore = true);
    void ExecuteTransaction(TSeat* seat);
    void CommitTransactionLog(std::unique_ptr<TSeat>, TPageCollectionTxEnv&, TAutoPtr<NTable::TChange>,
                              THPTimer &bookkeepingTimer);
    void UnpinTransactionPages(TSeat &seat);
    void ReleaseTxData(TSeat &seat, ui64 requested);
    void PostponeTransaction(TSeat*, TPageCollectionTxEnv&, TAutoPtr<NTable::TChange>, THPTimer &bookkeepingTimer);
    void EnqueueActivation(TSeat* seat, bool activate);
    void PlanTransactionActivation();
    void MakeLogSnapshot();
    void TryActivateWaitingTransaction(TIntrusivePtr<NPageCollection::TPagesWaitPad>&& waitPad, TVector<NSharedCache::TEvResult::TLoaded>&& pages, TPrivatePageCache::TPageCollection* collectionInfo);
    void ActivateWaitingTransaction(TTransactionWaitPad& transaction);
    void LogWaitingTransaction(const TTransactionWaitPad& transaction);
    void AddPartStorePageCollections(const NTable::TPartView &partView, const THashMap<NTable::TTag, ECacheMode>& cacheModes);
    void AddPageCollection(const TIntrusivePtr<TPrivatePageCache::TPageCollection> &pageCollection);
    void DropPartStorePageCollections(const NTable::TPart &part);
    void DropPageCollection(const TLogoBlobID& pageCollectionId);
    void StartNewBackup();

    void UpdateCacheModesForPartStore(NTable::TPartView& partView, const THashMap<NTable::TTag, ECacheMode>& cacheModes);
    void UpdateCachePagesForDatabase(bool pendingOnly = false);
    void RequestStickyPagesForPartStore(NTable::TPartView& partView, const THashSet<NTable::TTag>& stickyColumns);
    THashSet<NTable::TTag> GetStickyColumns(ui32 tableId);
    THashMap<NTable::TTag, ECacheMode> GetCacheModes(ui32 tableId);
    ECacheMode GetCacheMode(const TVector<NTable::TPartScheme::TColumn>& columns, const THashMap<NTable::TTag, ECacheMode>& cacheModes);
    THolder<TScanSnapshot> PrepareScanSnapshot(ui32 table,
        const NTable::TCompactionParams* params, TRowVersion snapshot = TRowVersion::Max());
    void ReleaseScanLocks(TIntrusivePtr<TBarrier>, const NTable::TSubset&);
    void StartScan(ui64 serial, ui32 table);
    void StartScan(ui64 task, TResource*);
    void StartSeat(ui64 task, TResource*);
    void PostponedScanCleared(NResourceBroker::TEvResourceBroker::TEvResourceAllocated *msg, const TActorContext &ctx);

    void ApplyFollowerUpdate(THolder<TEvTablet::TFUpdateBody> update);
    void ApplyFollowerAuxUpdate(const TString &auxBody);
    void ApplyFollowerPostponedUpdates();
    void AddFollowerPartSwitch(const NKikimrExecutorFlat::TTablePartSwitch &switchProto,
        const NKikimrExecutorFlat::TFollowerPartSwitchAux::TBySwitch *aux, ui32 updateStep, ui32 step);
    void ApplyExternalPartSwitch(TPendingPartSwitch &partSwitch);

    void Wakeup(TEvents::TEvWakeup::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTablet::TEvDropLease::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvLeaseExtend::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTablet::TEvConfirmLeaderResult::TPtr &ev);
    void Handle(TEvTablet::TEvCommitResult::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTablet::TEvSnapshotConfirmed::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvActivateExecution::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvActivateLowExecution::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvBrokenTransaction::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvents::TEvFlushLog::TPtr &ev);
    void Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr&);
    void Handle(TEvPrivate::TEvRetryGcRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(NSharedCache::TEvResult::TPtr &ev);
    void Handle(NSharedCache::TEvUpdated::TPtr &ev);
    void Handle(NResourceBroker::TEvResourceBroker::TEvResourceAllocated::TPtr&);
    void Handle(NOps::TEvScanStat::TPtr &ev, const TActorContext &ctx);
    void Handle(NOps::TEvResult::TPtr &ev);
    void ProcessIoStats(
            NBlockIO::EDir dir, NBlockIO::EPriority priority,
            ui64 bytes, ui64 ops,
            NBlockIO::TEvStat::TByCnGr&& groupBytes,
            NBlockIO::TEvStat::TByCnGr&& groupOps,
            const TActorContext& ctx);
    void ProcessIoStats(
            NBlockIO::EDir dir, NBlockIO::EPriority priority,
            NBlockIO::TEvStat::TByCnGr&& groupBytes,
            NBlockIO::TEvStat::TByCnGr&& groupOps,
            const TActorContext& ctx);
    void Handle(NBlockIO::TEvStat::TPtr &ev, const TActorContext &ctx);
    void Handle(NOps::TEvResult *ops, TProdCompact *msg, bool cancelled);
    void Handle(TEvBlobStorage::TEvGetResult::TPtr&, const TActorContext&);
    void Handle(TEvTablet::TEvGcForStepAckResponse::TPtr &ev);
    void Handle(NBackup::TEvSnapshotCompleted::TPtr &ev);
    void Handle(NBackup::TEvChangelogFailed::TPtr &ev);

    void UpdateUsedTabletMemory();
    void UpdateCounters(const TActorContext &ctx);
    void ForceSendCounters();
    void UpdateYellow();
    void UpdateCompactions();
    void Handle(TEvTablet::TEvCheckBlobstorageStatusResult::TPtr &ev);

    void ReadResourceProfile();
    TString CheckBorrowConsistency();

    // ICompactionBackend API

    ui64 OwnerTabletId() const override;
    const NTable::TScheme& DatabaseScheme() override;
    TIntrusiveConstPtr<NTable::TRowScheme> RowScheme(ui32 table) const override;
    const NTable::TScheme::TTableInfo* TableScheme(ui32 table) override;
    ui64 TableMemSize(ui32 table, NTable::TEpoch epoch) override;
    NTable::TPartView TablePart(ui32 table, const TLogoBlobID& label) override;
    TVector<NTable::TPartView> TableParts(ui32 table) override;
    TVector<TIntrusiveConstPtr<NTable::TColdPart>> TableColdParts(ui32 table) override;
    const NTable::TRowVersionRanges& TableRemovedRowVersions(ui32 table) override;
    ui64 BeginCompaction(THolder<NTable::TCompactionParams> params) override;
    bool CancelCompaction(ui64 compactionId) override;
    void RequestChanges(ui32 table) override;

    void PlanCompactionChangesActivation();
    void Handle(TEvPrivate::TEvActivateCompactionChanges::TPtr& ev, const TActorContext& ctx);
    void CommitCompactionChanges(
            ui32 tableId,
            const NTable::TCompactionChanges& changes,
            NKikimrCompaction::ECompactionStrategy strategy);
    void ApplyCompactionChanges(
            TCompactionChangesCtx& ctx,
            const NTable::TCompactionChanges& changes,
            NKikimrCompaction::ECompactionStrategy strategy);

    void RenderHtmlCounters(TStringStream& str) const;
    void RenderJsonCounters(TStringStream& str) const;

    float CalcRejectProbability() const;

public:
    void Describe(IOutputStream &out) const override
    {
        out
            << (Stats->IsFollower() ? "Follower" : "Leader")
            << "{" << Owner->TabletID()
            << ":" << Generation() << ":" << Step() << "}";
    }

    // IExecutor interface
    void Boot(TEvTablet::TEvBoot::TPtr &ev, const TActorContext &ctx) override;
    void Restored(TEvTablet::TEvRestored::TPtr &ev, const TActorContext &ctx) override;
    void DetachTablet() override;
    ui64 DoExecute(TAutoPtr<ITransaction> transaction, ETxMode mode);
    void Execute(TAutoPtr<ITransaction> transaction, const TActorContext &ctx) override;
    ui64 Enqueue(TAutoPtr<ITransaction> transaction) override;
    ui64 EnqueueLowPriority(TAutoPtr<ITransaction> transaction) override;
    bool CancelTransaction(ui64 id) override;

    void LeaseConfirmed(ui64 confirmedCookie);
    TLeaseCommit* AddLeaseConfirm();
    TLeaseCommit* AttachLeaseCommit(TLogCommit* commit, bool force = false);
    TLeaseCommit* EnsureReadOnlyLease(TMonotonic at);
    void ConfirmReadOnlyLease(TMonotonic at) override;
    void ConfirmReadOnlyLease(TMonotonic at, std::function<void()> callback) override;
    void ConfirmReadOnlyLease(std::function<void()> callback) override;

    TString BorrowSnapshot(ui32 tableId, const TTableSnapshotContext& snap, TRawVals from, TRawVals to, ui64 loaner) const override;

    ui64 MakeScanSnapshot(ui32 table) override;
    void DropScanSnapshot(ui64 snapId) override;
    ui64 QueueScan(ui32 tableId, TAutoPtr<NTable::IScan> scan, ui64 cookie, const TScanOptions& options) override;
    bool CancelScan(ui32 tableId, ui64 taskId) override;

    TFinishedCompactionInfo GetFinishedCompactionInfo(ui32 tableId) const override;
    bool HasSchemaChanges(ui32 table) const override;
    bool HasSchemaChanges(const NTable::TPartView& partView, const NTable::TScheme::TTableInfo& tableInfo, const NTable::TRowScheme& rowScheme) const;
    ui64 CompactBorrowed(ui32 tableId) override;
    ui64 CompactMemTable(ui32 tableId) override;
    ui64 CompactTable(ui32 tableId) override;
    bool CompactTables() override;

    void StartVacuum(ui64 vacuumGeneration) override;

    void Handle(NMemory::TEvMemTableRegistered::TPtr &ev);
    void Handle(NMemory::TEvMemTableCompact::TPtr &ev);

    void AllowBorrowedGarbageCompaction(ui32 tableId) override;

    void FollowerAttached(ui32 totalFollowers) override;
    void FollowerDetached(ui32 totalFollowers) override;
    void FollowerSyncComplete() override;
    void FollowerGcApplied(ui32 step, TDuration followerSyncDelay) override;
    void FollowerBoot(TEvTablet::TEvFBoot::TPtr &ev, const TActorContext &ctx) override;
    void FollowerUpdate(THolder<TEvTablet::TFUpdateBody> update) override;
    void FollowerAuxUpdate(TString upd) override;

    void RenderHtmlPage(NMon::TEvRemoteHttpInfo::TPtr &ev) const override;
    void RenderHtmlCounters(NMon::TEvRemoteHttpInfo::TPtr &ev) const override;
    void RenderHtmlDb(NMon::TEvRemoteHttpInfo::TPtr &ev, const TActorContext &ctx) const override;
    void GetTabletCounters(TEvTablet::TEvGetCounters::TPtr &ev) override;

    void UpdateConfig(TEvTablet::TEvUpdateConfig::TPtr &ev) override;

    void SendUserAuxUpdateToFollowers(TString upd, const TActorContext &ctx) override;

    THashMap<TLogoBlobID, TVector<ui64>> GetBorrowedParts() const override;
    bool HasLoanedParts() const override;

    bool HasBorrowed(ui32 table, ui64 selfTabletId) const override;

    const TExecutorStats& GetStats() const override;
    NMetrics::TResourceMetrics* GetResourceMetrics() const override;
    TExecutorCounters* GetCounters() override;

    void RegisterExternalTabletCounters(TAutoPtr<TTabletCountersBase> appCounters) override;

    virtual void SetPreloadTablesData(THashSet<ui32> tables) override;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::FLAT_EXECUTOR;
    }

    TExecutor(NFlatExecutorSetup::ITablet *owner, const TActorId& ownerActorId);
    ~TExecutor();

    bool OnUnhandledException(const std::exception& exc) override;

    STFUNC(StateInit);
    STFUNC(StateBoot);
    STFUNC(StateWork);
    STFUNC(StateFollowerBoot);
    STFUNC(StateFollower);

    // database interface
    const NTable::TScheme& Scheme() const override;
    ui64 TabletId() const { return Owner->TabletID(); }

    float GetRejectProbability() const override;
    void MaybeRelaxRejectProbability();

    TActorId GetLauncher() const { return Launcher; }
};

}}

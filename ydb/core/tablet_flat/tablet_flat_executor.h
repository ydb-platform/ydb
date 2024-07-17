#pragma once
#include "defs.h"

#include "flat_scan_iface.h"

#include <ydb/core/base/tablet.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <library/cpp/lwtrace/shuttle.h>
#include <util/generic/maybe.h>
#include <util/system/type_name.h>
#include <util/generic/variant.h>

////////////////////////////////////////////
namespace NKikimr {
class TTabletCountersBase;

namespace NTable {
    class TDatabase;
    class TScheme;
}

namespace NTabletFlatExecutor {

class TTransactionContext;
class TExecutor;
struct TPageCollectionTxEnv;
struct TSeat;

class TTableSnapshotContext : public TThrRefBase, TNonCopyable {
    friend class TExecutor;
    friend struct TPageCollectionTxEnv;

    class TImpl;
    THolder<TImpl> Impl;
public:
    TTableSnapshotContext();
    virtual ~TTableSnapshotContext();
    virtual TConstArrayRef<ui32> TablesToSnapshot() const = 0;

public:
    NTable::TSnapEdge Edge(ui32 table) const;
};

class TMemoryGCToken : public TThrRefBase {
public:
    TMemoryGCToken(ui64 size, ui64 taskId)
        : Size(size)
        , TaskId(taskId)
    {}
    virtual ~TMemoryGCToken() {}

    bool IsDropped() const {
        return AtomicLoad(&Dropped);
    }
    void Drop() {
        AtomicStore(&Dropped, true);
    }

    void Describe(IOutputStream &out) const noexcept
    {
        out << "Res{";

        if (TaskId) {
            out << TaskId;
        } else {
            out << "static";
        }

        out << " " << Size << "b}";
    }

    const ui64 Size;
    const ui64 TaskId;

private:
    volatile bool Dropped = false;
};

class TMemoryToken {
public:
    TMemoryToken(TIntrusivePtr<TMemoryGCToken> gcToken)
        : GCToken(gcToken)
    {}

    ~TMemoryToken() {
        GCToken->Drop();
    }

    const TIntrusivePtr<TMemoryGCToken> GCToken;
};

struct IExecuting {
    /* Functionality available only in tx execution contextt */

    virtual ~IExecuting() = default;

    virtual void MakeSnapshot(TIntrusivePtr<TTableSnapshotContext>) = 0;
    virtual void DropSnapshot(TIntrusivePtr<TTableSnapshotContext>) = 0;
    virtual void MoveSnapshot(const TTableSnapshotContext&, ui32 src, ui32 dst) = 0;
    virtual void ClearSnapshot(const TTableSnapshotContext&) = 0;
    virtual void LoanTable(ui32 tableId, const TString &partsInfo) = 0; // attach table parts to table (called on part destination)
    virtual void CleanupLoan(const TLogoBlobID &bundleId, ui64 from) = 0; // mark loan completion (called on part source)
    virtual void ConfirmLoan(const TLogoBlobID &bundleId, const TLogoBlobID &borrowId) = 0; // confirm loan update delivery (called on part destination)
    virtual void EnableReadMissingReferences() noexcept = 0;
    virtual void DisableReadMissingReferences() noexcept = 0;
    virtual ui64 MissingReferencesSize() const noexcept = 0;
};

class TTxMemoryProviderBase : TNonCopyable {
public:
    TTxMemoryProviderBase(ui64 memoryLimit, ui64 taskId)
        : MemoryLimit(memoryLimit)
        , TaskId(taskId)
        , RequestedMemory(0)
        , NotEnoughMemoryCount(0)
    {}

    ~TTxMemoryProviderBase() {}

    ui64 GetMemoryLimit() const
    {
        if (MemoryToken)
            return MemoryToken->GCToken->Size;
        return MemoryLimit;
    }

    ui64 GetTaskId() const
    {
        if (MemoryToken)
            return MemoryToken->GCToken->TaskId;
        return TaskId;
    }

    void RequestMemory(ui64 bytes)
    {
        Y_ABORT_UNLESS(!MemoryGCToken);
        RequestedMemory += bytes;
    }

    ui64 GetRequestedMemory() const { return RequestedMemory; }

    void NotEnoughMemory(ui32 add = 1)
    {
        NotEnoughMemoryCount += add;
    }

    ui32 GetNotEnoughMemoryCount() const { return NotEnoughMemoryCount; }

    /**
     * Memory token should be captured only when tx is finished
     * or within ReleaseTxData method.
     */
    TAutoPtr<TMemoryToken> HoldMemory()
    {
        return HoldMemory(MemoryLimit);
    }

    TAutoPtr<TMemoryToken> HoldMemory(ui64 size)
    {
        Y_ABORT_UNLESS(!MemoryGCToken);
        Y_ABORT_UNLESS(size <= MemoryLimit);
        Y_ABORT_UNLESS(size > 0);
        MemoryGCToken = new TMemoryGCToken(size, TaskId);
        return new TMemoryToken(MemoryGCToken);
    }

    void UseMemoryToken(TAutoPtr<TMemoryToken> token)
    {
        Y_ABORT_UNLESS(!MemoryToken);
        MemoryToken = std::move(token);
    }

    TAutoPtr<TMemoryToken> ExtractMemoryToken()
    {
        return std::move(MemoryToken);
    }

    TIntrusivePtr<TMemoryGCToken> GetMemoryGCToken() const { return MemoryGCToken; }

private:
    const ui64 MemoryLimit;
    const ui64 TaskId;
    ui64 RequestedMemory;
    ui32 NotEnoughMemoryCount;
    TIntrusivePtr<TMemoryGCToken> MemoryGCToken;
    TAutoPtr<TMemoryToken> MemoryToken;
};

class TTxMemoryProvider : public TTxMemoryProviderBase {
public:
    TTxMemoryProvider(ui64 memoryLimit, ui64 taskId)
        : TTxMemoryProviderBase(memoryLimit, taskId)
    {}
    ~TTxMemoryProvider() {}

private:
    using TTxMemoryProviderBase::RequestMemory;
    using TTxMemoryProviderBase::UseMemoryToken;
    using TTxMemoryProviderBase::ExtractMemoryToken;
};

class TTransactionContext : public TTxMemoryProviderBase {
    friend class TExecutor;

public:
    TTransactionContext(ui64 tablet, ui32 gen, ui32 step, NTable::TDatabase &db, IExecuting &env,
                        ui64 memoryLimit, ui64 taskId, NWilson::TSpan &transactionSpan)
        : TTxMemoryProviderBase(memoryLimit, taskId)
        , Tablet(tablet)
        , Generation(gen)
        , Step(step)
        , Env(env)
        , DB(db)
        , TransactionSpan(transactionSpan)
    {}

    ~TTransactionContext() {}

    /**
     * Request transaction to restart at some later time
     *
     * Transaction's Execute method must return false after calling this method.
     */
    void Reschedule() {
        Rescheduled_ = true;
    }

    bool IsRescheduled() const {
        return Rescheduled_;
    }

    void StartExecutionSpan() noexcept {
        TransactionExecutionSpan = NWilson::TSpan(TWilsonTablet::TabletDetailed, TransactionSpan.GetTraceId(), "Tablet.Transaction.Execute");
    }

    void FinishExecutionSpan() noexcept {
        TransactionExecutionSpan.EndOk();
    }

public:
    const ui64 Tablet = Max<ui32>();
    const ui32 Generation = Max<ui32>();
    const ui32 Step = Max<ui32>();
    IExecuting &Env;
    NTable::TDatabase &DB;
    NWilson::TSpan &TransactionSpan;
    NWilson::TSpan TransactionExecutionSpan;

private:
    bool Rescheduled_ = false;
};

struct TCompactedPartLoans {
    TLogoBlobID MetaInfoId;
    ui64 Lender;

    TCompactedPartLoans() = default;
    TCompactedPartLoans(const TLogoBlobID &metaId, ui64 lender)
        : MetaInfoId(metaId)
        , Lender(lender)
    {}
};

struct TFinishedCompactionInfo {
    ui64 Edge = 0;
    TInstant FullCompactionTs;

    TFinishedCompactionInfo() = default;

    TFinishedCompactionInfo(ui64 edge, TInstant ts)
        : Edge(edge)
        , FullCompactionTs(ts)
    {}
};

enum class ETerminationReason {
    None = 0,
    MemoryLimitExceeded = 1,
};


class ITransaction : TNonCopyable {
public:
    using TTransactionContext = NTabletFlatExecutor::TTransactionContext;

    ITransaction() = default;

    ITransaction(NLWTrace::TOrbit &&orbit)
        : Orbit(std::move(orbit))
    { }

    ITransaction(NWilson::TTraceId &&traceId)
        : TxSpan(NWilson::TSpan(TWilsonTablet::TabletBasic, std::move(traceId), "Tablet.Transaction"))
    { }

    virtual ~ITransaction() = default;
    /// @return true if execution complete and transaction is ready for commit
    virtual bool Execute(TTransactionContext &txc, const TActorContext &ctx) = 0;
    virtual void Complete(const TActorContext &ctx) = 0;
    virtual void Terminate(ETerminationReason reason, const TActorContext &/*ctx*/) {
        Y_ABORT("Unexpected transaction termination (reason %" PRIu32 ")", (ui32)reason);
    }
    virtual void ReleaseTxData(TTxMemoryProvider &/*provider*/, const TActorContext &/*ctx*/) {}
    virtual TTxType GetTxType() const { return UnknownTxType; }

    virtual void Describe(IOutputStream &out) const noexcept
    {
        out << TypeName(*this);
    }

    void SetupTxSpanName() noexcept {
        if (TxSpan) {
            TxSpan.Attribute("Type", TypeName(*this));
        }
    }

    void SetupTxSpan(NWilson::TTraceId traceId) noexcept {
        TxSpan = NWilson::TSpan(TWilsonTablet::TabletBasic, std::move(traceId), "Tablet.Transaction");
        if (TxSpan) {
            TxSpan.Attribute("Type", TypeName(*this));
        }
    }

public:
    NLWTrace::TOrbit Orbit;

    NWilson::TSpan TxSpan;
};

template<typename T>
class TTransactionBase : public ITransaction {
protected:
    typedef T TSelf;
    typedef TTransactionBase<TSelf> TBase;

    TSelf * const Self;
public:
    TTransactionBase(T *self)
        : Self(self)
    {}

    TTransactionBase(T *self, NLWTrace::TOrbit &&orbit)
        : ITransaction(std::move(orbit))
        , Self(self)
    { }

    TTransactionBase(T *self, NWilson::TTraceId &&traceId)
        : ITransaction(std::move(traceId))
        , Self(self)
    { }
};

struct TExecutorStats {
    bool IsActive = false;
    bool IsFollower = false;
    bool IsAnyChannelYellowMove = false;
    bool IsAnyChannelYellowStop = false;
    ui64 TxInFly = 0;
    ui64 TxPending = 0;
    const THashMap<TLogoBlobID, TCompactedPartLoans>* CompactedPartLoans = nullptr;
    const bool* HasSharedBlobs = nullptr;

    TVector<ui32> YellowMoveChannels;
    TVector<ui32> YellowStopChannels;

    ui32 FollowersCount = 0;

    bool IsYellowMoveChannel(ui32 channel) const {
        auto it = std::lower_bound(YellowMoveChannels.begin(), YellowMoveChannels.end(), channel);
        return it != YellowMoveChannels.end() && *it == channel;
    }

    bool IsYellowStopChannel(ui32 channel) const {
        auto it = std::lower_bound(YellowStopChannels.begin(), YellowStopChannels.end(), channel);
        return it != YellowStopChannels.end() && *it == channel;
    }

protected:
    virtual ~TExecutorStats() {}
};

struct TScanOptions {
    enum class EReadPrio {
        Default,
        Fast,
        Bulk,
        Low,
    };

    struct TReadAheadDefaults {
        // nothing
    };

    struct TReadAheadOptions {
        ui64 ReadAheadLo;
        ui64 ReadAheadHi;
    };

    struct TResourceBrokerDefaults {
        // nothing
    };

    struct TResourceBrokerOptions {
        TString Type;
        ui32 Prio;
    };

    struct TResourceBrokerDisabled {
        // nothing
    };

    struct TSnapshotNone {
        // nothing
    };

    struct TSnapshotById {
        ui64 SnapshotId;
    };

    struct TSnapshotByRowVersion {
        TRowVersion RowVersion;
    };

    struct TActorPoolDefault {
        // nothing
    };

    struct TActorPoolById {
        ui32 PoolId;
    };

    EReadPrio ReadPrio = EReadPrio::Default;
    std::variant<TReadAheadDefaults, TReadAheadOptions> ReadAhead;
    std::variant<TResourceBrokerDefaults, TResourceBrokerOptions, TResourceBrokerDisabled> ResourceBroker;
    std::variant<TSnapshotNone, TSnapshotById, TSnapshotByRowVersion> Snapshot;
    std::variant<TActorPoolDefault, TActorPoolById> ActorPool;

    TScanOptions& SetReadPrio(EReadPrio prio) {
        ReadPrio = prio;
        return *this;
    }

    TScanOptions& SetReadAhead(ui64 lo, ui64 hi) {
        ReadAhead = TReadAheadOptions{ lo, hi };
        return *this;
    }

    TScanOptions& SetResourceBroker(TString type, ui32 prio) {
        ResourceBroker = TResourceBrokerOptions{ std::move(type), prio };
        return *this;
    }

    TScanOptions& DisableResourceBroker() {
        ResourceBroker = TResourceBrokerDisabled{ };
        return *this;
    }

    TScanOptions& SetSnapshotId(ui64 snapshotId) {
        Snapshot = TSnapshotById{ snapshotId };
        return *this;
    }

    TScanOptions& SetSnapshotRowVersion(TRowVersion rowVersion) {
        Snapshot = TSnapshotByRowVersion{ rowVersion };
        return *this;
    }

    TScanOptions& SetActorPoolId(ui32 poolId) {
        ActorPool = TActorPoolById{ poolId };
        return *this;
    }

    bool IsResourceBrokerDisabled() const {
        return std::holds_alternative<TResourceBrokerDisabled>(ResourceBroker);
    }
};

namespace NFlatExecutorSetup {
    struct ITablet : TNonCopyable {
        virtual ~ITablet() {}

        virtual void ActivateExecutor(const TActorContext &ctx) = 0;
        virtual void Detach(const TActorContext &ctx) = 0;

        TTabletStorageInfo* Info() const { return TabletInfo.Get(); }
        ui64 TabletID() const { return TabletInfo->TabletID; }
        TTabletTypes::EType TabletType() const { return TabletInfo->TabletType; }
        const TActorId& Tablet() const { return TabletActorID; }
        const TActorId& ExecutorID() const { return ExecutorActorID; }
        const TActorId& LauncherID() const { return LauncherActorID; }

        virtual void SnapshotComplete(TIntrusivePtr<TTableSnapshotContext> snapContext, const TActorContext &ctx); // would be FAIL in default implementation
        virtual void CompletedLoansChanged(const TActorContext &ctx); // would be no-op in default implementation
        virtual void CompactionComplete(ui32 tableId, const TActorContext &ctx); // would be no-op in default implementation

        virtual void ScanComplete(NTable::EAbort status, TAutoPtr<IDestructable> prod, ui64 cookie, const TActorContext &ctx);

        virtual bool ReassignChannelsEnabled() const;
        virtual void OnYellowChannelsChanged();
        virtual void OnRejectProbabilityRelaxed();

        // memory usage excluding transactions and executor cache.
        virtual ui64 GetMemoryUsage() const { return 50 << 10; }

        virtual void OnLeaderUserAuxUpdate(TString) { /* default */ }

        virtual bool ReadOnlyLeaseEnabled();
        virtual TDuration ReadOnlyLeaseDuration();
        virtual void ReadOnlyLeaseDropped();

        virtual void OnFollowersCountChanged();

        virtual void OnFollowerSchemaUpdated();
        virtual void OnFollowerDataUpdated();

        // create transaction?
    protected:
        ITablet(TTabletStorageInfo *info, const TActorId &tablet)
            : TabletActorID(tablet)
            , TabletInfo(info)
        {
            Y_ABORT_UNLESS(TTabletTypes::TypeInvalid != TabletInfo->TabletType);
        }

        TActorId ExecutorActorID;
        TActorId TabletActorID;
        TActorId LauncherActorID;

        void UpdateTabletInfo(TIntrusivePtr<TTabletStorageInfo> info, const TActorId& launcherID = {});
    private:
        TIntrusivePtr<TTabletStorageInfo> TabletInfo;
    };


    ////////////////////////////////////////////
    // tablet -> executor
    struct IExecutor : TNonCopyable {
        virtual ~IExecutor() {}

        // tablet assigned as leader, could begin loading
        virtual void Boot(TEvTablet::TEvBoot::TPtr &ev, const TActorContext &ctx) = 0;
        // tablet generation restoration complete, tablet could act as leader
        virtual void Restored(TEvTablet::TEvRestored::TPtr &ev, const TActorContext &ctx) = 0;
        // die!
        virtual void DetachTablet(const TActorContext &ctx) = 0;

        // tablet assigned as follower (or follower connection refreshed), must begin loading
        virtual void FollowerBoot(TEvTablet::TEvFBoot::TPtr &ev, const TActorContext &ctx) = 0;
        // next follower incremental update
        virtual void FollowerUpdate(THolder<TEvTablet::TFUpdateBody> upd) = 0;
        virtual void FollowerAuxUpdate(TString upd) = 0;
        virtual void FollowerAttached(ui32 totalFollowers) = 0;
        virtual void FollowerDetached(ui32 totalFollowers) = 0;
        // all known followers are synced to us (called once)
        virtual void FollowerSyncComplete() = 0;
        // all followers had completed log with requested gc-barrier
        virtual void FollowerGcApplied(ui32 step, TDuration followerSyncDelay) = 0;

        virtual void Execute(TAutoPtr<ITransaction> transaction, const TActorContext &ctx) = 0;
        virtual void Enqueue(TAutoPtr<ITransaction> transaction, const TActorContext &ctx) = 0;

        virtual void ConfirmReadOnlyLease(TMonotonic at) = 0;
        virtual void ConfirmReadOnlyLease(TMonotonic at, std::function<void()> callback) = 0;
        virtual void ConfirmReadOnlyLease(std::function<void()> callback) = 0;

        /* Make blob with data required for table bootstapping. Note:
            1. Once non-trivial blob obtained and commited in tx all of its
                borrowed bundles have to be eventually released (see db).
            2. Call accepts subset range in form [from, to).
            3. [-inf, +inf), the complete set, is encoded as ({ }, { }).
            4. May return empty blob on lack of some vital pages in cache.
         */
        virtual TString BorrowSnapshot(ui32 tableId, const TTableSnapshotContext&, TRawVals from, TRawVals to, ui64 loaner) const = 0;
        // Prepare snapshot which can later be used for scan task.
        virtual ui64 MakeScanSnapshot(ui32 table) = 0;
        virtual void DropScanSnapshot(ui64 snapId) = 0;
        virtual ui64 QueueScan(ui32 tableId, TAutoPtr<NTable::IScan> scan, ui64 cookie, const TScanOptions& options = TScanOptions()) = 0;
        virtual bool CancelScan(ui32 tableId, ui64 taskId) = 0;

        // edge and ts of last full compaction
        virtual TFinishedCompactionInfo GetFinishedCompactionInfo(ui32 tableId) const = 0;

        // Forces full compaction of the specified table in the near future
        // Returns 0 if can't compact, otherwise compaction ID
        virtual ui64 CompactBorrowed(ui32 tableId) = 0;
        virtual ui64 CompactMemTable(ui32 tableId) = 0;
        virtual ui64 CompactTable(ui32 tableId) = 0;
        virtual bool CompactTables() = 0;

        // Signal executor that it's ok to compact borrowed data in the given
        // table even if there's no local modifications. Useful after finishing
        // snapshot transfer on datashard split/merge so any mvcc data and/or
        // erases can be compacted normally.
        virtual void AllowBorrowedGarbageCompaction(ui32 tableId) = 0;

        virtual void RenderHtmlPage(NMon::TEvRemoteHttpInfo::TPtr&) const = 0;
        virtual void RenderHtmlCounters(NMon::TEvRemoteHttpInfo::TPtr&) const = 0;
        virtual void RenderHtmlDb(NMon::TEvRemoteHttpInfo::TPtr &ev, const TActorContext &ctx) const = 0;
        virtual void RegisterExternalTabletCounters(TAutoPtr<TTabletCountersBase> appCounters) = 0;
        virtual void GetTabletCounters(TEvTablet::TEvGetCounters::TPtr&) = 0;

        virtual void UpdateConfig(TEvTablet::TEvUpdateConfig::TPtr&) = 0;

        virtual void SendUserAuxUpdateToFollowers(TString upd, const TActorContext &ctx) = 0;

        // Returns parts owned by this tablet and borrowed by other tablets
        virtual THashMap<TLogoBlobID, TVector<ui64>> GetBorrowedParts() const = 0;
        virtual bool HasLoanedParts() const = 0;

        virtual bool HasBorrowed(ui32 table, ui64 selfTabletId) const = 0;

        // This method lets executor know about new yellow channels
        virtual void OnYellowChannels(TVector<ui32> yellowMoveChannels, TVector<ui32> yellowStopChannels) = 0;

        virtual const TExecutorStats& GetStats() const = 0;
        virtual NMetrics::TResourceMetrics* GetResourceMetrics() const = 0;

        /* This stange looking functionallity probably should be dropped */

        virtual float GetRejectProbability() const = 0;

        // Returns current database scheme (executor must be active)
        virtual const NTable::TScheme& Scheme() const noexcept = 0;

        virtual void SetPreloadTablesData(THashSet<ui32> tables) = 0;

        ui32 Generation() const { return Generation0; }
        ui32 Step() const { return Step0; }
    protected:
        //
        IExecutor()
            : Generation0(0)
            , Step0(0)
        {}

        ui32 Generation0;
        ui32 Step0;
    };

    IActor* CreateExecutor(ITablet *owner, const TActorId& ownerActorId);
};

}} // end of the NKikimr namespace

#pragma once
#include "defs.h"
#include "flat_executor.h"
#include "flat_boot_cookie.h"
#include "flat_boot_util.h"
#include "flat_load_blob_queue.h"

namespace NKikimr {
namespace NTabletFlatExecutor {

class TCommitManager;

namespace NBoot {
    class IStep;
    class TRoot;
    class TBundleLoadStep;
    class TRedo;
    class TMemTable;
    class TStages;
    class TGCLog;
    class TLoans;
    class TAlter;
    class TTurns;
    class TSnap;
    class TLoadBlobs;
    struct TBack;

    struct TResult {
        TAutoPtr<NTable::TDatabase> Database;
        TAutoPtr<TCommitManager> CommitManager;
        TAutoPtr<TLogicSnap> Snap;
        TAutoPtr<TLogicRedo> Redo;
        TAutoPtr<TExecutorGCLogic> GcLogic;
        TAutoPtr<TLogicAlter> Alter;
        TAutoPtr<TCompactionLogicState> Comp;
        TAutoPtr<TExecutorBorrowLogic> Loans;
        THashMap<ui32, NTable::TRowVersionRanges> RemovedRowVersions;

        TVector<TIntrusivePtr<TPrivatePageCache::TInfo>> PageCaches;
        bool ShouldSnapshotScheme = false;
    };
}

class TExecutorBootLogic
    : private ILoadBlob
{
    friend class NBoot::TBundleLoadStep;
    friend class NBoot::TRedo;
    friend class NBoot::TStages;
    friend class NBoot::TLoadBlobs;
    friend class NBoot::TMemTable;
    friend class NBoot::TGCLog;
    friend class NBoot::TLoans;
    friend class NBoot::TAlter;
    friend class NBoot::TTurns;
    friend class NBoot::TSnap;
public:
    enum EOpResult {
        OpResultUnhandled,
        OpResultContinue,
        OpResultBroken,
        OpResultComplete,
    };

private:
    using ELnLev = NUtil::ELnLev;
    using IOps = NActors::IActorOps;
    using TCookie = NBoot::TCookie;

private:
    bool Restored = false;

    IOps * const Ops = nullptr;
    const TActorId SelfId;
    TAutoPtr<NBoot::TBack> State_;
    TAutoPtr<NBoot::TResult> Result_;
    TAutoPtr<NBoot::TRoot> Steps;
    TActorId LeaseWaiter;

    TMonotonic BootTimestamp;

    const TIntrusiveConstPtr<TTabletStorageInfo> Info;

    TLoadBlobQueue LoadBlobQueue;

    THashMap<TLogoBlobID, TIntrusivePtr<NBoot::TLoadBlobs>> EntriesToLoad;
    THashMap<const NPageCollection::IPageCollection*, TIntrusivePtr<NBoot::IStep>> Loads;

    ui32 GroupResolveCachedChannel;
    ui32 GroupResolveCachedGeneration;
    ui32 GroupResolveCachedGroup;

    EOpResult CheckCompletion();

    void PrepareEnv(bool follower, ui32 generation, TExecutorCaches caches) noexcept;
    void StartLeaseWaiter(TMonotonic bootTimestamp, const TEvTablet::TDependencyGraph& graph) noexcept;
    ui32 GetBSGroupFor(const TLogoBlobID &logo) const;
    ui32 GetBSGroupID(ui32 channel, ui32 generation);
    void LoadEntry(TIntrusivePtr<NBoot::TLoadBlobs>);
    NBoot::TSpawned LoadPages(NBoot::IStep*, TAutoPtr<NPageCollection::TFetch> req);

    void OnBlobLoaded(const TLogoBlobID& id, TString body, uintptr_t cookie) override;
    void SeenBlob(const TLogoBlobID& id);

    inline NBoot::TResult& Result() const noexcept { return *Result_; }
    inline NBoot::TBack& State() const noexcept {return *State_; }
public:
    TExecutorBootLogic(IOps*, const TActorId&, TTabletStorageInfo *info, ui64 maxBytesInFly);
    ~TExecutorBootLogic();

    void Describe(IOutputStream&) const noexcept;
    EOpResult ReceiveBoot(TEvTablet::TEvBoot::TPtr &ev, TExecutorCaches &&caches);
    EOpResult ReceiveFollowerBoot(TEvTablet::TEvFBoot::TPtr &ev, TExecutorCaches &&caches);
    EOpResult ReceiveRestored(TEvTablet::TEvRestored::TPtr &ev);
    EOpResult Receive(::NActors::IEventHandle&);

    void FollowersSyncComplete();
    void Cancel();

    TAutoPtr<NBoot::TResult> ExtractState() noexcept;

    TExecutorCaches DetachCaches();
};

}}

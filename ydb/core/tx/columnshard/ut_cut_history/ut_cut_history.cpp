#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/testlib/actor_helpers.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/tx/columnshard/blobs_action/bs/blob_manager.h>
#include <ydb/core/tx/columnshard/blobs_action/bs/gc.h>
#include <ydb/core/tx/columnshard/blobs_action/bs/history_cutter.h>
#include <ydb/core/tx/columnshard/blobs_action/common/const.h>
#include <ydb/core/tx/columnshard/blobs_action/counters/storage.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/size_literals.h>
#include <util/generic/vector.h>

namespace NKikimr {

namespace {

TIntrusivePtr<TTabletStorageInfo> MakeTabletInfo(ui64 tabletId, ui32 nChannels, const TVector<std::pair<ui32, ui32>>& historySlots) {
    auto info = MakeIntrusive<TTabletStorageInfo>();
    info->TabletID = tabletId;
    info->TabletType = TTabletTypes::ColumnShard;
    info->Channels.resize(nChannels);
    for (ui32 ch = 0; ch < nChannels; ++ch) {
        info->Channels[ch].Channel = ch;
        info->Channels[ch].Type = TBlobStorageGroupType(TBlobStorageGroupType::ErasureNone);
        for (const auto& [fromGen, group] : historySlots) {
            TTabletChannelInfo::THistoryEntry e;
            e.FromGeneration = fromGen;
            e.GroupID = group;
            info->Channels[ch].History.push_back(e);
        }
    }
    return info;
}

static constexpr ui32 BlobSize = 1_KB;

TLogoBlobID MakeBlob(ui64 tabletId, ui32 channel, ui32 gen, ui32 step = 1, ui32 cookie = 1) {
    return TLogoBlobID(tabletId, gen, step, channel, BlobSize, cookie);
}

NOlap::TUnifiedBlobId MakeUnifiedBlob(const TLogoBlobID& logo) {
    return NOlap::TUnifiedBlobId(logo.TabletID(), logo);
}

class TCutHistoryController: public NYDBTest::ICSController {
public:
    bool IsCSCutHistoryEnabled() const override {
        return true;
    }

    void OnHistoryEntryNominated(const ui32 channel, const ui32 fromGeneration) override {
        TGuard<TMutex> g(Mutex);
        Nominated.emplace_back(channel, fromGeneration);
    }

    void OnHistoryEntryCut(const ui32 channel, const ui32 fromGeneration) override {
        TGuard<TMutex> g(Mutex);
        Cut.emplace_back(channel, fromGeneration);
    }

    TVector<std::pair<ui32, ui32>> GetNominated() const {
        TGuard<TMutex> g(Mutex);
        return Nominated;
    }

    TVector<std::pair<ui32, ui32>> GetCut() const {
        TGuard<TMutex> g(Mutex);
        return Cut;
    }

    void Reset() {
        TGuard<TMutex> g(Mutex);
        Nominated.clear();
        Cut.clear();
    }

private:
    mutable TMutex Mutex;
    TVector<std::pair<ui32, ui32>> Nominated;
    TVector<std::pair<ui32, ui32>> Cut;
};

}   // anonymous namespace

using TEntryKey = NOlap::NBlobOperations::NBlobStorage::TEntryKey;
using THistoryCutterWrapper = NOlap::NBlobOperations::NBlobStorage::THistoryCutterWrapper;
using ECutState = NOlap::NBlobOperations::NBlobStorage::ECutState;

// Only a live sensor instance is needed: values are asserted through the cutter's own state.
static const NColumnShard::THistoryCutterCounters& TestSignals() {
    static const NColumnShard::TBlobsManagerCounters counters("UT_BlobsManager");
    return counters.HistoryCutterCounters;
}

class TTestableHistoryCutter: public THistoryCutterWrapper {
public:
    using THistoryCutterWrapper::GetCutStateForTest;
    using THistoryCutterWrapper::IsDrained;
    using THistoryCutterWrapper::IsNominationPendingForTest;
    using THistoryCutterWrapper::IsNominationTriggeredForTest;
    using THistoryCutterWrapper::THistoryCutterWrapper;
};

struct TCutterEnv {
    TIntrusivePtr<TTabletStorageInfo> Info;
    std::shared_ptr<NOlap::TBlobManager> Bm;
    std::shared_ptr<NOlap::NDataSharing::TStorageSharedBlobsManager> Shared;
};

// The cutter holds weak_ptrs to both managers, so keep this object in scope ahead of the cutter.
TCutterEnv MakeCutterEnv(
    const ui64 tabletId, const ui32 gen, const ui32 nChannels = 3, const TVector<std::pair<ui32, ui32>>& history = { { 0, 100 }, { 5, 200 } }) {
    auto info = MakeTabletInfo(tabletId, nChannels, history);
    return { info, std::make_shared<NOlap::TBlobManager>(info, gen, NOlap::TTabletId(tabletId)),
        std::make_shared<NOlap::NDataSharing::TStorageSharedBlobsManager>(
            NOlap::NBlobOperations::TGlobal::DefaultStorageId, NOlap::TTabletId(tabletId)) };
}

// Runs callbacks under a real actor context so Send/Register paths execute for real.
struct TEvRunInActor: public NActors::TEventLocal<TEvRunInActor, EventSpaceBegin(NActors::TEvents::ES_PRIVATE)> {
    std::function<void(const NActors::TActorContext&)> Fn;

    explicit TEvRunInActor(std::function<void(const NActors::TActorContext&)> fn)
        : Fn(std::move(fn))
    {
    }
};

class TRunnerActor: public NActors::TActor<TRunnerActor> {
public:
    TRunnerActor()
        : NActors::TActor<TRunnerActor>(&TRunnerActor::StateWork) {
    }

    STFUNC(StateWork) {
        if (auto* run = ev->CastAsLocal<TEvRunInActor>()) {
            run->Fn(ActorContext());
        }
    }
};

// Stands in for Local: observers see an edge actor's events zero or several times, so a real actor counts them.
class TNominationCounter: public NActors::TActor<TNominationCounter> {
public:
    explicit TNominationCounter(ui32& count)
        : NActors::TActor<TNominationCounter>(&TNominationCounter::StateWork)
        , Count(count)
    {
    }

    STFUNC(StateWork) {
        if (ev->GetTypeRewrite() == NColumnShard::TEvPrivate::TEvCutHistoryNominate::EventType) {
            ++Count;
        }
    }

private:
    ui32& Count;
};

// Advances LastCollectedGenStep past the current generation so HasCollectedBeforeCurrentGeneration() is true.
static void CommitFirstGcRound(
    std::shared_ptr<NOlap::TBlobManager>& bm, ui32 currentGen, std::shared_ptr<NOlap::NDataSharing::TStorageSharedBlobsManager>&) {
    const NOlap::TGenStep barrier{ currentGen, 0 };
    bm->OnGCStartOnComplete(barrier);
    bm->OnGCFinishedOnComplete(barrier);
}

Y_UNIT_TEST_SUITE(TCutHistoryCutter) {
    // Channels 0-2, history {fromGen=0, group=100} and active {fromGen=5, group=200}; only ch >= 2 is tracked.

    Y_UNIT_TEST(SeenGroupsCheck) {
        // History [{0,100}, {5,100}, {10,200}]: {5,100} is blocked by the earlier {0,100}, the others pass.
        using TEntry = TTabletChannelInfo::THistoryEntry;
        std::vector<TEntry> hist;
        auto addEntry = [&](ui32 fromGen, ui32 group) {
            TEntry e;
            e.FromGeneration = fromGen;
            e.GroupID = group;
            hist.push_back(e);
        };
        addEntry(0, 100);
        addEntry(5, 100);
        addEntry(10, 200);

        UNIT_ASSERT(THistoryCutterWrapper::SeenGroupsCheckPasses(hist, /*fromGen=*/0));
        UNIT_ASSERT(!THistoryCutterWrapper::SeenGroupsCheckPasses(hist, /*fromGen=*/5));
        UNIT_ASSERT(THistoryCutterWrapper::SeenGroupsCheckPasses(hist, /*fromGen=*/10));
        UNIT_ASSERT(!THistoryCutterWrapper::SeenGroupsCheckPasses(hist, /*fromGen=*/99));

        // {5,100} is blocked by {0,100} unless that one was cut: cut entries are transparent.
        UNIT_ASSERT(THistoryCutterWrapper::SeenGroupsCheckPasses(hist, /*fromGen=*/5, /*cutFromGenerations=*/{ 0 }));
        UNIT_ASSERT(!THistoryCutterWrapper::SeenGroupsCheckPasses(hist, /*fromGen=*/5, /*cutFromGenerations=*/{ 10 }));
    }

    // Shared-out blobs must pin the entry: they are in no GC queue, so the drain gate must block the cut.
    Y_UNIT_TEST(SharedBlobsPinDrainGate) {
        TActorSystemStub actorSystemStub;
        actorSystemStub.AppData.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        static constexpr ui64 TabletId = 888;
        static constexpr ui64 BorrowerTabletId = 999;
        // History: {fromGen=0, group=100}, {fromGen=5, group=200 (active)}.
        auto [info, bm, shared] = MakeCutterEnv(TabletId, /*gen=*/5);

        TTestableHistoryCutter cutter(info, bm, shared, TActorId(), TestSignals());
        const TEntryKey key{ /*channel=*/2, /*fromGeneration=*/0 };

        UNIT_ASSERT(cutter.IsDrained(key));

        // Share out one of OUR blobs living in the old range (channel 2, gen 1 < 5).
        const NOlap::TUnifiedBlobId sharedOut(/*dsGroup=*/100, TLogoBlobID(TabletId, /*gen=*/1, /*step=*/1, /*channel=*/2, 100, 1));
        UNIT_ASSERT(shared->UpsertSharedBlobOnLoad(sharedOut, NOlap::TTabletId(BorrowerTabletId)));
        UNIT_ASSERT_C(!cutter.IsDrained(key), "shared-out blob in the old range must pin the entry");
    }

    // Sharing must block the cut itself, not merely the drain gate, and must release it again.
    Y_UNIT_TEST(SharedBlobBlocksCutUntilReleased) {
        TTestBasicRuntime runtime;
        TAppPrepare app;
        runtime.Initialize(app.Unwrap());
        runtime.GetAppData().ColumnShardConfig.SetCutHistoryMeasureOnly(false);
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();

        static constexpr ui64 TabletId = 3044;
        static constexpr ui64 BorrowerTabletId = 999;
        static constexpr ui32 DataChannel = 2;
        static constexpr ui32 OldFromGen = 0;
        static constexpr ui32 OldGroup = 100;
        static constexpr ui32 CurrentGen = 5;

        const auto edgeTablet = runtime.AllocateEdgeActor();
        const auto edgeLauncher = runtime.AllocateEdgeActor();
        const auto runner = runtime.Register(new TRunnerActor());
        auto runInActor = [&](std::function<void(const NActors::TActorContext&)> fn) {
            runtime.Send(new IEventHandle(runner, edgeTablet, new TEvRunInActor(std::move(fn))));
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
        };

        auto [info, bm, shared] = MakeCutterEnv(TabletId, CurrentGen, /*nChannels=*/3, { { OldFromGen, OldGroup }, { CurrentGen, 200 } });
        TTestableHistoryCutter cutter(info, bm, shared, edgeTablet, TestSignals());
        cutter.SetLauncherActorId(edgeLauncher);
        const TEntryKey key{ DataChannel, OldFromGen };

        // Everything else a cut needs is in place, so sharing is the only thing left to hold it.
        CommitFirstGcRound(bm, CurrentGen, shared);
        bm->AddMoveDataRowOnComplete(DataChannel, OldFromGen, CurrentGen, OldGroup);

        const NOlap::TUnifiedBlobId sharedOut(/*dsGroup=*/OldGroup, TLogoBlobID(TabletId, /*gen=*/1, /*step=*/1, DataChannel, 100, 1));
        UNIT_ASSERT(shared->UpsertSharedBlobOnLoad(sharedOut, NOlap::TTabletId(BorrowerTabletId)));

        runtime.AdvanceCurrentTime(THistoryCutterWrapper::DefaultNominateCadence + TDuration::Seconds(1));
        runInActor([&](const NActors::TActorContext& ctx) {
            cutter.TryNominate(ctx);
        });
        UNIT_ASSERT_C(guard->GetCut().empty(), "cut an entry whose range still holds a shared-out blob");
        UNIT_ASSERT_C(cutter.GetCutStateForTest(key) != ECutState::Cut, "the entry reached Cut while shared out");

        // Releasing the share is what makes the range genuinely empty.
        NOlap::TTabletsByBlob released;
        released.Add(NOlap::TTabletId(BorrowerTabletId), sharedOut);
        shared->RemoveSharedBlobs(released);

        runtime.AdvanceCurrentTime(THistoryCutterWrapper::DefaultNominateCadence + TDuration::Seconds(1));
        bool nominated = false;
        runInActor([&](const NActors::TActorContext& ctx) {
            nominated = cutter.TryNominate(ctx);
        });
        UNIT_ASSERT_C(nominated, "the entry was not nominated after the share was released");
        UNIT_ASSERT_C(cutter.GetCutStateForTest(key) == ECutState::Cut, "no cut after the share was released");
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvTablet::TEvCutTabletHistory>(edgeLauncher));
    }

    // An entry whose range still holds queued blobs must not be nominated (HasNoBlobsInRange arm).
    Y_UNIT_TEST(QueuedBlobsPinDrainGate) {
        TActorSystemStub actorSystemStub;
        actorSystemStub.AppData.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        static constexpr ui64 TabletId = 888;
        static constexpr ui32 ChannelCount = 3;
        static constexpr ui32 OldFromGen = 0;
        static constexpr ui32 OldGroup = 100;
        static constexpr ui32 ActiveFromGen = 5;
        static constexpr ui32 ActiveGroup = 200;
        static constexpr ui32 DataChannel = 2;
        static constexpr ui32 OtherChannel = 1;
        static constexpr ui32 GenInOldRange = 1;
        const TVector<std::pair<ui32, ui32>> history{ { OldFromGen, OldGroup }, { ActiveFromGen, ActiveGroup } };
        const TEntryKey key{ DataChannel, OldFromGen };

        // Both owners must outlive the cutter: an expired weak_ptr looks exactly like "not drained".
        auto makeCutter = [&](std::shared_ptr<NOlap::TBlobManager>& bmOut,
                              std::shared_ptr<NOlap::NDataSharing::TStorageSharedBlobsManager>& sharedOut) {
            auto info = MakeTabletInfo(TabletId, ChannelCount, history);
            bmOut = std::make_shared<NOlap::TBlobManager>(info, ActiveFromGen, NOlap::TTabletId(TabletId));
            sharedOut = std::make_shared<NOlap::NDataSharing::TStorageSharedBlobsManager>(
                NOlap::NBlobOperations::TGlobal::DefaultStorageId, NOlap::TTabletId(TabletId));
            return TTestableHistoryCutter(info, bmOut, sharedOut, TActorId(), TestSignals());
        };

        std::shared_ptr<NOlap::TBlobManager> bm;
        std::shared_ptr<NOlap::NDataSharing::TStorageSharedBlobsManager> shared;
        auto cutter = makeCutter(bm, shared);
        UNIT_ASSERT_C(cutter.IsDrained(key), "empty queues: the old entry starts drained");

        // A blob still awaiting collection, inside the entry's range.
        bm->DeleteBlobOnComplete(NOlap::TTabletId(TabletId), MakeUnifiedBlob(MakeBlob(TabletId, DataChannel, GenInOldRange)));
        UNIT_ASSERT_C(!cutter.IsDrained(key), "a blob still in the delete queue must pin the entry");

        // Same channel, but the active entry's generation — outside this range.
        std::shared_ptr<NOlap::TBlobManager> bmOutside;
        std::shared_ptr<NOlap::NDataSharing::TStorageSharedBlobsManager> sharedOutside;
        auto cutterOutside = makeCutter(bmOutside, sharedOutside);
        bmOutside->DeleteBlobOnComplete(NOlap::TTabletId(TabletId), MakeUnifiedBlob(MakeBlob(TabletId, DataChannel, ActiveFromGen)));
        UNIT_ASSERT_C(cutterOutside.IsDrained(key), "a blob outside the range must not pin the entry");

        // Another channel entirely.
        std::shared_ptr<NOlap::TBlobManager> bmOtherChannel;
        std::shared_ptr<NOlap::NDataSharing::TStorageSharedBlobsManager> sharedOtherChannel;
        auto cutterOtherChannel = makeCutter(bmOtherChannel, sharedOtherChannel);
        bmOtherChannel->DeleteBlobOnComplete(NOlap::TTabletId(TabletId), MakeUnifiedBlob(MakeBlob(TabletId, OtherChannel, GenInOldRange)));
        UNIT_ASSERT_C(cutterOtherChannel.IsDrained(key), "a blob on another channel must not pin the entry");
    }

    // A blob of another tablet in our delete queue must not pin our history entry.
    Y_UNIT_TEST(ForeignBlobInDeleteQueueDoesNotPin) {
        TActorSystemStub actorSystemStub;
        actorSystemStub.AppData.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        static constexpr ui64 OurTabletId = 1111;
        static constexpr ui64 ForeignTabletId = 2222;
        static constexpr ui32 DataChannel = 2;
        static constexpr ui32 OldFromGen = 0;
        static constexpr ui32 ActiveFromGen = 5;
        static constexpr ui32 GenInOldRange = 1;
        const TEntryKey key{ DataChannel, OldFromGen };

        auto info = MakeTabletInfo(OurTabletId, 3, { { OldFromGen, 100 }, { ActiveFromGen, 200 } });
        auto bm = std::make_shared<NOlap::TBlobManager>(info, ActiveFromGen, NOlap::TTabletId(OurTabletId));
        auto shared = std::make_shared<NOlap::NDataSharing::TStorageSharedBlobsManager>(
            NOlap::NBlobOperations::TGlobal::DefaultStorageId, NOlap::TTabletId(OurTabletId));
        TTestableHistoryCutter cutter(info, bm, shared, TActorId(), TestSignals());

        UNIT_ASSERT_C(cutter.IsDrained(key), "empty queues: old entry starts drained");

        // A foreign tablet's blob in our delete queue must not pin our entry.
        bm->DeleteBlobOnComplete(NOlap::TTabletId(ForeignTabletId), MakeUnifiedBlob(MakeBlob(ForeignTabletId, DataChannel, GenInOldRange)));
        UNIT_ASSERT_C(cutter.IsDrained(key), "a foreign blob in our delete queue must not pin our history entry");

        // Our own blob in the same range still does pin it.
        bm->DeleteBlobOnComplete(NOlap::TTabletId(OurTabletId), MakeUnifiedBlob(MakeBlob(OurTabletId, DataChannel, GenInOldRange)));
        UNIT_ASSERT_C(!cutter.IsDrained(key), "our own blob in the delete queue must pin the entry");
    }

    Y_UNIT_TEST(InFlightGCTaskPinsDrainGate) {
        TActorSystemStub actorSystemStub;
        actorSystemStub.AppData.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        static constexpr ui64 TabletId = 889;
        static constexpr ui32 CurrentGen = 5;
        const TVector<std::pair<ui32, ui32>> history{ { 0, 100 }, { CurrentGen, 200 } };
        const TEntryKey key{ 2, 0 };

        auto info = MakeTabletInfo(TabletId, 3, history);
        auto bm = std::make_shared<NOlap::TBlobManager>(info, CurrentGen, NOlap::TTabletId(TabletId));
        auto shared = std::make_shared<NOlap::NDataSharing::TStorageSharedBlobsManager>(
            NOlap::NBlobOperations::TGlobal::DefaultStorageId, NOlap::TTabletId(TabletId));
        TTestableHistoryCutter cutter(info, bm, shared, TActorId(), TestSignals());
        UNIT_ASSERT(cutter.IsDrained(key));

        auto storageCounters = std::make_shared<NOlap::NBlobOperations::TStorageCounters>(NOlap::NBlobOperations::TGlobal::DefaultStorageId);
        auto gcCounters =
            std::make_shared<NOlap::NBlobOperations::TRemoveGCCounters>(NOlap::NBlobOperations::TConsumerCounters("GC", *storageCounters));
        auto task = bm->BuildGCTask(NOlap::NBlobOperations::TGlobal::DefaultStorageId, bm, shared, gcCounters);
        UNIT_ASSERT_C(task, "the first GC round carries the barrier even with empty queues");
        UNIT_ASSERT_C(!cutter.IsDrained(key), "a GC task in flight must pin every entry");

        const TGenStep barrier{ CurrentGen, 0 };
        bm->OnGCStartOnComplete(barrier);
        bm->OnGCFinishedOnComplete(barrier);
        UNIT_ASSERT_C(cutter.IsDrained(key), "the pin is released once the task commits");
    }

    Y_UNIT_TEST(OrphanedDeleteMarkUnderCutEntryIsErasedNotCollected) {
        TActorSystemStub actorSystemStub;
        actorSystemStub.AppData.ColumnShardConfig.SetCutHistoryMeasureOnly(false);
        actorSystemStub.AppData.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        static constexpr ui64 TabletId = 890;
        static constexpr ui32 CurrentGen = 7;
        static constexpr ui32 DataChannel = 2;
        const TVector<std::pair<ui32, ui32>> history{ { 5, 200 } };

        auto info = MakeTabletInfo(TabletId, 3, history);
        auto bm = std::make_shared<NOlap::TBlobManager>(info, CurrentGen, NOlap::TTabletId(TabletId));
        auto shared = std::make_shared<NOlap::NDataSharing::TStorageSharedBlobsManager>(
            NOlap::NBlobOperations::TGlobal::DefaultStorageId, NOlap::TTabletId(TabletId));

        const TLogoBlobID orphan(TabletId, /*gen=*/0, /*step=*/0, DataChannel, BlobSize, /*cookie=*/1);
        UNIT_ASSERT_VALUES_EQUAL(info->GroupFor(orphan), Max<ui32>());
        bm->DeleteBlobOnComplete(NOlap::TTabletId(TabletId), NOlap::TUnifiedBlobId(Max<ui32>(), orphan));

        auto storageCounters = std::make_shared<NOlap::NBlobOperations::TStorageCounters>(NOlap::NBlobOperations::TGlobal::DefaultStorageId);
        auto gcCounters =
            std::make_shared<NOlap::NBlobOperations::TRemoveGCCounters>(NOlap::NBlobOperations::TConsumerCounters("GC", *storageCounters));
        auto task = bm->BuildGCTask(NOlap::NBlobOperations::TGlobal::DefaultStorageId, bm, shared, gcCounters);
        UNIT_ASSERT(task);
        const TString sentinelPrefix = "g=" + ToString(Max<ui32>()) + ";";
        for (const auto& [address, lists] : task->GetListsByGroupId()) {
            UNIT_ASSERT_C(
                !address.DebugString().StartsWith(sentinelPrefix), "no GC request may target the sentinel group: " + address.DebugString());
        }
        bm->OnGCStartOnComplete(TGenStep{ CurrentGen, 0 });
        bm->OnGCFinishedOnComplete(TGenStep{ CurrentGen, 0 });
        UNIT_ASSERT_C(bm->HasNoBlobsInRange(DataChannel, 0, 5), "the orphaned mark left the delete queue with the task");
    }

    // Happy path: after the first GC round, a proven entry gets TEvCutTabletHistory.
    Y_UNIT_TEST(HappyPathCutsAfterFirstGcRound) {
        TTestBasicRuntime runtime;
        TAppPrepare app;
        runtime.Initialize(app.Unwrap());
        runtime.GetAppData().ColumnShardConfig.SetCutHistoryMeasureOnly(false);
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();

        static constexpr ui64 TabletId = 3030;
        static constexpr ui32 DataChannel = 2;
        static constexpr ui32 OldFromGen = 0;
        static constexpr ui32 OldGroup = 100;
        static constexpr ui32 ActiveFromGen = 5;
        static constexpr ui32 ActiveGroup = 200;
        static constexpr ui32 CurrentGen = ActiveFromGen;

        const auto edgeTablet = runtime.AllocateEdgeActor();
        const auto edgeLauncher = runtime.AllocateEdgeActor();
        const auto runner = runtime.Register(new TRunnerActor());
        auto runInActor = [&](std::function<void(const NActors::TActorContext&)> fn) {
            runtime.Send(new IEventHandle(runner, edgeTablet, new TEvRunInActor(std::move(fn))));
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
        };

        auto [info, bm, shared] =
            MakeCutterEnv(TabletId, CurrentGen, /*nChannels=*/3, { { OldFromGen, OldGroup }, { ActiveFromGen, ActiveGroup } });
        CommitFirstGcRound(bm, CurrentGen, shared);
        // A cut requires the row MoveData persists for exactly this interval.
        bm->AddMoveDataRowOnComplete(DataChannel, OldFromGen, ActiveFromGen, OldGroup);
        TTestableHistoryCutter cutter(info, bm, shared, edgeTablet, TestSignals());
        cutter.SetLauncherActorId(edgeLauncher);
        const TEntryKey key{ DataChannel, OldFromGen };

        bool nominated = false;
        runInActor([&](const NActors::TActorContext& ctx) {
            nominated = cutter.TryNominate(ctx);
        });
        UNIT_ASSERT(nominated);
        UNIT_ASSERT_VALUES_EQUAL(guard->GetNominated().size(), 1);
        UNIT_ASSERT(cutter.GetCutStateForTest(key) == ECutState::Cut);
        UNIT_ASSERT_VALUES_EQUAL(guard->GetCut().size(), 1);

        auto cutReq = runtime.GrabEdgeEvent<TEvTablet::TEvCutTabletHistory>(edgeLauncher);
        UNIT_ASSERT(cutReq);
        UNIT_ASSERT_VALUES_EQUAL(cutReq->Get()->Record.GetTabletID(), TabletId);
        UNIT_ASSERT_VALUES_EQUAL(cutReq->Get()->Record.GetChannel(), DataChannel);
        UNIT_ASSERT_VALUES_EQUAL(cutReq->Get()->Record.GetFromGeneration(), OldFromGen);
        UNIT_ASSERT_VALUES_EQUAL(cutReq->Get()->Record.GetGroupID(), OldGroup);

        // A cut entry is never nominated again.
        const auto nominationsAfterCut = guard->GetNominated().size();
        runInActor([&](const NActors::TActorContext& ctx) {
            nominated = cutter.TryNominate(ctx);
        });
        UNIT_ASSERT_C(!nominated, "a cut entry must not be renominated");
        UNIT_ASSERT_VALUES_EQUAL(guard->GetNominated().size(), nominationsAfterCut);
        UNIT_ASSERT_VALUES_EQUAL(guard->GetCut().size(), 1);
    }

    // Entry is not cut before the first GC round commits; cut right after it commits.
    Y_UNIT_TEST(NoCutBeforeFirstGcRound) {
        TTestBasicRuntime runtime;
        TAppPrepare app;
        runtime.Initialize(app.Unwrap());
        runtime.GetAppData().ColumnShardConfig.SetCutHistoryMeasureOnly(false);
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();

        static constexpr ui64 TabletId = 3031;
        static constexpr ui32 DataChannel = 2;
        static constexpr ui32 OldFromGen = 0;
        static constexpr ui32 OldGroup = 100;
        static constexpr ui32 CurrentGen = 5;

        const auto edgeTablet = runtime.AllocateEdgeActor();
        const auto edgeLauncher = runtime.AllocateEdgeActor();
        const auto runner = runtime.Register(new TRunnerActor());
        auto runInActor = [&](std::function<void(const NActors::TActorContext&)> fn) {
            runtime.Send(new IEventHandle(runner, edgeTablet, new TEvRunInActor(std::move(fn))));
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
        };

        auto [info, bm, shared] = MakeCutterEnv(TabletId, CurrentGen, /*nChannels=*/3, { { OldFromGen, OldGroup }, { CurrentGen, 200 } });
        // GC round NOT yet committed.
        TTestableHistoryCutter cutter(info, bm, shared, edgeTablet, TestSignals());
        cutter.SetLauncherActorId(edgeLauncher);
        const TEntryKey key{ DataChannel, OldFromGen };

        // GC not committed → HasCollectedBeforeCurrentGeneration is false; entry stays None.
        runInActor([&](const NActors::TActorContext& ctx) {
            cutter.TryNominate(ctx);
        });
        UNIT_ASSERT(cutter.GetCutStateForTest(key) == ECutState::None);
        UNIT_ASSERT(guard->GetCut().empty());

        // Commit the GC round; next nomination must now cut directly.
        CommitFirstGcRound(bm, CurrentGen, shared);
        // A cut requires the row MoveData persists for exactly this interval.
        bm->AddMoveDataRowOnComplete(DataChannel, OldFromGen, CurrentGen, OldGroup);
        runtime.AdvanceCurrentTime(THistoryCutterWrapper::DefaultNominateCadence + TDuration::Seconds(1));
        bool nominated = false;
        runInActor([&](const NActors::TActorContext& ctx) {
            nominated = cutter.TryNominate(ctx);
        });
        UNIT_ASSERT(nominated);
        UNIT_ASSERT(cutter.GetCutStateForTest(key) == ECutState::Cut);
        UNIT_ASSERT_VALUES_EQUAL(guard->GetCut().size(), 1);
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvTablet::TEvCutTabletHistory>(edgeLauncher));
    }

    // A group appearing twice in one channel's history: the earlier entry is cut first, then the later one is also cut.
    Y_UNIT_TEST(DuplicateGroupBothEntriesCut) {
        TTestBasicRuntime runtime;
        TAppPrepare app;
        runtime.Initialize(app.Unwrap());
        runtime.GetAppData().ColumnShardConfig.SetCutHistoryMeasureOnly(false);
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();

        static constexpr ui64 TabletId = 3032;
        static constexpr ui32 DataChannel = 2;
        static constexpr ui32 GroupX = 100;
        static constexpr ui32 GroupZ = 300;
        static constexpr ui32 CurrentGen = 21;
        // History: {0,X},{10,X},{20,Z(active)} — X appears at both fromGen=0 and fromGen=10.
        const TVector<std::pair<ui32, ui32>> hist{ { 0, GroupX }, { 10, GroupX }, { 20, GroupZ } };

        const auto edgeTablet = runtime.AllocateEdgeActor();
        const auto edgeLauncher = runtime.AllocateEdgeActor();
        const auto runner = runtime.Register(new TRunnerActor());
        auto runInActor = [&](std::function<void(const NActors::TActorContext&)> fn) {
            runtime.Send(new IEventHandle(runner, edgeTablet, new TEvRunInActor(std::move(fn))));
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
        };

        auto [info, bm, shared] = MakeCutterEnv(TabletId, CurrentGen, /*nChannels=*/3, hist);
        CommitFirstGcRound(bm, CurrentGen, shared);
        // A cut requires the row MoveData persists for exactly this interval.
        bm->AddMoveDataRowOnComplete(DataChannel, 0, 10, GroupX);
        bm->AddMoveDataRowOnComplete(DataChannel, 10, 20, GroupX);
        TTestableHistoryCutter cutter(info, bm, shared, edgeTablet, TestSignals());
        cutter.SetLauncherActorId(edgeLauncher);

        const TEntryKey keyX0{ DataChannel, 0 };
        const TEntryKey keyX10{ DataChannel, 10 };

        // SeenGroupsCheckPasses blocks keyX10 until keyX0 is cut first; first TryNominate cuts only keyX0.
        bool nominated = false;
        runInActor([&](const NActors::TActorContext& ctx) {
            nominated = cutter.TryNominate(ctx);
        });
        UNIT_ASSERT(nominated);
        UNIT_ASSERT_C(cutter.GetCutStateForTest(keyX0) == ECutState::Cut, "earlier X entry must be cut first");
        UNIT_ASSERT_C(cutter.GetCutStateForTest(keyX10) == ECutState::None, "later X entry must not be cut before the earlier one");
        auto cutX0 = runtime.GrabEdgeEvent<TEvTablet::TEvCutTabletHistory>(edgeLauncher);
        UNIT_ASSERT(cutX0);
        UNIT_ASSERT_VALUES_EQUAL(cutX0->Get()->Record.GetFromGeneration(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(cutX0->Get()->Record.GetGroupID(), GroupX);

        // After the earlier entry is cut, advance past the cadence; the next TryNominate cuts keyX10.
        runtime.AdvanceCurrentTime(THistoryCutterWrapper::DefaultNominateCadence + TDuration::Seconds(1));
        runInActor([&](const NActors::TActorContext& ctx) {
            nominated = cutter.TryNominate(ctx);
        });
        UNIT_ASSERT(nominated);
        UNIT_ASSERT_C(cutter.GetCutStateForTest(keyX10) == ECutState::Cut, "later X entry must also be cut");
        auto cutX10 = runtime.GrabEdgeEvent<TEvTablet::TEvCutTabletHistory>(edgeLauncher);
        UNIT_ASSERT(cutX10);
        UNIT_ASSERT_VALUES_EQUAL(cutX10->Get()->Record.GetFromGeneration(), 10u);
        UNIT_ASSERT_VALUES_EQUAL(cutX10->Get()->Record.GetGroupID(), GroupX);
    }

    // DoNotKeep (in-flight GC) pins IsDrained; after the round commits, nomination succeeds and the entry is cut.
    Y_UNIT_TEST(QueuedBlobBlocksCutUntilGcRoundCommits) {
        TTestBasicRuntime runtime;
        TAppPrepare app;
        runtime.Initialize(app.Unwrap());
        runtime.GetAppData().ColumnShardConfig.SetCutHistoryMeasureOnly(false);
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();

        static constexpr ui64 TabletId = 3033;
        static constexpr ui32 DataChannel = 2;
        static constexpr ui32 OldFromGen = 0;
        static constexpr ui32 OldGroup = 100;
        static constexpr ui32 CurrentGen = 5;

        const auto edgeTablet = runtime.AllocateEdgeActor();
        const auto edgeLauncher = runtime.AllocateEdgeActor();
        const auto runner = runtime.Register(new TRunnerActor());
        auto runInActor = [&](std::function<void(const NActors::TActorContext&)> fn) {
            runtime.Send(new IEventHandle(runner, edgeTablet, new TEvRunInActor(std::move(fn))));
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
        };

        auto [info, bm, shared] = MakeCutterEnv(TabletId, CurrentGen, /*nChannels=*/3, { { OldFromGen, OldGroup }, { CurrentGen, 200 } });
        TTestableHistoryCutter cutter(info, bm, shared, edgeTablet, TestSignals());
        cutter.SetLauncherActorId(edgeLauncher);
        const TEntryKey key{ DataChannel, OldFromGen };

        // Start a GC round (advance 60s first to pass the throttle window); it pins IsDrained while in flight.
        runtime.AdvanceCurrentTime(TDuration::Seconds(60));
        auto storageCounters = std::make_shared<NOlap::NBlobOperations::TStorageCounters>(NOlap::NBlobOperations::TGlobal::DefaultStorageId);
        auto gcCounters =
            std::make_shared<NOlap::NBlobOperations::TRemoveGCCounters>(NOlap::NBlobOperations::TConsumerCounters("GC", *storageCounters));
        bool taskCreated = false;
        runInActor([&](const NActors::TActorContext&) {
            auto task = bm->BuildGCTask(NOlap::NBlobOperations::TGlobal::DefaultStorageId, bm, shared, gcCounters);
            taskCreated = (task != nullptr);
        });
        UNIT_ASSERT_C(taskCreated, "first GC round must produce a task");
        UNIT_ASSERT_C(!cutter.IsDrained(key), "GC task in flight must pin the drain gate");

        // TryNominate skips entries whose drain gate is pinned.
        runInActor([&](const NActors::TActorContext& ctx) {
            cutter.TryNominate(ctx);
        });
        UNIT_ASSERT(guard->GetNominated().empty());

        // Commit the round: gate opens and HasCollectedBeforeCurrentGeneration becomes true.
        CommitFirstGcRound(bm, CurrentGen, shared);
        // A cut requires the row MoveData persists for exactly this interval.
        bm->AddMoveDataRowOnComplete(DataChannel, OldFromGen, CurrentGen, OldGroup);
        UNIT_ASSERT_C(cutter.IsDrained(key), "drain gate must open after GC commits");

        // Advance past the nomination cadence — the first TryNominate call set LastNominateAt.
        runtime.AdvanceCurrentTime(THistoryCutterWrapper::DefaultNominateCadence + TDuration::Seconds(1));
        bool nominated = false;
        runInActor([&](const NActors::TActorContext& ctx) {
            nominated = cutter.TryNominate(ctx);
        });
        UNIT_ASSERT(nominated);
        UNIT_ASSERT(cutter.GetCutStateForTest(key) == ECutState::Cut);
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvTablet::TEvCutTabletHistory>(edgeLauncher));
    }

    // Hive erases a cut entry wherever it sits, and the lookup then hands its generations to the previous entry.
    Y_UNIT_TEST(MiddleEntryCutRemapsItsRangeToThePreviousGroup) {
        auto info = MakeTabletInfo(/*tabletId=*/4040, /*nChannels=*/4, { { 1, 100 }, { 5, 200 }, { 9, 300 } });
        auto& history = info->Channels[3].History;
        history.erase(history.begin() + 1);

        for (const ui32 gen : { 1u, 2u, 4u, 5u, 8u }) {
            UNIT_ASSERT_VALUES_EQUAL_C(info->GroupFor(3, gen), 100u, "gen " << gen);
        }
        for (const ui32 gen : { 9u, 10u }) {
            UNIT_ASSERT_VALUES_EQUAL_C(info->GroupFor(3, gen), 300u, "gen " << gen);
        }
        UNIT_ASSERT_VALUES_EQUAL_C(info->GroupFor(2, 5), 200u, "a cut on one channel leaves the others alone");
    }

    // Middle-entry cut targets only its own entry: TEvCutTabletHistory carries G (not G0 or G2).
    Y_UNIT_TEST(MiddleEntryCutTargetsOnlyItsOwnGroup) {
        TTestBasicRuntime runtime;
        TAppPrepare app;
        runtime.Initialize(app.Unwrap());
        runtime.GetAppData().ColumnShardConfig.SetCutHistoryMeasureOnly(false);
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();

        static constexpr ui64 TabletId = 4040;
        static constexpr ui32 Channel = 3;
        static constexpr ui32 GroupG0 = 100;
        static constexpr ui32 GroupG = 200;
        static constexpr ui32 GroupG2 = 300;
        static constexpr ui32 CurrentGen = 10;

        const auto edgeTablet = runtime.AllocateEdgeActor();
        const auto edgeLauncher = runtime.AllocateEdgeActor();
        const auto runner = runtime.Register(new TRunnerActor());
        auto runInActor = [&](std::function<void(const NActors::TActorContext&)> fn) {
            runtime.Send(new IEventHandle(runner, edgeTablet, new TEvRunInActor(std::move(fn))));
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
        };

        auto [info, bm, shared] = MakeCutterEnv(TabletId, CurrentGen, /*nChannels=*/4, { { 1, GroupG0 }, { 5, GroupG }, { 9, GroupG2 } });
        CommitFirstGcRound(bm, CurrentGen, shared);
        // A cut requires the row MoveData persists for exactly this interval.
        bm->AddMoveDataRowOnComplete(Channel, 5, 9, GroupG);
        TTestableHistoryCutter cutter(info, bm, shared, edgeTablet, TestSignals());
        cutter.SetLauncherActorId(edgeLauncher);
        // Pin ch2 and ch3 gen=1 via the delete queue, in an actor context because that path logs, so only {ch3, gen=5} drains.
        runInActor([&](const NActors::TActorContext&) {
            bm->DeleteBlobOnComplete(NOlap::TTabletId(TabletId), MakeUnifiedBlob(MakeBlob(TabletId, 2, 1)));
            bm->DeleteBlobOnComplete(NOlap::TTabletId(TabletId), MakeUnifiedBlob(MakeBlob(TabletId, 2, 5)));
            bm->DeleteBlobOnComplete(NOlap::TTabletId(TabletId), MakeUnifiedBlob(MakeBlob(TabletId, Channel, 1)));
        });
        const TEntryKey middle{ Channel, 5 };

        bool nominated = false;
        runInActor([&](const NActors::TActorContext& ctx) {
            nominated = cutter.TryNominate(ctx);
        });
        UNIT_ASSERT(nominated);
        UNIT_ASSERT(cutter.GetCutStateForTest(middle) == ECutState::Cut);

        auto cutReq = runtime.GrabEdgeEvent<TEvTablet::TEvCutTabletHistory>(edgeLauncher);
        UNIT_ASSERT(cutReq);
        UNIT_ASSERT_VALUES_EQUAL(cutReq->Get()->Record.GetChannel(), Channel);
        UNIT_ASSERT_VALUES_EQUAL(cutReq->Get()->Record.GetFromGeneration(), 5u);
        UNIT_ASSERT_VALUES_EQUAL(cutReq->Get()->Record.GetGroupID(), GroupG);
    }

    // After the middle cut the G0 entry's window grows to [1, 9); a queued blob keeps it uncut.
    Y_UNIT_TEST(EarlierEntryStaysPinnedAfterMiddleCut) {
        TTestBasicRuntime runtime;
        TAppPrepare app;
        runtime.Initialize(app.Unwrap());
        runtime.GetAppData().ColumnShardConfig.SetCutHistoryMeasureOnly(false);
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();

        static constexpr ui64 TabletId = 4141;
        static constexpr ui32 Channel = 3;
        static constexpr ui32 GroupG0 = 100;
        static constexpr ui32 GroupG2 = 300;
        static constexpr ui32 CurrentGen = 10;

        const auto edgeTablet = runtime.AllocateEdgeActor();
        const auto runner = runtime.Register(new TRunnerActor());
        auto runInActor = [&](std::function<void(const NActors::TActorContext&)> fn) {
            runtime.Send(new IEventHandle(runner, edgeTablet, new TEvRunInActor(std::move(fn))));
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
        };

        // Hive already erased {5, G}, so the tablet boots with C3 = [1->G0, 9->G2]; C2 has only its active entry.
        auto [info, bm, shared] = MakeCutterEnv(TabletId, CurrentGen, /*nChannels=*/4, { { 1, GroupG0 }, { 9, GroupG2 } });
        info->Channels[2].History.erase(info->Channels[2].History.begin());
        CommitFirstGcRound(bm, CurrentGen, shared);
        TTestableHistoryCutter cutter(info, bm, shared, edgeTablet, TestSignals());
        const TEntryKey earlier{ Channel, 1 };

        // A blob in range pins the entry, queued in an actor context because that path logs; TryNominate must leave it uncut.
        runInActor([&](const NActors::TActorContext&) {
            bm->DeleteBlobOnComplete(NOlap::TTabletId(TabletId), MakeUnifiedBlob(MakeBlob(TabletId, Channel, 1)));
        });

        runInActor([&](const NActors::TActorContext& ctx) {
            cutter.TryNominate(ctx);
        });
        UNIT_ASSERT(cutter.GetCutStateForTest(earlier) == ECutState::None);
        UNIT_ASSERT(guard->GetCut().empty());
    }

    // A second RequestNomination while an event is already in flight does not send a second event.
    Y_UNIT_TEST(DeferredNominationDeduplicatedWhileEventInFlight) {
        TTestBasicRuntime runtime;
        TAppPrepare app;
        runtime.Initialize(app.Unwrap());
        runtime.GetAppData().ColumnShardConfig.SetCutHistoryMeasureOnly(false);
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();

        static constexpr ui64 TabletId = 9002;
        static constexpr ui32 CurrentGen = 5;
        const auto edgeTablet = runtime.AllocateEdgeActor();
        const auto runner = runtime.Register(new TRunnerActor());
        auto runInActor = [&](std::function<void(const NActors::TActorContext&)> fn) {
            runtime.Send(new IEventHandle(runner, edgeTablet, new TEvRunInActor(std::move(fn))));
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
        };

        auto [info, bm, shared] = MakeCutterEnv(TabletId, CurrentGen, /*nChannels=*/4, { { 0, 100 }, { 5, 200 } });
        ui32 nominateCount = 0;
        const auto nominationActor = runtime.Register(new TNominationCounter(nominateCount));
        TTestableHistoryCutter cutter(info, bm, shared, nominationActor, TestSignals());

        // First trigger: event queued, pending flag set.
        runInActor([&](const NActors::TActorContext&) {
            cutter.RequestNomination(true);
        });
        UNIT_ASSERT_VALUES_EQUAL_C(nominateCount, 1u, "one event after first request");
        UNIT_ASSERT_C(cutter.IsNominationPendingForTest(), "pending after first request");

        // Second trigger while event still in flight: no second event sent.
        runInActor([&](const NActors::TActorContext&) {
            cutter.RequestNomination(true);
        });
        UNIT_ASSERT_VALUES_EQUAL_C(nominateCount, 1u, "no second event while first is in flight");
        UNIT_ASSERT_C(cutter.IsNominationPendingForTest(), "still pending after second request");
        UNIT_ASSERT_C(cutter.IsNominationTriggeredForTest(), "still triggered after second request");
    }

    // After the in-flight event is consumed, the next RequestNomination sends a fresh event.
    Y_UNIT_TEST(DeferredNominationSendsNewEventAfterConsumption) {
        TTestBasicRuntime runtime;
        TAppPrepare app;
        runtime.Initialize(app.Unwrap());
        runtime.GetAppData().ColumnShardConfig.SetCutHistoryMeasureOnly(false);
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();

        static constexpr ui64 TabletId = 9003;
        static constexpr ui32 CurrentGen = 5;
        const auto edgeTablet = runtime.AllocateEdgeActor();
        const auto runner = runtime.Register(new TRunnerActor());
        auto runInActor = [&](std::function<void(const NActors::TActorContext&)> fn) {
            runtime.Send(new IEventHandle(runner, edgeTablet, new TEvRunInActor(std::move(fn))));
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
        };

        auto [info, bm, shared] = MakeCutterEnv(TabletId, CurrentGen, /*nChannels=*/4, { { 0, 100 }, { 5, 200 } });
        ui32 nominateCount = 0;
        const auto nominationActor = runtime.Register(new TNominationCounter(nominateCount));
        TTestableHistoryCutter cutter(info, bm, shared, nominationActor, TestSignals());

        // First trigger queues the event.
        runInActor([&](const NActors::TActorContext&) {
            cutter.RequestNomination(true);
        });
        UNIT_ASSERT_VALUES_EQUAL_C(nominateCount, 1u, "one event after first request");

        // Consume the event: clears NominationPending and runs TryNominate.
        runInActor([&](const NActors::TActorContext& ctx) {
            cutter.OnNominationEvent(ctx);
        });
        UNIT_ASSERT_C(!cutter.IsNominationPendingForTest(), "NominationPending must be cleared after event is consumed");

        // A new trigger must now send a fresh event.
        runInActor([&](const NActors::TActorContext&) {
            cutter.RequestNomination(true);
        });
        UNIT_ASSERT_VALUES_EQUAL_C(nominateCount, 2u, "new event queued after pending was cleared");
    }

}   // Y_UNIT_TEST_SUITE(TCutHistoryCutter)

}   // namespace NKikimr

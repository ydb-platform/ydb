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

TIntrusivePtr<TTabletStorageInfo> MakeTabletInfo(ui64 tabletId, ui32 nChannels, const TVector<std::pair<ui32, ui32>>& historySlots)
{
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
using ESeedState = NOlap::NBlobOperations::NBlobStorage::ESeedState;

// Only a live sensor instance is needed: values are asserted through the cutter's own state.
static const NColumnShard::THistoryCutterCounters& TestSignals() {
    static const NColumnShard::TBlobsManagerCounters counters("UT_BlobsManager");
    return counters.HistoryCutterCounters;
}

class TTestableHistoryCutter: public THistoryCutterWrapper {
public:
    using THistoryCutterWrapper::DecrementCounter;
    using THistoryCutterWrapper::GetCounterForTest;
    using THistoryCutterWrapper::GetCutStateForTest;
    using THistoryCutterWrapper::GetDisprovalAttemptsForTest;
    using THistoryCutterWrapper::GetPortionKeysCountForTest;
    using THistoryCutterWrapper::GetReseedEpochForTest;
    using THistoryCutterWrapper::GetSeedingStateForTest;
    using THistoryCutterWrapper::GetTombstoneCountForTest;
    using THistoryCutterWrapper::IsChannelPoisonedForTest;
    using THistoryCutterWrapper::IsDrained;
    using THistoryCutterWrapper::IsNominationPendingForTest;
    using THistoryCutterWrapper::IsNominationTriggeredForTest;
    using THistoryCutterWrapper::StartSweepForTest;
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
        : NActors::TActor<TRunnerActor>(&TRunnerActor::StateWork)
    {
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

class TCutRequestCounter: public NActors::TActor<TCutRequestCounter> {
public:
    explicit TCutRequestCounter(ui32& count)
        : NActors::TActor<TCutRequestCounter>(&TCutRequestCounter::StateWork)
        , Count(count)
    {
    }

    STFUNC(StateWork) {
        if (ev->GetTypeRewrite() == TEvTablet::TEvCutTabletHistory::EventType) {
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

Y_UNIT_TEST_SUITE(TCutHistoryCutterCounters) {
    // Channels 0-2, history {fromGen=0, group=100} and active {fromGen=5, group=200}; only ch >= 2 is tracked.

    Y_UNIT_TEST(DecrementToZeroOnPortionRemoved) {
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();
        static constexpr ui64 TabletId = 222;
        auto [info, bm, shared] = MakeCutterEnv(TabletId, 5);
        TTestableHistoryCutter cutter(info, 5, bm, shared, TActorId(), TestSignals());
        const TEntryKey key{ 2, 0 };

        THashMap<ui64, std::vector<NOlap::TUnifiedBlobId>> portionBlobs;
        portionBlobs[42].push_back(MakeUnifiedBlob(MakeBlob(TabletId, 2, 3)));
        cutter.OnBootComplete(portionBlobs);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(key), 1);

        cutter.OnPortionRemoved(42);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(key), 0);
        UNIT_ASSERT(!cutter.IsChannelPoisonedForTest(2));
    }

    Y_UNIT_TEST(ForeignBlobIgnored) {
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();
        static constexpr ui64 TabletId = 333;
        auto [info, bm, shared] = MakeCutterEnv(TabletId, 5);
        TTestableHistoryCutter cutter(info, 5, bm, shared, TActorId(), TestSignals());

        THashMap<ui64, std::vector<NOlap::TUnifiedBlobId>> portionBlobs;
        portionBlobs[55].push_back(MakeUnifiedBlob(MakeBlob(/*foreign*/ 999, 2, 3)));
        cutter.OnBootComplete(portionBlobs);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(TEntryKey{ 2, 0 }), 0);

        cutter.OnPortionRemoved(55);
        UNIT_ASSERT(!cutter.IsChannelPoisonedForTest(2));
    }

    Y_UNIT_TEST(ActiveEntryBlobIgnored) {
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();
        static constexpr ui64 TabletId = 444;
        static constexpr ui32 CurrentGen = 5;
        auto [info, bm, shared] = MakeCutterEnv(TabletId, CurrentGen, 3, { { 0, 100 }, { CurrentGen, 200 } });
        TTestableHistoryCutter cutter(info, CurrentGen, bm, shared, TActorId(), TestSignals());

        THashMap<ui64, std::vector<NOlap::TUnifiedBlobId>> portionBlobs;
        portionBlobs[77].push_back(MakeUnifiedBlob(MakeBlob(TabletId, 2, CurrentGen)));
        cutter.OnBootComplete(portionBlobs);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(TEntryKey{ 2, 0 }), 0);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(TEntryKey{ 2, CurrentGen }), 0);
    }

    Y_UNIT_TEST(BootCompleteWithEmptyMap) {
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();
        static constexpr ui64 TabletId = 555;
        auto [info, bm, shared] = MakeCutterEnv(TabletId, 3, 3, { { 0, 100 }, { 3, 200 } });
        TTestableHistoryCutter cutter(info, 3, bm, shared, TActorId(), TestSignals());

        cutter.OnBootComplete({});
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(TEntryKey{ 2, 0 }), 0);
        UNIT_ASSERT(!cutter.IsSweepInFlight());
        UNIT_ASSERT(cutter.GetSweepCandidates()->empty());
    }

    Y_UNIT_TEST(DoubleBlobPerPortionDeduplicates) {
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();
        static constexpr ui64 TabletId = 666;
        auto [info, bm, shared] = MakeCutterEnv(TabletId, 5);
        TTestableHistoryCutter cutter(info, 5, bm, shared, TActorId(), TestSignals());
        const TEntryKey key{ 2, 0 };

        THashMap<ui64, std::vector<NOlap::TUnifiedBlobId>> portionBlobs;
        portionBlobs[88].push_back(MakeUnifiedBlob(MakeBlob(TabletId, 2, 1, 1, 1)));
        portionBlobs[88].push_back(MakeUnifiedBlob(MakeBlob(TabletId, 2, 2, 1, 2)));
        cutter.OnBootComplete(portionBlobs);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(key), 1);

        cutter.OnPortionRemoved(88);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(key), 0);
        UNIT_ASSERT(!cutter.IsChannelPoisonedForTest(2));
    }

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

    // Disproved candidates return to None, and survivors failing the final re-check get no barrier.
    Y_UNIT_TEST(SweepDisprovalPath) {
        TActorSystemStub actorSystemStub;
        actorSystemStub.AppData.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        // 3 channels; history: {fromGen=0, group=100}, {fromGen=5, group=200}, {fromGen=10, group=300 (active)}.
        auto info = MakeTabletInfo(/*tabletId=*/777, /*nChannels=*/3, { { 0, 100 }, { 5, 200 }, { 10, 300 } });
        // An expired manager weak_ptr keeps IsDrained() false, so the re-check never reaches the cut.
        TTestableHistoryCutter cutter(info, /*currentGen=*/20, std::weak_ptr<NOlap::TBlobManager>(),
            std::weak_ptr<NOlap::NDataSharing::TStorageSharedBlobsManager>(), TActorId(), TestSignals());

        const TEntryKey keyA{ /*channel=*/2, /*fromGeneration=*/0 };
        const TEntryKey keyB{ /*channel=*/2, /*fromGeneration=*/5 };
        cutter.StartSweepForTest({ keyA, keyB });
        UNIT_ASSERT(cutter.IsSweepInFlight());
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetSweepCandidates()->size(), 2);

        const auto ctx = NActors::TActivationContext::AsActorContext();

        cutter.OnBatchComplete({ keyA }, /*exhausted=*/true, ctx);

        UNIT_ASSERT(!cutter.IsSweepInFlight());
        UNIT_ASSERT(cutter.GetCutStateForTest(keyA) == ECutState::None);
        // Survivor keyB failed the final re-check (IsDrained false) → also None, not cut.
        UNIT_ASSERT(cutter.GetCutStateForTest(keyB) == ECutState::None);
    }

    // Shared-out blobs must pin the entry: they are in no GC queue, so the drain gate must block the cut.
    Y_UNIT_TEST(SharedBlobsPinDrainGate) {
        TActorSystemStub actorSystemStub;
        actorSystemStub.AppData.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        static constexpr ui64 TabletId = 888;
        static constexpr ui64 BorrowerTabletId = 999;
        // History: {fromGen=0, group=100}, {fromGen=5, group=200 (active)}.
        auto [info, bm, shared] = MakeCutterEnv(TabletId, /*gen=*/5);

        TTestableHistoryCutter cutter(info, /*currentGen=*/5, bm, shared, TActorId(), TestSignals());
        const TEntryKey key{ /*channel=*/2, /*fromGeneration=*/0 };

        UNIT_ASSERT(cutter.IsDrained(key));

        // Share out one of OUR blobs living in the old range (channel 2, gen 1 < 5).
        const NOlap::TUnifiedBlobId sharedOut(/*dsGroup=*/100, TLogoBlobID(TabletId, /*gen=*/1, /*step=*/1, /*channel=*/2, 100, 1));
        UNIT_ASSERT(shared->UpsertSharedBlobOnLoad(sharedOut, NOlap::TTabletId(BorrowerTabletId)));
        UNIT_ASSERT_C(!cutter.IsDrained(key), "shared-out blob in the old range must pin the entry");
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
            return TTestableHistoryCutter(info, ActiveFromGen, bmOut, sharedOut, TActorId(), TestSignals());
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
        TTestableHistoryCutter cutter(info, ActiveFromGen, bm, shared, TActorId(), TestSignals());

        UNIT_ASSERT_C(cutter.IsDrained(key), "empty queues: old entry starts drained");

        // A foreign tablet's blob in our delete queue must not pin our entry.
        bm->DeleteBlobOnComplete(NOlap::TTabletId(ForeignTabletId), MakeUnifiedBlob(MakeBlob(ForeignTabletId, DataChannel, GenInOldRange)));
        UNIT_ASSERT_C(cutter.IsDrained(key), "a foreign blob in our delete queue must not pin our history entry");

        // Our own blob in the same range still does pin it.
        bm->DeleteBlobOnComplete(NOlap::TTabletId(OurTabletId), MakeUnifiedBlob(MakeBlob(OurTabletId, DataChannel, GenInOldRange)));
        UNIT_ASSERT_C(!cutter.IsDrained(key), "our own blob in the delete queue must pin the entry");
    }

    // Underflow poisons the channel; nomination then skips it though every other gate is open.
    Y_UNIT_TEST(UnderflowPoisonsChannelAndBlocksNomination) {
        TActorSystemStub actorSystemStub;
        actorSystemStub.AppData.ColumnShardConfig.SetCutHistoryMeasureOnly(false);
        actorSystemStub.AppData.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();
        static constexpr ui64 TabletId = 1010;
        static constexpr ui32 DataChannel = 2;
        static constexpr ui32 OldFromGen = 0;
        auto [info, bm, shared] = MakeCutterEnv(TabletId, /*gen=*/5, /*nChannels=*/3, { { OldFromGen, 100 }, { 5, 200 } });
        TTestableHistoryCutter cutter(info, /*currentGen=*/5, bm, shared, TActorId(), TestSignals());
        const TEntryKey key{ DataChannel, OldFromGen };

        // Positive control: without the poison every nomination gate is open.
        UNIT_ASSERT(cutter.IsDrained(key));
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(key), 0);
        UNIT_ASSERT(!cutter.IsChannelPoisonedForTest(DataChannel));

        // Decrement with no matching counter — the underflow branch.
        cutter.DecrementCounter(key);
        UNIT_ASSERT(cutter.IsChannelPoisonedForTest(DataChannel));

        // The poisoned channel yields no candidates, so TryNominate returns before any actor-system send.
        const auto ctx = NActors::TActivationContext::AsActorContext();
        UNIT_ASSERT(!cutter.TryNominate(ctx));
        UNIT_ASSERT(guard->GetNominated().empty());
        UNIT_ASSERT(!cutter.IsSweepInFlight());
    }

    // OnBootComplete must clear every piece of ephemeral state; each field is asserted.
    Y_UNIT_TEST(BootCompleteResetsEphemeralState) {
        TActorSystemStub actorSystemStub;
        actorSystemStub.AppData.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();
        static constexpr ui64 TabletId = 2020;
        static constexpr ui32 CurrentGen = 10;
        auto [info, bm, shared] = MakeCutterEnv(TabletId, CurrentGen, /*nChannels=*/4, { { 0, 100 }, { 5, 200 }, { CurrentGen, 300 } });
        TTestableHistoryCutter cutter(info, CurrentGen, bm, shared, TActorId(), TestSignals());
        const auto ctx = NActors::TActivationContext::AsActorContext();

        const TEntryKey keyOld{ /*channel=*/2, /*fromGeneration=*/0 };
        const TEntryKey keyMid{ /*channel=*/2, /*fromGeneration=*/5 };
        static constexpr ui32 PoisonChannel = 3;

        // Dirty everything reachable: counters, backoff, poison, and an in-flight sweep with a cursor.
        THashMap<ui64, std::vector<NOlap::TUnifiedBlobId>> oldPortions;
        oldPortions[1].push_back(MakeUnifiedBlob(MakeBlob(TabletId, 2, 1)));
        cutter.OnBootComplete(oldPortions);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(keyOld), 1);

        cutter.StartSweepForTest({ keyOld });
        cutter.OnBatchComplete({ keyOld }, /*exhausted=*/true, ctx);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetDisprovalAttemptsForTest(keyOld), 1);

        cutter.DecrementCounter(TEntryKey{ PoisonChannel, 0 });
        UNIT_ASSERT(cutter.IsChannelPoisonedForTest(PoisonChannel));

        cutter.StartSweepForTest({ keyMid });
        cutter.SetPortionSnapshot({ { NOlap::TInternalPathId::FromRawValue(1), 7 } });
        UNIT_ASSERT(cutter.IsSweepInFlight());
        UNIT_ASSERT(cutter.HasPortionSnapshot());
        UNIT_ASSERT(cutter.GetCutStateForTest(keyMid) == ECutState::Verifying);

        // Reboot with a different portion map: nothing from above may survive.
        THashMap<ui64, std::vector<NOlap::TUnifiedBlobId>> newPortions;
        newPortions[2].push_back(MakeUnifiedBlob(MakeBlob(TabletId, 2, 7, /*step=*/1, /*cookie=*/2)));
        cutter.OnBootComplete(newPortions);

        UNIT_ASSERT(!cutter.IsSweepInFlight());
        UNIT_ASSERT(cutter.GetSweepCandidates()->empty());
        UNIT_ASSERT(cutter.GetActiveSweepCandidates()->empty());
        UNIT_ASSERT(!cutter.HasPortionSnapshot());
        UNIT_ASSERT(cutter.GetCutStateForTest(keyMid) == ECutState::None);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetDisprovalAttemptsForTest(keyOld), 0);
        UNIT_ASSERT(!cutter.IsChannelPoisonedForTest(PoisonChannel));
        // Counters reflect only the new map: gen=7 falls in entry {2,5}, not {2,0}.
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(keyOld), 0);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(keyMid), 1);
    }

    // Backoff formula: 5m doubling per attempt, shift clamped at 12, capped at 6h.
    Y_UNIT_TEST(DisprovedCooldownFormula) {
        UNIT_ASSERT_VALUES_EQUAL(THistoryCutterWrapper::GetDisprovedCooldown(0), TDuration::Minutes(5));
        UNIT_ASSERT_VALUES_EQUAL(THistoryCutterWrapper::GetDisprovedCooldown(1), TDuration::Minutes(10));
        UNIT_ASSERT_VALUES_EQUAL(THistoryCutterWrapper::GetDisprovedCooldown(2), TDuration::Minutes(20));
        UNIT_ASSERT_VALUES_EQUAL(THistoryCutterWrapper::GetDisprovedCooldown(6), TDuration::Minutes(320));
        UNIT_ASSERT_VALUES_EQUAL(THistoryCutterWrapper::GetDisprovedCooldown(7), THistoryCutterWrapper::DisprovedRetryMaxCooldown);
        UNIT_ASSERT_VALUES_EQUAL(THistoryCutterWrapper::GetDisprovedCooldown(12), THistoryCutterWrapper::DisprovedRetryMaxCooldown);
        UNIT_ASSERT_VALUES_EQUAL(THistoryCutterWrapper::GetDisprovedCooldown(Max<ui32>()), THistoryCutterWrapper::DisprovedRetryMaxCooldown);
    }

    // Happy path: after the first GC round, a proven entry gets TEvCutTabletHistory.
    Y_UNIT_TEST(SweepHappyPathCutsAfterFirstGcRound) {
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
        TTestableHistoryCutter cutter(info, CurrentGen, bm, shared, edgeTablet, TestSignals());
        cutter.SetLauncherActorId(edgeLauncher);
        cutter.OnBootComplete({});
        const TEntryKey key{ DataChannel, OldFromGen };

        bool nominated = false;
        runInActor([&](const NActors::TActorContext& ctx) {
            nominated = cutter.TryNominate(ctx);
        });
        UNIT_ASSERT(nominated);
        UNIT_ASSERT(runtime.GrabEdgeEvent<NColumnShard::TEvPrivate::TEvStartCutHistorySweep>(edgeTablet));
        UNIT_ASSERT_VALUES_EQUAL(guard->GetNominated().size(), 1);
        UNIT_ASSERT(cutter.GetCutStateForTest(key) == ECutState::Verifying);

        // Clean exhausted sweep → direct cut, no barrier actor.
        cutter.SetPortionSnapshot({});
        runInActor([&](const NActors::TActorContext& ctx) {
            cutter.OnBatchComplete({}, /*exhausted=*/true, ctx);
        });
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
        TTestableHistoryCutter cutter(info, CurrentGen, bm, shared, edgeTablet, TestSignals());
        cutter.SetLauncherActorId(edgeLauncher);
        cutter.OnBootComplete({});
        const TEntryKey key{ DataChannel, OldFromGen };

        // First round: GC not committed → entry stays None after a clean sweep.
        runInActor([&](const NActors::TActorContext& ctx) {
            cutter.TryNominate(ctx);
        });
        UNIT_ASSERT(runtime.GrabEdgeEvent<NColumnShard::TEvPrivate::TEvStartCutHistorySweep>(edgeTablet));
        cutter.SetPortionSnapshot({});
        runInActor([&](const NActors::TActorContext& ctx) {
            cutter.OnBatchComplete({}, /*exhausted=*/true, ctx);
        });
        UNIT_ASSERT(cutter.GetCutStateForTest(key) == ECutState::None);
        UNIT_ASSERT(guard->GetCut().empty());

        // Commit the GC round; next nomination must now cut.
        CommitFirstGcRound(bm, CurrentGen, shared);
        runtime.AdvanceCurrentTime(THistoryCutterWrapper::DefaultNominateCadence + TDuration::Seconds(1));
        bool nominated = false;
        runInActor([&](const NActors::TActorContext& ctx) {
            nominated = cutter.TryNominate(ctx);
        });
        UNIT_ASSERT(nominated);
        UNIT_ASSERT(runtime.GrabEdgeEvent<NColumnShard::TEvPrivate::TEvStartCutHistorySweep>(edgeTablet));
        cutter.SetPortionSnapshot({});
        runInActor([&](const NActors::TActorContext& ctx) {
            cutter.OnBatchComplete({}, /*exhausted=*/true, ctx);
        });
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
        // History: {0,X},{5,200},{10,X},{20,Z(active)} — X appears at both fromGen=0 and fromGen=10.
        const TVector<std::pair<ui32, ui32>> hist{ { 0, GroupX }, { 5, 200u }, { 10, GroupX }, { 20, GroupZ } };

        const auto edgeTablet = runtime.AllocateEdgeActor();
        const auto edgeLauncher = runtime.AllocateEdgeActor();
        const auto runner = runtime.Register(new TRunnerActor());
        auto runInActor = [&](std::function<void(const NActors::TActorContext&)> fn) {
            runtime.Send(new IEventHandle(runner, edgeTablet, new TEvRunInActor(std::move(fn))));
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
        };

        auto [info, bm, shared] = MakeCutterEnv(TabletId, CurrentGen, /*nChannels=*/3, hist);
        CommitFirstGcRound(bm, CurrentGen, shared);
        TTestableHistoryCutter cutter(info, CurrentGen, bm, shared, edgeTablet, TestSignals());
        cutter.SetLauncherActorId(edgeLauncher);
        cutter.OnBootComplete({});

        const TEntryKey keyX0{ DataChannel, 0 };
        const TEntryKey keyX10{ DataChannel, 10 };

        // SeenGroupsCheckPasses requires the earlier same-group entry to be cut first.
        cutter.StartSweepForTest({ keyX0 });
        cutter.SetPortionSnapshot({});
        runInActor([&](const NActors::TActorContext& ctx) {
            cutter.OnBatchComplete({}, /*exhausted=*/true, ctx);
        });
        UNIT_ASSERT_C(cutter.GetCutStateForTest(keyX0) == ECutState::Cut, "earlier X entry must be cut");
        auto cutX0 = runtime.GrabEdgeEvent<TEvTablet::TEvCutTabletHistory>(edgeLauncher);
        UNIT_ASSERT(cutX0);
        UNIT_ASSERT_VALUES_EQUAL(cutX0->Get()->Record.GetFromGeneration(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(cutX0->Get()->Record.GetGroupID(), GroupX);

        // After the earlier entry is cut, the later {10,X} entry can also be cut.
        cutter.StartSweepForTest({ keyX10 });
        cutter.SetPortionSnapshot({});
        runInActor([&](const NActors::TActorContext& ctx) {
            cutter.OnBatchComplete({}, /*exhausted=*/true, ctx);
        });
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
        TTestableHistoryCutter cutter(info, CurrentGen, bm, shared, edgeTablet, TestSignals());
        cutter.SetLauncherActorId(edgeLauncher);
        cutter.OnBootComplete({});
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
        UNIT_ASSERT_C(cutter.IsDrained(key), "drain gate must open after GC commits");

        // Advance past the nomination cadence — the first TryNominate call set LastNominateAt.
        runtime.AdvanceCurrentTime(THistoryCutterWrapper::DefaultNominateCadence + TDuration::Seconds(1));
        bool nominated = false;
        runInActor([&](const NActors::TActorContext& ctx) {
            nominated = cutter.TryNominate(ctx);
        });
        UNIT_ASSERT(nominated);
        UNIT_ASSERT(runtime.GrabEdgeEvent<NColumnShard::TEvPrivate::TEvStartCutHistorySweep>(edgeTablet));
        cutter.SetPortionSnapshot({});
        runInActor([&](const NActors::TActorContext& ctx) {
            cutter.OnBatchComplete({}, /*exhausted=*/true, ctx);
        });
        UNIT_ASSERT(cutter.GetCutStateForTest(key) == ECutState::Cut);
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvTablet::TEvCutTabletHistory>(edgeLauncher));
    }

    // One Attempts increment per disproving sweep; cooldown gates renomination in mock time.
    Y_UNIT_TEST(DisprovalBackoffGatesRenomination) {
        TTestBasicRuntime runtime;
        TAppPrepare app;
        runtime.Initialize(app.Unwrap());
        runtime.GetAppData().ColumnShardConfig.SetCutHistoryMeasureOnly(false);
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();

        static constexpr ui64 TabletId = 4040;
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
        TTestableHistoryCutter cutter(info, CurrentGen, bm, shared, edgeTablet, TestSignals());
        cutter.SetLauncherActorId(edgeLauncher);
        cutter.OnBootComplete({});
        const TEntryKey key{ DataChannel, OldFromGen };

        auto tryNominate = [&](bool expected) {
            bool result = !expected;
            runInActor([&](const NActors::TActorContext& ctx) {
                result = cutter.TryNominate(ctx);
            });
            UNIT_ASSERT_VALUES_EQUAL(result, expected);
        };
        auto disproveSweep = [&]() {
            UNIT_ASSERT(runtime.GrabEdgeEvent<NColumnShard::TEvPrivate::TEvStartCutHistorySweep>(edgeTablet));
            cutter.SetPortionSnapshot({});
            runInActor([&](const NActors::TActorContext& ctx) {
                cutter.OnBatchComplete({ key }, /*exhausted=*/true, ctx);
            });
        };

        tryNominate(true);
        disproveSweep();
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetDisprovalAttemptsForTest(key), 1);

        // Attempts=1 after one disproval, so cooldown(1)=10m: 2m and 6m stay blocked, 11m passes.
        runtime.AdvanceCurrentTime(TDuration::Minutes(2));
        tryNominate(false);
        runtime.AdvanceCurrentTime(TDuration::Minutes(4));
        tryNominate(false);
        runtime.AdvanceCurrentTime(TDuration::Minutes(5));
        tryNominate(true);
        disproveSweep();
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetDisprovalAttemptsForTest(key), 2);

        // attempts=2 → 20m window: 12m blocked, 21m clears.
        runtime.AdvanceCurrentTime(TDuration::Minutes(12));
        tryNominate(false);
        runtime.AdvanceCurrentTime(TDuration::Minutes(9));
        tryNominate(true);

        // Nothing disproves the entry now: commit the GC round so the sweep cuts directly.
        CommitFirstGcRound(bm, CurrentGen, shared);
        UNIT_ASSERT(runtime.GrabEdgeEvent<NColumnShard::TEvPrivate::TEvStartCutHistorySweep>(edgeTablet));
        cutter.SetPortionSnapshot({});
        runInActor([&](const NActors::TActorContext& ctx) {
            cutter.OnBatchComplete({}, /*exhausted=*/true, ctx);
        });
        UNIT_ASSERT(cutter.GetCutStateForTest(key) == ECutState::Cut);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetDisprovalAttemptsForTest(key), 0);
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvTablet::TEvCutTabletHistory>(edgeLauncher));
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
        TTestableHistoryCutter cutter(info, CurrentGen, bm, shared, TActorId(), TestSignals());
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

    // A live portion in the range blocks nomination; cleanup's erase opens the gate.
    Y_UNIT_TEST(LivePortionBlocksNomination) {
        TActorSystemStub actorSystemStub;
        actorSystemStub.AppData.ColumnShardConfig.SetCutHistoryMeasureOnly(false);
        actorSystemStub.AppData.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();
        static constexpr ui64 TabletId = 3131;
        static constexpr ui32 DataChannel = 2;
        static constexpr ui32 OldFromGen = 0;
        static constexpr ui32 CurrentGen = 5;
        static constexpr ui64 PortionId = 77;

        auto [info, bm, shared] = MakeCutterEnv(TabletId, CurrentGen, /*nChannels=*/3, { { OldFromGen, 100 }, { CurrentGen, 200 } });
        TTestableHistoryCutter cutter(info, CurrentGen, bm, shared, TActorId(), TestSignals());
        const TEntryKey key{ DataChannel, OldFromGen };
        const auto ctx = NActors::TActivationContext::AsActorContext();

        THashMap<ui64, std::vector<NOlap::TUnifiedBlobId>> portionBlobs;
        portionBlobs[PortionId].push_back(MakeUnifiedBlob(MakeBlob(TabletId, DataChannel, OldFromGen + 3)));
        cutter.OnBootComplete(portionBlobs);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(key), 1);
        UNIT_ASSERT(cutter.IsDrained(key));

        // Drained but not empty: the counter alone must hold nomination back.
        UNIT_ASSERT_C(!cutter.TryNominate(ctx), "a live portion in the range must block nomination");
        UNIT_ASSERT(guard->GetNominated().empty());

        cutter.OnPortionRemoved(PortionId);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(key), 0);
        UNIT_ASSERT(!cutter.IsChannelPoisonedForTest(DataChannel));
    }

    // A round blocked only by an in-flight GC task sets no disproval cooldown, so the next round can nominate.
    Y_UNIT_TEST(GcBlockedRoundDoesNotStartBackoff) {
        TActorSystemStub actorSystemStub;
        actorSystemStub.AppData.ColumnShardConfig.SetCutHistoryMeasureOnly(false);
        actorSystemStub.AppData.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();
        static constexpr ui64 TabletId = 3232;
        static constexpr ui32 CurrentGen = 5;
        auto [info, bm, shared] = MakeCutterEnv(TabletId, CurrentGen, /*nChannels=*/3, { { 0, 100 }, { CurrentGen, 200 } });
        TTestableHistoryCutter cutter(info, CurrentGen, bm, shared, TActorId(), TestSignals());
        const TEntryKey key{ 2, 0 };
        const auto ctx = NActors::TActivationContext::AsActorContext();

        auto storageCounters = std::make_shared<NOlap::NBlobOperations::TStorageCounters>(NOlap::NBlobOperations::TGlobal::DefaultStorageId);
        auto gcCounters =
            std::make_shared<NOlap::NBlobOperations::TRemoveGCCounters>(NOlap::NBlobOperations::TConsumerCounters("GC", *storageCounters));
        auto task = bm->BuildGCTask(NOlap::NBlobOperations::TGlobal::DefaultStorageId, bm, shared, gcCounters);
        UNIT_ASSERT_C(task, "first GC round carries the barrier even with empty queues");
        UNIT_ASSERT_C(!cutter.IsDrained(key), "GC task in flight pins every entry");

        cutter.StartSweepForTest({ key });
        cutter.SetPortionSnapshot({});
        cutter.OnBatchComplete({}, /*exhausted=*/true, ctx);

        UNIT_ASSERT_C(!cutter.IsSweepInFlight(), "the blocked round must finish");
        UNIT_ASSERT(cutter.GetCutStateForTest(key) == ECutState::None);
        UNIT_ASSERT_VALUES_EQUAL_C(cutter.GetDisprovalAttemptsForTest(key), 0, "GC-blocked round must not start the backoff");
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
        TTestableHistoryCutter cutter(info, CurrentGen, bm, shared, edgeTablet, TestSignals());
        cutter.SetLauncherActorId(edgeLauncher);
        cutter.OnBootComplete({});
        const TEntryKey middle{ Channel, 5 };

        cutter.StartSweepForTest({ middle });
        cutter.SetPortionSnapshot({});
        runInActor([&](const NActors::TActorContext& ctx) {
            cutter.OnBatchComplete({}, /*exhausted=*/true, ctx);
        });
        UNIT_ASSERT(cutter.GetCutStateForTest(middle) == ECutState::Cut);

        auto cutReq = runtime.GrabEdgeEvent<TEvTablet::TEvCutTabletHistory>(edgeLauncher);
        UNIT_ASSERT(cutReq);
        UNIT_ASSERT_VALUES_EQUAL(cutReq->Get()->Record.GetChannel(), Channel);
        UNIT_ASSERT_VALUES_EQUAL(cutReq->Get()->Record.GetFromGeneration(), 5u);
        UNIT_ASSERT_VALUES_EQUAL(cutReq->Get()->Record.GetGroupID(), GroupG);
    }

    // After the middle cut the G0 entry's window grows to [1, 9); its live portion keeps it uncut.
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
        static constexpr ui64 PortionP6 = 6;

        const auto edgeTablet = runtime.AllocateEdgeActor();
        const auto runner = runtime.Register(new TRunnerActor());
        auto runInActor = [&](std::function<void(const NActors::TActorContext&)> fn) {
            runtime.Send(new IEventHandle(runner, edgeTablet, new TEvRunInActor(std::move(fn))));
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
        };

        // Hive already erased {5, G}, so the tablet boots with C3 = [1->G0, 9->G2]; C2 has only its active entry.
        auto [info, bm, shared] = MakeCutterEnv(TabletId, CurrentGen, /*nChannels=*/4, { { 1, GroupG0 }, { 9, GroupG2 } });
        info->Channels[2].History.erase(info->Channels[2].History.begin());
        TTestableHistoryCutter cutter(info, CurrentGen, bm, shared, edgeTablet, TestSignals());
        const TEntryKey earlier{ Channel, 1 };

        THashMap<ui64, std::vector<NOlap::TUnifiedBlobId>> portionBlobs;
        portionBlobs[PortionP6].push_back(MakeUnifiedBlob(MakeBlob(TabletId, Channel, 1)));
        portionBlobs[PortionP6].push_back(MakeUnifiedBlob(MakeBlob(TabletId, Channel, 2)));
        cutter.OnBootComplete(portionBlobs);
        // The counter counts live portions, not blobs: P6 with both g1 and g2 is one.
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(earlier), 1);

        // Inject the entry into the sweep directly to exercise the final re-check, then complete the sweep.
        cutter.StartSweepForTest({ earlier });
        cutter.SetPortionSnapshot({});
        runInActor([&](const NActors::TActorContext& ctx) {
            cutter.OnBatchComplete({}, /*exhausted=*/true, ctx);
        });
        UNIT_ASSERT(cutter.GetCutStateForTest(earlier) == ECutState::None);
        UNIT_ASSERT(guard->GetCut().empty());
    }

    // BeginSeeding increments ReseedEpoch and resets state; FinishSeeding transitions to Seeded.
    Y_UNIT_TEST(SeedingStateMachineTransitions) {
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();
        static constexpr ui64 TabletId = 5050;
        auto [info, bm, shared] = MakeCutterEnv(TabletId, 5);
        TTestableHistoryCutter cutter(info, 5, bm, shared, TActorId(), TestSignals());

        UNIT_ASSERT(cutter.GetSeedingStateForTest() == ESeedState::Unseeded);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetReseedEpochForTest(), 0u);

        cutter.BeginSeeding();
        UNIT_ASSERT(cutter.GetSeedingStateForTest() == ESeedState::Seeding);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetReseedEpochForTest(), 1u);

        cutter.FinishSeeding();
        UNIT_ASSERT(cutter.GetSeedingStateForTest() == ESeedState::Seeded);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetReseedEpochForTest(), 1u);

        // Second seeding round increments the epoch again.
        cutter.BeginSeeding();
        UNIT_ASSERT(cutter.GetSeedingStateForTest() == ESeedState::Seeding);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetReseedEpochForTest(), 2u);
        cutter.FinishSeeding();
        UNIT_ASSERT(cutter.GetSeedingStateForTest() == ESeedState::Seeded);
    }

    // TryNominate returns false while Unseeded or Seeding; returns true after Seeded.
    Y_UNIT_TEST(TryNominateGatedBySeededState) {
        TTestBasicRuntime runtime;
        TAppPrepare app;
        runtime.Initialize(app.Unwrap());
        runtime.GetAppData().ColumnShardConfig.SetCutHistoryMeasureOnly(false);
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();
        static constexpr ui64 TabletId = 5151;
        static constexpr ui32 CurrentGen = 5;

        const auto edgeTablet = runtime.AllocateEdgeActor();
        const auto runner = runtime.Register(new TRunnerActor());
        auto runInActor = [&](std::function<void(const NActors::TActorContext&)> fn) {
            runtime.Send(new IEventHandle(runner, edgeTablet, new TEvRunInActor(std::move(fn))));
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
        };

        auto [info, bm, shared] = MakeCutterEnv(TabletId, CurrentGen, /*nChannels=*/3, { { 0, 100 }, { CurrentGen, 200 } });
        TTestableHistoryCutter cutter(info, CurrentGen, bm, shared, edgeTablet, TestSignals());

        bool result = false;
        runInActor([&](const NActors::TActorContext& ctx) {
            result = cutter.TryNominate(ctx);
        });
        UNIT_ASSERT_C(!result, "Unseeded state must block nomination");
        UNIT_ASSERT(guard->GetNominated().empty());

        cutter.BeginSeeding();
        runInActor([&](const NActors::TActorContext& ctx) {
            result = cutter.TryNominate(ctx);
        });
        UNIT_ASSERT_C(!result, "Seeding state must block nomination");

        cutter.FinishSeeding();
        runInActor([&](const NActors::TActorContext& ctx) {
            result = cutter.TryNominate(ctx);
        });
        UNIT_ASSERT_C(result, "Seeded state must allow nomination");
        UNIT_ASSERT(runtime.GrabEdgeEvent<NColumnShard::TEvPrivate::TEvStartCutHistorySweep>(edgeTablet));
    }

    // OnBootComplete = BeginSeeding + ApplySeedBatch + FinishSeeding; portions seeded into counters are correct.
    Y_UNIT_TEST(OnBootCompleteSeeds) {
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();
        static constexpr ui64 TabletId = 5252;
        auto [info, bm, shared] = MakeCutterEnv(TabletId, 5);
        TTestableHistoryCutter cutter(info, 5, bm, shared, TActorId(), TestSignals());

        THashMap<ui64, std::vector<NOlap::TUnifiedBlobId>> portionBlobs;
        portionBlobs[10].push_back(MakeUnifiedBlob(MakeBlob(TabletId, 2, 3)));
        cutter.OnBootComplete(portionBlobs);

        UNIT_ASSERT(cutter.GetSeedingStateForTest() == ESeedState::Seeded);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetReseedEpochForTest(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetPortionKeysCountForTest(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(TEntryKey{ 2, 0 }), 1u);
    }

    // Portions removed during seeding are tombstoned; FinishSeeding erases them from PortionKeys.
    Y_UNIT_TEST(TombstonedPortionIsNotSeeded) {
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();
        static constexpr ui64 TabletId = 5353;
        auto [info, bm, shared] = MakeCutterEnv(TabletId, 5);
        TTestableHistoryCutter cutter(info, 5, bm, shared, TActorId(), TestSignals());

        cutter.BeginSeeding();
        UNIT_ASSERT(cutter.GetSeedingStateForTest() == ESeedState::Seeding);

        // A portion erased while seeding runs, before the scan batch that still lists it.
        cutter.OnPortionRemoved(99);
        UNIT_ASSERT_VALUES_EQUAL_C(cutter.GetTombstoneCountForTest(), 1u, "removed portion must be tombstoned during Seeding");

        // The batch was read before the erase and still lists the portion.
        THashMap<ui64, std::vector<NOlap::TUnifiedBlobId>> batch;
        batch[99].push_back(MakeUnifiedBlob(MakeBlob(TabletId, 2, 3)));
        cutter.ApplySeedBatch(batch);
        UNIT_ASSERT_VALUES_EQUAL_C(cutter.GetPortionKeysCountForTest(), 0u, "an erased portion must not be seeded");
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(TEntryKey{ 2, 0 }), 0u);

        cutter.FinishSeeding();
        UNIT_ASSERT(cutter.GetSeedingStateForTest() == ESeedState::Seeded);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetPortionKeysCountForTest(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(TEntryKey{ 2, 0 }), 0u);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetTombstoneCountForTest(), 0u);
    }

    // FailSeeding puts the cutter in Failed state and blocks nominations; positive control: re-seed succeeds.
    Y_UNIT_TEST(FailSeedingTerminatesWithFailedState) {
        TTestBasicRuntime runtime;
        TAppPrepare app;
        runtime.Initialize(app.Unwrap());
        runtime.GetAppData().ColumnShardConfig.SetCutHistoryMeasureOnly(false);
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();
        static constexpr ui64 TabletId = 6060;
        static constexpr ui32 OldFromGen = 0;
        static constexpr ui32 ActiveFromGen = 5;

        const auto edgeTablet = runtime.AllocateEdgeActor();
        const auto edgeLauncher = runtime.AllocateEdgeActor();
        const auto runner = runtime.Register(new TRunnerActor());
        auto runInActor = [&](std::function<void(const NActors::TActorContext&)> fn) {
            runtime.Send(new IEventHandle(runner, edgeTablet, new TEvRunInActor(std::move(fn))));
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
        };

        auto [info, bm, shared] = MakeCutterEnv(TabletId, ActiveFromGen, /*nChannels=*/3, { { OldFromGen, 100 }, { ActiveFromGen, 200 } });
        CommitFirstGcRound(bm, ActiveFromGen, shared);
        TTestableHistoryCutter cutter(info, ActiveFromGen, bm, shared, edgeTablet, TestSignals());
        cutter.SetLauncherActorId(edgeLauncher);

        cutter.BeginSeeding();
        UNIT_ASSERT(cutter.GetSeedingStateForTest() == ESeedState::Seeding);

        cutter.FailSeeding(NOlap::TInternalPathId{}, 99, "injected test error");
        UNIT_ASSERT(cutter.GetSeedingStateForTest() == ESeedState::Failed);

        // In Failed state TryNominate must refuse.
        bool nominated = false;
        runInActor([&](const NActors::TActorContext& ctx) {
            nominated = cutter.TryNominate(ctx);
        });
        UNIT_ASSERT_C(!nominated, "TryNominate must return false when seeding Failed");
        UNIT_ASSERT_C(guard->GetNominated().empty(), "no nomination must fire in Failed state");

        // Positive control: after a new seeding run the cutter becomes Seeded and nominations work.
        cutter.BeginSeeding();
        cutter.FinishSeeding();
        UNIT_ASSERT(cutter.GetSeedingStateForTest() == ESeedState::Seeded);
        runInActor([&](const NActors::TActorContext& ctx) {
            nominated = cutter.TryNominate(ctx);
        });
        UNIT_ASSERT_C(nominated, "TryNominate must succeed after re-seeding to Seeded");
        UNIT_ASSERT_C(!guard->GetNominated().empty(), "nomination must fire after successful re-seed");
    }

    // OnBatchComplete aborts the cut when ReseedEpoch changes between nomination and completion.
    Y_UNIT_TEST(StaleNominationAbortedOnReseed) {
        TActorSystemStub actorSystemStub;
        actorSystemStub.AppData.ColumnShardConfig.SetCutHistoryMeasureOnly(false);
        actorSystemStub.AppData.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();
        static constexpr ui64 TabletId = 5454;
        auto [info, bm, shared] = MakeCutterEnv(TabletId, 5, /*nChannels=*/3, { { 0, 100 }, { 5, 200 } });
        CommitFirstGcRound(bm, 5, shared);
        TTestableHistoryCutter cutter(info, 5, bm, shared, TActorId(), TestSignals());
        const TEntryKey key{ 2, 0 };
        const auto ctx = NActors::TActivationContext::AsActorContext();

        cutter.OnBootComplete({});
        cutter.StartSweepForTest({ key });
        const ui64 epochAtNominate = cutter.GetReseedEpochForTest();

        // A re-seed happens while the sweep is in flight: epoch advances.
        cutter.BeginSeeding();
        cutter.FinishSeeding();
        UNIT_ASSERT_C(cutter.GetReseedEpochForTest() > epochAtNominate, "re-seed must advance the epoch");

        // The in-flight sweep completing now must NOT produce a cut.
        cutter.SetPortionSnapshot({});
        cutter.OnBatchComplete({}, /*exhausted=*/true, ctx);
        UNIT_ASSERT_C(cutter.GetCutStateForTest(key) == ECutState::None, "stale nomination must not cut after a re-seed");
        UNIT_ASSERT_C(guard->GetCut().empty(), "no TEvCutTabletHistory for a stale nomination");
    }

    // Counter reaching zero while Seeded sends TEvCutHistoryNominate, not an inline TryNominate.
    Y_UNIT_TEST(DeferredNominationOnCounterZero) {
        TTestBasicRuntime runtime;
        TAppPrepare app;
        runtime.Initialize(app.Unwrap());
        runtime.GetAppData().ColumnShardConfig.SetCutHistoryMeasureOnly(false);
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();

        static constexpr ui64 TabletId = 9001;
        static constexpr ui32 CurrentGen = 5;
        const auto edgeTablet = runtime.AllocateEdgeActor();
        const auto runner = runtime.Register(new TRunnerActor());
        auto runInActor = [&](std::function<void(const NActors::TActorContext&)> fn) {
            runtime.Send(new IEventHandle(runner, edgeTablet, new TEvRunInActor(std::move(fn))));
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
        };

        auto [info, bm, shared] = MakeCutterEnv(TabletId, CurrentGen);
        // Use a counter actor as TabletActorId; edge-actor observers are unreliable for event counting.
        ui32 nominateCount = 0;
        const auto nominationActor = runtime.Register(new TNominationCounter(nominateCount));
        TTestableHistoryCutter cutter(info, CurrentGen, bm, shared, nominationActor, TestSignals());
        const TEntryKey key{ 2, 0 };

        THashMap<ui64, std::vector<NOlap::TUnifiedBlobId>> portionBlobs;
        portionBlobs[42].push_back(MakeUnifiedBlob(MakeBlob(TabletId, 2, 3)));
        // OnBootComplete runs outside an actor context; FinishSeeding sets NominationPending without sending.
        cutter.OnBootComplete(portionBlobs);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(key), 1u);
        // Consume the boot nomination so NominationPending is clear before the portion removal.
        runInActor([&](const NActors::TActorContext& ctx) {
            cutter.OnNominationEvent(ctx);
        });
        UNIT_ASSERT_VALUES_EQUAL_C(nominateCount, 0u, "boot nomination not sent (no ctx); count must stay zero");

        runInActor([&](const NActors::TActorContext&) {
            cutter.OnPortionRemoved(42);
        });

        UNIT_ASSERT_VALUES_EQUAL_C(nominateCount, 1u, "exactly one TEvCutHistoryNominate after counter reaches zero");
        UNIT_ASSERT_C(cutter.IsNominationPendingForTest(), "NominationPending must be true while event is in flight");
        UNIT_ASSERT_C(cutter.IsNominationTriggeredForTest(), "a counter-triggered nomination must be marked triggered");
        // TryNominate must not have run inline.
        UNIT_ASSERT_C(guard->GetNominated().empty(), "sweep must not start until the deferred event is processed");
    }

    // A second counter reaching zero while an event is already in flight does not send a second event.
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

        // Four channels so portions on channels 2 and 3 each carry their own counter.
        auto [info, bm, shared] = MakeCutterEnv(TabletId, CurrentGen, /*nChannels=*/4, { { 0, 100 }, { 5, 200 } });
        ui32 nominateCount = 0;
        const auto nominationActor = runtime.Register(new TNominationCounter(nominateCount));
        TTestableHistoryCutter cutter(info, CurrentGen, bm, shared, nominationActor, TestSignals());

        THashMap<ui64, std::vector<NOlap::TUnifiedBlobId>> portionBlobs;
        portionBlobs[42].push_back(MakeUnifiedBlob(MakeBlob(TabletId, 2, 3)));
        portionBlobs[43].push_back(MakeUnifiedBlob(MakeBlob(TabletId, 3, 3)));
        // Consume the boot nomination so NominationPending is clear before the first removal.
        cutter.OnBootComplete(portionBlobs);
        runInActor([&](const NActors::TActorContext& ctx) {
            cutter.OnNominationEvent(ctx);
        });

        // First zero: one event queued, pending flag set.
        runInActor([&](const NActors::TActorContext&) {
            cutter.OnPortionRemoved(42);
        });
        UNIT_ASSERT_VALUES_EQUAL_C(nominateCount, 1u, "one event after first zero");
        UNIT_ASSERT_C(cutter.IsNominationPendingForTest(), "pending after first zero");

        // Second zero while event still in flight: no second event, flag stays set and triggered.
        runInActor([&](const NActors::TActorContext&) {
            cutter.OnPortionRemoved(43);
        });
        UNIT_ASSERT_VALUES_EQUAL_C(nominateCount, 1u, "no second event while first is in flight");
        UNIT_ASSERT_C(cutter.IsNominationPendingForTest(), "still pending after second zero");
        UNIT_ASSERT_C(cutter.IsNominationTriggeredForTest(), "still triggered after second zero");
    }

    // After the in-flight event is consumed, the next counter reaching zero sends a fresh event.
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
        TTestableHistoryCutter cutter(info, CurrentGen, bm, shared, nominationActor, TestSignals());

        THashMap<ui64, std::vector<NOlap::TUnifiedBlobId>> portionBlobs;
        portionBlobs[42].push_back(MakeUnifiedBlob(MakeBlob(TabletId, 2, 3)));
        portionBlobs[43].push_back(MakeUnifiedBlob(MakeBlob(TabletId, 3, 3)));
        // Consume the boot nomination so NominationPending is clear before the first removal.
        cutter.OnBootComplete(portionBlobs);
        runInActor([&](const NActors::TActorContext& ctx) {
            cutter.OnNominationEvent(ctx);
        });

        runInActor([&](const NActors::TActorContext&) {
            cutter.OnPortionRemoved(42);
        });
        UNIT_ASSERT_VALUES_EQUAL_C(nominateCount, 1u, "one event after first zero");

        // Consume the event: clears NominationPending and runs TryNominate.
        runInActor([&](const NActors::TActorContext& ctx) {
            cutter.OnNominationEvent(ctx);
        });
        UNIT_ASSERT_C(!cutter.IsNominationPendingForTest(), "NominationPending must be cleared after event is consumed");

        // A new zero must now send a fresh event.
        runInActor([&](const NActors::TActorContext&) {
            cutter.OnPortionRemoved(43);
        });
        UNIT_ASSERT_VALUES_EQUAL_C(nominateCount, 2u, "new event queued after pending was cleared");
    }

    // Completing the seeding transition inside an actor context sends one nomination event.
    Y_UNIT_TEST(DeferredNominationOnSeedingComplete) {
        TTestBasicRuntime runtime;
        TAppPrepare app;
        runtime.Initialize(app.Unwrap());
        runtime.GetAppData().ColumnShardConfig.SetCutHistoryMeasureOnly(false);
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();

        static constexpr ui64 TabletId = 9004;
        static constexpr ui32 CurrentGen = 5;
        const auto edgeTablet = runtime.AllocateEdgeActor();
        const auto runner = runtime.Register(new TRunnerActor());
        auto runInActor = [&](std::function<void(const NActors::TActorContext&)> fn) {
            runtime.Send(new IEventHandle(runner, edgeTablet, new TEvRunInActor(std::move(fn))));
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
        };

        auto [info, bm, shared] = MakeCutterEnv(TabletId, CurrentGen);
        ui32 nominateCount = 0;
        const auto nominationActor = runtime.Register(new TNominationCounter(nominateCount));
        TTestableHistoryCutter cutter(info, CurrentGen, bm, shared, nominationActor, TestSignals());

        cutter.BeginSeeding();
        UNIT_ASSERT(cutter.GetSeedingStateForTest() == ESeedState::Seeding);

        runInActor([&](const NActors::TActorContext&) {
            cutter.FinishSeeding();
        });
        UNIT_ASSERT_VALUES_EQUAL_C(nominateCount, 1u, "FinishSeeding must request exactly one nomination");
        UNIT_ASSERT_C(cutter.IsNominationPendingForTest(), "nomination must be pending after seeding completes");
        UNIT_ASSERT_C(cutter.IsNominationTriggeredForTest(), "seeding-triggered nomination must be marked triggered");
    }

    // While seeding (non-Seeded state), a counter reaching zero must not request a nomination.
    Y_UNIT_TEST(DeferredNominationGatedBySeededState) {
        TTestBasicRuntime runtime;
        TAppPrepare app;
        runtime.Initialize(app.Unwrap());
        runtime.GetAppData().ColumnShardConfig.SetCutHistoryMeasureOnly(false);
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();

        static constexpr ui64 TabletId = 9005;
        static constexpr ui32 CurrentGen = 5;
        const auto edgeTablet = runtime.AllocateEdgeActor();
        const auto runner = runtime.Register(new TRunnerActor());
        auto runInActor = [&](std::function<void(const NActors::TActorContext&)> fn) {
            runtime.Send(new IEventHandle(runner, edgeTablet, new TEvRunInActor(std::move(fn))));
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
        };

        auto [info, bm, shared] = MakeCutterEnv(TabletId, CurrentGen);
        ui32 nominateCount = 0;
        const auto nominationActor = runtime.Register(new TNominationCounter(nominateCount));
        TTestableHistoryCutter cutter(info, CurrentGen, bm, shared, nominationActor, TestSignals());

        // Put one counter into the cutter while it is in Seeding state.
        cutter.BeginSeeding();
        THashMap<ui64, std::vector<NOlap::TUnifiedBlobId>> portionBlobs;
        portionBlobs[42].push_back(MakeUnifiedBlob(MakeBlob(TabletId, 2, 3)));
        cutter.ApplySeedBatch(portionBlobs);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(TEntryKey{ 2, 0 }), 1u);

        // Removing the portion in Seeding state must not request a nomination.
        runInActor([&](const NActors::TActorContext&) {
            cutter.OnPortionRemoved(42);
        });
        UNIT_ASSERT_VALUES_EQUAL_C(nominateCount, 0u, "no nomination event while in Seeding state");
        UNIT_ASSERT_C(!cutter.IsNominationPendingForTest(), "NominationPending must stay false in Seeding state");

        // Positive control: completing the seeding triggers exactly one nomination.
        runInActor([&](const NActors::TActorContext&) {
            cutter.FinishSeeding();
        });
        UNIT_ASSERT_VALUES_EQUAL_C(nominateCount, 1u, "FinishSeeding must request nomination once Seeded");
    }

}   // TCutHistoryCutterCounters

}   // namespace NKikimr

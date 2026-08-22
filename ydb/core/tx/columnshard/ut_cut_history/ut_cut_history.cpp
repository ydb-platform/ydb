// Unit tests for the CutHistory (KIKIMR-26208) two-tier nomination engine.
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

// ---- helpers ----------------------------------------------------------------

// Build a TTabletStorageInfo with N channels, each having the given history.
// historySlots: vector of (fromGeneration, groupId) pairs — first is oldest.
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

// Test controller that enables CutHistory and counts nominated/cut events.
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

// The suite only needs a live sensor instance to write into; values are asserted
// through the cutter's own state, not through monitoring.
static const NColumnShard::THistoryCutterCounters& TestSignals() {
    static const NColumnShard::TBlobsManagerCounters counters("UT_BlobsManager");
    return counters.HistoryCutterCounters;
}

// Exposes the protected sweep test hooks to this suite only.
class TTestableHistoryCutter: public THistoryCutterWrapper {
public:
    using THistoryCutterWrapper::DecrementCounter;
    using THistoryCutterWrapper::GetCounterForTest;
    using THistoryCutterWrapper::GetCutStateForTest;
    using THistoryCutterWrapper::GetDisprovalAttemptsForTest;
    using THistoryCutterWrapper::IsChannelPoisonedForTest;
    using THistoryCutterWrapper::IsDrained;
    using THistoryCutterWrapper::StartSweepForTest;
    using THistoryCutterWrapper::THistoryCutterWrapper;
};

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

// ---- tests ------------------------------------------------------------------

Y_UNIT_TEST_SUITE(TCutHistoryCutterCounters) {
    /*
     * With 3 channels (0, 1, 2) and two history entries per channel:
     *   History[0]: fromGen=0, group=100
     *   History[1]: fromGen=5, group=200  <-- active
     *
     * Generation=5 => blobs written at gen=0..4 are in history[0], gen>=5 in history[1].
     * Only data channels (ch >= 2) are tracked.
     */

    Y_UNIT_TEST(IncrementOnPortionAdded) {
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();
        static constexpr ui64 TabletId = 111;
        auto info = MakeTabletInfo(TabletId, 3, { { 0, 100 }, { 5, 200 } });
        auto bm = std::make_shared<NOlap::TBlobManager>(info, 5, NOlap::TTabletId(TabletId));
        auto shared = std::make_shared<NOlap::NDataSharing::TStorageSharedBlobsManager>(
            NOlap::NBlobOperations::TGlobal::DefaultStorageId, NOlap::TTabletId(TabletId));
        TTestableHistoryCutter cutter(info, 5, bm, shared, TActorId(), TestSignals());

        // TPortionDataAccessor is not constructible here; OnBootComplete drives the
        // same IncrementCounter path.
        THashMap<ui64, std::vector<NOlap::TUnifiedBlobId>> portionBlobs;
        portionBlobs[42].push_back(MakeUnifiedBlob(MakeBlob(TabletId, 2, 3)));
        cutter.OnBootComplete(portionBlobs);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(TEntryKey{ 2, 0 }), 1);
    }

    Y_UNIT_TEST(DecrementToZeroOnPortionRemoved) {
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();
        static constexpr ui64 TabletId = 222;
        auto info = MakeTabletInfo(TabletId, 3, { { 0, 100 }, { 5, 200 } });
        auto bm = std::make_shared<NOlap::TBlobManager>(info, 5, NOlap::TTabletId(TabletId));
        auto shared = std::make_shared<NOlap::NDataSharing::TStorageSharedBlobsManager>(
            NOlap::NBlobOperations::TGlobal::DefaultStorageId, NOlap::TTabletId(TabletId));
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
        auto info = MakeTabletInfo(TabletId, 3, { { 0, 100 }, { 5, 200 } });
        auto bm = std::make_shared<NOlap::TBlobManager>(info, 5, NOlap::TTabletId(TabletId));
        auto shared = std::make_shared<NOlap::NDataSharing::TStorageSharedBlobsManager>(
            NOlap::NBlobOperations::TGlobal::DefaultStorageId, NOlap::TTabletId(TabletId));
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
        auto info = MakeTabletInfo(TabletId, 3, { { 0, 100 }, { CurrentGen, 200 } });
        auto bm = std::make_shared<NOlap::TBlobManager>(info, CurrentGen, NOlap::TTabletId(TabletId));
        auto shared = std::make_shared<NOlap::NDataSharing::TStorageSharedBlobsManager>(
            NOlap::NBlobOperations::TGlobal::DefaultStorageId, NOlap::TTabletId(TabletId));
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
        auto info = MakeTabletInfo(TabletId, 3, { { 0, 100 }, { 3, 200 } });
        auto bm = std::make_shared<NOlap::TBlobManager>(info, 3, NOlap::TTabletId(TabletId));
        auto shared = std::make_shared<NOlap::NDataSharing::TStorageSharedBlobsManager>(
            NOlap::NBlobOperations::TGlobal::DefaultStorageId, NOlap::TTabletId(TabletId));
        TTestableHistoryCutter cutter(info, 3, bm, shared, TActorId(), TestSignals());

        cutter.OnBootComplete({});
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(TEntryKey{ 2, 0 }), 0);
        UNIT_ASSERT(!cutter.IsSweepInFlight());
        UNIT_ASSERT(cutter.GetSweepCandidates()->empty());
    }

    Y_UNIT_TEST(DoubleBlobPerPortionDeduplicates) {
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();
        static constexpr ui64 TabletId = 666;
        auto info = MakeTabletInfo(TabletId, 3, { { 0, 100 }, { 5, 200 } });
        auto bm = std::make_shared<NOlap::TBlobManager>(info, 5, NOlap::TTabletId(TabletId));
        auto shared = std::make_shared<NOlap::NDataSharing::TStorageSharedBlobsManager>(
            NOlap::NBlobOperations::TGlobal::DefaultStorageId, NOlap::TTabletId(TabletId));
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
        // History: [{fromGen=0, group=100}, {fromGen=5, group=100}, {fromGen=10, group=200}]
        //   entry {fromGen=0, group=100}: no earlier entries → passes
        //   entry {fromGen=5, group=100}: earlier entry {0,100} already has group 100 → blocked
        //   entry {fromGen=10, group=200}: earlier entries have groups {100,100}; 200 not seen → passes
        //   entry {fromGen=10, group=200} is the active (last) entry — TryNominate skips it via loop
        //   bound, but SeenGroupsCheckPasses itself does not exclude it.
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
        // Non-existent fromGeneration → not found → false.
        UNIT_ASSERT(!THistoryCutterWrapper::SeenGroupsCheckPasses(hist, /*fromGen=*/99));

        // Entry {5,100} is blocked by {0,100} — unless {0,100} was already cut:
        // cut entries are transparent for the same-group walk.
        UNIT_ASSERT(THistoryCutterWrapper::SeenGroupsCheckPasses(hist, /*fromGen=*/5, /*cutFromGenerations=*/{ 0 }));
        // A cut entry of a DIFFERENT generation does not unblock it.
        UNIT_ASSERT(!THistoryCutterWrapper::SeenGroupsCheckPasses(hist, /*fromGen=*/5, /*cutFromGenerations=*/{ 10 }));
    }

    // Sweep disproval path: disproved candidates return to None (with retry cooldown),
    // and no barrier is attempted for survivors that fail the final re-check.
    Y_UNIT_TEST(SweepDisprovalPath) {
        TActorSystemStub actorSystemStub;
        actorSystemStub.AppData.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        // 3 channels; history: {fromGen=0, group=100}, {fromGen=5, group=200}, {fromGen=10, group=300 (active)}.
        auto info = MakeTabletInfo(/*tabletId=*/777, /*nChannels=*/3, { { 0, 100 }, { 5, 200 }, { 10, 300 } });
        // Standalone wrapper: expired manager weak_ptr makes IsDrained() false, so the
        // final re-check can never reach the barrier-send branch in this test.
        TTestableHistoryCutter cutter(info, /*currentGen=*/20, std::weak_ptr<NOlap::TBlobManager>(),
            std::weak_ptr<NOlap::NDataSharing::TStorageSharedBlobsManager>(), TActorId(), TestSignals());

        const TEntryKey keyA{ /*channel=*/2, /*fromGeneration=*/0 };
        const TEntryKey keyB{ /*channel=*/2, /*fromGeneration=*/5 };
        cutter.StartSweepForTest({ keyA, keyB });
        UNIT_ASSERT(cutter.IsSweepInFlight());
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetSweepCandidates()->size(), 2);

        const auto ctx = NActors::TActivationContext::AsActorContext();

        // Batch 1 result: keyA disproved (blob found), cursor exhausted.
        cutter.OnBatchComplete({ keyA }, /*exhausted=*/true, ctx);

        UNIT_ASSERT(!cutter.IsSweepInFlight());
        // Disproved entry: back to None, never SentBarrier.
        UNIT_ASSERT(cutter.GetCutStateForTest(keyA) == ECutState::None);
        // Survivor keyB failed the final re-check (IsDrained false) → also None, no barrier.
        UNIT_ASSERT(cutter.GetCutStateForTest(keyB) == ECutState::None);
    }

    // The drain gate must treat our blobs shared out to other tablets as pinning
    // the entry: while shared they are in no GC queue, but a hard barrier would
    // collect them under the borrower.
    Y_UNIT_TEST(SharedBlobsPinDrainGate) {
        TActorSystemStub actorSystemStub;
        actorSystemStub.AppData.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        static constexpr ui64 TabletId = 888;
        static constexpr ui64 BorrowerTabletId = 999;
        // History: {fromGen=0, group=100}, {fromGen=5, group=200 (active)}.
        auto info = MakeTabletInfo(TabletId, /*nChannels=*/3, { { 0, 100 }, { 5, 200 } });
        auto bm = std::make_shared<NOlap::TBlobManager>(info, /*gen=*/5, NOlap::TTabletId(TabletId));
        auto shared = std::make_shared<NOlap::NDataSharing::TStorageSharedBlobsManager>(
            NOlap::NBlobOperations::TGlobal::DefaultStorageId, NOlap::TTabletId(TabletId));

        TTestableHistoryCutter cutter(info, /*currentGen=*/5, bm, shared, TActorId(), TestSignals());
        const TEntryKey key{ /*channel=*/2, /*fromGeneration=*/0 };

        // Empty queues and empty shared registry: the old entry is drained.
        UNIT_ASSERT(cutter.IsDrained(key));

        // Share out one of OUR blobs living in the old range (channel 2, gen 1 < 5).
        const NOlap::TUnifiedBlobId sharedOut(/*dsGroup=*/100, TLogoBlobID(TabletId, /*gen=*/1, /*step=*/1, /*channel=*/2, 100, 1));
        UNIT_ASSERT(shared->UpsertSharedBlobOnLoad(sharedOut, NOlap::TTabletId(BorrowerTabletId)));
        UNIT_ASSERT_C(!cutter.IsDrained(key), "shared-out blob in the old range must pin the entry");
    }

    // The blob-manager arm of the drain gate: an entry whose range still holds queued
    // blobs must not be nominated. Remove HasNoBlobsInRange() from IsDrained() and this
    // test must fail.
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

        // Both owners must outlive the cutter: it keeps weak_ptrs to them, and IsDrained()
        // answers false for an expired pointer, which would look exactly like "not drained".
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

    // Underflow poisons the channel; nomination then skips it though every other gate is open.
    Y_UNIT_TEST(UnderflowPoisonsChannelAndBlocksNomination) {
        TActorSystemStub actorSystemStub;
        actorSystemStub.AppData.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();
        static constexpr ui64 TabletId = 1010;
        static constexpr ui32 DataChannel = 2;
        static constexpr ui32 OldFromGen = 0;
        auto info = MakeTabletInfo(TabletId, /*nChannels=*/3, { { OldFromGen, 100 }, { 5, 200 } });
        auto bm = std::make_shared<NOlap::TBlobManager>(info, /*gen=*/5, NOlap::TTabletId(TabletId));
        auto shared = std::make_shared<NOlap::NDataSharing::TStorageSharedBlobsManager>(
            NOlap::NBlobOperations::TGlobal::DefaultStorageId, NOlap::TTabletId(TabletId));
        TTestableHistoryCutter cutter(info, /*currentGen=*/5, bm, shared, TActorId(), TestSignals());
        const TEntryKey key{ DataChannel, OldFromGen };

        // Positive control: without the poison every nomination gate is open.
        UNIT_ASSERT(cutter.IsDrained(key));
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetCounterForTest(key), 0);
        UNIT_ASSERT(!cutter.IsChannelPoisonedForTest(DataChannel));

        // Decrement with no matching counter — the underflow branch.
        cutter.DecrementCounter(key);
        UNIT_ASSERT(cutter.IsChannelPoisonedForTest(DataChannel));

        // The poisoned channel yields no candidates (the stub context is safe here:
        // an empty batch means TryNominate returns before any actor-system send).
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
        auto info = MakeTabletInfo(TabletId, /*nChannels=*/4, { { 0, 100 }, { 5, 200 }, { CurrentGen, 300 } });
        auto bm = std::make_shared<NOlap::TBlobManager>(info, CurrentGen, NOlap::TTabletId(TabletId));
        auto shared = std::make_shared<NOlap::NDataSharing::TStorageSharedBlobsManager>(
            NOlap::NBlobOperations::TGlobal::DefaultStorageId, NOlap::TTabletId(TabletId));
        TTestableHistoryCutter cutter(info, CurrentGen, bm, shared, TActorId(), TestSignals());
        const auto ctx = NActors::TActivationContext::AsActorContext();

        const TEntryKey keyOld{ /*channel=*/2, /*fromGeneration=*/0 };
        const TEntryKey keyMid{ /*channel=*/2, /*fromGeneration=*/5 };
        static constexpr ui32 PoisonChannel = 3;

        // Dirty everything reachable: counters, disproval backoff, poison, and an
        // in-flight sweep with Verifying state and a portion cursor.
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

    // Happy path against real actors: nominate, sweep, hard barrier at nextFromGen-1, cut.
    Y_UNIT_TEST(SweepHappyPathSendsHardBarrier) {
        TTestBasicRuntime runtime;
        TAppPrepare app;
        runtime.Initialize(app.Unwrap());
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
        const auto edgeBs = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeBlobStorageProxyID(OldGroup), edgeBs);
        const auto runner = runtime.Register(new TRunnerActor());
        auto runInActor = [&](std::function<void(const NActors::TActorContext&)> fn) {
            runtime.Send(new IEventHandle(runner, edgeTablet, new TEvRunInActor(std::move(fn))));
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
        };

        auto info = MakeTabletInfo(TabletId, /*nChannels=*/3, { { OldFromGen, OldGroup }, { ActiveFromGen, ActiveGroup } });
        auto bm = std::make_shared<NOlap::TBlobManager>(info, CurrentGen, NOlap::TTabletId(TabletId));
        auto shared = std::make_shared<NOlap::NDataSharing::TStorageSharedBlobsManager>(
            NOlap::NBlobOperations::TGlobal::DefaultStorageId, NOlap::TTabletId(TabletId));
        TTestableHistoryCutter cutter(info, CurrentGen, bm, shared, edgeTablet, TestSignals());
        cutter.SetLauncherActorId(edgeLauncher);
        const TEntryKey key{ DataChannel, OldFromGen };

        // Empty queues: the old entry is drained and must be nominated.
        bool nominated = false;
        runInActor([&](const NActors::TActorContext& ctx) {
            nominated = cutter.TryNominate(ctx);
        });
        UNIT_ASSERT(nominated);
        UNIT_ASSERT(runtime.GrabEdgeEvent<NColumnShard::TEvPrivate::TEvStartCutHistorySweep>(edgeTablet));
        UNIT_ASSERT_VALUES_EQUAL(guard->GetNominated().size(), 1);
        UNIT_ASSERT(cutter.GetCutStateForTest(key) == ECutState::Verifying);

        // Clean exhausted sweep (empty snapshot, nothing disproved) → barrier send.
        cutter.SetPortionSnapshot({});
        runInActor([&](const NActors::TActorContext& ctx) {
            cutter.OnBatchComplete({}, /*exhausted=*/true, ctx);
        });
        UNIT_ASSERT(cutter.GetCutStateForTest(key) == ECutState::SentBarrier);

        auto collect = runtime.GrabEdgeEvent<TEvBlobStorage::TEvCollectGarbage>(edgeBs);
        UNIT_ASSERT(collect);
        UNIT_ASSERT(collect->Get()->Hard);
        UNIT_ASSERT(collect->Get()->Collect);
        UNIT_ASSERT_VALUES_EQUAL(collect->Get()->TabletId, TabletId);
        UNIT_ASSERT_VALUES_EQUAL(collect->Get()->Channel, DataChannel);
        UNIT_ASSERT_VALUES_EQUAL(collect->Get()->CollectGeneration, ActiveFromGen - 1);

        runtime.Send(new IEventHandle(collect->Sender, edgeBs, new TEvBlobStorage::TEvCollectGarbageResult(NKikimrProto::OK, TabletId,
                                                                   CurrentGen, collect->Get()->PerGenerationCounter, DataChannel)));

        auto cutReq = runtime.GrabEdgeEvent<TEvTablet::TEvCutTabletHistory>(edgeLauncher);
        UNIT_ASSERT(cutReq);
        UNIT_ASSERT_VALUES_EQUAL(cutReq->Get()->Record.GetTabletID(), TabletId);
        UNIT_ASSERT_VALUES_EQUAL(cutReq->Get()->Record.GetChannel(), DataChannel);
        UNIT_ASSERT_VALUES_EQUAL(cutReq->Get()->Record.GetFromGeneration(), OldFromGen);
        UNIT_ASSERT_VALUES_EQUAL(cutReq->Get()->Record.GetGroupID(), OldGroup);

        auto done = runtime.GrabEdgeEvent<NColumnShard::TEvPrivate::TEvCutHistoryBarrierDone>(edgeTablet);
        UNIT_ASSERT(done);
        UNIT_ASSERT(done->Get()->Ok);

        cutter.OnBarrierResult(key, done->Get()->Ok);
        UNIT_ASSERT(cutter.GetCutStateForTest(key) == ECutState::Cut);
        UNIT_ASSERT_VALUES_EQUAL(guard->GetCut().size(), 1);
    }

    // One Attempts increment per disproving sweep; cooldown gates renomination in mock time.
    Y_UNIT_TEST(DisprovalBackoffGatesRenomination) {
        TTestBasicRuntime runtime;
        TAppPrepare app;
        runtime.Initialize(app.Unwrap());
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();

        static constexpr ui64 TabletId = 4040;
        static constexpr ui32 DataChannel = 2;
        static constexpr ui32 OldFromGen = 0;
        static constexpr ui32 OldGroup = 100;
        static constexpr ui32 CurrentGen = 5;

        const auto edgeTablet = runtime.AllocateEdgeActor();
        const auto edgeBs = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeBlobStorageProxyID(OldGroup), edgeBs);
        const auto runner = runtime.Register(new TRunnerActor());
        auto runInActor = [&](std::function<void(const NActors::TActorContext&)> fn) {
            runtime.Send(new IEventHandle(runner, edgeTablet, new TEvRunInActor(std::move(fn))));
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
        };

        auto info = MakeTabletInfo(TabletId, /*nChannels=*/3, { { OldFromGen, OldGroup }, { CurrentGen, 200 } });
        auto bm = std::make_shared<NOlap::TBlobManager>(info, CurrentGen, NOlap::TTabletId(TabletId));
        auto shared = std::make_shared<NOlap::NDataSharing::TStorageSharedBlobsManager>(
            NOlap::NBlobOperations::TGlobal::DefaultStorageId, NOlap::TTabletId(TabletId));
        TTestableHistoryCutter cutter(info, CurrentGen, bm, shared, edgeTablet, TestSignals());
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

        // After one disproval Attempts=1, so renomination waits cooldown(1)=10m
        // measured from that disproval: 2m and 6m stay blocked, 11m passes.
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

        // This time nothing disproves the entry: the sweep survives to the barrier
        // and the backoff record is erased before SentBarrier.
        UNIT_ASSERT(runtime.GrabEdgeEvent<NColumnShard::TEvPrivate::TEvStartCutHistorySweep>(edgeTablet));
        cutter.SetPortionSnapshot({});
        runInActor([&](const NActors::TActorContext& ctx) {
            cutter.OnBatchComplete({}, /*exhausted=*/true, ctx);
        });
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvBlobStorage::TEvCollectGarbage>(edgeBs));
        UNIT_ASSERT(cutter.GetCutStateForTest(key) == ECutState::SentBarrier);
        UNIT_ASSERT_VALUES_EQUAL(cutter.GetDisprovalAttemptsForTest(key), 0);
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

}   // TCutHistoryCutterCounters

}   // namespace NKikimr

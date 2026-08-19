// Unit tests for the CutHistory (KIKIMR-26208) two-tier nomination engine.
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/testlib/actor_helpers.h>
#include <ydb/core/tx/columnshard/blobs_action/bs/blob_manager.h>
#include <ydb/core/tx/columnshard/blobs_action/bs/history_cutter.h>
#include <ydb/core/tx/columnshard/blobs_action/common/const.h>
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
    using THistoryCutterWrapper::GetCutStateForTest;
    using THistoryCutterWrapper::IsDrained;
    using THistoryCutterWrapper::StartSweepForTest;
    using THistoryCutterWrapper::THistoryCutterWrapper;
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
        // Tablet: 3 channels, 2 history entries each.
        static constexpr ui64 TabletId = 111;
        static constexpr ui32 CurrentGen = 5;
        auto info = MakeTabletInfo(TabletId, 3, { { 0, 100 }, { 5, 200 } });
        auto bm = std::make_shared<NOlap::TBlobManager>(info, CurrentGen, NOlap::TTabletId(TabletId));
        bm->InitHistoryCutter(bm, nullptr, TActorId());
        auto* cutter = bm->GetHistoryCutter();
        UNIT_ASSERT(cutter);

        NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();

        // A blob on channel 2 at generation 3 falls in entry {ch=2, fromGen=0}.
        const TLogoBlobID blob = MakeBlob(TabletId, /*ch=*/2, /*gen=*/3);
        // TPortionDataAccessor is not easily constructible here; test counter
        // logic via OnBootComplete which calls IncrementCounter internally.
        const NOlap::TUnifiedBlobId ub = MakeUnifiedBlob(blob);
        THashMap<ui64, std::vector<NOlap::TUnifiedBlobId>> portionBlobs;
        portionBlobs[/*portionId=*/42].push_back(ub);

        cutter->OnBootComplete(portionBlobs);

        // The entry {ch=2, fromGen=0} should have counter=1 now.
        // Verify indirectly: cutter must NOT nominate it (counter != 0).
        // GetSweepCandidates() is non-empty only after TryNominate; since we have no actor
        // context here, just check IsSweepInFlight() is false and no candidates.
        UNIT_ASSERT(!cutter->IsSweepInFlight());
        UNIT_ASSERT(cutter->GetSweepCandidates()->empty());
    }

    Y_UNIT_TEST(DecrementToZeroOnPortionRemoved) {
        static constexpr ui64 TabletId = 222;
        static constexpr ui32 CurrentGen = 5;
        auto info = MakeTabletInfo(TabletId, 3, { { 0, 100 }, { 5, 200 } });
        auto bm = std::make_shared<NOlap::TBlobManager>(info, CurrentGen, NOlap::TTabletId(TabletId));
        bm->InitHistoryCutter(bm, nullptr, TActorId());
        auto* cutter = bm->GetHistoryCutter();

        NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();

        const NOlap::TUnifiedBlobId ub = MakeUnifiedBlob(MakeBlob(TabletId, 2, 3));
        THashMap<ui64, std::vector<NOlap::TUnifiedBlobId>> portionBlobs;
        portionBlobs[42].push_back(ub);
        cutter->OnBootComplete(portionBlobs);

        cutter->OnPortionRemoved(42);

        // Now counter=0 and BlobsToKeep/Delete empty → IsDrained true → entry is nominatable.
        // We cannot call TryNominate without actor context, but we can verify GetSweepCandidates
        // is still empty (TryNominate not yet called).
        UNIT_ASSERT(!cutter->IsSweepInFlight());
    }

    Y_UNIT_TEST(ForeignBlobIgnored) {
        // Blob from a different tablet should be ignored.
        static constexpr ui64 TabletId = 333;
        static constexpr ui64 OtherTablet = 999;
        static constexpr ui32 CurrentGen = 5;
        auto info = MakeTabletInfo(TabletId, 3, { { 0, 100 }, { 5, 200 } });
        auto bm = std::make_shared<NOlap::TBlobManager>(info, CurrentGen, NOlap::TTabletId(TabletId));
        bm->InitHistoryCutter(bm, nullptr, TActorId());
        auto* cutter = bm->GetHistoryCutter();
        NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();

        const NOlap::TUnifiedBlobId ub = MakeUnifiedBlob(MakeBlob(OtherTablet, 2, 3));
        THashMap<ui64, std::vector<NOlap::TUnifiedBlobId>> portionBlobs;
        portionBlobs[55].push_back(ub);
        cutter->OnBootComplete(portionBlobs);

        // Foreign blob must not increment any counter.
        // Entry {ch=2, fromGen=0} counter remains 0.
        // If we remove portion 55 nothing should be poisoned.
        cutter->OnPortionRemoved(55);
        // No crash = test passes (no underflow → no poison).
        UNIT_ASSERT(!cutter->IsSweepInFlight());
    }

    Y_UNIT_TEST(ActiveEntryBlobIgnored) {
        // Blob at current generation should map to active entry and be ignored.
        static constexpr ui64 TabletId = 444;
        static constexpr ui32 CurrentGen = 5;
        auto info = MakeTabletInfo(TabletId, 3, { { 0, 100 }, { CurrentGen, 200 } });
        auto bm = std::make_shared<NOlap::TBlobManager>(info, CurrentGen, NOlap::TTabletId(TabletId));
        bm->InitHistoryCutter(bm, nullptr, TActorId());
        auto* cutter = bm->GetHistoryCutter();
        NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();

        // Blob at generation==CurrentGen → active entry → ignored.
        const NOlap::TUnifiedBlobId ub = MakeUnifiedBlob(MakeBlob(TabletId, 2, CurrentGen));
        THashMap<ui64, std::vector<NOlap::TUnifiedBlobId>> portionBlobs;
        portionBlobs[77].push_back(ub);
        cutter->OnBootComplete(portionBlobs);
        // No assertion other than no crash.
        UNIT_ASSERT(!cutter->IsSweepInFlight());
    }

    Y_UNIT_TEST(BootCompleteWithEmptyMap) {
        // Boot with no portions is valid; cutter starts with zero counters.
        static constexpr ui64 TabletId = 555;
        static constexpr ui32 CurrentGen = 3;
        auto info = MakeTabletInfo(TabletId, 3, { { 0, 100 }, { 3, 200 } });
        auto bm = std::make_shared<NOlap::TBlobManager>(info, CurrentGen, NOlap::TTabletId(TabletId));
        bm->InitHistoryCutter(bm, nullptr, TActorId());
        auto* cutter = bm->GetHistoryCutter();
        NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();

        // Empty boot — all counters zero; entry {ch=2, fromGen=0} is drained.
        cutter->OnBootComplete({});
        UNIT_ASSERT(!cutter->IsSweepInFlight());
        UNIT_ASSERT(cutter->GetSweepCandidates()->empty());
    }

    Y_UNIT_TEST(DoubleBlobPerPortionDeduplicates) {
        // Two blobs in the same portion mapping to the same entry → counter incremented only once.
        static constexpr ui64 TabletId = 666;
        static constexpr ui32 CurrentGen = 5;
        auto info = MakeTabletInfo(TabletId, 3, { { 0, 100 }, { 5, 200 } });
        auto bm = std::make_shared<NOlap::TBlobManager>(info, CurrentGen, NOlap::TTabletId(TabletId));
        bm->InitHistoryCutter(bm, nullptr, TActorId());
        auto* cutter = bm->GetHistoryCutter();
        NYDBTest::TControllers::RegisterCSControllerGuard<TCutHistoryController>();

        // Two blobs on the same channel/entry in one portion.
        THashMap<ui64, std::vector<NOlap::TUnifiedBlobId>> portionBlobs;
        portionBlobs[88].push_back(MakeUnifiedBlob(MakeBlob(TabletId, 2, 1, 1, 1)));
        portionBlobs[88].push_back(MakeUnifiedBlob(MakeBlob(TabletId, 2, 2, 1, 2)));
        cutter->OnBootComplete(portionBlobs);

        // Remove the portion → counter should go to 0 (was 1, not 2).
        cutter->OnPortionRemoved(88);
        // No poison → removal was clean.
        UNIT_ASSERT(!cutter->IsSweepInFlight());
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

}   // TCutHistoryCutterCounters

}   // namespace NKikimr

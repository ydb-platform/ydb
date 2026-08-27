#include "tablet_info_helper.h"

#include <ydb/core/testlib/actor_helpers.h>
#include <ydb/core/tx/columnshard/blobs_action/bs/blob_manager.h>
#include <ydb/core/tx/columnshard/data_sharing/manager/shared_blobs.h>
#include <ydb/core/tx/columnshard/engines/scheme/objects_cache.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/move/move.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/test_helper/portion_test_helper.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/size_literals.h>

namespace NKikimr {

using NTestMoveData::MakeTabletInfo;

static constexpr ui32 BlobSize = 1_KB;

static NOlap::TUnifiedBlobId MakeDsBlobId(ui32 dsGroup, ui64 tabletId, ui32 gen, ui32 step, ui32 channel) {
    TLogoBlobID logo(tabletId, gen, step, channel, BlobSize, 0);
    return NOlap::TUnifiedBlobId(dsGroup, logo);
}

// Exposes the protected test hooks of the production class to this suite only.
class TMoveDataActualizerTestable: public NOlap::NActualizer::TMoveDataActualizer {
public:
    using NOlap::NActualizer::TMoveDataActualizer::AddToInitialAndPendingForTest;
    using NOlap::NActualizer::TMoveDataActualizer::ConfirmPortionForTest;
    using NOlap::NActualizer::TMoveDataActualizer::IsInInitialPortionIds;
    using NOlap::NActualizer::TMoveDataActualizer::IsInPendingPortionIds;
    using NOlap::NActualizer::TMoveDataActualizer::IsInPortionsToMove;
    using NOlap::NActualizer::TMoveDataActualizer::SimulateTaskSubmissionForTest;
    using NOlap::NActualizer::TMoveDataActualizer::TMoveDataActualizer;
};

class TSoftMemoryLimitController: public NYDBTest::ICSController {
private:
    const ui64 SoftMemoryLimit;

public:
    TSoftMemoryLimitController(const ui64 softMemoryLimit)
        : SoftMemoryLimit(softMemoryLimit)
    {
    }

    ui64 DoGetMetadataRequestSoftMemoryLimit(const ui64 /*defaultValue*/) const override {
        return SoftMemoryLimit;
    }
};

Y_UNIT_TEST_SUITE(TMoveDataTest) {
    // The BlobsToDelete leg of the operator gate: a queued blob whose TUnifiedBlobId carries
    // the group directly. The BlobsToKeep leg, which resolves the group through channel
    // history instead, is TestMoveDataKeepQueue.
    Y_UNIT_TEST(TestMoveDataDeleteQueue) {
        TActorSystemStub actorSystemStub;
        actorSystemStub.AppData.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        static constexpr ui64 TabletId = 42;
        static constexpr ui32 OldGroup = 100;
        static constexpr ui32 NewGroup = 200;
        static constexpr ui32 ReassignGen = 5;

        auto tabletInfo = MakeTabletInfo(TabletId, { { 0, OldGroup }, { ReassignGen, NewGroup } }, TBlobStorageGroupType::ErasureNone);
        UNIT_ASSERT_VALUES_EQUAL(tabletInfo->GroupFor(2, 1), OldGroup);
        UNIT_ASSERT_VALUES_EQUAL(tabletInfo->GroupFor(2, 7), NewGroup);

        NOlap::TBlobManager mgr(tabletInfo, 3, NOlap::TTabletId(TabletId));
        UNIT_ASSERT_C(!mgr.HasBlobsForGroups({ OldGroup }), "empty queues must match nothing");

        mgr.DeleteBlobOnComplete(NOlap::TTabletId(TabletId), MakeDsBlobId(OldGroup, TabletId, 1, 1, 2));
        UNIT_ASSERT_C(mgr.HasBlobsForGroups({ OldGroup }), "blob in the old group must match");
        UNIT_ASSERT_C(!mgr.HasBlobsForGroups({ NewGroup }), "the group it was not written to must not match");
        UNIT_ASSERT_C(!mgr.HasBlobsForGroups({ 999u }), "an unrelated group must not match");
        // The gate is polled on every wakeup, so the query has to be non-destructive.
        UNIT_ASSERT_C(mgr.HasBlobsForGroups({ OldGroup }), "repeated query must give the same answer");
    }

    // TestMoveDataF1Invariant: pinning the F1 fix — after task submission (RemoveFromActiveQueue),
    // InitialPortionIds is preserved so the portion can re-enter PendingPortionIds on failure.
    // Uses test helpers that bypass the full TTieringProcessContext.
    Y_UNIT_TEST(TestMoveDataF1Invariant) {
        static constexpr ui64 PortionId = 7;
        static constexpr ui32 Group = 50;

        THashSet<ui32> targetGroups = { Group };
        NOlap::TVersionedIndex dummyVersionedIndex;
        TMoveDataActualizerTestable actualizer(targetGroups, dummyVersionedIndex);

        // Step 1: inject portion into Initial + Pending.
        actualizer.AddToInitialAndPendingForTest(PortionId);
        UNIT_ASSERT(actualizer.IsInInitialPortionIds(PortionId));
        UNIT_ASSERT(actualizer.IsInPendingPortionIds(PortionId));
        UNIT_ASSERT(!actualizer.IsInPortionsToMove(PortionId));

        // Step 2: confirm portion (accessor validated, blobs match target group).
        actualizer.ConfirmPortionForTest(PortionId);
        UNIT_ASSERT(actualizer.IsInInitialPortionIds(PortionId));
        UNIT_ASSERT(!actualizer.IsInPendingPortionIds(PortionId));
        UNIT_ASSERT(actualizer.IsInPortionsToMove(PortionId));
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataPortionsCount(), 1);

        // Step 3: simulate successful task submission (DoExtractTasks SUCCESS path).
        // Must NOT remove from InitialPortionIds.
        actualizer.SimulateTaskSubmissionForTest(PortionId);
        UNIT_ASSERT_C(actualizer.IsInInitialPortionIds(PortionId), "F1: InitialPortionIds must survive task submission");
        UNIT_ASSERT_C(!actualizer.IsInPortionsToMove(PortionId), "F1: PortionsToMove must be cleared after submission");
        // The submitted portion is in flight: it must still count towards the response
        // gate until the change commits (old blobs enter delete queues only then).
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataPortionsCount(), 1);

        // Step 4: simulate change failure → ReturnToIndexes → AddPortion.
        // Since InitialPortionIds still contains PortionId and it's no longer in
        // PortionAddress or PendingPortionIds, it must re-enter PendingPortionIds.
        actualizer.AddToInitialAndPendingForTest(PortionId);
        UNIT_ASSERT_C(actualizer.IsInPendingPortionIds(PortionId), "F1: after failure return, portion must be back in PendingPortionIds");
        // Re-added portion moved from in-flight back to pending — counted once, not twice.
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataPortionsCount(), 1);
    }

    // TestMoveDataKeepQueue: HasBlobsForGroups must see blobs resident in BlobsToKeep,
    // resolved through TabletInfo->GroupFor(channel, generation).
    Y_UNIT_TEST(TestMoveDataKeepQueue) {
        TActorSystemStub actorSystemStub;
        actorSystemStub.AppData.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        static constexpr ui64 TabletId = 45;
        static constexpr ui32 OldGroup = 100;
        static constexpr ui32 NewGroup = 200;
        static constexpr ui32 ReassignGen = 5;

        auto tabletInfo = MakeTabletInfo(TabletId, { { 0, OldGroup }, { ReassignGen, NewGroup } }, TBlobStorageGroupType::ErasureNone);

        // Generation 3 < ReassignGen: batches allocate blobs resolving into OldGroup.
        NOlap::TBlobManager mgr(tabletInfo, 3, NOlap::TTabletId(TabletId));
        auto batch = mgr.StartBlobBatch();
        batch.AllocateNextBlobId(TString("payload"));
        mgr.SaveBlobBatchOnComplete(std::move(batch));

        UNIT_ASSERT_C(mgr.HasBlobsForGroups({ OldGroup }), "BlobsToKeep: blob in old group must match via GroupFor");
        UNIT_ASSERT_C(!mgr.HasBlobsForGroups({ NewGroup }), "BlobsToKeep: new group must not match");

        // Generation 7 >= ReassignGen: same channels now resolve into NewGroup.
        NOlap::TBlobManager mgrNew(tabletInfo, 7, NOlap::TTabletId(TabletId));
        auto batchNew = mgrNew.StartBlobBatch();
        batchNew.AllocateNextBlobId(TString("payload"));
        mgrNew.SaveBlobBatchOnComplete(std::move(batchNew));

        UNIT_ASSERT_C(mgrNew.HasBlobsForGroups({ NewGroup }), "BlobsToKeep: blob after reassign must match new group");
        UNIT_ASSERT_C(!mgrNew.HasBlobsForGroups({ OldGroup }), "BlobsToKeep: old group must not match after reassign");
    }

    // TestMoveDataSharedBlobs: the shared/borrowed registry leg of the operator gate.
    // Borrowed blobs are foreign-tablet blobs whose group comes from the persisted
    // DS:<group>:<logoblobid> form (GetDsGroup), not from our channel history.
    Y_UNIT_TEST(TestMoveDataSharedBlobs) {
        static constexpr ui64 TabletId = 46;
        static constexpr ui64 ForeignTabletId = 99;
        static constexpr ui32 OldGroup = 100;
        static constexpr ui32 NewGroup = 200;

        NOlap::NDataSharing::TStorageSharedBlobsManager shared(NOlap::IStoragesManager::DefaultStorageId, NOlap::TTabletId(TabletId));
        UNIT_ASSERT(!shared.HasBlobsForGroups({ OldGroup }));

        const auto borrowed = MakeDsBlobId(OldGroup, ForeignTabletId, /*gen=*/1, /*step=*/1, /*channel=*/2);
        UNIT_ASSERT(shared.UpsertBorrowedBlobOnLoad(borrowed, NOlap::TTabletId(ForeignTabletId)));

        UNIT_ASSERT_C(shared.HasBlobsForGroups({ OldGroup }), "borrowed blob in old group must match via GetDsGroup");
        UNIT_ASSERT_C(!shared.HasBlobsForGroups({ NewGroup }), "unrelated group must not match");
    }

    // The selection rule that decides whether a portion is rewritten at all. Exercised
    // directly: a TPortionDataAccessor needs arrow-backed portion metadata to build, and
    // ActualizePortionInfo's remaining work is bookkeeping the other cases already cover.
    Y_UNIT_TEST(HasBlobInGroupsSelectsOnlyTargetGroups) {
        static constexpr ui64 TabletId = 46;
        static constexpr ui32 TargetGroup = 100;
        static constexpr ui32 OtherGroup = 200;
        static constexpr ui32 ThirdGroup = 300;
        static constexpr ui32 Gen = 3;
        static constexpr ui32 Step = 1;
        static constexpr ui32 Channel = 2;
        const THashSet<ui32> targets{ TargetGroup };

        const auto inTarget = MakeDsBlobId(TargetGroup, TabletId, Gen, Step, Channel);
        const auto outsideTarget = MakeDsBlobId(OtherGroup, TabletId, Gen, Step, Channel);
        const auto thirdParty = MakeDsBlobId(ThirdGroup, TabletId, Gen, Step, Channel);

        using TActualizer = NOlap::NActualizer::TMoveDataActualizer;
        UNIT_ASSERT_C(TActualizer::HasBlobInGroups({ inTarget }, targets), "a blob in a target group must select the portion");
        UNIT_ASSERT_C(!TActualizer::HasBlobInGroups({ outsideTarget }, targets), "a blob outside the target groups must not select it");
        UNIT_ASSERT_C(
            TActualizer::HasBlobInGroups({ outsideTarget, inTarget }, targets), "one blob in a target group is enough, even alongside others");
        UNIT_ASSERT_C(
            !TActualizer::HasBlobInGroups({ outsideTarget, thirdParty }, targets), "no blob in a target group means the portion stays put");
        UNIT_ASSERT_C(!TActualizer::HasBlobInGroups({}, targets), "a portion with no blobs is never selected");
        UNIT_ASSERT_C(!TActualizer::HasBlobInGroups({ inTarget }, {}), "an empty target set selects nothing");
    }

    Y_UNIT_TEST(MoveDataCompletionGateClassifier) {
        using NOlap::NActualizer::ClassifyMoveDataGate;
        using NOlap::NActualizer::EMoveDataGate;
        using NOlap::NActualizer::TMoveDataQueueSizes;

        static constexpr bool VacuumDone = true;
        static constexpr bool HasBlobs = true;
        const TMoveDataQueueSizes empty;

        UNIT_ASSERT(ClassifyMoveDataGate(VacuumDone, empty, !HasBlobs) == EMoveDataGate::Ready);

        // Vacuum dominates everything, portions dominate GC — the order picks the sensor.
        UNIT_ASSERT(ClassifyMoveDataGate(!VacuumDone, empty, !HasBlobs) == EMoveDataGate::BlockedByVacuum);
        UNIT_ASSERT(ClassifyMoveDataGate(!VacuumDone, TMoveDataQueueSizes{ 1, 1, 1 }, HasBlobs) == EMoveDataGate::BlockedByVacuum);
        UNIT_ASSERT(ClassifyMoveDataGate(VacuumDone, TMoveDataQueueSizes{ 1, 0, 0 }, HasBlobs) == EMoveDataGate::BlockedByPortions);
        UNIT_ASSERT(ClassifyMoveDataGate(VacuumDone, empty, HasBlobs) == EMoveDataGate::BlockedByGC);

        // Each queue component alone must block: InFlight in particular, or a submitted
        // rewrite whose old blobs are not yet in the delete queues slips past the gate.
        UNIT_ASSERT(ClassifyMoveDataGate(VacuumDone, TMoveDataQueueSizes{ 1, 0, 0 }, !HasBlobs) == EMoveDataGate::BlockedByPortions);
        UNIT_ASSERT(ClassifyMoveDataGate(VacuumDone, TMoveDataQueueSizes{ 0, 1, 0 }, !HasBlobs) == EMoveDataGate::BlockedByPortions);
        UNIT_ASSERT(ClassifyMoveDataGate(VacuumDone, TMoveDataQueueSizes{ 0, 0, 1 }, !HasBlobs) == EMoveDataGate::BlockedByPortions);
    }

    // Hive rebinds our channels only after we answer, so portions created during the session
    // still land in the doomed group. They are adopted until the admission deadline, and
    // ignored after it - the deadline is what stops a busy tablet feeding itself forever.
    Y_UNIT_TEST(AdoptsPortionsCreatedDuringTheSessionUntilTheDeadline) {
        const auto pathId = NOlap::TInternalPathId::FromRawValue(1);
        auto cache = std::make_shared<NOlap::TSchemaObjectsCache>();
        NOlap::TVersionedIndex versionedIndex;
        versionedIndex.AddIndex(NOlap::TSnapshot(1, 1), cache->UpsertIndexInfo(NOlap::NTest::MakePortionTestIndexInfo()));

        TMoveDataActualizerTestable actualizer(THashSet<ui32>{ 100 }, versionedIndex);
        const TInstant start = TInstant::Seconds(1000);
        actualizer.Refresh(NOlap::NActualizer::TAddExternalContext(start, {}));

        auto makePortion = [&](const ui64 portionId) {
            return NOlap::NTest::MakeTestCompactedPortion(pathId, portionId, 10, 19, 10, NOlap::TSnapshot(1, 1), std::nullopt);
        };

        const THashMap<ui64, NOlap::TPortionInfo::TPtr> noPortions;
        actualizer.AddPortion(makePortion(1), NOlap::NActualizer::TAddExternalContext(start + TDuration::Minutes(1), noPortions));
        UNIT_ASSERT_C(actualizer.IsInPendingPortionIds(1), "a portion created inside the window must be adopted");

        actualizer.AddPortion(makePortion(2), NOlap::NActualizer::TAddExternalContext(start + TDuration::Hours(1), noPortions));
        UNIT_ASSERT_C(!actualizer.IsInPendingPortionIds(2), "past the deadline the session must stop adopting");
    }

    Y_UNIT_TEST(MoveDataMetadataRequestsBatching) {
        static constexpr ui64 PortionsCount = 7;
        const auto pathId = NOlap::TInternalPathId::FromRawValue(1);
        const THashSet<ui32> targetGroups = { 100 };

        auto cache = std::make_shared<NOlap::TSchemaObjectsCache>();
        NOlap::TVersionedIndex versionedIndex;
        versionedIndex.AddIndex(NOlap::TSnapshot(1, 1), cache->UpsertIndexInfo(NOlap::NTest::MakePortionTestIndexInfo()));

        THashMap<ui64, NOlap::TPortionInfo::TPtr> portions;
        for (ui64 portionId = 1; portionId <= PortionsCount; ++portionId) {
            portions.emplace(
                portionId, NOlap::NTest::MakeTestCompactedPortion(pathId, portionId, 10, 19, 10, NOlap::TSnapshot(1, 1), std::nullopt));
        }
        const ui64 portionMemory = portions.at(1)->PredictAccessorsMemory(portions.at(1)->GetSchema(versionedIndex));
        UNIT_ASSERT_GT(portionMemory, 0);

        auto buildWithLimit = [&](const ui64 softLimit, const THashMap<ui64, NOlap::TPortionInfo::TPtr>& knownPortions) {
            auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TSoftMemoryLimitController>(softLimit);
            auto actualizer = std::make_shared<TMoveDataActualizerTestable>(targetGroups, versionedIndex);
            for (ui64 portionId = 1; portionId <= PortionsCount; ++portionId) {
                actualizer->AddToInitialAndPendingForTest(portionId);
            }
            return actualizer->BuildMoveDataMetadataRequests(knownPortions, actualizer);
        };

        auto batchSizes = [](const std::vector<NOlap::TCSMetadataRequest>& requests) {
            std::vector<ui32> result;
            for (auto&& request : requests) {
                result.emplace_back(request.GetRequest()->GetSize());
            }
            Sort(result.begin(), result.end(), std::greater<ui32>());
            return result;
        };

        auto portionIds = [](const std::vector<NOlap::TCSMetadataRequest>& requests) {
            THashSet<ui64> result;
            for (auto&& request : requests) {
                for (auto&& portionId : request.GetRequest()->GetPortionIds()) {
                    UNIT_ASSERT_C(result.emplace(portionId).second, "a portion must not be requested twice");
                }
            }
            return result;
        };

        {
            const auto requests = buildWithLimit(3 * portionMemory, portions);
            UNIT_ASSERT_VALUES_EQUAL(requests.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(batchSizes(requests), (std::vector<ui32>{ 3, 3, 1 }));
            UNIT_ASSERT_VALUES_EQUAL(portionIds(requests).size(), PortionsCount);
        }
        {
            const auto requests = buildWithLimit(PortionsCount * portionMemory + 1, portions);
            UNIT_ASSERT_VALUES_EQUAL(requests.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(requests.front().GetRequest()->GetSize(), PortionsCount);
        }
        {
            // The testing default of 0 makes every portion its own request.
            const auto requests = buildWithLimit(0, portions);
            UNIT_ASSERT_VALUES_EQUAL(requests.size(), PortionsCount);
            UNIT_ASSERT_VALUES_EQUAL(batchSizes(requests), (std::vector<ui32>(PortionsCount, 1)));
        }
        {
            auto knownPortions = portions;
            knownPortions.erase(PortionsCount);
            const auto requests = buildWithLimit(3 * portionMemory, knownPortions);
            const auto ids = portionIds(requests);
            UNIT_ASSERT_VALUES_EQUAL(ids.size(), PortionsCount - 1);
            UNIT_ASSERT_C(!ids.contains(PortionsCount), "a portion the engine no longer knows must not be requested");
        }
        {
            auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TSoftMemoryLimitController>(3 * portionMemory);
            auto actualizer = std::make_shared<TMoveDataActualizerTestable>(targetGroups, versionedIndex);
            UNIT_ASSERT(actualizer->BuildMoveDataMetadataRequests(portions, actualizer).empty());
        }
    }

}   // Y_UNIT_TEST_SUITE

}   // namespace NKikimr

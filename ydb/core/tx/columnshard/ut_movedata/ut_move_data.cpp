#include "tablet_info_helper.h"

#include <ydb/core/testlib/actor_helpers.h>
#include <ydb/core/tx/columnshard/blobs_action/bs/blob_manager.h>
#include <ydb/core/tx/columnshard/blobs_action/bs/gc.h>
#include <ydb/core/tx/columnshard/blobs_action/counters/storage.h>
#include <ydb/core/tx/columnshard/data_sharing/manager/shared_blobs.h>
#include <ydb/core/tx/columnshard/engines/scheme/objects_cache.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/move/move.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/max/meta.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/ro_controller.h>
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

// Every actualizer test lives under one path; portion ids alone tell the cases apart.
static NOlap::TInternalPathId TestPathId() {
    return NOlap::TInternalPathId::FromRawValue(1);
}

// Builds a TIndexInfo with a MAX index; inheritPortionStorage controls where the index blob lives.
static NOlap::TIndexInfo MakeMaxIndexInfo(const bool inheritPortionStorage) {
    static constexpr ui32 kPkColId = 1;
    static constexpr ui32 kMaxIndexId = 100;
    NKikimrSchemeOp::TColumnTableSchema proto;
    *proto.MutableColumns()->Add() = NArrow::NTest::TTestColumn("pk", NScheme::TTypeInfo(NScheme::NTypeIds::Uint64)).CreateColumn(kPkColId);
    proto.AddKeyColumnNames("pk");
    proto.SetVersion(1);
    *proto.AddIndexes() = NOlap::NIndexes::TIndexMetaContainer(std::make_shared<NOlap::NIndexes::NMax::TIndexMeta>(kMaxIndexId, "pk_max",
                                                                   NOlap::IStoragesManager::DefaultStorageId, inheritPortionStorage, kPkColId))
                              .SerializeToProto();
    auto cache = std::make_shared<NOlap::TSchemaObjectsCache>();
    auto result = NOlap::TIndexInfo::BuildFromProto(1, proto, NOlap::TTestStoragesManager::GetInstance(), cache);
    AFL_VERIFY(result);
    return std::move(*result);
}

// Builds a compacted portion assigned to the given tier.
static NOlap::TPortionInfo::TPtr MakeTieredPortion(const ui64 portionId, const TString& tierName, const NOlap::TIndexInfo& indexInfo) {
    TString serialized = NArrow::SerializeBatchNoCompression(NOlap::NTest::MakePortionTestPKBatch(10, 19));
    NKikimrTxColumnShard::TIndexPortionMeta metaProto;
    metaProto.SetIsCompacted(true);
    metaProto.SetPrimaryKeyBorders(serialized);
    metaProto.SetTierName(tierName);
    metaProto.MutableRecordSnapshotMin()->SetPlanStep(1);
    metaProto.MutableRecordSnapshotMin()->SetTxId(1);
    metaProto.MutableRecordSnapshotMax()->SetPlanStep(1);
    metaProto.MutableRecordSnapshotMax()->SetTxId(1);
    metaProto.SetDeletionsCount(0);
    metaProto.SetCompactionLevel(0);
    metaProto.SetRecordsCount(10);
    metaProto.SetColumnRawBytes(100);
    metaProto.SetColumnBlobBytes(100);
    metaProto.SetIndexRawBytes(0);
    metaProto.SetIndexBlobBytes(0);
    metaProto.SetNumSlices(1);
    metaProto.MutableCompactedPortion()->MutableAppearanceSnapshot()->SetPlanStep(1);
    metaProto.MutableCompactedPortion()->MutableAppearanceSnapshot()->SetTxId(1);
    NOlap::TPortionMetaConstructor metaConstructor;
    NOlap::TFakeGroupSelector groupSelector;
    AFL_VERIFY(metaConstructor.LoadMetadata(metaProto, indexInfo, groupSelector));
    NOlap::TCompactedPortionInfoConstructor constructor(TestPathId(), portionId);
    constructor.SetSchemaVersion(1);
    constructor.SetAppearanceSnapshot(NOlap::TSnapshot(1, 1));
    constructor.MutableMeta() = metaConstructor;
    return constructor.Build();
}

// A compacted default-tier portion: admission accepts it without a schema lookup.
static NOlap::TPortionInfo::TPtr MakeDefaultTierPortion(const ui64 portionId) {
    return NOlap::NTest::MakeTestCompactedPortion(TestPathId(), portionId, 10, 19, 10, NOlap::TSnapshot(1, 1), std::nullopt);
}

// Owns the cache and the versioned index an actualizer refers to for the whole test.
struct TActualizerSchema {
    std::shared_ptr<NOlap::TSchemaObjectsCache> Cache = std::make_shared<NOlap::TSchemaObjectsCache>();
    NOlap::TVersionedIndex Index;

    explicit TActualizerSchema(NOlap::TIndexInfo&& indexInfo) {
        Index.AddIndex(NOlap::TSnapshot(1, 1), Cache->UpsertIndexInfo(std::move(indexInfo)));
    }

    const NOlap::TIndexInfo& GetIndexInfo() const {
        return Index.GetLastSchema()->GetIndexInfo();
    }
};

// Exposes the protected test hooks of the production class to this suite only.
class TMoveDataActualizerTestable: public NOlap::NActualizer::TMoveDataActualizer {
public:
    using NOlap::NActualizer::TMoveDataActualizer::AddToInitialAndPendingForTest;
    using NOlap::NActualizer::TMoveDataActualizer::ConfirmPortionForTest;
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
    // BlobsToDelete leg: the group comes straight off TUnifiedBlobId (keep leg: TestMoveDataKeepQueue).
    Y_UNIT_TEST(TestMoveDataDeleteQueue) {
        TActorSystemStub actorSystemStub;
        actorSystemStub.AppData.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        static constexpr ui64 TabletId = 42;
        static constexpr ui32 OldGroup = 100;
        static constexpr ui32 NewGroup = 200;
        static constexpr ui32 ReassignGen = 5;

        auto tabletInfo = MakeTabletInfo(TabletId, { { 0, OldGroup }, { ReassignGen, NewGroup } }, TBlobStorageGroupType::ErasureNone);
        NOlap::TBlobManager mgr(tabletInfo, 3, NOlap::TTabletId(TabletId));
        UNIT_ASSERT_C(!mgr.HasBlobsForGroups({ OldGroup }), "empty queues must match nothing");

        mgr.DeleteBlobOnComplete(NOlap::TTabletId(TabletId), MakeDsBlobId(OldGroup, TabletId, 1, 1, 2));
        UNIT_ASSERT_C(mgr.HasBlobsForGroups({ OldGroup }), "blob in the old group must match");
        UNIT_ASSERT_C(!mgr.HasBlobsForGroups({ NewGroup }), "the group it was not written to must not match");
        UNIT_ASSERT_C(!mgr.HasBlobsForGroups({ 999u }), "an unrelated group must not match");
        // The gate is polled on every wakeup, so the query has to be non-destructive.
        UNIT_ASSERT_C(mgr.HasBlobsForGroups({ OldGroup }), "repeated query must give the same answer");
    }

    // A delete-only GC task drains the queue and sets no barrier, and the gate must still wait for its commit.
    Y_UNIT_TEST(TestMoveDataGateHeldWhileDeleteOnlyGCInFlight) {
        auto controllerGuard = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TReadOnlyController>();
        TActorSystemStub actorSystemStub;
        actorSystemStub.AppData.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        static constexpr ui64 TabletId = 44;
        static constexpr ui32 OldGroup = 100;
        static constexpr ui32 NewGroup = 200;
        static constexpr ui32 ReassignGen = 5;
        static constexpr ui32 TabletGen = 3;

        auto tabletInfo = MakeTabletInfo(TabletId, { { 0, OldGroup }, { ReassignGen, NewGroup } }, TBlobStorageGroupType::ErasureNone);
        auto mgr = std::make_shared<NOlap::TBlobManager>(tabletInfo, TabletGen, NOlap::TTabletId(TabletId));
        auto shared = std::make_shared<NOlap::NDataSharing::TStorageSharedBlobsManager>(
            NOlap::NBlobOperations::TGlobal::DefaultStorageId, NOlap::TTabletId(TabletId));
        NOlap::NBlobOperations::TStorageCounters storageCounters(NOlap::NBlobOperations::TGlobal::DefaultStorageId);
        auto counters = storageCounters.GetConsumerCounter(NOlap::NBlobOperations::EConsumer::GC)->GetRemoveGCCounters();

        // The first GC of an incarnation collects up to the current step; once it commits, the next task has no barrier to set.
        const NOlap::TGenStep barrier(TabletGen, 0);
        UNIT_ASSERT_C(
            mgr->BuildGCTask(NOlap::NBlobOperations::TGlobal::DefaultStorageId, mgr, shared, counters), "the first GC must set a barrier");
        mgr->OnGCStartOnComplete(barrier);
        mgr->OnGCFinishedOnComplete(barrier);

        mgr->DeleteBlobOnComplete(NOlap::TTabletId(TabletId), MakeDsBlobId(OldGroup, TabletId, 1, 1, 2));
        auto task = mgr->BuildGCTask(NOlap::NBlobOperations::TGlobal::DefaultStorageId, mgr, shared, counters);
        UNIT_ASSERT_C(task, "a queued delete must produce a GC task");
        UNIT_ASSERT_C(mgr->HasBlobsForGroups({ OldGroup }), "the gate must stay closed while a delete-only GC task is in flight");

        mgr->OnGCFinishedOnComplete(std::nullopt);
        UNIT_ASSERT_C(!mgr->HasBlobsForGroups({ OldGroup }), "the gate must open once the task commits");
    }

    // Empty queues are not barrier coverage: until the first GC round of this incarnation commits, nothing proves the old generations are collected.
    Y_UNIT_TEST(FirstGCRoundIsRequiredBeforeTheGateOpens) {
        auto controllerGuard = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TReadOnlyController>();
        TActorSystemStub actorSystemStub;
        actorSystemStub.AppData.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        static constexpr ui64 TabletId = 45;
        static constexpr ui32 OldGroup = 100;
        static constexpr ui32 NewGroup = 200;
        static constexpr ui32 ReassignGen = 5;
        static constexpr ui32 TabletGen = 7;

        auto tabletInfo = MakeTabletInfo(TabletId, { { 0, OldGroup }, { ReassignGen, NewGroup } }, TBlobStorageGroupType::ErasureNone);
        auto mgr = std::make_shared<NOlap::TBlobManager>(tabletInfo, TabletGen, NOlap::TTabletId(TabletId));
        auto shared = std::make_shared<NOlap::NDataSharing::TStorageSharedBlobsManager>(
            NOlap::NBlobOperations::TGlobal::DefaultStorageId, NOlap::TTabletId(TabletId));
        NOlap::NBlobOperations::TStorageCounters storageCounters(NOlap::NBlobOperations::TGlobal::DefaultStorageId);
        auto counters = storageCounters.GetConsumerCounter(NOlap::NBlobOperations::EConsumer::GC)->GetRemoveGCCounters();

        UNIT_ASSERT_C(!mgr->HasBlobsForGroups({ OldGroup }), "the queues start empty");
        UNIT_ASSERT_C(!mgr->HasCollectedBeforeCurrentGeneration(), "empty queues alone must not answer for barrier coverage");

        UNIT_ASSERT_C(
            mgr->BuildGCTask(NOlap::NBlobOperations::TGlobal::DefaultStorageId, mgr, shared, counters), "the first GC must set a barrier");
        const NOlap::TGenStep barrier(TabletGen, 0);
        mgr->OnGCStartOnComplete(barrier);
        UNIT_ASSERT_C(!mgr->HasCollectedBeforeCurrentGeneration(), "a barrier that BlobStorage has not acknowledged proves nothing");

        mgr->OnGCFinishedOnComplete(barrier);
        UNIT_ASSERT_C(mgr->HasCollectedBeforeCurrentGeneration(), "the committed first round covers every earlier generation");
    }

    // A failed rewrite returns its portion through AddPortion; it must re-enter Pending even past the admission deadline.
    Y_UNIT_TEST(SubmittedPortionReentersPendingWhenItsChangeFails) {
        static constexpr ui64 PortionId = 7;
        TActualizerSchema schema(NOlap::NTest::MakePortionTestIndexInfo());
        TMoveDataActualizerTestable actualizer(THashSet<ui32>{ 100 }, schema.Index);
        const TInstant start = TInstant::Seconds(1000);
        actualizer.Refresh(NOlap::NActualizer::TAddExternalContext(start, {}), {});

        actualizer.AddToInitialAndPendingForTest(PortionId);
        actualizer.ConfirmPortionForTest(PortionId);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().ConfirmedToMove, 1);

        actualizer.SimulateTaskSubmissionForTest(PortionId);
        UNIT_ASSERT_C(!actualizer.IsInPortionsToMove(PortionId), "a submitted portion must leave PortionsToMove");
        // In flight still counts: old blobs enter the delete queues only on commit.
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().InFlight, 1);

        // Past the deadline only InitialPortionIds membership admits the returned portion, so it must have survived submission.
        const THashMap<ui64, NOlap::TPortionInfo::TPtr> noPortions;
        actualizer.AddPortion(
            MakeDefaultTierPortion(PortionId), NOlap::NActualizer::TAddExternalContext(start + TDuration::Hours(1), noPortions));
        UNIT_ASSERT_C(actualizer.IsInPendingPortionIds(PortionId), "a portion whose rewrite failed must re-enter PendingPortionIds");
        // Moved from in-flight back to pending: counted once, not twice.
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().GetTotal(), 1);
    }

    // Keep leg: the group is resolved through TabletInfo->GroupFor(channel, generation).
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

    // Shared/borrowed leg: the group comes from the persisted DS:<group>:<id> form, not our history.
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

    // Exercised directly: a TPortionDataAccessor needs arrow-backed metadata to build.
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

    Y_UNIT_TEST(FreezeCleanupWatermarkRaisesToRunningOldest) {
        using NOlap::NActualizer::FreezeCleanupWatermark;
        const TInstant kT = TInstant::Seconds(100);
        UNIT_ASSERT_VALUES_EQUAL(FreezeCleanupWatermark(TInstant::Zero(), kT), kT);
        UNIT_ASSERT_VALUES_EQUAL(FreezeCleanupWatermark(kT, std::nullopt), kT);
        UNIT_ASSERT_VALUES_EQUAL(FreezeCleanupWatermark(kT - TDuration::Seconds(1), kT), kT);
        UNIT_ASSERT_VALUES_EQUAL(FreezeCleanupWatermark(kT, kT), kT);
        UNIT_ASSERT_VALUES_EQUAL(FreezeCleanupWatermark(TInstant::Zero(), std::nullopt), TInstant::Zero());
    }

    // Portions created mid-session still land in the doomed group; the deadline bounds adoption.
    Y_UNIT_TEST(AdoptsPortionsCreatedDuringTheSessionUntilTheDeadline) {
        TActualizerSchema schema(NOlap::NTest::MakePortionTestIndexInfo());
        TMoveDataActualizerTestable actualizer(THashSet<ui32>{ 100 }, schema.Index);
        const TInstant start = TInstant::Seconds(1000);
        actualizer.Refresh(NOlap::NActualizer::TAddExternalContext(start, {}), {});

        const THashMap<ui64, NOlap::TPortionInfo::TPtr> noPortions;
        actualizer.AddPortion(MakeDefaultTierPortion(1), NOlap::NActualizer::TAddExternalContext(start + TDuration::Minutes(1), noPortions));
        UNIT_ASSERT_C(actualizer.IsInPendingPortionIds(1), "a portion created inside the window must be adopted");

        actualizer.AddPortion(MakeDefaultTierPortion(2), NOlap::NActualizer::TAddExternalContext(start + TDuration::Hours(1), noPortions));
        UNIT_ASSERT_C(!actualizer.IsInPendingPortionIds(2), "past the deadline the session must stop adopting");
    }

    // A compaction-level move removes and re-adds the same portion; past the deadline it must still be moved.
    Y_UNIT_TEST(PortionChangedAfterTheDeadlineIsStillMoved) {
        TActualizerSchema schema(NOlap::NTest::MakePortionTestIndexInfo());
        TMoveDataActualizerTestable actualizer(THashSet<ui32>{ 100 }, schema.Index);
        const TInstant start = TInstant::Seconds(1000);
        const auto portion = MakeDefaultTierPortion(1);
        const THashMap<ui64, NOlap::TPortionInfo::TPtr> portions{ { 1, portion } };
        actualizer.Refresh(NOlap::NActualizer::TAddExternalContext(start, portions), {});
        UNIT_ASSERT_C(actualizer.IsInPendingPortionIds(1), "a default-tier portion must be admitted at session start");

        const THashMap<ui64, NOlap::TPortionInfo::TPtr> noPortions;
        actualizer.RemovePortion(1);
        actualizer.AddPortion(portion, NOlap::NActualizer::TAddExternalContext(start + TDuration::Hours(1), noPortions));
        UNIT_ASSERT_C(actualizer.IsInPendingPortionIds(1), "a portion the session started with must stay tracked across a level move");
    }

    Y_UNIT_TEST(MoveDataMetadataRequestsBatching) {
        static constexpr ui64 PortionsCount = 7;
        const THashSet<ui32> targetGroups = { 100 };
        TActualizerSchema schema(NOlap::NTest::MakePortionTestIndexInfo());

        THashMap<ui64, NOlap::TPortionInfo::TPtr> portions;
        for (ui64 portionId = 1; portionId <= PortionsCount; ++portionId) {
            portions.emplace(portionId, MakeDefaultTierPortion(portionId));
        }
        const ui64 portionMemory = portions.at(1)->PredictAccessorsMemory(portions.at(1)->GetSchema(schema.Index));
        UNIT_ASSERT_GT(portionMemory, 0);

        auto buildWithLimit = [&](const ui64 softLimit, const THashMap<ui64, NOlap::TPortionInfo::TPtr>& knownPortions) {
            auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TSoftMemoryLimitController>(softLimit);
            auto actualizer = std::make_shared<TMoveDataActualizerTestable>(targetGroups, schema.Index);
            for (ui64 portionId = 1; portionId <= PortionsCount; ++portionId) {
                actualizer->AddToInitialAndPendingForTest(portionId);
            }
            return actualizer->BuildMoveDataMetadataRequests(knownPortions, {}, actualizer, TInstant::Seconds(1000));
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
            auto actualizer = std::make_shared<TMoveDataActualizerTestable>(targetGroups, schema.Index);
            UNIT_ASSERT(actualizer->BuildMoveDataMetadataRequests(portions, {}, actualizer, TInstant::Seconds(1000)).empty());
        }
    }

    // Every background pass and every reply rebuilds the requests, so a pending portion must be asked for once until answered.
    Y_UNIT_TEST(PendingPortionIsRequestedOnceUntilAnswered) {
        TActualizerSchema schema(NOlap::NTest::MakePortionTestIndexInfo());
        THashMap<ui64, NOlap::TPortionInfo::TPtr> portions;
        for (ui64 portionId = 1; portionId <= 3; ++portionId) {
            portions.emplace(portionId, MakeDefaultTierPortion(portionId));
        }
        // A zero soft limit makes every portion its own request.
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TSoftMemoryLimitController>(0);
        auto actualizer = std::make_shared<TMoveDataActualizerTestable>(THashSet<ui32>{ 100 }, schema.Index);
        for (ui64 portionId = 1; portionId <= 3; ++portionId) {
            actualizer->AddToInitialAndPendingForTest(portionId);
        }
        auto requested = [&](const TInstant now) {
            TVector<ui64> result;
            for (auto&& request : actualizer->BuildMoveDataMetadataRequests(portions, {}, actualizer, now)) {
                for (auto&& portionId : request.GetRequest()->GetPortionIds()) {
                    result.emplace_back(portionId);
                }
            }
            Sort(result);
            return result;
        };

        const TInstant start = TInstant::Seconds(1000);
        UNIT_ASSERT_VALUES_EQUAL(requested(start), (TVector<ui64>{ 1, 2, 3 }));
        UNIT_ASSERT_VALUES_EQUAL_C(requested(start + TDuration::Seconds(1)), TVector<ui64>(), "outstanding requests must not be repeated");

        // An answer that could not resolve portion 1 leaves it pending, so it is asked for again.
        actualizer->OnMetadataRequestAnswered({ 1 }, start);
        UNIT_ASSERT_VALUES_EQUAL(requested(start + TDuration::Seconds(2)), (TVector<ui64>{ 1 }));

        // Requests that never got an answer are repeated after the expiry; the fresh request for portion 1 is not.
        const TInstant pastExpiry = start + NOlap::NActualizer::TMoveDataActualizer::MetadataRequestExpiry + TDuration::Seconds(1);
        UNIT_ASSERT_VALUES_EQUAL(requested(pastExpiry), (TVector<ui64>{ 2, 3 }));
    }

    // Request A expires and B replaces it: a late answer to A must leave B outstanding.
    Y_UNIT_TEST(LateAnswerToExpiredRequestKeepsItsSuccessor) {
        TActualizerSchema schema(NOlap::NTest::MakePortionTestIndexInfo());
        const THashMap<ui64, NOlap::TPortionInfo::TPtr> portions = { { 1, MakeDefaultTierPortion(1) } };
        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TSoftMemoryLimitController>(0);
        auto actualizer = std::make_shared<TMoveDataActualizerTestable>(THashSet<ui32>{ 100 }, schema.Index);
        actualizer->AddToInitialAndPendingForTest(1);
        auto requestCount = [&](const TInstant now) {
            return actualizer->BuildMoveDataMetadataRequests(portions, {}, actualizer, now).size();
        };

        const TInstant first = TInstant::Seconds(1000);
        const TInstant second = first + NOlap::NActualizer::TMoveDataActualizer::MetadataRequestExpiry + TDuration::Seconds(1);
        UNIT_ASSERT_VALUES_EQUAL(requestCount(first), 1);
        UNIT_ASSERT_VALUES_EQUAL_C(requestCount(second), 1, "the unanswered first request must expire");

        actualizer->OnMetadataRequestAnswered({ 1 }, first);
        UNIT_ASSERT_VALUES_EQUAL_C(
            requestCount(second + TDuration::Seconds(1)), 0, "a late answer to the expired request cleared its successor");

        actualizer->OnMetadataRequestAnswered({ 1 }, second);
        UNIT_ASSERT_VALUES_EQUAL_C(
            requestCount(second + TDuration::Seconds(2)), 1, "the answered portion is still pending, so it is asked for again");
    }

    // Tiered portions are admitted iff at least one entity resolves to DefaultStorageId.
    Y_UNIT_TEST(AdmissionAdmitsTieredPortionWithDefaultStorageEntity) {
        const THashSet<ui32> targetGroups = { 100 };
        const TInstant start = TInstant::Seconds(1000);

        // InheritPortionStorage=false: the index blob stays in BlobStorage, so the portion is admitted.
        {
            TActualizerSchema schema(MakeMaxIndexInfo(false));
            TMoveDataActualizerTestable actualizer(targetGroups, schema.Index);
            const THashMap<ui64, NOlap::TPortionInfo::TPtr> portions = { { 1, MakeTieredPortion(1, "tier1", schema.GetIndexInfo()) } };
            actualizer.Refresh(NOlap::NActualizer::TAddExternalContext(start, portions), {});
            UNIT_ASSERT_C(actualizer.IsInPendingPortionIds(1), "tiered portion with its index in BlobStorage must be admitted");
        }

        // InheritPortionStorage=true: every entity is in tier storage, so the portion is skipped.
        {
            TActualizerSchema schema(MakeMaxIndexInfo(true));
            TMoveDataActualizerTestable actualizer(targetGroups, schema.Index);
            const THashMap<ui64, NOlap::TPortionInfo::TPtr> portions = { { 2, MakeTieredPortion(2, "tier1", schema.GetIndexInfo()) } };
            actualizer.Refresh(NOlap::NActualizer::TAddExternalContext(start, portions), {});
            UNIT_ASSERT_C(!actualizer.IsInPendingPortionIds(2), "tiered portion with every entity in tier storage must be skipped");
        }
    }

    Y_UNIT_TEST(AbortedMoveReleasesAPortionThatStoppedQualifying) {
        static constexpr ui64 PortionId = 11;
        const TInstant start = TInstant::Seconds(1000);
        TActualizerSchema schema(MakeMaxIndexInfo(true));
        TMoveDataActualizerTestable actualizer(THashSet<ui32>{ 100 }, schema.Index);

        actualizer.AddToInitialAndPendingForTest(PortionId);
        actualizer.ConfirmPortionForTest(PortionId);
        actualizer.SimulateTaskSubmissionForTest(PortionId);
        UNIT_ASSERT_VALUES_EQUAL_C(actualizer.GetMoveDataQueueSizes().GetTotal(), 1, "a submitted portion must hold the response back");

        // The task aborted and the engine returned the portion, but it no longer has an entity in default storage.
        const THashMap<ui64, NOlap::TPortionInfo::TPtr> noPortions;
        actualizer.AddPortion(
            MakeTieredPortion(PortionId, "tier1", schema.GetIndexInfo()), NOlap::NActualizer::TAddExternalContext(start, noPortions));
        UNIT_ASSERT_VALUES_EQUAL_C(actualizer.GetMoveDataQueueSizes().GetTotal(), 0,
            "a returned portion that stopped qualifying must leave the queues, otherwise MoveDataResponse is never sent");
    }

}   // Y_UNIT_TEST_SUITE

}   // namespace NKikimr

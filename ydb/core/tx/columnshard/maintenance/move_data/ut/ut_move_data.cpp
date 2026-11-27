#include <ydb/core/tx/columnshard/data_locks/locks/list.h>
#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>
#include <ydb/core/tx/columnshard/data_sharing/manager/shared_blobs.h>
#include <ydb/core/tx/columnshard/engines/changes/actualization/construction/context.h>
#include <ydb/core/tx/columnshard/engines/portions/written.h>
#include <ydb/core/tx/columnshard/engines/scheme/objects_cache.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/move/move.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/min_max/meta.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/extractor/default.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/ro_controller.h>
#include <ydb/core/tx/columnshard/test_helper/portion_test_helper.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/size_literals.h>
#include <util/string/builder.h>

namespace NKikimr {

static constexpr ui32 BlobSize = 1_KB;
static constexpr ui32 OldGroup = 100;
static constexpr ui32 NewGroup = 200;

static NOlap::TUnifiedBlobId MakeDsBlobId(ui32 dsGroup, ui64 tabletId, ui32 gen, ui32 step, ui32 channel) {
    TLogoBlobID logo(tabletId, gen, step, channel, BlobSize, 0);
    return NOlap::TUnifiedBlobId(dsGroup, logo);
}

// Every actualizer test lives under one path; portion ids alone tell the cases apart.
static NOlap::TInternalPathId TestPathId() {
    return NOlap::TInternalPathId::FromRawValue(1);
}

static NOlap::TPortionInfo::TPtr MakeDefaultTierPortion(const ui64 portionId) {
    return NOlap::NTest::MakeTestCompactedPortion(TestPathId(), portionId, 10, 19, 10, NOlap::TSnapshot(1, 1), std::nullopt);
}

// Owns the cache and the versioned index an actualizer refers to for the whole test.
struct TActualizerSchema {
    std::shared_ptr<NOlap::TSchemaObjectsCache> Cache = std::make_shared<NOlap::TSchemaObjectsCache>();
    NOlap::TVersionedIndex Index;

    explicit TActualizerSchema(NOlap::TIndexInfo&& indexInfo) {
        Index.AddIndex(NOlap::TSnapshot(1, 1), Cache->GetOrInsertIndexInfo(std::move(indexInfo)));
    }

    const NOlap::TIndexInfo& GetIndexInfo() const {
        return Index.GetLastSchema()->GetIndexInfo();
    }
};

Y_UNIT_TEST_SUITE(TMoveDataTest) {
    // Borrowed leg: the group comes from the persisted DS:<group>:<id> form, not our history.
    Y_UNIT_TEST(TestMoveDataBorrowedBlobs) {
        static constexpr ui64 TabletId = 46;
        static constexpr ui64 ForeignTabletId = 99;

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
    }

    Y_UNIT_TEST(CandidateReconciliationIsBoundedWhileErasingTerminalEntries) {
        TActualizerSchema schema(NOlap::NTest::MakePortionTestIndexInfo());
        NOlap::NActualizer::TMoveDataActualizer actualizer(schema.Index);
        THashMap<ui64, NOlap::TPortionInfo::TPtr> portions;
        constexpr size_t PortionCount = 4096;
        for (ui64 id = 1; id <= PortionCount; ++id) {
            actualizer.AddCandidate(id, true);
        }
        const auto selected = actualizer.GetMoveDataQueueSizes().ConfirmedToMove;
        UNIT_ASSERT_VALUES_EQUAL(selected, PortionCount);
        actualizer.UpdatePortionStates(portions, {});
        UNIT_ASSERT_VALUES_EQUAL_C(selected - actualizer.GetMoveDataQueueSizes().GetTotal(), 1024,
            "one pass must erase exactly its candidate budget without revisiting entries");
        for (ui32 pass = 1; pass < 5; ++pass) {
            actualizer.UpdatePortionStates(portions, {});
        }
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().GetTotal(), 0);
        actualizer.UpdatePortionStates(portions, {});   // Re-enter after erasing the last candidate.
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().GetTotal(), 0);
    }

    Y_UNIT_TEST(RewriteRetriesAfterLockReleaseWithoutAddPortion) {
        TActualizerSchema schema(NOlap::NTest::MakePortionTestIndexInfo());
        NOlap::NActualizer::TMoveDataActualizer actualizer(schema.Index);
        auto portion = MakeDefaultTierPortion(1);
        THashMap<ui64, NOlap::TPortionInfo::TPtr> portions{ { 1, portion } };
        actualizer.AddCandidate(1, true);
        auto locks = std::make_shared<NOlap::NDataLocks::TManager>();
        NColumnShard::TEngineLogsCounters counters;
        auto controller = std::make_shared<NOlap::NActualizer::TController>();
        const auto extract = [&] {
            NOlap::NActualizer::TTieringProcessContext tasks(
                512_MB, NOlap::TSaverContext(NOlap::TTestStoragesManager::GetInstance()), locks, schema.Index, counters, controller);
            NOlap::NActualizer::TInternalTasksContext internal;
            actualizer.ExtractTasks(tasks, NOlap::NActualizer::TExternalTasksContext(portions), internal);
            size_t result = 0;
            for (const auto& [_, addressTasks] : tasks.GetTasks()) {
                result += addressTasks.size();
            }
            return result;
        };
        UNIT_ASSERT_VALUES_EQUAL(extract(), 1);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().RewriteStarted, 1);
        auto lock = locks->RegisterLock<NOlap::NDataLocks::TListPortionsLock>(
            "move", std::vector<NOlap::TPortionInfo::TConstPtr>{ portion }, NOlap::NDataLocks::ELockCategory::Actualization);
        UNIT_ASSERT_VALUES_EQUAL(extract(), 0);
        // An aborted rewrite releases its lock. Deliberately omit ReturnToIndexes/AddPortion.
        lock->Release(*locks);
        UNIT_ASSERT_VALUES_EQUAL(extract(), 1);
        portion->SetRemoveSnapshot(NOlap::TSnapshot(2, 1));
        // Even before reconciliation, retirement must prevent another rewrite.
        UNIT_ASSERT_VALUES_EQUAL(extract(), 0);
        actualizer.UpdatePortionStates(portions, {});
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().RewriteStarted, 0);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().GetTotal(), 0);
        portions.clear();
        actualizer.UpdatePortionStates(portions, {});
        UNIT_ASSERT_VALUES_EQUAL(extract(), 0);   // The rewrite cursor survives erasure of its last candidate.
    }

    // DoIsLocked ignores the category, so Actualization-vs-Compaction separation rests on LockCategoriesInteraction; assert both ways.
    Y_UNIT_TEST(ActualizationLockAndCompactionExcludeEachOther) {
        using namespace NOlap::NDataLocks;
        const auto portion = MakeDefaultTierPortion(1);
        const std::vector<NOlap::TPortionInfo::TConstPtr> portions{ portion };

        {
            TManager manager;
            auto guard = manager.RegisterLock<TListPortionsLock>("movedata::rewrite", portions, ELockCategory::Actualization);
            UNIT_ASSERT_C(manager.IsLocked(*portion, ELockCategory::Compaction),
                "a compaction planner must see the move's Actualization lock, or it can pick a portion being rewritten");
            UNIT_ASSERT_C(manager.IsLocked(*portion, ELockCategory::Actualization), "the move must see its own lock");
            UNIT_ASSERT_C(!manager.IsLocked(*portion, ELockCategory::Scan), "a reader must not be blocked by the move");
        }
        {
            TManager manager;
            auto guard = manager.RegisterLock<TListPortionsLock>("compaction::merge", portions, ELockCategory::Compaction);
            UNIT_ASSERT_C(manager.IsLocked(*portion, ELockCategory::Actualization),
                "the move must see a compaction lock, or DoExtractTasks hands out a portion compaction owns");
        }
        {
            TManager manager;
            const std::vector<NOlap::TPortionInfo::TConstPtr> other{ MakeDefaultTierPortion(2) };
            auto guard = manager.RegisterLock<TListPortionsLock>("movedata::other", other, ELockCategory::Actualization);
            UNIT_ASSERT_C(!manager.IsLocked(*portion, ELockCategory::Compaction), "a lock on another portion must not block this one");
        }
    }

}   // Y_UNIT_TEST_SUITE

}   // namespace NKikimr

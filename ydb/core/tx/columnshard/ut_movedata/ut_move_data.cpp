#include <ydb/core/tx/columnshard/data_locks/locks/list.h>
#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>
#include <ydb/core/tx/columnshard/data_sharing/manager/shared_blobs.h>
#include <ydb/core/tx/columnshard/engines/changes/actualization/construction/context.h>
#include <ydb/core/tx/columnshard/engines/portions/written.h>
#include <ydb/core/tx/columnshard/engines/scheme/objects_cache.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/move/move.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/max/meta.h>
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
    constructor.SetSchemaVersion(indexInfo.GetVersion());
    constructor.SetAppearanceSnapshot(NOlap::TSnapshot(1, 1));
    constructor.MutableMeta() = metaConstructor;
    return constructor.Build();
}

// A compacted default-tier portion: admission accepts it without a schema lookup.
static NOlap::TPortionInfo::TPtr MakeDefaultTierPortion(const ui64 portionId) {
    return NOlap::NTest::MakeTestCompactedPortion(TestPathId(), portionId, 10, 19, 10, NOlap::TSnapshot(1, 1), std::nullopt);
}

// Classification reads only the immutable blob identities; chunk metadata is irrelevant to these queue transitions.
static void ClassifyPortion(NOlap::NActualizer::TMoveDataActualizer& actualizer, const NOlap::TPortionInfo::TPtr& portion, const ui32 group) {
    NOlap::TPortionDataAccessor accessor(portion, std::vector<NOlap::TUnifiedBlobId>{ MakeDsBlobId(group, 46, 1, 1, 2) },
        std::vector<NOlap::TColumnRecord>{}, std::vector<NOlap::TIndexChunk>{}, false);
    actualizer.ActualizePortionInfo(accessor);
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

    // Notifications neither adopt new writes nor lose a seeded candidate across a level move.
    Y_UNIT_TEST(NotificationsPreserveSeededMembershipAndRejections) {
        TActualizerSchema schema(NOlap::NTest::MakePortionTestIndexInfo());
        NOlap::NActualizer::TMoveDataActualizer actualizer({ OldGroup }, schema.Index);
        auto portion = MakeDefaultTierPortion(1);
        THashMap<ui64, NOlap::TPortionInfo::TPtr> portions{ { 1, portion } };
        const NOlap::NActualizer::TAddExternalContext context(TInstant::Seconds(1000), portions);
        actualizer.Seed(context, {});

        portions.emplace(2, MakeDefaultTierPortion(2));
        actualizer.AddPortion(portions.at(2), context);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().GetTotal(), 1);
        actualizer.RemovePortion(1);
        actualizer.AddPortion(portion, context);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().Pending, 1);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().Retired, 0);

        ClassifyPortion(actualizer, portion, NewGroup);
        actualizer.RemovePortion(1);
        actualizer.AddPortion(portion, context);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().Pending, 0);
        portion->SetRemoveSnapshot(NOlap::TSnapshot(2, 1));
        actualizer.UpdatePortionStates(portions, {});
        ClassifyPortion(actualizer, portion, NewGroup);   // A late result must not reclassify an erased candidate.
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().GetTotal(), 0);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().Retired, 0);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().Rejected, 1);
    }

    // A seeded portion that compaction or the move retired keeps the gate closed until cleanup erases it from the granule.
    Y_UNIT_TEST(RetiredSeededPortionHoldsTheGateUntilCleanupErasesIt) {
        TActualizerSchema schema(NOlap::NTest::MakePortionTestIndexInfo());
        NOlap::NActualizer::TMoveDataActualizer actualizer(THashSet<ui32>{ 100 }, schema.Index);
        const TInstant start = TInstant::Seconds(1000);
        const THashMap<ui64, std::shared_ptr<NOlap::TWrittenPortionInfo>> noUncommitted;
        THashMap<ui64, NOlap::TPortionInfo::TPtr> portions{ { 1, MakeDefaultTierPortion(1) } };
        actualizer.Seed(NOlap::NActualizer::TAddExternalContext(start, portions), noUncommitted);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().Pending, 1);

        ClassifyPortion(actualizer, portions.at(1), OldGroup);

        // No actualizer notification: retirement is discovered from the catalogue.
        const auto retiredPortion = portions.at(1);
        retiredPortion->SetRemoveSnapshot(NOlap::TSnapshot(2, 1));
        actualizer.UpdatePortionStates(portions, noUncommitted);
        const auto retired = actualizer.GetMoveDataQueueSizes();
        UNIT_ASSERT_VALUES_EQUAL_C(retired.GetTotal(), 0, "a retired portion is no longer waiting for a rewrite");
        UNIT_ASSERT_VALUES_EQUAL_C(retired.Retired, 1, "a retired portion that cleanup has not erased must hold the gate");

        // A portion the session did not seed is not ours, so its retirement holds nothing.
        portions.emplace(2, MakeDefaultTierPortion(2));
        portions.at(2)->SetRemoveSnapshot(NOlap::TSnapshot(2, 1));
        actualizer.UpdatePortionStates(portions, noUncommitted);
        UNIT_ASSERT_VALUES_EQUAL_C(actualizer.GetMoveDataQueueSizes().Retired, 1, "an unseeded portion must not count");

        portions.erase(1);
        actualizer.UpdatePortionStates(portions, noUncommitted);
        UNIT_ASSERT_VALUES_EQUAL_C(actualizer.GetMoveDataQueueSizes().Retired, 0, "the gate must open once cleanup erased the portion");

        // A late accessor response cannot resurrect a cleaned-up candidate.
        ClassifyPortion(actualizer, retiredPortion, OldGroup);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().GetTotal(), 0);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().Retired, 0);
    }

    // A portion seeded with a remove snapshot, as after an aborted write, waits for cleanup instead of a rewrite.
    Y_UNIT_TEST(PortionSeededAlreadyRetiredHoldsTheGateUntilCleanupErasesIt) {
        TActualizerSchema schema(NOlap::NTest::MakePortionTestIndexInfo());
        NOlap::NActualizer::TMoveDataActualizer actualizer(THashSet<ui32>{ 100 }, schema.Index);
        const TInstant start = TInstant::Seconds(1000);
        const THashMap<ui64, std::shared_ptr<NOlap::TWrittenPortionInfo>> noUncommitted;
        auto portion = MakeDefaultTierPortion(1);
        portion->SetRemoveSnapshot(NOlap::TSnapshot(2, 1));
        THashMap<ui64, NOlap::TPortionInfo::TPtr> portions{ { 1, portion } };
        actualizer.Seed(NOlap::NActualizer::TAddExternalContext(start, portions), noUncommitted);

        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().Pending, 1);
        ClassifyPortion(actualizer, portion, OldGroup);
        const auto seeded = actualizer.GetMoveDataQueueSizes();
        UNIT_ASSERT_VALUES_EQUAL_C(seeded.GetTotal(), 0, "a portion already retired has nothing to rewrite");
        UNIT_ASSERT_VALUES_EQUAL_C(seeded.Retired, 1, "its cleanup must still hold the gate");

        portions.erase(1);
        actualizer.UpdatePortionStates(portions, noUncommitted);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().Retired, 0);
    }

    Y_UNIT_TEST(RetirementDuringClassificationPreservesTheTargetCheck) {
        for (const bool Target : { false, true }) {
            TActualizerSchema schema(NOlap::NTest::MakePortionTestIndexInfo());
            auto actualizer = std::make_shared<NOlap::NActualizer::TMoveDataActualizer>(THashSet<ui32>{ OldGroup }, schema.Index);
            auto portion = MakeDefaultTierPortion(1);
            THashMap<ui64, NOlap::TPortionInfo::TPtr> portions{ { 1, portion } };
            actualizer->Seed(NOlap::NActualizer::TAddExternalContext(TInstant::Seconds(1000), portions), {});
            portion->SetRemoveSnapshot(NOlap::TSnapshot(2, 1));
            actualizer->UpdatePortionStates(portions, {});
            UNIT_ASSERT_VALUES_EQUAL(actualizer->GetMoveDataQueueSizes().Pending, 1);
            UNIT_ASSERT_VALUES_EQUAL(actualizer->GetMoveDataQueueSizes().Retired, 0);

            NOlap::NActualizer::TMoveDataMetadataBudget budget(0);
            UNIT_ASSERT_VALUES_EQUAL(actualizer->BuildMoveDataMetadataRequests(portions, {}, actualizer, budget).size(), 1);
            ClassifyPortion(*actualizer, portion, Target ? OldGroup : NewGroup);
            UNIT_ASSERT_VALUES_EQUAL(actualizer->GetMoveDataQueueSizes().GetTotal(), 0);
            UNIT_ASSERT_VALUES_EQUAL(actualizer->GetMoveDataQueueSizes().Retired, Target ? 1 : 0);
            UNIT_ASSERT_VALUES_EQUAL(actualizer->GetMoveDataQueueSizes().ConfirmedToMove, 0);
            UNIT_ASSERT_VALUES_EQUAL(actualizer->GetMoveDataQueueSizes().Rejected, Target ? 0 : 1);
        }
    }

    Y_UNIT_TEST(CleanupBeforeClassificationDoesNotLeavePendingWork) {
        TActualizerSchema schema(NOlap::NTest::MakePortionTestIndexInfo());
        NOlap::NActualizer::TMoveDataActualizer actualizer({ OldGroup }, schema.Index);
        auto portion = MakeDefaultTierPortion(1);
        portion->SetRemoveSnapshot(NOlap::TSnapshot(2, 1));
        THashMap<ui64, NOlap::TPortionInfo::TPtr> portions{ { 1, portion } };
        actualizer.Seed(NOlap::NActualizer::TAddExternalContext(TInstant::Seconds(1000), portions), {});
        portions.erase(1);
        actualizer.UpdatePortionStates(portions, {});
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().GetTotal(), 0);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().Retired, 0);

        // The callback can arrive after cleanup; it must not reinsert the old portion.
        ClassifyPortion(actualizer, portion, OldGroup);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().GetTotal(), 0);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().Retired, 0);
    }

    Y_UNIT_TEST(CandidateReconciliationIsBoundedWhileErasingTerminalEntries) {
        TActualizerSchema schema(NOlap::NTest::MakePortionTestIndexInfo());
        NOlap::NActualizer::TMoveDataActualizer actualizer({ OldGroup }, schema.Index);
        THashMap<ui64, NOlap::TPortionInfo::TPtr> portions;
        constexpr ui64 PortionCount = 4097;
        for (ui64 id = 1; id <= PortionCount; ++id) {
            portions.emplace(id, MakeDefaultTierPortion(id));
        }
        actualizer.Seed(NOlap::NActualizer::TAddExternalContext(TInstant::Seconds(1000), portions), {});
        // Reject interleaved candidates before the first bounded pass.
        for (ui64 id = 1; id <= PortionCount; ++id) {
            ClassifyPortion(actualizer, portions.at(id), id % 2 ? NewGroup : OldGroup);
        }
        const auto selected = actualizer.GetMoveDataQueueSizes().ConfirmedToMove;
        UNIT_ASSERT_VALUES_EQUAL(selected, PortionCount / 2);
        portions.clear();
        actualizer.UpdatePortionStates(portions, {});
        UNIT_ASSERT_VALUES_EQUAL_C(selected - actualizer.GetMoveDataQueueSizes().GetTotal(), 1024,
            "one pass must erase exactly its candidate budget without revisiting entries");
        for (ui32 pass = 1; pass < 5; ++pass) {
            actualizer.UpdatePortionStates(portions, {});
        }
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().GetTotal(), 0);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().Rejected, (PortionCount + 1) / 2);
        actualizer.UpdatePortionStates(portions, {});   // Re-enter after erasing the last candidate.
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().GetTotal(), 0);
    }

    Y_UNIT_TEST(RewriteRetriesAfterLockReleaseWithoutAddPortion) {
        TActualizerSchema schema(NOlap::NTest::MakePortionTestIndexInfo());
        NOlap::NActualizer::TMoveDataActualizer actualizer({ OldGroup }, schema.Index);
        auto portion = MakeDefaultTierPortion(1);
        THashMap<ui64, NOlap::TPortionInfo::TPtr> portions{ { 1, portion } };
        actualizer.Seed(NOlap::NActualizer::TAddExternalContext(TInstant::Seconds(1000), portions), {});
        ClassifyPortion(actualizer, portion, OldGroup);
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
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().Retired, 1);
        portions.clear();
        actualizer.UpdatePortionStates(portions, {});
        UNIT_ASSERT_VALUES_EQUAL(extract(), 0);   // The rewrite cursor survives erasure of its last candidate.
    }

    Y_UNIT_TEST(MoveDataMetadataRequestsUsePortionSchema) {
        static constexpr ui64 PortionsCount = 7;
        TActualizerSchema schema(NOlap::NTest::MakePortionTestIndexInfo());
        NKikimrSchemeOp::TColumnTableSchema wideSchema;
        wideSchema.SetVersion(2);
        wideSchema.AddKeyColumnNames("pk");
        for (ui32 columnId = 0; columnId < 64; ++columnId) {
            const TString name = columnId ? TString(TStringBuilder() << "value" << columnId) : "pk";
            *wideSchema.AddColumns() = NArrow::NTest::TTestColumn(name, NScheme::TTypeInfo(NScheme::NTypeIds::Uint64)).CreateColumn(columnId);
        }
        auto wideIndexInfo = NOlap::TIndexInfo::BuildFromProto(1, wideSchema, NOlap::TTestStoragesManager::GetInstance(), schema.Cache);
        UNIT_ASSERT(wideIndexInfo);
        schema.Index.AddIndex(NOlap::TSnapshot(2, 1), schema.Cache->GetOrInsertIndexInfo(std::move(*wideIndexInfo)));

        // Rotate the wide portion through every id so the check does not depend on hash iteration order.
        for (ui64 widePortionId = 1; widePortionId <= PortionsCount; ++widePortionId) {
            THashMap<ui64, NOlap::TPortionInfo::TPtr> portions;
            ui64 totalMemory = 0;
            ui64 narrowMemory = 0;
            ui64 wideMemory = 0;
            for (ui64 portionId = 1; portionId <= PortionsCount; ++portionId) {
                auto portion = MakeDefaultTierPortion(portionId);
                if (portionId == widePortionId) {
                    portion = MakeTieredPortion(portionId, NOlap::IStoragesManager::DefaultStorageId, schema.GetIndexInfo());
                }
                const ui64 memory = portion->PredictAccessorsMemory(portion->GetSchema(schema.Index));
                totalMemory += memory;
                if (portionId == widePortionId) {
                    wideMemory = memory;
                } else {
                    narrowMemory = memory;
                }
                portions.emplace(portionId, std::move(portion));
            }
            UNIT_ASSERT_GT(wideMemory, (PortionsCount - 1) * narrowMemory);
            NOlap::NActualizer::TMoveDataMetadataBudget budget(totalMemory + 1);
            auto actualizer = std::make_shared<NOlap::NActualizer::TMoveDataActualizer>(THashSet<ui32>{ 100 }, schema.Index);
            actualizer->Seed(NOlap::NActualizer::TAddExternalContext(TInstant::Seconds(1000), portions), {});
            const auto requests = actualizer->BuildMoveDataMetadataRequests(portions, {}, actualizer, budget);
            UNIT_ASSERT_VALUES_EQUAL_C(requests.size(), 1, "the sum of per-portion estimates fits in one batch");
            UNIT_ASSERT_VALUES_EQUAL(requests.front().GetRequest()->GetSize(), PortionsCount);
        }
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
            auto actualizer = std::make_shared<NOlap::NActualizer::TMoveDataActualizer>(targetGroups, schema.Index);
            actualizer->Seed(NOlap::NActualizer::TAddExternalContext(TInstant::Seconds(1000), portions), {});
            actualizer->UpdatePortionStates(knownPortions, {});
            std::vector<NOlap::TCSMetadataRequest> requests;
            while (actualizer->GetMoveDataQueueSizes().Pending) {
                NOlap::NActualizer::TMoveDataMetadataBudget budget(softLimit);
                auto batch = actualizer->BuildMoveDataMetadataRequests(knownPortions, {}, actualizer, budget);
                UNIT_ASSERT_VALUES_EQUAL(batch.size(), 1);
                for (ui64 id : batch.front().GetRequest()->GetPortionIds()) {
                    ClassifyPortion(*actualizer, knownPortions.at(id), OldGroup);
                }
                requests.emplace_back(std::move(batch.front()));
            }
            return requests;
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
                    result.emplace(portionId);
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
    }

    Y_UNIT_TEST(MetadataBudgetIsSharedAcrossActualizers) {
        TActualizerSchema schema(NOlap::NTest::MakePortionTestIndexInfo());
        THashMap<ui64, NOlap::TPortionInfo::TPtr> firstPortions{ { 1, MakeDefaultTierPortion(1) } };
        THashMap<ui64, NOlap::TPortionInfo::TPtr> secondPortions{ { 2, MakeDefaultTierPortion(2) }, { 3, MakeDefaultTierPortion(3) } };
        auto first = std::make_shared<NOlap::NActualizer::TMoveDataActualizer>(THashSet<ui32>{ OldGroup }, schema.Index);
        auto second = std::make_shared<NOlap::NActualizer::TMoveDataActualizer>(THashSet<ui32>{ OldGroup }, schema.Index);
        first->Seed(NOlap::NActualizer::TAddExternalContext(TInstant::Seconds(1000), firstPortions), {});
        second->Seed(NOlap::NActualizer::TAddExternalContext(TInstant::Seconds(1000), secondPortions), {});
        const ui64 memory = firstPortions.at(1)->PredictAccessorsMemory(firstPortions.at(1)->GetSchema(schema.Index));
        for (const bool limitByMemory : { false, true }) {
            NOlap::NActualizer::TMoveDataMetadataBudget budget(limitByMemory ? memory + 1 : 100 * memory);
            budget.PortionsLeft = limitByMemory ? 1024 : 2;
            const auto firstRequests = first->BuildMoveDataMetadataRequests(firstPortions, {}, first, budget);
            UNIT_ASSERT_VALUES_EQUAL(firstRequests.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(firstRequests.front().GetRequest()->GetSize(), 1);
            const auto requests = second->BuildMoveDataMetadataRequests(secondPortions, {}, second, budget);
            UNIT_ASSERT_VALUES_EQUAL(requests.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(requests.front().GetRequest()->GetSize(), 1);
            UNIT_ASSERT(budget.IsExhausted());
            UNIT_ASSERT(second->BuildMoveDataMetadataRequests(secondPortions, {}, second, budget).empty());
        }
    }

    Y_UNIT_TEST(MetadataScanResumesAndRetriesWithoutResults) {
        TActualizerSchema schema(NOlap::NTest::MakePortionTestIndexInfo());
        auto actualizer = std::make_shared<NOlap::NActualizer::TMoveDataActualizer>(THashSet<ui32>{ OldGroup }, schema.Index);
        THashMap<ui64, NOlap::TPortionInfo::TPtr> portions;
        constexpr ui64 PortionCount = 2051;
        for (ui64 id = 1; id <= PortionCount; ++id) {
            portions.emplace(id, MakeDefaultTierPortion(id));
        }
        actualizer->Seed(NOlap::NActualizer::TAddExternalContext(TInstant::Seconds(1000), portions), {});
        THashSet<ui64> requested;
        for (ui32 pass = 0; pass < 3; ++pass) {
            NOlap::NActualizer::TMoveDataMetadataBudget budget(Max<ui64>());
            const auto batch = actualizer->BuildMoveDataMetadataRequests(portions, {}, actualizer, budget);
            UNIT_ASSERT_VALUES_EQUAL(batch.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(batch.front().GetRequest()->GetSize(), 1024);
            UNIT_ASSERT_VALUES_EQUAL(budget.PortionsLeft, 0);
            for (ui64 id : batch.front().GetRequest()->GetPortionIds()) {
                const bool fresh = requested.emplace(id).second;
                UNIT_ASSERT_C(pass == 2 || fresh, "pending portions must progress before retrying earlier failures");
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(requested.size(), PortionCount);
        // No callback succeeded, so all portions remain pending for a later retry.
        UNIT_ASSERT_VALUES_EQUAL(actualizer->GetMoveDataQueueSizes().Pending, PortionCount);

        // Replies can finish almost every candidate between batches without invalidating the scan position.
        for (ui64 id = 2; id <= PortionCount; ++id) {
            ClassifyPortion(*actualizer, portions.at(id), NewGroup);
        }
        UNIT_ASSERT_VALUES_EQUAL(actualizer->GetMoveDataQueueSizes().Pending, 1);
        UNIT_ASSERT_VALUES_EQUAL(actualizer->GetMoveDataQueueSizes().Rejected, PortionCount - 1);
        for (ui32 pass = 0; pass < 2; ++pass) {
            NOlap::NActualizer::TMoveDataMetadataBudget budget(Max<ui64>());
            const auto batch = actualizer->BuildMoveDataMetadataRequests(portions, {}, actualizer, budget);
            UNIT_ASSERT_VALUES_EQUAL_C(budget.PortionsLeft, 1023, "erased entries must consume neither memory nor scan budget");
            UNIT_ASSERT_VALUES_EQUAL(batch.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(batch.front().GetRequest()->GetSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(batch.front().GetRequest()->GetPortionIds().front(), 1);
        }

        portions.clear();
        for (ui32 pass = 0; pass < 3; ++pass) {
            actualizer->UpdatePortionStates(portions, {});
        }
        UNIT_ASSERT_VALUES_EQUAL(actualizer->GetMoveDataQueueSizes().GetTotal(), 0);
        UNIT_ASSERT_VALUES_EQUAL(actualizer->GetMoveDataQueueSizes().Rejected, PortionCount - 1);
        NOlap::NActualizer::TMoveDataMetadataBudget budget(Max<ui64>());
        UNIT_ASSERT(actualizer->BuildMoveDataMetadataRequests(portions, {}, actualizer, budget).empty());
    }

    // Tiered portions are admitted iff at least one entity resolves to DefaultStorageId.
    Y_UNIT_TEST(AdmissionAdmitsTieredPortionWithDefaultStorageEntity) {
        const THashSet<ui32> targetGroups = { 100 };
        const TInstant start = TInstant::Seconds(1000);

        // InheritPortionStorage=false: the index blob stays in BlobStorage, so the portion is admitted.
        {
            TActualizerSchema schema(MakeMaxIndexInfo(false));
            NOlap::NActualizer::TMoveDataActualizer actualizer(targetGroups, schema.Index);
            const THashMap<ui64, NOlap::TPortionInfo::TPtr> portions = { { 1, MakeTieredPortion(1, "tier1", schema.GetIndexInfo()) } };
            actualizer.Seed(NOlap::NActualizer::TAddExternalContext(start, portions), {});
            UNIT_ASSERT_VALUES_EQUAL_C(
                actualizer.GetMoveDataQueueSizes().Pending, 1, "tiered portion with its index in BlobStorage must be admitted");
        }

        // InheritPortionStorage=true: every entity is in tier storage, so the portion is skipped.
        {
            TActualizerSchema schema(MakeMaxIndexInfo(true));
            NOlap::NActualizer::TMoveDataActualizer actualizer(targetGroups, schema.Index);
            const THashMap<ui64, NOlap::TPortionInfo::TPtr> portions = { { 2, MakeTieredPortion(2, "tier1", schema.GetIndexInfo()) } };
            actualizer.Seed(NOlap::NActualizer::TAddExternalContext(start, portions), {});
            UNIT_ASSERT_VALUES_EQUAL_C(
                actualizer.GetMoveDataQueueSizes().GetTotal(), 0, "tiered portion with every entity in tier storage must be skipped");
        }
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

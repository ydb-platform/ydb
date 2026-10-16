#include "tablet_info_helper.h"

#include <ydb/core/tablet_flat/test/libs/table/test_dbase.h>
#include <ydb/core/testlib/actor_helpers.h>
#include <ydb/core/tx/columnshard/blobs_action/bs/blob_manager.h>
#include <ydb/core/tx/columnshard/blobs_action/bs/gc.h>
#include <ydb/core/tx/columnshard/blobs_action/counters/storage.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
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

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/size_literals.h>
#include <util/string/builder.h>

namespace NKikimr {

using NTestMoveData::MakeTabletInfo;

static constexpr ui32 BlobSize = 1_KB;
static constexpr ui32 OldGroup = 100;
static constexpr ui32 NewGroup = 200;
static constexpr ui32 ReassignGen = 5;

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
        Index.AddIndex(NOlap::TSnapshot(1, 1), Cache->UpsertIndexInfo(std::move(indexInfo)));
    }

    const NOlap::TIndexInfo& GetIndexInfo() const {
        return Index.GetLastSchema()->GetIndexInfo();
    }
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

// A blob manager of one generation over the OldGroup -> NewGroup history, with everything BuildGCTask needs.
struct TBlobManagerFixture {
    TActorSystemStub ActorSystemStub;
    TIntrusivePtr<TTabletStorageInfo> TabletInfo;
    std::shared_ptr<NOlap::TBlobManager> Manager;
    std::shared_ptr<NOlap::NDataSharing::TStorageSharedBlobsManager> Shared;
    std::shared_ptr<NOlap::NBlobOperations::TStorageCounters> StorageCounters;
    std::shared_ptr<NOlap::NBlobOperations::TRemoveGCCounters> Counters;

    TBlobManagerFixture(const ui64 tabletId, const ui32 tabletGen) {
        ActorSystemStub.AppData.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        TabletInfo = MakeTabletInfo(tabletId, { { 0, OldGroup }, { ReassignGen, NewGroup } }, TBlobStorageGroupType::ErasureNone);
        Manager = std::make_shared<NOlap::TBlobManager>(TabletInfo, tabletGen, NOlap::TTabletId(tabletId));
        Shared = std::make_shared<NOlap::NDataSharing::TStorageSharedBlobsManager>(
            NOlap::NBlobOperations::TGlobal::DefaultStorageId, NOlap::TTabletId(tabletId));
        StorageCounters = std::make_shared<NOlap::NBlobOperations::TStorageCounters>(NOlap::NBlobOperations::TGlobal::DefaultStorageId);
        Counters = StorageCounters->GetConsumerCounter(NOlap::NBlobOperations::EConsumer::GC)->GetRemoveGCCounters();
    }

    std::shared_ptr<NOlap::NBlobOperations::NBlobStorage::TGCTask> BuildGCTask() {
        return Manager->BuildGCTask(NOlap::NBlobOperations::TGlobal::DefaultStorageId, Manager, Shared, Counters);
    }
};

struct TGCDbSchema: NIceDb::Schema {
    using TTables = SchemaTables<NColumnShard::Schema::BlobsToKeep, NColumnShard::Schema::BlobsToDelete, NColumnShard::Schema::BlobsToDeleteWT>;
};

class TGCDbFixture {
    class TGroupSelector: public NOlap::IBlobGroupSelector {
        const TIntrusivePtr<TTabletStorageInfo> Info;

    public:
        explicit TGroupSelector(const ui64 tabletId)
            : Info(MakeTabletInfo(tabletId, { { 0, OldGroup }, { ReassignGen, NewGroup } }))
        {
        }

        ui32 GetGroup(const TLogoBlobID& blob) const override {
            return Info->GroupFor(blob.Channel(), blob.Generation());
        }
    };

    TActorSystemStub ActorSystemStub;
    NTable::NTest::TDbExec Database;
    const ui64 TabletId;
    const TGroupSelector Selector;

public:
    explicit TGCDbFixture(const ui64 tabletId)
        : TabletId(tabletId)
        , Selector(tabletId)
    {
        Database.Begin();
        NIceDb::TNiceDb db(*Database.operator->());
        db.Materialize<TGCDbSchema>();
        Database.Commit();
    }

    template <class TAction>
    void Transaction(TAction&& action, const bool commit = true) {
        Database.Begin();
        NOlap::TBlobManagerDb db(*Database.operator->());
        action(db);
        if (commit) {
            Database.Commit();
        } else {
            Database.Reject();
        }
    }

    template <class TTable>
    void Insert(const NOlap::TUnifiedBlobId& blob, const bool newEncoding, const ui64 rowTabletId) {
        Transaction([&](NOlap::TBlobManagerDb& blobs) {
            NIceDb::TNiceDb db(blobs.GetDatabase());
            const TString encoded = newEncoding ? blob.ToStringNew() : blob.ToStringLegacy();
            if constexpr (std::is_same_v<TTable, NColumnShard::Schema::BlobsToDeleteWT>) {
                db.Table<TTable>().Key(encoded, rowTabletId).Update();
            } else {
                db.Table<TTable>().Key(encoded).Update();
            }
        });
    }

    bool HasBlobs(const THashSet<ui32>& groups) {
        bool found = true;
        Transaction([&](NOlap::TBlobManagerDb& db) {
            UNIT_ASSERT(db.HasGCBlobsForGroups(groups, &Selector, NOlap::TTabletId(TabletId), found));
        });
        return found;
    }

    void Restart() {
        Database.Replay(NTable::NTest::EPlay::Boot);
    }
};

Y_UNIT_TEST_SUITE(TMoveDataTest) {
    Y_UNIT_TEST(DurableGCCheckReadsEveryTableAndEncoding) {
        static constexpr ui64 TabletId = 42;
        const auto checkTable = []<class TTable>() {
            for (const bool newEncoding : { false, true }) {
                TGCDbFixture db(TabletId);
                UNIT_ASSERT(!db.HasBlobs({ OldGroup }));
                const auto oldBlob = MakeDsBlobId(OldGroup, TabletId, 1, 1, 2);
                db.Insert<TTable>(oldBlob, newEncoding, TabletId);
                UNIT_ASSERT(db.HasBlobs({ OldGroup }));
                UNIT_ASSERT(!db.HasBlobs({ NewGroup }));
                UNIT_ASSERT(!db.HasBlobs({}));
                db.Restart();
                UNIT_ASSERT(db.HasBlobs({ OldGroup }));
                db.Transaction([&](NOlap::TBlobManagerDb& blobs) {
                    if constexpr (std::is_same_v<TTable, NColumnShard::Schema::BlobsToKeep>) {
                        blobs.EraseBlobToKeep(oldBlob);
                    } else {
                        blobs.EraseBlobToDelete(oldBlob, NOlap::TTabletId(TabletId));
                    }
                });
                UNIT_ASSERT(!db.HasBlobs({ OldGroup }));
                db.Insert<TTable>(MakeDsBlobId(NewGroup, TabletId, ReassignGen + 1, 2, 2), newEncoding, TabletId);
                UNIT_ASSERT(!db.HasBlobs({ OldGroup }));
                UNIT_ASSERT(db.HasBlobs({ NewGroup }));
            }
        };
        checkTable.template operator()<NColumnShard::Schema::BlobsToKeep>();
        checkTable.template operator()<NColumnShard::Schema::BlobsToDelete>();
        checkTable.template operator()<NColumnShard::Schema::BlobsToDeleteWT>();
    }

    Y_UNIT_TEST(DurableGCCheckUsesExplicitGroupForNewEncoding) {
        static constexpr ui64 TabletId = 42;
        TGCDbFixture db(TabletId);
        // The encoded group wins even when our channel history resolves the generation elsewhere.
        db.Insert<NColumnShard::Schema::BlobsToKeep>(MakeDsBlobId(OldGroup, TabletId, ReassignGen + 1, 1, 2), true, TabletId);
        UNIT_ASSERT(db.HasBlobs({ OldGroup }));
        UNIT_ASSERT(!db.HasBlobs({ NewGroup }));
    }

    Y_UNIT_TEST(DurableGCCheckIgnoresBorrowedBlobsAndForeignUnlinks) {
        static constexpr ui64 TabletId = 42;
        static constexpr ui64 ForeignTabletId = 99;
        const auto checkTable = []<class TTable>() {
            for (const bool newEncoding : { false, true }) {
                TGCDbFixture db(TabletId);
                const auto borrowed = MakeDsBlobId(OldGroup, ForeignTabletId, 1, 1, 2);
                db.Insert<TTable>(borrowed, newEncoding, TabletId);
                UNIT_ASSERT(!db.HasBlobs({ OldGroup }));
                db.Restart();
                UNIT_ASSERT(!db.HasBlobs({ OldGroup }));
            }
        };
        checkTable.template operator()<NColumnShard::Schema::BlobsToKeep>();
        checkTable.template operator()<NColumnShard::Schema::BlobsToDelete>();
        checkTable.template operator()<NColumnShard::Schema::BlobsToDeleteWT>();
        for (const bool newEncoding : { false, true }) {
            TGCDbFixture db(TabletId);
            const auto owned = MakeDsBlobId(OldGroup, TabletId, 1, 1, 2);
            db.Insert<NColumnShard::Schema::BlobsToDeleteWT>(owned, newEncoding, ForeignTabletId);
            UNIT_ASSERT(!db.HasBlobs({ OldGroup }));
            db.Insert<NColumnShard::Schema::BlobsToDeleteWT>(owned, newEncoding, TabletId);
            UNIT_ASSERT(db.HasBlobs({ OldGroup }));
        }
    }

    Y_UNIT_TEST(DurableGCCheckSurvivesTaskExtractionUntilCleanupCommits) {
        auto controllerGuard = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TReadOnlyController>();
        static constexpr ui64 TabletId = 44;
        static constexpr ui32 TabletGen = 3;
        TBlobManagerFixture f(TabletId, TabletGen);
        TGCDbFixture db(TabletId);
        const NOlap::TGenStep barrier(TabletGen, 0);
        UNIT_ASSERT(f.BuildGCTask());
        f.Manager->OnGCStartOnComplete(barrier);
        f.Manager->OnGCFinishedOnComplete(barrier);

        const auto blob = MakeDsBlobId(OldGroup, TabletId, 1, 1, 2);
        db.Transaction([&](NOlap::TBlobManagerDb& blobs) {
            f.Manager->DeleteBlobOnExecute(NOlap::TTabletId(TabletId), blob, blobs);
        });
        f.Manager->DeleteBlobOnComplete(NOlap::TTabletId(TabletId), blob);
        UNIT_ASSERT(f.BuildGCTask());
        UNIT_ASSERT(db.HasBlobs({ OldGroup }));
        UNIT_ASSERT(!db.HasBlobs({ NewGroup }));
        db.Restart();
        UNIT_ASSERT(db.HasBlobs({ OldGroup }));

        const auto cleanup = [&](NOlap::TBlobManagerDb& blobs) {
            blobs.EraseBlobToDelete(blob, NOlap::TTabletId(TabletId));
        };
        db.Transaction(cleanup, false);
        UNIT_ASSERT(db.HasBlobs({ OldGroup }));
        db.Transaction(cleanup);
        f.Manager->OnGCFinishedOnComplete(std::nullopt);
        UNIT_ASSERT(!db.HasBlobs({ OldGroup }));
        db.Restart();
        UNIT_ASSERT(!db.HasBlobs({ OldGroup }));
    }

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

    // Live groups are rejected, so a portion created after the session started cannot hold a target blob.
    Y_UNIT_TEST(PortionCreatedAfterTheSessionStartedIsNotAdopted) {
        TActualizerSchema schema(NOlap::NTest::MakePortionTestIndexInfo());
        NOlap::NActualizer::TMoveDataActualizer actualizer(THashSet<ui32>{ 100 }, schema.Index);
        const TInstant start = TInstant::Seconds(1000);
        actualizer.Seed(NOlap::NActualizer::TAddExternalContext(start, {}), {});

        const THashMap<ui64, NOlap::TPortionInfo::TPtr> noPortions;
        actualizer.AddPortion(MakeDefaultTierPortion(1), NOlap::NActualizer::TAddExternalContext(start + TDuration::Minutes(1), noPortions));
        UNIT_ASSERT_VALUES_EQUAL_C(actualizer.GetMoveDataQueueSizes().GetTotal(), 0, "adopting it would hold the response back");
    }

    // A compaction-level move removes and re-adds the same portion; it stays ours however late it returns.
    Y_UNIT_TEST(PortionReaddedAfterALevelMoveIsStillMoved) {
        TActualizerSchema schema(NOlap::NTest::MakePortionTestIndexInfo());
        NOlap::NActualizer::TMoveDataActualizer actualizer(THashSet<ui32>{ 100 }, schema.Index);
        const TInstant start = TInstant::Seconds(1000);
        const auto portion = MakeDefaultTierPortion(1);
        const THashMap<ui64, NOlap::TPortionInfo::TPtr> portions{ { 1, portion } };
        actualizer.Seed(NOlap::NActualizer::TAddExternalContext(start, portions), {});
        UNIT_ASSERT_VALUES_EQUAL_C(actualizer.GetMoveDataQueueSizes().Pending, 1, "a default-tier portion must be admitted at session start");

        actualizer.RemovePortion(1);
        actualizer.AddPortion(portion, NOlap::NActualizer::TAddExternalContext(start + TDuration::Hours(1), portions));
        const auto readded = actualizer.GetMoveDataQueueSizes();
        UNIT_ASSERT_VALUES_EQUAL_C(readded.Pending, 1, "a portion the session started with must stay tracked across a level move");
        UNIT_ASSERT_VALUES_EQUAL_C(readded.Retired, 0, "a portion that came back is live again, not awaiting cleanup");
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
        actualizer.RefreshPortions(portions, noUncommitted);
        const auto retired = actualizer.GetMoveDataQueueSizes();
        UNIT_ASSERT_VALUES_EQUAL_C(retired.GetTotal(), 0, "a retired portion is no longer waiting for a rewrite");
        UNIT_ASSERT_VALUES_EQUAL_C(retired.Retired, 1, "a retired portion that cleanup has not erased must hold the gate");

        // A portion the session did not seed is not ours, so its retirement holds nothing.
        portions.emplace(2, MakeDefaultTierPortion(2));
        portions.at(2)->SetRemoveSnapshot(NOlap::TSnapshot(2, 1));
        actualizer.RefreshPortions(portions, noUncommitted);
        UNIT_ASSERT_VALUES_EQUAL_C(actualizer.GetMoveDataQueueSizes().Retired, 1, "an unseeded portion must not count");

        portions.erase(1);
        const auto& actualizerView = actualizer;
        UNIT_ASSERT_VALUES_EQUAL(actualizerView.GetMoveDataQueueSizes().Retired, 1);
        actualizer.RefreshPortions(portions, noUncommitted);
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
        const auto& actualizerView = actualizer;
        UNIT_ASSERT_VALUES_EQUAL(actualizerView.GetMoveDataQueueSizes().Retired, 1);
        actualizer.RefreshPortions(portions, noUncommitted);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().Retired, 0);
    }

    Y_UNIT_TEST(RetiredNonTargetPortionDoesNotBlockCompletion) {
        TActualizerSchema schema(NOlap::NTest::MakePortionTestIndexInfo());
        NOlap::NActualizer::TMoveDataActualizer actualizer({ OldGroup }, schema.Index);
        auto portion = MakeDefaultTierPortion(1);
        portion->SetRemoveSnapshot(NOlap::TSnapshot(2, 1));
        THashMap<ui64, NOlap::TPortionInfo::TPtr> portions{ { 1, portion } };
        actualizer.Seed(NOlap::NActualizer::TAddExternalContext(TInstant::Seconds(1000), portions), {});

        auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TSoftMemoryLimitController>(0);
        auto sharedActualizer = std::make_shared<NOlap::NActualizer::TMoveDataActualizer>(THashSet<ui32>{ OldGroup }, schema.Index);
        sharedActualizer->Seed(NOlap::NActualizer::TAddExternalContext(TInstant::Seconds(1000), portions), {});
        UNIT_ASSERT_VALUES_EQUAL(sharedActualizer->BuildMoveDataMetadataRequests(portions, {}, sharedActualizer).size(), 1);

        ClassifyPortion(actualizer, portion, NewGroup);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().GetTotal(), 0);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().Retired, 0);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().Rejected, 1);
        UNIT_ASSERT(portions.contains(1));   // A read can keep this portion alive without keeping MoveData alive.
    }

    Y_UNIT_TEST(RejectedPortionStaysRejectedAcrossRetirementAndLevelMoves) {
        TActualizerSchema schema(NOlap::NTest::MakePortionTestIndexInfo());
        NOlap::NActualizer::TMoveDataActualizer actualizer({ OldGroup }, schema.Index);
        auto portion = MakeDefaultTierPortion(1);
        THashMap<ui64, NOlap::TPortionInfo::TPtr> portions{ { 1, portion } };
        const NOlap::NActualizer::TAddExternalContext context(TInstant::Seconds(1000), portions);
        actualizer.Seed(context, {});
        ClassifyPortion(actualizer, portion, NewGroup);
        actualizer.RemovePortion(1);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().Retired, 0);
        actualizer.AddPortion(portion, context);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().Pending, 0);
        portion->SetRemoveSnapshot(NOlap::TSnapshot(2, 1));
        actualizer.RefreshPortions(portions, {});
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().Retired, 0);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().Rejected, 1);
    }

    Y_UNIT_TEST(RetirementDuringClassificationPreservesTheTargetCheck) {
        for (const bool Target : { false, true }) {
            TActualizerSchema schema(NOlap::NTest::MakePortionTestIndexInfo());
            NOlap::NActualizer::TMoveDataActualizer actualizer({ OldGroup }, schema.Index);
            auto portion = MakeDefaultTierPortion(1);
            THashMap<ui64, NOlap::TPortionInfo::TPtr> portions{ { 1, portion } };
            actualizer.Seed(NOlap::NActualizer::TAddExternalContext(TInstant::Seconds(1000), portions), {});
            portion->SetRemoveSnapshot(NOlap::TSnapshot(2, 1));
            actualizer.RefreshPortions(portions, {});
            UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().Pending, 1);
            UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().Retired, 0);

            ClassifyPortion(actualizer, portion, Target ? OldGroup : NewGroup);
            UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().GetTotal(), 0);
            UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().Retired, Target ? 1 : 0);
            UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().ConfirmedToMove, 0);
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
        actualizer.RefreshPortions(portions, {});
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().GetTotal(), 0);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().Retired, 0);

        // The callback can arrive after cleanup; it must not reinsert the old portion.
        ClassifyPortion(actualizer, portion, OldGroup);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().GetTotal(), 0);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().Retired, 0);
    }

    Y_UNIT_TEST(CandidateReconciliationIsBoundedAndErasurePreservesProgress) {
        TActualizerSchema schema(NOlap::NTest::MakePortionTestIndexInfo());
        NOlap::NActualizer::TMoveDataActualizer actualizer({ OldGroup }, schema.Index);
        THashMap<ui64, NOlap::TPortionInfo::TPtr> portions;
        constexpr ui64 PortionCount = 4097;
        for (ui64 id = 1; id <= PortionCount; ++id) {
            portions.emplace(id, MakeDefaultTierPortion(id));
        }
        actualizer.Seed(NOlap::NActualizer::TAddExternalContext(TInstant::Seconds(1000), portions), {});
        // Erase entries at and between the cursors through classification before any reconciliation.
        for (ui64 id = 1; id <= PortionCount; ++id) {
            ClassifyPortion(actualizer, portions.at(id), id % 2 ? NewGroup : OldGroup);
        }
        const auto selected = actualizer.GetMoveDataQueueSizes().ConfirmedToMove;
        UNIT_ASSERT_VALUES_EQUAL(selected, PortionCount / 2);
        portions.clear();
        actualizer.RefreshPortions(portions, {});
        UNIT_ASSERT_GT(actualizer.GetMoveDataQueueSizes().GetTotal(), 0);
        UNIT_ASSERT_LT(actualizer.GetMoveDataQueueSizes().GetTotal(), selected);
        for (ui32 pass = 0; pass < 10 && actualizer.GetMoveDataQueueSizes().GetTotal(); ++pass) {
            actualizer.RefreshPortions(portions, {});
        }
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().GetTotal(), 0);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().Rejected, (PortionCount + 1) / 2);
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
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().InFlight, 1);
        auto lock = locks->RegisterLock<NOlap::NDataLocks::TListPortionsLock>(
            "move", std::vector<NOlap::TPortionInfo::TConstPtr>{ portion }, NOlap::NDataLocks::ELockCategory::Actualization);
        UNIT_ASSERT_VALUES_EQUAL(extract(), 0);
        // An aborted rewrite releases its lock. Deliberately omit ReturnToIndexes/AddPortion.
        lock->Release(*locks);
        UNIT_ASSERT_VALUES_EQUAL(extract(), 1);
        portion->SetRemoveSnapshot(NOlap::TSnapshot(2, 1));
        // Even before reconciliation, retirement must prevent another rewrite.
        UNIT_ASSERT_VALUES_EQUAL(extract(), 0);
        actualizer.RefreshPortions(portions, {});
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().InFlight, 0);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes().Retired, 1);
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
        schema.Index.AddIndex(NOlap::TSnapshot(2, 1), schema.Cache->UpsertIndexInfo(std::move(*wideIndexInfo)));

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
            auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TSoftMemoryLimitController>(totalMemory + 1);
            auto actualizer = std::make_shared<NOlap::NActualizer::TMoveDataActualizer>(THashSet<ui32>{ 100 }, schema.Index);
            actualizer->Seed(NOlap::NActualizer::TAddExternalContext(TInstant::Seconds(1000), portions), {});
            const auto requests = actualizer->BuildMoveDataMetadataRequests(portions, {}, actualizer);
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
            auto guard = NYDBTest::TControllers::RegisterCSControllerGuard<TSoftMemoryLimitController>(softLimit);
            auto actualizer = std::make_shared<NOlap::NActualizer::TMoveDataActualizer>(targetGroups, schema.Index);
            actualizer->Seed(NOlap::NActualizer::TAddExternalContext(TInstant::Seconds(1000), portions), {});
            return actualizer->BuildMoveDataMetadataRequests(knownPortions, {}, actualizer);
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

    // Recovered barriers and queues only; the keep list is sized to hit the per-task GC limit.
    class TRecoveredGcDb: public NOlap::IBlobManagerDb {
    public:
        TGenStep Last;
        TGenStep Prepared;
        std::vector<NOlap::TUnifiedBlobId> Keeps;

        bool LoadGCBarrierPreparation(TGenStep& genStep) override {
            genStep = Prepared;
            return true;
        }

        void SaveGCBarrierPreparation(const TGenStep&) override {
        }

        bool LoadLastGcBarrier(TGenStep& genStep) override {
            genStep = Last;
            return true;
        }

        void SaveLastGcBarrier(const TGenStep&) override {
        }

        bool LoadLists(std::vector<NOlap::TUnifiedBlobId>& blobsToKeep, NOlap::TTabletsByBlob&, const NOlap::IBlobGroupSelector*,
            const NOlap::TTabletId) override {
            blobsToKeep = Keeps;
            return true;
        }

        void AddBlobToKeep(const NOlap::TUnifiedBlobId&) override {
        }

        void EraseBlobToKeep(const NOlap::TUnifiedBlobId&) override {
        }

        void AddBlobToDelete(const NOlap::TUnifiedBlobId&, const NOlap::TTabletId) override {
        }

        void EraseBlobToDelete(const NOlap::TUnifiedBlobId&, const NOlap::TTabletId) override {
        }

        bool LoadTierLists(const TString&, NOlap::TTabletsByBlob&, std::deque<NOlap::TUnifiedBlobId>&, const NOlap::TTabletId) override {
            return true;
        }

        void AddTierBlobToDelete(const TString&, const NOlap::TUnifiedBlobId&, const NOlap::TTabletId) override {
        }

        void RemoveTierBlobToDelete(const TString&, const NOlap::TUnifiedBlobId&, const NOlap::TTabletId) override {
        }

        void AddTierDraftBlobId(const TString&, const NOlap::TUnifiedBlobId&) override {
        }

        void RemoveTierDraftBlobId(const TString&, const NOlap::TUnifiedBlobId&) override {
        }

        void AddBlobSharing(const TString&, const NOlap::TUnifiedBlobId&, const NOlap::TTabletId) override {
        }

        void RemoveBlobSharing(const TString&, const NOlap::TUnifiedBlobId&, const NOlap::TTabletId) override {
        }

        void AddBorrowedBlob(const TString&, const NOlap::TUnifiedBlobId&, const NOlap::TTabletId) override {
        }

        void RemoveBorrowedBlob(const TString&, const NOlap::TUnifiedBlobId&) override {
        }
    };

    // A barrier recovered from an older generation proves nothing about this one: the all-history broadcast must repeat until one of this generation went out.
    Y_UNIT_TEST(FirstGCBroadcastRepeatsUntilABarrierOfThisGenerationGoesOut) {
        auto controllerGuard = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TReadOnlyController>();
        static constexpr ui64 TabletId = 47;
        static constexpr ui32 TabletGen = 7;
        static constexpr ui32 DataChannel = 2;
        // One more than the per-task keep limit, so the first task cannot advance past the recovered barrier.
        static constexpr ui32 KeepCount = 500001;

        TBlobManagerFixture f(TabletId, TabletGen);
        // The repeated broadcast is part of the MoveData contract and stays behind its flag.
        f.ActorSystemStub.AppData.FeatureFlags.SetEnableColumnshardMoveData(true);
        TRecoveredGcDb db;
        db.Last = TGenStep(4, 0);
        db.Prepared = TGenStep(4, 1);
        for (ui32 i = 0; i < KeepCount; ++i) {
            db.Keeps.emplace_back(NewGroup, TLogoBlobID(TabletId, ReassignGen, 1 + i / 1000, DataChannel, BlobSize, i % 1000));
        }
        UNIT_ASSERT(f.Manager->LoadState(db, NOlap::TTabletId(TabletId)));
        const NOlap::NBlobOperations::NBlobStorage::TBlobAddress oldGroupAddress(OldGroup, DataChannel);

        auto first = f.BuildGCTask();
        UNIT_ASSERT_C(first, "the recovered barrier must produce a task");
        UNIT_ASSERT_C(first->GetListsByGroupId().contains(oldGroupAddress), "the first task broadcasts to every historical group");
        f.Manager->OnGCStartOnComplete(TGenStep(4, 1));
        f.Manager->OnGCFinishedOnComplete(TGenStep(4, 1));

        auto second = f.BuildGCTask();
        UNIT_ASSERT_C(second, "the remaining keep entry must produce a task");
        UNIT_ASSERT_C(second->GetListsByGroupId().contains(oldGroupAddress),
            "the barrier of this generation must reach the historical group, or an orphan written before the crash stays behind the gate");
        f.Manager->OnGCStartOnComplete(TGenStep(TabletGen, 0));
        f.Manager->OnGCFinishedOnComplete(TGenStep(TabletGen, 0));
    }

}   // Y_UNIT_TEST_SUITE

}   // namespace NKikimr

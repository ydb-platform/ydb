#include <ydb/core/tablet_flat/test/libs/table/test_dbase.h>
#include <ydb/core/tablet_flat/test/libs/table/test_envs.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/common/blob.h>
#include <ydb/core/tx/columnshard/engines/db_wrapper.h>
#include <ydb/core/tx/columnshard/engines/portions/constructor_portion.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

using namespace NColumnShard;
using namespace NTable::NTest;

namespace {

using TPortions = Schema::IndexPortions;
using TColumnsV2 = Schema::IndexColumnsV2;

// Bytes ceiling passed to every seeding loader call (matches SeedChargeCeiling = 4 * 4 MiB).
static constexpr ui64 kBytesLimit = 16 * 1024 * 1024;

TString MakePortionMeta() {
    NKikimrTxColumnShard::TIndexPortionMeta meta;
    auto* snap = meta.MutableCompactedPortion()->MutableAppearanceSnapshot();
    snap->SetPlanStep(1);
    snap->SetTxId(1);
    return meta.SerializeAsString();
}

// Materialize the full columnshard schema inside an active TDbExec transaction.
void InitSchema(TDbExec& exec) {
    exec.Begin();
    NIceDb::TNiceDb(*exec.operator->()).Materialize<Schema>();
    exec.Commit();
}

// Write `count` IndexPortions rows starting at portionId=startOffset with a minimal valid Metadata.
void WritePortions(TDbExec& exec, ui32 count, ui64 startOffset = 0) {
    const TString metaBytes = MakePortionMeta();
    exec.Begin();
    NIceDb::TNiceDb ndb(*exec.operator->());
    for (ui32 i = 0; i < count; ++i) {
        ndb.Table<TPortions>()
            .Key(1UL, startOffset + (ui64)i)
            .Update(NIceDb::TUpdate<TPortions::Metadata>(metaBytes))
            .Update(NIceDb::TUpdate<TPortions::SchemaVersion>(1UL))
            .Update(NIceDb::TUpdate<TPortions::XPlanStep>(0UL))
            .Update(NIceDb::TUpdate<TPortions::XTxId>(0UL));
    }
    exec.Commit();
}

// Snap + compact `table` into B-tree parts, the production default; flat parts are unreachable here because TDbExec::Compact hides TConf.
void SnapAndCompactBTree(TDbExec& exec, ui32 table) {
    exec.Snap(table);
    NTable::TDatabase& db = *exec.operator->();
    TAutoPtr<NTable::TSubset> subset = db.Subset(table, NTable::TEpoch::Max(), {}, {});
    Y_ENSURE(subset->TxStatus.empty());
    TLogoBlobID logo(1, 1, 9999, 1, 0, 0);
    auto* family = db.GetScheme().DefaultFamilyFor(table);
    NTable::NPage::TConf conf{ true, 8291, family->Large };
    for (const auto& p : db.GetScheme().GetTableInfo(table)->ByKeyFilterPrefixes) {
        conf.ByKeyFilterPrefixes.push_back(NTable::NPage::TConf::TByKeyFilterPrefix{ p.PrefixLength, p.FalsePositiveProbability });
    }
    conf.MaxRows = subset->MaxRows();
    conf.MinRowVersion = subset->MinRowVersion();
    conf.SmallEdge = family->Small;
    TAutoPtr<NTable::IPages> env = new TForwardEnv(128, 256, subset->Scheme->Tags(true), Max<ui32>());
    auto eggs = TCompaction(env, conf).WithRemovedRowVersions(db.GetRemovedRowVersions(table)).Do(*subset, logo);
    Y_ENSURE(!eggs.NoResult());
    for (const auto& part : eggs.Parts) {
        Y_ENSURE(part->IndexPages.HasBTree(), "produced part lacks B-tree index (TConf::WriteBTreeIndex=true)");
    }
    TVector<NTable::TPartView> partViews;
    for (auto& part : eggs.Parts) {
        partViews.push_back({ part, nullptr, part->Slices });
    }
    db.Replace(table, *subset, std::move(partViews), {});
}

}   // anonymous namespace

Y_UNIT_TEST_SUITE(TSeedingLoader) {
    // Rows compacted into parts → Precharge reports nonzero BytesPrecharged.
    Y_UNIT_TEST(PortionsInPartsHaveNonzeroBytesPrecharged) {
        TDbExec exec;
        InitSchema(exec);
        WritePortions(exec, 10);
        SnapAndCompactBTree(exec, TPortions::TableId);

        TTestEnv testEnv;
        NTable::TDatabase& db = *exec.operator->();
        db.Begin(NTable::TTxStamp(1, 9000), testEnv);
        NOlap::TFakeGroupSelector sel;
        NOlap::TDbWrapper wrapper(db, &sel);
        auto result = wrapper.LoadPortionsSeeding({ NOlap::TInternalPathId{}, 0 }, 1000, kBytesLimit,
            [](std::unique_ptr<NOlap::TPortionInfoConstructor>&&, const NKikimrTxColumnShard::TIndexPortionMeta&) {
                return true;
            });
        db.Commit(NTable::TTxStamp(1, 9000), false);

        UNIT_ASSERT(result.Ready);
        UNIT_ASSERT_GT(result.BytesPrecharged, 0u);
    }

    // Rows still in the memtable → Precharge reports zero BytesPrecharged but rows are returned.
    Y_UNIT_TEST(PortionsInMemtableHaveZeroBytesPrecharged) {
        TDbExec exec;
        InitSchema(exec);
        WritePortions(exec, 10);
        // No Snap/Compact: rows stay in the memtable.

        TTestEnv testEnv;
        NTable::TDatabase& db = *exec.operator->();
        db.Begin(NTable::TTxStamp(1, 9000), testEnv);
        NOlap::TFakeGroupSelector sel;
        NOlap::TDbWrapper wrapper(db, &sel);
        ui32 rowsSeen = 0;
        auto result = wrapper.LoadPortionsSeeding({ NOlap::TInternalPathId{}, 0 }, 1000, kBytesLimit,
            [&rowsSeen](std::unique_ptr<NOlap::TPortionInfoConstructor>&&, const NKikimrTxColumnShard::TIndexPortionMeta&) {
                ++rowsSeen;
                return true;
            });
        db.Commit(NTable::TTxStamp(1, 9000), false);

        UNIT_ASSERT(result.Ready);
        UNIT_ASSERT_VALUES_EQUAL(result.BytesPrecharged, 0u);
        UNIT_ASSERT_VALUES_EQUAL(rowsSeen, 10u);
    }

    // TNoEnv withholds all pages → Precharge returns Ready=false (page fault).
    Y_UNIT_TEST(PageFaultReturnsNotReady) {
        TDbExec exec;
        InitSchema(exec);
        WritePortions(exec, 5);
        SnapAndCompactBTree(exec, TPortions::TableId);

        TNoEnv noEnv;
        NTable::TDatabase& db = *exec.operator->();
        db.Begin(NTable::TTxStamp(1, 9000), noEnv);
        NOlap::TFakeGroupSelector sel;
        NOlap::TDbWrapper wrapper(db, &sel);
        auto result = wrapper.LoadPortionsSeeding({ NOlap::TInternalPathId{}, 0 }, 1000, kBytesLimit,
            [](std::unique_ptr<NOlap::TPortionInfoConstructor>&&, const NKikimrTxColumnShard::TIndexPortionMeta&) {
                return true;
            });
        db.Commit(NTable::TTxStamp(1, 9000), false);

        UNIT_ASSERT(!result.Ready);
    }

    // After a page-fault attempt, a retry with TTestEnv returns all rows exactly once.
    Y_UNIT_TEST(PageFaultRetryReturnsRows) {
        TDbExec exec;
        InitSchema(exec);
        WritePortions(exec, 5);
        SnapAndCompactBTree(exec, TPortions::TableId);

        NTable::TDatabase& db = *exec.operator->();

        {
            TNoEnv noEnv;
            db.Begin(NTable::TTxStamp(1, 9000), noEnv);
            NOlap::TFakeGroupSelector sel;
            NOlap::TDbWrapper wrapper(db, &sel);
            auto r = wrapper.LoadPortionsSeeding({ NOlap::TInternalPathId{}, 0 }, 1000, kBytesLimit,
                [](std::unique_ptr<NOlap::TPortionInfoConstructor>&&, const NKikimrTxColumnShard::TIndexPortionMeta&) {
                    return true;
                });
            db.Commit(NTable::TTxStamp(1, 9000), false);
            UNIT_ASSERT(!r.Ready);
        }
        {
            TTestEnv testEnv;
            db.Begin(NTable::TTxStamp(1, 9001), testEnv);
            NOlap::TFakeGroupSelector sel;
            NOlap::TDbWrapper wrapper(db, &sel);
            ui32 rowsSeen = 0;
            auto r = wrapper.LoadPortionsSeeding({ NOlap::TInternalPathId{}, 0 }, 1000, kBytesLimit,
                [&rowsSeen](std::unique_ptr<NOlap::TPortionInfoConstructor>&&, const NKikimrTxColumnShard::TIndexPortionMeta&) {
                    ++rowsSeen;
                    return true;
                });
            db.Commit(NTable::TTxStamp(1, 9001), false);
            UNIT_ASSERT(r.Ready);
            UNIT_ASSERT_VALUES_EQUAL(rowsSeen, 5u);
        }
    }

    // BytesPrecharged > target (=1) triggers batch-size shrink: nextN = N*target/cost < N.
    Y_UNIT_TEST(HighCostShrinksN) {
        TDbExec exec;
        InitSchema(exec);
        WritePortions(exec, 50);
        SnapAndCompactBTree(exec, TPortions::TableId);

        TTestEnv testEnv;
        NTable::TDatabase& db = *exec.operator->();
        db.Begin(NTable::TTxStamp(1, 9000), testEnv);
        NOlap::TFakeGroupSelector sel;
        NOlap::TDbWrapper wrapper(db, &sel);
        const ui64 N = 100;
        auto result = wrapper.LoadPortionsSeeding({ NOlap::TInternalPathId{}, 0 }, N, kBytesLimit,
            [](std::unique_ptr<NOlap::TPortionInfoConstructor>&&, const NKikimrTxColumnShard::TIndexPortionMeta&) {
                return true;
            });
        db.Commit(NTable::TTxStamp(1, 9000), false);

        UNIT_ASSERT(result.Ready);
        UNIT_ASSERT_GT(result.BytesPrecharged, 0u);
        const ui64 cost = result.BytesPrecharged;
        const ui64 target = 1;
        const ui64 nextN = Max<ui64>(1, N * target / cost);
        UNIT_ASSERT_LT(nextN, N);
    }

    // Portions split between compacted parts and the memtable are returned exactly once (no double-count).
    Y_UNIT_TEST(SplitMemtableAndPartsReturnedOnce) {
        TDbExec exec;
        InitSchema(exec);
        // portionIds 1..5 → compact into parts.
        WritePortions(exec, 5, 1);
        SnapAndCompactBTree(exec, TPortions::TableId);
        // portionIds 6..10 → stay in the memtable.
        WritePortions(exec, 5, 6);

        TTestEnv testEnv;
        NTable::TDatabase& db = *exec.operator->();
        db.Begin(NTable::TTxStamp(1, 9000), testEnv);
        NOlap::TFakeGroupSelector sel;
        NOlap::TDbWrapper wrapper(db, &sel);
        ui32 rowsSeen = 0;
        ui32 idMask = 0;
        auto result = wrapper.LoadPortionsSeeding({ NOlap::TInternalPathId{}, 0 }, 1000, kBytesLimit,
            [&](std::unique_ptr<NOlap::TPortionInfoConstructor>&& constructor, const NKikimrTxColumnShard::TIndexPortionMeta&) {
                ++rowsSeen;
                idMask |= 1u << (constructor->GetPortionIdVerified() - 1);
                return true;
            });
        db.Commit(NTable::TTxStamp(1, 9000), false);

        UNIT_ASSERT(result.Ready);
        UNIT_ASSERT_GT(result.BytesPrecharged, 0u);
        // The mask is what proves "exactly once": one row visited twice while another is missed still counts ten.
        UNIT_ASSERT_VALUES_EQUAL(rowsSeen, 10u);
        UNIT_ASSERT_VALUES_EQUAL(idMask, 0x3FFu);
    }

    // Corrupt BlobIds bytes in IndexColumnsV2 → LoadColumnsSeeding returns Error (not a crash).
    Y_UNIT_TEST(CorruptBlobIdsYieldsError) {
        TDbExec exec;
        InitSchema(exec);

        exec.Begin();
        {
            NIceDb::TNiceDb ndb(*exec.operator->());
            ndb.Table<TColumnsV2>()
                .Key(1UL, 1UL)
                .Update(NIceDb::TUpdate<TColumnsV2::Metadata>(TString()))
                .Update(NIceDb::TUpdate<TColumnsV2::BlobIds>(TString("corrupt_bytes")));
        }
        exec.Commit();

        SnapAndCompactBTree(exec, TColumnsV2::TableId);

        TTestEnv testEnv;
        NTable::TDatabase& db = *exec.operator->();
        db.Begin(NTable::TTxStamp(1, 9000), testEnv);
        NOlap::TFakeGroupSelector sel;
        NOlap::TDbWrapper wrapper(db, &sel);
        const NOlap::TInternalPathId pathId = NOlap::TInternalPathId::FromRawValue(1);
        auto result =
            wrapper.LoadColumnsSeeding({ NOlap::TInternalPathId{}, 0 }, { pathId, 999 }, kBytesLimit, [](NOlap::TColumnChunkLoadContextV2&&) {
            });
        db.Commit(NTable::TTxStamp(1, 9000), false);

        UNIT_ASSERT(result.Ready);
        UNIT_ASSERT(!result.Error.IsSuccess());
    }
}

}   // namespace NKikimr

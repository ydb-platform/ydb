#include <ydb/core/kqp/common/kqp_tx_manager.h>

#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme_types/scheme_type_info.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

namespace {

TVector<TCell> SingleCell(ui32 value) {
    return { TCell::Make(value) };
}

// The key schema of the SingleCell borders below.
TVector<NScheme::TTypeInfo> Uint32KeyColumnTypes() {
    return { NScheme::TTypeInfo(NScheme::NTypeIds::Uint32) };
}

} // namespace

Y_UNIT_TEST_SUITE(TxManagerShardMeta) {

    Y_UNIT_TEST(AddShardStoresAndReturnsMeta) {
        auto txManager = CreateKqpTransactionManager();

        TShardMeta meta;
        meta.TableId = TTableId(1, 100);
        meta.Borders = TOwnedTableRange(SingleCell(10), true, SingleCell(20), false);
        meta.PartitioningVersion = 7;

        txManager->AddShard(1, false, "/Root/Table", meta);
        UNIT_ASSERT(txManager->GetShardMeta(1).IsKnown());
        UNIT_ASSERT(txManager->GetShardMeta(1).TableId == meta.TableId);
        UNIT_ASSERT(SameBorders(txManager->GetShardMeta(1).Borders, meta.Borders));
        UNIT_ASSERT_VALUES_EQUAL(txManager->GetShardMeta(1).PartitioningVersion, 7);
    }

    Y_UNIT_TEST(EmptyMetaDoesNotClobberKnownMeta) {
        auto txManager = CreateKqpTransactionManager();

        TShardMeta meta;
        meta.TableId = TTableId(1, 100);
        meta.Borders = TOwnedTableRange(SingleCell(10), true, SingleCell(20), false);
        meta.PartitioningVersion = 7;

        txManager->AddShard(1, false, "/Root/Table", meta);
        txManager->AddShard(1, false, "/Root/Table");

        UNIT_ASSERT(txManager->GetShardMeta(1).IsKnown());
        UNIT_ASSERT_VALUES_EQUAL(txManager->GetShardMeta(1).PartitioningVersion, 7);
        UNIT_ASSERT(SameBorders(txManager->GetShardMeta(1).Borders, meta.Borders));
    }

    Y_UNIT_TEST(VersionOnlyMetaKeepsBorders) {
        // A caller that knows only the version (no borders) must not clear borders.
        auto txManager = CreateKqpTransactionManager();

        TShardMeta withBorders;
        withBorders.TableId = TTableId(1, 100);
        withBorders.Borders = TOwnedTableRange(SingleCell(10), true, SingleCell(20), false);
        withBorders.PartitioningVersion = 7;

        TShardMeta versionOnly;
        versionOnly.TableId = TTableId(1, 100);
        versionOnly.PartitioningVersion = 9;

        txManager->AddShard(1, false, "/Root/Table", withBorders);
        txManager->AddShard(1, false, "/Root/Table", versionOnly);

        UNIT_ASSERT(txManager->GetShardMeta(1).IsKnown());
        UNIT_ASSERT_VALUES_EQUAL(txManager->GetShardMeta(1).PartitioningVersion, 9);
        UNIT_ASSERT(SameBorders(txManager->GetShardMeta(1).Borders, withBorders.Borders));
    }

    Y_UNIT_TEST(VersionMax) {
        auto txManager = CreateKqpTransactionManager();

        TShardMeta meta;
        meta.TableId = TTableId(1, 100);
        meta.Borders = TOwnedTableRange(SingleCell(10), true, SingleCell(20), false);
        meta.PartitioningVersion = 5;
        txManager->AddShard(1, false, "/Root/Table", meta);

        TShardMeta older;
        older.TableId = meta.TableId;
        older.Borders = meta.Borders;
        older.PartitioningVersion = 3;
        txManager->AddShard(1, false, "/Root/Table", older);
        UNIT_ASSERT_VALUES_EQUAL(txManager->GetShardMeta(1).PartitioningVersion, 5);

        TShardMeta newer;
        newer.TableId = meta.TableId;
        newer.Borders = meta.Borders;
        newer.PartitioningVersion = 12;
        txManager->AddShard(1, false, "/Root/Table", newer);
        UNIT_ASSERT_VALUES_EQUAL(txManager->GetShardMeta(1).PartitioningVersion, 12);
    }

    Y_UNIT_TEST(BorderMismatchAborts) {
        auto txManager = CreateKqpTransactionManager();

        TShardMeta meta;
        meta.TableId = TTableId(1, 100);
        meta.Borders = TOwnedTableRange(SingleCell(10), true, SingleCell(20), false);
        meta.PartitioningVersion = 5;
        txManager->AddShard(1, false, "/Root/Table", meta);

        TShardMeta conflicting;
        conflicting.TableId = meta.TableId;
        conflicting.Borders = TOwnedTableRange(SingleCell(10), true, SingleCell(30), false);
        conflicting.PartitioningVersion = 6;

        UNIT_ASSERT_EXCEPTION(txManager->AddShard(1, false, "/Root/Table", conflicting), yexception);
    }

    Y_UNIT_TEST(EmptyMetaIsUnknown) {
        // A shard registered without metadata has no known meta: AddShard with an
        // empty meta (no borders, no version) stores nothing.
        auto txManager = CreateKqpTransactionManager();
        txManager->AddShard(42, false, "/Root/Table");
        UNIT_ASSERT(!txManager->GetShardMeta(42).IsKnown());
    }

    Y_UNIT_TEST(SetTableMeta) {
        auto txManager = CreateKqpTransactionManager();

        const TTableId tableId(1, 100);
        UNIT_ASSERT(!txManager->GetTableMeta(tableId).InitialPartitioning);
        UNIT_ASSERT(txManager->GetTableMeta(tableId).KeyColumnTypes.empty());

        txManager->SetTableMeta(tableId, IKqpTransactionManager::TTableMeta{
            .KeyColumnTypes = Uint32KeyColumnTypes()});
        UNIT_ASSERT(!txManager->GetTableMeta(tableId).InitialPartitioning);
        UNIT_ASSERT_VALUES_EQUAL(txManager->GetTableMeta(tableId).KeyColumnTypes.size(), 1u);

        // Overwrites the previously stored metadata.
        txManager->SetTableMeta(tableId, {});
        UNIT_ASSERT(!txManager->GetTableMeta(tableId).InitialPartitioning);
        UNIT_ASSERT(txManager->GetTableMeta(tableId).KeyColumnTypes.empty());
    }

    Y_UNIT_TEST(MoveShardToLeavesFreshTargetMetaUnknown) {
        // Metadata is not inherited on split/merge: the target covers a different
        // key range, and it learns its exact borders from its own UpdateShards pass.
        auto txManager = CreateKqpTransactionManager();

        TShardMeta meta;
        meta.TableId = TTableId(1, 100);
        meta.Borders = TOwnedTableRange(SingleCell(10), true, SingleCell(20), false);
        meta.PartitioningVersion = 5;
        txManager->AddShard(1, false, "/Root/Table", meta);
        txManager->SetTableMeta(TTableId(1, 100), IKqpTransactionManager::TTableMeta{
            .KeyColumnTypes = Uint32KeyColumnTypes()});

        UNIT_ASSERT(txManager->MoveShardTo(1, {2}));
        UNIT_ASSERT(!txManager->HasShard(1));
        UNIT_ASSERT(!txManager->GetShardMeta(2).IsKnown());
    }

    Y_UNIT_TEST(MoveShardToKeepsKnownTargetMeta) {
        auto txManager = CreateKqpTransactionManager();

        txManager->SetTableMeta(TTableId(1, 100), IKqpTransactionManager::TTableMeta{
            .KeyColumnTypes = Uint32KeyColumnTypes()});

        TShardMeta fromMeta;
        fromMeta.TableId = TTableId(1, 100);
        fromMeta.Borders = TOwnedTableRange(SingleCell(10), true, SingleCell(20), false);
        fromMeta.PartitioningVersion = 5;
        txManager->AddShard(1, false, "/Root/Table", fromMeta);

        TShardMeta targetMeta;
        targetMeta.TableId = TTableId(1, 100);
        targetMeta.Borders = TOwnedTableRange(SingleCell(10), true, SingleCell(25), false);
        targetMeta.PartitioningVersion = 9;
        txManager->AddShard(2, false, "/Root/Table", targetMeta);

        UNIT_ASSERT(txManager->MoveShardTo(1, {2}));
        UNIT_ASSERT(txManager->GetShardMeta(2).IsKnown());
        UNIT_ASSERT(SameBorders(txManager->GetShardMeta(2).Borders, targetMeta.Borders));
        UNIT_ASSERT_VALUES_EQUAL(txManager->GetShardMeta(2).PartitioningVersion, 9);
    }

    Y_UNIT_TEST(MoveShardToFreshTargetThenOwnWriteFillsMeta) {
        // A fresh (EnsureShards-only) target registered by the transfer, then
        // receiving its own write: its own true borders must be accepted.
        auto txManager = CreateKqpTransactionManager();

        TShardMeta meta;
        meta.TableId = TTableId(1, 100);
        meta.Borders = TOwnedTableRange(SingleCell(10), true, SingleCell(20), false);
        meta.PartitioningVersion = 5;
        txManager->AddShard(1, false, "/Root/Table", meta);
        txManager->SetTableMeta(TTableId(1, 100), IKqpTransactionManager::TTableMeta{
            .KeyColumnTypes = Uint32KeyColumnTypes()});

        UNIT_ASSERT(txManager->MoveShardTo(1, {2}));

        TShardMeta targetMeta;
        targetMeta.TableId = TTableId(1, 100);
        targetMeta.Borders = TOwnedTableRange(SingleCell(10), true, SingleCell(12), false);
        targetMeta.PartitioningVersion = 6;
        txManager->AddShard(2, false, "/Root/Table", targetMeta);

        UNIT_ASSERT(txManager->GetShardMeta(2).IsKnown());
        UNIT_ASSERT(SameBorders(txManager->GetShardMeta(2).Borders, targetMeta.Borders));
        UNIT_ASSERT_VALUES_EQUAL(txManager->GetShardMeta(2).PartitioningVersion, 6);
    }
}

Y_UNIT_TEST_SUITE(SameBorders) {

    // TTableRange holds non-owning TConstArrayRef borders, so the cell vectors
    // must outlive the ranges under comparison.
    Y_UNIT_TEST(IdenticalRanges) {
        const TVector<TCell> from = SingleCell(10);
        const TVector<TCell> to = SingleCell(20);
        const TTableRange a(from, true, to, false);
        const TTableRange b(from, true, to, false);
        UNIT_ASSERT(SameBorders(a, b));
    }

    Y_UNIT_TEST(DifferentCells) {
        const TVector<TCell> from = SingleCell(10);
        const TVector<TCell> to1 = SingleCell(20);
        const TVector<TCell> to2 = SingleCell(30);
        const TTableRange a(from, true, to1, false);
        const TTableRange b(from, true, to2, false);
        UNIT_ASSERT(!SameBorders(a, b));
    }

    Y_UNIT_TEST(DifferentInclusivity) {
        const TVector<TCell> from = SingleCell(10);
        const TVector<TCell> to = SingleCell(20);
        const TTableRange a(from, true, to, false);
        const TTableRange b(from, true, to, true);
        UNIT_ASSERT(!SameBorders(a, b));
    }

    Y_UNIT_TEST(DifferentBorderLengths) {
        const TVector<TCell> from = SingleCell(10);
        const TVector<TCell> to = SingleCell(20);
        const TTableRange a(from, true, TVector<TCell>{}, false);
        const TTableRange b(from, true, to, false);
        UNIT_ASSERT(!SameBorders(a, b));
    }

    Y_UNIT_TEST(MultiColumnBorders) {
        const TVector<TCell> fromA{TCell::Make(1), TCell::Make(2)};
        const TVector<TCell> toA{TCell::Make(3), TCell::Make(4)};
        const TVector<TCell> fromB{TCell::Make(1), TCell::Make(2)};
        const TVector<TCell> toB{TCell::Make(3), TCell::Make(4)};
        const TVector<TCell> fromC{TCell::Make(1), TCell::Make(5)};

        const TTableRange a(fromA, true, toA, false);
        const TTableRange b(fromB, true, toB, false);
        UNIT_ASSERT(SameBorders(a, b));

        const TTableRange c(fromC, true, toB, false);
        UNIT_ASSERT(!SameBorders(a, c));
    }

    Y_UNIT_TEST(NullCells) {
        const TVector<TCell> nullKey{TCell()};
        const TTableRange a(nullKey, true, TVector<TCell>{}, false);
        const TTableRange b(nullKey, true, TVector<TCell>{}, false);
        UNIT_ASSERT(SameBorders(a, b));
    }

    Y_UNIT_TEST(PointFlag) {
        const TVector<TCell> point = SingleCell(10);
        const TTableRange a(point);
        const TTableRange b(point);
        UNIT_ASSERT(SameBorders(a, b));

        const TTableRange c(point, true, point, true, false);
        UNIT_ASSERT(!SameBorders(a, c));
    }

    Y_UNIT_TEST(OwnedVsOwned) {
        const TOwnedTableRange a(SingleCell(10), true, SingleCell(20), false);
        const TOwnedTableRange b(SingleCell(10), true, SingleCell(20), false);
        UNIT_ASSERT(SameBorders(a, b));
    }
}

} // namespace NKikimr::NKqp

#include <ydb/core/tablet_flat/flat_dbase_scheme.h>
#include <ydb/core/tablet_flat/test/libs/table/test_dbase.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NTable {

using namespace NTest;

TAlter MakeAlter()
{
    TAlter alter;
    alter
        .AddTable("t", 1)
        .AddColumn(1, "key", 1, ETypes::String, false, false)
        .AddColumn(1, "value", 2, ETypes::Uint64, false, false)
        .AddColumnToKey(1, 1);
    return alter;
}

Y_UNIT_TEST_SUITE(TVersionedErase) {

    class TUncommittedObserver final : public ITransactionObserver {
    public:
        TVector<ui64> Skipped;

        void OnSkipUncommitted(ui64 txId) override { Skipped.push_back(txId); }
        void OnSkipCommitted(const TRowVersion&) override { }
        void OnSkipCommitted(const TRowVersion&, ui64) override { }
        void OnApplyCommitted(const TRowVersion&) override { }
        void OnApplyCommitted(const TRowVersion&, ui64) override { }
    };

    Y_UNIT_TEST(CanTruncateRespectsTransactionState) {
        TDbExec db;
        const TRowVersion version(5, 1);
        UNIT_ASSERT(!db->CanTruncate(1, version));
        db.ToLine().Begin();
        UNIT_ASSERT(!db->CanTruncate(1, version));
        UNIT_ASSERT_EXCEPTION_CONTAINS(db->Truncate(1, version), yexception, "missing table");
        db.Apply(*MakeAlter().Flush());
        UNIT_ASSERT(db->CanTruncate(1, version));
        db.Commit();
        UNIT_ASSERT(!db->CanTruncate(1, version));

        const auto row = *db.SchemedCookRow(1).Col("a", 1_u64);
        db.ToLine().Begin().Add(1, row);
        UNIT_ASSERT(!db->CanTruncate(1, version));
        UNIT_ASSERT_EXCEPTION_CONTAINS(db->Truncate(1, version), yexception, "modified table");
        db.Commit();

        db.ToLine().Begin();
        const auto head = db->Head(1);
        UNIT_ASSERT(db->CanTruncate(1, version));
        UNIT_ASSERT(db->CanTruncate(1, version));
        UNIT_ASSERT(db->Head(1) == head);
        UNIT_ASSERT(db->GetVersionedMetadata(1).empty());
        db->Truncate(1, version);
        UNIT_ASSERT(!db->CanTruncate(1, version.Next()));
        db.Reject();

        db.ToLine().Begin();
        UNIT_ASSERT(db->CanTruncate(1, version));
        db->Truncate(1);
        UNIT_ASSERT(!db->CanTruncate(1, version));
        UNIT_ASSERT_EXCEPTION_CONTAINS(db->Truncate(1, version), yexception, "truncated table");
        db.Reject();
        db.ToLine().Iter(1).Has(row);
    }

    Y_UNIT_TEST(CanImportPartsRetainsFloorUntilPhysicalCommit) {
        TDbExec db;
        UNIT_ASSERT(!db->CanImportParts(1));
        db.ToLine().Begin().Apply(*MakeAlter().Flush());
        UNIT_ASSERT(db->CanImportParts(1));
        db.Commit();
        UNIT_ASSERT(db->CanImportParts(1));

        db.ToLine().Begin().Apply(*TAlter().DropTable(1));
        UNIT_ASSERT(!db->CanImportParts(1));
        db.Reject();
        UNIT_ASSERT(db->CanImportParts(1));

        const auto row = *db.SchemedCookRow(1).Col("a", 1_u64);
        db.ToLine().Begin().Add(1, row).Commit();
        db.Snap(1).Compact(1);
        db.ToLine().Begin();
        db->Truncate(1, TRowVersion(5, 1));
        UNIT_ASSERT(!db->CanImportParts(1));
        db.Commit();
        UNIT_ASSERT(!db->CanImportParts(1));

        db.ToLine().Begin();
        db->RemoveRowVersions(1, TRowVersion::Min(), TRowVersion(5, 1));
        db.Commit();
        UNIT_ASSERT(!db->HasEraseAll(1));
        UNIT_ASSERT(!db->CanImportParts(1));

        db.ToLine().Begin();
        db->Truncate(1);
        UNIT_ASSERT(!db->CanImportParts(1));
        db.Reject();
        UNIT_ASSERT(!db->CanImportParts(1));

        db.ToLine().Begin();
        db->Truncate(1);
        UNIT_ASSERT(!db->CanImportParts(1));
        db.Commit();
        UNIT_ASSERT(db->CanImportParts(1));
    }

    Y_UNIT_TEST(PartImportsAndEraseCannotShareTransaction) {
        TDbExec db;
        db.ToLine().Begin().Apply(*MakeAlter().Flush()).Commit();
        const TRowVersion version(5, 1);

        db.ToLine().Begin();
        UNIT_ASSERT(db->CanImportParts(1));
        db->PreparePartImport(1);
        UNIT_ASSERT(db->CanImportParts(1));
        UNIT_ASSERT(!db->CanTruncate(1, version));
        UNIT_ASSERT_EXCEPTION(db->Truncate(1, version), yexception);
        UNIT_ASSERT(db->GetVersionedMetadata(1).empty());
        db->RollbackChanges();
        UNIT_ASSERT(db->CanTruncate(1, version));
        db->Truncate(1, version);
        UNIT_ASSERT_EXCEPTION(db->PreparePartImport(1), yexception);
        db.Reject();

        db.ToLine().Begin();
        db->PreparePartImport(1);
        db.Commit();
        db.ToLine().Begin();
        UNIT_ASSERT(db->CanTruncate(1, version));
        db.Reject();
    }

    Y_UNIT_TEST(MergeRejectsEraseMetadata) {
        TDbExec source;
        source.ToLine().Begin().Apply(*MakeAlter().Flush()).Commit();
        const auto row = *source.SchemedCookRow(1).Col("a", 1_u64);
        source.ToLine().Begin().WriteVer(TRowVersion(6, 1)).Add(1, row).Commit();
        source.Snap(1).Compact(1);
        const auto part = source->GetTableParts(1).at(0);
        const TIntrusiveConstPtr<TColdPart> cold = new TColdPart(part->Label, part->Epoch);
        NPage::TTxStatusBuilder builder;
        builder.AddCommitted(123, TRowVersion(6, 1));
        const TIntrusiveConstPtr<TTxStatusPart> status = new TTxStatusPart(
            part->Label, part->Epoch, new NPage::TTxStatusPage(builder.Finish()));

        for (bool expired : {false, true}) {
            TDbExec db;
            db.ToLine().Begin().Apply(*MakeAlter().Flush()).Commit();
            db.ToLine().Begin();
            db->Truncate(1, TRowVersion(5, 1));
            db.Commit();
            if (expired) {
                db.ToLine().Begin();
                db->RemoveRowVersions(1, TRowVersion::Min(), TRowVersion(5, 1));
                db.Commit();
                UNIT_ASSERT(!db->HasEraseAll(1));
            }
            UNIT_ASSERT(!db->GetVersionedMetadata(1).empty());
            UNIT_ASSERT(!db->CanImportParts(1));
            UNIT_ASSERT_EXCEPTION(db->Merge(1, part), yexception);
            UNIT_ASSERT_EXCEPTION(db->Merge(1, cold), yexception);
            UNIT_ASSERT_EXCEPTION(db->Merge(1, status), yexception);
            UNIT_ASSERT(db->GetTableParts(1).empty());
            UNIT_ASSERT(db->GetTableColdParts(1).empty());
            UNIT_ASSERT(!db->HasCommittedTx(1, 123));
        }
    }

    Y_UNIT_TEST(RollbackKeepsRows) {
        TDbExec db;
        db.To(1).Begin().Apply(*MakeAlter().Flush()).Commit();

        const auto row = *db.SchemedCookRow(1).Col("a", 1_u64);
        db.To(2).Begin().Add(1, row).Commit();

        db.To(3).Begin();
        db->Truncate(1, TRowVersion(5, 1));
        db.Reject();

        UNIT_ASSERT(db->GetVersionedMetadata(1).empty());
        db.To(4).Iter(1).Has(row);
    }

    Y_UNIT_TEST(HeadHidesAndSnapshotSees) {
        TDbExec db;
        db.To(1).Begin().Apply(*MakeAlter().Flush()).Commit();

        const auto oldRow = *db.SchemedCookRow(1).Col("a", 1_u64);
        const auto newRow = *db.SchemedCookRow(1).Col("b", 2_u64);
        db.To(2).Begin().Add(1, oldRow).Commit();

        db.To(3).Begin();
        db->Truncate(1, TRowVersion(5, 1));
        db.Commit();

        db.To(4).ReadVer(TRowVersion::Max()).Iter(1).NoKey(oldRow);
        db.To(5).ReadVer(TRowVersion(4, 1)).Iter(1).Has(oldRow);

        db.To(6).Begin().WriteVer(TRowVersion(6, 1)).Add(1, newRow).Commit();
        db.To(7).ReadVer(TRowVersion::Max()).Iter(1).NoKey(oldRow).Has(newRow);
        db.To(8).ReadVer(TRowVersion(4, 1)).Iter(1).Has(oldRow).NoKey(newRow);

        const auto& metadata = db->GetVersionedMetadata(1);
        UNIT_ASSERT_VALUES_EQUAL(metadata.size(), 1);
        UNIT_ASSERT(metadata[0].Version == TRowVersion(5, 1));
        UNIT_ASSERT_VALUES_EQUAL(metadata[0].Effects.size(), 1);
    }

    Y_UNIT_TEST(RepeatedEraseRetainsNewestWriteBoundary) {
        TDbExec db;
        db.To(1).Begin().Apply(*MakeAlter().Flush()).Commit();

        const auto first = *db.SchemedCookRow(1).Col("a", 1_u64);
        const auto second = *db.SchemedCookRow(1).Col("b", 2_u64);
        const auto third = *db.SchemedCookRow(1).Col("c", 3_u64);

        db.To(2).Begin().Add(1, first).Commit();
        db.To(3).Begin();
        db->Truncate(1, TRowVersion(5, 1));
        db.Commit();

        db.To(4).Begin();
        db->Truncate(1, TRowVersion(5, 1));
        db.Commit();

        const auto& metadata = db->GetVersionedMetadata(1);
        UNIT_ASSERT_VALUES_EQUAL(metadata.size(), 1);
        UNIT_ASSERT(metadata[0].Version == TRowVersion(5, 1));
        UNIT_ASSERT_VALUES_EQUAL(metadata[0].Effects.size(), 1);

        db.To(5).Begin().WriteVer(TRowVersion(6, 1)).Add(1, second).Commit();
        db.To(6).Begin();
        db->Truncate(1, TRowVersion(7, 1));
        db.Commit();
        db.To(7).ReadVer(TRowVersion::Max()).Iter(1).NoKey(first).NoKey(second);
        db.ReadVer(TRowVersion(4, 1)).Iter(1).Has(first).NoKey(second);
        db.ReadVer(TRowVersion(6, 1)).Iter(1).NoKey(first).Has(second);

        // No new sources need hiding, but the write boundary still advances.
        db.To(8).Begin();
        db->Truncate(1, TRowVersion(9, 1));
        db.Commit();
        UNIT_ASSERT(db->GetVersionedMetadata(1).back().Version == TRowVersion(9, 1));

        for (const auto version : {TRowVersion(8, 1), TRowVersion(9, 1)}) {
            db.ToLine().Begin().WriteVer(version);
            UNIT_ASSERT_EXCEPTION(db.Add(1, third), yexception);
            db.Reject();
        }
        db.ToLine().Begin().WriteVer(TRowVersion(10, 1)).Add(1, third).Commit();
        db.ToLine().ReadVer(TRowVersion::Max()).Iter(1).Has(third).NoKey(second);
    }

    Y_UNIT_TEST(RemoveRowVersionsDoesNotResurrect) {
        TDbExec db;
        db.To(1).Begin().Apply(*MakeAlter().Flush()).Commit();

        const auto row = *db.SchemedCookRow(1).Col("a", 1_u64);
        db.To(2).Begin().Add(1, row).Commit();
        db.To(3).Begin();
        db->Truncate(1, TRowVersion(5, 1));
        db.Commit();

        db.To(4).Begin();
        db->RemoveRowVersions(1, TRowVersion::Min(), TRowVersion(5, 1));
        db.Commit();

        db.To(5).ReadVer(TRowVersion::Max()).Iter(1).NoKey(row);
        db.To(6).ReadVer(TRowVersion(4, 1)).Iter(1).Has(row);
        UNIT_ASSERT(!db->GetVersionedMetadata(1).empty());
    }

    Y_UNIT_TEST(EarlierEraseIsRejected) {
        TDbExec db;
        db.To(1).Begin().Apply(*MakeAlter().Flush()).Commit();

        const auto row = *db.SchemedCookRow(1).Col("a", 1_u64);
        db.To(2).Begin().Add(1, row).Commit();
        db.To(3).Begin();
        db->Truncate(1, TRowVersion(10, 1));
        db.Commit();

        db.To(4).Begin();
        UNIT_ASSERT(!db->CanTruncate(1, TRowVersion(5, 1)));
        UNIT_ASSERT_EXCEPTION_CONTAINS(db->Truncate(1, TRowVersion(5, 1)), yexception,
            "older than the latest erase");
        db.Reject();

        db.To(5).ReadVer(TRowVersion(4, 1)).Iter(1).Has(row);
        db.To(6).ReadVer(TRowVersion(5, 1)).Iter(1).Has(row);
        db.To(7).ReadVer(TRowVersion(10, 1)).Select(1).NoKey(row);
    }

    Y_UNIT_TEST(EraseMustFollowEverySourceVersion) {
        for (ui32 storage = 0; storage != 3; ++storage) {
            TDbExec db;
            db.ToLine().Begin().Apply(*MakeAlter().Flush()).Commit();
            const auto row = *db.SchemedCookRow(1).Col("a", 1_u64);
            db.ToLine().Begin().WriteVer(TRowVersion(7, 1)).Add(1, row).Commit();
            if (storage >= 1) {
                db.Snap(1);
            }
            if (storage >= 2) {
                db.Compact(1);
            }

            for (const auto version : {TRowVersion(6, 1), TRowVersion(7, 1)}) {
                db.ToLine().Begin();
                UNIT_ASSERT(!db->CanTruncate(1, version));
                UNIT_ASSERT_EXCEPTION_CONTAINS(db->Truncate(1, version), yexception,
                    storage == 2 ? "every part row version" : "every source row version");
                db.Reject();
                UNIT_ASSERT(db->GetVersionedMetadata(1).empty());
                db.ToLine().Iter(1).Has(row);
            }

            db.ToLine().Begin();
            UNIT_ASSERT(db->CanTruncate(1, TRowVersion(7, 2)));
            db->Truncate(1, TRowVersion(7, 2));
            db.Commit();
            db.ToLine().ReadVer(TRowVersion(7, 1)).Select(1).Has(row);
            db.ToLine().ReadVer(TRowVersion(7, 2)).Select(1).NoKey(row);
        }
    }

    Y_UNIT_TEST(EraseRejectsOpenTransactions) {
        for (ui32 storage = 0; storage != 3; ++storage) {
            TDbExec db;
            db.ToLine().Begin().Apply(*MakeAlter().Flush()).Commit();
            const auto row = *db.SchemedCookRow(1).Col("a", 1_u64);
            db.ToLine().Begin().WriteTx(123).Add(1, row).Commit();
            if (storage >= 1) {
                db.Snap(1);
            }
            if (storage >= 2) {
                db.Compact(1);
            }

            db.ToLine().Begin();
            UNIT_ASSERT(!db->CanTruncate(1, TRowVersion(5, 1)));
            UNIT_ASSERT_EXCEPTION_CONTAINS(db->Truncate(1, TRowVersion(5, 1)), yexception,
                "open transactions");
            db.Reject();
            UNIT_ASSERT(db->HasOpenTx(1, 123));
            db.ToLine().ReadTx(123).Iter(1).Has(row);
            db.ToLine().ReadTx(123).Select(1).Has(row);
            auto observer = MakeIntrusive<TUncommittedObserver>();
            const TRowTool tool(db.RowSchemeFor(1));
            const auto key = tool.Split(row, true, false);
            const auto result = db->SelectRowVersion(1, key.Key, 0, nullptr, observer);
            UNIT_ASSERT_VALUES_EQUAL(result.Ready, EReady::Gone);
            UNIT_ASSERT_VALUES_EQUAL(observer->Skipped, TVector<ui64>{123});

            db.ToLine().Begin().WriteVer(TRowVersion(4, 1)).CommitTx(1, 123).Commit();
            UNIT_ASSERT(!db->HasOpenTx(1, 123));
            // Sources containing deltas retain a conservative maximum version
            // until compaction resolves the transaction.
            db.ToLine().Begin();
            UNIT_ASSERT(!db->CanTruncate(1, TRowVersion(5, 1)));
            db.Reject();
            db.Snap(1).Compact(1);

            db.ToLine().Begin();
            UNIT_ASSERT(db->CanTruncate(1, TRowVersion(5, 1)));
            db->Truncate(1, TRowVersion(5, 1));
            db.Commit();
            db.ToLine().ReadVer(TRowVersion(4, 1)).Select(1).Has(row);
            db.ToLine().ReadVer(TRowVersion(5, 1)).Select(1).NoKey(row);
        }
    }

    Y_UNIT_TEST(EraseRejectsOpenRowLocks) {
        TDbExec db;
        db.ToLine().Begin().Apply(*MakeAlter().Flush()).Commit();
        const auto row = *db.SchemedCookRow(1).Col("a", 1_u64);
        db.ToLine().Begin().WriteVer(TRowVersion(1, 1)).Add(1, row).Commit();
        db.ToLine().Begin().WriteTx(123).LockRowN(1, ELockMode::Exclusive, "a").Commit();

        db.ToLine().Begin();
        UNIT_ASSERT(!db->CanTruncate(1, TRowVersion(5, 1)));
        db.Reject();
        UNIT_ASSERT(db->HasOpenTx(1, 123));
        const auto locked = db.ToLine().SelectRowVersionN(1, "a");
        UNIT_ASSERT_VALUES_EQUAL(locked.LockMode, ELockMode::Exclusive);
        UNIT_ASSERT_VALUES_EQUAL(locked.LockTxId, 123);

        db.ToLine().Begin().RemoveTx(1, 123).Commit();
        db.Snap(1).Compact(1);
        db.ToLine().Begin();
        UNIT_ASSERT(db->CanTruncate(1, TRowVersion(5, 1)));
        db->Truncate(1, TRowVersion(5, 1));
        db.Commit();
        db.ToLine().Select(1).NoKey(row);
    }

    Y_UNIT_TEST(UpdatesMustFollowEraseVersion) {
        TDbExec db;
        db.ToLine().Begin().Apply(*MakeAlter().Flush()).Commit();
        const auto row = *db.SchemedCookRow(1).Col("a", 1_u64);
        db.ToLine().Begin().Add(1, row).Commit();
        db.ToLine().Begin();
        db->Truncate(1, TRowVersion(5, 1));
        db.Commit();

        for (const auto version : {TRowVersion(4, 1), TRowVersion(5, 1)}) {
            db.ToLine().Begin().WriteVer(version);
            UNIT_ASSERT_EXCEPTION(db.Add(1, row), yexception);
            db.Reject();
            db.ToLine().Begin().WriteVer(version);
            UNIT_ASSERT_EXCEPTION(db.EraseN(1, "a"), yexception);
            db.Reject();
        }

        db.ToLine().Begin().WriteVer(TRowVersion(5, 2)).Add(1, row).Commit();
        db.ToLine().ReadVer(TRowVersion(5, 1)).Select(1).NoKey(row);
        db.ToLine().ReadVer(TRowVersion(5, 2)).Select(1).Has(row);
    }

    Y_UNIT_TEST(TransactionCommitsMustFollowEraseVersion) {
        TDbExec db;
        db.ToLine().Begin().Apply(*MakeAlter().Flush()).Commit();
        db.ToLine().Begin();
        db->Truncate(1, TRowVersion(5, 1));
        db.Commit();
        const auto row = *db.SchemedCookRow(1).Col("a", 1_u64);
        db.ToLine().Begin().WriteTx(123).Add(1, row).Commit();

        for (const auto version : {TRowVersion(4, 1), TRowVersion(5, 1)}) {
            db.ToLine().Begin().WriteVer(version);
            UNIT_ASSERT_EXCEPTION(db.CommitTx(1, 123), yexception);
            db.Reject();
            UNIT_ASSERT(db->HasOpenTx(1, 123));
            db.ToLine().ReadTx(123).Select(1).Has(row);
        }

        db.ToLine().Begin().WriteVer(TRowVersion(6, 1)).CommitTx(1, 123).Commit();
        // A repeated commit would otherwise move the transaction backwards.
        db.ToLine().Begin().WriteVer(TRowVersion(5, 1));
        UNIT_ASSERT_EXCEPTION(db.CommitTx(1, 123), yexception);
        db.Reject();
        db.ToLine().ReadVer(TRowVersion(5, 1)).Select(1).NoKey(row);
        db.ToLine().ReadVer(TRowVersion(6, 1)).Select(1).Has(row);
    }

    Y_UNIT_TEST(ResolvedTransactionsCannotBypassEraseVersion) {
        TDbExec db;
        db.ToLine().Begin().Apply(*MakeAlter().Flush()).Commit();
        db.ToLine().Begin().WriteVer(TRowVersion(4, 1)).CommitTx(1, 123).Commit();
        db.ToLine().Begin();
        db->Truncate(1, TRowVersion(5, 1));
        db.Commit();
        UNIT_ASSERT(db->HasCommittedTx(1, 123));
        const auto row = *db.SchemedCookRow(1).Col("a", 1_u64);

        db.ToLine().Begin().WriteTx(123);
        UNIT_ASSERT_EXCEPTION(db.Add(1, row), yexception);
        db.Reject();
        db.ToLine().Begin().WriteTx(123);
        UNIT_ASSERT_EXCEPTION(db.LockRowN(1, ELockMode::Exclusive, "a"), yexception);
        db.Reject();
        db.ToLine().Select(1).NoKey(row);
    }

    Y_UNIT_TEST(EmptyTableEraseBoundarySurvivesRollbackAndRestore) {
        TDbExec db;
        db.ToLine().Begin().Apply(*MakeAlter().Flush());
        db->Truncate(1, TRowVersion(5, 1));
        db.Commit();
        db.ToLine().Begin();
        db->Truncate(1, TRowVersion(9, 1));
        db.Commit();
        const auto metadata = db->GetVersionedMetadata(1);

        db.ToLine().Begin();
        db->Truncate(1, TRowVersion(12, 1));
        db.Reject();
        const auto row = *db.SchemedCookRow(1).Col("a", 1_u64);
        db.ToLine().Begin().WriteVer(TRowVersion(9, 1));
        UNIT_ASSERT_EXCEPTION(db.Add(1, row), yexception);
        db.Reject();
        db.ToLine().Begin().WriteVer(TRowVersion(10, 1)).Add(1, row).Commit();
        db.ToLine().Select(1).Has(row);

        TDbExec restored;
        restored.ToLine().Begin().Apply(*MakeAlter().Flush()).Commit();
        restored->SetVersionedMetadata(1, metadata);
        restored.ToLine().Begin().WriteVer(TRowVersion(9, 1));
        UNIT_ASSERT_EXCEPTION(restored.Add(1, row), yexception);
        restored.Reject();
        restored.ToLine().Begin().WriteVer(TRowVersion(10, 1)).Add(1, row).Commit();
        restored.ToLine().Select(1).Has(row);
    }

    Y_UNIT_TEST(PhysicalTruncateResetsWriteBoundaryTransactionally) {
        TDbExec db;
        db.ToLine().Begin().Apply(*MakeAlter().Flush()).Commit();
        const auto row = *db.SchemedCookRow(1).Col("a", 1_u64);
        db.ToLine().Begin().Add(1, row).Commit();
        db.ToLine().Begin();
        db->Truncate(1, TRowVersion(5, 1));
        db.Commit();

        db.ToLine().Begin();
        db->Truncate(1);
        db.Add(1, row);
        db.Reject();
        db.ToLine().Select(1).NoKey(row);
        db.ToLine().Begin();
        UNIT_ASSERT_EXCEPTION(db.Add(1, row), yexception);
        db.Reject();

        db.ToLine().Begin();
        db->Truncate(1);
        db.Add(1, row).Commit();
        UNIT_ASSERT(db->GetVersionedMetadata(1).empty());
        db.ToLine().Select(1).Has(row);
    }

    Y_UNIT_TEST(ExpiredPartsUpdateCounters) {
        TDbExec db;
        db.To(1).Begin().Apply(*MakeAlter().Flush()).Commit();

        const auto row = *db.SchemedCookRow(1).Col("a", 1_u64);
        db.To(2).Begin().Add(1, row).Commit();
        db.Snap(1).Compact(1);
        UNIT_ASSERT_VALUES_EQUAL(db->Counters().Parts.PartsCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(db->Counters().Parts.RowsTotal, 1);

        db.To(3).Begin();
        db->Truncate(1, TRowVersion(5, 1));
        db.Commit();

        db.To(4).Begin();
        db->RemoveRowVersions(1, TRowVersion::Min(), TRowVersion(5, 1));
        db.Commit();

        UNIT_ASSERT_VALUES_EQUAL(db.BackLog().Expired.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(db->Counters().Parts.PartsCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(db->Counters().Parts.RowsTotal, 0);
        for (const auto& [tabletId, parts] : db->Counters().PartsPerTablet) {
            Y_UNUSED(tabletId);
            UNIT_ASSERT_VALUES_EQUAL(parts.PartsCount, 0);
        }
        UNIT_ASSERT(!db->HasEraseAll(1));
        db.To(5).Iter(1).NoKey(row);

        db.To(6).Begin().WriteVer(TRowVersion(5, 1));
        UNIT_ASSERT_EXCEPTION(db.Add(1, row), yexception);
        db.Reject();
        db.To(7).Begin().WriteVer(TRowVersion(6, 1)).Add(1, row).Commit();
        db.To(8).Iter(1).Has(row);
    }

    Y_UNIT_TEST(UnchangedMetadataIsNotLogged) {
        TDbExec db;
        db.To(1).Begin().Apply(*MakeAlter().Flush()).Commit();
        const auto row = *db.SchemedCookRow(1).Col("a", 1_u64);
        db.To(2).Begin().Add(1, row).Commit();
        db.Snap(1).Compact(1);

        db.To(10).Begin();
        db->Truncate(1, TRowVersion(5, 1));
        db->RemoveRowVersions(1, TRowVersion::Min(), TRowVersion(3, 1));
        db.Commit();
        UNIT_ASSERT_VALUES_EQUAL(db.BackLog().VersionedMetadata.size(), 1);

        db.To(11).Begin();
        db->RemoveRowVersions(1, TRowVersion::Min(), TRowVersion(4, 1));
        db.Commit();
        UNIT_ASSERT(db.BackLog().VersionedMetadata.empty());
        UNIT_ASSERT(db.BackLog().Expired.empty());
        UNIT_ASSERT_VALUES_EQUAL(db->GetVersionedMetadata(1).size(), 1);

        db.To(12).Begin();
        db->RemoveRowVersions(1, TRowVersion(4, 1), TRowVersion(5, 1));
        db.Commit();
        UNIT_ASSERT_VALUES_EQUAL(db.BackLog().Expired.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(db.BackLog().VersionedMetadata.size(), 1);
        const auto& retired = db.BackLog().VersionedMetadata.at(1);
        UNIT_ASSERT_VALUES_EQUAL(retired.size(), 1);
        UNIT_ASSERT(retired[0].Version == TRowVersion(5, 1));
        UNIT_ASSERT(retired[0].Effects.empty());
    }

    Y_UNIT_TEST(FilteredRunsSurviveSnapshotChanges) {
        TDbExec db;
        db.To(1).Begin().Apply(*MakeAlter().Flush()).Commit();
        const auto oldRow = *db.SchemedCookRow(1).Col("b", 1_u64);
        const auto first = *db.SchemedCookRow(1).Col("a", 2_u64);
        const auto last = *db.SchemedCookRow(1).Col("c", 3_u64);
        db.To(2).Begin().Add(1, oldRow).Commit();
        db.Snap(1).Compact(1);

        db.To(10).Begin();
        db->Truncate(1, TRowVersion(5, 1));
        db.Commit();
        db.To(11).Begin().WriteVer(TRowVersion(6, 1)).Add(1, first).Commit();
        db.Snap(1).Compact(1, false);
        db.To(20).Begin().WriteVer(TRowVersion(6, 1)).Add(1, last).Commit();
        db.Snap(1).Compact(1, false);

        {
            auto head = db.IterData(1);
            head.Seek({ }, ESeek::Lower).Is(first);
            UNIT_ASSERT_VALUES_EQUAL(head->KeepRuns().size(), 1);

            auto again = db.ReadVer(TRowVersion(6, 1)).IterData(1);
            again.Seek({ }, ESeek::Lower).Is(first);
            UNIT_ASSERT_VALUES_EQUAL(again->KeepRuns().size(), 1);
            UNIT_ASSERT(head->KeepRuns()[0] == again->KeepRuns()[0]);

            // Source visibility is unchanged at the erase boundary, even though
            // MVCC hides the surviving parts' newer rows at this snapshot.
            auto boundary = db.ReadVer(TRowVersion(5, 1)).IterData(1);
            boundary.Seek({ }, ESeek::Lower).Is(EReady::Gone);
            UNIT_ASSERT_VALUES_EQUAL(boundary->KeepRuns().size(), 1);
            UNIT_ASSERT(head->KeepRuns()[0] == boundary->KeepRuns()[0]);

            const TRowTool tool(*(*head).Scheme);
            const auto point = tool.Split(oldRow, true, false);
            const auto tags = (*head).Scheme->Tags();
            UNIT_ASSERT_VALUES_EQUAL(db->Precharge(1, point.Key, point.Key, tags, 0,
                Max<ui64>(), Max<ui64>()).ItemsPrecharged, 0);
            UNIT_ASSERT(db->Precharge(1, point.Key, point.Key, tags, 0,
                Max<ui64>(), Max<ui64>(), EDirection::Forward, TRowVersion(4, 1)).ItemsPrecharged > 0);

            // Replacing the cached visibility interval must not invalidate live readers.
            db.ReadVer(TRowVersion(4, 1)).IterData(1)
                .Seek({ }, ESeek::Lower).Is(oldRow).Next().Is(EReady::Gone);
            head.Next().Is(last).Next().Is(EReady::Gone);
            again.Next().Is(last).Next().Is(EReady::Gone);

            TChecker<TWrapDbReverseIter, TDatabase&> reverse(
                *db.operator->(), { }, 1, (*head).Scheme);
            reverse.Seek({ }, ESeek::Lower).Is(last).Next().Is(first).Next().Is(EReady::Gone);
        }

        // A metadata change and its rollback must both invalidate cached visibility.
        db.To(30).Begin();
        db->Truncate(1, TRowVersion(10, 1));
        db.IterData(1).Seek({ }, ESeek::Lower).Is(EReady::Gone);
        db.Reject();
        db.IterData(1).Seek({ }, ESeek::Lower).Is(first).Next().Is(last).Next().Is(EReady::Gone);

        // Incrementally adding a part also invalidates the cached run.
        const auto added = *db.SchemedCookRow(1).Col("d", 4_u64);
        db.To(31).Begin().WriteVer(TRowVersion(6, 1)).Add(1, added).Commit();
        db.Snap(1).Compact(1, false);
        db.IterData(1).Seek({ }, ESeek::Lower).Is(first).Next().Is(last).Next().Is(added).Next().Is(EReady::Gone);

        // A second erase bounds the reusable interval on both sides.
        db.To(40).Begin();
        db->Truncate(1, TRowVersion(10, 1));
        db.Commit();
        const auto newest = *db.SchemedCookRow(1).Col("e", 5_u64);
        db.To(41).Begin().WriteVer(TRowVersion(11, 1)).Add(1, newest).Commit();
        db.Snap(1).Compact(1, false);

        auto middle = db.ReadVer(TRowVersion(9, 1)).IterData(1);
        middle.Seek({ }, ESeek::Lower).Is(first);
        UNIT_ASSERT_VALUES_EQUAL(middle->KeepRuns().size(), 1);
        auto earlier = db.ReadVer(TRowVersion(6, 1)).IterData(1);
        earlier.Seek({ }, ESeek::Lower).Is(first);
        UNIT_ASSERT_VALUES_EQUAL(earlier->KeepRuns().size(), 1);
        UNIT_ASSERT(middle->KeepRuns()[0] == earlier->KeepRuns()[0]);

        auto boundary = db.ReadVer(TRowVersion(10, 1)).IterData(1);
        boundary.Seek({ }, ESeek::Lower).Is(EReady::Gone);
        UNIT_ASSERT_VALUES_EQUAL(boundary->KeepRuns().size(), 1);
        UNIT_ASSERT(middle->KeepRuns()[0] != boundary->KeepRuns()[0]);
        auto head = db.ReadVer(TRowVersion::Max()).IterData(1);
        head.Seek({ }, ESeek::Lower).Is(newest).Next().Is(EReady::Gone);
        UNIT_ASSERT_VALUES_EQUAL(head->KeepRuns().size(), 1);
        UNIT_ASSERT(boundary->KeepRuns()[0] == head->KeepRuns()[0]);
        middle.Next().Is(last).Next().Is(added).Next().Is(EReady::Gone);
        earlier.Next().Is(last).Next().Is(added).Next().Is(EReady::Gone);
    }

    Y_UNIT_TEST(EraseCacheDoesNotHideSnapshotRows) {
        TDbExec db;
        auto alter = MakeAlter();
        alter.SetEraseCache(1, true, 2, 8192);
        db.To(1).Begin().Apply(*alter.Flush()).Commit();

        const auto row = *db.SchemedCookRow(1).Col("b", 1_u64);
        db.To(2).Begin().WriteVer(TRowVersion(1, 1)).Add(1, row).Commit();
        db.To(3).Begin();
        db->Truncate(1, TRowVersion(5, 1));
        db.Commit();

        // Tombstones written after the erase must not hide the old snapshot.
        db.To(4).Begin().WriteVer(TRowVersion(6, 1)).EraseN(1, "a").EraseN(1, "c").Commit();
        db.To(5).IterData(1).Seek({ }, ESeek::Lower).Is(EReady::Gone);
        db.To(6).ReadVer(TRowVersion(4, 1)).IterData(1)
            .Seek({ }, ESeek::Lower).Is(row).Next().Is(EReady::Gone);
    }

}

}
}

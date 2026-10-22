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

    Y_UNIT_TEST(SeveralEffectsAtOneVersionAndCoalesce) {
        TDbExec db;
        db.To(1).Begin().Apply(*MakeAlter().Flush()).Commit();

        const auto first = *db.SchemedCookRow(1).Col("a", 1_u64);
        const auto second = *db.SchemedCookRow(1).Col("b", 2_u64);
        const auto third = *db.SchemedCookRow(1).Col("c", 3_u64);

        db.To(2).Begin().Add(1, first).Commit();
        db.To(3).Begin();
        db->Truncate(1, TRowVersion(5, 1));
        db.Commit();

        db.To(4).Begin().WriteVer(TRowVersion(6, 1)).Add(1, second).Commit();
        db.To(5).Begin();
        db->Truncate(1, TRowVersion(5, 1));
        db.Commit();

        const auto& metadata = db->GetVersionedMetadata(1);
        UNIT_ASSERT_VALUES_EQUAL(metadata.size(), 1);
        UNIT_ASSERT(metadata[0].Version == TRowVersion(5, 1));
        UNIT_ASSERT_VALUES_EQUAL(metadata[0].Effects.size(), 2);

        db.To(6).ReadVer(TRowVersion::Max()).Iter(1).NoKey(first).NoKey(second);
        db.To(7).ReadVer(TRowVersion(4, 1)).Iter(1).Has(first);

        const auto versionBefore = db->GetVersionedMetadata(1)[0].Version;
        const auto effectsBefore = db->GetVersionedMetadata(1)[0].Effects.size();
        db.To(8).Begin();
        db->Truncate(1, TRowVersion(9, 1));
        db.Commit();
        UNIT_ASSERT(db->GetVersionedMetadata(1)[0].Version == versionBefore);
        UNIT_ASSERT_VALUES_EQUAL(db->GetVersionedMetadata(1)[0].Effects.size(), effectsBefore);

        db.To(9).Begin().WriteVer(TRowVersion(7, 1)).Add(1, third).Commit();
        db.To(10).ReadVer(TRowVersion::Max()).Iter(1).Has(third).NoKey(second);
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

    Y_UNIT_TEST(EarlierEraseIsNotCoalesced) {
        TDbExec db;
        db.To(1).Begin().Apply(*MakeAlter().Flush()).Commit();

        const auto row = *db.SchemedCookRow(1).Col("a", 1_u64);
        db.To(2).Begin().Add(1, row).Commit();
        db.To(3).Begin();
        db->Truncate(1, TRowVersion(10, 1));
        db.Commit();

        db.To(4).Begin();
        db->Truncate(1, TRowVersion(5, 1));
        db.Commit();

        db.To(5).ReadVer(TRowVersion(4, 1)).Iter(1).Has(row);
        db.To(6).ReadVer(TRowVersion(5, 1)).Iter(1).NoKey(row);
        db.To(7).ReadVer(TRowVersion(5, 1)).Select(1).NoKey(row);
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
        UNIT_ASSERT(db.BackLog().VersionedMetadata.at(1).empty());
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

        // At the head these tombstones surround a hidden row. Their versions
        // alone cannot describe when the whole range became invisible.
        db.To(4).Begin().WriteVer(TRowVersion(2, 1)).EraseN(1, "a").EraseN(1, "c").Commit();
        db.To(5).IterData(1).Seek({ }, ESeek::Lower).Is(EReady::Gone);
        db.To(6).ReadVer(TRowVersion(4, 1)).IterData(1)
            .Seek({ }, ESeek::Lower).Is(row).Next().Is(EReady::Gone);
    }

}

}
}

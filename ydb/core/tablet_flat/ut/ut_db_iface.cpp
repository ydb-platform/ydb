#include <ydb/core/tablet_flat/flat_dbase_scheme.h>
#include <ydb/core/tablet_flat/test/libs/table/test_dbase.h>
#include <ydb/core/tablet_flat/test/libs/rows/cook.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/stream/file.h>

namespace NKikimr {
namespace NTable {

Y_UNIT_TEST_SUITE(DBase) {
    using namespace NTest;

    TAlter MakeAlter(const ui32 table = 1)
    {
        TAlter alter;

        alter
            .AddTable(Sprintf("me_%02u", table), table)
            .AddColumn(table, "key",    1, ETypes::String, false)
            .AddColumn(table, "arg1",   4, ETypes::Uint64, false, Cimple(77_u64))
            .AddColumn(table, "arg2",   5, ETypes::String, false)
            .AddColumnToKey(table, 1);

        return alter;
    }

    Y_UNIT_TEST(Basics)
    {
        TDbExec me;

        me.To(10).Begin().Apply(*MakeAlter().Flush()).Commit();

        const auto nil = *me.SchemedCookRow(1).Col(nullptr);
        const auto foo = *me.SchemedCookRow(1).Col("foo", 33_u64);
        const auto bar = *me.SchemedCookRow(1).Col("bar", 11_u64);
        const auto ba1 = *me.SchemedCookRow(1).Col("bar", ECellOp::Empty, "yo");
        const auto ba2 = *me.SchemedCookRow(1).Col("bar", ECellOp::Reset, "me");
        const auto ba3 = *me.SchemedCookRow(1).Col("bar", 99_u64, ECellOp::Reset);
        const auto ba4 = *me.SchemedCookRow(1).Col("bar", ECellOp::Null, "eh");

        me.To(11).Iter(1).NoKey(foo).NoKey(bar);

        /*_ 10: Check rollback and next working tx  */

        me.To(12).Begin().Add(1, foo).Add(1, nil).Commit();
        me.To(13).Iter(1).Has(foo).HasN(nullptr, 77_u64).NoKey(bar);
        me.To(14).Begin().Add(1, bar).Reject();
        me.To(15).Iter(1).Has(foo).NoKey(bar);
        me.To(16).Begin().Add(1, bar).Commit();
        me.To(17).Iter(1).Has(foo).Has(bar);
        me.To(18).Affects(0, { 1 });

        /* The following update just puts flush event to redo log, content
            of alter script have no any other sense in context of this test.
            Flush event will be checked on changes rollup and in redo log
            ABI tests which uses log from this test.
         */

        me.To(19).Begin().Apply(*TAlter().SetRoom(1, 0, 1, 2, 1)).Commit();

        /*_ 20: Check that log is applied correctly */

        me.To(20).Replay(EPlay::Boot).Iter(1).Has(foo).Has(bar);
        me.To(22).Replay(EPlay::Redo).Iter(1).Has(foo).Has(bar);

        /*_ 30: Check erase of some row and update  */

        me.To(30).Begin().Add(1, ba1).Add(1, foo, ERowOp::Erase).Commit();
        me.To(31).Iter(1, false).NoKey(foo).NoVal(bar);
        me.To(32).Iter(1, false).HasN("bar", 11_u64, "yo"); /* omited arg1  */
        me.To(33).Begin().Add(1, ba2).Commit();
        me.To(34).Iter(1, false).HasN("bar", 77_u64, "me"); /* default arg1 */
        me.To(35).Begin().Add(1, ba3).Commit();
        me.To(36).Iter(1, false).HasN("bar", 99_u64, nullptr);
        me.To(37).Begin().Add(1, ba4).Commit();
        me.To(38).Iter(1, false).HasN("bar", nullptr, "eh").Has(ba4);

        /*_ 40: Finally reboot again and check result */

        me.To(40).Replay(EPlay::Boot).Iter(1, false).Has(ba4).NoKey(foo);
        me.To(41).Replay(EPlay::Redo).Iter(1, false).Has(ba4).NoKey(foo);

        /*_ 50: Erase and update the same row in tx */

        me.To(50).Begin().Add(1, bar, ERowOp::Erase).Add(1, bar).Commit();
        me.To(51).Iter(1, false).Has(bar).NoKey(foo);

        /*_ 60: Check rows counters after compaction */

        me.To(60).Snap(1).Compact(1, false).Iter(1, false).Has(bar);

        UNIT_ASSERT(me->Counters().Parts.RowsTotal == 3);
        UNIT_ASSERT(me->Counters().Parts.RowsErase == 1);
        UNIT_ASSERT(me.GetLog().size() == 10);
    }

    Y_UNIT_TEST(Select)
    {
        TDbExec me;

        me.To(10).Begin().Apply(*MakeAlter().Flush()).Commit();

        const auto bar = *me.SchemedCookRow(1).Col("bar", 11_u64);
        const auto ba1 = *me.SchemedCookRow(1).Col("bar", ECellOp::Empty, "yo");
        const auto ba2 = *me.SchemedCookRow(1).Col("bar", ECellOp::Reset, "me");
        const auto ba3 = *me.SchemedCookRow(1).Col("bar", 99_u64, ECellOp::Reset);
        const auto ba4 = *me.SchemedCookRow(1).Col("bar", ECellOp::Null, "eh");

        me.To(11).Begin().Add(1, bar).Commit().Select(1).Has(bar);
        me.To(12).Snap(1).Compact(1, false).Select(1).Has(bar);
        me.To(20).Begin().Add(1, ba1).Commit().Snap(1).Compact(1, false);
        me.To(21).Select(1).HasN("bar", 11_u64, "yo");
        me.To(30).Begin().Add(1, ba2).Commit().Snap(1).Compact(1, false);
        me.To(31).Select(1).HasN("bar", 77_u64, "me");
        me.To(40).Begin().Add(1, ba3).Commit().Snap(1);
        me.To(41).Select(1).HasN("bar", 99_u64, nullptr);
        me.To(50).Begin().Add(1, ba4).Commit().Snap(1);
        me.To(51).Select(1).Has(ba4);
        me.To(60).Begin().Add(1, ba3, ERowOp::Erase).Commit().Snap(1);
        me.To(61).Select(1).NoKey(bar);
        me.To(62).Begin().Add(1, bar, ERowOp::Upsert).Commit();
        me.To(63).Select(1).Has(bar);

        /* Ensure that test was acomplished on non-trivial subset */

        const auto subset = me->Subset(1, TEpoch::Max(), { }, { });

        UNIT_ASSERT(subset->Flatten.size() == 3 && subset->Frozen.size() == 3);
    }

    Y_UNIT_TEST(Defaults)
    {
        TDbExec me;

        const TString large30("0123456789abcdef0123456789abcd");
        const TString large35("0123456789abcdef0123456789abcdef012");

        me.To(10).Begin().Apply(*MakeAlter().Flush());

        const auto nuke = *me.SchemedCookRow(1).Col(nullptr, 11_u64);

        me.To(11).Add(1, nuke).Commit().Iter(1).Has(nuke);

        me.To(12).Begin().Apply(
            *TAlter()
                .AddColumn(1, "sub", 2, ETypes::Uint32, false, Cimple(77_u32))
                .AddColumn(1, "en0", 6, ETypes::String, false, TCell("", 0))
                .AddColumn(1, "en1", 7, ETypes::String, false, TCell(large30))
                .AddColumn(1, "en2", 8, ETypes::String, false, TCell(large35))
                .DropColumn(1, 5)
                .AddColumnToKey(1, 2));

        const auto foo = *me.SchemedCookRow(1).Col("foo", nullptr, 33_u64);
        const auto cap = *me.SchemedCookRow(1).Col("cap", TOmit{ }, 33_u64);
        const auto bar = *me.SchemedCookRow(1).Col("bar", 11_u32, 33_u64, "me", "you");
        const auto ba8 = *me.SchemedCookRow(1).Col("bar", 11_u32, 33_u64, "");
        const auto ba9 = *me.SchemedCookRow(1).Col("bar", 11_u32, 33_u64, nullptr);

        me.To(13).Put(1, foo, cap, bar).Commit();
        me.Iter(1)
                .To(14).HasN(nullptr, 77_u32, 11_u64, "", large30, large35)
                .To(15).HasN("foo", nullptr, 33_u64, "", large30, large35)
                .To(15).HasN("cap", 77_u32, 33_u64, "", large30, large35)
                .To(16).HasN("bar", 11_u32, 33_u64, "me", "you", large35)
                .To(17).NoVal(ba8).NoVal(ba9);

        me.To(20).Replay(EPlay::Boot); /* check incomplete keys rollup */

        me.Iter(1)
            .To(21).HasN(nullptr, 77_u32, 11_u64, "", large30, large35)
            .To(22).HasN("foo", nullptr, 33_u64, "", large30, large35)
            .To(23).HasN("cap", 77_u32, 33_u64, "", large30, large35)
            .To(24).HasN("bar", 11_u32, 33_u64, "me", "you", large35);
    }

    Y_UNIT_TEST(Subsets)
    {
        TDbExec me;

        me.To(10).Begin().Apply(*MakeAlter().Flush()).Commit();

        const auto ro1 = *me.SchemedCookRow(1).Col("row1", 11_u64, "foo");
        const auto ro2 = *me.SchemedCookRow(1).Col("row2", 22_u64, "bar");
        const auto ro3 = *me.SchemedCookRow(1).Col("row3", 33_u64, "foo");
        const auto ro4 = *me.SchemedCookRow(1).Col("row4", 44_u64, "bar");

        me.Begin().Put(1, ro1, ro2).Commit().Snap(1).Compact(1, false);
        me.Begin().Put(1, ro3, ro4).Commit().Snap(1).Compact(1, false);

        { /*_ Check memtable snapshots for compactions */
            auto subset = me->Subset(1, TEpoch::Max(), { }, { });

            UNIT_ASSERT(subset->Flatten.size() == 2 && subset->Frozen.size() == 0);
            UNIT_ASSERT(subset->Flatten[0]->Stat.Rows == 2);
            UNIT_ASSERT(subset->Flatten[1]->Stat.Rows == 2);
        }

        me.To(12).Iter(1).Has(ro1).Has(ro2).Has(ro3).Has(ro4);
        me.Compact(1, true); /* should collapse all parts to one */
        me.To(13).Iter(1).Has(ro1).Has(ro2).Has(ro3).Has(ro4);

        { /*_ Check Replace(...) with non-trivail subsets */
            auto subset = me->Subset(1, TEpoch::Max(), { }, { });

            UNIT_ASSERT(subset->Flatten.size() == 1 && subset->Frozen.size() == 0);
            UNIT_ASSERT(subset->Flatten[0]->Stat.Rows == 4);
            UNIT_ASSERT(subset->Flatten[0]->Stat.Drops == 0);
        }

        { /*_ Ensure that debug dumper doesn't crash */
            TStringStream dump;

            me.Begin()->DebugDump(dump, *DbgRegistry());

            UNIT_ASSERT(dump.Str().size() > 20 && dump.Str().size() < 10000);
        }
    }

    Y_UNIT_TEST(Garbage)
    {
        TDbExec me;

        me.To(10).Begin().Apply(*MakeAlter().Flush()).Commit();

        const auto ro1 = *me.SchemedCookRow(1).Col("foo", 11_u64, "boo");
        const auto ro2 = *me.SchemedCookRow(1).Col("foo", 22_u64);
        const auto ro3 = *me.SchemedCookRow(1).Col("foo");
        const auto ro4 = *me.SchemedCookRow(1).Col("foo", 99_u64);

        /*_ 10: Cut columns from table updating reduced row each time   */

        me.Begin().Add(1, ro1).Commit().Snap(1);

        me.To(11).Compact(1, false).Iter(1).Has(ro1);

        me.Begin().Apply(*TAlter().DropColumn(1, 5)).Add(1, ro2).Commit();

        me.To(12).Compact(1, false).Iter(1).Has(ro2);

        me.Begin().Apply(*TAlter().DropColumn(1, 4)).Add(1, ro3).Commit();

        me.To(14).Compact(1, false).Iter(1).Has(ro3);

        { /*_ Ressurect column, ** hack allowed only in this UT ** */
            auto raw = TAlter().AddColumn(1, "new", 4, ETypes::Uint64, false).Flush();

            me.Begin().Apply(*raw).Add(1, ro4).Commit();
        }

        UNIT_ASSERT(me->Counters().MemTableOps == 2);
        UNIT_ASSERT(me->Counters().MemTableBytes > 0);
        UNIT_ASSERT(me->Counters().Parts.PlainBytes > 0);

        me.To(16).Compact(1, false).Iter(1).Has(ro4); /* NOOP compaction */

        /*_ 20: Add one more table to be garbage in final blow */

        me.To(20).Begin().Apply(*MakeAlter(2).Flush()).Commit();
        me.To(21).Begin().Put(2, ro1, ro2).Commit().Snap(2).Compact(2);

        UNIT_ASSERT(me->Counters().Parts.PartsCount == 4);

        /*_ 40: Finally drop entire table and check presence of garbage */

        me.Begin().Apply(*TAlter().DropTable(1).DropTable(2)).Commit();

        UNIT_ASSERT(me.BackLog().Garbage.size() == 2);
        UNIT_ASSERT(me.BackLog().Garbage[0]->Flatten.size() == 3);
        UNIT_ASSERT(me.BackLog().Garbage[0]->Frozen.size() == 1);
        UNIT_ASSERT(me.BackLog().Garbage[1]->Flatten.size() == 1);
        UNIT_ASSERT(me.BackLog().Garbage[1]->Frozen.size() == 0);
        UNIT_ASSERT(me->Counters().MemTableOps == 0);
        UNIT_ASSERT(me->Counters().MemTableBytes == 0);
        UNIT_ASSERT(me->Counters().Parts.RowsTotal == 0);
        UNIT_ASSERT(me->Counters().Parts.RowsErase == 0);
        UNIT_ASSERT(me->Counters().Parts.PartsCount == 0);
        UNIT_ASSERT(me->Counters().Parts.PlainBytes == 0);
        UNIT_ASSERT(me->Counters().Parts.FlatIndexBytes == 0);
        UNIT_ASSERT(me->Counters().Parts.BTreeIndexBytes == 0);
        UNIT_ASSERT(me->Counters().Parts.OtherBytes == 0);
    }

    Y_UNIT_TEST(Affects)
    {
        TDbExec me;

        me.To(10).Begin().Apply(*MakeAlter(2)).Commit();

        const auto row = *me.SchemedCookRow(2).Col("foo", 11_u64, "bar");

        /*_ 10: Basic affects on one table modification */

        me.To(12).Begin().Apply(*MakeAlter(1)).Add(1, row);
        me.To(14).Commit().Affects(0, { 1 }).Select(1).Has(row);

        /*_ 20: Rerolling the same scheme has no effects */

        me.To(20).Begin().Apply(*MakeAlter(1)).Apply(*MakeAlter(2));
        me.To(21).Commit().Affects(0, { }).Select(1).Has(row);

        UNIT_ASSERT(me.BackLog().Scheme == nullptr);

        /*_ 30: Affects from table drop and snapshots   */

        me.To(30).Begin().Apply(*TAlter().DropTable(1));
        me.To(31).Commit().Affects(0, { });
        me.To(32).Begin().Add(2, row).Commit().Affects(0, { 2 });
        auto epoch = me.To(33).Begin().Snapshot(2);
        UNIT_ASSERT(epoch == TEpoch::FromIndex(2));
        me.Commit().Affects(0, { 2 });
        me.To(34).Compact(2);

        UNIT_ASSERT(me->Counters().MemTableOps == 0);
        UNIT_ASSERT(me.BackLog().Snapshots == 1);

        {
            const auto subset = me->Subset(2, TEpoch::Max(), { }, { });

            UNIT_ASSERT(subset->Flatten.size() == 1 && subset->Frozen.size() == 0);
        }

        me.To(40).Replay(EPlay::Boot).Replay(EPlay::Redo);
    }

    Y_UNIT_TEST(WideKey)
    {
        const TIntrusivePtr<TGrowHeap> heap = new TGrowHeap(128 * 1024);

        for (auto keys: { 128, 512, 1024, 4096, 8192, 9999 }) {
            auto alter = MakeAlter();

            for (auto sub : xrange(keys)) {
                auto name = Sprintf("sub_%04u", sub);
                alter.AddColumn(1, name, 6 + sub, ETypes::Uint32, false);
                alter.AddColumnToKey(1, 6 + sub);
            }

            TRow row(heap);

            row.Do(1, "foo").Do(4, 77_u64);

            for (auto sub : xrange(keys))
                row.Do(6 + sub, ui32(7 + sub));

            TDbExec me;

            me.To(keys * 10 + 1).Begin().Apply(*alter).Add(1, row);
            me.To(keys * 10 + 2).Commit().Iter(1).Has(row);
            me.To(keys * 10 + 3).Compact(1, true).Iter(1).Has(row);
        }
    }

    Y_UNIT_TEST(Annex)
    {
        auto alter = MakeAlter(1);

        alter.SetRedo(32).SetFamilyBlobs(1, 0, Max<ui32>(), 24);

        TDbExec me;

        me.To(10).Begin().Apply(*alter.Flush()).Commit();

        const TString large30("0123456789abcdef0123456789abcd");
        const TString large35("0123456789abcdef0123456789abcdef012");
        const TString large42("0123456789abcdef0123456789abcdef0123456789");
        const TString large44("0123456789abcdef0123456789abcdef0123456789ab");

        const auto foo  = *me.SchemedCookRow(1).Col("foo", 77_u64, "foo");
        const auto ro30 = *me.SchemedCookRow(1).Col("l30", 30_u64, large30);
        const auto ro35 = *me.SchemedCookRow(1).Col("l35", 35_u64, large35);
        const auto ro42 = *me.SchemedCookRow(1).Col("l42", 42_u64, large42);
        const auto ro44 = *me.SchemedCookRow(1).Col("l44", 44_u64, large44);
        const auto zap  = *me.SchemedCookRow(1).Col("zap", 55_u64, "zap");

        /*_ 10: Regular write without producing annex to redo log */

        me.To(12).Begin().Put(1, foo, ro30).Commit();
        me.To(13).Iter(1, false).Has(foo).Has(ro30);

        UNIT_ASSERT(me.BackLog().Annex.size() == 0);

        /*_ 20: Put oversized blobs that should be placed to annex */

        me.To(20).Begin().Put(1, zap, ro35, ro42).Commit();
        me.To(21).Iter(1, false).Has(zap).Has(ro35).Has(ro42);

        UNIT_ASSERT(me.BackLog().Annex.size() == 2);
        UNIT_ASSERT(me.BackLog().Annex[0].Data.size() == 8 + 35);
        UNIT_ASSERT(me.BackLog().Annex[1].Data.size() == 8 + 42);

        /*_ 26: Annex should be promoted to external blobs in TMemTable */

        auto subset = me.To(26).Snap(1)->Subset(1, TEpoch::Max(), { }, { });

        UNIT_ASSERT(subset->Frozen.size() == 1 && subset->Flatten.size() == 0);
        UNIT_ASSERT(subset->Frozen[0]->GetBlobs()->Tail() == 2);

        /* Extra 8 bytes in each annex or external blob is occupied by
            NPage::TLabel prefix which prefixes many binary units in NTable
         */

        UNIT_ASSERT(subset->Frozen[0]->GetBlobs()->GetRaw(0).Data.size() == 8 + 35);
        UNIT_ASSERT(subset->Frozen[0]->GetBlobs()->GetRaw(1).Data.size() == 8 + 42);

        /*_ 28: Test blobs refereces generation after TMemTable snapshot */

        me.To(28).Begin().Put(1, ro44).Commit().Iter(1, false).Has(ro44);

        UNIT_ASSERT(me.BackLog().Annex.size() == 1);
        UNIT_ASSERT(me.BackLog().Annex[0].Data.size() == 8 + 44);

        /*_ 30: Check that redo log replaying handle annex correctly */

        me.To(30).Replay(EPlay::Boot).Iter(1).Has(foo, ro30, ro35, ro42, ro44);
        me.To(31).Replay(EPlay::Redo).Iter(1).Has(foo, ro30, ro35, ro42, ro44);

        /*_ 40: Compaction should turn all largeXX to external blobs */

        auto last = me.To(40).Snap(1).Compact(1)->Subset(1, TEpoch::FromIndex(666), { }, { });

        UNIT_ASSERT(last->Frozen.size() == 0 && last->Flatten.size() == 1);

        UNIT_ASSERT(last->Flatten[0]->Blobs->Total() == 4);
        UNIT_ASSERT(last->Flatten[0]->Blobs->Glob(0).Bytes() == 8 + 30);
        UNIT_ASSERT(last->Flatten[0]->Blobs->Glob(1).Bytes() == 8 + 35);
        UNIT_ASSERT(last->Flatten[0]->Blobs->Glob(2).Bytes() == 8 + 42);
        UNIT_ASSERT(last->Flatten[0]->Blobs->Glob(3).Bytes() == 8 + 44);

        UNIT_ASSERT(me->Counters().Parts.LargeItems == 4);
        UNIT_ASSERT(me->Counters().Parts.LargeBytes == 4 * 8 + 30 + 35 + 42 + 44);

        me.To(41).Iter(1).Has(foo).Has(foo, ro30, ro35, ro42, ro44, zap);

        /*_ 50: Finally, check that annexed counter really grows */

        me.To(50).Begin().Put(1, zap).Commit().Snap(1);

        auto closed = me->Subset(1, TEpoch::Max(), { }, { });

        UNIT_ASSERT(closed->Frozen.at(0)->GetBlobs()->Head == 3);
    }

    Y_UNIT_TEST(AnnexRollbackChanges)
    {
        auto alter = MakeAlter(1);

        alter.SetRedo(32).SetFamilyBlobs(1, 0, Max<ui32>(), 24);

        TDbExec me;

        me.To(10).Begin().Apply(*alter.Flush()).Commit();

        const TString large35("0123456789abcdef0123456789abcdef012");
        const TString large42("0123456789abcdef0123456789abcdef0123456789");

        me.To(12).Begin().PutN(1, "l35", 35_u64, large35);
        me.To(13).RollbackChanges().PutN(1, "l42", 42_u64, large42).Commit();
        me.To(14).Iter(1)
            .Seek({}, ESeek::Lower).IsN("l42", 42_u64, large42)
            .Next().Is(EReady::Gone);

        UNIT_ASSERT(me.BackLog().Annex.size() == 1);
        UNIT_ASSERT(me.BackLog().Annex[0].Data.size() == 8 + 42);

        me.To(21).Replay(EPlay::Boot).Iter(1)
            .Seek({}, ESeek::Lower).IsN("l42", 42_u64, large42)
            .Next().Is(EReady::Gone);
        me.To(22).Replay(EPlay::Redo).Iter(1)
            .Seek({}, ESeek::Lower).IsN("l42", 42_u64, large42)
            .Next().Is(EReady::Gone);
    }

    Y_UNIT_TEST(Outer)
    {
        auto alter = MakeAlter(1);

        TDbExec me;

        me.To(10).Begin().Apply(*alter.SetFamilyBlobs(1, 0, 17, 999)).Commit();

        const TString small17("0123456789abcdef0");
        const TString small21("0123456789abcdef01234");

        const auto foo  = *me.SchemedCookRow(1).Col("foo", 77_u64, "foo");
        const auto ro17 = *me.SchemedCookRow(1).Col("s17", 17_u64, small17);
        const auto ro21 = *me.SchemedCookRow(1).Col("s21", 21_u64, small21);

        /*_ 10: Prepare two parts with attached outer blobs */

        me.To(12).Begin().Put(1, foo, ro17).Commit().Snap(1).Compact(1, false);
        me.To(14).Begin().Put(1, foo, ro21).Commit().Snap(1).Compact(1, false);
        me.To(16).Iter(1, false).Has(foo, ro17, ro21);

        auto subset = me.To(18)->Subset(1, TEpoch::Max(), { }, { });

        UNIT_ASSERT(subset->Flatten.size() == 2 && subset->Flatten[0]->Small);
        UNIT_ASSERT(subset->Flatten[0]->Small->Stats().Items == 1);
        UNIT_ASSERT(me->Counters().Parts.SmallItems == 2);
        UNIT_ASSERT(me->Counters().Parts.SmallBytes == 2 * 8 + 17 + 21);
        UNIT_ASSERT(me->Counters().Parts.OtherBytes > 30);

        /*_ 20: Compact all data to the single final part */

        auto last = me.To(20).Compact(1, true)->Subset(1, TEpoch::FromIndex(666), { }, { });

        UNIT_ASSERT(last->Flatten.size() == 1 && last->Flatten[0]->Small);
        UNIT_ASSERT(last->Flatten[0]->Small->Stats().Items == 2);
        UNIT_ASSERT(last->Flatten[0]->Small->Stats().Size == 54);
        UNIT_ASSERT(me->Counters().Parts.SmallItems == 2);
        UNIT_ASSERT(me->Counters().Parts.SmallBytes == 2 * 8 + 17 + 21);
        UNIT_ASSERT(me->Counters().Parts.OtherBytes > 30);

        /*_ 30: Clean database and check that all data gone */

        me.To(30).Begin().Apply(*TAlter().DropTable(1)).Commit();

        UNIT_ASSERT(me->Counters().Parts.SmallItems == 0);
        UNIT_ASSERT(me->Counters().Parts.SmallBytes == 0);
        UNIT_ASSERT(me->Counters().Parts.OtherBytes == 0);
    }

    Y_UNIT_TEST(VersionBasics)
    {
        TDbExec me;

        me.To(10).Begin().Apply(*MakeAlter().Flush()).Commit();

        const auto nil = *me.SchemedCookRow(1).Col(nullptr);
        const auto foo = *me.SchemedCookRow(1).Col("foo", 33_u64);
        const auto bar = *me.SchemedCookRow(1).Col("bar", 11_u64);
        const auto ba1 = *me.SchemedCookRow(1).Col("bar", ECellOp::Empty, "yo");
        const auto ba2 = *me.SchemedCookRow(1).Col("bar", ECellOp::Reset, "me");
        const auto ba3 = *me.SchemedCookRow(1).Col("bar", 99_u64, ECellOp::Reset);
        const auto ba4 = *me.SchemedCookRow(1).Col("bar", ECellOp::Null, "eh");

        me.To(11).Iter(1).NoKey(foo).NoKey(bar);

        /*_ 10: Check rollback and next working tx  */

        me.To(12).Begin().WriteVer({1, 50}).Add(1, foo).Add(1, nil).Commit();
        me.To(13).Iter(1).Has(foo).HasN(nullptr, 77_u64).NoKey(bar);
        me.To(14).Begin().WriteVer({1, 51}).Add(1, bar).Reject();
        me.To(15).Iter(1).Has(foo).NoKey(bar);
        me.To(16).Begin().WriteVer({1, 52}).Add(1, bar).Commit();
        me.To(17).Iter(1).Has(foo).Has(bar);
        me.To(18).Affects(0, { 1 });

        /* Check versioned queries */
        me.To(19).ReadVer({0, 0}).Iter(1).NoKey(foo).NoKey(bar);
        me.To(20).ReadVer({1, 50}).Iter(1).Has(foo).NoKey(bar);
        me.To(21).ReadVer({1, 51}).Iter(1).Has(foo).NoKey(bar);
        me.To(22).ReadVer({1, 52}).Iter(1).Has(foo).Has(bar);

        /* Not sure what this does, copied from Basics test */

        me.To(23).Begin().Apply(*TAlter().SetRoom(1, 0, 1, 2, 1)).Commit();

        /*_ 20: Check that log is applied correctly */

        me.To(24).Replay(EPlay::Boot).Iter(1).Has(foo).Has(bar);
        me.To(25).Replay(EPlay::Redo).Iter(1).Has(foo).Has(bar);

        /* History should have been restored as well */

        me.To(26).ReadVer({0, 0}).Iter(1).NoKey(foo).NoKey(bar);
        me.To(27).ReadVer({1, 50}).Iter(1).Has(foo).NoKey(bar);
        me.To(28).ReadVer({1, 51}).Iter(1).Has(foo).NoKey(bar);
        me.To(28).ReadVer({1, 52}).Iter(1).Has(foo).Has(bar);

        /*_ 30: Check erase of some row and update  */

        me.To(30).Begin().WriteVer({2, 60}).Add(1, ba1).Add(1, foo, ERowOp::Erase).Commit();
        me.To(31).Iter(1, false).NoKey(foo).NoVal(bar);
        me.To(32).Iter(1, false).HasN("bar", 11_u64, "yo"); /* omited arg1  */
        me.To(33).Begin().WriteVer({2, 61}).Add(1, ba2).Commit();
        me.To(34).Iter(1, false).HasN("bar", 77_u64, "me"); /* default arg1 */
        me.To(35).Begin().WriteVer({2, 62}).Add(1, ba3).Commit();
        me.To(36).Iter(1, false).HasN("bar", 99_u64, nullptr);
        me.To(37).Begin().WriteVer({2, 63}).Add(1, ba4).Commit();
        me.To(38).Iter(1, false).HasN("bar", nullptr, "eh").Has(ba4);

        /*_ 40: Finally reboot again and check result */

        me.To(40).Replay(EPlay::Boot).Iter(1, false).Has(ba4).NoKey(foo);
        me.To(41).Replay(EPlay::Redo).Iter(1, false).Has(ba4).NoKey(foo);

        /*_ 50: Erase and update the same row in tx */

        me.To(50).Begin().WriteVer({3, 70}).Add(1, bar, ERowOp::Erase).Add(1, bar).Commit();
        me.To(51).Iter(1, false).Has(bar).NoKey(foo);

        /*_ 60: Check rows counters after compaction */

        me.To(60).Snap(1).Compact(1, false).Iter(1, false).Has(bar);

        UNIT_ASSERT(me->Counters().Parts.RowsTotal == 3);
        UNIT_ASSERT(me->Counters().Parts.RowsErase == 1);
        UNIT_ASSERT(me.GetLog().size() == 10);

        /* Check history after compaction is correct */

        for (int base = 70; base <= 90; base += 10) {
            me.To(base + 0).ReadVer({0, 0}).Iter(1).NoKey(foo).NoKey(bar);
            me.To(base + 1).ReadVer({1, 50}).Iter(1).Has(foo).NoKey(bar);
            me.To(base + 2).ReadVer({1, 51}).Iter(1).Has(foo).NoKey(bar);
            me.To(base + 3).ReadVer({1, 52}).Iter(1).Has(foo).Has(bar);
            me.To(base + 4).ReadVer({2, 60}).Iter(1, false).NoKey(foo).NoVal(bar);
            me.To(base + 5).ReadVer({2, 60}).Iter(1, false).HasN("bar", 11_u64, "yo");
            me.To(base + 6).ReadVer({2, 61}).Iter(1, false).HasN("bar", 77_u64, "me");
            me.To(base + 7).ReadVer({2, 62}).Iter(1, false).HasN("bar", 99_u64, nullptr);
            me.To(base + 8).ReadVer({2, 63}).Iter(1, false).HasN("bar", nullptr, "eh").Has(ba4);

            // Run compaction for the next iteration
            me.To(base + 9).Compact(1);
        }
    }

    void RunVersionChecks(bool compactMemTables, bool compactFinal) {
        TDbExec me;

        const ui32 table = 1;
        me.To(10)
            .Begin()
            .Apply(*TAlter()
                .AddTable("me_1", table)
                .AddColumn(table, "key",    1, ETypes::Uint64, false)
                .AddColumn(table, "arg1",   4, ETypes::Uint64, false, Cimple(10004_u64))
                .AddColumn(table, "arg2",   5, ETypes::Uint64, false, Cimple(10005_u64))
                .AddColumnToKey(table, 1))
            .Commit();

        // Add a "null" key with a 0/40 version, for a better code coverage in
        // the version iteration cases, so compaction would actually try
        // descending below 1/50 and won't bail out.
        me.To(15).Begin().WriteVer({0, 40}).Put(table, *me.SchemedCookRow(table).Col(nullptr)).Commit();

        // Create 1000 rows with 3 consecutive versions each, filling different columns
        me.To(20).Begin();
        for (ui64 i = 1000; i < 2000; ++i) {
            // Version 1/50, keys added, but all columns are empty (default)
            me.WriteVer({1, 50}).Put(table, *me.SchemedCookRow(table).Col(i, ECellOp::Empty, ECellOp::Empty));
            // Version 2/60, keys updated with arg1=i
            me.WriteVer({2, 60}).Put(table, *me.SchemedCookRow(table).Col(i, i, ECellOp::Empty));
            // Version 3/70, keys updated with arg2=3000-i
            me.WriteVer({3, 70}).Put(table, *me.SchemedCookRow(table).Col(i, ECellOp::Empty, 3000-i));
        }
        me.Commit().Snap(table);
        if (compactMemTables) {
            me.Compact(table, false);
        }

        // Create 1000 more rows using different memtables
        me.To(22).Begin();
        for (ui64 i = 2000; i < 3000; ++i) {
            me.WriteVer({1, 50}).Put(table, *me.SchemedCookRow(table).Col(i, ECellOp::Empty, ECellOp::Empty));
        }
        me.Commit().Snap(table);
        if (compactMemTables) {
            me.Compact(table, false);
        }

        me.To(23).Begin();
        for (ui64 i = 2000; i < 3000; ++i) {
            me.WriteVer({2, 60}).Put(table, *me.SchemedCookRow(table).Col(i, i, ECellOp::Empty));
        }
        me.Commit().Snap(table);
        if (compactMemTables) {
            me.Compact(table, false);
        }

        me.To(24).Begin();
        for (ui64 i = 2000; i < 3000; ++i) {
            me.WriteVer({3, 70}).Put(table, *me.SchemedCookRow(table).Col(i, ECellOp::Empty, 3000-i));
        }
        me.Commit().Snap(table);
        if (compactMemTables) {
            me.Compact(table, false);
        }

        // Run optional final compaction
        if (compactFinal) {
            me.To(25).Compact(table);
        }

        // Verify that results are correct for each version and row
        me.To(30);
        for (ui64 i = 1000; i < 3000; ++i) {
            // No key before 1/50
            me.ReadVer({1, 49}).Select(table).NoKey(*me.SchemedCookRow(table).Col(i));
            me.ReadVer({1, 49}).Iter(table).NoKey(*me.SchemedCookRow(table).Col(i));
            // Key with default values until 2/60
            me.ReadVer({2, 59}).Select(table).HasN(i, 10004_u64, 10005_u64);
            me.ReadVer({2, 59}).Iter(table).HasN(i, 10004_u64, 10005_u64);
            // Key with arg1 set until 3/70
            me.ReadVer({3, 69}).Select(table).HasN(i, i, 10005_u64);
            me.ReadVer({3, 69}).Iter(table).HasN(i, i, 10005_u64);
            // Key with both arg1 and arg2 starting with 3/70
            me.ReadVer({3, 70}).Select(table).HasN(i, i, 3000-i);
            me.ReadVer({3, 70}).Iter(table).HasN(i, i, 3000-i);
        }

        // Verify iteration before everything
        me.To(31).ReadVer({0, 0}).Iter(table, false)
            .Seek({ }, ESeek::Lower).Is(EReady::Gone);

        // Verify iteration before 1/50
        me.To(32).ReadVer({1, 49}).Iter(table, false)
            .Seek({ }, ESeek::Lower).Is(*me.SchemedCookRow(table).Col(nullptr, 10004_u64, 10005_u64))
            .Next().Is(EReady::Gone);

        // Verify iteration before 2/60
        {
            auto checker = me.To(33).ReadVer({2, 59}).Iter(table, false)
                .Seek({ }, ESeek::Lower).Is(*me.SchemedCookRow(table).Col(nullptr, 10004_u64, 10005_u64));
            for (ui64 i = 1000; i < 3000; ++i) {
                checker.Next().Is(*me.SchemedCookRow(table).Col(i, 10004_u64, 10005_u64));
            }
            checker.Next().Is(EReady::Gone);
        }

        // Verify iteration before 3/70
        {
            auto checker = me.To(34).ReadVer({3, 69}).Iter(table, false)
                .Seek({ }, ESeek::Lower).Is(*me.SchemedCookRow(table).Col(nullptr, 10004_u64, 10005_u64));
            for (ui64 i = 1000; i < 3000; ++i) {
                checker.Next().Is(*me.SchemedCookRow(table).Col(i, i, 10005_u64));
            }
            checker.Next().Is(EReady::Gone);
        }

        // Verify iteration at 3/70
        {
            auto checker = me.To(35).ReadVer({3, 70}).Iter(table, false)
                .Seek({ }, ESeek::Lower).Is(*me.SchemedCookRow(table).Col(nullptr, 10004_u64, 10005_u64));
            for (ui64 i = 1000; i < 3000; ++i) {
                checker.Next().Is(*me.SchemedCookRow(table).Col(i, i, 3000-i));
            }
            checker.Next().Is(EReady::Gone);
        }

        // Verify iteration at HEAD
        {
            auto checker = me.To(36).ReadVer(TRowVersion::Max()).Iter(table, false)
                .Seek({ }, ESeek::Lower).Is(*me.SchemedCookRow(table).Col(nullptr, 10004_u64, 10005_u64));
            for (ui64 i = 1000; i < 3000; ++i) {
                checker.Next().Is(*me.SchemedCookRow(table).Col(i, i, 3000-i));
            }
            checker.Next().Is(EReady::Gone);
        }
    }

    Y_UNIT_TEST(VersionPureMem) {
        RunVersionChecks(false, false);
    }

    Y_UNIT_TEST(VersionPureParts) {
        RunVersionChecks(true, false);
    }

    Y_UNIT_TEST(VersionCompactedMem) {
        RunVersionChecks(false, true);
    }

    Y_UNIT_TEST(VersionCompactedParts) {
        RunVersionChecks(true, true);
    }

    // Regression test for KIKIMR-15506
    Y_UNIT_TEST(KIKIMR_15506_MissingSnapshotKeys) {
        TDbExec me;

        const ui32 table = 1;
        me.To(10)
            .Begin()
            .Apply(*TAlter()
                .AddTable("me_1", table)
                .AddColumn(table, "key", 1, ETypes::Uint64, false)
                .AddColumn(table, "val", 2, ETypes::Uint64, false, Cimple(0_u64))
                .AddColumnToKey(table, 1)
                .SetEraseCache(table, true, 2, 8192))
            .Commit();

        auto dumpCache = [&]() -> TString {
            if (auto* cache = me->DebugGetTableErasedKeysCache(table)) {
                TStringStream stream;
                stream << cache->DumpRanges();
                return stream.Str();
            } else {
                return nullptr;
            }
        };

        // Write a bunch of rows at v1/50
        me.To(20).Begin();
        for (ui64 i = 1; i <= 18; ++i) {
            if (i != 9) {
                me.WriteVer({1, 50}).Put(table, *me.SchemedCookRow(table).Col(i, i));
            }
        }
        me.Commit();

        // Erase a bunch of rows at v2/50
        me.To(21).Begin();
        for (ui64 i = 1; i <= 16; ++i) {
            if (i != 9) {
                me.WriteVer({2, 50}).Add(table, *me.SchemedCookRow(table).Col(i), ERowOp::Erase);
            }
        }
        me.Commit();

        // Verify we can only see 2 last rows at v3/50 (all other are deleted)
        me.To(22).ReadVer({3, 50}).IterData(table)
            .Seek({ }, ESeek::Lower).Is(*me.SchemedCookRow(table).Col(17_u64, 17_u64))
            .Next().Is(*me.SchemedCookRow(table).Col(18_u64, 18_u64))
            .Next().Is(EReady::Gone);

        UNIT_ASSERT_VALUES_EQUAL(dumpCache(), "TKeyRangeCache{ [{1}, {16}] }");

        // Add a new row at v4/50 (it's expected to invalidate the cached range)
        me.To(23).Begin();
        me.WriteVer({4, 50}).Put(table, *me.SchemedCookRow(table).Col(9_u64, 9_u64));
        me.Commit();

        UNIT_ASSERT_VALUES_EQUAL(dumpCache(), "TKeyRangeCache{ [{1}, {9}) }");

        // Verify we can only see 2 last rows at v3/50 (erased range shouldn't be cached incorrectly)
        me.To(24).ReadVer({3, 50}).IterData(table)
            .Seek({ }, ESeek::Lower).Is(*me.SchemedCookRow(table).Col(17_u64, 17_u64))
            .Next().Is(*me.SchemedCookRow(table).Col(18_u64, 18_u64))
            .Next().Is(EReady::Gone);

        UNIT_ASSERT_VALUES_EQUAL(dumpCache(), "TKeyRangeCache{ [{1}, {9}), [{10}, {16}] }");

        // Verify we can see all 3 rows at v5/50 (bug would cause as to skip over the key 9)
        me.To(25).ReadVer({5, 50}).IterData(table)
            .Seek({ }, ESeek::Lower).Is(*me.SchemedCookRow(table).Col(9_u64, 9_u64))
            .Next().Is(*me.SchemedCookRow(table).Col(17_u64, 17_u64))
            .Next().Is(*me.SchemedCookRow(table).Col(18_u64, 18_u64))
            .Next().Is(EReady::Gone);
    }

    void TestEraseCacheWithUncommittedChanges(bool compact) {
        TDbExec me;

        const ui32 table = 1;
        me.To(10)
            .Begin()
            .Apply(*TAlter()
                .AddTable("me_1", table)
                .AddColumn(table, "key", 1, ETypes::Uint64, false)
                .AddColumn(table, "val", 2, ETypes::Uint64, false, Cimple(0_u64))
                .AddColumnToKey(table, 1)
                .SetEraseCache(table, true, 2, 8192))
            .Commit();

        auto dumpCache = [&]() -> TString {
            if (auto* cache = me->DebugGetTableErasedKeysCache(table)) {
                TStringStream stream;
                stream << cache->DumpRanges();
                return stream.Str();
            } else {
                return nullptr;
            }
        };

        // Write a bunch of rows at v1/50
        me.To(20).Begin();
        for (ui64 i = 1; i <= 18; ++i) {
            if (i != 9) {
                me.WriteVer({1, 50}).Put(table, *me.SchemedCookRow(table).Col(i, i));
            }
        }
        me.Commit();
        if (compact) {
            me.Compact(table, false);
        }

        // Erase a bunch of rows at v2/50
        me.To(21).Begin();
        for (ui64 i = 1; i <= 16; ++i) {
            if (i != 9) {
                me.WriteVer({2, 50}).Add(table, *me.SchemedCookRow(table).Col(i), ERowOp::Erase);
            }
        }
        me.Commit();
        if (compact) {
            me.Compact(table, false);
        }

        // Verify we can only see 2 last rows at v3/50 (all other are deleted)
        me.To(22).ReadVer({3, 50}).IterData(table)
            .Seek({ }, ESeek::Lower).Is(*me.SchemedCookRow(table).Col(17_u64, 17_u64))
            .Next().Is(*me.SchemedCookRow(table).Col(18_u64, 18_u64))
            .Next().Is(EReady::Gone);

        UNIT_ASSERT_VALUES_EQUAL(dumpCache(), "TKeyRangeCache{ [{1}, {16}] }");

        // Write an uncommitted row in tx 123
        me.To(23).Begin();
        me.WriteTx(123).Put(table, *me.SchemedCookRow(table).Col(9_u64, 9_u64));
        me.Commit();
        if (compact) {
            me.Compact(table, false);
        }

        UNIT_ASSERT_VALUES_EQUAL(dumpCache(), "TKeyRangeCache{ [{1}, {9}) }");

        // Verify we can only see all 3 rows in tx 123 and erase cache is correct
        me.To(24).ReadTx(123).IterData(table)
            .Seek({ }, ESeek::Lower).Is(*me.SchemedCookRow(table).Col(9_u64, 9_u64))
            .Next().Is(*me.SchemedCookRow(table).Col(17_u64, 17_u64))
            .Next().Is(*me.SchemedCookRow(table).Col(18_u64, 18_u64))
            .Next().Is(EReady::Gone);

        UNIT_ASSERT_VALUES_EQUAL(dumpCache(), "TKeyRangeCache{ [{1}, {9}), [{10}, {16}] }");

        // Rollback tx 123
        me.To(25).Begin();
        me.RemoveTx(table, 123);
        me.Commit();
        if (compact) {
            me.Compact(table, false);
        }

        // Verify we can only see 2 last rows at v3/50 (all other are deleted)
        me.To(26).ReadVer({3, 50}).IterData(table)
            .Seek({ }, ESeek::Lower).Is(*me.SchemedCookRow(table).Col(17_u64, 17_u64))
            .Next().Is(*me.SchemedCookRow(table).Col(18_u64, 18_u64))
            .Next().Is(EReady::Gone);

        UNIT_ASSERT_VALUES_EQUAL(dumpCache(), "TKeyRangeCache{ [{1}, {16}] }");
    }

    Y_UNIT_TEST(EraseCacheWithUncommittedChanges) {
        TestEraseCacheWithUncommittedChanges(false);
    }

    Y_UNIT_TEST(EraseCacheWithUncommittedChangesCompacted) {
        TestEraseCacheWithUncommittedChanges(true);
    }

    Y_UNIT_TEST(AlterAndUpsertChangesVisibility) {
        TDbExec me;

        const ui32 table1 = 1;
        const ui32 table2 = 2;
        me.To(10).Begin();

        me.To(20).Apply(*TAlter()
                .AddTable("me_1", table1)
                .AddColumn(table1, "key",    1, ETypes::Uint64, false)
                .AddColumn(table1, "arg1",   4, ETypes::Uint64, false, Cimple(10004_u64))
                .AddColumn(table1, "arg2",   5, ETypes::Uint64, false, Cimple(10005_u64))
                .AddColumnToKey(table1, 1));
        me.To(21).PutN(table1, 1_u64, 11_u64, 12_u64);
        me.To(22).Select(table1).HasN(1_u64, 11_u64, 12_u64);
        me.To(23).Select(table1).NoKeyN(2_u64);

        me.To(30).Apply(*TAlter()
                .AddTable("me_2", table2)
                .AddColumn(table2, "key",    1, ETypes::Uint64, false)
                .AddColumn(table2, "arg1",   4, ETypes::Uint64, false, Cimple(20004_u64))
                .AddColumn(table2, "arg2",   5, ETypes::Uint64, false, Cimple(20005_u64))
                .AddColumnToKey(table2, 1));
        me.To(31).PutN(table2, 2_u64, 21_u64, 22_u64);
        me.To(32).Select(table2).NoKeyN(1_u64);
        me.To(33).Select(table2).HasN(2_u64, 21_u64, 22_u64);

        me.Commit();

        me.To(40).Begin();
        me.To(41).Apply(*TAlter()
                .DropColumn(table2, 5)
                .AddColumn(table2, "arg3", 6, ETypes::Uint64, false, Cimple(20006_u64)));
        me.To(42).Select(table2).HasN(2_u64, 21_u64, 20006_u64);
        me.To(43).PutN(table2, 2_u64, ECellOp::Empty, 23_u64);
        me.To(44).Select(table2).HasN(2_u64, 21_u64, 23_u64);
        me.Reject();

        me.To(50).Begin();
        me.To(51).Select(table2).HasN(2_u64, 21_u64, 22_u64);
        me.To(52).PutN(table2, 2_u64, 24_u64, ECellOp::Empty);
        me.To(53).Select(table2).HasN(2_u64, 24_u64, 22_u64);
        me.Commit();

        me.To(60).Replay(EPlay::Boot);
        me.To(61).Select(table1).HasN(1_u64, 11_u64, 12_u64);
        me.To(62).Select(table2).HasN(2_u64, 24_u64, 22_u64);
        me.To(63).Replay(EPlay::Redo);
        me.To(64).Select(table1).HasN(1_u64, 11_u64, 12_u64);
        me.To(65).Select(table2).HasN(2_u64, 24_u64, 22_u64);
    }

    Y_UNIT_TEST(UncommittedChangesVisibility) {
        TDbExec me;

        const ui32 table1 = 1;

        me.To(10).Begin();
        me.To(11).Apply(*TAlter()
                .AddTable("me_1", table1)
                .AddColumn(table1, "key",    1, ETypes::Uint64, false)
                .AddColumn(table1, "arg1",   4, ETypes::Uint64, false, Cimple(10004_u64))
                .AddColumn(table1, "arg2",   5, ETypes::Uint64, false, Cimple(10005_u64))
                .AddColumnToKey(table1, 1));
        me.To(12).PutN(table1, 1_u64, 11_u64, 12_u64);
        me.To(13).WriteTx(123).PutN(table1, 1_u64, ECellOp::Empty, 13_u64);
        UNIT_ASSERT(me->HasOpenTx(table1, 123));
        me.To(14).Select(table1).HasN(1_u64, 11_u64, 12_u64);
        me.To(15).ReadTx(123).Select(table1).HasN(1_u64, 11_u64, 13_u64);
        me.To(16).Commit();

        me.To(20).Begin();
        me.To(21).Select(table1).HasN(1_u64, 11_u64, 12_u64);
        me.To(22).CommitTx(table1, 123);
        UNIT_ASSERT(!me->HasOpenTx(table1, 123));
        UNIT_ASSERT(me->HasCommittedTx(table1, 123));
        me.To(23).Select(table1).HasN(1_u64, 11_u64, 13_u64);
        me.To(24).Reject();

        UNIT_ASSERT(me->HasOpenTx(table1, 123));
        UNIT_ASSERT(!me->HasCommittedTx(table1, 123));

        me.To(30).Begin();
        me.To(31).Select(table1).HasN(1_u64, 11_u64, 12_u64);
        me.To(32).RemoveTx(table1, 123);
        UNIT_ASSERT(!me->HasOpenTx(table1, 123));
        UNIT_ASSERT(me->HasRemovedTx(table1, 123));
        me.To(33).Reject();

        UNIT_ASSERT(me->HasOpenTx(table1, 123));
        UNIT_ASSERT(!me->HasRemovedTx(table1, 123));

        me.To(40).Begin();
        me.To(41).CommitTx(table1, 123);
        me.To(42).Commit();

        UNIT_ASSERT(!me->HasOpenTx(table1, 123));
        UNIT_ASSERT(me->HasCommittedTx(table1, 123));

        me.To(50).Select(table1).HasN(1_u64, 11_u64, 13_u64);
        me.To(51).Snap(table1).Compact(table1);

        UNIT_ASSERT(!me->HasOpenTx(table1, 123));
        UNIT_ASSERT(!me->HasCommittedTx(table1, 123));

        me.To(52).Select(table1).HasN(1_u64, 11_u64, 13_u64);
    }

    Y_UNIT_TEST(UncommittedChangesCommitWithUpdates) {
        TDbExec me;

        const ui32 table1 = 1;

        me.To(10).Begin();
        me.To(11).Apply(*TAlter()
                .AddTable("me_1", table1)
                .AddColumn(table1, "key",    1, ETypes::Uint64, false)
                .AddColumn(table1, "arg1",   4, ETypes::Uint64, false, Cimple(10004_u64))
                .AddColumn(table1, "arg2",   5, ETypes::Uint64, false, Cimple(10005_u64))
                .AddColumnToKey(table1, 1));
        me.To(12).PutN(table1, 1_u64, 11_u64, 12_u64);
        me.To(13).Commit();

        me.To(20).Begin();
        me.To(21).WriteTx(123).PutN(table1, 1_u64, ECellOp::Empty, 22_u64);
        me.To(22).Commit();

        me.To(30).Begin();
        me.To(31).WriteVer({ 1, 51 });
        me.To(32).CommitTx(table1, 123);
        me.To(33).PutN(table1, 1_u64, 21_u64, ECellOp::Empty);
        me.To(34).Commit();

        me.To(41).ReadVer({ 1, 50 }).Select(table1).HasN(1_u64, 11_u64, 12_u64);
        me.To(42).ReadVer({ 1, 51 }).Select(table1).HasN(1_u64, 21_u64, 22_u64);
    }

    Y_UNIT_TEST(ReplayNewTable) {
        TDbExec me;

        const ui32 table1 = 1;

        me.To(10).Begin();
        me.To(11).Apply(*TAlter()
                .AddTable("me_1", table1)
                .AddColumn(table1, "key",    1, ETypes::Uint64, false)
                .AddColumn(table1, "arg1",   4, ETypes::Uint64, false, Cimple(10004_u64))
                .AddColumn(table1, "arg2",   5, ETypes::Uint64, false, Cimple(10005_u64))
                .AddColumnToKey(table1, 1));
        me.To(13).Commit();
        me.To(14).Affects(0, { });

        me.To(21).Replay(EPlay::Boot);
        me.To(22).Replay(EPlay::Redo);
    }

    Y_UNIT_TEST(SnapshotNewTable) {
        TDbExec me;

        const ui32 table1 = 1;

        me.To(10).Begin();
        me.To(11).Apply(*TAlter()
                .AddTable("me_1", table1)
                .AddColumn(table1, "key",    1, ETypes::Uint64, false)
                .AddColumn(table1, "arg1",   4, ETypes::Uint64, false, Cimple(10004_u64))
                .AddColumn(table1, "arg2",   5, ETypes::Uint64, false, Cimple(10005_u64))
                .AddColumnToKey(table1, 1));
        me.To(12).Snapshot(table1);
        me.To(13).Commit();
        me.To(14).Affects(0, { });

        me.To(21).Replay(EPlay::Boot);
        me.To(22).Replay(EPlay::Redo);
    }

    Y_UNIT_TEST(DropModifiedTable) {
        TDbExec me;

        const ui32 table1 = 1;

        me.To(10).Begin();
        me.To(11).Apply(*TAlter()
                .AddTable("me_1", table1)
                .AddColumn(table1, "key",    1, ETypes::Uint64, false)
                .AddColumn(table1, "arg1",   4, ETypes::Uint64, false, Cimple(10004_u64))
                .AddColumn(table1, "arg2",   5, ETypes::Uint64, false, Cimple(10005_u64))
                .AddColumnToKey(table1, 1));
        me.To(12).Commit();

        me.To(20).Begin();
        me.To(21).PutN(table1, 1_u64, 11_u64, 12_u64);
        me.To(22).Commit();

        me.To(30).Begin();
        me.To(31).Select(table1).HasN(1_u64, 11_u64, 12_u64);
        me.To(32).Apply(*TAlter()
                .AddColumn(table1, "arg3",  6, ETypes::Uint64, false, Cimple(10006_u64)));
        me.To(33).Select(table1).HasN(1_u64, 11_u64, 12_u64, 10006_u64);
        me.To(34).Apply(*TAlter()
                .DropTable(table1));
        me.To(35).Reject();

        me.To(40).Begin();
        me.To(41).Select(table1).HasN(1_u64, 11_u64, 12_u64);
        me.To(42).Apply(*TAlter()
                .AddColumn(table1, "arg3",  6, ETypes::Uint64, false, Cimple(10006_u64)));
        me.To(43).Select(table1).HasN(1_u64, 11_u64, 12_u64, 10006_u64);
        me.To(44).Apply(*TAlter()
                .DropTable(table1));
        me.To(45).Commit();

        me.To(51).Replay(EPlay::Boot);
        me.To(52).Replay(EPlay::Redo);
    }

    Y_UNIT_TEST(KIKIMR_15598_Many_MemTables) {
        TDbExec me;

        const ui32 table = 1;
        me.To(10)
            .Begin()
            .Apply(*TAlter()
                .AddTable("me_1", table)
                .AddColumn(table, "key", 1, ETypes::Uint64, false)
                .AddColumn(table, "val", 2, ETypes::Uint64, false, Cimple(0_u64))
                .AddColumnToKey(table, 1))
            .Commit();

        ui64 count = 65537;

        // Add 65537 rows, each in its own memtable
        for (ui64 i = 1; i <= count; ++i) {
            me.To(100000 + i)
                .Begin()
                .Put(table, *me.SchemedCookRow(table).Col(i, i))
                .Commit();
            // Simulate an unsuccessful compaction attempt
            me.Snap(table);
        }

        // Check all rows exist on iteration
        auto check = me.To(200000).IterData(table);
        check.Seek({ }, ESeek::Lower);
        for (ui64 i = 1; i <= count; ++i) {
            check.To(200000 + i).Is(*me.SchemedCookRow(table).Col(i, i));
            check.Next();
        }
        check.To(300000).Is(EReady::Gone);
    }

}

}
}

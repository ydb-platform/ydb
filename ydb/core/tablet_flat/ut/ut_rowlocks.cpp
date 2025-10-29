#include <ydb/core/tablet_flat/flat_dbase_scheme.h>
#include <ydb/core/tablet_flat/test/libs/table/test_dbase.h>
#include <ydb/core/tablet_flat/test/libs/rows/cook.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/stream/file.h>

namespace NKikimr::NTable {

Y_UNIT_TEST_SUITE(DBRowLocks) {

    using namespace NTest;

    Y_UNIT_TEST(LockSurvivesCompactions) {
        TDbExec me;

        const ui32 table1 = 1;

        me.ToLine().Begin();
        me.ToLine().Apply(*TAlter()
                .AddTable("me_1", table1)
                .AddColumn(table1, "key",    1, ETypes::Uint64, false)
                .AddColumn(table1, "arg1",   4, ETypes::Uint64, false, Cimple(10004_u64))
                .AddColumn(table1, "arg2",   5, ETypes::Uint64, false, Cimple(10005_u64))
                .AddColumnToKey(table1, 1));
        me.ToLine().WriteVer({1, 11}).PutN(table1, 1_u64, 11_u64, 12_u64);
        me.ToLine().Commit();

        me.ToLine().Snap(table1).Compact(table1, false);

        me.ToLine().Begin();
        me.ToLine().WriteTx(123).LockRowN(table1, ELockMode::Exclusive, 1_u64);
        me.ToLine().Commit();

        me.ToLine().Begin();
        {
            auto r1 = me.ToLine().SelectRowVersionN(table1, 1_u64);
            UNIT_ASSERT_VALUES_EQUAL(r1.Ready, EReady::Data);
            UNIT_ASSERT_VALUES_EQUAL(r1.RowVersion, TRowVersion(1, 11));
            UNIT_ASSERT_VALUES_EQUAL(r1.LockMode, ELockMode::Exclusive);
            UNIT_ASSERT_VALUES_EQUAL(r1.LockTxId, 123u);
        }
        me.ToLine().Commit();

        me.ToLine().Snap(table1).Compact(table1, false);

        me.ToLine().Begin();
        {
            auto r2 = me.ToLine().SelectRowVersionN(table1, 1_u64);
            UNIT_ASSERT_VALUES_EQUAL(r2.Ready, EReady::Data);
            UNIT_ASSERT_VALUES_EQUAL(r2.RowVersion, TRowVersion(1, 11));
            UNIT_ASSERT_VALUES_EQUAL(r2.LockMode, ELockMode::Exclusive);
            UNIT_ASSERT_VALUES_EQUAL(r2.LockTxId, 123u);
        }
        me.ToLine().Commit();

        me.ToLine().Compact(table1);

        me.ToLine().Begin();
        {
            auto r3 = me.ToLine().SelectRowVersionN(table1, 1_u64);
            UNIT_ASSERT_VALUES_EQUAL(r3.Ready, EReady::Data);
            UNIT_ASSERT_VALUES_EQUAL(r3.RowVersion, TRowVersion(1, 11));
            UNIT_ASSERT_VALUES_EQUAL(r3.LockMode, ELockMode::Exclusive);
            UNIT_ASSERT_VALUES_EQUAL(r3.LockTxId, 123u);
        }
        me.ToLine().Commit();
    }

    Y_UNIT_TEST(LockOverCompactedErase) {
        TDbExec me;

        const ui32 table1 = 1;

        me.ToLine().Begin();
        me.ToLine().Apply(*TAlter()
                .AddTable("me_1", table1)
                .AddColumn(table1, "key",    1, ETypes::Uint64, false)
                .AddColumn(table1, "arg1",   4, ETypes::Uint64, false, Cimple(10004_u64))
                .AddColumn(table1, "arg2",   5, ETypes::Uint64, false, Cimple(10005_u64))
                .AddColumnToKey(table1, 1));
        me.ToLine().WriteVer({1, 11}).PutN(table1, 1_u64, 11_u64, 12_u64);
        me.ToLine().Commit();

        me.ToLine().Snap(table1).Compact(table1, false);

        me.ToLine().Begin();
        me.ToLine().WriteVer({1, 11}).EraseN(table1, 1_u64);
        me.ToLine().Commit();

        me.ToLine().Begin();
        me.ToLine().WriteTx(123).LockRowN(table1, ELockMode::Exclusive, 1_u64);
        me.ToLine().Commit();

        me.ToLine().Snap(table1).Compact(table1, false);

        me.ToLine().Begin();
        {
            auto r1 = me.ToLine().SelectRowVersionN(table1, 1_u64);
            UNIT_ASSERT_VALUES_EQUAL(r1.Ready, EReady::Data);
            UNIT_ASSERT_VALUES_EQUAL(r1.LockMode, ELockMode::Exclusive);
            UNIT_ASSERT_VALUES_EQUAL(r1.LockTxId, 123u);
        }
        me.ToLine().Commit();

        me.ToLine().Compact(table1, true);

        me.ToLine().Begin();
        {
            auto r2 = me.ToLine().SelectRowVersionN(table1, 1_u64);
            UNIT_ASSERT_VALUES_EQUAL(r2.Ready, EReady::Gone);
            UNIT_ASSERT_VALUES_EQUAL(r2.LockMode, ELockMode::Exclusive);
            UNIT_ASSERT_VALUES_EQUAL(r2.LockTxId, 123u);
        }
        me.ToLine().Commit();
    }

    Y_UNIT_TEST(CommitTxAfterLockThenCompact) {
        TDbExec me;

        const ui32 table1 = 1;

        me.ToLine().Begin();
        me.ToLine().Apply(*TAlter()
                .AddTable("me_1", table1)
                .AddColumn(table1, "key",    1, ETypes::Uint64, false)
                .AddColumn(table1, "arg1",   4, ETypes::Uint64, false, Cimple(10004_u64))
                .AddColumn(table1, "arg2",   5, ETypes::Uint64, false, Cimple(10005_u64))
                .AddColumnToKey(table1, 1));
        me.ToLine().WriteVer({1, 11}).PutN(table1, 1_u64, 11_u64, 12_u64);
        me.ToLine().Commit();

        me.ToLine().Snap(table1).Compact(table1, false);

        me.ToLine().Begin();
        me.ToLine().WriteTx(123).LockRowN(table1, ELockMode::Exclusive, 1_u64);
        me.ToLine().Commit();

        me.ToLine().Snap(table1).Compact(table1, false);

        me.ToLine().Begin();
        me.ToLine().WriteTx(234).PutN(table1, 1_u64, 22_u64);
        me.ToLine().Commit();

        me.ToLine().Snap(table1).Compact(table1, false);

        me.ToLine().Begin();
        {
            auto r = me.ToLine().SelectRowVersionN(table1, 1_u64);
            UNIT_ASSERT_VALUES_EQUAL(r.Ready, EReady::Data);
            UNIT_ASSERT_VALUES_EQUAL(r.RowVersion, TRowVersion(1, 11));
            UNIT_ASSERT_VALUES_EQUAL(r.LockMode, ELockMode::Exclusive);
            UNIT_ASSERT_VALUES_EQUAL(r.LockTxId, 123u);
        }
        me.ToLine().WriteVer({2, 22}).CommitTx(table1, 234u);
        {
            auto r = me.ToLine().SelectRowVersionN(table1, 1_u64);
            UNIT_ASSERT_VALUES_EQUAL(r.Ready, EReady::Data);
            UNIT_ASSERT_VALUES_EQUAL(r.RowVersion, TRowVersion(2, 22));
            UNIT_ASSERT_VALUES_EQUAL(r.LockMode, ELockMode::None);
        }
        me.ToLine().Commit();

        me.ToLine().Snap(table1).Compact(table1);

        me.ToLine().Begin();
        {
            auto r = me.ToLine().SelectRowVersionN(table1, 1_u64);
            UNIT_ASSERT_VALUES_EQUAL(r.Ready, EReady::Data);
            UNIT_ASSERT_VALUES_EQUAL(r.RowVersion, TRowVersion(2, 22));
            UNIT_ASSERT_VALUES_EQUAL(r.LockMode, ELockMode::None);
        }
        me.ToLine().Commit();
    }

    Y_UNIT_TEST(CommitLockThenCompactRowVersion) {
        TDbExec me;

        const ui32 table1 = 1;

        me.ToLine().Begin();
        me.ToLine().Apply(*TAlter()
                .AddTable("me_1", table1)
                .AddColumn(table1, "key",    1, ETypes::Uint64, false)
                .AddColumn(table1, "arg1",   4, ETypes::Uint64, false, Cimple(10004_u64))
                .AddColumn(table1, "arg2",   5, ETypes::Uint64, false, Cimple(10005_u64))
                .AddColumnToKey(table1, 1));
        me.ToLine().WriteVer({1, 11}).PutN(table1, 1_u64, 11_u64, 12_u64);
        me.ToLine().Commit();

        me.ToLine().Begin();
        me.ToLine().WriteTx(123).LockRowN(table1, ELockMode::Exclusive, 1_u64);
        me.ToLine().Commit();

        me.ToLine().Begin();
        {
            auto r1 = me.ToLine().SelectRowVersionN(table1, 1_u64);
            UNIT_ASSERT_VALUES_EQUAL(r1.Ready, EReady::Data);
            UNIT_ASSERT_VALUES_EQUAL(r1.RowVersion, TRowVersion(1, 11));
            UNIT_ASSERT_VALUES_EQUAL(r1.LockMode, ELockMode::Exclusive);
            UNIT_ASSERT_VALUES_EQUAL(r1.LockTxId, 123u);
        }
        me.ToLine().WriteVer({2, 22}).CommitTx(table1, 123);
        {
            auto r2 = me.ToLine().SelectRowVersionN(table1, 1_u64);
            UNIT_ASSERT_VALUES_EQUAL(r2.Ready, EReady::Data);
            UNIT_ASSERT_VALUES_EQUAL(r2.RowVersion, TRowVersion(1, 11));
            UNIT_ASSERT_VALUES_EQUAL(r2.LockMode, ELockMode::None);
        }
        me.ToLine().Commit();

        me.ToLine().Snap(table1).Compact(table1);

        me.ToLine().Begin();
        {
            auto r3 = me.ToLine().SelectRowVersionN(table1, 1_u64);
            UNIT_ASSERT_VALUES_EQUAL(r3.Ready, EReady::Data);
            UNIT_ASSERT_VALUES_EQUAL(r3.RowVersion, TRowVersion(1, 11));
            UNIT_ASSERT_VALUES_EQUAL(r3.LockMode, ELockMode::None);
        }
        me.ToLine().Commit();
    }

    Y_UNIT_TEST(OverwriteLockThenCompact) {
        TDbExec me;

        const ui32 table1 = 1;

        me.ToLine().Begin();
        me.ToLine().Apply(*TAlter()
                .AddTable("me_1", table1)
                .AddColumn(table1, "key",    1, ETypes::Uint64, false)
                .AddColumn(table1, "arg1",   4, ETypes::Uint64, false, Cimple(10004_u64))
                .AddColumn(table1, "arg2",   5, ETypes::Uint64, false, Cimple(10005_u64))
                .AddColumnToKey(table1, 1));
        me.ToLine().WriteVer({1, 11}).PutN(table1, 1_u64, 11_u64, 12_u64);
        me.ToLine().Commit();

        me.ToLine().Begin();
        me.ToLine().WriteTx(123).LockRowN(table1, ELockMode::Exclusive, 1_u64);
        {
            auto r = me.ToLine().SelectRowVersionN(table1, 1_u64);
            UNIT_ASSERT_VALUES_EQUAL(r.Ready, EReady::Data);
            UNIT_ASSERT_VALUES_EQUAL(r.RowVersion, TRowVersion(1, 11));
            UNIT_ASSERT_VALUES_EQUAL(r.LockMode, ELockMode::Exclusive);
            UNIT_ASSERT_VALUES_EQUAL(r.LockTxId, 123u);
        }
        me.ToLine().Commit();

        me.ToLine().Snap(table1).Compact(table1, false);

        me.ToLine().Begin();
        me.ToLine().WriteTx(234).LockRowN(table1, ELockMode::Multi, 1_u64);
        {
            auto r = me.ToLine().SelectRowVersionN(table1, 1_u64);
            UNIT_ASSERT_VALUES_EQUAL(r.Ready, EReady::Data);
            UNIT_ASSERT_VALUES_EQUAL(r.RowVersion, TRowVersion(1, 11));
            UNIT_ASSERT_VALUES_EQUAL(r.LockMode, ELockMode::Multi);
            UNIT_ASSERT_VALUES_EQUAL(r.LockTxId, 234u);
        }
        me.ToLine().Commit();

        me.ToLine().Snap(table1).Compact(table1, false);

        me.ToLine().Begin();
        {
            auto r = me.ToLine().SelectRowVersionN(table1, 1_u64);
            UNIT_ASSERT_VALUES_EQUAL(r.Ready, EReady::Data);
            UNIT_ASSERT_VALUES_EQUAL(r.RowVersion, TRowVersion(1, 11));
            UNIT_ASSERT_VALUES_EQUAL(r.LockMode, ELockMode::Multi);
            UNIT_ASSERT_VALUES_EQUAL(r.LockTxId, 234u);
        }
        me.ToLine().Commit();

        me.ToLine().Compact(table1);

        me.ToLine().Begin();
        {
            auto r = me.ToLine().SelectRowVersionN(table1, 1_u64);
            UNIT_ASSERT_VALUES_EQUAL(r.Ready, EReady::Data);
            UNIT_ASSERT_VALUES_EQUAL(r.RowVersion, TRowVersion(1, 11));
            UNIT_ASSERT_VALUES_EQUAL(r.LockMode, ELockMode::Multi);
            UNIT_ASSERT_VALUES_EQUAL(r.LockTxId, 234u);
        }
        me.ToLine().Commit();
    }

    Y_UNIT_TEST(LockOpenTxAndTxDataAccounting) {
        TDbExec me;

        const ui32 table1 = 1;

        me.ToLine().Begin();
        me.ToLine().Apply(*TAlter()
                .AddTable("me_1", table1)
                .AddColumn(table1, "key",    1, ETypes::Uint64, false)
                .AddColumn(table1, "arg1",   4, ETypes::Uint64, false, Cimple(10004_u64))
                .AddColumn(table1, "arg2",   5, ETypes::Uint64, false, Cimple(10005_u64))
                .AddColumnToKey(table1, 1));
        me.ToLine().WriteVer({1, 11}).PutN(table1, 1_u64, 11_u64, 12_u64);
        me.ToLine().Commit();

        me.ToLine().Begin();
        me.ToLine().WriteTx(123).LockRowN(table1, ELockMode::Exclusive, 1_u64);
        me.ToLine().Commit();

        UNIT_ASSERT(me->HasOpenTx(table1, 123u));
        UNIT_ASSERT(me->HasTxData(table1, 123u));

        me.ToLine().Snap(table1).Compact(table1);

        UNIT_ASSERT(me->HasOpenTx(table1, 123u));
        UNIT_ASSERT(me->HasTxData(table1, 123u));
    }

    Y_UNIT_TEST(MultipleCommittedRowLocks) {
        TDbExec me;

        const ui32 table1 = 1;

        me.ToLine().Begin();
        me.ToLine().Apply(*TAlter()
                .AddTable("me_1", table1)
                .AddColumn(table1, "key",    1, ETypes::Uint64, false)
                .AddColumn(table1, "arg1",   4, ETypes::Uint64, false, Cimple(10004_u64))
                .AddColumn(table1, "arg2",   5, ETypes::Uint64, false, Cimple(10005_u64))
                .AddColumnToKey(table1, 1));
        me.ToLine().Commit();

        me.ToLine().Begin();
        me.ToLine().WriteTx(101).LockRowN(table1, ELockMode::Multi, 1_u64);
        me.ToLine().WriteTx(102).LockRowN(table1, ELockMode::Multi, 1_u64);
        me.ToLine().Commit();

        me.ToLine().Snap(table1).Compact(table1, false);

        me.ToLine().Begin();
        me.ToLine().WriteTx(103).LockRowN(table1, ELockMode::Multi, 1_u64);
        me.ToLine().WriteTx(104).LockRowN(table1, ELockMode::Multi, 1_u64);
        me.ToLine().Commit();

        me.ToLine().Snap(table1).Compact(table1, false);

        me.ToLine().Begin();
        me.ToLine().WriteTx(105).LockRowN(table1, ELockMode::Multi, 1_u64);
        me.ToLine().WriteTx(106).LockRowN(table1, ELockMode::Multi, 1_u64);
        me.ToLine().Commit();

        me.ToLine().Snap(table1);

        me.ToLine().Begin();
        me.ToLine().WriteTx(107).LockRowN(table1, ELockMode::Multi, 1_u64);
        me.ToLine().WriteTx(108).LockRowN(table1, ELockMode::Multi, 1_u64);
        me.ToLine().Commit();

        me.ToLine().Snap(table1);

        me.ToLine().Begin();
        {
            auto r = me.ToLine().SelectRowVersionN(table1, 1_u64);
            UNIT_ASSERT_VALUES_EQUAL(r.Ready, EReady::Gone);
            UNIT_ASSERT_VALUES_EQUAL(r.LockMode, ELockMode::Multi);
            UNIT_ASSERT_VALUES_EQUAL(r.LockTxId, 108u);
        }
        me.ToLine().Commit();

        me.ToLine().Begin();
        me.ToLine().WriteVer({1, 101}).CommitTx(table1, 101);
        me.ToLine().WriteVer({1, 102}).CommitTx(table1, 102);
        me.ToLine().WriteVer({1, 103}).CommitTx(table1, 103);
        me.ToLine().WriteVer({1, 104}).CommitTx(table1, 104);
        me.ToLine().WriteVer({1, 105}).CommitTx(table1, 105);
        me.ToLine().WriteVer({1, 106}).CommitTx(table1, 106);
        me.ToLine().WriteVer({1, 107}).CommitTx(table1, 107);
        me.ToLine().WriteVer({1, 108}).CommitTx(table1, 108);
        me.ToLine().Commit();

        me.ToLine().Begin();
        {
            auto r = me.ToLine().SelectRowVersionN(table1, 1_u64);
            UNIT_ASSERT_VALUES_EQUAL(r.Ready, EReady::Gone);
            UNIT_ASSERT_VALUES_EQUAL(r.LockMode, ELockMode::None);
        }
        me.ToLine().Commit();

        me.ToLine().Snap(table1).Compact(table1, true);

        me.ToLine().Begin();
        {
            auto r = me.ToLine().SelectRowVersionN(table1, 1_u64);
            UNIT_ASSERT_VALUES_EQUAL(r.Ready, EReady::Gone);
            UNIT_ASSERT_VALUES_EQUAL(r.LockMode, ELockMode::None);
        }
        me.ToLine().Commit();
    }

    Y_UNIT_TEST(LocksCommittedRemovedIteration) {
        TDbExec me;

        const ui32 table1 = 1;

        me.ToLine().Begin();
        me.ToLine().Apply(*TAlter()
                .AddTable("me_1", table1)
                .AddColumn(table1, "key",    1, ETypes::Uint64, false)
                .AddColumn(table1, "arg1",   4, ETypes::Uint64, false, Cimple(10004_u64))
                .AddColumn(table1, "arg2",   5, ETypes::Uint64, false, Cimple(10005_u64))
                .AddColumnToKey(table1, 1));
        me.ToLine().WriteVer({1, 11}).PutN(table1, 1_u64, 11_u64, 12_u64);
        me.ToLine().WriteVer({1, 12}).PutN(table1, 5_u64, 13_u64, 14_u64);
        me.ToLine().WriteVer({1, 13}).PutN(table1, 9_u64, 15_u64, 16_u64);
        me.ToLine().Commit();

        me.ToLine().Snap(table1).Compact(table1, false);

        me.ToLine().Begin();
        me.ToLine().WriteTx(101).LockRowN(table1, ELockMode::Multi, 1_u64);
        me.ToLine().WriteTx(102).LockRowN(table1, ELockMode::Multi, 6_u64);
        me.ToLine().WriteTx(103).LockRowN(table1, ELockMode::Multi, 9_u64);
        me.ToLine().Commit();

        me.ToLine().Snap(table1).Compact(table1, false);

        me.ToLine().Begin();
        me.ToLine().WriteTx(104).PutN(table1, 5_u64, 17_u64, 18_u64);
        me.ToLine().WriteTx(105).LockRowN(table1, ELockMode::Multi, 9_u64);
        me.ToLine().Commit();

        me.ToLine().Snap(table1).Compact(table1, false);

        me.ToLine().Begin();
        me.ToLine().WriteTx(106).LockRowN(table1, ELockMode::Multi, 5_u64);
        me.ToLine().WriteTx(107).PutN(table1, 5_u64, 19_u64, 20_u64);
        me.ToLine().WriteTx(108).LockRowN(table1, ELockMode::Multi, 5_u64);
        me.ToLine().WriteTx(109).PutN(table1, 5_u64, 21_u64, 22_u64);
        me.ToLine().WriteTx(110).LockRowN(table1, ELockMode::Multi, 9_u64);
        me.ToLine().Commit();

        me.ToLine().Begin();
        me.ToLine().WriteVer({1, 14}).CommitTx(table1, 101);
        me.ToLine().WriteVer({1, 15}).CommitTx(table1, 102);
        me.ToLine().WriteVer({1, 16}).CommitTx(table1, 103);
        me.ToLine().WriteVer({1, 17}).CommitTx(table1, 104);
        me.ToLine().WriteVer({1, 18}).CommitTx(table1, 105);
        me.ToLine().WriteVer({1, 19}).RemoveTx(table1, 106);
        me.ToLine().WriteVer({1, 20}).CommitTx(table1, 107);
        me.ToLine().WriteVer({1, 21}).CommitTx(table1, 108);
        me.ToLine().WriteVer({1, 22}).CommitTx(table1, 109);
        me.ToLine().WriteVer({1, 23}).RemoveTx(table1, 110);
        me.ToLine().Commit();

        me.ToLine().Begin();
        {
            auto it = me.ToLine().ReadVer({1, 16}).Iter(table1, false);
            it.ToLine().Seek({ }, ESeek::Lower)
                .IsOpN(ERowOp::Upsert, 1_u64, 11_u64, 12_u64)
                .IsVer({1, 11});
            it.ToLine().Next()
                .IsOpN(ERowOp::Upsert, 5_u64, 13_u64, 14_u64)
                .IsVer({1, 12});
            it.ToLine().Next()
                .IsOpN(ERowOp::Upsert, 9_u64, 15_u64, 16_u64)
                .IsVer({1, 13});
            it.ToLine().Next().Is(EReady::Gone);
        }
        {
            auto it = me.ToLine().ReadVer({1, 21}).Iter(table1, false);
            it.ToLine().Seek({ }, ESeek::Lower)
                .IsOpN(ERowOp::Upsert, 1_u64, 11_u64, 12_u64)
                .IsVer({1, 11});
            it.ToLine().Next()
                .IsOpN(ERowOp::Upsert, 5_u64, 19_u64, 20_u64)
                .IsVer({1, 20});
            it.ToLine().Next()
                .IsOpN(ERowOp::Upsert, 9_u64, 15_u64, 16_u64)
                .IsVer({1, 13});
            it.ToLine().Next().Is(EReady::Gone);
        }
        {
            auto it = me.ToLine().ReadVer(TRowVersion::Max()).Iter(table1, false);
            it.ToLine().Seek({ }, ESeek::Lower)
                .IsOpN(ERowOp::Upsert, 1_u64, 11_u64, 12_u64)
                .IsVer({1, 11});
            it.ToLine().Next()
                .IsOpN(ERowOp::Upsert, 5_u64, 21_u64, 22_u64)
                .IsVer({1, 22});
            it.ToLine().Next()
                .IsOpN(ERowOp::Upsert, 9_u64, 15_u64, 16_u64)
                .IsVer({1, 13});
            it.ToLine().Next().Is(EReady::Gone);
        }
        me.ToLine().Commit();
    }

    Y_UNIT_TEST(LocksReplay) {
        TDbExec me;

        const ui32 table1 = 1;

        me.ToLine().Begin();
        me.ToLine().Apply(*TAlter()
                .AddTable("me_1", table1)
                .AddColumn(table1, "key",    1, ETypes::Uint64, false)
                .AddColumn(table1, "arg1",   4, ETypes::Uint64, false, Cimple(10004_u64))
                .AddColumn(table1, "arg2",   5, ETypes::Uint64, false, Cimple(10005_u64))
                .AddColumnToKey(table1, 1));
        me.ToLine().WriteVer({1, 11}).PutN(table1, 1_u64, 11_u64, 12_u64);
        me.ToLine().Commit();

        me.ToLine().Begin();
        me.ToLine().WriteTx(101).LockRowN(table1, ELockMode::Multi, 1_u64);
        me.ToLine().Commit();

        me.ToLine().Replay(EPlay::Boot);

        me.ToLine().Begin();
        {
            auto r = me.ToLine().SelectRowVersionN(table1, 1_u64);
            UNIT_ASSERT_VALUES_EQUAL(r.Ready, EReady::Data);
            UNIT_ASSERT_VALUES_EQUAL(r.RowVersion, TRowVersion(1, 11));
            UNIT_ASSERT_VALUES_EQUAL(r.LockMode, ELockMode::Multi);
            UNIT_ASSERT_VALUES_EQUAL(r.LockTxId, 101u);
        }
        me.ToLine().Commit();

        me.ToLine().Replay(EPlay::Redo);

        me.ToLine().Begin();
        {
            auto r = me.ToLine().SelectRowVersionN(table1, 1_u64);
            UNIT_ASSERT_VALUES_EQUAL(r.Ready, EReady::Data);
            UNIT_ASSERT_VALUES_EQUAL(r.RowVersion, TRowVersion(1, 11));
            UNIT_ASSERT_VALUES_EQUAL(r.LockMode, ELockMode::Multi);
            UNIT_ASSERT_VALUES_EQUAL(r.LockTxId, 101u);
        }
        me.ToLine().Commit();
    }

    Y_UNIT_TEST(LocksMvccCompact) {
        TDbExec me;

        const ui32 table1 = 1;

        me.ToLine().Begin();
        me.ToLine().Apply(*TAlter()
                .AddTable("me_1", table1)
                .AddColumn(table1, "key",    1, ETypes::Uint64, false)
                .AddColumn(table1, "arg1",   4, ETypes::Uint64, false, Cimple(10004_u64))
                .AddColumn(table1, "arg2",   5, ETypes::Uint64, false, Cimple(10005_u64))
                .AddColumnToKey(table1, 1));
        me.ToLine().Commit();

        me.ToLine().Begin();
        me.ToLine().WriteVer({1, 11}).PutN(table1, 1_u64, 11_u64, 12_u64);
        me.ToLine().WriteVer({2, 11}).PutN(table1, 1_u64, 21_u64, 22_u64);
        me.ToLine().WriteVer({1, 12}).PutN(table1, 3_u64, 13_u64, 14_u64);
        me.ToLine().WriteVer({2, 12}).PutN(table1, 3_u64, 23_u64, 24_u64);
        me.ToLine().WriteVer({1, 13}).PutN(table1, 5_u64, 15_u64, 16_u64);
        me.ToLine().WriteVer({2, 13}).PutN(table1, 5_u64, 25_u64, 26_u64);
        me.ToLine().WriteTx(101).LockRowN(table1, ELockMode::Multi, 1_u64);
        me.ToLine().WriteTx(102).LockRowN(table1, ELockMode::Multi, 3_u64);
        me.ToLine().WriteTx(103).LockRowN(table1, ELockMode::Multi, 5_u64);
        me.ToLine().Commit();

        me.ToLine().Snap(table1).Compact(table1, false);

        // Note: intentionally overwrite locked keys. This should force locks
        // to become implicitly removed, and should not break full compaction
        // later. In other words scan should not feed locks after the first
        // committed row for a given key.
        me.ToLine().Begin();
        me.ToLine().WriteVer({3, 11}).PutN(table1, 1_u64, 31_u64, 32_u64);
        me.ToLine().WriteVer({3, 12}).PutN(table1, 3_u64, 33_u64, 34_u64);
        me.ToLine().WriteVer({3, 13}).PutN(table1, 5_u64, 35_u64, 36_u64);
        me.ToLine().Commit();

        me.ToLine().Snap(table1).Compact(table1, false);

        UNIT_ASSERT(me->HasTxData(table1, 101u));
        UNIT_ASSERT(me->HasTxData(table1, 102u));
        UNIT_ASSERT(me->HasTxData(table1, 103u));

        me.ToLine().Compact(table1, true);

        UNIT_ASSERT(!me->HasTxData(table1, 101u));
        UNIT_ASSERT(!me->HasTxData(table1, 102u));
        UNIT_ASSERT(!me->HasTxData(table1, 103u));
    }

} // Y_UNIT_TEST_SUITE(DBRowLocks)

} // namespace NKikimr::NTable

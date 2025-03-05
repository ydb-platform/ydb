#include "defs.h"
#include <ydb/core/tx/locks/locks.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>

#include <ydb/core/tablet_flat/flat_dbase_apply.h>
#include <ydb/core/tablet_flat/flat_exec_commit.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr {

using namespace NSchemeShard;
using namespace Tests;

struct TTableId;

namespace NTest {
    enum EFakeIds : ui64 {
        ESchemeShardId = 1000,
        EUserTableId = 10001
    };

    ///
    class TFakeDataShard {
    public:
        ui64 PathOwnerId = 0;
        ui64 CurrentSchemeShardId = 0;
        TRowVersion CompleteVersion = TRowVersion::Min();

        TFakeDataShard() {
            InitSchema();
        }

        static ui64 TabletID() { return 0; }
        static ui32 Generation() { return 0; }
        TRowVersion LastCompleteTxVersion() const { return CompleteVersion; }

        bool IsUserTable(const TTableId& tableId) const;

        //

        template <typename T> void SetCounter(T, ui64) const {}
        template <typename T> void IncCounter(T, ui64 num = 1) const { Y_UNUSED(num); }
        template <typename T> void IncCounter(T, const TDuration&) const {}

    private:
        NTable::TScheme Schema;

        static ui32 NumSysTables() { return 10; }

        void InitSchema() {
            TableInfos[EUserTableId].LocalTid = EUserTableId;
            TableInfos[EUserTableId].Name = "user____Table";
            TableInfos[EUserTableId].KeyColumnTypes.push_back(NScheme::TTypeInfo(NScheme::NTypeIds::Uint32));

            NTable::TAlter delta;

            for (ui32 tableId = 0; tableId < NumSysTables(); ++tableId) {
                delta.AddTable(TString("Table") + ('A'+tableId), tableId);
                delta.AddColumn(tableId, "key", 0, NScheme::NTypeIds::Uint32, false);
                delta.AddColumnToKey(tableId, 0);
            }

            delta.AddTable("user____Table", EUserTableId);
            delta.AddColumn(EUserTableId, "key", 0, NScheme::NTypeIds::Uint32, false);
            delta.AddColumnToKey(EUserTableId, 0);


            NTable::TSchemeModifier(Schema).Apply(*delta.Flush());
        }

    public:
        TTabletCountersBase* TabletCounters = nullptr;
        THashMap<ui64, NDataShard::TUserTable> TableInfos;
    };
}

}

namespace NKikimr {

using namespace NDataShard;

namespace NTest {

    bool TFakeDataShard::IsUserTable(const TTableId& tableId) const {
        return tableId.PathId.LocalPathId >= NumSysTables();
    }

    ///
    class TLockTester {
    public:
        template <typename T>
        struct TPointKey {
            T Key;
            TCell Cell;

            TPointKey(const T& key)
                : Key(key)
                , Cell((const char*)&Key, sizeof(T))
            {}

            TArrayRef<const TCell> GetRow() const {
                return TArrayRef<const TCell>(&Cell, 1);
            }
        };

        template <typename T>
        struct TRangeKey {
            enum EFlags : ui32 {
                EIncFrom = 0x1,
                EIncTo = 0x2
            };

            T KeyFrom;
            T KeyTo;
            ui32 Flags;
            TCell Cells[2];

            TRangeKey(const T& from, const T& to, ui32 flags = 0)
                : KeyFrom(from)
                , KeyTo(to)
                , Flags(flags)
            {
                Cells[0] = TCell((const char*)&KeyFrom, sizeof(T));
                Cells[1] = TCell((const char*)&KeyTo, sizeof(T));
            }

            TTableRange GetRowsRange() const {
                return TTableRange(TArrayRef<const TCell>(&Cells[0], 1), Flags & EIncFrom,
                                   TArrayRef<const TCell>(&Cells[0], 1), Flags & EIncTo);
            }
        };

        TLockTester(const TTableId tableId = TTableId(ESchemeShardId, EUserTableId))
            : TableId(tableId)
            , Locks(&DataShard)
        {
            ui64 tid = tableId.PathId.LocalPathId;
            ui64 sid = tableId.PathId.OwnerId;

            TmpLock.PathId = tid;
            TmpLock.SchemeShard = sid;

            TmpLockVec.reserve(4);
            TmpLockVec.emplace_back(TCell::Make(TmpLock.LockId));
            TmpLockVec.emplace_back(TCell::Make(TmpLock.DataShard));
            TmpLockVec.emplace_back(TCell::Make(TmpLock.SchemeShard));
            TmpLockVec.emplace_back(TCell::Make(TmpLock.PathId));

            Locks.UpdateSchema(tableId.PathId, DataShard.TableInfos[tid].KeyColumnTypes);
        }

        //

        template <typename T>
        void SetLock(const TPointKey<T>& key) {
            Locks.SetLock(TableId, key.GetRow());
        }

        template <typename T>
        void SetLock(const TRangeKey<T>& range) {
            Locks.SetLock(TableId, range.GetRowsRange());
        }

        template <typename T>
        void BreakLocks(const TPointKey<T>& key) {
            Locks.BreakLocks(TableId, key.GetRow());
        }

        void BreakSetLocks() {
            Locks.BreakSetLocks();
        }

        //

        void EraseLock(ui64 lockId) {
            Locks.EraseLock(LockAsRowKey(lockId));
        }

        // Don't use generation:counter here
        bool CheckLock(ui64 lockId) {
            return ! Locks.GetLock(LockAsRowKey(lockId)).IsEmpty();
        }

        //

        void StartTx(TLocksUpdate& update) {
            update.LockTxId = 0;
            Locks.SetupUpdate(&update);
        }

        void StartTx(ui64 lockTxId, TLocksUpdate& update) {
            update.LockTxId = lockTxId;
            Locks.SetupUpdate(&update);
        }

        TVector<TSysLocks::TLock> ApplyTxLocks() {
            auto [locks, _] = Locks.ApplyLocks();
            Locks.ResetUpdate();
            return locks;
        }

        template <typename T>
        void Select(const TVector<T>& selects, bool breakSetLocks = false) {
            for (auto& value : selects) {
                SetLock(TLockTester::TPointKey<T>(value));
            }
            if (breakSetLocks)
                BreakSetLocks();
        }

        template <typename T>
        void Select(const TVector<std::pair<T, T>>& rangeSelects, ui32 rangeFlags = 0) {
            for (auto& range : rangeSelects) {
                SetLock(TLockTester::TRangeKey<T>(range.first, range.second, rangeFlags));
            }
        }

        template <typename T>
        void Select(const TVector<std::pair<T, T>>& rangeSelects, bool breakSetLocks, ui32 rangeFlags = 0) {
            for (auto& range : rangeSelects) {
                SetLock(TLockTester::TRangeKey<T>(range.first, range.second, rangeFlags));
            }
            if (breakSetLocks)
                BreakSetLocks();
        }

        template <typename T>
        void Update(const TVector<T>& updates) {
            for (auto& value : updates) {
                BreakLocks(TLockTester::TPointKey<T>(value));
            }
        }

        void PromoteCompleteVersion(const TRowVersion& completeVersion) {
            DataShard.CompleteVersion = Max(DataShard.CompleteVersion, completeVersion);
        }

    private:
        TTableId TableId;
        NTest::TFakeDataShard DataShard;
        TSysLocks Locks;
        TSysTables::TLocksTable::TLock TmpLock;
        TVector<TCell> TmpLockVec;

        ui64 LockId() const { return Locks.CurrentLockTxId(); }

        TArrayRef<const TCell> LockAsRowKey(ui64 lockId) {
            TmpLockVec[0] = TCell::Make(TmpLock.LockId = lockId);

            return TArrayRef<const TCell>(TmpLockVec.data(), TmpLockVec.size());
        }

        static TTableId LocksTableId() { return TTableId(TSysTables::SysSchemeShard, TSysTables::SysTableLocks); }
    };

    template <typename T>
    static void EmulateTx(TLockTester& tester, ui64 lockTxId,
                   const TVector<T>& selects, const TVector<T>& updates,
                   const TVector<std::pair<T, T>>& rangeSelects, ui32 rangeFlags = 0) {
        TLocksUpdate txLocks;
        tester.StartTx(lockTxId, txLocks);

        tester.Select(selects);
        tester.Update(updates);
        tester.Select(rangeSelects, rangeFlags);

        tester.ApplyTxLocks();
    }

    static void RemoveLock(TLockTester& tester, ui64 lockTxId) {
        TLocksUpdate txLocks;
        tester.StartTx(txLocks);

        tester.EraseLock(lockTxId);

        tester.ApplyTxLocks();
    }

    template <typename T>
    static void GeneratePoints(TVector<T>& points, ui32 numVals, T value, std::function<void (T&)> next) {
        points.reserve(points.size() + numVals);
        for (ui32 i = 0; i < numVals; ++i) {
            points.emplace_back(value);
            if (next)
                next(value);
        }
    }
}

// TODO: correctness, times
Y_UNIT_TEST_SUITE(TDataShardLocksTest) {

Y_UNIT_TEST(MvccTestOooTxDoesntBreakPrecedingReadersLocks) {
    NTest::TLockTester tester;

    {
        // lock tx
        TLocksUpdate update;
        update.CheckVersion = TRowVersion(1, 1);
        tester.StartTx(10, update);
        tester.Select(TVector<ui32>({10, 15, 25, 30}));
        for (auto lock : tester.ApplyTxLocks())
            UNIT_ASSERT(!lock.IsError());
    }

    {
        // Ooo write
        TLocksUpdate update;
        update.BreakVersion = TRowVersion(1, 10);
        tester.StartTx(update);
        tester.Update(TVector<ui32>({15, 25}));
        tester.ApplyTxLocks();
    }

    {
        // Check locks
        TLocksUpdate update;
        update.CheckVersion = TRowVersion(1, 5);
        tester.StartTx(update);
        UNIT_ASSERT(tester.CheckLock(10));
    }
}

Y_UNIT_TEST(MvccTestOutdatedLocksRemove) {
    NTest::TLockTester tester;

    {
        // Lock tx
        TLocksUpdate update;
        update.CheckVersion = TRowVersion(1, 1);
        tester.StartTx(10, update);
        tester.Select(TVector<ui32>({10}));
        for (auto lock : tester.ApplyTxLocks())
            UNIT_ASSERT(!lock.IsError());
    }

    {
        // Ooo write breaks set lock
        TLocksUpdate update;
        update.BreakVersion = TRowVersion(1, 10);
        tester.StartTx(update);
        tester.Update(TVector<ui32>({10}));
        tester.ApplyTxLocks();
    }

    {
        // Another lock
        TLocksUpdate update;
        update.CheckVersion = TRowVersion(1, 15);
        tester.StartTx(12, update);
        tester.Select(TVector<ui32>({22}));
        for (auto lock : tester.ApplyTxLocks())
            UNIT_ASSERT(!lock.IsError());
    }

    tester.PromoteCompleteVersion(TRowVersion(1, 10));

    {
        // next lock operation causes cleanup
        TLocksUpdate update;
        tester.StartTx(update);
        tester.ApplyTxLocks();
    }

    {
        // lock is removed
        TLocksUpdate update;
        update.CheckVersion = TRowVersion(1, 5);
        tester.StartTx(update);
        UNIT_ASSERT(!tester.CheckLock(10));
    }
}

Y_UNIT_TEST(MvccTestBreakEdge) {
    NTest::TLockTester tester;

    {
        // lock tx
        TLocksUpdate update;
        update.CheckVersion = TRowVersion(1, 1);
        tester.StartTx(10, update);
        tester.Select(TVector<ui32>({10, 15, 25, 30}));
        for (auto lock : tester.ApplyTxLocks())
            UNIT_ASSERT(!lock.IsError());
    }

    {
        // Ooo write
        TLocksUpdate update;
        update.BreakVersion = TRowVersion(1, 10);
        tester.StartTx(update);
        tester.Update(TVector<ui32>({15, 25}));
        tester.ApplyTxLocks();
    }

    {
        // Check locks
        TLocksUpdate update;
        update.CheckVersion = TRowVersion(1, 5);
        tester.StartTx(update);
        UNIT_ASSERT(tester.CheckLock(10));
        tester.ApplyTxLocks();
    }

    {
        // At this point lock is broken
        TLocksUpdate update;
        update.CheckVersion = TRowVersion(1, 15);
        tester.StartTx(update);
        UNIT_ASSERT(!tester.CheckLock(10));
    }
}

Y_UNIT_TEST(MvccTestWriteBreaksLocks) {
    NTest::TLockTester tester;

    {
        // lock tx
        TLocksUpdate update;
        update.CheckVersion = TRowVersion(1, 1);
        tester.StartTx(10, update);
        tester.Select(TVector<ui32>({10, 15, 25, 30}));
        for (auto lock : tester.ApplyTxLocks())
            UNIT_ASSERT(!lock.IsError());
    }

    {
        // subsequent write
        TLocksUpdate update;
        tester.StartTx(update);
        tester.Update(TVector<ui32>({15, 25}));
        tester.ApplyTxLocks();
    }

    {
        // lock is broken permanently
        TLocksUpdate update;
        update.CheckVersion = TRowVersion(1, 5);
        tester.StartTx(update);
        UNIT_ASSERT(!tester.CheckLock(10));
    }
}

Y_UNIT_TEST(MvccTestAlreadyBrokenLocks) {
    NTest::TLockTester tester;

    {
        // lock tx
        TLocksUpdate update;
        update.CheckVersion = TRowVersion(1, 1);
        tester.StartTx(10, update);
        tester.Select(TVector<ui32>({10, 15, 25, 30}), true);
        for (auto lock : tester.ApplyTxLocks())
            UNIT_ASSERT(lock.IsError() && lock.Counter == TSysTables::TLocksTable::TLock::ErrorAlreadyBroken);
    }
}

Y_UNIT_TEST(Points_OneTx) {
    NTest::TLockTester tester;

    ui32 numVals = 100 * 1000;
    ui64 txId = 100;
    TVector<ui32> selects;
    TVector<ui32> updates;
    TVector<std::pair<ui32, ui32>> ranges;

    std::function<void (ui32&)> fInc = [](ui32& val) { ++val; };
    NTest::GeneratePoints(selects, numVals, 0u, fInc);
    NTest::EmulateTx(tester, txId, selects, updates, ranges);
}

// 100000 tx, 1 points per tx
Y_UNIT_TEST(Points_ManyTx) {
    NTest::TLockTester tester;

    ui32 numVals = 100 * 1000;
    ui64 txId = 100;
    TVector<ui32> selects;
    TVector<ui32> updates;
    TVector<std::pair<ui32, ui32>> ranges;

    selects.resize(1);
    for (ui32 i = 0; i < numVals; ++i, ++txId) {
        selects[0] = i;
        NTest::EmulateTx(tester, txId, selects, updates, ranges);
    }
}

// TODO: 1000 tx, 100 points per tx
// TODO: 10000 tx, 10 points per tx

Y_UNIT_TEST(Points_ManyTx_BreakAll) {
    NTest::TLockTester tester;

    ui32 numVals = 100 * 1000;
    ui64 txId = 100;
    TVector<ui32> selects;
    TVector<ui32> updates;
    TVector<std::pair<ui32, ui32>> ranges;

    selects.resize(1);
    for (ui32 i = 0; i < numVals; ++i, ++txId) {
        selects[0] = i;
        NTest::EmulateTx(tester, txId, selects, updates, ranges);
    }

    txId = 100;
    selects.resize(0);
    updates.resize(1);
    for (ui32 i = 0; i < numVals; ++i, ++txId) {
        updates[0] = i;
        NTest::EmulateTx(tester, txId, selects, updates, ranges);
    }
}

Y_UNIT_TEST(Points_ManyTx_RemoveAll) {
    NTest::TLockTester tester;

    ui32 numVals = 100 * 1000;
    ui64 txId = 100;
    TVector<ui32> selects;
    TVector<ui32> updates;
    TVector<std::pair<ui32, ui32>> ranges;

    selects.resize(1);
    for (ui32 i = 0; i < numVals; ++i, ++txId) {
        selects[0] = i;
        NTest::EmulateTx(tester, txId, selects, updates, ranges);
    }

    txId = 100;
    selects.resize(0);
    updates.resize(1);
    for (ui32 i = 0; i < numVals; ++i, ++txId) {
        NTest::RemoveLock(tester, txId);
    }
}

Y_UNIT_TEST(Points_ManyTx_BreakHalf_RemoveHalf) {
    NTest::TLockTester tester;

    ui32 numVals = 100 * 1000;
    ui64 txId = 100;
    TVector<ui32> selects;
    TVector<ui32> updates;
    TVector<std::pair<ui32, ui32>> ranges;

    selects.resize(1);
    for (ui32 i = 0; i < numVals; ++i, ++txId) {
        selects[0] = i;
        NTest::EmulateTx(tester, txId, selects, updates, ranges);
    }

    txId = 100;
    selects.resize(0);
    updates.resize(1);
    for (ui32 i = 0; i < numVals; ++i, ++txId) {
        if (i%2) {
            updates[0] = i;
            NTest::EmulateTx(tester, txId, selects, updates, ranges);
        } else {
            NTest::RemoveLock(tester, txId);
        }
    }
}

void CheckLocksCacheUsage(bool waitForLocksStore) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false)
        // Note: disable volatile transactions, since they don't persist lock
        // state, and this test actually checks validated lock persistence.
        .SetEnableDataShardVolatileTransactions(false);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();

    auto sender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    //runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_TRACE);
    //runtime.SetLogPriority(NKikimrServices::KQP_YQL, NLog::PRI_TRACE);

    InitRoot(server, sender);

    TAutoPtr<IEventHandle> handle;
    NTabletPipe::TClientConfig pipeConfig;
    pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
    auto pipe = runtime.ConnectToPipe(ChangeStateStorage(SchemeRoot, serverSettings.Domain), sender, 0, pipeConfig);

    // Create table with two shards.
    ui64 txId;
    {
        auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        request->Record.SetExecTimeoutPeriod(Max<ui64>());
        auto &tx = *request->Record.MutableTransaction()->MutableModifyScheme();
        tx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateTable);
        tx.SetWorkingDir("/Root");
        auto &desc = *tx.MutableCreateTable();
        desc.SetName("table-1");
        {
            auto col = desc.AddColumns();
            col->SetName("key");
            col->SetType("Uint32");
        }
        {
            auto col = desc.AddColumns();
            col->SetName("value");
            col->SetType("Uint32");
        }
        desc.AddKeyColumnNames("key");
        desc.SetUniformPartitionsCount(2);

        runtime.Send(new IEventHandle(MakeTxProxyID(), sender, request.Release()));
        auto reply = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(handle);
        UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus(), TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress);
        txId = reply->Record.GetTxId();
    }
    {
        auto request = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>();
        request->Record.SetTxId(txId);
        runtime.SendToPipe(pipe, sender, request.Release());
        runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvNotifyTxCompletionResult>(handle);
    }

    {
        auto request = MakeSQLRequest("UPSERT INTO `/Root/table-1` (key, value) VALUES (1,0x80000002),(0x80000001,2)");
        runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId()), sender, request.Release()));
        runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(handle);
    }

    {
        auto request = MakeSQLRequest("UPSERT INTO `/Root/table-1` (key, value) SELECT value as key, value FROM `/Root/table-1`");
        runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId()), sender, request.Release()));

        // Get shard IDs.
        ui64 shards[2];
        {
            auto request = MakeHolder<TEvTxUserProxy::TEvNavigate>();
            request->Record.MutableDescribePath()->SetPath("/Root/table-1");
            runtime.Send(new IEventHandle(MakeTxProxyID(), sender, request.Release()));
            auto reply = runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvDescribeSchemeResult>(handle);
            for (auto i = 0; i < 2; ++i)
                shards[i] = reply->GetRecord().GetPathDescription()
                    .GetTablePartitions(i).GetDatashardId();
        }

        // Query should be split to two phases and second one
        // should use RS. Hold one RS to 'pause' one transaction
        // and then restart its shard. This will cause locks
        // cache to be used.
        auto captureRS = [shards](TAutoPtr<IEventHandle> &event) -> auto {
            if (event->GetTypeRewrite() == TEvTxProcessing::EvReadSet) {
                auto &rec = event->Get<TEvTxProcessing::TEvReadSet>()->Record;
                if (rec.GetTabletDest() == shards[0])
                    return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(captureRS);
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTxProcessing::EvReadSet, 1);
            runtime.DispatchEvents(options);
        }

        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        // If we want to wait until locks are stored then wait for log commits.
        if (waitForLocksStore) {
            struct IsCommitResult {
                bool operator()(IEventHandle& ev)
                {
                    if (ev.GetTypeRewrite() == TEvTablet::EvCommitResult) {
                        if (ev.Cookie == (ui64)NKikimr::NTabletFlatExecutor::ECommit::Redo) {
                            if (!Recipient)
                                Recipient = ev.Recipient;
                            else if (Recipient != ev.Recipient)
                                return true;
                        }
                    }

                    return false;
                }

                TActorId Recipient;
            };

            TDispatchOptions options;
            options.FinalEvents.emplace_back(IsCommitResult(), 1);
            runtime.DispatchEvents(options);
        }

        runtime.Register(CreateTabletKiller(shards[0]));
        runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(handle);
    }

    {
        auto request = MakeSQLRequest("SELECT * FROM `/Root/table-1`");
        runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId()), sender, request.Release()));
        auto reply = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(handle);
        auto &resp = reply->Record.GetRef().GetResponse();
        UNIT_ASSERT_VALUES_EQUAL(resp.YdbResultsSize(), 1);

        if (waitForLocksStore)
            UNIT_ASSERT_VALUES_EQUAL(resp.GetYdbResults(0).rows_size(), 4);
        else {
            // We don't actually know whether we killed tablet before locks were stored or after.
            // So either 2 or 4 records are allowed.
            UNIT_ASSERT(resp.GetYdbResults(0).rows_size() == 4
                        || resp.GetYdbResults(0).rows_size() == 2);
        }
    }
}

Y_UNIT_TEST(UseLocksCache) {
    CheckLocksCacheUsage(true);
    CheckLocksCacheUsage(false);
}
// TODO

}
}

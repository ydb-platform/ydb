#include "datashard_ut_common_kqp.h"
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common_tx.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/protos/query_stats.pb.h>

namespace NKikimr {

using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NKikimr::NDataShard::NTxHelpers;
using namespace Tests;

Y_UNIT_TEST_SUITE(DataShardReadCommitted) {

struct TTestEnv {
    TPortManager PortManager;
    TServer::TPtr Server;
    TActorId Sender;

    TDisableDataShardLogBatching DisableDataShardLogBatching;

    TTableId TableId;
    TVector<ui64> Shards;

    TTestActorRuntime& GetRuntime() { return *Server->GetRuntime(); }

    TTestEnv() {
        NKikimrConfig::TAppConfig app;
        TServerSettings serverSettings(PortManager.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetAppConfig(app);

        Server = new TServer(serverSettings);
        auto& runtime = GetRuntime();
        Sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(Server, Sender);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key UInt32, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS"
        );

        TableId = ResolveTableId(Server, Sender, "/Root/table");
        UNIT_ASSERT(TableId);
        Shards = GetTableShards(Server, Sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(Shards.size(), 1u);
    }

    auto GetAll() {
        const auto& self = *this;
        return std::tie(Server, GetRuntime(), self.Sender, self.TableId, self.Shards);
    }
};


Y_UNIT_TEST(PessimisticNoneModeSimple) {
    // PESSIMISTIC_NONE reads don't set locks but can read writes from the same tx.

    TTestEnv env;
    auto [server, runtime, sender, tableId, shards] = env.GetAll();

    ExecSQL(server, sender, R"(
        UPSERT INTO `/Root/table` (key, value) VALUES (1, 100), (2, 200);
    )");

    TTransactionState tx(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);

    // Read doesn't acquire locks
    UNIT_ASSERT_VALUES_EQUAL(tx.ReadKey(tableId, shards[0], 1), "1, 100\n");
    UNIT_ASSERT_VALUES_EQUAL(tx.Locks.size(), 0);

    // Write to key 1, this shouldn't break anything
    ExecSQL(server, sender, R"(
        UPSERT INTO `/Root/table` (key, value) VALUES (1, 101);
    )");

    // Lock key 2 and write to it with the same lock id.
    UNIT_ASSERT_VALUES_EQUAL(
        tx.LockRows(tableId, shards.at(0), {2}),
        "OK");
    UNIT_ASSERT_VALUES_EQUAL(tx.Locks.size(), 1);

    // Write with the same LockTxId to another key
    UNIT_ASSERT_VALUES_EQUAL(
        tx.Write(tableId, shards.at(0), TWriteOperation::Upsert(2, 201)),
        "OK");
    UNIT_ASSERT_VALUES_EQUAL(tx.Locks.size(), 2);

    // Read with the new snapshot. Check that the read sees the committed update.
    tx.ResetSnapshot();
    UNIT_ASSERT_VALUES_EQUAL(tx.ReadRange(tableId, shards[0], 1, 2), "1, 101\n2, 201\n");
    UNIT_ASSERT_VALUES_EQUAL(tx.Locks.size(), 2);

    // Check that we can commit the tx.
    UNIT_ASSERT_VALUES_EQUAL(tx.WriteCommit(tableId, shards.at(0)), "OK");

    // Check that other txs see the update from tx.
    TTransactionState tx2(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    UNIT_ASSERT_VALUES_EQUAL(tx2.ReadKey(tableId, shards[0], 2), "2, 201\n");
    UNIT_ASSERT_VALUES_EQUAL(tx2.Locks.size(), 0);
}

Y_UNIT_TEST(PessimisticNoneModeWriteWrite) {
    // PESSIMISTIC_NONE allows blind writes over committed updates (the expectation is that
    // they are prevented separately via the TEvLockRows mechanism).

    TTestEnv env;
    auto [server, runtime, sender, tableId, shards] = env.GetAll();

    ExecSQL(server, sender, R"(
        UPSERT INTO `/Root/table` (key, value) VALUES (1, 100), (2, 200);
    )");

    TTransactionState tx(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);

    // Start tx by reading key 1, this should not acquire locks, but will acquire
    // a snapshot.
    UNIT_ASSERT_VALUES_EQUAL(
        tx.ReadKey(tableId, shards.at(0), 1),
        "1, 100\n");
    UNIT_ASSERT_VALUES_EQUAL(tx.Locks.size(), 0u);

    // Write to key 2, which would be with version above the tx1 snapshot
    ExecSQL(server, sender, R"(
        UPSERT INTO `/Root/table` (key, value) VALUES (2, 201);
    )");

    // Commit along with writing to key 1. This is where PESSIMISTIC_NONE differs
    // from OPTIMISTIC_SNAPSHOT_ISOLATION:  Since this key is modified between
    // the snapshot and commit timestamps, with the snapshot isolation it would abort,
    // but PESSIMISTIC_NONE allows blind writes.
    UNIT_ASSERT_VALUES_EQUAL(
        tx.WriteCommit(tableId, shards.at(0), TWriteOperation::Upsert(2, 202)),
        "OK");

    // Check that tx committed successfully.
    UNIT_ASSERT_VALUES_EQUAL(
        KqpSimpleExec(runtime, R"(
            SELECT key, value FROM `/Root/table` ORDER BY key;
        )"),
        "{ items { uint32_value: 1 } items { int32_value: 100 } }, "
        "{ items { uint32_value: 2 } items { int32_value: 202 } }");
}

Y_UNIT_TEST(PessimisticNoneModeWriteWriteUncommitted) {
    // PESSIMISTIC_NONE uncommitted writes still conflict to uphold the
    // "first written, first committed" localdb uncommitted writes invariant.

    TTestEnv env;
    auto [server, runtime, sender, tableId, shards] = env.GetAll();

    ExecSQL(server, sender, R"(
        UPSERT INTO `/Root/table` (key, value) VALUES (1, 100), (2, 200);
    )");

    TTransactionState tx1(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    TTransactionState tx2(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);

    // Make an uncommitted write in tx1 to keys 1 and 2
    UNIT_ASSERT_VALUES_EQUAL(
        tx1.Write(tableId, shards.at(0), TWriteOperation::Upsert(1, 101)),
        "OK");
    UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.back().GetHasWrites(), true);

    UNIT_ASSERT_VALUES_EQUAL(
        tx1.Write(tableId, shards.at(0), TWriteOperation::Upsert(2, 201)),
        "OK");
    UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.size(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.back().GetHasWrites(), true);

    // Make an uncommitted write in tx2 to key 2
    UNIT_ASSERT_VALUES_EQUAL(
        tx2.Write(tableId, shards.at(0), TWriteOperation::Upsert(2, 202)),
        "OK");
    UNIT_ASSERT_VALUES_EQUAL(tx2.Locks.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(tx2.Locks.back().GetHasWrites(), true);

    // Commit tx2, it must succeed
    UNIT_ASSERT_VALUES_EQUAL(
        tx2.WriteCommit(tableId, shards.at(0)),
        "OK");

    // Try committing tx1, it must be broken now
    UNIT_ASSERT_VALUES_EQUAL(
        tx1.WriteCommit(tableId, shards.at(0)),
        "ERROR: STATUS_LOCKS_BROKEN");

    // We should not observe a commit by tx1.
    UNIT_ASSERT_VALUES_EQUAL(
        KqpSimpleExec(runtime, R"(
            SELECT key, value FROM `/Root/table` ORDER BY key;
        )"),
        "{ items { uint32_value: 1 } items { int32_value: 100 } }, "
        "{ items { uint32_value: 2 } items { int32_value: 202 } }");
}

Y_UNIT_TEST(BlockedWritesAndConflicts) {
    // Test that
    // 1) Transactions using the TEvLockRows + TEvWrite(PESSIMISTIC_NONE) write protocol block on
    // conflicting transactions, but are able to proceed after conflicting transactions commit.
    // 2) Conflicting SERIALIZABLE writes cause the LOCKS_BROKEN error for read committed transactions.

    TTestEnv env;
    auto [server, runtime, sender, tableId, shards] = env.GetAll();

    ExecSQL(server, sender, R"(
        UPSERT INTO `/Root/table` (key, value) VALUES (1, 100);
    )");

    TTransactionState tx1(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    TTransactionState tx2(runtime, NKikimrDataEvents::OPTIMISTIC);
    TTransactionState tx3(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);

    // tx1 locks the row and updates it.
    UNIT_ASSERT_VALUES_EQUAL(
        tx1.LockRows(tableId, shards.at(0), {1}),
        "OK");
    UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.size(), 1);

    UNIT_ASSERT_VALUES_EQUAL(
        tx1.Write(tableId, shards.at(0), TWriteOperation::Upsert(1, 101)),
        "OK");

    // SERIALIZABLE tx2 updates the row without locking.
    UNIT_ASSERT_VALUES_EQUAL(
        tx2.Write(tableId, shards.at(0), TWriteOperation::Upsert(1, 102)),
        "OK");

    // Should block, the row is still locked by tx1.
    auto tx3LockPromise = tx3.SendLockRows(tableId, shards.at(0), {1});
    UNIT_ASSERT_VALUES_EQUAL(
        tx3LockPromise.NextString(TDuration::Seconds(1)),
        "<timeout>");

    // Commit tx1, tx3 should successfully lock the row.
    UNIT_ASSERT_VALUES_EQUAL(
        tx1.WriteCommit(tableId, shards.at(0)),
        "OK");

    UNIT_ASSERT_VALUES_EQUAL(
        tx3LockPromise.NextString(),
        "OK");

    // Commit tx2, this should break the tx3 LockTxId, so that it should be unable to commit.
    UNIT_ASSERT_VALUES_EQUAL(
        tx2.WriteCommit(tableId, shards.at(0)),
        "OK");

    UNIT_ASSERT_VALUES_EQUAL(
        tx3.WriteCommit(tableId, shards.at(0)),
        "ERROR: STATUS_LOCKS_BROKEN");
}

Y_UNIT_TEST(CommitAfterDeadlockResolution) {
    // Test that when two transactions enter a deadlock, one of them is able to commit
    // after deadlock resolution.

    TTestEnv env;
    auto [server, runtime, sender, tableId, shards] = env.GetAll();

    ExecSQL(server, sender, R"(
        UPSERT INTO `/Root/table` (key, value) VALUES (1, 100), (2, 200);
    )");

    TTransactionState tx1(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    TTransactionState tx2(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);

    // tx1 locks key 1 and updates it.
    UNIT_ASSERT_VALUES_EQUAL(
        tx1.LockRows(tableId, shards.at(0), {1}),
        "OK");
    UNIT_ASSERT_VALUES_EQUAL(tx1.Locks.size(), 1);

    UNIT_ASSERT_VALUES_EQUAL(
        tx1.Write(tableId, shards.at(0), TWriteOperation::Upsert(1, 101)),
        "OK");

    // tx2 locks key 2 and updates it.
    UNIT_ASSERT_VALUES_EQUAL(
        tx2.LockRows(tableId, shards.at(0), {2}),
        "OK");
    UNIT_ASSERT_VALUES_EQUAL(tx2.Locks.size(), 1);

    UNIT_ASSERT_VALUES_EQUAL(
        tx2.Write(tableId, shards.at(0), TWriteOperation::Upsert(2, 202)),
        "OK");

    // tx1 tries locking key 2 and blocks.
    auto tx1LockPromise = tx1.SendLockRows(tableId, shards.at(0), {2});
    UNIT_ASSERT_VALUES_EQUAL(
        tx1LockPromise.NextString(TDuration::Seconds(1)),
        "<timeout>");

    // tx2 tries locking key 1 and fails.
    UNIT_ASSERT_VALUES_EQUAL(
        tx2.LockRows(tableId, shards.at(0), {1}),
        "ERROR: STATUS_DEADLOCK");

    UNIT_ASSERT_VALUES_EQUAL(
        tx2.Rollback(shards.at(0)),
        "OK"
    );

    // tx1 unblocks
    UNIT_ASSERT_VALUES_EQUAL(
        tx1LockPromise.NextString(),
        "OK");

    UNIT_ASSERT_VALUES_EQUAL(
        tx1.Write(tableId, shards.at(0), TWriteOperation::Upsert(2, 201)),
        "OK");

    // tx1 should be able to commit now.
    UNIT_ASSERT_VALUES_EQUAL(
        tx1.WriteCommit(tableId, shards.at(0)),
        "OK");

    // We should now observe the commit by tx1.
    UNIT_ASSERT_VALUES_EQUAL(
        KqpSimpleExec(runtime, R"(
            SELECT key, value FROM `/Root/table` ORDER BY key;
        )"),
        "{ items { uint32_value: 1 } items { int32_value: 101 } }, "
        "{ items { uint32_value: 2 } items { int32_value: 201 } }");
}

Y_UNIT_TEST(ReadAfterSplit) {
    // Tests that a multi-page PESSIMISTIC_NONE range read that spans a split
    // fails with "Read conflict with concurrent transaction".
    //
    // The split erases tx1's write lock (calling OnRemoved which marks it
    // IsBroken=true).  When TTxReadContinue::ApplyLocks runs after the split,
    // it sees the broken write lock and returns the error.
    //
    // The precise race: TTxReadContinue must run AFTER the TTxStartSplit that
    // erases tx1's lock (setting IsBroken=true) but BEFORE the final
    // TTxStartSplit that calls CancelReadIterators (when no locks remain).
    //
    // TTxStartSplit erases one persistent lock per execution and reschedules
    // itself. The flat executor activates the next transaction by sending
    // TEvActivateExecution. We use a second lock (tx0) so the sequence is:
    //   TTxStartSplit #1: erase tx0's lock  → TEvActivateExecution captured
    //   TTxStartSplit #2: erase tx1's lock (IsBroken=true) → TEvActivateExecution
    //   TTxStartSplit #3: no locks → MakeSnapshot → CancelReadIterators
    //
    // By sending ReadAck while activation #1 is captured, TTxReadContinue gets
    // queued; then unblocking activation #1 runs #2 (tx1 broken), and
    // TTxReadContinue runs before #3 (CancelReadIterators), seeing the broken
    // write lock and returning ABORTED.

    TTestEnv env;
    auto [server, runtime, sender, tableId, shards] = env.GetAll();

    ExecSQL(server, sender, R"(
        UPSERT INTO `/Root/table` (key, value) VALUES (1, 100), (10, 1000), (20, 2000);
    )");

    TTransactionState tx0(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    TTransactionState tx1(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);

    UNIT_ASSERT_VALUES_EQUAL(
        tx0.Write(tableId, shards.at(0), TWriteOperation::Upsert(1, 101)),
        "OK");

    // tx1 writes key 5 to establish a write lock on the shard.
    UNIT_ASSERT_VALUES_EQUAL(
        tx1.Write(tableId, shards.at(0), TWriteOperation::Upsert(5, 500)),
        "OK");

    // Start a range read with MaxRows=1 so the quota is exhausted after the
    // first row, blocking the shard (QuotaBlocked=true) until TEvReadAck.
    // This keeps the read iterator and state.Lock alive across the split.
    auto readPromise = tx1.SendReadRange(tableId, shards.at(0), 1, 20, /*maxRowsQuota=*/1);

    // Receive first row (key=1). TReadOperation has run: state.Lock is set.
    UNIT_ASSERT_VALUES_EQUAL(readPromise.NextString(), "1, 100\n");

    TBlockEvents<TEvDataShard::TEvSplit> blockedSplit(runtime);

    SetSplitMergePartCountLimit(&runtime, -1);
    ui64 splitTxId = AsyncSplitTable(server, sender, "/Root/table", shards.at(0), 10);

    runtime.WaitFor("TEvSplit", [&]{ return !blockedSplit.empty(); });

    blockedSplit.Unblock().Stop();
    TBlockEvents<IEventHandle> blockActivation(runtime, [](const IEventHandle::TPtr& ev) {
        return ev.Get()->GetTypeName().Contains("TEvActivateExecution");
    });

    runtime.WaitFor("TEvActivateExecution", [&]{ return !blockActivation.empty(); });
    // Send TEvReadAck to trigger TTxReadContinue.  ApplyLocks sees the broken
    // write lock and returns ABORTED ("Read conflict with concurrent transaction").
    readPromise.SendAck();
    blockActivation.Unblock().Stop();

    UNIT_ASSERT_VALUES_EQUAL(
        readPromise.NextString(),
        TStringBuilder() << "ERROR: ABORTED");

    WaitTxNotification(server, sender, splitTxId);
}

} // Y_UNIT_TEST_SUITE(DataShardReadCommitted)

} // namespace NKikimr

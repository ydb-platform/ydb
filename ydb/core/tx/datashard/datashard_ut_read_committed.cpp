#include "datashard_ut_common_kqp.h"
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common_tx.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/protos/query_stats.pb.h>

namespace NKikimr {

using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NKikimr::NDataShard::NTxHelpers;
using namespace Tests;

Y_UNIT_TEST_SUITE(DataShardReadCommitted) {

std::tuple<TTestActorRuntime&, Tests::TServer::TPtr, TActorId> TestCreateServer(const TServerSettings& serverSettings) {
    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto& runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

    InitRoot(server, sender);

    return {runtime, server, sender};
}

Y_UNIT_TEST(PessimisticNoneModeSimple) {
    // PESSIMISTIC_NONE reads don't set locks but can read writes from the same tx.

    TPortManager pm;
    NKikimrConfig::TAppConfig app;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false)
        .SetAppConfig(app);

    auto [runtime, server, sender] = TestCreateServer(serverSettings);

    TDisableDataShardLogBatching disableDataShardLogBatching;

    UNIT_ASSERT_VALUES_EQUAL(
        KqpSchemeExec(runtime, R"(
            CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key));
        )"),
        "SUCCESS"
    );

    ExecSQL(server, sender, R"(
        UPSERT INTO `/Root/table` (key, value) VALUES (1, 100), (2, 200);
    )");

    const auto tableId = ResolveTableId(server, sender, "/Root/table");
    UNIT_ASSERT(tableId);
    const auto shards = GetTableShards(server, sender, "/Root/table");
    UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

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

    TPortManager pm;
    NKikimrConfig::TAppConfig app;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false)
        .SetAppConfig(app);

    auto [runtime, server, sender] = TestCreateServer(serverSettings);

    TDisableDataShardLogBatching disableDataShardLogBatching;

    UNIT_ASSERT_VALUES_EQUAL(
        KqpSchemeExec(runtime, R"(
            CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key));
        )"),
        "SUCCESS"
    );

    ExecSQL(server, sender, R"(
        UPSERT INTO `/Root/table` (key, value) VALUES (1, 100), (2, 200);
    )");

    const auto tableId = ResolveTableId(server, sender, "/Root/table");
    UNIT_ASSERT(tableId);
    const auto shards = GetTableShards(server, sender, "/Root/table");
    UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

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
        "{ items { int32_value: 1 } items { int32_value: 100 } }, "
        "{ items { int32_value: 2 } items { int32_value: 202 } }");
}

Y_UNIT_TEST(PessimisticNoneModeWriteWriteUncommitted) {
    // PESSIMISTIC_NONE uncommitted writes still conflict to uphold the
    // "first written, first committed" localdb uncommitted writes invariant.

    TPortManager pm;
    NKikimrConfig::TAppConfig app;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false)
        .SetAppConfig(app);

    auto [runtime, server, sender] = TestCreateServer(serverSettings);

    TDisableDataShardLogBatching disableDataShardLogBatching;

    UNIT_ASSERT_VALUES_EQUAL(
        KqpSchemeExec(runtime, R"(
            CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key));
        )"),
        "SUCCESS"
    );

    ExecSQL(server, sender, R"(
        UPSERT INTO `/Root/table` (key, value) VALUES (1, 100), (2, 200);
    )");

    const auto tableId = ResolveTableId(server, sender, "/Root/table");
    UNIT_ASSERT(tableId);
    const auto shards = GetTableShards(server, sender, "/Root/table");
    UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

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
        "{ items { int32_value: 1 } items { int32_value: 100 } }, "
        "{ items { int32_value: 2 } items { int32_value: 202 } }");
}

} // Y_UNIT_TEST_SUITE(DataShardReadCommitted)

} // namespace NKikimr

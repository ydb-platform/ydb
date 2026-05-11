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

    tx.ResetSnapshot();
    UNIT_ASSERT_VALUES_EQUAL(tx.ReadRange(tableId, shards[0], 1, 2), "1, 101\n2, 201\n");
    UNIT_ASSERT_VALUES_EQUAL(tx.Locks.size(), 2);

    // Check that we can commit the tx.
    UNIT_ASSERT_VALUES_EQUAL(tx.WriteCommit(tableId, shards.at(0)), "OK");

    // Check that other txs see the update
    TTransactionState tx2(runtime, NKikimrDataEvents::PESSIMISTIC_NONE);
    UNIT_ASSERT_VALUES_EQUAL(tx2.ReadKey(tableId, shards[0], 2), "2, 201\n");
    UNIT_ASSERT_VALUES_EQUAL(tx2.Locks.size(), 0);
}

} // Y_UNIT_TEST_SUITE(DataShardReadCommitted)

} // namespace NKikimr

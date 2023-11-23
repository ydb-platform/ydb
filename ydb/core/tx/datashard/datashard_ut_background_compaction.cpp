#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include "datashard_active_transaction.h"

#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/read_table.h>

#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NSchemeShard;
using namespace Tests;

namespace {

NKikimrTxDataShard::TEvCompactTableResult CompactTable(
        Tests::TServer::TPtr server,
        NKikimrTxDataShard::TEvGetInfoResponse::TUserTable& userTable,
        ui64 tabletId,
        ui64 ownerId)
{
    return CompactTable(*server->GetRuntime(), tabletId, TTableId(ownerId, userTable.GetPathId()));
}

} // namespace

Y_UNIT_TEST_SUITE(DataShardBackgroundCompaction) {
    Y_UNIT_TEST(ShouldCompact) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100), (3, 300), (5, 500);");

        auto shards = GetTableShards(server, sender, "/Root/table-1");

        auto [tables, ownerId] = GetTables(server, shards.at(0));
        auto compactionResult = CompactTable(server, tables["table-1"], shards.at(0), ownerId);
        UNIT_ASSERT_VALUES_EQUAL(compactionResult.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::OK);
    }

    Y_UNIT_TEST(ShouldNotCompactWhenBorrowed) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100), (3, 300), (5, 500);");

        auto shards = GetTableShards(server, sender, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1UL);
        const auto originalShard = shards.at(0);

        SetSplitMergePartCountLimit(&runtime, -1);
        ui64 txId = AsyncSplitTable(server, sender, "/Root/table-1", shards.at(0), 3);
        WaitTxNotification(server, sender, txId);

        shards = GetTableShards(server, sender, "/Root/table-1");
        UNIT_ASSERT(shards.size() > 1);

        for (auto shard: shards) {
            auto [tables, ownerId] = GetTables(server, shard);
            auto compactionResult = CompactTable(server, tables["table-1"], shard, ownerId);
            UNIT_ASSERT_VALUES_EQUAL(compactionResult.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::BORROWED);
        }

        {
            auto [tables, ownerId] = GetTables(server, originalShard);
            // try to compact original table (should be inactive now)
            auto compactionResult = CompactTable(server, tables["table-1"], originalShard, ownerId);
            UNIT_ASSERT_VALUES_EQUAL(compactionResult.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::FAILED);
        }
    }

    Y_UNIT_TEST(ShouldNotCompactWhenCopyTable) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100), (3, 300), (5, 500);");

        auto txIdCopy = AsyncCreateCopyTable(server, sender, "/Root", "table-2", "/Root/table-1");
        WaitTxNotification(server, sender, txIdCopy);

        {
            auto shards = GetTableShards(server, sender, "/Root/table-1");

            auto [tables, ownerId] = GetTables(server, shards.at(0));
            auto compactionResult = CompactTable(server, tables["table-1"], shards.at(0), ownerId);
            UNIT_ASSERT_VALUES_EQUAL(compactionResult.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::LOANED);
        }

        {
            auto shards = GetTableShards(server, sender, "/Root/table-2");

            auto [tables, ownerId] = GetTables(server, shards.at(0));
            auto compactionResult = CompactTable(server, tables["table-2"], shards.at(0), ownerId);
            UNIT_ASSERT_VALUES_EQUAL(compactionResult.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::BORROWED);
        }
    }

    Y_UNIT_TEST(ShouldNotCompactEmptyTable) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        auto shards = GetTableShards(server, sender, "/Root/table-1");

        auto [tables, ownerId] = GetTables(server, shards.at(0));
        auto compactionResult = CompactTable(server, tables["table-1"], shards.at(0), ownerId);
        UNIT_ASSERT(compactionResult.GetStatus() == NKikimrTxDataShard::TEvCompactTableResult::NOT_NEEDED);
    }

    Y_UNIT_TEST(ShouldNotCompactSecondTime) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100), (3, 300), (5, 500);");

        auto shards = GetTableShards(server, sender, "/Root/table-1");

        auto [tables, ownerId] = GetTables(server, shards.at(0));
        auto compactionResult = CompactTable(server, tables["table-1"], shards.at(0), ownerId);
        UNIT_ASSERT_VALUES_EQUAL(compactionResult.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::OK);

        compactionResult = CompactTable(server, tables["table-1"], shards.at(0), ownerId);
        UNIT_ASSERT_VALUES_EQUAL(compactionResult.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::NOT_NEEDED);
    }
}

} // namespace NKikimr

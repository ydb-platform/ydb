#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include "datashard_active_transaction.h"

#include <ydb/core/tx/tx_proxy/proxy.h>

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NSchemeShard;
using namespace Tests;

Y_UNIT_TEST_SUITE(DataShardReplication) {

    Y_UNIT_TEST(SimpleApplyChanges) {
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

        CreateShardedTable(server, sender, "/Root", "table-1", TShardedTableOptions()
            .Replicated(true)
            .ReplicationConsistency(EReplicationConsistency::Strong)
        );
        CreateShardedTable(server, sender, "/Root", "table-2", TShardedTableOptions()
            .Replicated(true)
            .ReplicationConsistency(EReplicationConsistency::Strong)
        );

        auto shards1 = GetTableShards(server, sender, "/Root/table-1");
        auto shards2 = GetTableShards(server, sender, "/Root/table-2");
        auto tableId1 = ResolveTableId(server, sender, "/Root/table-1");
        auto tableId2 = ResolveTableId(server, sender, "/Root/table-2");

        ApplyChanges(server, shards1.at(0), tableId1, "my-source", {
            TChange{ .Offset = 0, .WriteTxId = 123, .Key = 1, .Value = 11 },
            TChange{ .Offset = 1, .WriteTxId = 234, .Key = 2, .Value = 22 },
            TChange{ .Offset = 2, .WriteTxId = 345, .Key = 2, .Value = 33 },
        });

        ApplyChanges(server, shards1.at(0), tableId1, "my-source", {
            TChange{ .Offset = 1, .WriteTxId = 234, .Key = 2, .Value = 22 },
        });

        ApplyChanges(server, shards2.at(0), tableId2, "my-source", {
            TChange{ .Offset = 3, .WriteTxId = 345, .Key = 4, .Value = 44 },
        });

        CommitWrites(server, { "/Root/table-1" }, 123);

        {
            auto result = ReadShardedTable(server, "/Root/table-1");
            UNIT_ASSERT_VALUES_EQUAL(result,
                "key = 1, value = 11\n");
        }

        CommitWrites(server, { "/Root/table-1" }, 234);

        {
            auto result = ReadShardedTable(server, "/Root/table-1");
            UNIT_ASSERT_VALUES_EQUAL(result,
                "key = 1, value = 11\n"
                "key = 2, value = 22\n");
        }

        CommitWrites(server, { "/Root/table-1", "/Root/table-2" }, 345);

        {
            auto result = ReadShardedTable(server, "/Root/table-1");
            UNIT_ASSERT_VALUES_EQUAL(result,
                "key = 1, value = 11\n"
                "key = 2, value = 33\n");
        }

        {
            auto result = ReadShardedTable(server, "/Root/table-2");
            UNIT_ASSERT_VALUES_EQUAL(result,
                "key = 4, value = 44\n");
        }
    }

    void DoSplitMergeChanges(bool withReboots) {
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

        CreateShardedTable(server, sender, "/Root", "table-1", TShardedTableOptions()
            .Replicated(true)
            .ReplicationConsistency(EReplicationConsistency::Strong)
        );
        CreateShardedTable(server, sender, "/Root", "table-2", TShardedTableOptions()
            .Replicated(true)
            .ReplicationConsistency(EReplicationConsistency::Strong)
        );

        auto shards1 = GetTableShards(server, sender, "/Root/table-1");
        auto tableId1 = ResolveTableId(server, sender, "/Root/table-1");

        ApplyChanges(server, shards1.at(0), tableId1, "my-source", {
            TChange{ .Offset = 0, .WriteTxId = 123, .Key = 1, .Value = 11 },
            TChange{ .Offset = 1, .WriteTxId = 123, .Key = 1, .Value = 22 },
            TChange{ .Offset = 2, .WriteTxId = 123, .Key = 5, .Value = 33 },
            TChange{ .Offset = 3, .WriteTxId = 123, .Key = 5, .Value = 44 },
        });

        // Split would fail otherwise :(
        SetSplitMergePartCountLimit(server->GetRuntime(), -1);

        auto senderSplit = runtime.AllocateEdgeActor();
        ui64 txId = AsyncSplitTable(server, senderSplit, "/Root/table-1", shards1.at(0), 5);
        WaitTxNotification(server, senderSplit, txId);

        shards1 = GetTableShards(server, sender, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(shards1.size(), 2u);

        txId = AsyncSplitTable(server, senderSplit, "/Root/table-1", shards1.at(1), 10);
        WaitTxNotification(server, senderSplit, txId);

        shards1 = GetTableShards(server, sender, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(shards1.size(), 3u);

        // Compact tables so we can merge them later
        for (ui64 shardId : shards1) {
            CompactTable(runtime, shardId, tableId1, true);
        }

        if (withReboots) {
            for (ui64 shardId : shards1) {
                RebootTablet(runtime, shardId, sender);
            }
        }

        // We expect this change to be ignored (let's pretend this was from some very old worker)
        ApplyChanges(server, shards1.at(1), tableId1, "my-source", {
            TChange{ .Offset = 2, .WriteTxId = 123, .Key = 5, .Value = 33 },
        });

        // Apply some newer changes that are specific to the right shard
        ApplyChanges(server, shards1.at(1), tableId1, "my-source", {
            TChange{ .Offset = 8, .WriteTxId = 123, .Key = 6, .Value = 77 },
            TChange{ .Offset = 9, .WriteTxId = 123, .Key = 6, .Value = 88 },
        });

        CommitWrites(server, { "/Root/table-1" }, 123);

        if (withReboots) {
            for (ui64 shardId : shards1) {
                RebootTablet(runtime, shardId, sender);
            }
        }

        {
            auto result = ReadShardedTable(server, "/Root/table-1");
            UNIT_ASSERT_VALUES_EQUAL(result,
                "key = 1, value = 22\n"
                "key = 5, value = 44\n"
                "key = 6, value = 88\n");
        }

        if (withReboots) {
            for (ui64 shardId : shards1) {
                RebootTablet(runtime, shardId, sender);
            }
        }

        // Merge shards back into a single shard
        txId = AsyncMergeTable(server, senderSplit, "/Root/table-1", shards1);
        WaitTxNotification(server, senderSplit, txId);

        shards1 = GetTableShards(server, sender, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(shards1.size(), 1u);

        if (withReboots) {
            for (ui64 shardId : shards1) {
                RebootTablet(runtime, shardId, sender);
            }
        }

        // We expect changes 4-7 to be applied, but change 8 to be ignored, then 10 applied
        ApplyChanges(server, shards1.at(0), tableId1, "my-source", {
            TChange{ .Offset = 4, .WriteTxId = 234, .Key = 2, .Value = 91 },
            TChange{ .Offset = 5, .WriteTxId = 234, .Key = 2, .Value = 92 },
            TChange{ .Offset = 6, .WriteTxId = 234, .Key = 10, .Value = 93 },
            TChange{ .Offset = 7, .WriteTxId = 234, .Key = 10, .Value = 94 },
            TChange{ .Offset = 8, .WriteTxId = 234, .Key = 6, .Value = 77 },
            TChange{ .Offset = 10, .WriteTxId = 234, .Key = 7, .Value = 95 },
        });

        CommitWrites(server, { "/Root/table-1" }, 234);

        {
            auto result = ReadShardedTable(server, "/Root/table-1");
            UNIT_ASSERT_VALUES_EQUAL(result,
                "key = 1, value = 22\n"
                "key = 2, value = 92\n"
                "key = 5, value = 44\n"
                "key = 6, value = 88\n"
                "key = 7, value = 95\n"
                "key = 10, value = 94\n");
        }
    }

    Y_UNIT_TEST(SplitMergeChanges) {
        DoSplitMergeChanges(false);
    }

    Y_UNIT_TEST(SplitMergeChangesReboots) {
        DoSplitMergeChanges(true);
    }

    Y_UNIT_TEST(ReplicatedTable) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1", TShardedTableOptions().Replicated(true));

        ExecSQL(server, sender, "SELECT * FROM `/Root/table-1`");
        ExecSQL(server, sender, "INSERT INTO `/Root/table-1` (key, value) VALUES (1, 10);", true,
            Ydb::StatusIds::GENERIC_ERROR);

        WaitTxNotification(server, sender, AsyncAlterDropReplicationConfig(server, "/Root", "table-1"));
        ExecSQL(server, sender, "INSERT INTO `/Root/table-1` (key, value) VALUES (1, 10);");
    }

    Y_UNIT_TEST(ApplyChangesToReplicatedTable) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1", TShardedTableOptions()
            .Replicated(true)
            .ReplicationConsistency(EReplicationConsistency::Weak)
        );

        auto shards = GetTableShards(server, sender, "/Root/table-1");
        auto tableId = ResolveTableId(server, sender, "/Root/table-1");

        ApplyChanges(server, shards.at(0), tableId, "my-source", {
            TChange{ .Offset = 0, .WriteTxId = 0, .Key = 1, .Value = 11 },
            TChange{ .Offset = 1, .WriteTxId = 0, .Key = 2, .Value = 22 },
            TChange{ .Offset = 2, .WriteTxId = 0, .Key = 3, .Value = 33 },
        });

        auto result = ReadShardedTable(server, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(result,
            "key = 1, value = 11\n"
            "key = 2, value = 22\n"
            "key = 3, value = 33\n"
        );
    }

    Y_UNIT_TEST(ApplyChangesToCommonTable) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1", TShardedTableOptions());

        auto shards = GetTableShards(server, sender, "/Root/table-1");
        auto tableId = ResolveTableId(server, sender, "/Root/table-1");

        ApplyChanges(server, shards.at(0), tableId, "my-source", {
            TChange{ .Offset = 0, .WriteTxId = 0, .Key = 1, .Value = 11 },
        }, NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED);
    }

}

} // namespace NKikimr

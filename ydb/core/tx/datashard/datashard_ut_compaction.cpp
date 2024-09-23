#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NSchemeShard;
using namespace Tests;

Y_UNIT_TEST_SUITE(DataShardCompaction) {
    Y_UNIT_TEST(CompactBorrowed) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        // runtime.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100), (2, 200), (3, 300), (4, 400);");

        auto shards1 = GetTableShards(server, sender, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(shards1.size(), 1u);

        // Split would fail otherwise :(
        SetSplitMergePartCountLimit(server->GetRuntime(), -1);

        auto senderSplit = runtime.AllocateEdgeActor();
        ui64 txId = AsyncSplitTable(server, senderSplit, "/Root/table-1", shards1.at(0), 3);
        WaitTxNotification(server, senderSplit, txId);

        auto shards2 = GetTableShards(server, sender, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(shards2.size(), 2u);

        {
            auto stats = WaitTableStats(runtime, shards2.at(0));
            Cerr << "Received shard stats:" << Endl << stats.DebugString();
            const auto& ownersProto = stats.GetUserTablePartOwners();
            THashSet<ui64> owners(ownersProto.begin(), ownersProto.end());
            // NOTE: datashard always adds current shard to part owners, even if there are no parts
            UNIT_ASSERT_VALUES_EQUAL(owners.size(), 2u);
            UNIT_ASSERT(owners.contains(shards1.at(0)));
            UNIT_ASSERT(owners.contains(shards2.at(0)));
        }

        {
            auto tableId = ResolveTableId(server, sender, "/Root/table-1");
            auto result = CompactBorrowed(runtime, shards2.at(0), tableId);
            Cerr << "Compact result " << result.DebugString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(result.GetTabletId(), shards2.at(0));
            UNIT_ASSERT_VALUES_EQUAL(result.GetPathId().GetOwnerId(), tableId.PathId.OwnerId);
            UNIT_ASSERT_VALUES_EQUAL(result.GetPathId().GetLocalId(), tableId.PathId.LocalPathId);
        }

        for (int i = 0; i < 5; ++i) {
            auto stats = WaitTableStats(runtime, shards2.at(0));
            // Cerr << "Received shard stats:" << Endl << stats.DebugString() << Endl;
            const auto& ownersProto = stats.GetUserTablePartOwners();
            THashSet<ui64> owners(ownersProto.begin(), ownersProto.end());
            if (i < 4) {
                if (owners.size() > 1) {
                    continue;
                }
                if (owners.contains(shards1.at(0))) {
                    continue;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(owners.size(), 1u);
            UNIT_ASSERT(owners.contains(shards2.at(0)));
        }

        {
            auto result = ReadShardedTable(server, "/Root/table-1");
            UNIT_ASSERT_VALUES_EQUAL(result,
                "key = 1, value = 100\n"
                "key = 2, value = 200\n"
                "key = 3, value = 300\n"
                "key = 4, value = 400\n");
        }
    }

    Y_UNIT_TEST(CompactBorrowedTxStatus) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        // runtime.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", TShardedTableOptions()
            .Replicated(true)
            .ReplicationConsistency(EReplicationConsistency::Strong)
        );

        auto shards1 = GetTableShards(server, sender, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(shards1.size(), 1u);

        auto tableId = ResolveTableId(server, sender, "/Root/table-1");

        // We make some uncommitted changes first
        ApplyChanges(server, shards1.at(0), tableId, "my-source", {
            TChange{ .Offset = 0, .WriteTxId = 123, .Key = 1, .Value = 100 },
            TChange{ .Offset = 1, .WriteTxId = 123, .Key = 2, .Value = 200 },
            TChange{ .Offset = 2, .WriteTxId = 123, .Key = 3, .Value = 300 },
            TChange{ .Offset = 3, .WriteTxId = 123, .Key = 4, .Value = 400 },
        });

        // Split would fail otherwise :(
        SetSplitMergePartCountLimit(server->GetRuntime(), -1);

        // Redundant split, so we get an sst with uncommitted changes
        auto senderSplit = runtime.AllocateEdgeActor();
        ui64 txId = AsyncSplitTable(server, senderSplit, "/Root/table-1", shards1.at(0), 10);
        WaitTxNotification(server, senderSplit, txId);

        auto shards2 = GetTableShards(server, sender, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(shards2.size(), 2u);

        // Now we commit changes and split again, making borrowed sst + tx status
        CommitWrites(server, { "/Root/table-1" }, 123);
        txId = AsyncSplitTable(server, senderSplit, "/Root/table-1", shards2.at(0), 3);
        WaitTxNotification(server, senderSplit, txId);

        auto shards3 = GetTableShards(server, sender, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(shards3.size(), 3u);

        // Check stats on the first table, it would have borrowed parts from two shards
        {
            auto stats = WaitTableStats(runtime, shards3.at(0));
            // Cerr << "Received shard stats:" << Endl << stats.DebugString();
            const auto& ownersProto = stats.GetUserTablePartOwners();
            THashSet<ui64> owners(ownersProto.begin(), ownersProto.end());
            // NOTE: datashard always adds current shard to part owners, even if there are no parts
            UNIT_ASSERT_VALUES_EQUAL(owners.size(), 3u);
            UNIT_ASSERT(owners.contains(shards1.at(0)));
            UNIT_ASSERT(owners.contains(shards2.at(0)));
            UNIT_ASSERT(owners.contains(shards3.at(0)));
        }

        CompactBorrowed(runtime, shards3.at(0), tableId);

        for (int i = 0; i < 5; ++i) {
            auto stats = WaitTableStats(runtime, shards3.at(0));
            // Cerr << "Received shard stats:" << Endl << stats.DebugString();
            const auto& ownersProto = stats.GetUserTablePartOwners();
            THashSet<ui64> owners(ownersProto.begin(), ownersProto.end());
            if (i < 4) {
                if (owners.size() > 1) {
                    continue;
                }
                if (owners.contains(shards1.at(0)) || owners.contains(shards2.at(0))) {
                    continue;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(owners.size(), 1u);
            UNIT_ASSERT(owners.contains(shards3.at(0)));
        }

        {
            auto result = ReadShardedTable(server, "/Root/table-1");
            UNIT_ASSERT_VALUES_EQUAL(result,
                "key = 1, value = 100\n"
                "key = 2, value = 200\n"
                "key = 3, value = 300\n"
                "key = 4, value = 400\n");
        }
    }
}

} // namespace NKikimr

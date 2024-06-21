#include "defs.h"
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include "datashard_ut_common_kqp.h"

#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>

#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <library/cpp/testing/unittest/registar.h>

template <>
inline void Out<NKikimrTxDataShard::TEvBuildIndexProgressResponse::EStatus>
    (IOutputStream& o, NKikimrTxDataShard::TEvBuildIndexProgressResponse::EStatus status)
{
    o << NKikimrTxDataShard::TEvBuildIndexProgressResponse::EStatus_Name(status);
}

namespace NKikimr {

using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;


Y_UNIT_TEST_SUITE(TTxDataShardBuildIndexScan) {

    static void DoBuildIndex(Tests::TServer::TPtr server, TActorId sender,
                             const TString& tableFrom, const TString& tableTo,
                             const TRowVersion& snapshot,
                             const NKikimrTxDataShard::TEvBuildIndexProgressResponse::EStatus& expected) {
        auto &runtime = *server->GetRuntime();
        TVector<ui64> datashards = GetTableShards(server, sender, tableFrom);
        TTableId tableId = ResolveTableId(server, sender, tableFrom);

        for (auto tid: datashards) {
            auto ev = new TEvDataShard::TEvBuildIndexCreateRequest;
            NKikimrTxDataShard::TEvBuildIndexCreateRequest& rec = ev->Record;
            rec.SetBuildIndexId(1);

            rec.SetTabletId(tid);
            rec.SetOwnerId(tableId.PathId.OwnerId);
            rec.SetPathId(tableId.PathId.LocalPathId);

            rec.SetTargetName(tableTo);
            rec.AddIndexColumns("value");
            rec.AddIndexColumns("key");

            rec.SetSnapshotTxId(snapshot.TxId);
            rec.SetSnapshotStep(snapshot.Step);

            runtime.SendToPipe(tid, sender, ev, 0, GetPipeConfigWithRetries());

            while (true) {
                TAutoPtr<IEventHandle> handle;
                auto reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvBuildIndexProgressResponse>(handle);

                if (expected == NKikimrTxDataShard::TEvBuildIndexProgressResponse::DONE
                    && reply->Record.GetStatus() == NKikimrTxDataShard::TEvBuildIndexProgressResponse::ACCEPTED) {
                    Cerr << "skip ACCEPTED" << Endl;
                    continue;
                }

                if (expected != NKikimrTxDataShard::TEvBuildIndexProgressResponse::INPROGRESS
                    && reply->Record.GetStatus() == NKikimrTxDataShard::TEvBuildIndexProgressResponse::INPROGRESS) {
                    Cerr << "skip INPROGRESS" << Endl;
                    continue;
                }

                NYql::TIssues issues;
                NYql::IssuesFromMessage(reply->Record.GetIssues(), issues);
                UNIT_ASSERT_VALUES_EQUAL_C(reply->Record.GetStatus(), expected, issues.ToString());
                break;
            }
        }
    }

    static void CreateShardedTableForIndex(
        Tests::TServer::TPtr server, TActorId sender,
        const TString &root, const TString &name,
        ui64 shards, bool enableOutOfOrder)
    {
        NLocalDb::TCompactionPolicyPtr policy = NLocalDb::CreateDefaultUserTablePolicy();
        policy->KeepEraseMarkers = true;

        auto opts = TShardedTableOptions()
                        .Shards(shards)
                        .EnableOutOfOrder(enableOutOfOrder)
                        .Policy(policy.Get())
                        .ShadowData(EShadowDataMode::Enabled)
                        .Columns({{"value", "Uint32", true, false}, {"key", "Uint32", true, false}});

        CreateShardedTable(server, sender, root, name, opts);
    }

    Y_UNIT_TEST(RunScan) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);

        // Allow manipulating shadow data using normal schemeshard operations
        runtime.GetAppData().AllowShadowDataInSchemeShardForTests = true;

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1, false);

        // Upsert some initial values
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100), (3, 300), (5, 500);");

        CreateShardedTableForIndex(server, sender, "/Root", "table-2", 1, false);

        auto snapshot = CreateVolatileSnapshot(server, { "/Root/table-1" });

        DoBuildIndex(server, sender, "/Root/table-1", "/Root/table-2", snapshot, NKikimrTxDataShard::TEvBuildIndexProgressResponse::DONE);

        // Writes to shadow data should not be visible yet
        auto data = ReadShardedTable(server, "/Root/table-2");
        UNIT_ASSERT_VALUES_EQUAL(data, "");

        // Alter table: disable shadow data and change compaction policy
        auto policy = NLocalDb::CreateDefaultUserTablePolicy();
        policy->KeepEraseMarkers = false;
        WaitTxNotification(server, AsyncAlterAndDisableShadow(server, "/Root", "table-2", policy.Get()));

        // Shadow data must be visible now
        auto data2 = ReadShardedTable(server, "/Root/table-2");
        UNIT_ASSERT_VALUES_EQUAL(data2,
                                 "value = 100, key = 1\n"
                                 "value = 300, key = 3\n"
                                 "value = 500, key = 5\n");
    }

    Y_UNIT_TEST(ShadowBorrowCompaction) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings
            .SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NLog::PRI_DEBUG);

        // Allow manipulating shadow data using normal schemeshard operations
        runtime.GetAppData().AllowShadowDataInSchemeShardForTests = true;

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1, false);

        // Upsert some initial values
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500);");

        CreateShardedTableForIndex(server, sender, "/Root", "table-2", 1, false);

        auto observer = runtime.AddObserver<TEvDataShard::TEvCompactBorrowed>([&](TEvDataShard::TEvCompactBorrowed::TPtr& event) {
            Cerr << "Captured TEvDataShard::TEvCompactBorrowed from " << runtime.FindActorName(event->Sender) << " to " << runtime.FindActorName(event->GetRecipientRewrite()) << Endl;
            if (runtime.FindActorName(event->Sender) == "FLAT_SCHEMESHARD_ACTOR") {
                event.Reset();
            }
        });

        auto snapshot = CreateVolatileSnapshot(server, { "/Root/table-1" });

        DoBuildIndex(server, sender, "/Root/table-1", "/Root/table-2", snapshot, NKikimrTxDataShard::TEvBuildIndexProgressResponse::DONE);

        // Writes to shadow data should not be visible yet
        auto data = ReadShardedTable(server, "/Root/table-2");
        UNIT_ASSERT_VALUES_EQUAL(data, "");

        // Split index
        auto shards1 = GetTableShards(server, sender, "/Root/table-2");
        UNIT_ASSERT_VALUES_EQUAL(shards1.size(), 1u);

        // Split would fail otherwise :(
        SetSplitMergePartCountLimit(server->GetRuntime(), -1);

        auto senderSplit = runtime.AllocateEdgeActor();
        ui64 txId = AsyncSplitTable(server, senderSplit, "/Root/table-2", shards1.at(0), 300);
        WaitTxNotification(server, senderSplit, txId);

        auto shards2 = GetTableShards(server, sender, "/Root/table-2");
        UNIT_ASSERT_VALUES_EQUAL(shards2.size(), 2u);

        for (auto shardIndex : xrange(2u)) {
            auto stats = WaitTableStats(runtime, shards2.at(shardIndex));
            // Cerr << "Received shard stats:" << Endl << stats.DebugString() << Endl;

            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetRowCount(), shardIndex == 0 ? 2 : 3);

            THashSet<ui64> owners(stats.GetUserTablePartOwners().begin(), stats.GetUserTablePartOwners().end());
            // Note: datashard always adds current shard to part owners, even if there are no parts
            UNIT_ASSERT_VALUES_EQUAL(owners, (THashSet<ui64>{shards1.at(0), shards2.at(shardIndex)}));
            
            auto tableId = ResolveTableId(server, sender, "/Root/table-2");
            auto result = CompactBorrowed(runtime, shards2.at(shardIndex), tableId);
            // Cerr << "Compact result " << result.DebugString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(result.GetTabletId(), shards2.at(shardIndex));
            UNIT_ASSERT_VALUES_EQUAL(result.GetPathId().GetOwnerId(), tableId.PathId.OwnerId);
            UNIT_ASSERT_VALUES_EQUAL(result.GetPathId().GetLocalId(), tableId.PathId.LocalPathId);

            for (int i = 0; i < 5 && (owners.size() > 1 || owners.contains(shards1.at(0))); ++i) {
                auto stats = WaitTableStats(runtime, shards2.at(shardIndex));
                owners = THashSet<ui64>(stats.GetUserTablePartOwners().begin(), stats.GetUserTablePartOwners().end());
            }

            UNIT_ASSERT_VALUES_EQUAL(owners, (THashSet<ui64>{shards2.at(shardIndex)}));
        }

        // Alter table: disable shadow data and change compaction policy
        auto policy = NLocalDb::CreateDefaultUserTablePolicy();
        policy->KeepEraseMarkers = false;
        WaitTxNotification(server, AsyncAlterAndDisableShadow(server, "/Root", "table-2", policy.Get()));

        // Shadow data must be visible now
        auto data2 = ReadShardedTable(server, "/Root/table-2");
        UNIT_ASSERT_VALUES_EQUAL(data2,
                                 "value = 100, key = 1\n"
                                 "value = 200, key = 2\n"
                                 "value = 300, key = 3\n"
                                 "value = 400, key = 4\n"
                                 "value = 500, key = 5\n");
    }
}

} // namespace NKikimr

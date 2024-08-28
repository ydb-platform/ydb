#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include "datashard_ut_common_kqp.h"
#include "datashard_active_transaction.h"

#include <ydb/core/formats/factory.h>
#include <ydb/core/tx/long_tx_service/public/lock_handle.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>
#include <ydb/core/testlib/actors/block_events.h>

#include <ydb/core/kqp/ut/common/kqp_ut_common.h> // Y_UNIT_TEST_(TWIN|QUAD)
#include <ydb/core/mind/local.h>

#include <ydb/library/yql/minikql/mkql_node_printer.h>

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;

namespace {

    class TTextFormatBuilder : public IBlockBuilder {
    public:
        bool Start(
                const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns,
                ui64 maxRowsInBlock,
                ui64 maxBytesInBlock,
                TString& err) override
        {
            Y_UNUSED(maxRowsInBlock);
            Y_UNUSED(maxBytesInBlock);
            Y_UNUSED(err);
            Columns = columns;
            Buffer.clear();
            return true;
        }

        void AddRow(const NKikimr::TDbTupleRef& key, const NKikimr::TDbTupleRef& value) override {
            Y_UNUSED(key);

            for (size_t index = 0; index < value.ColumnCount; ++index) {
                if (index != 0) {
                    Buffer.append(", ");
                }
                Buffer.append(Columns[index].first);
                Buffer.append(" = ");
                DbgPrintValue(Buffer, value.Columns[index], value.Types[index]);
            }
            Buffer.append('\n');
        }

        TString Finish() override {
            // FIXME: cannot move data out, interface is weird :-/
            return Buffer;
        }

        size_t Bytes() const override {
            return Buffer.size();
        }

    private:
        std::unique_ptr<IBlockBuilder> Clone() const override {
            return std::make_unique<TTextFormatBuilder>();
        }

    private:
        std::vector<std::pair<TString, NScheme::TTypeInfo>> Columns;
        TString Buffer;
    };

    void RegisterFormats(TServerSettings& settings) {
        if (!settings.Formats) {
            settings.Formats = new TFormatFactory;
        }

        settings.Formats->RegisterBlockBuilder(std::make_unique<TTextFormatBuilder>(), "debug_text");
    }

    TString DoReadColumns(
            TServer::TPtr server,
            const TString& path,
            const TRowVersion& snapshot = TRowVersion::Max())
    {
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        const auto shards = GetTableShards(server, sender, path);
        const auto tableId = ResolveTableId(server, sender, path);

        TString result;

        for (ui64 shardId : shards) {
            TString fromKey;
            bool fromKeyInclusive = true;
            for (;;) {
                auto req = MakeHolder<TEvDataShard::TEvReadColumnsRequest>();
                req->Record.SetTableId(tableId.PathId.LocalPathId);
                if (!snapshot.IsMax()) {
                    req->Record.SetSnapshotStep(snapshot.Step);
                    req->Record.SetSnapshotTxId(snapshot.TxId);
                }
                req->Record.SetFromKey(fromKey);
                req->Record.SetFromKeyInclusive(fromKeyInclusive);
                req->Record.AddColumns("key");
                req->Record.AddColumns("value");
                req->Record.SetFormat("debug_text");
                runtime.SendToPipe(shardId, sender, req.Release());

                auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvReadColumnsResponse>(sender);
                auto* msg = ev->Get();
                //Cerr << msg->Record.DebugString() << Endl;
                UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetStatus(), 0u);
                result.append(msg->Record.GetBlocks());

                if (msg->Record.GetEndOfShard()) {
                    break; // go to the next shard
                }

                if (msg->Record.GetLastKey()) {
                    fromKey = msg->Record.GetLastKey();
                    fromKeyInclusive = !msg->Record.GetLastKeyInclusive();
                }
            }
        }

        return result;
    }

    ui64 GetSnapshotCount(TTestActorRuntime& runtime, ui64 shard) {
        auto sender = runtime.AllocateEdgeActor();
        auto request = MakeHolder<TEvTablet::TEvLocalMKQL>();
        TString miniKQL = R"___((
            (let range '('IncFrom
                '('Oid (Uint64 '0) (Void))
                '('Tid (Uint64 '0) (Void))
                '('Step (Uint64 '0) (Void))
                '('TxId (Uint64 '0) (Void))))
            (let select '('Oid 'Tid 'Step 'TxId))
            (let options '())
            (let pgmReturn (AsList
                (SetResult 'myRes (Length (Member (SelectRange 'Snapshots range select options) 'List)))))
            (return pgmReturn)
        ))___";

        request->Record.MutableProgram()->MutableProgram()->SetText(miniKQL);
        runtime.SendToPipe(shard, sender, request.Release(), 0, GetPipeConfigWithRetries());

        auto ev = runtime.GrabEdgeEventRethrow<TEvTablet::TEvLocalMKQLResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetStatus(), 0);
        return ev->Get()->Record
                .GetExecutionEngineEvaluatedResponse()
                .GetValue()
                .GetStruct(0)
                .GetOptional()
                .GetUint64();
    }

} // namespace

Y_UNIT_TEST_SUITE(DataShardSnapshots) {

    Y_UNIT_TEST(VolatileSnapshotSplit) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);
        RegisterFormats(serverSettings);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1), (2, 2), (3, 3);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 10), (20, 20), (30, 30);");

        auto snapshot = CreateVolatileSnapshot(server, { "/Root/table-1", "/Root/table-2" });

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 11), (2, 22), (3, 33), (4, 44);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 11), (20, 22), (30, 33), (40, 44);");

        auto table1snapshot = DoReadColumns(server, "/Root/table-1", snapshot);
        UNIT_ASSERT_VALUES_EQUAL(table1snapshot, "key = 1, value = 1\nkey = 2, value = 2\nkey = 3, value = 3\n");
        auto table2snapshot = DoReadColumns(server, "/Root/table-2", snapshot);
        UNIT_ASSERT_VALUES_EQUAL(table2snapshot, "key = 10, value = 10\nkey = 20, value = 20\nkey = 30, value = 30\n");

        auto table1head = DoReadColumns(server, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(table1head, "key = 1, value = 11\nkey = 2, value = 22\nkey = 3, value = 33\nkey = 4, value = 44\n");
        auto table2head = DoReadColumns(server, "/Root/table-2");
        UNIT_ASSERT_VALUES_EQUAL(table2head, "key = 10, value = 11\nkey = 20, value = 22\nkey = 30, value = 33\nkey = 40, value = 44\n");

        // Split/merge would fail otherwise :(
        SetSplitMergePartCountLimit(server->GetRuntime(), -1);

        // Split table in two shards
        {
            //Cerr << "----Split Begin----" << Endl;
            auto senderSplit = runtime.AllocateEdgeActor();
            auto tablets = GetTableShards(server, senderSplit, "/Root/table-1");
            ui64 txId = AsyncSplitTable(server, senderSplit, "/Root/table-1", tablets.at(0), 3);
            WaitTxNotification(server, senderSplit, txId);
            //Cerr << "----Split End----" << Endl;
        }

        auto table1snapshotAfterSplit = DoReadColumns(server, "/Root/table-1", snapshot);
        UNIT_ASSERT_VALUES_EQUAL(table1snapshotAfterSplit, "key = 1, value = 1\nkey = 2, value = 2\nkey = 3, value = 3\n");
        auto table1headAfterSplit = DoReadColumns(server, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(table1headAfterSplit, "key = 1, value = 11\nkey = 2, value = 22\nkey = 3, value = 33\nkey = 4, value = 44\n");
    }

    Y_UNIT_TEST(VolatileSnapshotMerge) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);
        RegisterFormats(serverSettings);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        // Split/merge would fail otherwise :(
        SetSplitMergePartCountLimit(server->GetRuntime(), -1);

        // Split table in two shards (before it has any data)
        {
            //Cerr << "----Split Begin----" << Endl;
            auto senderSplit = runtime.AllocateEdgeActor();
            auto tablets = GetTableShards(server, senderSplit, "/Root/table-1");
            ui64 txId = AsyncSplitTable(server, senderSplit, "/Root/table-1", tablets.at(0), 3);
            WaitTxNotification(server, senderSplit, txId);
            //Cerr << "----Split End----" << Endl;
        }

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1), (2, 2), (3, 3);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 10), (20, 20), (30, 30);");

        auto snapshot = CreateVolatileSnapshot(server, { "/Root/table-1", "/Root/table-2" });

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 11), (2, 22), (3, 33), (4, 44);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 11), (20, 22), (30, 33), (40, 44);");

        auto table1snapshot = DoReadColumns(server, "/Root/table-1", snapshot);
        UNIT_ASSERT_VALUES_EQUAL(table1snapshot, "key = 1, value = 1\nkey = 2, value = 2\nkey = 3, value = 3\n");
        auto table2snapshot = DoReadColumns(server, "/Root/table-2", snapshot);
        UNIT_ASSERT_VALUES_EQUAL(table2snapshot, "key = 10, value = 10\nkey = 20, value = 20\nkey = 30, value = 30\n");

        auto table1head = DoReadColumns(server, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(table1head, "key = 1, value = 11\nkey = 2, value = 22\nkey = 3, value = 33\nkey = 4, value = 44\n");
        auto table2head = DoReadColumns(server, "/Root/table-2");
        UNIT_ASSERT_VALUES_EQUAL(table2head, "key = 10, value = 11\nkey = 20, value = 22\nkey = 30, value = 33\nkey = 40, value = 44\n");

        // Merge table back into a single shard
        {
            //Cerr << "----Merge Begin----" << Endl;
            auto senderMerge = runtime.AllocateEdgeActor();
            auto tablets = GetTableShards(server, senderMerge, "/Root/table-1");
            ui64 txId = AsyncMergeTable(server, senderMerge, "/Root/table-1", tablets);
            WaitTxNotification(server, senderMerge, txId);
            //Cerr << "----Merge End----" << Endl;
        }

        auto table1headAfterMerge = DoReadColumns(server, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(table1headAfterMerge, "key = 1, value = 11\nkey = 2, value = 22\nkey = 3, value = 33\nkey = 4, value = 44\n");
        auto table1snapshotAfterMerge = DoReadColumns(server, "/Root/table-1", snapshot);
        UNIT_ASSERT_VALUES_EQUAL(table1snapshotAfterMerge, "key = 1, value = 1\nkey = 2, value = 2\nkey = 3, value = 3\n");
    }

    Y_UNIT_TEST(VolatileSnapshotAndLocalMKQLUpdate) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);
        RegisterFormats(serverSettings);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1), (2, 2), (3, 3);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 10), (20, 20), (30, 30);");

        auto snapshot = CreateVolatileSnapshot(server, { "/Root/table-1", "/Root/table-2" });

        // Update user table using a local minikql tx
        {
            unsigned newKey = 2;
            unsigned newValue = 42;
            auto programText = Sprintf(R"((
                (let row1_ '('('key (Uint32 '%u))))
                (let upd1_ '('('value (Uint32 '%u))))
                (let ret_ (AsList
                    (UpdateRow '__user__table-1 row1_ upd1_)
                ))
                (return ret_)
            ))", newKey, newValue);

            auto shards = GetTableShards(server, sender, "/Root/table-1");

            auto request = MakeHolder<TEvTablet::TEvLocalMKQL>();
            request->Record.MutableProgram()->MutableProgram()->SetText(programText);
            runtime.SendToPipe(shards.at(0), sender, request.Release(), 0, GetPipeConfigWithRetries());

            auto ev = runtime.GrabEdgeEvent<TEvTablet::TEvLocalMKQLResponse>(sender);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetStatus(), 0);
        }

        // Snapshot must not be damaged
        auto table1snapshot = DoReadColumns(server, "/Root/table-1", snapshot);
        UNIT_ASSERT_VALUES_EQUAL(table1snapshot,
            "key = 1, value = 1\n"
            "key = 2, value = 2\n"
            "key = 3, value = 3\n");

        // New row must be visible in non-snapshot reads
        auto table1head = DoReadColumns(server, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(table1head,
            "key = 1, value = 1\n"
            "key = 2, value = 42\n"
            "key = 3, value = 3\n");
    }

    Y_UNIT_TEST(VolatileSnapshotReadTable) {
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

        CreateShardedTable(server, sender, "/Root", "table-1", 2);
        CreateShardedTable(server, sender, "/Root", "table-2", 2);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1), (2, 2), (3, 3);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 10), (20, 20), (30, 30);");

        auto snapshot = CreateVolatileSnapshot(server, { "/Root/table-1", "/Root/table-2" });

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 11), (2, 22), (3, 33), (4, 44);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 11), (20, 22), (30, 33), (40, 44);");

        auto table1head = ReadShardedTable(server, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(table1head,
            "key = 1, value = 11\n"
            "key = 2, value = 22\n"
            "key = 3, value = 33\n"
            "key = 4, value = 44\n");

        auto table2head = ReadShardedTable(server, "/Root/table-2");
        UNIT_ASSERT_VALUES_EQUAL(table2head,
            "key = 10, value = 11\n"
            "key = 20, value = 22\n"
            "key = 30, value = 33\n"
            "key = 40, value = 44\n");

        auto table1snapshot = ReadShardedTable(server, "/Root/table-1", snapshot);
        UNIT_ASSERT_VALUES_EQUAL(table1snapshot,
            "key = 1, value = 1\n"
            "key = 2, value = 2\n"
            "key = 3, value = 3\n");

        auto table2snapshot = ReadShardedTable(server, "/Root/table-2", snapshot);
        UNIT_ASSERT_VALUES_EQUAL(table2snapshot,
            "key = 10, value = 10\n"
            "key = 20, value = 20\n"
            "key = 30, value = 30\n");

        auto table1badsnapshot = ReadShardedTable(server, "/Root/table-1", snapshot.Prev());
        UNIT_ASSERT_VALUES_EQUAL(table1badsnapshot, "ERROR: WrongRequest\n");

        auto table1snapshotagain = ReadShardedTable(server, "/Root/table-1", snapshot);
        UNIT_ASSERT_VALUES_EQUAL(table1snapshot,
            "key = 1, value = 1\n"
            "key = 2, value = 2\n"
            "key = 3, value = 3\n");

        {
            auto senderDiscard = runtime.AllocateEdgeActor();
            auto tablets = GetTableShards(server, senderDiscard, "/Root/table-1");
            const auto tableId = ResolveTableId(server, senderDiscard, "/Root/table-1");
            for (ui64 shardId : tablets) {
                auto req = MakeHolder<TEvDataShard::TEvDiscardVolatileSnapshotRequest>();
                req->Record.SetOwnerId(tableId.PathId.OwnerId);
                req->Record.SetPathId(tableId.PathId.LocalPathId);
                req->Record.SetStep(snapshot.Step);
                req->Record.SetTxId(snapshot.TxId);
                runtime.SendToPipe(shardId, senderDiscard, req.Release());

                using TResponse = NKikimrTxDataShard::TEvDiscardVolatileSnapshotResponse;

                auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvDiscardVolatileSnapshotResponse>(senderDiscard);
                UNIT_ASSERT_C(ev->Get()->Record.GetStatus() == TResponse::DISCARDED,
                    "Unexpected status " << TResponse::EStatus_Name(ev->Get()->Record.GetStatus()));
            }
        }

        auto table1snapshotdiscarded = ReadShardedTable(server, "/Root/table-1", snapshot);
        UNIT_ASSERT_VALUES_EQUAL(table1snapshotdiscarded, "ERROR: WrongRequest\n");
    }

    Y_UNIT_TEST(VolatileSnapshotRefreshDiscard) {
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

        CreateShardedTable(server, sender, "/Root", "table-1", 2);
        CreateShardedTable(server, sender, "/Root", "table-2", 2);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1), (2, 2), (3, 3);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 10), (20, 20), (30, 30);");

        auto snapshot = CreateVolatileSnapshot(server, { "/Root/table-1", "/Root/table-2" });

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 11), (2, 22), (3, 33), (4, 44);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 11), (20, 22), (30, 33), (40, 44);");

        auto table1snapshot1 = ReadShardedTable(server, "/Root/table-1", snapshot);
        UNIT_ASSERT_VALUES_EQUAL(table1snapshot1,
            "key = 1, value = 1\n"
            "key = 2, value = 2\n"
            "key = 3, value = 3\n");

        UNIT_ASSERT(RefreshVolatileSnapshot(server, { "/Root/table-1", "/Root/table-2" }, snapshot));

        auto table1snapshot2 = ReadShardedTable(server, "/Root/table-1", snapshot);
        UNIT_ASSERT_VALUES_EQUAL(table1snapshot2,
            "key = 1, value = 1\n"
            "key = 2, value = 2\n"
            "key = 3, value = 3\n");

        UNIT_ASSERT(DiscardVolatileSnapshot(server, { "/Root/table-1", "/Root/table-2" }, snapshot));

        auto table1snapshot3 = ReadShardedTable(server, "/Root/table-1", snapshot);
        UNIT_ASSERT_VALUES_EQUAL(table1snapshot3, "ERROR: WrongRequest\n");

        UNIT_ASSERT(!RefreshVolatileSnapshot(server, { "/Root/table-1", "/Root/table-2" }, snapshot));
        UNIT_ASSERT(!DiscardVolatileSnapshot(server, { "/Root/table-1", "/Root/table-2" }, snapshot));
    }

    Y_UNIT_TEST(VolatileSnapshotTimeout) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(1000);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 2);
        CreateShardedTable(server, sender, "/Root", "table-2", 2);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1), (2, 2), (3, 3);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 10), (20, 20), (30, 30);");

        auto snapshot = CreateVolatileSnapshot(server, { "/Root/table-1", "/Root/table-2" }, TDuration::MilliSeconds(10000));

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 11), (2, 22), (3, 33), (4, 44);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 11), (20, 22), (30, 33), (40, 44);");

        auto table1snapshot1 = ReadShardedTable(server, "/Root/table-1", snapshot);
        UNIT_ASSERT_VALUES_EQUAL(table1snapshot1,
            "key = 1, value = 1\n"
            "key = 2, value = 2\n"
            "key = 3, value = 3\n");

        Cerr << "---- Sleeping ----" << Endl;
        SimulateSleep(server, TDuration::Seconds(60));

        auto table1snapshot2 = ReadShardedTable(server, "/Root/table-1", snapshot);
        UNIT_ASSERT_VALUES_EQUAL(table1snapshot2,
            "ERROR: WrongRequest\n");
    }

    Y_UNIT_TEST(VolatileSnapshotTimeoutRefresh) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(1000);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 2);
        CreateShardedTable(server, sender, "/Root", "table-2", 2);

        const auto shards1 = GetTableShards(server, sender, "/Root/table-1");

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1), (2, 2), (3, 3);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 10), (20, 20), (30, 30);");

        auto snapshot1 = CreateVolatileSnapshot(server, { "/Root/table-1", "/Root/table-2" }, TDuration::MilliSeconds(10000));

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 11), (2, 22), (3, 33), (4, 44);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 11), (20, 22), (30, 33), (40, 44);");

        auto snapshot2 = CreateVolatileSnapshot(server, { "/Root/table-1", "/Root/table-2" }, TDuration::MilliSeconds(10000));

        // Snapshots table must have 2 rows
        UNIT_ASSERT_VALUES_EQUAL(GetSnapshotCount(runtime, shards1[0]), 2u);

        auto table1snapshot1 = ReadShardedTable(server, "/Root/table-1", snapshot1);
        UNIT_ASSERT_VALUES_EQUAL(table1snapshot1,
            "key = 1, value = 1\n"
            "key = 2, value = 2\n"
            "key = 3, value = 3\n");

        auto table1snapshot2 = ReadShardedTable(server, "/Root/table-1", snapshot2);
        UNIT_ASSERT_VALUES_EQUAL(table1snapshot2,
            "key = 1, value = 11\n"
            "key = 2, value = 22\n"
            "key = 3, value = 33\n"
            "key = 4, value = 44\n");

        // Refresh snapshot1 every 5 seconds for 60 seconds
        for (size_t i = 0; i < 12; ++i) {
            Cerr << "---- Refresh attempt: " << (i + 1) << " ----" << Endl;
            UNIT_ASSERT(RefreshVolatileSnapshot(server, { "/Root/table-1", "/Root/table-2" }, snapshot1));
            SimulateSleep(server, TDuration::Seconds(5));
        }

        // Test snapshot1 still works correctly
        auto table1snapshot1again = ReadShardedTable(server, "/Root/table-1", snapshot1);
        UNIT_ASSERT_VALUES_EQUAL(table1snapshot1again,
            "key = 1, value = 1\n"
            "key = 2, value = 2\n"
            "key = 3, value = 3\n");

        // Test snapshot2 no longer works
        auto table1snapshot2again = ReadShardedTable(server, "/Root/table-1", snapshot2);
        UNIT_ASSERT_VALUES_EQUAL(table1snapshot2again,
            "ERROR: WrongRequest\n");

        // Snapshots table must have 1 row
        UNIT_ASSERT_VALUES_EQUAL(GetSnapshotCount(runtime, shards1[0]), 1u);
    }

    Y_UNIT_TEST(VolatileSnapshotCleanupOnReboot) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(1000);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        const auto shards1 = GetTableShards(server, sender, "/Root/table-1");

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1), (2, 2), (3, 3);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 10), (20, 20), (30, 30);");

        auto snapshot = CreateVolatileSnapshot(server, { "/Root/table-1", "/Root/table-2" }, TDuration::MilliSeconds(10000));

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 11), (2, 22), (3, 33), (4, 44);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 11), (20, 22), (30, 33), (40, 44);");

        // Snapshots table must have 1 row
        UNIT_ASSERT_VALUES_EQUAL(GetSnapshotCount(runtime, shards1[0]), 1u);

        auto table1snapshot1 = ReadShardedTable(server, "/Root/table-1", snapshot);
        UNIT_ASSERT_VALUES_EQUAL(table1snapshot1,
            "key = 1, value = 1\n"
            "key = 2, value = 2\n"
            "key = 3, value = 3\n");

        // Start ReadTable using snapshot and pause on quota requests
        StartReadShardedTable(server, "/Root/table-1", snapshot);

        auto table1snapshot2 = ReadShardedTable(server, "/Root/table-1", snapshot);
        UNIT_ASSERT_VALUES_EQUAL(table1snapshot2,
            "key = 1, value = 1\n"
            "key = 2, value = 2\n"
            "key = 3, value = 3\n");

        UNIT_ASSERT(DiscardVolatileSnapshot(server, { "/Root/table-1", "/Root/table-2" }, snapshot));

        auto table1snapshot3 = ReadShardedTable(server, "/Root/table-1", snapshot);
        UNIT_ASSERT_VALUES_EQUAL(table1snapshot3,
            "ERROR: WrongRequest\n");

        // Snapshots table must still have snapshot (used by paused ReadTable)
        UNIT_ASSERT_VALUES_EQUAL(GetSnapshotCount(runtime, shards1[0]), 1u);

        RebootTablet(runtime, shards1[0], sender);

        // Snapshots table should be cleaned up on reboot
        UNIT_ASSERT_VALUES_EQUAL(GetSnapshotCount(runtime, shards1[0]), 0u);
    }

    Y_UNIT_TEST(VolatileSnapshotCleanupOnFinish) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(1000);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        const auto shards1 = GetTableShards(server, sender, "/Root/table-1");

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1), (2, 2), (3, 3);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 10), (20, 20), (30, 30);");

        auto snapshot = CreateVolatileSnapshot(server, { "/Root/table-1", "/Root/table-2" }, TDuration::MilliSeconds(10000));

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 11), (2, 22), (3, 33), (4, 44);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 11), (20, 22), (30, 33), (40, 44);");

        // Snapshots table must have 1 row
        UNIT_ASSERT_VALUES_EQUAL(GetSnapshotCount(runtime, shards1[0]), 1u);

        auto table1snapshot1 = ReadShardedTable(server, "/Root/table-1", snapshot);
        UNIT_ASSERT_VALUES_EQUAL(table1snapshot1,
            "key = 1, value = 1\n"
            "key = 2, value = 2\n"
            "key = 3, value = 3\n");

        // Start ReadTable using snapshot and pause on quota requests
        auto state = StartReadShardedTable(server, "/Root/table-1", snapshot);

        UNIT_ASSERT(DiscardVolatileSnapshot(server, { "/Root/table-1", "/Root/table-2" }, snapshot));

        auto table1snapshot2 = ReadShardedTable(server, "/Root/table-1", snapshot);
        UNIT_ASSERT_VALUES_EQUAL(table1snapshot2,
            "ERROR: WrongRequest\n");

        // Snapshots table must still have snapshot (used by paused ReadTable)
        UNIT_ASSERT_VALUES_EQUAL(GetSnapshotCount(runtime, shards1[0]), 1u);

        // Resume paused ReadTable and check the result
        ResumeReadShardedTable(server, state);
        UNIT_ASSERT_VALUES_EQUAL(state.Result,
            "key = 1, value = 1\n"
            "key = 2, value = 2\n"
            "key = 3, value = 3\n");

        // Snapshots table should be cleaned up after ReadTable has finished
        UNIT_ASSERT_VALUES_EQUAL(GetSnapshotCount(runtime, shards1[0]), 0u);
    }

    Y_UNIT_TEST(MvccSnapshotTailCleanup) {
        TPortManager pm;
        TServerSettings::TControls controls;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetKeepSnapshotTimeout(TDuration::Seconds(2))
            .SetControls(controls);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;
        CreateShardedTable(server, sender, "/Root", "table-1", 1);

        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));

        SimulateSleep(server, TDuration::Seconds(1));

        auto beginSnapshotRequest = [&](TString& sessionId, TString& txId, const TString& query) -> TString {
            return KqpSimpleBegin(runtime, sessionId, txId, query);
        };

        auto continueSnapshotRequest = [&](const TString& sessionId, const TString& txId, const TString& query) -> TString {
            return KqpSimpleContinue(runtime, sessionId, txId, query);
        };

        auto execSnapshotRequest = [&](const TString& query) -> TString {
            TString sessionId, txId;
            TString result = beginSnapshotRequest(sessionId, txId, query);
            CloseSession(runtime, sessionId);
            return result;
        };

        // Start with a snapshot read that persists necessary flags and advances edges for the first time
        UNIT_ASSERT_VALUES_EQUAL(
            execSnapshotRequest(Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");
        SimulateSleep(runtime, TDuration::Seconds(2));

        // Create a new snapshot, it should still observe the same state
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            beginSnapshotRequest(sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

        // Insert a new row and wait for result, this will roll over into a new step
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 2)"));

        bool failed = false;
        for (int i = 0; i < 5; ++i) {
            // Idle cleanup is roughly every 15 seconds
            runtime.SimulateSleep(TDuration::Seconds(15));
            auto result = continueSnapshotRequest(sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                ORDER BY key
                )"));
            if (result.StartsWith("ERROR:")) {
                Cerr << "... got expected failure: " << result << Endl;
                failed = true;
                break;
            }
            UNIT_ASSERT_VALUES_EQUAL(
                result,
                "{ items { uint32_value: 1 } items { uint32_value: 1 } }");
        }

        UNIT_ASSERT_C(failed, "Snapshot was not cleaned up");
    }

    Y_UNIT_TEST(MvccSnapshotAndSplit) {
        TPortManager pm;
        TServerSettings::TControls controls;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetControls(controls);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        // Don't allow granular timecast side-stepping mediator time hacks in this test
        TBlockEvents<TEvMediatorTimecast::TEvGranularUpdate> blockGranularUpdate(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;
        CreateShardedTable(server, sender, "/Root", "table-1", 1);

        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));

        SimulateSleep(server, TDuration::Seconds(1));

        auto execSimpleRequest = [&](const TString& query) -> TString {
            return KqpSimpleExec(runtime, query);
        };

        auto beginSnapshotRequest = [&](TString& sessionId, TString& txId, const TString& query) -> TString {
            return KqpSimpleBegin(runtime, sessionId, txId, query);
        };

        auto continueSnapshotRequest = [&](const TString& sessionId, const TString& txId, const TString& query) -> TString {
            return KqpSimpleContinue(runtime, sessionId, txId, query);
        };

        auto execSnapshotRequest = [&](const TString& query) -> TString {
            TString sessionId, txId;
            TString result = beginSnapshotRequest(sessionId, txId, query);
            CloseSession(runtime, sessionId);
            return result;
        };

        auto waitFor = [&](const auto& condition, const TString& description) {
            if (!condition()) {
                Cerr << "... waiting for " << description << Endl;
                TDispatchOptions options;
                options.CustomFinalCondition = [&]() {
                    return condition();
                };
                runtime.DispatchEvents(options);
                UNIT_ASSERT_C(condition(), "... failed to wait for " << description);
            }
        };

        // Start with a snapshot read that persists necessary flags and advances edges for the first time
        UNIT_ASSERT_VALUES_EQUAL(
            execSnapshotRequest(Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");
        SimulateSleep(runtime, TDuration::Seconds(2));

        bool captureSplit = true;
        bool captureTimecast = false;
        TVector<THolder<IEventHandle>> capturedSplit;
        TVector<THolder<IEventHandle>> capturedTimecast;
        auto captureEvents = [&](TAutoPtr<IEventHandle> &ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case TEvDataShard::TEvSplit::EventType: {
                    if (captureSplit) {
                        Cerr << "... captured TEvSplit" << Endl;
                        capturedSplit.emplace_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                }
                case TEvMediatorTimecast::EvUpdate: {
                    auto update = ev->Get<TEvMediatorTimecast::TEvUpdate>();
                    auto lastStep = update->Record.GetTimeBarrier();
                    if (captureTimecast) {
                        Cerr << "... captured TEvUpdate with step " << lastStep << Endl;
                        capturedTimecast.emplace_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    } else {
                        Cerr << "... observed TEvUpdate with step " << lastStep << Endl;
                    }
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserverFunc = runtime.SetObserverFunc(captureEvents);

        // Split would fail otherwise :(
        SetSplitMergePartCountLimit(server->GetRuntime(), -1);

        // Start splitting table into two shards
        auto senderSplit = runtime.AllocateEdgeActor();
        auto tablets = GetTableShards(server, senderSplit, "/Root/table-1");
        auto splitTxId = AsyncSplitTable(server, senderSplit, "/Root/table-1", tablets.at(0), 4);

        // Wait until schemeshard wants to split the source shard
        waitFor([&]{ return capturedSplit.size() > 0; }, "captured split");

        // Create a new snapshot and verify initial state
        // This snapshot must be lightweight and must not advance any edges
        TString sessionId, txId;
        TString senderImmediateWriteSessionId = CreateSessionRPC(runtime);
        TString senderImmediateWriteTxId;
        UNIT_ASSERT_VALUES_EQUAL(
            beginSnapshotRequest(sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

        // Finish the split
        captureSplit = false;
        captureTimecast = true;
        for (auto& ev : capturedSplit) {
            runtime.Send(ev.Release(), 0, true);
        }
        WaitTxNotification(server, senderSplit, splitTxId);

        // Send an immediate write after the finished split
        // In a buggy case it starts executing despite a blocked timecast
        auto f = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 2)
            )"), senderImmediateWriteSessionId, senderImmediateWriteTxId, true));

        // We sleep a little so datashard commits changes in buggy case
        SimulateSleep(runtime, TDuration::MicroSeconds(1));

        // Unblock timecast, so datashard time can finally catch up
        captureTimecast = false;

        // Wait for the commit result
        {
            auto response = AwaitResponse(runtime, f);
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
        }

        // Snapshot must not have been damaged by the write above
        UNIT_ASSERT_VALUES_EQUAL(
            continueSnapshotRequest(sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

        // But new immediate read must observe all writes we have performed
        UNIT_ASSERT_VALUES_EQUAL(
            execSimpleRequest(Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key in (1, 2, 3)
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } }");
    }

    Y_UNIT_TEST(MvccSnapshotReadWithLongPlanQueue) {
        TPortManager pm;
        TServerSettings::TControls controls;

        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetControls(controls);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;
        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (1, 1)"));

        SimulateSleep(server, TDuration::Seconds(1));

        auto beginSnapshotRequest = [&](TString& sessionId, TString& txId, const TString& query) -> TString {
            return KqpSimpleBegin(runtime, sessionId, txId, query);
        };

        auto continueSnapshotRequest = [&](const TString& sessionId, const TString& txId, const TString& query) -> TString {
            return KqpSimpleContinue(runtime, sessionId, txId, query);
        };

        // Prime table1 with a snapshot read
        {
            TString sessionId, txId;
            UNIT_ASSERT_VALUES_EQUAL(
                beginSnapshotRequest(sessionId, txId, Q_(R"(
                    SELECT key, value FROM `/Root/table-1`
                    WHERE key in (1, 2, 3)
                    ORDER BY key
                    )")),
                "{ items { uint32_value: 1 } items { uint32_value: 1 } }");
        }

        // Arrange for a distributed tx stuck at readset exchange
        TString sessionIdBlocker = CreateSessionRPC(runtime);
        TString txIdBlocker;
        {
            auto result = KqpSimpleBegin(runtime, sessionIdBlocker, txIdBlocker, Q_(R"(
                SELECT * FROM `/Root/table-1`
                UNION ALL
                SELECT * FROM `/Root/table-2`)"));
            UNIT_ASSERT_VALUES_EQUAL(
                result,
                "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
                "{ items { uint32_value: 1 } items { uint32_value: 1 } }");
        }

        auto waitFor = [&](const auto& condition, const TString& description) {
            if (!condition()) {
                Cerr << "... waiting for " << description << Endl;
                TDispatchOptions options;
                options.CustomFinalCondition = [&]() {
                    return condition();
                };
                runtime.DispatchEvents(options);
                UNIT_ASSERT_C(condition(), "... failed to wait for " << description);
            }
        };

        // Observe all received plan step messages
        size_t observedPlanSteps = 0;
        size_t observedPlanStepTxs = 0;
        // Capture and block all readset messages
        TVector<THolder<IEventHandle>> readSets;
        auto captureRS = [&](TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case TEvTxProcessing::TEvPlanStep::EventType: {
                    const auto* msg = ev->Get<TEvTxProcessing::TEvPlanStep>();
                    Cerr << "... observed TEvPlanStep" << Endl;
                    observedPlanSteps++;
                    observedPlanStepTxs += msg->Record.TransactionsSize();
                    break;
                }
                case TEvTxProcessing::TEvReadSet::EventType: {
                    Cerr << "... captured TEvReadSet" << Endl;
                    readSets.push_back(THolder(ev.Release()));
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserverFunc = runtime.SetObserverFunc(captureRS);

        // Send a commit request, it would block on readset exchange
        SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (99, 99);
            UPSERT INTO `/Root/table-2` (key, value) VALUES (99, 99); )"), sessionIdBlocker, txIdBlocker, true));

        waitFor([&] { return readSets.size() >= 2; }, "2 blocked readsets");

        UNIT_ASSERT_VALUES_EQUAL(observedPlanStepTxs, 2);

        // Sanity check: non-conflicting immediate write will succeed
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 2)"));

        // Start creating many persistent snapshots, overflowing plan queue (20 x 1000ms = 20 seconds)
        observedPlanSteps = 0;
        observedPlanStepTxs = 0;
        TActorId senderCreateSnapshot = runtime.AllocateEdgeActor();
        for (int i = 0; i < 10; ++i) {
            SimulateSleep(runtime, TDuration::MilliSeconds(1000));
            SendCreateVolatileSnapshot(runtime, senderCreateSnapshot, { "/Root/table-1" }, TDuration::Minutes(10));
        }

        waitFor([&] { return observedPlanStepTxs >= 10; }, "10 planned transactions");

        // Start reading from a snapshot, we would see 2 committed rows and mark all active transactions
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            beginSnapshotRequest(sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key in (1, 2, 3)
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } }");

        // Now schedule creation of 10 more snapshots
        for (int i = 0; i < 10; ++i) {
            SimulateSleep(runtime, TDuration::MilliSeconds(1000));
            SendCreateVolatileSnapshot(runtime, senderCreateSnapshot, { "/Root/table-1" }, TDuration::Minutes(10));
        }

        waitFor([&] { return observedPlanStepTxs >= 20; }, "20 planned transactions");

        // Start reading from a fresh snapshot, we should still observe 2 committed rows, and won't mark anything new
        UNIT_ASSERT_VALUES_EQUAL(
            beginSnapshotRequest(sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key in (1, 2, 3)
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } }");

        // Insert one more row, in a buggy case it would be assigned a version below the snapshot
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 3)"));

        // Read from snapshot again, unless buggy it should not be corrupted
        UNIT_ASSERT_VALUES_EQUAL(
            continueSnapshotRequest(sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key in (1, 2, 3)
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } }");
    }

    struct TLockSnapshot {
        ui64 LockId = 0;
        ui32 LockNodeId = 0;
        TRowVersion MvccSnapshot = TRowVersion::Min();
    };

    struct TLockInfo {
        ui64 LockId;
        ui64 DataShard;
        ui32 Generation;
        ui64 Counter;
        ui64 SchemeShard;
        ui64 PathId;

        friend bool operator==(const TLockInfo& a, const TLockInfo& b) = default;
    };

    struct TInjectLocks {
        NKikimrDataEvents::TKqpLocks::ELocksOp Op = NKikimrDataEvents::TKqpLocks::Commit;
        TVector<TLockInfo> Locks;

        TInjectLocks& AddLocks(const TVector<TLockInfo>& locks) {
            Locks.insert(Locks.end(), locks.begin(), locks.end());
            return *this;
        }
    };

    class TInjectLockSnapshotObserver {
    public:
        TInjectLockSnapshotObserver(TTestActorRuntime& runtime)
            : Runtime(runtime)
        {
            PrevObserver = runtime.SetObserverFunc([this](TAutoPtr<IEventHandle>& ev) {
                return this->Process(ev);
            });
        }

        ~TInjectLockSnapshotObserver() {
            Runtime.SetObserverFunc(PrevObserver);
        }

        TTestActorRuntime::EEventAction Process(TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvDataShard::TEvRead::EventType: {
                    auto& record = ev->Get<TEvDataShard::TEvRead>()->Record;
                    Cerr << "TEvRead:" << Endl;
                    Cerr << record.DebugString() << Endl;
                    Last = {};
                    if (record.GetLockTxId()) {
                        Last.LockId = record.GetLockTxId();
                        Last.LockNodeId = record.GetLockNodeId();
                    } else if (Inject.LockId) {
                        record.SetLockTxId(Inject.LockId);
                        if (Inject.LockNodeId) {
                            record.SetLockNodeId(Inject.LockNodeId);
                        }
                        Cerr << "TEvRead: injected LockId" << Endl;
                    }
                    if (record.HasSnapshot()) {
                        Last.MvccSnapshot.Step = record.GetSnapshot().GetStep();
                        Last.MvccSnapshot.TxId = record.GetSnapshot().GetTxId();
                    } else if (Inject.MvccSnapshot) {
                        record.MutableSnapshot()->SetStep(Inject.MvccSnapshot.Step);
                        record.MutableSnapshot()->SetTxId(Inject.MvccSnapshot.TxId);
                        Cerr << "TEvRead: injected MvccSnapshot" << Endl;
                    }
                    break;
                }
                case TEvDataShard::TEvProposeTransaction::EventType: {
                    auto& record = ev->Get<TEvDataShard::TEvProposeTransaction>()->Record;
                    Cerr << "TEvProposeTransaction:" << Endl;
                    Cerr << record.DebugString() << Endl;
                    if (record.GetTxKind() == NKikimrTxDataShard::TX_KIND_DATA) {
                        NKikimrTxDataShard::TDataTransaction tx;
                        Y_ABORT_UNLESS(tx.ParseFromString(record.GetTxBody()));
                        Cerr << "TxBody (original):" << Endl;
                        Cerr << tx.DebugString() << Endl;
                        if (tx.HasMiniKQL()) {
                            using namespace NKikimr::NMiniKQL;
                            TScopedAlloc alloc(__LOCATION__);
                            TTypeEnvironment typeEnv(alloc);
                            auto node = DeserializeRuntimeNode(tx.GetMiniKQL(), typeEnv);
                            Cerr << "MiniKQL:" << Endl;
                            Cerr << PrintNode(node.GetNode()) << Endl;
                        }
                        if (tx.HasKqpTransaction()) {
                            if (InjectClearTasks && tx.GetKqpTransaction().TasksSize() > 0) {
                                tx.MutableKqpTransaction()->ClearTasks();
                                TString txBody;
                                Y_ABORT_UNLESS(tx.SerializeToString(&txBody));
                                record.SetTxBody(txBody);
                                Cerr << "TxBody: cleared Tasks" << Endl;
                            }
                            if (InjectLocks) {
                                auto* protoLocks = tx.MutableKqpTransaction()->MutableLocks();
                                protoLocks->SetOp(InjectLocks->Op);
                                protoLocks->ClearLocks();
                                TSet<ui64> shards;
                                for (auto& lock : InjectLocks->Locks) {
                                    auto* protoLock = protoLocks->AddLocks();
                                    protoLock->SetLockId(lock.LockId);
                                    protoLock->SetDataShard(lock.DataShard);
                                    protoLock->SetGeneration(lock.Generation);
                                    protoLock->SetCounter(lock.Counter);
                                    protoLock->SetSchemeShard(lock.SchemeShard);
                                    protoLock->SetPathId(lock.PathId);
                                    shards.insert(lock.DataShard);
                                }
                                protoLocks->ClearSendingShards();
                                for (ui64 shard : shards) {
                                    protoLocks->AddSendingShards(shard);
                                    protoLocks->AddReceivingShards(shard);
                                }
                                TString txBody;
                                Y_ABORT_UNLESS(tx.SerializeToString(&txBody));
                                record.SetTxBody(txBody);
                                Cerr << "TxBody: injected Locks" << Endl;
                            }
                            for (const auto& task : tx.GetKqpTransaction().GetTasks()) {
                                if (task.HasProgram() && task.GetProgram().GetRaw()) {
                                    using namespace NKikimr::NMiniKQL;
                                    TScopedAlloc alloc(__LOCATION__);
                                    TTypeEnvironment typeEnv(alloc);
                                    auto node = DeserializeRuntimeNode(task.GetProgram().GetRaw(), typeEnv);
                                    Cerr << "Task program:" << Endl;
                                    Cerr << PrintNode(node.GetNode()) << Endl;
                                }
                            }
                        }
                        Last = {};
                        if (tx.GetLockTxId()) {
                            Last.LockId = tx.GetLockTxId();
                            Last.LockNodeId = tx.GetLockNodeId();
                        } else if (Inject.LockId) {
                            tx.SetLockTxId(Inject.LockId);
                            if (Inject.LockNodeId) {
                                tx.SetLockNodeId(Inject.LockNodeId);
                            }
                            TString txBody;
                            Y_ABORT_UNLESS(tx.SerializeToString(&txBody));
                            record.SetTxBody(txBody);
                            Cerr << "TxBody: injected LockId" << Endl;
                        }
                        if (record.HasMvccSnapshot()) {
                            Last.MvccSnapshot.Step = record.GetMvccSnapshot().GetStep();
                            Last.MvccSnapshot.TxId = record.GetMvccSnapshot().GetTxId();
                        } else if (Inject.MvccSnapshot) {
                            record.MutableMvccSnapshot()->SetStep(Inject.MvccSnapshot.Step);
                            record.MutableMvccSnapshot()->SetTxId(Inject.MvccSnapshot.TxId);
                            Cerr << "TEvProposeTransaction: injected MvccSnapshot" << Endl;
                        }
                    }
                    break;
                }
                case TEvDataShard::TEvProposeTransactionResult::EventType: {
                    auto& record = ev->Get<TEvDataShard::TEvProposeTransactionResult>()->Record;
                    Cerr << "TEvProposeTransactionResult:" << Endl;
                    Cerr << record.DebugString() << Endl;
                    LastLocks.clear();
                    for (auto& protoLock : record.GetTxLocks()) {
                        auto& lock = LastLocks.emplace_back();
                        lock.LockId = protoLock.GetLockId();
                        lock.DataShard = protoLock.GetDataShard();
                        lock.Generation = protoLock.GetGeneration();
                        lock.Counter = protoLock.GetCounter();
                        lock.SchemeShard = protoLock.GetSchemeShard();
                        lock.PathId = protoLock.GetPathId();
                    }
                    break;
                }
                case TEvTxProcessing::TEvReadSet::EventType: {
                    if (BlockReadSets) {
                        Cerr << "... blocked TEvReadSet" << Endl;
                        BlockedReadSets.push_back(THolder(ev.Release()));
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                }
                case TEvChangeExchange::TEvApplyRecords::EventType: {
                    if (BlockApplyRecords) {
                        Cerr << "... blocked ApplyRecords" << Endl;
                        BlockedApplyRecords.push_back(THolder(ev.Release()));
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                }
            }
            return PrevObserver(ev);
        }

        void UnblockReadSets() {
            BlockReadSets = false;
            for (auto& ev : BlockedReadSets) {
                Runtime.Send(ev.Release(), 0, true);
            }
        }

    private:
        TTestActorRuntime& Runtime;
        TTestActorRuntime::TEventObserver PrevObserver;

    public:
        TLockSnapshot Last;
        TLockSnapshot Inject;
        TVector<TLockInfo> LastLocks;
        std::optional<TInjectLocks> InjectLocks;
        bool InjectClearTasks = false;
        bool BlockReadSets = false;
        TVector<THolder<IEventHandle>> BlockedReadSets;
        bool BlockApplyRecords = false;
        TVector<THolder<IEventHandle>> BlockedApplyRecords;
    };

    Y_UNIT_TEST(MvccSnapshotLockedWrites) {
        TPortManager pm;
        TServerSettings::TControls controls;
        controls.MutableDataShardControls()->SetEnableLockedWrites(1);

        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetControls(controls);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        server->SetupRootStoragePools(sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;
        CreateShardedTable(server, sender, "/Root", "table-1", 1);

        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));

        SimulateSleep(server, TDuration::Seconds(1));

        TInjectLockSnapshotObserver observer(runtime);

        // Start a snapshot read transaction
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

        // We should have been acquiring locks
        TLockSnapshot snapshot = observer.Last;
        Y_ABORT_UNLESS(snapshot.LockId != 0);
        Y_ABORT_UNLESS(snapshot.MvccSnapshot);

        // Perform an immediate write, pretending it happens as part of the above snapshot tx
        observer.Inject = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 2)
                )")), "<empty>");
        observer.Inject = {};

        // Start another snapshot read, it should not see above write (it's uncommitted)
        TString sessionId2, txId2;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId2, txId2, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

        // Perform another read using the first snapshot tx, it must see its own writes
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleContinue(runtime, sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } }");

        // Now commit with additional changes (temporarily needed to trigger lock commits)
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleCommit(runtime, sessionId, txId, Q_(R"(
                UPSERT INTO `Root/table-1` (key, value) VALUES (3, 3)
                )")),
            "<empty>");

        // Verify new snapshots observe all committed changes
        // This is only possible with new engine at this time
        TString sessionId3, txId3;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId3, txId3, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } }, "
            "{ items { uint32_value: 3 } items { uint32_value: 3 } }");
    }

    Y_UNIT_TEST(MvccSnapshotLockedWritesRestart) {
        TPortManager pm;
        TServerSettings::TControls controls;
        controls.MutableDataShardControls()->SetEnableLockedWrites(1);

        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetControls(controls);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;
        CreateShardedTable(server, sender, "/Root", "table-1", 1);

        auto shards1 = GetTableShards(server, sender, "/Root/table-1");

        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));

        SimulateSleep(server, TDuration::Seconds(1));

        TInjectLockSnapshotObserver observer(runtime);

        // Start a snapshot read transaction
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

        // We should have been acquiring locks
        TLockSnapshot snapshot = observer.Last;
        Y_ABORT_UNLESS(snapshot.LockId != 0);
        Y_ABORT_UNLESS(snapshot.MvccSnapshot);

        // Perform an immediate write, pretending it happens as part of the above snapshot tx
        // We expect read lock to be upgraded to write lock and become persistent
        observer.Inject = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 2)
                )")),
            "<empty>");
        observer.Inject = {};

        // Reboot tablet, persistent locks must not be lost
        RebootTablet(runtime, shards1[0], sender);

        // Start another snapshot read, it should not see above write (it's uncommitted)
        TString sessionId2, txId2;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId2, txId2, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

        // Perform another read using the first snapshot tx, it must see its own writes
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleContinue(runtime, sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } }");

        // Now commit with additional changes (temporarily needed to trigger lock commits)
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleCommit(runtime, sessionId, txId, Q_(R"(
                UPSERT INTO `Root/table-1` (key, value) VALUES (3, 3)
                )")),
            "<empty>");

        // Verify new snapshots observe all committed changes
        // This is only possible with new engine at this time
        TString sessionId3, txId3;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId3, txId3, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } }, "
            "{ items { uint32_value: 3 } items { uint32_value: 3 } }");
    }

    Y_UNIT_TEST(MvccSnapshotLockedWritesWithoutConflicts) {
        TPortManager pm;
        TServerSettings::TControls controls;
        controls.MutableDataShardControls()->SetEnableLockedWrites(1);

        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetControls(controls);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;
        CreateShardedTable(server, sender, "/Root", "table-1", 1);

        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));

        SimulateSleep(server, TDuration::Seconds(1));

        TInjectLockSnapshotObserver observer(runtime);

        // Start a snapshot read transaction
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

        // We will reuse this snapshot
        auto snapshot = observer.Last.MvccSnapshot;

        using NLongTxService::TLockHandle;
        TLockHandle lock1handle(123, runtime.GetActorSystem(0));
        TLockHandle lock2handle(234, runtime.GetActorSystem(0));

        // Write uncommitted changes to key 2 with tx 123
        observer.Inject.LockId = 123;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `Root/table-1` (key, value) VALUES (2, 21)
                )")),
            "<empty>");
        auto locks1 = observer.LastLocks;
        observer.Inject = {};

        // Write uncommitted changes to key 2 with tx 234
        observer.Inject.LockId = 234;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `Root/table-1` (key, value) VALUES (2, 22)
                )")),
            "<empty>");
        auto locks2 = observer.LastLocks;
        observer.Inject = {};

        // Verify these changes are not visible yet
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

        // Send a dummy upsert that we will be used as commit carrier for tx 123
        observer.InjectClearTasks = true;
        observer.InjectLocks.emplace().Locks = locks1;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `Root/table-1` (key, value) VALUES (1, 1)
                )")),
            "<empty>");
        observer.InjectClearTasks = false;
        observer.InjectLocks.reset();

        // Verify tx 123 changes are now visible
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, { items { uint32_value: 2 } items { uint32_value: 21 } }");

        // Send a dummy upsert that we will be used as commit carrier for tx 234
        observer.InjectClearTasks = true;
        observer.InjectLocks.emplace().Locks = locks2;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `Root/table-1` (key, value) VALUES (1, 1)
                )")),
            "<empty>");
        observer.InjectClearTasks = false;
        observer.InjectLocks.reset();

        // Verify tx 234 changes are now visible
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 22 } }");

        // The still open read tx must have broken locks now
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleCommit(runtime, sessionId, txId, Q_(R"(
                UPSERT INTO `Root/table-1` (key, value) VALUES (3, 3)
                )")),
            "ERROR: ABORTED");
    }

    Y_UNIT_TEST(MvccSnapshotLockedWritesWithConflicts) {
        TPortManager pm;
        TServerSettings::TControls controls;
        controls.MutableDataShardControls()->SetEnableLockedWrites(1);

        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetControls(controls);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;
        CreateShardedTable(server, sender, "/Root", "table-1", 1);

        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));

        SimulateSleep(server, TDuration::Seconds(1));

        TInjectLockSnapshotObserver observer(runtime);

        // Start a snapshot read transaction
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

        // We will reuse this snapshot
        auto snapshot = observer.Last.MvccSnapshot;

        using NLongTxService::TLockHandle;
        TLockHandle lock1handle(123, runtime.GetActorSystem(0));
        TLockHandle lock2handle(234, runtime.GetActorSystem(0));

        // Write uncommitted changes to key 2 with tx 123
        observer.Inject.LockId = 123;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `Root/table-1` (key, value) VALUES (2, 21)
                )")),
            "<empty>");
        auto locks1 = observer.LastLocks;
        observer.Inject = {};

        // Write uncommitted changes to key 2 with tx 234
        observer.Inject.LockId = 234;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `Root/table-1` (key, value) VALUES (2, 22)
                )")),
            "<empty>");
        auto locks2 = observer.LastLocks;
        observer.Inject = {};

        // Verify these changes are not visible yet
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

        // Verify the open tx can commit writes (not broken yet)
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleCommit(runtime, sessionId, txId, Q_(R"(
                UPSERT INTO `Root/table-1` (key, value) VALUES (3, 3)
                )")),
            "<empty>");

        // Send a dummy upsert that we will be used as commit carrier for tx 234
        observer.InjectClearTasks = true;
        observer.InjectLocks.emplace().Locks = locks2;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `Root/table-1` (key, value) VALUES (1, 1)
                )")),
            "<empty>");
        observer.InjectClearTasks = false;
        observer.InjectLocks.reset();

        // Verify tx 234 changes are now visible
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 22 } }, "
            "{ items { uint32_value: 3 } items { uint32_value: 3 } }");

        // Send a dummy upsert that we will be used as commit carrier for tx 123
        // It must not be able to commit, since it was broken by tx 234
        observer.InjectClearTasks = true;
        observer.InjectLocks.emplace().Locks = locks1;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `Root/table-1` (key, value) VALUES (1, 1)
                )")),
            "ERROR: ABORTED");
        observer.InjectClearTasks = false;
        observer.InjectLocks.reset();
    }

    std::unique_ptr<TEvDataShard::TEvRead> PrepareRead(
            ui64 readId,
            const TTableId& tableId,
            const TRowVersion& snapshot,
            const TVector<ui32>& columns)
    {
        auto request = std::make_unique<TEvDataShard::TEvRead>();
        auto& record = request->Record;
        record.SetReadId(readId);
        record.MutableTableId()->SetOwnerId(tableId.PathId.OwnerId);
        record.MutableTableId()->SetTableId(tableId.PathId.LocalPathId);
        record.MutableTableId()->SetSchemaVersion(tableId.SchemaVersion);
        record.MutableSnapshot()->SetStep(snapshot.Step);
        record.MutableSnapshot()->SetTxId(snapshot.TxId);
        for (ui32 columnId : columns) {
            record.AddColumns(columnId);
        }
        record.SetResultFormat(NKikimrDataEvents::FORMAT_CELLVEC);
        return request;
    }

    void AddReadRange(TEvDataShard::TEvRead& request, ui32 fromKey, ui32 toKey) {
        TVector<TCell> fromKeyCells = { TCell::Make(fromKey) };
        TVector<TCell> toKeyCells = { TCell::Make(toKey) };
        auto fromBuf = TSerializedCellVec::Serialize(fromKeyCells);
        auto toBuf = TSerializedCellVec::Serialize(toKeyCells);
        request.Ranges.emplace_back(fromBuf, toBuf, true, true);
    }

    TString ReadResultRowsString(const TEvDataShard::TEvReadResult& result) {
        TStringBuilder builder;
        for (size_t row = 0; row < result.GetRowsCount(); ++row) {
            auto rowCells = result.GetCells(row);
            for (size_t i = 0; i < rowCells.size(); ++i) {
                if (i != 0) {
                    builder << ' ';
                }
                builder << rowCells[i].AsValue<ui32>();
            }
            builder << '\n';
        }
        return builder;
    }

    Y_UNIT_TEST(MvccSnapshotReadLockedWrites) {
        TPortManager pm;
        TServerSettings::TControls controls;
        controls.MutableDataShardControls()->SetEnableLockedWrites(1);

        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetControls(controls);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;
        CreateShardedTable(server, sender, "/Root", "table-1", TShardedTableOptions()
            .Columns({{"key", "Uint32", true, false}, {"value", "Uint32", false, false}, {"value2", "Uint32", false, false}}));

        auto shards = GetTableShards(server, sender, "/Root/table-1");
        auto tableId = ResolveTableId(server, sender, "/Root/table-1");

        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value, value2) VALUES (1, 1, 1)"));

        SimulateSleep(server, TDuration::Seconds(1));

        TInjectLockSnapshotObserver observer(runtime);

        // Start a snapshot read transaction
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
                SELECT key, value, value2 FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } items { uint32_value: 1 } }");

        // We will reuse this snapshot
        auto snapshot = observer.Last.MvccSnapshot;

        using NLongTxService::TLockHandle;
        TLockHandle lock1handle(123, runtime.GetActorSystem(0));
        TLockHandle lock2handle(234, runtime.GetActorSystem(0));
        TLockHandle lock3handle(345, runtime.GetActorSystem(0));

        // Write uncommitted changes to key 2 with tx 123
        observer.Inject.LockId = 123;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `Root/table-1` (key, value, value2) VALUES (2, 21, 201)
                )")),
            "<empty>");
        auto locks1 = observer.LastLocks;
        observer.Inject = {};

        // Write uncommitted changes to key 2 with tx 234
        observer.Inject.LockId = 234;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `Root/table-1` (key, value) VALUES (2, 22)
                )")),
            "<empty>");
        auto locks2 = observer.LastLocks;
        observer.Inject = {};

        // Write uncommitted changes to key 2 with tx 345
        observer.Inject.LockId = 345;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `Root/table-1` (key, value) VALUES (2, 23)
                )")),
            "<empty>");
        auto locks3 = observer.LastLocks;
        observer.Inject = {};

        // Try to read uncommitted rows in tx 123
        {
            auto readSender = runtime.AllocateEdgeActor();
            auto request = PrepareRead(1, tableId, snapshot, { 1, 2, 3 });
            AddReadRange(*request, 1, 3);
            request->Record.SetLockTxId(123);
            request->Record.SetLockNodeId(runtime.GetNodeId(0));
            auto clientId = runtime.ConnectToPipe(shards.at(0), readSender, 0, NKikimr::NTabletPipe::TClientConfig());
            runtime.SendToPipe(clientId, readSender, request.release());
            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvReadResult>(readSender);
            auto* response = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(response->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(
                ReadResultRowsString(*response),
                "1 1 1\n"
                "2 21 201\n");
        }

        // Commit changes in tx 123
        observer.InjectClearTasks = true;
        observer.InjectLocks.emplace().Locks = locks1;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `Root/table-1` (key, value) VALUES (0, 0)
                )")),
            "<empty>");
        observer.InjectClearTasks = false;
        observer.InjectLocks.reset();

        // Try to read uncommitted rows in tx 234
        {
            auto readSender = runtime.AllocateEdgeActor();
            auto request = PrepareRead(1, tableId, snapshot, { 1, 2, 3 });
            AddReadRange(*request, 1, 3);
            request->Record.SetLockTxId(234);
            request->Record.SetLockNodeId(runtime.GetNodeId(0));
            auto clientId = runtime.ConnectToPipe(shards.at(0), readSender, 0, NKikimr::NTabletPipe::TClientConfig());
            runtime.SendToPipe(clientId, readSender, request.release());
            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvReadResult>(readSender);
            auto* response = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(response->Record.GetStatus().GetCode(), Ydb::StatusIds::ABORTED);
        }

        // Try to read uncommitted rows in tx 345
        {
            auto readSender = runtime.AllocateEdgeActor();
            auto request = PrepareRead(1, tableId, snapshot, { 1, 2, 3 });
            AddReadRange(*request, 1, 3);
            request->Record.SetLockTxId(345);
            request->Record.SetLockNodeId(runtime.GetNodeId(0));
            request->Record.SetMaxRowsInResult(1);
            auto clientId = runtime.ConnectToPipe(shards.at(0), readSender, 0, NKikimr::NTabletPipe::TClientConfig());
            runtime.SendToPipe(clientId, readSender, request.release());
            {
                auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvReadResult>(readSender);
                auto* response = ev->Get();
                UNIT_ASSERT_VALUES_EQUAL(response->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
                UNIT_ASSERT_VALUES_EQUAL(
                    ReadResultRowsString(*response),
                    "1 1 1\n");
            }
            {
                auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvReadResult>(readSender);
                auto* response = ev->Get();
                UNIT_ASSERT_VALUES_EQUAL(response->Record.GetStatus().GetCode(), Ydb::StatusIds::ABORTED);
            }
        }
    }

    Y_UNIT_TEST(MvccSnapshotLockedWritesWithReadConflicts) {
        TPortManager pm;
        TServerSettings::TControls controls;
        controls.MutableDataShardControls()->SetEnableLockedWrites(1);

        NKikimrConfig::TAppConfig app;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetControls(controls)
            .SetAppConfig(app);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;
        CreateShardedTable(server, sender, "/Root", "table-1", TShardedTableOptions()
            .Columns({{"key", "Uint32", true, false}, {"value", "Uint32", false, false}, {"value2", "Uint32", false, false}}));

        auto shards = GetTableShards(server, sender, "/Root/table-1");
        auto tableId = ResolveTableId(server, sender, "/Root/table-1");

        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value, value2) VALUES (1, 1, 1)"));

        SimulateSleep(server, TDuration::Seconds(1));

        TInjectLockSnapshotObserver observer(runtime);

        // Start a snapshot read transaction
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
                SELECT key, value, value2 FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } items { uint32_value: 1 } }");

        // We will reuse this snapshot
        auto snapshot = observer.Last.MvccSnapshot;

        using NLongTxService::TLockHandle;
        TLockHandle lock1handle(123, runtime.GetActorSystem(0));
        TLockHandle lock2handle(234, runtime.GetActorSystem(0));

        // Write uncommitted changes to key 2 with tx 123
        observer.Inject.LockId = 123;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value, value2) VALUES (2, 21, 201)
                )")),
            "<empty>");
        auto locks1 = observer.LastLocks;
        observer.Inject = {};

        // Write uncommitted changes to key 2 with tx 234
        observer.Inject.LockId = 234;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 22)
                )")),
            "<empty>");
        auto locks2 = observer.LastLocks;
        observer.Inject = {};

        // Read uncommitted rows in tx 123
        observer.Inject.LockId = 123;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                SELECT key, value, value2 FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 21 } items { uint32_value: 201 } }");
        observer.Inject = {};

        // Commit changes in tx 123
        observer.InjectClearTasks = true;
        observer.InjectLocks.emplace().Locks = locks1;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (0, 0)
                )")),
            "<empty>");
        observer.InjectClearTasks = false;
        observer.InjectLocks.reset();

        // Read uncommitted rows in tx 234 without value2 column
        // It should succeed, since result does not depend on tx 123 changes
        observer.Inject.LockId = 234;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 22 } }");
        observer.Inject = {};

        // Read uncommitted rows in tx 234 with the limit 1
        // It should succeed, since result does not depend on tx 123 changes
        observer.Inject.LockId = 234;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                SELECT key, value, value2 FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                LIMIT 1
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } items { uint32_value: 1 } }");
        observer.Inject = {};

        // Read uncommitted rows in tx 234 with the limit 1
        // It should succeed, since result does not depend on tx 123 changes
        observer.Inject.LockId = 234;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                SELECT key, value, value2 FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "ERROR: ABORTED");
        observer.Inject = {};
    }

    Y_UNIT_TEST(LockedWriteBulkUpsertConflict) {
        TPortManager pm;
        TServerSettings::TControls controls;
        controls.MutableDataShardControls()->SetEnableLockedWrites(1);

        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetControls(controls);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;
        CreateShardedTable(server, sender, "/Root", "table-1", 1);

        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));

        SimulateSleep(server, TDuration::Seconds(1));

        TInjectLockSnapshotObserver observer(runtime);

        // Start a snapshot read transaction
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

        // We will reuse this snapshot
        auto snapshot = observer.Last.MvccSnapshot;

        using NLongTxService::TLockHandle;
        TLockHandle lock1handle(123, runtime.GetActorSystem(0));

        // Write uncommitted changes to key 2 with tx 123
        observer.Inject.LockId = 123;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 21)
                )")),
            "<empty>");
        auto locks1 = observer.LastLocks;
        observer.Inject = {};

        // Write to key 2 using bulk upsert
        {
            using TRows = TVector<std::pair<TSerializedCellVec, TString>>;
            using TRowTypes = TVector<std::pair<TString, Ydb::Type>>;

            auto types = std::make_shared<TRowTypes>();

            Ydb::Type type;
            type.set_type_id(Ydb::Type::UINT32);
            types->emplace_back("key", type);
            types->emplace_back("value", type);

            auto rows = std::make_shared<TRows>();

            TVector<TCell> key{ TCell::Make(ui32(2)) };
            TVector<TCell> values{ TCell::Make(ui32(22)) };
            TSerializedCellVec serializedKey(key);
            TString serializedValues(TSerializedCellVec::Serialize(values));
            rows->emplace_back(serializedKey, serializedValues);

            auto upsertSender = runtime.AllocateEdgeActor();
            auto actor = NTxProxy::CreateUploadRowsInternal(upsertSender, "/Root/table-1", types, rows);
            runtime.Register(actor);

            auto ev = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvUploadRowsResponse>(upsertSender);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, Ydb::StatusIds::SUCCESS);
        }

        // Commit changes in tx 123
        observer.InjectClearTasks = true;
        observer.InjectLocks.emplace().Locks = locks1;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (0, 0)
                )")),
            "ERROR: ABORTED");
        observer.InjectClearTasks = false;
        observer.InjectLocks.reset();
    }

    Y_UNIT_TEST(LockedWriteReuseAfterCommit) {
        TPortManager pm;
        TServerSettings::TControls controls;
        controls.MutableDataShardControls()->SetEnableLockedWrites(1);

        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetControls(controls);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;
        CreateShardedTable(server, sender, "/Root", "table-1", 1);

        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));

        SimulateSleep(server, TDuration::Seconds(1));

        TInjectLockSnapshotObserver observer(runtime);

        // Start a snapshot read transaction
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

        // We will reuse this snapshot
        auto snapshot = observer.Last.MvccSnapshot;

        using NLongTxService::TLockHandle;
        TLockHandle lock1handle(123, runtime.GetActorSystem(0));

        // Write uncommitted changes to key 2 with tx 123
        observer.Inject.LockId = 123;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 21)
                )")),
            "<empty>");
        auto locks1 = observer.LastLocks;
        observer.Inject = {};

        // Commit changes in tx 123
        observer.InjectClearTasks = true;
        observer.InjectLocks.emplace().Locks = locks1;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (0, 0)
                )")),
            "<empty>");
        observer.InjectClearTasks = false;
        observer.InjectLocks.reset();

        // Write uncommitted changes to key 3 with tx 123
        // The lock for tx 123 was committed and removed, and cannot be reused
        // until all changes are fully compacted. Otherwise new changes will
        // appear as immediately committed in the past.
        observer.Inject.LockId = 123;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 31)
                )")),
            "ERROR: ABORTED");
        auto locks2 = observer.LastLocks;
        observer.Inject = {};
    }

    Y_UNIT_TEST(LockedWriteDistributedCommitSuccess) {
        TPortManager pm;
        TServerSettings::TControls controls;
        controls.MutableDataShardControls()->SetEnableLockedWrites(1);

        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetControls(controls);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;
        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 1)"));

        SimulateSleep(server, TDuration::Seconds(1));

        TInjectLockSnapshotObserver observer(runtime);

        // Start a snapshot read transaction
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

        // We will reuse this snapshot
        auto snapshot = observer.Last.MvccSnapshot;

        using NLongTxService::TLockHandle;
        TLockHandle lock1handle(123, runtime.GetActorSystem(0));

        // Write uncommitted changes to key 2 with tx 123
        observer.Inject.LockId = 123;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 21)
                )")),
            "<empty>");
        auto locks1 = observer.LastLocks;
        observer.Inject = {};

        // Write uncommitted changes to key 20 with tx 123
        observer.Inject.LockId = 123;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-2` (key, value) VALUES (20, 21)
                )")),
            "<empty>");
        auto locks2 = observer.LastLocks;
        observer.Inject = {};

        // Commit changes in tx 123
        observer.InjectClearTasks = true;
        observer.InjectLocks.emplace();
        observer.InjectLocks->AddLocks(locks1);
        observer.InjectLocks->AddLocks(locks2);
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (0, 0);
                UPSERT INTO `/Root/table-2` (key, value) VALUES (0, 0);
                )")),
            "<empty>");
        observer.InjectClearTasks = false;
        observer.InjectLocks.reset();

        // Verify changes are now visible
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 21 } }");

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                SELECT key, value FROM `/Root/table-2`
                WHERE key >= 10 AND key <= 30
                ORDER BY key
                )")),
            "{ items { uint32_value: 10 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 20 } items { uint32_value: 21 } }");

    }

    Y_UNIT_TEST(LockedWriteDistributedCommitAborted) {
        TPortManager pm;
        TServerSettings::TControls controls;
        controls.MutableDataShardControls()->SetEnableLockedWrites(1);

        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetControls(controls);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;
        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 1)"));

        SimulateSleep(server, TDuration::Seconds(1));

        TInjectLockSnapshotObserver observer(runtime);

        // Start a snapshot read transaction
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

        // We will reuse this snapshot
        auto snapshot = observer.Last.MvccSnapshot;

        using NLongTxService::TLockHandle;
        TLockHandle lock1handle(123, runtime.GetActorSystem(0));

        // Write uncommitted changes to key 2 with tx 123
        observer.Inject.LockId = 123;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 21)
                )")),
            "<empty>");
        auto locks1 = observer.LastLocks;
        observer.Inject = {};

        // Write uncommitted changes to key 20 with tx 123
        observer.Inject.LockId = 123;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-2` (key, value) VALUES (20, 21)
                )")),
            "<empty>");
        auto locks2 = observer.LastLocks;
        observer.Inject = {};

        // Write to key 20, it will break tx 123
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-2` (key, value) VALUES (20, 22)
                )")),
            "<empty>");

        // Commit changes in tx 123
        observer.InjectClearTasks = true;
        observer.InjectLocks.emplace();
        observer.InjectLocks->AddLocks(locks1);
        observer.InjectLocks->AddLocks(locks2);
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (0, 0);
                UPSERT INTO `/Root/table-2` (key, value) VALUES (0, 0);
                )")),
            "ERROR: ABORTED");
        observer.InjectClearTasks = false;
        observer.InjectLocks.reset();

        // Verify changes are not visible
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                SELECT key, value FROM `/Root/table-2`
                WHERE key >= 10 AND key <= 30
                ORDER BY key
                )")),
            "{ items { uint32_value: 10 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 20 } items { uint32_value: 22 } }");
    }

    Y_UNIT_TEST(LockedWriteDistributedCommitFreeze) {
        TPortManager pm;
        TServerSettings::TControls controls;
        controls.MutableDataShardControls()->SetEnableLockedWrites(1);

        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetControls(controls);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;
        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 1)"));

        SimulateSleep(server, TDuration::Seconds(1));

        TInjectLockSnapshotObserver observer(runtime);

        // Start a snapshot read transaction
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

        // We will reuse this snapshot
        auto snapshot = observer.Last.MvccSnapshot;

        using NLongTxService::TLockHandle;
        TLockHandle lock1handle(123, runtime.GetActorSystem(0));

        // Write uncommitted changes to key 2 with tx 123
        observer.Inject.LockId = 123;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 21)
                )")),
            "<empty>");
        auto locks1 = observer.LastLocks;
        observer.Inject = {};

        // Write uncommitted changes to key 20 with tx 123
        observer.Inject.LockId = 123;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-2` (key, value) VALUES (20, 21)
                )")),
            "<empty>");
        auto locks2 = observer.LastLocks;
        observer.Inject = {};

        // Note: disable volatile transactions, since this test verifies lock
        // freezing and volatile transactions work without them.
        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(false);

        // Commit changes in tx 123
        observer.BlockReadSets = true;
        observer.InjectClearTasks = true;
        observer.InjectLocks.emplace();
        observer.InjectLocks->AddLocks(locks1);
        observer.InjectLocks->AddLocks(locks2);
        auto commitSender = CreateSessionRPC(runtime);
        auto writeSender = CreateSessionRPC(runtime);
        TString commitSenderTxId;
        TString writeSenderTxId;
        auto commitFuture = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (0, 0);
            UPSERT INTO `/Root/table-2` (key, value) VALUES (0, 0);
            )"), commitSender, commitSenderTxId, true));

        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT(!observer.BlockedReadSets.empty());
        observer.InjectClearTasks = false;
        observer.InjectLocks.reset();

        // Restore volatile transactions back to default
        runtime.GetAppData(0).FeatureFlags.ClearEnableDataShardVolatileTransactions();

        auto writeFuture = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 22)
            )"), writeSender, writeSenderTxId, true));
        runtime.SimulateSleep(TDuration::Seconds(1));

        // Verify changes are not visible yet
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

        observer.UnblockReadSets();

        {

            auto response = AwaitResponse(runtime, commitFuture);
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
        }

        {
            auto response = AwaitResponse(runtime, writeFuture);
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
        }
    }

    Y_UNIT_TEST(LockedWriteDistributedCommitCrossConflict) {
        TPortManager pm;
        TServerSettings::TControls controls;
        controls.MutableDataShardControls()->SetEnableLockedWrites(1);

        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetControls(controls);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;
        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 1)"));

        SimulateSleep(server, TDuration::Seconds(1));

        TInjectLockSnapshotObserver observer(runtime);

        // Start a snapshot read transaction
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

        // We will reuse this snapshot
        auto snapshot = observer.Last.MvccSnapshot;

        using NLongTxService::TLockHandle;
        TLockHandle lock1handle(123, runtime.GetActorSystem(0));
        TLockHandle lock2handle(234, runtime.GetActorSystem(0));

        // Write uncommitted changes to key 2 with tx 123
        observer.Inject.LockId = 123;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 21)
                )")),
            "<empty>");
        auto locks1 = observer.LastLocks;
        observer.Inject = {};

        // Write uncommitted changes to key 20 with tx 123
        observer.Inject.LockId = 123;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-2` (key, value) VALUES (20, 21)
                )")),
            "<empty>");
        auto locks2 = observer.LastLocks;
        observer.Inject = {};

        // Write uncommitted changes to key 3 with tx 234
        observer.Inject.LockId = 234;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 22)
                )")),
            "<empty>");
        auto locks3 = observer.LastLocks;
        observer.Inject = {};

        // Write uncommitted changes to key 30 with tx 234
        observer.Inject.LockId = 234;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-2` (key, value) VALUES (30, 22)
                )")),
            "<empty>");
        auto locks4 = observer.LastLocks;
        observer.Inject = {};

        // Note: disable volatile transactions, since reads to keys are blocked
        // until readsets arrive, and this test relies on tx cross blocking,
        // which doesn't happen with volatile transactions.
        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(false);

        // Commit changes in tx 123 (we expect locks to be ready for sending)
        observer.BlockReadSets = true;
        //observer.InjectClearTasks = true;
        observer.InjectLocks.emplace();
        observer.InjectLocks->AddLocks(locks1);
        observer.InjectLocks->AddLocks(locks2);
        TString commitSender1 = CreateSessionRPC(runtime);
        TString commitSender1TxId;
        TString commitSender2 = CreateSessionRPC(runtime);
        TString commitSender2TxId;
        auto commitSender1Future = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 21);
            UPSERT INTO `/Root/table-2` (key, value) VALUES (30, 21);
            )"), commitSender1, commitSender1TxId, true));
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT(!observer.BlockedReadSets.empty());
        //observer.InjectClearTasks = false;
        observer.InjectLocks.reset();

        // Restore volatile transactions back to default
        runtime.GetAppData(0).FeatureFlags.ClearEnableDataShardVolatileTransactions();

        // Commit changes in tx 234 (we expect it to be blocked and broken by 123)
        observer.BlockReadSets = true;
        //observer.InjectClearTasks = true;
        observer.InjectLocks.emplace();
        observer.InjectLocks->AddLocks(locks3);
        observer.InjectLocks->AddLocks(locks4);
        auto commitSender2Future = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 22);
            UPSERT INTO `/Root/table-2` (key, value) VALUES (20, 22);
            )"), commitSender2, commitSender2TxId, true));
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT(!observer.BlockedReadSets.empty());
        //observer.InjectClearTasks = false;
        observer.InjectLocks.reset();

        // Verify changes are not visible yet
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

        observer.UnblockReadSets();

        {
            auto response = AwaitResponse(runtime, commitSender1Future);
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
        }

        {
            auto response = AwaitResponse(runtime, commitSender2Future);
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::ABORTED);
        }

        // Verify only changes from commit 123 are visible
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 21 } }, "
            "{ items { uint32_value: 3 } items { uint32_value: 21 } }");

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                SELECT key, value FROM `/Root/table-2`
                WHERE key >= 10 AND key <= 30
                ORDER BY key
                )")),
            "{ items { uint32_value: 10 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 20 } items { uint32_value: 21 } }, "
            "{ items { uint32_value: 30 } items { uint32_value: 21 } }");
    }

    Y_UNIT_TEST(LockedWriteCleanupOnSplit) {
        TPortManager pm;
        TServerSettings::TControls controls;
        controls.MutableDataShardControls()->SetEnableLockedWrites(1);

        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetControls(controls);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;
        CreateShardedTable(server, sender, "/Root", "table-1", 1);

        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));

        SimulateSleep(server, TDuration::Seconds(1));

        TInjectLockSnapshotObserver observer(runtime);

        // Start a snapshot read transaction
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

        // We will reuse this snapshot
        auto snapshot = observer.Last.MvccSnapshot;

        using NLongTxService::TLockHandle;
        TLockHandle lock1handle(123, runtime.GetActorSystem(0));

        // Write uncommitted changes to key 2 with tx 123
        observer.Inject.LockId = 123;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 21)
                )")),
            "<empty>");
        auto locks1 = observer.LastLocks;
        observer.Inject = {};

        auto shards = GetTableShards(server, sender, "/Root/table-1");
        auto tableId = ResolveTableId(server, sender, "/Root/table-1");

        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        // Check shard has some open transactions
        {
            auto checkSender = runtime.AllocateEdgeActor();
            runtime.SendToPipe(shards.at(0), checkSender, new TEvDataShard::TEvGetOpenTxs(tableId.PathId));
            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvGetOpenTxsResult>(checkSender);
            UNIT_ASSERT_C(!ev->Get()->OpenTxs.empty(), "at shard " << shards.at(0));
        }

        // Split/merge would fail otherwise :(
        SetSplitMergePartCountLimit(server->GetRuntime(), -1);

        // Split table in two shards
        {
            //Cerr << "----Split Begin----" << Endl;
            auto senderSplit = runtime.AllocateEdgeActor();
            auto tablets = GetTableShards(server, senderSplit, "/Root/table-1");
            ui64 txId = AsyncSplitTable(server, senderSplit, "/Root/table-1", tablets.at(0), 3);
            WaitTxNotification(server, senderSplit, txId);
            //Cerr << "----Split End----" << Endl;
        }

        shards = GetTableShards(server, sender, "/Root/table-1");

        // Check new shards don't have any open transactions
        {
            auto checkSender = runtime.AllocateEdgeActor();
            for (auto shardId : shards) {
                runtime.SendToPipe(shardId, checkSender, new TEvDataShard::TEvGetOpenTxs(tableId.PathId));
                auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvGetOpenTxsResult>(checkSender);
                UNIT_ASSERT_C(ev->Get()->OpenTxs.empty(), "at shard " << shardId);
            }
        }
    }

    Y_UNIT_TEST(LockedWriteCleanupOnCopyTable) {
        TPortManager pm;
        TServerSettings::TControls controls;
        controls.MutableDataShardControls()->SetEnableLockedWrites(1);

        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetControls(controls);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;
        CreateShardedTable(server, sender, "/Root", "table-1", 1);

        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));

        SimulateSleep(server, TDuration::Seconds(1));

        TInjectLockSnapshotObserver observer(runtime);

        // Start a snapshot read transaction
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

        // We will reuse this snapshot
        auto snapshot = observer.Last.MvccSnapshot;

        using NLongTxService::TLockHandle;
        TLockHandle lock1handle(123, runtime.GetActorSystem(0));

        // Write uncommitted changes to key 2 with tx 123
        observer.Inject.LockId = 123;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 21)
                )")),
            "<empty>");
        auto locks1 = observer.LastLocks;
        observer.Inject = {};

        auto shards = GetTableShards(server, sender, "/Root/table-1");
        auto tableId = ResolveTableId(server, sender, "/Root/table-1");

        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        // Check shard has some open transactions
        {
            auto checkSender = runtime.AllocateEdgeActor();
            runtime.SendToPipe(shards.at(0), checkSender, new TEvDataShard::TEvGetOpenTxs(tableId.PathId));
            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvGetOpenTxsResult>(checkSender);
            UNIT_ASSERT_C(!ev->Get()->OpenTxs.empty(), "at shard " << shards.at(0));
        }

        // Copy table
        {
            auto senderCopy = runtime.AllocateEdgeActor();
            ui64 txId = AsyncCreateCopyTable(server, senderCopy, "/Root", "table-2", "/Root/table-1");
            WaitTxNotification(server, senderCopy, txId);
        }

        auto shards2 = GetTableShards(server, sender, "/Root/table-2");
        auto tableId2 = ResolveTableId(server, sender, "/Root/table-2");

        // Check new shards don't have any open transactions
        {
            auto checkSender = runtime.AllocateEdgeActor();
            for (auto shardId : shards2) {
                runtime.SendToPipe(shardId, checkSender, new TEvDataShard::TEvGetOpenTxs(tableId2.PathId));
                auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvGetOpenTxsResult>(checkSender);
                UNIT_ASSERT_C(ev->Get()->OpenTxs.empty(), "at shard " << shardId);
            }
        }

        // Commit changes in tx 123
        observer.InjectClearTasks = true;
        observer.InjectLocks.emplace().Locks = locks1;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (0, 0)
                )")),
            "<empty>");
        observer.InjectClearTasks = false;
        observer.InjectLocks.reset();

        // Check original table has those changes visible
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 21 } }");

        // Check table copy does not have those changes
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                SELECT key, value FROM `/Root/table-2`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");
    }

    Y_UNIT_TEST_TWIN(LockedWriteWithAsyncIndex, WithRestart) {
        TPortManager pm;
        TServerSettings::TControls controls;
        controls.MutableDataShardControls()->SetEnableLockedWrites(1);

        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetControls(controls);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;
        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        WaitTxNotification(server, sender,
            AsyncAlterAddIndex(server, "/Root", "/Root/table-1",
                TShardedTableOptions::TIndex{"by_value", {"value"}, {}, NKikimrSchemeOp::EIndexTypeGlobalAsync}));

        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));

        SimulateSleep(server, TDuration::Seconds(1));

        TInjectLockSnapshotObserver observer(runtime);

        // Start a snapshot read transaction
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

        // We will reuse this snapshot
        auto snapshot = observer.Last.MvccSnapshot;

        using NLongTxService::TLockHandle;
        TLockHandle lock1handle(123, runtime.GetActorSystem(0));
        TLockHandle lock2handle(234, runtime.GetActorSystem(0));

        // Write uncommitted changes to keys 1 and 2 using tx 123
        observer.Inject.LockId = 123;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 11), (2, 21)
                )")),
            "<empty>");
        auto locks1 = observer.LastLocks;
        observer.Inject = {};

        // Write uncommitted changes to keys 1 and 2 using tx 234
        observer.Inject.LockId = 234;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 12), (2, 22)
                )")),
            "<empty>");
        auto locks2 = observer.LastLocks;
        observer.Inject = {};

        SimulateSleep(server, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleStaleRoExec(runtime, Q_(R"(
                SELECT key, value
                FROM `/Root/table-1` VIEW by_value
                WHERE value in (1, 11, 21, 12, 22)
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

        if (WithRestart) {
            observer.BlockApplyRecords = true;
        }

        // Commit changes in tx 123
        observer.InjectClearTasks = true;
        observer.InjectLocks.emplace().Locks = locks1;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (0, 0)
                )")),
            "<empty>");
        observer.InjectClearTasks = false;
        observer.InjectLocks.reset();

        SimulateSleep(server, TDuration::Seconds(1));

        if (WithRestart) {
            UNIT_ASSERT(!observer.BlockedApplyRecords.empty());
            observer.BlockedApplyRecords.clear();
            observer.BlockApplyRecords = false;

            auto shards = GetTableShards(server, sender, "/Root/table-1");
            RebootTablet(runtime, shards.at(0), sender);

            SimulateSleep(server, TDuration::Seconds(1));
        }

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleStaleRoExec(runtime, Q_(R"(
                SELECT key, value
                FROM `/Root/table-1` VIEW by_value
                WHERE value in (1, 11, 21, 12, 22)
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 11 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 21 } }");

        // Commit changes in tx 234
        observer.InjectClearTasks = true;
        observer.InjectLocks.emplace().Locks = locks2;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (0, 0)
                )")),
            "ERROR: ABORTED");
        observer.InjectClearTasks = false;
        observer.InjectLocks.reset();
    }

    Y_UNIT_TEST(LockedWritesLimitedPerKey) {
        TPortManager pm;
        TServerSettings::TControls controls;
        controls.MutableDataShardControls()->SetEnableLockedWrites(1);
        controls.MutableDataShardControls()->SetMaxLockedWritesPerKey(2);

        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetControls(controls);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;
        CreateShardedTable(server, sender, "/Root", "table-1", 1);

        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));

        SimulateSleep(server, TDuration::Seconds(1));

        TInjectLockSnapshotObserver observer(runtime);

        // Start a snapshot read transaction
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

        // We will reuse this snapshot
        auto snapshot = observer.Last.MvccSnapshot;

        using NLongTxService::TLockHandle;
        std::optional<TLockHandle> lock1handle(std::in_place, 123, runtime.GetActorSystem(0));
        std::optional<TLockHandle> lock2handle(std::in_place, 234, runtime.GetActorSystem(0));
        std::optional<TLockHandle> lock3handle(std::in_place, 345, runtime.GetActorSystem(0));

        // Write uncommitted changes to key 2 with tx 123
        observer.Inject.LockId = 123;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 21)
                )")),
            "<empty>");
        auto locks1 = observer.LastLocks;
        observer.Inject = {};

        // Write uncommitted changes to key 2 with tx 234
        observer.Inject.LockId = 234;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 22)
                )")),
            "<empty>");
        auto locks2 = observer.LastLocks;
        observer.Inject = {};

        // Write uncommitted changes to key 2 with tx 345
        observer.Inject.LockId = 345;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 23)
                )")),
            "ERROR: GENERIC_ERROR");
        observer.Inject = {};

        // Abort tx 234, this would allow adding one more change to key 2
        lock2handle.reset();
        SimulateSleep(server, TDuration::Seconds(1));

        // Write uncommitted changes to key 2 with tx 345
        observer.Inject.LockId = 345;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 23)
                )")),
            "<empty>");
        auto locks3 = observer.LastLocks;
        observer.Inject = {};

        // Write uncommitted changes to key 3 with tx 123
        observer.Inject.LockId = 123;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 31)
                )")),
            "<empty>");
        UNIT_ASSERT(locks1 == observer.LastLocks);
        observer.Inject = {};

        // Commit changes in tx 123
        observer.InjectClearTasks = true;
        observer.InjectLocks.emplace().Locks = locks1;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (0, 0)
                )")),
            "<empty>");
        observer.InjectClearTasks = false;
        observer.InjectLocks.reset();

        // Commit changes in tx 345
        observer.InjectClearTasks = true;
        observer.InjectLocks.emplace().Locks = locks3;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (0, 0)
                )")),
            "<empty>");
        observer.InjectClearTasks = false;
        observer.InjectLocks.reset();

        // Check table has those changes visible
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 23 } }, "
            "{ items { uint32_value: 3 } items { uint32_value: 31 } }");
    }

    Y_UNIT_TEST(VolatileSnapshotRenameTimeout) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(1000);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 2);
        CreateShardedTable(server, sender, "/Root", "table-2", 2);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1), (2, 2), (3, 3);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 10), (20, 20), (30, 30);");

        auto snapshot = CreateVolatileSnapshot(server, { "/Root/table-1", "/Root/table-2" }, TDuration::MilliSeconds(10000));

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 11), (2, 22), (3, 33), (4, 44);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 11), (20, 22), (30, 33), (40, 44);");

        auto table1snapshot1 = ReadShardedTable(server, "/Root/table-1", snapshot);
        UNIT_ASSERT_VALUES_EQUAL(table1snapshot1,
            "key = 1, value = 1\n"
            "key = 2, value = 2\n"
            "key = 3, value = 3\n");

        WaitTxNotification(server, sender, AsyncMoveTable(server, "/Root/table-1", "/Root/table-1-moved"));

        auto table1snapshot2 = ReadShardedTable(server, "/Root/table-1-moved", snapshot);
        UNIT_ASSERT_VALUES_EQUAL(table1snapshot2,
            "key = 1, value = 1\n"
            "key = 2, value = 2\n"
            "key = 3, value = 3\n");

        Cerr << "---- Sleeping ----" << Endl;
        SimulateSleep(server, TDuration::Seconds(60));

        auto table1snapshot3 = ReadShardedTable(server, "/Root/table-1-moved", snapshot);
        UNIT_ASSERT_VALUES_EQUAL(table1snapshot3,
            "ERROR: WrongRequest\n");
    }

    Y_UNIT_TEST(LockedWriteWithAsyncIndexAndVolatileCommit) {
        TPortManager pm;
        TServerSettings::TControls controls;
        controls.MutableDataShardControls()->SetEnableLockedWrites(1);

        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetControls(controls);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;
        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        WaitTxNotification(server, sender,
            AsyncAlterAddIndex(server, "/Root", "/Root/table-1",
                TShardedTableOptions::TIndex{"by_value", {"value"}, {}, NKikimrSchemeOp::EIndexTypeGlobalAsync}));

        CreateShardedTable(server, sender, "/Root", "table-2", 1);
        WaitTxNotification(server, sender,
            AsyncAlterAddIndex(server, "/Root", "/Root/table-2",
                TShardedTableOptions::TIndex{"by_value", {"value"}, {}, NKikimrSchemeOp::EIndexTypeGlobalAsync}));

        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 10)"));

        SimulateSleep(server, TDuration::Seconds(1));

        TInjectLockSnapshotObserver observer(runtime);

        // Start a snapshot read transaction
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

        // We will reuse this snapshot
        auto snapshot = observer.Last.MvccSnapshot;

        using NLongTxService::TLockHandle;
        TLockHandle lock1handle(123, runtime.GetActorSystem(0));

        // Write uncommitted changes to keys 1 and 2 using tx 123
        observer.Inject.LockId = 123;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 11), (2, 21)
                )")),
            "<empty>");
        auto locks1 = observer.LastLocks;
        observer.Inject = {};

        // Write uncommitted changes to keys 10 and 20 using tx 123
        observer.Inject.LockId = 123;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 110), (20, 210)
                )")),
            "<empty>");
        auto locks2 = observer.LastLocks;
        observer.Inject = {};

        SimulateSleep(server, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleStaleRoExec(runtime, Q_(R"(
                SELECT key, value
                FROM `/Root/table-1` VIEW by_value
                WHERE value in (1, 11, 21)
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleStaleRoExec(runtime, Q_(R"(
                SELECT key, value
                FROM `/Root/table-2` VIEW by_value
                WHERE value in (10, 110, 210)
                ORDER BY key
                )")),
            "{ items { uint32_value: 10 } items { uint32_value: 10 } }");

        // Commit changes in tx 123
        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(true);
        observer.InjectClearTasks = true;
        observer.InjectLocks.emplace().AddLocks(locks1).AddLocks(locks2);
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (0, 0);
                UPSERT INTO `/Root/table-2` (key, value) VALUES (0, 0);
                )")),
            "<empty>");
        observer.InjectClearTasks = false;
        observer.InjectLocks.reset();
        runtime.GetAppData(0).FeatureFlags.ClearEnableDataShardVolatileTransactions();

        SimulateSleep(server, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleStaleRoExec(runtime, Q_(R"(
                SELECT key, value
                FROM `/Root/table-1` VIEW by_value
                WHERE value in (1, 11, 21)
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 11 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 21 } }");

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleStaleRoExec(runtime, Q_(R"(
                SELECT key, value
                FROM `/Root/table-2` VIEW by_value
                WHERE value in (10, 110, 210)
                ORDER BY key
                )")),
            "{ items { uint32_value: 10 } items { uint32_value: 110 } }, "
            "{ items { uint32_value: 20 } items { uint32_value: 210 } }");
    }

    Y_UNIT_TEST(LockedWriteWithPendingVolatileCommit) {
        TServerSettings::TControls controls;
        controls.MutableDataShardControls()->SetEnableLockedWrites(1);

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetControls(controls);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;
        auto opts = TShardedTableOptions()
                        .Columns({
                            {"key", "Uint32", true, false},
                            {"value", "Uint32", false, false},
                            {"value2", "Uint32", false, false}});
        CreateShardedTable(server, sender, "/Root", "table-1", opts);
        CreateShardedTable(server, sender, "/Root", "table-2", opts);

        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 10)"));

        SimulateSleep(runtime, TDuration::Seconds(1));

        TInjectLockSnapshotObserver observer(runtime);
        observer.BlockReadSets = true;

        Cerr << "!!! Sending volatile upsert" << Endl;
        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(true);
        TString volatileSessionId = CreateSessionRPC(runtime, "/Root");
        auto upsertResult = SendRequest(
            runtime,
            MakeSimpleRequestRPC(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 2);
                UPSERT INTO `/Root/table-2` (key, value) VALUES (20, 20);
                )", volatileSessionId, "", true),
            "/Root");
        SimulateSleep(runtime, TDuration::Seconds(1));
        runtime.GetAppData(0).FeatureFlags.ClearEnableDataShardVolatileTransactions();

        // Should be 2 expectations + 2 commit decisions
        UNIT_ASSERT(!upsertResult.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(observer.BlockedReadSets.size(), 4u);

        // Start a snapshot read transaction, make sure not to touch the uncommitted 2 key
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 1
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

        // We will reuse this snapshot
        auto snapshot = observer.Last.MvccSnapshot;

        using NLongTxService::TLockHandle;
        TLockHandle lock1handle(123, runtime.GetActorSystem(0));

        // Write uncommitted changes to keys 1 and 2 using tx 123
        observer.Inject.LockId = 123;
        observer.Inject.LockNodeId = runtime.GetNodeId(0);
        observer.Inject.MvccSnapshot = snapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value2) VALUES (1, 11), (2, 22)
                )")),
            "<empty>");
        auto locks1 = observer.LastLocks;
        observer.Inject = {};

        // Commit changes in tx 123
        observer.InjectClearTasks = true;
        observer.InjectLocks.emplace().Locks = locks1;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value2) VALUES (0, 0)
                )")),
            "<empty>");
        observer.InjectClearTasks = false;
        observer.InjectLocks.reset();

        // This compaction verifies there's no commit race with the waiting
        // distributed transaction. If commits happen in incorrect order we
        // would observe unexpected results.
        const auto shard1 = GetTableShards(server, sender, "/Root/table-1").at(0);
        const auto tableId1 = ResolveTableId(server, sender, "/Root/table-1");
        CompactTable(runtime, shard1, tableId1, false);

        observer.UnblockReadSets();
        UNIT_ASSERT_VALUES_EQUAL(
            FormatResult(AwaitResponse(runtime, std::move(upsertResult))),
            "<empty>");

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                SELECT key, value, value2 FROM `/Root/table-1`
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } items { uint32_value: 11 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } items { uint32_value: 22 } }");
    }

    Y_UNIT_TEST(ReadIteratorLocalSnapshotThenRestart) {
        NKikimrConfig::TAppConfig app;

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetAppConfig(app);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;
        TShardedTableOptions opts;
        CreateShardedTable(server, sender, "/Root", "table-1", opts);
        CreateShardedTable(server, sender, "/Root", "table-2", opts);

        const auto shards1 = GetTableShards(server, sender, "/Root/table-1");

        // Perform a snapshot read, this will persist "reads from snapshots" flag
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                SELECT key, value
                FROM `/Root/table-1`
                UNION ALL
                SELECT key, value
                FROM `/Root/table-2`
                )")),
            "");

        // Insert rows using a single-shard write
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1), (2, 2), (3, 3)"));

        bool haveReadResult = false;
        bool haveReadResultSnapshot = false;
        bool blockReads = false;
        bool blockReadAcks = true;
        bool blockReadResults = true;
        std::vector<std::unique_ptr<IEventHandle>> reads;
        std::vector<std::unique_ptr<IEventHandle>> readAcks;
        std::vector<std::unique_ptr<IEventHandle>> readResults;

        auto readObserverHolder = runtime.AddObserver<TEvDataShard::TEvRead>([&](auto& ev) {
            if (blockReads) {
                reads.emplace_back(ev.Release());
            } else {
                ev->Get()->Record.SetMaxRowsInResult(1);
            }
        });
        auto readResultObserverHolder = runtime.AddObserver<TEvDataShard::TEvReadResult>([&](auto& ev) {
            if (!haveReadResult) {
                haveReadResult = true;
                haveReadResultSnapshot = ev->Get()->Record.HasSnapshot();
            } else if (blockReadResults) {
                readResults.emplace_back(ev.Release());
            }
        });
        auto readAckObserverHolder = runtime.AddObserver<TEvDataShard::TEvReadAck>([&](auto& ev) {
            if (blockReadAcks) {
                readAcks.emplace_back(ev.Release());
            }
        });

        TString sessionId = CreateSessionRPC(runtime, "/Root");
        auto readFuture = SendRequest(runtime,
            MakeSimpleRequestRPC("SELECT key, value FROM `/Root/table-1` ORDER BY key", sessionId, "", true /* commitTx */),
            "/Root");

        auto waitFor = [&](const auto& condition, const TString& description) {
            if (!condition()) {
                Cerr << "... waiting for " << description << Endl;
                TDispatchOptions options;
                options.CustomFinalCondition = [&]() {
                    return condition();
                };
                runtime.DispatchEvents(options);
                UNIT_ASSERT_C(condition(), "... failed to wait for " << description);
            }
        };

        waitFor([&]{ return haveReadResult; }, "read result");
        UNIT_ASSERT(haveReadResultSnapshot);

        blockReads = true;
        RebootTablet(runtime, shards1.at(0), sender);
        waitFor([&]{ return reads.size() > 0; }, "read retry");
        UNIT_ASSERT_VALUES_EQUAL(reads.size(), 1u);

        // Update all keys in a single operation
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 11), (2, 22), (3, 33)"));

        blockReads = false;
        blockReadAcks = false;
        blockReadResults = false;
        readAcks.clear();
        readResults.clear();
        for (auto& ev : reads) {
            runtime.Send(ev.release(), 0, true);
        }
        reads.clear();

        auto readResponse = AwaitResponse(runtime, std::move(readFuture));
        UNIT_ASSERT_VALUES_EQUAL(
            FormatResult(readResponse),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } }, "
            "{ items { uint32_value: 3 } items { uint32_value: 3 } }");
    }

    Y_UNIT_TEST(ReadIteratorLocalSnapshotThenWrite) {
        NKikimrConfig::TAppConfig app;

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(100)
            .SetAppConfig(app);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        // Don't allow granular timecast side-stepping mediator time hacks in this test
        TBlockEvents<TEvMediatorTimecast::TEvGranularUpdate> blockGranularUpdate(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;
        TShardedTableOptions opts;
        CreateShardedTable(server, sender, "/Root", "table-1", opts);
        CreateShardedTable(server, sender, "/Root", "table-2", opts);

        // Perform a snapshot read, this will persist "reads from snapshots" flag
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                SELECT key, value
                FROM `/Root/table-1`
                UNION ALL
                SELECT key, value
                FROM `/Root/table-2`
                )")),
            "");

        // Insert rows using a single-shard write
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1), (2, 2), (3, 3)"));

        // Wait until mediator goes idle
        size_t timecastUpdates = 0;
        auto observerHolder = runtime.AddObserver([&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvMediatorTimecast::TEvUpdate::EventType: {
                    ++timecastUpdates;
                    break;
                }
                case TEvDataShard::TEvRead::EventType: {
                    auto* msg = ev->Get<TEvDataShard::TEvRead>();
                    msg->Record.SetMaxRowsInResult(1);
                    break;
                }
            }
        });

        auto waitFor = [&](const auto& condition, const TString& description) {
            if (!condition()) {
                Cerr << "... waiting for " << description << Endl;
                TDispatchOptions options;
                options.CustomFinalCondition = [&]() {
                    return condition();
                };
                runtime.DispatchEvents(options);
                UNIT_ASSERT_C(condition(), "... failed to wait for " << description);
            }
        };

        waitFor([&]{ return timecastUpdates >= 3; }, "at least 3 timecast updates");

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                SELECT key, value
                FROM `/Root/table-1`
                ORDER BY key
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } }, "
            "{ items { uint32_value: 3 } items { uint32_value: 3 } }");

        auto start = runtime.GetCurrentTime();
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 11), (2, 22), (3, 33)"));
        auto duration = runtime.GetCurrentTime() - start;
        UNIT_ASSERT_C(duration <= TDuration::MilliSeconds(200), "UPSERT takes too much time: " << duration);
    }

    Y_UNIT_TEST(UncommittedWriteRestartDuringCommit) {
        NKikimrConfig::TAppConfig app;

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(100)
            .SetAppConfig(app)
            // Bug was with non-volatile transactions
            .SetEnableDataShardVolatileTransactions(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::KQP_SESSION, NLog::PRI_TRACE);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;
        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        // Insert some initial data
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 10);"));
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 20);"));

        const auto shards1 = GetTableShards(server, sender, "/Root/table-1");

        TString sessionId, txId;

        // Start inserting a row into table-1
        Cerr << "... sending initial upsert" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 30);
                )")),
            "<empty>");

        // We want to block readsets
        std::vector<std::unique_ptr<IEventHandle>> readSets;
        auto blockReadSets = runtime.AddObserver<TEvTxProcessing::TEvReadSet>([&](TEvTxProcessing::TEvReadSet::TPtr& ev) {
            readSets.emplace_back(ev.Release());
        });

        // Start committing an additional read/write
        // Note: select on table-1 flushes accumulated changes
        // Note: select on table-2 ensures we have an outgoing readset
        Cerr << "... sending commit request" << Endl;
        auto commitFuture = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            UNION ALL
            SELECT key, value FROM `/Root/table-2`
            ORDER BY key;

            UPSERT INTO `/Root/table-2` (key, value) VALUES (4, 40);
            )"), sessionId, txId, true /* commitTx */));

        WaitFor(runtime, [&]{ return readSets.size() >= 2; }, "readset exchange");
        UNIT_ASSERT_VALUES_EQUAL(readSets.size(), 2u);

        // We want to block local boot to make sure it stays down during rollback
        std::vector<std::unique_ptr<IEventHandle>> blockedLocalBoot;
        auto blockLocalBoot = runtime.AddObserver<TEvLocal::TEvBootTablet>([&](TEvLocal::TEvBootTablet::TPtr& ev) {
            Cerr << "... blocking TEvLocal::TEvBootTablet" << Endl;
            blockedLocalBoot.emplace_back(std::move(ev.Release()));
        });

        // Kill current datashard actor with TEvPoison (so it doesn't have a chance to reply)
        Cerr << "... sending TEvPoison to " << shards1.at(0) << Endl;
        ForwardToTablet(runtime, shards1.at(0), sender, new TEvents::TEvPoison);

        // Wait until hive tries to boot a new instance (old instance is dead by that point)
        WaitFor(runtime, [&]{ return blockedLocalBoot.size() > 0; }, "blocked local boot", 3);

        // Stop blocking and resend readsets
        blockReadSets.Remove();
        Cerr << "... resending readsets" << Endl;
        for (auto& ev : readSets) {
            runtime.Send(ev.release(), 0, true);
        }
        readSets.clear();

        // Wait until commit fails with UNDETERMINED
        Cerr << "... waiting for commit result" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(
            FormatResult(AwaitResponse(runtime, std::move(commitFuture))),
            "ERROR: UNDETERMINED");

        // Sleep a little to make sure everything settles
        Cerr << "... sleeping for 1 second" << Endl;
        runtime.SimulateSleep(TDuration::Seconds(1));

        // We want to detect a restarting datashard and block its progress queue
        TActorId shard1actor;
        std::vector<std::unique_ptr<IEventHandle>> blockedProgress;
        auto blockProgressQueue = runtime.AddObserver([&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvTablet::TEvBoot::EventType: {
                    auto* msg = ev->Get<TEvTablet::TEvBoot>();
                    Cerr << "... observed TEvBoot for " << msg->TabletID << " at " << ev->GetRecipientRewrite() << Endl;
                    if (msg->TabletID == shards1.at(0)) {
                        shard1actor = ev->GetRecipientRewrite();
                    }
                    break;
                }
                case EventSpaceBegin(TKikimrEvents::ES_PRIVATE) + 0 /* EvProgressTransaction */: {
                    if (shard1actor && ev->GetRecipientRewrite() == shard1actor) {
                        Cerr << "... blocking TEvProgressTranasction at " << ev->GetRecipientRewrite() << Endl;
                        blockedProgress.emplace_back(ev.Release());
                        return;
                    }
                    break;
                }
            }
        });

        // Unblock local boot
        blockLocalBoot.Remove();
        Cerr << "... unblocking local boot" << Endl;
        for (auto& ev : blockedLocalBoot) {
            runtime.Send(ev.release(), 0, true);
        }
        blockedLocalBoot.clear();

        // Wait until a new instance starts and is blocked at progress queue handling
        WaitFor(runtime, [&]{ return blockedProgress.size() > 0; }, "blocked progress", 10);

        // Sleep a little to make sure datashard subscribes to lock and handles the response
        Cerr << "... sleeping for 1 second" << Endl;
        runtime.SimulateSleep(TDuration::Seconds(1));

        // Unblock progress queue and resend blocked messages
        Cerr << "... resending progress queue" << Endl;
        blockProgressQueue.Remove();
        for (auto& ev : blockedProgress) {
            runtime.Send(ev.release(), 0, true);
        }
        blockedProgress.clear();

        // Sleep a little to make sure everything settles
        Cerr << "... sleeping for 1 second" << Endl;
        runtime.SimulateSleep(TDuration::Seconds(1));

        // Now make a read query, we must not observe partial commit
        Cerr << "... checking final table state" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                UNION ALL
                SELECT key, value FROM `/Root/table-2`
                ORDER BY key;
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 10 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 20 } }, "
            "{ items { uint32_value: 3 } items { uint32_value: 30 } }, "
            "{ items { uint32_value: 4 } items { uint32_value: 40 } }");
    }

    /**
     * This observer forces newly created nodes to start on particular nodes
     */
    class TCreateTabletNodePinning {
    public:
        TCreateTabletNodePinning(TTestActorRuntime& runtime)
            : Runtime(runtime)
            , Node1(Runtime.GetNodeId(0))
            , Observer(Runtime.AddObserver<TEvHive::TEvCreateTablet>([this](auto& ev) { this->Process(ev); }))
        {}

        void SetNodeIndexes(std::initializer_list<ui32> nodeIndexes) {
            AllowedNodes.clear();
            for (ui32 nodeIndex : nodeIndexes) {
                AllowedNodes.push_back(Node1 + nodeIndex);
            }
        }

        void Remove() {
            Observer.Remove();
        }

    private:
        void Process(TEvHive::TEvCreateTablet::TPtr& ev) {
            auto* msg = ev->Get();
            msg->Record.ClearAllowedNodeIDs();
            for (ui32 nodeId : AllowedNodes) {
                msg->Record.AddAllowedNodeIDs(nodeId);
            }
        }

    private:
        TTestActorRuntime& Runtime;
        const ui32 Node1;
        std::vector<ui32> AllowedNodes;
        TTestActorRuntime::TEventObserverHolder Observer;
    };

    /**
     * This observer allows blocking timecast updates at different nodes
     */
    class TMediatorTimeCastBlocking {
    public:
        TMediatorTimeCastBlocking(TTestActorRuntime& runtime)
            : Runtime(runtime)
            , Node1(Runtime.GetNodeId(0))
            , Observer(Runtime.AddObserver<TEvMediatorTimecast::TEvUpdate>([this](auto& ev) { this->Process(ev); }))
        {}

        void SetMaxAllowedStep(ui32 nodeIndex, ui64 step) {
            while (Nodes.size() <= nodeIndex) {
                Nodes.emplace_back();
            }
            Nodes[nodeIndex].MaxAllowedStep = step;
        }

        void Unblock(ui32 nodeIndex) {
            while (Nodes.size() <= nodeIndex) {
                Nodes.emplace_back();
            }
            auto& state = Nodes[nodeIndex];
            state.MaxAllowedStep = Max<ui64>();
            for (auto& kv : state.Buckets) {
                if (kv.second.LastBlocked) {
                    Runtime.Send(kv.second.LastBlocked.release(), nodeIndex, true);
                }
            }
        }

        ui64 WaitNextStep() {
            ui64 step = LastStep;
            WaitFor(Runtime, [&]{ return LastStep > step; }, "next timecast step", 10);
            return LastStep;
        }

        void Remove() {
            Observer.Remove();
        }

    private:
        void Process(TEvMediatorTimecast::TEvUpdate::TPtr& ev) {
            auto* msg = ev->Get();
            ui32 nodeId = ev->GetRecipientRewrite().NodeId();
            ui32 nodeIndex = nodeId - Node1;
            auto bucket = msg->Record.GetBucket();
            auto step = msg->Record.GetTimeBarrier();
            Cerr << "... observed step " << step << " at node " << nodeId << " bucket " << bucket << Endl;
            LastStep = Max(LastStep, step);
            while (Nodes.size() <= nodeIndex) {
                Nodes.emplace_back();
            }
            auto& state = Nodes[nodeIndex];
            auto& bucketState = state.Buckets[bucket];
            ui64 prevObserved = bucketState.LastObservedStep;
            bucketState.LastObservedStep = step;
            bucketState.LastBlocked.reset();
            if (step > state.MaxAllowedStep) {
                Cerr << "... blocked step " << step << " at node " << nodeId << " bucket " << bucket << Endl;
                // Generate an blocked event we would resend when on unblock
                auto* newMsg = new TEvMediatorTimecast::TEvUpdate;
                newMsg->Record = msg->Record;
                bucketState.LastBlocked.reset(new IEventHandle(
                    ev->Recipient,
                    ev->Sender,
                    newMsg,
                    ev->Flags,
                    ev->Cookie));
                bucketState.LastBlocked->Rewrite(ev->GetTypeRewrite(), ev->GetRecipientRewrite());
                // Modify the original event to send the max allowed step when necessary
                if (prevObserved < state.MaxAllowedStep) {
                    Cerr << "... synthetic step " << state.MaxAllowedStep << " at node " << nodeId << " bucket " << bucket << Endl;
                    msg->Record.SetTimeBarrier(state.MaxAllowedStep);
                } else {
                    ev.Reset();
                }
                return;
            }
        }

    private:
        struct TBucketState {
            ui64 LastObservedStep = 0;
            std::unique_ptr<IEventHandle> LastBlocked;
        };

        struct TNodeState {
            ui64 MaxAllowedStep = Max<ui64>();
            std::map<ui32, TBucketState> Buckets;
        };

    public:
        ui64 LastStep = 0;

    private:
        TTestActorRuntime& Runtime;
        const ui32 Node1;
        std::vector<TNodeState> Nodes;
        TTestActorRuntime::TEventObserverHolder Observer;
    };

    /**
     * Observer for blocking split at src tablets
     */
    class TSplitSrcBlocking {
    public:
        TSplitSrcBlocking(TTestActorRuntime& runtime)
            : Runtime(runtime)
            , Node1(Runtime.GetNodeId(0))
            , Observer(Runtime.AddObserver<TEvDataShard::TEvSplit>([this](auto& ev) { this->Process(ev); }))
        {}

        void Unblock() {
            Observer.Remove();
            for (auto& ev : Blocked) {
                ui32 nodeIndex = ev->GetRecipientRewrite().NodeId() - Node1;
                Runtime.Send(ev.release(), nodeIndex, true);
            }
            Blocked.clear();
        }

    private:
        void Process(TEvDataShard::TEvSplit::TPtr& ev) {
            Cerr << "... blocking TEvSplit" << Endl;
            Blocked.emplace_back(ev.Release());
        }

    private:
        TTestActorRuntime& Runtime;
        const ui32 Node1;
        std::vector<std::unique_ptr<IEventHandle>> Blocked;
        TTestActorRuntime::TEventObserverHolder Observer;
    };

    Y_UNIT_TEST(RepeatableReadAfterSplitRace) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetNodeCount(2)
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(100);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        // Don't allow granular timecast side-stepping mediator time hacks in this test
        TBlockEvents<TEvMediatorTimecast::TEvGranularUpdate> blockGranularUpdate(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::KQP_SESSION, NLog::PRI_TRACE);

        InitRoot(server, sender);

        TCreateTabletNodePinning createTabletNodePinning(runtime);

        // Create table-1 at node 1
        createTabletNodePinning.SetNodeIndexes({ 0 });
        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        auto shards1 = GetTableShards(server, sender, "/Root/table-1");

        // Create table-2 at node 2
        createTabletNodePinning.SetNodeIndexes({ 1 });
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        // Insert initial values
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 10);"));
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 20);"));

        TSplitSrcBlocking splitSrcBlocking(runtime);
        TMediatorTimeCastBlocking mediatorTimeCastBlocking(runtime);

        // We need to make a snapshot read to force unprotected reads
        // This will also ensure both nodes have an updated mediator time cast
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                UNION ALL
                SELECT key, value FROM `/Root/table-2`
                ORDER BY key;
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 10 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 20 } }");

        // One more upsert to table-2, this will bump mediator time at node 2 past the snapshot
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (3, 30);"));

        // We won't allow node 2 to go past that write version
        mediatorTimeCastBlocking.SetMaxAllowedStep(1, mediatorTimeCastBlocking.LastStep);

        // Start split of table-1 at key 10
        auto splitSender = runtime.AllocateEdgeActor();
        SetSplitMergePartCountLimit(server->GetRuntime(), -1);
        Cerr << "... starting split of table-1" << Endl;
        ui64 splitTxId = AsyncSplitTable(server, splitSender, "/Root/table-1", shards1.at(0), 10);

        // We want to mediator time to advance, so the next snapshot is greater than the last write version
        mediatorTimeCastBlocking.WaitNextStep();

        // Start a snapshot read from table-1
        // This will run at node 1 shard where mediator time is recent
        Cerr << "... reading from table-1" << Endl;
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                ORDER BY key;
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 10 } }");

        // Now unblock TEvSplit and wait until it finishes
        splitSrcBlocking.Unblock();

        // Wait until split finishes, so we can continue working with the new shards
        Cerr << "... waiting for split to finish" << Endl;
        WaitTxNotification(server, splitSender, splitTxId);

        // Start an upsert into newly split table-1
        TString upsertSessionId = CreateSessionRPC(runtime);
        auto upsertFuture = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (4, 40);
            )"), upsertSessionId, "", /* commit */ true));

        // We cannot wait for it to finish (bug may be fixed)
        // So we sleep for several seconds instead
        Cerr << "... sleeping for 2 seconds" << Endl;
        runtime.SimulateSleep(TDuration::Seconds(2));

        // Unblock mediator timecast at node 2
        mediatorTimeCastBlocking.Unblock(1);

        // Wait until upsert finishes
        UNIT_ASSERT_VALUES_EQUAL(
            FormatResult(AwaitResponse(runtime, std::move(upsertFuture))),
            "<empty>");

        // Repeat read in a previous tx, this read must be repeatable
        // In other words we must not observe the new 4/40 row
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleContinue(runtime, sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                UNION ALL
                SELECT key, value FROM `/Root/table-2`
                ORDER BY key;
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 10 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 20 } }, "
            "{ items { uint32_value: 3 } items { uint32_value: 30 } }");
    }

    Y_UNIT_TEST(DelayedWriteReadableAfterSplit) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetNodeCount(2)
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(100);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        // Don't allow granular timecast side-stepping mediator time hacks in this test
        TBlockEvents<TEvMediatorTimecast::TEvGranularUpdate> blockGranularUpdate(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::KQP_SESSION, NLog::PRI_TRACE);

        InitRoot(server, sender);

        TCreateTabletNodePinning createTabletNodePinning(runtime);

        // Create table-1 at node 1
        createTabletNodePinning.SetNodeIndexes({ 0 });
        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        auto shards1 = GetTableShards(server, sender, "/Root/table-1");

        // Create table-2 at node 2
        createTabletNodePinning.SetNodeIndexes({ 1 });
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        // Insert initial values
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 10);"));
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 20);"));

        TSplitSrcBlocking splitSrcBlocking(runtime);
        TMediatorTimeCastBlocking mediatorTimeCastBlocking(runtime);

        // We need to make a snapshot read to force unprotected reads
        // This will also ensure both nodes have an updated mediator time cast
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                UNION ALL
                SELECT key, value FROM `/Root/table-2`
                ORDER BY key;
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 10 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 20 } }");

        // One more upsert to table-2, this will bump mediator time past the snapshot
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (3, 30);"));

        // Wait for the next step, we will expect to read at that step
        ui64 step = mediatorTimeCastBlocking.WaitNextStep();
        Cerr << "... expecting next read at step " << step << Endl;

        // We won't allow node 2 to go past that snapshot
        mediatorTimeCastBlocking.SetMaxAllowedStep(1, step);

        // Start a snapshot read from table-1
        // This will run at node 1 shard where mediator time is recent
        Cerr << "... reading from table-1" << Endl;
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                ORDER BY key;
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 10 } }");

        // Start split of table-1 at key 10
        auto splitSender = runtime.AllocateEdgeActor();
        SetSplitMergePartCountLimit(server->GetRuntime(), -1);
        Cerr << "... starting split of table-1" << Endl;
        ui64 splitTxId = AsyncSplitTable(server, splitSender, "/Root/table-1", shards1.at(0), 10);

        // Perform an immediate write, which will happen after the above snapshot
        // We also wait for the result to make sure mediator time advances at node 1
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (4, 40);
            )")),
            "<empty>");

        // Unblock split at src
        splitSrcBlocking.Unblock();

        // Wait until split finishes, so we can continue working with the new shards
        Cerr << "... waiting for split to finish" << Endl;
        WaitTxNotification(server, splitSender, splitTxId);

        // Start an immediate read from the new left shard of table-1
        TString readSessionId = CreateSessionRPC(runtime);
        Cerr << "... starting immediate read from table-1" << Endl;
        auto readFuture = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            WHERE key <= 5
            ORDER BY key;
            )"), readSessionId, "", /* commit */ true));

        // We cannot wait for it to finish (bug may be fixed)
        // So we sleep for several seconds instead
        Cerr << "... sleeping for 2 seconds" << Endl;
        runtime.SimulateSleep(TDuration::Seconds(2));

        // Unblock mediator timecast at node 2
        mediatorTimeCastBlocking.Unblock(1);

        // Wait until read finishes, we must observe previously inserted row
        Cerr << "... waiting for table-1 read to finish" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(
            FormatResult(AwaitResponse(runtime, std::move(readFuture))),
            "{ items { uint32_value: 1 } items { uint32_value: 10 } }, "
            "{ items { uint32_value: 4 } items { uint32_value: 40 } }");

        // Repeat read in a previous tx, this read must be repeatable
        // In other words we must not observe the new 4/40 row
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleContinue(runtime, sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                UNION ALL
                SELECT key, value FROM `/Root/table-2`
                ORDER BY key;
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 10 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 20 } }, "
            "{ items { uint32_value: 3 } items { uint32_value: 30 } }");
    }

    Y_UNIT_TEST(DelayedWriteReplyAfterSplit) {
        TPortManager pm;
        TServerSettings::TControls controls;
        // This test needs to make sure mediator time does not advance while
        // certain operations are running. Unfortunately, volatile planning
        // may happen every 1ms, and it's too hard to guarantee time stays
        // still for such a short time. We disable volatile planning to make
        // coordinator ticks are 100ms apart.
        controls.MutableCoordinatorControls()->SetVolatilePlanLeaseMs(0);
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetNodeCount(2)
            .SetControls(controls)
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(100);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        // Don't allow granular timecast side-stepping mediator time hacks in this test
        TBlockEvents<TEvMediatorTimecast::TEvGranularUpdate> blockGranularUpdate(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_COORDINATOR, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_MEDIATOR_TIMECAST, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::KQP_SESSION, NLog::PRI_TRACE);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;
        TCreateTabletNodePinning createTabletNodePinning(runtime);

        // Create table-1 at node 1
        createTabletNodePinning.SetNodeIndexes({ 0 });
        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        auto shards1 = GetTableShards(server, sender, "/Root/table-1");

        // Create table-2 at node 2
        createTabletNodePinning.SetNodeIndexes({ 1 });
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        // Insert initial values
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 10);"));
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 20);"));

        TSplitSrcBlocking splitSrcBlocking(runtime);
        TMediatorTimeCastBlocking mediatorTimeCastBlocking(runtime);

        // We need to make a snapshot read to force unprotected reads
        // This will also ensure both nodes have an updated mediator time cast
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                UNION ALL
                SELECT key, value FROM `/Root/table-2`
                ORDER BY key;
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 10 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 20 } }");

        // One more upsert to table-2, this will bump mediator time past the snapshot
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (3, 30);"));

        // Wait for the next step, we will expect to read at that step
        ui64 step = mediatorTimeCastBlocking.WaitNextStep();
        Cerr << "... expecting next read at step " << step << Endl;

        // We won't allow neither node 1 nor node 2 to go past that snapshot
        mediatorTimeCastBlocking.SetMaxAllowedStep(0, step);
        mediatorTimeCastBlocking.SetMaxAllowedStep(1, step);

        // Start a snapshot read from table-1
        // This will run at node 1 shard
        Cerr << "... reading from table-1" << Endl;
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                ORDER BY key;
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 10 } }");

        // Start split of table-1 at key 10
        auto splitSender = runtime.AllocateEdgeActor();
        SetSplitMergePartCountLimit(server->GetRuntime(), -1);
        Cerr << "... starting split of table-1" << Endl;
        ui64 splitTxId = AsyncSplitTable(server, splitSender, "/Root/table-1", shards1.at(0), 10);

        // Start upserting a row into table-1, since mediator time is blocked
        // this will not be able to reply until mediator time advances.
        TString upsertSessionId = CreateSessionRPC(runtime);
        Cerr << "... starting upsert into table-1" << Endl;
        auto upsertFuture = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (4, 40);
            )"), upsertSessionId, "", /* commit */ true));

        // Sleep a little, so the above upsert is fully executed at future step and reply is enqueued
        runtime.SimulateSleep(TDuration::MicroSeconds(100));

        // We expect mediator time to be at the last step
        UNIT_ASSERT_VALUES_EQUAL(mediatorTimeCastBlocking.LastStep, step);

        // Now unblock the split and wait until it finishes
        splitSrcBlocking.Unblock();
        Cerr << "... waiting for split to finish" << Endl;
        WaitTxNotification(server, splitSender, splitTxId);

        // We expect mediator time to still be at the last step
        UNIT_ASSERT_VALUES_EQUAL(mediatorTimeCastBlocking.LastStep, step);

        // The new shard should be ready to take requests, validate by an immediate read
        // The new row must not be visible, since mediator time is still in the past,
        // and the write could not have sent a successful reply yet.
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key <= 5
                ORDER BY key;
                )")),
            "{ items { uint32_value: 1 } items { uint32_value: 10 } }");

        // Now that we confirmed the new shard is readable, allow mediator time to advance at node 1
        mediatorTimeCastBlocking.Unblock(0);

        // Sleep for a couple of seconds and check whether upsert has replied
        runtime.SimulateSleep(TDuration::Seconds(2));

        // When upsert replies, only errors are allowed, otherwise new reads
        // at node 2 would keep reading stale values.
        if (upsertFuture.HasValue() || upsertFuture.HasException()) {
            Cerr << "... upsert finished before unblocking node 2" << Endl;
            auto upsertResult = FormatResult(upsertFuture.ExtractValueSync());
            if (upsertResult == "<empty>") {
                // It was successful, double check that node 2 can see the result
                // For obvious reasons it cannot (due to blocked mediator time)
                UNIT_ASSERT_VALUES_EQUAL(
                    KqpSimpleExec(runtime, Q_(R"(
                        SELECT key, value FROM `/Root/table-1`
                        WHERE key <= 5
                        ORDER BY key;
                        )")),
                    "{ items { uint32_value: 1 } items { uint32_value: 10 } }, "
                    "{ items { uint32_value: 4 } items { uint32_value: 40 } }, ");
            } else {
                // Otherwise we must get an UNDETERMINED error
                // The upsert hasn't happened yet, but will happen later
                UNIT_ASSERT_VALUES_EQUAL(
                    upsertResult,
                    "ERROR: UNDETERMINED");
            }
        } else {
            // Try to be future proof, in case we implement waiting with dst shards
            Cerr << "... upsert did not finish before unblocking node 2" << Endl;
        }
    }

    Y_UNIT_TEST(PostMergeNotCompactedTooEarly) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(100);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        KqpSchemeExec(runtime, R"(
            CREATE TABLE `/Root/table` (key int, value bytes, PRIMARY KEY (key))
            WITH (AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1,
                  PARTITION_AT_KEYS = (5));
        )");

        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);
        const auto tableId = ResolveTableId(server, sender, "/Root/table");

        for (int i = 0; i < 20; ++i) {
            Cerr << "... upserting key " << i << Endl;
            auto query = Sprintf(R"(
                UPSERT INTO `/Root/table` (key, value) VALUES (%d, '%s');
            )", i, TString(128 * 1024, 'x').c_str());
            ExecSQL(server, sender, query);
            if (i >= 5) {
                Cerr << "... compacting shard " << shards.at(1) << Endl;
                CompactTable(runtime, shards.at(1), tableId, false);
            } else if (i == 4) {
                Cerr << "... compacting shard " << shards.at(0) << Endl;
                CompactTable(runtime, shards.at(0), tableId, false);
            }
        }

        // Read (and snapshot) current data, so it doesn't go away on compaction
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, "SELECT COUNT(*) FROM `/Root/table`;"),
            "{ items { uint64_value: 20 } }");

        // Delete all the data in shard 0, this is small and will stay in memtable
        // But when borrowed dst compaction will have pressure to compact it all
        ExecSQL(server, sender, "DELETE FROM `/Root/table` WHERE key < 5");

        std::vector<TEvDataShard::TEvSplitTransferSnapshot::TPtr> snapshots;
        auto captureSnapshots = runtime.AddObserver<TEvDataShard::TEvSplitTransferSnapshot>(
            [&](TEvDataShard::TEvSplitTransferSnapshot::TPtr& ev) {
                auto* msg = ev->Get();
                Cerr << "... captured snapshot from " << msg->Record.GetSrcTabletId() << Endl;
                snapshots.emplace_back(ev.Release());
            });

        Cerr << "... merging table" << Endl;
        SetSplitMergePartCountLimit(server->GetRuntime(), -1);
        ui64 txId = AsyncMergeTable(server, sender, "/Root/table", shards);
        Cerr << "... started merge " << txId << Endl;
        WaitFor(runtime, [&]{ return snapshots.size() >= 2; }, "both src tablet snapshots");

        std::vector<TEvBlobStorage::TEvGet::TPtr> gets;
        auto captureGets = runtime.AddObserver<TEvBlobStorage::TEvGet>(
            [&](TEvBlobStorage::TEvGet::TPtr& ev) {
                auto* msg = ev->Get();
                if (msg->Queries[0].Id.TabletID() == shards.at(1)) {
                    Cerr << "... blocking blob get of " << msg->Queries[0].Id << Endl;
                    gets.emplace_back(ev.Release());
                }
            });

        // Release snapshot for shard 0 then shard 1
        captureSnapshots.Remove();
        Cerr << "... unlocking snapshots from tablet " << shards.at(0) << Endl;
        for (auto& ev : snapshots) {
            if (ev && ev->Get()->Record.GetSrcTabletId() == shards.at(0)) {
                runtime.Send(ev.Release(), 0, true);
            }
        }
        Cerr << "... unblocking snapshots from tablet " << shards.at(1) << Endl;
        for (auto& ev : snapshots) {
            if (ev && ev->Get()->Record.GetSrcTabletId() == shards.at(1)) {
                runtime.Send(ev.Release(), 0, true);
            }
        }

        // Let it commit above snapshots and incorrectly compact after the first one is loaded and merged
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT(gets.size() > 0);

        Cerr << "... unblocking blob gets" << Endl;
        captureGets.Remove();
        for (auto& ev : gets) {
            runtime.Send(ev.Release(), 0, true);
        }

        // Let it finish loading the second snapshot
        runtime.SimulateSleep(TDuration::Seconds(1));

        // Wait for merge to complete and start a borrowed compaction
        // When bug is present it will cause newly compacted to part to have epoch larger than previously compacted
        WaitTxNotification(server, sender, txId);
        const auto merged = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(merged.size(), 1u);
        Cerr << "... compacting borrowed parts in shard " << merged.at(0) << Endl;
        CompactBorrowed(runtime, merged.at(0), tableId);

        // Validate we have an expected number of rows
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, "SELECT COUNT(*) FROM `/Root/table`;"),
            "{ items { uint64_value: 15 } }");
    }

    Y_UNIT_TEST(PipelineAndMediatorRestoreRace) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(100)
            .SetEnableDataShardVolatileTransactions(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table1` (key int, value int, PRIMARY KEY (key));
                CREATE TABLE `/Root/table2` (key int, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS");

        const auto shards1 = GetTableShards(server, sender, "/Root/table1");
        UNIT_ASSERT_VALUES_EQUAL(shards1.size(), 1u);

        // Upsert initial data
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                UPSERT INTO `/Root/table1` (key, value) VALUES (1, 10);
                UPSERT INTO `/Root/table2` (key, value) VALUES (2, 20);
            )"),
            "<empty>");

        // Make sure shards have unprotected reads enabled
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table1`
                UNION ALL
                SELECT key, value FROM `/Root/table2`
                ORDER BY key;
            )"),
            "{ items { int32_value: 1 } items { int32_value: 10 } }, "
            "{ items { int32_value: 2 } items { int32_value: 20 } }");

        std::vector<TEvTxProcessing::TEvReadSet::TPtr> readsets;
        auto blockReadSets = runtime.AddObserver<TEvTxProcessing::TEvReadSet>(
            [&](TEvTxProcessing::TEvReadSet::TPtr& ev) {
                auto* msg = ev->Get();
                Cerr << "... blocking readset for " << msg->Record.GetTabletDest() << Endl;
                readsets.push_back(std::move(ev));
            });

        size_t planSteps = 0;
        auto observePlanSteps = runtime.AddObserver<TEvTxProcessing::TEvPlanStep>(
            [&](TEvTxProcessing::TEvPlanStep::TPtr& ev) {
                auto* msg = ev->Get();
                Cerr << "... observed plan step " << msg->Record.GetStep() << " for " << msg->Record.GetTabletID() << Endl;
                ++planSteps;
            });

        // Create a "staircase" of transactions at different steps

        // Upsert1 will have outgoing readsets from both shards
        Cerr << "... sending upsert1" << Endl;
        auto upsert1 = KqpSimpleSend(runtime, R"(
            SELECT key, value FROM `/Root/table1` WHERE key = 1;
            SELECT key, value FROM `/Root/table2` WHERE key = 2;
            UPSERT INTO `/Root/table1` (key, value) VALUES (3, 30), (5, 50);
            UPSERT INTO `/Root/table2` (key, value) VALUES (4, 40);
        )");
        WaitFor(runtime, [&]{ return planSteps >= 2; }, "upsert1 plan step");
        UNIT_ASSERT_VALUES_EQUAL(planSteps, 2u);
        WaitFor(runtime, [&]{ return readsets.size() >= 2; }, "upsert1 readsets");
        UNIT_ASSERT_VALUES_EQUAL(readsets.size(), 2u);

        // Upsert2 will be blocked by dependencies (key 5) at table1, but not table2
        Cerr << "... sending upsert2" << Endl;
        auto upsert2 = KqpSimpleSend(runtime, R"(
            SELECT key, value FROM `/Root/table2` WHERE key = 2;
            UPSERT INTO `/Root/table1` (key, value) VALUES (5, 55), (7, 70);
        )");
        WaitFor(runtime, [&]{ return planSteps >= 4; }, "upsert2 plan step");
        UNIT_ASSERT_VALUES_EQUAL(planSteps, 4u);
        WaitFor(runtime, [&]{ return readsets.size() >= 3; }, "upsert2 readset from table2");
        UNIT_ASSERT_VALUES_EQUAL(readsets.size(), 3u);

        // Upsert3 will be blocked by dependencies (key 7) at table1, but not table2
        Cerr << "... sending upsert3" << Endl;
        auto upsert3 = KqpSimpleSend(runtime, R"(
            SELECT key, value FROM `/Root/table2` WHERE key = 2;
            UPSERT INTO `/Root/table1` (key, value) VALUES (7, 77), (9, 90);
        )");
        WaitFor(runtime, [&]{ return planSteps >= 6; }, "upsert3 plan step");
        UNIT_ASSERT_VALUES_EQUAL(planSteps, 6u);
        WaitFor(runtime, [&]{ return readsets.size() >= 4; }, "upsert3 readset from table2");
        UNIT_ASSERT_VALUES_EQUAL(readsets.size(), 4u);

        // Sleep a little to make sure everything is persisted at table1 and mediator time advanced
        runtime.SimulateSleep(TDuration::MilliSeconds(200));

        // Now restart table1 shard while blocking mediator timecast registration
        std::vector<TEvMediatorTimecast::TEvRegisterTabletResult::TPtr> registrations;
        auto blockRegistrations = runtime.AddObserver<TEvMediatorTimecast::TEvRegisterTabletResult>(
            [&](TEvMediatorTimecast::TEvRegisterTabletResult::TPtr& ev) {
                Cerr << "... blocking timecast registration result for " << ev->GetRecipientRewrite() << Endl;
                registrations.push_back(std::move(ev));
            });

        // ... waiting for the new tablet actor booting
        TActorId shardActor;
        auto waitBoot = runtime.AddObserver<TEvTablet::TEvBoot>(
            [&](TEvTablet::TEvBoot::TPtr& ev) {
                auto* msg = ev->Get();
                if (msg->TabletID == shards1.at(0)) {
                    shardActor = ev->GetRecipientRewrite();
                    Cerr << "... booting " << msg->TabletID << " with actor " << shardActor << Endl;
                }
            });

        // ... and blocking progress transactions
        size_t allowProgress = 0;
        std::vector<TAutoPtr<IEventHandle>> blockedProgress;
        auto blockProgress = runtime.AddObserver([&](TAutoPtr<IEventHandle>& ev) {
            if (shardActor &&
                ev->GetRecipientRewrite() == shardActor &&
                ev->GetTypeRewrite() == EventSpaceBegin(TKikimrEvents::ES_PRIVATE) + 0 /* EvProgressTransaction */)
            {
                if (allowProgress > 0) {
                    Cerr << "... allowing EvProgressTransaction for " << ev->GetRecipientRewrite() << Endl;
                    --allowProgress;
                } else {
                    Cerr << "... blocking EvProgressTransaction for " << ev->GetRecipientRewrite() << Endl;
                    blockedProgress.push_back(std::move(ev));
                }
            }
        });

        Cerr << "... rebooting " << shards1.at(0) << Endl;
        GracefulRestartTablet(runtime, shards1.at(0), sender);

        WaitFor(runtime, [&]{ return registrations.size() >= 1; }, "timecast registration");
        UNIT_ASSERT_VALUES_EQUAL(registrations.size(), 1u);

        WaitFor(runtime, [&]{ return readsets.size() >= 8; }, "readsets resent");
        UNIT_ASSERT_VALUES_EQUAL(readsets.size(), 8u);

        // We need to unblock two transactions
        // The first is already marked incomplete
        // The second will be added to the pipeline, but blocked by dependencies
        for (int i = 0; i < 2; ++i) {
            WaitFor(runtime, [&]{ return blockedProgress.size() >= 1; }, "blocked progress");
            UNIT_ASSERT_VALUES_EQUAL(blockedProgress.size(), 1);

            Cerr << "... unblocking a single progress tx" << Endl;
            allowProgress += blockedProgress.size();
            for (auto& ev : blockedProgress) {
                runtime.Send(ev.Release(), 0, true);
            }
            blockedProgress.clear();
        }

        WaitFor(runtime, [&]{ return blockedProgress.size() >= 1; }, "blocked progress");
        UNIT_ASSERT_VALUES_EQUAL(blockedProgress.size(), 1);

        runtime.SimulateSleep(TDuration::MilliSeconds(1));

        Cerr << "... unblocking timecast registration" << Endl;
        blockRegistrations.Remove();
        for (auto& ev : registrations) {
            runtime.Send(ev.Release(), 0, true);
        }

        runtime.SimulateSleep(TDuration::MilliSeconds(200));

        // Unblock the final transaction
        // It's going to be before the restored worst-case unprotected read edge
        // However because it's no the complete/incomplete tail it will not update lists properly
        Cerr << "... unblocking final progress tx" << Endl;
        blockProgress.Remove();
        for (auto& ev : blockedProgress) {
            runtime.Send(ev.Release(), 0, true);
        }
        blockedProgress.clear();

        runtime.SimulateSleep(TDuration::MilliSeconds(1));

        // Perform snapshot read that will try to mark all pending transactions as logically complete/incomplete
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table1` WHERE key = 1
                UNION ALL
                SELECT key, value FROM `/Root/table2` WHERE key = 2
                ORDER BY key;
            )"),
            "{ items { int32_value: 1 } items { int32_value: 10 } }, "
            "{ items { int32_value: 2 } items { int32_value: 20 } }");

        Cerr << "... unblocking readsets" << Endl;
        blockReadSets.Remove();
        for (auto& ev : readsets) {
            runtime.Send(ev.Release(), 0, true);
        }
        readsets.clear();

        UNIT_ASSERT_VALUES_EQUAL(
            FormatResult(AwaitResponse(runtime, std::move(upsert1))),
            "{ items { int32_value: 1 } items { int32_value: 10 } }\n"
            "{ items { int32_value: 2 } items { int32_value: 20 } }");

        UNIT_ASSERT_VALUES_EQUAL(
            FormatResult(AwaitResponse(runtime, std::move(upsert2))),
            "{ items { int32_value: 2 } items { int32_value: 20 } }");

        UNIT_ASSERT_VALUES_EQUAL(
            FormatResult(AwaitResponse(runtime, std::move(upsert3))),
            "{ items { int32_value: 2 } items { int32_value: 20 } }");
    }

    Y_UNIT_TEST(UncommittedChangesRenameTable) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(100)
            .SetEnableDataShardVolatileTransactions(true);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table1` (key int, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS");

        ExecSQL(server, sender, "UPSERT INTO `/Root/table1` (key, value) VALUES (2, 22);");

        TString sessionId = CreateSessionRPC(runtime);
        TString txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, R"(
                UPSERT INTO `/Root/table1` (key, value) VALUES (1, 11), (3, 33);
                SELECT key, value FROM `/Root/table1` ORDER BY key;
            )"),
            "{ items { int32_value: 1 } items { int32_value: 11 } }, "
            "{ items { int32_value: 2 } items { int32_value: 22 } }, "
            "{ items { int32_value: 3 } items { int32_value: 33 } }");

        auto shards = GetTableShards(server, sender, "/Root/table1");
        auto tableId1 = ResolveTableId(server, sender, "/Root/table1");

        // Check shard has open transactions
        {
            runtime.SendToPipe(shards.at(0), sender, new TEvDataShard::TEvGetOpenTxs(tableId1.PathId));
            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvGetOpenTxsResult>(sender);
            UNIT_ASSERT_C(!ev->Get()->OpenTxs.empty(), "at shard " << shards.at(0));
        }

        WaitTxNotification(server, sender, AsyncMoveTable(server, "/Root/table1", "/Root/table1moved"));
        auto tableId2 = ResolveTableId(server, sender, "/Root/table1moved");

        runtime.SimulateSleep(TDuration::Seconds(1));

        // Check shard doesn't have open transactions
        {
            runtime.SendToPipe(shards.at(0), sender, new TEvDataShard::TEvGetOpenTxs(tableId2.PathId));
            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvGetOpenTxsResult>(sender);
            UNIT_ASSERT_C(ev->Get()->OpenTxs.empty(), "at shard " << shards.at(0));
        }

        RebootTablet(runtime, shards.at(0), sender);

        // The original table was removed
        // We must not be able to commit the transaction
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleCommit(runtime, sessionId, txId, "SELECT 1"),
            "ERROR: ABORTED");

        runtime.SimulateSleep(TDuration::Seconds(1));

        // Check shard doesn't have open transactions
        {
            runtime.SendToPipe(shards.at(0), sender, new TEvDataShard::TEvGetOpenTxs(tableId2.PathId));
            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvGetOpenTxsResult>(sender);
            UNIT_ASSERT_C(ev->Get()->OpenTxs.empty(), "at shard " << shards.at(0));
        }
    }

}

} // namespace NKikimr

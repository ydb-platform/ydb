#include "datashard_ut_common.h"
#include "datashard_ut_common_kqp.h"
#include "datashard_active_transaction.h"

#include <ydb/core/formats/factory.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <ydb/core/kqp/ut/common/kqp_ut_common.h> // Y_UNIT_TEST_(TWIN|QUAD)

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
                const TVector<std::pair<TString, NScheme::TTypeId>>& columns,
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
        TVector<std::pair<TString, NScheme::TTypeId>> Columns;
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

    Y_UNIT_TEST_WITH_MVCC(VolatileSnapshotSplit) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetEnableMvcc(WithMvcc)
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

    Y_UNIT_TEST_WITH_MVCC(VolatileSnapshotMerge) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetEnableMvcc(WithMvcc)
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

    Y_UNIT_TEST_WITH_MVCC(VolatileSnapshotAndLocalMKQLUpdate) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetEnableMvcc(WithMvcc)
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

    Y_UNIT_TEST_WITH_MVCC(VolatileSnapshotReadTable) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetEnableMvcc(WithMvcc)
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.GetAppData().AllowReadTableImmediate = true;

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

    Y_UNIT_TEST_WITH_MVCC(VolatileSnapshotRefreshDiscard) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetEnableMvcc(WithMvcc)
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.GetAppData().AllowReadTableImmediate = true;

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

    Y_UNIT_TEST_WITH_MVCC(VolatileSnapshotTimeout) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetEnableMvcc(WithMvcc)
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(1000);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.GetAppData().AllowReadTableImmediate = true;

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

    Y_UNIT_TEST_WITH_MVCC(VolatileSnapshotTimeoutRefresh) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetEnableMvcc(WithMvcc)
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(1000);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.GetAppData().AllowReadTableImmediate = true;

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

    Y_UNIT_TEST_WITH_MVCC(VolatileSnapshotCleanupOnReboot) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetEnableMvcc(WithMvcc)
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(1000);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.GetAppData().AllowReadTableImmediate = true;

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

    Y_UNIT_TEST_WITH_MVCC(VolatileSnapshotCleanupOnFinish) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetEnableMvcc(WithMvcc)
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(1000);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.GetAppData().AllowReadTableImmediate = true;

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

    // Regression test for KIKIMR-12289
    Y_UNIT_TEST(VolatileSnapshotCleanupOnReboot_KIKIMR_12289) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMvcc(false)
            .SetDomainPlanResolution(1000);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.GetAppData().AllowReadTableImmediate = true;

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

        // Verify snapshot is not among removed versions at the first shard
        for (TString table : { "/Root/table-1" }) {
            auto shards = GetTableShards(server, sender, table);
            auto ranges = GetRemovedRowVersions(server, shards.at(0));
            UNIT_ASSERT(ranges.size() >= 1);
            UNIT_ASSERT_VALUES_EQUAL(ranges.begin()->Lower, TRowVersion::Min());
            UNIT_ASSERT_VALUES_EQUAL(ranges.begin()->Upper, snapshot);
        }

        RebootTablet(runtime, shards1[0], sender);

        // Snapshots table should be cleaned up on reboot
        UNIT_ASSERT_VALUES_EQUAL(GetSnapshotCount(runtime, shards1[0]), 0u);

        // Verify snapshot is removed at the first shard
        for (TString table : { "/Root/table-1" }) {
            auto shards = GetTableShards(server, sender, table);
            auto ranges = GetRemovedRowVersions(server, shards.at(0));
            UNIT_ASSERT(ranges.size() >= 1);
            UNIT_ASSERT_VALUES_EQUAL(ranges.begin()->Lower, TRowVersion::Min());
            UNIT_ASSERT(ranges.begin()->Upper > snapshot);
        }
    }

    // Regression test for KIKIMR-12289
    Y_UNIT_TEST(VolatileSnapshotCleanupOnFinish_KIKIMR_12289) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMvcc(false)
            .SetDomainPlanResolution(1000);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.GetAppData().AllowReadTableImmediate = true;

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

        // Verify snapshot is not among removed versions at the first shard
        for (TString table : { "/Root/table-1" }) {
            auto shards = GetTableShards(server, sender, table);
            auto ranges = GetRemovedRowVersions(server, shards.at(0));
            UNIT_ASSERT(ranges.size() >= 1);
            UNIT_ASSERT_VALUES_EQUAL(ranges.begin()->Lower, TRowVersion::Min());
            UNIT_ASSERT_VALUES_EQUAL(ranges.begin()->Upper, snapshot);
        }

        // Resume paused ReadTable and check the result
        ResumeReadShardedTable(server, state);
        UNIT_ASSERT_VALUES_EQUAL(state.Result,
            "key = 1, value = 1\n"
            "key = 2, value = 2\n"
            "key = 3, value = 3\n");

        // Snapshots table should be cleaned up after ReadTable has finished
        UNIT_ASSERT_VALUES_EQUAL(GetSnapshotCount(runtime, shards1[0]), 0u);

        // Verify snapshot is removed at the first shard
        for (TString table : { "/Root/table-1" }) {
            auto shards = GetTableShards(server, sender, table);
            auto ranges = GetRemovedRowVersions(server, shards.at(0));
            UNIT_ASSERT(ranges.size() >= 1);
            UNIT_ASSERT_VALUES_EQUAL(ranges.begin()->Lower, TRowVersion::Min());
            UNIT_ASSERT(ranges.begin()->Upper > snapshot);
        }
    }

    Y_UNIT_TEST(SwitchMvccSnapshots) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMvcc(false)
            .SetDomainPlanResolution(1000);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.GetAppData().AllowReadTableImmediate = true;

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        auto shards1 = GetTableShards(server, sender, "/Root/table-1");
        auto shards2 = GetTableShards(server, sender, "/Root/table-2");
        auto tableId1 = ResolveTableId(server, sender, "/Root/table-1");
        auto tableId2 = ResolveTableId(server, sender, "/Root/table-2");

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1), (2, 2), (3, 3);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 10), (20, 20), (30, 30);");

        auto snapshot1 = CreateVolatileSnapshot(server, { "/Root/table-1", "/Root/table-2" }, TDuration::MilliSeconds(30000));

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 11), (2, 22), (3, 33), (4, 44);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 11), (20, 22), (30, 33), (40, 44);");

        runtime.GetAppData().FeatureFlags.SetEnableMvccForTest(true);
        RebootTablet(runtime, shards1.at(0), sender);
        RebootTablet(runtime, shards2.at(0), sender);

        auto snapshot2 = CreateVolatileSnapshot(server, { "/Root/table-1", "/Root/table-2" }, TDuration::MilliSeconds(30000));

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 111), (2, 222), (3, 333), (4, 444);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 111), (20, 222), (30, 333), (40, 444);");

        auto snapshot3 = CreateVolatileSnapshot(server, { "/Root/table-1", "/Root/table-2" }, TDuration::MilliSeconds(30000));

        {
            const auto result = CompactTable(runtime, shards1.at(0), tableId1);
            UNIT_ASSERT(result.GetStatus() == NKikimrTxDataShard::TEvCompactTableResult::OK);
        }
        {
            const auto result = CompactTable(runtime, shards2.at(0), tableId2);
            UNIT_ASSERT(result.GetStatus() == NKikimrTxDataShard::TEvCompactTableResult::OK);
        }

        // None of created snapshots should be removed
        auto removed1 = GetRemovedRowVersions(server, shards1.at(0));
        UNIT_ASSERT(!removed1.Contains(snapshot1, snapshot1.Next()));
        UNIT_ASSERT(!removed1.Contains(snapshot2, snapshot2.Next()));
        UNIT_ASSERT(!removed1.Contains(snapshot3, snapshot3.Next()));

        // Versions to the left and to the right of the first snapshot must be removed
        UNIT_ASSERT(removed1.Contains(snapshot1.Prev(), snapshot1));
        UNIT_ASSERT(removed1.Contains(snapshot1.Next(), snapshot1.Next().Next()));

        auto table1snapshot3 = ReadShardedTable(server, "/Root/table-1", snapshot3);
        UNIT_ASSERT_VALUES_EQUAL(table1snapshot3,
            "key = 1, value = 111\n"
            "key = 2, value = 222\n"
            "key = 3, value = 333\n"
            "key = 4, value = 444\n");

        auto table1snapshot2 = ReadShardedTable(server, "/Root/table-1", snapshot2);
        UNIT_ASSERT_VALUES_EQUAL(table1snapshot2,
            "key = 1, value = 11\n"
            "key = 2, value = 22\n"
            "key = 3, value = 33\n"
            "key = 4, value = 44\n");

        auto table1snapshot1 = ReadShardedTable(server, "/Root/table-1", snapshot1);
        UNIT_ASSERT_VALUES_EQUAL(table1snapshot1,
            "key = 1, value = 1\n"
            "key = 2, value = 2\n"
            "key = 3, value = 3\n");
    }

    Y_UNIT_TEST_TWIN(MvccSnapshotTailCleanup, UseNewEngine) {
        TPortManager pm;
        TServerSettings::TControls controls;
        controls.MutableDataShardControls()->SetPrioritizedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetUnprotectedMvccSnapshotReads(1);
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
            .SetEnableKqpSessionActor(UseNewEngine)
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
            auto reqSender = runtime.AllocateEdgeActor();
            sessionId = CreateSession(runtime, reqSender);
            auto ev = ExecRequest(runtime, reqSender, MakeBeginRequest(sessionId, query));
            auto& response = ev->Get()->Record.GetRef();
            UNIT_ASSERT_VALUES_EQUAL(response.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
            txId = response.GetResponse().GetTxMeta().id();
            UNIT_ASSERT_VALUES_EQUAL(response.GetResponse().GetResults().size(), 1u);
            return response.GetResponse().GetResults()[0].GetValue().ShortDebugString();
        };

        auto continueSnapshotRequest = [&](const TString& sessionId, const TString& txId, const TString& query) -> TString {
            auto reqSender = runtime.AllocateEdgeActor();
            auto ev = ExecRequest(runtime, reqSender, MakeContinueRequest(sessionId, txId, query));
            auto& response = ev->Get()->Record.GetRef();
            if (response.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
                return TStringBuilder() << "ERROR: " << response.GetYdbStatus();
            }
            UNIT_ASSERT_VALUES_EQUAL(response.GetResponse().GetResults().size(), 1u);
            return response.GetResponse().GetResults()[0].GetValue().ShortDebugString();
        };

        auto execSnapshotRequest = [&](const TString& query) -> TString {
            auto reqSender = runtime.AllocateEdgeActor();
            TString sessionId, txId;
            TString result = beginSnapshotRequest(sessionId, txId, query);
            CloseSession(runtime, reqSender, sessionId);
            return result;
        };

        // Start with a snapshot read that persists necessary flags and advances edges for the first time
        UNIT_ASSERT_VALUES_EQUAL(
            execSnapshotRequest(Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                ORDER BY key
                )")),
            "Struct { "
            "List { Struct { Optional { Uint32: 1 } } Struct { Optional { Uint32: 1 } } } "
            "} Struct { Bool: false }");
        SimulateSleep(runtime, TDuration::Seconds(2));

        // Create a new snapshot, it should still observe the same state
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            beginSnapshotRequest(sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                ORDER BY key
                )")),
            "Struct { "
            "List { Struct { Optional { Uint32: 1 } } Struct { Optional { Uint32: 1 } } } "
            "} Struct { Bool: false }");

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
                "Struct { "
                "List { Struct { Optional { Uint32: 1 } } Struct { Optional { Uint32: 1 } } } "
                "} Struct { Bool: false }");
        }

        UNIT_ASSERT_C(failed, "Snapshot was not cleaned up");
    }

    Y_UNIT_TEST_TWIN(MvccSnapshotAndSplit, UseNewEngine) {
        TPortManager pm;
        TServerSettings::TControls controls;
        controls.MutableDataShardControls()->SetPrioritizedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetUnprotectedMvccSnapshotReads(1);
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
            .SetEnableKqpSessionActor(UseNewEngine)
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

        auto execSimpleRequest = [&](const TString& query) -> TString {
            auto reqSender = runtime.AllocateEdgeActor();
            auto ev = ExecRequest(runtime, reqSender, MakeSimpleRequest(query));
            auto& response = ev->Get()->Record.GetRef();
            UNIT_ASSERT_VALUES_EQUAL(response.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(response.GetResponse().GetResults().size(), 1u);
            return response.GetResponse().GetResults()[0].GetValue().ShortDebugString();
        };

        auto beginSnapshotRequest = [&](TString& sessionId, TString& txId, const TString& query) -> TString {
            auto reqSender = runtime.AllocateEdgeActor();
            sessionId = CreateSession(runtime, reqSender);
            auto ev = ExecRequest(runtime, reqSender, MakeBeginRequest(sessionId, query));
            auto& response = ev->Get()->Record.GetRef();
            UNIT_ASSERT_VALUES_EQUAL(response.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
            txId = response.GetResponse().GetTxMeta().id();
            UNIT_ASSERT_VALUES_EQUAL(response.GetResponse().GetResults().size(), 1u);
            return response.GetResponse().GetResults()[0].GetValue().ShortDebugString();
        };

        auto continueSnapshotRequest = [&](const TString& sessionId, const TString& txId, const TString& query) -> TString {
            auto reqSender = runtime.AllocateEdgeActor();
            auto ev = ExecRequest(runtime, reqSender, MakeContinueRequest(sessionId, txId, query));
            auto& response = ev->Get()->Record.GetRef();
            if (response.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
                return TStringBuilder() << "ERROR: " << response.GetYdbStatus();
            }
            UNIT_ASSERT_VALUES_EQUAL(response.GetResponse().GetResults().size(), 1u);
            return response.GetResponse().GetResults()[0].GetValue().ShortDebugString();
        };

        auto execSnapshotRequest = [&](const TString& query) -> TString {
            auto reqSender = runtime.AllocateEdgeActor();
            TString sessionId, txId;
            TString result = beginSnapshotRequest(sessionId, txId, query);
            CloseSession(runtime, reqSender, sessionId);
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
            "Struct { "
            "List { Struct { Optional { Uint32: 1 } } Struct { Optional { Uint32: 1 } } } "
            "} Struct { Bool: false }");
        SimulateSleep(runtime, TDuration::Seconds(2));

        bool captureSplit = true;
        bool captureTimecast = false;
        TVector<THolder<IEventHandle>> capturedSplit;
        TVector<THolder<IEventHandle>> capturedTimecast;
        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle> &ev) -> auto {
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
        UNIT_ASSERT_VALUES_EQUAL(
            beginSnapshotRequest(sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                ORDER BY key
                )")),
            "Struct { "
            "List { Struct { Optional { Uint32: 1 } } Struct { Optional { Uint32: 1 } } } "
            "} Struct { Bool: false }");

        // Finish the split
        captureSplit = false;
        captureTimecast = true;
        for (auto& ev : capturedSplit) {
            runtime.Send(ev.Release(), 0, true);
        }
        WaitTxNotification(server, senderSplit, splitTxId);

        // Send an immediate write after the finished split
        // In a buggy case it starts executing despite a blocked timecast
        auto senderImmediateWrite = runtime.AllocateEdgeActor();
        SendRequest(runtime, senderImmediateWrite, MakeSimpleRequest(Q_(R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 2)
            )")));

        // We sleep a little so datashard commits changes in buggy case
        SimulateSleep(runtime, TDuration::MicroSeconds(1));

        // Unblock timecast, so datashard time can finally catch up
        captureTimecast = false;

        // Wait for the commit result
        {
            auto ev = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(senderImmediateWrite);
            auto& response = ev->Get()->Record.GetRef();
            UNIT_ASSERT_VALUES_EQUAL(response.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
        }

        // Snapshot must not have been damaged by the write above
        UNIT_ASSERT_VALUES_EQUAL(
            continueSnapshotRequest(sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                ORDER BY key
                )")),
            "Struct { "
            "List { Struct { Optional { Uint32: 1 } } Struct { Optional { Uint32: 1 } } } "
            "} Struct { Bool: false }");

        // But new immediate read must observe all writes we have performed
        UNIT_ASSERT_VALUES_EQUAL(
            execSimpleRequest(Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key in (1, 2, 3)
                ORDER BY key
                )")),
            "Struct { "
            "List { Struct { Optional { Uint32: 1 } } Struct { Optional { Uint32: 1 } } } "
            "List { Struct { Optional { Uint32: 2 } } Struct { Optional { Uint32: 2 } } } "
            "} Struct { Bool: false }");
    }

    Y_UNIT_TEST_TWIN(MvccSnapshotReadWithLongPlanQueue, UseNewEngine) {
        TPortManager pm;
        TServerSettings::TControls controls;
        controls.MutableDataShardControls()->SetPrioritizedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetUnprotectedMvccSnapshotReads(1);

        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
            .SetEnableKqpSessionActor(UseNewEngine)
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
            auto reqSender = runtime.AllocateEdgeActor();
            sessionId = CreateSession(runtime, reqSender);
            auto ev = ExecRequest(runtime, reqSender, MakeBeginRequest(sessionId, query));
            auto& response = ev->Get()->Record.GetRef();
            UNIT_ASSERT_VALUES_EQUAL(response.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
            txId = response.GetResponse().GetTxMeta().id();
            UNIT_ASSERT_VALUES_EQUAL(response.GetResponse().GetResults().size(), 1u);
            return response.GetResponse().GetResults()[0].GetValue().ShortDebugString();
        };

        auto continueSnapshotRequest = [&](const TString& sessionId, const TString& txId, const TString& query) -> TString {
            auto reqSender = runtime.AllocateEdgeActor();
            auto ev = ExecRequest(runtime, reqSender, MakeContinueRequest(sessionId, txId, query));
            auto& response = ev->Get()->Record.GetRef();
            if (response.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
                return TStringBuilder() << "ERROR: " << response.GetYdbStatus();
            }
            UNIT_ASSERT_VALUES_EQUAL(response.GetResponse().GetResults().size(), 1u);
            return response.GetResponse().GetResults()[0].GetValue().ShortDebugString();
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
                "Struct { "
                "List { Struct { Optional { Uint32: 1 } } Struct { Optional { Uint32: 1 } } } "
                "} Struct { Bool: false }");
        }

        // Arrange for a distributed tx stuck at readset exchange
        auto senderBlocker = runtime.AllocateEdgeActor();
        TString sessionIdBlocker = CreateSession(runtime, senderBlocker);
        TString txIdBlocker;
        {
            auto ev = ExecRequest(runtime, sender, MakeBeginRequest(sessionIdBlocker, Q_(R"(
                SELECT * FROM `/Root/table-1`
                UNION ALL
                SELECT * FROM `/Root/table-2`)")));
            auto& response = ev->Get()->Record.GetRef();
            UNIT_ASSERT_VALUES_EQUAL(response.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
            txIdBlocker = response.GetResponse().GetTxMeta().id();
            UNIT_ASSERT_VALUES_EQUAL(response.GetResponse().GetResults().size(), 1u);
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
        auto captureRS = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) -> auto {
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
        SendRequest(runtime, senderBlocker, MakeCommitRequest(sessionIdBlocker, txIdBlocker, Q_(R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (99, 99);
            UPSERT INTO `/Root/table-2` (key, value) VALUES (99, 99))")));

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
            "Struct { "
            "List { Struct { Optional { Uint32: 1 } } Struct { Optional { Uint32: 1 } } } "
            "List { Struct { Optional { Uint32: 2 } } Struct { Optional { Uint32: 2 } } } "
            "} Struct { Bool: false }");

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
            "Struct { "
            "List { Struct { Optional { Uint32: 1 } } Struct { Optional { Uint32: 1 } } } "
            "List { Struct { Optional { Uint32: 2 } } Struct { Optional { Uint32: 2 } } } "
            "} Struct { Bool: false }");

        // Insert one more row, in a buggy case it would be assigned a version below the snapshot
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 3)"));

        // Read from snapshot again, unless buggy it should not be corrupted
        UNIT_ASSERT_VALUES_EQUAL(
            continueSnapshotRequest(sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key in (1, 2, 3)
                ORDER BY key
                )")),
            "Struct { "
            "List { Struct { Optional { Uint32: 1 } } Struct { Optional { Uint32: 1 } } } "
            "List { Struct { Optional { Uint32: 2 } } Struct { Optional { Uint32: 2 } } } "
            "} Struct { Bool: false }");
    }

    Y_UNIT_TEST_TWIN(MvccSnapshotLockedWrites, UseNewEngine) {
        TPortManager pm;
        TServerSettings::TControls controls;
        controls.MutableDataShardControls()->SetPrioritizedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetUnprotectedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetEnableLockedWrites(1);

        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
            .SetEnableKqpSessionActor(UseNewEngine)
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

        auto execSimpleRequest = [&](const TString& query, Ydb::StatusIds::StatusCode expectedStatus = Ydb::StatusIds::SUCCESS) -> TString {
            auto reqSender = runtime.AllocateEdgeActor();
            auto ev = ExecRequest(runtime, reqSender, MakeSimpleRequest(query));
            auto& response = ev->Get()->Record.GetRef();
            UNIT_ASSERT_VALUES_EQUAL(response.GetYdbStatus(), expectedStatus);
            if (response.GetResponse().GetResults().size() == 0) {
                return "";
            }
            UNIT_ASSERT_VALUES_EQUAL(response.GetResponse().GetResults().size(), 1u);
            return response.GetResponse().GetResults()[0].GetValue().ShortDebugString();
        };

        auto beginSnapshotRequest = [&](TString& sessionId, TString& txId, const TString& query) -> TString {
            auto reqSender = runtime.AllocateEdgeActor();
            sessionId = CreateSession(runtime, reqSender);
            auto ev = ExecRequest(runtime, reqSender, MakeBeginRequest(sessionId, query));
            auto& response = ev->Get()->Record.GetRef();
            UNIT_ASSERT_VALUES_EQUAL(response.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
            txId = response.GetResponse().GetTxMeta().id();
            UNIT_ASSERT_VALUES_EQUAL(response.GetResponse().GetResults().size(), 1u);
            return response.GetResponse().GetResults()[0].GetValue().ShortDebugString();
        };

        auto continueSnapshotRequest = [&](const TString& sessionId, const TString& txId, const TString& query) -> TString {
            auto reqSender = runtime.AllocateEdgeActor();
            auto ev = ExecRequest(runtime, reqSender, MakeContinueRequest(sessionId, txId, query));
            auto& response = ev->Get()->Record.GetRef();
            if (response.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
                return TStringBuilder() << "ERROR: " << response.GetYdbStatus();
            }
            if (response.GetResponse().GetResults().size() == 0) {
                return "";
            }
            UNIT_ASSERT_VALUES_EQUAL(response.GetResponse().GetResults().size(), 1u);
            return response.GetResponse().GetResults()[0].GetValue().ShortDebugString();
        };

        auto commitSnapshotRequest = [&](const TString& sessionId, const TString& txId, const TString& query) -> TString {
            auto reqSender = runtime.AllocateEdgeActor();
            auto ev = ExecRequest(runtime, reqSender, MakeCommitRequest(sessionId, txId, query));
            auto& response = ev->Get()->Record.GetRef();
            if (response.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
                return TStringBuilder() << "ERROR: " << response.GetYdbStatus();
            }
            if (response.GetResponse().GetResults().size() == 0) {
                return "";
            }
            UNIT_ASSERT_VALUES_EQUAL(response.GetResponse().GetResults().size(), 1u);
            return response.GetResponse().GetResults()[0].GetValue().ShortDebugString();
        };

        ui64 lastLockTxId = 0;
        ui32 lastLockNodeId = 0;
        TRowVersion lastMvccSnapshot = TRowVersion::Min();
        ui64 injectLockTxId = 0;
        ui32 injectLockNodeId = 0;
        TRowVersion injectMvccSnapshot = TRowVersion::Min();
        auto capturePropose = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case TEvDataShard::TEvProposeTransaction::EventType: {
                    auto& record = ev->Get<TEvDataShard::TEvProposeTransaction>()->Record;
                    Cerr << "TEvProposeTransaction:" << Endl;
                    Cerr << record.DebugString() << Endl;
                    if (record.GetTxKind() == NKikimrTxDataShard::TX_KIND_DATA) {
                        NKikimrTxDataShard::TDataTransaction tx;
                        Y_VERIFY(tx.ParseFromString(record.GetTxBody()));
                        Cerr << "TxBody:" << Endl;
                        Cerr << tx.DebugString() << Endl;
                        if (tx.HasMiniKQL()) {
                            using namespace NKikimr::NMiniKQL;
                            TScopedAlloc alloc;
                            TTypeEnvironment typeEnv(alloc);
                            auto node = DeserializeRuntimeNode(tx.GetMiniKQL(), typeEnv);
                            Cerr << "MiniKQL:" << Endl;
                            Cerr << PrintNode(node.GetNode()) << Endl;
                        }
                        if (tx.HasKqpTransaction()) {
                            for (const auto& task : tx.GetKqpTransaction().GetTasks()) {
                                if (task.HasProgram() && task.GetProgram().GetRaw()) {
                                    using namespace NKikimr::NMiniKQL;
                                    TScopedAlloc alloc;
                                    TTypeEnvironment typeEnv(alloc);
                                    auto node = DeserializeRuntimeNode(task.GetProgram().GetRaw(), typeEnv);
                                    Cerr << "Task program:" << Endl;
                                    Cerr << PrintNode(node.GetNode()) << Endl;
                                }
                            }
                        }
                        if (tx.GetLockTxId()) {
                            lastLockTxId = tx.GetLockTxId();
                            lastLockNodeId = tx.GetLockNodeId();
                        } else if (injectLockTxId) {
                            tx.SetLockTxId(injectLockTxId);
                            if (injectLockNodeId) {
                                tx.SetLockNodeId(injectLockNodeId);
                            }
                            TString txBody;
                            Y_VERIFY(tx.SerializeToString(&txBody));
                            record.SetTxBody(txBody);
                        }
                        if (record.HasMvccSnapshot()) {
                            lastMvccSnapshot.Step = record.GetMvccSnapshot().GetStep();
                            lastMvccSnapshot.TxId = record.GetMvccSnapshot().GetTxId();
                        } else if (injectMvccSnapshot) {
                            record.MutableMvccSnapshot()->SetStep(injectMvccSnapshot.Step);
                            record.MutableMvccSnapshot()->SetTxId(injectMvccSnapshot.TxId);
                        }
                    }
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserverFunc = runtime.SetObserverFunc(capturePropose);

        // Start a snapshot read transaction
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            beginSnapshotRequest(sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "Struct { "
            "List { Struct { Optional { Uint32: 1 } } Struct { Optional { Uint32: 1 } } } "
            "} Struct { Bool: false }");

        // We should have been acquiring locks
        Y_VERIFY(lastLockTxId != 0);
        ui64 snapshotLockTxId = lastLockTxId;
        ui32 snapshotLockNodeId = lastLockNodeId;
        Y_VERIFY(lastMvccSnapshot);
        auto snapshotVersion = lastMvccSnapshot;

        // Perform an immediate write, pretending it happens as part of the above snapshot tx
        injectLockTxId = snapshotLockTxId;
        injectLockNodeId = snapshotLockNodeId;
        injectMvccSnapshot = snapshotVersion;
        UNIT_ASSERT_VALUES_EQUAL(
            execSimpleRequest(Q_(R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 2)
                )"),
                UseNewEngine ? Ydb::StatusIds::SUCCESS : Ydb::StatusIds::UNAVAILABLE),
            "");
        injectLockTxId = 0;
        injectLockNodeId = 0;
        injectMvccSnapshot = TRowVersion::Min();

        // Old engine doesn't support LockNodeId
        // There's nothing to test unless we can write uncommitted data 
        if (!UseNewEngine) {
            return;
        }

        // Start another snapshot read, it should not see above write (it's uncommitted)
        TString sessionId2, txId2;
        UNIT_ASSERT_VALUES_EQUAL(
            beginSnapshotRequest(sessionId2, txId2, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "Struct { "
            "List { Struct { Optional { Uint32: 1 } } Struct { Optional { Uint32: 1 } } } "
            "} Struct { Bool: false }");

        // Perform another read using the first snapshot tx, it must see its own writes
        UNIT_ASSERT_VALUES_EQUAL(
            continueSnapshotRequest(sessionId, txId, Q_(R"(
                SELECT key, value FROM `/Root/table-1`
                WHERE key >= 1 AND key <= 3
                ORDER BY key
                )")),
            "Struct { "
            "List { Struct { Optional { Uint32: 1 } } Struct { Optional { Uint32: 1 } } } "
            "List { Struct { Optional { Uint32: 2 } } Struct { Optional { Uint32: 2 } } } "
            "} Struct { Bool: false }");

        // Now commit with additional changes (temporarily needed to trigger lock commits)
        UNIT_ASSERT_VALUES_EQUAL(
            commitSnapshotRequest(sessionId, txId, Q_(R"(
                UPSERT INTO `Root/table-1` (key, value) VALUES (3, 3)
                )")),
            "");

        if (UseNewEngine) {
            // Verify new snapshots observe all committed changes
            // This is only possible with new engine at this time
            TString sessionId3, txId3;
            UNIT_ASSERT_VALUES_EQUAL(
                beginSnapshotRequest(sessionId3, txId3, Q_(R"(
                    SELECT key, value FROM `/Root/table-1`
                    WHERE key >= 1 AND key <= 3
                    ORDER BY key
                    )")),
                "Struct { "
                "List { Struct { Optional { Uint32: 1 } } Struct { Optional { Uint32: 1 } } } "
                "List { Struct { Optional { Uint32: 2 } } Struct { Optional { Uint32: 2 } } } "
                "List { Struct { Optional { Uint32: 3 } } Struct { Optional { Uint32: 3 } } } "
                "} Struct { Bool: false }");
        }
    }

}

} // namespace NKikimr

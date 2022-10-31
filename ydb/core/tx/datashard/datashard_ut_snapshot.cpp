#include "datashard_ut_common.h"
#include "datashard_ut_common_kqp.h"
#include "datashard_active_transaction.h"

#include <ydb/core/formats/factory.h>
#include <ydb/core/tx/long_tx_service/public/lock_handle.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>

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
                const TVector<std::pair<TString, NScheme::TTypeInfo>>& columns,
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
        TVector<std::pair<TString, NScheme::TTypeInfo>> Columns;
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

    Y_UNIT_TEST(MvccSnapshotTailCleanup) {
        TPortManager pm;
        TServerSettings::TControls controls;
        controls.MutableDataShardControls()->SetPrioritizedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetUnprotectedMvccSnapshotReads(1);
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
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
            sessionId = CreateSessionRPC(runtime);
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

    Y_UNIT_TEST(MvccSnapshotAndSplit) {
        TPortManager pm;
        TServerSettings::TControls controls;
        controls.MutableDataShardControls()->SetPrioritizedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetUnprotectedMvccSnapshotReads(1);
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
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
            sessionId = CreateSessionRPC(runtime);
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

    Y_UNIT_TEST(MvccSnapshotReadWithLongPlanQueue) {
        TPortManager pm;
        TServerSettings::TControls controls;
        controls.MutableDataShardControls()->SetPrioritizedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetUnprotectedMvccSnapshotReads(1);

        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
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
            sessionId = CreateSessionRPC(runtime);
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
        TString sessionIdBlocker = CreateSessionRPC(runtime);
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
    };

    struct TInjectLocks {
        NKikimrTxDataShard::TKqpLocks_ELocksOp Op = NKikimrTxDataShard::TKqpLocks::Commit;
        TVector<TLockInfo> Locks;

        void AddLocks(const TVector<TLockInfo>& locks) {
            Locks.insert(Locks.end(), locks.begin(), locks.end());
        }
    };

    class TInjectLockSnapshotObserver {
    public:
        TInjectLockSnapshotObserver(TTestActorRuntime& runtime)
            : Runtime(runtime)
        {
            PrevObserver = runtime.SetObserverFunc([this](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
                return this->Process(ev);
            });
        }

        ~TInjectLockSnapshotObserver() {
            Runtime.SetObserverFunc(PrevObserver);
        }

        TTestActorRuntime::EEventAction Process(TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvDataShard::TEvProposeTransaction::EventType: {
                    auto& record = ev->Get<TEvDataShard::TEvProposeTransaction>()->Record;
                    Cerr << "TEvProposeTransaction:" << Endl;
                    Cerr << record.DebugString() << Endl;
                    if (record.GetTxKind() == NKikimrTxDataShard::TX_KIND_DATA) {
                        NKikimrTxDataShard::TDataTransaction tx;
                        Y_VERIFY(tx.ParseFromString(record.GetTxBody()));
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
                                Y_VERIFY(tx.SerializeToString(&txBody));
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
                                Y_VERIFY(tx.SerializeToString(&txBody));
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
                            Y_VERIFY(tx.SerializeToString(&txBody));
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
            return PrevObserver(Runtime, ev);
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
        controls.MutableDataShardControls()->SetPrioritizedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetUnprotectedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetEnableLockedWrites(1);

        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
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
        Y_VERIFY(snapshot.LockId != 0);
        Y_VERIFY(snapshot.MvccSnapshot);

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
        controls.MutableDataShardControls()->SetPrioritizedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetUnprotectedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetEnableLockedWrites(1);

        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
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
        Y_VERIFY(snapshot.LockId != 0);
        Y_VERIFY(snapshot.MvccSnapshot);

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
        controls.MutableDataShardControls()->SetPrioritizedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetUnprotectedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetEnableLockedWrites(1);

        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
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
        controls.MutableDataShardControls()->SetPrioritizedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetUnprotectedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetEnableLockedWrites(1);

        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
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
        record.SetResultFormat(NKikimrTxDataShard::CELLVEC);
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
        controls.MutableDataShardControls()->SetPrioritizedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetUnprotectedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetEnableLockedWrites(1);

        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
            .SetControls(controls);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;
        CreateShardedTable(server, sender, "/Root", "table-1", TShardedTableOptions()
            .Shards(1)
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
        controls.MutableDataShardControls()->SetPrioritizedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetUnprotectedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetEnableLockedWrites(1);

        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
            .SetControls(controls);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;
        CreateShardedTable(server, sender, "/Root", "table-1", TShardedTableOptions()
            .Shards(1)
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
        controls.MutableDataShardControls()->SetPrioritizedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetUnprotectedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetEnableLockedWrites(1);

        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
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
            TSerializedCellVec serializedKey(TSerializedCellVec::Serialize(key));
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
        controls.MutableDataShardControls()->SetPrioritizedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetUnprotectedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetEnableLockedWrites(1);

        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
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
        controls.MutableDataShardControls()->SetPrioritizedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetUnprotectedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetEnableLockedWrites(1);

        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
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
        controls.MutableDataShardControls()->SetPrioritizedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetUnprotectedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetEnableLockedWrites(1);

        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
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
        controls.MutableDataShardControls()->SetPrioritizedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetUnprotectedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetEnableLockedWrites(1);

        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
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
        observer.BlockReadSets = true;
        observer.InjectClearTasks = true;
        observer.InjectLocks.emplace();
        observer.InjectLocks->AddLocks(locks1);
        observer.InjectLocks->AddLocks(locks2);
        auto commitSender = runtime.AllocateEdgeActor();
        SendRequest(runtime, commitSender, MakeSimpleRequest(Q_(R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (0, 0);
            UPSERT INTO `/Root/table-2` (key, value) VALUES (0, 0);
            )")));
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT(!observer.BlockedReadSets.empty());
        observer.InjectClearTasks = false;
        observer.InjectLocks.reset();

        auto writeSender = runtime.AllocateEdgeActor();
        SendRequest(runtime, writeSender, MakeSimpleRequest(Q_(R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 22)
            )")));
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
            auto ev = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(commitSender);
            auto& response = ev->Get()->Record.GetRef();
            UNIT_ASSERT_VALUES_EQUAL(response.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
        }

        {
            auto ev = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(writeSender);
            auto& response = ev->Get()->Record.GetRef();
            UNIT_ASSERT_VALUES_EQUAL(response.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
        }
    }

    Y_UNIT_TEST(LockedWriteDistributedCommitCrossConflict) {
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

        // Commit changes in tx 123 (we expect locks to be ready for sending)
        observer.BlockReadSets = true;
        //observer.InjectClearTasks = true;
        observer.InjectLocks.emplace();
        observer.InjectLocks->AddLocks(locks1);
        observer.InjectLocks->AddLocks(locks2);
        auto commitSender1 = runtime.AllocateEdgeActor();
        SendRequest(runtime, commitSender1, MakeSimpleRequest(Q_(R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 21);
            UPSERT INTO `/Root/table-2` (key, value) VALUES (30, 21);
            )")));
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT(!observer.BlockedReadSets.empty());
        //observer.InjectClearTasks = false;
        observer.InjectLocks.reset();

        // Commit changes in tx 234 (we expect it to be blocked and broken by 123)
        observer.BlockReadSets = true;
        //observer.InjectClearTasks = true;
        observer.InjectLocks.emplace();
        observer.InjectLocks->AddLocks(locks3);
        observer.InjectLocks->AddLocks(locks4);
        auto commitSender2 = runtime.AllocateEdgeActor();
        SendRequest(runtime, commitSender2, MakeSimpleRequest(Q_(R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 22);
            UPSERT INTO `/Root/table-2` (key, value) VALUES (20, 22);
            )")));
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
            auto ev = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(commitSender1);
            auto& response = ev->Get()->Record.GetRef();
            UNIT_ASSERT_VALUES_EQUAL(response.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
        }

        {
            auto ev = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(commitSender2);
            auto& response = ev->Get()->Record.GetRef();
            UNIT_ASSERT_VALUES_EQUAL(response.GetYdbStatus(), Ydb::StatusIds::ABORTED);
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
        controls.MutableDataShardControls()->SetPrioritizedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetUnprotectedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetEnableLockedWrites(1);

        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
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
        controls.MutableDataShardControls()->SetPrioritizedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetUnprotectedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetEnableLockedWrites(1);

        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMvcc(true)
            .SetEnableMvccSnapshotReads(true)
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
        constexpr bool UseNewEngine = true;

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
            "Struct { "
            "List { Struct { Optional { Uint32: 1 } } Struct { Optional { Uint32: 1 } } } "
            "} Struct { Bool: false }");

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
            "Struct { "
            "List { Struct { Optional { Uint32: 1 } } Struct { Optional { Uint32: 1 } } } "
            "} Struct { Bool: false }");

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
            "Struct { "
            "List { Struct { Optional { Uint32: 1 } } Struct { Optional { Uint32: 11 } } } "
            "List { Struct { Optional { Uint32: 2 } } Struct { Optional { Uint32: 21 } } } "
            "} Struct { Bool: false }");

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

}

} // namespace NKikimr

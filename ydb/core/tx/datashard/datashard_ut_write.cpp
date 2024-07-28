#include "datashard_active_transaction.h"
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/base/tablet_pipecache.h>
#include "datashard_ut_common_kqp.h"

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;

Y_UNIT_TEST_SUITE(DataShardWrite) {
    const TString expectedTableState = "key = 0, value = 1\nkey = 2, value = 3\nkey = 4, value = 5\n";

    std::tuple<TTestActorRuntime&, Tests::TServer::TPtr, TActorId> TestCreateServer(std::optional<TServerSettings> serverSettings = {}) {
        if (!serverSettings) {
            TPortManager pm;
            serverSettings = TServerSettings(pm.GetPort(2134));
            serverSettings->SetDomainName("Root").SetUseRealThreads(false);
        }

        Tests::TServer::TPtr server = new TServer(serverSettings.value());
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        return {runtime, server, sender};
    }

    Y_UNIT_TEST_TWIN(ExecSQLUpsertImmediate, EvWrite) {
        auto [runtime, server, sender] = TestCreateServer();

        TShardedTableOptions opts;
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);

        auto rows = EvWrite ? TEvWriteRows{{{0, 1}}, {{2, 3}}, {{4, 5}}} : TEvWriteRows{};
        auto evWriteObservers = ReplaceEvProposeTransactionWithEvWrite(runtime, rows);

        Cout << "========= Send immediate write =========\n";
        {
            ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (0, 1);"));
            ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 3);"));
            ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (4, 5);"));
        }

        Cout << "========= Read table =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);
            UNIT_ASSERT_VALUES_EQUAL(tableState, expectedTableState);
        }
    }

    Y_UNIT_TEST_QUAD(ExecSQLUpsertPrepared, EvWrite, Volatile) {
        auto [runtime, server, sender] = TestCreateServer();

        runtime.GetAppData().FeatureFlags.SetEnableDataShardVolatileTransactions(Volatile);

        TShardedTableOptions opts;
        auto [shards1, tableId1] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        auto [shards2, tableId2] = CreateShardedTable(server, sender, "/Root", "table-2", opts);

        auto rows = EvWrite ? TEvWriteRows{{tableId1, {0, 1}}, {tableId2, {2, 3}}} : TEvWriteRows{};
        auto evWriteObservers = ReplaceEvProposeTransactionWithEvWrite(runtime, rows);

        Cout << "========= Send distributed write =========\n";
        {
            ExecSQL(server, sender, Q_(
                "UPSERT INTO `/Root/table-1` (key, value) VALUES (0, 1);"
                "UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 3);"));
        }

        Cout << "========= Read tables =========\n";
        {
            auto tableState1 = ReadTable(server, shards1, tableId1);
            auto tableState2 = ReadTable(server, shards2, tableId2);
            UNIT_ASSERT_VALUES_EQUAL(tableState1, "key = 0, value = 1\n");
            UNIT_ASSERT_VALUES_EQUAL(tableState2, "key = 2, value = 3\n");
        }
    }

    Y_UNIT_TEST(UpsertImmediate) {
        auto [runtime, server, sender] = TestCreateServer();

        TShardedTableOptions opts;
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const ui64 shard = shards[0];
        const ui32 rowCount = 3;

        ui64 txId = 100;

        Cout << "========= Send immediate write =========\n";
        {
            const auto writeResult = Upsert(runtime, sender, shard, tableId, opts.Columns_, rowCount, txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
            
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetOrigin(), shard);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetStep(), 0);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetOrderId(), txId);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetTxId(), txId);

            const auto& tableAccessStats = writeResult.GetTxStats().GetTableAccessStats(0);
            UNIT_ASSERT_VALUES_EQUAL(tableAccessStats.GetTableInfo().GetName(), "/Root/table-1");
            UNIT_ASSERT_VALUES_EQUAL(tableAccessStats.GetUpdateRow().GetCount(), rowCount);
        }

        Cout << "========= Read table =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);
            UNIT_ASSERT_VALUES_EQUAL(tableState, expectedTableState);
        }
    }

    Y_UNIT_TEST(UpsertImmediateManyColumns) {
        auto [runtime, server, sender] = TestCreateServer();

        auto opts = TShardedTableOptions().Columns({{"key64", "Uint64", true, false}, {"key32", "Uint32", true, false},
                                                    {"value64", "Uint64", false, false}, {"value32", "Uint32", false, false}, {"valueUtf8", "Utf8", false, false}});
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const ui64 shard = shards[0];
        const ui32 rowCount = 3;

        ui64 txId = 100;

        Cout << "========= Send immediate upsert =========\n";
        {
            Upsert(runtime, sender, shard, tableId, opts.Columns_, rowCount, txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        }

        Cout << "========= Read table =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);
            UNIT_ASSERT_VALUES_EQUAL(tableState, "key64 = 0, key32 = 1, value64 = 2, value32 = 3, valueUtf8 = String_4\n"
                                                 "key64 = 5, key32 = 6, value64 = 7, value32 = 8, valueUtf8 = String_9\n"
                                                 "key64 = 10, key32 = 11, value64 = 12, value32 = 13, valueUtf8 = String_14\n");
        }

        Cout << "========= Send immediate delete =========\n";
        {
            Delete(runtime, sender, shard, tableId, opts.Columns_, 1, ++txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        }

        Cout << "========= Read table with 1th row deleted =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);
            UNIT_ASSERT_VALUES_EQUAL(tableState, "key64 = 5, key32 = 6, value64 = 7, value32 = 8, valueUtf8 = String_9\n"
                                                 "key64 = 10, key32 = 11, value64 = 12, value32 = 13, valueUtf8 = String_14\n");
        }
    }

    Y_UNIT_TEST(WriteImmediateBadRequest) {
        auto [runtime, server, sender] = TestCreateServer();

        auto opts = TShardedTableOptions().Columns({{"key", "Utf8", true, false}});
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const ui64 shard = shards[0];

        Cout << "========= Send immediate write with huge key=========\n";
        {
            TString hugeStringValue(NLimits::MaxWriteKeySize + 1, 'X');
            TSerializedCellMatrix matrix({TCell(hugeStringValue.c_str(), hugeStringValue.size())}, 1, 1);

            auto evWrite = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(100, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
            ui64 payloadIndex = NKikimr::NEvWrite::TPayloadWriter<NKikimr::NEvents::TDataEvents::TEvWrite>(*evWrite).AddDataToPayload(matrix.ReleaseBuffer());
            evWrite->AddOperation(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT, tableId, {1}, payloadIndex, NKikimrDataEvents::FORMAT_CELLVEC);

            const auto writeResult = Write(runtime, sender, shard, std::move(evWrite), NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetIssues().size(), 1);
            UNIT_ASSERT(writeResult.GetIssues(0).message().Contains("Row key size of 1049601 bytes is larger than the allowed threshold 1049600"));
        }

        Cout << "========= Send immediate write with OPERATION_UNSPECIFIED =========\n";
        {
            TString stringValue('X');
            TSerializedCellMatrix matrix({TCell(stringValue.c_str(), stringValue.size())}, 1, 1);

            auto evWrite = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(100, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
            ui64 payloadIndex = NKikimr::NEvWrite::TPayloadWriter<NKikimr::NEvents::TDataEvents::TEvWrite>(*evWrite).AddDataToPayload(matrix.ReleaseBuffer());
            auto operation = evWrite->Record.AddOperations();
            operation->SetType(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UNSPECIFIED);
            operation->SetPayloadFormat(NKikimrDataEvents::FORMAT_CELLVEC);
            operation->SetPayloadIndex(payloadIndex);
            operation->MutableTableId()->SetOwnerId(tableId.PathId.OwnerId);
            operation->MutableTableId()->SetTableId(tableId.PathId.LocalPathId);
            operation->MutableTableId()->SetSchemaVersion(tableId.SchemaVersion);
            operation->MutableColumnIds()->Add(1);

            const auto writeResult = Write(runtime, sender, shards[0], std::move(evWrite), NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetIssues().size(), 1);
            UNIT_ASSERT(writeResult.GetIssues(0).message().Contains("OPERATION_UNSPECIFIED operation is not supported now"));
        }
    }

    Y_UNIT_TEST(WriteImmediateSeveralOperations) {
        auto [runtime, server, sender] = TestCreateServer();

        TShardedTableOptions opts;
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);

        const ui64 shard = shards[0];
        const ui32 rowCount = 3;

        Cout << "========= Send immediate write with several operations =========\n";
        {
            auto evWrite = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
            const std::vector<ui32> columnIds = {1,2};

            for (ui32 row = 0; row < rowCount; ++row) {
                TVector<TCell> cells;

                for (ui32 col = 0; col < columnIds.size(); ++col) {
                    ui32 value32 = row * columnIds.size() + col;
                    cells.emplace_back(TCell((const char*)&value32, sizeof(ui32)));
                }

                TSerializedCellMatrix matrix(cells, 1, columnIds.size());
                ui64 payloadIndex = NKikimr::NEvWrite::TPayloadWriter<NKikimr::NEvents::TDataEvents::TEvWrite>(*evWrite).AddDataToPayload(matrix.ReleaseBuffer());
                evWrite->AddOperation(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT, tableId, columnIds, payloadIndex, NKikimrDataEvents::FORMAT_CELLVEC);
            }
            
            const auto writeResult = Write(runtime, sender, shard, std::move(evWrite));

            const auto& tableAccessStats = writeResult.GetTxStats().GetTableAccessStats(0);
            UNIT_ASSERT_VALUES_EQUAL(tableAccessStats.GetUpdateRow().GetCount(), rowCount);
        }

        Cout << "========= Read table =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);
            UNIT_ASSERT_VALUES_EQUAL(tableState, expectedTableState);
        }
    }    

    Y_UNIT_TEST(DeleteImmediate) {
        auto [runtime, server, sender] = TestCreateServer();

        TShardedTableOptions opts;
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const ui64 shard = shards[0];

        ui64 txId = 100;

        Cout << "========= Send immediate upsert =========\n";
        {
            Upsert(runtime, sender, shard, tableId, opts.Columns_, 3, txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        }

        Cout << "========= Read table =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);
            UNIT_ASSERT_VALUES_EQUAL(tableState, expectedTableState);
        }

        Cout << "========= Send immediate delete =========\n";
        {
            const auto writeResult = Delete(runtime, sender, shard, tableId, opts.Columns_, 1, ++txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);

            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetOrigin(), shard);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetStep(), 0);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetOrderId(), txId);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetTxId(), txId);

            const auto& tableAccessStats = writeResult.GetTxStats().GetTableAccessStats(0);
            UNIT_ASSERT_VALUES_EQUAL(tableAccessStats.GetTableInfo().GetName(), "/Root/table-1");
            UNIT_ASSERT_VALUES_EQUAL(tableAccessStats.GetEraseRow().GetCount(), 1);
        }

        Cout << "========= Read table with 1th row deleted =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);
            UNIT_ASSERT_VALUES_EQUAL(tableState, "key = 2, value = 3\nkey = 4, value = 5\n");
        }
    }   

    Y_UNIT_TEST(ReplaceImmediate) {
        auto [runtime, server, sender] = TestCreateServer();

        TShardedTableOptions opts;
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const ui64 shard = shards[0];
        const ui32 rowCount = 3;

        ui64 txId = 100;

        Cout << "========= Send immediate replace =========\n";
        {
            const auto writeResult = Replace(runtime, sender, shard, tableId, opts.Columns_, rowCount, txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
            
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetOrigin(), shard);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetStep(), 0);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetOrderId(), txId);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetTxId(), txId);

            const auto& tableAccessStats = writeResult.GetTxStats().GetTableAccessStats(0);
            UNIT_ASSERT_VALUES_EQUAL(tableAccessStats.GetTableInfo().GetName(), "/Root/table-1");
            UNIT_ASSERT_VALUES_EQUAL(tableAccessStats.GetUpdateRow().GetCount(), rowCount);
        }

        Cout << "========= Read table =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);
            UNIT_ASSERT_VALUES_EQUAL(tableState, expectedTableState);
        }
    }    

    Y_UNIT_TEST(ReplaceImmediate_DefaultValue) {
        auto [runtime, server, sender] = TestCreateServer();

        TShardedTableOptions opts;
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const ui64 shard = shards[0];
        const ui32 rowCount = 1;

        ui64 txId = 100;

        Cout << "========= Send immediate upsert =========\n";
        {
            Upsert(runtime, sender, shard, tableId, opts.Columns_, rowCount, txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        }

        Cout << "========= Send immediate replace =========\n";
        {
            std::vector<ui32> columnIds = {1};

            // Set only key and leave value default
            ui32 value = 0;
            TVector<TCell> cells;
            cells.emplace_back(TCell((const char*)&value, sizeof(ui32)));

            TSerializedCellMatrix matrix(cells, rowCount, columnIds.size());
            TString blobData = matrix.ReleaseBuffer();

            auto request = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(++txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
            ui64 payloadIndex = NKikimr::NEvWrite::TPayloadWriter<NKikimr::NEvents::TDataEvents::TEvWrite>(*request).AddDataToPayload(std::move(blobData));
            request->AddOperation(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE, tableId, columnIds, payloadIndex, NKikimrDataEvents::FORMAT_CELLVEC);

            auto writeResult = Write(runtime, sender, shard, std::move(request), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);

            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetOrigin(), shard);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetStep(), 0);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetOrderId(), txId);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetTxId(), txId);

            const auto& tableAccessStats = writeResult.GetTxStats().GetTableAccessStats(0);
            UNIT_ASSERT_VALUES_EQUAL(tableAccessStats.GetTableInfo().GetName(), "/Root/table-1");
            UNIT_ASSERT_VALUES_EQUAL(tableAccessStats.GetUpdateRow().GetCount(), rowCount);
        }

        Cout << "========= Read table =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);
            UNIT_ASSERT_VALUES_EQUAL(tableState, "key = 0, value = NULL\n");
        }
    }

    Y_UNIT_TEST(InsertImmediate) {
        auto [runtime, server, sender] = TestCreateServer();

        TShardedTableOptions opts;
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const ui64 shard = shards[0];
        const ui32 rowCount = 3;

        ui64 txId = 100;

        Cout << "========= Send immediate insert, keys 0, 2, 4 =========\n";
        {
            const auto writeResult = Insert(runtime, sender, shard, tableId, opts.Columns_, rowCount, txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
            
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetOrigin(), shard);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetStep(), 0);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetOrderId(), txId);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetTxId(), txId);

            const auto& tableAccessStats = writeResult.GetTxStats().GetTableAccessStats(0);
            UNIT_ASSERT_VALUES_EQUAL(tableAccessStats.GetTableInfo().GetName(), "/Root/table-1");
            UNIT_ASSERT_VALUES_EQUAL(tableAccessStats.GetUpdateRow().GetCount(), rowCount);
        }

        Cout << "========= Read table =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);
            UNIT_ASSERT_VALUES_EQUAL(tableState, expectedTableState);
        }

        Cout << "========= Send immediate insert with duplicate, keys -2, 0, 2 =========\n";
        {
            auto request = MakeWriteRequest(++txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT, tableId, opts.Columns_, rowCount, -2);
            const auto writeResult = Write(runtime, sender, shard, std::move(request), NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
        }

        Cout << "========= Read table =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);
            UNIT_ASSERT_VALUES_EQUAL(tableState, expectedTableState);
        }
    }    

    Y_UNIT_TEST(UpdateImmediate) {
        auto [runtime, server, sender] = TestCreateServer();

        TShardedTableOptions opts;
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const ui64 shard = shards[0];
        const ui32 rowCount = 3;

        ui64 txId = 100;

        Cout << "========= Send immediate update to empty table, it should be no op =========\n";
        {
            Update(runtime, sender, shard, tableId, opts.Columns_, rowCount, txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        }     
        
        Cout << "========= Read table =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);
            UNIT_ASSERT_VALUES_EQUAL(tableState, "");
        }         

        Cout << "========= Send immediate insert =========\n";
        {
            Insert(runtime, sender, shard, tableId, opts.Columns_, rowCount, txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        }

        Cout << "========= Read table =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);
            UNIT_ASSERT_VALUES_EQUAL(tableState, expectedTableState);
        }

        Cout << "========= Send immediate upsert, change one row =========\n";
        {
            UpsertOneKeyValue(runtime, sender, shard, tableId, opts.Columns_, 0, 555, txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        }

        Cout << "========= Read table =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);
            UNIT_ASSERT_VALUES_EQUAL(tableState, "key = 0, value = 555\nkey = 2, value = 3\nkey = 4, value = 5\n");
        }

        Cout << "========= Send immediate update, it should override all the rows =========\n";
        {
            const auto writeResult = Update(runtime, sender, shard, tableId, opts.Columns_, rowCount, txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);

            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetOrigin(), shard);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetStep(), 0);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetOrderId(), txId);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetTxId(), txId);

            const auto& tableAccessStats = writeResult.GetTxStats().GetTableAccessStats(0);
            UNIT_ASSERT_VALUES_EQUAL(tableAccessStats.GetTableInfo().GetName(), "/Root/table-1");
            UNIT_ASSERT_VALUES_EQUAL(tableAccessStats.GetUpdateRow().GetCount(), rowCount);
        }

        Cout << "========= Read table =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);
            UNIT_ASSERT_VALUES_EQUAL(tableState, expectedTableState);
        }
    }

    Y_UNIT_TEST_TWIN(UpsertPrepared, Volatile) {
        auto [runtime, server, sender] = TestCreateServer();

        TShardedTableOptions opts;
        const TString tableName = "table-1";
        const auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", tableName, opts);
        const ui64 shard = shards[0];
        const ui64 coordinator = ChangeStateStorage(Coordinator, server->GetSettings().Domain);
        const ui32 rowCount = 3;

        ui64 txId = 100;
        ui64 minStep, maxStep;

        Cout << "========= Send prepare =========\n";
        {
            const auto writeResult = Upsert(runtime, sender, shard, tableId, opts.Columns_, rowCount, txId, 
                Volatile ? NKikimrDataEvents::TEvWrite::MODE_VOLATILE_PREPARE : NKikimrDataEvents::TEvWrite::MODE_PREPARE);

            UNIT_ASSERT_GT(writeResult.GetMinStep(), 0);
            UNIT_ASSERT_GT(writeResult.GetMaxStep(), writeResult.GetMinStep());
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetOrigin(), shard);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetTxId(), txId);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetDomainCoordinators().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetDomainCoordinators(0), coordinator);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetTabletInfo().GetTabletId(), shard);

            minStep = writeResult.GetMinStep();
            maxStep = writeResult.GetMaxStep();
        }

        Cout << "========= Send propose to coordinator =========\n";
        SendProposeToCoordinator(
            runtime, sender, shards, {
                .TxId = txId,
                .Coordinator = coordinator,
                .MinStep = minStep,
                .MaxStep = maxStep,
                .Volatile = Volatile,
            });

        Cout << "========= Wait for completed transaction =========\n";
        {
            auto writeResult = WaitForWriteCompleted(runtime, sender);

            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetOrigin(), shard);
            UNIT_ASSERT_GE(writeResult.GetStep(), minStep);
            UNIT_ASSERT_LE(writeResult.GetStep(), maxStep);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetOrderId(), txId);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetTxId(), txId);

            const auto& tableAccessStats = writeResult.GetTxStats().GetTableAccessStats(0);
            UNIT_ASSERT_VALUES_EQUAL(tableAccessStats.GetTableInfo().GetName(), "/Root/" + tableName);
            UNIT_ASSERT_VALUES_EQUAL(tableAccessStats.GetUpdateRow().GetCount(), rowCount);
        }

        Cout << "========= Read table =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);
            UNIT_ASSERT_VALUES_EQUAL(tableState, expectedTableState);
        }
    }

    Y_UNIT_TEST(CancelImmediate) {
        auto [runtime, server, sender] = TestCreateServer();

        TShardedTableOptions opts;
        const TString tableName = "table-1";
        const auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", tableName, opts);
        const ui64 shard = shards[0];
        const ui32 rowCount = 3;

        ui64 txId = 100;

        TActorId shardActorId = ResolveTablet( runtime, shard, 0, false);

        Cout << "========= Send immediate =========\n";
        {
            auto request = MakeWriteRequest(txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT, tableId, opts.Columns_, rowCount);
            runtime.Send(new IEventHandle(shardActorId, sender, request.release()), 0, true);
        }

        Cout << "========= Send cancel to tablet =========\n";
        {
            auto request = std::make_unique<TEvDataShard::TEvCancelTransactionProposal>(txId);
            runtime.Send(new IEventHandle(shardActorId, sender, request.release()), 0, true);
        }

        Cout << "========= Wait for STATUS_CANCELLED result =========\n";
        {
            const auto writeResult = WaitForWriteCompleted(runtime, sender, NKikimrDataEvents::TEvWriteResult::STATUS_CANCELLED);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetTxId(), txId);
        }

        Cout << "========= Send immediate upserts =========\n";
        {
            ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (0, 1);"));
            Upsert(runtime, sender, shard, tableId, opts.Columns_, rowCount, txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        }
    }

    Y_UNIT_TEST_TWIN(UpsertPreparedManyTables, Volatile) {
        auto [runtime, server, sender] = TestCreateServer();

        TShardedTableOptions opts;
        const TString tableName1 = "table-1";
        const TString tableName2 = "table-2";
        const auto [shards1, tableId1] = CreateShardedTable(server, sender, "/Root", tableName1, opts);
        const auto [shards2, tableId2] = CreateShardedTable(server, sender, "/Root", tableName2, opts);
        const ui64 tabletId1 = shards1[0];
        const ui64 tabletId2 = shards2[0];
        const ui64 rowCount = 3;
        
        ui64 txId = 100;
        ui64 minStep1, maxStep1;
        ui64 minStep2, maxStep2;
        ui64 coordinator;

        Cerr << "===== Write prepared to table 1" << Endl;
        {
            const auto writeResult = Upsert(runtime, sender, tabletId1, tableId1, opts.Columns_, rowCount, txId, 
                Volatile ? NKikimrDataEvents::TEvWrite::MODE_VOLATILE_PREPARE : NKikimrDataEvents::TEvWrite::MODE_PREPARE);

            minStep1 = writeResult.GetMinStep();
            maxStep1 = writeResult.GetMaxStep();
            coordinator = writeResult.GetDomainCoordinators(0);
        }

        Cerr << "===== Write prepared to table 2" << Endl;
        {
            const auto writeResult = Upsert(runtime, sender, tabletId2, tableId2, opts.Columns_, rowCount, txId, NKikimrDataEvents::TEvWrite::MODE_PREPARE);

            minStep2 = writeResult.GetMinStep();
            maxStep2 = writeResult.GetMaxStep();
        }

        Cerr << "========= Send propose to coordinator" << Endl;
        SendProposeToCoordinator(
            runtime, sender, {tabletId1, tabletId2}, {
                .TxId = txId,
                .Coordinator = coordinator,
                .MinStep = Max(minStep1, minStep2),
                .MaxStep = Min(maxStep1, maxStep2),
                .Volatile = Volatile,
            });

        Cerr << "========= Wait for completed transactions" << Endl;
        for (ui8 i = 0; i < 1; ++i)
        {
            const auto writeResult = WaitForWriteCompleted(runtime, sender);

            UNIT_ASSERT_GE(writeResult.GetStep(), Max(minStep1, minStep2));
            UNIT_ASSERT_LE(writeResult.GetStep(), Min(maxStep1, maxStep2));
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetOrderId(), txId);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetTxId(), txId);

            UNIT_ASSERT_VALUES_EQUAL(writeResult.TxLocksSize(), 0);

            const auto& tableAccessStats = writeResult.GetTxStats().GetTableAccessStats(0);
            UNIT_ASSERT_VALUES_EQUAL(tableAccessStats.GetUpdateRow().GetCount(), rowCount);
            UNIT_ASSERT_VALUES_EQUAL(tableAccessStats.GetTableInfo().GetName(), "/Root/" + (writeResult.GetOrigin() == tabletId1 ? tableName1 : tableName2));
        }

        Cout << "========= Read from tables" << Endl;
        {
            auto tableState = ReadTable(server, shards1, tableId1);
            UNIT_ASSERT_VALUES_EQUAL(tableState, expectedTableState);
            tableState = ReadTable(server, shards2, tableId2);
            UNIT_ASSERT_VALUES_EQUAL(tableState, expectedTableState);
        }
    }

    
    Y_UNIT_TEST_TWIN(UpsertPreparedNoTxCache, Volatile) {
        auto [runtime, server, sender] = TestCreateServer();

        TShardedTableOptions opts;
        opts.DataTxCacheSize(0);  //disabling the tx cache for forced serialization/deserialization of txs
        const auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const ui64 shard = shards[0];
        const ui32 rowCount = 3;

        ui64 txId = 100;
        ui64 minStep, maxStep;
        ui64 coordinator;

        Cout << "========= Send prepare =========\n";
        {
            const auto writeResult = Upsert(runtime, sender, shard, tableId, opts.Columns_, rowCount, txId, 
                Volatile ? NKikimrDataEvents::TEvWrite::MODE_VOLATILE_PREPARE : NKikimrDataEvents::TEvWrite::MODE_PREPARE);

            minStep = writeResult.GetMinStep();
            maxStep = writeResult.GetMaxStep();
            coordinator = writeResult.GetDomainCoordinators(0);
        }

        Cout << "========= Send propose to coordinator =========\n";
        SendProposeToCoordinator(
            runtime, sender, shards, {
                .TxId = txId,
                .Coordinator = coordinator,
                .MinStep = minStep,
                .MaxStep = maxStep,
                .Volatile = Volatile,
            });

        Cout << "========= Wait for completed transaction =========\n";
        {
            WaitForWriteCompleted(runtime, sender);
        }

        Cout << "========= Read table =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);
            UNIT_ASSERT_VALUES_EQUAL(tableState, expectedTableState);
        }

    }

    Y_UNIT_TEST_TWIN(DeletePrepared, Volatile) {
        auto [runtime, server, sender] = TestCreateServer();

        TShardedTableOptions opts;
        const TString tableName = "table-1";
        const auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", tableName, opts);
        const ui64 shard = shards[0];
        const ui64 coordinator = ChangeStateStorage(Coordinator, server->GetSettings().Domain);

        ui64 txId = 100;
        ui64 minStep, maxStep;

        Cout << "========= Send immediate upsert =========\n";
        {
            Upsert(runtime, sender, shard, tableId, opts.Columns_, 3, txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        }

        Cout << "========= Read table =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);
            UNIT_ASSERT_VALUES_EQUAL(tableState, expectedTableState);
        }

        Cout << "========= Send delete prepare =========\n";
        {
            const auto writeResult = Delete(runtime, sender, shard, tableId, opts.Columns_, 1, ++txId, Volatile ? NKikimrDataEvents::TEvWrite::MODE_VOLATILE_PREPARE : NKikimrDataEvents::TEvWrite::MODE_PREPARE);

            UNIT_ASSERT_GT(writeResult.GetMinStep(), 0);
            UNIT_ASSERT_GT(writeResult.GetMaxStep(), writeResult.GetMinStep());
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetOrigin(), shard);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetTxId(), txId);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetDomainCoordinators().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetDomainCoordinators(0), coordinator);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetTabletInfo().GetTabletId(), shard);

            minStep = writeResult.GetMinStep();
            maxStep = writeResult.GetMaxStep();
        }

        Cout << "========= Send propose to coordinator =========\n";
        SendProposeToCoordinator(
            runtime, sender, shards, {
                .TxId = txId,
                .Coordinator = coordinator,
                .MinStep = minStep,
                .MaxStep = maxStep,
                .Volatile = Volatile,
            });

        Cout << "========= Wait for completed transaction =========\n";
        {
            auto writeResult = WaitForWriteCompleted(runtime, sender);

            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetOrigin(), shard);
            UNIT_ASSERT_GE(writeResult.GetStep(), minStep);
            UNIT_ASSERT_LE(writeResult.GetStep(), maxStep);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetOrderId(), txId);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetTxId(), txId);

            const auto& tableAccessStats = writeResult.GetTxStats().GetTableAccessStats(0);
            UNIT_ASSERT_VALUES_EQUAL(tableAccessStats.GetTableInfo().GetName(), "/Root/" + tableName);
            UNIT_ASSERT_VALUES_EQUAL(tableAccessStats.GetEraseRow().GetCount(), 1);
        }

        Cout << "========= Read table =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);
            UNIT_ASSERT_VALUES_EQUAL(tableState, "key = 2, value = 3\nkey = 4, value = 5\n");
        }
    }

    Y_UNIT_TEST(RejectOnChangeQueueOverflow) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false).SetChangesQueueItemsLimit(1);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        auto opts = TShardedTableOptions().Indexes({{"by_value", {"value"}, {}, NKikimrSchemeOp::EIndexTypeGlobalAsync}});

        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);

        TVector<THolder<IEventHandle>> blockedEnqueueRecords;
        auto prevObserverFunc = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NChangeExchange::TEvChangeExchange::EvEnqueueRecords) {
                blockedEnqueueRecords.emplace_back(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        const ui64 shard = shards[0];
        const ui32 rowCount = 1;

        ui64 txId = 100;

        Cout << "========= Send immediate write, expecting success =========\n";
        {
            Upsert(runtime, sender, shard, tableId, opts.Columns_, rowCount, ++txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        }

        UNIT_ASSERT_VALUES_EQUAL(blockedEnqueueRecords.size(), rowCount);

        Cout << "========= Send immediate write, expecting overloaded =========\n";
        {
            Upsert(runtime, sender, shard, tableId, opts.Columns_, rowCount, ++txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED);
        }

        Cout << "========= Send immediate write + OverloadSubscribe, expecting overloaded =========\n";
        {
            ui64 secNo = 55;

            auto request = MakeWriteRequest(++txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT, tableId, opts.Columns_, rowCount);
            request->Record.SetOverloadSubscribe(secNo);
           
            auto writeResult = Write(runtime, sender, shard, std::move(request), NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetOverloadSubscribed(), secNo);
        }

    }  // Y_UNIT_TEST

    using TWriteRequestPtr = std::unique_ptr<NEvents::TDataEvents::TEvWrite>;
    using TModifyWriteRequestCallback = std::function<void(const TWriteRequestPtr&)>;

    void PrepareMultiShardWrite(
            TTestActorRuntime& runtime,
            const TActorId& sender,
            const TTableId& tableId,
            ui64 txId, ui64& coordinator, ui64& minStep, ui64& maxStep,
            ui64 shardId,
            i32 key,
            i32 value,
            ui64 arbiterShard,
            const std::vector<ui64>& sendingShards,
            const std::vector<ui64>& receivingShards,
            const TModifyWriteRequestCallback& modifyWriteRequest = [](auto&){})
    {
        Cerr << "========= Sending prepare to " << shardId << " =========" << Endl;

        auto req = std::make_unique<NEvents::TDataEvents::TEvWrite>(txId, NKikimrDataEvents::TEvWrite::MODE_VOLATILE_PREPARE);

        const std::vector<ui32> columnIds({ 1, 2 });

        TVector<TCell> cells;
        cells.push_back(TCell::Make(key));
        cells.push_back(TCell::Make(value));
        TSerializedCellMatrix matrix(cells, 1, columnIds.size());
        TString data = matrix.ReleaseBuffer();
        const ui64 payloadIndex = req->AddPayload(TRope(std::move(data)));
        req->AddOperation(NKikimrDataEvents::TEvWrite_TOperation::OPERATION_UPSERT, tableId, columnIds, payloadIndex, NKikimrDataEvents::FORMAT_CELLVEC);

        auto* kqpLocks = req->Record.MutableLocks();
        kqpLocks->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
        if (arbiterShard) {
            kqpLocks->SetArbiterShard(arbiterShard);
        }
        for (ui64 sendingShard : sendingShards) {
            kqpLocks->AddSendingShards(sendingShard);
        }
        for (ui64 receivingShard : receivingShards) {
            kqpLocks->AddReceivingShards(receivingShard);
        }

        modifyWriteRequest(req);

        SendViaPipeCache(runtime, shardId, sender, std::move(req));

        auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvWriteResult>(sender);
        auto* msg = ev->Get();
        UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED);
        minStep = Max(minStep, msg->Record.GetMinStep());
        maxStep = Min(maxStep, msg->Record.GetMaxStep());
        UNIT_ASSERT_VALUES_EQUAL(msg->Record.DomainCoordinatorsSize(), 1);
        if (coordinator == 0) {
            coordinator = msg->Record.GetDomainCoordinators(0);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(coordinator, msg->Record.GetDomainCoordinators(0));
        }
    }

    void RunUpsertWithArbiter(
            TTestActorRuntime& runtime,
            const TTableId& tableId,
            const std::vector<ui64>& shards,
            const ui64 txId,
            const size_t expectedReadSets,
            int keyBase = 1,
            int keyFactor = 10,
            const std::unordered_set<size_t>& shardsWithoutPrepare = {},
            const std::unordered_set<size_t>& shardsWithBrokenLocks = {},
            NKikimrDataEvents::TEvWriteResult::EStatus expectedStatus = NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED,
            const std::unordered_map<ui64, NKikimrDataEvents::TEvWriteResult::EStatus>& expectedShardStatus = {})
    {
        auto sender = runtime.AllocateEdgeActor();

        ui64 coordinator = 0;
        ui64 minStep = 0;
        ui64 maxStep = Max<ui64>();

        size_t expectedResults = 0;
        for (size_t i = 0; i < shards.size(); ++i) {
            if (shardsWithoutPrepare.contains(i)) {
                continue;
            }

            const ui64 shardId = shards.at(i);
            const i32 key = i * keyFactor + keyBase;
            const i32 value = key * keyFactor + keyBase;

            PrepareMultiShardWrite(
                runtime,
                sender,
                tableId,
                txId, coordinator, minStep, maxStep,
                shardId,
                key,
                value,
                /* arbiter */ shards.at(0),
                /* sending */ shards,
                /* receiving */ shards,
                [&](const TWriteRequestPtr& req) {
                    // We use a lock that should have never existed to simulate a broken lock
                    if (shardsWithBrokenLocks.contains(i)) {
                        auto* kqpLock = req->Record.MutableLocks()->AddLocks();
                        kqpLock->SetLockId(txId);
                        kqpLock->SetDataShard(shardId);
                        kqpLock->SetGeneration(1);
                        kqpLock->SetCounter(1);
                        kqpLock->SetSchemeShard(tableId.PathId.OwnerId);
                        kqpLock->SetPathId(tableId.PathId.LocalPathId);
                    }
                });

            ++expectedResults;
        }

        size_t observedReadSets = 0;
        auto observeReadSets = runtime.AddObserver<TEvTxProcessing::TEvReadSet>(
            [&](TEvTxProcessing::TEvReadSet::TPtr&) {
                ++observedReadSets;
            });

        Cerr << "========= Sending propose to coordinator " << coordinator << " =========" << Endl;
        SendProposeToCoordinator(
            runtime, sender, shards, {
                .TxId = txId,
                .Coordinator = coordinator,
                .MinStep = minStep,
                .MaxStep = maxStep,
                .Volatile = true,
            });

        Cerr << "========= Waiting for write results =========" << Endl;
        for (size_t i = 0; i < expectedResults; ++i) {
            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvWriteResult>(sender);
            auto* msg = ev->Get();
            auto it = expectedShardStatus.find(msg->Record.GetOrigin());
            if (it != expectedShardStatus.end()) {
                UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetStatus(), it->second);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetStatus(), expectedStatus);
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(observedReadSets, expectedReadSets);
    }

    Y_UNIT_TEST(UpsertNoLocksArbiter) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key))
                WITH (PARTITION_AT_KEYS = (10, 20, 30));
            )"),
            "SUCCESS");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 4u);

        RunUpsertWithArbiter(
            runtime, tableId, shards,
            /* txId */ 1000001,
            // arbiter will send 6 readsets (3 decisions + 3 expectations)
            // shards will send 2 readsets each (decision + expectation)
            /* expectedReadSets */ 6 + 3 * 2);

        Cerr << "========= Checking table =========" << Endl;
        {
            auto tableState = ReadTable(server, shards, tableId);
            UNIT_ASSERT_VALUES_EQUAL(tableState,
                "key = 1, value = 11\n"
                "key = 11, value = 111\n"
                "key = 21, value = 211\n"
                "key = 31, value = 311\n");
        }
    }

    Y_UNIT_TEST(UpsertLostPrepareArbiter) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key))
                WITH (PARTITION_AT_KEYS = (10, 20, 30));
            )"),
            "SUCCESS");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 4u);

        for (size_t i = 0; i < shards.size(); ++i) {
            RunUpsertWithArbiter(
                runtime, tableId, shards,
                /* txId */ 1000001 + i,
                // arbiter will send 3 or 6 readsets (3 nodata or 3 decisions + 3 expectations)
                // shards will send 1 or 2 readsets each (nodata or decistion + expectation)
                /* expectedReadSets */ (i == 0 ? 3 + 3 * 2 : 6 + 2 * 2 + 1),
                /* keyBase */ 1 + i,
                /* keyFactor */ 10,
                /* shardsWithoutPrepare */ { i },
                /* shardsWithBrokenLocks */ {},
                /* expectedStatus */ NKikimrDataEvents::TEvWriteResult::STATUS_ABORTED);
        }

        Cerr << "========= Checking table =========" << Endl;
        {
            auto tableState = ReadTable(server, shards, tableId);
            UNIT_ASSERT_VALUES_EQUAL(tableState, "");
        }
    }

    Y_UNIT_TEST(UpsertBrokenLockArbiter) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key))
                WITH (PARTITION_AT_KEYS = (10, 20, 30));
            )"),
            "SUCCESS");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 4u);

        for (size_t i = 0; i < shards.size(); ++i) {
            RunUpsertWithArbiter(
                runtime, tableId, shards,
                /* txId */ 1000001 + i,
                // arbiter will send 3 or 6 readsets (3 aborts or 3 decisions + 3 expectations)
                // shards will send 1 or 2 readsets each (abort or decision + expectation)
                /* expectedReadSets */ (i == 0 ? 3 + 3 * 2 : 6 + 2 * 2 + 1),
                /* keyBase */ 1 + i,
                /* keyFactor */ 10,
                /* shardsWithoutPrepare */ {},
                /* shardsWithBrokenLocks */ { i },
                /* expectedStatus */ NKikimrDataEvents::TEvWriteResult::STATUS_ABORTED,
                /* expectedShardStatus */ { { shards[i], NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN } });
        }

        Cerr << "========= Checking table =========" << Endl;
        {
            auto tableState = ReadTable(server, shards, tableId);
            UNIT_ASSERT_VALUES_EQUAL(tableState, "");
        }
    }

    Y_UNIT_TEST(UpsertNoLocksArbiterRestart) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key))
                WITH (PARTITION_AT_KEYS = (10, 20, 30));
            )"),
            "SUCCESS");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 4u);

        ui64 txId = 1000001;
        ui64 coordinator = 0;
        ui64 minStep = 0;
        ui64 maxStep = Max<ui64>();

        for (size_t i = 0; i < shards.size(); ++i) {
            ui64 shardId = shards.at(i);
            i32 key = 10 * i + 1;
            i32 value = 100 * i + 11;
            PrepareMultiShardWrite(
                runtime,
                sender,
                tableId,
                txId, coordinator, minStep, maxStep,
                shardId,
                key,
                value,
                /* arbiter */ shards.at(0),
                /* sending */ shards,
                /* receiving */ shards);
        }

        std::vector<TEvTxProcessing::TEvReadSet::TPtr> blockedReadSets;
        auto blockReadSets = runtime.AddObserver<TEvTxProcessing::TEvReadSet>(
            [&](TEvTxProcessing::TEvReadSet::TPtr& ev) {
                Cerr << "... blocking readset" << Endl;
                blockedReadSets.push_back(std::move(ev));
            });

        Cerr << "========= Sending propose to coordinator " << coordinator << " =========" << Endl;
        SendProposeToCoordinator(
            runtime, sender, shards, {
                .TxId = txId,
                .Coordinator = coordinator,
                .MinStep = minStep,
                .MaxStep = maxStep,
                .Volatile = true,
            });

        // arbiter will send 3 expectations
        // shards will send 1 commit decision + 1 expectation
        size_t expectedReadSets = 3 + 3 * 2;
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(blockedReadSets.size(), expectedReadSets);

        // Reboot arbiter
        blockedReadSets.clear();
        Cerr << "========= Rebooting arbiter =========" << Endl;
        RebootTablet(runtime, shards.at(0), sender);
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(blockedReadSets.size(), expectedReadSets);

        blockReadSets.Remove();
        Cerr << "========= Unblocking readsets =========" << Endl;
        for (auto& ev : blockedReadSets) {
            runtime.Send(ev.Release(), 0, true);
        }
        runtime.SimulateSleep(TDuration::Seconds(1));

        Cerr << "========= Checking table =========" << Endl;
        {
            auto tableState = ReadTable(server, shards, tableId);
            UNIT_ASSERT_VALUES_EQUAL(tableState,
                "key = 1, value = 11\n"
                "key = 11, value = 111\n"
                "key = 21, value = 211\n"
                "key = 31, value = 311\n");
        }
    }

    Y_UNIT_TEST(UpsertLostPrepareArbiterRestart) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key))
                WITH (PARTITION_AT_KEYS = (10, 20, 30));
            )"),
            "SUCCESS");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 4u);

        ui64 txId = 1000001;
        ui64 coordinator = 0;
        ui64 minStep = 0;
        ui64 maxStep = Max<ui64>();

        // Note: we skip prepare at the last shard, so tx must abort
        for (size_t i = 0; i < shards.size()-1; ++i) {
            ui64 shardId = shards.at(i);
            i32 key = 10 * i + 1;
            i32 value = 100 * i + 11;
            PrepareMultiShardWrite(
                runtime,
                sender,
                tableId,
                txId, coordinator, minStep, maxStep,
                shardId,
                key,
                value,
                /* arbiter */ shards.at(0),
                /* sending */ shards,
                /* receiving */ shards);
        }

        std::vector<TEvTxProcessing::TEvReadSet::TPtr> blockedReadSets;
        auto blockReadSets = runtime.AddObserver<TEvTxProcessing::TEvReadSet>(
            [&](TEvTxProcessing::TEvReadSet::TPtr& ev) {
                Cerr << "... blocking readset" << Endl;
                blockedReadSets.push_back(std::move(ev));
            });

        Cerr << "========= Sending propose to coordinator " << coordinator << " =========" << Endl;
        SendProposeToCoordinator(
            runtime, sender, shards, {
                .TxId = txId,
                .Coordinator = coordinator,
                .MinStep = minStep,
                .MaxStep = maxStep,
                .Volatile = true,
            });

        // arbiter will send 3 expectations
        // shards will send 1 commit decision + 1 expectation
        size_t expectedReadSets = 3 + 2 * 2;
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(blockedReadSets.size(), expectedReadSets);

        // Reboot arbiter
        blockedReadSets.clear();
        Cerr << "========= Rebooting arbiter =========" << Endl;
        RebootTablet(runtime, shards.at(0), sender);
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(blockedReadSets.size(), expectedReadSets);

        blockReadSets.Remove();
        Cerr << "========= Unblocking readsets =========" << Endl;
        for (auto& ev : blockedReadSets) {
            runtime.Send(ev.Release(), 0, true);
        }
        runtime.SimulateSleep(TDuration::Seconds(1));

        Cerr << "========= Checking table =========" << Endl;
        {
            auto tableState = ReadTable(server, shards, tableId);
            UNIT_ASSERT_VALUES_EQUAL(tableState, "");
        }
    }

} // Y_UNIT_TEST_SUITE
} // namespace NKikimr

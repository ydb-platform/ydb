#include "datashard_active_transaction.h"
#include "datashard_ut_read_table.h"
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/long_tx_service/public/lock_handle.h>
#include "datashard_ut_common_kqp.h"

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;
using namespace NDataShardReadTableTest;

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
        runtime.GetAppData().AllowReadTableImmediate = true;

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
            auto tableState = TReadTableState(server, MakeReadTableSettings("/Root/table-1")).All();
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
            auto tableState1 = TReadTableState(server, MakeReadTableSettings("/Root/table-1")).All();
            auto tableState2 = TReadTableState(server, MakeReadTableSettings("/Root/table-2")).All();
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
            auto tableState = TReadTableState(server, MakeReadTableSettings("/Root/table-1")).All();
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
            auto tableState = TReadTableState(server, MakeReadTableSettings("/Root/table-1")).All();
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
            auto tableState = TReadTableState(server, MakeReadTableSettings("/Root/table-1")).All();
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
            auto tableState = TReadTableState(server, MakeReadTableSettings("/Root/table-1")).All();
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
            auto tableState = TReadTableState(server, MakeReadTableSettings("/Root/table-1")).All();
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
            auto tableState = TReadTableState(server, MakeReadTableSettings("/Root/table-1")).All();
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
            auto tableState = TReadTableState(server, MakeReadTableSettings("/Root/table-1")).All();
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
            auto tableState = TReadTableState(server, MakeReadTableSettings("/Root/table-1")).All();
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
            auto tableState = TReadTableState(server, MakeReadTableSettings("/Root/table-1")).All();
            UNIT_ASSERT_VALUES_EQUAL(tableState, expectedTableState);
        }

        Cout << "========= Send immediate insert with duplicate, keys -2, 0, 2 =========\n";
        {
            auto request = MakeWriteRequest(++txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT, tableId, opts.Columns_, rowCount, -2);
            const auto writeResult = Write(runtime, sender, shard, std::move(request), NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
        }

        Cout << "========= Read table =========\n";
        {
            auto tableState = TReadTableState(server, MakeReadTableSettings("/Root/table-1")).All();
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
            auto tableState = TReadTableState(server, MakeReadTableSettings("/Root/table-1")).All();
            UNIT_ASSERT_VALUES_EQUAL(tableState, "");
        }         

        Cout << "========= Send immediate insert =========\n";
        {
            Insert(runtime, sender, shard, tableId, opts.Columns_, rowCount, txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        }

        Cout << "========= Read table =========\n";
        {
            auto tableState = TReadTableState(server, MakeReadTableSettings("/Root/table-1")).All();
            UNIT_ASSERT_VALUES_EQUAL(tableState, expectedTableState);
        }

        Cout << "========= Send immediate upsert, change one row =========\n";
        {
            UpsertOneKeyValue(runtime, sender, shard, tableId, opts.Columns_, 0, 555, txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        }

        Cout << "========= Read table =========\n";
        {
            auto tableState = TReadTableState(server, MakeReadTableSettings("/Root/table-1")).All();
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
            auto tableState = TReadTableState(server, MakeReadTableSettings("/Root/table-1")).All();
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
            auto tableState = TReadTableState(server, MakeReadTableSettings("/Root/" + tableName)).All();
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
            auto tableState = TReadTableState(server, MakeReadTableSettings("/Root/" + tableName1)).All();
            UNIT_ASSERT_VALUES_EQUAL(tableState, expectedTableState);
            tableState = TReadTableState(server, MakeReadTableSettings("/Root/"+ tableName2)).All();
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
            auto tableState = TReadTableState(server, MakeReadTableSettings("/Root/table-1")).All();
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
            auto tableState = TReadTableState(server, MakeReadTableSettings("/Root/table-1")).All();
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
            auto tableState = TReadTableState(server, MakeReadTableSettings("/Root/" + tableName)).All();
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
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table`
                ORDER BY key;
            )"),
            "{ items { int32_value: 1 } items { int32_value: 11 } }, "
            "{ items { int32_value: 11 } items { int32_value: 111 } }, "
            "{ items { int32_value: 21 } items { int32_value: 211 } }, "
            "{ items { int32_value: 31 } items { int32_value: 311 } }");
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
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table`
                ORDER BY key;
            )"),
            "");
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
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table`
                ORDER BY key;
            )"),
            "");
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
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table`
                ORDER BY key;
            )"),
            "{ items { int32_value: 1 } items { int32_value: 11 } }, "
            "{ items { int32_value: 11 } items { int32_value: 111 } }, "
            "{ items { int32_value: 21 } items { int32_value: 211 } }, "
            "{ items { int32_value: 31 } items { int32_value: 311 } }");
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
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table`
                ORDER BY key;
            )"),
            "");
    }

    Y_UNIT_TEST(ImmediateAndPlannedCommittedOpsRace) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            // It's easier to reproduce without volatile transactions, since
            // then we can block pipeline by blocking readsets
            .SetEnableDataShardVolatileTransactions(false);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key))
                WITH (PARTITION_AT_KEYS = (10));
            )"),
            "SUCCESS");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);

        TVector<TShardedTableOptions::TColumn> columns{
            {"key", "Int32", true, false},
            {"value", "Int32", false, false},
        };

        const ui64 coordinator = ChangeStateStorage(Coordinator, server->GetSettings().Domain);

        const ui64 lockTxId1 = 1234567890001;
        const ui64 lockTxId2 = 1234567890002;
        const ui64 lockTxId3 = 1234567890003;
        const ui64 lockNodeId = runtime.GetNodeId(0);
        NLongTxService::TLockHandle lockHandle1(lockTxId1, runtime.GetActorSystem(0));
        NLongTxService::TLockHandle lockHandle2(lockTxId2, runtime.GetActorSystem(0));
        NLongTxService::TLockHandle lockHandle3(lockTxId3, runtime.GetActorSystem(0));

        auto shard1 = shards.at(0);
        auto shard1actor = ResolveTablet(runtime, shard1);
        auto shard2 = shards.at(1);

        NKikimrDataEvents::TLock lock1shard1;
        NKikimrDataEvents::TLock lock1shard2;
        NKikimrDataEvents::TLock lock2;

        // 1. Make a read (lock1 shard1)
        auto read1sender = runtime.AllocateEdgeActor();
        {
            Cerr << "... making a read from " << shard1 << Endl;
            auto req = std::make_unique<TEvDataShard::TEvRead>();
            {
                auto& record = req->Record;
                record.SetReadId(1);
                record.MutableTableId()->SetOwnerId(tableId.PathId.OwnerId);
                record.MutableTableId()->SetTableId(tableId.PathId.LocalPathId);
                record.AddColumns(1);
                record.AddColumns(2);
                record.SetLockTxId(lockTxId1);
                record.SetLockNodeId(lockNodeId);
                record.SetResultFormat(NKikimrDataEvents::FORMAT_CELLVEC);
                i32 key = 1;
                TVector<TCell> keys;
                keys.push_back(TCell::Make(key));
                req->Keys.push_back(TSerializedCellVec(TSerializedCellVec::Serialize(keys)));
            }
            ForwardToTablet(runtime, shard1, read1sender, req.release());
            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvReadResult>(read1sender);
            auto* res = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetFinished(), true);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetTxLocks().size(), 1u);
            lock1shard1 = res->Record.GetTxLocks().at(0);
            UNIT_ASSERT_C(lock1shard1.GetCounter() < 1000, "Unexpected lock in the result: " << lock1shard1.ShortDebugString());
        }

        // 2. Make an uncommitted write (lock1 shard2)
        {
            Cerr << "... making an uncommmited write to " << shard2 << Endl;
            auto req = MakeWriteRequestOneKeyValue(
                std::nullopt,
                NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE,
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                tableId,
                columns,
                11, 1101);
            req->SetLockId(lockTxId1, lockNodeId);
            auto result = Write(runtime, sender, shard2, std::move(req));
            UNIT_ASSERT_VALUES_EQUAL(result.GetTxLocks().size(), 1u);
            lock1shard2 = result.GetTxLocks().at(0);
            UNIT_ASSERT_C(lock1shard2.GetCounter() < 1000, "Unexpected lock in the result: " << lock1shard2.ShortDebugString());
        }

        // 3. Make an uncommitted write (lock2 shard1)
        {
            Cerr << "... making an uncommmited write to " << shard1 << Endl;
            auto req = MakeWriteRequestOneKeyValue(
                std::nullopt,
                NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE,
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                tableId,
                columns,
                2, 202);
            req->SetLockId(lockTxId2, lockNodeId);
            auto result = Write(runtime, sender, shard1, std::move(req));
            UNIT_ASSERT_VALUES_EQUAL(result.GetTxLocks().size(), 1u);
            lock2 = result.GetTxLocks().at(0);
            UNIT_ASSERT_C(lock2.GetCounter() < 1000, "Unexpected lock in the result: " << lock2.ShortDebugString());
        }

        // 4. Break lock2 so later we could make an aborted distributed commit
        {
            Cerr << "... making an immediate write to " << shard1 << Endl;
            auto req = MakeWriteRequestOneKeyValue(
                std::nullopt,
                NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE,
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                tableId,
                columns,
                2, 203);
            Write(runtime, sender, shard1, std::move(req));
        }

        // Start blocking readsets
        TBlockEvents<TEvTxProcessing::TEvReadSet> blockedReadSets(runtime);

        // Prepare an upsert (readsets flow between shards)
        ui64 txId1 = 1234567890011;
        auto tx1sender = runtime.AllocateEdgeActor();
        {
            auto req1 = MakeWriteRequestOneKeyValue(
                txId1,
                NKikimrDataEvents::TEvWrite::MODE_PREPARE,
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                tableId,
                columns,
                3, 304);
            req1->Record.MutableLocks()->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
            req1->Record.MutableLocks()->AddSendingShards(shard1);
            req1->Record.MutableLocks()->AddSendingShards(shard2);
            req1->Record.MutableLocks()->AddReceivingShards(shard1);
            req1->Record.MutableLocks()->AddReceivingShards(shard2);
            *req1->Record.MutableLocks()->AddLocks() = lock1shard1;

            auto req2 = MakeWriteRequestOneKeyValue(
                txId1,
                NKikimrDataEvents::TEvWrite::MODE_PREPARE,
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                tableId,
                columns,
                13, 1304);
            req2->Record.MutableLocks()->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
            req2->Record.MutableLocks()->AddSendingShards(shard1);
            req2->Record.MutableLocks()->AddSendingShards(shard2);
            req2->Record.MutableLocks()->AddReceivingShards(shard1);
            req2->Record.MutableLocks()->AddReceivingShards(shard2);
            *req2->Record.MutableLocks()->AddLocks() = lock1shard2;

            Cerr << "... preparing tx1 at " << shard1 << Endl;
            auto res1 = Write(runtime, tx1sender, shard1, std::move(req1));
            Cerr << "... preparing tx1 at " << shard2 << Endl;
            auto res2 = Write(runtime, tx1sender, shard2, std::move(req2));

            ui64 minStep = Max(res1.GetMinStep(), res2.GetMinStep());
            ui64 maxStep = Min(res1.GetMaxStep(), res2.GetMaxStep());

            Cerr << "... planning tx1 at " << coordinator << Endl;
            SendProposeToCoordinator(
                runtime, tx1sender, shards, {
                    .TxId = txId1,
                    .Coordinator = coordinator,
                    .MinStep = minStep,
                    .MaxStep = maxStep,
                });
        }

        runtime.WaitFor("blocked readsets", [&]{ return blockedReadSets.size() >= 2; });
        UNIT_ASSERT_VALUES_EQUAL(blockedReadSets.size(), 2u);

        // Start blocking new plan steps
        TBlockEvents<TEvTxProcessing::TEvPlanStep> blockedPlanSteps(runtime);

        // Prepare an upsert (readset flows from shard 1 to shard 2, already broken)
        // Must not conflict with other transactions
        ui64 txId2 = 1234567890012;
        auto tx2sender = runtime.AllocateEdgeActor();
        {
            auto req1 = MakeWriteRequestOneKeyValue(
                txId2,
                NKikimrDataEvents::TEvWrite::MODE_PREPARE,
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                tableId,
                columns,
                5, 505);
            req1->Record.MutableLocks()->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
            req1->Record.MutableLocks()->AddSendingShards(shard1);
            req1->Record.MutableLocks()->AddReceivingShards(shard2);
            *req1->Record.MutableLocks()->AddLocks() = lock2;

            auto req2 = MakeWriteRequestOneKeyValue(
                txId2,
                NKikimrDataEvents::TEvWrite::MODE_PREPARE,
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                tableId,
                columns,
                15, 1505);
            req2->Record.MutableLocks()->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
            req2->Record.MutableLocks()->AddSendingShards(shard1);
            req2->Record.MutableLocks()->AddReceivingShards(shard2);

            Cerr << "... preparing tx2 at " << shard1 << Endl;
            auto res1 = Write(runtime, tx2sender, shard1, std::move(req1));
            Cerr << "... preparing tx2 at " << shard2 << Endl;
            auto res2 = Write(runtime, tx2sender, shard2, std::move(req2));

            ui64 minStep = Max(res1.GetMinStep(), res2.GetMinStep());
            ui64 maxStep = Min(res1.GetMaxStep(), res2.GetMaxStep());

            Cerr << "... planning tx2 at " << coordinator << Endl;
            SendProposeToCoordinator(
                runtime, tx2sender, shards, {
                    .TxId = txId2,
                    .Coordinator = coordinator,
                    .MinStep = minStep,
                    .MaxStep = maxStep,
                });
        }

        runtime.WaitFor("blocked plan steps", [&]{ return blockedPlanSteps.size() >= 2; });
        UNIT_ASSERT_VALUES_EQUAL(blockedPlanSteps.size(), 2u);

        // Block TEvPrivate::TEvProgressTransaction for shard1
        TBlockEvents<IEventHandle> blockedProgress(runtime,
            [&](const TAutoPtr<IEventHandle>& ev) {
                return ev->GetRecipientRewrite() == shard1actor &&
                    ev->GetTypeRewrite() == EventSpaceBegin(TKikimrEvents::ES_PRIVATE) + 0;
            });

        blockedPlanSteps.Unblock();
        runtime.WaitFor("blocked progress", [&]{ return blockedProgress.size() >= 1; });
        runtime.SimulateSleep(TDuration::MilliSeconds(1)); // let it commit
        UNIT_ASSERT_VALUES_EQUAL(blockedProgress.size(), 1u);

        // Make an unrelated immediate write, this will pin write (and future snapshot) version to tx2
        {
            Cerr << "... making an immediate write to " << shard1 << Endl;
            auto req = MakeWriteRequestOneKeyValue(
                std::nullopt,
                NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE,
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                tableId,
                columns,
                4, 406);
            Write(runtime, sender, shard1, std::move(req));
        }

        // Block commit attempts at shard1
        TBlockEvents<TEvBlobStorage::TEvPut> blockedCommits(runtime,
            [&](const TEvBlobStorage::TEvPut::TPtr& ev) {
                auto* msg = ev->Get();
                return msg->Id.TabletID() == shard1 && msg->Id.Channel() == 0;
            });

        // Make an uncommitted write to a key overlapping with tx1
        // Since tx1 has been validated, and reads are pinned at tx2, tx3 will
        // be after tx1 and blocked by a read dependency. Since tx2 has not
        // entered the pipeline yet, version will not be above tx2.
        auto tx3sender = runtime.AllocateEdgeActor();
        {
            Cerr << "... starting uncommitted upsert at " << shard1 << Endl;
            auto req = MakeWriteRequestOneKeyValue(
                std::nullopt,
                NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE,
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                tableId,
                columns,
                3, 307);
            req->SetLockId(lockTxId3, lockNodeId);
            runtime.SendToPipe(shard1, tx3sender, req.release());
        }

        // Wait for some time and make sure there have been no unexpected
        // commits, which would indicate the upsert is blocked by tx1.
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(blockedCommits.size(), 0u,
            "The uncommitted upsert didn't block. Something may have changed and the test needs to be revised.");

        // Now, while blocking commits, unblock progress and let tx2 to execute,
        // which will abort due to broken locks.
        blockedProgress.Unblock();
        blockedProgress.Stop();

        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        size_t commitsAfterTx2 = blockedCommits.size();
        Cerr << "... observed " << commitsAfterTx2 << " commits after tx2 unblock" << Endl;
        UNIT_ASSERT_C(commitsAfterTx2 >= 2,
            "Expected tx2 to produce at least 2 commits (store out rs + abort tx)"
            << ", observed " << commitsAfterTx2 << ". Something may have changed.");

        // Now, while still blocking commits, unblock readsets
        // Everything will unblock and execute tx1 then tx3
        blockedReadSets.Unblock();
        blockedReadSets.Stop();

        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        size_t commitsAfterTx3 = blockedCommits.size() - commitsAfterTx2;
        Cerr << "... observed " << commitsAfterTx3 << " more commits after readset unblock" << Endl;
        UNIT_ASSERT_C(commitsAfterTx3 >= 2,
            "Expected at least 2 commits after readset unblock (tx1, tx3), but only "
            << commitsAfterTx3 << " have been observed.");

        // Finally, stop blocking commits
        // We expect completion handlers to run in tx3, tx1, tx2 order, triggering the bug
        blockedCommits.Unblock();
        blockedCommits.Stop();

        runtime.SimulateSleep(TDuration::MilliSeconds(1));

        // Check tx3 reply
        {
            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvWriteResult>(tx3sender);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        }

        // Check tx1 reply
        {
            auto ev1 = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvWriteResult>(tx1sender);
            UNIT_ASSERT_VALUES_EQUAL(ev1->Get()->Record.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
            auto ev2 = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvWriteResult>(tx1sender);
            UNIT_ASSERT_VALUES_EQUAL(ev2->Get()->Record.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        }

        // Check tx2 reply
        {
            auto ev1 = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvWriteResult>(tx2sender);
            UNIT_ASSERT_VALUES_EQUAL(ev1->Get()->Record.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN);
            auto ev2 = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvWriteResult>(tx2sender);
            UNIT_ASSERT_VALUES_EQUAL(ev2->Get()->Record.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN);
        }
    }

    Y_UNIT_TEST(PreparedDistributedWritePageFault) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableDataShardVolatileTransactions(false);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        // Use a policy without levels and very small page sizes, effectively making each row on its own page
        NLocalDb::TCompactionPolicyPtr policy = NLocalDb::CreateDefaultTablePolicy();
        policy->MinDataPageSize = 1;

        auto opts = TShardedTableOptions()
                .Columns({{"key", "Int32", true, false},
                          {"value", "Int32", false, false}})
                .Policy(policy.Get());
        const auto& columns = opts.Columns_;
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table", opts);
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        const ui64 coordinator = ChangeStateStorage(Coordinator, server->GetSettings().Domain);

        const ui64 lockTxId1 = 1234567890001;
        const ui64 lockNodeId = runtime.GetNodeId(0);
        NLongTxService::TLockHandle lockHandle1(lockTxId1, runtime.GetActorSystem(0));

        auto shard1 = shards.at(0);
        NKikimrDataEvents::TLock lock1shard1;

        // 1. Make an uncommitted write (lock1 shard1)
        {
            Cerr << "... making an uncommmited write to " << shard1 << Endl;
            auto req = MakeWriteRequestOneKeyValue(
                std::nullopt,
                NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE,
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                tableId,
                columns,
                1, 11);
            req->SetLockId(lockTxId1, lockNodeId);
            auto result = Write(runtime, sender, shard1, std::move(req));
            UNIT_ASSERT_VALUES_EQUAL(result.GetTxLocks().size(), 1u);
            lock1shard1 = result.GetTxLocks().at(0);
            UNIT_ASSERT_C(lock1shard1.GetCounter() < 1000, "Unexpected lock in the result: " << lock1shard1.ShortDebugString());
        }

        // 2. Compact and reboot the tablet
        Cerr << "... compacting shard " << shard1 << Endl;
        CompactTable(runtime, shard1, tableId, false);
        Cerr << "... rebooting shard " << shard1 << Endl;
        RebootTablet(runtime, shard1, sender);
        runtime.SimulateSleep(TDuration::Seconds(1));

        // 3. Prepare a distributed write (single shard for simplicity)
        ui64 txId1 = 1234567890011;
        auto tx1sender = runtime.AllocateEdgeActor();
        {
            auto req1 = MakeWriteRequestOneKeyValue(
                txId1,
                NKikimrDataEvents::TEvWrite::MODE_PREPARE,
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                tableId,
                columns,
                1, 22);
            req1->Record.MutableLocks()->SetOp(NKikimrDataEvents::TKqpLocks::Commit);

            Cerr << "... preparing tx1 at " << shard1 << Endl;
            auto res1 = Write(runtime, tx1sender, shard1, std::move(req1));

            // Reboot, making sure tx is only loaded after it's planned
            // This causes tx to skip conflicts cache and go to execution
            // The first attempt to execute will page fault looking for conflicts
            // Tx will be released, and will trigger the bug on restore
            Cerr << "... rebooting shard " << shard1 << Endl;
            RebootTablet(runtime, shard1, sender);
            runtime.SimulateSleep(TDuration::Seconds(1));

            ui64 minStep = res1.GetMinStep();
            ui64 maxStep = res1.GetMaxStep();

            Cerr << "... planning tx1 at " << coordinator << Endl;
            SendProposeToCoordinator(
                runtime, tx1sender, { shard1 }, {
                    .TxId = txId1,
                    .Coordinator = coordinator,
                    .MinStep = minStep,
                    .MaxStep = maxStep,
                });
        }

        // 4. Check tx1 reply (it must succeed)
        {
            Cerr << "... waiting for tx1 result" << Endl;
            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvWriteResult>(tx1sender);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        }
    }

} // Y_UNIT_TEST_SUITE
} // namespace NKikimr

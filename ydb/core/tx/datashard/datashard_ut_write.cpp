#include "datashard_active_transaction.h"
#include "datashard_ut_read_table.h"
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NSchemeShard;
using namespace Tests;
using namespace NDataShardReadTableTest;

Y_UNIT_TEST_SUITE(DataShardWrite) {
    std::tuple<TTestActorRuntime&, Tests::TServer::TPtr, TActorId> TestCreateServer() {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.GetAppData().AllowReadTableImmediate = true;

        InitRoot(server, sender);

        return {runtime, server, sender};
    }

    Y_UNIT_TEST_TWIN(Upsert, EvWrite) {
        auto [runtime, server, sender] = TestCreateServer();

        auto opts = TShardedTableOptions();
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);

        auto rows = EvWrite ? TEvWriteRows{{{0, 1}}, {{2, 3}}, {{4, 5}}} : TEvWriteRows{};
        auto evWriteObservers = ReplaceEvProposeTransactionWithEvWrite(runtime, rows);

        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (0, 1);"));
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 3);"));
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (4, 5);"));

        auto table1state = TReadTableState(server, MakeReadTableSettings("/Root/table-1")).All();

        UNIT_ASSERT_VALUES_EQUAL(table1state, "key = 0, value = 1\n"
                                              "key = 2, value = 3\n"
                                              "key = 4, value = 5\n");
    }

    Y_UNIT_TEST(WriteImmediateOnShard) {
        auto [runtime, server, sender] = TestCreateServer();

        auto opts = TShardedTableOptions().Columns({{"key", "Uint32", true, false}, {"value", "Uint32", false, false}});
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);

        const ui32 rowCount = 3;
        ui64 txId = 100;
        Write(runtime, sender, shards[0], tableId, opts.Columns_, rowCount, txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);

        auto table1state = TReadTableState(server, MakeReadTableSettings("/Root/table-1")).All();

        UNIT_ASSERT_VALUES_EQUAL(table1state, "key = 0, value = 1\n"
                                              "key = 2, value = 3\n"
                                              "key = 4, value = 5\n");
    }

    Y_UNIT_TEST(WriteImmediateOnShardManyColumns) {
        auto [runtime, server, sender] = TestCreateServer();

        auto opts = TShardedTableOptions().Columns({{"key64", "Uint64", true, false}, {"key32", "Uint32", true, false},
                                                    {"value64", "Uint64", false, false}, {"value32", "Uint32", false, false}, {"valueUtf8", "Utf8", false, false}});
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);

        const ui32 rowCount = 3;
        ui64 txId = 100;
        Write(runtime, sender, shards[0], tableId, opts.Columns_, rowCount, txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);

        auto table1state = TReadTableState(server, MakeReadTableSettings("/Root/table-1")).All();

        UNIT_ASSERT_VALUES_EQUAL(table1state, "key64 = 0, key32 = 1, value64 = 2, value32 = 3, valueUtf8 = String_4\n"
                                              "key64 = 5, key32 = 6, value64 = 7, value32 = 8, valueUtf8 = String_9\n"
                                              "key64 = 10, key32 = 11, value64 = 12, value32 = 13, valueUtf8 = String_14\n");
    }

    Y_UNIT_TEST(WriteImmediateHugeKey) {
        auto [runtime, server, sender] = TestCreateServer();

        auto opts = TShardedTableOptions().Columns({{"key", "Utf8", true, false}});
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);

        TString hugeStringValue(NLimits::MaxWriteKeySize + 1, 'X');
        TSerializedCellMatrix matrix({TCell(hugeStringValue.c_str(), hugeStringValue.size())}, 1, 1);

        auto evWrite = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(100, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        ui64 payloadIndex = NKikimr::NEvWrite::TPayloadWriter<NKikimr::NEvents::TDataEvents::TEvWrite>(*evWrite).AddDataToPayload(matrix.ReleaseBuffer());
        evWrite->AddOperation(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT, tableId, {1}, payloadIndex, NKikimrDataEvents::FORMAT_CELLVEC);

        const auto& record = Write(runtime, sender, shards[0], std::move(evWrite), NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);

        UNIT_ASSERT_VALUES_EQUAL(record.GetIssues().size(), 1);
        UNIT_ASSERT(record.GetIssues(0).message().Contains("Row key size of 1049601 bytes is larger than the allowed threshold 1049600"));
    }

    Y_UNIT_TEST(WriteOnShard) {
        auto [runtime, server, sender] = TestCreateServer();

        TShardedTableOptions opts;
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);

        const ui32 rowCount = 3;
        ui64 txId = 100;
        Write(runtime, sender, shards[0], tableId, opts.Columns_, rowCount, txId, NKikimrDataEvents::TEvWrite::MODE_PREPARE);

        auto table1state = TReadTableState(server, MakeReadTableSettings("/Root/table-1")).All();

        UNIT_ASSERT_VALUES_EQUAL(table1state, "");
    }
}
}
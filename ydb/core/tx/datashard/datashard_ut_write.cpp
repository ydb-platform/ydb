#include "datashard_active_transaction.h"
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common_tx.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/test_tli.h>
#include <ydb/core/protos/query_stats.pb.h>
#include <ydb/core/tx/long_tx_service/public/lock_handle.h>
#include <ydb/library/services/services.pb.h>
#include "datashard_ut_common_kqp.h"

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;

Y_UNIT_TEST_SUITE(DataShardWrite) {

    constexpr i16 operator""_i16(unsigned long long val) { return static_cast<i16>(val); }
    constexpr i32 operator""_i32(unsigned long long val) { return static_cast<i32>(val); }
    constexpr ui32 operator""_ui32(unsigned long long val) { return static_cast<ui32>(val); }

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

    Y_UNIT_TEST(ExecSQLUpsertImmediate) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings
            .SetDomainName("Root")
            .SetUseRealThreads(false);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TShardedTableOptions opts;
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);

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

    Y_UNIT_TEST(UpsertWithDefaults) {

        auto [runtime, server, sender] = TestCreateServer();

        // Define a table with different column types
        auto opts = TShardedTableOptions()
            .Columns({
                {"key", "Uint64", true, false},           // key (id=1)
                {"uint8_val", "Uint8", false, false},     // id=2
                {"uint16_val", "Uint16", false, false},   // id=3
                {"uint32_val", "Uint32", false, false},   // id=4
                {"uint64_val", "Uint64", false, false},   // id=5
            });

        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const ui64 shard = shards[0];
        ui64 txId = 100;

        Cout << "========= Insert initial data =========\n";
        {
            TVector<ui32> columnIds = {1, 2, 3, 4, 5}; // all columns
            TVector<TCell> cells = {
                TCell::Make(ui64(1)),     // key = 1
                TCell::Make(ui8(2)),
                TCell::Make(ui16(3)),
                TCell::Make(ui32(4)),
                TCell::Make(ui64(5)),
            };

            auto result = Upsert(runtime, sender, shard, tableId, txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, columnIds, cells);

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        }

        Cout << "========= Verify initial data =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);
            UNIT_ASSERT_STRINGS_EQUAL(tableState,
                "key = 1, uint8_val = 2, uint16_val = 3, uint32_val = 4, uint64_val = 5\n");
        }

        Cout << "========= Upsert exist row with default values in columns 2, 3 =========\n";
        {
            TVector<ui32> columnIds = {1, 4, 5, 2, 3}; // key and numeric columns
            ui32 defaultFilledColumns = 2;

            TVector<TCell> cells = {
                TCell::Make(ui64(1)),    // key = 1
                TCell::Make(ui32(44)),
                TCell::Make(ui64(55)),
                TCell::Make(ui8(22)),
                TCell::Make(ui16(33)),

                TCell::Make(ui64(333)),    // key = 333
                TCell::Make(ui32(44)),
                TCell::Make(ui64(55)),
                TCell::Make(ui8(22)),
                TCell::Make(ui16(33))

            };

            auto result = UpsertWithDefaultValues(runtime, sender, shard, tableId, txId,
                                    NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, columnIds, cells, defaultFilledColumns);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        }

        Cout << "========= Verify results =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);

            UNIT_ASSERT_STRINGS_EQUAL(tableState,
                "key = 1, uint8_val = 2, uint16_val = 3, uint32_val = 44, uint64_val = 55\n"
                "key = 333, uint8_val = 22, uint16_val = 33, uint32_val = 44, uint64_val = 55\n");
        }

        Cout << "========= Try replace with default values in columns 2, 3 (should fail) =========\n";
        {
            TVector<ui32> columnIds = {1, 4, 5, 2, 3};
            ui32 defaultFilledColumns = 2;
            TVector<TCell> cells = {
                TCell::Make(ui64(1)),    // key = 1
                TCell::Make(ui32(944)),
                TCell::Make(ui64(955)),
                TCell::Make(ui8(92)),
                TCell::Make(ui16(933))
            };

            auto request = MakeWriteRequest(txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE, tableId, columnIds, cells, defaultFilledColumns);
            auto result = Write(runtime, sender, shard, std::move(request), NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);

            UNIT_ASSERT(result.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
        }

        Cout << "========= Verify results after fail operations =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);

            UNIT_ASSERT_STRINGS_EQUAL(tableState,
                "key = 1, uint8_val = 2, uint16_val = 3, uint32_val = 44, uint64_val = 55\n"
                "key = 333, uint8_val = 22, uint16_val = 33, uint32_val = 44, uint64_val = 55\n");
        }

        Cout << "========= Try upsert with default values in too much columns  (should fail) =========\n";
        {
            TVector<ui32> columnIds = {1, 4, 5, 2, 3};
            ui32 defaultFilledColumns = 9;
            TVector<TCell> cells = {
                TCell::Make(ui64(1)),    // key = 1
                TCell::Make(ui32(944)),
                TCell::Make(ui64(955)),
                TCell::Make(ui8(92)),
                TCell::Make(ui16(933))
            };

            auto result = UpsertWithDefaultValues(runtime, sender, shard, tableId, txId,
                NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, columnIds, cells, defaultFilledColumns, NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
            UNIT_ASSERT(result.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
        }
    }

    Y_UNIT_TEST(AsyncIndexKeySizeConstraint) {
        auto [runtime, server, sender] = TestCreateServer();

        auto opts = TShardedTableOptions()
            .Columns({
                {"key", "String", true, false},
                {"val1", "String", false, false},
                {"val2", "String", false, false},
                {"val3", "String", false, false}

            }).Indexes({{"by_val12", {"val1", "val2"}, {}, NKikimrSchemeOp::EIndexTypeGlobalAsync}});

        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const ui64 shard = shards[0];
        ui64 txId = 100;

        auto bigString = TString(NLimits::MaxWriteKeySize + 1,  'a');
        auto halfBigString1 = TString(NLimits::MaxWriteKeySize / 2,  'a');
        auto halfBigString2 = TString(bigString.size() - halfBigString1.size(),  'a');

        Cout << "========= Insert normal data {sad, qwe, null} =========\n";
        {
            TVector<ui32> columnIds = {1, 2};
            TVector<TCell> cells = {
                TCell("sad"),
                TCell("qwe"),
            };

            auto result = Upsert(runtime, sender, shard, tableId, txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, columnIds, cells, NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        }

        Cout << "========= Verify data =========\n";
        {
            auto expectedState = "key = sad\\0, val1 = qwe\\0, val2 = NULL, val3 = NULL\n";
            auto tableState = ReadTable(server, shards, tableId);
            UNIT_ASSERT_STRINGS_EQUAL(tableState, expectedState);
        }

        Cout << "========= Insert data with big string in index column =========\n";
        {
            TVector<ui32> columnIds = {1, 2};
            TVector<TCell> cells = {
                TCell("sad"),
                TCell("not qwe string"), // should not change
                TCell("asdasdg"),
                TCell(bigString.c_str(), bigString.size())
            };

            auto result = Upsert(runtime, sender, shard, tableId, txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, columnIds, cells, NKikimrDataEvents::TEvWriteResult::STATUS_CONSTRAINT_VIOLATION);

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_CONSTRAINT_VIOLATION);
            UNIT_ASSERT_VALUES_EQUAL(result.GetIssues().size(), 1);
            UNIT_ASSERT(result.GetIssues(0).message().Contains("Size of key in secondary index is more than 1049600"));
        }

        Cout << "========= Verify data (no changes should be) =========\n";
        {
            auto expectedState = "key = sad\\0, val1 = qwe\\0, val2 = NULL, val3 = NULL\n";
            auto tableState = ReadTable(server, shards, tableId);
            UNIT_ASSERT_STRINGS_EQUAL(tableState, expectedState);
        }

        Cout << "========= Insert initial data with half of threshold constraint in index column =========\n";
        {
            TVector<ui32> columnIds = {1, 2};
            TVector<TCell> cells = {
                TCell("sad"),
                TCell("qwe"),
                TCell("xyz"),
                TCell(halfBigString1.c_str(), halfBigString1.size())
            };

            auto result = Upsert(runtime, sender, shard, tableId, txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, columnIds, cells, NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        }

        Cout << "========= Verify data =========\n";
        {
            auto expectedState = "key = sad\\0, val1 = qwe\\0, val2 = NULL, val3 = NULL\n"
                                                    "key = xyz\\0, val1 = " + halfBigString1 + ", val2 = NULL, val3 = NULL\n";
            auto tableState = ReadTable(server, shards, tableId);
            UNIT_ASSERT_STRINGS_EQUAL(tableState, expectedState);
        }

        Cout << "========= Upsert data with other half of threshold constraint in index column (should fail) =========\n";
        {
            TVector<ui32> columnIds = {1, 3};
            TVector<TCell> cells = {
                TCell("sad"),
                TCell("qwe"),
                TCell("xyz"),
                TCell(halfBigString2.c_str(), halfBigString2.size())
            };

            auto result = Upsert(runtime, sender, shard, tableId, txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, columnIds, cells, NKikimrDataEvents::TEvWriteResult::STATUS_CONSTRAINT_VIOLATION);

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_CONSTRAINT_VIOLATION);
            UNIT_ASSERT_VALUES_EQUAL(result.GetIssues().size(), 1);
            UNIT_ASSERT(result.GetIssues(0).message().Contains("Size of key in secondary index is more than 1049600"));
        }

        Cout << "========= Verify data =========\n";
        {
            auto expectedState = "key = sad\\0, val1 = qwe\\0, val2 = NULL, val3 = NULL\n"
                                                    "key = xyz\\0, val1 = " + halfBigString1 + ", val2 = NULL, val3 = NULL\n";
            auto tableState = ReadTable(server, shards, tableId);
            UNIT_ASSERT_STRINGS_EQUAL(tableState, expectedState);
        }

        Cout << "========= Upsert data with threshold constraint in index column and key column in sum (should fail) =========\n";
        {
            TVector<ui32> columnIds = {1, 2};
            TVector<TCell> cells = {
                TCell(halfBigString1.c_str(), halfBigString1.size()),
                TCell(halfBigString2.c_str(), halfBigString2.size())
            };

            auto result = Upsert(runtime, sender, shard, tableId, txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, columnIds, cells, NKikimrDataEvents::TEvWriteResult::STATUS_CONSTRAINT_VIOLATION);

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_CONSTRAINT_VIOLATION);
            UNIT_ASSERT_VALUES_EQUAL(result.GetIssues().size(), 1);
            UNIT_ASSERT(result.GetIssues(0).message().Contains("Size of key in secondary index is more than 1049600"));
        }

        Cout << "========= Verify data =========\n";
        {
            auto expectedState = "key = sad\\0, val1 = qwe\\0, val2 = NULL, val3 = NULL\n"
                                                    "key = xyz\\0, val1 = " + halfBigString1 + ", val2 = NULL, val3 = NULL\n";
            auto tableState = ReadTable(server, shards, tableId);
            UNIT_ASSERT_STRINGS_EQUAL(tableState, expectedState);
        }

        Cout << "========= Upsert data with threshold constraint for key in NOT index column =========\n";
        {
            TVector<ui32> columnIds = {1, 2, 3, 4};
            TVector<TCell> cells = {
                TCell("sad"),
                TCell("qwe"),
                TCell("zxc"),
                TCell(bigString.c_str(), bigString.size())
            };

            auto result = Upsert(runtime, sender, shard, tableId, txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, columnIds, cells, NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        }

        Cout << "========= Verify data =========\n";
        {
            auto expectedState = "key = sad\\0, val1 = qwe\\0, val2 = zxc\\0, val3 = " + bigString + "\n"
                                                    "key = xyz\\0, val1 = " + halfBigString1 + ", val2 = NULL, val3 = NULL\n";
            auto tableState = ReadTable(server, shards, tableId);
            UNIT_ASSERT_STRINGS_EQUAL(tableState, expectedState);
        }

        return;
    }

    Y_UNIT_TEST(IncrementImmediate) {

        auto [runtime, server, sender] = TestCreateServer();

        // Define a table with different column types
        auto opts = TShardedTableOptions()
            .Columns({
                {"key", "Uint64", true, false},           // key (id=1)
                {"uint8_val", "Uint8", false, false},     // id=2
                {"uint16_val", "Uint16", false, false},   // id=3
                {"uint32_val", "Uint32", false, false},   // id=4
                {"uint64_val", "Uint64", false, false},   // id=5
                {"int8_val", "Int8", false, false},       // id=6
                {"int16_val", "Int16", false, false},     // id=7
                {"int32_val", "Int32", false, false},     // id=8
                {"int64_val", "Int64", false, false},     // id=9
                {"utf8_val", "Utf8", false, false},       // id=10 (not numeric)
                {"double_val", "Double", false, false}   // id=11 (not supported by increment)
            });

        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const ui64 shard = shards[0];
        ui64 txId = 100;

        Cout << "========= Insert initial data =========\n";
        {
            TVector<ui32> columnIds = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}; // all columns
            TVector<TCell> cells = {
                TCell::Make(ui64(1)),     // key = 1
                TCell::Make(ui8(10)),     // uint8_val
                TCell::Make(ui16(100)),   // uint16_val
                TCell::Make(ui32(1000)),  // uint32_val
                TCell::Make(ui64(10000)), // uint64_val
                TCell::Make(i8(-10)),     // int8_val
                TCell::Make(i16(-50)),    // int16_val
                TCell::Make(i32(-500)),   // int32_val
                TCell::Make(i64(-5000)),  // int64_val
                TCell::Make("text"),      // utf8_val
                TCell::Make(3.14),        // double_val

                TCell::Make(ui64(3)),     // key = 3
                TCell::Make(ui8(15)),     // uint8_val
                TCell::Make(ui16(150)),   // uint16_val
                TCell::Make(ui32(1500)),  // uint32_val
                TCell::Make(ui64(15000)), // uint64_val
                TCell::Make(i8(-15)),     // int8_val
                TCell::Make(i16(-55)),    // int16_val
                TCell::Make(i32(-550)),   // int32_val
                TCell::Make(i64(-5500)),  // int64_val
                TCell::Make("othertext"), // utf8_val
                TCell::Make(3.15)         // double_val
            };

            auto result = Upsert(runtime, sender, shard, tableId, txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, columnIds, cells);

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        }

        Cout << "========= Verify initial data =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);
            UNIT_ASSERT_STRINGS_EQUAL(tableState,
                "key = 1, uint8_val = 10, uint16_val = 100, uint32_val = 1000, "
                "uint64_val = 10000, int8_val = -10, int16_val = -50, int32_val = -500, int64_val = -5000, "
                "utf8_val = text\\0, double_val = 3.14\n"

                "key = 3, uint8_val = 15, uint16_val = 150, uint32_val = 1500, "
                "uint64_val = 15000, int8_val = -15, int16_val = -55, int32_val = -550, int64_val = -5500, "
                "utf8_val = othertext\\0, double_val = 3.15\n"
            );
        }

        Cout << "========= Increment numeric columns =========\n";
        {
            TVector<ui32> columnIds = {1, 2, 3, 4, 5, 6, 7, 8, 9}; // key and numerical columns
            TVector<TCell> increments = {
                TCell::Make(ui64(1)),    // key
                TCell::Make(ui8(5)),     // +5 to uint8_val
                TCell::Make(ui16(50)),   // +50 to uint16_val
                TCell::Make(ui32(500)),  // +500 to uint32_val
                TCell::Make(ui64(5000)), // +5000 to uint64_val
                TCell::Make(i8(5)),      // +5 to int8_val
                TCell::Make(i16(10)),    // +50 to int16_val
                TCell::Make(i32(100)),   // +100 to int32_val
                TCell::Make(i64(1000))   // +1000 to int64_val
            };

            auto result = Increment(runtime, sender, shard, tableId, txId,
                                  NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, columnIds, increments);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        }

        Cout << "========= Verify increment results =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);

            UNIT_ASSERT_STRINGS_EQUAL(tableState,
                "key = 1, uint8_val = 15, uint16_val = 150, uint32_val = 1500, "
                "uint64_val = 15000, int8_val = -5, int16_val = -40, int32_val = -400, int64_val = -4000, "
                "utf8_val = text\\0, double_val = 3.14\n"

                "key = 3, uint8_val = 15, uint16_val = 150, uint32_val = 1500, "
                "uint64_val = 15000, int8_val = -15, int16_val = -55, int32_val = -550, int64_val = -5500, "
                "utf8_val = othertext\\0, double_val = 3.15\n"
            );
        }

        Cout << "========= Try increment several rows (one don't exist) =========\n";
        {
            TVector<ui32> columnIds = {1,2}; //key column and uint8_val column
            TVector<TCell> increments = {TCell::Make(ui64(7)), TCell::Make(ui8(3)), // id 7 don't exist
                                         TCell::Make(ui64(1)), TCell::Make(ui8(3))}; // id 1 exist

            auto result = Increment(runtime, sender, shard, tableId, txId,
                                  NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, columnIds, increments);
            UNIT_ASSERT(result.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        }

        Cout << "========= Verify data changed after increments =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);

            UNIT_ASSERT_STRINGS_EQUAL(tableState,
                "key = 1, uint8_val = 18, uint16_val = 150, uint32_val = 1500, "
                "uint64_val = 15000, int8_val = -5, int16_val = -40, int32_val = -400, int64_val = -4000, "
                "utf8_val = text\\0, double_val = 3.14\n"

                "key = 3, uint8_val = 15, uint16_val = 150, uint32_val = 1500, "
                "uint64_val = 15000, int8_val = -15, int16_val = -55, int32_val = -550, int64_val = -5500, "
                "utf8_val = othertext\\0, double_val = 3.15\n"
            );
        }

        Cout << "========= Try increment utf-8 column (should fail) =========\n";
        {
            TVector<ui32> columnIds = {1, 10}; // id, utf8_val
            TVector<TCell> increments = {
                TCell::Make(ui64(1)),
                TCell::Make("new_text"),
            };

            auto result = Increment(runtime, sender, shard, tableId, txId++,
                                  NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, columnIds, increments, NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
            UNIT_ASSERT(result.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
        }

        Cout << "========= Try increment double column (should fail) =========\n";
        {
            TVector<ui32> columnIds = {1, 11}; // id, double_val
            TVector<TCell> increments = {
                TCell::Make(ui64(1)),
                TCell::Make(double(1.0))
            };

            auto result = Increment(runtime, sender, shard, tableId, txId++,
                                  NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, columnIds, increments, NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
            UNIT_ASSERT(result.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
        }

        Cout << "========= Verify data remains unchanged after failed increments =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);

            UNIT_ASSERT_STRINGS_EQUAL(tableState,
                "key = 1, uint8_val = 18, uint16_val = 150, uint32_val = 1500, "
                "uint64_val = 15000, int8_val = -5, int16_val = -40, int32_val = -400, int64_val = -4000, "
                "utf8_val = text\\0, double_val = 3.14\n"

                "key = 3, uint8_val = 15, uint16_val = 150, uint32_val = 1500, "
                "uint64_val = 15000, int8_val = -15, int16_val = -55, int32_val = -550, int64_val = -5500, "
                "utf8_val = othertext\\0, double_val = 3.15\n"
            );
        }

        Cout << "========= Try increment key columns =========\n";
        {
            TVector<ui32> columnIds = {1, 1}; // id, id
            TVector<TCell> increments = {
                TCell::Make(ui64(1)),
                TCell::Make(ui64(3))
            };

            auto result = Increment(runtime, sender, shard, tableId, txId++,
                                NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, columnIds, increments, NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
            UNIT_ASSERT(result.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
        }

        Cout << "========= Verify data remains unchanged =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);

            UNIT_ASSERT_STRINGS_EQUAL(tableState,
                "key = 1, uint8_val = 18, uint16_val = 150, uint32_val = 1500, "
                "uint64_val = 15000, int8_val = -5, int16_val = -40, int32_val = -400, int64_val = -4000, "
                "utf8_val = text\\0, double_val = 3.14\n"

                "key = 3, uint8_val = 15, uint16_val = 150, uint32_val = 1500, "
                "uint64_val = 15000, int8_val = -15, int16_val = -55, int32_val = -550, int64_val = -5500, "
                "utf8_val = othertext\\0, double_val = 3.15\n"
            );
        }

        Cout << "========= Try increment with overflow =========\n";
        {
            TVector<ui32> columnIds = {1, 2, 3, 4, 5, 6, 7, 8, 9}; // id, id
            TVector<TCell> increments = {
                TCell::Make(ui64(1)),                 // key
                TCell::Make(ui8(~0)),                 // +2^8 - 1
                TCell::Make(ui16(~0)),                // +2^16 - 1
                TCell::Make(ui32(~0)),                // +2^32 - 1
                TCell::Make(ui64(~0ll)),              // +2^64 - 1
                TCell::Make(i8(-((1 << 7) - 1))),     // - (2^7-1)
                TCell::Make(i16(-((1 << 15) - 1))),   // - (2^15-1)
                TCell::Make(i32(-((1 << 31) - 1))),   // - (2^31-1)
                TCell::Make(i64(-((1ll << 63) - 1)))  // - (2^63-1)
            };

            auto result = Increment(runtime, sender, shard, tableId, txId++,
                                NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, columnIds, increments, NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
            UNIT_ASSERT(result.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        }

        Cout << "========= Verify data =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);

            UNIT_ASSERT_STRINGS_EQUAL(tableState,
                "key = 1, uint8_val = 17, uint16_val = 149, uint32_val = 1499, "
                "uint64_val = 14999, int8_val = 124, int16_val = 32729, int32_val = 2147483249, int64_val = 9223372036854771809, "
                "utf8_val = text\\0, double_val = 3.14\n"

                "key = 3, uint8_val = 15, uint16_val = 150, uint32_val = 1500, "
                "uint64_val = 15000, int8_val = -15, int16_val = -55, int32_val = -550, int64_val = -5500, "
                "utf8_val = othertext\\0, double_val = 3.15\n"
            );
        }

        Cout << "========= Try increment no delta columns =========\n";
        {
            TVector<ui32> columnIds = {1}; // id
            TVector<TCell> increments = {TCell::Make(ui64(1))};

            auto result = Increment(runtime, sender, shard, tableId, txId++,
                                NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, columnIds, increments, NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
            UNIT_ASSERT(result.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        }

        Cout << "========= Verify data =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);

            UNIT_ASSERT_STRINGS_EQUAL(tableState,
                "key = 1, uint8_val = 17, uint16_val = 149, uint32_val = 1499, "
                "uint64_val = 14999, int8_val = 124, int16_val = 32729, int32_val = 2147483249, int64_val = 9223372036854771809, "
                "utf8_val = text\\0, double_val = 3.14\n"

                "key = 3, uint8_val = 15, uint16_val = 150, uint32_val = 1500, "
                "uint64_val = 15000, int8_val = -15, int16_val = -55, int32_val = -550, int64_val = -5500, "
                "utf8_val = othertext\\0, double_val = 3.15\n"
            );
        }
    }

    Y_UNIT_TEST(UpsertIncrement) {

        auto [runtime, server, sender] = TestCreateServer();

        // Define a table with different column types
        auto opts = TShardedTableOptions()
            .Columns({
                {"key", "Uint64", true, false},           // key (id=1)
                {"uint32_val", "Uint32", false, false},   // id=2
                {"uint64_val", "Uint64", false, false}    // id=3
            });

        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const ui64 shard = shards[0];
        ui64 txId = 100;

        Cout << "========= Upsert a row using upsert-increment =========\n";
        {
            TVector<ui32> columnIds = {1, 2, 3}; // key and numeric columns
            TVector<TCell> increments = {
                TCell::Make(ui64(1)),    // key
                TCell::Make(ui32(500)),  // +500 to uint32_val
                TCell::Make(ui64(5000)), // +5000 to uint64_val
            };

            auto result = UpsertIncrement(runtime, sender, shard, tableId, txId,
                NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, columnIds, increments);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        }

        Cout << "========= Verify the inserted row =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);

            UNIT_ASSERT_STRINGS_EQUAL(tableState,
                "key = 1, uint32_val = 500, uint64_val = 5000\n"
            );
        }

        Cout << "========= Try to insert a row and increment another row =========\n";
        {
            TVector<ui32> columnIds = {1, 2, 3}; // key column and uint8_val column
            TVector<TCell> increments = {
                TCell::Make(ui64(7)), TCell::Make(ui32(20)), TCell::Make(ui64(200)),  // id 7 don't exist
                TCell::Make(ui64(1)), TCell::Make(ui32(-30)), TCell::Make(ui64(-300)) // id 1 exists
            };

            auto result = UpsertIncrement(runtime, sender, shard, tableId, txId,
                NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, columnIds, increments);
            UNIT_ASSERT(result.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        }

        Cout << "========= Verify data after increments =========\n";
        {
            auto tableState = ReadTable(server, shards, tableId);

            UNIT_ASSERT_STRINGS_EQUAL(tableState,
                "key = 1, uint32_val = 470, uint64_val = 4700\n"
                "key = 7, uint32_val = 20, uint64_val = 200\n"
            );
        }
    }

    Y_UNIT_TEST(UpsertIncrementPrecharge) {
        auto [runtime, server, sender] = TestCreateServer();

        auto opts = TShardedTableOptions()
            .Columns({
                {"key", "Uint64", true, false},           // key (id=1)
                {"uint32_val", "Uint32", false, false},   // id=2
                {"uint64_val", "Uint64", false, false}    // id=3
            });

        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const ui64 shard = shards[0];
        ui64 txId = 100;

        Cout << "========= Upsert 1000 rows =========\n";
        {
            TVector<ui32> columnIds = {1, 2, 3}; // key and numeric columns
            TVector<TCell> increments;
            for (ui64 id = 1; id <= 1000; id++) {
                increments.push_back(TCell::Make(id));
                increments.push_back(TCell::Make(ui32(1)));
                increments.push_back(TCell::Make(ui64(1)));
            }
            auto result = UpsertIncrement(runtime, sender, shard, tableId, txId,
                NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, columnIds, increments);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        }

        Cout << "========= Compact and reboot to clear cache =========\n";
        CompactTable(runtime, shard, tableId, false);
        RebootTablet(runtime, shard, sender);
        runtime.SimulateSleep(TDuration::Seconds(1));

        auto getTabletNotReadyCount = [&]() -> ui64 {
            auto edge = runtime.AllocateEdgeActor();
            runtime.SendToPipe(shard, edge, new TEvTablet::TEvGetCounters(),
                0, GetPipeConfigWithRetries());
            auto ev = runtime.GrabEdgeEventRethrow<TEvTablet::TEvGetCountersResponse>(edge);
            UNIT_ASSERT(ev);
            for (const auto& counter : ev->Get()->Record.GetTabletCounters().GetAppCounters().GetCumulativeCounters()) {
                if (counter.GetName() == "DataShard/TxTabletNotReady") {
                    return counter.GetValue();
                }
            }
            return 0;
        };

        ui64 notReadyBefore = getTabletNotReadyCount();

        Cout << "========= UPSERT_INCREMENT on rows 1, 500, 1000 =========\n";
        {
            TVector<ui32> columnIds = {1, 2, 3};
            TVector<TCell> increments = {
                TCell::Make(ui64(1)), TCell::Make(ui32(10)), TCell::Make(ui64(100)),
                TCell::Make(ui64(500)), TCell::Make(ui32(10)), TCell::Make(ui64(100)),
                TCell::Make(ui64(1000)), TCell::Make(ui32(10)), TCell::Make(ui64(100)),
            };

            auto result = UpsertIncrement(runtime, sender, shard, tableId, txId,
                NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, columnIds, increments);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        }

        ui64 notReadyAfter = getTabletNotReadyCount();

        // Should have at most 1 TabletNotReady
        Cout << "TabletNotReady count after UPSERT_INCREMENT: " << (notReadyAfter - notReadyBefore) << "\n";
        UNIT_ASSERT_C(notReadyAfter - notReadyBefore <= 1, "Too many page faults: " << (notReadyAfter - notReadyBefore));
    }

    Y_UNIT_TEST_TWIN(ExecSQLUpsertPrepared, Volatile) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings
            .SetDomainName("Root")
            .SetUseRealThreads(false);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        runtime.GetAppData().FeatureFlags.SetEnableDataShardVolatileTransactions(Volatile);

        TShardedTableOptions opts;
        auto [shards1, tableId1] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        auto [shards2, tableId2] = CreateShardedTable(server, sender, "/Root", "table-2", opts);

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

        auto opts = TShardedTableOptions().Columns({
            {"key", "Utf8", true, true},
            {"value1", "Uint32", false, false},
            {"value2", "Uint32", false, true},
        });
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const ui64 shard = shards[0];

        auto prepareEvWrite = [&](
            NKikimrDataEvents::TEvWrite::TOperation::EOperationType opType,
            TSerializedCellMatrix matrix,
            const std::vector<ui32>& columnIds) {
            auto evWrite = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(100, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
            ui64 payloadIndex = NKikimr::NEvWrite::TPayloadWriter<NKikimr::NEvents::TDataEvents::TEvWrite>(*evWrite).AddDataToPayload(matrix.ReleaseBuffer());
            evWrite->AddOperation(opType, tableId, columnIds, payloadIndex, NKikimrDataEvents::FORMAT_CELLVEC);
            return evWrite;
        };

        Cout << "========= Send immediate write with huge key=========\n";
        {
            TString hugeStringValue(NLimits::MaxWriteKeySize + 1, 'X');
            TSerializedCellMatrix matrix({TCell(hugeStringValue.c_str(), hugeStringValue.size()), TCell::Make(ui32(123))}, 1, 2);

            const auto writeResult = Write(
                runtime, sender, shard,
                prepareEvWrite(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT, std::move(matrix), {1, 3}),
                NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
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

        Cout << "========= Send immediate write with NULL value for NOT NULL column=========\n";
        {
            TString stringValue("KEY");
            TSerializedCellMatrix matrix(
                {TCell(stringValue.data(), stringValue.size()), TCell()},
                1, 2);

            const auto writeResult = Write(
                runtime, sender, shard,
                prepareEvWrite(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT, matrix, {1, 3}),
                NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
            UNIT_ASSERT_VALUES_EQUAL(writeResult.GetIssues().size(), 1);
            UNIT_ASSERT(writeResult.GetIssues(0).message().Contains("NULL value for NON NULL column"));
        }

        Cout << "========= Send immediate write without data for NOT NULL column=========\n";
        {
            TString stringValue("KEY");
            TSerializedCellMatrix matrix(
                {TCell(stringValue.data(), stringValue.size()), TCell::Make(ui32(123))},
                1, 2);

            // Insert should fail.
            const auto insertResult = Write(
                runtime, sender, shard,
                prepareEvWrite(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT, matrix, {1, 2}),
                NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
            UNIT_ASSERT_VALUES_EQUAL(insertResult.GetIssues().size(), 1);
            UNIT_ASSERT(insertResult.GetIssues(0).message().Contains("Missing inserted values for NON NULL column"));

            // Update should complete successfully.
            const auto updateResult = Write(
                runtime, sender, shard,
                prepareEvWrite(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE, matrix, {1, 2}),
                NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
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
            const auto writeResult = Write(runtime, sender, shard, std::move(request), NKikimrDataEvents::TEvWriteResult::STATUS_CONSTRAINT_VIOLATION);
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

    Y_UNIT_TEST(CancelMultipleImmediateTransactionsFromQueue) {
        auto [runtime, server, sender] = TestCreateServer();

        TShardedTableOptions opts;
        const TString tableName = "table-1";
        const auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", tableName, opts);
        const ui64 shard = shards[0];

        TActorId shardActorId = ResolveTablet(runtime, shard, 0, false);

        const ui64 txId1 = 101, txId2 = 102, txId3 = 103, txId4 = 104, txId5 = 105;

        auto sendWrite = [&](ui64 txId, ui32 key, ui32 value) {
            auto request = MakeWriteRequestOneKeyValue(
                txId,
                NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE,
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                tableId, opts.Columns_, key, value);
            runtime.Send(new IEventHandle(shardActorId, sender, request.release()), 0, true);
        };

        // All 6 writes and both cancels are sent with viaActorSystem=true — they are
        // queued in the shard mailbox but NOT dispatched yet.  Because the actor mailbox
        // is FIFO, the shard will:
        //   1. Enqueue tx1 → ProposeQueue sends TEvDelayedProposeTransaction to self (to
        //      the END of the same mailbox, after all messages already in it)
        //   2. Enqueue tx2 (first), tx2 (retry), tx3, tx4, tx5 (no new drive event —
        //      idempotent scalar queue).  The two tx2 items are linked via NextForTxId.
        //   3. Cancel tx2 → ProposeQueue.Cancel walks the NextForTxId chain, removes
        //      BOTH tx2 items, sends 2 CANCELLED replies
        //   4. Cancel tx3 → ProposeQueue.Cancel removes tx3, sends 1 CANCELLED reply
        //   5. TEvDelayedProposeTransaction fires → drains remaining queue: tx1, tx4, tx5
        // So we expect 6 total replies: 2×CANCELLED(tx2) + 1×CANCELLED(tx3) +
        // 3×COMPLETED(tx1, tx4, tx5).

        Cout << "========= Enqueue 6 immediate writes into ProposeQueue =========\n";
        sendWrite(txId1, 1, 10);
        sendWrite(txId2, 2, 20);         // first attempt
        sendWrite(txId2, 2, 20);         // retry of the same txId — goes into NextForTxId chain
        sendWrite(txId3, 3, 30);
        sendWrite(txId4, 4, 40);
        sendWrite(txId5, 5, 50);

        Cout << "========= Send cancels for tx2 and tx3 (still not dispatched) =========\n";
        {
            auto cancel2 = std::make_unique<TEvDataShard::TEvCancelTransactionProposal>(txId2);
            runtime.Send(new IEventHandle(shardActorId, sender, cancel2.release()), 0, true);
            auto cancel3 = std::make_unique<TEvDataShard::TEvCancelTransactionProposal>(txId3);
            runtime.Send(new IEventHandle(shardActorId, sender, cancel3.release()), 0, true);
        }

        Cout << "========= Collect 6 write results =========\n";
        // Use GrabEdgeEventRethrow directly: WaitForWriteCompleted asserts a fixed status
        // and would fail if it received STATUS_CANCELLED for tx2/tx3.
        // Cancel(txId2) sends 2 replies (one per NextForTxId chain element), so we
        // expect 6 results total, not 5.
        THashMap<ui64, NKikimrDataEvents::TEvWriteResult::EStatus> results;
        ui32 cancelledTx2Count = 0;
        for (int i = 0; i < 6; ++i) {
            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvWriteResult>(sender);
            const auto& rec = ev->Get()->Record;
            if (rec.GetTxId() == txId2 &&
                rec.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_CANCELLED) {
                ++cancelledTx2Count;
            }
            results[rec.GetTxId()] = rec.GetStatus();
        }

        // Both retries of tx2 must be cancelled
        UNIT_ASSERT_VALUES_EQUAL(cancelledTx2Count, 2u);
        UNIT_ASSERT_VALUES_EQUAL(results[txId1], NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        UNIT_ASSERT_VALUES_EQUAL(results[txId2], NKikimrDataEvents::TEvWriteResult::STATUS_CANCELLED);
        UNIT_ASSERT_VALUES_EQUAL(results[txId3], NKikimrDataEvents::TEvWriteResult::STATUS_CANCELLED);
        UNIT_ASSERT_VALUES_EQUAL(results[txId4], NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        UNIT_ASSERT_VALUES_EQUAL(results[txId5], NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);

        Cout << "========= Verify table state: only rows 1, 4, 5 =========\n";
        {
            auto tableData = ReadShardedTable(server, "/Root/table-1");
            UNIT_ASSERT_VALUES_EQUAL(tableData,
                "key = 1, value = 10\n"
                "key = 4, value = 40\n"
                "key = 5, value = 50\n");
        }

        Cout << "========= Verify shard still works correctly =========\n";
        {
            Upsert(runtime, sender, shard, tableId, opts.Columns_, 1, 106,
                NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        }
    }

    // Companion test to CancelMultipleFromQueue that exercises the
    // TEvProposeTransaction::EventType branch in SendCancelledProposeReply.
    // Enqueues a TX_KIND_DATA|Immediate TEvProposeTransaction (old MiniKQL path)
    // into ProposeQueue and cancels it before the queue is drained.
    Y_UNIT_TEST(CancelImmediateProposeTransaction) {
        auto [runtime, server, sender] = TestCreateServer();

        TShardedTableOptions opts;
        const TString tableName = "table-1";
        const auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", tableName, opts);
        const ui64 shard = shards[0];
        Y_UNUSED(shard);
        Y_UNUSED(tableId);

        TActorId shardActorId = ResolveTablet(runtime, shard, 0, false);

        const ui64 txId = 201;

        // Both messages are sent with viaActorSystem=true — they sit in the shard
        // mailbox in FIFO order and are NOT dispatched until the runtime runs.
        // ProposeQueue appends TEvDelayedProposeTransaction to the END of the
        // mailbox when the first propose is enqueued, so the order is:
        //   propose(txId) → cancel(txId) → TEvDelayedProposeTransaction
        // The cancel fires before the drive event, so ProposeQueue.Cancel finds
        // the item, removes it, and calls SendCancelledProposeReply through the
        // TEvProposeTransaction::EventType branch, sending CANCELLED immediately.
        // When TEvDelayedProposeTransaction fires the queue is already empty.
        Cout << "========= Enqueue immediate TEvProposeTransaction into ProposeQueue =========\n";
        {
            auto request = std::make_unique<TEvDataShard::TEvProposeTransaction>(
                NKikimrTxDataShard::TX_KIND_DATA,
                sender,
                txId,
                /* txBody = */ TString{},
                NDataShard::TTxFlags::Immediate);
            runtime.Send(new IEventHandle(shardActorId, sender, request.release()), 0, true);
        }

        Cout << "========= Cancel while still in ProposeQueue =========\n";
        {
            auto cancel = std::make_unique<TEvDataShard::TEvCancelTransactionProposal>(txId);
            runtime.Send(new IEventHandle(shardActorId, sender, cancel.release()), 0, true);
        }

        Cout << "========= Expect CANCELLED TEvProposeTransactionResult =========\n";
        {
            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvProposeTransactionResult>(sender);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetTxId(), txId);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetStatus(),
                NKikimrTxDataShard::TEvProposeTransactionResult::CANCELLED);
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

    Y_UNIT_TEST(ImmediateAndPlannedCommittedOpsRace) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            // It's easier to reproduce without volatile transactions, since
            // then we can block pipeline by blocking readsets
            .SetEnableDataShardVolatileTransactions(false)
            // We need to block transaction post validation, which is
            // impossible with volatile execution.
            .SetEnableDataShardWriteAlwaysVolatile(false);

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
        size_t commitsBeforeTx2 = blockedCommits.size();
        UNIT_ASSERT_VALUES_EQUAL_C(commitsBeforeTx2, 0u,
            "The uncommitted upsert didn't block. Something may have changed and the test needs to be revised.");

        // Now, while blocking commits, unblock progress and let tx2 to execute,
        // which will abort due to broken locks.
        blockedProgress.Unblock();
        blockedProgress.Stop();

        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        size_t commitsAfterTx2 = blockedCommits.size() - commitsBeforeTx2;
        Cerr << "... observed " << commitsAfterTx2 << " commits after tx2 unblock" << Endl;
        UNIT_ASSERT_C(commitsAfterTx2 >= 2,
            "Expected tx2 to produce at least 2 commits (store out rs + abort tx)"
            << ", observed " << commitsAfterTx2 << ". Something may have changed.");

        // Now, while still blocking commits, unblock readsets
        // Everything will unblock and execute tx1 then tx3
        blockedReadSets.Unblock();
        blockedReadSets.Stop();

        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        size_t commitsAfterTx3 = blockedCommits.size() - commitsAfterTx2 - commitsBeforeTx2;
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

    Y_UNIT_TEST(DelayedVolatileTxAndEvWrite) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableDataShardVolatileTransactions(true);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, a int, b int, c int, PRIMARY KEY (key))
                WITH (PARTITION_AT_KEYS = (10));
            )"),
            "SUCCESS");

        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);
        const auto tableId = ResolveTableId(server, sender, "/Root/table");

        auto [tablesMap, ownerId_] = GetTablesByPathId(server, shards.at(0));

        // Start blocking readsets
        TBlockEvents<TEvTxProcessing::TEvReadSet> blockedReadSets(runtime);

        // Prepare a distributed upsert
        Cerr << "... starting a distributed upsert" << Endl;
        auto upsertFuture = KqpSimpleSend(runtime, R"(
            UPSERT INTO `/Root/table` (key, a, b, c) VALUES (1, 2, 2, 2), (11, 12, 12, 12);
        )");
        runtime.WaitFor("blocked readsets", [&]{ return blockedReadSets.size() >= 4; });

        // 1. Make an upsert to (key, b)
        {
            Cerr << "... making a write to " << shards.at(0) << Endl;
            auto req = MakeWriteRequest(
                std::nullopt,
                NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE,
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                tableId,
                { 1_ui32, 3_ui32 },
                { TCell::Make(1_i32), TCell::Make(3_i32) });
            Write(runtime, sender, shards.at(0), std::move(req));
        }

        // 1. Make an upsert to (key, c)
        {
            Cerr << "... making a write to " << shards.at(0) << Endl;
            auto req = MakeWriteRequest(
                std::nullopt,
                NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE,
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                tableId,
                { 1_ui32, 4_ui32 },
                { TCell::Make(1_i32), TCell::Make(4_i32) });
            Write(runtime, sender, shards.at(0), std::move(req));
        }

        // Unblock readsets
        blockedReadSets.Stop().Unblock();

        runtime.SimulateSleep(TDuration::MilliSeconds(1));

        // Make a validating read, the volatile tx changes must not be lost
        Cerr << "... validating table" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, a, b, c FROM `/Root/table` ORDER BY key;
            )"),
            "{ items { int32_value: 1 } items { int32_value: 2 } items { int32_value: 3 } items { int32_value: 4 } }, "
            "{ items { int32_value: 11 } items { int32_value: 12 } items { int32_value: 12 } items { int32_value: 12 } }");
    }

    Y_UNIT_TEST(DoubleWriteUncommittedThenDoubleReadWithCommit) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetNodeCount(2)
            .SetUseRealThreads(false);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key Int64, index Int64, value Int64, ballast String, PRIMARY KEY (key, index));
            )"),
            "SUCCESS");

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, index, value) VALUES
                (251, 0, 1000),
                (251, 1, 1001),
                (251, 2, 1002),
                (251, 3, 1003),
                (251, 4, 1004),
                (251, 5, 1005),
                (252, 0, 2000),
                (252, 1, 2001),
                (252, 2, 2002),
                (252, 3, 2003),
                (252, 4, 2004),
                (252, 5, 2005);
        )");

        TString sqlWrite = R"(
            DECLARE $write0 AS List<Struct<key:Int64, index:Int64, value:Int64>>;
            DECLARE $ballast AS Bytes;
            $write0_keys = (SELECT DISTINCT(key) FROM AS_TABLE($write0));
            $write0_last_index = (
                SELECT w.key AS key, MAX(t.index) AS index
                FROM $write0_keys AS w
                INNER JOIN `/Root/table` AS t ON t.key = w.key
                GROUP BY w.key
            );
            UPSERT INTO `/Root/table` (
                SELECT w.key AS key,
                    COALESCE(li.index + 1, 0) + w.index AS index,
                    w.value AS value,
                    $ballast AS ballast
                FROM AS_TABLE($write0) AS w
                LEFT JOIN $write0_last_index AS li ON li.key = w.key
            );
        )";

        TString sqlRead = R"(
            DECLARE $read0 AS Int64;
            DECLARE $read1 AS Int64;
            SELECT index, value FROM `/Root/table` WHERE key = $read0 ORDER BY index;
            SELECT index, value FROM `/Root/table` WHERE key = $read1 ORDER BY index;
        )";

        TString sessionId = CreateSessionRPC(runtime, "/Root");

        // auto chunkReads = runtime.AddObserver<TEvDataShard::TEvRead>(
        //     [&](TEvDataShard::TEvRead::TPtr& ev) {
        //         ev->Get()->Record.SetMaxRowsInResult(1);
        //     });

        Cerr << "... sending write request" << Endl;
        Ydb::Table::ExecuteDataQueryRequest writeRequest;
        writeRequest.set_session_id(sessionId);
        writeRequest.mutable_tx_control()->set_commit_tx(false);
        writeRequest.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
        writeRequest.mutable_query()->set_yql_text(sqlWrite);
        auto& writeParams = *writeRequest.mutable_parameters();
        if (auto* p = &writeParams["$write0"]) {
            auto* t = p->mutable_type()->mutable_list_type()->mutable_item()->mutable_struct_type();
            if (auto* m = t->add_members()) {
                m->set_name("key");
                m->mutable_type()->set_type_id(Ydb::Type::INT64);
            }
            if (auto* m = t->add_members()) {
                m->set_name("index");
                m->mutable_type()->set_type_id(Ydb::Type::INT64);
            }
            if (auto* m = t->add_members()) {
                m->set_name("value");
                m->mutable_type()->set_type_id(Ydb::Type::INT64);
            }
            if (auto* row = p->mutable_value()->add_items()) {
                row->add_items()->set_int64_value(251);
                row->add_items()->set_int64_value(0);
                row->add_items()->set_int64_value(5001);
            }
            if (auto* row = p->mutable_value()->add_items()) {
                row->add_items()->set_int64_value(252);
                row->add_items()->set_int64_value(0);
                row->add_items()->set_int64_value(5002);
            }
        }
        if (auto* p = &writeParams["$ballast"]) {
            p->mutable_type()->set_type_id(Ydb::Type::STRING);
            p->mutable_value()->set_bytes_value("xxx");
        }
        auto writeResponse = AwaitResponse(runtime, SendRequest(runtime, std::move(writeRequest), "/Root"));
        UNIT_ASSERT_C(writeResponse.operation().status() == Ydb::StatusIds::SUCCESS, "ERROR: " << writeResponse.operation().status());
        Ydb::Table::ExecuteQueryResult writeResult;
        writeResponse.operation().result().UnpackTo(&writeResult);
        TString txId = writeResult.tx_meta().id();

        Cerr << "... sending read request" << Endl;
        Ydb::Table::ExecuteDataQueryRequest readRequest;
        readRequest.set_session_id(sessionId);
        readRequest.mutable_tx_control()->set_commit_tx(true);
        readRequest.mutable_tx_control()->set_tx_id(txId);
        readRequest.mutable_query()->set_yql_text(sqlRead);
        auto& readParams = *readRequest.mutable_parameters();
        if (auto* p = &readParams["$read0"]) {
            p->mutable_type()->set_type_id(Ydb::Type::INT64);
            p->mutable_value()->set_int64_value(251);
        }
        if (auto* p = &readParams["$read1"]) {
            p->mutable_type()->set_type_id(Ydb::Type::INT64);
            p->mutable_value()->set_int64_value(252);
        }
        auto readResponse = AwaitResponse(runtime, SendRequest(runtime, std::move(readRequest), "/Root"));
        UNIT_ASSERT_C(readResponse.operation().status() == Ydb::StatusIds::SUCCESS, "ERROR: " << readResponse.operation().status());
        Ydb::Table::ExecuteQueryResult readResult;
        readResponse.operation().result().UnpackTo(&readResult);
        UNIT_ASSERT_VALUES_EQUAL(FormatResult(readResult),
            "{ items { int64_value: 0 } items { int64_value: 1000 } }, "
            "{ items { int64_value: 1 } items { int64_value: 1001 } }, "
            "{ items { int64_value: 2 } items { int64_value: 1002 } }, "
            "{ items { int64_value: 3 } items { int64_value: 1003 } }, "
            "{ items { int64_value: 4 } items { int64_value: 1004 } }, "
            "{ items { int64_value: 5 } items { int64_value: 1005 } }, "
            "{ items { int64_value: 6 } items { int64_value: 5001 } }\n"
            "{ items { int64_value: 0 } items { int64_value: 2000 } }, "
            "{ items { int64_value: 1 } items { int64_value: 2001 } }, "
            "{ items { int64_value: 2 } items { int64_value: 2002 } }, "
            "{ items { int64_value: 3 } items { int64_value: 2003 } }, "
            "{ items { int64_value: 4 } items { int64_value: 2004 } }, "
            "{ items { int64_value: 5 } items { int64_value: 2005 } }, "
            "{ items { int64_value: 6 } items { int64_value: 5002 } }");
        Cerr << readResult.DebugString();
    }

    Y_UNIT_TEST(WriteCommitVersion) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key))
                WITH (PARTITION_AT_KEYS = (10, 20));;
            )"),
            "SUCCESS"
        );

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES
                (1, 1001),
                (11, 1002),
                (21, 1003);
        )");

        std::deque<std::optional<TRowVersion>> commitVersions;
        auto commitVersionObserver = runtime.AddObserver<NKikimr::NEvents::TDataEvents::TEvWriteResult>([&](auto& ev) {
            auto* msg = ev->Get();
            if (msg->Record.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED) {
                if (msg->Record.HasCommitVersion()) {
                    commitVersions.emplace_back(TRowVersion::FromProto(msg->Record.GetCommitVersion()));
                } else {
                    commitVersions.emplace_back();
                }
            }
        });

        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, R"(
                UPSERT INTO `/Root/table` (key, value) VALUES (2, 2001);
                SELECT key, value FROM `/Root/table` WHERE key < 10 ORDER BY key;
            )"),
            "{ items { int32_value: 1 } items { int32_value: 1001 } }, "
            "{ items { int32_value: 2 } items { int32_value: 2001 } }"
        );
        UNIT_ASSERT_VALUES_EQUAL(commitVersions.size(), 1u);
        UNIT_ASSERT_C(!commitVersions.front(), "Unexpected commit version: " << commitVersions.front().value());
        commitVersions.clear();

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleCommit(runtime, sessionId, txId, R"(SELECT 1)"),
            "{ items { int32_value: 1 } }"
        );
        UNIT_ASSERT_VALUES_EQUAL(commitVersions.size(), 1u);
        UNIT_ASSERT_C(commitVersions.front(), "Missing commit version");

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table` WHERE key < 20 ORDER BY key;
            )"),
            "{ items { int32_value: 1 } items { int32_value: 1001 } }, "
            "{ items { int32_value: 2 } items { int32_value: 2001 } }, "
            "{ items { int32_value: 11 } items { int32_value: 1002 } }"
        );

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                UPSERT INTO `/Root/table` (key, value) VALUES (12, 3001);
            )"),
            "<empty>"
        );
        UNIT_ASSERT_VALUES_EQUAL(commitVersions.size(), 2u);
        UNIT_ASSERT_C(commitVersions[0] < commitVersions[1],
            "Unexpected commit version: " << commitVersions[0].value() << " then " << commitVersions[1].value()
        );

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                UPSERT INTO `/Root/table` (key, value) VALUES
                    (13, 4001),
                    (22, 4002);
            )"),
            "<empty>"
        );
        UNIT_ASSERT_VALUES_EQUAL(commitVersions.size(), 4u);
        UNIT_ASSERT_C(commitVersions[1] < commitVersions[2] && commitVersions[2] == commitVersions[3],
            "Unexpected commit version: " << commitVersions[1].value() << " then "
            << commitVersions[2].value() << " and " << commitVersions[3].value()
        );
    }

    Y_UNIT_TEST(WriteUniqueRowsInsertDuplicateBeforeCommit) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/counts` (key int, rows int, PRIMARY KEY (key));
                CREATE TABLE `/Root/rows` (key int, subkey int, value int, PRIMARY KEY (key, subkey));
            )"),
            "SUCCESS"
        );

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/counts` (key, rows) VALUES
                (42, 2);
            UPSERT INTO `/Root/rows` (key, subkey, value) VALUES
                (42, 1, 101),
                (42, 2, 102);
        )");

        TString sessionId1, txId1;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId1, txId1, R"(
                UPDATE `/Root/counts` SET rows = rows + 1 WHERE key = 42;
                SELECT rows FROM `/Root/counts` WHERE key = 42;
            )"),
            "{ items { int32_value: 3 } }"
        );

        TString sessionId2, txId2;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId2, txId2, R"(
                UPDATE `/Root/counts` SET rows = rows + 1 WHERE key = 42;
                SELECT rows FROM `/Root/counts` WHERE key = 42;
            )"),
            "{ items { int32_value: 3 } }"
        );

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleCommit(runtime, sessionId2, txId2, R"(
                INSERT INTO `/Root/rows` (key, subkey, value) VALUES
                    (42, 3, 203);
            )"),
            "<empty>"
        );

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleContinue(runtime, sessionId1, txId1, R"(
                INSERT INTO `/Root/rows` (key, subkey, value) VALUES
                    (42, 3, 303);
            )"),
            "ERROR: ABORTED"
        );
    }

    Y_UNIT_TEST(WriteUniqueRowsInsertDuplicateAtCommit) {
        // KQP will flush INSERTs before commit, so this test uses the TTransactionState
        // helper instead of SQL statements.
        using namespace NKikimr::NDataShard::NTxHelpers;
        using TOperation = NKikimr::NDataShard::NTxHelpers::TWriteOperation;

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/counts` (key int, rows int, PRIMARY KEY (key));
                CREATE TABLE `/Root/rows` (key int, subkey int, PRIMARY KEY (key, subkey));
            )"),
            "SUCCESS"
        );

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/counts` (key, rows) VALUES
                (42, 2);
            UPSERT INTO `/Root/rows` (key, subkey) VALUES
                (42, 1),
                (42, 2);
        )");

        auto getTableInfo = [&](const TString& table) {
            const auto tableId = ResolveTableId(server, sender, table);
            UNIT_ASSERT(tableId);
            const auto shards = GetTableShards(server, sender, table);
            UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);
            return std::pair(tableId, shards[0]);
        };

        const auto [countsTableId, countsTableShard] = getTableInfo("/Root/counts");
        const auto [rowsTableId, rowsTableShard] = getTableInfo("/Root/rows");

        TTransactionState tx1(runtime, NKikimrDataEvents::OPTIMISTIC);

        UNIT_ASSERT_VALUES_EQUAL(
            tx1.ReadKey(countsTableId, countsTableShard, 42),
            "42, 2\n");

        TTransactionState tx2(runtime, NKikimrDataEvents::OPTIMISTIC);

        UNIT_ASSERT_VALUES_EQUAL(
            tx2.ReadKey(countsTableId, countsTableShard, 42),
            "42, 2\n");

        // Insert the (42, 3) row and commit tx2.
        {
            tx2.InitCommit({countsTableShard, rowsTableShard});
            auto w1 = tx2.PrepareCommit(countsTableId, countsTableShard);
            auto w2 = tx2.PrepareCommit(rowsTableId, rowsTableShard, TOperation::Insert(42, 3));
            tx2.SendPlan();
            UNIT_ASSERT_VALUES_EQUAL(w1.NextString(), "OK");
            UNIT_ASSERT_VALUES_EQUAL(w2.NextString(), "OK");
        }

        // Try inserting the (42, 3) row while committing tx1.
        // The commit should fail with STATUS_LOCKS_BROKEN error and not STATUS_CONSTRAINT_VIOLATION
        // because the conflicting row didn't exist at the time of tx1 snapshot.
        {
            tx1.InitCommit({countsTableShard, rowsTableShard});
            auto w1 = tx1.PrepareCommit(countsTableId, countsTableShard);
            auto w2 = tx1.PrepareCommit(rowsTableId, rowsTableShard, TOperation::Insert(42, 3));
            tx1.SendPlan();
            UNIT_ASSERT_VALUES_EQUAL(w1.NextString(), "OK");
            UNIT_ASSERT_VALUES_EQUAL(w2.NextString(), "ERROR: STATUS_LOCKS_BROKEN");
        }
    }

    Y_UNIT_TEST_TWIN(DistributedInsertReadSetWithoutLocks, Volatile) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key))
                WITH (PARTITION_AT_KEYS = (10));
            )"),
            "SUCCESS"
        );

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES
                (1, 1001),
                (11, 1002);
        )");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);

        auto shard1 = shards.at(0);
        auto shard2 = shards.at(1);

        TVector<TShardedTableOptions::TColumn> columns{
            {"key", "Int32", true, false},
            {"value", "Int32", false, false},
        };

        const ui64 coordinator = ChangeStateStorage(Coordinator, server->GetSettings().Domain);

        // Prepare an insert (readsets flow between shards)
        ui64 txId1 = 1234567890011;
        auto tx1sender = runtime.AllocateEdgeActor();
        {
            auto req1 = MakeWriteRequestOneKeyValue(
                txId1,
                Volatile ? NKikimrDataEvents::TEvWrite::MODE_VOLATILE_PREPARE : NKikimrDataEvents::TEvWrite::MODE_PREPARE,
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT,
                tableId,
                columns,
                2, 1003);
            req1->Record.MutableLocks()->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
            req1->Record.MutableLocks()->AddSendingShards(shard1);
            req1->Record.MutableLocks()->AddSendingShards(shard2);
            req1->Record.MutableLocks()->AddReceivingShards(shard1);
            req1->Record.MutableLocks()->AddReceivingShards(shard2);

            auto req2 = MakeWriteRequestOneKeyValue(
                txId1,
                Volatile ? NKikimrDataEvents::TEvWrite::MODE_VOLATILE_PREPARE : NKikimrDataEvents::TEvWrite::MODE_PREPARE,
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT,
                tableId,
                columns,
                12, 1004);
            req2->Record.MutableLocks()->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
            req2->Record.MutableLocks()->AddSendingShards(shard1);
            req2->Record.MutableLocks()->AddSendingShards(shard2);
            req2->Record.MutableLocks()->AddReceivingShards(shard1);
            req2->Record.MutableLocks()->AddReceivingShards(shard2);

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
                    .Volatile = Volatile,
                });
        }

        // Check tx1 replies
        {
            auto ev1 = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvWriteResult>(tx1sender);
            auto ev2 = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvWriteResult>(tx1sender);
            UNIT_ASSERT_C(
                ev1->Get()->Record.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED &&
                ev2->Get()->Record.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED,
                "Unexpected status: " << ev1->Get()->Record.GetStatus() << " and " << ev2->Get()->Record.GetStatus()
            );
        }

        // Validate commit was fully applied
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table` ORDER BY key;
            )"),
            "{ items { int32_value: 1 } items { int32_value: 1001 } }, "
            "{ items { int32_value: 2 } items { int32_value: 1003 } }, "
            "{ items { int32_value: 11 } items { int32_value: 1002 } }, "
            "{ items { int32_value: 12 } items { int32_value: 1004 } }"
        );
    }

    Y_UNIT_TEST_TWIN(DistributedInsertWithoutLocks, Volatile) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key))
                WITH (PARTITION_AT_KEYS = (10));
            )"),
            "SUCCESS"
        );

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES
                (1, 1001),
                (11, 1002);
        )");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);

        auto shard1 = shards.at(0);
        auto shard2 = shards.at(1);

        TVector<TShardedTableOptions::TColumn> columns{
            {"key", "Int32", true, false},
            {"value", "Int32", false, false},
        };

        const ui64 coordinator = ChangeStateStorage(Coordinator, server->GetSettings().Domain);

        // Prepare an insert (readsets flow between shards)
        ui64 txId1 = 1234567890011;
        auto tx1sender = runtime.AllocateEdgeActor();
        {
            auto req1 = MakeWriteRequestOneKeyValue(
                txId1,
                Volatile ? NKikimrDataEvents::TEvWrite::MODE_VOLATILE_PREPARE : NKikimrDataEvents::TEvWrite::MODE_PREPARE,
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT,
                tableId,
                columns,
                2, 1003);
            req1->Record.MutableLocks()->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
            req1->Record.MutableLocks()->AddSendingShards(shard1);
            req1->Record.MutableLocks()->AddSendingShards(shard2);
            req1->Record.MutableLocks()->AddReceivingShards(shard1);
            req1->Record.MutableLocks()->AddReceivingShards(shard2);

            auto req2 = MakeWriteRequestOneKeyValue(
                txId1,
                Volatile ? NKikimrDataEvents::TEvWrite::MODE_VOLATILE_PREPARE : NKikimrDataEvents::TEvWrite::MODE_PREPARE,
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT,
                tableId,
                columns,
                12, 1004);
            req2->Record.MutableLocks()->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
            req2->Record.MutableLocks()->AddSendingShards(shard1);
            req2->Record.MutableLocks()->AddSendingShards(shard2);
            req2->Record.MutableLocks()->AddReceivingShards(shard1);
            req2->Record.MutableLocks()->AddReceivingShards(shard2);

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
                    .Volatile = Volatile,
                });
        }

        // Check tx1 replies
        {
            auto ev1 = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvWriteResult>(tx1sender);
            auto ev2 = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvWriteResult>(tx1sender);
            UNIT_ASSERT_C(
                ev1->Get()->Record.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED &&
                ev2->Get()->Record.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED,
                "Unexpected status: " << ev1->Get()->Record.GetStatus() << " and " << ev2->Get()->Record.GetStatus()
            );
        }

        // Validate commit was not partially applied
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table` ORDER BY key;
            )"),
            "{ items { int32_value: 1 } items { int32_value: 1001 } }, "
            "{ items { int32_value: 2 } items { int32_value: 1003 } }, "
            "{ items { int32_value: 11 } items { int32_value: 1002 } }, "
            "{ items { int32_value: 12 } items { int32_value: 1004 } }"
        );
    }

    Y_UNIT_TEST_TWIN(DistributedInsertDuplicateWithLocks, Volatile) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key))
                WITH (PARTITION_AT_KEYS = (10));
            )"),
            "SUCCESS"
        );

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES
                (1, 1001),
                (11, 1002);
        )");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);

        auto shard1 = shards.at(0);
        auto shard2 = shards.at(1);

        TVector<TShardedTableOptions::TColumn> columns{
            {"key", "Int32", true, false},
            {"value", "Int32", false, false},
        };

        const ui64 coordinator = ChangeStateStorage(Coordinator, server->GetSettings().Domain);

        const ui64 lockTxId1 = 1234567890001;
        const ui64 lockNodeId = runtime.GetNodeId(0);
        NLongTxService::TLockHandle lockHandle1(lockTxId1, runtime.GetActorSystem(0));

        NKikimrDataEvents::TLock lock1shard1;
        NKikimrDataEvents::TLock lock1shard2;

        // Make a read (lock1 shard1)
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

        // Make a read (lock1 shard2)
        auto read2sender = runtime.AllocateEdgeActor();
        {
            Cerr << "... making a read from " << shard2 << Endl;
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
                i32 key = 11;
                TVector<TCell> keys;
                keys.push_back(TCell::Make(key));
                req->Keys.push_back(TSerializedCellVec(TSerializedCellVec::Serialize(keys)));
            }
            ForwardToTablet(runtime, shard2, read2sender, req.release());
            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvReadResult>(read2sender);
            auto* res = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetFinished(), true);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetTxLocks().size(), 1u);
            lock1shard2 = res->Record.GetTxLocks().at(0);
            UNIT_ASSERT_C(lock1shard2.GetCounter() < 1000, "Unexpected lock in the result: " << lock1shard2.ShortDebugString());
        }

        // Prepare an insert (readsets flow between shards)
        ui64 txId1 = 1234567890011;
        auto tx1sender = runtime.AllocateEdgeActor();
        {
            auto req1 = MakeWriteRequestOneKeyValue(
                txId1,
                Volatile ? NKikimrDataEvents::TEvWrite::MODE_VOLATILE_PREPARE : NKikimrDataEvents::TEvWrite::MODE_PREPARE,
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT,
                tableId,
                columns,
                1, 1003);
            req1->Record.MutableLocks()->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
            req1->Record.MutableLocks()->AddSendingShards(shard1);
            req1->Record.MutableLocks()->AddSendingShards(shard2);
            req1->Record.MutableLocks()->AddReceivingShards(shard1);
            req1->Record.MutableLocks()->AddReceivingShards(shard2);
            *req1->Record.MutableLocks()->AddLocks() = lock1shard1;

            auto req2 = MakeWriteRequestOneKeyValue(
                txId1,
                Volatile ? NKikimrDataEvents::TEvWrite::MODE_VOLATILE_PREPARE : NKikimrDataEvents::TEvWrite::MODE_PREPARE,
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT,
                tableId,
                columns,
                12, 1004);
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
                    .Volatile = Volatile,
                });
        }

        // Check tx1 replies
        {
            auto ev1 = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvWriteResult>(tx1sender);
            auto ev2 = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvWriteResult>(tx1sender);
            UNIT_ASSERT_C(
                ev1->Get()->Record.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_CONSTRAINT_VIOLATION ||
                ev2->Get()->Record.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_CONSTRAINT_VIOLATION,
                "Unexpected status: " << ev1->Get()->Record.GetStatus() << " and " << ev2->Get()->Record.GetStatus()
            );
        }

        // Validate commit was not partially applied
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table` ORDER BY key;
            )"),
            "{ items { int32_value: 1 } items { int32_value: 1001 } }, "
            "{ items { int32_value: 11 } items { int32_value: 1002 } }"
        );
    }

    Y_UNIT_TEST(VolatileAndNonVolatileWritePlanStepCommitFailure) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key))
                WITH (PARTITION_AT_KEYS = (10));
            )"),
            "SUCCESS"
        );

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES
                (1, 1001),
                (11, 1002);
        )");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);

        auto shard1 = shards.at(0);
        auto shard2 = shards.at(1);

        TVector<TShardedTableOptions::TColumn> columns{
            {"key", "Int32", true, false},
            {"value", "Int32", false, false},
        };

        const ui64 coordinator = ChangeStateStorage(Coordinator, server->GetSettings().Domain);

        // Prepare upsert 1 (non-volatile)
        ui64 txId1 = 1234567890011;
        auto tx1sender = runtime.AllocateEdgeActor();
        ui64 tx1minStep;
        ui64 tx1maxStep;
        {
            auto req1 = MakeWriteRequestOneKeyValue(
                txId1,
                NKikimrDataEvents::TEvWrite::MODE_PREPARE,
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                tableId,
                columns,
                2, 1003);

            auto req2 = MakeWriteRequestOneKeyValue(
                txId1,
                NKikimrDataEvents::TEvWrite::MODE_PREPARE,
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                tableId,
                columns,
                12, 1003);

            Cerr << "... preparing tx1 at " << shard1 << Endl;
            auto res1 = Write(runtime, tx1sender, shard1, std::move(req1));
            Cerr << "... preparing tx1 at " << shard2 << Endl;
            auto res2 = Write(runtime, tx1sender, shard2, std::move(req2));

            tx1minStep = Max(res1.GetMinStep(), res2.GetMinStep());
            tx1maxStep = Min(res1.GetMaxStep(), res2.GetMaxStep());
        }

        // Prepare upsert 2 (volatile)
        ui64 txId2 = 1234567890012;
        auto tx2sender = runtime.AllocateEdgeActor();
        ui64 tx2minStep;
        ui64 tx2maxStep;
        {
            auto req1 = MakeWriteRequestOneKeyValue(
                txId2,
                NKikimrDataEvents::TEvWrite::MODE_VOLATILE_PREPARE,
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                tableId,
                columns,
                2, 1004);
            req1->Record.MutableLocks()->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
            req1->Record.MutableLocks()->AddSendingShards(shard1);
            req1->Record.MutableLocks()->AddSendingShards(shard2);
            req1->Record.MutableLocks()->AddReceivingShards(shard1);
            req1->Record.MutableLocks()->AddReceivingShards(shard2);

            auto req2 = MakeWriteRequestOneKeyValue(
                txId2,
                NKikimrDataEvents::TEvWrite::MODE_VOLATILE_PREPARE,
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                tableId,
                columns,
                13, 1004);
            req2->Record.MutableLocks()->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
            req2->Record.MutableLocks()->AddSendingShards(shard1);
            req2->Record.MutableLocks()->AddSendingShards(shard2);
            req2->Record.MutableLocks()->AddReceivingShards(shard1);
            req2->Record.MutableLocks()->AddReceivingShards(shard2);

            Cerr << "... preparing tx2 at " << shard1 << Endl;
            auto res1 = Write(runtime, tx2sender, shard1, std::move(req1));
            Cerr << "... preparing tx2 at " << shard2 << Endl;
            auto res2 = Write(runtime, tx2sender, shard2, std::move(req2));

            tx2minStep = Max(res1.GetMinStep(), res2.GetMinStep());
            tx2maxStep = Min(res1.GetMaxStep(), res2.GetMaxStep());
        }

        // Block plan steps at shard 1
        TBlockEvents<TEvTxProcessing::TEvPlanStep> blockedPlan(runtime,
            [shard1](const auto& ev) {
                if (ev->Get()->Record.GetTabletID() == shard1) {
                    Cerr << "... blocking plan step at " << shard1 << Endl;
                    return true;
                }
                return false;
            });

        Cerr << "... planning tx1 at " << coordinator << Endl;
        SendProposeToCoordinator(
            runtime, tx1sender, shards, {
                .TxId = txId1,
                .Coordinator = coordinator,
                .MinStep = tx1minStep,
                .MaxStep = tx1maxStep,
                .Volatile = false,
            });
        while (auto ev = runtime.GrabEdgeEventRethrow<TEvTxProxy::TEvProposeTransactionStatus>(tx1sender, TDuration::Seconds(10))) {
            if (ev->Get()->GetStatus() == TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusPlanned) {
                break;
            }
        }

        Cerr << "... planning tx2 at " << coordinator << Endl;
        SendProposeToCoordinator(
            runtime, tx2sender, shards, {
                .TxId = txId2,
                .Coordinator = coordinator,
                .MinStep = tx2minStep,
                .MaxStep = tx2maxStep,
                .Volatile = true,
            });
        while (auto ev = runtime.GrabEdgeEventRethrow<TEvTxProxy::TEvProposeTransactionStatus>(tx2sender, TDuration::Seconds(10))) {
            if (ev->Get()->GetStatus() == TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusPlanned) {
                break;
            }
        }

        runtime.WaitFor("blocked plan steps", [&]{
            return blockedPlan.size() >= 2;
        });
        Cerr << "... blocked " << blockedPlan.size() << " plan steps" << Endl;
        runtime.SimulateSleep(TDuration::MilliSeconds(10));

        // Block commits at shard 1
        TBlockEvents<TEvBlobStorage::TEvPut> blockedCommits(runtime,
            [shard1](const auto& ev) {
                auto* msg = ev->Get();
                if (msg->Id.TabletID() == shard1 && msg->Id.Channel() == 0) {
                    Cerr << "... blocking put " << msg->Id << Endl;
                    return true;
                }
                return false;
            });

        // Block TEvPrivate::TEvProgressTransaction at shard 1
        TBlockEvents<IEventHandle> blockedProgress(runtime,
            [actor = ResolveTablet(runtime, shard1)](const TAutoPtr<IEventHandle>& ev) {
                return ev->GetRecipientRewrite() == actor &&
                    ev->GetTypeRewrite() == EventSpaceBegin(TKikimrEvents::ES_PRIVATE) + 0;
            });

        Cerr << "... unblocking plan steps" << Endl;
        blockedPlan.Unblock();
        runtime.SimulateSleep(TDuration::MilliSeconds(10));
        UNIT_ASSERT_C(blockedCommits.size() > 0, "expected to block some commits");
        Cerr << "... blocked " << blockedCommits.size() << " commits" << Endl;

        Cerr << "... replying ERROR to all blocked commits" << Endl;
        blockedCommits.Stop();
        for (auto& ev : blockedCommits) {
            auto proxy = ev->Recipient;
            ui32 groupId = GroupIDFromBlobStorageProxyID(proxy);
            auto res = ev->Get()->MakeErrorResponse(NKikimrProto::ERROR, "Something went wrong", TGroupId::FromValue(groupId));
            runtime.Send(new IEventHandle(ev->Sender, proxy, res.release()), 0, true);
        }
        blockedCommits.clear();

        // Shard 1 will restart and we will block plan steps again
        // However volatile transaction will be migrated, and since it already
        // has a plan step it might erroneously execute before another
        // non-volatile transaction (since plan step commit failed). It is
        // important that migration transforms such known steps to predicted
        // plan steps.
        runtime.WaitFor("blocked plan steps", [&]{
            return blockedPlan.size() >= 2;
        });
        runtime.SimulateSleep(TDuration::MilliSeconds(10));
        Cerr << "... unblocking plan steps again" << Endl;
        blockedPlan.Stop().Unblock();

        // Everything should commit successfully

        // Check tx1 replies
        {
            auto ev1 = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvWriteResult>(tx1sender, TDuration::Seconds(10));
            auto ev2 = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvWriteResult>(tx1sender, TDuration::Seconds(10));
            UNIT_ASSERT(ev1 && ev2);
            UNIT_ASSERT_C(
                ev1->Get()->Record.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED &&
                ev2->Get()->Record.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED,
                "Unexpected status: " << ev1->Get()->Record.GetStatus() << " and " << ev2->Get()->Record.GetStatus()
            );
        }

        // Check tx2 replies
        {
            auto ev1 = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvWriteResult>(tx2sender, TDuration::Seconds(10));
            auto ev2 = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvWriteResult>(tx2sender, TDuration::Seconds(10));
            UNIT_ASSERT(ev1 && ev2);
            UNIT_ASSERT_C(
                ev1->Get()->Record.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED &&
                ev2->Get()->Record.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED,
                "Unexpected status: " << ev1->Get()->Record.GetStatus() << " and " << ev2->Get()->Record.GetStatus()
            );
        }

        // Validate the final result (tx1 should have executed before tx2)
        Cerr << "... validating table" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table` ORDER BY key;
            )"),
            "{ items { int32_value: 1 } items { int32_value: 1001 } }, "
            "{ items { int32_value: 2 } items { int32_value: 1004 } }, "
            "{ items { int32_value: 11 } items { int32_value: 1002 } }, "
            "{ items { int32_value: 12 } items { int32_value: 1003 } }, "
            "{ items { int32_value: 13 } items { int32_value: 1004 } }");
    }

    Y_UNIT_TEST(UncommittedUpdateLockMissingRow) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS"
        );

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES
                (1, 1001);
        )");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        UNIT_ASSERT(tableId);
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        // Observe write results and corresponding locks
        size_t writeResults = 0;
        std::deque<NKikimrDataEvents::TLock> writeLocks;
        auto writeResultsObserver = runtime.AddObserver<NEvents::TDataEvents::TEvWriteResult>(
            [&](NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
                auto* msg = ev->Get();
                for (const auto& lock : msg->Record.GetTxLocks()) {
                    writeLocks.push_back(lock);
                }
                ++writeResults;
            });

        // Conditionally change UPSERT into UPDATE operations
        size_t writesChanged = 0;
        size_t writesObserved = 0;
        bool changeUpsertToUpdate = false;
        auto changeUpsertToUpdateObserver = runtime.AddObserver<NEvents::TDataEvents::TEvWrite>(
            [&](NEvents::TDataEvents::TEvWrite::TPtr& ev) {
                if (changeUpsertToUpdate) {
                    auto* msg = ev->Get();
                    for (auto& op : *msg->Record.MutableOperations()) {
                        if (op.GetType() == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT) {
                            op.SetType(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE);
                            ++writesChanged;
                        }
                    }
                }
                ++writesObserved;
            });

        // Only a single existing row should be updated
        // Bug: the missing row was not locked in any way
        TString sessionId, txId;
        changeUpsertToUpdate = true;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, R"(
                PRAGMA kikimr.KqpForceImmediateEffectsExecution="true";
                UPSERT INTO `/Root/table` (key, value) VALUES (2, 2001);
            )"),
            "<empty>");
        changeUpsertToUpdate = false;
        UNIT_ASSERT_VALUES_EQUAL(writesObserved, 1u);
        UNIT_ASSERT_VALUES_EQUAL(writesChanged, 1u);
        UNIT_ASSERT_VALUES_EQUAL(writeResults, 1u);
        UNIT_ASSERT_VALUES_EQUAL(writeLocks.size(), 1u);
        {
            const auto& lock = writeLocks.back();
            UNIT_ASSERT_C(
                !lock.GetHasWrites() && lock.GetCounter() < 1000000,
                "Unexpected lock: " << lock.DebugString());
        }

        // Upsert a value which conflicts with the update above
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                UPSERT INTO `/Root/table` (key, value) VALUES (2, 3001), (3, 3002);
            )"),
            "<empty>");

        // Observe current state of the table
        // Now the above update could only commit after keys 2 and 3 appeared
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table` ORDER BY key;
            )"),
            "{ items { int32_value: 1 } items { int32_value: 1001 } }, "
            "{ items { int32_value: 2 } items { int32_value: 3001 } }, "
            "{ items { int32_value: 3 } items { int32_value: 3002 } }");

        // Since we cannot possibly commit without also updating 2 the commit must abort
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleCommit(runtime, sessionId, txId, R"(
                UPSERT INTO `/Root/table` (key, value) VALUES (3, 2002);
            )"),
            "ERROR: ABORTED");
    }

    Y_UNIT_TEST(UncommittedUpdateLockNewRowAboveSnapshot) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key))
                WITH (PARTITION_AT_KEYS = (10));
            )"),
            "SUCCESS"
        );

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES
                (1, 1001),
                (11, 1002);
        )");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        UNIT_ASSERT(tableId);
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);

        // Observe write results and corresponding locks
        size_t writeResults = 0;
        std::deque<NKikimrDataEvents::TLock> writeLocks;
        auto writeResultsObserver = runtime.AddObserver<NEvents::TDataEvents::TEvWriteResult>(
            [&](NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
                auto* msg = ev->Get();
                for (const auto& lock : msg->Record.GetTxLocks()) {
                    writeLocks.push_back(lock);
                }
                ++writeResults;
            });

        // Conditionally change UPSERT into UPDATE operations
        size_t writesChanged = 0;
        size_t writesObserved = 0;
        bool changeUpsertToUpdate = false;
        auto changeUpsertToUpdateObserver = runtime.AddObserver<NEvents::TDataEvents::TEvWrite>(
            [&](NEvents::TDataEvents::TEvWrite::TPtr& ev) {
                if (changeUpsertToUpdate) {
                    auto* msg = ev->Get();
                    for (auto& op : *msg->Record.MutableOperations()) {
                        if (op.GetType() == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT) {
                            op.SetType(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE);
                            ++writesChanged;
                        }
                    }
                }
                ++writesObserved;
            });

        // Perform a SELECT query which will establish a repeatable read snapshot
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, R"(
                PRAGMA kikimr.KqpForceImmediateEffectsExecution="true";
                SELECT key, value FROM `/Root/table` WHERE key = 11;
            )"),
            "{ items { int32_value: 11 } items { int32_value: 1002 } }");

        // Make sure timecast advances past the snapshot
        runtime.SimulateSleep(TDuration::Seconds(2));

        // Upsert some new rows above the snapshot
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                UPSERT INTO `/Root/table` (key, value) VALUES (2, 3001), (3, 3002);
            )"),
            "<empty>");

        writeResults = 0;
        writeLocks.clear();
        writesChanged = 0;
        writesObserved = 0;

        // Bug: row doesn't exist in the snapshot, but there was no lock and no conflict detection
        // When fixed the read lock should report "already broken", which would result in an aborted error
        changeUpsertToUpdate = true;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleContinue(runtime, sessionId, txId, R"(
                PRAGMA kikimr.KqpForceImmediateEffectsExecution="true";
                UPSERT INTO `/Root/table` (key, value) VALUES (2, 2001);
            )"),
            "ERROR: ABORTED");
        changeUpsertToUpdate = false;
        UNIT_ASSERT_C(writesObserved >= 1u, writesObserved);
        UNIT_ASSERT_C(writesChanged >= 1u, writesChanged);
        UNIT_ASSERT_C(writeResults >= 1u, writeResults);
        UNIT_ASSERT_VALUES_EQUAL(writeLocks.size(), 1u);
        {
            const auto& lock = writeLocks.back();
            UNIT_ASSERT_C(
                !lock.GetHasWrites() && lock.GetCounter() == TSysTables::TLocksTable::TLock::ErrorAlreadyBroken,
                "Unexpected lock: " << lock.DebugString());
        }
    }

    Y_UNIT_TEST(UncommittedUpdateLockDeletedRowAboveSnapshot) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key))
                WITH (PARTITION_AT_KEYS = (10));
            )"),
            "SUCCESS"
        );

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES
                (1, 1001),
                (11, 1002);
        )");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        UNIT_ASSERT(tableId);
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);

        // Observe write results and corresponding locks
        size_t writeResults = 0;
        std::deque<NKikimrDataEvents::TLock> writeLocks;
        auto writeResultsObserver = runtime.AddObserver<NEvents::TDataEvents::TEvWriteResult>(
            [&](NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
                auto* msg = ev->Get();
                for (const auto& lock : msg->Record.GetTxLocks()) {
                    writeLocks.push_back(lock);
                }
                ++writeResults;
            });

        // Conditionally change UPSERT into UPDATE operations
        size_t writesChanged = 0;
        size_t writesObserved = 0;
        bool changeUpsertToUpdate = false;
        auto changeUpsertToUpdateObserver = runtime.AddObserver<NEvents::TDataEvents::TEvWrite>(
            [&](NEvents::TDataEvents::TEvWrite::TPtr& ev) {
                if (changeUpsertToUpdate) {
                    auto* msg = ev->Get();
                    for (auto& op : *msg->Record.MutableOperations()) {
                        if (op.GetType() == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT) {
                            op.SetType(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE);
                            ++writesChanged;
                        }
                    }
                }
                ++writesObserved;
            });

        // Perform a SELECT query which will establish a repeatable read snapshot
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, R"(
                PRAGMA kikimr.KqpForceImmediateEffectsExecution="true";
                SELECT key, value FROM `/Root/table` WHERE key = 11;
            )"),
            "{ items { int32_value: 11 } items { int32_value: 1002 } }");

        // Make sure timecast advances past the snapshot
        runtime.SimulateSleep(TDuration::Seconds(2));

        // Delete an existing row above the snapshot
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                DELETE FROM `/Root/table` WHERE key = 1;
            )"),
            "<empty>");

        writeResults = 0;
        writeLocks.clear();
        writesChanged = 0;
        writesObserved = 0;

        // Row exists in the snapshot, but the update doesn't make sense since it would never be able to commit
        changeUpsertToUpdate = true;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleContinue(runtime, sessionId, txId, R"(
                PRAGMA kikimr.KqpForceImmediateEffectsExecution="true";
                UPSERT INTO `/Root/table` (key, value) VALUES (1, 2001);
            )"),
            "ERROR: ABORTED");
        changeUpsertToUpdate = false;
        UNIT_ASSERT_C(writesObserved >= 1u, writesObserved);
        UNIT_ASSERT_C(writesChanged >= 1u, writesChanged);
        UNIT_ASSERT_C(writeResults >= 1u, writeResults);
        UNIT_ASSERT_VALUES_EQUAL(writeLocks.size(), 1u);
        {
            const auto& lock = writeLocks.back();
            UNIT_ASSERT_C(
                !lock.GetHasWrites() && lock.GetCounter() == TSysTables::TLocksTable::TLock::ErrorAlreadyBroken,
                "Unexpected lock: " << lock.DebugString());
        }
    }

    Y_UNIT_TEST(UncommittedUpdateLockUncommittedNewRow) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key))
                WITH (PARTITION_AT_KEYS = (10));
            )"),
            "SUCCESS"
        );

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES
                (1, 1001),
                (11, 1002);
        )");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        UNIT_ASSERT(tableId);
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);

        // Observe write results and corresponding locks
        size_t writeResults = 0;
        std::deque<NKikimrDataEvents::TLock> writeLocks;
        auto writeResultsObserver = runtime.AddObserver<NEvents::TDataEvents::TEvWriteResult>(
            [&](NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
                auto* msg = ev->Get();
                for (const auto& lock : msg->Record.GetTxLocks()) {
                    writeLocks.push_back(lock);
                }
                ++writeResults;
            });

        // Conditionally change UPSERT into UPDATE operations
        size_t writesChanged = 0;
        size_t writesObserved = 0;
        bool changeUpsertToUpdate = false;
        auto changeUpsertToUpdateObserver = runtime.AddObserver<NEvents::TDataEvents::TEvWrite>(
            [&](NEvents::TDataEvents::TEvWrite::TPtr& ev) {
                if (changeUpsertToUpdate) {
                    auto* msg = ev->Get();
                    for (auto& op : *msg->Record.MutableOperations()) {
                        if (op.GetType() == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT) {
                            op.SetType(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE);
                            ++writesChanged;
                        }
                    }
                }
                ++writesObserved;
            });

        // Perform a SELECT query which will establish a repeatable read snapshot
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, R"(
                PRAGMA kikimr.KqpForceImmediateEffectsExecution="true";
                SELECT key, value FROM `/Root/table` WHERE key = 11;
            )"),
            "{ items { int32_value: 11 } items { int32_value: 1002 } }");

        // Make sure timecast advances past the snapshot
        runtime.SimulateSleep(TDuration::Seconds(2));

        // Upsert some new rows above the snapshot
        TString sessionId2, txId2;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId2, txId2, R"(
                PRAGMA kikimr.KqpForceImmediateEffectsExecution="true";
                UPSERT INTO `/Root/table` (key, value) VALUES (2, 3001), (3, 3002);
            )"),
            "<empty>");

        writeResults = 0;
        writeLocks.clear();
        writesChanged = 0;
        writesObserved = 0;

        // Row doesn't exist in the snapshot and there's a chance for tx to commit
        // The conflict with tx2 must be recorded however
        changeUpsertToUpdate = true;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleContinue(runtime, sessionId, txId, R"(
                PRAGMA kikimr.KqpForceImmediateEffectsExecution="true";
                UPSERT INTO `/Root/table` (key, value) VALUES (2, 2001);
            )"),
            "<empty>");
        changeUpsertToUpdate = false;
        UNIT_ASSERT_C(writesObserved == 1u, writesObserved);
        UNIT_ASSERT_C(writesChanged == 1u, writesChanged);
        UNIT_ASSERT_C(writeResults == 1u, writeResults);
        UNIT_ASSERT_VALUES_EQUAL(writeLocks.size(), 1u);
        {
            const auto& lock = writeLocks.back();
            UNIT_ASSERT_C(
                !lock.GetHasWrites() && lock.GetCounter() < 1000000,
                "Unexpected lock: " << lock.DebugString());
        }

        // Now we commit tx2, which makes it impossible to commit tx1
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleCommit(runtime, sessionId2, txId2, R"(
                SELECT 1;
            )"),
            "{ items { int32_value: 1 } }");

        // Trying to commit tx1 should fail with an aborted error now
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleCommit(runtime, sessionId, txId, R"(
                UPSERT INTO `/Root/table` (key, value) VALUES (3, 2002);
            )"),
            "ERROR: ABORTED");
    }

    Y_UNIT_TEST(UncommittedUpdateLockUncommittedDeleteRow) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key))
                WITH (PARTITION_AT_KEYS = (10));
            )"),
            "SUCCESS"
        );

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES
                (1, 1001),
                (11, 1002);
        )");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        UNIT_ASSERT(tableId);
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);

        // Observe write results and corresponding locks
        size_t writeResults = 0;
        std::deque<NKikimrDataEvents::TLock> writeLocks;
        auto writeResultsObserver = runtime.AddObserver<NEvents::TDataEvents::TEvWriteResult>(
            [&](NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
                auto* msg = ev->Get();
                for (const auto& lock : msg->Record.GetTxLocks()) {
                    writeLocks.push_back(lock);
                }
                ++writeResults;
            });

        // Conditionally change UPSERT into UPDATE operations
        size_t writesChanged = 0;
        size_t writesObserved = 0;
        bool changeUpsertToUpdate = false;
        auto changeUpsertToUpdateObserver = runtime.AddObserver<NEvents::TDataEvents::TEvWrite>(
            [&](NEvents::TDataEvents::TEvWrite::TPtr& ev) {
                if (changeUpsertToUpdate) {
                    auto* msg = ev->Get();
                    for (auto& op : *msg->Record.MutableOperations()) {
                        if (op.GetType() == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT) {
                            op.SetType(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE);
                            ++writesChanged;
                        }
                    }
                }
                ++writesObserved;
            });

        // Perform a SELECT query which will establish a repeatable read snapshot
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, R"(
                PRAGMA kikimr.KqpForceImmediateEffectsExecution="true";
                SELECT key, value FROM `/Root/table` WHERE key = 11;
            )"),
            "{ items { int32_value: 11 } items { int32_value: 1002 } }");

        // Make sure timecast advances past the snapshot
        runtime.SimulateSleep(TDuration::Seconds(2));

        // Delete a row in an uncommitted transaction
        TString sessionId2, txId2;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId2, txId2, R"(
                PRAGMA kikimr.KqpForceImmediateEffectsExecution="true";
                DELETE FROM `/Root/table` WHERE key = 1;
            )"),
            "<empty>");
        UNIT_ASSERT_VALUES_EQUAL(writeResults, 1u);

        writeResults = 0;
        writeLocks.clear();
        writesChanged = 0;
        writesObserved = 0;

        // Row with key=1 exist in the snapshot and must be updated (might commit later)
        // The conflict with tx2 must be recorded however
        changeUpsertToUpdate = true;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleContinue(runtime, sessionId, txId, R"(
                PRAGMA kikimr.KqpForceImmediateEffectsExecution="true";
                UPSERT INTO `/Root/table` (key, value) VALUES (1, 2001);
            )"),
            "<empty>");
        changeUpsertToUpdate = false;
        UNIT_ASSERT_C(writesObserved == 1u, writesObserved);
        UNIT_ASSERT_C(writesChanged == 1u, writesChanged);
        UNIT_ASSERT_C(writeResults == 1u, writeResults);
        UNIT_ASSERT_VALUES_EQUAL(writeLocks.size(), 1u);
        {
            const auto& lock = writeLocks.back();
            UNIT_ASSERT_C(
                lock.GetHasWrites() && lock.GetCounter() < 1000000,
                "Unexpected lock: " << lock.DebugString());
        }

        // Now we commit tx2, which makes it impossible to commit tx1
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleCommit(runtime, sessionId2, txId2, R"(
                SELECT 1;
            )"),
            "{ items { int32_value: 1 } }");

        // Trying to commit tx1 should fail with an aborted error now
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleCommit(runtime, sessionId, txId, R"(
                UPSERT INTO `/Root/table` (key, value) VALUES (2, 2002);
            )"),
            "ERROR: ABORTED");
    }

    Y_UNIT_TEST(LocksBrokenStats) {
        auto [runtime, server, sender] = TestCreateServer();

        TShardedTableOptions opts;
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const ui64 shard = shards[0];

        // Insert initial data
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100);"));

        TString sessionId, txId;
        KqpSimpleBegin(runtime, sessionId, txId, Q_("SELECT * FROM `/Root/table-1` WHERE key = 1;"));
        UNIT_ASSERT(!txId.empty());

        // Breaker write - breaks the locks established by the SELECT above
        auto writeRequest = MakeWriteRequestOneKeyValue(
            std::nullopt,  // No lock context - this will break locks
            NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE,
            NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            tableId,
            opts.Columns_,
            1,  // key
            200 // value
        );

        auto writeResult = Write(runtime, sender, shard, std::move(writeRequest), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        UNIT_ASSERT_VALUES_EQUAL(writeResult.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);

        // Verify breaker stats
        UNIT_ASSERT(writeResult.HasTxStats());
        UNIT_ASSERT_VALUES_EQUAL(writeResult.GetTxStats().GetLocksBrokenAsBreaker(), 1u);

        // Now try to commit the victim transaction - it should fail with LOCKS_BROKEN
        TMaybe<NKikimrDataEvents::TEvWriteResult> victimRecord;
        auto observer = runtime.AddObserver<NEvents::TDataEvents::TEvWriteResult>([&](NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
            if (ev->Get()->Record.GetOrigin() == shard &&
                ev->Get()->Record.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN) {
                victimRecord = ev->Get()->Record;
            }
        });

        auto commitResult = KqpSimpleCommit(runtime, sessionId, txId, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 300);"));
        UNIT_ASSERT_VALUES_EQUAL(commitResult, "ERROR: ABORTED");

        // Verify victim was captured
        UNIT_ASSERT(victimRecord.Defined());

        auto tableState = ReadTable(server, shards, tableId);
        UNIT_ASSERT(tableState.find("key = 1, value = 200") != TString::npos);
    }

    Y_UNIT_TEST(TliLocksBrokenByWrite) {
        TStringStream ss;
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetLogBackend(new TStreamLogBackend(&ss));

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TLI, NLog::PRI_INFO);

        InitRoot(server, sender);

        TShardedTableOptions opts;
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const ui64 shard = shards[0];

        // Insert initial data
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100);"));

        TString sessionId, txId;
        KqpSimpleBegin(runtime, sessionId, txId, Q_("SELECT * FROM `/Root/table-1` WHERE key = 1;"));
        UNIT_ASSERT(!txId.empty());

        // Breaker write - breaks the locks established by the SELECT above
        auto writeRequest = MakeWriteRequestOneKeyValue(
            std::nullopt,  // No lock context - this will break locks
            NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE,
            NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            tableId,
            opts.Columns_,
            1,  // key
            200 // value
        );

        auto writeResult = Write(runtime, sender, shard, std::move(writeRequest), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        UNIT_ASSERT_VALUES_EQUAL(writeResult.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);

        // Verify breaker stats
        UNIT_ASSERT(writeResult.HasTxStats());
        UNIT_ASSERT_VALUES_EQUAL(writeResult.GetTxStats().GetLocksBrokenAsBreaker(), 1u);

        // Verify TLI logs contain the lock breaking information
        // The log should contain: Component: DataShard, TabletId, TxId, Message: "Write transaction broke other locks", BrokenLocks: [...]
        TVector<std::pair<TString, ui64>> regexToMatchCount{
            {NTestTli::ConstructRegexToCheckLogs("INFO", "DataShard", "Write transaction broke other locks"), 1},
        };

        NTestTli::CheckRegexMatch(ss.Str(), regexToMatchCount);
    }

    Y_UNIT_TEST(ImmediateWriteVolatileTxIdOnPageFault) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TDisableDataShardLogBatching disableDataShardLogBatching;

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key))
                WITH (PARTITION_AT_KEYS = (10));
            )"),
            "SUCCESS"
        );

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);

        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES
                (1, 1001),
                (11, 1002);
        )");

        // Start blocking readsets
        TBlockEvents<TEvTxProcessing::TEvReadSet> blockedReadSets(runtime);

        // Prepare a distributed upsert
        Cerr << "... starting a distributed upsert" << Endl;
        auto distributedUpsertFuture = KqpSimpleSend(runtime, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES (2, 2001), (12, 2002);
        )");
        runtime.WaitFor("blocked readsets", [&]{ return blockedReadSets.size() >= 4; });

        runtime.SimulateSleep(TDuration::Seconds(1));

        Cerr << "... compacting shard " << shards.at(0) << Endl;
        CompactTable(runtime, shards.at(0), tableId, false);
        Cerr << "... rebooting shard " << shards.at(0) << Endl;
        RebootTablet(runtime, shards.at(0), sender);

        runtime.SimulateSleep(TDuration::Seconds(1));

        Cerr << "... sending an immediate upsert" << Endl;
        auto immediateUpsertFuture = KqpSimpleSend(runtime, R"(
            UPSERT INTO `/Root/table` (key, value) VALUES (2, 3001);
        )");

        runtime.SimulateSleep(TDuration::Seconds(1));

        // Check shard open transactions
        {
            runtime.SendToPipe(shards.at(0), sender, new TEvDataShard::TEvGetOpenTxs(tableId.PathId));
            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvGetOpenTxsResult>(sender);
            for (ui64 txId : ev->Get()->OpenTxs) {
                UNIT_ASSERT_C(txId > 1000000, "unexpected open tx " << txId << " at shard " << shards.at(0));
            }
        }
    }

    Y_UNIT_TEST(TxCompleteLagForEvWrite) {
        // Verifies that a planned-but-not-completed TEvWrite transaction
        // contributes to the TxCompleteLag metric.
        auto [runtime, server, sender] = TestCreateServer();

        TShardedTableOptions opts;
        const auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const ui64 shard = shards[0];
        const ui64 coordinator = ChangeStateStorage(Coordinator, server->GetSettings().Domain);
        const TActorId shardActor = ResolveTablet(runtime, shard);

        auto getDataTxCompleteLag = [&]() -> ui64 {
            auto edge = runtime.AllocateEdgeActor();
            runtime.SendToPipe(shard, edge, new TEvDataShard::TEvGetInfoRequest(),
                0, GetPipeConfigWithRetries());
            TAutoPtr<IEventHandle> handle;
            auto* resp = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvGetInfoResponse>(handle);
            UNIT_ASSERT(resp);
            return resp->Record.GetActivities().GetDataTxCompleteLag();
        };

        auto getTxCompleteLagCounter = [&]() -> ui64 {
            auto edge = runtime.AllocateEdgeActor();
            runtime.SendToPipe(shard, edge, new TEvTablet::TEvGetCounters(),
                0, GetPipeConfigWithRetries());
            auto ev = runtime.GrabEdgeEventRethrow<TEvTablet::TEvGetCountersResponse>(edge);
            UNIT_ASSERT(ev);
            for (const auto& counter : ev->Get()->Record.GetTabletCounters().GetAppCounters().GetSimpleCounters()) {
                if (counter.GetName() == "DataShard/TxCompleteLag") {
                    return counter.GetValue();
                }
            }
            UNIT_ASSERT_C(false, "DataShard/TxCompleteLag counter not found");
            return 0;
        };

        Cout << "========= Baseline: no pending tx, lag must be 0 =========\n";
        UNIT_ASSERT_VALUES_EQUAL(getDataTxCompleteLag(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(getTxCompleteLagCounter(), 0u);

        Cout << "========= Prepare a distributed write =========\n";
        const ui64 txId = 100;
        const ui32 rowCount = 1;
        ui64 minStep, maxStep;
        {
            const auto writeResult = Upsert(runtime, sender, shard, tableId, opts.Columns_, rowCount,
                txId, NKikimrDataEvents::TEvWrite::MODE_PREPARE);
            minStep = writeResult.GetMinStep();
            maxStep = writeResult.GetMaxStep();
        }

        // Block TEvPrivate::TEvProgressTransaction so the write stays in the plan
        // queue after the plan step is processed. This keeps the oldest-planned
        // WriteTx pinned at its plan step while mediator time keeps advancing.
        TBlockEvents<IEventHandle> blockedProgress(runtime,
            [shardActor](const TAutoPtr<IEventHandle>& ev) {
                return ev->GetRecipientRewrite() == shardActor &&
                    ev->GetTypeRewrite() == EventSpaceBegin(TKikimrEvents::ES_PRIVATE) + 0;
            });

        Cout << "========= Send propose to coordinator =========\n";
        SendProposeToCoordinator(runtime, sender, shards, {
            .TxId = txId,
            .Coordinator = coordinator,
            .MinStep = minStep,
            .MaxStep = maxStep,
        });

        runtime.WaitFor("blocked progress transaction",
            [&]{ return blockedProgress.size() >= 1; });

        // Sleep long enough for mediator time to advance past the plan step and
        // for at least one periodic tablet wakeup (5s) to refresh the counter.
        Cout << "========= Let mediator time advance past the plan step =========\n";
        runtime.SimulateSleep(TDuration::Seconds(6));

        const ui64 lag = getDataTxCompleteLag();
        Cout << "========= Observed DataTxCompleteLag = " << lag << " ms =========\n";
        UNIT_ASSERT_C(lag > 0,
            "Expected non-zero DataTxCompleteLag for a pending WriteTx, got " << lag);

        const ui64 counterLag = getTxCompleteLagCounter();
        Cout << "========= Observed TxCompleteLag counter = " << counterLag << " ms =========\n";
        UNIT_ASSERT_C(counterLag > 0,
            "Expected non-zero TxCompleteLag counter for a pending WriteTx, got " << counterLag);

        // Unblock and let the write finish so the lag returns to 0.
        blockedProgress.Stop().Unblock();
        WaitForWriteCompleted(runtime, sender);

        // Another periodic wakeup is needed for the counter to be refreshed.
        runtime.SimulateSleep(TDuration::Seconds(6));
        UNIT_ASSERT_VALUES_EQUAL(getDataTxCompleteLag(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(getTxCompleteLagCounter(), 0u);
    }

    Y_UNIT_TEST(PipelinedUncommittedWriteChain) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false);
        serverSettings.AppConfig->MutableFeatureFlags()->SetEnableDataShardPipelinedUncommittedWrites(true);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TShardedTableOptions opts;
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const auto& columns = opts.Columns_;
        const ui64 shard = shards.at(0);

        const ui64 lockTxId = 1234567890001;
        const ui64 lockNodeId = runtime.GetNodeId(0);
        NLongTxService::TLockHandle lockHandle(lockTxId, runtime.GetActorSystem(0));

        NKikimrDataEvents::TLock lock;
        {
            auto result = UncommittedWrite(runtime, sender, shard, tableId, columns, lockTxId, lockNodeId, 1, 11, 0, 1);
            UNIT_ASSERT_VALUES_EQUAL(result.GetTxLocks().size(), 1u);
            lock = result.GetTxLocks().at(0);
            UNIT_ASSERT_VALUES_EQUAL(WriteSeqNumOf(lock).GetWriterIndex(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(WriteSeqNumOf(lock).GetWriteSeqNum(), 1u);
        }
        {
            auto result = UncommittedWrite(runtime, sender, shard, tableId, columns, lockTxId, lockNodeId, 2, 22, 0, 2);
            UNIT_ASSERT_VALUES_EQUAL(result.GetTxLocks().size(), 1u);
            lock = result.GetTxLocks().at(0);
            UNIT_ASSERT_VALUES_EQUAL(WriteSeqNumOf(lock).GetWriteSeqNum(), 2u);
        }

        CommitLock(runtime, sender, shard, lock);

        auto tableState = ReadTable(server, shards, tableId);
        UNIT_ASSERT_VALUES_EQUAL(tableState, "key = 1, value = 11\nkey = 2, value = 22\n");
    }

    Y_UNIT_TEST(PipelinedUncommittedWriteSeqNumGap) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false);
        serverSettings.AppConfig->MutableFeatureFlags()->SetEnableDataShardPipelinedUncommittedWrites(true);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TShardedTableOptions opts;
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const auto& columns = opts.Columns_;
        const ui64 shard = shards.at(0);

        const ui64 lockTxId = 1234567890001;
        const ui64 lockNodeId = runtime.GetNodeId(0);
        NLongTxService::TLockHandle lockHandle(lockTxId, runtime.GetActorSystem(0));

        NKikimrDataEvents::TLock lock;
        {
            auto result = UncommittedWrite(runtime, sender, shard, tableId, columns, lockTxId, lockNodeId, 1, 11, 0, 1);
            lock = result.GetTxLocks().at(0);
        }

        // A gap between EvWrites is a protocol error: the next batch must
        // continue the chain at current + 1. current == 1, so seq 3 is rejected.
        {
            auto result = UncommittedWrite(runtime, sender, shard, tableId, columns, lockTxId, lockNodeId, 2, 22, 0, 3,
                NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
            UNIT_ASSERT_C(result.GetIssues(0).message().Contains("must continue the writer chain"),
                result.GetIssues(0).message());
            UNIT_ASSERT_C(result.GetIssues(0).message().Contains("at 2"),
                result.GetIssues(0).message());
        }

        CommitLock(runtime, sender, shard, lock);

        auto tableState = ReadTable(server, shards, tableId);
        UNIT_ASSERT_VALUES_EQUAL(tableState, "key = 1, value = 11\n");
    }

    Y_UNIT_TEST(PipelinedUncommittedWriteMultipleWriters) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false);
        serverSettings.AppConfig->MutableFeatureFlags()->SetEnableDataShardPipelinedUncommittedWrites(true);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TShardedTableOptions opts;
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const auto& columns = opts.Columns_;
        const ui64 shard = shards.at(0);

        const ui64 lockTxId = 1234567890001;
        const ui64 lockNodeId = runtime.GetNodeId(0);
        NLongTxService::TLockHandle lockHandle(lockTxId, runtime.GetActorSystem(0));

        UncommittedWrite(runtime, sender, shard, tableId, columns, lockTxId, lockNodeId, 1, 11, 0, 1);

        // The interface admits many writers per lock, DataShard implements one
        auto result = UncommittedWrite(runtime, sender, shard, tableId, columns, lockTxId, lockNodeId, 2, 22, 1, 1,
            NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
        UNIT_ASSERT_C(result.GetIssues(0).message().Contains("Multiple writers"), result.GetIssues(0).message());
        UNIT_ASSERT_C(result.GetIssues(0).message().Contains("writer 0"), result.GetIssues(0).message());
        UNIT_ASSERT_C(result.GetIssues(0).message().Contains("got 1"), result.GetIssues(0).message());
    }

    Y_UNIT_TEST(PipelinedUncommittedWriteCommitIndexMismatch) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false);
        serverSettings.AppConfig->MutableFeatureFlags()->SetEnableDataShardPipelinedUncommittedWrites(true);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TShardedTableOptions opts;
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const auto& columns = opts.Columns_;
        const ui64 shard = shards.at(0);

        const ui64 lockTxId = 1234567890001;
        const ui64 lockNodeId = runtime.GetNodeId(0);
        NLongTxService::TLockHandle lockHandle(lockTxId, runtime.GetActorSystem(0));

        UncommittedWrite(runtime, sender, shard, tableId, columns, lockTxId, lockNodeId, 1, 11, 0, 1);

        NKikimrDataEvents::TLock lock;
        {
            auto result = UncommittedWrite(runtime, sender, shard, tableId, columns, lockTxId, lockNodeId, 2, 22, 0, 2);
            lock = result.GetTxLocks().at(0);
            UNIT_ASSERT_VALUES_EQUAL(WriteSeqNumOf(lock).GetWriteSeqNum(), 2u);
        }

        // Commit as if the second flush had never been observed by the shard
        NKikimrDataEvents::TLock staleLock = lock;
        staleLock.MutableWriteSeqNums(0)->SetWriteSeqNum(1);
        CommitLock(runtime, sender, shard, staleLock, NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN);

        auto tableState = ReadTable(server, shards, tableId);
        UNIT_ASSERT_VALUES_EQUAL(tableState, "");
    }

    Y_UNIT_TEST(PipelinedUncommittedWriteCommitExtraWriter) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false);
        serverSettings.AppConfig->MutableFeatureFlags()->SetEnableDataShardPipelinedUncommittedWrites(true);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TShardedTableOptions opts;
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const auto& columns = opts.Columns_;
        const ui64 shard = shards.at(0);

        const ui64 lockTxId = 1234567890001;
        const ui64 lockNodeId = runtime.GetNodeId(0);
        NLongTxService::TLockHandle lockHandle(lockTxId, runtime.GetActorSystem(0));

        UncommittedWrite(runtime, sender, shard, tableId, columns, lockTxId, lockNodeId, 1, 11, 0, 1);

        NKikimrDataEvents::TLock lock;
        {
            auto result = UncommittedWrite(runtime, sender, shard, tableId, columns, lockTxId, lockNodeId, 2, 22, 0, 2);
            lock = result.GetTxLocks().at(0);
        }

        // The echoed set must exactly match what the shard has.
        NKikimrDataEvents::TLock extraWriterLock = lock;
        auto* extra = extraWriterLock.AddWriteSeqNums();
        extra->SetWriterIndex(1);
        extra->SetWriteSeqNum(1);
        CommitLock(runtime, sender, shard, extraWriterLock, NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN);

        auto tableState = ReadTable(server, shards, tableId);
        UNIT_ASSERT_VALUES_EQUAL(tableState, "");
    }

    Y_UNIT_TEST(PipelinedUncommittedWriteCommitWithoutWriteSeqNum) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false);
        serverSettings.AppConfig->MutableFeatureFlags()->SetEnableDataShardPipelinedUncommittedWrites(true);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TShardedTableOptions opts;
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const auto& columns = opts.Columns_;
        const ui64 shard = shards.at(0);

        const ui64 lockTxId = 1234567890001;
        const ui64 lockNodeId = runtime.GetNodeId(0);
        NLongTxService::TLockHandle lockHandle(lockTxId, runtime.GetActorSystem(0));

        NKikimrDataEvents::TLock lock;
        {
            auto result = UncommittedWrite(runtime, sender, shard, tableId, columns, lockTxId, lockNodeId, 1, 11, 0, 1);
            lock = result.GetTxLocks().at(0);
        }

        // An empty echo must not pass validation either.
        NKikimrDataEvents::TLock lockWithoutWriteSeqNum = lock;
        lockWithoutWriteSeqNum.ClearWriteSeqNums();
        CommitLock(runtime, sender, shard, lockWithoutWriteSeqNum, NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN);

        auto tableState = ReadTable(server, shards, tableId);
        UNIT_ASSERT_VALUES_EQUAL(tableState, "");
    }

    Y_UNIT_TEST(PipelinedUncommittedWriteAlreadyApplied) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false);
        serverSettings.AppConfig->MutableFeatureFlags()->SetEnableDataShardPipelinedUncommittedWrites(true);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TShardedTableOptions opts;
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const auto& columns = opts.Columns_;
        const ui64 shard = shards.at(0);

        const ui64 lockTxId = 1234567890001;
        const ui64 lockNodeId = runtime.GetNodeId(0);
        NLongTxService::TLockHandle lockHandle(lockTxId, runtime.GetActorSystem(0));

        UncommittedWrite(runtime, sender, shard, tableId, columns, lockTxId, lockNodeId, 1, 11, 0, 1);

        NKikimrDataEvents::TLock lock;
        NKikimrQueryStats::TTableAccessStats access;
        {
            auto result = UncommittedWrite(runtime, sender, shard, tableId, columns, lockTxId, lockNodeId, 2, 22, 0, 2);
            lock = result.GetTxLocks().at(0);
            UNIT_ASSERT_VALUES_EQUAL(WriteSeqNumOf(lock).GetWriteSeqNum(), 2u);

            UNIT_ASSERT_VALUES_EQUAL(result.GetTxStats().TableAccessStatsSize(), 1u);
            access = result.GetTxStats().GetTableAccessStats(0);
            UNIT_ASSERT_VALUES_EQUAL(access.GetUpdateRow().GetRows(), 1u);
        }

        {
            // A duplicate delivery of the same flush must not re-apply it, but must report
            // it the same way, statistics included: KQP never saw the first reply.
            auto result = UncommittedWrite(runtime, sender, shard, tableId, columns, lockTxId, lockNodeId, 2, 22, 0, 2,
                NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
            UNIT_ASSERT(result.GetIsDuplicate());
            UNIT_ASSERT_VALUES_EQUAL(result.GetTxLocks().size(), 1u);
            const auto& echoed = result.GetTxLocks().at(0);
            UNIT_ASSERT_VALUES_EQUAL(WriteSeqNumOf(echoed).GetWriterIndex(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(WriteSeqNumOf(echoed).GetWriteSeqNum(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(echoed.GetGeneration(), lock.GetGeneration());
            UNIT_ASSERT_VALUES_EQUAL(echoed.GetCounter(), lock.GetCounter());

            UNIT_ASSERT_VALUES_EQUAL(result.GetTxStats().TableAccessStatsSize(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(result.GetTxStats().GetTableAccessStats(0).DebugString(),
                access.DebugString());
        }

        {
            // An older write's result is not remembered, so its duplicate is an error
            auto result = UncommittedWrite(runtime, sender, shard, tableId, columns, lockTxId, lockNodeId, 1, 11, 0, 1,
                NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
            UNIT_ASSERT_C(result.GetIssues(0).message().Contains("already applied"), result.GetIssues(0).message());
            UNIT_ASSERT_C(result.GetIssues(0).message().Contains("writer is at 2"), result.GetIssues(0).message());
        }

        CommitLock(runtime, sender, shard, lock);

        auto tableState = ReadTable(server, shards, tableId);
        UNIT_ASSERT_VALUES_EQUAL(tableState, "key = 1, value = 11\nkey = 2, value = 22\n");
    }

    // A multi-operation write (WriteSeqNums [1, 2]) must be detected as a duplicate
    // when resent, not rejected as STATUS_BAD_REQUEST on the first operation.
    Y_UNIT_TEST(PipelinedUncommittedWriteAlreadyAppliedMultiOperation) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false);
        serverSettings.AppConfig->MutableFeatureFlags()->SetEnableDataShardPipelinedUncommittedWrites(true);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TShardedTableOptions opts;
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const auto& columns = opts.Columns_;
        const ui64 shard = shards.at(0);

        const ui64 lockTxId = 1234567890001;
        const ui64 lockNodeId = runtime.GetNodeId(0);
        NLongTxService::TLockHandle lockHandle(lockTxId, runtime.GetActorSystem(0));

        NKikimrDataEvents::TLock lock;
        NKikimrQueryStats::TTableAccessStats access;
        {
            TVector<TUncommittedWriteOp> ops{{1, 11, 1}, {2, 22, 2}};
            auto result = UncommittedWrite(runtime, sender, shard, tableId, columns,
                lockTxId, lockNodeId, 0, ops);
            UNIT_ASSERT_VALUES_EQUAL(result.GetTxLocks().size(), 1u);
            lock = result.GetTxLocks().at(0);
            UNIT_ASSERT_VALUES_EQUAL(WriteSeqNumOf(lock).GetWriteSeqNum(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(result.GetTxStats().TableAccessStatsSize(), 1u);
            access = result.GetTxStats().GetTableAccessStats(0);
            UNIT_ASSERT_VALUES_EQUAL(access.GetUpdateRow().GetRows(), 2u);
        }

        {
            // A duplicate delivery of the same multi-operation batch must be detected
            // as a duplicate, not rejected as STATUS_BAD_REQUEST.
            TVector<TUncommittedWriteOp> ops{{1, 11, 1}, {2, 22, 2}};
            auto result = UncommittedWrite(runtime, sender, shard, tableId, columns,
                lockTxId, lockNodeId, 0, ops,
                NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
            UNIT_ASSERT(result.GetIsDuplicate());
            UNIT_ASSERT_VALUES_EQUAL(result.GetTxLocks().size(), 1u);
            const auto& echoed = result.GetTxLocks().at(0);
            UNIT_ASSERT_VALUES_EQUAL(WriteSeqNumOf(echoed).GetWriterIndex(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(WriteSeqNumOf(echoed).GetWriteSeqNum(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(echoed.GetGeneration(), lock.GetGeneration());
            UNIT_ASSERT_VALUES_EQUAL(echoed.GetCounter(), lock.GetCounter());
            UNIT_ASSERT_VALUES_EQUAL(result.GetTxStats().TableAccessStatsSize(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(result.GetTxStats().GetTableAccessStats(0).DebugString(),
                access.DebugString());
        }

        {
            // A stale single-operation write (seq num 1 < current 2) is still an error
            auto result = UncommittedWrite(runtime, sender, shard, tableId, columns,
                lockTxId, lockNodeId, 1, 11, 0, 1,
                NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
            UNIT_ASSERT_C(result.GetIssues(0).message().Contains("already applied"),
                result.GetIssues(0).message());
            UNIT_ASSERT_C(result.GetIssues(0).message().Contains("writer is at 2"),
                result.GetIssues(0).message());
        }

        {
            // A gap within one EvWrite: seq nums must be ascending and contiguous.
            TVector<TUncommittedWriteOp> ops{{3, 33, 3}, {4, 44, 5}};
            auto result = UncommittedWrite(runtime, sender, shard, tableId, columns,
                lockTxId, lockNodeId, 0, ops,
                NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
            UNIT_ASSERT_C(result.GetIssues(0).message().Contains("ascending and contiguous"),
                result.GetIssues(0).message());
        }

        {
            // Out-of-order seq nums within one batch are a protocol error:
            // KQP allocates them sequentially, so descending is never valid.
            TVector<TUncommittedWriteOp> ops{{4, 44, 4}, {3, 33, 3}};
            auto result = UncommittedWrite(runtime, sender, shard, tableId, columns,
                lockTxId, lockNodeId, 0, ops,
                NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
            UNIT_ASSERT_C(result.GetIssues(0).message().Contains("ascending"),
                result.GetIssues(0).message());
        }

        {
            // A gap between EvWrites: the next batch must continue the chain
            // exactly at current + 1. current == 2, so batch must start at 3.
            TVector<TUncommittedWriteOp> ops{{4, 44, 4}, {5, 55, 5}};
            auto result = UncommittedWrite(runtime, sender, shard, tableId, columns,
                lockTxId, lockNodeId, 0, ops,
                NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
            UNIT_ASSERT_C(result.GetIssues(0).message().Contains("must continue the writer chain"),
                result.GetIssues(0).message());
            UNIT_ASSERT_C(result.GetIssues(0).message().Contains("at 3"),
                result.GetIssues(0).message());
        }

        {
            // Partial overlap: the batch starts at current instead of current + 1.
            // current == 2, batch [2, 3] — must start at 3, not 2.
            TVector<TUncommittedWriteOp> ops{{2, 222, 2}, {3, 33, 3}};
            auto result = UncommittedWrite(runtime, sender, shard, tableId, columns,
                lockTxId, lockNodeId, 0, ops,
                NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
            UNIT_ASSERT_C(result.GetIssues(0).message().Contains("must continue the writer chain"),
                result.GetIssues(0).message());
            UNIT_ASSERT_C(result.GetIssues(0).message().Contains("at 3"),
                result.GetIssues(0).message());
        }

        CommitLock(runtime, sender, shard, lock);

        auto tableState = ReadTable(server, shards, tableId);
        UNIT_ASSERT_VALUES_EQUAL(tableState, "key = 1, value = 11\nkey = 2, value = 22\n");
    }

    // A write that applies no rows still takes a position in the chain
    Y_UNIT_TEST(PipelinedUncommittedWriteAlreadyAppliedMissingRow) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false);
        serverSettings.AppConfig->MutableFeatureFlags()->SetEnableDataShardPipelinedUncommittedWrites(true);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TShardedTableOptions opts;
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const auto& columns = opts.Columns_;
        const ui64 shard = shards.at(0);

        const ui64 lockTxId = 1234567890001;
        const ui64 lockNodeId = runtime.GetNodeId(0);
        NLongTxService::TLockHandle lockHandle(lockTxId, runtime.GetActorSystem(0));

        auto updateMissingRow = [&](NKikimrDataEvents::TEvWriteResult::EStatus expected) {
            auto req = MakeWriteRequestOneKeyValue(std::nullopt,
                NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE,
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE,
                tableId, columns, 1, 11);
            req->SetLockId(lockTxId, lockNodeId);
            req->Record.MutableOperations(0)->MutableWriteSeqNum()->SetWriteSeqNum(1);
            return Write(runtime, sender, shard, std::move(req), expected);
        };

        NKikimrQueryStats::TTableAccessStats access;
        {
            auto result = updateMissingRow(NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
            const auto& lock = result.GetTxLocks().at(0);
            UNIT_ASSERT(!lock.GetHasWrites());
            UNIT_ASSERT_VALUES_EQUAL(WriteSeqNumOf(lock).GetWriteSeqNum(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(result.GetTxStats().TableAccessStatsSize(), 1u);
            access = result.GetTxStats().GetTableAccessStats(0);
        }

        {
            auto result = updateMissingRow(NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
            UNIT_ASSERT(result.GetIsDuplicate());
            UNIT_ASSERT_VALUES_EQUAL(result.GetTxLocks().size(), 1u);
            const auto& echoed = result.GetTxLocks().at(0);
            UNIT_ASSERT(!echoed.GetHasWrites());
            UNIT_ASSERT_VALUES_EQUAL(echoed.GetSchemeShard(), tableId.PathId.OwnerId);
            UNIT_ASSERT_VALUES_EQUAL(echoed.GetPathId(), tableId.PathId.LocalPathId);
            UNIT_ASSERT_VALUES_EQUAL(WriteSeqNumOf(echoed).GetWriteSeqNum(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(result.GetTxStats().TableAccessStatsSize(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(result.GetTxStats().GetTableAccessStats(0).DebugString(),
                access.DebugString());
        }
    }

    Y_UNIT_TEST(PipelinedUncommittedWriteRepliesBeforeCommit) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false);
        serverSettings.AppConfig->MutableFeatureFlags()->SetEnableDataShardPipelinedUncommittedWrites(true);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TShardedTableOptions opts;
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const auto& columns = opts.Columns_;
        const ui64 shard = shards.at(0);
        const ui64 lockNodeId = runtime.GetNodeId(0);

        TBlockEvents<TEvTablet::TEvCommit> blockedCommits(runtime,
            [&](const TEvTablet::TEvCommit::TPtr& ev) { return ev->Get()->TabletID == shard; });

        const ui64 lockTxId1 = 1234567890001;
        NLongTxService::TLockHandle lockHandle1(lockTxId1, runtime.GetActorSystem(0));
        {
            auto result = UncommittedWrite(runtime, sender, shard, tableId, columns, lockTxId1, lockNodeId, 1, 11, 0, 1);
            UNIT_ASSERT_VALUES_EQUAL(result.GetTxLocks().size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(WriteSeqNumOf(result.GetTxLocks().at(0)).GetWriteSeqNum(), 1u);
        }

        // The reply above was released before the executor even issued the commit
        runtime.WaitFor("pipelined write commit", [&]{ return blockedCommits.size() >= 1; });

        // Legacy (no WriteSeqNum) must still wait for persistence.
        const ui64 lockTxId2 = 1234567890002;
        NLongTxService::TLockHandle lockHandle2(lockTxId2, runtime.GetActorSystem(0));
        auto legacySender = runtime.AllocateEdgeActor();
        {
            auto req = MakeWriteRequestOneKeyValue(std::nullopt,
                NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE,
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                tableId, columns, 2, 22);
            req->SetLockId(lockTxId2, lockNodeId);
            runtime.SendToPipe(shard, legacySender, req.release());
        }

        {
            auto ev = runtime.GrabEdgeEvent<NEvents::TDataEvents::TEvWriteResult>(legacySender, TDuration::MilliSeconds(200));
            UNIT_ASSERT_C(!ev, "Legacy uncommitted write must not reply while its commit is blocked");
        }

        blockedCommits.Unblock();
        blockedCommits.Stop();

        {
            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvWriteResult>(legacySender);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        }
    }

    // Drops write 2's tablet commit and restarts, losing an acknowledged write.
    // Returns the lock KQP believes it holds.
    NKikimrDataEvents::TLock LoseUncommittedWrite(
            TTestActorRuntime& runtime, const TActorId& sender, ui64 shard,
            const TTableId& tableId, const TVector<TShardedTableOptions::TColumn>& columns,
            ui64 lockTxId, ui64 lockNodeId)
    {
        TDisableDataShardLogBatching disableDataShardLogBatching;

        NKikimrDataEvents::TLock lock;
        {
            TBlockEvents<TEvTablet::TEvCommit> blockedCommits(runtime,
                [&](const TEvTablet::TEvCommit::TPtr& ev) { return ev->Get()->TabletID == shard; });

            // Write 1 persists normally
            UncommittedWrite(runtime, sender, shard, tableId, columns, lockTxId, lockNodeId, 1, 11, 0, 1);
            runtime.WaitFor("write 1 commit", [&]{ return blockedCommits.size() >= 1; });
            blockedCommits.Unblock();

            // Write 2 is acknowledged early, before its commit is durable
            auto result = UncommittedWrite(runtime, sender, shard, tableId, columns, lockTxId, lockNodeId, 2, 22, 0, 2);
            UNIT_ASSERT_VALUES_EQUAL(result.GetTxLocks().size(), 1u);
            lock = result.GetTxLocks().at(0);
            UNIT_ASSERT_VALUES_EQUAL(WriteSeqNumOf(lock).GetWriteSeqNum(), 2u);

            // The reply was released before the executor even issued the commit
            runtime.WaitFor("write 2 commit", [&]{ return blockedCommits.size() >= 1; });

            // Stop intercepting without delivering: write 2 is lost for good.
            blockedCommits.Stop();
        }

        RebootTablet(runtime, shard, sender);

        return lock;
    }

    Y_UNIT_TEST(PipelinedUncommittedWriteLostAfterRestart) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false);
        serverSettings.AppConfig->MutableFeatureFlags()->SetEnableDataShardPipelinedUncommittedWrites(true);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TShardedTableOptions opts;
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const auto& columns = opts.Columns_;
        const ui64 shard = shards.at(0);

        const ui64 lockTxId = 1234567890001;
        const ui64 lockNodeId = runtime.GetNodeId(0);
        NLongTxService::TLockHandle lockHandle(lockTxId, runtime.GetActorSystem(0));

        auto staleLock = LoseUncommittedWrite(runtime, sender, shard, tableId, columns, lockTxId, lockNodeId);

        // The chain is short of what KQP reports: the transaction aborts.
        CommitLock(runtime, sender, shard, staleLock, NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN);

        auto tableState = ReadTable(server, shards, tableId);
        UNIT_ASSERT_VALUES_EQUAL(tableState, "");
    }

    Y_UNIT_TEST(PipelinedUncommittedWriteRestoredFromStorage) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false);
        serverSettings.AppConfig->MutableFeatureFlags()->SetEnableDataShardPipelinedUncommittedWrites(true);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TShardedTableOptions opts;
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const auto& columns = opts.Columns_;
        const ui64 shard = shards.at(0);

        const ui64 lockTxId = 1234567890001;
        const ui64 lockNodeId = runtime.GetNodeId(0);
        NLongTxService::TLockHandle lockHandle(lockTxId, runtime.GetActorSystem(0));

        LoseUncommittedWrite(runtime, sender, shard, tableId, columns, lockTxId, lockNodeId);

        // Restored from storage as 1, not from in-memory migration, so 2 is free again.
        NKikimrDataEvents::TLock lock;
        {
            auto result = UncommittedWrite(runtime, sender, shard, tableId, columns, lockTxId, lockNodeId, 2, 222, 0, 2);
            UNIT_ASSERT_VALUES_EQUAL(result.GetTxLocks().size(), 1u);
            lock = result.GetTxLocks().at(0);
            UNIT_ASSERT_VALUES_EQUAL(WriteSeqNumOf(lock).GetWriteSeqNum(), 2u);
        }

        CommitLock(runtime, sender, shard, lock);

        auto tableState = ReadTable(server, shards, tableId);
        UNIT_ASSERT_VALUES_EQUAL(tableState, "key = 1, value = 11\nkey = 2, value = 222\n");
    }

    Y_UNIT_TEST(PipelinedUncommittedWriteAlreadyAppliedAfterStateMigration) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false);
        serverSettings.AppConfig->MutableFeatureFlags()->SetEnableDataShardPipelinedUncommittedWrites(true);
        serverSettings.FeatureFlags.SetEnableDataShardInMemoryStateMigration(true);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TShardedTableOptions opts;
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const auto& columns = opts.Columns_;
        const ui64 shard = shards.at(0);

        const ui64 lockTxId = 1234567890001;
        const ui64 lockNodeId = runtime.GetNodeId(0);
        NLongTxService::TLockHandle lockHandle(lockTxId, runtime.GetActorSystem(0));

        // Wait for write 1's commit to become durable before rebooting.
        NKikimrDataEvents::TLock lock;
        {
            TDisableDataShardLogBatching disableDataShardLogBatching;
            TBlockEvents<TEvTablet::TEvCommit> blockedCommits(runtime,
                [&](const TEvTablet::TEvCommit::TPtr& ev) { return ev->Get()->TabletID == shard; });

            auto result = UncommittedWrite(runtime, sender, shard, tableId, columns, lockTxId, lockNodeId, 1, 11, 0, 1);
            lock = result.GetTxLocks().at(0);

            runtime.WaitFor("write 1 commit", [&]{ return blockedCommits.size() >= 1; });
            blockedCommits.Unblock();
        }

        // The in-memory state actor transfers the lock state, including the serialized result.
        RebootTablet(runtime, shard, sender);

        // A duplicate of write 1 must report the unchanged lock and the replayed result.
        {
            auto result = UncommittedWrite(runtime, sender, shard, tableId, columns, lockTxId, lockNodeId, 1, 11, 0, 1,
                NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
            UNIT_ASSERT(result.GetIsDuplicate());
            UNIT_ASSERT_VALUES_EQUAL(result.GetTxLocks().size(), 1u);
            const auto& echoed = result.GetTxLocks().at(0);
            UNIT_ASSERT_VALUES_EQUAL(WriteSeqNumOf(echoed).GetWriterIndex(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(WriteSeqNumOf(echoed).GetWriteSeqNum(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(echoed.GetGeneration(), lock.GetGeneration());
            UNIT_ASSERT_VALUES_EQUAL(echoed.GetCounter(), lock.GetCounter());
            UNIT_ASSERT_VALUES_EQUAL(echoed.GetSchemeShard(), tableId.PathId.OwnerId);
            UNIT_ASSERT_VALUES_EQUAL(echoed.GetPathId(), tableId.PathId.LocalPathId);
            // The stored result was replayed via in-memory state migration
            UNIT_ASSERT(result.GetTxStats().TableAccessStatsSize() > 0);
        }

        CommitLock(runtime, sender, shard, lock);

        auto tableState = ReadTable(server, shards, tableId);
        UNIT_ASSERT_VALUES_EQUAL(tableState, "key = 1, value = 11\n");
    }

    // A duplicate arriving after a restart without in-memory state migration:
    // the lock's WriteSeqNum is restored from storage, but the serialized result
    // is gone, so the duplicate is answered with a bare STATUS_COMPLETED (no stats).
    Y_UNIT_TEST(PipelinedUncommittedWriteDuplicateAfterRestartNoResult) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false);
        serverSettings.AppConfig->MutableFeatureFlags()->SetEnableDataShardPipelinedUncommittedWrites(true);
        serverSettings.FeatureFlags.SetEnableDataShardInMemoryStateMigration(false);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TShardedTableOptions opts;
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const auto& columns = opts.Columns_;
        const ui64 shard = shards.at(0);

        const ui64 lockTxId = 1234567890001;
        const ui64 lockNodeId = runtime.GetNodeId(0);
        NLongTxService::TLockHandle lockHandle(lockTxId, runtime.GetActorSystem(0));

        // Wait for write 1's commit to become durable before rebooting.
        NKikimrDataEvents::TLock lock;
        {
            TDisableDataShardLogBatching disableDataShardLogBatching;
            TBlockEvents<TEvTablet::TEvCommit> blockedCommits(runtime,
                [&](const TEvTablet::TEvCommit::TPtr& ev) { return ev->Get()->TabletID == shard; });

            auto result = UncommittedWrite(runtime, sender, shard, tableId, columns, lockTxId, lockNodeId, 1, 11, 0, 1);
            lock = result.GetTxLocks().at(0);

            runtime.WaitFor("write 1 commit", [&]{ return blockedCommits.size() >= 1; });
            blockedCommits.Unblock();
        }

        // No in-memory state migration: the lock row is restored from storage,
        // but the serialized result blob is lost.
        RebootTablet(runtime, shard, sender);

        // A duplicate of write 1 is still detected (lock has WriteSeqNum=1 from storage),
        // but without the stored result it degrades to a bare STATUS_COMPLETED.
        {
            auto result = UncommittedWrite(runtime, sender, shard, tableId, columns, lockTxId, lockNodeId, 1, 11, 0, 1,
                NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
            UNIT_ASSERT(result.GetIsDuplicate());
            UNIT_ASSERT_VALUES_EQUAL(result.GetTxLocks().size(), 1u);
            const auto& echoed = result.GetTxLocks().at(0);
            UNIT_ASSERT_VALUES_EQUAL(WriteSeqNumOf(echoed).GetWriteSeqNum(), 1u);
            // No stored result to replay: stats are absent
            UNIT_ASSERT_VALUES_EQUAL(result.GetTxStats().TableAccessStatsSize(), 0u);
        }

        CommitLock(runtime, sender, shard, lock);

        auto tableState = ReadTable(server, shards, tableId);
        UNIT_ASSERT_VALUES_EQUAL(tableState, "key = 1, value = 11\n");
    }

    // A multi-operation duplicate arriving after in-memory state migration:
    // the serialized result for the whole batch must be replayed, not just one op.
    Y_UNIT_TEST(PipelinedUncommittedWriteMultiOpDuplicateAfterStateMigration) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false);
        serverSettings.AppConfig->MutableFeatureFlags()->SetEnableDataShardPipelinedUncommittedWrites(true);
        serverSettings.FeatureFlags.SetEnableDataShardInMemoryStateMigration(true);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TShardedTableOptions opts;
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const auto& columns = opts.Columns_;
        const ui64 shard = shards.at(0);

        const ui64 lockTxId = 1234567890001;
        const ui64 lockNodeId = runtime.GetNodeId(0);
        NLongTxService::TLockHandle lockHandle(lockTxId, runtime.GetActorSystem(0));

        // Wait for the multi-op write's commit to become durable before rebooting.
        NKikimrDataEvents::TLock lock;
        NKikimrQueryStats::TTableAccessStats access;
        {
            TDisableDataShardLogBatching disableDataShardLogBatching;
            TBlockEvents<TEvTablet::TEvCommit> blockedCommits(runtime,
                [&](const TEvTablet::TEvCommit::TPtr& ev) { return ev->Get()->TabletID == shard; });

            TVector<TUncommittedWriteOp> ops{{1, 11, 1}, {2, 22, 2}};
            auto result = UncommittedWrite(runtime, sender, shard, tableId, columns,
                lockTxId, lockNodeId, 0, ops);
            lock = result.GetTxLocks().at(0);
            access = result.GetTxStats().GetTableAccessStats(0);

            runtime.WaitFor("multi-op write commit", [&]{ return blockedCommits.size() >= 1; });
            blockedCommits.Unblock();
        }

        // The in-memory state actor transfers the lock state, including the serialized result.
        RebootTablet(runtime, shard, sender);

        // A duplicate of the multi-op batch must replay the stored result with both rows' stats.
        {
            TVector<TUncommittedWriteOp> ops{{1, 11, 1}, {2, 22, 2}};
            auto result = UncommittedWrite(runtime, sender, shard, tableId, columns,
                lockTxId, lockNodeId, 0, ops,
                NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
            UNIT_ASSERT(result.GetIsDuplicate());
            UNIT_ASSERT_VALUES_EQUAL(result.GetTxLocks().size(), 1u);
            const auto& echoed = result.GetTxLocks().at(0);
            UNIT_ASSERT_VALUES_EQUAL(WriteSeqNumOf(echoed).GetWriteSeqNum(), 2u);
            // The stored result was replayed via in-memory state migration
            UNIT_ASSERT_VALUES_EQUAL(result.GetTxStats().TableAccessStatsSize(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(result.GetTxStats().GetTableAccessStats(0).DebugString(),
                access.DebugString());
        }

        CommitLock(runtime, sender, shard, lock);

        auto tableState = ReadTable(server, shards, tableId);
        UNIT_ASSERT_VALUES_EQUAL(tableState, "key = 1, value = 11\nkey = 2, value = 22\n");
    }

    Y_UNIT_TEST(PipelinedUncommittedWriteBreaksOwnLock) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false);
        serverSettings.AppConfig->MutableFeatureFlags()->SetEnableDataShardPipelinedUncommittedWrites(true);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TShardedTableOptions opts;
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const auto& columns = opts.Columns_;
        const ui64 shard = shards.at(0);
        const ui64 lockNodeId = runtime.GetNodeId(0);

        // Take a snapshot, then commit a newer version of key 1 behind it
        const auto snapshot = CreateVolatileSnapshot(server, {"/Root/table-1"});
        {
            auto req = MakeWriteRequestOneKeyValue(std::nullopt,
                NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE,
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                tableId, columns, 1, 11);
            Write(runtime, sender, shard, std::move(req));
        }

        // Reading the old snapshot skips the newer row as invisible, breaking its own lock.
        const ui64 lockTxId = 1234567890001;
        NLongTxService::TLockHandle lockHandle(lockTxId, runtime.GetActorSystem(0));
        auto req = MakeWriteRequestOneKeyValue(std::nullopt,
            NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE,
            NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT,
            tableId, columns, 1, 22);
        req->SetLockId(lockTxId, lockNodeId);
        req->Record.MutableMvccSnapshot()->SetStep(snapshot.Step);
        req->Record.MutableMvccSnapshot()->SetTxId(snapshot.TxId);
        auto* writeSeqNum = req->Record.MutableOperations(0)->MutableWriteSeqNum();
        writeSeqNum->SetWriterIndex(0);
        writeSeqNum->SetWriteSeqNum(1);
        auto result = Write(runtime, sender, shard, std::move(req));

        UNIT_ASSERT_VALUES_EQUAL(result.GetTxLocks().size(), 1u);
        const auto& lock = result.GetTxLocks().at(0);
        UNIT_ASSERT_C(lock.GetCounter() >= NKikimr::TSysTables::TLocksTable::TLock::ErrorMin,
            "Expected a broken lock, got " << lock.ShortDebugString());
    }

    Y_UNIT_TEST(PipelinedUncommittedWriteBadRequest) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false);
        serverSettings.AppConfig->MutableFeatureFlags()->SetEnableDataShardPipelinedUncommittedWrites(true);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TShardedTableOptions opts;
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const auto& columns = opts.Columns_;
        const ui64 shard = shards.at(0);

        const ui64 lockTxId = 1234567890001;
        const ui64 lockNodeId = runtime.GetNodeId(0);
        NLongTxService::TLockHandle lockHandle(lockTxId, runtime.GetActorSystem(0));

        // WriteSeqNum without LockTxId
        {
            auto req = MakeWriteRequestOneKeyValue(std::nullopt,
                NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE,
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                tableId, columns, 1, 11);
            req->Record.MutableOperations(0)->MutableWriteSeqNum()->SetWriteSeqNum(1);
            Write(runtime, sender, shard, std::move(req), NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
        }

        // WriteSeqNum with a prepare mode: an uncommitted write has to be immediate
        for (auto txMode : {NKikimrDataEvents::TEvWrite::MODE_PREPARE,
                            NKikimrDataEvents::TEvWrite::MODE_VOLATILE_PREPARE})
        {
            auto req = MakeWriteRequestOneKeyValue(1234567890011, txMode,
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                tableId, columns, 1, 11);
            req->SetLockId(lockTxId, lockNodeId);
            req->Record.MutableOperations(0)->MutableWriteSeqNum()->SetWriteSeqNum(1);
            Write(runtime, sender, shard, std::move(req), NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
        }

        // WriteSeqNum with Locks{Op=Commit}
        {
            auto req = MakeWriteRequestOneKeyValue(std::nullopt,
                NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE,
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                tableId, columns, 1, 11);
            req->SetLockId(lockTxId, lockNodeId);
            req->Record.MutableOperations(0)->MutableWriteSeqNum()->SetWriteSeqNum(1);
            req->Record.MutableLocks()->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
            Write(runtime, sender, shard, std::move(req), NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
        }

        // WriteSeqNum with zero operations
        {
            auto req = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
            req->SetLockId(lockTxId, lockNodeId);
            req->Record.AddOperations()->MutableWriteSeqNum()->SetWriteSeqNum(1);
            Write(runtime, sender, shard, std::move(req), NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
        }

        // A writer without a position in its chain
        {
            auto req = MakeWriteRequestOneKeyValue(std::nullopt,
                NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE,
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                tableId, columns, 1, 11);
            req->SetLockId(lockTxId, lockNodeId);
            req->Record.MutableOperations(0)->MutableWriteSeqNum()->SetWriterIndex(1);
            Write(runtime, sender, shard, std::move(req), NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
        }
    }

    // A batch where some operations have WriteSeqNum and others don't must be rejected.
    Y_UNIT_TEST(PipelinedUncommittedWriteMixedBatchRejected) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false);
        serverSettings.AppConfig->MutableFeatureFlags()->SetEnableDataShardPipelinedUncommittedWrites(true);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TShardedTableOptions opts;
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const auto& columns = opts.Columns_;
        const ui64 shard = shards.at(0);

        const ui64 lockTxId = 1234567890001;
        const ui64 lockNodeId = runtime.GetNodeId(0);
        NLongTxService::TLockHandle lockHandle(lockTxId, runtime.GetActorSystem(0));

        // First op has WriteSeqNum=1, second op has no WriteSeqNum (0) — mixed batch.
        TVector<TUncommittedWriteOp> ops{{1, 11, 1}, {2, 22, 0}};
        UncommittedWrite(runtime, sender, shard, tableId, columns,
            lockTxId, lockNodeId, 0, ops,
            NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
    }

    // A shard that is not pipelining (rolling restart: KQP already sends WriteSeqNum) must still
    // wait for persistence before replying, and still track the chain it is sent.
    Y_UNIT_TEST(PipelinedUncommittedWriteShardNotPipelining) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false);
        serverSettings.AppConfig->MutableFeatureFlags()->SetEnableDataShardPipelinedUncommittedWrites(false);

        auto [runtime, server, sender] = TestCreateServer(serverSettings);

        TShardedTableOptions opts;
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const auto& columns = opts.Columns_;
        const ui64 shard = shards.at(0);

        const ui64 lockTxId = 1234567890001;
        const ui64 lockNodeId = runtime.GetNodeId(0);
        NLongTxService::TLockHandle lockHandle(lockTxId, runtime.GetActorSystem(0));

        TBlockEvents<TEvTablet::TEvCommit> blockedCommits(runtime,
            [&](const TEvTablet::TEvCommit::TPtr& ev) { return ev->Get()->TabletID == shard; });

        auto writeSender = runtime.AllocateEdgeActor();
        {
            auto req = MakeWriteRequestOneKeyValue(std::nullopt,
                NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE,
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                tableId, columns, 1, 11);
            req->SetLockId(lockTxId, lockNodeId);
            req->Record.MutableOperations(0)->MutableWriteSeqNum()->SetWriteSeqNum(1);
            runtime.SendToPipe(shard, writeSender, req.release());
        }

        {
            // With the flag off, the shard must still wait for persistence before replying
            auto ev = runtime.GrabEdgeEvent<NEvents::TDataEvents::TEvWriteResult>(writeSender, TDuration::MilliSeconds(200));
            UNIT_ASSERT_C(!ev, "With the flag off, the pipelined write must still wait for persistence");
        }

        blockedCommits.Unblock();
        blockedCommits.Stop();

        {
            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvWriteResult>(writeSender);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetTxLocks().size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(WriteSeqNumOf(ev->Get()->Record.GetTxLocks().at(0)).GetWriteSeqNum(), 1u);
        }

        // The chain is still tracked even with the flag off
        {
            auto result = UncommittedWrite(runtime, sender, shard, tableId, columns, lockTxId, lockNodeId, 2, 22, 0, 2);
            UNIT_ASSERT_VALUES_EQUAL(WriteSeqNumOf(result.GetTxLocks().at(0)).GetWriteSeqNum(), 2u);
        }
        UncommittedWrite(runtime, sender, shard, tableId, columns, lockTxId, lockNodeId, 3, 33, 0, 1,
            NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
    }

} // Y_UNIT_TEST_SUITE(DataShardWrite)
} // namespace NKikimr

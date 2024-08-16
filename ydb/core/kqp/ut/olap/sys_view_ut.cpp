#include "helpers/local.h"
#include "helpers/query_executor.h"
#include "helpers/typed_local.h"
#include "helpers/writer.h"
#include "helpers/get_value.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpOlapSysView) {
    Y_UNIT_TEST(StatsSysView) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        TLocalHelper(kikimr).CreateTestOlapTable();
        for (ui64 i = 0; i < 100; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000 + i * 10000, 1000);
        }
        csController->WaitCompactions(TDuration::Seconds(10));

        auto tableClient = kikimr.GetTableClient();
        auto selectQuery = TString(R"(
            SELECT PathId, Kind, TabletId, Sum(Rows) as Rows
            FROM `/Root/olapStore/.sys/store_primary_index_portion_stats`
            GROUP BY PathId, Kind, TabletId
            ORDER BY TabletId, Kind, PathId
        )");

        auto rows = ExecuteScanQuery(tableClient, selectQuery);

        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), 3ull);
        UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[1].at("PathId")), 3ull);
        UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[2].at("PathId")), 3ull);
        UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[0].at("Kind")), "INSERTED");
        UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[1].at("Kind")), "INSERTED");
        UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[2].at("Kind")), "INSERTED");
        UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("TabletId")), 72075186224037888ull);
        UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[1].at("TabletId")), 72075186224037889ull);
        UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[2].at("TabletId")), 72075186224037890ull);
        UNIT_ASSERT_VALUES_EQUAL(
            GetUint64(rows[0].at("Rows")) + GetUint64(rows[1].at("Rows")) + GetUint64(rows[2].at("Rows")),
            100 * 1000); // >= 90% of 100K inserted rows
    }

    Y_UNIT_TEST(StatsSysViewTable) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();

        TLocalHelper(kikimr).CreateTestOlapTable("olapTable_1");
        TLocalHelper(kikimr).CreateTestOlapTable("olapTable_2");
        for (ui64 i = 0; i < 10; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable_1", 0, 1000000 + i * 10000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable_2", 0, 1000000 + i * 10000, 2000);
        }
        csController->WaitCompactions(TDuration::Seconds(10));

        auto tableClient = kikimr.GetTableClient();
        {
            auto selectQuery = TString(R"(
                SELECT PathId, Kind, TabletId
                FROM `/Root/olapStore/olapTable_1/.sys/primary_index_stats`
                GROUP BY PathId, TabletId, Kind
                ORDER BY PathId, TabletId, Kind
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows.front().at("PathId")), 3ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows.back().at("PathId")), 3ull);
        }
        {
            auto selectQuery = TString(R"(
                SELECT PathId, Kind, TabletId
                FROM `/Root/olapStore/olapTable_2/.sys/primary_index_stats`
                GROUP BY PathId, TabletId, Kind
                ORDER BY PathId, TabletId, Kind
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows.front().at("PathId")), 4ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows.back().at("PathId")), 4ull);
        }
        {
            auto selectQuery = TString(R"(
                SELECT *
                FROM `/Root/olapStore/olapTable_1/.sys/primary_index_stats`
                WHERE
                    PathId > UInt64("3")
                ORDER BY PathId, Kind, TabletId
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 0);
        }
    }

    Y_UNIT_TEST(StatsSysViewEnumStringBytes) {
        ui64 rawBytesPK1;
        ui64 bytesPK1;
        {
            auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
            auto settings = TKikimrSettings()
                .SetWithSampleTables(false);
            TKikimrRunner kikimr(settings);
            Tests::NCommon::TLoggerInit(kikimr).Initialize();
            TTypedLocalHelper helper("", kikimr, "olapTable", "olapStore12");
            helper.CreateTestOlapTable();
            helper.FillPKOnly(0, 800000);
            csController->WaitCompactions(TDuration::Seconds(10));
            helper.GetVolumes(rawBytesPK1, bytesPK1, false);
        }

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        ui64 rawBytesUnpack1PK = 0;
        ui64 bytesUnpack1PK = 0;
        ui64 rawBytesPackAndUnpack2PK;
        ui64 bytesPackAndUnpack2PK;
        const ui32 rowsCount = 800000;
        const ui32 groupsCount = 512;
        {
            auto settings = TKikimrSettings()
                .SetWithSampleTables(false);
            TKikimrRunner kikimr(settings);
            Tests::NCommon::TLoggerInit(kikimr).Initialize();
            TTypedLocalHelper helper("Utf8", kikimr);
            helper.CreateTestOlapTable();
            NArrow::NConstruction::TStringPoolFiller sPool(groupsCount, 52);
            helper.FillTable(sPool, 0, rowsCount);
            csController->WaitCompactions(TDuration::Seconds(10));
            helper.PrintCount();
            {
                auto d = helper.GetDistribution();
                Y_ABORT_UNLESS(d.GetCount() == rowsCount);
                Y_ABORT_UNLESS(d.GetGroupsCount() == groupsCount);
                Y_ABORT_UNLESS(d.GetMaxCount() - d.GetMinCount() <= 1);
            }
            helper.GetVolumes(rawBytesUnpack1PK, bytesUnpack1PK, false);
            auto tableClient = kikimr.GetTableClient();
            helper.ExecuteSchemeQuery(TStringBuilder() << "ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field, `ENCODING.DICTIONARY.ENABLED`=`true`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field1, `ENCODING.DICTIONARY.ENABLED`=`true`);", NYdb::EStatus::SCHEME_ERROR);
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field, `ENCODING.DICTIONARY.ENABLED1`=`true`);", NYdb::EStatus::GENERIC_ERROR);
            helper.FillTable(sPool, 1, rowsCount);
            csController->WaitCompactions(TDuration::Seconds(10));
            {
                helper.GetVolumes(rawBytesPackAndUnpack2PK, bytesPackAndUnpack2PK, false);
                helper.PrintCount();
                {
                    auto d = helper.GetDistribution();
                    Cerr << d.DebugString() << Endl;
                    Y_ABORT_UNLESS(d.GetCount() == 2 * rowsCount);
                    Y_ABORT_UNLESS(d.GetGroupsCount() == groupsCount);
                    Y_ABORT_UNLESS(d.GetMaxCount() - d.GetMinCount() <= 2);
                }
            }
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field, `SERIALIZER.CLASS_NAME`=`ARROW_SERIALIZER`, `COMPRESSION.TYPE`=`zstd`);");
            csController->WaitCompactions(TDuration::Seconds(10));
        }
        const ui64 rawBytesUnpack = rawBytesUnpack1PK - rawBytesPK1;
        const ui64 bytesUnpack = bytesUnpack1PK - bytesPK1;
        const ui64 rawBytesPack = rawBytesPackAndUnpack2PK - rawBytesUnpack1PK - rawBytesPK1;
        const ui64 bytesPack = bytesPackAndUnpack2PK - bytesUnpack1PK - bytesPK1;
        TStringBuilder result;
        result << "unpacked data: " << rawBytesUnpack << " / " << bytesUnpack << Endl;
        result << "packed data: " << rawBytesPack << " / " << bytesPack << Endl;
        result << "frq_diff: " << 1.0 * bytesPack / bytesUnpack << Endl;
        result << "frq_compression: " << 1.0 * bytesPack / rawBytesPack << Endl;
        result << "pk_size : " << rawBytesPK1 << " / " << bytesPK1 << Endl;
        Cerr << result << Endl;
        Y_ABORT_UNLESS(bytesPack / bytesUnpack < 0.1);
    }

    Y_UNIT_TEST(StatsSysViewBytesPackActualization) {
        ui64 rawBytesPK1;
        ui64 bytesPK1;
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTypedLocalHelper helper("", kikimr, "olapTable", "olapStore");
        helper.CreateTestOlapTable();
        helper.FillPKOnly(0, 800000);
        csController->WaitCompactions(TDuration::Seconds(10));

        helper.GetVolumes(rawBytesPK1, bytesPK1, false, {"pk_int"});
        auto tableClient = kikimr.GetTableClient();
        {
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=pk_int, `SERIALIZER.CLASS_NAME`=`ARROW_SERIALIZER`, `COMPRESSION.TYPE`=`zstd`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);");
            csController->WaitActualization(TDuration::Seconds(10));
            ui64 rawBytesPK2;
            ui64 bytesPK2;
            helper.GetVolumes(rawBytesPK2, bytesPK2, false, {"pk_int"});
            AFL_VERIFY(rawBytesPK2 == rawBytesPK1)("pk1", rawBytesPK1)("pk2", rawBytesPK2);
            AFL_VERIFY(bytesPK2 < bytesPK1 / 3)("pk1", bytesPK1)("pk2", bytesPK2);
        }
        {
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=pk_int, `SERIALIZER.CLASS_NAME`=`ARROW_SERIALIZER`, `COMPRESSION.TYPE`=`lz4`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);");
            csController->WaitActualization(TDuration::Seconds(10));
            ui64 rawBytesPK2;
            ui64 bytesPK2;
            helper.GetVolumes(rawBytesPK2, bytesPK2, false, {"pk_int"});
            AFL_VERIFY(rawBytesPK2 == rawBytesPK1)("pk1", rawBytesPK1)("pk2", rawBytesPK2);
            AFL_VERIFY(bytesPK2 < bytesPK1 * 1.01 && bytesPK1 < bytesPK2 * 1.01)("pk1", bytesPK1)("pk2", bytesPK2);
        }
    }

    Y_UNIT_TEST(StatsSysViewBytesColumnActualization) {
        ui64 rawBytes1;
        ui64 bytes1;
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTypedLocalHelper helper("Utf8", kikimr);
        helper.CreateTestOlapTable();
        NArrow::NConstruction::TStringPoolFiller sPool(3, 52);
        helper.FillTable(sPool, 0, 800000);
        csController->WaitCompactions(TDuration::Seconds(10));

        helper.GetVolumes(rawBytes1, bytes1, false, {"new_column_ui64"});
        AFL_VERIFY(rawBytes1 == 0);
        AFL_VERIFY(bytes1 == 0);
        auto tableClient = kikimr.GetTableClient();
        {
            helper.ExecuteSchemeQuery("ALTER TABLESTORE `/Root/olapStore` ADD COLUMN new_column_ui64 Uint64;");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);");
            csController->WaitActualization(TDuration::Seconds(10));
            ui64 rawBytes2;
            ui64 bytes2;
            helper.GetVolumes(rawBytes2, bytes2, false, {"new_column_ui64"});
            AFL_VERIFY(rawBytes2 == 6500041)("real", rawBytes2);
            AFL_VERIFY(bytes2 == 45360)("b", bytes2);
        }
    }

    Y_UNIT_TEST(StatsSysViewBytesDictActualization) {
        ui64 rawBytes1;
        ui64 bytes1;
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTypedLocalHelper helper("Utf8", kikimr);
        helper.CreateTestOlapTable();
        NArrow::NConstruction::TStringPoolFiller sPool(3, 52);
        helper.FillTable(sPool, 0, 800000);
        csController->WaitCompactions(TDuration::Seconds(10));

        helper.GetVolumes(rawBytes1, bytes1, false, {"field"});
        auto tableClient = kikimr.GetTableClient();
        {
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field, `ENCODING.DICTIONARY.ENABLED`=`true`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);");
            csController->WaitActualization(TDuration::Seconds(10));
            ui64 rawBytes2;
            ui64 bytes2;
            helper.GetVolumes(rawBytes2, bytes2, false, {"field"});
            AFL_VERIFY(rawBytes2 == rawBytes1)("f1", rawBytes1)("f2", rawBytes2);
            AFL_VERIFY(bytes2 < bytes1 * 0.5)("f1", bytes1)("f2", bytes2);
        }
        {
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field, `ENCODING.DICTIONARY.ENABLED`=`false`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);");
            csController->WaitActualization(TDuration::Seconds(10));
            ui64 rawBytes2;
            ui64 bytes2;
            helper.GetVolumes(rawBytes2, bytes2, false, {"field"});
            AFL_VERIFY(rawBytes2 == rawBytes1)("f1", rawBytes1)("f2", rawBytes2);
            AFL_VERIFY(bytes2 < bytes1 * 1.01 && bytes1 < bytes2 * 1.01)("f1", bytes1)("f2", bytes2);
        }
    }

    Y_UNIT_TEST(StatsSysViewBytesDictStatActualization) {
        ui64 rawBytes1;
        ui64 bytes1;
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTypedLocalHelper helper("Utf8", kikimr);
        helper.CreateTestOlapTable();
        NArrow::NConstruction::TStringPoolFiller sPool(3, 52);
        helper.FillTable(sPool, 0, 800000);
        csController->WaitCompactions(TDuration::Seconds(10));

        helper.GetVolumes(rawBytes1, bytes1, false, {"field"});
        auto tableClient = kikimr.GetTableClient();
        {
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field, `ENCODING.DICTIONARY.ENABLED`=`true`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=pk_int_max, TYPE=MAX, FEATURES=`{\"column_name\" : \"pk_int\"}`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);");
            csController->WaitActualization(TDuration::Seconds(40));
            {
                ui64 rawBytes2;
                ui64 bytes2;
                helper.GetVolumes(rawBytes2, bytes2, false, { "field" });
                AFL_VERIFY(rawBytes2 == rawBytes1)("f1", rawBytes1)("f2", rawBytes2);
                AFL_VERIFY(bytes2 < bytes1 * 0.5)("f1", bytes1)("f2", bytes2);
                std::vector<NJson::TJsonValue> stats;
                helper.GetStats(stats, true);
                AFL_VERIFY(stats.size() == 3);
                for (auto&& i : stats) {
                    AFL_VERIFY(i.IsArray());
                    AFL_VERIFY(i.GetArraySafe().size() == 1);
                    AFL_VERIFY(i.GetArraySafe()[0]["chunk_idx"].GetInteger() == 0);
                    AFL_VERIFY(i.GetArraySafe()[0]["entity_id"].GetInteger() == 4);
                    AFL_VERIFY(i.GetArraySafe()[0]["data"].GetIntegerRobust() >= 799992);
                    AFL_VERIFY(i.GetArraySafe()[0]["data"].GetIntegerRobust() <= 799999);
                    AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("json", i);
                }
            }
        }
        {
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=DROP_INDEX, NAME=pk_int_max);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);");
            csController->WaitActualization(TDuration::Seconds(30));
            {
                std::vector<NJson::TJsonValue> stats;
                helper.GetStats(stats, true);
                AFL_VERIFY(stats.size() == 3);
                for (auto&& i : stats) {
                    AFL_VERIFY(i.IsArray());
                    AFL_VERIFY(i.GetArraySafe().size() == 0)("json", i);
                }
            }
        }
        {
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=pk_int_max, TYPE=MAX, FEATURES=`{\"column_name\" : \"pk_int\"}`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);");
            csController->WaitActualization(TDuration::Seconds(40));
            {
                std::vector<NJson::TJsonValue> stats;
                helper.GetStats(stats, true);
                AFL_VERIFY(stats.size() == 3);
                for (auto&& i : stats) {
                    AFL_VERIFY(i.IsArray());
                    AFL_VERIFY(i.GetArraySafe().size() == 1);
                    AFL_VERIFY(i.GetArraySafe()[0]["chunk_idx"].GetInteger() == 0);
                    AFL_VERIFY(i.GetArraySafe()[0]["entity_id"].GetInteger() == 5)("json", i);
                    AFL_VERIFY(i.GetArraySafe()[0]["data"].GetIntegerRobust() >= 799992);
                    AFL_VERIFY(i.GetArraySafe()[0]["data"].GetIntegerRobust() <= 799999);
                    AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("json", i);
                }
            }
        }
    }

    Y_UNIT_TEST(StatsSysViewColumns) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr.GetTestServer()).CreateTestOlapTable();
        for (ui64 i = 0; i < 10; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000 + i * 10000, 2000);
        }
        csController->WaitCompactions(TDuration::Seconds(10));

        auto tableClient = kikimr.GetTableClient();

        {
            auto selectQuery = TString(R"(
                SELECT TabletId, PathId, Kind
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                ORDER BY PathId, Kind, TabletId
                LIMIT 4;
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), 3ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[0].at("Kind")), "INSERTED");
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[3].at("PathId")), 3ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[3].at("Kind")), "INSERTED");
        }
        {
            auto selectQuery = TString(R"(
                SELECT SUM(BlobRangeSize) as Bytes, SUM(Rows) as Rows, PathId, TabletId
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                GROUP BY PathId, TabletId
                ORDER BY Bytes
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3);
            UNIT_ASSERT_LE(GetUint64(rows[0].at("Bytes")), GetUint64(rows[1].at("Bytes")));
        }
        {
            auto selectQuery = TString(R"(
                SELECT Sum(Rows) as Rows, Kind, Sum(ColumnRawBytes) as RawBytes, PathId
                FROM `/Root/olapStore/.sys/store_primary_index_portion_stats`
                GROUP BY Kind, PathId
                ORDER BY PathId, Kind, Rows
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("Rows")), 20000);
        }
    }

    Y_UNIT_TEST(StatsSysViewRanges) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetCompactionControl(NYDBTest::EOptimizerCompactionWeightControl::Disable);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        TLocalHelper(kikimr).CreateTestOlapTable("olapTable_1");
        TLocalHelper(kikimr).CreateTestOlapTable("olapTable_2");
        TLocalHelper(kikimr).CreateTestOlapTable("olapTable_3");

        for (ui64 i = 0; i < 10; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable_1", 0, 1000000 + i * 10000, 2000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable_2", 0, 1000000 + i * 10000, 3000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable_3", 0, 1000000 + i * 10000, 5000);
        }
        csController->WaitCompactions(TDuration::Seconds(10));

        auto tableClient = kikimr.GetTableClient();

        {
            auto selectQuery = TString(R"(
                SELECT *
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
        }

        {
            auto selectQuery = TString(R"(
                SELECT PathId, Kind, TabletId
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE
                    PathId == UInt64("3") AND Activity = true
                GROUP BY TabletId, PathId, Kind
                ORDER BY TabletId, Kind
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), 3ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[0].at("Kind")), "INSERTED");
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[1].at("PathId")), 3ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[2].at("Kind")), "INSERTED");
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[2].at("PathId")), 3ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[1].at("Kind")), "INSERTED");
        }

        {
            auto selectQuery = TString(R"(
                SELECT PathId, Kind, TabletId
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                GROUP BY PathId, Kind, TabletId
                ORDER BY PathId DESC, Kind DESC, TabletId DESC
                ;
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            ui32 numExpected = 3 * 3;
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), numExpected);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), 5ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[0].at("Kind")), "INSERTED");
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[numExpected - 1].at("PathId")), 3ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[numExpected - 1].at("Kind")), "INSERTED");
        }

        {
            auto selectQuery = TString(R"(
                SELECT PathId, Kind, TabletId
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE
                    PathId > UInt64("0") AND PathId < UInt32("4")
                    OR PathId > UInt64("4") AND PathId <= UInt64("5")
                GROUP BY PathId, Kind, TabletId
                ORDER BY
                    PathId DESC, Kind DESC, TabletId DESC
                ;
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            ui32 numExpected = 2 * 3;
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), numExpected);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), 5ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[0].at("Kind")), "INSERTED");
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[numExpected - 1].at("PathId")), 3ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[numExpected - 1].at("Kind")), "INSERTED");
        }
    }

    Y_UNIT_TEST(StatsSysViewFilter) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();

        TLocalHelper(kikimr.GetTestServer()).CreateTestOlapTable();
        for (ui64 i = 0; i < 10; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000 + i * 10000, 2000);
        }
        csController->WaitCompactions(TDuration::Seconds(10));

        auto tableClient = kikimr.GetTableClient();

        {
            auto selectQuery = TString(R"(
                SELECT PathId, Kind, TabletId, Sum(BlobRangeSize) as Bytes
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                GROUP BY PathId, Kind, TabletId
                ORDER BY PathId, Kind, TabletId;
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_GE(rows.size(), 3);
        }

        {
            auto selectQuery = TString(R"(
                SELECT PathId, Kind, TabletId, Sum(BlobRangeSize) as Bytes
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                GROUP BY PathId, Kind, TabletId
                ORDER BY PathId, Kind, TabletId;
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_GE(rows.size(), 3);
        }

        {
            auto selectQuery = TString(R"(
                SELECT *
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE Kind == 'EVICTED'
                ORDER BY PathId, Kind, TabletId;
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_GE(rows.size(), 0);
        }

        {
            auto selectQuery = TString(R"(
                SELECT PathId, Kind, TabletId
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE Kind IN ('SPLIT_COMPACTED', 'INACTIVE', 'EVICTED', 'INSERTED')
                GROUP BY PathId, Kind, TabletId
                ORDER BY PathId, Kind, TabletId;
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3);
        }
    }

    Y_UNIT_TEST(StatsSysViewAggregation) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();

        TLocalHelper(kikimr.GetTestServer()).CreateTestOlapTable("olapTable_1");
        TLocalHelper(kikimr.GetTestServer()).CreateTestOlapTable("olapTable_2");
        TLocalHelper(kikimr.GetTestServer()).CreateTestOlapTable("olapTable_3");

        for (ui64 i = 0; i < 100; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable_1", 0, 1000000 + i * 10000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable_2", 0, 1000000 + i * 10000, 2000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable_3", 0, 1000000 + i * 10000, 3000);
        }
        csController->WaitCompactions(TDuration::Seconds(10));

        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto tableClient = kikimr.GetTableClient();

        {
            auto selectQuery = TString(R"(
                SELECT
                    SUM(Rows) as rows,
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE
                    Kind != 'INACTIVE'
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1ull);
        }

        {
            auto selectQuery = TString(R"(
                SELECT
                    PathId,
                    SUM(Rows) as rows,
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE
                    Kind != 'INACTIVE'
                GROUP BY
                    PathId
                ORDER BY
                    PathId
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), 3);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[1].at("PathId")), 4);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[2].at("PathId")), 5);
        }

        {
            auto selectQuery = TString(R"(
                SELECT
                    PathId,
                    SUM(Rows) as rows,
                    SUM(BlobRangeSize) as bytes,
                    SUM(RawBytes) as bytes_raw
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE
                    Kind IN ('INSERTED', 'SPLIT_COMPACTED', 'COMPACTED')
                GROUP BY PathId
                ORDER BY rows DESC
                LIMIT 10
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), 5);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[1].at("PathId")), 4);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[2].at("PathId")), 3);
        }

        {
            auto selectQuery = TString(R"(
                SELECT
                    PathId,
                    SUM(Rows) as rows,
                    SUM(BlobRangeSize) as bytes,
                    SUM(RawBytes) as bytes_raw
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE
                    PathId == UInt64("3") AND Kind IN ('INSERTED', 'SPLIT_COMPACTED', 'COMPACTED')
                GROUP BY PathId
                ORDER BY rows DESC
                LIMIT 10
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), 3);
        }

        {
            auto selectQuery = TString(R"(
                SELECT
                    PathId,
                    SUM(Rows) as rows,
                    SUM(BlobRangeSize) as bytes,
                    SUM(RawBytes) as bytes_raw
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE
                    PathId >= UInt64("4") AND Kind IN ('INSERTED', 'SPLIT_COMPACTED', 'COMPACTED')
                GROUP BY PathId
                ORDER BY rows DESC
                LIMIT 10
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 2ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), 5);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[1].at("PathId")), 4);
        }

        {
            auto selectQuery = TString(R"(
                SELECT PathId, TabletId, Kind
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                GROUP BY PathId, TabletId, Kind
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            // 3 Tables with 3 Shards each and 2 KindId-s of stats
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3 * 3);
        }

        {
            auto selectQuery = TString(R"(
                SELECT
                    count(distinct(PathId)) as PathsCount,
                    count(distinct(Kind)) as KindsCount,
                    count(distinct(TabletId)) as TabletsCount
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathsCount")), 3ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("KindsCount")), 1);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("TabletsCount")), 4);
        }

        {
            auto selectQuery = TString(R"(
                SELECT PathId, count(*), sum(Rows), sum(BlobRangeSize), sum(RawBytes)
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                GROUP BY PathId
                ORDER BY PathId
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3ull);
            for (ui64 pathId = 3, row = 0; pathId <= 5; ++pathId, ++row) {
                UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[row].at("PathId")), pathId);
            }
        }
    }
}

} // namespace

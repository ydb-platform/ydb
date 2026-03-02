#include "helpers/local.h"
#include "helpers/query_executor.h"
#include "helpers/typed_local.h"
#include "helpers/writer.h"
#include "helpers/get_value.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract/index_info.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpOlapSysView) {


    Y_UNIT_TEST(GranulePathId_Store) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        auto helper = TLocalHelper(kikimr);
        const ui32 storeShardsCount = 101;
        const ui32 tableShardsCount = 17;
        const ui32 tablesCount = 1013;
        TVector<TString> tableNames;
        for (ui32 i = 0; i < tablesCount; ++i) {
            tableNames.push_back(TStringBuilder() << "table" << i);
        }
        helper.CreateTestOlapTables(tableNames, "columnStore", storeShardsCount, tableShardsCount);
        const auto tablets = csController->GetActiveTablets();
        UNIT_ASSERT_VALUES_EQUAL(tablets.size(), storeShardsCount);

        auto tableClient = kikimr.GetTableClient();

        {
            //check the store
            auto selectQuery = TString(R"(
                SELECT PathId, TabletId, InternalPathId,
                FROM `/Root/columnStore/.sys/store_primary_index_granule_stats`
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery, true);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), tableShardsCount * tablesCount);
            THashMap<NColumnShard::TSchemeShardLocalPathId, THashMap<ui64, NColumnShard::TInternalPathId>> result;
            for (const auto& row : rows) {
                result[NColumnShard::TSchemeShardLocalPathId::FromRawValue(GetUint64(row.at("PathId")))][GetUint64(row.at("TabletId"))] =
                    NColumnShard::TInternalPathId::FromRawValue(GetUint64(row.at("InternalPathId")));
            }
            UNIT_ASSERT_VALUES_EQUAL(result.size(), tablesCount);

            for (const auto& [tabletId, pathIdTranslator]  : tablets) {
                const auto& pathIds = pathIdTranslator->GetSchemeShardLocalPathIds();
                for (const auto& pathId : pathIds) {
                    const auto& internalPathId = pathIdTranslator->ResolveInternalPathId(pathId, false);
                    UNIT_ASSERT(internalPathId.has_value());
                    UNIT_ASSERT(result.contains(pathId) && result[pathId].contains(tabletId));
                    UNIT_ASSERT_VALUES_EQUAL(result[pathId][tabletId], *internalPathId);
                }
            }
        }

        {
            //check a table in the store
            auto selectQuery = TString(R"(
                SELECT PathId, TabletId, InternalPathId,
                FROM `/Root/columnStore/table2/.sys/primary_index_granule_stats`
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery, true);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), tableShardsCount);
            THashMap<NColumnShard::TSchemeShardLocalPathId, THashMap<ui64, NColumnShard::TInternalPathId>> result;
            for (const auto& row : rows) {
                result[NColumnShard::TSchemeShardLocalPathId::FromRawValue(GetUint64(row.at("PathId")))][GetUint64(row.at("TabletId"))] =
                    NColumnShard::TInternalPathId::FromRawValue(GetUint64(row.at("InternalPathId")));
            }
            UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);
            const auto& pathId = result.begin()->first;
            UNIT_ASSERT_VALUES_EQUAL(result[pathId].size(), tableShardsCount);

            for (const auto& [tabletId, pathIdTranslator]  : tablets) {
                if (const auto& internalPathId = pathIdTranslator->ResolveInternalPathId(pathId, false)) {
                    UNIT_ASSERT(result[pathId].contains(tabletId));
                    UNIT_ASSERT_VALUES_EQUAL(result[pathId][tabletId], *internalPathId);
                }
            }
        }
    }

    Y_UNIT_TEST(GranulePathId_Standalone) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        auto helper = TLocalHelper(kikimr);
        const ui32 tableShardsCount = 1201;
        helper.CreateTestOlapStandaloneTable("table", tableShardsCount);
        const auto tablets = csController->GetActiveTablets();
        UNIT_ASSERT_VALUES_EQUAL(tablets.size(), tableShardsCount);
        auto tableClient = kikimr.GetTableClient();
        auto selectQuery = TString(R"(
            SELECT PathId, TabletId, InternalPathId,
            FROM `/Root/table/.sys/primary_index_granule_stats`
        )");
        auto rows = ExecuteScanQuery(tableClient, selectQuery, true);
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), tableShardsCount);
        THashMap<NColumnShard::TSchemeShardLocalPathId, THashMap<ui64, NColumnShard::TInternalPathId>> result;
        for (const auto& row : rows) {
            result[NColumnShard::TSchemeShardLocalPathId::FromRawValue(GetUint64(row.at("PathId")))][GetUint64(row.at("TabletId"))] =
                NColumnShard::TInternalPathId::FromRawValue(GetUint64(row.at("InternalPathId")));
        }
        UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);
        const auto& pathId = result.begin()->first;
        UNIT_ASSERT_VALUES_EQUAL(result[pathId].size(), tableShardsCount);

        for (const auto& [tabletId, pathIdTranslator]  : tablets) {
            const auto& internalPathId = pathIdTranslator->ResolveInternalPathId(pathId, false);
            UNIT_ASSERT(internalPathId.has_value());
            UNIT_ASSERT(result[pathId].contains(tabletId));
            UNIT_ASSERT_VALUES_EQUAL(result[pathId][tabletId], *internalPathId);
        }
    }

    Y_UNIT_TEST(StatsSysView) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true);
        TKikimrRunner kikimr(settings);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        auto helper = TLocalHelper(kikimr);
        helper.CreateTestOlapTable();
        const auto describe = kikimr.GetTestClient().Describe(kikimr.GetTestServer().GetRuntime(), "/Root/olapStore/olapTable");
        const auto tablePathId = describe.GetPathId();
        helper.SetForcedCompaction();
        for (ui64 i = 0; i < 100; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000 + i * 10000, 1000);
        }
        csController->WaitCompactions(TDuration::Seconds(10));

        auto tableClient = kikimr.GetTableClient();
        auto selectQuery = TString(R"(
            SELECT PathId, Kind, TabletId, Sum(Rows) as Rows
            FROM `/Root/olapStore/.sys/store_primary_index_portion_stats`
            WHERE Activity == 1
            GROUP BY PathId, Kind, TabletId
            ORDER BY TabletId, Kind, PathId
        )");

        auto rows = ExecuteScanQuery(tableClient, selectQuery);

        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), tablePathId);
        UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[1].at("PathId")), tablePathId);
        UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[2].at("PathId")), tablePathId);
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
        auto describe = kikimr.GetTestClient().Describe(kikimr.GetTestServer().GetRuntime(), "/Root/olapStore/olapTable_1");
        const auto tablePathId1 = describe.GetPathId();

        TLocalHelper(kikimr).CreateTestOlapTable("olapTable_2");
        describe = kikimr.GetTestClient().Describe(kikimr.GetTestServer().GetRuntime(), "/Root/olapStore/olapTable_2");
        const auto tablePathId2 = describe.GetPathId();


        for (ui64 i = 0; i < 10; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable_1", 0, 1000000 + i * 10000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable_2", 0, 1000000 + i * 10000, 2000);
        }

        csController->WaitCompactions(TDuration::Seconds(5));
        auto tableClient = kikimr.GetTableClient();
        {
            auto selectQuery = TString(R"(
                SELECT PathId, Kind, TabletId
                FROM `/Root/olapStore/olapTable_1/.sys/primary_index_stats`
                WHERE Activity = 1
                GROUP BY PathId, TabletId, Kind
                ORDER BY PathId, TabletId, Kind
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows.front().at("PathId")), tablePathId1);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows.back().at("PathId")), tablePathId1);
        }
        {
            auto selectQuery = TString(R"(
                SELECT PathId, Kind, TabletId
                FROM `/Root/olapStore/olapTable_2/.sys/primary_index_stats`
                WHERE Activity = 1
                GROUP BY PathId, TabletId, Kind
                ORDER BY PathId, TabletId, Kind
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows.front().at("PathId")), tablePathId2);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows.back().at("PathId")), tablePathId2);
        }
        {
            auto selectQuery = Sprintf(R"(
                SELECT *
                FROM `/Root/olapStore/olapTable_1/.sys/primary_index_stats`
                WHERE
                    PathId > UInt64("%lu")
                ORDER BY PathId, Kind, TabletId
            )", tablePathId1);

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 0);
        }
        {
            auto selectQuery = TString(R"(
                SELECT Sum(Rows) as Rows
                FROM `/Root/olapStore/olapTable_1/.sys/primary_index_portion_stats`
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows.front().at("Rows")), 10 * 1000);
        }
    }

    Y_UNIT_TEST(StatsSysViewEnumStringBytes) {
        ui64 rawBytes1;
        ui64 bytes1;
        ui64 count1;
        ui64 rawBytes2;
        ui64 bytes2;
        ui64 count2;
        ui64 rawBytes3;
        ui64 bytes3;
        ui64 count3;
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        csController->SetOverrideBlobSplitSettings(NOlap::NSplitter::TSplitSettings());
        auto settings = TKikimrSettings().SetColumnShardAlterObjectEnabled(true).SetWithSampleTables(false);
        csController->DisableBackground(NYDBTest::ICSController::EBackground::Compaction);
        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTypedLocalHelper helper("Utf8", kikimr, "olapTable", "olapStore12");
        helper.CreateTestOlapTable(1, 1);

        const ui32 rowsCount = 800000;
        const ui32 groupsCount = 512;

        NArrow::NConstruction::TStringPoolFiller sPool(groupsCount, 52);

        for (ui32 i = 0; i < 10; ++i) {
            helper.FillTable(sPool, i, rowsCount / 10);
        }
        {
            auto alterQuery =
                TStringBuilder() <<
                R"(ALTER OBJECT `/Root/olapStore12` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`, `COMPACTION_PLANNER.FEATURES`=`
                  {"levels" : [{"class_name" : "Zero", "portions_live_duration" : "180s", "expected_blobs_size" : 2048},
                               {"class_name" : "Zero", "expected_blobs_size" : 2048}, {"class_name" : "Zero"}]}`);
                )";
            auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
        helper.GetVolumes(rawBytes1, bytes1, count1, false, { "field" });

        helper.ExecuteSchemeQuery(TStringBuilder() << "ALTER OBJECT `/Root/olapStore12` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, "
                                                      "NAME=field, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`DICTIONARY`);");
        helper.ExecuteSchemeQuery(
            "ALTER OBJECT `/Root/olapStore12` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field1, "
            "`DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`DICTIONARY`);",
            NYdb::EStatus::SCHEME_ERROR);
        helper.ExecuteSchemeQuery(
            "ALTER OBJECT `/Root/olapStore12` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field, "
            "`DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`DICTIONARY1`);",
            NYdb::EStatus::GENERIC_ERROR);
        helper.ExecuteSchemeQuery(
            "ALTER OBJECT `/Root/olapStore12` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field, "
            "`DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME1`=`DICTIONARY`);",
            NYdb::EStatus::GENERIC_ERROR);

        helper.ExecuteSchemeQuery(
            "ALTER OBJECT `/Root/olapStore12` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);");
        helper.GetVolumes(rawBytes2, bytes2, count2, false, { "field" });
        csController->WaitActualization(TDuration::Seconds(5));
        {
            auto db = kikimr.GetQueryClient();

            auto result = db.ExecuteQuery(R"(SELECT COUNT(*) FROM `/Root/olapStore12/olapTable`;)", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
            TString output = FormatResultSetYson(result.GetResultSet(0));
            Cout << output << Endl;
            CompareYson(output, R"([[800000u;]])");
        }

        csController->EnableBackground(NYDBTest::ICSController::EBackground::Compaction);
        csController->WaitCompactions(TDuration::Seconds(10));
        {
            auto db = kikimr.GetQueryClient();

            auto result =
                db.ExecuteQuery(R"(SELECT COUNT(*) FROM `/Root/olapStore12/olapTable`;)", NYdb::NQuery::TTxControl::BeginTx().CommitTx())
                    .ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
            TString output = FormatResultSetYson(result.GetResultSet(0));
            Cout << output << Endl;
            CompareYson(output, R"([[800000u;]])");

        }
        helper.GetVolumes(rawBytes3, bytes3, count3, false, { "field" });
        AFL_VERIFY(bytes2 * 10 < bytes1);
        AFL_VERIFY(bytes3 * 90 < bytes1);
        AFL_VERIFY(rawBytes3 * 10 < rawBytes1);
        Cerr << rawBytes1 << "/" << bytes1 << "/" << count1 << Endl;
        Cerr << rawBytes2 << "/" << bytes2 << "/" << count2 << Endl;
        Cerr << rawBytes3 << "/" << bytes3 << "/" << count3 << Endl;
    }

    Y_UNIT_TEST(StatsSysViewBytesPackActualization) {
        ui64 rawBytesPK1;
        ui64 bytesPK1;

        ui64 count1;
        ui64 count2;
        ui64 count3;

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        auto* CSConfig = settings.AppConfig.MutableColumnShardConfig();
        CSConfig->SetDefaultCompression(NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4);
        CSConfig->SetAlterObjectEnabled(true);

        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTypedLocalHelper helper("", kikimr, "olapTable", "olapStore");
        helper.CreateTestOlapTable();
        helper.FillPKOnly(0, 800000);
        csController->WaitCompactions(TDuration::Seconds(10));

        helper.GetVolumes(rawBytesPK1, bytesPK1, count1, false, {"pk_int"});
        auto tableClient = kikimr.GetTableClient();
        {
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=pk_int, `SERIALIZER.CLASS_NAME`=`ARROW_SERIALIZER`, `COMPRESSION.TYPE`=`zstd`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);");
            csController->WaitActualization(TDuration::Seconds(10));
            ui64 rawBytesPK2;
            ui64 bytesPK2;
            helper.GetVolumes(rawBytesPK2, bytesPK2, count2, false, {"pk_int"});
            AFL_VERIFY(rawBytesPK2 == rawBytesPK1)("pk1", rawBytesPK1)("pk2", rawBytesPK2);
            AFL_VERIFY(bytesPK2 < bytesPK1 / 3)("pk1", bytesPK1)("pk2", bytesPK2);
        }
        {
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=pk_int, `SERIALIZER.CLASS_NAME`=`ARROW_SERIALIZER`, `COMPRESSION.TYPE`=`lz4`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);");
            csController->WaitActualization(TDuration::Seconds(10));
            ui64 rawBytesPK2;
            ui64 bytesPK2;
            helper.GetVolumes(rawBytesPK2, bytesPK2, count3, false, {"pk_int"});
            AFL_VERIFY(rawBytesPK2 == rawBytesPK1)("pk1", rawBytesPK1)("pk2", rawBytesPK2);
            AFL_VERIFY(bytesPK2 < bytesPK1 * 1.01 && bytesPK1 < bytesPK2 * 1.01)("pk1", bytesPK1)("pk2", bytesPK2);
        }
    }

    Y_UNIT_TEST(StatsSysViewBytesColumnActualization) {
        ui64 rawBytes1;
        ui64 bytes1;
        ui64 count1;
        ui64 count2;
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        auto settings = TKikimrSettings()
            .SetColumnShardAlterObjectEnabled(true)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTypedLocalHelper helper("Utf8", kikimr);
        helper.CreateTestOlapTable();
        NArrow::NConstruction::TStringPoolFiller sPool(3, 52);
        helper.FillTable(sPool, 0, 800000);
        csController->WaitCompactions(TDuration::Seconds(5));
        helper.FillTable(sPool, 0.5, 800000);
        csController->WaitCompactions(TDuration::Seconds(5));

        helper.GetVolumes(rawBytes1, bytes1, count1, false, {"new_column_ui64"});
        AFL_VERIFY(rawBytes1 == 0);
        AFL_VERIFY(bytes1 == 0);
        auto tableClient = kikimr.GetTableClient();
        {
            helper.ExecuteSchemeQuery("ALTER TABLESTORE `/Root/olapStore` ADD COLUMN new_column_ui64 Uint64;");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);");
            csController->WaitActualization(TDuration::Seconds(10));
            ui64 rawBytes2;
            ui64 bytes2;
            helper.GetVolumes(rawBytes2, bytes2, count2, false, { "new_column_ui64", NOlap::IIndexInfo::SPEC_COL_DELETE_FLAG });
            AFL_VERIFY(rawBytes2 == 0)("real", rawBytes2);
            AFL_VERIFY(bytes2 == 0)("b", bytes2);
        }
    }

    Y_UNIT_TEST(StatsSysViewBytesDictActualization) {
        ui64 rawBytes1;
        ui64 bytes1;
        ui64 count1;
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        auto* CSConfig = settings.AppConfig.MutableColumnShardConfig();
        CSConfig->SetDefaultCompression(NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4);
        CSConfig->SetAlterObjectEnabled(true);
        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTypedLocalHelper helper("Utf8", kikimr);
        helper.CreateTestOlapTable();
        NArrow::NConstruction::TStringPoolFiller sPool(3, 52);
        helper.FillTable(sPool, 0, 800000);
        csController->WaitCompactions(TDuration::Seconds(10));

        helper.GetVolumes(rawBytes1, bytes1, count1, false, {"field"});
        auto tableClient = kikimr.GetTableClient();
        {
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`DICTIONARY`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);");
            csController->WaitActualization(TDuration::Seconds(10));
            ui64 rawBytes2;
            ui64 bytes2;
            ui64 count2;
            helper.GetVolumes(rawBytes2, bytes2, count2, false, { "field" });
            AFL_VERIFY(rawBytes2 * 2 < rawBytes1)("f1", rawBytes1)("f2", rawBytes2);
            AFL_VERIFY(bytes2 < bytes1 * 0.5)("f1", bytes1)("f2", bytes2);
        }
        {
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`PLAIN`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);");
            csController->WaitActualization(TDuration::Seconds(10));
            ui64 rawBytes2;
            ui64 bytes2;
            ui64 count2;
            helper.GetVolumes(rawBytes2, bytes2, count2, false, {"field"});
            AFL_VERIFY(rawBytes2 == rawBytes1)("f1", rawBytes1)("f2", rawBytes2);
            AFL_VERIFY(bytes2 < bytes1 * 1.01 && bytes1 < bytes2 * 1.01)("f1", bytes1)("f2", bytes2);
        }
    }

    Y_UNIT_TEST(StatsSysViewBytesDictStatActualization) {
        ui64 rawBytes1;
        ui64 bytes1;
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        csController->SetSmallSizeDetector(Max<ui32>());
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        auto* CSConfig = settings.AppConfig.MutableColumnShardConfig();
        CSConfig->SetDefaultCompression(NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4);
        CSConfig->SetAlterObjectEnabled(true);
        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTypedLocalHelper helper("Utf8", kikimr);
        helper.CreateTestOlapTable();
        NArrow::NConstruction::TStringPoolFiller sPool(3, 52);
        helper.FillTable(sPool, 0, 800000);
        csController->WaitCompactions(TDuration::Seconds(10));

        ui64 count1;
        ui64 count2;

        helper.GetVolumes(rawBytes1, bytes1, count1, false, {"field"});
        auto tableClient = kikimr.GetTableClient();
        {
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`DICTIONARY`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=pk_int_max, TYPE=MAX, FEATURES=`{\"column_name\" : \"pk_int\"}`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);");
            csController->WaitActualization(TDuration::Seconds(40));
            csController->WaitCompactions(TDuration::Seconds(5));
            {
                ui64 rawBytes2;
                ui64 bytes2;
                helper.GetVolumes(rawBytes2, bytes2, count2, false, { "field" });
                AFL_VERIFY(rawBytes2 * 2 < rawBytes1)("f1", rawBytes1)("f2", rawBytes2);
                AFL_VERIFY(bytes2 < bytes1 * 0.5)("f1", bytes1)("f2", bytes2);
                std::vector<NJson::TJsonValue> stats;
                helper.GetStats(stats, true);
                AFL_VERIFY(stats.size() == 3)("count", stats.size());
//                for (auto&& i : stats) {
//                    AFL_VERIFY(i.IsArray());
//                    AFL_VERIFY(i.GetArraySafe().size() == 1);
//                    AFL_VERIFY(i.GetArraySafe()[0]["chunk_idx"].GetInteger() == 0);
//                    AFL_VERIFY(i.GetArraySafe()[0]["entity_id"].GetInteger() == 4);
//                    AFL_VERIFY(i.GetArraySafe()[0]["data"].GetIntegerRobust() >= 799992);
//                    AFL_VERIFY(i.GetArraySafe()[0]["data"].GetIntegerRobust() <= 799999);
//                    AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("json", i);
//                }
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
//                for (auto&& i : stats) {
//                    AFL_VERIFY(i.IsArray());
//                    AFL_VERIFY(i.GetArraySafe().size() == 0)("json", i);
//                }
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
//                for (auto&& i : stats) {
//                    AFL_VERIFY(i.IsArray());
//                    AFL_VERIFY(i.GetArraySafe().size() == 1);
//                    AFL_VERIFY(i.GetArraySafe()[0]["chunk_idx"].GetInteger() == 0);
//                    AFL_VERIFY(i.GetArraySafe()[0]["entity_id"].GetInteger() == 5)("json", i);
//                    AFL_VERIFY(i.GetArraySafe()[0]["data"].GetIntegerRobust() >= 799992);
//                    AFL_VERIFY(i.GetArraySafe()[0]["data"].GetIntegerRobust() <= 799999);
//                    AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("json", i);
//                }
            }
        }
    }

    Y_UNIT_TEST(StatsSysViewColumns) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableColumnShardConfig()->SetDefaultCompactionPreset("tiling");
        auto settings = TKikimrSettings(appConfig).SetWithSampleTables(false);
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr.GetTestServer()).CreateTestOlapTable();
        const auto describe = kikimr.GetTestClient().Describe(kikimr.GetTestServer().GetRuntime(), "/Root/olapStore/olapTable");
        const auto tablePathId = describe.GetPathId();

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
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), tablePathId);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[0].at("Kind")), "SPLIT_COMPACTED");
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[3].at("PathId")), tablePathId);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[3].at("Kind")), "SPLIT_COMPACTED");
        }
        {
            auto selectQuery = TString(R"(
                SELECT SUM(BlobRangeSize) as Bytes, SUM(Rows) as Rows, PathId, TabletId
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE Activity == 1
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
                WHERE Activity == 1
                GROUP BY Kind, PathId
                ORDER BY PathId, Kind, Rows
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_LE(rows.size(), 2);
            int totalRows = 0;
            for (const auto& row : rows) {
                totalRows += GetUint64(row.at("Rows"));
            }
            UNIT_ASSERT_VALUES_EQUAL(totalRows, 20000);
        }
    }

    Y_UNIT_TEST(StatsSysViewRanges) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableColumnShardConfig()->SetDefaultCompactionPreset("tiling");
        auto settings = TKikimrSettings(appConfig).SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetCompactionControl(NYDBTest::EOptimizerCompactionWeightControl::Disable);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        TLocalHelper(kikimr).CreateTestOlapTable("olapTable_1");
        auto describe = kikimr.GetTestClient().Describe(kikimr.GetTestServer().GetRuntime(), "/Root/olapStore/olapTable_1");
        const auto tablePathId1 = describe.GetPathId();

        TLocalHelper(kikimr).CreateTestOlapTable("olapTable_2");
        describe = kikimr.GetTestClient().Describe(kikimr.GetTestServer().GetRuntime(), "/Root/olapStore/olapTable_2");
        const auto tablePathId2 = describe.GetPathId();

        TLocalHelper(kikimr).CreateTestOlapTable("olapTable_3");
        describe = kikimr.GetTestClient().Describe(kikimr.GetTestServer().GetRuntime(), "/Root/olapStore/olapTable_3");
        const auto tablePathId3 = describe.GetPathId();

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
                ORDER BY PathId
                LIMIT 10
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
        }

        {
            auto selectQuery = TString(R"(
                SELECT *
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
        }

        {
            auto selectQuery = Sprintf(R"(
                SELECT PathId, Kind, TabletId
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE
                    PathId == UInt64("%lu") AND Activity == 1
                GROUP BY TabletId, PathId, Kind
                ORDER BY TabletId, Kind
            )", tablePathId1);

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), tablePathId1);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[0].at("Kind")), "SPLIT_COMPACTED");
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[1].at("PathId")), tablePathId1);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[2].at("Kind")), "SPLIT_COMPACTED");
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[2].at("PathId")), tablePathId1);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[1].at("Kind")), "SPLIT_COMPACTED");
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
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), tablePathId3);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[0].at("Kind")), "SPLIT_COMPACTED");
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[numExpected - 1].at("PathId")), tablePathId1);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[numExpected - 1].at("Kind")), "SPLIT_COMPACTED");
        }

        {
            auto selectQuery = Sprintf(R"(
                SELECT PathId, Kind, TabletId
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE
                    PathId > UInt64("0") AND PathId < UInt32("%lu")
                    OR PathId > UInt64("%lu") AND PathId <= UInt64("%lu")
                GROUP BY PathId, Kind, TabletId
                ORDER BY
                    PathId DESC, Kind DESC, TabletId DESC
                ;
            )", tablePathId2, tablePathId2, tablePathId3);

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            ui32 numExpected = 2 * 3;
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), numExpected);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), tablePathId3);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[0].at("Kind")), "SPLIT_COMPACTED");
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[numExpected - 1].at("PathId")), tablePathId1);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[numExpected - 1].at("Kind")), "SPLIT_COMPACTED");
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
                WHERE Activity == 1
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
                WHERE Activity == 1
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
                AND Activity == 1
                GROUP BY PathId, Kind, TabletId
                ORDER BY PathId, Kind, TabletId;
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3);
        }
    }

    Y_UNIT_TEST(StatsSysViewAggregation) {
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true);
        TKikimrRunner kikimr(settings);
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        TLocalHelper helper(kikimr.GetTestServer());

        helper.CreateTestOlapTable("olapTable_1");
        auto describe = kikimr.GetTestClient().Describe(kikimr.GetTestServer().GetRuntime(), "/Root/olapStore/olapTable_1");
        const auto tablePathId1 = describe.GetPathId();

        helper.CreateTestOlapTable("olapTable_2");
        describe = kikimr.GetTestClient().Describe(kikimr.GetTestServer().GetRuntime(), "/Root/olapStore/olapTable_2");
        const auto tablePathId2 = describe.GetPathId();

        helper.CreateTestOlapTable("olapTable_3");
        describe = kikimr.GetTestClient().Describe(kikimr.GetTestServer().GetRuntime(), "/Root/olapStore/olapTable_3");
        const auto tablePathId3 = describe.GetPathId();

        helper.SetForcedCompaction();

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
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), tablePathId1);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[1].at("PathId")), tablePathId2);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[2].at("PathId")), tablePathId3);
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
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), tablePathId3);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[1].at("PathId")), tablePathId2);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[2].at("PathId")), tablePathId1);
        }

        {
            auto selectQuery = Sprintf(R"(
                SELECT
                    PathId,
                    SUM(Rows) as rows,
                    SUM(BlobRangeSize) as bytes,
                    SUM(RawBytes) as bytes_raw
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE
                    PathId == UInt64("%lu") AND Kind IN ('INSERTED', 'SPLIT_COMPACTED', 'COMPACTED')
                GROUP BY PathId
                ORDER BY rows DESC
                LIMIT 10
            )", tablePathId1);

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), tablePathId1);
        }

        {
            auto selectQuery = Sprintf(R"(
                SELECT
                    PathId,
                    SUM(Rows) as rows,
                    SUM(BlobRangeSize) as bytes,
                    SUM(RawBytes) as bytes_raw
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE
                    PathId >= UInt64("%lu") AND Kind IN ('INSERTED', 'SPLIT_COMPACTED', 'COMPACTED')
                GROUP BY PathId
                ORDER BY rows DESC
                LIMIT 10
            )", tablePathId2);

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 2ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), tablePathId3);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[1].at("PathId")), tablePathId2);
        }

        {
            auto selectQuery = TString(R"(
                SELECT PathId, TabletId, Kind
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE Activity == 1
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
                WHERE Activity == 1
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
                WHERE Activity == 1
                GROUP BY PathId
                ORDER BY PathId
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3ull);
            const TVector<TLocalPathId> tablePaths{tablePathId1, tablePathId2, tablePathId3};
            for (size_t i = 0; i < tablePaths.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[i].at("PathId")), tablePaths[i]);
            }
        }
    }
}

} // namespace

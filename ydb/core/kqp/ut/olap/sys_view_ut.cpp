#include "helpers/local.h"
#include "helpers/plan_step.h"
#include "helpers/query_executor.h"
#include "helpers/typed_local.h"
#include "helpers/writer.h"
#include "helpers/get_value.h"

#include <library/cpp/lwtrace/all.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/tx/columnshard/engines/reader/tracing/data_source_probes.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract/index_info.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

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
        for (size_t i = 0; i < 3; ++i) {
            UNIT_ASSERT_C(IsIn({"SPLIT_COMPACTED", "INSERTED"}, GetUtf8(rows[i].at("Kind"))), GetUtf8(rows[i].at("Kind")));
        }
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

    Y_UNIT_TEST(StatsSysViewChunksLimit) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();

        TLocalHelper(kikimr).CreateTestOlapTable("olapTable");
        for (ui64 i = 0; i < 50; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000 + i * 10000, 1000);
        }
        csController->WaitCompactions(TDuration::Seconds(10));

        auto tableClient = kikimr.GetTableClient();
        auto selectQuery = TString(R"(
            SELECT *
            FROM `/Root/olapStore/olapTable/.sys/primary_index_stats`
            LIMIT 1
        )");

        auto rows = ExecuteScanQuery(tableClient, selectQuery);
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
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

    Y_UNIT_TEST(StatsSysViewOrderByPKWithLimit) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableFeatureFlags()->SetEnableSysViewOrderByLimitPushdown(true);
        TKikimrRunner kikimr(settings);
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();

        TLocalHelper(kikimr).CreateTestOlapTable();
        for (ui64 i = 0; i < 10; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000 + i * 10000, 1000);
        }
        csController->WaitCompactions(TDuration::Seconds(5));

        const ui32 limit = 3;

        auto tableClient = kikimr.GetTableClient();
        // ORDER BY must be a PK prefix of the sys view (PathId, TabletId, PortionId, InternalEntityId, ChunkIdx)
        // so that the scan is executed as sorted with the limit pushed down to the columnshard.
        auto allRows = ExecuteScanQuery(tableClient, R"(
            SELECT PathId, TabletId, PortionId
            FROM `/Root/olapStore/olapTable/.sys/primary_index_stats`
            ORDER BY PathId, TabletId, PortionId
        )");
        UNIT_ASSERT_GT(allRows.size(), limit);

        {
            auto rows = ExecuteScanQuery(tableClient, Sprintf(R"(
                SELECT PathId, TabletId, PortionId
                FROM `/Root/olapStore/olapTable/.sys/primary_index_stats`
                ORDER BY PathId, TabletId, PortionId
                LIMIT %u
            )", limit));
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), limit);
            for (ui32 i = 0; i < limit; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[i].at("TabletId")), GetUint64(allRows[i].at("TabletId")));
                UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[i].at("PortionId")), GetUint64(allRows[i].at("PortionId")));
            }
        }
        {
            auto rows = ExecuteScanQuery(tableClient, Sprintf(R"(
                SELECT PathId, TabletId, PortionId
                FROM `/Root/olapStore/olapTable/.sys/primary_index_stats`
                ORDER BY PathId DESC, TabletId DESC, PortionId DESC
                LIMIT %u
            )", limit));
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), limit);
            for (ui32 i = 0; i < limit; ++i) {
                const auto& expected = allRows[allRows.size() - 1 - i];
                UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[i].at("TabletId")), GetUint64(expected.at("TabletId")));
                UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[i].at("PortionId")), GetUint64(expected.at("PortionId")));
            }
        }
        // default cap is not exceeded here, so the normal (non-passthrough) sorted-limit path is what ran above
        UNIT_ASSERT_VALUES_EQUAL(csController->GetSysViewLimitPassthroughsCount().Val(), 0);
    }

    Y_UNIT_TEST(StatsSysViewOrderByPKWithLimitPassthrough) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableFeatureFlags()->SetEnableSysViewOrderByLimitPushdown(true);
        // Cap at 1 so the sync point switches to passthrough as soon as it holds a second portion.
        settings.AppConfig.MutableColumnShardConfig()->MutableLimitSyncPointConfig()->SetSysViewMaxHeldPortions(1);
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        // Single shard + no compaction: every write stays a separate portion on one tablet, so the
        // per-tablet limit sync point holds more than the cap and the passthrough path is exercised.
        csController->DisableBackground(NYDBTest::ICSController::EBackground::Compaction);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable("olapTable", "olapStore", 1, 1);
        for (ui64 i = 0; i < 10; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000 + i * 10000, 1000);
        }
        csController->WaitActualization(TDuration::Seconds(5));

        const ui32 limit = 3;

        auto tableClient = kikimr.GetTableClient();
        auto allRows = ExecuteScanQuery(tableClient, R"(
            SELECT PathId, TabletId, PortionId
            FROM `/Root/olapStore/olapTable/.sys/primary_index_stats`
            ORDER BY PathId, TabletId, PortionId
        )");
        UNIT_ASSERT_GT(allRows.size(), limit);
        // Precondition for the passthrough path: the single tablet must hold more portions than the cap.
        THashSet<ui64> portionIds;
        for (const auto& row : allRows) {
            portionIds.insert(GetUint64(row.at("PortionId")));
        }
        UNIT_ASSERT_GT(portionIds.size(), 1u);

        for (const bool desc : {false, true}) {
            const TString dir = desc ? "DESC" : "ASC";
            auto rows = ExecuteScanQuery(tableClient, Sprintf(R"(
                SELECT PathId, TabletId, PortionId
                FROM `/Root/olapStore/olapTable/.sys/primary_index_stats`
                ORDER BY PathId %s, TabletId %s, PortionId %s
                LIMIT %u
            )", dir.c_str(), dir.c_str(), dir.c_str(), limit));
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), limit);
            for (ui32 i = 0; i < limit; ++i) {
                const auto& expected = desc ? allRows[allRows.size() - 1 - i] : allRows[i];
                UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[i].at("TabletId")), GetUint64(expected.at("TabletId")));
                UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[i].at("PortionId")), GetUint64(expected.at("PortionId")));
            }
        }

        // A full-PK ORDER BY drains the sync point one source at a time. PathId and TabletId are constant within a
        // shard, so ordering by that prefix leaves the heap unable to drain and the cap is what bounds it.
        UNIT_ASSERT_VALUES_EQUAL(csController->GetSysViewLimitPassthroughsCount().Val(), 0);
        auto prefixRows = ExecuteScanQuery(tableClient, Sprintf(R"(
            SELECT PathId, TabletId
            FROM `/Root/olapStore/olapTable/.sys/primary_index_stats`
            ORDER BY PathId, TabletId
            LIMIT %u
        )", limit));
        UNIT_ASSERT_VALUES_EQUAL(prefixRows.size(), limit);
        UNIT_ASSERT_GT(csController->GetSysViewLimitPassthroughsCount().Val(), 0);
    }

    Y_UNIT_TEST(StatsSysViewOrderByPKWithIndexes) {
        const TString tablePath = "/Root/olapStore/olapTable";
        const ui32 insertRowsCount = 10;
        const ui32 limitSweepMax = 64;

        auto settings = TKikimrSettings().SetColumnShardAlterObjectEnabled(true).SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableSysViewOrderByLimitPushdown(true);
        TKikimrRunner kikimr(settings);
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();

        auto helper = TLocalHelper(kikimr);
        helper.CreateTestOlapTable("olapTable", "olapStore", 1, 1);
        helper.SetForcedCompaction();
        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();

        const auto executeSchemeQuery = [&](const TString& query) {
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        };

        // Indexes and columns share one schema entity id counter: the index created here gets the id
        // right after the initial columns and new_column_ui64 gets a greater one. A portion emits its
        // column chunks before its index chunks, so its rows interleave on InternalEntityId.
        const auto prepareInterleavedPortions = [&]() {
            // bulk upsert requires every column of the current schema, so write before ADD COLUMN
            WriteTestData(kikimr, tablePath, 1000000, 300000000, 1000);
            executeSchemeQuery(
                R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_level, TYPE=MIN_MAX, FEATURES=`{"column_name" : "level"}`);)");
            executeSchemeQuery("ALTER TABLESTORE `/Root/olapStore` ADD COLUMN new_column_ui64 Uint64;");
            {
                auto db = kikimr.GetQueryClient();
                TStringBuilder insertQuery;
                insertQuery << "INSERT INTO `" << tablePath << "` (timestamp, uid, resource_id, level, new_column_ui64) VALUES";
                for (ui32 rowIdx = 0; rowIdx < insertRowsCount; ++rowIdx) {
                    insertQuery << (rowIdx ? "," : "") << " (Timestamp('1970-01-01T00:00:0" << rowIdx % 10 << "Z'), 'uid_" << rowIdx
                                << "', '" << rowIdx << "', " << rowIdx << ", " << rowIdx << "u)";
                }
                insertQuery << ";";
                auto result = db.ExecuteQuery(insertQuery, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            }
            // compaction merges the batches into a portion carrying both the index chunk and the
            // new_column_ui64 record; actualization is the fallback that rewrites any leftover
            // portion with the index
            csController->WaitCompactions(TDuration::Seconds(5));
            executeSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);");
            AdvancePlanStep(kikimr);
            csController->WaitActualization(TDuration::Seconds(10));
        };
        prepareInterleavedPortions();

        // ORDER BY must be a PK prefix of the sys view (PathId, TabletId, PortionId, InternalEntityId, ChunkIdx)
        // so that the scan is executed as sorted and KQP relies on the shard output order.
        const auto buildStatsQuery = [&](const bool desc, const std::optional<ui32> limit) {
            const TString direction = desc ? " DESC" : "";
            TStringBuilder query;
            query << "SELECT PathId, TabletId, PortionId, InternalEntityId, ChunkIdx, EntityType, EntityName" << Endl
                  << "FROM `" << tablePath << "/.sys/primary_index_stats`" << Endl
                  << "ORDER BY PathId" << direction << ", TabletId" << direction << ", PortionId" << direction << ", InternalEntityId"
                  << direction << ", ChunkIdx" << direction << Endl;
            if (limit) {
                query << "LIMIT " << *limit << Endl;
            }
            return TString(query);
        };

        using TRowKey = std::tuple<ui64, ui64, ui64, ui64, ui64>;
        const auto readKeys = [](const auto& rows) {
            std::vector<TRowKey> keys;
            for (auto&& row : rows) {
                keys.emplace_back(GetUint64(row.at("PathId")), GetUint64(row.at("TabletId")), GetUint64(row.at("PortionId")),
                    GetUint32(row.at("InternalEntityId")), GetUint64(row.at("ChunkIdx")));
            }
            return keys;
        };
        const auto keyToString = [](const TRowKey& key) {
            return TStringBuilder() << "(" << std::get<0>(key) << "," << std::get<1>(key) << "," << std::get<2>(key) << ","
                                    << std::get<3>(key) << "," << std::get<4>(key) << ")";
        };

        auto fullRows = ExecuteScanQuery(tableClient, buildStatsQuery(false, std::nullopt));
        auto keys = readKeys(fullRows);
        UNIT_ASSERT(keys.size());

        {
            // the test scenario must actually produce a portion where an index entity id is below a
            // column entity id, otherwise nothing is exercised
            THashMap<std::pair<ui64, ui64>, std::pair<ui64, ui64>> colMaxIdxMinByPortion;
            for (ui32 i = 0; i < fullRows.size(); ++i) {
                const auto portion = std::make_pair(std::get<1>(keys[i]), std::get<2>(keys[i]));
                const ui64 entityId = std::get<3>(keys[i]);
                auto& [colMax, idxMin] = colMaxIdxMinByPortion.try_emplace(portion, 0, Max<ui64>()).first->second;
                if (GetUtf8(fullRows[i].at("EntityType")) == "IDX") {
                    idxMin = Min(idxMin, entityId);
                } else {
                    colMax = Max(colMax, entityId);
                }
            }
            bool interleaved = false;
            for (auto&& [_, colMaxIdxMin] : colMaxIdxMinByPortion) {
                interleaved = interleaved || colMaxIdxMin.second < colMaxIdxMin.first;
            }
            TStringBuilder rowsDump;
            for (ui32 i = 0; i < fullRows.size(); ++i) {
                rowsDump << keyToString(keys[i]) << GetUtf8(fullRows[i].at("EntityType")) << ":" << GetUtf8(fullRows[i].at("EntityName"))
                         << ";";
            }
            UNIT_ASSERT_C(interleaved, "expected a portion with an index entity id below a column entity id, got: " << rowsDump);
        }

        auto sortedKeys = keys;
        std::sort(sortedKeys.begin(), sortedKeys.end());
        for (ui32 i = 0; i < keys.size(); ++i) {
            UNIT_ASSERT_C(keys[i] == sortedKeys[i],
                TStringBuilder() << "unsorted ASC output at row " << i << ": " << keyToString(keys[i]) << " != " << keyToString(sortedKeys[i]));
        }

        {
            auto rows = ExecuteScanQuery(tableClient, buildStatsQuery(true, std::nullopt));
            auto descKeys = readKeys(rows);
            std::reverse(descKeys.begin(), descKeys.end());
            UNIT_ASSERT_VALUES_EQUAL(descKeys.size(), sortedKeys.size());
            for (ui32 i = 0; i < descKeys.size(); ++i) {
                UNIT_ASSERT_C(descKeys[i] == sortedKeys[i],
                    TStringBuilder() << "unsorted DESC output at row " << i << ": " << keyToString(descKeys[i]) << " != "
                                     << keyToString(sortedKeys[i]));
            }
        }

        for (ui32 limit = 1; limit <= Min<ui32>(keys.size(), limitSweepMax); ++limit) {
            auto rows = ExecuteScanQuery(tableClient, buildStatsQuery(false, limit));
            auto limitKeys = readKeys(rows);
            UNIT_ASSERT_VALUES_EQUAL(limitKeys.size(), limit);
            for (ui32 i = 0; i < limit; ++i) {
                UNIT_ASSERT_C(limitKeys[i] == sortedKeys[i],
                    TStringBuilder() << "wrong row for limit " << limit << " at " << i << ": " << keyToString(limitKeys[i]) << " != "
                                     << keyToString(sortedKeys[i]));
            }
        }

        // DESC+LIMIT is the main passthrough trigger: sources arrive ascending, so the top-k are the
        // last k keys in reverse; interleaved entity ids exercise the per-source PK reorder here too.
        for (ui32 limit = 1; limit <= Min<ui32>(keys.size(), limitSweepMax); ++limit) {
            auto rows = ExecuteScanQuery(tableClient, buildStatsQuery(true, limit));
            auto limitKeys = readKeys(rows);
            UNIT_ASSERT_VALUES_EQUAL(limitKeys.size(), limit);
            for (ui32 i = 0; i < limit; ++i) {
                const auto& expected = sortedKeys[sortedKeys.size() - 1 - i];
                UNIT_ASSERT_C(limitKeys[i] == expected,
                    TStringBuilder() << "wrong row for DESC limit " << limit << " at " << i << ": " << keyToString(limitKeys[i])
                                     << " != " << keyToString(expected));
            }
        }
    }

    // Highest-risk scenario: interleaved entity ids (per-source PK reorder) AND passthrough (cross-source stream to
    // KQP) must both be correct at once. Single shard + compaction disabled keeps many portions; cap = 1 forces the
    // limit sync point onto the passthrough path.
    Y_UNIT_TEST(StatsSysViewOrderByPKWithIndexesPassthrough) {
        const TString tablePath = "/Root/olapStore/olapTable";
        const ui32 insertRowsCount = 10;
        const ui32 limitSweepMax = 64;

        auto settings = TKikimrSettings().SetColumnShardAlterObjectEnabled(true).SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableSysViewOrderByLimitPushdown(true);
        settings.AppConfig.MutableColumnShardConfig()->MutableLimitSyncPointConfig()->SetSysViewMaxHeldPortions(1);
        TKikimrRunner kikimr(settings);
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        // no compaction merge: every write stays a separate portion on the one tablet, so the sync point holds
        // more than the cap and passes the rest straight through to KQP
        csController->DisableBackground(NYDBTest::ICSController::EBackground::Compaction);

        auto helper = TLocalHelper(kikimr);
        helper.CreateTestOlapTable("olapTable", "olapStore", 1, 1);
        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();

        const auto executeSchemeQuery = [&](const TString& query) {
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        };

        // same interleaving as StatsSysViewOrderByPKWithIndexes: the index entity id lands below new_column_ui64,
        // and a portion emits column chunks before index chunks, so rows interleave on InternalEntityId
        WriteTestData(kikimr, tablePath, 1000000, 300000000, 1000);
        executeSchemeQuery(
            R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_level, TYPE=MIN_MAX, FEATURES=`{"column_name" : "level"}`);)");
        executeSchemeQuery("ALTER TABLESTORE `/Root/olapStore` ADD COLUMN new_column_ui64 Uint64;");
        {
            auto db = kikimr.GetQueryClient();
            TStringBuilder insertQuery;
            insertQuery << "INSERT INTO `" << tablePath << "` (timestamp, uid, resource_id, level, new_column_ui64) VALUES";
            for (ui32 rowIdx = 0; rowIdx < insertRowsCount; ++rowIdx) {
                insertQuery << (rowIdx ? "," : "") << " (Timestamp('1970-01-01T00:00:0" << rowIdx % 10 << "Z'), 'uid_" << rowIdx
                            << "', '" << rowIdx << "', " << rowIdx << ", " << rowIdx << "u)";
            }
            insertQuery << ";";
            auto result = db.ExecuteQuery(insertQuery, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        // actualization rewrites the existing portions to carry the index chunk (compaction is off)
        executeSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);");
        AdvancePlanStep(kikimr);
        csController->WaitActualization(TDuration::Seconds(10));

        const auto buildStatsQuery = [&](const bool desc, const std::optional<ui32> limit) {
            const TString direction = desc ? " DESC" : "";
            TStringBuilder query;
            query << "SELECT PathId, TabletId, PortionId, InternalEntityId, ChunkIdx" << Endl
                  << "FROM `" << tablePath << "/.sys/primary_index_stats`" << Endl
                  << "ORDER BY PathId" << direction << ", TabletId" << direction << ", PortionId" << direction << ", InternalEntityId"
                  << direction << ", ChunkIdx" << direction << Endl;
            if (limit) {
                query << "LIMIT " << *limit << Endl;
            }
            return TString(query);
        };
        using TRowKey = std::tuple<ui64, ui64, ui64, ui64, ui64>;
        const auto readKeys = [](const auto& rows) {
            std::vector<TRowKey> keys;
            for (auto&& row : rows) {
                keys.emplace_back(GetUint64(row.at("PathId")), GetUint64(row.at("TabletId")), GetUint64(row.at("PortionId")),
                    GetUint32(row.at("InternalEntityId")), GetUint64(row.at("ChunkIdx")));
            }
            return keys;
        };

        auto fullRows = ExecuteScanQuery(tableClient, buildStatsQuery(false, std::nullopt));
        auto keys = readKeys(fullRows);
        UNIT_ASSERT(keys.size());
        // precondition: the single tablet must hold more portions than the cap, otherwise passthrough never triggers
        THashSet<ui64> portionIds;
        for (auto&& key : keys) {
            portionIds.insert(std::get<2>(key));
        }
        UNIT_ASSERT_GT(portionIds.size(), 1u);

        const auto keyToString = [](const TRowKey& key) {
            return TStringBuilder() << "(" << std::get<0>(key) << "," << std::get<1>(key) << "," << std::get<2>(key) << ","
                                    << std::get<3>(key) << "," << std::get<4>(key) << ")";
        };
        auto sortedKeys = keys;
        std::sort(sortedKeys.begin(), sortedKeys.end());

        for (ui32 limit = 1; limit <= Min<ui32>(keys.size(), limitSweepMax); ++limit) {
            auto rows = ExecuteScanQuery(tableClient, buildStatsQuery(true, limit));
            auto limitKeys = readKeys(rows);
            UNIT_ASSERT_VALUES_EQUAL(limitKeys.size(), limit);
            for (ui32 i = 0; i < limit; ++i) {
                const auto& expected = sortedKeys[sortedKeys.size() - 1 - i];
                UNIT_ASSERT_C(limitKeys[i] == expected,
                    TStringBuilder() << "wrong row for DESC limit " << limit << " at " << i << ": " << keyToString(limitKeys[i])
                                     << " != " << keyToString(expected));
            }
        }

        // ordering by the constant PathId/TabletId prefix ties every source, so the heap cannot drain and the cap bounds it
        UNIT_ASSERT_VALUES_EQUAL(csController->GetSysViewLimitPassthroughsCount().Val(), 0);
        ExecuteScanQuery(tableClient, TStringBuilder() << "SELECT PathId, TabletId FROM `" << tablePath
                                                      << "/.sys/primary_index_stats` ORDER BY PathId, TabletId LIMIT 3");
        UNIT_ASSERT_GT(csController->GetSysViewLimitPassthroughsCount().Val(), 0);
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
//                    YDB_LOG_INFO("",
//                          {"json", i});
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
//                    YDB_LOG_INFO("",
//                          {"json", i});
//                }
            }
        }
    }

    Y_UNIT_TEST(StatsSysViewColumns) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
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
        auto settings = TKikimrSettings().SetWithSampleTables(false);
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
            UNIT_ASSERT_C(IsIn({"SPLIT_COMPACTED", "INSERTED"}, GetUtf8(rows[0].at("Kind"))), GetUtf8(rows[0].at("Kind")));
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[1].at("PathId")), tablePathId1);
            UNIT_ASSERT_C(IsIn({"SPLIT_COMPACTED", "INSERTED"}, GetUtf8(rows[2].at("Kind"))), GetUtf8(rows[2].at("Kind")));
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[2].at("PathId")), tablePathId1);
            UNIT_ASSERT_C(IsIn({"SPLIT_COMPACTED", "INSERTED"}, GetUtf8(rows[1].at("Kind"))), GetUtf8(rows[1].at("Kind")));
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
            UNIT_ASSERT_GE(rows.size(), numExpected);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), tablePathId3);
            UNIT_ASSERT_C(IsIn({"SPLIT_COMPACTED", "INSERTED"}, GetUtf8(rows[0].at("Kind"))), GetUtf8(rows[0].at("Kind")));
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[numExpected - 1].at("PathId")), tablePathId1);
            UNIT_ASSERT_C(IsIn({"SPLIT_COMPACTED", "INSERTED"}, GetUtf8(rows[numExpected - 1].at("Kind"))), GetUtf8(rows[numExpected - 1].at("Kind")));
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
            UNIT_ASSERT_GE(rows.size(), numExpected);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), tablePathId3);
            UNIT_ASSERT_C(IsIn({"SPLIT_COMPACTED", "INSERTED"}, GetUtf8(rows[0].at("Kind"))), GetUtf8(rows[0].at("Kind")));
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[numExpected - 1].at("PathId")), tablePathId1);
            UNIT_ASSERT_C(IsIn({"SPLIT_COMPACTED", "INSERTED"}, GetUtf8(rows[numExpected - 1].at("Kind"))), GetUtf8(rows[numExpected - 1].at("Kind")));
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

    Y_UNIT_TEST(FetchAddedColumnWithProgramTracing) {
        class TDummyProbeExecutor: public NLWTrace::IExecutor {
        protected:
            bool DoExecute(NLWTrace::TOrbit& /*orbit*/, const NLWTrace::TParams& /*params*/) override {
                return true;
            }
        };

        auto& probe = NOlap::NReader::NLWTrace_YDB_CS_DATA_SOURCE::lwtrace_ProgramFetchOriginalData;
        TDummyProbeExecutor executor;
        UNIT_ASSERT(probe.Probe.Attach(&executor));
        Y_DEFER {
            probe.Probe.Detach(&executor);
        };

        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();

        TLocalHelper(kikimr).CreateTestOlapTable();
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 100);
        csController->WaitCompactions(TDuration::Seconds(5));

        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();
        {
            auto alterResult = session.ExecuteSchemeQuery(
                "ALTER TABLESTORE `/Root/olapStore` ADD COLUMN new_column_ui64 Uint64;").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        auto rows = ExecuteScanQuery(tableClient, R"(
            SELECT timestamp, uid, new_column_ui64
            FROM `/Root/olapStore/olapTable`
        )");
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 100);
    }
}

} // namespace

#include "helpers/get_value.h"
#include "helpers/query_executor.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/algorithm.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpOlapPortionId) {
    Y_UNIT_TEST(SystemColumnAndPushdown) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        TKikimrRunner kikimr(settings);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->DisableBackground(NYDBTest::ICSController::EBackground::Compaction);

        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();
        {
            const auto result = session
                                    .ExecuteSchemeQuery(R"(
                CREATE TABLE `/Root/ColumnTable` (
                    Key Uint64 NOT NULL,
                    Value String,
                    PRIMARY KEY (Key)
                )
                WITH (STORE = COLUMN, PARTITION_COUNT = 1);
            )")
                                    .GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto queryClient = kikimr.GetQueryClient();
        {
            auto result = queryClient
                              .ExecuteQuery(R"(
                    INSERT INTO `/Root/ColumnTable` (Key, Value) VALUES (1u, "one"), (2u, "two");
                )",
                                  NYdb::NQuery::TTxControl::BeginTx().CommitTx())
                              .ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = queryClient
                              .ExecuteQuery(R"(
                    INSERT INTO `/Root/ColumnTable` (Key, Value) VALUES (3u, "three"), (4u, "four");
                )",
                                  NYdb::NQuery::TTxControl::BeginTx().CommitTx())
                              .ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        THashMap<ui64, ui64> portionRows;
        {
            auto rows = ExecuteScanQuery(tableClient, R"(
                SELECT PortionId, Rows
                FROM `/Root/ColumnTable/.sys/primary_index_portion_stats`
                WHERE Activity == 1
                ORDER BY PortionId
            )");
            UNIT_ASSERT_C(rows.size() >= 2, rows.size());
            for (const auto& row : rows) {
                portionRows[GetUint64(row.at("PortionId"))] = GetUint64(row.at("Rows"));
            }
        }

        {
            auto rows = ExecuteScanQuery(tableClient, R"(
                SELECT *
                FROM `/Root/ColumnTable`
                ORDER BY Key
            )");
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 4);
            UNIT_ASSERT(!rows.front().contains("_yql_portion_id"));
            UNIT_ASSERT(rows.front().contains("Key"));
            UNIT_ASSERT(rows.front().contains("Value"));
        }

        {
            auto it = tableClient.StreamExecuteScanQuery(R"(
                SELECT _yql_portion_id FROM `/Root/ColumnTable`
            )")
                          .GetValueSync();
            TString issues;
            if (it.IsSuccess()) {
                auto streamPart = it.ReadNext().GetValueSync();
                UNIT_ASSERT_C(!streamPart.IsSuccess(), streamPart.GetIssues().ToString());
                issues = streamPart.GetIssues().ToString();
            } else {
                issues = it.GetIssues().ToString();
            }
            UNIT_ASSERT_C(issues.Contains("Column not found") || issues.Contains("_yql_portion_id"), issues);
        }

        {
            auto rows = ExecuteScanQuery(tableClient, R"(
                PRAGMA kikimr.EnableSystemColumns = "true";
                SELECT _yql_portion_id AS PortionId, COUNT(*) AS Cnt
                FROM `/Root/ColumnTable`
                GROUP BY _yql_portion_id
                ORDER BY PortionId
            )");
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), portionRows.size());
            for (const auto& row : rows) {
                const ui64 portionId = GetUint64(row.at("PortionId"));
                UNIT_ASSERT(portionRows.contains(portionId));
                UNIT_ASSERT_VALUES_EQUAL(GetUint64(row.at("Cnt")), portionRows[portionId]);
            }
        }

        const ui64 firstPortionId = portionRows.begin()->first;
        const ui64 firstPortionRows = portionRows.begin()->second;
        {
            auto rows = ExecuteScanQuery(tableClient, TStringBuilder() << R"(
                PRAGMA kikimr.EnableSystemColumns = "true";
                SELECT Key
                FROM `/Root/ColumnTable`
                WHERE _yql_portion_id = )" << firstPortionId << R"(
                ORDER BY Key
            )");
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), firstPortionRows);
        }

        if (portionRows.size() >= 2) {
            auto it = portionRows.begin();
            const ui64 a = it->first;
            const ui64 aRows = it->second;
            ++it;
            const ui64 b = it->first;
            const ui64 bRows = it->second;
            auto rows = ExecuteScanQuery(tableClient, TStringBuilder() << R"(
                PRAGMA kikimr.EnableSystemColumns = "true";
                SELECT Key
                FROM `/Root/ColumnTable`
                WHERE _yql_portion_id IN ()" << a << ", " << b << R"()
                ORDER BY Key
            )");
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), aRows + bRows);
        }

        {
            TVector<ui64> sortedIds;
            for (const auto& [id, _] : portionRows) {
                sortedIds.push_back(id);
            }
            Sort(sortedIds);
            const ui64 minId = sortedIds.front();
            const ui64 maxId = sortedIds.back();

            ui64 greaterRows = 0;
            ui64 lessRows = 0;
            ui64 betweenRows = 0;
            for (const auto& [id, rows] : portionRows) {
                if (id > minId) {
                    greaterRows += rows;
                }
                if (id < maxId) {
                    lessRows += rows;
                }
                if (id >= minId && id <= maxId) {
                    betweenRows += rows;
                }
            }

            {
                auto rows = ExecuteScanQuery(tableClient, TStringBuilder() << R"(
                    PRAGMA kikimr.EnableSystemColumns = "true";
                    SELECT Key
                    FROM `/Root/ColumnTable`
                    WHERE _yql_portion_id > )" << minId << R"(
                    ORDER BY Key
                )");
                UNIT_ASSERT_VALUES_EQUAL(rows.size(), greaterRows);
            }
            {
                auto rows = ExecuteScanQuery(tableClient, TStringBuilder() << R"(
                    PRAGMA kikimr.EnableSystemColumns = "true";
                    SELECT Key
                    FROM `/Root/ColumnTable`
                    WHERE _yql_portion_id < )" << maxId << R"(
                    ORDER BY Key
                )");
                UNIT_ASSERT_VALUES_EQUAL(rows.size(), lessRows);
            }
            {
                auto rows = ExecuteScanQuery(tableClient, TStringBuilder() << R"(
                    PRAGMA kikimr.EnableSystemColumns = "true";
                    SELECT Key
                    FROM `/Root/ColumnTable`
                    WHERE _yql_portion_id >= )" << minId << R"( AND _yql_portion_id <= )" << maxId << R"(
                    ORDER BY Key
                )");
                UNIT_ASSERT_VALUES_EQUAL(rows.size(), betweenRows);
            }
            {
                auto rows = ExecuteScanQuery(tableClient, TStringBuilder() << R"(
                    PRAGMA kikimr.EnableSystemColumns = "true";
                    SELECT Key
                    FROM `/Root/ColumnTable`
                    WHERE _yql_portion_id > )" << maxId << R"(
                    ORDER BY Key
                )");
                UNIT_ASSERT_VALUES_EQUAL(rows.size(), 0);
            }
        }

        {
            NYdb::NTable::TStreamExecScanQuerySettings scanSettings;
            scanSettings.Explain(true);
            auto it = tableClient
                          .StreamExecuteScanQuery(TStringBuilder() << R"(
                PRAGMA kikimr.EnableSystemColumns = "true";
                SELECT Key
                FROM `/Root/ColumnTable`
                WHERE _yql_portion_id = )" << firstPortionId,
                              scanSettings)
                          .GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            auto result = CollectStreamResult(it);
            UNIT_ASSERT(result.QueryStats);
            const auto& ast = result.QueryStats->Getquery_ast();
            UNIT_ASSERT_C(ast.find("KqpOlapFilter") != std::string::npos, ast);
        }
    }

    Y_UNIT_TEST(SystemColumnPartitionId) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        TKikimrRunner kikimr(settings);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->DisableBackground(NYDBTest::ICSController::EBackground::Compaction);

        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();
        {
            const auto result = session
                                    .ExecuteSchemeQuery(R"(
                CREATE TABLE `/Root/ColumnTable` (
                    Key Uint64 NOT NULL,
                    Value String,
                    PRIMARY KEY (Key)
                )
                WITH (STORE = COLUMN, PARTITION_COUNT = 3);
            )")
                                    .GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto queryClient = kikimr.GetQueryClient();
        {
            auto result = queryClient
                              .ExecuteQuery(R"(
                    INSERT INTO `/Root/ColumnTable` (Key, Value) VALUES
                        (1u, "a"), (2u, "b"), (3u, "c"), (4u, "d"), (5u, "e"),
                        (6u, "f"), (7u, "g"), (8u, "h"), (9u, "i"), (10u, "j");
                )",
                                  NYdb::NQuery::TTxControl::BeginTx().CommitTx())
                              .ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        THashMap<ui64, ui64> tabletRows;
        {
            auto rows = ExecuteScanQuery(tableClient, R"(
                SELECT TabletId, SUM(Rows) AS Rows
                FROM `/Root/ColumnTable/.sys/primary_index_portion_stats`
                WHERE Activity == 1
                GROUP BY TabletId
                ORDER BY TabletId
            )");
            UNIT_ASSERT_C(rows.size() >= 1, rows.size());
            for (const auto& row : rows) {
                tabletRows[GetUint64(row.at("TabletId"))] = GetUint64(row.at("Rows"));
            }
        }

        {
            auto rows = ExecuteScanQuery(tableClient, R"(
                SELECT *
                FROM `/Root/ColumnTable`
                ORDER BY Key
            )");
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 10);
            UNIT_ASSERT(!rows.front().contains("_yql_partition_id"));
            UNIT_ASSERT(rows.front().contains("Key"));
            UNIT_ASSERT(rows.front().contains("Value"));
        }

        {
            auto it = tableClient.StreamExecuteScanQuery(R"(
                SELECT _yql_partition_id FROM `/Root/ColumnTable`
            )")
                          .GetValueSync();
            TString issues;
            if (it.IsSuccess()) {
                auto streamPart = it.ReadNext().GetValueSync();
                UNIT_ASSERT_C(!streamPart.IsSuccess(), streamPart.GetIssues().ToString());
                issues = streamPart.GetIssues().ToString();
            } else {
                issues = it.GetIssues().ToString();
            }
            UNIT_ASSERT_C(issues.Contains("Column not found") || issues.Contains("_yql_partition_id"), issues);
        }

        {
            auto rows = ExecuteScanQuery(tableClient, R"(
                PRAGMA kikimr.EnableSystemColumns = "true";
                SELECT _yql_partition_id AS PartitionId, COUNT(*) AS Cnt
                FROM `/Root/ColumnTable`
                GROUP BY _yql_partition_id
                ORDER BY PartitionId
            )");
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), tabletRows.size());
            for (const auto& row : rows) {
                const ui64 partitionId = GetUint64(row.at("PartitionId"));
                UNIT_ASSERT(tabletRows.contains(partitionId));
                UNIT_ASSERT_VALUES_EQUAL(GetUint64(row.at("Cnt")), tabletRows[partitionId]);
            }
        }

        const ui64 firstTabletId = tabletRows.begin()->first;
        const ui64 firstTabletRows = tabletRows.begin()->second;
        {
            auto rows = ExecuteScanQuery(tableClient, TStringBuilder() << R"(
                PRAGMA kikimr.EnableSystemColumns = "true";
                SELECT Key
                FROM `/Root/ColumnTable`
                WHERE _yql_partition_id = )" << firstTabletId << R"(
                ORDER BY Key
            )");
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), firstTabletRows);
        }

        if (tabletRows.size() >= 2) {
            auto it = tabletRows.begin();
            const ui64 a = it->first;
            const ui64 aRows = it->second;
            ++it;
            const ui64 b = it->first;
            const ui64 bRows = it->second;
            auto rows = ExecuteScanQuery(tableClient, TStringBuilder() << R"(
                PRAGMA kikimr.EnableSystemColumns = "true";
                SELECT Key
                FROM `/Root/ColumnTable`
                WHERE _yql_partition_id IN ()" << a << ", " << b << R"()
                ORDER BY Key
            )");
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), aRows + bRows);
        }

        if (tabletRows.size() >= 2) {
            TVector<ui64> sortedIds;
            for (const auto& [id, _] : tabletRows) {
                sortedIds.push_back(id);
            }
            Sort(sortedIds);
            const ui64 minId = sortedIds.front();
            const ui64 maxId = sortedIds.back();

            ui64 greaterRows = 0;
            ui64 geMaxRows = 0;
            for (const auto& [id, rows] : tabletRows) {
                if (id > minId) {
                    greaterRows += rows;
                }
                if (id >= maxId) {
                    geMaxRows += rows;
                }
            }

            {
                auto rows = ExecuteScanQuery(tableClient, TStringBuilder() << R"(
                    PRAGMA kikimr.EnableSystemColumns = "true";
                    SELECT Key
                    FROM `/Root/ColumnTable`
                    WHERE _yql_partition_id > )" << minId << R"(
                    ORDER BY Key
                )");
                UNIT_ASSERT_VALUES_EQUAL(rows.size(), greaterRows);
            }
            {
                auto rows = ExecuteScanQuery(tableClient, TStringBuilder() << R"(
                    PRAGMA kikimr.EnableSystemColumns = "true";
                    SELECT Key
                    FROM `/Root/ColumnTable`
                    WHERE _yql_partition_id >= )" << maxId << R"(
                    ORDER BY Key
                )");
                UNIT_ASSERT_VALUES_EQUAL(rows.size(), geMaxRows);
            }
        }

        {
            NYdb::NTable::TStreamExecScanQuerySettings scanSettings;
            scanSettings.Explain(true);
            auto it = tableClient
                          .StreamExecuteScanQuery(TStringBuilder() << R"(
                PRAGMA kikimr.EnableSystemColumns = "true";
                SELECT Key
                FROM `/Root/ColumnTable`
                WHERE _yql_partition_id = )" << firstTabletId,
                              scanSettings)
                          .GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            auto result = CollectStreamResult(it);
            UNIT_ASSERT(result.QueryStats);
            const auto& ast = result.QueryStats->Getquery_ast();
            UNIT_ASSERT_C(ast.find("KqpOlapFilter") != std::string::npos, ast);
        }
    }
}

}   // namespace NKikimr::NKqp

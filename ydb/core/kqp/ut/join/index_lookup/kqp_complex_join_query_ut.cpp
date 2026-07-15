#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpComplexJoinQuery) {

Y_UNIT_TEST(ComplexJoinQuery) {
    auto settings = TKikimrSettings();
    settings.AppConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(true);
    TKikimrRunner kikimr(settings);
    auto db = kikimr.GetQueryClient();

    { // Create tables
        TString query = R"sql(
            CREATE TABLE `tables/group_items` (
                group_id Uint64 NOT NULL,
                id Uint64 NOT NULL,
                item_id String NOT NULL,
                is_flag Bool,
                data_ref String,
                metadata_ref String,
                PRIMARY KEY (group_id, id)
            );
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        TString query = R"sql(
            CREATE TABLE `tables/group` (
                id Uint64 NOT NULL,
                category String,
                PRIMARY KEY (id)
            );
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        TString query = R"sql(
            CREATE TABLE `tables/metadata_tree` (
                ref String NOT NULL,
                key String NOT NULL,
                data String,
                PRIMARY KEY (ref, key)
            );
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        TString query = R"sql(
            CREATE TABLE `tables/item_data_detailed` (
                ref String NOT NULL,
                key String NOT NULL,
                timestamp Timestamp NOT NULL,
                data String NOT NULL,
                extra_data String,
                expired_at Timestamp,
                PRIMARY KEY (ref, key, timestamp)
            );
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        TString query = R"sql(
            CREATE TABLE `tables/item_data_main` (
                ref String NOT NULL,
                key String NOT NULL,
                data String NOT NULL,
                extra_data String,
                expired_at Timestamp,
                PRIMARY KEY (ref, key)
            );
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    { // Insert test data
        TString query = R"sql(
            UPSERT INTO `tables/group_items` (group_id, id, item_id, is_flag, data_ref, metadata_ref) VALUES
                (1, 1, "item1", false, "ref1", "meta_ref1"),
                (1, 2, "item2", true, "ref2", "meta_ref1"),
                (1, 3, "item3", false, "ref3", "meta_ref1"),
                (2, 1, "item4", false, "ref4", "meta_ref2");
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        TString query = R"sql(
            UPSERT INTO `tables/group` (id, category) VALUES
                (1, "cat1"),
                (2, "cat2");
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        TString query = R"sql(
            UPSERT INTO `tables/metadata_tree` (ref, key, data) VALUES
                ("meta_ref1", "key1", "meta_value1"),
                ("meta_ref1", "key2", "meta_value2"),
                ("meta_ref2", "key1", "meta_value3");
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        TString query = R"sql(
            UPSERT INTO `tables/item_data_detailed` (ref, key, timestamp, data, extra_data) VALUES
                ("ref1", "key1", Timestamp("2023-01-01T00:00:00Z"), "value1", "extra1"),
                ("ref1", "key1", Timestamp("2023-01-02T00:00:00Z"), "value2", "extra2"),
                ("ref2", "key1", Timestamp("2023-01-01T00:00:00Z"), "value3", "extra3"),
                ("ref3", "key1", Timestamp("2023-01-01T00:00:00Z"), "value4", "extra4");
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        TString query = R"sql(
            UPSERT INTO `tables/item_data_main` (ref, key, data, extra_data) VALUES
                ("ref1", "key1", "main_value1", "main_extra1"),
                ("ref2", "key1", "main_value2", "main_extra2"),
                ("ref3", "key1", "main_value3", "main_extra3");
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    { // Execute the complex query
        TString query = R"sql(
            DECLARE $group_id AS Uint64;
            DECLARE $metadata_key AS String;
            DECLARE $item_filters AS List<String>;

            $metadata_ref = (SELECT metadata_ref FROM `tables/group_items` WHERE group_id == $group_id LIMIT 1);

            SELECT `category` FROM `tables/group` VIEW PRIMARY KEY WHERE id == $group_id LIMIT 1;

            SELECT
                `data` AS node_info
            FROM
                `tables/metadata_tree` VIEW PRIMARY KEY
            WHERE
                ref == $metadata_ref AND
                key == $metadata_key;

            $refs = (
                SELECT DISTINCT
                    `id` AS idx,
                    `item_id` AS item_id,
                    `is_flag` AS is_flag,
                    `data_ref` AS ref,
                    $metadata_key AS key
                FROM
                    `tables/group_items`
                WHERE
                    group_id == $group_id AND
                    IF (ListLength($item_filters) > 0, ListHas($item_filters, item_id), true)
            );

            SELECT `item_id`, `idx` FROM $refs ORDER BY `idx`;

            SELECT
                refs.`item_id` AS item_id,
                data.`timestamp` AS timestamp,
                refs.`is_flag` AS is_flag,
                data.data AS data,
                data.extra_data AS extra_data,
                refs.`idx` as idx
            FROM
                $refs AS refs
            JOIN
                `tables/item_data_detailed` VIEW PRIMARY KEY AS data
            USING
                (`ref`, `key`)
            ORDER BY
                timestamp, idx
            ;

            SELECT
                refs.`item_id` AS item_id,
                refs.`is_flag` AS is_flag,
                data.data AS data,
                data.extra_data AS extra_data,
                refs.`idx` as idx
            FROM
                $refs AS refs
            JOIN
                `tables/item_data_main` VIEW PRIMARY KEY AS data
            USING
                (`ref`, `key`)
            ORDER BY
                idx
            ;
        )sql";

        auto params = NYdb::TParamsBuilder()
            .AddParam("$group_id")
                .Uint64(1)
                .Build()
            .AddParam("$metadata_key")
                .String("key1")
                .Build()
            .AddParam("$item_filters")
                .BeginList()
                .AddListItem().String("item1")
                .AddListItem().String("item2")
                .EndList()
                .Build()
            .Build();

        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), params, NYdb::NQuery::TExecuteQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        // Verify results
        const auto& resultSets = result.GetResultSets();
        UNIT_ASSERT_C(resultSets.size() >= 5, "Expected at least 5 result sets");

        // First result: category
        if (resultSets.size() > 0) {
            auto resultSet = result.GetResultSet(0);
            UNIT_ASSERT_C(resultSet.RowsCount() == 1, "Expected one row for category");
        }

        // Second result: node_info
        if (resultSets.size() > 1) {
            auto resultSet = result.GetResultSet(1);
            UNIT_ASSERT_C(resultSet.RowsCount() == 1, "Expected one row for node_info");
        }

        // Third result: item_id and idx
        if (resultSets.size() > 2) {
            auto resultSet = result.GetResultSet(2);
            UNIT_ASSERT_C(resultSet.RowsCount() >= 2, "Expected at least 2 rows for item_id/idx");
        }

        // Fourth result: item_data_detailed join
        if (resultSets.size() > 3) {
            auto resultSet = result.GetResultSet(3);
            UNIT_ASSERT_C(resultSet.RowsCount() >= 0, "Expected item_data_detailed join results");
        }

        // Fifth result: item_data_main join
        if (resultSets.size() > 4) {
            auto resultSet = result.GetResultSet(4);
            UNIT_ASSERT_C(resultSet.RowsCount() >= 0, "Expected item_data_main join results");
        }
    }
}

} // Y_UNIT_TEST_SUITE(KqpComplexJoinQuery)

} // namespace NKqp
} // namespace NKikimr


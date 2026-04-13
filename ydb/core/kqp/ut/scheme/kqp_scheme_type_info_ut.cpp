#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/test_client.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>

#include <util/system/datetime.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(KqpSchemeTypeInfo) {

    // Test case reproducing issue #24742
    // The problem is triggered when:
    // 1. Decimal is the FIRST element of the key
    // 2. When trying to search NULL / NOT NULL decimals in tables when using indexes / main tables
    // VERIFY TTypeInfo(): requirement !NTypeIds::IsParametrizedType(typeId) failed

    // Test with Decimal as FIRST key column and NULL search on main table
    Y_UNIT_TEST(DecimalAsFirstKeyWithNullSearchOnMainTable) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        // Create table with Decimal as FIRST key column
        auto createResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/TestDecimalFirstKey` (
                DecKey Decimal(22, 9),
                Value String,
                PRIMARY KEY (DecKey)
            );
        )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(createResult.GetStatus(), EStatus::SUCCESS, createResult.GetIssues().ToString());

        // Insert data with Decimal key values
        auto insertResult = session.ExecuteDataQuery(R"(
            INSERT INTO `/Root/TestDecimalFirstKey` (DecKey, Value) VALUES
                (CAST("10.123456789" AS Decimal(22, 9)), "value1"),
                (CAST("20.987654321" AS Decimal(22, 9)), "value2"),
                (CAST("30.555555555" AS Decimal(22, 9)), "value3");
        )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(insertResult.GetStatus(), EStatus::SUCCESS, insertResult.GetIssues().ToString());

        // Search by Decimal key - should not crash
        auto selectResult = session.ExecuteDataQuery(R"(
            SELECT DecKey, Value FROM `/Root/TestDecimalFirstKey`
            WHERE DecKey = CAST("10.123456789" AS Decimal(22, 9));
        )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(selectResult.GetStatus(), EStatus::SUCCESS, selectResult.GetIssues().ToString());

        // Validate results
        CompareYson(R"([
            [["10.123456789"];["value1"]]
        ])", FormatResultSetYson(selectResult.GetResultSet(0)));
    }

    // Test with Decimal as FIRST key column and NULL search on main table (with nullable Decimal column)
    Y_UNIT_TEST(DecimalAsFirstKeyWithNullableDecimalSearchOnMainTable) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        // Create table with Decimal as FIRST key (NOT NULL) and nullable Decimal column
        auto createResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/TestDecimalWithNullable` (
                DecKey Decimal(22, 9),
                DecValue Decimal(22, 9),
                Value String,
                PRIMARY KEY (DecKey)
            );
        )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(createResult.GetStatus(), EStatus::SUCCESS, createResult.GetIssues().ToString());

        // Insert data with some NULL Decimal values
        auto insertResult = session.ExecuteDataQuery(R"(
            INSERT INTO `/Root/TestDecimalWithNullable` (DecKey, DecValue, Value) VALUES
                (CAST("10.123456789" AS Decimal(22, 9)), CAST("1.111111111" AS Decimal(22, 9)), "value1"),
                (CAST("20.987654321" AS Decimal(22, 9)), NULL, "value2"),
                (CAST("30.555555555" AS Decimal(22, 9)), CAST("3.333333333" AS Decimal(22, 9)), "value3");
        )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(insertResult.GetStatus(), EStatus::SUCCESS, insertResult.GetIssues().ToString());

        // Search for NULL Decimal values - this should trigger the bug
        auto selectNullResult = session.ExecuteDataQuery(R"(
            SELECT DecKey, DecValue, Value FROM `/Root/TestDecimalWithNullable`
            WHERE DecValue IS NULL;
        )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(selectNullResult.GetStatus(), EStatus::SUCCESS, selectNullResult.GetIssues().ToString());

        // Validate results - should return row with NULL DecValue
        CompareYson(R"([
            [["20.987654321"];#;["value2"]]
        ])", FormatResultSetYson(selectNullResult.GetResultSet(0)));

        // Search for NOT NULL Decimal values - this should also trigger the bug
        auto selectNotNullResult = session.ExecuteDataQuery(R"(
            SELECT DecKey, DecValue, Value FROM `/Root/TestDecimalWithNullable`
            WHERE DecValue IS NOT NULL
            ORDER BY DecKey;
        )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(selectNotNullResult.GetStatus(), EStatus::SUCCESS, selectNotNullResult.GetIssues().ToString());

        // Validate results - should return rows with non-NULL DecValue
        CompareYson(R"([
            [["10.123456789"];["1.111111111"];["value1"]];
            [["30.555555555"];["3.333333333"];["value3"]]
        ])", FormatResultSetYson(selectNotNullResult.GetResultSet(0)));
    }

    // Test with Decimal as FIRST key column and NULL search using INDEX on DecValue
    // Note: This test creates an index on DecValue column (not on the key) to test index usage
    Y_UNIT_TEST(DecimalAsFirstKeyWithNullSearchUsingIndex) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        // Create table with Decimal as FIRST key column and nullable Decimal column
        auto createResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/TestDecimalWithIndex` (
                DecKey Decimal(22, 9),
                DecValue Decimal(22, 9),
                Value String,
                PRIMARY KEY (DecKey)
            );
        )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(createResult.GetStatus(), EStatus::SUCCESS, createResult.GetIssues().ToString());

        // Create index on nullable Decimal column - this index will be used for queries on DecValue
        auto indexResult = session.ExecuteSchemeQuery(R"(
            ALTER TABLE `/Root/TestDecimalWithIndex` ADD INDEX DecValueIndex GLOBAL SYNC ON (DecValue);
        )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(indexResult.GetStatus(), EStatus::SUCCESS, indexResult.GetIssues().ToString());

        // Insert data with some NULL Decimal values
        auto insertResult = session.ExecuteDataQuery(R"(
            INSERT INTO `/Root/TestDecimalWithIndex` (DecKey, DecValue, Value) VALUES
                (CAST("10.123456789" AS Decimal(22, 9)), CAST("1.111111111" AS Decimal(22, 9)), "value1"),
                (CAST("20.987654321" AS Decimal(22, 9)), NULL, "value2"),
                (CAST("30.555555555" AS Decimal(22, 9)), CAST("3.333333333" AS Decimal(22, 9)), "value3"),
                (CAST("40.444444444" AS Decimal(22, 9)), NULL, "value4");
        )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(insertResult.GetStatus(), EStatus::SUCCESS, insertResult.GetIssues().ToString());

        // Search for NULL Decimal values using index - this should trigger the bug
        auto selectNullResult = session.ExecuteDataQuery(R"(
            SELECT DecKey, DecValue, Value FROM `/Root/TestDecimalWithIndex`
            WHERE DecValue IS NULL
            ORDER BY DecKey;
        )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(selectNullResult.GetStatus(), EStatus::SUCCESS, selectNullResult.GetIssues().ToString());

        // Validate results - should return rows with NULL DecValue
        CompareYson(R"([
            [["20.987654321"];#;["value2"]];
            [["40.444444444"];#;["value4"]]
        ])", FormatResultSetYson(selectNullResult.GetResultSet(0)));

        // Search for NOT NULL Decimal values using index - this should also trigger the bug
        auto selectNotNullResult = session.ExecuteDataQuery(R"(
            SELECT DecKey, DecValue, Value FROM `/Root/TestDecimalWithIndex`
            WHERE DecValue IS NOT NULL
            ORDER BY DecKey;
        )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(selectNotNullResult.GetStatus(), EStatus::SUCCESS, selectNotNullResult.GetIssues().ToString());

        // Validate results - should return rows with non-NULL DecValue
        CompareYson(R"([
            [["10.123456789"];["1.111111111"];["value1"]];
            [["30.555555555"];["3.333333333"];["value3"]]
        ])", FormatResultSetYson(selectNotNullResult.GetResultSet(0)));
    }

    // Test with Decimal as FIRST key column and specific Decimal value search using INDEX
    Y_UNIT_TEST(DecimalAsFirstKeyWithDecimalValueSearchUsingIndex) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        // Create table with Decimal as FIRST key column
        auto createResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/TestDecimalIndexValue` (
                DecKey Decimal(22, 9),
                DecValue Decimal(22, 9),
                Value String,
                PRIMARY KEY (DecKey)
            );
        )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(createResult.GetStatus(), EStatus::SUCCESS, createResult.GetIssues().ToString());

        // Create index on Decimal column
        auto indexResult = session.ExecuteSchemeQuery(R"(
            ALTER TABLE `/Root/TestDecimalIndexValue` ADD INDEX DecValueIndex GLOBAL SYNC ON (DecValue);
        )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(indexResult.GetStatus(), EStatus::SUCCESS, indexResult.GetIssues().ToString());

        // Insert data
        auto insertResult = session.ExecuteDataQuery(R"(
            INSERT INTO `/Root/TestDecimalIndexValue` (DecKey, DecValue, Value) VALUES
                (CAST("10.123456789" AS Decimal(22, 9)), CAST("1.111111111" AS Decimal(22, 9)), "value1"),
                (CAST("20.987654321" AS Decimal(22, 9)), CAST("2.222222222" AS Decimal(22, 9)), "value2"),
                (CAST("30.555555555" AS Decimal(22, 9)), CAST("1.111111111" AS Decimal(22, 9)), "value3");
        )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(insertResult.GetStatus(), EStatus::SUCCESS, insertResult.GetIssues().ToString());

        // Wait for index to be built
        Sleep(TDuration::Seconds(1));

        // Search by specific Decimal value using index - this should trigger the bug
        auto selectResult = session.ExecuteDataQuery(R"(
            SELECT DecKey, DecValue, Value FROM `/Root/TestDecimalIndexValue`
            WHERE DecValue = CAST("1.111111111" AS Decimal(22, 9))
            ORDER BY DecKey;
        )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(selectResult.GetStatus(), EStatus::SUCCESS, selectResult.GetIssues().ToString());

        // Validate results - should return rows with matching DecValue
        CompareYson(R"([
            [["10.123456789"];["1.111111111"];["value1"]];
            [["30.555555555"];["1.111111111"];["value3"]]
        ])", FormatResultSetYson(selectResult.GetResultSet(0)));
    }

    // Test with composite key where Decimal is FIRST, and NULL search
    Y_UNIT_TEST(DecimalAsFirstInCompositeKeyWithNullSearch) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        // Create table with Decimal as FIRST in composite key
        auto createResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/TestDecimalCompositeKey` (
                DecKey Decimal(22, 9),
                UintKey Uint64,
                DecValue Decimal(22, 9),
                Value String,
                PRIMARY KEY (DecKey, UintKey)
            );
        )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(createResult.GetStatus(), EStatus::SUCCESS, createResult.GetIssues().ToString());

        // Insert data
        auto insertResult = session.ExecuteDataQuery(R"(
            INSERT INTO `/Root/TestDecimalCompositeKey` (DecKey, UintKey, DecValue, Value) VALUES
                (CAST("10.123456789" AS Decimal(22, 9)), 1, NULL, "value1"),
                (CAST("20.987654321" AS Decimal(22, 9)), 2, CAST("2.222222222" AS Decimal(22, 9)), "value2"),
                (CAST("30.555555555" AS Decimal(22, 9)), 3, NULL, "value3");
        )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(insertResult.GetStatus(), EStatus::SUCCESS, insertResult.GetIssues().ToString());

        // Search for NULL Decimal values - this should trigger the bug
        auto selectNullResult = session.ExecuteDataQuery(R"(
            SELECT DecKey, UintKey, DecValue, Value FROM `/Root/TestDecimalCompositeKey`
            WHERE DecValue IS NULL
            ORDER BY DecKey, UintKey;
        )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(selectNullResult.GetStatus(), EStatus::SUCCESS, selectNullResult.GetIssues().ToString());

        // Validate results - should return rows with NULL DecValue
        CompareYson(R"([
            [["10.123456789"];[1u];#;["value1"]];
            [["30.555555555"];[3u];#;["value3"]]
        ])", FormatResultSetYson(selectNullResult.GetResultSet(0)));
    }

    // QueryService versions of the tests

    // Test with Decimal as FIRST key column and NULL search on main table using QueryService
    Y_UNIT_TEST(DecimalAsFirstKeyWithNullSearchOnMainTableQueryService) {
        TKikimrRunner kikimr;
        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();
        auto queryClient = kikimr.GetQueryClient();

        // Create table with Decimal as FIRST key column
        auto createResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/TestDecimalFirstKeyQS` (
                DecKey Decimal(22, 9),
                Value String,
                PRIMARY KEY (DecKey)
            );
        )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(createResult.GetStatus(), EStatus::SUCCESS, createResult.GetIssues().ToString());

        // Insert data with Decimal key values using QueryService
        auto insertResult = queryClient.ExecuteQuery(R"(
            INSERT INTO `/Root/TestDecimalFirstKeyQS` (DecKey, Value) VALUES
                (CAST("10.123456789" AS Decimal(22, 9)), "value1"),
                (CAST("20.987654321" AS Decimal(22, 9)), "value2"),
                (CAST("30.555555555" AS Decimal(22, 9)), "value3");
        )", NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(insertResult.GetStatus(), EStatus::SUCCESS, insertResult.GetIssues().ToString());

        // Search by Decimal key using QueryService - should not crash
        auto selectResult = queryClient.ExecuteQuery(R"(
            SELECT DecKey, Value FROM `/Root/TestDecimalFirstKeyQS`
            WHERE DecKey = CAST("10.123456789" AS Decimal(22, 9));
        )", NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(selectResult.GetStatus(), EStatus::SUCCESS, selectResult.GetIssues().ToString());

        // Validate results
        CompareYson(R"([
            [["10.123456789"];["value1"]]
        ])", FormatResultSetYson(selectResult.GetResultSet(0)));
    }

    // Test with Decimal as FIRST key column and NULL search using QueryService
    Y_UNIT_TEST(DecimalAsFirstKeyWithNullableDecimalSearchOnMainTableQueryService) {
        TKikimrRunner kikimr;
        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();
        auto queryClient = kikimr.GetQueryClient();

        // Create table with Decimal as FIRST key and nullable Decimal column
        auto createResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/TestDecimalWithNullableQS` (
                DecKey Decimal(22, 9),
                DecValue Decimal(22, 9),
                Value String,
                PRIMARY KEY (DecKey)
            );
        )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(createResult.GetStatus(), EStatus::SUCCESS, createResult.GetIssues().ToString());

        // Insert data with some NULL Decimal values using QueryService
        auto insertResult = queryClient.ExecuteQuery(R"(
            INSERT INTO `/Root/TestDecimalWithNullableQS` (DecKey, DecValue, Value) VALUES
                (CAST("10.123456789" AS Decimal(22, 9)), CAST("1.111111111" AS Decimal(22, 9)), "value1"),
                (CAST("20.987654321" AS Decimal(22, 9)), NULL, "value2"),
                (CAST("30.555555555" AS Decimal(22, 9)), CAST("3.333333333" AS Decimal(22, 9)), "value3");
        )", NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(insertResult.GetStatus(), EStatus::SUCCESS, insertResult.GetIssues().ToString());

        // Search for NULL Decimal values using QueryService - this should trigger the bug
        auto selectNullResult = queryClient.ExecuteQuery(R"(
            SELECT DecKey, DecValue, Value FROM `/Root/TestDecimalWithNullableQS`
            WHERE DecValue IS NULL;
        )", NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(selectNullResult.GetStatus(), EStatus::SUCCESS, selectNullResult.GetIssues().ToString());

        // Validate results - should return row with NULL DecValue
        CompareYson(R"([
            [["20.987654321"];#;["value2"]]
        ])", FormatResultSetYson(selectNullResult.GetResultSet(0)));

        // Search for NOT NULL Decimal values using QueryService - this should also trigger the bug
        auto selectNotNullResult = queryClient.ExecuteQuery(R"(
            SELECT DecKey, DecValue, Value FROM `/Root/TestDecimalWithNullableQS`
            WHERE DecValue IS NOT NULL
            ORDER BY DecKey;
        )", NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(selectNotNullResult.GetStatus(), EStatus::SUCCESS, selectNotNullResult.GetIssues().ToString());

        // Validate results - should return rows with non-NULL DecValue
        CompareYson(R"([
            [["10.123456789"];["1.111111111"];["value1"]];
            [["30.555555555"];["3.333333333"];["value3"]]
        ])", FormatResultSetYson(selectNotNullResult.GetResultSet(0)));
    }

    // Test with Decimal as FIRST key column and NULL search using INDEX with QueryService
    Y_UNIT_TEST(DecimalAsFirstKeyWithNullSearchUsingIndexQueryService) {
        TKikimrRunner kikimr;
        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();
        auto queryClient = kikimr.GetQueryClient();

        // Create table with Decimal as FIRST key column and nullable Decimal column
        auto createResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/TestDecimalWithIndexQS` (
                DecKey Decimal(22, 9),
                DecValue Decimal(22, 9),
                Value String,
                PRIMARY KEY (DecKey)
            );
        )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(createResult.GetStatus(), EStatus::SUCCESS, createResult.GetIssues().ToString());

        // Create index on nullable Decimal column
        auto indexResult = session.ExecuteSchemeQuery(R"(
            ALTER TABLE `/Root/TestDecimalWithIndexQS` ADD INDEX DecValueIndex GLOBAL SYNC ON (DecValue);
        )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(indexResult.GetStatus(), EStatus::SUCCESS, indexResult.GetIssues().ToString());

        // Insert data with some NULL Decimal values using QueryService
        auto insertResult = queryClient.ExecuteQuery(R"(
            INSERT INTO `/Root/TestDecimalWithIndexQS` (DecKey, DecValue, Value) VALUES
                (CAST("10.123456789" AS Decimal(22, 9)), CAST("1.111111111" AS Decimal(22, 9)), "value1"),
                (CAST("20.987654321" AS Decimal(22, 9)), NULL, "value2"),
                (CAST("30.555555555" AS Decimal(22, 9)), CAST("3.333333333" AS Decimal(22, 9)), "value3"),
                (CAST("40.444444444" AS Decimal(22, 9)), NULL, "value4");
        )", NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(insertResult.GetStatus(), EStatus::SUCCESS, insertResult.GetIssues().ToString());

        // Search for NULL Decimal values using index with QueryService - this should trigger the bug
        auto selectNullResult = queryClient.ExecuteQuery(R"(
            SELECT DecKey, DecValue, Value FROM `/Root/TestDecimalWithIndexQS`
            WHERE DecValue IS NULL
            ORDER BY DecKey;
        )", NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(selectNullResult.GetStatus(), EStatus::SUCCESS, selectNullResult.GetIssues().ToString());

        // Validate results - should return rows with NULL DecValue
        CompareYson(R"([
            [["20.987654321"];#;["value2"]];
            [["40.444444444"];#;["value4"]]
        ])", FormatResultSetYson(selectNullResult.GetResultSet(0)));

        // Search for NOT NULL Decimal values using index with QueryService - this should also trigger the bug
        auto selectNotNullResult = queryClient.ExecuteQuery(R"(
            SELECT DecKey, DecValue, Value FROM `/Root/TestDecimalWithIndexQS`
            WHERE DecValue IS NOT NULL
            ORDER BY DecKey;
        )", NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(selectNotNullResult.GetStatus(), EStatus::SUCCESS, selectNotNullResult.GetIssues().ToString());

        // Validate results - should return rows with non-NULL DecValue
        CompareYson(R"([
            [["10.123456789"];["1.111111111"];["value1"]];
            [["30.555555555"];["3.333333333"];["value3"]]
        ])", FormatResultSetYson(selectNotNullResult.GetResultSet(0)));
    }

    // Test with composite key where Decimal is FIRST, and NULL search using QueryService
    Y_UNIT_TEST(DecimalAsFirstInCompositeKeyWithNullSearchQueryService) {
        TKikimrRunner kikimr;
        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();
        auto queryClient = kikimr.GetQueryClient();

        // Create table with Decimal as FIRST in composite key
        auto createResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/TestDecimalCompositeKeyQS` (
                DecKey Decimal(22, 9),
                UintKey Uint64,
                DecValue Decimal(22, 9),
                Value String,
                PRIMARY KEY (DecKey, UintKey)
            );
        )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(createResult.GetStatus(), EStatus::SUCCESS, createResult.GetIssues().ToString());

        // Insert data using QueryService
        auto insertResult = queryClient.ExecuteQuery(R"(
            INSERT INTO `/Root/TestDecimalCompositeKeyQS` (DecKey, UintKey, DecValue, Value) VALUES
                (CAST("10.123456789" AS Decimal(22, 9)), 1, NULL, "value1"),
                (CAST("20.987654321" AS Decimal(22, 9)), 2, CAST("2.222222222" AS Decimal(22, 9)), "value2"),
                (CAST("30.555555555" AS Decimal(22, 9)), 3, NULL, "value3");
        )", NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(insertResult.GetStatus(), EStatus::SUCCESS, insertResult.GetIssues().ToString());

        // Search for NULL Decimal values using QueryService - this should trigger the bug
        auto selectNullResult = queryClient.ExecuteQuery(R"(
            SELECT DecKey, UintKey, DecValue, Value FROM `/Root/TestDecimalCompositeKeyQS`
            WHERE DecValue IS NULL
            ORDER BY DecKey, UintKey;
        )", NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(selectNullResult.GetStatus(), EStatus::SUCCESS, selectNullResult.GetIssues().ToString());

        // Validate results - should return rows with NULL DecValue
        CompareYson(R"([
            [["10.123456789"];[1u];#;["value1"]];
            [["30.555555555"];[3u];#;["value3"]]
        ])", FormatResultSetYson(selectNullResult.GetResultSet(0)));
    }

}

} // namespace NKqp
} // namespace NKikimr

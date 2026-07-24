#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <yql/essentials/public/langver/yql_langver.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {
namespace {

using namespace NYdb;

enum class EOptimizer {
    NewRbo,
    Legacy,
};

struct TResultRow {
    TString ItemId;
    i64 Amount = 0;
};

void CreateTables(TKikimrRunner& kikimr) {
    auto session =
        kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    const auto result = session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/item` (
            i_item_sk Int64 NOT NULL,
            i_item_id String NOT NULL,
            i_color String NOT NULL,
            PRIMARY KEY (i_item_sk)
        ) WITH (STORE = COLUMN);

        CREATE TABLE `/Root/store_sales` (
            ss_ticket_number Int64 NOT NULL,
            ss_item_sk Int64 NOT NULL,
            amount Int64 NOT NULL,
            PRIMARY KEY (ss_ticket_number)
        ) WITH (STORE = COLUMN);
    )").GetValueSync();
    UNIT_ASSERT_C(
        result.IsSuccess(),
        "HARNESS_ASSUMPTION_FAILED: cannot create tables: "
            << result.GetIssues().ToString());
}

void InsertRows(TKikimrRunner& kikimr) {
    TValueBuilder items;
    items.BeginList()
        .AddListItem()
            .BeginStruct()
                .AddMember("i_item_sk").Int64(1)
                .AddMember("i_item_id").String("same")
                .AddMember("i_color").String("black")
            .EndStruct()
        .AddListItem()
            .BeginStruct()
                .AddMember("i_item_sk").Int64(2)
                .AddMember("i_item_id").String("same")
                .AddMember("i_color").String("orchid")
            .EndStruct()
        .EndList();
    auto result = kikimr.GetTableClient()
        .BulkUpsert("/Root/item", items.Build())
        .GetValueSync();
    UNIT_ASSERT_C(
        result.IsSuccess(),
        "HARNESS_ASSUMPTION_FAILED: cannot populate item: "
            << result.GetIssues().ToString());

    TValueBuilder sales;
    sales.BeginList()
        .AddListItem()
            .BeginStruct()
                .AddMember("ss_ticket_number").Int64(1)
                .AddMember("ss_item_sk").Int64(1)
                .AddMember("amount").Int64(10)
            .EndStruct()
        .EndList();
    result = kikimr.GetTableClient()
        .BulkUpsert("/Root/store_sales", sales.Build())
        .GetValueSync();
    UNIT_ASSERT_C(
        result.IsSuccess(),
        "HARNESS_ASSUMPTION_FAILED: cannot populate store_sales: "
            << result.GetIssues().ToString());
}

TVector<TResultRow> Observe(EOptimizer optimizer) {
    NKikimrConfig::TAppConfig appConfig;
    auto* tableConfig = appConfig.MutableTableServiceConfig();
    tableConfig->SetEnableNewRBO(optimizer == EOptimizer::NewRbo);
    tableConfig->SetEnableFallbackToYqlOptimizer(false);
    tableConfig->SetAllowOlapDataQuery(true);
    tableConfig->SetDefaultLangVer(NYql::GetMaxLangVersion());
    tableConfig->SetBackportMode(
        NKikimrConfig::TTableServiceConfig_EBackportMode_All);

    TKikimrRunner kikimr(
        TKikimrSettings(appConfig).SetWithSampleTables(false));
    CreateTables(kikimr);
    InsertRows(kikimr);

    auto session =
        kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
    auto result = session.ExecuteQuery(
        R"(
            PRAGMA YqlSelect = 'force';
            PRAGMA AnsiImplicitCrossJoin;
            PRAGMA ydb.CostBasedOptimizationLevel = '0';

            SELECT
                i_item_id AS ItemId,
                amount AS Amount
            FROM
                `/Root/store_sales`,
                `/Root/item`
            WHERE
                i_item_id IN (
                    SELECT i_item_id
                    FROM `/Root/item`
                    WHERE i_color IN ('orchid', 'chiffon', 'lace')
                )
                AND ss_item_sk == i_item_sk
            ORDER BY ItemId, Amount;
        )",
        NYdb::NQuery::TTxControl::NoTx(),
        NYdb::NQuery::TExecuteQuerySettings()).ExtractValueSync();
    UNIT_ASSERT_C(
        result.IsSuccess(),
        "HARNESS_ASSUMPTION_FAILED: query failed: "
            << result.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL_C(
        result.GetResultSets().size(),
        1,
        "HARNESS_ASSUMPTION_FAILED: query returned an unexpected result count");

    TResultSetParser parser(result.GetResultSet(0));
    TVector<TResultRow> rows;
    while (parser.TryNextRow()) {
        rows.push_back({
            .ItemId = TString(parser.ColumnParser("ItemId").GetString()),
            .Amount = parser.ColumnParser("Amount").GetInt64(),
        });
    }
    return rows;
}

void AssertExpected(const TVector<TResultRow>& rows, TStringBuf context) {
    UNIT_ASSERT_VALUES_EQUAL_C(
        rows.size(),
        1,
        context << " returned an unexpected row count");
    UNIT_ASSERT_VALUES_EQUAL_C(
        rows[0].ItemId,
        "same",
        context << " returned an unexpected item id");
    UNIT_ASSERT_VALUES_EQUAL_C(
        rows[0].Amount,
        10,
        context << " returned an unexpected amount");
}

Y_UNIT_TEST_SUITE(StringInRuntimeDiagnostic) {
    Y_UNIT_TEST(SharedIUSemiJoinPreservesFactJoinPredicate) {
        const auto legacy = Observe(EOptimizer::Legacy);
        AssertExpected(legacy, "legacy optimizer");

        const auto newRbo = Observe(EOptimizer::NewRbo);
        AssertExpected(
            newRbo,
            "CONFIRMED_MISMATCH: new RBO changed the result of the shared-IU "
            "String IN query while CBO was disabled");
    }
}

} // namespace
} // namespace NKikimr::NKqp

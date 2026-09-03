#include <ydb/core/kqp/ut/indexes/json/common/kqp_indexes_json_ut_common.h>

namespace NKikimr::NKqp {

using namespace NYdb::NQuery;
using namespace NYdb;

namespace {

void ValidateOneOfTwoIndexesSelected(TQueryClient& db, const std::string& predicate,
    const TString& idxA, const TString& idxB, const std::string& tableName = "TestTable")
{
    const auto settings = TExecuteQuerySettings().ExecMode(EExecMode::Explain);
    const auto query = std::format("SELECT * FROM {} WHERE {};", tableName, predicate);

    const auto result = db.ExecuteQuery(query, TTxControl::NoTx(), settings).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), "Explain failed for predicate [" + predicate + "]: " + result.GetIssues().ToString());

    NJson::TJsonValue planJson;
    UNIT_ASSERT_C(NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &planJson, true),
        "Failed to parse plan JSON for predicate [" + predicate + "]");

    const int count = CountPlanNodesByKv(planJson, "Index", idxA) + CountPlanNodesByKv(planJson, "Index", idxB);
    UNIT_ASSERT_C(count == 1,
        "Expected exactly one of (" + idxA + ", " + idxB + ") to be auto-selected for: " + predicate + ", got " + std::to_string(count));
}

TString ExecuteAndAssertJsonPlan(TQueryClient& db, const TString& sql, size_t expectedIndexNodes, const TString& expectedYson,
    TParams params = TParamsBuilder().Build(), const TString& indexName = "json_idx")
{
    const auto settings = TExecuteQuerySettings().StatsMode(EStatsMode::Full);
    auto result = db.ExecuteQuery(sql, TTxControl::NoTx(), params, settings).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    UNIT_ASSERT_C(result.GetStats() && result.GetStats()->GetPlan(), "Execution plan is missing");

    NJson::TJsonValue planJson;
    UNIT_ASSERT_C(NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &planJson, true), "Failed to parse execution plan JSON");
    UNIT_ASSERT_VALUES_EQUAL_C(CountPlanNodesByKv(planJson, "Index", indexName), expectedIndexNodes, sql);

    const TString actual = FormatResultSetYson(result.GetResultSet(0));
    CompareYson(expectedYson, actual, sql);
    return actual;
}

} // namespace

Y_UNIT_TEST_SUITE(KqpJsonIndexesAutoSelect) {
    Y_UNIT_TEST(FullRangeIsNotAutoSelected) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            const auto addIndexResult = db.ExecuteQuery(R"(
                ALTER TABLE TestTable ADD INDEX json_idx_2 GLOBAL USING json ON (Text)
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(addIndexResult.IsSuccess(), addIndexResult.GetIssues().ToString());

            const auto settings = TExecuteQuerySettings().ExecMode(EExecMode::Explain);
            const auto query = R"(SELECT * FROM TestTable WHERE JSON_EXISTS(Text, '$[*]');)";

            const auto result = db.ExecuteQuery(query, TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(),
                "JSON index was not auto-selected: full-range search cannot be performed using full-text search");

            NJson::TJsonValue planJson;
            UNIT_ASSERT_C(NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &planJson, true), "Failed to parse plan JSON");
            UNIT_ASSERT_VALUES_EQUAL(CountPlanNodesByKv(planJson, "Index", "json_idx"), 0);
            UNIT_ASSERT_VALUES_EQUAL(CountPlanNodesByKv(planJson, "Index", "json_idx_2"), 0);
        }, /* enableJsonIndexAutoSelect */ true);
    }

    Y_UNIT_TEST(JsonExists) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1'))");
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == 2)'))");
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 == true && @.k2 == false)'))");
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 == null || @.k2 == "str")'))");
        }, /* enableJsonIndexAutoSelect */ true);
    }

    Y_UNIT_TEST(JsonValue) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            ValidateAutoSelect(db, "JSON_VALUE(Text, '$.k1' RETURNING Bool)");
            ValidateAutoSelect(db, "JSON_VALUE(Text, '$.k1' RETURNING Int64) == 10");
            ValidateAutoSelect(db, "JSON_VALUE(Text, '$.k1' RETURNING Int64) == -10");
            ValidateAutoSelect(db, "JSON_VALUE(Text, '$.k1' RETURNING Int64) != 10");
            ValidateAutoSelect(db, "JSON_VALUE(Text, '$.k1' RETURNING Int64) >= 10");
            ValidateAutoSelect(db, "JSON_VALUE(Text, '$.k1' RETURNING Int64) BETWEEN 10 AND 20");
            ValidateAutoSelect(db, "JSON_VALUE(Text, '$.k1' RETURNING Int64) NOT BETWEEN 10 AND 20");
            ValidateAutoSelect(db, "JSON_VALUE(Text, '$.k1' RETURNING Int64) IN (1, 2, 3, 4)");
        }, /* enableJsonIndexAutoSelect */ true);
    }

    Y_UNIT_TEST(AndOrCombinations) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2'))");
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2'))");
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2') AND JSON_EXISTS(Text, '$.k3'))");
            ValidateAutoSelect(db, R"((JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2')) OR JSON_EXISTS(Text, '$.k3'))");
        }, /* enableJsonIndexAutoSelect */ true);
    }

    Y_UNIT_TEST(PrimaryColumnPredicate) {
        auto kikimr = Kikimr(/* enableJsonIndex */ true, /* enableJsonIndexAutoSelect */ true);
        auto db = kikimr.GetQueryClient();

        CreateTestTable(db, "JsonDocument", /* withIndex */ true);

        // JI predicate
        ValidateAutoSelect(db, "JSON_EXISTS(Text, '$.k1')");

        // JI predicate with primary -> primary wins
        ValidateNoAutoSelect(db, "Key > 5 AND JSON_EXISTS(Text, '$.k1')");
        ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.k1') AND Key > 5");
        ValidateNoAutoSelect(db, "Key = 1 AND JSON_EXISTS(Text, '$.k1')");
        ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.k1') AND Key = 1");

        // Without JI predicate
        ValidateNoAutoSelect(db, "Key > 5");
        ValidateNoAutoSelect(db, "Key = 1");
    }

    Y_UNIT_TEST(SecondaryColumnPredicate) {
        auto kikimr = Kikimr(/* enableJsonIndex */ true, /* enableJsonIndexAutoSelect */ true);
        auto db = kikimr.GetQueryClient();

        {
            const std::string query = R"(
                CREATE TABLE TestTable (
                    Key Uint64,
                    Text JsonDocument,
                    Data Utf8,
                    PRIMARY KEY (Key),
                    INDEX json_idx GLOBAL USING json ON (Text),
                    INDEX data_idx GLOBAL ON (Data)
                );
            )";

            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // JI predicate
        ValidateAutoSelect(db, "JSON_EXISTS(Text, '$.k1')");

        // JI predicate with secondary -> secondary wins
        ValidateNoAutoSelect(db, "Data = 'b' AND JSON_EXISTS(Text, '$.k1')");
        ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.k1') AND Data = 'b'");
        ValidateNoAutoSelect(db, "Data >= 'a' AND JSON_EXISTS(Text, '$.k1')");
        ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.k1') AND Data >= 'a'");

        // Without JI predicate
        ValidateNoAutoSelect(db, "Data = 'b'");
        ValidateNoAutoSelect(db, "Data >= 'a'");
    }

    Y_UNIT_TEST(DataColumnPredicate) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            ValidateAutoSelect(db, "JSON_EXISTS(Text, '$.k1') AND Data = 'd1'");
            ValidateAutoSelect(db, "Data = 'd1' AND JSON_EXISTS(Text, '$.k1')");

            ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.k1') OR Data = 'd1'");
            ValidateNoAutoSelect(db, "Data = 'd1' OR JSON_EXISTS(Text, '$.k1')");

            ValidateAutoSelect(db, "Data = 'd1' AND JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2')");
            ValidateAutoSelect(db, "JSON_EXISTS(Text, '$.k1') AND Data = 'd1' AND JSON_EXISTS(Text, '$.k2')");
            ValidateAutoSelect(db, "JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2') AND Data = 'd1'");

            ValidateNoAutoSelect(db, "Data = 'd1' OR JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2')");
            ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.k1') OR Data = 'd1' OR JSON_EXISTS(Text, '$.k2')");
            ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2') OR Data = 'd1'");

            ValidateNoAutoSelect(db, "Data = 'd1' OR JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2')");
            ValidateAutoSelect(db, "Data = 'd1' AND JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2')");

            ValidateAutoSelect(db, "JSON_EXISTS(Text, '$.k1') OR Data = 'd1' AND JSON_EXISTS(Text, '$.k2')");
            ValidateAutoSelect(db, "JSON_EXISTS(Text, '$.k1') AND Data = 'd1' OR JSON_EXISTS(Text, '$.k2')");

            ValidateAutoSelect(db, "JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2') AND Data = 'd1'");
            ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2') OR Data = 'd1'");
        }, /* enableJsonIndexAutoSelect */ true);
    }

    Y_UNIT_TEST(TwoJsonIndexes) {
        auto kikimr = Kikimr(/* enableJsonIndex */ true, /* enableJsonIndexAutoSelect */ true);
        auto db = kikimr.GetQueryClient();

        {
            const std::string query = R"(
                CREATE TABLE TestTable (
                    Key Uint64,
                    Text JsonDocument,
                    Extra JsonDocument,
                    Data Utf8,
                    PRIMARY KEY (Key),
                    INDEX json_idx_text GLOBAL USING json ON (Text),
                    INDEX json_idx_extra GLOBAL USING json ON (Extra)
                );
            )";

            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        ValidateAutoSelect(db, "JSON_EXISTS(Text, '$.k1')", "json_idx_text");
        ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.k1')", "json_idx_extra");

        ValidateAutoSelect(db, "JSON_EXISTS(Extra, '$.k1')", "json_idx_extra");
        ValidateNoAutoSelect(db, "JSON_EXISTS(Extra, '$.k1')", "json_idx_text");

        // Cross-column predicates are not supported
        ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Extra, '$.k1')");
        ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Extra, '$.k1')");
    }

    Y_UNIT_TEST(WrongColumn) {
        auto kikimr = Kikimr(/* enableJsonIndex */ true, /* enableJsonIndexAutoSelect */ true);
        auto db = kikimr.GetQueryClient();

        {
            const std::string query = R"(
                CREATE TABLE TestTable (
                    Key Uint64,
                    Text JsonDocument,
                    Data JsonDocument,
                    PRIMARY KEY (Key),
                    INDEX json_idx GLOBAL USING json ON (Text),
                );
            )";

            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        ValidateAutoSelect(db, "JSON_EXISTS(Text, '$.k1')");

        ValidateAutoSelect(db, "JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Data, '$.k1')");
        ValidateAutoSelect(db, "JSON_EXISTS(Data, '$.k1') AND JSON_EXISTS(Text, '$.k1')");

        ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Data, '$.k1')");
        ValidateNoAutoSelect(db, "JSON_EXISTS(Data, '$.k1') OR JSON_EXISTS(Text, '$.k1')");

        ValidateNoAutoSelect(db, "JSON_EXISTS(Data, '$.k1')");
    }

    Y_UNIT_TEST(NoJsonIndex) {
        auto kikimr = Kikimr(/* enableJsonIndex */ true, /* enableJsonIndexAutoSelect */ true);
        auto db = kikimr.GetQueryClient();

        CreateTestTable(db, "JsonDocument", /* withIndex */ false);
        FillTestTable(db, "TestTable", "JsonDocument");

        ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.k1')");

        {
            const std::string query = R"(
                ALTER TABLE TestTable ADD INDEX json_idx GLOBAL USING json ON (Text)
            )";

            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1'))");
    }

    Y_UNIT_TEST(Negation) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            // JE
            ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.key' TRUE ON ERROR)");
            ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.key') IS NULL");
            ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.key') IS NOT NULL");
            ValidateNoAutoSelect(db, "COALESCE(JSON_EXISTS(Text, '$.key'), true)");

            ValidateNoAutoSelect(db, "NOT JSON_EXISTS(Text, '$.key')");
            ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.key') == false");
            ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.key') != true");
            ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.key') == Just(false)");
            ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.key') != Just(true)");

            // JV
            ValidateNoAutoSelect(db, "JSON_VALUE(Text, '$.key' RETURNING Bool DEFAULT TRUE ON EMPTY)");
            ValidateNoAutoSelect(db, "JSON_VALUE(Text, '$.key' RETURNING Bool DEFAULT TRUE ON ERROR)");
            ValidateNoAutoSelect(db, "JSON_VALUE(Text, '$.key' RETURNING Bool DEFAULT TRUE ON EMPTY DEFAULT TRUE ON ERROR)");
            ValidateNoAutoSelect(db, "JSON_VALUE(Text, '$.key' RETURNING Bool) IS NULL");
            ValidateNoAutoSelect(db, "JSON_VALUE(Text, '$.key' RETURNING Bool) IS NOT NULL");
            ValidateNoAutoSelect(db, "COALESCE(JSON_VALUE(Text, '$.key' RETURNING Bool), true)");

            ValidateNoAutoSelect(db, "NOT JSON_VALUE(Text, '$.key' RETURNING Bool)");
            ValidateNoAutoSelect(db, "JSON_VALUE(Text, '$.key' RETURNING Bool) == false");
            ValidateNoAutoSelect(db, "JSON_VALUE(Text, '$.key' RETURNING Bool) != true");
            ValidateNoAutoSelect(db, "JSON_VALUE(Text, '$.key' RETURNING Bool) == Just(false)");
            ValidateNoAutoSelect(db, "JSON_VALUE(Text, '$.key' RETURNING Bool) != Just(true)");

            ValidateNoAutoSelect(db, "JSON_VALUE(Text, '$.key' RETURNING Int32) IS NULL");
            ValidateNoAutoSelect(db, "JSON_VALUE(Text, '$.key' RETURNING Int32) IS NOT NULL");
            ValidateNoAutoSelect(db, "JSON_VALUE(Text, '$.key' RETURNING Int32) NOT IN (1, 2, 3)");
        }, /* enableJsonIndexAutoSelect */ true);
    }

    Y_UNIT_TEST(FlagDisabled) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            ValidateNoAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1'))");
            ValidateNoAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == 2)'))");
            ValidateNoAutoSelect(db, "JSON_VALUE(Text, '$.k1' RETURNING Bool)");
            ValidateNoAutoSelect(db, "JSON_VALUE(Text, '$.k1' RETURNING Int64) == 10");
            ValidateNoAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2'))");
            ValidateNoAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2'))");
        }, /* enableJsonIndexAutoSelect */ false);
    }

    Y_UNIT_TEST(PassingInJE) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            // Basic PASSING with literal values
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1 ? (@ == $v)' PASSING 1 AS v))");
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1 ? (@ == $v)' PASSING true AS v))");
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1 ? (@ == $v)' PASSING "str"u AS v))");
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1 ? (@ == $v)' PASSING null AS v))");

            // PASSING with filter predicate at root
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 == $v)' PASSING 1 AS v))");

            // PASSING with multiple variables
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 == $v1 && @.k2 == $v2)' PASSING 1 AS v1, 2 AS v2))");

            // PASSING with range comparison
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1 ? (@ > $v)' PASSING 5 AS v))");
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1 ? (@ >= $lo && @ <= $hi)' PASSING 5 AS lo, 10 AS hi))");

            // PASSING combined with AND
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1 ? (@ == $v)' PASSING 1 AS v) AND JSON_EXISTS(Text, '$.k2'))");
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2 ? (@ == $v)' PASSING 2 AS v))");

            // PASSING combined with OR
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1 ? (@ == $v)' PASSING 1 AS v) OR JSON_EXISTS(Text, '$.k2'))");

            // Non-autoselectable: TRUE ON ERROR changes semantics
            ValidateNoAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1 ? (@ == $v)' PASSING 1 AS v TRUE ON ERROR))");
        }, /* enableJsonIndexAutoSelect */ true);
    }

    Y_UNIT_TEST(PassingInJV) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            // Basic PASSING with literal integer variable in jsonpath filter
            ValidateAutoSelect(db, R"(JSON_VALUE(Text, '$.k1 ? (@ > $v)' PASSING 5 AS v RETURNING Int64) == 10)");
            ValidateAutoSelect(db, R"(JSON_VALUE(Text, '$.k1 ? (@ == $v)' PASSING 10 AS v RETURNING Int64) == 10)");

            // PASSING with boolean variable
            ValidateAutoSelect(db, R"(JSON_VALUE(Text, '$.k1 ? (@ == $v)' PASSING true AS v RETURNING Bool))");

            // PASSING with multiple variables
            ValidateAutoSelect(db, R"(JSON_VALUE(Text, '$.k1 ? (@ > $lo && @ < $hi)' PASSING 5 AS lo, 20 AS hi RETURNING Int64) == 10)");

            // PASSING combined with AND
            ValidateAutoSelect(db, R"(JSON_VALUE(Text, '$.k1 ? (@ > $v)' PASSING 5 AS v RETURNING Int64) == 10 AND JSON_EXISTS(Text, '$.k2'))");
            ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1') AND JSON_VALUE(Text, '$.k2 ? (@ == $v)' PASSING 2 AS v RETURNING Int64) == 2)");

            // PASSING combined with OR
            ValidateAutoSelect(db, R"(JSON_VALUE(Text, '$.k1 ? (@ == $v)' PASSING 10 AS v RETURNING Int64) == 10 OR JSON_EXISTS(Text, '$.k2'))");

            // Non-autoselectable: DEFAULT ON EMPTY/ERROR changes semantics
            ValidateNoAutoSelect(db, R"(JSON_VALUE(Text, '$.k1 ? (@ > $v)' PASSING 5 AS v RETURNING Int64 DEFAULT -1 ON EMPTY) == 10)");
            ValidateNoAutoSelect(db, R"(JSON_VALUE(Text, '$.k1 ? (@ > $v)' PASSING 5 AS v RETURNING Int64 DEFAULT -1 ON ERROR) == 10)");
        }, /* enableJsonIndexAutoSelect */ true);
    }

    Y_UNIT_TEST(PassingInJE_WithParameters) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            // SQL parameter as PASSING value - integer
            ValidateAutoSelectWithDecl(db, "DECLARE $v AS Int64;",
                R"(JSON_EXISTS(Text, '$.k1 ? (@ == $v)' PASSING $v AS v))");

            // SQL parameter as PASSING value - boolean
            ValidateAutoSelectWithDecl(db, "DECLARE $v AS Bool;",
                R"(JSON_EXISTS(Text, '$.k1 ? (@ == $v)' PASSING $v AS v))");

            // SQL parameter as PASSING value - string
            ValidateAutoSelectWithDecl(db, "DECLARE $v AS Utf8;",
                R"(JSON_EXISTS(Text, '$.k1 ? (@ == $v)' PASSING $v AS v))");

            // Multiple SQL parameters as PASSING values
            ValidateAutoSelectWithDecl(db, "DECLARE $lo AS Int64; DECLARE $hi AS Int64;",
                R"(JSON_EXISTS(Text, '$.k1 ? (@ >= $lo && @ <= $hi)' PASSING $lo AS lo, $hi AS hi))");

            // SQL parameter at root filter
            ValidateAutoSelectWithDecl(db, "DECLARE $v AS Int64;",
                R"(JSON_EXISTS(Text, '$ ? (@.k1 == $v)' PASSING $v AS v))");

            // Combined with AND
            ValidateAutoSelectWithDecl(db, "DECLARE $v AS Int64;",
                R"(JSON_EXISTS(Text, '$.k1 ? (@ == $v)' PASSING $v AS v) AND JSON_EXISTS(Text, '$.k2'))");

            // Combined with OR
            ValidateAutoSelectWithDecl(db, "DECLARE $v AS Int64;",
                R"(JSON_EXISTS(Text, '$.k1 ? (@ == $v)' PASSING $v AS v) OR JSON_EXISTS(Text, '$.k2'))");

            // Non-autoselectable: TRUE ON ERROR
            ValidateNoAutoSelectWithDecl(db, "DECLARE $v AS Int64;",
                R"(JSON_EXISTS(Text, '$.k1 ? (@ == $v)' PASSING $v AS v TRUE ON ERROR))");
        }, /* enableJsonIndexAutoSelect */ true);
    }

    Y_UNIT_TEST(PassingInJV_WithParameters) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            // SQL parameter as PASSING value - integer
            ValidateAutoSelectWithDecl(db, "DECLARE $v AS Int64;",
                R"(JSON_VALUE(Text, '$.k1 ? (@ == $v)' PASSING $v AS v RETURNING Int64) == 10)");

            // SQL parameter as PASSING value with range comparison
            ValidateAutoSelectWithDecl(db, "DECLARE $v AS Int64;",
                R"(JSON_VALUE(Text, '$.k1 ? (@ > $v)' PASSING $v AS v RETURNING Int64) == 10)");

            // Multiple SQL parameters as PASSING values
            ValidateAutoSelectWithDecl(db, "DECLARE $lo AS Int64; DECLARE $hi AS Int64;",
                R"(JSON_VALUE(Text, '$.k1 ? (@ > $lo && @ < $hi)' PASSING $lo AS lo, $hi AS hi RETURNING Int64) > 0)");

            // SQL parameter as PASSING value - boolean
            ValidateAutoSelectWithDecl(db, "DECLARE $v AS Bool;",
                R"(JSON_VALUE(Text, '$.k1 ? (@ == $v)' PASSING $v AS v RETURNING Bool))");

            // Combined with AND
            ValidateAutoSelectWithDecl(db, "DECLARE $v AS Int64;",
                R"(JSON_VALUE(Text, '$.k1 ? (@ > $v)' PASSING $v AS v RETURNING Int64) > 0 AND JSON_EXISTS(Text, '$.k2'))");

            // Combined with OR
            ValidateAutoSelectWithDecl(db, "DECLARE $v AS Int64;",
                R"(JSON_VALUE(Text, '$.k1 ? (@ == $v)' PASSING $v AS v RETURNING Int64) == 10 OR JSON_EXISTS(Text, '$.k2'))");

            // Non-autoselectable: DEFAULT ON EMPTY/ERROR
            ValidateNoAutoSelectWithDecl(db, "DECLARE $v AS Int64;",
                R"(JSON_VALUE(Text, '$.k1 ? (@ > $v)' PASSING $v AS v RETURNING Int64 DEFAULT -1 ON EMPTY) > 0)");
            ValidateNoAutoSelectWithDecl(db, "DECLARE $v AS Int64;",
                R"(JSON_VALUE(Text, '$.k1 ? (@ > $v)' PASSING $v AS v RETURNING Int64 DEFAULT -1 ON ERROR) > 0)");
        }, /* enableJsonIndexAutoSelect */ true);
    }

    Y_UNIT_TEST(TwoJsonIndexes_SameColumn) {
        auto kikimr = Kikimr(/* enableJsonIndex */ true, /* enableJsonIndexAutoSelect */ true);
        auto db = kikimr.GetQueryClient();

        {
            const std::string query = R"(
                CREATE TABLE TestTable (
                    Key Uint64,
                    Text JsonDocument,
                    Data Utf8,
                    PRIMARY KEY (Key),
                    INDEX json_idx_a GLOBAL USING json ON (Text),
                    INDEX json_idx_b GLOBAL USING json ON (Text)
                );
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                UPSERT INTO TestTable (Key, Text, Data) VALUES
                    (1, JsonDocument('{"color": "red", "size": 10}'), "item1"),
                    (2, JsonDocument('{"color": "blue", "size": 20}'), "item2"),
                    (3, JsonDocument('{"color": "red", "size": 30}'), "item3"),
                    (4, JsonDocument('{"weight": 5}'), "item4"),
                    (5, JsonDocument('{}'), "item5");
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Exactly one of the two indexes must appear in the query plan.
        ValidateOneOfTwoIndexesSelected(db, "JSON_EXISTS(Text, '$.color')", "json_idx_a", "json_idx_b");
        ValidateOneOfTwoIndexesSelected(db, "JSON_EXISTS(Text, '$.size')", "json_idx_a", "json_idx_b");
        ValidateOneOfTwoIndexesSelected(db, "JSON_VALUE(Text, '$.size' RETURNING Int64) == 10", "json_idx_a", "json_idx_b");
    }

    Y_UNIT_TEST(TwoJsonIndexes_DifferentColumns_SingleColumnPredicates) {
        auto kikimr = Kikimr(/* enableJsonIndex */ true, /* enableJsonIndexAutoSelect */ true);
        auto db = kikimr.GetQueryClient();

        {
            const std::string query = R"(
                CREATE TABLE TestTable (
                    Key Uint64,
                    Text  JsonDocument,
                    Extra JsonDocument,
                    Data  Utf8,
                    PRIMARY KEY (Key),
                    INDEX json_idx_text  GLOBAL USING json ON (Text),
                    INDEX json_idx_extra GLOBAL USING json ON (Extra)
                );
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                UPSERT INTO TestTable (Key, Text, Extra, Data) VALUES
                    (1, JsonDocument('{"a": 1, "b": "hello"}'),JsonDocument('{"x": 10, "y": true}'), "row1"),
                    (2, JsonDocument('{"a": 2}'), JsonDocument('{"x": 20, "y": false}'), "row2"),
                    (3, JsonDocument('{"b": "world"}'), JsonDocument('{"x": 10, "z": null}'), "row3"),
                    (4, JsonDocument('{"a": 1, "c": 3}'), JsonDocument('{"w": 99}'), "row4"),
                    (5, JsonDocument('{}'), JsonDocument('{}'), "row5");
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Predicate on Text -> must use json_idx_text, not json_idx_extra.
        ValidateAutoSelect(db, "JSON_EXISTS(Text, '$.a')", "json_idx_text",  "TestTable");
        ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.a')", "json_idx_extra", "TestTable");
        ValidateAutoSelect(db, "JSON_EXISTS(Text, '$.b')", "json_idx_text",  "TestTable");

        // Predicate on Extra -> must use json_idx_extra, not json_idx_text.
        ValidateAutoSelect (db, "JSON_EXISTS(Extra, '$.x')", "json_idx_extra", "TestTable");
        ValidateNoAutoSelect(db, "JSON_EXISTS(Extra, '$.x')", "json_idx_text",  "TestTable");
        ValidateAutoSelect (db, "JSON_EXISTS(Extra, '$.y')", "json_idx_extra", "TestTable");

        // Multiple predicates on the same column still use a single index.
        ValidateAutoSelect (db, "JSON_EXISTS(Text, '$.a') AND JSON_EXISTS(Text, '$.b')", "json_idx_text", "TestTable");
        ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.a') AND JSON_EXISTS(Text, '$.b')", "json_idx_extra", "TestTable");
    }

    Y_UNIT_TEST(TwoJsonIndexes_DifferentColumns_MixedPredicates) {
        auto kikimr = Kikimr(/* enableJsonIndex */ true, /* enableJsonIndexAutoSelect */ true);
        auto db = kikimr.GetQueryClient();

        {
            const std::string query = R"(
                CREATE TABLE TestTable (
                    Key Uint64,
                    Text  JsonDocument,
                    Extra JsonDocument,
                    Data  Utf8,
                    PRIMARY KEY (Key),
                    INDEX json_idx_text  GLOBAL USING json ON (Text),
                    INDEX json_idx_extra GLOBAL USING json ON (Extra)
                );
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                UPSERT INTO TestTable (Key, Text, Extra, Data) VALUES
                    (1, JsonDocument('{"a": 1}'), JsonDocument('{"x": 10}'), "row1"),
                    (2, JsonDocument('{"a": 2}'), JsonDocument('{"y": 20}'), "row2"),
                    (3, JsonDocument('{"b": "hi"}'), JsonDocument('{"x": 10}'), "row3"),
                    (4, JsonDocument('{"a": 1}'), JsonDocument('{"z": 30}'), "row4"),
                    (5, JsonDocument('{}'), JsonDocument('{}'), "row5");
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // AND of predicates from two different indexed columns
        ValidateAutoSelect(db, "JSON_EXISTS(Text, '$.a') AND JSON_EXISTS(Extra, '$.x')",
            "json_idx_extra", "TestTable");

        // OR of predicates from two different indexed columns
        ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.a') OR JSON_EXISTS(Extra, '$.x')",
            "json_idx_text",  "TestTable");
        ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.a') OR JSON_EXISTS(Extra, '$.x')",
            "json_idx_extra", "TestTable");

        ValidateNoAutoSelect(db,
            "JSON_EXISTS(Text, '$.a') OR JSON_EXISTS(Extra, '$.x') AND JSON_EXISTS(Extra, '$.y')",
            "json_idx_text", "TestTable");
        ValidateNoAutoSelect(db,
            "JSON_EXISTS(Text, '$.a') OR JSON_EXISTS(Extra, '$.x') AND JSON_EXISTS(Extra, '$.y')",
            "json_idx_extra", "TestTable");

        ValidateNoAutoSelect(db,
            "JSON_VALUE(Text, '$.a' RETURNING Int64) == 1 OR JSON_EXISTS(Extra, '$.x')",
            "json_idx_text", "TestTable");
        ValidateNoAutoSelect(db,
            "JSON_VALUE(Text, '$.a' RETURNING Int64) == 1 OR JSON_EXISTS(Extra, '$.x')",
            "json_idx_extra", "TestTable");

        ValidateNoAutoSelect(db,
            "JSON_EXISTS(Text, '$.a') OR JSON_VALUE(Extra, '$.x' RETURNING Int64) == 10",
            "json_idx_text", "TestTable");
        ValidateNoAutoSelect(db,
            "JSON_EXISTS(Text, '$.a') OR JSON_VALUE(Extra, '$.x' RETURNING Int64) == 10",
            "json_idx_extra", "TestTable");
    }

    Y_UNIT_TEST_TWIN(AutoSelectSqlForms, Compact) {
        auto kikimr = KikimrJson(/* enableJsonIndexAutoSelect */ true, Compact);
        auto db = kikimr.GetQueryClient();

        {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE TestTable (
                    Key Uint64,
                    Text JsonDocument,
                    Data Utf8,
                    Active Bool,
                    PRIMARY KEY (Key),
                    INDEX json_idx GLOBAL USING json ON (Text)
                );
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = db.ExecuteQuery(R"(
                UPSERT INTO TestTable (Key, Text, Data, Active) VALUES
                    (1, JsonDocument('{"kind":"cat","score":10}'), "first"u, true),
                    (2, JsonDocument('{"kind":"dog","score":20}'), "second"u, true),
                    (3, JsonDocument('{"kind":"cat","score":30}'), "third"u, false),
                    (4, JsonDocument('{"other":true}'), "fourth"u, false);
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        ExecuteAndAssertJsonPlan(db, R"(
            SELECT d.Key FROM TestTable AS d
            WHERE JSON_VALUE(d.Text, '$.kind' RETURNING Utf8) = "cat"u ORDER BY d.Key;
        )", 1, "[[[1u]];[[3u]]]");

        auto kindParams = TParamsBuilder()
            .AddParam("$kind").Utf8("cat").Build()
            .Build();
        ExecuteAndAssertJsonPlan(db, R"(
            DECLARE $kind AS Utf8;
            SELECT Key FROM TestTable
            WHERE JSON_VALUE(Text, '$.kind' RETURNING Utf8) = $kind ORDER BY Key;
        )", 1, "[[[1u]];[[3u]]]", kindParams);

        ExecuteAndAssertJsonPlan(db, R"(
            SELECT Key FROM TestTable
            WHERE Active = true AND (Data != "missing"u AND
                  (JSON_VALUE(Text, '$.kind' RETURNING Utf8) = "cat"u)) ORDER BY Key;
        )", 1, "[[[1u]]]");

        ExecuteAndAssertJsonPlan(db, R"(
            $docs = SELECT Key, Text FROM TestTable;
            SELECT Key FROM $docs
            WHERE JSON_VALUE(Text, '$.kind' RETURNING Utf8) = "cat"u ORDER BY Key;
        )", 1, "[[[1u]];[[3u]]]");

        ExecuteAndAssertJsonPlan(db, R"(
            SELECT d.Key FROM (SELECT Key, Text FROM TestTable) AS d
            WHERE JSON_VALUE(d.Text, '$.kind' RETURNING Utf8) = "cat"u ORDER BY d.Key;
        )", 1, "[[[1u]];[[3u]]]");

        ExecuteAndAssertJsonPlan(db, R"(
            SELECT Data FROM TestTable
            WHERE JSON_VALUE(Text, '$.kind' RETURNING Utf8) = "cat"u
            ORDER BY Key DESC LIMIT 1;
        )", 1, R"([[["third"]]])");

        ExecuteAndAssertJsonPlan(db, R"(
            SELECT Key FROM TestTable VIEW PRIMARY KEY
            WHERE JSON_VALUE(Text, '$.kind' RETURNING Utf8) = "cat"u ORDER BY Key;
        )", 0, "[[[1u]];[[3u]]]");

        ExecuteAndAssertJsonPlan(db, R"(
            SELECT Key FROM TestTable
            WHERE String::AsciiToUpper(JSON_VALUE(Text, '$.kind' RETURNING Utf8)) = "CAT"
            ORDER BY Key;
        )", 0, "[[[1u]];[[3u]]]");
    }

    Y_UNIT_TEST_TWIN(JsonIndexOptimizerLifecycle, Compact) {
        auto kikimr = KikimrJson(/* enableJsonIndexAutoSelect */ true, Compact);
        auto db = kikimr.GetQueryClient();

        {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE `/Root/Lifecycle` (
                    Key Uint64,
                    Text JsonDocument,
                    PRIMARY KEY (Key)
                );
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = db.ExecuteQuery(R"(
                UPSERT INTO `/Root/Lifecycle` (Key, Text) VALUES
                    (1, JsonDocument('{"kind":"target"}')),
                    (2, JsonDocument('{"kind":"other"}'));
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        const TString sql = R"(
            SELECT Key FROM `/Root/Lifecycle`
            WHERE JSON_VALUE(Text, '$.kind' RETURNING Utf8) = "target"u
            ORDER BY Key;
        )";

        ExecuteAndAssertJsonPlan(db, sql, 0, "[[[1u]]]");
        ExecuteAndAssertJsonPlan(db, sql, 0, "[[[1u]]]");

        {
            auto result = db.ExecuteQuery(R"(
                ALTER TABLE `/Root/Lifecycle`
                    ADD INDEX json_idx GLOBAL USING json ON (Text);
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        ExecuteAndAssertJsonPlan(db, sql, 1, "[[[1u]]]");

        {
            auto result = db.ExecuteQuery(R"(
                UPDATE `/Root/Lifecycle`
                SET Text = JsonDocument('{"kind":"target"}')
                WHERE Key = 2;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        ExecuteAndAssertJsonPlan(db, sql, 1, "[[[1u]];[[2u]]]");

        {
            auto result = db.ExecuteQuery(R"(
                ALTER TABLE `/Root/Lifecycle` DROP INDEX json_idx;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                UPDATE `/Root/Lifecycle`
                SET Text = JsonDocument('{"kind":"other"}')
                WHERE Key = 1;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        ExecuteAndAssertJsonPlan(db, sql, 0, "[[[2u]]]");

        {
            auto result = db.ExecuteQuery(R"(
                ALTER TABLE `/Root/Lifecycle`
                    ADD INDEX json_idx GLOBAL USING json ON (Text);
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                INSERT INTO `/Root/Lifecycle` (Key, Text) VALUES
                    (3, JsonDocument('{"kind":"target"}'));
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        ExecuteAndAssertJsonPlan(db, sql, 1, "[[[2u]];[[3u]]]");
    }

    Y_UNIT_TEST_TWIN(JsonIndexImplSchemaVersionBump, Compact) {
        auto kikimr = KikimrJson(/* enableJsonIndexAutoSelect */ true, Compact);
        auto db = kikimr.GetQueryClient();

        {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE `/Root/SchemaDocs` (
                    Key Uint64,
                    Text JsonDocument,
                    PRIMARY KEY (Key),
                    INDEX json_idx GLOBAL USING json ON (Text)
                );
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = db.ExecuteQuery(R"(
                UPSERT INTO `/Root/SchemaDocs` (Key, Text) VALUES
                    (1, JsonDocument('{"kind":"target"}')),
                    (2, JsonDocument('{"kind":"other"}'));
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        const TString sql = R"(
            SELECT Key FROM `/Root/SchemaDocs` VIEW json_idx
            WHERE JSON_VALUE(Text, '$.kind' RETURNING Utf8) = "target"u
            ORDER BY Key;
        )";

        const TString expected = ExecuteAndAssertJsonPlan(db, sql, 1, "[[[1u]]]");
        UNIT_ASSERT_VALUES_EQUAL_C(
            ExecuteAndAssertJsonPlan(db, sql, 1, "[[[1u]]]"), expected, sql);

        Tests::TClient& client = kikimr.GetTestClient();
        const TString scheme = R"(
            Name: "indexImplTable"
            PartitionConfig {
                PartitioningPolicy {
                    MinPartitionsCount: 1
                    SizeToSplit: 100500
                }
            }
        )";
        auto alter = client.AlterTable("/Root/SchemaDocs/json_idx", scheme, {});
        UNIT_ASSERT_VALUES_EQUAL_C(alter->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK,
            alter->Record.ShortDebugString());

        UNIT_ASSERT_VALUES_EQUAL_C(
            ExecuteAndAssertJsonPlan(db, sql, 1, "[[[1u]]]"), expected, sql);
    }

    Y_UNIT_TEST(Prefixed) {
        auto kikimr = KikimrJsonPrefix(true);
        auto db = kikimr.GetQueryClient();

        {
            std::string query = R"(
                CREATE TABLE TestTable (
                    Key Uint64,
                    UserId Uint64,
                    Text JsonDocument,
                    PRIMARY KEY (Key),
                    INDEX json_idx GLOBAL USING json ON (UserId, Text)
                );
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            std::string query = R"(
                UPSERT INTO TestTable (Key, UserId, Text) VALUES
                    (1, 100, JsonDocument('{"k1": "v1"}')),
                    (2, 100, JsonDocument('{"k2": "v2"}')),
                    (3, 200, JsonDocument('{"k1": "v1"}')),
                    (4, 200, JsonDocument('{"k3": "v3"}'));
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Complete prefix equality allows auto-selection; omitting it keeps the table scan.
        ValidateAutoSelect(db, "UserId=100 AND JSON_EXISTS(Text, '$.k1')", "json_idx", "TestTable");
        ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.k1')", "json_idx", "TestTable");
    }

    Y_UNIT_TEST(PrefixedMultiColumn) {
        auto kikimr = KikimrJsonPrefix(true);
        auto db = kikimr.GetQueryClient();

        {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE TestTable (
                    Key Uint64,
                    Tenant Utf8,
                    UserId Uint64,
                    Text JsonDocument,
                    PRIMARY KEY (Key),
                    INDEX json_idx GLOBAL USING json ON (Tenant, UserId, Text)
                );
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = db.ExecuteQuery(R"(
                UPSERT INTO TestTable (Key, Tenant, UserId, Text) VALUES
                    (1, "acme"u,   100, JsonDocument('{"kind":"cats","score":10}')),
                    (2, "acme"u,   100, JsonDocument('{"kind":"dogs","score":20}')),
                    (3, "acme"u,   200, JsonDocument('{"kind":"cats","score":20}')),
                    (4, "globex"u, 100, JsonDocument('{"kind":"cats","score":30}'));
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        ValidateAutoSelect(db,
            R"(Tenant = "acme"u AND UserId = 100 AND JSON_EXISTS(Text, '$.kind'))",
            "json_idx", "TestTable");
        ValidateAutoSelect(db,
            R"(100 = UserId AND JSON_VALUE(Text, '$.score' RETURNING Int64) = 20 AND "acme"u = Tenant)",
            "json_idx", "TestTable");
        ValidateAutoSelectWithDecl(db,
            "DECLARE $tenant AS Utf8;\nDECLARE $uid AS Uint64;",
            R"(UserId = $uid AND JSON_EXISTS(Text, '$.kind') AND Tenant = $tenant)",
            "json_idx", "TestTable");

        ValidateNoAutoSelect(db,
            R"(UserId = 100 AND JSON_EXISTS(Text, '$.kind'))",
            "json_idx", "TestTable");
        ValidateNoAutoSelect(db,
            R"(Tenant = "acme"u AND JSON_EXISTS(Text, '$.kind'))",
            "json_idx", "TestTable");
        ValidateNoAutoSelect(db,
            R"((Tenant = "acme"u OR Tenant = "globex"u) AND UserId = 100 AND JSON_EXISTS(Text, '$.kind'))",
            "json_idx", "TestTable");
        ValidateNoAutoSelect(db,
            R"(Tenant = "acme"u AND UserId > 0 AND JSON_EXISTS(Text, '$.kind'))",
            "json_idx", "TestTable");
    }
}

}  // namespace NKikimr::NKqp

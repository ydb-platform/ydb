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

} // namespace

Y_UNIT_TEST_SUITE(KqpJsonIndexesAutoSelect) {
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
        ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.a') AND JSON_EXISTS(Extra, '$.x')",
            "json_idx_text",  "TestTable");
        ValidateNoAutoSelect(db, "JSON_EXISTS(Text, '$.a') AND JSON_EXISTS(Extra, '$.x')",
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
}

}  // namespace NKikimr::NKqp

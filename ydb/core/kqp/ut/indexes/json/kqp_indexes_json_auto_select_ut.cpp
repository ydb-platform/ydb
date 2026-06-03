#include <ydb/core/kqp/ut/indexes/json/common/kqp_indexes_json_ut_common.h>

namespace NKikimr::NKqp {

using namespace NYdb::NQuery;
using namespace NYdb;

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
}

}  // namespace NKikimr::NKqp

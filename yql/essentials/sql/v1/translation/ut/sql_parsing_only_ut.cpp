#include "sql_ut.h"

#include <yql/essentials/sql/v1/format/sql_format.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/translation/sql.h>

#include <yql/essentials/providers/common/provider/yql_provider_names.h>

#include <library/cpp/iterator/cartesian_product.h>

#include <util/generic/overloaded.h>

using namespace NSQLTranslationV1;

Y_UNIT_TEST_SUITE(SqlParsingOnly) {

/// This function is used in BACKWARD COMPATIBILITY tests below that LIMIT the sets of token that CAN NOT be used
/// as identifiers in different contexts in a SQL request
///\return list of tokens that failed this check
TVector<TString> ValidateTokens(const THashSet<TString>& forbidden, const std::function<TString(const TString&)>& makeRequest) {
    THashMap<TString, bool> allTokens;
    for (const auto& t : NSQLFormat::GetKeywords()) {
        allTokens[t] = !forbidden.contains((t));
    }
    for (const auto& f : forbidden) {
        UNIT_ASSERT(allTokens.contains(f)); // check that forbidden list contains tokens only(argument check)
    }
    TVector<TString> failed;
    for (const auto& [token, allowed] : allTokens) {
        if (SqlToYql(makeRequest(token)).IsOk() != allowed) {
            failed.push_back(token);
        }
    }
    return failed;
}

Y_UNIT_TEST(TokensAsColumnName) { // id_expr
    auto failed = ValidateTokens({"ALL", "ANY", "AS", "ASSUME", "ASYMMETRIC", "AUTOMAP", "BETWEEN", "BITCAST",
                                  "CALLABLE", "CASE", "CAST", "CUBE", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
                                  "DICT", "DISTINCT", "ENUM", "ERASE", "EXCEPT", "EXISTS", "FLOW", "FROM", "FULL",
                                  "HAVING", "HOP", "INTERSECT", "JSON_EXISTS", "JSON_QUERY", "JSON_VALUE", "LIMIT", "LIST",
                                  "NOT", "OPTIONAL", "PROCESS", "REDUCE", "REPEATABLE", "RESOURCE", "RETURN", "RETURNING", "ROLLUP",
                                  "SELECT", "SET", "STREAM", "STRUCT", "SYMMETRIC", "TAGGED", "TUPLE", "UNBOUNDED",
                                  "UNION", "VARIANT", "WHEN", "WHERE", "WINDOW", "WITHOUT"},
                                 [](const TString& token) {
                                     TStringBuilder req;
                                     req << "SELECT " << token << " FROM Plato.Input";
                                     return req;
                                 });
    UNIT_ASSERT_VALUES_EQUAL(failed, TVector<TString>{});
}

Y_UNIT_TEST(TokensAsWithoutColumnName) { // id_without
    auto failed = ValidateTokens({"ALL", "AS", "ASSUME", "ASYMMETRIC", "AUTOMAP", "BETWEEN", "BITCAST",
                                  "CALLABLE", "CASE", "CAST", "CUBE", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
                                  "DICT", "DISTINCT", "EMPTY_ACTION", "ENUM", "EXCEPT", "EXISTS", "FALSE", "FLOW", "FROM", "FULL",
                                  "HAVING", "HOP", "INTERSECT", "JSON_EXISTS", "JSON_QUERY", "JSON_VALUE", "LIMIT", "LIST",
                                  "NOT", "NULL", "OPTIONAL", "PROCESS", "REDUCE", "REPEATABLE", "RESOURCE", "RETURN", "RETURNING", "ROLLUP",
                                  "SELECT", "SET", "STRUCT", "SYMMETRIC", "TAGGED", "TRUE", "TUPLE", "UNBOUNDED",
                                  "UNION", "VARIANT", "WHEN", "WHERE", "WINDOW", "WITHOUT"},
                                 [](const TString& token) {
                                     TStringBuilder req;
                                     req << "SELECT * WITHOUT " << token << " FROM Plato.Input";
                                     return req;
                                 });
    UNIT_ASSERT_VALUES_EQUAL(failed, TVector<TString>{});
}

Y_UNIT_TEST(TokensAsColumnNameInAddColumn) { // id_schema
    auto failed = ValidateTokens({"ANY", "AUTOMAP", "CALLABLE", "COLUMN", "DICT", "ENUM", "ERASE", "FALSE", "FLOW",
                                  "LIST", "OPTIONAL", "REPEATABLE", "RESOURCE",
                                  "SET", "STREAM", "STRUCT", "TAGGED", "TRUE", "TUPLE", "VARIANT"},
                                 [](const TString& token) {
                                     TStringBuilder req;
                                     req << "ALTER TABLE ydb.Input ADD COLUMN " << token << " Bool";
                                     return req;
                                 });
    UNIT_ASSERT_VALUES_EQUAL(failed, TVector<TString>{});
}

Y_UNIT_TEST(TokensAsColumnAlias) {
    auto failed = ValidateTokens({"AUTOMAP", "FALSE",
                                  "REPEATABLE", "TRUE"},
                                 [](const TString& token) {
                                     TStringBuilder req;
                                     req << "SELECT Col as " << token << " FROM Plato.Input";
                                     return req;
                                 });
    UNIT_ASSERT_VALUES_EQUAL(failed, TVector<TString>{});
}

Y_UNIT_TEST(TokensAsTableName) { // id_table_or_type
    auto failed = ValidateTokens({"ANY", "AUTOMAP", "COLUMN", "ERASE", "FALSE",
                                  "REPEATABLE", "STREAM", "TRUE"},
                                 [](const TString& token) {
                                     TStringBuilder req;
                                     req << "SELECT * FROM Plato." << token;
                                     return req;
                                 });
    UNIT_ASSERT_VALUES_EQUAL(failed, TVector<TString>{});
}

Y_UNIT_TEST(TokensAsTableAlias) { // id_table
    auto failed = ValidateTokens({"AUTOMAP", "CALLABLE", "DICT", "ENUM", "FALSE", "FLOW",
                                  "LIST", "OPTIONAL", "REPEATABLE", "RESOURCE",
                                  "SET", "STRUCT", "TAGGED", "TRUE", "TUPLE", "VARIANT"},
                                 [](const TString& token) {
                                     TStringBuilder req;
                                     req << "SELECT * FROM Plato.Input AS " << token;
                                     return req;
                                 });
    UNIT_ASSERT_VALUES_EQUAL(failed, TVector<TString>{});
}

Y_UNIT_TEST(TokensAsHints) { // id_hint
    auto failed = ValidateTokens({"AUTOMAP", "CALLABLE", "COLUMNS", "DICT", "ENUM", "FALSE", "FLOW",
                                  "LIST", "OPTIONAL", "REPEATABLE", "RESOURCE",
                                  "SCHEMA", "SET", "STRUCT", "TAGGED", "TRUE", "TUPLE", "VARIANT",
                                  "WATERMARK"},
                                 [](const TString& token) {
                                     TStringBuilder req;
                                     req << "SELECT * FROM Plato.Input WITH " << token;
                                     return req;
                                 });
    UNIT_ASSERT_VALUES_EQUAL(failed, TVector<TString>{});
}

Y_UNIT_TEST(TokensAsWindow) { // id_window
    auto failed = ValidateTokens({"AUTOMAP", "CALLABLE", "DICT", "ENUM", "FALSE", "FLOW", "GROUPS", "LIST", "OPTIONAL",
                                  "RANGE", "REPEATABLE", "RESOURCE", "ROWS", "SET", "STRUCT", "TAGGED", "TRUE", "TUPLE", "VARIANT"},
                                 [](const TString& token) {
                                     TStringBuilder req;
                                     req << "SELECT * FROM Plato.Input WINDOW " << token << " AS ()";
                                     return req;
                                 });
    UNIT_ASSERT_VALUES_EQUAL(failed, TVector<TString>{});
}

Y_UNIT_TEST(TokensAsIdExprIn) { // id_expr_in
    auto failed = ValidateTokens({"ALL", "ANY", "AS", "ASSUME", "ASYMMETRIC", "AUTOMAP", "BETWEEN", "BITCAST",
                                  "CALLABLE", "CASE", "CAST", "COMPACT", "CUBE", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
                                  "DICT", "DISTINCT", "ENUM", "ERASE", "EXCEPT", "EXISTS", "FLOW", "FROM", "FULL",
                                  "HAVING", "HOP", "INTERSECT", "JSON_EXISTS", "JSON_QUERY", "JSON_VALUE", "LIMIT", "LIST",
                                  "NOT", "OPTIONAL", "PROCESS", "REDUCE", "REPEATABLE", "RESOURCE", "RETURN", "RETURNING", "ROLLUP",
                                  "SELECT", "SET", "STREAM", "STRUCT", "SYMMETRIC", "TAGGED", "TUPLE", "UNBOUNDED",
                                  "UNION", "VARIANT", "WHEN", "WHERE", "WINDOW", "WITHOUT"},
                                 [](const TString& token) {
                                     TStringBuilder req;
                                     req << "SELECT * FROM Plato.Input WHERE q IN " << token;
                                     return req;
                                 });
    UNIT_ASSERT_VALUES_EQUAL(failed, TVector<TString>{});
}

Y_UNIT_TEST(TableHints) {
    UNIT_ASSERT(SqlToYql("SELECT * FROM plato.Input WITH INFER_SCHEMA").IsOk());
    UNIT_ASSERT(SqlToYql("SELECT * FROM plato.Input WITH (INFER_SCHEMA)").IsOk());
}

Y_UNIT_TEST(TableHintsTableRef) {
    ExpectFailWithError(
        R"sql(
            $input = SELECT 1 AS ts;
            SELECT * FROM $input WITH XLOCK;
        )sql",
        "<main>:3:20: Error: Hint 'XLOCK' requires a table\n");

    auto res = SqlToYql("$input = SELECT 1 AS ts; SELECT * FROM $input WITH WATERMARK = ts");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"watermark", "WatermarkGenerator"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["watermark"], 0);
    UNIT_ASSERT_VALUES_EQUAL(stat["WatermarkGenerator"], 1);
}

Y_UNIT_TEST(TableHintsSelect) {
    ExpectFailWithError(
        R"sql(
            SELECT * FROM (SELECT 1 AS ts) WITH XLOCK;
        )sql",
        "<main>:2:20: Error: Hint 'XLOCK' requires a table\n");

    auto res = SqlToYql("SELECT * FROM (SELECT 1 AS ts) WITH WATERMARK = ts");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"watermark", "WatermarkGenerator"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["watermark"], 0);
    UNIT_ASSERT_VALUES_EQUAL(stat["WatermarkGenerator"], 1);
}

Y_UNIT_TEST(SelectStarStripsConfiguredSystemColumnPrefixes) {
    NSQLTranslation::TTranslationSettings settings;
    settings.ExtraSystemColumnPrefixes = {"__ydb_row_id"};
    auto res = SqlToYqlWithSettings("SELECT * FROM plato.Input;", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"RemoveSystemMembers", "RemovePrefixMembers"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["RemoveSystemMembers"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["RemovePrefixMembers"], 1);
}

Y_UNIT_TEST(SelectSystemColumnIsNotStrippedConfiguredSystemColumnPrefixes) {
    NSQLTranslation::TTranslationSettings settings;
    settings.ExtraSystemColumnPrefixes = {"__ydb_row_id"};
    auto res = SqlToYqlWithSettings("SELECT __ydb_row_id FROM plato.Input;", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"RemoveSystemMembers", "RemovePrefixMembers"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["RemoveSystemMembers"], 0);
    UNIT_ASSERT_VALUES_EQUAL(stat["RemovePrefixMembers"], 0);
}

Y_UNIT_TEST(SelectStarKeepsDefaultCleanupWithoutSystemColumnPrefixes) {
    auto res = SqlToYql("SELECT * FROM plato.Input;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"RemoveSystemMembers", "RemovePrefixMembers"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["RemoveSystemMembers"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["RemovePrefixMembers"], 0);
}

Y_UNIT_TEST(QualifiedAsteriskStripsConfiguredSystemColumnPrefixes) {
    NSQLTranslation::TTranslationSettings settings;
    settings.ExtraSystemColumnPrefixes = {"__ydb_row_id"};
    auto res = SqlToYqlWithSettings(
        "PRAGMA DisableSimpleColumns;"
        "SELECT interested_table.* FROM plato.Input AS interested_table;",
        settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "RemovePrefixMembers") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("interested_table._yql_"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("interested_table.__ydb_row_id"));
        }
    };
    TWordCountHive stat = {{TString("RemovePrefixMembers"), 0}};
    VerifyProgram(res, stat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(stat["RemovePrefixMembers"], 1);
}

Y_UNIT_TEST(TableHintsValues) {
    ExpectFailWithError(
        R"sql(
            SELECT * FROM (VALUES (1)) WITH XLOCK;
        )sql",
        "<main>:2:45: Error: Hint 'XLOCK' requires a table\n");

    auto res = SqlToYql("SELECT * FROM (VALUES (1)) WITH WATERMARK = ts");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"watermark", "WatermarkGenerator"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["watermark"], 0);
    UNIT_ASSERT_VALUES_EQUAL(stat["WatermarkGenerator"], 1);
}

Y_UNIT_TEST(TableRefAnonymousTableHintsRejected) {
    ExpectFailWithError(
        R"sql(
            USE plato;
            $input = SELECT 1 AS value;
            SELECT * FROM @$input WITH XLOCK;
        )sql",
        "<main>:4:40: Error: Hints are not supported for anonymous tables\n");
}

Y_UNIT_TEST(TableRefSubqueryHintsRejected) {
    ExpectFailWithError(
        R"sql(
            $input = SELECT 1 AS value;
            SELECT * FROM $input WITH XLOCK;
        )sql",
        "<main>:3:20: Error: Hint 'XLOCK' requires a table\n");
}

Y_UNIT_TEST(AsTableHintsRejected) {
    ExpectFailWithError(
        R"sql(
            SELECT * FROM AS_TABLE([<|value:1|>]) WITH XLOCK;
        )sql",
        "<main>:2:56: Error: Hint 'XLOCK' requires a table\n");
}

Y_UNIT_TEST(InNoHints) {
    TString query = "SELECT * FROM plato.Input WHERE key IN (1,2,3)";

    VerifySqlInHints(query, {"'('('warnNoAnsi))"}, {});
    VerifySqlInHints(query, {"'()"}, false);
    VerifySqlInHints(query, {"'('('ansi))"}, true);
}

Y_UNIT_TEST(InHintCompact) {
    // should parse COMPACT as hint
    TString query = "SELECT * FROM plato.Input WHERE key IN COMPACT(1, 2, 3)";

    VerifySqlInHints(query, {"'('isCompact)"});
}

Y_UNIT_TEST(InHintSubquery) {
    // should parse tableSource as hint
    TString query = "$subq = (SELECT key FROM plato.Input); SELECT * FROM plato.Input WHERE key IN $subq";

    VerifySqlInHints(query, {"'('tableSource)"});
}

Y_UNIT_TEST(InHintCompactSubquery) {
    TString query = "$subq = (SELECT key FROM plato.Input); SELECT * FROM plato.Input WHERE key IN COMPACT $subq";

    VerifySqlInHints(query, {"'('isCompact)", "'('tableSource)"});
}

Y_UNIT_TEST(CompactKeywordNotReservedForNames) {
    UNIT_ASSERT(SqlToYql("SELECT COMPACT FROM plato.Input WHERE COMPACT IN COMPACT(1, 2, 3)").IsOk());
    UNIT_ASSERT(SqlToYql("USE plato; SELECT * FROM COMPACT").IsOk());
}

Y_UNIT_TEST(FamilyKeywordNotReservedForNames) {
    // FIXME: check if we can get old behaviour
    // UNIT_ASSERT(SqlToYql("USE plato; CREATE TABLE FAMILY (FAMILY Uint32, PRIMARY KEY (FAMILY));").IsOk());
    // UNIT_ASSERT(SqlToYql("USE plato; SELECT FAMILY FROM FAMILY").IsOk());
    UNIT_ASSERT(SqlToYql("USE plato; SELECT FAMILY FROM Input").IsOk());
}

Y_UNIT_TEST(ResetKeywordNotReservedForNames) {
    UNIT_ASSERT(SqlToYql("USE ydb; CREATE TABLE RESET (RESET Uint32, PRIMARY KEY (RESET));").IsOk());
    UNIT_ASSERT(SqlToYql("USE ydb; SELECT RESET FROM RESET").IsOk());
}

Y_UNIT_TEST(SyncKeywordNotReservedForNames) {
    UNIT_ASSERT(SqlToYql("USE ydb; CREATE TABLE SYNC (SYNC Uint32, PRIMARY KEY (SYNC));").IsOk());
    UNIT_ASSERT(SqlToYql("USE ydb; SELECT SYNC FROM SYNC").IsOk());
}

Y_UNIT_TEST(AsyncKeywordNotReservedForNames) {
    UNIT_ASSERT(SqlToYql("USE ydb; CREATE TABLE ASYNC (ASYNC Uint32, PRIMARY KEY (ASYNC));").IsOk());
    UNIT_ASSERT(SqlToYql("USE ydb; SELECT ASYNC FROM ASYNC").IsOk());
}

Y_UNIT_TEST(DisableKeywordNotReservedForNames) {
    UNIT_ASSERT(SqlToYql("USE ydb; CREATE TABLE DISABLE (DISABLE Uint32, PRIMARY KEY (DISABLE));").IsOk());
    UNIT_ASSERT(SqlToYql("USE ydb; SELECT DISABLE FROM DISABLE").IsOk());
}

Y_UNIT_TEST(ChangefeedKeywordNotReservedForNames) {
    UNIT_ASSERT(SqlToYql("USE ydb; CREATE TABLE CHANGEFEED (CHANGEFEED Uint32, PRIMARY KEY (CHANGEFEED));").IsOk());
    UNIT_ASSERT(SqlToYql("USE ydb; SELECT CHANGEFEED FROM CHANGEFEED").IsOk());
}

Y_UNIT_TEST(ReplicationKeywordNotReservedForNames) {
    UNIT_ASSERT(SqlToYql("USE ydb; CREATE TABLE REPLICATION (REPLICATION Uint32, PRIMARY KEY (REPLICATION));").IsOk());
    UNIT_ASSERT(SqlToYql("USE ydb; SELECT REPLICATION FROM REPLICATION").IsOk());
}

Y_UNIT_TEST(TransferKeywordNotReservedForNames) {
    UNIT_ASSERT(SqlToYql("USE ydb; CREATE TABLE TRANSFER (TRANSFER Uint32, PRIMARY KEY (TRANSFER));").IsOk());
    UNIT_ASSERT(SqlToYql("USE ydb; SELECT TRANSFER FROM TRANSFER").IsOk());
}

Y_UNIT_TEST(SecondsKeywordNotReservedForNames) {
    UNIT_ASSERT(SqlToYql("USE ydb; CREATE TABLE SECONDS (SECONDS Uint32, PRIMARY KEY (SECONDS));").IsOk());
    UNIT_ASSERT(SqlToYql("USE ydb; SELECT SECONDS FROM SECONDS").IsOk());
}

Y_UNIT_TEST(MillisecondsKeywordNotReservedForNames) {
    UNIT_ASSERT(SqlToYql("USE ydb; CREATE TABLE MILLISECONDS (MILLISECONDS Uint32, PRIMARY KEY (MILLISECONDS));").IsOk());
    UNIT_ASSERT(SqlToYql("USE ydb; SELECT MILLISECONDS FROM MILLISECONDS").IsOk());
}

Y_UNIT_TEST(MicrosecondsKeywordNotReservedForNames) {
    UNIT_ASSERT(SqlToYql("USE ydb; CREATE TABLE MICROSECONDS (MICROSECONDS Uint32, PRIMARY KEY (MICROSECONDS));").IsOk());
    UNIT_ASSERT(SqlToYql("USE ydb; SELECT MICROSECONDS FROM MICROSECONDS").IsOk());
}

Y_UNIT_TEST(NanosecondsKeywordNotReservedForNames) {
    UNIT_ASSERT(SqlToYql("USE ydb; CREATE TABLE NANOSECONDS (NANOSECONDS Uint32, PRIMARY KEY (NANOSECONDS));").IsOk());
    UNIT_ASSERT(SqlToYql("USE ydb; SELECT NANOSECONDS FROM NANOSECONDS").IsOk());
}

Y_UNIT_TEST(Jubilee) {
    NYql::TAstParseResult res = SqlToYql("USE plato; INSERT INTO Arcadia (r2000000) VALUES (\"2M GET!!!\");");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(NoStatements) {
    using NSQLTranslation::TTranslationSettings;

    using TAstParseResult = std::variant<
        NYql::TAstParseResult,
        TVector<NYql::TAstParseResult>>;

    using TParser = std::function<TAstParseResult(
        const TString&, const TTranslationSettings&)>;

    auto translator = [] {
        NSQLTranslationV1::TLexers lexers = {
            .Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory(),
        };
        NSQLTranslationV1::TParsers parsers = {
            .Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory(),
        };
        return NSQLTranslationV1::MakeTranslator(lexers, parsers);
    }();

    const TVector<TString> inputs = {
        "",
        " ",
        "\n",
        "--!syntax_pg",
        "-- comment",
        "/* comment */",
        ";",
        ";;",
        "/* SELECT 1 */;",
    };

    const TVector<bool> flags = {false, true};

    const TVector<TParser> parsers = {
        [&](const TString& input, const TTranslationSettings& settings) -> TAstParseResult {
            NYql::TAstParseResult result = translator->TextToAst(
                input, settings, /*warningRules=*/nullptr, /*stmtParseInfo=*/nullptr);

            return std::move(result);
        },

        [&](const TString& input, const TTranslationSettings& settings) -> TAstParseResult {
            NYql::TIssues issues;
            google::protobuf::Message* message = translator->TextToMessage(
                input, "", issues, /*maxErrors=*/8, settings);
            UNIT_ASSERT_C(message, issues.ToString());

            NSQLTranslation::TSQLHints hints;
            NYql::TAstParseResult result = translator->TextAndMessageToAst(
                input, *message, hints, settings);

            return std::move(result);
        },

        [&](const TString& input, const TTranslationSettings& settings) -> TAstParseResult {
            TVector<NYql::TAstParseResult> results = translator->TextToManyAst(
                input, settings, /*warningRules=*/nullptr, /*stmtParseInfo=*/nullptr);

            return std::move(results);
        },
    };

    for (const auto& [input, allow, parse] : CartesianProduct(inputs, flags, parsers)) {
        google::protobuf::Arena arena;
        TTranslationSettings settings;
        settings.Arena = &arena;

        if (allow) {
            settings.Flags.emplace("AllowNoStatements");
        }

        TAstParseResult result = parse(input, settings);

        if (allow && std::holds_alternative<NYql::TAstParseResult>(result)) {
            const auto& res = std::get<NYql::TAstParseResult>(result);

            UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

            TWordCountHive stat = {"world"};
            TString program = VerifyProgram(res, stat);

            UNIT_ASSERT_NO_DIFF(program, "(\n(return world)\n)\n");
        } else if (allow && std::holds_alternative<TVector<NYql::TAstParseResult>>(result)) {
            const auto& res = std::get<TVector<NYql::TAstParseResult>>(result);

            UNIT_ASSERT_EQUAL(res.size(), 0);
        } else {
            auto res = std::visit(
                TOverloaded{
                    [](NYql::TAstParseResult&& x) {
                        return std::move(x);
                    },
                    [](TVector<NYql::TAstParseResult>&& xs) {
                        UNIT_ASSERT_EQUAL(xs.size(), 1);
                        return std::move(xs.back());
                    },
                }, std::move(result));

            UNIT_ASSERT(!res.IsOk());
            UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "Error: At least one statement was expected, but got none, code: 4603");
        }
    }
}

Y_UNIT_TEST(ParserRecovery) {
    NYql::TAstParseResult res = SqlToYql(" select1; select2;");
    UNIT_ASSERT(!res.IsOk());

    TString issues = Err2Str(res);
    UNIT_ASSERT_STRING_CONTAINS(issues, "extraneous input 'select1' expecting");
    UNIT_ASSERT_STRING_CONTAINS(issues, "extraneous input 'select2' expecting");
}

Y_UNIT_TEST(QualifiedAsteriskBefore) {
    NYql::TAstParseResult res = SqlToYql(
        "PRAGMA DisableSimpleColumns;"
        "select interested_table.*, LENGTH(value) AS megahelpful_len  from plato.Input as interested_table;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        static bool SeenStar = false;
        if (word == "FlattenMembers") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("interested_table."));
        } else if (word == "SqlProjectItem") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("megahelpful_len")));
            UNIT_ASSERT_VALUES_EQUAL(SeenStar, true);
        } else if (word == "SqlProjectStarItem") {
            SeenStar = true;
        }
    };
    TWordCountHive elementStat = {{TString("FlattenMembers"), 0}, {TString("SqlProjectItem"), 0}, {TString("SqlProjectStarItem"), 0}};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["FlattenMembers"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SqlProjectItem"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SqlProjectStarItem"]);
}

Y_UNIT_TEST(QualifiedAsteriskAfter) {
    NYql::TAstParseResult res = SqlToYql(
        "PRAGMA DisableSimpleColumns;"
        "select LENGTH(value) AS megahelpful_len, interested_table.*  from plato.Input as interested_table;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        static bool SeenStar = false;
        if (word == "FlattenMembers") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("interested_table."));
        } else if (word == "SqlProjectItem") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("megahelpful_len")));
            UNIT_ASSERT_VALUES_EQUAL(SeenStar, false);
        } else if (word == "SqlProjectStarItem") {
            SeenStar = true;
        }
    };
    TWordCountHive elementStat = {{TString("FlattenMembers"), 0}, {TString("SqlProjectItem"), 0}, {TString("SqlProjectStarItem"), 0}};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["FlattenMembers"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SqlProjectItem"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SqlProjectStarItem"]);
}

Y_UNIT_TEST(QualifiedMembers) {
    NYql::TAstParseResult res = SqlToYql("select interested_table.key, interested_table.value from plato.Input as interested_table;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        const bool fieldKey = TString::npos != line.find(Quote("key"));
        const bool fieldValue = TString::npos != line.find(Quote("value"));
        const bool refOnTable = TString::npos != line.find("interested_table.");
        if (word == "SqlProjectItem") {
            UNIT_ASSERT(fieldKey || fieldValue);
            UNIT_ASSERT(!refOnTable);
        } else if (word == "Write!") {
            UNIT_ASSERT(fieldKey && fieldValue && !refOnTable);
        }
    };
    TWordCountHive elementStat = {{TString("SqlProjectStarItem"), 0}, {TString("SqlProjectItem"), 0}, {TString("Write!"), 0}};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(0, elementStat["SqlProjectStarItem"]);
    UNIT_ASSERT_VALUES_EQUAL(2, elementStat["SqlProjectItem"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write!"]);
}

Y_UNIT_TEST(ExplainQueryPlan) {
    UNIT_ASSERT(SqlToYql("EXPLAIN SELECT 1;").IsOk());
    UNIT_ASSERT(SqlToYql("EXPLAIN QUERY PLAN SELECT 1;").IsOk());
}

Y_UNIT_TEST(JoinParseCorrect) {
    NYql::TAstParseResult res = SqlToYql(
        "PRAGMA DisableSimpleColumns;"
        " SELECT table_bb.*, table_aa.key as megakey"
        " FROM plato.Input AS table_aa"
        " JOIN plato.Input AS table_bb"
        " ON table_aa.value == table_bb.value;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "SelectMembers") {
            UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("table_aa."));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("table_bb."));
        } else if (word == "SqlProjectItem") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("megakey")));
        } else if (word == "SqlColumn") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("table_aa")));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("key")));
        }
    };
    TWordCountHive elementStat = {{TString("SqlProjectItem"), 0}, {TString("SqlProjectStarItem"), 0}, {TString("SelectMembers"), 0}, {TString("SqlColumn"), 0}};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SqlProjectItem"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SqlProjectStarItem"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SelectMembers"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SqlColumn"]);
}

Y_UNIT_TEST(Join3Table) {
    NYql::TAstParseResult res = SqlToYql(
        " PRAGMA DisableSimpleColumns;"
        " SELECT table_bb.*, table_aa.key as gigakey, table_cc.* "
        " FROM plato.Input AS table_aa"
        " JOIN plato.Input AS table_bb ON table_aa.key == table_bb.key"
        " JOIN plato.Input AS table_cc ON table_aa.subkey == table_cc.subkey;");
    Err2Str(res);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "SelectMembers") {
            UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("table_aa."));
            UNIT_ASSERT(line.find("table_bb.") != TString::npos || line.find("table_cc.") != TString::npos);
        } else if (word == "SqlProjectItem") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("gigakey")));
        } else if (word == "SqlColumn") {
            const auto posTableAA = line.find(Quote("table_aa"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, posTableAA);
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("key")));
            UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("table_aa", posTableAA + 3));
        }
    };
    TWordCountHive elementStat = {{TString("SqlProjectItem"), 0}, {TString("SqlProjectStarItem"), 0}, {TString("SelectMembers"), 0}, {TString("SqlColumn"), 0}};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SqlProjectItem"]);
    UNIT_ASSERT_VALUES_EQUAL(2, elementStat["SqlProjectStarItem"]);
    UNIT_ASSERT_VALUES_EQUAL(2, elementStat["SelectMembers"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SqlColumn"]);
}

Y_UNIT_TEST(DisabledJoinCartesianProduct) {
    NYql::TAstParseResult res = SqlToYql("pragma DisableAnsiImplicitCrossJoin; use plato; select * from A,B,C");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRINGS_EQUAL(Err2Str(res), "<main>:1:67: Error: Cartesian product of tables is disabled. Please use explicit CROSS JOIN or enable it via PRAGMA AnsiImplicitCrossJoin\n");
}

Y_UNIT_TEST(JoinCartesianProduct) {
    NYql::TAstParseResult res = SqlToYql("pragma AnsiImplicitCrossJoin; use plato; select * from A,B,C");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "EquiJoin") {
            auto pos = line.find("Cross");
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, pos);
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("Cross", pos + 1));
        }
    };
    TWordCountHive elementStat = {{TString("EquiJoin"), 0}};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["EquiJoin"]);
}

Y_UNIT_TEST(CreateAlterUserWithLoginNoLogin) {
    {
        auto reqCreateUser = SqlToYql(R"(
            USE plato;
            CREATE USER user1;
        )");

        UNIT_ASSERT(reqCreateUser.IsOk());

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            Y_UNUSED(word);
            UNIT_ASSERT(line.find("nullPassword") != TString::npos);
        };

        TWordCountHive elementStat = {{TString("createUser"), 0}};
        VerifyProgram(reqCreateUser, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(elementStat["createUser"], 1);
    }

    {
        auto reqAlterUser = SqlToYql(R"(
                    USE plato;
                    ALTER USER user1;
                )");

        UNIT_ASSERT(!reqAlterUser.IsOk());
        UNIT_ASSERT_STRING_CONTAINS(reqAlterUser.Issues.ToString(), "Error: mismatched input ';' expecting {ENCRYPTED, HASH, LOGIN, NOLOGIN, PASSWORD, RENAME, WITH}");
    }

    {
        auto reqCreateUserLogin = SqlToYql(R"(
            USE plato;
            CREATE USER user1 LOgin;
        )");

        UNIT_ASSERT(reqCreateUserLogin.IsOk());

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "createUser") {
                UNIT_ASSERT(line.find("nullPassword") != TString::npos);
            }
        };

        TWordCountHive elementStat = {{TString("alterUser"), 0}, {TString("createUser"), 0}};
        VerifyProgram(reqCreateUserLogin, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(elementStat["createUser"], 1);
        UNIT_ASSERT_VALUES_EQUAL(elementStat["alterUser"], 0);
    }

    {
        auto reqAlterUserLogin = SqlToYql(R"(
            USE plato;
            ALTER USER user1 LOgin;
        )");

        UNIT_ASSERT(reqAlterUserLogin.IsOk());

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "alterUser") {
                UNIT_ASSERT(line.find("nullPassword") == TString::npos);
            }
        };

        TWordCountHive elementStat = {{TString("alterUser"), 0}, {TString("createUser"), 0}};
        VerifyProgram(reqAlterUserLogin, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(elementStat["createUser"], 0);
        UNIT_ASSERT_VALUES_EQUAL(elementStat["alterUser"], 1);
    }

    {
        auto reqPasswordAndNoLogin = SqlToYql(R"(
            USE plato;
            CREATE USER user1 PASSWORD '123' NOLOGIN;
        )");

        UNIT_ASSERT(reqPasswordAndNoLogin.IsOk());

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            Y_UNUSED(word);
            UNIT_ASSERT(line.find("nullPassword") == TString::npos);
        };

        TWordCountHive elementStat = {{TString("createUser"), 0}};
        VerifyProgram(reqPasswordAndNoLogin, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(elementStat["createUser"], 1);
    }

    {
        auto reqAlterUserNullPassword = SqlToYql(R"(
            USE plato;
            ALTER USER user1 PASSWORD NULL;
        )");

        UNIT_ASSERT(reqAlterUserNullPassword.IsOk());

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            Y_UNUSED(word);
            UNIT_ASSERT(line.find("nullPassword") != TString::npos);
        };

        TWordCountHive elementStat = {{TString("alterUser"), 0}};
        VerifyProgram(reqAlterUserNullPassword, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(elementStat["alterUser"], 1);
    }

    auto reqLogin = SqlToYql(R"(
        USE plato;
        CREATE USER user1 LOGIN;
    )");

    UNIT_ASSERT(reqLogin.IsOk());

    auto reqNoLogin = SqlToYql(R"(
        USE plato;
        CREATE USER user1 NOLOGIN;
    )");

    UNIT_ASSERT(reqNoLogin.IsOk());

    auto reqLoginNoLogin = SqlToYql(R"(
        USE plato;
        CREATE USER user1 LOGIN NOLOGIN;
    )");

    UNIT_ASSERT(!reqLoginNoLogin.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(reqLoginNoLogin.Issues.ToString(), "Error: Conflicting or redundant options");

    auto reqAlterLoginNoLogin = SqlToYql(R"(
        USE plato;
        CREATE USER user1 LOGIN;
        ALTER USER user1 NOLOGIN;
    )");

    UNIT_ASSERT(reqAlterLoginNoLogin.IsOk());

    auto reqAlterLoginNoLoginWithPassword = SqlToYql(R"(
        USE plato;
        CREATE USER user1 LOGIN;
        ALTER USER user1 PASSWORD '321' NOLOGIN;
    )");

    UNIT_ASSERT(reqAlterLoginNoLoginWithPassword.IsOk());
}

Y_UNIT_TEST(CreateUserWithHash) {
    auto reqCreateUser = SqlToYql(R"(
                USE plato;
                CREATE USER user1 HASH '{
                    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                    "type": "argon2id"
                }';
            )");

    UNIT_ASSERT(reqCreateUser.IsOk());

    auto reqCreateUserWithNoLogin = SqlToYql(R"(
                USE plato;
                CREATE USER user1 HASH '{
                    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                    "type": "argon2id"
                }'
                NOLOGIN;
            )");

    UNIT_ASSERT(reqCreateUserWithNoLogin.IsOk());

    auto reqCreateUserWithPassword = SqlToYql(R"(
                USE plato;
                CREATE USER user1 HASH '{
                    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                    "type": "argon2id"
                }'
                PASSWORD '123';
            )");

    UNIT_ASSERT(!reqCreateUserWithPassword.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(reqCreateUserWithPassword.Issues.ToString(), "Error: Conflicting or redundant options");

    auto reqAlterUser = SqlToYql(R"(
                USE plato;
                CREATE USER user1;
                ALTER USER user1 HASH '{
                    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                    "type": "argon2id"
                }';
            )");

    UNIT_ASSERT(reqAlterUser.IsOk());
}

Y_UNIT_TEST(CreateUserQoutas) {
    {
        auto req = SqlToYql(R"(
                use plato;
                CREATE USER user1 PASSWORD passwd;
            )");

        TString error = "<main>:3:43: Error: mismatched input 'passwd' expecting {NULL, STRING_VALUE}\n";
        UNIT_ASSERT_VALUES_EQUAL(Err2Str(req), error);
        UNIT_ASSERT(!req.Root);
    }

    {
        auto req = SqlToYql(R"(
                use plato;
                CREATE USER user2 PASSWORD NULL;
            )");

        UNIT_ASSERT(req.Root);
    }

    {
        auto req = SqlToYql(R"(
                use plato;
                CREATE USER user3 PASSWORD '';
            )");

        UNIT_ASSERT(req.Root);
    }

    {
        auto req = SqlToYql(R"(
                use plato;
                CREATE USER user1 PASSWORD 'password1';
                CREATE USER user2 PASSWORD 'password2';
                CREATE USER user3;
            )");

        UNIT_ASSERT(req.Root);
    }
}

Y_UNIT_TEST(JoinWithoutConcreteColumns) {
    NYql::TAstParseResult res = SqlToYql(
        " use plato;"
        " SELECT a.v, b.value"
        "     FROM `Input1` VIEW `ksv` AS a"
        "     JOIN `Input2` AS b"
        "     ON a.k == b.key;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "SqlProjectItem") {
            UNIT_ASSERT(line.find(Quote("a.v")) != TString::npos || line.find(Quote("b.value")) != TString::npos);
        } else if (word == "SqlColumn") {
            const auto posTableA = line.find(Quote("a"));
            const auto posTableB = line.find(Quote("b"));
            if (posTableA != TString::npos) {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("v")));
            } else {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, posTableB);
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("value")));
            }
        }
    };
    TWordCountHive elementStat = {{TString("SqlProjectStarItem"), 0}, {TString("SqlProjectItem"), 0}, {TString("SqlColumn"), 0}};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(0, elementStat["SqlProjectStarItem"]);
    UNIT_ASSERT_VALUES_EQUAL(2, elementStat["SqlProjectItem"]);
    UNIT_ASSERT_VALUES_EQUAL(2, elementStat["SqlColumn"]);
}

Y_UNIT_TEST(JoinWithSameValues) {
    NYql::TAstParseResult res = SqlToYql("SELECT a.value, b.value FROM plato.Input AS a JOIN plato.Input as b ON a.key == b.key;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "SqlProjectItem") {
            const bool isValueFromA = TString::npos != line.find(Quote("a.value"));
            const bool isValueFromB = TString::npos != line.find(Quote("b.value"));
            UNIT_ASSERT(isValueFromA || isValueFromB);
        }
        if (word == "Write!") {
            UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("a.a."));
            UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("b.b."));
        }
    };
    TWordCountHive elementStat = {{TString("SqlProjectStarItem"), 0}, {TString("SqlProjectItem"), 0}, {"Write!", 0}};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(0, elementStat["SqlProjectStarItem"]);
    UNIT_ASSERT_VALUES_EQUAL(2, elementStat["SqlProjectItem"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write!"]);
}

Y_UNIT_TEST(SameColumnsForDifferentTables) {
    NYql::TAstParseResult res = SqlToYql("SELECT a.key, b.key FROM plato.Input as a JOIN plato.Input as b on a.key==b.key;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(SameColumnsForDifferentTablesFullJoin) {
    NYql::TAstParseResult res = SqlToYql("SELECT a.key, b.key, a.value, b.value FROM plato.Input AS a FULL JOIN plato.Input AS b USING(key);");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(JoinStreamLookupStrategyHint) {
    {
        NYql::TAstParseResult res = SqlToYql("SELECT * FROM plato.Input AS a JOIN /*+ StreamLookup() */ plato.Input AS b USING(key);");
        UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    }
    // case insensitive
    {
        NYql::TAstParseResult res = SqlToYql("SELECT * FROM plato.Input AS a JOIN /*+ streamlookup() */ plato.Input AS b USING(key);");
        UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    }
}

Y_UNIT_TEST(JoinConflictingStrategyHint) {
    {
        NYql::TAstParseResult res = SqlToYql("SELECT * FROM plato.Input AS a JOIN /*+ StreamLookup() */ /*+ Merge() */   plato.Input AS b USING(key);");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_STRINGS_EQUAL(Err2Str(res), "<main>:1:91: Error: Conflicting join strategy hints\n");
    }
}

Y_UNIT_TEST(JoinDuplicatingStrategyHint) {
    {
        NYql::TAstParseResult res = SqlToYql("SELECT * FROM plato.Input AS a JOIN /*+ StreamLookup() */ /*+ StreamLookup() */   plato.Input AS b USING(key);");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_STRINGS_EQUAL(Err2Str(res), "<main>:1:98: Error: Duplicate join strategy hint\n");
    }
}

Y_UNIT_TEST(WarnCrossJoinStrategyHint) {
    NYql::TAstParseResult res = SqlToYql("SELECT * FROM plato.Input AS a CROSS JOIN /*+ merge() */ plato.Input AS b;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_STRINGS_EQUAL(Err2Str(res), "<main>:1:32: Warning: Non-default join strategy will not be used for CROSS JOIN, code: 4534\n");
}

Y_UNIT_TEST(WarnCartesianProductStrategyHint) {
    NYql::TAstParseResult res = SqlToYql("pragma AnsiImplicitCrossJoin; use plato; SELECT * FROM A, /*+ merge() */ B;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_STRINGS_EQUAL(Err2Str(res), "<main>:1:74: Warning: Non-default join strategy will not be used for CROSS JOIN, code: 4534\n");
}

Y_UNIT_TEST(WarnUnknownJoinStrategyHint) {
    NYql::TAstParseResult res = SqlToYql("SELECT * FROM plato.Input AS a JOIN /*+ xmerge() */ plato.Input AS b USING (key);");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_STRINGS_EQUAL(Err2Str(res), "<main>:1:41: Warning: Unsupported join hint: xmerge, code: 4534\n");
}

Y_UNIT_TEST(NoWarnDiscardAtTopLevel) {
    NYql::TAstParseResult res = SqlToYql("DISCARD SELECT 1");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_STRINGS_EQUAL(Err2Str(res), "");
}

Y_UNIT_TEST(WarnDiscardInSubqueryFromClause) {
    NYql::TAstParseResult res = SqlToYql("SELECT * FROM (DISCARD SELECT 1)");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_STRINGS_EQUAL(Err2Str(res),
                              "<main>:1:16: Warning: DISCARD can only be used at the top level, not inside subqueries, code: 4541\n");
}

Y_UNIT_TEST(WarnDiscardInSubqueryFromClauseMultiline) {
    NYql::TAstParseResult res = SqlToYql(
        "SELECT * FROM \n"
        "(DISCARD SELECT 1)");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_STRINGS_EQUAL(Err2Str(res),
                              "<main>:2:2: Warning: DISCARD can only be used at the top level, not inside subqueries, code: 4541\n");
}

Y_UNIT_TEST(WarnDiscardInUnionAllSecondSubquery) {
    NYql::TAstParseResult res = SqlToYql("SELECT 1 UNION ALL (DISCARD SELECT 2)");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_STRINGS_EQUAL(Err2Str(res),
                              "<main>:1:21: Warning: DISCARD within set operators has no effect in second or later subqueries, code: 4541\n");
}

Y_UNIT_TEST(WarnDiscardInIntersectSecondSubquery) {
    NYql::TAstParseResult res = SqlToYql("SELECT 1 INTERSECT (DISCARD SELECT 2)");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_STRINGS_EQUAL(Err2Str(res),
                              "<main>:1:21: Warning: DISCARD within set operators has no effect in second or later subqueries, code: 4541\n");
}

Y_UNIT_TEST(WarnDiscardInExceptSecondSubquery) {
    NYql::TAstParseResult res = SqlToYql("SELECT 1 EXCEPT (DISCARD SELECT 2)");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_STRINGS_EQUAL(Err2Str(res),
                              "<main>:1:18: Warning: DISCARD within set operators has no effect in second or later subqueries, code: 4541\n");
}

Y_UNIT_TEST(WarnDiscardInUnionAllThirdSubquery) {
    NYql::TAstParseResult res = SqlToYql("SELECT 1 UNION ALL SELECT 2 UNION ALL (DISCARD SELECT 3)");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_STRINGS_EQUAL(Err2Str(res),
                              "<main>:1:40: Warning: DISCARD within set operators has no effect in second or later subqueries, code: 4541\n");
}

Y_UNIT_TEST(NoWarnDiscardFirstInUnionAll) {
    NYql::TAstParseResult res = SqlToYql("DISCARD SELECT 1 UNION ALL SELECT 2");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_STRINGS_EQUAL(Err2Str(res), "");
}

Y_UNIT_TEST(WarnDiscardInNamedSubquery) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
            $sub = (DISCARD SELECT 1);
            SELECT * FROM $sub
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res),
                                "Warning: DISCARD can only be used at the top level, not inside subqueries");
}

Y_UNIT_TEST(ReverseLabels) {
    NYql::TAstParseResult res = SqlToYql("select in.key as subkey, subkey as key from plato.Input as in;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(AutogenerationAliasWithoutCollisionConflict1) {
    NYql::TAstParseResult res = SqlToYql("select LENGTH(Value), key as column1 from plato.Input;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(AutogenerationAliasWithoutCollision2Conflict2) {
    NYql::TAstParseResult res = SqlToYql("select key as column0, LENGTH(Value) from plato.Input;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(InputAliasForQualifiedAsterisk) {
    NYql::TAstParseResult res = SqlToYql("use plato; select zyuzya.*, key from plato.Input as zyuzya;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(SelectSupportsResultColumnsWithTrailingComma) {
    NYql::TAstParseResult res = SqlToYql("select a, b, c, from plato.Input;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(SelectOrderByLabeledColumn) {
    NYql::TAstParseResult res = SqlToYql("pragma DisableOrderedColumns; select key as goal from plato.Input order by goal");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "DataSource") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("plato"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("Input"));

            UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("goal"));
        } else if (word == "Sort") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("goal"));

            UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("key"));
        }
    };
    TWordCountHive elementStat = {{TString("DataSource"), 0}, {TString("Sort"), 0}};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["DataSource"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Sort"]);
}

Y_UNIT_TEST(SelectOrderBySimpleExpr) {
    NYql::TAstParseResult res = SqlToYql("select a from plato.Input order by a + a");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(SelectAssumeOrderByTableRowAccess) {
    NYql::TAstParseResult res = SqlToYql("$key = 'foo';select * from plato.Input assume order by TableRow().$key");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(SelectOrderByDuplicateLabels) {
    NYql::TAstParseResult res = SqlToYql("select a from plato.Input order by a, a");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(SelectOrderByExpression) {
    NYql::TAstParseResult res = SqlToYql("select * from plato.Input as i order by cast(key as uint32) + cast(subkey as uint32)");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Sort") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("\"+MayWarn\""));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("key"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("subkey"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("(Bool 'true)"));

            UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("i.key"));
            UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("i.subkey"));
        }
    };
    TWordCountHive elementStat = {{TString("Sort"), 0}};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Sort"]);
}

Y_UNIT_TEST(SelectOrderByExpressionDesc) {
    NYql::TAstParseResult res = SqlToYql("pragma disablesimplecolumns; select i.*, key, subkey from plato.Input as i order by cast(i.key as uint32) - cast(i.subkey as uint32) desc");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Sort") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("\"-MayWarn\""));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("\"key\""));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("\"subkey\""));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("(Bool 'false)"));
        } else if (word == "Write!") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'columns"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("\"key\""));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("\"subkey\""));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("prefix"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("\"i.\""));
        }
    };
    TWordCountHive elementStat = {{TString("Sort"), 0}, {TString("Write!"), 0}};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Sort"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write!"]);
}

Y_UNIT_TEST(SelectOrderByExpressionAsc) {
    NYql::TAstParseResult res = SqlToYql("select i.key, i.subkey from plato.Input as i order by cast(key as uint32) % cast(i.subkey as uint32) asc");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Sort") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("\"%MayWarn\""));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("\"key\""));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("\"subkey\""));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("(Bool 'true)"));
        } else if (word == "Write!") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'columns"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("\"key\""));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("\"subkey\""));
            UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("i."));
        }
    };
    TWordCountHive elementStat = {{TString("Sort"), 0}, {TString("Write!"), 0}};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Sort"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write!"]);
}

Y_UNIT_TEST(ReferenceToKeyInSubselect) {
    NYql::TAstParseResult res = SqlToYql("select b.key from (select a.key from plato.Input as a) as b;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(OrderByCastValue) {
    NYql::TAstParseResult res = SqlToYql("select i.key, i.subkey from plato.Input as i order by cast(key as uint32) desc;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(GroupByCastValue) {
    NYql::TAstParseResult res = SqlToYql("select count(1) from plato.Input as i group by cast(key as uint8);");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(KeywordInSelectColumns) {
    NYql::TAstParseResult res = SqlToYql("select in, s.check from (select 1 as in, \"test\" as check) as s;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(SelectAllGroupBy) {
    NYql::TAstParseResult res = SqlToYql("select * from plato.Input group by subkey;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(CreateObjectWithFeatures) {
    NYql::TAstParseResult res = SqlToYql("USE plato; CREATE OBJECT secretId (TYPE SECRET) WITH (Key1=Value1, K2=V2);");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'('\"K2\" '\"V2\") '('\"Key1\" '\"Value1\")"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("createObject"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}, {TString("SECRET"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SECRET"]);
}

Y_UNIT_TEST(CreateObjectIfNotExists) {
    NYql::TAstParseResult res = SqlToYql("USE plato; CREATE OBJECT IF NOT EXISTS secretId (TYPE SECRET) WITH (Key1=Value1, K2=V2);");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("createObjectIfNotExists"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}, {TString("SECRET"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SECRET"]);
}

Y_UNIT_TEST(CreateObjectWithFeaturesStrings) {
    NYql::TAstParseResult res = SqlToYql(R"(USE plato; CREATE OBJECT secretId (TYPE SECRET) WITH (Key1="Value1", K2='V2', K3=V3, K4='', K5=`aaa`, K6='a\'aa');)");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'('\"K2\" '\"V2\") '('\"K3\" '\"V3\") '('\"K4\" '\"\") '('\"K5\" '\"aaa\") '('\"K6\" '\"a'aa\") '('\"Key1\" '\"Value1\")"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("createObject"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}, {TString("SECRET"), 0}};
    VerifyProgram(res, elementStat, verifyLine);
}

Y_UNIT_TEST(UpsertObjectWithFeatures) {
    NYql::TAstParseResult res = SqlToYql("USE plato; UPSERT OBJECT secretId (TYPE SECRET) WITH (Key1=Value1, K2=V2);");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'('\"K2\" '\"V2\") '('\"Key1\" '\"Value1\")"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("upsertObject"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}, {TString("SECRET"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SECRET"]);
}

Y_UNIT_TEST(CreateObjectWithFeaturesAndFlags) {
    NYql::TAstParseResult res = SqlToYql("USE plato; CREATE OBJECT secretId (TYPE SECRET) WITH (Key1=Value1, K2=V2, RECURSE);");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'('\"Key1\" '\"Value1\") '('\"RECURSE\")"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("createObject"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}, {TString("SECRET"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SECRET"]);
}

Y_UNIT_TEST(CreateObjectWithFeaturesWithoutCluster) {
    ExpectFailWithError(R"sql(
        CREATE OBJECT secretId (TYPE SECRET)
        WITH (Key1=Value1, K2=V2);
    )sql", "<main>:2:9: Error: No cluster name given and no default cluster is selected\n");
}

Y_UNIT_TEST(Select1Type) {
    NYql::TAstParseResult res = SqlToYql("SELECT 1 type;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(SelectTableType) {
    NYql::TAstParseResult res = SqlToYql("USE plato; SELECT * from T type;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(CreateObjectNoFeatures) {
    NYql::TAstParseResult res = SqlToYql("USE plato; CREATE OBJECT secretId (TYPE SECRET);");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("'features"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("createObject"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}, {TString("SECRET"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SECRET"]);
}

Y_UNIT_TEST(AlterObjectWithFeatures) {
    NYql::TAstParseResult res = SqlToYql(
        "USE plato;\n"
        "declare $path as String;\n"
        "ALTER OBJECT secretId (TYPE SECRET) SET (Key1=$path, K2=V2);");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'features"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'\"Key1\" (EvaluateAtom \"$path\""));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'\"K2\" '\"V2\""));

            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("alterObject"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}, {TString("SECRET"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SECRET"]);
}

Y_UNIT_TEST(AlterObjectNoFeatures) {
    NYql::TAstParseResult res = SqlToYql("USE ydb;   ALTER OBJECT secretId (TYPE SECRET);");
    UNIT_ASSERT(!res.IsOk());
}

Y_UNIT_TEST(DropObjectNoFeatures) {
    NYql::TAstParseResult res = SqlToYql("USE plato; DROP OBJECT secretId (TYPE SECRET);");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("'features"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("dropObject"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}, {TString("SECRET"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SECRET"]);
}

Y_UNIT_TEST(DropObjectWithFeatures) {
    NYql::TAstParseResult res = SqlToYql("USE plato; DROP OBJECT secretId (TYPE SECRET) WITH (A, B, C);");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'features"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("dropObject"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}, {TString("SECRET"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SECRET"]);
}

Y_UNIT_TEST(DropObjectWithOneOption) {
    NYql::TAstParseResult res = SqlToYql("USE plato; DROP OBJECT secretId (TYPE SECRET) WITH OVERRIDE;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'features"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'\"OVERRIDE\""));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("dropObject"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}, {TString("SECRET"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SECRET"]);
}

Y_UNIT_TEST(DropObjectIfExists) {
    NYql::TAstParseResult res = SqlToYql("USE plato; DROP OBJECT IF EXISTS secretId (TYPE SECRET);");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("dropObjectIfExists"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}, {TString("SECRET"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SECRET"]);
}

Y_UNIT_TEST(PrimaryKeyParseCorrect) {
    NYql::TAstParseResult res = SqlToYql("USE ydb; CREATE TABLE tableName (Key Uint32, Subkey Int64, Value String, PRIMARY KEY (Key, Subkey));");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("\"Key\""));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("\"Subkey\""));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}, {TString("primarykey"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["primarykey"]);
}

Y_UNIT_TEST(AlterDatabaseAst) {
    NYql::TAstParseResult request = SqlToYql("USE ydb;   ALTER DATABASE `/Root/test` OWNER TO user1;");
    UNIT_ASSERT_C(request.IsOk(), request.Issues.ToString());

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        Y_UNUSED(word);

        UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(
                                                      R"(let world (Write! world sink (Key '('databasePath (String '"/Root/test"))) (Void) '('('mode 'alterDatabase) '('owner '"user1"))))"));
    };

    TWordCountHive elementStat({TString("\'mode \'alterDatabase")});
    VerifyProgram(request, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["\'mode \'alterDatabase"]);
}

Y_UNIT_TEST(AlterDatabaseSetting) {
    NYql::TAstParseResult res = SqlToYql("USE ydb;   ALTER DATABASE `/Root/test` SET (key1 = 1);");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
        if (word == "Write!") {
            UNIT_ASSERT_STRING_CONTAINS(line, "(Key '('databasePath (String '\"/Root/test\")))");
            UNIT_ASSERT_STRING_CONTAINS(line, "'('('mode 'alterDatabase) '('\"KEY1\" (Uint64 '\"1\")))");
        }
    };

    TWordCountHive elementStat = {{"Write!"}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(elementStat["Write!"], 1);
}

Y_UNIT_TEST(AlterDatabaseSettings) {
    NYql::TAstParseResult res = SqlToYql("USE ydb;   ALTER DATABASE `/Root/test` SET (key1 = 1, key2 = \"2\", key3 = true);");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
        if (word == "Write!") {
            UNIT_ASSERT_STRING_CONTAINS(line, "(Key '('databasePath (String '\"/Root/test\")))");
            UNIT_ASSERT_STRING_CONTAINS(line, "'('mode 'alterDatabase)");
            UNIT_ASSERT_STRING_CONTAINS(line, "'('\"KEY1\" (Uint64 '\"1\"))");
            UNIT_ASSERT_STRING_CONTAINS(line, "'('\"KEY2\" (String '\"2\"))");
            UNIT_ASSERT_STRING_CONTAINS(line, "'('\"KEY3\" (Bool '\"true\"))");
        }
    };

    TWordCountHive elementStat = {{"Write!"}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(elementStat["Write!"], 1);
}

Y_UNIT_TEST(TruncateTableAstYdb) {
    auto executeTruncateRequest = [](const TString& sql, const TString& tableName) {
        TVerifyLineFunc verifyLine = [&tableName](const TString& word, const TString& line) {
            TStringBuilder sb;

            if (tableName == "@tmp") {
                sb << R"((let world (Write! world sink (TempTable '"tmp") (Void) '('('mode 'truncateTable)))))";
            } else {
                sb << R"((let world (Write! world sink (Key '('tablescheme (String '")"
                   << tableName
                   << R"("))) (Void) '('('mode 'truncateTable)))))";
            }

            Y_UNUSED(word);
            UNIT_ASSERT_VALUES_UNEQUAL(
                TString::npos,
                line.find(static_cast<TString>(sb)));
        };

        NYql::TAstParseResult request = SqlToYql(sql, 1, TString(NYql::KikimrProviderName));
        UNIT_ASSERT_C(request.IsOk(), request.Issues.ToString());
        TWordCountHive elementStat({TString("\'mode \'truncateTable")});
        VerifyProgram(request, elementStat, verifyLine);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["\'mode \'truncateTable"]);
    };

    executeTruncateRequest("USE ydb;   TRUNCATE TABLE `/Root/test/table`;", "/Root/test/table");
    executeTruncateRequest("USE ydb;   TRUNCATE TABLE plato.`Input`;", "Input");
    executeTruncateRequest("USE ydb;   TRUNCATE TABLE `/Root/test/table` WITH INFER_SCHEMA;", "/Root/test/table");
    executeTruncateRequest("USE ydb;   TRUNCATE TABLE @tmp;", "@tmp");
}

Y_UNIT_TEST(TruncateTableAstNotYdb) {
    auto executeTruncateRequest = [](const TString& sql, const TString& tableName) {
        TVerifyLineFunc verifyLine = [&tableName](const TString& word, const TString& line) {
            TStringBuilder sb;

            if (tableName == "@tmp") {
                sb << R"((let world (Write! world sink (TempTable '"tmp") (Void) '('('mode 'truncateTable)))))";
            } else {
                sb << R"((let world (Write! world sink (Key '('tablescheme (String '")"
                   << tableName
                   << R"("))) (Void) '('('mode 'truncateTable)))))";
            }

            Y_UNUSED(word);
            UNIT_ASSERT_VALUES_UNEQUAL(
                TString::npos,
                line.find(static_cast<TString>(sb)));
        };

        NYql::TAstParseResult request = SqlToYql(sql);
        UNIT_ASSERT_C(!request.IsOk(), request.Issues.ToString());
        UNIT_ASSERT_STRING_CONTAINS(request.Issues.ToString(), "TRUNCATE TABLE is unsupported for");
    };

    executeTruncateRequest("USE plato;   TRUNCATE TABLE `/Root/test/table`;", "/Root/test/table");
    executeTruncateRequest("USE plato;   TRUNCATE TABLE plato.`Input`;", "Input");
    executeTruncateRequest("USE plato;   TRUNCATE TABLE `/Root/test/table` WITH INFER_SCHEMA;", "/Root/test/table");
    executeTruncateRequest("USE plato;   TRUNCATE TABLE @tmp;", "@tmp");
}

Y_UNIT_TEST(CreateTableDublicateOptions) {
    {
        NYql::TAstParseResult req = SqlToYql(R"sql(
            USE ydb;
            CREATE TABLE tbl (
                k Uint64,
                v Utf8 NOT NULL NOT NULL,
                PRIMARY KEY (k)
            );
        )sql");

        UNIT_ASSERT(!req.IsOk());
        UNIT_ASSERT_STRING_CONTAINS(req.Issues.ToString(), R"('NOT NULL' option can be specified only once)");
    }

    {
        NYql::TAstParseResult req = SqlToYql(R"sql(
            USE ydb;
            CREATE TABLE tbl (
                k Uint64,
                v Utf8 (NOT NULL, NOT NULL),
                PRIMARY KEY (k)
            );
        )sql");

        UNIT_ASSERT(!req.IsOk());
        UNIT_ASSERT_STRING_CONTAINS(req.Issues.ToString(), R"('NOT NULL' option can be specified only once)");
    }

    {
        NYql::TAstParseResult req = SqlToYql(R"sql(
            USE ydb;
            CREATE TABLE tbl (
                k Uint64,
                v Uint64 DEFAULT 0 DEFAULT 1,
                PRIMARY KEY (k)
            );
        )sql");

        UNIT_ASSERT(!req.IsOk());
        UNIT_ASSERT_STRING_CONTAINS(req.Issues.ToString(), R"('DEFAULT' option can be specified only once)");
    }

    {
        NYql::TAstParseResult req = SqlToYql(R"sql(
            USE ydb;
            CREATE TABLE tbl (
                k Uint64,
                v Uint64 (DEFAULT 1, DEFAULT 0),
                PRIMARY KEY (k)
            );
        )sql");

        UNIT_ASSERT(!req.IsOk());
        UNIT_ASSERT_STRING_CONTAINS(req.Issues.ToString(), R"('DEFAULT' option can be specified only once)");
    }

    {
        NYql::TAstParseResult req = SqlToYql(R"sql(
            USE ydb;
            CREATE TABLE tbl (
                k Uint64,
                v Utf8 FAMILY family_large FAMILY family_large,
                PRIMARY KEY (k),
                FAMILY default (
                    DATA = "ssd",
                    COMPRESSION = "off"
                ),
                FAMILY family_large (
                    DATA = "rot",
                    COMPRESSION = "lz4"
                )
            );
        )sql");

        UNIT_ASSERT(!req.IsOk());
        UNIT_ASSERT_STRING_CONTAINS(req.Issues.ToString(), R"('FAMILY' option can be specified only once)");
    }

    {
        NYql::TAstParseResult req = SqlToYql(R"sql(
            USE ydb;
            CREATE TABLE tbl (
                k Uint64,
                v Utf8 (FAMILY family_large, FAMILY family_large),
                PRIMARY KEY (k),
                FAMILY default (
                    DATA = "ssd",
                    COMPRESSION = "off"
                ),
                FAMILY family_large (
                    DATA = "rot",
                    COMPRESSION = "lz4"
                )
            );
        )sql");

        UNIT_ASSERT(!req.IsOk());
        UNIT_ASSERT_STRING_CONTAINS(req.Issues.ToString(), R"('FAMILY' option can be specified only once)");
    }
}

Y_UNIT_TEST(CreateTableFamilyAndNotNullInOrder) {
    NYql::TAstParseResult familyBeforeConstraint = SqlToYql(R"sql(
            USE ydb;
            CREATE TABLE tbl (
                k Uint64,
                v Utf8 FAMILY family_large NOT NULL,
                PRIMARY KEY (k),
                FAMILY default (
                    DATA = "ssd",
                    COMPRESSION = "off"
                ),
                FAMILY family_large (
                    DATA = "rot",
                    COMPRESSION = "lz4"
                )
            );
        )sql");

    UNIT_ASSERT_C(familyBeforeConstraint.IsOk(), familyBeforeConstraint.Issues.ToString());

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write!") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos,
                                       line.find(R"__('('columnConstrains '('('not_null))) '('"family_large")))))__"));
        }
    };

    TWordCountHive elementStat = {{TString("Write!"), 0}};
    VerifyProgram(familyBeforeConstraint, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write!"]);
}

Y_UNIT_TEST(CreateTableFamilyAndNotNullReversed) {
    NYql::TAstParseResult familyAfterConstraint = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE tbl (
            k Uint64,
            v Utf8 NOT NULL FAMILY family_large,
            PRIMARY KEY (k),
            FAMILY default (
                DATA = "ssd",
                COMPRESSION = "off"
            ),
            FAMILY family_large (
                DATA = "rot",
                COMPRESSION = "lz4"
            )
        );
    )sql");

    UNIT_ASSERT_C(familyAfterConstraint.IsOk(), familyAfterConstraint.Issues.ToString());

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write!") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos,
                                       line.find(R"__('('columnConstrains '('('not_null))) '('"family_large")))))__"));
        }
    };

    TWordCountHive elementStat = {{TString("Write!"), 0}};
    VerifyProgram(familyAfterConstraint, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write!"]);
}

Y_UNIT_TEST(CreateTableNotNullInsideDefault) {
    {
        NYql::TAstParseResult req = SqlToYql(R"sql(
                USE ydb;
                CREATE TABLE tbl (
                    k Uint64,
                    v Bool DEFAULT false NOT NULL,
                    PRIMARY KEY (k)
                );
            )sql");

        UNIT_ASSERT(!req.IsOk());
        UNIT_ASSERT_STRING_CONTAINS(req.Issues.ToString(), R"('DEFAULT' option can not use expr which contains literall 'NOT NULL')");
    }

    {
        NYql::TAstParseResult req = SqlToYql(R"sql(
                USE ydb;
                CREATE TABLE tbl (
                    k Uint64,
                    v Bool DEFAULT (false + true NOT NULL),
                    PRIMARY KEY (k)
                );
            )sql");

        UNIT_ASSERT(!req.IsOk());
        UNIT_ASSERT_STRING_CONTAINS(req.Issues.ToString(), R"('DEFAULT' option can not use expr which contains literall 'NOT NULL')");
    }

    {
        NYql::TAstParseResult req = SqlToYql(R"sql(
            USE ydb;
            CREATE TABLE tbl (
                k Uint64,
                v Bool DEFAULT (NULL NOT NULL),
                PRIMARY KEY (k)
            );
        )sql");

        UNIT_ASSERT(!req.IsOk());
        UNIT_ASSERT_STRING_CONTAINS(req.Issues.ToString(), R"('DEFAULT' option can not use expr which contains literall 'NOT NULL')");
    }
}

Y_UNIT_TEST(CreateTableDefaultAndNotNullInOrderWithComma) {
    NYql::TAstParseResult defaultBeforeConstraint = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE tbl (
            k Uint64,
            v Bool (DEFAULT false, NOT NULL),
            PRIMARY KEY (k)
        );
    )sql");

    UNIT_ASSERT_C(defaultBeforeConstraint.IsOk(), defaultBeforeConstraint.Issues.ToString());

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write!") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos,
                                       line.find(R"__(('columnConstrains '('('not_null) '('default (Bool '"false")))) '()))))__"));
        }
    };

    TWordCountHive elementStat = {{TString("Write!"), 0}};
    VerifyProgram(defaultBeforeConstraint, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write!"]);
}

Y_UNIT_TEST(CreateTableDefaultAndNotNullReversed) {
    NYql::TAstParseResult defaultAfterConstraint = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE tbl (
            k Uint64,
            v Uint64 NOT NULL DEFAULT 0,
            PRIMARY KEY (k)
        );
    )sql");

    UNIT_ASSERT_C(defaultAfterConstraint.IsOk(), defaultAfterConstraint.Issues.ToString());

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write!") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos,
                                       line.find(R"__('('columnConstrains '('('not_null) '('default (Int32 '"0")))) '()))))__"));
        }
    };

    TWordCountHive elementStat = {{TString("Write!"), 0}};
    VerifyProgram(defaultAfterConstraint, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write!"]);
}

Y_UNIT_TEST(CreateTableNonNullableYqlTypeAstCorrect) {
    NYql::TAstParseResult res = SqlToYql("USE plato; CREATE TABLE t (a int32 not null);");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write!") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos,
                                       line.find(R"__((Write! world sink (Key '('tablescheme (String '"t"))) values '('('mode 'create) '('columns '('('"a" (DataType 'Int32) '('columnConstrains '('('not_null))) '())))))))__"));
        }
    };

    TWordCountHive elementStat = {{TString("Write!"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write!"]);
}

Y_UNIT_TEST(CreateTableNullableYqlTypeAstCorrect) {
    NYql::TAstParseResult res = SqlToYql("USE plato; CREATE TABLE t (a int32);");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write!") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos,
                                       line.find(R"__((Write! world sink (Key '('tablescheme (String '"t"))) values '('('mode 'create) '('columns '('('"a" (AsOptionalType (DataType 'Int32)) '('columnConstrains '()) '()))))))__"));
        }
    };

    TWordCountHive elementStat = {{TString("Write!"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write!"]);
}

Y_UNIT_TEST(CreateTableNonNullablePgTypeAstCorrect) {
    NYql::TAstParseResult res = SqlToYql("USE plato; CREATE TABLE t (a pg_int4 not null);");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write!") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos,
                                       line.find(R"__((Write! world sink (Key '('tablescheme (String '"t"))) values '('('mode 'create) '('columns '('('"a" (PgType '_int4) '('columnConstrains '('('not_null))) '())))))))__"));
        }
    };

    TWordCountHive elementStat = {{TString("Write!"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write!"]);
}

Y_UNIT_TEST(CreateTableNullablePgTypeAstCorrect) {
    NYql::TAstParseResult res = SqlToYql("USE plato; CREATE TABLE t (a pg_int4);");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    res.Root->PrettyPrintTo(Cout, PRETTY_FLAGS);

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write!") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos,
                                       line.find(R"__((Write! world sink (Key '('tablescheme (String '"t"))) values '('('mode 'create) '('columns '('('"a" (AsOptionalType (PgType '_int4)) '('columnConstrains '()) '()))))))__"));
        }
    };

    TWordCountHive elementStat = {{TString("Write!"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write!"]);
}

Y_UNIT_TEST(CreateTableNullPkColumnsAreAllowed) {
    NYql::TAstParseResult res = SqlToYql("USE ydb; CREATE TABLE t (a int32, primary key(a));");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write!") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos,
                                       line.find(R"__((Write! world sink (Key '('tablescheme (String '"t"))) values '('('mode 'create) '('columns '('('"a" (AsOptionalType (DataType 'Int32)) '('columnConstrains '()) '()))) '('primarykey '('"a")))))__"));
        }
    };

    TWordCountHive elementStat = {{TString("Write!"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write!"]);
}

Y_UNIT_TEST(CreateTableNotNullPkColumnsAreIdempotentAstCorrect) {
    NYql::TAstParseResult res = SqlToYql("USE ydb; CREATE TABLE t (a int32 not null, primary key(a));");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write!") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos,
                                       line.find(R"__((Write! world sink (Key '('tablescheme (String '"t"))) values '('('mode 'create) '('columns '('('"a" (DataType 'Int32) '('columnConstrains '('('not_null))) '()))) '('primarykey '('"a"))))))__"));
        }
    };

    TWordCountHive elementStat = {{TString("Write!"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write!"]);
}

Y_UNIT_TEST(CreateTableWithIfNotExists) {
    NYql::TAstParseResult res = SqlToYql("USE ydb; CREATE TABLE IF NOT EXISTS t (a int32, primary key(a));");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write!") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos,
                                       line.find(R"__((Write! world sink (Key '('tablescheme (String '"t"))) values '('('mode 'create_if_not_exists) '('columns '('('"a" (AsOptionalType (DataType 'Int32)) '('columnConstrains '()) '()))) '('primarykey '('"a")))))__"));
        }
    };

    TWordCountHive elementStat = {{TString("Write!"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write!"]);
}

Y_UNIT_TEST(CreateTableWithIfNotExistsYt) {
    NYql::TAstParseResult res = SqlToYql("USE plato; CREATE TABLE IF NOT EXISTS t (a int32);");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write!") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos,
                                       line.find(R"__((Write! world sink (Key '('tablescheme (String '"t"))) values '('('mode 'create_if_not_exists) '('columns '('('"a" (AsOptionalType (DataType 'Int32)) '('columnConstrains '()) '()))))))__"));
        }
    };
    TWordCountHive elementStat = {{TString("Write!"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write!"]);
}

Y_UNIT_TEST(CreateTempTable) {
    NYql::TAstParseResult res = SqlToYql("USE ydb; CREATE TEMP TABLE t (a int32, primary key(a));");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write!") {
            UNIT_ASSERT_VALUES_UNEQUAL_C(TString::npos,
                                         line.find(R"__((Write! world sink (Key '('tablescheme (String '"t"))) values '('('mode 'create) '('columns '('('"a" (AsOptionalType (DataType 'Int32)) '('columnConstrains '()) '()))) '('primarykey '('"a")) '('temporary))))__"), line);
        }
    };

    TWordCountHive elementStat = {{TString("Write!"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write!"]);
}

Y_UNIT_TEST(CreateTemporaryTable) {
    NYql::TAstParseResult res = SqlToYql("USE ydb; CREATE TEMPORARY TABLE t (a int32, primary key(a));");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write!") {
            UNIT_ASSERT_VALUES_UNEQUAL_C(TString::npos,
                                         line.find(R"__((Write! world sink (Key '('tablescheme (String '"t"))) values '('('mode 'create) '('columns '('('"a" (AsOptionalType (DataType 'Int32)) '('columnConstrains '()) '()))) '('primarykey '('"a")) '('temporary))))__"), line);
        }
    };

    TWordCountHive elementStat = {{TString("Write!"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write!"]);
}

Y_UNIT_TEST(CreateTableWithoutTypes) {
    NYql::TAstParseResult res = SqlToYql("USE ydb; CREATE TABLE t (a, primary key(a));");
    UNIT_ASSERT(!res.IsOk());
}

Y_UNIT_TEST(CreateTableAsSelectWithTypes) {
    NYql::TAstParseResult res = SqlToYql("USE ydb; CREATE TABLE t (a int32, primary key(a)) AS SELECT * FROM ts;");
    UNIT_ASSERT(!res.IsOk());
}

Y_UNIT_TEST(CreateTableWithOrderBy) {
    const auto res = SqlToYql("USE plato; CREATE TABLE t (a text, b bytes, order by(b asc, a desc));");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write!") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(R"__('('orderby '('('"b" '0) '('"a" '1))))__"));
        }
    };

    TWordCountHive elementStat = {{TString("Write!"), 0}};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write!"]);
}

Y_UNIT_TEST(CreateTableAsSelect) {
    NYql::TAstParseResult res = SqlToYql("USE ydb; CREATE TABLE t (a, b, primary key(a)) AS SELECT * FROM ts;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write!") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos,
                                       line.find(R"__((let world (Write! world sink (Key '('tablescheme (String '"t"))) values '('('mode 'create) '('columns '('('"a") '('"b"))) '('primarykey '('"a"))))))__"));
        }
        if (word == "Read!") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos,
                                       line.find(R"__((Read! world (DataSource '"ydb" '"ydb") (Key '('table (String '"ts"))) (Void) '()))__"));
        }
    };

    TWordCountHive elementStat = {{TString("Write!"), 0}, {TString("Read!"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write!"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Read!"]);
}

Y_UNIT_TEST(CreateTableAsSelectOnlyPrimary) {
    NYql::TAstParseResult res = SqlToYql("USE ydb; CREATE TABLE t (primary key(a)) AS SELECT * FROM ts;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write!") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos,
                                       line.find(R"__((let world (Write! world sink (Key '('tablescheme (String '"t"))) values '('('mode 'create) '('columns '()) '('primarykey '('"a"))))))__"));
        }
        if (word == "Read!") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos,
                                       line.find(R"__((Read! world (DataSource '"ydb" '"ydb") (Key '('table (String '"ts"))))__"));
        }
    };

    TWordCountHive elementStat = {{TString("Write!"), 0}, {TString("Read!"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write!"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Read!"]);
}

Y_UNIT_TEST(CreateTableAsValuesFail) {
    NYql::TAstParseResult res = SqlToYql("USE plato; CREATE TABLE t (a, primary key(a)) AS VALUES (1), (2);");
    UNIT_ASSERT(!res.IsOk());
}

Y_UNIT_TEST(CreateTableDuplicatedPkColumnsFail) {
    NYql::TAstParseResult res = SqlToYql("USE plato; CREATE TABLE t (a int32 not null, primary key(a, a));");
    UNIT_ASSERT(!res.IsOk());
}

Y_UNIT_TEST(DeleteFromTableByKey) {
    NYql::TAstParseResult res = SqlToYql("delete from plato.Input where key = 200;", 10, "kikimr");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("('mode 'delete)"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(DeleteFromTable) {
    NYql::TAstParseResult res = SqlToYql("delete from plato.Input;", 10, "kikimr");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("('mode 'delete)"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(DeleteFromTableBatch) {
    NYql::TAstParseResult res = SqlToYql("batch delete from plato.Input;", 10, "kikimr");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("('mode 'delete)"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'('is_batch 'true)"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(DeleteFromTableBatchReturning) {
    NYql::TAstParseResult res = SqlToYql("batch delete from plato.Input returning *;", 10, "kikimr");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:6: Error: BATCH DELETE is unsupported with RETURNING\n");
}

Y_UNIT_TEST(DeleteFromTableOnValues) {
    NYql::TAstParseResult res = SqlToYql("delete from plato.Input on (key) values (1);",
                                         10, "kikimr");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("('mode 'delete_on)"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(DeleteFromTableOnSelect) {
    NYql::TAstParseResult res = SqlToYql(
        "delete from plato.Input on select key from plato.Input where value > 0;", 10, "kikimr");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("('mode 'delete_on)"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(DeleteFromTableOnBatch) {
    NYql::TAstParseResult res = SqlToYql("batch delete from plato.Input on (key) values (1);",
                                         10, "kikimr");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:6: Error: BATCH DELETE is unsupported with ON\n");
}

Y_UNIT_TEST(UpdateByValues) {
    NYql::TAstParseResult res = SqlToYql("update plato.Input set key = 777, value = 'cool' where key = 200;", 10, "kikimr");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("('mode 'update)"));
        } else if (word == "AsStruct") {
            const bool isKey = line.find("key") != TString::npos;
            const bool isValue = line.find("value") != TString::npos;
            UNIT_ASSERT(isKey || isValue);
            if (isKey) {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("777")));
            } else if (isValue) {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("cool")));
            }
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}, {TString("AsStruct"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["AsStruct"]);
}

Y_UNIT_TEST(UpdateByValuesBatch) {
    NYql::TAstParseResult res = SqlToYql("batch update plato.Input set key = 777, value = 'cool' where key = 200;", 10, "kikimr");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("('mode 'update)"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'('is_batch 'true)"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(UpdateByValuesBatchReturning) {
    NYql::TAstParseResult res = SqlToYql("batch update plato.Input set value = 'cool' where key = 200 returning key;", 10, "kikimr");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:6: Error: BATCH UPDATE is unsupported with RETURNING\n");
}

Y_UNIT_TEST(UpdateByMultiValues) {
    NYql::TAstParseResult res = SqlToYql("update plato.Input set (key, value, subkey) = ('2','ddd',':') where key = 200;", 10, "kikimr");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("('mode 'update)"));
        } else if (word == "AsStruct") {
            const bool isKey = line.find("key") != TString::npos;
            const bool isSubkey = line.find("subkey") != TString::npos;
            const bool isValue = line.find("value") != TString::npos;
            UNIT_ASSERT(isKey || isSubkey || isValue);
            if (isKey && !isSubkey) {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("2")));
            } else if (isSubkey) {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote(":")));
            } else if (isValue) {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("ddd")));
            }
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}, {TString("AsStruct"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["AsStruct"]);
}

Y_UNIT_TEST(UpdateBySelect) {
    NYql::TAstParseResult res = SqlToYql("update plato.Input set (key, value, subkey) = (select key, value, subkey from plato.Input where key = 911) where key = 200;", 10, "kikimr");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    int lineIndex = 0;
    int writeLineIndex = -1;
    bool found = false;

    TVerifyLineFunc verifyLine = [&lineIndex, &writeLineIndex, &found](const TString& word, const TString& line) {
        if (word == "Write") {
            writeLineIndex = lineIndex;
            found = line.find("('mode 'update)") != TString::npos;
        } else if (word == "mode") {
            found |= lineIndex == writeLineIndex + 1 && line.find("('mode 'update)") != TString::npos;
            UNIT_ASSERT(found);
        }

        ++lineIndex;
    };

    TWordCountHive elementStat = {{TString("Write"), 0}, {TString("mode"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(UpdateSelfModifyAll) {
    NYql::TAstParseResult res = SqlToYql("update plato.Input set subkey = subkey + 's';", 10, "kikimr");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("('mode 'update)"));
        } else if (word == "AsStruct") {
            const bool isSubkey = line.find("subkey") != TString::npos;
            UNIT_ASSERT(isSubkey);
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("subkey")));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("s")));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}, {TString("AsStruct"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["AsStruct"]);
}

Y_UNIT_TEST(UpdateOnValues) {
    NYql::TAstParseResult res = SqlToYql("update plato.Input on (key, value) values (5, 'cool')", 10, "kikimr");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("('mode 'update_on)"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(UpdateOnSelect) {
    NYql::TAstParseResult res = SqlToYql(
        "update plato.Input on select key, value + 1 as value from plato.Input", 10, "kikimr");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("('mode 'update_on)"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(UpdateOnBatch) {
    NYql::TAstParseResult res = SqlToYql("batch update plato.Input on (key, value) values (5, 'cool')", 10, "kikimr");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:6: Error: BATCH UPDATE is unsupported with ON\n");
}

// UNION

Y_UNIT_TEST(UnionAllTest) {
    NYql::TAstParseResult res = SqlToYql("PRAGMA DisableEmitUnionMerge; SELECT key FROM plato.Input UNION ALL select subkey FROM plato.Input;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{TString("UnionAll"), 0}};
    VerifyProgram(res, elementStat, {});
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["UnionAll"]);
}

Y_UNIT_TEST(UnionAllMergeTest) {
    NYql::TAstParseResult res = SqlToYql("PRAGMA EmitUnionMerge; SELECT key FROM plato.Input UNION ALL select subkey FROM plato.Input;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{TString("UnionMerge"), 0}};
    VerifyProgram(res, elementStat, {});
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["UnionMerge"]);
}

Y_UNIT_TEST(UnionTest) {
    NYql::TAstParseResult res = SqlToYql("SELECT key FROM plato.Input UNION select subkey FROM plato.Input;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{TString("Union"), 0}};
    VerifyProgram(res, elementStat, {});
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Union"]);
}

Y_UNIT_TEST(UnionDistinctTest) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::UnionDistinct.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(
        R"sql(SELECT key FROM plato.Input UNION DISTINCT SELECT subkey FROM plato.Input;)sql",
        settings);

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{TString("Union"), 0}};
    VerifyProgram(res, elementStat, {});
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Union"]);
}

Y_UNIT_TEST(LegacyNotNull2025_03) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::MakeLangVersion(2025, 3);

    NYql::TAstParseResult res = SqlToYqlWithSettings(
        R"sql(SELECT 1 NOT NULL)sql",
        settings);

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(LegacyNotNull2025_04) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::GetMaxLangVersion();

    NYql::TAstParseResult res = SqlToYqlWithSettings(
        R"sql(SELECT 1 NOT NULL)sql",
        settings);

    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "Missing IS keyword before NOT NULL");
}

Y_UNIT_TEST(UnionAggregationTest) {
    NYql::TAstParseResult res = SqlToYql(R"(
        PRAGMA DisableEmitUnionMerge;
        SELECT 1
        UNION ALL
            SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
        UNION
            SELECT 1 UNION SELECT 1 UNION SELECT 1 UNION SELECT 1
        UNION ALL
            SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1;
    )");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{TString("Union"), 0}, {TString("UnionAll"), 0}};
    VerifyProgram(res, elementStat, {});
    UNIT_ASSERT_VALUES_EQUAL(2, elementStat["UnionAll"]);
    UNIT_ASSERT_VALUES_EQUAL(3, elementStat["Union"]);
}

Y_UNIT_TEST(UnionMergeAggregationTest) {
    NYql::TAstParseResult res = SqlToYql(R"(
        PRAGMA EmitUnionMerge;
        SELECT 1
        UNION ALL
            SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
        UNION
            SELECT 1 UNION SELECT 1 UNION SELECT 1 UNION SELECT 1
        UNION ALL
            SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1;
    )");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{TString("Union"), 0}, {TString("UnionMerge"), 0}};
    VerifyProgram(res, elementStat, {});
    UNIT_ASSERT_VALUES_EQUAL(2, elementStat["UnionMerge"]);
    UNIT_ASSERT_VALUES_EQUAL(3, elementStat["Union"]);
}

Y_UNIT_TEST(UnionAssumeOrderByWarning) {
    {
        NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                SELECT a FROM x
                ASSUME ORDER BY a;
            )sql");
        UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
        UNIT_ASSERT_STRINGS_EQUAL(Err2Str(res), "");
    }
    {
        NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                SELECT a FROM x
                UNION ALL
                SELECT a FROM y
                ORDER BY a;
            )sql");
        UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
        UNIT_ASSERT_STRINGS_EQUAL(Err2Str(res), "");
    }
    {
        NYql::TAstParseResult warn = SqlToYql(R"sql(
                USE plato;
                SELECT a FROM x
                UNION ALL
                SELECT a FROM y
                ASSUME ORDER BY a;
            )sql");
        UNIT_ASSERT_C(warn.Root, warn.Issues.ToString());
        UNIT_ASSERT_STRINGS_EQUAL(
            warn.Issues.ToString(),
            "<main>:6:33: Warning: ASSUME ORDER BY is used, "
            "but UNION, INTERSECT and EXCEPT operators "
            "have no ordering guarantees, "
            "therefore consider using ORDER BY, code: 3\n");
    }
}

// INTERSECT

Y_UNIT_TEST(IntersectAllTest) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::ExceptIntersect.MinLangVer;
    NYql::TAstParseResult res = SqlToYqlWithSettings("SELECT key FROM plato.Input INTERSECT ALL SELECT subkey FROM plato.Input;", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{TString("IntersectAll"), 0}};
    VerifyProgram(res, elementStat, {});
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["IntersectAll"]);
}

Y_UNIT_TEST(IntersectTest) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::ExceptIntersect.MinLangVer;
    NYql::TAstParseResult res = SqlToYqlWithSettings("SELECT key FROM plato.Input INTERSECT SELECT subkey FROM plato.Input;", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{TString("Intersect"), 0}};
    VerifyProgram(res, elementStat, {});
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Intersect"]);
}

Y_UNIT_TEST(IntersectDistinctTest) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::ExceptIntersect.MinLangVer;
    NYql::TAstParseResult res = SqlToYqlWithSettings("SELECT key FROM plato.Input INTERSECT DISTINCT SELECT subkey FROM plato.Input;", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{TString("Intersect"), 0}};
    VerifyProgram(res, elementStat, {});
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Intersect"]);
}

Y_UNIT_TEST(IntersectAllPositionalTest) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::ExceptIntersect.MinLangVer;
    NYql::TAstParseResult res = SqlToYqlWithSettings("PRAGMA PositionalUnionAll; SELECT key FROM plato.Input INTERSECT ALL SELECT subkey FROM plato.Input;", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{TString("IntersectAllPositional"), 0}};
    VerifyProgram(res, elementStat, {});
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["IntersectAllPositional"]);
}

Y_UNIT_TEST(IntersectDistinctPositionalTest) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::ExceptIntersect.MinLangVer;
    NYql::TAstParseResult res = SqlToYqlWithSettings("PRAGMA PositionalUnionAll; SELECT key FROM plato.Input INTERSECT DISTINCT SELECT subkey FROM plato.Input;", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{TString("IntersectPositional"), 0}};
    VerifyProgram(res, elementStat, {});
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["IntersectPositional"]);
}

Y_UNIT_TEST(MultipleIntersectTest) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::ExceptIntersect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings("SELECT key FROM plato.Input INTERSECT SELECT subkey FROM plato.Input INTERSECT SELECT subkey FROM plato.Input;", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{TString("Intersect"), 0}};
    VerifyProgram(res, elementStat, {});
    UNIT_ASSERT_VALUES_EQUAL(2, elementStat["Intersect"]);

    res = SqlToYqlWithSettings("SELECT key FROM plato.Input INTERSECT SELECT subkey FROM plato.Input INTERSECT ALL SELECT subkey FROM plato.Input;", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    elementStat = {{TString("Intersect"), 0}};
    VerifyProgram(res, elementStat, {});
    UNIT_ASSERT_VALUES_EQUAL(2, elementStat["Intersect"]);

    res = SqlToYqlWithSettings("SELECT key FROM plato.Input INTERSECT SELECT subkey FROM plato.Input UNION SELECT subkey FROM plato.Input;", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    elementStat = {{TString("Intersect"), 0}, {TString("Union"), 0}};
    VerifyProgram(res, elementStat, {});
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Intersect"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Union"]);
}

// EXCEPT

Y_UNIT_TEST(ExceptAllTest) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::ExceptIntersect.MinLangVer;
    NYql::TAstParseResult res = SqlToYqlWithSettings("SELECT key FROM plato.Input EXCEPT ALL SELECT subkey FROM plato.Input;", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{TString("ExceptAll"), 0}};
    VerifyProgram(res, elementStat, {});
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["ExceptAll"]);
}

Y_UNIT_TEST(ExceptTest) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::ExceptIntersect.MinLangVer;
    NYql::TAstParseResult res = SqlToYqlWithSettings("SELECT key FROM plato.Input EXCEPT SELECT subkey FROM plato.Input;", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{TString("Except"), 0}};
    VerifyProgram(res, elementStat, {});
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Except"]);
}

Y_UNIT_TEST(ExceptDistinctTest) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::ExceptIntersect.MinLangVer;
    NYql::TAstParseResult res = SqlToYqlWithSettings("SELECT key FROM plato.Input EXCEPT DISTINCT SELECT subkey FROM plato.Input;", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{TString("Except"), 0}};
    VerifyProgram(res, elementStat, {});
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Except"]);
}

Y_UNIT_TEST(ExceptAllPositionalTest) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::ExceptIntersect.MinLangVer;
    NYql::TAstParseResult res = SqlToYqlWithSettings("PRAGMA PositionalUnionAll; SELECT key FROM plato.Input EXCEPT ALL SELECT subkey FROM plato.Input;", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{TString("ExceptAllPositional"), 0}};
    VerifyProgram(res, elementStat, {});
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["ExceptAllPositional"]);
}

Y_UNIT_TEST(ExceptDistinctPositionalTest) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::ExceptIntersect.MinLangVer;
    NYql::TAstParseResult res = SqlToYqlWithSettings("PRAGMA PositionalUnionAll; SELECT key FROM plato.Input EXCEPT DISTINCT SELECT subkey FROM plato.Input;", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{TString("ExceptPositional"), 0}};
    VerifyProgram(res, elementStat, {});
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["ExceptPositional"]);
}

Y_UNIT_TEST(MultipleExceptTest) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::ExceptIntersect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings("SELECT key FROM plato.Input EXCEPT SELECT subkey FROM plato.Input EXCEPT SELECT subkey FROM plato.Input;", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{TString("Except"), 0}};
    VerifyProgram(res, elementStat, {});
    UNIT_ASSERT_VALUES_EQUAL(2, elementStat["Except"]);

    res = SqlToYqlWithSettings("SELECT key FROM plato.Input EXCEPT SELECT subkey FROM plato.Input EXCEPT ALL SELECT subkey FROM plato.Input;", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    elementStat = {{TString("Except"), 0}};
    VerifyProgram(res, elementStat, {});
    UNIT_ASSERT_VALUES_EQUAL(2, elementStat["Except"]);

    res = SqlToYqlWithSettings("SELECT key FROM plato.Input EXCEPT SELECT subkey FROM plato.Input UNION SELECT subkey FROM plato.Input;", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    elementStat = {{TString("Except"), 0}, {TString("Union"), 0}};
    VerifyProgram(res, elementStat, {});
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Except"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Union"]);
}

Y_UNIT_TEST(DefaultYQL20367) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = 202502;

    NYql::TAstParseResult res = SqlToYqlWithSettings("SELECT 1 EXCEPT SELECT 1;", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(DisabledYQL20367) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = 202502;
    settings.Flags.emplace("DisableExceptIntersectBefore202503");

    NYql::TAstParseResult res = SqlToYqlWithSettings("SELECT 1 EXCEPT SELECT 1;", settings);
    UNIT_ASSERT(!res.IsOk());
}

Y_UNIT_TEST(DeclareDecimalParameter) {
    NYql::TAstParseResult res = SqlToYql("declare $value as Decimal(22,9); select $value as cnt;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(OptionalDecimal) {
    const auto optionality = [](TStringBuf query) -> size_t {
        NYql::TAstParseResult res = SqlToYql(TString(query));
        UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

        TWordCountHive stat = {"OptionalType"};
        VerifyProgram(res, stat, {});
        return stat["OptionalType"];
    };

    UNIT_ASSERT_EQUAL(0, optionality(R"sql(SELECT FormatType(Decimal(15, 6)))sql"));
    UNIT_ASSERT_EQUAL(1, optionality(R"sql(SELECT FormatType(Decimal(15, 6)?))sql"));
    UNIT_ASSERT_EQUAL(2, optionality(R"sql(SELECT FormatType(Decimal(15, 6)??))sql"));
    UNIT_ASSERT_EQUAL(3, optionality(R"sql(SELECT FormatType(Decimal(15, 6)???))sql"));
    UNIT_ASSERT_EQUAL(1, optionality(R"sql(SELECT FormatType(Optional<Decimal(15, 6)>))sql"));
    UNIT_ASSERT_EQUAL(2, optionality(R"sql(SELECT FormatType(Optional<Decimal(15, 6)>?))sql"));
    UNIT_ASSERT_EQUAL(3, optionality(R"sql(SELECT FormatType(Optional<Decimal(15, 6)>??))sql"));
}

Y_UNIT_TEST(SimpleGroupBy) {
    NYql::TAstParseResult res = SqlToYql("select count(1),z from plato.Input group by key as z order by z;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(EmptyColumnName0) {
    /// Now it's parsed well and error occur on validate step like "4:31:Empty struct member name is not allowed" in "4:31:Function: AddMember"
    NYql::TAstParseResult res = SqlToYql("insert into plato.Output (``, list1) values (0, AsList(0, 1, 2));");
    /// Verify that parsed well without crash
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(KikimrRollback) {
    NYql::TAstParseResult res = SqlToYql("use plato; select * from Input; rollback;", 10, "kikimr");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{TString("rollback"), 0}};
    VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["rollback"]);
}

Y_UNIT_TEST(PragmaFile) {
    NYql::TAstParseResult res = SqlToYql(R"(pragma file("HW", "sbr:181041334");)");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{TString(R"((let world (Configure! world (DataSource '"config") '"AddFileByUrl" '"HW" '"sbr:181041334")))"), 0}};
    VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat.cbegin()->second);
}

Y_UNIT_TEST(DoNotCrashOnNamedInFilter) {
    NYql::TAstParseResult res = SqlToYql("USE plato; $all = ($table_name) -> { return true; }; SELECT * FROM FILTER(Input, $all)");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(PragmasFileAndUdfOrder) {
    NYql::TAstParseResult res = SqlToYql(R"(
                PRAGMA file("libvideoplayers_udf.so", "https://proxy.sandbox.yandex-team.ru/235185290");
                PRAGMA udf("libvideoplayers_udf.so");
            )");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto programm = GetPrettyPrint(res);
    const auto file = programm.find("AddFileByUrl");
    const auto udfs = programm.find("ImportUdfs");
    UNIT_ASSERT(file < udfs);
}

Y_UNIT_TEST(PragmaPackageURLSyntaxError) {
    ExpectFailWithError(R"sql(
        PRAGMA Package("project.package", "yt://plato/{$_path/to/package");
    )sql", "<main>:2:43: Error: Failed to substitute parameters into url: "
           "'yt://plato/{$_path/to/package', reason: 'Missing }', position: 28\n");
}

Y_UNIT_TEST(ProcessUserType) {
    NYql::TAstParseResult res = SqlToYql("process plato.Input using Kikimr::PushData(TableRows());", 1, TString(NYql::KikimrProviderName));
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Kikimr.PushData") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("TupleType"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("TypeOf"));
        }
    };

    TWordCountHive elementStat = {{TString("Kikimr.PushData"), 0}};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Kikimr.PushData"]);
}

Y_UNIT_TEST(ProcessUserTypeAuth) {
    NYql::TAstParseResult res = SqlToYql("process plato.Input using YDB::PushData(TableRows(), AsTuple('oauth', SecureParam('api:oauth')));", 1, TString(NYql::KikimrProviderName));
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "YDB.PushData") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("TupleType"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("TypeOf"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("api:oauth"));
        }
    };

    TWordCountHive elementStat = {{TString("YDB.PushData"), 0}};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["YDB.PushData"]);
}

Y_UNIT_TEST(SelectStreamRtmr) {
    NYql::TAstParseResult res = SqlToYql(
        "USE plato; INSERT INTO Output SELECT STREAM key FROM Input;",
        10, TString(NYql::RtmrProviderName));
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    res = SqlToYql(
        "USE plato; INSERT INTO Output SELECT key FROM Input;",
        10, TString(NYql::RtmrProviderName));
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(SelectStreamRtmrJoinWithYt) {
    NYql::TAstParseResult res = SqlToYql(
        "USE plato; INSERT INTO Output SELECT STREAM key FROM Input LEFT JOIN hahn.ttt as t ON Input.key = t.Name;",
        10, TString(NYql::RtmrProviderName));
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(SelectStreamNonRtmr) {
    NYql::TAstParseResult res = SqlToYql(
        "USE plato; INSERT INTO Output SELECT STREAM key FROM Input;",
        10);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:31: Error: SELECT STREAM is unsupported for non-streaming sources\n");
}

Y_UNIT_TEST(GroupByHopRtmr) {
    NYql::TAstParseResult res = SqlToYql(R"(
                USE plato; INSERT INTO Output SELECT key, SUM(value) AS value FROM Input
                GROUP BY key, HOP(subkey, "PT10S", "PT30S", "PT20S");
            )", 10, TString(NYql::RtmrProviderName));
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(GroupByHopRtmrSubquery) {
    // 'use plato' intentially avoided
    NYql::TAstParseResult res = SqlToYql(R"(
                SELECT COUNT(*) AS value FROM (SELECT * FROM plato.Input)
                GROUP BY HOP(Data, "PT10S", "PT30S", "PT20S")
            )", 10, TString(NYql::RtmrProviderName));
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(GroupByHopRtmrSubqueryBinding) {
    NYql::TAstParseResult res = SqlToYql(R"(
        USE plato;
        $q = SELECT * FROM Input;
        INSERT INTO Output SELECT STREAM * FROM (
            SELECT COUNT(*) AS value FROM $q
            GROUP BY HOP(Data, "PT10S", "PT30S", "PT20S")
        );
    )", 10, TString(NYql::RtmrProviderName));
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(GroupByNoHopRtmr) {
    NYql::TAstParseResult res = SqlToYql(R"(
            USE plato; INSERT INTO Output SELECT STREAM key, SUM(value) AS value FROM Input
            GROUP BY key;
        )", 10, TString(NYql::RtmrProviderName));
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:22: Error: Streaming group by query must have a hopping window specification.\n");
}

Y_UNIT_TEST(KikimrInserts) {
    NYql::TAstParseResult res = SqlToYql(R"(
        USE plato;
        INSERT INTO Output SELECT key, value FROM Input;
        INSERT OR ABORT INTO Output SELECT key, value FROM Input;
        INSERT OR IGNORE INTO Output SELECT key, value FROM Input;
        INSERT OR REVERT INTO Output SELECT key, value FROM Input;
    )", 10, TString(NYql::KikimrProviderName));
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(InsertIntoNamedExpr) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
        $target = "target";
        INSERT INTO plato.$target (x) VALUES ((1));
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(WarnMissingIsBeforeNotNull) {
    NYql::TAstParseResult res = SqlToYql("select 1 NOT NULL");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Warning: Missing IS keyword before NOT NULL, code: 4507\n");
}

Y_UNIT_TEST(Subqueries) {
    NYql::TAstParseResult res = SqlToYql(R"(
        USE plato;
        $sq1 = (SELECT * FROM plato.Input);

        $sq2 = SELECT * FROM plato.Input;

        $squ1 = (
            SELECT * FROM plato.Input
            UNION ALL
            SELECT * FROM plato.Input
        );

        $squ2 =
            SELECT * FROM plato.Input
            UNION ALL
            SELECT * FROM plato.Input;

        $squ3 = (
            (SELECT * FROM plato.Input)
            UNION ALL
            (SELECT * FROM plato.Input)
        );

        SELECT * FROM $sq1;
        SELECT * FROM $sq2;
        SELECT * FROM $squ1;
        SELECT * FROM $squ2;
        SELECT * FROM $squ3;
    )");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(SubqueriesJoin) {
    NYql::TAstParseResult res = SqlToYql(R"(
        USE plato;

        $left = SELECT * FROM plato.Input1 WHERE value != "BadValue";
        $right = SELECT * FROM plato.Input2;

        SELECT * FROM $left AS l
        JOIN $right AS r
        ON l.key == r.key;
    )");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(AnyInBackticksAsTableName) {
    NYql::TAstParseResult res = SqlToYql("use plato; select * from `any`;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(AnyJoinForTableAndSubQuery) {
    NYql::TAstParseResult res = SqlToYql(R"(
        USE plato;

        $r = SELECT * FROM plato.Input2;

        SELECT * FROM ANY plato.Input1 AS l
        LEFT JOIN ANY $r AS r
        USING (key);
    )");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "EquiJoin") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'('left 'any)"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'('right 'any)"));
        }
    };

    TWordCountHive elementStat = {{TString("left"), 0}, {TString("right"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["left"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["right"]);
}

Y_UNIT_TEST(AnyJoinForTableAndTableSource) {
    NYql::TAstParseResult res = SqlToYql(R"(
        USE plato;

        $r = AsList(
            AsStruct("aaa" as key, "bbb" as subkey, "ccc" as value)
        );

        SELECT * FROM ANY plato.Input1 AS l
        LEFT JOIN ANY AS_TABLE($r) AS r
        USING (key);
    )");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "EquiJoin") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'('left 'any)"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'('right 'any)"));
        }
    };

    TWordCountHive elementStat = {{TString("left"), 0}, {TString("right"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["left"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["right"]);
}

Y_UNIT_TEST(AnyJoinNested) {
    NYql::TAstParseResult res = SqlToYql(R"(
        USE plato;

        FROM ANY Input1 as a
            JOIN Input2 as b ON a.key = b.key
            LEFT JOIN ANY Input3 as c ON a.key = c.key
            RIGHT JOIN ANY Input4 as d ON d.key = b.key
            CROSS JOIN Input5
        SELECT *;
    )");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{TString("left"), 0}, {TString("right"), 0}};
    VerifyProgram(res, elementStat);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["left"]);
    UNIT_ASSERT_VALUES_EQUAL(2, elementStat["right"]);
}

Y_UNIT_TEST(InlineAction) {
    NYql::TAstParseResult res = SqlToYql(
        "do begin\n"
        "  select 1\n"
        "; end do\n");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "");
}

Y_UNIT_TEST(FlattenByCorrelationName) {
    UNIT_ASSERT(SqlToYql("select * from plato.Input as t flatten by t.x").IsOk());
    UNIT_ASSERT(SqlToYql("select * from plato.Input as t flatten by t -- same as flatten by t.t").IsOk());
}

Y_UNIT_TEST(DiscoveryMode) {
    UNIT_ASSERT(SqlToYqlWithMode("insert into plato.Output select * from plato.Input", NSQLTranslation::ESqlMode::DISCOVERY).IsOk());
    UNIT_ASSERT(SqlToYqlWithMode("select * from plato.concat(Input1, Input2)", NSQLTranslation::ESqlMode::DISCOVERY).IsOk());
    UNIT_ASSERT(SqlToYqlWithMode("select * from plato.each(AsList(\"Input1\", \"Input2\"))", NSQLTranslation::ESqlMode::DISCOVERY).IsOk());
}

Y_UNIT_TEST(CubeWithAutoGeneratedLikeColumnName) {
    UNIT_ASSERT(SqlToYql("select key,subkey,group from plato.Input group by cube(key,subkey,group)").IsOk());
}

Y_UNIT_TEST(CubeWithAutoGeneratedLikeAlias) {
    UNIT_ASSERT(SqlToYql("select key,subkey,group from plato.Input group by cube(key,subkey,value as group)").IsOk());
}

Y_UNIT_TEST(FilterCanBeUsedAsColumnIdOrBind) {
    UNIT_ASSERT(SqlToYql("select filter from plato.Input").IsOk());
    UNIT_ASSERT(SqlToYql("select 1 as filter").IsOk());
    UNIT_ASSERT(SqlToYql("$filter = 1; select $filter").IsOk());
}

Y_UNIT_TEST(DuplicateSemicolonsAreAllowedBetweenTopLevelStatements) {
    UNIT_ASSERT(SqlToYql(";;select 1; ; select 2;/*comment*/;select 3;;--comment\n;select 4;;").IsOk());
}

Y_UNIT_TEST(DuplicateAndMissingTrailingSemicolonsAreAllowedBetweenActionStatements) {
    TString req =
        "define action $action($b,$c) as\n"
        "    ;;$d = $b + $c;\n"
        "    select $b;\n"
        "    select $c;;\n"
        "    select $d,\n"
        "end define;\n"
        "\n"
        "do $action(1,2);";
    UNIT_ASSERT(SqlToYql(req).IsOk());
}

Y_UNIT_TEST(DuplicateAndMissingTrailingSemicolonsAreAllowedBetweenInlineActionStatements) {
    TString req =
        "do begin\n"
        "    ;select 1,\n"
        "end do;\n"
        "evaluate for $i in AsList(1,2,3) do begin\n"
        "    select $i;;\n"
        "    select $i + $i;;\n"
        "end do;";
    UNIT_ASSERT(SqlToYql(req).IsOk());
}

Y_UNIT_TEST(DuplicateSemicolonsAreAllowedBetweenLambdaStatements) {
    TString req =
        "$x=1;\n"
        "$foo = ($a, $b)->{\n"
        "   ;;$v = $a + $b;\n"
        "   $bar = ($c) -> {; return $c << $x};;\n"
        "   return $bar($v);;\n"
        "};\n"
        "select $foo(1,2);";
    UNIT_ASSERT(SqlToYql(req).IsOk());
}

Y_UNIT_TEST(ForStatementLangVerFailure) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
            FOR $i IN AsList(1,2,3) DO BEGIN
                SELECT $i;
            END DO;
        )sql");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(
        Err2Str(res),
        "FOR without EVALUATE is not available before language version 2026.02");
}

Y_UNIT_TEST(ForStatementLangVerSuccess) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::ForWithoutEvaluate.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
            FOR $i IN AsList(1,2,3) DO BEGIN
                SELECT $i;
            END DO;
        )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(ParallelForStatementLangVer) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
        PARALLEL FOR $i IN AsList(1,2,3) DO BEGIN
            SELECT $i;
        END DO;
    )sql");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(
        Err2Str(res),
        "PARALLEL FOR is not available before language version 2026.02");
}

Y_UNIT_TEST(FunctionLangVerUnavailable) {
    NSQLTranslation::TTranslationSettings settings;
    settings.Flags.insert("CheckBuiltinLangVer");

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        SELECT FormatType(AsOptionalType(Int32));
    )sql", settings);

    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(
        Err2Str(res),
        "AsOptionalType is not available before language version 2026.01");
}

Y_UNIT_TEST(FunctionLangVerAvailable) {
    NSQLTranslation::TTranslationSettings settings;
    settings.Flags.insert("CheckBuiltinLangVer");
    settings.LangVer = NYql::MakeLangVersion(2026, 1);

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        SELECT FormatType(AsOptionalType(Int32));
    )sql", settings);

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(FunctionLangVerUnknown) {
    NSQLTranslation::TTranslationSettings settings;
    settings.Flags.insert("CheckBuiltinLangVer");

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        SELECT FormatType(OptionalType(Int32));
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(StringLiteralWithEscapedBackslash) {
    NYql::TAstParseResult res1 = SqlToYql(R"foo(SELECT 'a\\';)foo");
    NYql::TAstParseResult res2 = SqlToYql(R"foo(SELECT "a\\";)foo");
    UNIT_ASSERT(res1.Root);
    UNIT_ASSERT(res2.Root);

    TWordCountHive elementStat = {{TString("a\\"), 0}};

    VerifyProgram(res1, elementStat);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["a\\"]);

    VerifyProgram(res2, elementStat);
    UNIT_ASSERT_VALUES_EQUAL(2, elementStat["a\\"]);
}

Y_UNIT_TEST(StringMultiLineLiteralWithEscapes) {
    UNIT_ASSERT(SqlToYql("SELECT @@@foo@@@@bar@@@").IsOk());
    UNIT_ASSERT(SqlToYql("SELECT @@@@@@@@@").IsOk());
}

Y_UNIT_TEST(StringMultiLineLiteralConsequitiveAt) {
    UNIT_ASSERT(!SqlToYql("SELECT @").IsOk());
    UNIT_ASSERT(!SqlToYql("SELECT @@").IsOk());
    UNIT_ASSERT(!SqlToYql("SELECT @@@").IsOk());
    UNIT_ASSERT(SqlToYql("SELECT @@@@").IsOk());
    UNIT_ASSERT(SqlToYql("SELECT @@@@@").IsOk());

    UNIT_ASSERT(!SqlToYql("SELECT @@@@@@").IsOk());
    UNIT_ASSERT(!SqlToYql("SELECT @@@@@@@").IsOk());

    UNIT_ASSERT(SqlToYql("SELECT @@@@@@@@").IsOk());
    UNIT_ASSERT(SqlToYql("SELECT @@@@@@@@@").IsOk());
    UNIT_ASSERT(!SqlToYql("SELECT @@@@@@@@@@").IsOk());
}

Y_UNIT_TEST(ConstnessForListDictSetCreate) {
    auto req = "$foo = ($x, $y) -> (\"aaaa\");\n"
               "\n"
               "select\n"
               "    $foo(sum(key), ListCreate(String)),\n"
               "    $foo(sum(key), DictCreate(String, String)),\n"
               "    $foo(sum(key), SetCreate(String)),\n"
               "from (select 1 as key);";
    UNIT_ASSERT(SqlToYql(req).IsOk());
}

Y_UNIT_TEST(CanUseEmptyTupleInWindowPartitionBy) {
    auto req = "select sum(key) over w\n"
               "from plato.Input\n"
               "window w as (partition compact by (), (subkey), (), value || value as dvalue);";
    UNIT_ASSERT(SqlToYql(req).IsOk());
}

Y_UNIT_TEST(DenyAnsiOrderByLimitLegacyMode) {
    auto req = "pragma DisableAnsiOrderByLimitInUnionAll;\n"
               "use plato;\n"
               "\n"
               "select * from Input order by key limit 10\n"
               "union all\n"
               "select * from Input order by key limit 1;";

    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: DisableAnsiOrderByLimitInUnionAll pragma is deprecated and no longer supported\n");
}

Y_UNIT_TEST(ReduceUsingUdfWithShortcutsWorks) {
    auto req = "use plato;\n"
               "\n"
               "$arg = 'foo';\n"
               "$func = XXX::YYY($arg);\n"
               "\n"
               "REDUCE Input ON key using $func(subkey);\n"
               "REDUCE Input ON key using $func(UUU::VVV(TableRow()));\n";
    UNIT_ASSERT(SqlToYql(req).IsOk());
    req = "use plato;\n"
          "\n"
          "$arg = 'foo';\n"
          "$func = XXX::YYY($arg);\n"
          "\n"
          "REDUCE Input ON key using all $func(subkey);\n"
          "REDUCE Input ON key using all $func(UUU::VVV(TableRow()));";
    UNIT_ASSERT(SqlToYql(req).IsOk());
}

Y_UNIT_TEST(CombineLangverTest) {
    const auto req = R"sql(COMBINE plato.Input as A PRESORT A.key, A.subkey
                               WITH plato.Input as B PRESORT B.key, B.subkey
                               ON A.key = B.key AND A.subkey = B.subkey
                               USING Foo::DoBar((A.value), (B.value)))sql";
    const auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(
        Err2Str(res),
        "Error: COMBINE is not available before language version 2026.02");
}

// TODO: rewrite to boilerplate
NYql::TAstParseResult SqlToYql202602(const TString& query) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::MakeLangVersion(2026, 2);
    return SqlToYqlWithSettings(query, settings);
}

Y_UNIT_TEST(CombineSmokeTest) {
    const auto req = R"sql(COMBINE plato.Input as A PRESORT A.key, A.subkey
                               WITH plato.Input as B PRESORT B.key, B.subkey
                               ON A.key = B.key AND A.subkey = B.subkey
                               USING Foo::DoBar((A.value), (B.value)))sql";

    const auto res = SqlToYql202602(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {"SqlCombine ", "SqlCombineInput ", "Read! "};
    VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SqlCombine "]);
    UNIT_ASSERT_VALUES_EQUAL(2, elementStat["SqlCombineInput "]);
    UNIT_ASSERT_VALUES_EQUAL(2, elementStat["Read! "]);
}

Y_UNIT_TEST(CombineAsSubquery) {
    const auto req = R"sql(SELECT value, extra FROM (
                                   COMBINE plato.Input as A
                                   WITH plato.Input as B
                                   ON A.key = B.key
                                   USING Foo::DoBar((A.value), (B.value))
                               ))sql";
    const auto res = SqlToYql202602(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {"SqlCombine ", "SqlCombineInput ", "Read! "};
    VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SqlCombine "]);
    UNIT_ASSERT_VALUES_EQUAL(2, elementStat["SqlCombineInput "]);
    UNIT_ASSERT_VALUES_EQUAL(2, elementStat["Read! "]);
}

Y_UNIT_TEST(CombineSubselectInputs) {
    const auto req = R"sql(COMBINE (SELECT key, value, extra FROM plato.Input) as A
                               WITH (SELECT key, value, extra FROM plato.Input) as B
                               ON A.key = B.key
                               USING Foo::DoBar((A.value), (B.value)))sql";
    const auto res = SqlToYql202602(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {"SqlCombine ", "SqlCombineInput ", "Read! "};
    VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SqlCombine "]);
    UNIT_ASSERT_VALUES_EQUAL(2, elementStat["SqlCombineInput "]);
    UNIT_ASSERT_VALUES_EQUAL(2, elementStat["Read! "]);
}

Y_UNIT_TEST(CombineDiscard) {
    const auto req = R"sql(DISCARD COMBINE plato.Input as A
                               WITH plato.Input as B
                               ON A.key = B.key
                               USING Foo::DoBar((A.value), (B.value)))sql";
    const auto res = SqlToYql202602(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {"SqlCombine ", "SqlCombineInput ", "discard"};
    VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SqlCombine "]);
    UNIT_ASSERT_VALUES_EQUAL(2, elementStat["SqlCombineInput "]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["discard"]);
}

Y_UNIT_TEST(CombineInSubquery) {
    const auto req = R"sql(DEFINE SUBQUERY $q() as
                                   COMBINE plato.Input as A
                                   WITH plato.Input as B
                                   ON A.key = B.key
                                   USING Foo::Dobar((A.value), (B.value));
                               END DEFINE;
                               SELECT * FROM $q())sql";
    const auto res = SqlToYql202602(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {"SqlCombine ", "SqlCombineInput ", "UnorderedSubquery "};
    VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SqlCombine "]);
    UNIT_ASSERT_VALUES_EQUAL(2, elementStat["SqlCombineInput "]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["UnorderedSubquery "]);
}

Y_UNIT_TEST(CombineTableRowArgs) {
    const auto req = R"sql(COMBINE plato.Input as A PRESORT A.key, A.subkey
                               WITH plato.Input as B PRESORT B.key, B.subkey
                               ON A.key = B.key AND A.subkey = B.subkey
                               USING Foo::DoBar(TableRow(), TableRow()))sql";
    const auto res = SqlToYql202602(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {"SqlCombine ", "SqlCombineInput ", "RemoveSystemMembers "};
    VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SqlCombine "]);
    UNIT_ASSERT_VALUES_EQUAL(2, elementStat["SqlCombineInput "]);
    UNIT_ASSERT_VALUES_EQUAL(2, elementStat["RemoveSystemMembers "]);
}

Y_UNIT_TEST(CombineInvalidArgc) {
    const auto req = R"sql(COMBINE plato.Input as A PRESORT A.key, A.subkey
                               WITH plato.Input as B PRESORT B.key, B.subkey
                               ON A.key = B.key AND A.subkey = B.subkey
                               USING Foo::DoBar(TableRow()))sql";
    const auto res = SqlToYql202602(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(
        Err2Str(res),
        "Error: COMBINE requires exactly two expressions, specifying argument types");
}

Y_UNIT_TEST(CombineOnNotEquality) {
    const auto req = R"sql(COMBINE plato.Input as A
                               WITH plato.Input as B
                               ON A.key > B.key
                               USING Foo::DoBar((A.value), (B.value)))sql";
    const auto res = SqlToYql202602(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(
        Err2Str(res),
        "Error: COMBINE ON expression must be a conjunction of equality predicates");
}

Y_UNIT_TEST(CombineOnNonColumnArg) {
    const auto req = R"sql(COMBINE plato.Input as A
                               WITH plato.Input as B
                               ON A.key = 1
                               USING Foo::DoBar((A.value), (B.value)))sql";
    const auto res = SqlToYql202602(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(
        Err2Str(res),
        "Error: COMBINE: each equality predicate argument must depend on exactly one COMBINE input");
}

Y_UNIT_TEST(CombineOnColumnWithoutCorrelation) {
    const auto req = R"sql(COMBINE plato.Input as A
                               WITH plato.Input as B
                               ON key = B.key
                               USING Foo::DoBar((A.value), (B.value)))sql";
    const auto res = SqlToYql202602(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(
        Err2Str(res),
        "Error: COMBINE: column requires correlation name");
}

Y_UNIT_TEST(CombineOnUnknownCorrelation) {
    const auto req = R"sql(COMBINE plato.Input as A
                               WITH plato.Input as B
                               ON A.key = C.key
                               USING Foo::DoBar((A.value), (B.value)))sql";
    const auto res = SqlToYql202602(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(
        Err2Str(res),
        "Error: COMBINE: unknown correlation name: C");
}

Y_UNIT_TEST(CombineOnSameCorrelationBothSides) {
    const auto req = R"sql(COMBINE plato.Input as A
                               WITH plato.Input as B
                               ON A.key = A.subkey
                               USING Foo::DoBar((A.value), (B.value)))sql";
    const auto res = SqlToYql202602(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(
        Err2Str(res),
        "Error: COMBINE: different correlation names are required for combined tables");
}

Y_UNIT_TEST(YsonDisableStrict) {
    UNIT_ASSERT(SqlToYql("pragma yson.DisableStrict = \"false\";").IsOk());
    UNIT_ASSERT(SqlToYql("pragma yson.DisableStrict;").IsOk());
}

Y_UNIT_TEST(YsonStrict) {
    UNIT_ASSERT(SqlToYql("pragma yson.Strict = \"false\";").IsOk());
    UNIT_ASSERT(SqlToYql("pragma yson.Strict;").IsOk());
}

Y_UNIT_TEST(JoinByTuple) {
    auto req = "use plato;\n"
               "\n"
               "select * from T1 as a\n"
               "join T2 as b\n"
               "on AsTuple(a.key, a.subkey) = AsTuple(b.key, b.subkey);";
    UNIT_ASSERT(SqlToYql(req).IsOk());
}

Y_UNIT_TEST(JoinByStruct) {
    auto req = "use plato;\n"
               "\n"
               "select * from T1 as a\n"
               "join T2 as b\n"
               "on AsStruct(a.key as k, a.subkey as sk) = AsStruct(b.key as k, b.subkey as sk);";
    UNIT_ASSERT(SqlToYql(req).IsOk());
}

Y_UNIT_TEST(JoinByUdf) {
    auto req = "use plato;\n"
               "\n"
               "select a.align\n"
               "from T1 as a\n"
               "join T2 as b\n"
               "on Yson::SerializeJsonEncodeUtf8(a.align)=b.align;";
    auto res = SqlToYql(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(EscapedIdentifierAsLambdaArg) {
    auto req = "$f = ($`foo bar`, $x) -> { return $`foo bar` + $x; };\n"
               "\n"
               "select $f(1, 2);";
    auto res = SqlToYql(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    const auto programm = GetPrettyPrint(res);
    auto expected = R"((lambda '("$foo bar" "$x"))";
    UNIT_ASSERT(programm.find(expected) != TString::npos);
}

Y_UNIT_TEST(UdfSyntaxSugarOnlyCallable) {
    auto req = "SELECT Udf(DateTime::FromString)('2022-01-01');";
    auto res = SqlToYql(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    const auto programm = GetPrettyPrint(res);
    auto expected = R"((SqlCall '"DateTime2.FromString" '((PositionalArgs (String '"2022-01-01")) (AsStruct)) (TupleType)))";
    UNIT_ASSERT(programm.find(expected) != TString::npos);
}

Y_UNIT_TEST(UdfSyntaxSugarTypeNoRun) {
    auto req = "SELECT Udf(DateTime::FromString, String, Tuple<Int32, Float>, 'foo' as TypeConfig)('2022-01-01');";
    auto res = SqlToYql(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    const auto programm = GetPrettyPrint(res);
    auto expected = R"((SqlCall '"DateTime2.FromString" '((PositionalArgs (String '"2022-01-01")) (AsStruct)) (TupleType (DataType 'String) (TupleType (DataType 'Int32) (DataType 'Float))) '"foo"))";
    UNIT_ASSERT(programm.find(expected) != TString::npos);
}

Y_UNIT_TEST(UdfSyntaxSugarRunNoType) {
    auto req = "SELECT Udf(DateTime::FromString, String, Tuple<Int32, Float>, Void() as RunConfig)('2022-01-01');";
    auto res = SqlToYql(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    const auto programm = GetPrettyPrint(res);
    auto expected = R"((SqlCall '"DateTime2.FromString" '((PositionalArgs (String '"2022-01-01")) (AsStruct)) (TupleType (DataType 'String) (TupleType (DataType 'Int32) (DataType 'Float))) '"" (Void)))";
    UNIT_ASSERT(programm.find(expected) != TString::npos);
}

Y_UNIT_TEST(UdfSyntaxSugarFullTest) {
    auto req = "SELECT Udf(DateTime::FromString, String, Tuple<Int32, Float>, 'foo' as TypeConfig, Void() As RunConfig)('2022-01-01');";
    auto res = SqlToYql(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    const auto programm = GetPrettyPrint(res);
    auto expected = R"((SqlCall '"DateTime2.FromString" '((PositionalArgs (String '"2022-01-01")) (AsStruct)) (TupleType (DataType 'String) (TupleType (DataType 'Int32) (DataType 'Float))) '"foo" (Void)))";
    UNIT_ASSERT(programm.find(expected) != TString::npos);
}

Y_UNIT_TEST(UdfSyntaxSugarOtherRunConfigs) {
    auto req = "SELECT Udf(DateTime::FromString, String, Tuple<Int32, Float>, 'foo' as TypeConfig, '55' As RunConfig)('2022-01-01');";
    auto res = SqlToYql(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    const auto programm = GetPrettyPrint(res);
    auto expected = R"((SqlCall '"DateTime2.FromString" '((PositionalArgs (String '"2022-01-01")) (AsStruct)) (TupleType (DataType 'String) (TupleType (DataType 'Int32) (DataType 'Float))) '"foo" (String '"55")))";
    UNIT_ASSERT(programm.find(expected) != TString::npos);
}

Y_UNIT_TEST(UdfSyntaxSugarOtherRunConfigs2) {
    auto req = "SELECT Udf(DateTime::FromString, String, Tuple<Int32, Float>, 'foo' as TypeConfig, AsTuple(32, 'no', AsStruct(1e-9 As SomeFloat)) As RunConfig)('2022-01-01');";
    auto res = SqlToYql(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    const auto programm = GetPrettyPrint(res);
    auto expected = R"((SqlCall '"DateTime2.FromString" '((PositionalArgs (String '"2022-01-01")) (AsStruct)) (TupleType (DataType 'String) (TupleType (DataType 'Int32) (DataType 'Float))) '"foo" '((Int32 '"32") (String '"no") (AsStruct '('"SomeFloat" (Double '"1e-9"))))))";
    UNIT_ASSERT(programm.find(expected) != TString::npos);
}

Y_UNIT_TEST(UdfSyntaxSugarOptional) {
    auto req = R"(SELECT Udf(DateTime::FromString, String?, Int32??, Tuple<Int32, Float>, "foo" as TypeConfig, Void() As RunConfig)("2022-01-01");)";
    auto res = SqlToYql(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    const auto programm = GetPrettyPrint(res);
    auto expected = R"((SqlCall '"DateTime2.FromString" '((PositionalArgs (String '"2022-01-01")) (AsStruct)) (TupleType (OptionalType (DataType 'String)) (OptionalType (OptionalType (DataType 'Int32))) (TupleType (DataType 'Int32) (DataType 'Float))) '"foo" (Void)))";
    UNIT_ASSERT(programm.find(expected) != TString::npos);
}

Y_UNIT_TEST(CompactionPolicyParseCorrect) {
    NYql::TAstParseResult res = SqlToYql(
        R"( USE ydb;
                    CREATE TABLE tableName (Key Uint32, Value String, PRIMARY KEY (Key))
                    WITH ( COMPACTION_POLICY = "SomeCompactionPreset" );)");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("compactionPolicy"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("SomeCompactionPreset"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(AutoPartitioningBySizeParseCorrect) {
    NYql::TAstParseResult res = SqlToYql(
        R"( USE ydb;
                    CREATE TABLE tableName (Key Uint32, Value String, PRIMARY KEY (Key))
                    WITH ( AUTO_PARTITIONING_BY_SIZE = ENABLED );)");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("autoPartitioningBySize"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("ENABLED"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(UniformPartitionsParseCorrect) {
    NYql::TAstParseResult res = SqlToYql(
        R"( USE ydb;
                    CREATE TABLE tableName (Key Uint32, Value String, PRIMARY KEY (Key))
                    WITH ( UNIFORM_PARTITIONS = 16 );)");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("uniformPartitions"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("16"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(DateTimeTtlParseCorrect) {
    NYql::TAstParseResult res = SqlToYql(
        R"( USE ydb;
                    CREATE TABLE tableName (Key Uint32, CreatedAt Timestamp, PRIMARY KEY (Key))
                    WITH (TTL = Interval("P1D") On CreatedAt);)");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("setTtlSettings"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("tiers"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("evictionDelay"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("86400000"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(IntTtlParseCorrect) {
    NYql::TAstParseResult res = SqlToYql(
        R"( USE ydb;
                    CREATE TABLE tableName (Key Uint32, CreatedAt Uint32, PRIMARY KEY (Key))
                    WITH (TTL = Interval("P1D") On CreatedAt AS SECONDS);)");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("setTtlSettings"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("tiers"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("evictionDelay"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("86400000"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("columnUnit"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("seconds"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(TtlTieringParseCorrect) {
    NYql::TAstParseResult res = SqlToYql(
        R"( USE ydb;
                    CREATE TABLE tableName (Key Uint32, CreatedAt Uint32, PRIMARY KEY (Key))
                    WITH (TTL =
                            Interval("P1D") TO EXTERNAL DATA SOURCE Tier1,
                            Interval("P2D") TO EXTERNAL DATA SOURCE Tier2,
                            Interval("P30D") DELETE
                        ON CreatedAt AS SECONDS);)");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("setTtlSettings"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("tiers"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("evictionDelay"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("storageName"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("Tier1"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("Tier2"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("86400000"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("172800000"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("2592000000"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("columnUnit"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("seconds"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(TtlTieringWithOtherActionsParseCorrect) {
    NYql::TAstParseResult res = SqlToYql(
        R"( USE ydb;
                    ALTER TABLE tableName
                        ADD FAMILY cold (DATA = "rot"),
                        SET TTL
                            Interval("P1D") TO EXTERNAL DATA SOURCE Tier1,
                            Interval("P2D") TO EXTERNAL DATA SOURCE Tier2,
                            Interval("P30D") DELETE
                        ON CreatedAt,
                        ALTER COLUMN payload_v2 SET FAMILY cold,
                        ALTER FAMILY default SET DATA "ssd"
                    ;)");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("addColumnFamilies"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("cold"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("alterColumnFamilies"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("default"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("setTtlSettings"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("tiers"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("evictionDelay"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("storageName"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("Tier1"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("Tier2"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("86400000"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("172800000"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("2592000000"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(TieringParseCorrect) {
    NYql::TAstParseResult res = SqlToYql(
        R"( USE ydb;
                    CREATE TABLE tableName (Key Uint32, Value String, PRIMARY KEY (Key))
                    WITH ( TIERING = 'my_tiering' );)");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("tiering"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("my_tiering"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(StoreExternalBlobsParseCorrect) {
    NYql::TAstParseResult res = SqlToYql(
        R"( USE ydb;
                    CREATE TABLE tableName (Key Uint32, Value String, PRIMARY KEY (Key))
                    WITH ( STORE_EXTERNAL_BLOBS = ENABLED );)");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("storeExternalBlobs"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("ENABLED"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(ExternalDataChannelsCountParseCorrect) {
    NYql::TAstParseResult res = SqlToYql(
        R"( USE ydb;
                    CREATE TABLE tableName (Key Uint32, Value String, PRIMARY KEY (Key))
                    WITH ( EXTERNAL_DATA_CHANNELS_COUNT = 7 );)");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("externalDataChannelsCount"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("7"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(DefaultValueColumn2) {
    auto res = SqlToYql(R"( use ydb;
                $lambda = () -> {
                    RETURN CAST(RandomUuid(2) as String)
                };

                CREATE TABLE tableName (
                    Key Uint32 DEFAULT RandomNumber(1),
                    Value String DEFAULT $lambda,
                    PRIMARY KEY (Key)
                );
            )");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);

    UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, program.find("RandomNumber"));
    UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, program.find("RandomUuid"));
    UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, program.find("columnConstrains"));
    UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, program.find("columnConstrains"));
    UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, program.find("Write"));

#if 0
            Cerr << program << Endl;
#endif

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(DefaultValueColumn3) {
    auto res = SqlToYql(R"(
            use ydb;
            CREATE TABLE tableName (
                database_id Utf8,
                cloud_id Utf8,
                global_id Utf8 DEFAULT database_id || "=====",
                PRIMARY KEY (database_id)
            );
        )");

    UNIT_ASSERT_VALUES_EQUAL(Err2Str(res), "<main>:6:40: Error: Column reference \"database_id\" is not allowed in current scope\n");
    UNIT_ASSERT(!res.IsOk());
}

Y_UNIT_TEST(DefaultValueColumn) {
    auto res = SqlToYql(R"( use ydb;
            CREATE TABLE tableName (
                Key Uint32 FAMILY cold DEFAULT 5,
                Value String FAMILY default DEFAULT "empty",
                PRIMARY KEY (Key),
                FAMILY default (
                        DATA = "test",
                        COMPRESSION = "lz4"
                ),
                FAMILY cold (
                        DATA = "test",
                        COMPRESSION = "off"
                )
            );
        )");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

#if 0
            const auto program = GetPrettyPrint(res);
            Cerr << program << Endl;
#endif

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("default"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("columnConstrains"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("columnFamilies"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(ChangefeedParseCorrect) {
    auto res = SqlToYql(R"( USE ydb;
                CREATE TABLE tableName (
                    Key Uint32, PRIMARY KEY (Key),
                    CHANGEFEED feedName WITH (
                        MODE = 'KEYS_ONLY',
                        FORMAT = 'json',
                        INITIAL_SCAN = TRUE,
                        USER_SIDS = TRUE,
                        TRACE_IDS = TRUE,
                        VIRTUAL_TIMESTAMPS = FALSE,
                        BARRIERS_INTERVAL = Interval("PT1S"),
                        SCHEMA_CHANGES = FALSE,
                        RETENTION_PERIOD = Interval("P1D"),
                        TOPIC_MIN_ACTIVE_PARTITIONS = 10,
                        AWS_REGION = 'aws:region'
                    )
                );
            )");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("changefeed"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("mode"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("KEYS_ONLY"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("format"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("json"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("initial_scan"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("user_sids"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("trace_ids"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("true"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("virtual_timestamps"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("false"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("barriers_interval"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("schema_changes"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("retention_period"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("topic_min_active_partitions"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("aws_region"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("aws:region"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CloneForAsTableWorksWithCube) {
    UNIT_ASSERT(SqlToYql("SELECT * FROM AS_TABLE([<|k1:1, k2:1|>]) GROUP BY CUBE(k1, k2);").IsOk());
}

Y_UNIT_TEST(WindowPartitionByColumnProperlyEscaped) {
    NYql::TAstParseResult res = SqlToYql("SELECT SUM(key) OVER w FROM plato.Input WINDOW w AS (PARTITION BY `column with space`);");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "CalcOverWindow") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("\"column with space\""));
        }
    };

    TWordCountHive elementStat = {{TString("CalcOverWindow"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["CalcOverWindow"]);
}

Y_UNIT_TEST(WindowPartitionByExpressionWithoutAliasesAreAllowed) {
    NYql::TAstParseResult res = SqlToYql("SELECT SUM(key) OVER w FROM plato.Input as i WINDOW w AS (PARTITION BY ii.subkey);");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "AddMember") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("AddMember row 'group_w_0 (SqlAccess 'struct (Member row '\"ii\")"));
        }
        if (word == "CalcOverWindow") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("CalcOverWindow core '('\"group_w_0\")"));
        }
    };

    TWordCountHive elementStat = {{TString("CalcOverWindow"), 0}, {TString("AddMember"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["CalcOverWindow"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["AddMember"]);
}

Y_UNIT_TEST(WindowPartitionByOrderByWindowFunctionIsNotAllowed) {
    ExpectFailWithError(
        R"sql(SELECT Rank() OVER (PARTITION BY x ORDER BY Rank()) FROM (VALUES (1)) AS t(x))sql",
        "<main>:1:45: Error: Failed to use window function: Rank without window\n");
    ExpectFailWithError(
        R"sql(SELECT Rank() OVER (PARTITION BY x ORDER BY RowNumber()) FROM (VALUES (1)) AS t(x))sql",
        "<main>:1:45: Error: Failed to use window function RowNumber without window specification or in wrong place\n");
    ExpectFailWithError(
        R"sql(SELECT RowNumber() OVER (PARTITION BY x ORDER BY Rank()) FROM (VALUES (1)) AS t(x))sql",
        "<main>:1:50: Error: Failed to use window function: Rank without window\n");
    ExpectFailWithError(
        R"sql(SELECT RowNumber() OVER (PARTITION BY x ORDER BY RowNumber()) FROM (VALUES (1)) AS t(x))sql",
        "<main>:1:50: Error: Failed to use window function RowNumber without window specification or in wrong place\n");
}

Y_UNIT_TEST(PqReadByAfterUse) {
    ExpectFailWithError("use plato; pragma PqReadBy='plato2';",
                        "<main>:1:28: Error: Cluster in PqReadPqBy pragma differs from cluster specified in USE statement: plato2 != plato\n");

    UNIT_ASSERT(SqlToYql("pragma PqReadBy='plato2';").IsOk());
    UNIT_ASSERT(SqlToYql("pragma PqReadBy='plato2'; use plato;").IsOk());
    UNIT_ASSERT(SqlToYql("$x='plato'; use rtmr:$x; pragma PqReadBy='plato2';").IsOk());
    UNIT_ASSERT(SqlToYql("use plato; pragma PqReadBy='dq';").IsOk());
}

Y_UNIT_TEST(MrObject) {
    NYql::TAstParseResult res = SqlToYql(
        "declare $path as String;\n"
        "select * from plato.object($path, `format`, \"comp\" || \"ression\" as compression, 1 as bar) with schema (Int32 as y, String as x)");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "MrObject") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos,
                                       line.find(R"__((MrObject (EvaluateAtom "$path") '"format" '('('"compression" (Concat (String '"comp") (String '"ression"))) '('"bar" (Int32 '"1")))))__"));
        } else if (word == "userschema") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos,
                                       line.find(R"__('('('"userschema" (StructType '('"y" (DataType 'Int32)) '('"x" (DataType 'String))) '('"y" '"x"))))__"));
        }
    };

    TWordCountHive elementStat = {{TString("MrObject"), 0}, {TString("userschema"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["MrObject"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["userschema"]);
}

Y_UNIT_TEST(TableBindings) {
    NSQLTranslation::TTranslationSettings settings = GetSettingsWithS3Binding("foo");
    NYql::TAstParseResult res = SqlToYqlWithSettings(
        "select * from bindings.foo",
        settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "MrObject") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos,
                                       line.find(R"__((MrTableConcat (Key '('table (String '"path")))) (Void) '('('"bar" '"1") '('"compression" '"ccompression") '('"format" '"format") '('"partitionedby" '"key" '"subkey") '('"userschema" (SqlTypeFromYson)__"));
        }
    };

    TWordCountHive elementStat = {{TString("MrTableConcat"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["MrTableConcat"]);

    settings.DefaultCluster = "plato";
    settings.BindingsMode = NSQLTranslation::EBindingsMode::DISABLED;
    res = SqlToYqlWithSettings(
        "select * from bindings.foo",
        settings);
    UNIT_ASSERT_VALUES_EQUAL(Err2Str(res), "<main>:1:15: Error: Please remove 'bindings.' from your query, the support for this syntax has ended, code: 4601\n");
    UNIT_ASSERT(!res.IsOk());

    settings.BindingsMode = NSQLTranslation::EBindingsMode::DROP;
    res = SqlToYqlWithSettings(
        "select * from bindings.foo",
        settings);

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine2 = [](const TString& word, const TString& line) {
        if (word == "MrTableConcat") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos,
                                       line.find(R"__((MrTableConcat (Key '('table (String '"foo")))) (Void) '())))__"));
        }
    };

    TWordCountHive elementStat2 = {{TString("MrTableConcat"), 0}};
    VerifyProgram(res, elementStat2, verifyLine2);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat2["MrTableConcat"]);

    settings.BindingsMode = NSQLTranslation::EBindingsMode::DROP_WITH_WARNING;
    res = SqlToYqlWithSettings(
        "select * from bindings.foo",
        settings);
    UNIT_ASSERT_VALUES_EQUAL(Err2Str(res), "<main>:1:15: Warning: Please remove 'bindings.' from your query, the support for this syntax will be dropped soon, code: 4538\n");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat3 = {{TString("MrTableConcat"), 0}};
    VerifyProgram(res, elementStat3, verifyLine2);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat3["MrTableConcat"]);
}

Y_UNIT_TEST(TableBindingsWithInsert) {
    NSQLTranslation::TTranslationSettings settings = GetSettingsWithS3Binding("foo");
    NYql::TAstParseResult res = SqlToYqlWithSettings(
        "insert into bindings.foo with truncate (x, y) values (1, 2);",
        settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write!") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos,
                                       line.find(R"__((Write! world sink (Key '('table (String '"path"))) values '('('"bar" '"1") '('"compression" '"ccompression") '('"format" '"format") '('"partitionedby" '"key" '"subkey") '('"userschema" (SqlTypeFromYson)__"));
        }
    };

    TWordCountHive elementStat = {{TString("Write!"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write!"]);

    settings.DefaultCluster = "plato";
    settings.BindingsMode = NSQLTranslation::EBindingsMode::DISABLED;
    res = SqlToYqlWithSettings(
        "insert into bindings.foo with truncate (x, y) values (1, 2);",
        settings);
    UNIT_ASSERT_VALUES_EQUAL(Err2Str(res), "<main>:1:13: Error: Please remove 'bindings.' from your query, the support for this syntax has ended, code: 4601\n");
    UNIT_ASSERT(!res.IsOk());

    settings.BindingsMode = NSQLTranslation::EBindingsMode::DROP;
    res = SqlToYqlWithSettings(
        "insert into bindings.foo with truncate (x, y) values (1, 2);",
        settings);
    UNIT_ASSERT_VALUES_EQUAL(Err2Str(res), "");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine2 = [](const TString& word, const TString& line) {
        if (word == "Write!") {
            // UNIT_ASSERT_VALUES_EQUAL(line, "");
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos,
                                       line.find(R"__((Write! world sink (Key '('table (String '"foo"))) values '('('mode 'renew)))__"));
        }
    };

    TWordCountHive elementStat2 = {{TString("Write!"), 0}};
    VerifyProgram(res, elementStat2, verifyLine2);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat2["Write!"]);

    settings.BindingsMode = NSQLTranslation::EBindingsMode::DROP_WITH_WARNING;
    res = SqlToYqlWithSettings(
        "insert into bindings.foo with truncate (x, y) values (1, 2);",
        settings);
    UNIT_ASSERT_VALUES_EQUAL(Err2Str(res), "<main>:1:13: Warning: Please remove 'bindings.' from your query, the support for this syntax will be dropped soon, code: 4538\n");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat3 = {{TString("Write!"), 0}};
    VerifyProgram(res, elementStat3, verifyLine2);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat3["Write!"]);
}

Y_UNIT_TEST(TrailingCommaInWithout) {
    UNIT_ASSERT(SqlToYql("SELECT * WITHOUT stream, FROM plato.Input").IsOk());
    UNIT_ASSERT(SqlToYql("SELECT a.* WITHOUT a.intersect, FROM plato.Input AS a").IsOk());
    UNIT_ASSERT(SqlToYql("SELECT a.* WITHOUT col1, col2, a.col3, FROM plato.Input AS a").IsOk());
}

Y_UNIT_TEST(NoStackOverflowOnBigCaseStatement) {
    TStringBuilder req;
    req << "select case 1 + 123";
    for (size_t i = 0; i < 20000; ++i) {
        req << " when " << i << " then " << i + 1;
    }
    req << " else 100500 end;";
    UNIT_ASSERT(SqlToYql(req).IsOk());
}

Y_UNIT_TEST(CollectPreaggregatedInListLiteral) {
    UNIT_ASSERT(SqlToYql("SELECT [COUNT(DISTINCT a+b)] FROM plato.Input").IsOk());
}

Y_UNIT_TEST(SmartParenInGroupByClause) {
    UNIT_ASSERT(SqlToYql("SELECT * FROM plato.Input GROUP BY (k, v)").IsOk());
}

Y_UNIT_TEST(AlterTableRenameToIsCorrect) {
    UNIT_ASSERT(SqlToYql("USE ydb;   ALTER TABLE table RENAME TO moved").IsOk());
}

Y_UNIT_TEST(AlterTableAddDropColumnIsCorrect) {
    UNIT_ASSERT(SqlToYql("USE ydb;   ALTER TABLE table ADD COLUMN addc uint64, DROP COLUMN dropc, ADD addagain uint64").IsOk());
}

Y_UNIT_TEST(AlterTableSetTTLIsCorrect) {
    UNIT_ASSERT(SqlToYql("USE ydb;   ALTER TABLE table SET (TTL = Interval(\"PT3H\") ON column)").IsOk());
    UNIT_ASSERT(SqlToYql("USE ydb;   ALTER TABLE table SET (TTL = Interval(\"PT3H\") ON column AS SECONDS)").IsOk());
}

Y_UNIT_TEST(AlterTableSetTieringIsCorrect) {
    UNIT_ASSERT(SqlToYql("USE ydb;   ALTER TABLE table SET (TIERING = 'my_tiering')").IsOk());
}

Y_UNIT_TEST(AlterTableAddChangefeedIsCorrect) {
    UNIT_ASSERT(SqlToYql("USE ydb;   ALTER TABLE table ADD CHANGEFEED feed WITH (MODE = 'UPDATES', FORMAT = 'json')").IsOk());
}

Y_UNIT_TEST(AlterTableAlterChangefeedIsCorrect) {
    UNIT_ASSERT(SqlToYql("USE ydb;   ALTER TABLE table ALTER CHANGEFEED feed DISABLE").IsOk());
    ExpectFailWithError("USE ydb;   ALTER TABLE table ALTER CHANGEFEED feed SET (FORMAT = 'proto');",
                        "<main>:1:57: Error: FORMAT alter is not supported\n");
}

Y_UNIT_TEST(AlterTableDropChangefeedIsCorrect) {
    UNIT_ASSERT(SqlToYql("USE ydb;   ALTER TABLE table DROP CHANGEFEED feed").IsOk());
}

Y_UNIT_TEST(AlterTableSetPartitioningIsCorrect) {
    UNIT_ASSERT(SqlToYql("USE ydb;   ALTER TABLE table SET (AUTO_PARTITIONING_BY_SIZE = DISABLED)").IsOk());
}

Y_UNIT_TEST(AlterTableAddIndexWithIsSupported) {
    UNIT_ASSERT(SqlToYql("USE ydb;   ALTER TABLE table ADD INDEX idx GLOBAL ON (col) WITH (a=b)").IsOk());
}

Y_UNIT_TEST(AlterTableAddIndexLocalIsNotSupported) {
    ExpectFailWithError("USE ydb;   ALTER TABLE table ADD INDEX idx LOCAL ON (col)",
                        "<main>:1:40: Error: local index must specify subtype with USING\n");
}

Y_UNIT_TEST(AlterTableAddIndexLocalBloomFilter) {
    const auto result = SqlToYql(R"sql(
            USE ydb;
            ALTER TABLE table ADD INDEX idx
            LOCAL USING bloom_filter
            ON (col)
            WITH (
                false_positive_probability=0.05
            )
            )sql");

    UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
}

Y_UNIT_TEST(AlterTableAddIndexLocalBloomNgramFilter) {
    const auto result = SqlToYql(R"sql(
            USE ydb;
            ALTER TABLE table ADD INDEX idx
            LOCAL USING bloom_ngram_filter
            ON (col)
            WITH (
                ngram_size=3,
                hashes_count=2,
                filter_size_bytes=512,
                records_count=1024,
                case_sensitive=true
            )
            )sql");

    UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
}

Y_UNIT_TEST(CreateTableAddIndexLocalBloomFilter) {
    const auto result = SqlToYql(R"sql(
            USE ydb;
            CREATE TABLE table (
            pk INT32 NOT NULL,
            col String,
            INDEX idx LOCAL USING bloom_filter
                ON (col)
                WITH (false_positive_probability=0.05),
            PRIMARY KEY (pk))
            )sql");

    UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
}

Y_UNIT_TEST(CreateTableAddIndexLocalBloomNgramFilter) {
    const auto result = SqlToYql(R"sql(
            USE ydb;
            CREATE TABLE table (
            pk INT32 NOT NULL,
            col String,
            INDEX idx LOCAL USING bloom_ngram_filter
                ON (col)
                WITH (ngram_size=3, hashes_count=2, filter_size_bytes=512, records_count=1024, case_sensitive=true),
            PRIMARY KEY (pk))
            )sql");

    UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
}

Y_UNIT_TEST(AlterTableAddIndexGlobalBloomFilterIsNotSupported) {
    ExpectFailWithError("USE ydb; ALTER TABLE table ADD INDEX idx GLOBAL USING bloom_filter ON (col)",
                        "<main>:1:55: Error: BLOOM_FILTER index can only be LOCAL\n");
}

Y_UNIT_TEST(AlterTableAddIndexLocalBloomCoverIsNotSupported) {
    ExpectFailWithFuzzyError("USE ydb; ALTER TABLE table ADD INDEX idx LOCAL USING bloom_filter ON (col) COVER (payload)",
                             "Error: COVER is not supported for local bloom indexes");
}

Y_UNIT_TEST(AlterTableDropIndexIsCorrect) {
    const auto result = SqlToYql("USE ydb; ALTER TABLE table DROP INDEX idx");
    UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
}

Y_UNIT_TEST(CreateTableWithStatisticsIsSupported) {
    auto res = SqlToYql(R"sql(
            USE ydb;
            CREATE TABLE table (
                k Uint64 NOT NULL,
                a Uint64,
                b Utf8,
                PRIMARY KEY (k),
                STATISTICS s ON (a, b) WITH (COUNT_MIN_SKETCH)
            );
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "statisticsName");
            UNIT_ASSERT_STRING_CONTAINS(line, "statisticsColumns");
            UNIT_ASSERT_STRING_CONTAINS(line, "statisticsTypes");
            UNIT_ASSERT_STRING_CONTAINS(line, "COUNT_MIN_SKETCH");
        }
    };
    TWordCountHive elementStat = {"Write"};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateTableWithStatisticsOnUnknownColumnFails) {
    ExpectFailWithError(R"sql(
        USE ydb;
        CREATE TABLE table (
            k Uint64 NOT NULL,
            a Uint64,
            PRIMARY KEY (k),
            STATISTICS s ON (missing) WITH (COUNT_MIN_SKETCH)
        );
    )sql", "<main>:7:30: Error: Undefined column: missing\n");
}

Y_UNIT_TEST(CreateTableWithStatisticsWithoutWithIsSupported) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE table (
            k Uint64 NOT NULL,
            a Uint64,
            b Utf8,
            PRIMARY KEY (k),
            STATISTICS s ON (a, b)
        );
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(CreateTableWithStatisticsEmptyWithFails) {
    // Empty WITH () is rejected by the grammar: the statistic type list must be non-empty.
    ExpectFailWithFuzzyError(R"sql(
        USE ydb;
        CREATE TABLE table (
            k Uint64 NOT NULL,
            a Uint64,
            PRIMARY KEY (k),
            STATISTICS s ON (a) WITH ()
        );
    )sql", "<main>:7:38: Error: mismatched input");
}

Y_UNIT_TEST(AlterTableAddStatisticsIsSupported) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE table
            ADD STATISTICS s1 ON (a, b) WITH (COUNT_MIN_SKETCH),
            ADD STATISTICS s2 ON (a) WITH (COUNT_MIN_SKETCH);
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "addStatistics");
        }
    };
    TWordCountHive elementStat = {"Write"};
    VerifyProgram(res, elementStat, verifyLine);
}

Y_UNIT_TEST(AlterTableAddStatisticsDuplicateFails) {
    ExpectFailWithError(R"sql(
        USE ydb;
        ALTER TABLE table
            ADD STATISTICS s1 ON (a) WITH (COUNT_MIN_SKETCH),
            ADD STATISTICS s1 ON (b) WITH (COUNT_MIN_SKETCH);
    )sql", "<main>:5:28: Error: Statistics s1 must be defined once\n");
}

Y_UNIT_TEST(AlterTableAddStatisticsWithoutWithIsSupported) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE table
            ADD STATISTICS s1 ON (a, b);
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(AlterTableAddStatisticsEmptyWithFails) {
    // Empty WITH () is rejected by the grammar: the statistic type list must be non-empty.
    ExpectFailWithFuzzyError(R"sql(
        USE ydb;
        ALTER TABLE table
            ADD STATISTICS s1 ON (a) WITH ();
    )sql", "<main>:4:43: Error: mismatched input");
}

Y_UNIT_TEST(AlterTableDropStatisticsIsSupported) {
    auto res = SqlToYql("USE ydb; ALTER TABLE table DROP STATISTICS s");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "dropStatistics");
        }
    };
    TWordCountHive elementStat = {"Write"};
    VerifyProgram(res, elementStat, verifyLine);
}

Y_UNIT_TEST(StatisticsKeywordIsNotReserved) {
    // STATISTICS is a non-reserved keyword and must remain usable as an identifier.
    UNIT_ASSERT(SqlToYql("SELECT 1 AS statistics;").IsOk());
    UNIT_ASSERT(SqlToYql("USE ydb; SELECT statistics FROM plato.Input;").IsOk());
}

Y_UNIT_TEST(CreateTableWithLocalBloomFilterAndDropIndexIsCorrect) {
    const auto result = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE table (
            pk INT32 NOT NULL,
            col String,
            INDEX idx LOCAL USING bloom_filter
                ON (col)
                WITH (false_positive_probability=0.05),
            PRIMARY KEY (pk)
        );
        ALTER TABLE table DROP INDEX idx;
    )sql");

    UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
}

Y_UNIT_TEST(CreateTableWithLocalBloomNgramFilterAndDropIndexIsCorrect) {
    const auto result = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE table (
            pk INT32 NOT NULL,
            col String,
            INDEX idx LOCAL USING bloom_ngram_filter
                ON (col)
                WITH (ngram_size=3, hashes_count=2, filter_size_bytes=512, records_count=1024, case_sensitive=true),
            PRIMARY KEY (pk)
        );
        ALTER TABLE table DROP INDEX idx;
    )sql");

    UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
}

Y_UNIT_TEST(CreateTableWithMinMaxIndex) {
    const auto result = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE table (
            pk INT32 NOT NULL,
            col String,
            INDEX idx LOCAL USING min_max ON (col),
            PRIMARY KEY (pk))
    )sql");

    UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
}

Y_UNIT_TEST(AlterTableAddMinMaxIndex) {
    const auto result = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE table ADD INDEX idx
        LOCAL USING min_max
        ON (col)
    )sql");

    UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
}

Y_UNIT_TEST(AlterTableAddIndexGlobalMinMaxIsNotSupported) {
    ExpectFailWithError("USE ydb; ALTER TABLE table ADD INDEX idx GLOBAL USING min_max ON (col)",
                        "<main>:1:55: Error: MIN_MAX index can only be LOCAL\n");
}

Y_UNIT_TEST(AlterTableAddIndexLocalMinMaxCoverIsNotSupported) {
    ExpectFailWithError("USE ydb; ALTER TABLE table ADD INDEX idx LOCAL USING min_max ON (col) COVER (payload)",
                        "<main>:1:66: Error: COVER is not supported for local MIN_MAX index\n");
}

Y_UNIT_TEST(MinMaxIndexAddDrop) {
    const auto result = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE table ADD INDEX idx LOCAL USING min_max on (col);
        ALTER TABLE table DROP INDEX idx;
    )sql");

    UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
}

Y_UNIT_TEST(CreateTableAddIndexGlobalUnique) {
    NYql::TAstParseResult result = SqlToYql(R"sql(USE ydb;
            CREATE TABLE table (
                pk INT32 NOT NULL,
                col String,
                INDEX idx GLOBAL UNIQUE ON(col),
                PRIMARY KEY (pk))
    )sql");
    UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
    UNIT_ASSERT(result.Root);

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        Y_UNUSED(word);
        UNIT_ASSERT_STRING_CONTAINS(line, R"('indexType 'syncGlobalUnique)");
    };

    TWordCountHive elementStat({TString(R"('indexName '"idx")")});
    VerifyProgram(result, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["\'indexName \'\"idx\""]);
}

Y_UNIT_TEST(CreateTableAddIndexGlobalUniqueSync) {
    NYql::TAstParseResult result = SqlToYql(R"sql(USE ydb;
        CREATE TABLE table (
            pk INT32 NOT NULL,
            col String,
            INDEX idx GLOBAL UNIQUE SYNC ON(col),
            PRIMARY KEY (pk))
    )sql");
    UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
    UNIT_ASSERT(result.Root);

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        Y_UNUSED(word);
        UNIT_ASSERT_STRING_CONTAINS(line, R"('indexType 'syncGlobalUnique)");
    };

    TWordCountHive elementStat({TString(R"('indexName '"idx")")});
    VerifyProgram(result, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["\'indexName \'\"idx\""]);
}

Y_UNIT_TEST(CreateTableAddIndexGlobalUniqueAsync) {
    ExpectFailWithError(R"sql(USE ydb;
            CREATE TABLE table (
                pk INT32 NOT NULL,
                col String,
                INDEX idx GLOBAL UNIQUE ASYNC ON(col),
                PRIMARY KEY (pk))
    )sql", "<main>:5:41: Error: unique: alternative is not implemented yet: \n");
}

Y_UNIT_TEST(AlterTableAddIndexGlobalUnique) {
    NYql::TAstParseResult result = SqlToYql(R"sql(USE ydb;
                ALTER TABLE table ADD INDEX idx GLOBAL UNIQUE ON(col))sql");
    UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
    UNIT_ASSERT(result.Root);

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        Y_UNUSED(word);
        UNIT_ASSERT_STRING_CONTAINS(line, R"('indexType 'syncGlobalUnique)");
    };

    TWordCountHive elementStat({TString(R"('indexName '"idx")")});
    VerifyProgram(result, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["\'indexName \'\"idx\""]);
}

Y_UNIT_TEST(AlterTableAddIndexGlobalUniqueSync) {
    NYql::TAstParseResult result = SqlToYql(R"sql(USE ydb;
                ALTER TABLE table ADD INDEX idx GLOBAL UNIQUE SYNC ON(col))sql");
    UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
    UNIT_ASSERT(result.Root);

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        Y_UNUSED(word);
        UNIT_ASSERT_STRING_CONTAINS(line, R"('indexType 'syncGlobalUnique)");
    };

    TWordCountHive elementStat({TString(R"('indexName '"idx")")});
    VerifyProgram(result, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["\'indexName \'\"idx\""]);
}

Y_UNIT_TEST(AlterTableAddIndexGlobalUniqueAsync) {
    ExpectFailWithError(R"sql(USE ydb;
            ALTER TABLE table ADD INDEX idx GLOBAL UNIQUE ASYNC ON(col))sql",
                        "<main>:2:59: Error: unique: alternative is not implemented yet: \n");
}

Y_UNIT_TEST(CreateTableAddIndexVector) {
    const auto result = SqlToYql(R"sql(USE ydb;
                CREATE TABLE table (
                    pk INT32 NOT NULL,
                    col String,
                    INDEX idx GLOBAL USING vector_kmeans_tree
                        ON (col) COVER (col)
                        WITH (distance=cosine, vector_type=float, vector_dimension=1024, levels=3, clusters=10),
                    PRIMARY KEY (pk))
                    )sql");
    UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
}

Y_UNIT_TEST(AlterTableAddIndexVector) {
    const auto result = SqlToYql(R"sql(USE ydb;
                ALTER TABLE table ADD INDEX idx
                    GLOBAL USING vector_kmeans_tree
                    ON (col) COVER (col)
                    WITH (distance=cosine, vector_type="float", vector_dimension=1024, levels=3, clusters=10)
                    )sql");
    UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
}

Y_UNIT_TEST(AlterTableAddIndexDifferentSettings) {
    // index settings and their types are checked in KQP
    const auto result = SqlToYql(R"sql(USE ydb;
                ALTER TABLE table ADD INDEX idx
                    GLOBAL USING vector_kmeans_tree
                    ON (col) COVER (col)
                    WITH (distance=42, vector_type="float", vector_dimension=True, levels=none, clusters=10, asdf=qwerty)
                    )sql");
    UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
}

Y_UNIT_TEST(AlterTableAddIndexDuplicatedSetting) {
    ExpectFailWithError(R"sql(USE ydb;
            ALTER TABLE table ADD INDEX idx
                GLOBAL USING vector_kmeans_tree
                ON (col) COVER (col)
                WITH (distance=cosine, distance=42)
                )sql",
                        "<main>:5:49: Error: Duplicated distance\n");
}

Y_UNIT_TEST(AlterTableAddIndexUnknownSubtype) {
    ExpectFailWithError("USE ydb;   ALTER TABLE table ADD INDEX idx GLOBAL USING unknown ON (col)",
                        "<main>:1:57: Error: UNKNOWN index subtype is not supported\n");
}

Y_UNIT_TEST(AlterTableAlterIndexSetPartitioningIsCorrect) {
    const auto result = SqlToYql("USE ydb;   ALTER TABLE table ALTER INDEX index SET AUTO_PARTITIONING_MIN_PARTITIONS_COUNT 10");
    UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
}

Y_UNIT_TEST(AlterTableAlterIndexSetMultiplePartitioningSettings) {
    const auto result = SqlToYql("USE ydb;   ALTER TABLE table ALTER INDEX index SET "
                                 "(AUTO_PARTITIONING_BY_LOAD = ENABLED, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10)");
    UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
}

Y_UNIT_TEST(AlterTableAlterIndexResetPartitioningIsNotSupported) {
    ExpectFailWithError("USE ydb;   ALTER TABLE table ALTER INDEX index RESET (AUTO_PARTITIONING_MIN_PARTITIONS_COUNT)",
                        "<main>:1:55: Error: AUTO_PARTITIONING_MIN_PARTITIONS_COUNT reset is not supported\n");
}

Y_UNIT_TEST(AlterTableAlterIndexSetReadReplicasSettingsUncompatIsCorrect) {
    const auto result = SqlToYql("USE ydb;   ALTER TABLE table ALTER INDEX index SET READ_REPLICAS_SETTINGS \"PER_AZ:1\"");
    UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
}

Y_UNIT_TEST(AlterTableAlterIndexSetReadReplicasSettingsCompatIsCorrect) {
    const auto result = SqlToYql("USE ydb;   ALTER TABLE table ALTER INDEX index SET (READ_REPLICAS_SETTINGS = \"PER_AZ:1\")");
    UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
}

Y_UNIT_TEST(AlterTableAlterIndexSetLocalBloomFppIsCorrect) {
    const auto result = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE table ALTER INDEX idx SET (false_positive_probability = 0.06);
    )sql");
    UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
}

Y_UNIT_TEST(AlterTableAlterIndexResetReadReplicasSettingsIsNotSupported) {
    ExpectFailWithError("USE ydb;   ALTER TABLE table ALTER INDEX index RESET (READ_REPLICAS_SETTINGS)",
                        "<main>:1:55: Error: READ_REPLICAS_SETTINGS reset is not supported\n");
}

Y_UNIT_TEST(AlterTableAlterColumnDropNotNullAstCorrect) {
    auto reqDropNotNull = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE tableName (
            id Uint32,
            val Uint32 NOT NULL,
            PRIMARY KEY (id)
        );

        COMMIT;
        ALTER TABLE tableName ALTER COLUMN val DROP NOT NULL;
    )sql");

    UNIT_ASSERT(reqDropNotNull.IsOk());

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        Y_UNUSED(word);

        UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(
                                                      "'('changeColumnConstraints '('('drop_not_null)))"));
    };

    TWordCountHive elementStat({TString("\'mode \'alter")});
    VerifyProgram(reqDropNotNull, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["\'mode \'alter"]);
}

Y_UNIT_TEST(AlterTableAlterColumnSetNotNullAstCorrect) {
    auto reqSetNotNull = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE tableName (
            id Uint32,
            val Uint32,
            PRIMARY KEY (id)
        );

        COMMIT;
        ALTER TABLE tableName ALTER COLUMN val SET NOT NULL;
    )sql");

    UNIT_ASSERT(reqSetNotNull.IsOk());

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        Y_UNUSED(word);

        UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(
                                                      "'('changeColumnConstraints '('('set_not_null)))"));
    };

    TWordCountHive elementStat({TString("\'mode \'alter")});
    VerifyProgram(reqSetNotNull, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["\'mode \'alter"]);
}

Y_UNIT_TEST(AlterTableAlterColumnSetEncodingOffAstCorrect) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tableName ALTER COLUMN val SET ENCODING(OFF);
    )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const TString modeAlter = "'mode 'alter";
    const TString changeEncoding = "'('changeEncoding '('('('name 'off))))";

    TWordCountHive elementStat{{modeAlter, changeEncoding}};
    TString program = VerifyProgram(res, elementStat);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat[modeAlter]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat[changeEncoding]);
}

Y_UNIT_TEST(AlterTableAlterColumnSetEncodingDictAstCorrect) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tableName ALTER COLUMN val SET ENCODING(DICT);
    )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const TString modeAlter = "'mode 'alter";
    const TString changeEncoding = "'('changeEncoding '('('('name 'dict))))";

    TWordCountHive elementStat{{modeAlter, changeEncoding}};
    TString program = VerifyProgram(res, elementStat);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat[modeAlter]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat[changeEncoding]);
}

Y_UNIT_TEST(AlterTableAlterColumnSetEncodingEmptyAstCorrect) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tableName ALTER COLUMN val SET ENCODING();
    )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const TString modeAlter = "'mode 'alter";
    const TString changeEncoding = "'('changeEncoding '())";

    TWordCountHive elementStat{{modeAlter, changeEncoding}};
    TString program = VerifyProgram(res, elementStat);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat[modeAlter]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat[changeEncoding]);
}

Y_UNIT_TEST(AlterTableAlterColumnSetEncodingDictWithParamsAstCorrect) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tableName ALTER COLUMN val SET ENCODING(DICT(max_size=100));
    )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const TString modeAlter = "'mode 'alter";
    const TString changeEncoding = "'('changeEncoding '('('('name 'dict) '('max_size (Uint64 '\"100\")))))";

    TWordCountHive elementStat{{modeAlter, changeEncoding}};
    TString program = VerifyProgram(res, elementStat);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat[modeAlter]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat[changeEncoding]);
}

Y_UNIT_TEST(AlterTableSetEncodingDictAndOffOrderAndParamsAstCorrect) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tableName ALTER COLUMN val SET ENCODING(DICT(max_size=100), OFF);
    )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const TString modeAlter = "'mode 'alter";
    const TString changeEncoding = "'('changeEncoding '('('('name 'dict) '('max_size (Uint64 '\"100\"))) '('('name 'off))))";

    TWordCountHive elementStat{{modeAlter, changeEncoding}};
    TString program = VerifyProgram(res, elementStat);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat[modeAlter]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat[changeEncoding]);
}

Y_UNIT_TEST(CreateTableSetEncodingDictAstCorrect) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE tableName (
            id Uint32 ENCODING(DICT),
            PRIMARY KEY (id)
        );
    )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const TString modeAlter = "Write";
    const TString columnEncoding = "'('columnEncoding '('('('name 'dict))))";

    TWordCountHive elementStat{{modeAlter, columnEncoding}};
    TString program = VerifyProgram(res, elementStat);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat[modeAlter]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat[columnEncoding]);
}

Y_UNIT_TEST(CreateTableSetEncodingEmptyAstCorrect) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE tableName (
            id Uint32 ENCODING(),
            PRIMARY KEY (id)
        );
    )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const TString modeAlter = "Write";
    const TString columnEncoding = "'('columnEncoding '())";

    TWordCountHive elementStat{{modeAlter, columnEncoding}};
    TString program = VerifyProgram(res, elementStat);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat[modeAlter]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat[columnEncoding]);
}

Y_UNIT_TEST(CreateTableSetEncodingOffAstCorrect) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE tableName (
            id Uint32 ENCODING(OFF),
            PRIMARY KEY (id)
        );
    )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const TString modeAlter = "Write";
    const TString columnEncoding = "'('columnEncoding '('('('name 'off))))";

    TWordCountHive elementStat{{modeAlter, columnEncoding}};
    TString program = VerifyProgram(res, elementStat);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat[modeAlter]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat[columnEncoding]);
}

Y_UNIT_TEST(CreateTableSetEncodingDictWithParamsAstCorrect) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE tableName (
            id Uint32 ENCODING(DICT(max_size=100)),
            PRIMARY KEY (id)
        );
    )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const TString modeAlter = "Write";
    const TString columnEncoding = "'('columnEncoding '('('('name 'dict) '('max_size (Uint64 '\"100\")))))";

    TWordCountHive elementStat{{modeAlter, columnEncoding}};
    TString program = VerifyProgram(res, elementStat);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat[modeAlter]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat[columnEncoding]);
}

Y_UNIT_TEST(CreateTableSetEncodingDictAndOffOrderAndParamsAstCorrect) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE tableName (
            id Uint32 ENCODING(DICT(max_size=100), OFF),
            PRIMARY KEY (id)
        );
    )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const TString modeAlter = "Write";
    const TString columnEncoding = "'('columnEncoding '('('('name 'dict) '('max_size (Uint64 '\"100\"))) '('('name 'off))))";

    TWordCountHive elementStat{{modeAlter, columnEncoding}};
    TString program = VerifyProgram(res, elementStat);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat[modeAlter]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat[columnEncoding]);
}

Y_UNIT_TEST(CreateTableNotSetEncodingAstCorrect) {
    auto res = SqlToYql(R"sql(
            USE ydb;
            CREATE TABLE tableName (
                id Uint32,
                PRIMARY KEY (id)
            );
        )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const TString modeAlter = "Write";
    const TString columnEncoding = "columnEncoding";

    TWordCountHive elementStat{{modeAlter, columnEncoding}};
    TString program = VerifyProgram(res, elementStat);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat[modeAlter]);
    UNIT_ASSERT_VALUES_EQUAL(0, elementStat[columnEncoding]);
}

Y_UNIT_TEST(AlterTableAddColumnSetEncodingAstCorrect) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tableName ADD COLUMN val Uint32 ENCODING(DICT(max_size=100), OFF)
    )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const TString modeAlter = "Write";
    const TString columnEncoding = "'('columnEncoding '('('('name 'dict) '('max_size (Uint64 '\"100\"))) '('('name 'off))))";

    TWordCountHive elementStat{{modeAlter, columnEncoding}};
    TString program = VerifyProgram(res, elementStat);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat[modeAlter]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat[columnEncoding]);
}

Y_UNIT_TEST(AlterTableAddColumnNotSetEncodingAstCorrect) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tableName ADD COLUMN val Uint32;
    )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const TString modeAlter = "'mode 'alter";
    const TString columnEncoding = "columnEncoding";

    TWordCountHive elementStat{{modeAlter, columnEncoding}};
    TString program = VerifyProgram(res, elementStat);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat[modeAlter]);
    UNIT_ASSERT_VALUES_EQUAL(0, elementStat[columnEncoding]);
}

Y_UNIT_TEST(AlterTableYtNotSupported) {
    ExpectFailWithError("ALTER TABLE plato.table ADD COLUMN a int32",
                        "<main>:1:19: Error: ALTER TABLE is not supported for yt provider.\n");
}

Y_UNIT_TEST(AlterTableCompactIsCorrect) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE table COMPACT;
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(AlterTableCompactWithSettingsIsCorrect) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE table COMPACT WITH (CASCADE = true, PARALLEL = 2);
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(AlterTableCompactWithSettingsWrongValues) {
    ExpectFailWithError(R"sql(
            USE ydb;
            ALTER TABLE table COMPACT WITH (cascade = 23, PARALLEL = 2);
    )sql", "<main>:3:55: Error: CASCADE value should be a boolean\n");
    ExpectFailWithError(R"sql(
            USE ydb;
            ALTER TABLE table COMPACT WITH (CASCADE = true, parallel = "abc");
    )sql", "<main>:3:72: Error: PARALLEL value should be a Int32\n");
    ExpectFailWithError(R"sql(
            USE ydb;
            ALTER TABLE table COMPACT WITH (CASCADE = true, PARALLEL = 2, some_option = 3);
    )sql", "<main>:3:75: Error: SOME_OPTION: unknown setting for compact\n");
    ExpectFailWithError(R"sql(
            USE ydb;
            ALTER TABLE table COMPACT WITH (CASCADE = true, parallel = 5000000000);
    )sql", "<main>:3:72: Error: PARALLEL value should be a Int32\n");
}

Y_UNIT_TEST(AlterTableCompactWithSettingsDuplicatedValues) {
    ExpectFailWithError(R"sql(
            USE ydb;
            ALTER TABLE table COMPACT WITH (CASCADE = true, PARALLEL = 2, CASCADE = false);
    )sql", "<main>:3:75: Error: Duplicated CASCADE\n");
    ExpectFailWithError(R"sql(
            USE ydb;
            ALTER TABLE table COMPACT WITH (CASCADE = true, PARALLEL = 2, PARALLEL = 10);
    )sql", "<main>:3:75: Error: Duplicated PARALLEL\n");
}

Y_UNIT_TEST(AlterSequence) {
    UNIT_ASSERT(SqlToYql(R"(
            USE ydb;
            ALTER SEQUENCE sequence START WITH 10 INCREMENT 2 RESTART WITH 5;
        )")
                    .IsOk());
    UNIT_ASSERT(SqlToYql(R"(
            USE ydb;
            ALTER SEQUENCE sequence INCREMENT 2;
        )")
                    .IsOk());
    UNIT_ASSERT(SqlToYql(R"(
            USE plato;
            ALTER SEQUENCE sequence INCREMENT 2 START 1000;
        )")
                    .IsOk());
    UNIT_ASSERT(SqlToYql(R"(
            USE ydb;
            ALTER SEQUENCE sequence RESTART START 1000;
        )")
                    .IsOk());
    UNIT_ASSERT(SqlToYql(R"(
            USE ydb;
            ALTER SEQUENCE IF EXISTS sequence INCREMENT 1000 START 100 RESTART;
        )")
                    .IsOk());
    UNIT_ASSERT(SqlToYql(R"(
            USE ydb;
            ALTER SEQUENCE IF EXISTS sequence RESTART 1000 START WITH 100 INCREMENT BY 7;
        )")
                    .IsOk());
}

Y_UNIT_TEST(AlterSequenceIncorrect) {
    {
        NYql::TAstParseResult res = SqlToYql("USE ydb;   ALTER SEQUENCE sequence START WITH 10 INCREMENT 2 RESTART WITH 5 RESTART;");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:75: Error: Restart value defined more than once\n");
    }
    {
        NYql::TAstParseResult res = SqlToYql("USE ydb;   ALTER SEQUENCE sequence START WITH 10 INCREMENT 2 START 100 RESTART WITH 5;");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:60: Error: Start value defined more than once\n");
    }
    {
        NYql::TAstParseResult res = SqlToYql("USE ydb;   ALTER SEQUENCE sequence INCREMENT BY 7 START WITH 10 INCREMENT 2 RESTART WITH 5 RESTART;");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:62: Error: Increment defined more than once\n");
    }
    {
        NYql::TAstParseResult res = SqlToYql("USE ydb;   ALTER SEQUENCE sequence RESTART WITH 100 START WITH 10 INCREMENT 2 RESTART WITH 5;");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:77: Error: Restart value defined more than once\n");
    }
    {
        NYql::TAstParseResult res = SqlToYql("USE ydb;   ALTER SEQUENCE sequence RESTART WITH 1234234543563435151456 START WITH 10 INCREMENT 2;");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:49: Error: Failed to parse number from string: 1234234543563435151456, number limit overflow\n");
    }
    {
        NYql::TAstParseResult res = SqlToYql("USE ydb;   ALTER SEQUENCE sequence RESTART WITH 1 START WITH 9223372036854775817 INCREMENT 4;");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:12: Error: Start value: 9223372036854775817 cannot be greater than max value: 9223372036854775807\n");
    }
    {
        NYql::TAstParseResult res = SqlToYql("USE ydb;   ALTER SEQUENCE sequence RESTART WITH 9223372036854775827 START WITH 5 INCREMENT 4;");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:12: Error: Restart value: 9223372036854775827 cannot be greater than max value: 9223372036854775807\n");
    }
    {
        NYql::TAstParseResult res = SqlToYql("USE ydb;   ALTER SEQUENCE sequence RESTART WITH 1 START WITH 4 INCREMENT 0;");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:12: Error: Increment must not be zero\n");
    }
    {
        NYql::TAstParseResult res = SqlToYql("USE ydb;   ALTER SEQUENCE sequence RESTART WITH 0 START WITH 4 INCREMENT 1;");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:12: Error: Restart value: 0 cannot be less than min value: 1\n");
    }
    {
        NYql::TAstParseResult res = SqlToYql("USE ydb;   ALTER SEQUENCE sequence RESTART WITH 1 START WITH 0 INCREMENT 1;");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:12: Error: Start value: 0 cannot be less than min value: 1\n");
    }
    {
        NYql::TAstParseResult res = SqlToYql("USE ydb;   ALTER SEQUENCE sequence RESTART WITH 1 START WITH 1 INCREMENT 9223372036854775837;");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:12: Error: Increment: 9223372036854775837 cannot be greater than max value: 9223372036854775807\n");
    }
}

Y_UNIT_TEST(AlterSequenceCorrect) {
    {
        NYql::TAstParseResult res = SqlToYql("USE ydb;   ALTER SEQUENCE sequence START WITH 10 INCREMENT 2 RESTART WITH 5;");
        UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("sequence"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("alter"));
                UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("alter_if_exists"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("start"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("increment"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("restart"));
            }
        };

        TWordCountHive elementStat = {{TString("Write"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    {
        NYql::TAstParseResult res = SqlToYql("USE ydb;   ALTER SEQUENCE IF EXISTS sequence INCREMENT 2 RESTART;");
        UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("sequence"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("alter_if_exists"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("increment"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("restart"));
            }
        };

        TWordCountHive elementStat = {{TString("Write"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    {
        NYql::TAstParseResult res = SqlToYql("USE ydb;   ALTER SEQUENCE IF EXISTS sequence START 10 INCREMENT BY 2;");
        UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("sequence"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("alter_if_exists"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("start"));
                UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("restart"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("increment"));
            }
        };

        TWordCountHive elementStat = {{TString("Write"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }
}

Y_UNIT_TEST(ShowCreateTable) {
    NYql::TAstParseResult res = SqlToYql(R"(
                USE plato;
                SHOW CREATE TABLE user;
            )");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Read") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("showCreateTable"));
        }
    };

    TWordCountHive elementStat = {{TString("Read"), 0}, {TString("showCreateTable"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Read"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["showCreateTable"]);
}

Y_UNIT_TEST(ShowCreateView) {
    NYql::TAstParseResult res = SqlToYql(R"(
                USE plato;
                SHOW CREATE VIEW user;
            )");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Read") {
            UNIT_ASSERT_STRING_CONTAINS(line, "showCreateView");
        }
    };

    TWordCountHive elementStat = {{"Read"}, {"showCreateView"}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(elementStat["Read"], 1);
    UNIT_ASSERT_VALUES_EQUAL(elementStat["showCreateView"], 1);
}

Y_UNIT_TEST(ShowCreateExternalDataSource) {
    NYql::TAstParseResult res = SqlToYql(R"(
                USE plato;
                SHOW CREATE EXTERNAL DATA SOURCE source;
            )");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Read") {
            UNIT_ASSERT_STRING_CONTAINS(line, "showCreateExternalDataSource");
        }
    };

    TWordCountHive elementStat = {{"Read"}, {"showCreateExternalDataSource"}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(elementStat["Read"], 1);
    UNIT_ASSERT_VALUES_EQUAL(elementStat["showCreateExternalDataSource"], 1);
}

Y_UNIT_TEST(OptionalAliases) {
    UNIT_ASSERT(SqlToYql("USE plato; SELECT foo FROM (SELECT key foo FROM Input);").IsOk());
    UNIT_ASSERT(SqlToYql("USE plato; SELECT a.x FROM Input1 a JOIN Input2 b ON a.key = b.key;").IsOk());
    UNIT_ASSERT(SqlToYql("USE plato; SELECT a.x FROM (VALUES (1,2), (3,4)) a(x,key) JOIN Input b ON a.key = b.key;").IsOk());
}

Y_UNIT_TEST(TableNameConstness) {
    UNIT_ASSERT(SqlToYql("USE plato; $path = 'foo'; SELECT TableName($path), count(*) FROM Input;").IsOk());
    UNIT_ASSERT(SqlToYql("$path = 'foo'; SELECT TableName($path, 'yt'), count(*) FROM plato.Input;").IsOk());
    ExpectFailWithError("USE plato; SELECT TableName(), count(*) FROM plato.Input;",
                        "<main>:1:19: Error: Expression has to be an aggregation function or key column, because aggregation is used elsewhere in this subquery\n");
}

Y_UNIT_TEST(UseShouldWorkAsColumnName) {
    UNIT_ASSERT(SqlToYql("select use from (select 1 as use);").IsOk());
}

Y_UNIT_TEST(TrueFalseWorkAfterDollar) {
    UNIT_ASSERT(SqlToYql("$ true = false; SELECT $ true or false;").IsOk());
    UNIT_ASSERT(SqlToYql("$False = 0; SELECT $False;").IsOk());
}

Y_UNIT_TEST(WithSchemaEquals) {
    UNIT_ASSERT(SqlToYql("select * from plato.T with schema Struct<a:Int32, b:String>;").IsOk());
    UNIT_ASSERT(SqlToYql("select * from plato.T with columns = Struct<a:Int32, b:String>;").IsOk());
}

Y_UNIT_TEST(WithNonStructSchemaS3) {
    NSQLTranslation::TTranslationSettings settings;
    settings.ClusterMapping["s3bucket"] = NYql::S3ProviderName;
    UNIT_ASSERT(SqlToYqlWithSettings("select * from s3bucket.`foo` with schema (col1 Int32, String as col2, Int64 as col3);", settings).IsOk());
}

Y_UNIT_TEST(AllowNestedTuplesInGroupBy) {
    NYql::TAstParseResult res = SqlToYql("select count(*) from plato.Input group by 1 + (x, y, z);");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
        Y_UNUSED(word);
        UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("(Aggregate core '('\"group0\")"));
    };

    TWordCountHive elementStat({"Aggregate"});
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT(elementStat["Aggregate"] == 1);
}

Y_UNIT_TEST(AllowGroupByWithParens) {
    NYql::TAstParseResult res = SqlToYql("select count(*) from plato.Input group by (x, y as alias1, z);");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
        Y_UNUSED(word);
        UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("(Aggregate core '('\"x\" '\"alias1\" '\"z\")"));
    };

    TWordCountHive elementStat({"Aggregate"});
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT(elementStat["Aggregate"] == 1);
}

Y_UNIT_TEST(CreateAsyncReplicationParseGeneralCorrect) {
    const auto req = R"sql(
        USE plato;
        CREATE ASYNC REPLICATION MyReplication
        FOR table1 AS table2, table3 AS table4
        WITH (
            CONNECTION_STRING = "grpc://localhost:2135/?database=/MyDatabase",
            ENDPOINT = "localhost:2135",
            DATABASE = "/MyDatabase",
            CA_CERT = "-----BEGIN CERTIFICATE-----",
            TOKEN_SECRET_NAME = "token_secret_name",
            PASSWORD_SECRET_PATH = "password_secret_path",
            INITIAL_TOKEN_SECRET_PATH = "initial_token_secret_path"
        );
    )sql";
    const auto res = SqlToYql(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "MyReplication");
            UNIT_ASSERT_STRING_CONTAINS(line, "create");
            UNIT_ASSERT_STRING_CONTAINS(line, "table1");
            UNIT_ASSERT_STRING_CONTAINS(line, "table2");
            UNIT_ASSERT_STRING_CONTAINS(line, "table3");
            UNIT_ASSERT_STRING_CONTAINS(line, "table4");
            UNIT_ASSERT_STRING_CONTAINS(line, "connection_string");
            UNIT_ASSERT_STRING_CONTAINS(line, "grpc://localhost:2135/?database=/MyDatabase");
            UNIT_ASSERT_STRING_CONTAINS(line, "endpoint");
            UNIT_ASSERT_STRING_CONTAINS(line, "localhost:2135");
            UNIT_ASSERT_STRING_CONTAINS(line, "database");
            UNIT_ASSERT_STRING_CONTAINS(line, "/MyDatabase");
            UNIT_ASSERT_STRING_CONTAINS(line, "ca_cert");
            UNIT_ASSERT_STRING_CONTAINS(line, "-----BEGIN CERTIFICATE-----");
            UNIT_ASSERT_STRING_CONTAINS(line, "\"token_secret_name");
            UNIT_ASSERT_STRING_CONTAINS(line, "\"password_secret_path");
            UNIT_ASSERT_STRING_CONTAINS(line, "\"initial_token_secret_path");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateAsyncReplicationParseSecretPathsCorrect) {
    const auto req = R"sql(
        USE plato; PRAGMA TablePathPrefix = "/PathPrefix";
        CREATE ASYNC REPLICATION MyReplication
        FOR table AS table
        WITH (
            CONNECTION_STRING = "grpc://localhost:2135/?database=/MyDatabase",
            ENDPOINT = "localhost:2135",
            DATABASE = "/MyDatabase",
            TOKEN_SECRET_PATH = "/token_secret_path",
            PASSWORD_SECRET_NAME = "password_secret_name",
            INITIAL_TOKEN_SECRET_PATH = "initial_token_secret_path"
        );
    )sql";
    const auto res = SqlToYql(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "\"/token_secret_path");
            UNIT_ASSERT_STRING_CONTAINS(line, "\"password_secret_name");
            UNIT_ASSERT_STRING_CONTAINS(line, "/PathPrefix/initial_token_secret_path");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateAsyncReplicationUnsupportedSettings) {
    const auto settings = THashMap<TString, TString>{
        {"STATE", "DONE"},
        {"FAILOVER_MODE", "FORCE"},
    };

    for (const auto& [k, v] : settings) {
        const auto res = SqlToYql(std::format(
            R"sql(
                USE plato;
                CREATE ASYNC REPLICATION MyReplication
                FOR table1 AS table2, table3 AS table4
                WITH (
                    {} = "{}"
                )
            )sql",
            k.c_str(),
            v.c_str()));
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), std::format("<main>:6:{}: Error: {} is not supported in CREATE\n", 24 + k.size(), k.c_str()));
    }
}

Y_UNIT_TEST(CreateAsyncReplicationMutuallyExclusiveSettings) {
    static const TVector<std::pair<TString, TString>> MutuallyExclusiveSettings = {
        {"TOKEN_SECRET_NAME", "TOKEN_SECRET_PATH"},
        {"PASSWORD_SECRET_NAME", "PASSWORD_SECRET_PATH"},
        {"INITIAL_TOKEN_SECRET_NAME", "INITIAL_TOKEN_SECRET_PATH"},
    };

    for (const auto& [name, path] : MutuallyExclusiveSettings) {
        const auto res = SqlToYql(std::format(
            R"sql(
                USE plato;
                CREATE ASYNC REPLICATION MyReplication
                FOR table1 AS table2, table3 AS table4
                WITH (
                    {} = "",
                    {} = ""
                )
            )sql",
            name.c_str(),
            path.c_str()));
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(
            Err2Str(res),
            std::format(
                "<main>:7:{}: Error: {} and {} are mutually exclusive\n",
                24 + name.size(), name.c_str(), path.c_str()));
    }
}

Y_UNIT_TEST(AsyncReplicationInvalidCommitInterval) {
    auto req = R"(
            USE plato;
            CREATE ASYNC REPLICATION MyReplication
            FOR table1 AS table2, table3 AS table4
            WITH (
                COMMIT_INTERVAL = "FOO"
            );
        )";

    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:6:35: Error: Literal of Interval type is expected for COMMIT_INTERVAL\n");
}

Y_UNIT_TEST(AlterAsyncReplicationGeneralParsingCorrect) {
    auto req = R"sql(
        USE plato;
        ALTER ASYNC REPLICATION MyReplication
        SET (
            STATE = "DONE",
            FAILOVER_MODE = "FORCE"
        );
    )sql";
    auto res = SqlToYql(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "MyReplication");
            UNIT_ASSERT_STRING_CONTAINS(line, "alter");
            UNIT_ASSERT_STRING_CONTAINS(line, "state");
            UNIT_ASSERT_STRING_CONTAINS(line, "DONE");
            UNIT_ASSERT_STRING_CONTAINS(line, "failover_mode");
            UNIT_ASSERT_STRING_CONTAINS(line, "FORCE");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(AlterAsyncReplicationSecretsWithoutTablePathPrefixParsingCorrect) {
    auto req = R"sql(
        USE plato;
        ALTER ASYNC REPLICATION MyReplication
        SET (
            TOKEN_SECRET_NAME = "foo_secret",
            PASSWORD_SECRET_PATH = "bar_secret",
            INITIAL_TOKEN_SECRET_PATH = "/baz_secret"
        );
    )sql";
    auto res = SqlToYql(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "\"foo_secret");
            UNIT_ASSERT_STRING_CONTAINS(line, "\"bar_secret");
            UNIT_ASSERT_STRING_CONTAINS(line, "\"/baz_secret");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(AlterAsyncReplicationSecretsWithTablePathPrefixParsingCorrect) {
    auto req = R"sql(
        USE plato; PRAGMA TablePathPrefix = "/PathPrefix";
        ALTER ASYNC REPLICATION MyReplication
            SET(
                TOKEN_SECRET_NAME = "foo_secret",
                PASSWORD_SECRET_PATH = "bar_secret",
                INITIAL_TOKEN_SECRET_PATH = "/baz_secret"
            );
    )sql";
    auto res = SqlToYql(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "\"foo_secret");
            UNIT_ASSERT_STRING_CONTAINS(line, "/PathPrefix/bar_secret");
            UNIT_ASSERT_STRING_CONTAINS(line, "\"/baz_secret");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(AlterAsyncReplicationSettings) {
    const auto settings = THashMap<TString, TString>{
        {"connection_string", "grpc://localhost:2135/?database=/MyDatabase"},
        {"endpoint", "localhost:2135"},
        {"database", "/MyDatabase"},
        {"token", "foo"},
        {"token_secret_name", "foo_secret_name"},
        {"user", "user"},
        {"password", "bar"},
        {"password_secret_name", "bar_secret_name"},
        {"initial_token_secret_path", "baz_secret_path"},
        {"ca_cert", "-----BEGIN CERTIFICATE-----"},
    };

    for (const auto& [k, v] : settings) {
        const auto res = SqlToYql(std::format(
            R"sql(
                USE plato;
                ALTER ASYNC REPLICATION MyReplication
                SET (
                    {} = "{}"
                )
            )sql",
            k.c_str(),
            v.c_str()));
        UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

        TVerifyLineFunc verifyLine = [&k, &v](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("MyReplication"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("alter"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(k));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(v));
            }
        };

        TWordCountHive elementStat = {{TString("Write"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }
}

Y_UNIT_TEST(AlterAsyncReplicationUnsupportedSettings) {
    {
        auto req = R"(
                USE plato;
                ALTER ASYNC REPLICATION MyReplication SET (CONSISTENCY_LEVEL = "GLOBAL");
            )";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:80: Error: CONSISTENCY_LEVEL is not supported in ALTER\n");
    }
    {
        auto req = R"(
                USE plato;
                ALTER ASYNC REPLICATION MyReplication SET (COMMIT_INTERVAL = Interval("PT10S"));
            )";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:87: Error: COMMIT_INTERVAL is not supported in ALTER\n");
    }
    {
        static const TVector<std::pair<TString, TString>> MutuallyExclusiveSettings = {
            {"TOKEN_SECRET_NAME", "TOKEN_SECRET_PATH"},
            {"PASSWORD_SECRET_NAME", "PASSWORD_SECRET_PATH"},
            {"INITIAL_TOKEN_SECRET_NAME", "INITIAL_TOKEN_SECRET_PATH"},
        };

        for (const auto& [name, path] : MutuallyExclusiveSettings) {
            const auto res = SqlToYql(std::format(
                R"sql(
                    USE plato;
                    ALTER ASYNC REPLICATION MyReplication
                    SET (
                        {} = "",
                        {} = ""
                    )
                )sql",
                name.c_str(),
                path.c_str()));
            UNIT_ASSERT(!res.IsOk());
            UNIT_ASSERT_NO_DIFF(
                Err2Str(res),
                std::format(
                    "<main>:6:{}: Error: {} and {} are mutually exclusive\n",
                    28 + name.size(), name.c_str(), path.c_str()));
        }
    }
}

Y_UNIT_TEST(AsyncReplicationInvalidSettings) {
    auto req = R"(
            USE plato;
            ALTER ASYNC REPLICATION MyReplication SET (FOO = "BAR");
        )";
    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:62: Error: Unknown replication setting: FOO\n");
}

Y_UNIT_TEST(DropAsyncReplicationParseCorrect) {
    auto req = R"(
            USE plato;
            DROP ASYNC REPLICATION MyReplication;
        )";
    auto res = SqlToYql(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("MyReplication"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("drop"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(DropAsyncReplicationCascade) {
    auto req = R"(
            USE plato;
            DROP ASYNC REPLICATION MyReplication CASCADE;
        )";
    auto res = SqlToYql(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("dropCascade"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(PragmaCompactGroupBy) {
    auto req = "PRAGMA CompactGroupBy; SELECT key, COUNT(*) FROM plato.Input GROUP BY key;";
    auto res = SqlToYql(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Aggregate") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'('compact)"));
        }
    };

    TWordCountHive elementStat = {{TString("Aggregate"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Aggregate"]);
}

Y_UNIT_TEST(PragmaDisableCompactGroupBy) {
    auto req = "PRAGMA DisableCompactGroupBy; SELECT key, COUNT(*) FROM plato.Input GROUP /*+ compact() */ BY key;";
    auto res = SqlToYql(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Aggregate") {
            UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("'('compact)"));
        }
    };

    TWordCountHive elementStat = {{TString("Aggregate"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Aggregate"]);
}

Y_UNIT_TEST(AutoSampleWorksWithNamedSubquery) {
    UNIT_ASSERT(SqlToYql("$src = select * from plato.Input; select * from $src sample 0.2").IsOk());
}

Y_UNIT_TEST(AutoSampleWorksWithSubquery) {
    UNIT_ASSERT(SqlToYql("select * from (select * from plato.Input) sample 0.2").IsOk());
}

Y_UNIT_TEST(LinearAsColumnOrType) {
    UNIT_ASSERT(SqlToYql("select FormatType(Linear<Int32>)").IsOk());
    UNIT_ASSERT(SqlToYql("select Linear<2 from (select 1 as Linear)").IsOk());
    UNIT_ASSERT(SqlToYql("select FormatType(DynamicLinear<Int32>)").IsOk());
    UNIT_ASSERT(SqlToYql("select DynamicLinear<2 from (select 1 as DynamicLinear)").IsOk());
}

Y_UNIT_TEST(CreateTableTrailingComma) {
    UNIT_ASSERT(SqlToYql("USE ydb; CREATE TABLE tableName (Key Uint32, PRIMARY KEY (Key),);").IsOk());
    UNIT_ASSERT(SqlToYql("USE ydb; CREATE TABLE tableName (Key Uint32,);").IsOk());
    UNIT_ASSERT(SqlToYql(R"sql(USE ydb; CREATE TABLE tableName (Key Uint32) WITH (STORE = COLUMN,);)sql").IsOk());
}

Y_UNIT_TEST(BetweenSymmetric) {
    UNIT_ASSERT(SqlToYql("select 3 between symmetric 5 and 4;").IsOk());
    UNIT_ASSERT(SqlToYql("select 3 between asymmetric 5 and 4;").IsOk());
    UNIT_ASSERT(SqlToYql("use plato; select key between symmetric and and and from Input;").IsOk());
    UNIT_ASSERT(SqlToYql("use plato; select key between and and and from Input;").IsOk());
}

Y_UNIT_TEST(CreateSecret) {
    UNIT_ASSERT(SqlToYql(R"sql(
                USE plato;
                CREATE SECRET `secret-name` WITH (VALUE = "secret-value");
            )sql")
                    .IsOk());
    UNIT_ASSERT(SqlToYql(R"sql(
                USE plato;
                CREATE SECRET `secret-name` WITH (VALUE = "");
            )sql")
                    .IsOk());
    UNIT_ASSERT(SqlToYql(R"sql(
                USE plato;
                PRAGMA TablePathPrefix = "/PathPrefix";
                CREATE SECRET `secret-name` WITH (VALUE = "secret-value");
            )sql")
                    .IsOk());
    UNIT_ASSERT(SqlToYql(R"sql(
                USE plato;
                CREATE SECRET `secret-name` WITH (VALUE = "secret-value", INHERIT_PERMISSIONS = FALSE);
            )sql")
                    .IsOk());
    UNIT_ASSERT(SqlToYql(R"sql(
                USE plato;
                CREATE SECRET `secret-name` WITH (INHERIT_PERMISSIONS = TRUE, VALUE = "secret-value");
            )sql")
                    .IsOk());
}

Y_UNIT_TEST(CreateSecretWithExpressionCorrect) {
    const auto res = SqlToYql(R"sql(
                USE plato;
                DECLARE $foo AS String;
                CREATE SECRET `secret-name` WITH (VALUE = $foo);
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "Key '('secret");
            UNIT_ASSERT_STRING_CONTAINS(line, "'mode 'create");
            UNIT_ASSERT_STRING_CONTAINS(line, "secret-name");
            UNIT_ASSERT_STRING_CONTAINS(line, R"('"value_expr" (EvaluateExpr "$foo"))");
            UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("inherit_permissions"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateSecretCorrect) {
    { // basic case: some value, no other params are set
        auto res = SqlToYql(R"sql(
                USE plato;
                CREATE SECRET `secret-name` WITH (VALUE = "secret-value");
            )sql");
        UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_STRING_CONTAINS(line, "Key '('secret");
                UNIT_ASSERT_STRING_CONTAINS(line, "'mode 'create");
                UNIT_ASSERT_STRING_CONTAINS(line, "secret-name");
                UNIT_ASSERT_STRING_CONTAINS(line, R"("value" '"secret-value")");
                UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find(R"("value_expr")"));
                UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("inherit_permissions"));
            }
        };

        TWordCountHive elementStat = {{TString("Write"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }
    { // empty value; inherit_permissions is set to True
        auto res = SqlToYql(R"sql(
                USE plato;
                CREATE SECRET `secret-name` WITH (VALUE = "", INHERIT_PERMISSIONS = TRUE);
            )sql");
        UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_STRING_CONTAINS(line, "Key '('secret");
                UNIT_ASSERT_STRING_CONTAINS(line, "'mode 'create");
                UNIT_ASSERT_STRING_CONTAINS(line, "secret-name");
                UNIT_ASSERT_STRING_CONTAINS(line, R"("inherit_permissions" '"1")");
            }
        };

        TWordCountHive elementStat = {{TString("Write"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }
    { // inherit_permissions is set explicitly to its default value
        auto res = SqlToYql(R"sql(
                USE plato;
                PRAGMA TablePathPrefix = "/PathPrefix";
                CREATE SECRET `secret-name` WITH (VALUE = "secret-value", INHERIT_PERMISSIONS = FALSE);
            )sql");
        UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_STRING_CONTAINS(line, "Key '('secret");
                UNIT_ASSERT_STRING_CONTAINS(line, "'mode 'create");
                UNIT_ASSERT_STRING_CONTAINS(line, "/PathPrefix/secret-name");
                UNIT_ASSERT_STRING_CONTAINS(line, R"("value" '"secret-value")");
                UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find(R"("value_expr")"));
                UNIT_ASSERT_STRING_CONTAINS(line, R"("inherit_permissions" '"0")");
            }
        };

        TWordCountHive elementStat = {{TString("Write"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }
}

Y_UNIT_TEST(CreateSecretWithExpressionOk) {
    { // Named node on other named nodes
        const auto res = SqlToYql(R"sql(
                USE plato;
                DECLARE $sec1 AS String;
                DECLARE $sec2 AS String;
                CREATE SECRET `secret-name` WITH (VALUE = $sec1 || $sec2);
            )sql");
        UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    }

    { // Named node on pure inline subquery: $x = (SELECT 1); value = $x
        auto res = SqlToYql(R"sql(
                USE plato;
                $x = (SELECT 1);
                CREATE SECRET `x` WITH (VALUE = $x);
            )sql");
        UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    }

    { // Named node on effectful inline subquery: $x = (SELECT Max(value) FROM secrets); value = $x
        auto res = SqlToYql(R"sql(
                USE plato;
                $x = (SELECT Max(value) FROM secrets);
                CREATE SECRET `x` WITH (VALUE = $x);
            )sql");
        UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    }

    { // Pure inline subquery: value = (SELECT 1)
        NSQLTranslation::TTranslationSettings settings;
        settings.LangVer = NYql::NFeature::InlineSubquery.MinLangVer;
        auto res = SqlToYqlWithSettings(R"sql(
                USE plato;
                CREATE SECRET `x` WITH (VALUE = (SELECT 1));
            )sql", settings);
        UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    }

    { // Effectful inline subquery: value = (SELECT Max(value) FROM secrets)
        NSQLTranslation::TTranslationSettings settings;
        settings.LangVer = NYql::NFeature::InlineSubquery.MinLangVer;
        auto res = SqlToYqlWithSettings(R"sql(
                USE plato;
                CREATE SECRET `x` WITH (VALUE = (SELECT Max(value) FROM secrets));
            )sql", settings);
        UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    }
}

Y_UNIT_TEST(CreateSecretWithExpressionFail) {
    { // Named node on literal: $x = 1; value = $x
        auto res = SqlToYql(R"sql(
            USE plato;
            $x = 1;
            CREATE SECRET `x` WITH (VALUE = $x);
        )sql");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:4:45: Error: Unsupported type for parameter: VALUE. String was expected\n");
    }

    { // Named node on named node: $y = 1; $x = $y; value = $x
        auto res = SqlToYql(R"sql(
            USE plato;
            $y = 1;
            $x = $y;
            CREATE SECRET `x` WITH (VALUE = $x);
        )sql");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:5:45: Error: Unsupported type for parameter: VALUE. String was expected\n");
    }
}

Y_UNIT_TEST(CreateSecretIncorrect) {
    { // no value
        NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            CREATE SECRET `secret-name` WITH (INHERIT_PERMISSIONS = FALSE);
        )sql");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:13: Error: Parameter VALUE must be set\n");
    }
    { // value is not a string
        NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            CREATE SECRET `secret-name` WITH (VALUE = true);
        )sql");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:55: Error: Unsupported type for parameter: VALUE. String was expected\n");
    }
    { // value is set twice
        NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            CREATE SECRET `secret-name` WITH (VALUE = "value1", VALUE = "value2");
        )sql");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:65: Error: Duplicate parameter: VALUE\n");
    }
    { // value is set twice, once via expression
        NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            DECLARE $foo AS String;
            CREATE SECRET `secret-name` WITH (VALUE = "value1", VALUE = $foo);
        )sql");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:4:65: Error: Duplicate parameter: VALUE\n");
    }
    { // inherit_permissions is set twice
        NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            CREATE SECRET `secret-name` WITH (INHERIT_PERMISSIONS = FALSE, INHERIT_PERMISSIONS = TRUE);
        )sql");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:76: Error: Duplicate parameter: INHERIT_PERMISSIONS\n");
    }
    { // inherit_permissions is not bool
        NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            CREATE SECRET `secret-name` WITH (INHERIT_PERMISSIONS = "TRUE");
        )sql");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:69: Error: Unsupported value for parameter: INHERIT_PERMISSIONS. Bool was expected\n");
    }
    { // unknown parameter
        NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            CREATE SECRET `secret-name` WITH (VALUE = "secret-value", ABC = "abc");
        )sql");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:71: Error: Unknown parameter: ABC\n");
    }
    { // temporal object in secret name
        NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            CREATE SECRET @tmp WITH (VALUE = "abc");
        )sql");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:28: Error: '@' is not allowed prefix for secret name\n");
    }
    { // empty secret name
        NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            CREATE SECRET `` WITH (INHERIT_PERMISSIONS = FALSE);
        )sql");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:27: Error: Empty secret name\n");
    }
}

Y_UNIT_TEST(AlterSecret) {
    UNIT_ASSERT(SqlToYql(R"sql(
                USE ydb;
                ALTER SECRET `secret-name` WITH (VALUE = "secret-value");
            )sql")
                    .IsOk());
    UNIT_ASSERT(SqlToYql(R"sql(
                USE ydb;
                ALTER SECRET `secret-name` WITH (VALUE = "");
            )sql")
                    .IsOk());
    UNIT_ASSERT(SqlToYql(R"sql(
                USE plato;
                PRAGMA TablePathPrefix = "/PathPrefix";
                ALTER SECRET `secret-name` WITH (VALUE = "secret-value");
            )sql")
                    .IsOk());
}

Y_UNIT_TEST(AlterSecretWithExpressionCorrect) {
    const auto res = SqlToYql(R"sql(
                USE plato;
                DECLARE $foo AS String;
                ALTER SECRET `secret-name` WITH (VALUE = $foo);
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "Key '('secret");
            UNIT_ASSERT_STRING_CONTAINS(line, "'mode 'alter");
            UNIT_ASSERT_STRING_CONTAINS(line, "secret-name");
            UNIT_ASSERT_STRING_CONTAINS(line, R"('"value_expr" (EvaluateExpr "$foo"))");
            UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find(R"("value")"));
            UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("inherit_permissions"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(AlterSecretCorrect) {
    auto res = SqlToYql(R"sql(
                USE ydb;
                ALTER SECRET `secret-name` WITH (VALUE = "secret-value");
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "Key '('secret");
            UNIT_ASSERT_STRING_CONTAINS(line, "'mode 'alter");
            UNIT_ASSERT_STRING_CONTAINS(line, "secret-name");
            UNIT_ASSERT_STRING_CONTAINS(line, R"("value" '"secret-value")");
            UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find(R"("value_expr")"));
            UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("inherit_permissions"));
        }
    };
}

Y_UNIT_TEST(AlterSecretWithExpression) {
    const auto res = SqlToYql(R"sql(
                USE plato;
                DECLARE $sec1 AS String;
                DECLARE $sec2 AS String;
                ALTER SECRET `secret-name` WITH (VALUE = $sec1 || $sec2);
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(AlterSecretIncorrect) {
    { // no value
        NYql::TAstParseResult res = SqlToYql(R"sql(
            USE ydb;
            ALTER SECRET `secret-name`;
        )sql");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:38: Error: mismatched input ';' expecting WITH\n");
    }
    { // value is not a string
        NYql::TAstParseResult res = SqlToYql(R"sql(
            USE ydb;
            ALTER SECRET `secret-name` WITH (VALUE = true);
        )sql");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:54: Error: Unsupported type for parameter: VALUE. String was expected\n");
    }
    { // value is set twice
        NYql::TAstParseResult res = SqlToYql(R"sql(
            USE ydb;
            ALTER SECRET `secret-name` WITH (VALUE = "value1", VALUE = "value2");
        )sql");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:64: Error: Duplicate parameter: VALUE\n");
    }
    { // inherit_permissions is set
        NYql::TAstParseResult res = SqlToYql(R"sql(
            USE ydb;
            ALTER SECRET `secret-name` WITH (VALUE = "value", INHERIT_PERMISSIONS = FALSE);
        )sql");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:13: Error: parameter INHERIT_PERMISSIONS is not supported for alter operation\n");
    }
    { // unknown parameter
        NYql::TAstParseResult res = SqlToYql(R"sql(
            USE ydb;
            ALTER SECRET `secret-name` WITH (VALUE = "secret-value", abc = "abc");
        )sql");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:70: Error: Unknown parameter: ABC\n");
    }
    { // temporal object in secret name
        NYql::TAstParseResult res = SqlToYql(R"sql(
            USE ydb;
            ALTER SECRET @tmp WITH (VALUE = "abc");
        )sql");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:27: Error: '@' is not allowed prefix for secret name\n");
    }
}

Y_UNIT_TEST(DropSecret) {
    UNIT_ASSERT(SqlToYql(R"sql(
                USE plato;
                DROP SECRET `secret-name`;
            )sql")
                    .IsOk());
    UNIT_ASSERT(SqlToYql(R"sql(
                USE plato;
                PRAGMA TablePathPrefix = "/PathPrefix";
                DROP SECRET `secret-name`;
            )sql")
                    .IsOk());
}

Y_UNIT_TEST(DropSecretCorrect) {
    auto res = SqlToYql(R"sql(
                USE plato;
                DROP SECRET `secret-name`;
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "Key '('secret");
            UNIT_ASSERT_STRING_CONTAINS(line, "'mode 'drop");
            UNIT_ASSERT_STRING_CONTAINS(line, "secret-name");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(DropSecretIncorrect) {
    {
        NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            DROP SECRET `secret-name` WITH (VALUE = "abc");
        )sql");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:38: Error: extraneous input 'WITH' expecting {<EOF>, ';'}\n");
    }
    {
        NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            DROP SECRET SECRET `secret-name`;
        )sql");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:31: Error: extraneous input '`secret-name`' expecting {<EOF>, ';'}\n");
    }
    { // temporal object in secret name
        NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            DROP SECRET @tmp;
        )sql");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:26: Error: '@' is not allowed prefix for secret name\n");
    }
}

Y_UNIT_TEST(CreateSecretIfNotExists) {
    UNIT_ASSERT(SqlToYql(R"sql(
            USE plato;
            CREATE SECRET IF NOT EXISTS `secret-name` WITH (VALUE = "secret-value");
        )sql")
                    .IsOk());
}

Y_UNIT_TEST(CreateSecretIfNotExistsCorrect) {
    auto res = SqlToYql(R"sql(
            USE plato;
            CREATE SECRET IF NOT EXISTS `secret-name` WITH (VALUE = "secret-value");
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "Key '('secret");
            UNIT_ASSERT_STRING_CONTAINS(line, "'mode 'create_if_not_exists");
            UNIT_ASSERT_STRING_CONTAINS(line, "secret-name");
            UNIT_ASSERT_STRING_CONTAINS(line, R"("value" '"secret-value")");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateSecretOrReplace) {
    UNIT_ASSERT(SqlToYql(R"sql(
            USE plato;
            CREATE OR REPLACE SECRET `secret-name` WITH (VALUE = "secret-value");
        )sql")
                    .IsOk());
}

Y_UNIT_TEST(CreateSecretOrReplaceCorrect) {
    auto res = SqlToYql(R"sql(
            USE plato;
            CREATE OR REPLACE SECRET `secret-name` WITH (VALUE = "secret-value");
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "Key '('secret");
            UNIT_ASSERT_STRING_CONTAINS(line, "'mode 'create_or_replace");
            UNIT_ASSERT_STRING_CONTAINS(line, "secret-name");
            UNIT_ASSERT_STRING_CONTAINS(line, R"("value" '"secret-value")");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateSecretOrReplaceWithInheritPermissions) {
    auto res = SqlToYql(R"sql(
            USE plato;
            CREATE OR REPLACE SECRET `secret-name` WITH (VALUE = "secret-value", INHERIT_PERMISSIONS = TRUE);
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "'mode 'create_or_replace");
            UNIT_ASSERT_STRING_CONTAINS(line, R"("inherit_permissions" '"1")");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateSecretIfNotExistsWithExpression) {
    const auto res = SqlToYql(R"sql(
            USE plato;
            DECLARE $foo AS String;
            CREATE SECRET IF NOT EXISTS `secret-name` WITH (VALUE = $foo);
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "'mode 'create_if_not_exists");
            UNIT_ASSERT_STRING_CONTAINS(line, R"('"value_expr" (EvaluateExpr "$foo"))");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateSecretOrReplaceWithExpression) {
    const auto res = SqlToYql(R"sql(
            USE plato;
            DECLARE $foo AS String;
            CREATE OR REPLACE SECRET `secret-name` WITH (VALUE = $foo);
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "'mode 'create_or_replace");
            UNIT_ASSERT_STRING_CONTAINS(line, R"('"value_expr" (EvaluateExpr "$foo"))");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(AlterSecretIfExists) {
    UNIT_ASSERT(SqlToYql(R"sql(
            USE ydb;
            ALTER SECRET IF EXISTS `secret-name` WITH (VALUE = "secret-value");
        )sql")
                    .IsOk());
}

Y_UNIT_TEST(AlterSecretIfExistsCorrect) {
    auto res = SqlToYql(R"sql(
            USE ydb;
            ALTER SECRET IF EXISTS `secret-name` WITH (VALUE = "secret-value");
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "Key '('secret");
            UNIT_ASSERT_STRING_CONTAINS(line, "'mode 'alter_if_exists");
            UNIT_ASSERT_STRING_CONTAINS(line, "secret-name");
            UNIT_ASSERT_STRING_CONTAINS(line, R"("value" '"secret-value")");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(AlterSecretIfExistsWithExpression) {
    const auto res = SqlToYql(R"sql(
            USE plato;
            DECLARE $foo AS String;
            ALTER SECRET IF EXISTS `secret-name` WITH (VALUE = $foo);
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "'mode 'alter_if_exists");
            UNIT_ASSERT_STRING_CONTAINS(line, R"('"value_expr" (EvaluateExpr "$foo"))");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(DropSecretIfExists) {
    UNIT_ASSERT(SqlToYql(R"sql(
            USE plato;
            DROP SECRET IF EXISTS `secret-name`;
        )sql")
                    .IsOk());
}

Y_UNIT_TEST(DropSecretIfExistsCorrect) {
    auto res = SqlToYql(R"sql(
            USE plato;
            DROP SECRET IF EXISTS `secret-name`;
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "Key '('secret");
            UNIT_ASSERT_STRING_CONTAINS(line, "'mode 'drop_if_exists");
            UNIT_ASSERT_STRING_CONTAINS(line, "secret-name");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(DropSecretIfExistsWithTablePathPrefix) {
    UNIT_ASSERT(SqlToYql(R"sql(
            USE plato;
            PRAGMA TablePathPrefix = "/PathPrefix";
            DROP SECRET IF EXISTS `secret-name`;
        )sql")
                    .IsOk());
}

Y_UNIT_TEST(CreateSecretOrReplaceAndIfNotExists) {
    // Both OR REPLACE and IF NOT EXISTS are allowed together; OR REPLACE takes priority
    auto res = SqlToYql(R"sql(
            USE plato;
            CREATE OR REPLACE SECRET IF NOT EXISTS `secret-name` WITH (VALUE = "secret-value");
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "'mode 'create_or_replace");
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateSecretIfNotExistsIncorrect) {
    { // no value
        NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            CREATE SECRET IF NOT EXISTS `secret-name` WITH (INHERIT_PERMISSIONS = FALSE);
        )sql");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:13: Error: Parameter VALUE must be set\n");
    }
    { // temporal object in secret name
        NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            CREATE SECRET IF NOT EXISTS @tmp WITH (VALUE = "abc");
        )sql");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:42: Error: '@' is not allowed prefix for secret name\n");
    }
    { // empty secret name
        NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            CREATE SECRET IF NOT EXISTS `` WITH (INHERIT_PERMISSIONS = FALSE);
        )sql");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:41: Error: Empty secret name\n");
    }
}

Y_UNIT_TEST(CreateSecretOrReplaceIncorrect) {
    { // no value
        NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            CREATE OR REPLACE SECRET `secret-name` WITH (INHERIT_PERMISSIONS = FALSE);
        )sql");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:13: Error: Parameter VALUE must be set\n");
    }
    { // value is not a string
        NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            CREATE OR REPLACE SECRET `secret-name` WITH (VALUE = true);
        )sql");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:66: Error: Unsupported type for parameter: VALUE. String was expected\n");
    }
    { // temporal object in secret name
        NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            CREATE OR REPLACE SECRET @tmp WITH (VALUE = "abc");
        )sql");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:39: Error: '@' is not allowed prefix for secret name\n");
    }
}

Y_UNIT_TEST(AlterSecretIfExistsIncorrect) {
    { // no value
        NYql::TAstParseResult res = SqlToYql(R"sql(
            USE ydb;
            ALTER SECRET IF EXISTS `secret-name`;
        )sql");
        UNIT_ASSERT(!res.IsOk());
#if ANTLR_VER == 3
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:48: Error: Unexpected token ';' : syntax error...\n\n");
#else
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:48: Error: mismatched input ';' expecting WITH\n");
#endif
    }
    { // inherit_permissions is set
        NYql::TAstParseResult res = SqlToYql(R"sql(
            USE ydb;
            ALTER SECRET IF EXISTS `secret-name` WITH (VALUE = "value", INHERIT_PERMISSIONS = FALSE);
        )sql");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:13: Error: parameter INHERIT_PERMISSIONS is not supported for alter operation\n");
    }
    { // temporal object in secret name
        NYql::TAstParseResult res = SqlToYql(R"sql(
            USE ydb;
            ALTER SECRET IF EXISTS @tmp WITH (VALUE = "abc");
        )sql");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:37: Error: '@' is not allowed prefix for secret name\n");
    }
}

Y_UNIT_TEST(DropSecretIfExistsIncorrect) {
    { // temporal object in secret name
        NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            DROP SECRET IF EXISTS @tmp;
        )sql");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:36: Error: '@' is not allowed prefix for secret name\n");
    }
    { // WITH is not allowed for drop
        NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            DROP SECRET IF EXISTS `secret-name` WITH (VALUE = "abc");
        )sql");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:48: Error: extraneous input 'WITH' expecting {<EOF>, ';'}\n");
    }
}

} // Y_UNIT_TEST_SUITE(SqlParsingOnly)

#include "sql_ut.h"
#include "format/sql_format.h"
#include "lexer/lexer.h"

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/sql/sql.h>
#include <util/generic/map.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/split.h>

#include <format>

using namespace NSQLTranslation;

namespace {

TParsedTokenList Tokenize(const TString& query) {
    auto lexer = NSQLTranslationV1::MakeLexer(true);
    TParsedTokenList tokens;
    NYql::TIssues issues;
    UNIT_ASSERT_C(Tokenize(*lexer, query, "Query", tokens, issues, SQL_MAX_PARSER_ERRORS),
                  issues.ToString());

    return tokens;
}

TString ToString(const TParsedTokenList& tokens) {
    TStringBuilder reconstructedQuery;
    for (const auto& token : tokens) {
        if (token.Name == "WS" || token.Name == "EOF") {
            continue;
        }
        if (!reconstructedQuery.Empty()) {
            reconstructedQuery << ' ';
        }
        reconstructedQuery << token.Content;
    }
    return reconstructedQuery;
}

}

Y_UNIT_TEST_SUITE(AnsiMode) {
    Y_UNIT_TEST(PragmaAnsi) {
        UNIT_ASSERT(SqlToYql("PRAGMA ANSI 2016;").IsOk());
    }
}

Y_UNIT_TEST_SUITE(SqlParsingOnly) {
    ///This function is used in BACKWARD COMPATIBILITY tests below that LIMIT the sets of token that CAN NOT be used
    ///as identifiers in different contexts in a SQL request
    ///\return list of tokens that failed this check
    TVector<TString> ValidateTokens(const THashSet<TString>& forbidden, const std::function<TString (const TString& )>& makeRequest) {
        THashMap<TString, bool> allTokens;
        for (const auto& t: NSQLFormat::GetKeywords()) {
            allTokens[t] = !forbidden.contains((t));
        }
        for (const auto& f: forbidden) {
            UNIT_ASSERT(allTokens.contains(f)); //check that forbidden list contains tokens only(argument check)
        }
        TVector<TString> failed;
        for (const auto& [token, allowed]: allTokens) {
            if (SqlToYql(makeRequest(token)).IsOk() != allowed)
               failed.push_back(token);
        }
        return failed;
    }

    Y_UNIT_TEST(TokensAsColumnName) { //id_expr
        auto failed = ValidateTokens({
                "ALL", "ANY", "AS", "ASSUME", "ASYMMETRIC", "AUTOMAP", "BETWEEN", "BITCAST",
                "CALLABLE", "CASE", "CAST", "CUBE", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
                "DICT", "DISTINCT", "ENUM", "ERASE", "EXCEPT", "EXISTS", "FLOW", "FROM", "FULL", "GLOBAL",
                "HAVING", "HOP", "INTERSECT", "JSON_EXISTS", "JSON_QUERY", "JSON_VALUE", "LIMIT", "LIST", "LOCAL",
                "NOT", "OPTIONAL", "PROCESS", "REDUCE", "REPEATABLE", "RESOURCE", "RETURN", "RETURNING", "ROLLUP",
                "SELECT", "SET", "STREAM", "STRUCT", "SYMMETRIC", "TAGGED", "TUPLE", "UNBOUNDED",
                "UNION", "VARIANT", "WHEN", "WHERE", "WINDOW", "WITHOUT"
            },
            [](const TString& token){
                TStringBuilder req;
                req << "SELECT " << token << " FROM Plato.Input";
                return req;
            }
        );
        UNIT_ASSERT_VALUES_EQUAL(failed, TVector<TString>{});
    }

    Y_UNIT_TEST(TokensAsWithoutColumnName) { //id_without
        auto failed = ValidateTokens({
                "ALL", "AS", "ASSUME", "ASYMMETRIC", "AUTOMAP", "BETWEEN", "BITCAST",
                "CALLABLE", "CASE", "CAST", "CUBE", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
                "DICT", "DISTINCT", "EMPTY_ACTION", "ENUM", "EXCEPT", "EXISTS", "FALSE", "FLOW", "FROM", "FULL", "GLOBAL",
                "HAVING", "HOP", "INTERSECT", "JSON_EXISTS", "JSON_QUERY", "JSON_VALUE", "LIMIT", "LIST", "LOCAL",
                "NOT", "NULL", "OPTIONAL", "PROCESS", "REDUCE", "REPEATABLE", "RESOURCE", "RETURN", "RETURNING", "ROLLUP",
                "SELECT", "SET", "STRUCT", "SYMMETRIC", "TAGGED", "TRUE", "TUPLE", "UNBOUNDED",
                "UNION", "VARIANT", "WHEN", "WHERE", "WINDOW", "WITHOUT"
             },
             [](const TString& token){
                 TStringBuilder req;
                 req << "SELECT * WITHOUT " << token << " FROM Plato.Input";
                 return req;
             }
        );
        UNIT_ASSERT_VALUES_EQUAL(failed, TVector<TString>{});
    }

    Y_UNIT_TEST(TokensAsColumnNameInAddColumn) { //id_schema
        auto failed = ValidateTokens({
                "ANY", "AUTOMAP", "CALLABLE", "COLUMN", "DICT", "ENUM", "ERASE", "FALSE", "FLOW",
                "GLOBAL", "LIST", "OPTIONAL", "REPEATABLE", "RESOURCE",
                "SET", "STREAM", "STRUCT", "TAGGED", "TRUE", "TUPLE", "VARIANT"
            },
            [](const TString& token){
                 TStringBuilder req;
                 req << "ALTER TABLE Plato.Input ADD COLUMN " << token << " Bool";
                 return req;
            }
        );
        UNIT_ASSERT_VALUES_EQUAL(failed, TVector<TString>{});
    }

    Y_UNIT_TEST(TokensAsColumnAlias) {
        auto failed = ValidateTokens({
                 "AUTOMAP", "FALSE",
                 "GLOBAL", "REPEATABLE", "TRUE"
             },
             [](const TString& token){
                 TStringBuilder req;
                 req << "SELECT Col as " << token << " FROM Plato.Input";
                 return req;
             }
        );
        UNIT_ASSERT_VALUES_EQUAL(failed, TVector<TString>{});
    }

    Y_UNIT_TEST(TokensAsTableName) { //id_table_or_type
        auto failed = ValidateTokens({
                "ANY", "AUTOMAP", "COLUMN", "ERASE", "FALSE",
                "GLOBAL", "REPEATABLE", "STREAM", "TRUE"
            },
            [](const TString& token){
                TStringBuilder req;
                req << "SELECT * FROM Plato." << token;
                return req;
            }
        );
        UNIT_ASSERT_VALUES_EQUAL(failed, TVector<TString>{});
    }

    Y_UNIT_TEST(TokensAsTableAlias) { //id_table
        auto failed = ValidateTokens({
                "AUTOMAP", "CALLABLE", "DICT", "ENUM","FALSE", "FLOW",
                "GLOBAL", "LIST", "OPTIONAL", "REPEATABLE", "RESOURCE",
                "SET", "STRUCT", "TAGGED", "TRUE", "TUPLE", "VARIANT"
            },
            [](const TString& token){
                 TStringBuilder req;
                 req << "SELECT * FROM Plato.Input AS " << token;
                 return req;
            }
        );
        UNIT_ASSERT_VALUES_EQUAL(failed, TVector<TString>{});
    }

    Y_UNIT_TEST(TokensAsHints) { //id_hint
        auto failed = ValidateTokens({
                "AUTOMAP", "CALLABLE", "COLUMNS", "DICT", "ENUM", "FALSE", "FLOW",
                "GLOBAL", "LIST", "OPTIONAL", "REPEATABLE", "RESOURCE",
                "SCHEMA", "SET", "STRUCT", "TAGGED", "TRUE", "TUPLE", "VARIANT"
            },
            [](const TString& token){
                TStringBuilder req;
                req << "SELECT * FROM Plato.Input WITH " << token;
                return req;
            }
        );
        UNIT_ASSERT_VALUES_EQUAL(failed, TVector<TString>{});
    }

    Y_UNIT_TEST(TokensAsWindow) { //id_window
        auto failed = ValidateTokens({
                "AUTOMAP", "CALLABLE", "DICT", "ENUM", "FALSE", "FLOW", "GLOBAL", "GROUPS", "LIST", "OPTIONAL",
                "RANGE", "REPEATABLE", "RESOURCE", "ROWS", "SET", "STRUCT", "TAGGED" ,"TRUE", "TUPLE", "VARIANT"
            },
            [](const TString& token){
                TStringBuilder req;
                req << "SELECT * FROM Plato.Input WINDOW " << token << " AS ()";
                return req;
            }
        );
        UNIT_ASSERT_VALUES_EQUAL(failed, TVector<TString>{});
    }

    Y_UNIT_TEST(TokensAsIdExprIn) { //id_expr_in
        auto failed = ValidateTokens({
                "ALL", "ANY", "AS", "ASSUME", "ASYMMETRIC", "AUTOMAP", "BETWEEN", "BITCAST",
                "CALLABLE", "CASE", "CAST", "COMPACT", "CUBE", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
                "DICT", "DISTINCT", "ENUM", "ERASE", "EXCEPT", "EXISTS", "FLOW", "FROM", "FULL", "GLOBAL",
                "HAVING", "HOP", "INTERSECT", "JSON_EXISTS", "JSON_QUERY", "JSON_VALUE", "LIMIT", "LIST", "LOCAL",
                "NOT", "OPTIONAL", "PROCESS", "REDUCE", "REPEATABLE", "RESOURCE", "RETURN", "RETURNING", "ROLLUP",
                "SELECT", "SET", "STREAM", "STRUCT", "SYMMETRIC", "TAGGED", "TUPLE", "UNBOUNDED",
                "UNION", "VARIANT", "WHEN", "WHERE", "WINDOW", "WITHOUT"
            },
            [](const TString& token){
                TStringBuilder req;
                req << "SELECT * FROM Plato.Input WHERE q IN " << token;
                return req;
            }
        );
        UNIT_ASSERT_VALUES_EQUAL(failed, TVector<TString>{});
    }

    Y_UNIT_TEST(TableHints) {
        UNIT_ASSERT(SqlToYql("SELECT * FROM plato.Input WITH INFER_SCHEMA").IsOk());
        UNIT_ASSERT(SqlToYql("SELECT * FROM plato.Input WITH (INFER_SCHEMA)").IsOk());
    }

    Y_UNIT_TEST(InNoHints) {
        TString query = "SELECT * FROM plato.Input WHERE key IN (1,2,3)";

        VerifySqlInHints(query, { "'('('warnNoAnsi))" }, {});
        VerifySqlInHints(query, { "'()" }, false);
        VerifySqlInHints(query, { "'('('ansi))" }, true);
    }

    Y_UNIT_TEST(InHintCompact) {
        // should parse COMPACT as hint
        TString query = "SELECT * FROM plato.Input WHERE key IN COMPACT(1, 2, 3)";

        VerifySqlInHints(query, { "'('isCompact)" });
    }

    Y_UNIT_TEST(InHintSubquery) {
        // should parse tableSource as hint
        TString query = "$subq = (SELECT key FROM plato.Input); SELECT * FROM plato.Input WHERE key IN $subq";

        VerifySqlInHints(query, { "'('tableSource)" });
    }

    Y_UNIT_TEST(InHintCompactSubquery) {
        TString query = "$subq = (SELECT key FROM plato.Input); SELECT * FROM plato.Input WHERE key IN COMPACT $subq";

        VerifySqlInHints(query, { "'('isCompact)", "'('tableSource)" });
    }

    Y_UNIT_TEST(CompactKeywordNotReservedForNames) {
        UNIT_ASSERT(SqlToYql("SELECT COMPACT FROM plato.Input WHERE COMPACT IN COMPACT(1, 2, 3)").IsOk());
        UNIT_ASSERT(SqlToYql("USE plato; SELECT * FROM COMPACT").IsOk());
    }

    Y_UNIT_TEST(FamilyKeywordNotReservedForNames) {
        // FIXME: check if we can get old behaviour
        //UNIT_ASSERT(SqlToYql("USE plato; CREATE TABLE FAMILY (FAMILY Uint32, PRIMARY KEY (FAMILY));").IsOk());
        //UNIT_ASSERT(SqlToYql("USE plato; SELECT FAMILY FROM FAMILY").IsOk());
        UNIT_ASSERT(SqlToYql("USE plato; SELECT FAMILY FROM Input").IsOk());
    }

    Y_UNIT_TEST(ResetKeywordNotReservedForNames) {
        UNIT_ASSERT(SqlToYql("USE plato; CREATE TABLE RESET (RESET Uint32, PRIMARY KEY (RESET));").IsOk());
        UNIT_ASSERT(SqlToYql("USE plato; SELECT RESET FROM RESET").IsOk());
    }

    Y_UNIT_TEST(SyncKeywordNotReservedForNames) {
        UNIT_ASSERT(SqlToYql("USE plato; CREATE TABLE SYNC (SYNC Uint32, PRIMARY KEY (SYNC));").IsOk());
        UNIT_ASSERT(SqlToYql("USE plato; SELECT SYNC FROM SYNC").IsOk());
    }

    Y_UNIT_TEST(AsyncKeywordNotReservedForNames) {
        UNIT_ASSERT(SqlToYql("USE plato; CREATE TABLE ASYNC (ASYNC Uint32, PRIMARY KEY (ASYNC));").IsOk());
        UNIT_ASSERT(SqlToYql("USE plato; SELECT ASYNC FROM ASYNC").IsOk());
    }

    Y_UNIT_TEST(DisableKeywordNotReservedForNames) {
        UNIT_ASSERT(SqlToYql("USE plato; CREATE TABLE DISABLE (DISABLE Uint32, PRIMARY KEY (DISABLE));").IsOk());
        UNIT_ASSERT(SqlToYql("USE plato; SELECT DISABLE FROM DISABLE").IsOk());
    }

    Y_UNIT_TEST(ChangefeedKeywordNotReservedForNames) {
        UNIT_ASSERT(SqlToYql("USE plato; CREATE TABLE CHANGEFEED (CHANGEFEED Uint32, PRIMARY KEY (CHANGEFEED));").IsOk());
        UNIT_ASSERT(SqlToYql("USE plato; SELECT CHANGEFEED FROM CHANGEFEED").IsOk());
    }

    Y_UNIT_TEST(ReplicationKeywordNotReservedForNames) {
        UNIT_ASSERT(SqlToYql("USE plato; CREATE TABLE REPLICATION (REPLICATION Uint32, PRIMARY KEY (REPLICATION));").IsOk());
        UNIT_ASSERT(SqlToYql("USE plato; SELECT REPLICATION FROM REPLICATION").IsOk());
    }

    Y_UNIT_TEST(SecondsKeywordNotReservedForNames) {
        UNIT_ASSERT(SqlToYql("USE plato; CREATE TABLE SECONDS (SECONDS Uint32, PRIMARY KEY (SECONDS));").IsOk());
        UNIT_ASSERT(SqlToYql("USE plato; SELECT SECONDS FROM SECONDS").IsOk());
    }

    Y_UNIT_TEST(MillisecondsKeywordNotReservedForNames) {
        UNIT_ASSERT(SqlToYql("USE plato; CREATE TABLE MILLISECONDS (MILLISECONDS Uint32, PRIMARY KEY (MILLISECONDS));").IsOk());
        UNIT_ASSERT(SqlToYql("USE plato; SELECT MILLISECONDS FROM MILLISECONDS").IsOk());
    }

    Y_UNIT_TEST(MicrosecondsKeywordNotReservedForNames) {
        UNIT_ASSERT(SqlToYql("USE plato; CREATE TABLE MICROSECONDS (MICROSECONDS Uint32, PRIMARY KEY (MICROSECONDS));").IsOk());
        UNIT_ASSERT(SqlToYql("USE plato; SELECT MICROSECONDS FROM MICROSECONDS").IsOk());
    }

    Y_UNIT_TEST(NanosecondsKeywordNotReservedForNames) {
        UNIT_ASSERT(SqlToYql("USE plato; CREATE TABLE NANOSECONDS (NANOSECONDS Uint32, PRIMARY KEY (NANOSECONDS));").IsOk());
        UNIT_ASSERT(SqlToYql("USE plato; SELECT NANOSECONDS FROM NANOSECONDS").IsOk());
    }

    Y_UNIT_TEST(Jubilee) {
        NYql::TAstParseResult res = SqlToYql("USE plato; INSERT INTO Arcadia (r2000000) VALUES (\"2M GET!!!\");");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(QualifiedAsteriskBefore) {
        NYql::TAstParseResult res = SqlToYql(
            "PRAGMA DisableSimpleColumns;"
            "select interested_table.*, LENGTH(value) AS megahelpful_len  from plato.Input as interested_table;"
        );
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            static bool seenStar = false;
            if (word == "FlattenMembers") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("interested_table."));
            } else if (word == "SqlProjectItem") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("megahelpful_len")));
                UNIT_ASSERT_VALUES_EQUAL(seenStar, true);
            } else if (word == "SqlProjectStarItem") {
                seenStar = true;
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
            "select LENGTH(value) AS megahelpful_len, interested_table.*  from plato.Input as interested_table;"
        );
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            static bool seenStar = false;
            if (word == "FlattenMembers") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("interested_table."));
            } else if (word == "SqlProjectItem") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("megahelpful_len")));
                UNIT_ASSERT_VALUES_EQUAL(seenStar, false);
            } else if (word == "SqlProjectStarItem") {
                seenStar = true;
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
        UNIT_ASSERT(res.Root);

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

    Y_UNIT_TEST(JoinParseCorrect) {
        NYql::TAstParseResult res = SqlToYql(
            "PRAGMA DisableSimpleColumns;"
            " SELECT table_bb.*, table_aa.key as megakey"
            " FROM plato.Input AS table_aa"
            " JOIN plato.Input AS table_bb"
            " ON table_aa.value == table_bb.value;"
        );
        UNIT_ASSERT(res.Root);

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
            " JOIN plato.Input AS table_cc ON table_aa.subkey == table_cc.subkey;"
        );
        Err2Str(res);
        UNIT_ASSERT(res.Root);

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
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_STRINGS_EQUAL(res.Issues.ToString(), "<main>:1:67: Error: Cartesian product of tables is disabled. Please use explicit CROSS JOIN or enable it via PRAGMA AnsiImplicitCrossJoin\n");
    }

    Y_UNIT_TEST(JoinCartesianProduct) {
        NYql::TAstParseResult res = SqlToYql("pragma AnsiImplicitCrossJoin; use plato; select * from A,B,C");
        UNIT_ASSERT(res.Root);
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

    Y_UNIT_TEST(JoinWithoutConcreteColumns) {
        NYql::TAstParseResult res = SqlToYql(
            " use plato;"
            " SELECT a.v, b.value"
            "     FROM `Input1` VIEW `ksv` AS a"
            "     JOIN `Input2` AS b"
            "     ON a.k == b.key;"
        );
        UNIT_ASSERT(res.Root);

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
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "SqlProjectItem") {
                const bool isValueFromA = TString::npos != line.find(Quote("a.value"));
                const bool isValueFromB = TString::npos != line.find(Quote("b.value"));
                UNIT_ASSERT(isValueFromA || isValueFromB);
            } if (word == "Write!") {
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
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(SameColumnsForDifferentTablesFullJoin) {
        NYql::TAstParseResult res = SqlToYql("SELECT a.key, b.key, a.value, b.value FROM plato.Input AS a FULL JOIN plato.Input AS b USING(key);");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(JoinStreamLookupStrategyHint) {
        {
            NYql::TAstParseResult res = SqlToYql("SELECT * FROM plato.Input AS a JOIN /*+ StreamLookup() */ plato.Input AS b USING(key);");
            UNIT_ASSERT(res.Root);
        }
        //case insensitive
        {
            NYql::TAstParseResult res = SqlToYql("SELECT * FROM plato.Input AS a JOIN /*+ streamlookup() */ plato.Input AS b USING(key);");
            UNIT_ASSERT(res.Root);
        }
    }

    Y_UNIT_TEST(JoinConflictingStrategyHint) {
        {
            NYql::TAstParseResult res = SqlToYql("SELECT * FROM plato.Input AS a JOIN /*+ StreamLookup() */ /*+ Merge() */   plato.Input AS b USING(key);");
            UNIT_ASSERT(!res.Root);
            UNIT_ASSERT_STRINGS_EQUAL(res.Issues.ToString(), "<main>:1:91: Error: Conflicting join strategy hints\n");
        }
    }

    Y_UNIT_TEST(JoinDuplicatingStrategyHint) {
        {
            NYql::TAstParseResult res = SqlToYql("SELECT * FROM plato.Input AS a JOIN /*+ StreamLookup() */ /*+ StreamLookup() */   plato.Input AS b USING(key);");
            UNIT_ASSERT(!res.Root);
            UNIT_ASSERT_STRINGS_EQUAL(res.Issues.ToString(), "<main>:1:98: Error: Duplicate join strategy hint\n");
        }
    }

    Y_UNIT_TEST(WarnCrossJoinStrategyHint) {
        NYql::TAstParseResult res = SqlToYql("SELECT * FROM plato.Input AS a CROSS JOIN /*+ merge() */ plato.Input AS b;");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_STRINGS_EQUAL(res.Issues.ToString(), "<main>:1:32: Warning: Non-default join strategy will not be used for CROSS JOIN, code: 4534\n");
    }

    Y_UNIT_TEST(WarnCartesianProductStrategyHint) {
        NYql::TAstParseResult res = SqlToYql("pragma AnsiImplicitCrossJoin; use plato; SELECT * FROM A, /*+ merge() */ B;");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_STRINGS_EQUAL(res.Issues.ToString(), "<main>:1:74: Warning: Non-default join strategy will not be used for CROSS JOIN, code: 4534\n");
    }

    Y_UNIT_TEST(WarnUnknownJoinStrategyHint) {
        NYql::TAstParseResult res = SqlToYql("SELECT * FROM plato.Input AS a JOIN /*+ xmerge() */ plato.Input AS b USING (key);");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_STRINGS_EQUAL(res.Issues.ToString(), "<main>:1:41: Warning: Unsupported join strategy: xmerge, code: 4534\n");
    }

    Y_UNIT_TEST(ReverseLabels) {
        NYql::TAstParseResult res = SqlToYql("select in.key as subkey, subkey as key from plato.Input as in;");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(AutogenerationAliasWithoutCollisionConflict1) {
        NYql::TAstParseResult res = SqlToYql("select LENGTH(Value), key as column1 from plato.Input;");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(AutogenerationAliasWithoutCollision2Conflict2) {
        NYql::TAstParseResult res = SqlToYql("select key as column0, LENGTH(Value) from plato.Input;");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(InputAliasForQualifiedAsterisk) {
        NYql::TAstParseResult res = SqlToYql("use plato; select zyuzya.*, key from plato.Input as zyuzya;");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(SelectSupportsResultColumnsWithTrailingComma) {
        NYql::TAstParseResult res = SqlToYql("select a, b, c, from plato.Input;");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(SelectOrderByLabeledColumn) {
        NYql::TAstParseResult res = SqlToYql("pragma DisableOrderedColumns; select key as goal from plato.Input order by goal");
        UNIT_ASSERT(res.Root);
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
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(SelectOrderByDuplicateLabels) {
        NYql::TAstParseResult res = SqlToYql("select a from plato.Input order by a, a");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(SelectOrderByExpression) {
        NYql::TAstParseResult res = SqlToYql("select * from plato.Input as i order by cast(key as uint32) + cast(subkey as uint32)");
        UNIT_ASSERT(res.Root);
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
        UNIT_ASSERT(res.Root);
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
        UNIT_ASSERT(res.Root);
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
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(OrderByCastValue) {
        NYql::TAstParseResult res = SqlToYql("select i.key, i.subkey from plato.Input as i order by cast(key as uint32) desc;");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(GroupByCastValue) {
        NYql::TAstParseResult res = SqlToYql("select count(1) from plato.Input as i group by cast(key as uint8);");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(KeywordInSelectColumns) {
        NYql::TAstParseResult res = SqlToYql("select in, s.check from (select 1 as in, \"test\" as check) as s;");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(SelectAllGroupBy) {
        NYql::TAstParseResult res = SqlToYql("select * from plato.Input group by subkey;");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(CreateObjectWithFeatures) {
        NYql::TAstParseResult res = SqlToYql("USE plato; CREATE OBJECT secretId (TYPE SECRET) WITH (Key1=Value1, K2=V2);");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'('\"K2\" '\"V2\") '('\"Key1\" '\"Value1\")"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("createObject"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0}, {TString("SECRET"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SECRET"]);
    }

    Y_UNIT_TEST(CreateObjectIfNotExists) {
        NYql::TAstParseResult res = SqlToYql("USE plato; CREATE OBJECT IF NOT EXISTS secretId (TYPE SECRET) WITH (Key1=Value1, K2=V2);");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("createObjectIfNotExists"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0}, {TString("SECRET"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SECRET"]);
    }

    Y_UNIT_TEST(CreateObjectWithFeaturesStrings) {
        NYql::TAstParseResult res = SqlToYql("USE plato; CREATE OBJECT secretId (TYPE SECRET) WITH (Key1=\"Value1\", K2='V2', K3=V3, K4='', K5=`aaa`, K6='a\\'aa');");
        UNIT_ASSERT(res.Root);

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
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'('\"K2\" '\"V2\") '('\"Key1\" '\"Value1\")"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("upsertObject"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0}, {TString("SECRET"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SECRET"]);
    }

    Y_UNIT_TEST(CreateObjectWithFeaturesAndFlags) {
        NYql::TAstParseResult res = SqlToYql("USE plato; CREATE OBJECT secretId (TYPE SECRET) WITH (Key1=Value1, K2=V2, RECURSE);");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'('\"Key1\" '\"Value1\") '('\"RECURSE\")"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("createObject"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0}, {TString("SECRET"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SECRET"]);
    }

    Y_UNIT_TEST(Select1Type) {
        NYql::TAstParseResult res = SqlToYql("SELECT 1 type;");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(SelectTableType) {
        NYql::TAstParseResult res = SqlToYql("USE plato; SELECT * from T type;");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(CreateObjectNoFeatures) {
        NYql::TAstParseResult res = SqlToYql("USE plato; CREATE OBJECT secretId (TYPE SECRET);");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("'features"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("createObject"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0}, {TString("SECRET"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SECRET"]);
    }

    Y_UNIT_TEST(AlterObjectWithFeatures) {
        NYql::TAstParseResult res = SqlToYql(
            "USE plato;\n"
            "declare $path as String;\n"
            "ALTER OBJECT secretId (TYPE SECRET) SET (Key1=$path, K2=V2);"
        );
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'features"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'\"Key1\" (EvaluateAtom \"$path\""));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'\"K2\" '\"V2\""));

                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("alterObject"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0}, {TString("SECRET"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SECRET"]);
    }

    Y_UNIT_TEST(AlterObjectNoFeatures) {
        NYql::TAstParseResult res = SqlToYql("USE plato; ALTER OBJECT secretId (TYPE SECRET);");
        UNIT_ASSERT(!res.Root);
    }

    Y_UNIT_TEST(DropObjectNoFeatures) {
        NYql::TAstParseResult res = SqlToYql("USE plato; DROP OBJECT secretId (TYPE SECRET);");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("'features"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("dropObject"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0}, {TString("SECRET"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SECRET"]);
    }

    Y_UNIT_TEST(DropObjectWithFeatures) {
        NYql::TAstParseResult res = SqlToYql("USE plato; DROP OBJECT secretId (TYPE SECRET) WITH (A, B, C);");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'features"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("dropObject"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0}, {TString("SECRET"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SECRET"]);
    }

    Y_UNIT_TEST(DropObjectWithOneOption) {
        NYql::TAstParseResult res = SqlToYql("USE plato; DROP OBJECT secretId (TYPE SECRET) WITH OVERRIDE;");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'features"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'\"OVERRIDE\""));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("dropObject"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0}, {TString("SECRET"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SECRET"]);
    }

    Y_UNIT_TEST(DropObjectIfExists) {
        NYql::TAstParseResult res = SqlToYql("USE plato; DROP OBJECT IF EXISTS secretId (TYPE SECRET);");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("dropObjectIfExists"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0}, {TString("SECRET"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SECRET"]);
    }

    Y_UNIT_TEST(PrimaryKeyParseCorrect) {
        NYql::TAstParseResult res = SqlToYql("USE plato; CREATE TABLE tableName (Key Uint32, Subkey Int64, Value String, PRIMARY KEY (Key, Subkey));");
        UNIT_ASSERT(res.Root);

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

    Y_UNIT_TEST(CreateTableNonNullableYqlTypeAstCorrect) {
        NYql::TAstParseResult res = SqlToYql("USE plato; CREATE TABLE t (a int32 not null);");
        UNIT_ASSERT(res.Root);

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
        UNIT_ASSERT(res.Root);

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
        UNIT_ASSERT(res.Root);

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
        UNIT_ASSERT(res.Root);

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
        NYql::TAstParseResult res = SqlToYql("USE plato; CREATE TABLE t (a int32, primary key(a));");
        UNIT_ASSERT(res.Root);

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
        NYql::TAstParseResult res = SqlToYql("USE plato; CREATE TABLE t (a int32 not null, primary key(a));");
        UNIT_ASSERT(res.Root);

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
        NYql::TAstParseResult res = SqlToYql("USE plato; CREATE TABLE IF NOT EXISTS t (a int32, primary key(a));");
        UNIT_ASSERT(res.Root);

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

    Y_UNIT_TEST(CreateTempTable) {
        NYql::TAstParseResult res = SqlToYql("USE plato; CREATE TEMP TABLE t (a int32, primary key(a));");
        UNIT_ASSERT(res.Root);

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
        NYql::TAstParseResult res = SqlToYql("USE plato; CREATE TEMPORARY TABLE t (a int32, primary key(a));");
        UNIT_ASSERT(res.Root);

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
        NYql::TAstParseResult res = SqlToYql("USE plato; CREATE TABLE t (a, primary key(a));");
        UNIT_ASSERT(!res.Root);
    }

    Y_UNIT_TEST(CreateTableAsSelectWithTypes) {
        NYql::TAstParseResult res = SqlToYql("USE plato; CREATE TABLE t (a int32, primary key(a)) AS SELECT * FROM ts;");
        UNIT_ASSERT(!res.Root);
    }

    Y_UNIT_TEST(CreateTableAsSelect) {
        NYql::TAstParseResult res = SqlToYql("USE plato; CREATE TABLE t (a, b, primary key(a)) AS SELECT * FROM ts;");
        UNIT_ASSERT_C(res.Root, res.Issues.ToString());

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write!") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos,
                                           line.find(R"__((let world (Write! world sink (Key '('tablescheme (String '"t"))) values '('('mode 'create) '('columns '('('"a") '('"b"))) '('primarykey '('"a"))))))__"));
            }
            if (word == "Read!") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos,
                                           line.find(R"__((Read! world (DataSource '"yt" '"plato") (MrTableConcat (Key '('table (String '"ts")))))__"));
            }
        };

        TWordCountHive elementStat = {{TString("Write!"), 0}, {TString("Read!"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write!"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Read!"]);
    }

    Y_UNIT_TEST(CreateTableAsSelectOnlyPrimary) {
        NYql::TAstParseResult res = SqlToYql("USE plato; CREATE TABLE t (primary key(a)) AS SELECT * FROM ts;");
        UNIT_ASSERT_C(res.Root, res.Issues.ToString());

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write!") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos,
                                           line.find(R"__((let world (Write! world sink (Key '('tablescheme (String '"t"))) values '('('mode 'create) '('columns '()) '('primarykey '('"a"))))))__"));
            }
            if (word == "Read!") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos,
                                           line.find(R"__((Read! world (DataSource '"yt" '"plato") (MrTableConcat (Key '('table (String '"ts")))))__"));
            }
        };

        TWordCountHive elementStat = {{TString("Write!"), 0}, {TString("Read!"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write!"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Read!"]);
    }

    Y_UNIT_TEST(CreateTableAsValuesFail) {
        NYql::TAstParseResult res = SqlToYql("USE plato; CREATE TABLE t (a, primary key(a)) AS VALUES (1), (2);");
        UNIT_ASSERT(!res.Root);
    }

    Y_UNIT_TEST(CreateTableDuplicatedPkColumnsFail) {
        NYql::TAstParseResult res = SqlToYql("USE plato; CREATE TABLE t (a int32 not null, primary key(a, a));");
        UNIT_ASSERT(!res.Root);
    }

    Y_UNIT_TEST(DeleteFromTableByKey) {
        NYql::TAstParseResult res = SqlToYql("delete from plato.Input where key = 200;", 10, "kikimr");
        UNIT_ASSERT(res.Root);

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
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("('mode 'delete)"));
            }
        };

        TWordCountHive elementStat = {{TString("Write"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(DeleteFromTableOnValues) {
        NYql::TAstParseResult res = SqlToYql("delete from plato.Input on (key) values (1);",
            10, "kikimr");
        UNIT_ASSERT(res.Root);

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
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("('mode 'delete_on)"));
            }
        };

        TWordCountHive elementStat = {{TString("Write"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(UpdateByValues) {
        NYql::TAstParseResult res = SqlToYql("update plato.Input set key = 777, value = 'cool' where key = 200;", 10, "kikimr");
        UNIT_ASSERT(res.Root);

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

    Y_UNIT_TEST(UpdateByMultiValues) {
        NYql::TAstParseResult res = SqlToYql("update plato.Input set (key, value, subkey) = ('2','ddd',':') where key = 200;", 10, "kikimr");
        UNIT_ASSERT(res.Root);

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
        UNIT_ASSERT(res.Root);

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
        UNIT_ASSERT(res.Root);

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
        UNIT_ASSERT(res.Root);

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
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("('mode 'update_on)"));
            }
        };

        TWordCountHive elementStat = {{TString("Write"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(UnionAllTest) {
        NYql::TAstParseResult res = SqlToYql("SELECT key FROM plato.Input UNION ALL select subkey FROM plato.Input;");
        UNIT_ASSERT(res.Root);

        TWordCountHive elementStat = {{TString("UnionAll"), 0}};
        VerifyProgram(res, elementStat, {});
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["UnionAll"]);
    }

    Y_UNIT_TEST(UnionTest) {
        NYql::TAstParseResult res = SqlToYql("SELECT key FROM plato.Input UNION select subkey FROM plato.Input;");
        UNIT_ASSERT(res.Root);

        TWordCountHive elementStat = {{TString("Union"), 0}};
        VerifyProgram(res, elementStat, {});
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Union"]);
    }

    Y_UNIT_TEST(UnionAggregationTest) {
        NYql::TAstParseResult res = SqlToYql(R"(
            SELECT 1
            UNION ALL
                SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
            UNION
                SELECT 1 UNION SELECT 1 UNION SELECT 1 UNION SELECT 1
            UNION ALL
                SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1;
        )");
        UNIT_ASSERT(res.Root);

        TWordCountHive elementStat = {{TString("Union"), 0}, {TString("UnionAll"), 0}};
        VerifyProgram(res, elementStat, {});
        UNIT_ASSERT_VALUES_EQUAL(2, elementStat["UnionAll"]);
        UNIT_ASSERT_VALUES_EQUAL(3, elementStat["Union"]);
    }

    Y_UNIT_TEST(DeclareDecimalParameter) {
        NYql::TAstParseResult res = SqlToYql("declare $value as Decimal(22,9); select $value as cnt;");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(SimpleGroupBy) {
        NYql::TAstParseResult res = SqlToYql("select count(1),z from plato.Input group by key as z order by z;");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(EmptyColumnName0) {
        /// Now it's parsed well and error occur on validate step like "4:31:Empty struct member name is not allowed" in "4:31:Function: AddMember"
        NYql::TAstParseResult res = SqlToYql("insert into plato.Output (``, list1) values (0, AsList(0, 1, 2));");
        /// Verify that parsed well without crash
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(KikimrRollback) {
        NYql::TAstParseResult res = SqlToYql("use plato; select * from Input; rollback;", 10, "kikimr");
        UNIT_ASSERT(res.Root);

        TWordCountHive elementStat = {{TString("rollback"), 0}};
        VerifyProgram(res, elementStat);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["rollback"]);
    }

    Y_UNIT_TEST(PragmaFile) {
        NYql::TAstParseResult res = SqlToYql(R"(pragma file("HW", "sbr:181041334");)");
        UNIT_ASSERT(res.Root);

        TWordCountHive elementStat = {{TString(R"((let world (Configure! world (DataSource '"config") '"AddFileByUrl" '"HW" '"sbr:181041334")))"), 0}};
        VerifyProgram(res, elementStat);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat.cbegin()->second);
    }

    Y_UNIT_TEST(DoNotCrashOnNamedInFilter) {
        NYql::TAstParseResult res = SqlToYql("USE plato; $all = ($table_name) -> { return true; }; SELECT * FROM FILTER(Input, $all)");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(PragmasFileAndUdfOrder) {
        NYql::TAstParseResult res = SqlToYql(R"(
            PRAGMA file("libvideoplayers_udf.so", "https://proxy.sandbox.yandex-team.ru/235185290");
            PRAGMA udf("libvideoplayers_udf.so");
        )");
        UNIT_ASSERT(res.Root);

        const auto programm = GetPrettyPrint(res);
        const auto file = programm.find("AddFileByUrl");
        const auto udfs = programm.find("ImportUdfs");
        UNIT_ASSERT(file < udfs);
    }

    Y_UNIT_TEST(ProcessUserType) {
        NYql::TAstParseResult res = SqlToYql("process plato.Input using Kikimr::PushData(TableRows());", 1, TString(NYql::KikimrProviderName));
        UNIT_ASSERT(res.Root);

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
        UNIT_ASSERT(res.Root);

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
        UNIT_ASSERT(res.Root);

        res = SqlToYql(
            "USE plato; INSERT INTO Output SELECT key FROM Input;",
            10, TString(NYql::RtmrProviderName));
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(SelectStreamRtmrJoinWithYt) {
        NYql::TAstParseResult res = SqlToYql(
            "USE plato; INSERT INTO Output SELECT STREAM key FROM Input LEFT JOIN hahn.ttt as t ON Input.key = t.Name;",
            10, TString(NYql::RtmrProviderName));
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(SelectStreamNonRtmr) {
        NYql::TAstParseResult res = SqlToYql(
            "USE plato; INSERT INTO Output SELECT STREAM key FROM Input;",
            10);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:31: Error: SELECT STREAM is unsupported for non-streaming sources\n");
    }

    Y_UNIT_TEST(GroupByHopRtmr) {
        NYql::TAstParseResult res = SqlToYql(R"(
            USE plato; INSERT INTO Output SELECT key, SUM(value) AS value FROM Input
            GROUP BY key, HOP(subkey, "PT10S", "PT30S", "PT20S");
        )", 10, TString(NYql::RtmrProviderName));
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(GroupByHopRtmrSubquery) {
        // 'use plato' intentially avoided
        NYql::TAstParseResult res = SqlToYql(R"(
            SELECT COUNT(*) AS value FROM (SELECT * FROM plato.Input)
            GROUP BY HOP(Data, "PT10S", "PT30S", "PT20S")
        )", 10, TString(NYql::RtmrProviderName));
        UNIT_ASSERT(res.Root);
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
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(GroupByNoHopRtmr) {
        NYql::TAstParseResult res = SqlToYql(R"(
            USE plato; INSERT INTO Output SELECT STREAM key, SUM(value) AS value FROM Input
            GROUP BY key;
        )", 10, TString(NYql::RtmrProviderName));
        UNIT_ASSERT(!res.Root);
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
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(WarnMissingIsBeforeNotNull) {
        NYql::TAstParseResult res = SqlToYql("select 1 NOT NULL");
        UNIT_ASSERT(res.Root);
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
        UNIT_ASSERT(res.Root);
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
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(AnyInBackticksAsTableName) {
        NYql::TAstParseResult res = SqlToYql("use plato; select * from `any`;");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(AnyJoinForTableAndSubQuery) {
        NYql::TAstParseResult res = SqlToYql(R"(
            USE plato;

            $r = SELECT * FROM plato.Input2;

            SELECT * FROM ANY plato.Input1 AS l
            LEFT JOIN ANY $r AS r
            USING (key);
        )");

        UNIT_ASSERT(res.Root);

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

        UNIT_ASSERT(res.Root);

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

        UNIT_ASSERT(res.Root);

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
        UNIT_ASSERT(res.Root);
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
        UNIT_ASSERT( SqlToYql("SELECT @@@@").IsOk());
        UNIT_ASSERT( SqlToYql("SELECT @@@@@").IsOk());

        UNIT_ASSERT(!SqlToYql("SELECT @@@@@@").IsOk());
        UNIT_ASSERT(!SqlToYql("SELECT @@@@@@@").IsOk());

        UNIT_ASSERT( SqlToYql("SELECT @@@@@@@@").IsOk());
        UNIT_ASSERT( SqlToYql("SELECT @@@@@@@@@").IsOk());
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
        UNIT_ASSERT(!res.Root);
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
        UNIT_ASSERT(SqlToYql(req).IsOk());
    }

    Y_UNIT_TEST(EscapedIdentifierAsLambdaArg) {
        auto req = "$f = ($`foo bar`, $x) -> { return $`foo bar` + $x; };\n"
                   "\n"
                   "select $f(1, 2);";
        auto res = SqlToYql(req);
        UNIT_ASSERT(res.Root);
        const auto programm = GetPrettyPrint(res);
        auto expected = "(lambda '(\"$foo bar\" \"$x\")";
        UNIT_ASSERT(programm.find(expected) != TString::npos);
    }

    Y_UNIT_TEST(UdfSyntaxSugarOnlyCallable) {
        auto req = "SELECT Udf(DateTime::FromString)('2022-01-01');";
        auto res = SqlToYql(req);
        UNIT_ASSERT(res.Root);
        const auto programm = GetPrettyPrint(res);
        auto expected = "(SqlCall '\"DateTime.FromString\" '((PositionalArgs (String '\"2022-01-01\")) (AsStruct)) (TupleType (TypeOf '((String '\"2022-01-01\"))) (TypeOf (AsStruct)) (TupleType)))";
        UNIT_ASSERT(programm.find(expected) != TString::npos);
    }

    Y_UNIT_TEST(UdfSyntaxSugarTypeNoRun) {
        auto req = "SELECT Udf(DateTime::FromString, String, Tuple<Int32, Float>, 'foo' as TypeConfig)('2022-01-01');";
        auto res = SqlToYql(req);
        UNIT_ASSERT(res.Root);
        const auto programm = GetPrettyPrint(res);
        auto expected = "(SqlCall '\"DateTime.FromString\" '((PositionalArgs (String '\"2022-01-01\")) (AsStruct)) (TupleType (TypeOf '((String '\"2022-01-01\"))) (TypeOf (AsStruct)) (TupleType (DataType 'String) (TupleType (DataType 'Int32) (DataType 'Float)))) '\"foo\")";
        UNIT_ASSERT(programm.find(expected) != TString::npos);
    }

    Y_UNIT_TEST(UdfSyntaxSugarRunNoType) {
        auto req = "SELECT Udf(DateTime::FromString, String, Tuple<Int32, Float>, Void() as RunConfig)('2022-01-01');";
        auto res = SqlToYql(req);
        UNIT_ASSERT(res.Root);
        const auto programm = GetPrettyPrint(res);
        auto expected = "(SqlCall '\"DateTime.FromString\" '((PositionalArgs (String '\"2022-01-01\")) (AsStruct)) (TupleType (TypeOf '((String '\"2022-01-01\"))) (TypeOf (AsStruct)) (TupleType (DataType 'String) (TupleType (DataType 'Int32) (DataType 'Float)))) '\"\" (Void))";
        UNIT_ASSERT(programm.find(expected) != TString::npos);
    }

    Y_UNIT_TEST(UdfSyntaxSugarFullTest) {
        auto req = "SELECT Udf(DateTime::FromString, String, Tuple<Int32, Float>, 'foo' as TypeConfig, Void() As RunConfig)('2022-01-01');";
        auto res = SqlToYql(req);
        UNIT_ASSERT(res.Root);
        const auto programm = GetPrettyPrint(res);
        auto expected = "(SqlCall '\"DateTime.FromString\" '((PositionalArgs (String '\"2022-01-01\")) (AsStruct)) (TupleType (TypeOf '((String '\"2022-01-01\"))) (TypeOf (AsStruct)) (TupleType (DataType 'String) (TupleType (DataType 'Int32) (DataType 'Float)))) '\"foo\" (Void))";
        UNIT_ASSERT(programm.find(expected) != TString::npos);
    }

    Y_UNIT_TEST(UdfSyntaxSugarOtherRunConfigs) {
        auto req = "SELECT Udf(DateTime::FromString, String, Tuple<Int32, Float>, 'foo' as TypeConfig, '55' As RunConfig)('2022-01-01');";
        auto res = SqlToYql(req);
        UNIT_ASSERT(res.Root);
        const auto programm = GetPrettyPrint(res);
        auto expected = "(SqlCall '\"DateTime.FromString\" '((PositionalArgs (String '\"2022-01-01\")) (AsStruct)) (TupleType (TypeOf '((String '\"2022-01-01\"))) (TypeOf (AsStruct)) (TupleType (DataType 'String) (TupleType (DataType 'Int32) (DataType 'Float)))) '\"foo\" (String '\"55\"))";
        UNIT_ASSERT(programm.find(expected) != TString::npos);
    }

    Y_UNIT_TEST(UdfSyntaxSugarOtherRunConfigs2) {
        auto req = "SELECT Udf(DateTime::FromString, String, Tuple<Int32, Float>, 'foo' as TypeConfig, AsTuple(32, 'no', AsStruct(1e-9 As SomeFloat)) As RunConfig)('2022-01-01');";
        auto res = SqlToYql(req);
        UNIT_ASSERT(res.Root);
        const auto programm = GetPrettyPrint(res);
        auto expected = "(SqlCall '\"DateTime.FromString\" '((PositionalArgs (String '\"2022-01-01\")) (AsStruct)) (TupleType (TypeOf '((String '\"2022-01-01\"))) (TypeOf (AsStruct)) (TupleType (DataType 'String) (TupleType (DataType 'Int32) (DataType 'Float)))) '\"foo\" '((Int32 '\"32\") (String '\"no\") (AsStruct '('\"SomeFloat\" (Double '\"1e-9\")))))";
        UNIT_ASSERT(programm.find(expected) != TString::npos);
    }

    Y_UNIT_TEST(UdfSyntaxSugarOptional) {
        auto req = "SELECT Udf(DateTime::FromString, String?, Int32??, Tuple<Int32, Float>, \"foo\" as TypeConfig, Void() As RunConfig)(\"2022-01-01\");";
        auto res = SqlToYql(req);
        UNIT_ASSERT(res.Root);
        const auto programm = GetPrettyPrint(res);
        auto expected = "(SqlCall '\"DateTime.FromString\" '((PositionalArgs (String '\"2022-01-01\")) (AsStruct)) (TupleType (TypeOf '((String '\"2022-01-01\"))) (TypeOf (AsStruct)) (TupleType (OptionalType (DataType 'String)) (OptionalType (OptionalType (DataType 'Int32))) (TupleType (DataType 'Int32) (DataType 'Float)))) '\"foo\" (Void))";
        UNIT_ASSERT(programm.find(expected) != TString::npos);
    }

    Y_UNIT_TEST(CompactionPolicyParseCorrect) {
        NYql::TAstParseResult res = SqlToYql(
            R"( USE plato;
                CREATE TABLE tableName (Key Uint32, Value String, PRIMARY KEY (Key))
                WITH ( COMPACTION_POLICY = "SomeCompactionPreset" );)"
        );
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("compactionPolicy"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("SomeCompactionPreset"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(AutoPartitioningBySizeParseCorrect) {
        NYql::TAstParseResult res = SqlToYql(
            R"( USE plato;
                CREATE TABLE tableName (Key Uint32, Value String, PRIMARY KEY (Key))
                WITH ( AUTO_PARTITIONING_BY_SIZE = ENABLED );)"
        );
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("autoPartitioningBySize"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("ENABLED"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(UniformPartitionsParseCorrect) {
        NYql::TAstParseResult res = SqlToYql(
            R"( USE plato;
                CREATE TABLE tableName (Key Uint32, Value String, PRIMARY KEY (Key))
                WITH ( UNIFORM_PARTITIONS = 16 );)"
        );
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("uniformPartitions"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("16"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(DateTimeTtlParseCorrect) {
        NYql::TAstParseResult res = SqlToYql(
            R"( USE plato;
                CREATE TABLE tableName (Key Uint32, CreatedAt Timestamp, PRIMARY KEY (Key))
                WITH (TTL = Interval("P1D") On CreatedAt);)"
        );
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("setTtlSettings"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("expireAfter"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("86400000"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(IntTtlParseCorrect) {
        NYql::TAstParseResult res = SqlToYql(
            R"( USE plato;
                CREATE TABLE tableName (Key Uint32, CreatedAt Uint32, PRIMARY KEY (Key))
                WITH (TTL = Interval("P1D") On CreatedAt AS SECONDS);)"
        );
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("setTtlSettings"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("expireAfter"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("86400000"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("columnUnit"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("seconds"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(TieringParseCorrect) {
        NYql::TAstParseResult res = SqlToYql(
            R"( USE plato;
                CREATE TABLE tableName (Key Uint32, Value String, PRIMARY KEY (Key))
                WITH ( TIERING = 'my_tiering' );)"
        );
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("tiering"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("my_tiering"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(StoreExternalBlobsParseCorrect) {
        NYql::TAstParseResult res = SqlToYql(
            R"( USE plato;
                CREATE TABLE tableName (Key Uint32, Value String, PRIMARY KEY (Key))
                WITH ( STORE_EXTERNAL_BLOBS = ENABLED );)"
        );
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("storeExternalBlobs"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("ENABLED"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(DefaultValueColumn2) {
        auto res = SqlToYql(R"( use plato;
            $lambda = () -> {
                RETURN CAST(RandomUuid(2) as String)
            };

            CREATE TABLE tableName (
                Key Uint32 DEFAULT RandomNumber(1),
                Value String DEFAULT $lambda,
                PRIMARY KEY (Key)
            );
        )");

        UNIT_ASSERT_C(res.Root, Err2Str(res));

        const auto program = GetPrettyPrint(res);

        UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, program.find("RandomNumber"));
        UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, program.find("RandomUuid"));
        UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, program.find("columnConstrains"));
        UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, program.find("columnConstrains"));
        UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, program.find("Write"));

#if 0
        Cerr << program << Endl;
#endif

        TWordCountHive elementStat = { {TString("Write"), 0} };
        VerifyProgram(res, elementStat);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(DefaultValueColumn3) {
        auto res = SqlToYql(R"( use plato;

            CREATE TABLE tableName (
                database_id Utf8,
                cloud_id Utf8,
                global_id Utf8 DEFAULT database_id || "=====",
                PRIMARY KEY (database_id)
            );
        )");

        UNIT_ASSERT_VALUES_EQUAL(Err2Str(res), "<main>:6:40: Error: Column reference \"database_id\" is not allowed in current scope\n");
        UNIT_ASSERT(!res.Root);
    }

    Y_UNIT_TEST(DefaultValueColumn) {
        auto res = SqlToYql(R"( use plato;
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

        UNIT_ASSERT_C(res.Root, Err2Str(res));

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

        TWordCountHive elementStat = { {TString("Write"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(ChangefeedParseCorrect) {
        auto res = SqlToYql(R"( USE plato;
            CREATE TABLE tableName (
                Key Uint32, PRIMARY KEY (Key),
                CHANGEFEED feedName WITH (
                    MODE = 'KEYS_ONLY',
                    FORMAT = 'json',
                    INITIAL_SCAN = TRUE,
                    VIRTUAL_TIMESTAMPS = FALSE,
                    RESOLVED_TIMESTAMPS = Interval("PT1S"),
                    RETENTION_PERIOD = Interval("P1D"),
                    TOPIC_MIN_ACTIVE_PARTITIONS = 10,
                    AWS_REGION = 'aws:region'
                )
            );
        )");
        UNIT_ASSERT_C(res.Root, Err2Str(res));

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("changefeed"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("mode"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("KEYS_ONLY"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("format"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("json"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("initial_scan"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("true"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("virtual_timestamps"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("false"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("resolved_timestamps"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("retention_period"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("topic_min_active_partitions"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("aws_region"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("aws:region"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(CloneForAsTableWorksWithCube) {
        UNIT_ASSERT(SqlToYql("SELECT * FROM AS_TABLE([<|k1:1, k2:1|>]) GROUP BY CUBE(k1, k2);").IsOk());
    }

    Y_UNIT_TEST(WindowPartitionByColumnProperlyEscaped) {
        NYql::TAstParseResult res = SqlToYql("SELECT SUM(key) OVER w FROM plato.Input WINDOW w AS (PARTITION BY `column with space`);");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "CalcOverWindow") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("\"column with space\""));
            }
        };

        TWordCountHive elementStat = { {TString("CalcOverWindow"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["CalcOverWindow"]);
    }

    Y_UNIT_TEST(WindowPartitionByExpressionWithoutAliasesAreAllowed) {
        NYql::TAstParseResult res = SqlToYql("SELECT SUM(key) OVER w FROM plato.Input as i WINDOW w AS (PARTITION BY ii.subkey);");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "AddMember") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("AddMember row 'group_w_0 (SqlAccess 'struct (Member row '\"ii\")"));
            }
            if (word == "CalcOverWindow") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("CalcOverWindow core '('\"group_w_0\")"));
            }
        };

        TWordCountHive elementStat = { {TString("CalcOverWindow"), 0}, {TString("AddMember"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["CalcOverWindow"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["AddMember"]);
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
            "select * from plato.object($path, `format`, \"comp\" || \"ression\" as compression, 1 as bar) with schema (Int32 as y, String as x)"
        );
        UNIT_ASSERT(res.Root);

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
            settings
        );
        UNIT_ASSERT(res.Root);

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
            settings
        );
        UNIT_ASSERT_VALUES_EQUAL(Err2Str(res), "<main>:1:15: Error: Please remove 'bindings.' from your query, the support for this syntax has ended, code: 4601\n");
        UNIT_ASSERT(!res.Root);

        settings.BindingsMode = NSQLTranslation::EBindingsMode::DROP;
        res = SqlToYqlWithSettings(
            "select * from bindings.foo",
            settings
        );

        UNIT_ASSERT(res.Root);

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
            settings
        );
        UNIT_ASSERT_VALUES_EQUAL(Err2Str(res), "<main>:1:15: Warning: Please remove 'bindings.' from your query, the support for this syntax will be dropped soon, code: 4538\n");
        UNIT_ASSERT(res.Root);

        TWordCountHive elementStat3 = {{TString("MrTableConcat"), 0}};
        VerifyProgram(res, elementStat3, verifyLine2);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat3["MrTableConcat"]);
    }

    Y_UNIT_TEST(TableBindingsWithInsert) {
        NSQLTranslation::TTranslationSettings settings = GetSettingsWithS3Binding("foo");
        NYql::TAstParseResult res = SqlToYqlWithSettings(
            "insert into bindings.foo with truncate (x, y) values (1, 2);",
            settings
            );
        UNIT_ASSERT(res.Root);

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
            settings
        );
        UNIT_ASSERT_VALUES_EQUAL(Err2Str(res), "<main>:1:13: Error: Please remove 'bindings.' from your query, the support for this syntax has ended, code: 4601\n");
        UNIT_ASSERT(!res.Root);

        settings.BindingsMode = NSQLTranslation::EBindingsMode::DROP;
        res = SqlToYqlWithSettings(
            "insert into bindings.foo with truncate (x, y) values (1, 2);",
            settings
        );
        UNIT_ASSERT_VALUES_EQUAL(Err2Str(res), "");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine2 = [](const TString& word, const TString& line) {
            if (word == "Write!") {
                //UNIT_ASSERT_VALUES_EQUAL(line, "");
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
            settings
        );
        UNIT_ASSERT_VALUES_EQUAL(Err2Str(res), "<main>:1:13: Warning: Please remove 'bindings.' from your query, the support for this syntax will be dropped soon, code: 4538\n");
        UNIT_ASSERT(res.Root);

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
        for (size_t i = 0; i < 20000; ++i)  {
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
        UNIT_ASSERT(SqlToYql("USE plato; ALTER TABLE table RENAME TO moved").IsOk());
    }

    Y_UNIT_TEST(AlterTableAddDropColumnIsCorrect) {
        UNIT_ASSERT(SqlToYql("USE plato; ALTER TABLE table ADD COLUMN addc uint64, DROP COLUMN dropc, ADD addagain uint64").IsOk());
    }

    Y_UNIT_TEST(AlterTableSetTTLIsCorrect) {
        UNIT_ASSERT(SqlToYql("USE plato; ALTER TABLE table SET (TTL = Interval(\"PT3H\") ON column)").IsOk());
        UNIT_ASSERT(SqlToYql("USE plato; ALTER TABLE table SET (TTL = Interval(\"PT3H\") ON column AS SECONDS)").IsOk());
    }

    Y_UNIT_TEST(AlterTableSetTieringIsCorrect) {
        UNIT_ASSERT(SqlToYql("USE plato; ALTER TABLE table SET (TIERING = 'my_tiering')").IsOk());
    }

    Y_UNIT_TEST(AlterTableAddChangefeedIsCorrect) {
        UNIT_ASSERT(SqlToYql("USE plato; ALTER TABLE table ADD CHANGEFEED feed WITH (MODE = 'UPDATES', FORMAT = 'json')").IsOk());
    }

    Y_UNIT_TEST(AlterTableAlterChangefeedIsCorrect) {
        UNIT_ASSERT(SqlToYql("USE plato; ALTER TABLE table ALTER CHANGEFEED feed DISABLE").IsOk());
        ExpectFailWithError("USE plato; ALTER TABLE table ALTER CHANGEFEED feed SET (FORMAT = 'proto');",
            "<main>:1:57: Error: FORMAT alter is not supported\n");
    }

    Y_UNIT_TEST(AlterTableDropChangefeedIsCorrect) {
        UNIT_ASSERT(SqlToYql("USE plato; ALTER TABLE table DROP CHANGEFEED feed").IsOk());
    }

    Y_UNIT_TEST(AlterTableSetPartitioningIsCorrect) {
        UNIT_ASSERT(SqlToYql("USE plato; ALTER TABLE table SET (AUTO_PARTITIONING_BY_SIZE = DISABLED)").IsOk());
    }

    Y_UNIT_TEST(AlterTableAddIndexWithIsNotSupported) {
        ExpectFailWithError("USE plato; ALTER TABLE table ADD INDEX idx GLOBAL ON (col) WITH (a=b)",
            "<main>:1:40: Error: with: alternative is not implemented yet: 737:20: global_index\n");
    }

    Y_UNIT_TEST(AlterTableAddIndexLocalIsNotSupported) {
        ExpectFailWithError("USE plato; ALTER TABLE table ADD INDEX idx LOCAL ON (col)",
            "<main>:1:40: Error: local: alternative is not implemented yet: 737:35: local_index\n");
    }

    Y_UNIT_TEST(CreateTableAddIndexVector) {
        const auto result = SqlToYql(R"(USE plato;
            CREATE TABLE table (
                pk INT32 NOT NULL,
                col String, 
                INDEX idx GLOBAL USING vector_kmeans_tree 
                    ON (col) COVER (col)
                    WITH (distance=cosine, vector_type=float, vector_dimension=1024,),
                PRIMARY KEY (pk))
                )");
        UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
    }

    Y_UNIT_TEST(AlterTableAddIndexVector) {
        const auto result = SqlToYql(R"(USE plato; 
            ALTER TABLE table ADD INDEX idx 
                GLOBAL USING vector_kmeans_tree 
                ON (col) COVER (col)
                WITH (distance=cosine, vector_type="float", vector_dimension=1024) 
                )");
        UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
    }

    Y_UNIT_TEST(AlterTableAddIndexUnknownSubtype) {
        ExpectFailWithError("USE plato; ALTER TABLE table ADD INDEX idx GLOBAL USING unknown ON (col)",
            "<main>:1:57: Error: UNKNOWN index subtype is not supported\n");
    }

    Y_UNIT_TEST(AlterTableAddIndexMissedParameter) {
        ExpectFailWithError(R"(USE plato; 
            ALTER TABLE table ADD INDEX idx 
                GLOBAL USING vector_kmeans_tree 
                ON (col)
                WITH (distance=cosine, vector_type=float) 
                )",
            "<main>:5:52: Error: vector_dimension should be set\n");
    }

    Y_UNIT_TEST(AlterTableAlterIndexSetPartitioningIsCorrect) {
        const auto result = SqlToYql("USE plato; ALTER TABLE table ALTER INDEX index SET AUTO_PARTITIONING_MIN_PARTITIONS_COUNT 10");
        UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
    }

    Y_UNIT_TEST(AlterTableAlterIndexSetMultiplePartitioningSettings) {
        const auto result = SqlToYql("USE plato; ALTER TABLE table ALTER INDEX index SET "
            "(AUTO_PARTITIONING_BY_LOAD = ENABLED, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10)"
        );
        UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
    }

    Y_UNIT_TEST(AlterTableAlterIndexResetPartitioningIsNotSupported) {
        ExpectFailWithError("USE plato; ALTER TABLE table ALTER INDEX index RESET (AUTO_PARTITIONING_MIN_PARTITIONS_COUNT)",
            "<main>:1:55: Error: AUTO_PARTITIONING_MIN_PARTITIONS_COUNT reset is not supported\n"
        );
    }

    Y_UNIT_TEST(AlterTableAlterColumnDropNotNullAstCorrect) {
        auto reqSetNull = SqlToYql(R"(
            USE plato;
            CREATE TABLE tableName (
                id Uint32,
                val Uint32 NOT NULL,
                PRIMARY KEY (id)
            );

            COMMIT;
            ALTER TABLE tableName ALTER COLUMN val DROP NOT NULL;
        )");

        UNIT_ASSERT(reqSetNull.IsOk());
        UNIT_ASSERT(reqSetNull.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            Y_UNUSED(word);

            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(
                R"(let world (Write! world sink (Key '('tablescheme (String '"tableName"))) (Void) '('('mode 'alter) '('actions '('('alterColumns '('('"val" '('changeColumnConstraints '('('drop_not_null)))))))))))"
            ));
        };

        TWordCountHive elementStat({TString("\'mode \'alter")});
        VerifyProgram(reqSetNull, elementStat, verifyLine);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["\'mode \'alter"]);
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
        UNIT_ASSERT(SqlToYql("select * from s3bucket.`foo` with schema (col1 Int32, String as col2, Int64 as col3);", settings).IsOk());
    }

    Y_UNIT_TEST(AllowNestedTuplesInGroupBy) {
        NYql::TAstParseResult res = SqlToYql("select count(*) from plato.Input group by 1 + (x, y, z);");
        UNIT_ASSERT(res.Root);

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
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
            Y_UNUSED(word);
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("(Aggregate core '('\"x\" '\"alias1\" '\"z\")"));
        };

        TWordCountHive elementStat({"Aggregate"});
        VerifyProgram(res, elementStat, verifyLine);
        UNIT_ASSERT(elementStat["Aggregate"] == 1);
    }

    Y_UNIT_TEST(CreateAsyncReplicationParseCorrect) {
        auto req = R"(
            USE plato;
            CREATE ASYNC REPLICATION MyReplication
            FOR table1 AS table2, table3 AS table4
            WITH (
                CONNECTION_STRING = "grpc://localhost:2135/?database=/MyDatabase",
                ENDPOINT = "localhost:2135",
                DATABASE = "/MyDatabase"
            );
        )";
        auto res = SqlToYql(req);
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("MyReplication"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("create"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("table1"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("table2"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("table3"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("table4"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("connection_string"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("grpc://localhost:2135/?database=/MyDatabase"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("endpoint"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("localhost:2135"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("database"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("/MyDatabase"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(CreateAsyncReplicationUnsupportedSettings) {
        auto reqTpl = R"(
            USE plato;
            CREATE ASYNC REPLICATION MyReplication
            FOR table1 AS table2, table3 AS table4
            WITH (
                %s = "%s"
            )
        )";

        auto settings = THashMap<TString, TString>{
            {"STATE", "DONE"},
            {"FAILOVER_MODE", "FORCE"},
        };

        for (const auto& [k, v] : settings) {
            auto req = Sprintf(reqTpl, k.c_str(), v.c_str());
            auto res = SqlToYql(req);
            UNIT_ASSERT(!res.Root);
            UNIT_ASSERT_NO_DIFF(Err2Str(res), Sprintf("<main>:6:%zu: Error: %s is not supported in CREATE\n", 20 + k.size(), k.c_str()));
        }
    }

    Y_UNIT_TEST(AlterAsyncReplicationParseCorrect) {
        auto req = R"(
            USE plato;
            ALTER ASYNC REPLICATION MyReplication
            SET (
                STATE = "DONE",
                FAILOVER_MODE = "FORCE"
            );
        )";
        auto res = SqlToYql(req);
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("MyReplication"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("alter"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("state"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("DONE"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("failover_mode"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("FORCE"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(AlterAsyncReplicationUnsupportedSettings) {
        auto reqTpl = R"(
            USE plato;
            ALTER ASYNC REPLICATION MyReplication
            SET (
                %s = "%s"
            )
        )";

        auto settings = THashMap<TString, TString>{
            {"connection_string", "grpc://localhost:2135/?database=/MyDatabase"},
            {"endpoint", "localhost:2135"},
            {"database", "/MyDatabase"},
            {"token", "foo"},
            {"token_secret_name", "foo_secret_name"},
            {"user", "user"},
            {"password", "bar"},
            {"password_secret_name", "bar_secret_name"},
        };

        for (const auto& setting : settings) {
            auto& key = setting.first;
            auto& value = setting.second;
            auto req = Sprintf(reqTpl, key.c_str(), value.c_str());
            auto res = SqlToYql(req);
            UNIT_ASSERT(res.Root);
            
            TVerifyLineFunc verifyLine = [&key, &value](const TString& word, const TString& line) {
                if (word == "Write") {
                    UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("MyReplication"));
                    UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("alter"));
                    UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(key));
                    UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(value));
                }
            };

            TWordCountHive elementStat = { {TString("Write"), 0}};
            VerifyProgram(res, elementStat, verifyLine);

            UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
        }
    }

    Y_UNIT_TEST(AsyncReplicationInvalidSettings) {
        auto req = R"(
            USE plato;
            ALTER ASYNC REPLICATION MyReplication SET (FOO = "BAR");
        )";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:62: Error: Unknown replication setting: FOO\n");
    }

    Y_UNIT_TEST(DropAsyncReplicationParseCorrect) {
        auto req = R"(
            USE plato;
            DROP ASYNC REPLICATION MyReplication;
        )";
        auto res = SqlToYql(req);
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("MyReplication"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("drop"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(DropAsyncReplicationCascade) {
        auto req = R"(
            USE plato;
            DROP ASYNC REPLICATION MyReplication CASCADE;
        )";
        auto res = SqlToYql(req);
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("dropCascade"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(PragmaCompactGroupBy) {
        auto req = "PRAGMA CompactGroupBy; SELECT key, COUNT(*) FROM plato.Input GROUP BY key;";
        auto res = SqlToYql(req);
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Aggregate") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'('compact)"));
            }
        };

        TWordCountHive elementStat = { {TString("Aggregate"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Aggregate"]);
    }

    Y_UNIT_TEST(PragmaDisableCompactGroupBy) {
        auto req = "PRAGMA DisableCompactGroupBy; SELECT key, COUNT(*) FROM plato.Input GROUP /*+ compact() */ BY key;";
        auto res = SqlToYql(req);
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Aggregate") {
                UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("'('compact)"));
            }
        };

        TWordCountHive elementStat = { {TString("Aggregate"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Aggregate"]);
    }

    Y_UNIT_TEST(AutoSampleWorksWithNamedSubquery) {
        UNIT_ASSERT(SqlToYql("$src = select * from plato.Input; select * from $src sample 0.2").IsOk());
    }

    Y_UNIT_TEST(AutoSampleWorksWithSubquery) {
        UNIT_ASSERT(SqlToYql("select * from (select * from plato.Input) sample 0.2").IsOk());
    }

    Y_UNIT_TEST(CreateTableTrailingComma) {
        UNIT_ASSERT(SqlToYql("USE plato; CREATE TABLE tableName (Key Uint32, PRIMARY KEY (Key),);").IsOk());
        UNIT_ASSERT(SqlToYql("USE plato; CREATE TABLE tableName (Key Uint32,);").IsOk());
    }

    Y_UNIT_TEST(BetweenSymmetric) {
        UNIT_ASSERT(SqlToYql("select 3 between symmetric 5 and 4;").IsOk());
        UNIT_ASSERT(SqlToYql("select 3 between asymmetric 5 and 4;").IsOk());
        UNIT_ASSERT(SqlToYql("use plato; select key between symmetric and and and from Input;").IsOk());
        UNIT_ASSERT(SqlToYql("use plato; select key between and and and from Input;").IsOk());
    }
}

Y_UNIT_TEST_SUITE(ExternalFunction) {
    Y_UNIT_TEST(ValidUseFunctions) {

        UNIT_ASSERT(SqlToYql(
                "PROCESS plato.Input"
                " USING EXTERNAL FUNCTION('YANDEX-CLOUD', 'foo', <|a: 123, b: a + 641|>)"
                " WITH INPUT_TYPE=Struct<a:Int32>, OUTPUT_TYPE=Struct<b:Int32>,"
                " CONCURRENCY=3, OPTIMIZE_FOR='CALLS'").IsOk());

        // use CALLS without quotes, as keyword
        UNIT_ASSERT(SqlToYql(
                "PROCESS plato.Input"
                " USING EXTERNAL FUNCTION('YANDEX-CLOUD', 'foo')"
                " WITH INPUT_TYPE=Struct<a:Int32>, OUTPUT_TYPE=Struct<b:Int32>,"
                " OPTIMIZE_FOR=CALLS").IsOk());

        UNIT_ASSERT(SqlToYql(
                "PROCESS plato.Input"
                " USING EXTERNAL FUNCTION('YANDEX-CLOUD', 'foo', TableRow())"
                " WITH INPUT_TYPE=Struct<a:Int32>, OUTPUT_TYPE=Struct<b:Int32>,"
                " CONCURRENCY=3").IsOk());

        UNIT_ASSERT(SqlToYql(
                "PROCESS plato.Input"
                " USING EXTERNAL FUNCTION('YANDEX-CLOUD', 'foo')"
                " WITH INPUT_TYPE=Struct<a:Int32>, OUTPUT_TYPE=Struct<b:Int32>,"
                " CONCURRENCY=3, BATCH_SIZE=1000000, CONNECTION='yc-folder34fse-con',"
                " INIT=[0, 900]").IsOk());

        UNIT_ASSERT(SqlToYql(
                "PROCESS plato.Input"
                " USING EXTERNAL FUNCTION('YANDEX-CLOUD', 'bar', TableRow())"
                " WITH UNKNOWN_PARAM_1='837747712', UNKNOWN_PARAM_2=Tuple<Uint16, Utf8>,"
                " INPUT_TYPE=Struct<a:Int32>, OUTPUT_TYPE=Struct<b:Int32>").IsOk());
    }


    Y_UNIT_TEST(InValidUseFunctions) {
        ExpectFailWithError("PROCESS plato.Input USING some::udf(*) WITH INPUT_TYPE=Struct<a:Int32>",
                            "<main>:1:33: Error: PROCESS without USING EXTERNAL FUNCTION doesn't allow WITH block\n");

        ExpectFailWithError("PROCESS plato.Input USING EXTERNAL FUNCTION('YANDEX-CLOUD', 'jhhjfh88134d')"
                            " WITH INPUT_TYPE=Struct<a:Int32>, OUTPUT_TYPE=Struct<b:Int32>"
                            " ASSUME ORDER BY key",
                            "<main>:1:129: Error: PROCESS with USING EXTERNAL FUNCTION doesn't allow ASSUME block\n");

        ExpectFailWithError("PROCESS plato.Input USING EXTERNAL FUNCTION('YANDEX-CLOUD', 'foo', 'bar', 'baz')",
                            "<main>:1:15: Error: EXTERNAL FUNCTION requires from 2 to 3 arguments, but got: 4\n");

        ExpectFailWithError("PROCESS plato.Input\n"
                            " USING EXTERNAL FUNCTION('YANDEX-CLOUD', 'foo', <|field_1: a1, field_b: b1|>)\n"
                            " WITH INPUT_TYPE=Struct<a:Int32>, OUTPUT_TYPE=Struct<b:Int32>,\n"
                            " CONCURRENCY=3, BATCH_SIZE=1000000, CONNECTION='yc-folder34fse-con',\n"
                            " CONCURRENCY=5, INPUT_TYPE=Struct<b:Bool>,\n"
                            " INIT=[0, 900]\n",
                            "<main>:5:2: Error: WITH \"CONCURRENCY\" clause should be specified only once\n"
                            "<main>:5:17: Error: WITH \"INPUT_TYPE\" clause should be specified only once\n");
    }
}

Y_UNIT_TEST_SUITE(SqlToYQLErrors) {
    Y_UNIT_TEST(UdfSyntaxSugarMissingCall) {
        auto req = "SELECT Udf(DateTime::FromString, \"foo\" as RunConfig);";
        auto res = SqlToYql(req);
        TString a1 = Err2Str(res);
        TString a2("<main>:1:8: Error: Abstract Udf Node can't be used as a part of expression.\n");
        UNIT_ASSERT_NO_DIFF(a1, a2);
    }

    Y_UNIT_TEST(UdfSyntaxSugarIsNotCallable) {
        auto req = "SELECT Udf(123, \"foo\" as RunConfig);";
        auto res = SqlToYql(req);
        TString a1 = Err2Str(res);
        TString a2("<main>:1:8: Error: Udf: first argument must be a callable, like Foo::Bar\n");
        UNIT_ASSERT_NO_DIFF(a1, a2);
    }

    Y_UNIT_TEST(UdfSyntaxSugarNoArgs) {
        auto req = "SELECT Udf()();";
        auto res = SqlToYql(req);
        TString a1 = Err2Str(res);
        TString a2("<main>:1:8: Error: Udf: expected at least one argument\n");
        UNIT_ASSERT_NO_DIFF(a1, a2);
    }

    Y_UNIT_TEST(StrayUTF8) {
        /// 'c' in plato is russian here
        NYql::TAstParseResult res = SqlToYql("select * from сedar.Input");
        UNIT_ASSERT(!res.Root);

        TString a1 = Err2Str(res);
        TString a2(R"foo(<main>:1:14: Error: Unexpected character 'с' (Unicode character <1089>) : cannot match to any predicted input...

<main>:1:15: Error: Unexpected character : cannot match to any predicted input...

)foo");

        UNIT_ASSERT_NO_DIFF(a1, a2);
    }

    Y_UNIT_TEST(IvalidStringLiteralWithEscapedBackslash) {
        NYql::TAstParseResult res1 = SqlToYql(R"foo($bar = 'a\\'b';)foo");
        NYql::TAstParseResult res2 = SqlToYql(R"foo($bar = "a\\"b";)foo");
        UNIT_ASSERT(!res1.Root);
        UNIT_ASSERT(!res2.Root);

        UNIT_ASSERT_NO_DIFF(Err2Str(res1), "<main>:1:15: Error: Unexpected character : syntax error...\n\n");
        UNIT_ASSERT_NO_DIFF(Err2Str(res2), "<main>:1:15: Error: Unexpected character : syntax error...\n\n");
    }

    Y_UNIT_TEST(InvalidHexInStringLiteral) {
        NYql::TAstParseResult res = SqlToYql("select \"foo\\x1\\xfe\"");
        UNIT_ASSERT(!res.Root);
        TString a1 = Err2Str(res);
        TString a2 = "<main>:1:15: Error: Failed to parse string literal: Invalid hexadecimal value\n";

        UNIT_ASSERT_NO_DIFF(a1, a2);
    }

    Y_UNIT_TEST(InvalidOctalInMultilineStringLiteral) {
        NYql::TAstParseResult res = SqlToYql("select \"foo\n"
                                             "bar\n"
                                             "\\01\"");
        UNIT_ASSERT(!res.Root);
        TString a1 = Err2Str(res);
        TString a2 = "<main>:3:4: Error: Failed to parse string literal: Invalid octal value\n";

        UNIT_ASSERT_NO_DIFF(a1, a2);
    }

    Y_UNIT_TEST(InvalidDoubleAtString) {
        NYql::TAstParseResult res = SqlToYql("select @@@@@@");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:13: Error: Unexpected character : syntax error...\n\n");
    }

    Y_UNIT_TEST(InvalidDoubleAtStringWhichWasAcceptedEarlier) {
        NYql::TAstParseResult res = SqlToYql("SELECT @@foo@@ @ @@bar@@");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:7: Error: Unexpected token '@@foo@@' : cannot match to any predicted input...\n\n");
    }

    Y_UNIT_TEST(InvalidStringFromTable) {
        NYql::TAstParseResult res = SqlToYql("select \"FOO\"\"BAR from plato.foo");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:31: Error: Unexpected character : syntax error...\n\n");
    }

    Y_UNIT_TEST(InvalidDoubleAtStringFromTable) {
        NYql::TAstParseResult res = SqlToYql("select @@@@@@ from plato.foo");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:28: Error: Unexpected character : syntax error...\n\n");
    }

    Y_UNIT_TEST(SelectInvalidSyntax) {
        NYql::TAstParseResult res = SqlToYql("select 1 form Wat");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:14: Error: Unexpected token 'Wat' : cannot match to any predicted input...\n\n");
    }

    Y_UNIT_TEST(SelectNoCluster) {
        NYql::TAstParseResult res = SqlToYql("select foo from bar");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: No cluster name given and no default cluster is selected\n");
    }

    Y_UNIT_TEST(SelectDuplicateColumns) {
        NYql::TAstParseResult res = SqlToYql("select a, a from plato.Input");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:11: Error: Unable to use duplicate column names. Collision in name: a\n");
    }

    Y_UNIT_TEST(SelectDuplicateLabels) {
        NYql::TAstParseResult res = SqlToYql("select a as foo, b as foo from plato.Input");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:18: Error: Unable to use duplicate column names. Collision in name: foo\n");
    }

    Y_UNIT_TEST(SelectCaseWithoutThen) {
        NYql::TAstParseResult res = SqlToYql("select case when true 1;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res),
            "<main>:1:22: Error: Unexpected token absence : Missing THEN \n\n"
            "<main>:1:23: Error: Unexpected token absence : Missing END \n\n"
        );
    }

    Y_UNIT_TEST(SelectComplexCaseWithoutThen) {
        NYql::TAstParseResult res = SqlToYql(
            "SELECT *\n"
            "FROM plato.Input AS a\n"
            "WHERE CASE WHEN a.key = \"foo\" a.subkey ELSE a.value END\n"
        );
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:30: Error: Unexpected token absence : Missing THEN \n\n");
    }

    Y_UNIT_TEST(SelectCaseWithoutEnd) {
        NYql::TAstParseResult res = SqlToYql("select case a when b then c end from plato.Input");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: ELSE is required\n");
    }

    Y_UNIT_TEST(SelectWithBadAggregationNoInput) {
        NYql::TAstParseResult res = SqlToYql("select a, Min(b), c");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res),
           "<main>:1:1: Error: Column references are not allowed without FROM\n"
           "<main>:1:8: Error: Column reference 'a'\n"
           "<main>:1:1: Error: Column references are not allowed without FROM\n"
           "<main>:1:15: Error: Column reference 'b'\n"
           "<main>:1:1: Error: Column references are not allowed without FROM\n"
           "<main>:1:19: Error: Column reference 'c'\n"
        );
    }

    Y_UNIT_TEST(SelectWithBadAggregation) {
        ExpectFailWithError("select count(*), 1 + key from plato.Input",
            "<main>:1:22: Error: Column `key` must either be a key column in GROUP BY or it should be used in aggregation function\n");
    }

    Y_UNIT_TEST(SelectWithBadAggregatedTerms) {
        ExpectFailWithError("select key, 2 * subkey from plato.Input group by key",
            "<main>:1:17: Error: Column `subkey` must either be a key column in GROUP BY or it should be used in aggregation function\n");
    }

    Y_UNIT_TEST(SelectDistinctWithBadAggregation) {
        ExpectFailWithError("select distinct count(*), 1 + key from plato.Input",
            "<main>:1:31: Error: Column `key` must either be a key column in GROUP BY or it should be used in aggregation function\n");
        ExpectFailWithError("select distinct key, 2 * subkey from plato.Input group by key",
            "<main>:1:26: Error: Column `subkey` must either be a key column in GROUP BY or it should be used in aggregation function\n");
    }

    Y_UNIT_TEST(SelectWithBadAggregationInHaving) {
        ExpectFailWithError("select key from plato.Input group by key\n"
                            "having \"f\" || value == \"foo\"",
            "<main>:2:15: Error: Column `value` must either be a key column in GROUP BY or it should be used in aggregation function\n");
    }

    Y_UNIT_TEST(JoinWithNonAggregatedColumnInProjection) {
        ExpectFailWithError("select a.key, 1 + b.subkey\n"
                            "from plato.Input1 as a join plato.Input2 as b using(key)\n"
                            "group by a.key;",
            "<main>:1:19: Error: Column `b.subkey` must either be a key column in GROUP BY or it should be used in aggregation function\n");

        ExpectFailWithError("select a.key, 1 + b.subkey.x\n"
                            "from plato.Input1 as a join plato.Input2 as b using(key)\n"
                            "group by a.key;",
            "<main>:1:19: Error: Column must either be a key column in GROUP BY or it should be used in aggregation function\n");
    }

    Y_UNIT_TEST(SelectWithBadAggregatedTermsWithSources) {
        ExpectFailWithError("select key, 1 + a.subkey\n"
                            "from plato.Input1 as a\n"
                            "group by a.key;",
            "<main>:1:17: Error: Column `a.subkey` must either be a key column in GROUP BY or it should be used in aggregation function\n");
        ExpectFailWithError("select key, 1 + a.subkey.x\n"
                            "from plato.Input1 as a\n"
                            "group by a.key;",
            "<main>:1:17: Error: Column must either be a key column in GROUP BY or it should be used in aggregation function\n");
    }

    Y_UNIT_TEST(WarnForAggregationBySelectAlias) {
        NYql::TAstParseResult res = SqlToYql("select c + 1 as c from plato.Input\n"
                                             "group by  c");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res),
            "<main>:2:11: Warning: GROUP BY will aggregate by column `c` instead of aggregating by SELECT expression with same alias, code: 4532\n"
            "<main>:1:10: Warning: You should probably use alias in GROUP BY instead of using it here. Please consult documentation for more details, code: 4532\n");

        res = SqlToYql("select c + 1 as c from plato.Input\n"
                       "group by Math::Floor(c + 2) as c;");

        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res),
            "<main>:2:22: Warning: GROUP BY will aggregate by column `c` instead of aggregating by SELECT expression with same alias, code: 4532\n"
            "<main>:1:10: Warning: You should probably use alias in GROUP BY instead of using it here. Please consult documentation for more details, code: 4532\n");
    }

    Y_UNIT_TEST(NoWarnForAggregationBySelectAliasWhenAggrFunctionsAreUsedInAlias) {
        NYql::TAstParseResult res = SqlToYql("select\n"
                                             "    cast(avg(val) as int) as value,\n"
                                             "    value as key\n"
                                             "from\n"
                                             "    plato.Input\n"
                                             "group by value");

        UNIT_ASSERT(res.Root);
        UNIT_ASSERT(res.Issues.Size() == 0);

        res = SqlToYql("select\n"
                       "    cast(avg(val) over w as int) as value,\n"
                       "    value as key\n"
                       "from\n"
                       "    plato.Input\n"
                       "group by value\n"
                       "window w as ()");

        UNIT_ASSERT(res.Root);
        UNIT_ASSERT(res.Issues.Size() == 0);
    }

    Y_UNIT_TEST(NoWarnForAggregationBySelectAliasWhenQualifiedNameIsUsed) {
        NYql::TAstParseResult res = SqlToYql("select\n"
                                             "  Unwrap(a.key) as key\n"
                                             "from plato.Input as a\n"
                                             "join plato.Input2 as b using(k)\n"
                                             "group by a.key;");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT(res.Issues.Size() == 0);

        res = SqlToYql("select Unwrap(a.key) as key\n"
                       "from plato.Input as a\n"
                       "group by a.key;");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT(res.Issues.Size() == 0);
    }

    Y_UNIT_TEST(NoWarnForAggregationBySelectAliasWhenTrivialRenamingIsUsed) {
        NYql::TAstParseResult res = SqlToYql("select a.key as key\n"
                                             "from plato.Input as a\n"
                                             "group by key;");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT(res.Issues.Size() == 0);

        res = SqlToYql("select key as key\n"
                       "from plato.Input\n"
                       "group by key;");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT(res.Issues.Size() == 0);
    }

    Y_UNIT_TEST(ErrorByAggregatingByExpressionWithSameExpressionInSelect) {
        ExpectFailWithError("select k * 2 from plato.Input group by k * 2",
            "<main>:1:8: Error: Column `k` must either be a key column in GROUP BY or it should be used in aggregation function\n");
    }

    Y_UNIT_TEST(ErrorForAggregationBySelectAlias) {
        ExpectFailWithError("select key, Math::Floor(1.1 + a.subkey) as foo\n"
                            "from plato.Input as a\n"
                            "group by a.key, foo;",
            "<main>:3:17: Warning: GROUP BY will aggregate by column `foo` instead of aggregating by SELECT expression with same alias, code: 4532\n"
            "<main>:1:19: Warning: You should probably use alias in GROUP BY instead of using it here. Please consult documentation for more details, code: 4532\n"
            "<main>:1:31: Error: Column `a.subkey` must either be a key column in GROUP BY or it should be used in aggregation function\n");

        ExpectFailWithError("select c + 1 as c from plato.Input\n"
                            "group by Math::Floor(c + 2);",
            "<main>:2:22: Warning: GROUP BY will aggregate by column `c` instead of aggregating by SELECT expression with same alias, code: 4532\n"
            "<main>:1:10: Warning: You should probably use alias in GROUP BY instead of using it here. Please consult documentation for more details, code: 4532\n"
            "<main>:1:8: Error: Column `c` must either be a key column in GROUP BY or it should be used in aggregation function\n");
    }

    Y_UNIT_TEST(SelectWithDuplicateGroupingColumns) {
        NYql::TAstParseResult res = SqlToYql("select c from plato.Input group by c, c");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: Duplicate grouping column: c\n");
    }

    Y_UNIT_TEST(SelectWithBadAggregationInGrouping) {
        NYql::TAstParseResult res = SqlToYql("select a, Min(b), c group by c");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: Column references are not allowed without FROM\n"
            "<main>:1:30: Error: Column reference 'c'\n");
    }

    Y_UNIT_TEST(SelectWithOpOnBadAggregation) {
        ExpectFailWithError("select 1 + a + Min(b) from plato.Input",
            "<main>:1:12: Error: Column `a` must either be a key column in GROUP BY or it should be used in aggregation function\n");
    }

    Y_UNIT_TEST(SelectOrderByConstantNum) {
        NYql::TAstParseResult res = SqlToYql("select a from plato.Input order by 1");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:36: Error: Unable to ORDER BY constant expression\n");
    }

    Y_UNIT_TEST(SelectOrderByConstantExpr) {
        NYql::TAstParseResult res = SqlToYql("select a from plato.Input order by 1 * 42");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:38: Error: Unable to ORDER BY constant expression\n");
    }

    Y_UNIT_TEST(SelectOrderByConstantString) {
        NYql::TAstParseResult res = SqlToYql("select a from plato.Input order by \"nest\"");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:36: Error: Unable to ORDER BY constant expression\n");
    }

    Y_UNIT_TEST(SelectOrderByAggregated) {
        NYql::TAstParseResult res = SqlToYql("select a from plato.Input order by min(a)");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:36: Error: Unable to ORDER BY aggregated values\n");
    }

    Y_UNIT_TEST(ErrorInOrderByExpresison) {
        NYql::TAstParseResult res = SqlToYql("select key, value from plato.Input order by (key as zey)");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:45: Error: You should use in ORDER BY column name, qualified field, callable function or expression\n");
    }

    Y_UNIT_TEST(ErrorsInOrderByWhenColumnIsMissingInProjection) {
        ExpectFailWithError("select subkey from (select 1 as subkey) order by key", "<main>:1:50: Error: Column key is not in source column set\n");
        ExpectFailWithError("select subkey from plato.Input as a order by x.key", "<main>:1:46: Error: Unknown correlation name: x\n");
        ExpectFailWithError("select distinct a, b from plato.Input order by c", "<main>:1:48: Error: Column c is not in source column set. Did you mean a?\n");
        ExpectFailWithError("select count(*) as a from plato.Input order by c", "<main>:1:48: Error: Column c is not in source column set. Did you mean a?\n");
        ExpectFailWithError("select count(*) as a, b, from plato.Input group by b order by c", "<main>:1:63: Error: Column c is not in source column set. Did you mean a?\n");
        UNIT_ASSERT(SqlToYql("select a, b from plato.Input order by c").IsOk());
    }

    Y_UNIT_TEST(SelectAggregatedWhere) {
        NYql::TAstParseResult res = SqlToYql("select * from plato.Input where count(key)");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:33: Error: Can not use aggregated values in filtering\n");
    }

    Y_UNIT_TEST(DoubleFrom) {
        NYql::TAstParseResult res = SqlToYql("from plato.Input select * from plato.Input");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:27: Error: Only one FROM clause is allowed\n");
    }

    Y_UNIT_TEST(SelectJoinMissingCorrName) {
        NYql::TAstParseResult res = SqlToYql("select * from plato.Input1 as a join plato.Input2 as b on a.key == key");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:65: Error: JOIN: column requires correlation name\n");
    }

    Y_UNIT_TEST(SelectJoinMissingCorrName1) {
        NYql::TAstParseResult res = SqlToYql(
            "use plato;\n"
            "$foo = select * from Input1;\n"
            "select * from Input2 join $foo USING(key);\n"
        );
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:27: Error: JOIN: missing correlation name for source\n");
    }

    Y_UNIT_TEST(SelectJoinMissingCorrName2) {
        NYql::TAstParseResult res = SqlToYql(
            "use plato;\n"
            "$foo = select * from Input1;\n"
            "select * from Input2 cross join $foo;\n"
        );
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:33: Error: JOIN: missing correlation name for source\n");
    }

    Y_UNIT_TEST(SelectJoinEmptyCorrNames) {
        NYql::TAstParseResult res = SqlToYql(
            "$left = (SELECT * FROM plato.Input1 LIMIT 2);\n"
            "$right = (SELECT * FROM plato.Input2 LIMIT 2);\n"
            "SELECT * FROM $left FULL JOIN $right USING (key);\n"
        );
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:45: Error: At least one correlation name is required in join\n");
    }

    Y_UNIT_TEST(SelectJoinSameCorrNames) {
        NYql::TAstParseResult res = SqlToYql("SELECT Input.key FROM plato.Input JOIN plato.Input1 ON Input.key == Input.subkey\n");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:66: Error: JOIN: different correlation names are required for joined tables\n");
    }

    Y_UNIT_TEST(SelectJoinConstPredicateArg) {
        NYql::TAstParseResult res = SqlToYql("SELECT * FROM plato.Input1 as A JOIN plato.Input2 as B ON A.key == B.key AND A.subkey == \"wtf\"\n");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:87: Error: JOIN: each equality predicate argument must depend on exactly one JOIN input\n");
    }

    Y_UNIT_TEST(SelectJoinNonEqualityPredicate) {
        NYql::TAstParseResult res = SqlToYql("SELECT * FROM plato.Input1 as A JOIN plato.Input2 as B ON A.key == B.key AND A.subkey > B.subkey\n");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:87: Error: JOIN ON expression must be a conjunction of equality predicates\n");
    }

    Y_UNIT_TEST(SelectEquiJoinCorrNameOutOfScope) {
        NYql::TAstParseResult res = SqlToYql(
            "PRAGMA equijoin;\n"
            "SELECT * FROM plato.A JOIN plato.B ON A.key == C.key JOIN plato.C ON A.subkey == C.subkey;\n"
        );
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:45: Error: JOIN: can not use source: C in equality predicate, it is out of current join scope\n");
    }

    Y_UNIT_TEST(SelectEquiJoinNoRightSource) {
        NYql::TAstParseResult res = SqlToYql(
            "PRAGMA equijoin;\n"
            "SELECT * FROM plato.A JOIN plato.B ON A.key == B.key JOIN plato.C ON A.subkey == B.subkey;\n"
        );
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:79: Error: JOIN ON equality predicate must have one of its arguments from the rightmost source\n");
    }

    Y_UNIT_TEST(SelectEquiJoinOuterWithoutType) {
        NYql::TAstParseResult res = SqlToYql(
            "SELECT * FROM plato.A Outer JOIN plato.B ON A.key == B.key;\n"
        );
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:23: Error: Invalid join type: OUTER JOIN. OUTER keyword is optional and can only be used after LEFT, RIGHT or FULL\n");
    }

    Y_UNIT_TEST(SelectEquiJoinOuterWithWrongType) {
        NYql::TAstParseResult res = SqlToYql(
            "SELECT * FROM plato.A LEFT semi OUTER JOIN plato.B ON A.key == B.key;\n"
            );
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:33: Error: Invalid join type: LEFT SEMI OUTER JOIN. OUTER keyword is optional and can only be used after LEFT, RIGHT or FULL\n");
    }

    Y_UNIT_TEST(InsertNoCluster) {
        NYql::TAstParseResult res = SqlToYql("insert into Output (foo) values (1)");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: No cluster name given and no default cluster is selected\n");
    }

    Y_UNIT_TEST(InsertValuesNoLabels) {
        NYql::TAstParseResult res = SqlToYql("insert into plato.Output values (1)");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:19: Error: INSERT INTO ... VALUES requires specification of table columns\n");
    }

    Y_UNIT_TEST(UpsertValuesNoLabelsKikimr) {
        NYql::TAstParseResult res = SqlToYql("upsert into plato.Output values (1)", 10, TString(NYql::KikimrProviderName));
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:19: Error: UPSERT INTO ... VALUES requires specification of table columns\n");
    }

    Y_UNIT_TEST(ReplaceValuesNoLabelsKikimr) {
        NYql::TAstParseResult res = SqlToYql("replace into plato.Output values (1)", 10, TString(NYql::KikimrProviderName));
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:20: Error: REPLACE INTO ... VALUES requires specification of table columns\n");
    }

    Y_UNIT_TEST(InsertValuesInvalidLabels) {
        NYql::TAstParseResult res = SqlToYql("insert into plato.Output (foo) values (1, 2)");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:27: Error: VALUES have 2 columns, INSERT INTO expects: 1\n");
    }

    Y_UNIT_TEST(BuiltinFileOpNoArgs) {
        NYql::TAstParseResult res = SqlToYql("select FilePath()");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: FilePath() requires exactly 1 arguments, given: 0\n");
    }

    Y_UNIT_TEST(ProcessWithHaving) {
        NYql::TAstParseResult res = SqlToYql("process plato.Input using some::udf(value) having value == 1");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:15: Error: PROCESS does not allow HAVING yet! You may request it on yql@ maillist.\n");
    }

    Y_UNIT_TEST(ReduceNoBy) {
        NYql::TAstParseResult res = SqlToYql("reduce plato.Input using some::udf(value)");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:19: Error: Unexpected token absence : Missing ON \n\n<main>:1:25: Error: Unexpected token absence : Missing USING \n\n");
    }

    Y_UNIT_TEST(ReduceDistinct) {
        NYql::TAstParseResult res = SqlToYql("reduce plato.Input on key using some::udf(distinct value)");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:43: Error: DISTINCT can not be used in PROCESS/REDUCE\n");
    }

    Y_UNIT_TEST(CreateTableWithView) {
        NYql::TAstParseResult res = SqlToYql("CREATE TABLE plato.foo:bar (key INT);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:22: Error: Unexpected token ':' : syntax error...\n\n");
    }

    Y_UNIT_TEST(AsteriskWithSomethingAfter) {
        NYql::TAstParseResult res = SqlToYql("select *, LENGTH(value) from plato.Input;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: Unable to use plain '*' with other projection items. Please use qualified asterisk instead: '<table>.*' (<table> can be either table name or table alias).\n");
    }

    Y_UNIT_TEST(AsteriskWithSomethingBefore) {
        NYql::TAstParseResult res = SqlToYql("select LENGTH(value), * from plato.Input;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:23: Error: Unable to use plain '*' with other projection items. Please use qualified asterisk instead: '<table>.*' (<table> can be either table name or table alias).\n");
    }

    Y_UNIT_TEST(DuplicatedQualifiedAsterisk) {
        NYql::TAstParseResult res = SqlToYql("select in.*, key, in.* from plato.Input as in;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:19: Error: Unable to use twice same quialified asterisk. Invalid source: in\n");
    }

    Y_UNIT_TEST(BrokenLabel) {
        NYql::TAstParseResult res = SqlToYql("select in.*, key as `funny.label` from plato.Input as in;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:14: Error: Unable to use '.' in column name. Invalid column name: funny.label\n");
    }

    Y_UNIT_TEST(KeyConflictDetect0) {
        NYql::TAstParseResult res = SqlToYql("select key, in.key as key from plato.Input as in;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:13: Error: Unable to use duplicate column names. Collision in name: key\n");
    }

    Y_UNIT_TEST(KeyConflictDetect1) {
        NYql::TAstParseResult res = SqlToYql("select length(key) as key, key from plato.Input;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:28: Error: Unable to use duplicate column names. Collision in name: key\n");
    }

    Y_UNIT_TEST(KeyConflictDetect2) {
        NYql::TAstParseResult res = SqlToYql("select key, in.key from plato.Input as in;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: Duplicate column: key\n");
    }

    Y_UNIT_TEST(AutogenerationAliasWithCollisionConflict1) {
        UNIT_ASSERT(SqlToYql("select LENGTH(Value), key as column0 from plato.Input;").IsOk());
    }

    Y_UNIT_TEST(AutogenerationAliasWithCollisionConflict2) {
        UNIT_ASSERT(SqlToYql("select key as column1, LENGTH(Value) from plato.Input;").IsOk());
    }

    Y_UNIT_TEST(MissedSourceTableForQualifiedAsteriskOnSimpleSelect) {
        NYql::TAstParseResult res = SqlToYql("use plato; select Intop.*, Input.key from plato.Input;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:19: Error: Unknown correlation name: Intop\n");
    }

    Y_UNIT_TEST(MissedSourceTableForQualifiedAsteriskOnJoin) {
        NYql::TAstParseResult res = SqlToYql("use plato; select tmissed.*, t2.*, t1.key from plato.Input as t1 join plato.Input as t2 on t1.key==t2.key;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:19: Error: Unknown correlation name for asterisk: tmissed\n");
    }

    Y_UNIT_TEST(UnableToReferenceOnNotExistSubcolumn) {
        NYql::TAstParseResult res = SqlToYql("select b.subkey from (select key from plato.Input as a) as b;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: Column subkey is not in source column set\n");
    }

    Y_UNIT_TEST(ConflictOnSameNameWithQualify0) {
        NYql::TAstParseResult res = SqlToYql("select in.key, in.key as key from plato.Input as in;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: Duplicate column: key\n");
    }

    Y_UNIT_TEST(ConflictOnSameNameWithQualify1) {
        NYql::TAstParseResult res = SqlToYql("select in.key, length(key) as key from plato.Input as in;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: Duplicate column: key\n");
    }

    Y_UNIT_TEST(ConflictOnSameNameWithQualify2) {
        NYql::TAstParseResult res = SqlToYql("select key, in.key from plato.Input as in;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: Duplicate column: key\n");
    }

    Y_UNIT_TEST(ConflictOnSameNameWithQualify3) {
        NYql::TAstParseResult res = SqlToYql("select in.key, subkey as key from plato.Input as in;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: Duplicate column: key\n");
    }

    Y_UNIT_TEST(SelectFlattenBySameColumns) {
        NYql::TAstParseResult res = SqlToYql("select key from plato.Input flatten by (key, key as kk)");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:46: Error: Duplicate column name found: key in FlattenBy section\n");
    }

    Y_UNIT_TEST(SelectFlattenBySameAliases) {
        NYql::TAstParseResult res = SqlToYql("select key from plato.Input flatten by (key as kk, subkey as kk);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:52: Error: Duplicate alias found: kk in FlattenBy section\n");
    }

    Y_UNIT_TEST(SelectFlattenByExprSameAliases) {
        NYql::TAstParseResult res = SqlToYql("select key from plato.Input flatten by (key as kk, ListSkip(subkey,1) as kk);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:52: Error: Collision between alias and column name: kk in FlattenBy section\n");
    }

    Y_UNIT_TEST(SelectFlattenByConflictNameAndAlias0) {
        NYql::TAstParseResult res = SqlToYql("select key from plato.Input flatten by (key, subkey as key);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:46: Error: Collision between alias and column name: key in FlattenBy section\n");
    }

    Y_UNIT_TEST(SelectFlattenByConflictNameAndAlias1) {
        NYql::TAstParseResult res = SqlToYql("select key from plato.Input flatten by (key as kk, subkey as key);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:52: Error: Collision between alias and column name: key in FlattenBy section\n");
    }

    Y_UNIT_TEST(SelectFlattenByExprConflictNameAndAlias1) {
        NYql::TAstParseResult res = SqlToYql("select key from plato.Input flatten by (key as kk, ListSkip(subkey,1) as key);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:52: Error: Duplicate column name found: key in FlattenBy section\n");
    }

    Y_UNIT_TEST(SelectFlattenByUnnamedExpr) {
        NYql::TAstParseResult res = SqlToYql("select key from plato.Input flatten by (key, ListSkip(key, 1))");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:46: Error: Unnamed expression after FLATTEN BY is not allowed\n");
    }

    Y_UNIT_TEST(UseInOnStrings) {
        NYql::TAstParseResult res = SqlToYql("select * from plato.Input where \"foo\" in \"foovalue\";");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:42: Error: Unable to use IN predicate with string argument, it won't search substring - "
                                          "expecting tuple, list, dict or single column table source\n");
    }

    Y_UNIT_TEST(UseSubqueryInScalarContextInsideIn) {
        NYql::TAstParseResult res = SqlToYql("$q = (select key from plato.Input); select * from plato.Input where subkey in ($q);");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:79: Warning: Using subrequest in scalar context after IN, "
                                          "perhaps you should remove parenthesis here, code: 4501\n");
    }

    Y_UNIT_TEST(InHintsWithKeywordClash) {
        NYql::TAstParseResult res = SqlToYql("SELECT COMPACT FROM plato.Input WHERE COMPACT IN COMPACT `COMPACT`(1,2,3)");
        UNIT_ASSERT(!res.Root);
        // should try to parse last compact as call expression
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:58: Error: Unknown builtin: COMPACT\n");
    }

    Y_UNIT_TEST(ErrorColumnPosition) {
        NYql::TAstParseResult res = SqlToYql(
            "USE plato;\n"
            "SELECT \n"
            "value FROM (\n"
            "select key from Input\n"
            ");\n"
        );
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:1: Error: Column value is not in source column set\n");
    }

    Y_UNIT_TEST(PrimaryViewAbortMapReduce) {
        NYql::TAstParseResult res = SqlToYql("SELECT key FROM plato.Input VIEW PRIMARY KEY");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:17: Error: primary view is not supported for yt tables\n");
    }

    Y_UNIT_TEST(InsertAbortMapReduce) {
        NYql::TAstParseResult res = SqlToYql("INSERT OR ABORT INTO plato.Output SELECT key FROM plato.Input");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:0: Error: INSERT OR ABORT INTO is not supported for yt tables\n");
    }

    Y_UNIT_TEST(ReplaceIntoMapReduce) {
        NYql::TAstParseResult res = SqlToYql("REPLACE INTO plato.Output SELECT key FROM plato.Input");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:0: Error: Meaning of REPLACE INTO has been changed, now you should use INSERT INTO <table> WITH TRUNCATE ... for yt\n");
    }

    Y_UNIT_TEST(UpsertIntoMapReduce) {
        NYql::TAstParseResult res = SqlToYql("UPSERT INTO plato.Output SELECT key FROM plato.Input");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:0: Error: UPSERT INTO is not supported for yt tables\n");
    }

    Y_UNIT_TEST(UpdateMapReduce) {
        NYql::TAstParseResult res = SqlToYql("UPDATE plato.Output SET value = value + 1 WHERE key < 1");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:0: Error: UPDATE is unsupported for yt\n");
    }

    Y_UNIT_TEST(DeleteMapReduce) {
        NYql::TAstParseResult res = SqlToYql("DELETE FROM plato.Output WHERE key < 1");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:0: Error: DELETE is unsupported for yt\n");
    }

    Y_UNIT_TEST(ReplaceIntoWithTruncate) {
        NYql::TAstParseResult res = SqlToYql("REPLACE INTO plato.Output WITH TRUNCATE SELECT key FROM plato.Input");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:32: Error: Unable REPLACE INTO with truncate mode\n");
    }

    Y_UNIT_TEST(UpsertIntoWithTruncate) {
        NYql::TAstParseResult res = SqlToYql("UPSERT INTO plato.Output WITH TRUNCATE SELECT key FROM plato.Input");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:31: Error: Unable UPSERT INTO with truncate mode\n");
    }

    Y_UNIT_TEST(InsertIntoWithTruncateKikimr) {
        NYql::TAstParseResult res = SqlToYql("INSERT INTO plato.Output WITH TRUNCATE SELECT key FROM plato.Input", 10, TString(NYql::KikimrProviderName));
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:0: Error: INSERT INTO WITH TRUNCATE is not supported for kikimr tables\n");
    }

    Y_UNIT_TEST(InsertIntoWithWrongArgumentCount) {
        NYql::TAstParseResult res = SqlToYql("insert into plato.Output with truncate (key, value, subkey) values (5, '1', '2', '3');");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:53: Error: VALUES have 4 columns, INSERT INTO ... WITH TRUNCATE expects: 3\n");
    }

    Y_UNIT_TEST(UpsertWithWrongArgumentCount) {
        NYql::TAstParseResult res = SqlToYql("upsert into plato.Output (key, value, subkey) values (2, '3');", 10, TString(NYql::KikimrProviderName));
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:39: Error: VALUES have 2 columns, UPSERT INTO expects: 3\n");
    }

    Y_UNIT_TEST(GroupingSetByExprWithoutAlias) {
        NYql::TAstParseResult res = SqlToYql("SELECT key FROM plato.Input GROUP BY GROUPING SETS (cast(key as uint32), subkey);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:53: Error: Unnamed expressions are not supported in GROUPING SETS. Please use '<expr> AS <name>'.\n");
    }

    Y_UNIT_TEST(GroupingSetByExprWithoutAlias2) {
        NYql::TAstParseResult res = SqlToYql("SELECT key FROM plato.Input GROUP BY subkey || subkey, GROUPING SETS (\n"
                                             "cast(key as uint32), subkey);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:1: Error: Unnamed expressions are not supported in GROUPING SETS. Please use '<expr> AS <name>'.\n");
    }

    Y_UNIT_TEST(CubeByExprWithoutAlias) {
        NYql::TAstParseResult res = SqlToYql("SELECT key FROM plato.Input GROUP BY CUBE (key, subkey / key);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:56: Error: Unnamed expressions are not supported in CUBE. Please use '<expr> AS <name>'.\n");
    }

    Y_UNIT_TEST(RollupByExprWithoutAlias) {
        NYql::TAstParseResult res = SqlToYql("SELECT key FROM plato.Input GROUP BY ROLLUP (subkey / key);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:53: Error: Unnamed expressions are not supported in ROLLUP. Please use '<expr> AS <name>'.\n");
    }

    Y_UNIT_TEST(GroupByHugeCubeDeniedNoPragma) {
        NYql::TAstParseResult res = SqlToYql("SELECT key FROM plato.Input GROUP BY CUBE (key, subkey, value, key + subkey as sum, key - subkey as sub, key + val as keyval);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:119: Error: GROUP BY CUBE is allowed only for 5 columns, but you use 6\n");
    }

    Y_UNIT_TEST(GroupByInvalidPragma) {
        NYql::TAstParseResult res = SqlToYql("PRAGMA GroupByCubeLimit = '-4';");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:27: Error: Expected unsigned integer literal as a single argument for: GroupByCubeLimit\n");
    }

    Y_UNIT_TEST(GroupByHugeCubeDeniedPragme) {
        NYql::TAstParseResult res = SqlToYql("PRAGMA GroupByCubeLimit = '4'; SELECT key FROM plato.Input GROUP BY CUBE (key, subkey, value, key + subkey as sum, key - subkey as sub);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:132: Error: GROUP BY CUBE is allowed only for 4 columns, but you use 5\n");
    }

    Y_UNIT_TEST(GroupByFewBigCubes) {
        NYql::TAstParseResult res = SqlToYql("SELECT key FROM plato.Input GROUP BY CUBE(key, subkey, key + subkey as sum), CUBE(value, value + key + subkey as total);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: Unable to GROUP BY more than 64 groups, you try use 80 groups\n");
    }

    Y_UNIT_TEST(GroupByFewBigCubesWithPragmaLimit) {
        NYql::TAstParseResult res = SqlToYql("PRAGMA GroupByLimit = '16'; SELECT key FROM plato.Input GROUP BY GROUPING SETS(key, subkey, key + subkey as sum), ROLLUP(value, value + key + subkey as total);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:29: Error: Unable to GROUP BY more than 16 groups, you try use 18 groups\n");
    }

    Y_UNIT_TEST(NoGroupingColumn0) {
        NYql::TAstParseResult res = SqlToYql(
            "select count(1), key_first, val_first, grouping(key_first, val_first, nomind) as group\n"
            "from plato.Input group by grouping sets (cast(key as uint32) /100 as key_first, Substring(value, 1, 1) as val_first);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:71: Error: Column 'nomind' is not a grouping column\n");
    }

    Y_UNIT_TEST(NoGroupingColumn1) {
        NYql::TAstParseResult res = SqlToYql("select count(1), grouping(key, value) as group_duo from plato.Input group by cube (key, subkey);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:32: Error: Column 'value' is not a grouping column\n");
    }

    Y_UNIT_TEST(EmptyAccess0) {
        NYql::TAstParseResult res = SqlToYql("insert into plato.Output (list0, list1) values (AsList(0, 1, 2), AsList(``));");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:73: Error: Column reference \"\" is not allowed in current scope\n");
    }

    Y_UNIT_TEST(EmptyAccess1) {
        NYql::TAstParseResult res = SqlToYql("insert into plato.Output (list0, list1) values (AsList(0, 1, 2), ``);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:66: Error: Column reference \"\" is not allowed in current scope\n");
    }

    Y_UNIT_TEST(UseUnknownColumnInInsert) {
        NYql::TAstParseResult res = SqlToYql("insert into plato.Output (list0, list1) values (AsList(0, 1, 2), AsList(`test`));");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:73: Error: Column reference \"test\" is not allowed in current scope\n");
    }

    Y_UNIT_TEST(GroupByEmptyColumn) {
        NYql::TAstParseResult res = SqlToYql("select count(1) from plato.Input group by ``;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:43: Error: Column name can not be empty\n");
    }

    Y_UNIT_TEST(ConvertNumberOutOfBase) {
        NYql::TAstParseResult res = SqlToYql("select 0o80l;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: Failed to parse number from string: 0o80l, char: '8' is out of base: 8\n");
    }

    Y_UNIT_TEST(ConvertNumberOutOfRangeForInt64ButFitsInUint64) {
        NYql::TAstParseResult res = SqlToYql("select 0xc000000000000000l;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: Failed to parse 13835058055282163712 as integer literal of Int64 type: value out of range for Int64\n");
    }

    Y_UNIT_TEST(ConvertNumberOutOfRangeUint64) {
        NYql::TAstParseResult res = SqlToYql("select 0xc0000000000000000l;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: Failed to parse number from string: 0xc0000000000000000l, number limit overflow\n");

        res = SqlToYql("select 1234234543563435151456;\n");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: Failed to parse number from string: 1234234543563435151456, number limit overflow\n");
    }

    Y_UNIT_TEST(ConvertNumberNegativeOutOfRange) {
        NYql::TAstParseResult res = SqlToYql("select -9223372036854775808;\n"
                                             "select -9223372036854775809;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:8: Error: Failed to parse negative integer: -9223372036854775809, number limit overflow\n");
    }

    Y_UNIT_TEST(InvaildUsageReal0) {
        NYql::TAstParseResult res = SqlToYql("select .0;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:7: Error: Unexpected token '.' : cannot match to any predicted input...\n\n");
    }

    Y_UNIT_TEST(InvaildUsageReal1) {
        NYql::TAstParseResult res = SqlToYql("select .0f;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:7: Error: Unexpected token '.' : cannot match to any predicted input...\n\n");
    }

    Y_UNIT_TEST(InvaildUsageWinFunctionWithoutWindow) {
        NYql::TAstParseResult res = SqlToYql("select lead(key, 2) from plato.Input;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: Failed to use window function Lead without window specification\n");
    }

    Y_UNIT_TEST(DropTableWithIfExists) {
        NYql::TAstParseResult res = SqlToYql("DROP TABLE IF EXISTS plato.foo;");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("drop_if_exists"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(TooManyErrors) {
        const char* q = R"(
        USE plato;
        select A, B, C, D, E, F, G, H, I, J, K, L, M, N from (select b from `abc`);
)";

        NYql::TAstParseResult res = SqlToYql(q, 10);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res),
            R"(<main>:3:16: Error: Column A is not in source column set. Did you mean b?
<main>:3:19: Error: Column B is not in source column set. Did you mean b?
<main>:3:22: Error: Column C is not in source column set. Did you mean b?
<main>:3:25: Error: Column D is not in source column set. Did you mean b?
<main>:3:28: Error: Column E is not in source column set. Did you mean b?
<main>:3:31: Error: Column F is not in source column set. Did you mean b?
<main>:3:34: Error: Column G is not in source column set. Did you mean b?
<main>:3:37: Error: Column H is not in source column set. Did you mean b?
<main>:3:40: Error: Column I is not in source column set. Did you mean b?
<main>: Error: Too many issues, code: 1
)");
    };

    Y_UNIT_TEST(ShouldCloneBindingForNamedParameter) {
        NYql::TAstParseResult res = SqlToYql(R"($f = () -> {
    $value_type = TypeOf(1);
    $pair_type = StructType(
        TypeOf("2") AS key,
        $value_type AS value
    );

    RETURN TupleType(
        ListType($value_type),
        $pair_type);
};

select FormatType($f());
)");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(BlockedInvalidFrameBounds) {
        auto check = [](const TString& frame, const TString& err) {
            const TString prefix = "SELECT SUM(x) OVER w FROM plato.Input WINDOW w AS (PARTITION BY key ORDER BY subkey\n";
            NYql::TAstParseResult res = SqlToYql(prefix + frame + ")");
            UNIT_ASSERT(!res.Root);
            UNIT_ASSERT_NO_DIFF(Err2Str(res), err);
        };

        check("ROWS UNBOUNDED FOLLOWING", "<main>:2:5: Error: Frame cannot start from UNBOUNDED FOLLOWING\n");
        check("ROWS BETWEEN 5 PRECEDING AND UNBOUNDED PRECEDING", "<main>:2:29: Error: Frame cannot end with UNBOUNDED PRECEDING\n");
        check("ROWS BETWEEN CURRENT ROW AND 5 PRECEDING", "<main>:2:13: Error: Frame cannot start from CURRENT ROW and end with PRECEDING\n");
        check("ROWS BETWEEN 5 FOLLOWING AND CURRENT ROW", "<main>:2:14: Error: Frame cannot start from FOLLOWING and end with CURRENT ROW\n");
        check("ROWS BETWEEN 5 FOLLOWING AND 5 PRECEDING", "<main>:2:14: Error: Frame cannot start from FOLLOWING and end with PRECEDING\n");
    }

    Y_UNIT_TEST(BlockedRangeValueWithoutSingleOrderBy) {
        UNIT_ASSERT(SqlToYql("SELECT COUNT(*) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM plato.Input").IsOk());
        UNIT_ASSERT(SqlToYql("SELECT COUNT(*) OVER (RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM plato.Input").IsOk());

        auto res = SqlToYql("SELECT COUNT(*) OVER (RANGE 5 PRECEDING) FROM plato.Input");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:29: Error: RANGE with <offset> PRECEDING/FOLLOWING requires exactly one expression in ORDER BY partition clause\n");

        res = SqlToYql("SELECT COUNT(*) OVER (ORDER BY key, value RANGE 5 PRECEDING) FROM plato.Input");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:49: Error: RANGE with <offset> PRECEDING/FOLLOWING requires exactly one expression in ORDER BY partition clause\n");
    }

    Y_UNIT_TEST(NoColumnsInFrameBounds) {
        NYql::TAstParseResult res = SqlToYql(
            "SELECT SUM(x) OVER w FROM plato.Input WINDOW w AS (ROWS BETWEEN\n"
            " 1 + key PRECEDING AND 2 + key FOLLOWING);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:6: Error: Column reference \"key\" is not allowed in current scope\n");
    }

    Y_UNIT_TEST(WarnOnEmptyFrameBounds) {
        NYql::TAstParseResult res = SqlToYql(
            "SELECT SUM(x) OVER w FROM plato.Input WINDOW w AS (PARTITION BY key ORDER BY subkey\n"
            "ROWS BETWEEN 10 FOLLOWING AND 5 FOLLOWING)");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:14: Warning: Used frame specification implies empty window frame, code: 4520\n");
    }

    Y_UNIT_TEST(WarnOnRankWithUnorderedWindow) {
        NYql::TAstParseResult res = SqlToYql("SELECT RANK() OVER w FROM plato.Input WINDOW w AS ()");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Warning: Rank() is used with unordered window - all rows will be considered equal to each other, code: 4521\n");
    }

    Y_UNIT_TEST(WarnOnRankExprWithUnorderedWindow) {
        NYql::TAstParseResult res = SqlToYql("SELECT RANK(key) OVER w FROM plato.Input WINDOW w AS ()");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Warning: Rank(<expression>) is used with unordered window - the result is likely to be undefined, code: 4521\n");
    }

    Y_UNIT_TEST(AnyAsTableName) {
        NYql::TAstParseResult res = SqlToYql("use plato; select * from any;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:28: Error: Unexpected token ';' : syntax error...\n\n");
    }

    Y_UNIT_TEST(IncorrectOrderOfLambdaOptionalArgs) {
        NYql::TAstParseResult res = SqlToYql("$f = ($x?, $y)->($x + $y); select $f(1);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:12: Error: Non-optional argument can not follow optional one\n");
    }

    Y_UNIT_TEST(IncorrectOrderOfActionOptionalArgs) {
        NYql::TAstParseResult res = SqlToYql("define action $f($x?, $y) as select $x,$y; end define; do $f(1);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:23: Error: Non-optional argument can not follow optional one\n");
    }

    Y_UNIT_TEST(NotAllowedQuestionOnNamedNode) {
        NYql::TAstParseResult res = SqlToYql("$f = 1; select $f?;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:18: Error: Unexpected token '?' at the end of expression\n");
    }

    Y_UNIT_TEST(AnyAndCrossJoin) {
        NYql::TAstParseResult res = SqlToYql("use plato; select * from any Input1 cross join Input2");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:26: Error: ANY should not be used with Cross JOIN\n");

        res = SqlToYql("use plato; select * from Input1 cross join any Input2");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:44: Error: ANY should not be used with Cross JOIN\n");
    }

    Y_UNIT_TEST(AnyWithCartesianProduct) {
        NYql::TAstParseResult res = SqlToYql("pragma AnsiImplicitCrossJoin; use plato; select * from any Input1, Input2");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:56: Error: ANY should not be used with Cross JOIN\n");

        res = SqlToYql("pragma AnsiImplicitCrossJoin; use plato; select * from Input1, any Input2");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:64: Error: ANY should not be used with Cross JOIN\n");
    }

    Y_UNIT_TEST(ErrorPlainEndAsInlineActionTerminator) {
        NYql::TAstParseResult res = SqlToYql(
            "do begin\n"
            "  select 1\n"
            "; end\n");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:4:0: Error: Unexpected token absence : Missing DO \n\n");
    }

    Y_UNIT_TEST(ErrorMultiWayJoinWithUsing) {
        NYql::TAstParseResult res = SqlToYql(
            "USE plato;\n"
            "PRAGMA DisableSimpleColumns;\n"
            "SELECT *\n"
            "FROM Input1 AS a\n"
            "JOIN Input2 AS b USING(key)\n"
            "JOIN Input3 AS c ON a.key = c.key;\n"
        );
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res),
            "<main>:5:24: Error: Multi-way JOINs should be connected with ON clause instead of USING clause\n"
        );
    }

    Y_UNIT_TEST(RequireLabelInFlattenByWithDot) {
        NYql::TAstParseResult res = SqlToYql("select * from plato.Input flatten by x.y");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res),
            "<main>:1:40: Error: Unnamed expression after FLATTEN BY is not allowed\n"
        );
    }

    Y_UNIT_TEST(WarnUnnamedColumns) {
        NYql::TAstParseResult res = SqlToYql(
            "PRAGMA WarnUnnamedColumns;\n"
            "\n"
            "SELECT key, subkey, key || subkey FROM plato.Input ORDER BY subkey;\n");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:28: Warning: Autogenerated column name column2 will be used for expression, code: 4516\n");
    }

    Y_UNIT_TEST(WarnSourceColumnMismatch) {
        NYql::TAstParseResult res = SqlToYql(
            "insert into plato.Output (key, subkey, new_value, one_more_value) select key as Key, subkey, value, \"x\" from plato.Input;");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:51: Warning: Column names in SELECT don't match column specification in parenthesis. \"key\" doesn't match \"Key\". \"new_value\" doesn't match \"value\", code: 4517\n");
    }

    Y_UNIT_TEST(YtCaseInsensitive) {
        NYql::TAstParseResult res = SqlToYql("select * from PlatO.foo;");
        UNIT_ASSERT(res.Root);

        res = SqlToYql("use PlatO; select * from foo;");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(KikimrCaseSensitive) {
        NYql::TAstParseResult res = SqlToYql("select * from PlatO.foo;", 10, "kikimr");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:15: Error: Unknown cluster: PlatO\n");

        res = SqlToYql("use PlatO; select * from foo;", 10, "kikimr");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:5: Error: Unknown cluster: PlatO\n");
    }

    Y_UNIT_TEST(DiscoveryModeForbidden) {
        NYql::TAstParseResult res = SqlToYqlWithMode("insert into plato.Output select * from plato.range(\"\", Input1, Input4)", NSQLTranslation::ESqlMode::DISCOVERY);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:40: Error: range is not allowed in Discovery mode, code: 4600\n");

        res = SqlToYqlWithMode("insert into plato.Output select * from plato.like(\"\", \"Input%\")", NSQLTranslation::ESqlMode::DISCOVERY);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:40: Error: like is not allowed in Discovery mode, code: 4600\n");

        res = SqlToYqlWithMode("insert into plato.Output select * from plato.regexp(\"\", \"Input.\")", NSQLTranslation::ESqlMode::DISCOVERY);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:40: Error: regexp is not allowed in Discovery mode, code: 4600\n");

        res = SqlToYqlWithMode("insert into plato.Output select * from plato.filter(\"\", ($name) -> { return find($name, \"Input\") is not null; })", NSQLTranslation::ESqlMode::DISCOVERY);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:40: Error: filter is not allowed in Discovery mode, code: 4600\n");

        res = SqlToYqlWithMode("select Path from plato.folder(\"\") where Type == \"table\"", NSQLTranslation::ESqlMode::DISCOVERY);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:18: Error: folder is not allowed in Discovery mode, code: 4600\n");
    }

    Y_UNIT_TEST(YsonFuncWithoutArgs) {
        UNIT_ASSERT(SqlToYql("SELECT Yson::SerializeText(Yson::From());").IsOk());
    }

    Y_UNIT_TEST(CanNotUseOrderByInNonLastSelectInUnionAllChain) {
        auto req = "pragma AnsiOrderByLimitInUnionAll;\n"
                   "use plato;\n"
                   "\n"
                   "select * from Input order by key\n"
                   "union all\n"
                   "select * from Input order by key limit 1;";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:4:21: Error: ORDER BY within UNION ALL is only allowed after last subquery\n");
    }

    Y_UNIT_TEST(CanNotUseLimitInNonLastSelectInUnionAllChain) {
        auto req = "pragma AnsiOrderByLimitInUnionAll;\n"
                   "use plato;\n"
                   "\n"
                   "select * from Input limit 1\n"
                   "union all\n"
                   "select * from Input order by key limit 1;";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:4:21: Error: LIMIT within UNION ALL is only allowed after last subquery\n");
    }

    Y_UNIT_TEST(CanNotUseDiscardInNonFirstSelectInUnionAllChain) {
        auto req = "pragma AnsiOrderByLimitInUnionAll;\n"
                   "use plato;\n"
                   "\n"
                   "select * from Input\n"
                   "union all\n"
                   "discard select * from Input;";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:6:1: Error: DISCARD within UNION ALL is only allowed before first subquery\n");
    }

    Y_UNIT_TEST(CanNotUseIntoResultInNonLastSelectInUnionAllChain) {
        auto req = "use plato;\n"
                   "pragma AnsiOrderByLimitInUnionAll;\n"
                   "\n"
                   "select * from Input\n"
                   "union all\n"
                   "discard select * from Input;";

        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:6:1: Error: DISCARD within UNION ALL is only allowed before first subquery\n");
    }

    Y_UNIT_TEST(YsonStrictInvalidPragma) {
        auto res = SqlToYql("pragma yson.Strict = \"wrong\";");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:22: Error: Expected 'true', 'false' or no parameter for: Strict\n");
    }

    Y_UNIT_TEST(WarnTableNameInSomeContexts) {
        UNIT_ASSERT(SqlToYql("use plato; select TableName() from Input;").IsOk());
        UNIT_ASSERT(SqlToYql("use plato; select TableName(\"aaaa\");").IsOk());
        UNIT_ASSERT(SqlToYql("select TableName(\"aaaa\", \"yt\");").IsOk());

        auto res = SqlToYql("select TableName() from plato.Input;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: TableName requires either service name as second argument or current cluster name\n");

        res = SqlToYql("use plato;\n"
                       "select TableName() from Input1 as a join Input2 as b using(key);");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:8: Warning: TableName() may produce empty result when used in ambiguous context (with JOIN), code: 4525\n");

        res = SqlToYql("use plato;\n"
                       "select SOME(TableName()), key from Input group by key;");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:13: Warning: TableName() will produce empty result when used with aggregation.\n"
                                          "Please consult documentation for possible workaround, code: 4525\n");
    }

    Y_UNIT_TEST(WarnOnDistincWithHavingWithoutAggregations) {
        auto res = SqlToYql("select distinct key from plato.Input having key != '0';");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:49: Warning: The usage of HAVING without aggregations with SELECT DISTINCT is non-standard and will stop working soon. Please use WHERE instead., code: 4526\n");
    }

    Y_UNIT_TEST(FlattenByExprWithNestedNull) {
        auto res = SqlToYql("USE plato;\n"
                            "\n"
                            "SELECT * FROM (SELECT 1 AS region_id)\n"
                            "FLATTEN BY (\n"
                            "    CAST($unknown(region_id) AS List<String>) AS region\n"
                            ")");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:5:10: Error: Unknown name: $unknown\n");
    }

    Y_UNIT_TEST(EmptySymbolNameIsForbidden) {
        auto req = "    $`` = 1; select $``;";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:5: Error: Empty symbol name is not allowed\n");
    }

    Y_UNIT_TEST(WarnOnBinaryOpWithNullArg) {
        auto req = "select * from plato.Input where cast(key as Int32) != NULL";
        auto res = SqlToYql(req);
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:52: Warning: Binary operation != will return NULL here, code: 4529\n");

        req = "select 1 or null";
        res = SqlToYql(req);
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "");
    }

    Y_UNIT_TEST(ErrorIfTableSampleArgUsesColumns) {
        auto req = "SELECT key FROM plato.Input TABLESAMPLE BERNOULLI(MIN_OF(100.0, CAST(subkey as Int32)));";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:70: Error: Column reference \"subkey\" is not allowed in current scope\n");
    }

    Y_UNIT_TEST(DerivedColumnListForSelectIsNotSupportedYet) {
        auto req = "SELECT a,b,c FROM plato.Input as t(x,y,z);";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:35: Error: Derived column list is only supported for VALUES\n");
    }

    Y_UNIT_TEST(ErrorIfValuesHasDifferentCountOfColumns) {
        auto req = "VALUES (1,2,3), (4,5);";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:17: Error: All VALUES items should have same size: expecting 3, got 2\n");
    }

    Y_UNIT_TEST(ErrorIfDerivedColumnSizeExceedValuesColumnCount) {
        auto req = "SELECT * FROM(VALUES (1,2), (3,4)) as t(x,y,z);";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:40: Error: Derived column list size exceeds column count in VALUES\n");
    }

    Y_UNIT_TEST(WarnoOnAutogeneratedNamesForValues) {
        auto req = "PRAGMA WarnUnnamedColumns;\n"
                   "SELECT * FROM (VALUES (1,2,3,4), (5,6,7,8)) as t(x,y);";
        auto res = SqlToYql(req);
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:16: Warning: Autogenerated column names column2...column3 will be used here, code: 4516\n");
    }

    Y_UNIT_TEST(ErrUnionAllWithOrderByWithoutExplicitLegacyMode) {
        auto req = "use plato;\n"
                   "\n"
                   "select * from Input order by key\n"
                   "union all\n"
                   "select * from Input order by key;";

        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:21: Error: ORDER BY within UNION ALL is only allowed after last subquery\n");
    }

    Y_UNIT_TEST(ErrUnionAllWithLimitWithoutExplicitLegacyMode) {
        auto req = "use plato;\n"
                   "\n"
                   "select * from Input limit 10\n"
                   "union all\n"
                   "select * from Input limit 1;";

        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:21: Error: LIMIT within UNION ALL is only allowed after last subquery\n");
    }

    Y_UNIT_TEST(ErrUnionAllWithIntoResultWithoutExplicitLegacyMode) {
        auto req = "use plato;\n"
                   "\n"
                   "select * from Input into result aaa\n"
                   "union all\n"
                   "select * from Input;";

        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:21: Error: INTO RESULT within UNION ALL is only allowed after last subquery\n");
    }

    Y_UNIT_TEST(ErrUnionAllWithDiscardWithoutExplicitLegacyMode) {
        auto req = "use plato;\n"
                   "\n"
                   "select * from Input\n"
                   "union all\n"
                   "discard select * from Input;";

        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:5:1: Error: DISCARD within UNION ALL is only allowed before first subquery\n");
    }

    Y_UNIT_TEST(ErrUnionAllKeepsIgnoredOrderByWarning) {
        auto req = "use plato;\n"
                   "\n"
                   "SELECT * FROM (\n"
                   "  SELECT * FROM Input\n"
                   "  UNION ALL\n"
                   "  SELECT t.* FROM Input AS t ORDER BY t.key\n"
                   ");";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:4:3: Warning: ORDER BY without LIMIT in subquery will be ignored, code: 4504\n"
                                          "<main>:6:39: Error: Unknown correlation name: t\n");
    }

    Y_UNIT_TEST(InvalidTtlInterval) {
        auto req = R"(
            USE plato;
            CREATE TABLE tableName (Key Uint32, CreatedAt Timestamp, PRIMARY KEY (Key))
            WITH (TTL = 1 On CreatedAt);
        )";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:4:25: Error: Literal of Interval type is expected for TTL\n"
                                          "<main>:4:25: Error: Invalid TTL settings\n");
    }

    Y_UNIT_TEST(InvalidTtlUnit) {
        auto req = R"(
            USE plato;
            CREATE TABLE tableName (Key Uint32, CreatedAt Uint32, PRIMARY KEY (Key))
            WITH (TTL = Interval("P1D") On CreatedAt AS PICOSECONDS);
        )";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "<main>:4:56: Error: Unexpected token 'PICOSECONDS'");
    }

    Y_UNIT_TEST(InvalidChangefeedSink) {
        auto req = R"(
            USE plato;
            CREATE TABLE tableName (
                Key Uint32, PRIMARY KEY (Key),
                CHANGEFEED feedName WITH (SINK_TYPE = "S3", MODE = "KEYS_ONLY", FORMAT = "json")
            );
        )";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:5:55: Error: Unknown changefeed sink type: S3\n");
    }

    Y_UNIT_TEST(InvalidChangefeedSettings) {
        auto req = R"(
            USE plato;
            CREATE TABLE tableName (
                Key Uint32, PRIMARY KEY (Key),
                CHANGEFEED feedName WITH (SINK_TYPE = "local", FOO = "bar")
            );
        )";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:5:64: Error: Unknown changefeed setting: FOO\n");
    }

    Y_UNIT_TEST(InvalidChangefeedInitialScan) {
        auto req = R"(
            USE plato;
            CREATE TABLE tableName (
                Key Uint32, PRIMARY KEY (Key),
                CHANGEFEED feedName WITH (MODE = "KEYS_ONLY", FORMAT = "json", INITIAL_SCAN = "foo")
            );
        )";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:5:95: Error: Literal of Bool type is expected for INITIAL_SCAN\n");
    }

    Y_UNIT_TEST(InvalidChangefeedVirtualTimestamps) {
        auto req = R"(
            USE plato;
            CREATE TABLE tableName (
                Key Uint32, PRIMARY KEY (Key),
                CHANGEFEED feedName WITH (MODE = "KEYS_ONLY", FORMAT = "json", VIRTUAL_TIMESTAMPS = "foo")
            );
        )";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:5:101: Error: Literal of Bool type is expected for VIRTUAL_TIMESTAMPS\n");
    }

    Y_UNIT_TEST(InvalidChangefeedResolvedTimestamps) {
        auto req = R"(
            USE plato;
            CREATE TABLE tableName (
                Key Uint32, PRIMARY KEY (Key),
                CHANGEFEED feedName WITH (MODE = "KEYS_ONLY", FORMAT = "json", RESOLVED_TIMESTAMPS = "foo")
            );
        )";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:5:102: Error: Literal of Interval type is expected for RESOLVED_TIMESTAMPS\n");
    }

    Y_UNIT_TEST(InvalidChangefeedRetentionPeriod) {
        auto req = R"(
            USE plato;
            CREATE TABLE tableName (
                Key Uint32, PRIMARY KEY (Key),
                CHANGEFEED feedName WITH (MODE = "KEYS_ONLY", FORMAT = "json", RETENTION_PERIOD = "foo")
            );
        )";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:5:99: Error: Literal of Interval type is expected for RETENTION_PERIOD\n");
    }

    Y_UNIT_TEST(InvalidChangefeedTopicPartitions) {
        auto req = R"(
            USE plato;
            CREATE TABLE tableName (
                Key Uint32, PRIMARY KEY (Key),
                CHANGEFEED feedName WITH (MODE = "KEYS_ONLY", FORMAT = "json", TOPIC_MIN_ACTIVE_PARTITIONS = "foo")
            );
        )";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:5:110: Error: Literal of integer type is expected for TOPIC_MIN_ACTIVE_PARTITIONS\n");
    }

    Y_UNIT_TEST(InvalidChangefeedAwsRegion) {
        auto req = R"(
            USE plato;
            CREATE TABLE tableName (
                Key Uint32, PRIMARY KEY (Key),
                CHANGEFEED feedName WITH (MODE = "KEYS_ONLY", FORMAT = "json", AWS_REGION = true)
            );
        )";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:5:93: Error: Literal of String type is expected for AWS_REGION\n");
    }

    Y_UNIT_TEST(ErrJoinWithGroupingSetsWithoutCorrelationName) {
        auto req = "USE plato;\n"
                   "\n"
                   "SELECT k1, k2, subkey\n"
                   "FROM T1 AS a JOIN T2 AS b USING (key)\n"
                   "GROUP BY GROUPING SETS(\n"
                   "  (a.key as k1, b.subkey as k2),\n"
                   "  (k1),\n"
                   "  (subkey)\n"
                   ");";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:8:4: Error: Columns in grouping sets should have correlation name, error in key: subkey\n");
    }

    Y_UNIT_TEST(ErrJoinWithGroupByWithoutCorrelationName) {
        auto req = "USE plato;\n"
                   "\n"
                   "SELECT k1, k2,\n"
                   "    value\n"
                   "FROM T1 AS a JOIN T2 AS b USING (key)\n"
                   "GROUP BY a.key as k1, b.subkey as k2,\n"
                   "    value;";
        ExpectFailWithError(req,
            "<main>:7:5: Error: Columns in GROUP BY should have correlation name, error in key: value\n");
    }

    Y_UNIT_TEST(ErrWithMissingFrom) {
        auto req = "select 1 as key where 1 > 1;";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:25: Error: Filtering is not allowed without FROM\n");

        req = "select 1 + count(*);";
        res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:12: Error: Aggregation is not allowed without FROM\n");

        req = "select 1 as key, subkey + value;";
        res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: Column references are not allowed without FROM\n"
                                          "<main>:1:18: Error: Column reference 'subkey'\n"
                                          "<main>:1:1: Error: Column references are not allowed without FROM\n"
                                          "<main>:1:27: Error: Column reference 'value'\n");

        req = "select count(1) group by key;";
        res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: Column references are not allowed without FROM\n"
                                          "<main>:1:26: Error: Column reference 'key'\n");
    }

    Y_UNIT_TEST(ErrWithMissingFromForWindow) {
        auto req = "$c = () -> (1 + count(1) over w);\n"
                   "select $c();";
        ExpectFailWithError(req,
            "<main>:1:9: Error: Window and aggregation functions are not allowed in this context\n"
            "<main>:1:17: Error: Failed to use aggregation function Count without window specification or in wrong place\n");

        req = "$c = () -> (1 + lead(1) over w);\n"
              "select $c();";
        ExpectFailWithError(req,
            "<main>:1:17: Error: Window functions are not allowed in this context\n"
            "<main>:1:17: Error: Failed to use window function Lead without window specification or in wrong place\n");

        req = "select 1 + count(1) over w window w as ();";
        ExpectFailWithError(req,
            "<main>:1:1: Error: Window and aggregation functions are not allowed without FROM\n"
            "<main>:1:12: Error: Failed to use aggregation function Count without window specification or in wrong place\n");

        req = "select 1 + lead(1) over w window w as ();";
        ExpectFailWithError(req,
            "<main>:1:12: Error: Window functions are not allowed without FROM\n"
            "<main>:1:12: Error: Failed to use window function Lead without window specification or in wrong place\n");
    }

    Y_UNIT_TEST(ErrWithMissingFromForInplaceWindow) {
        auto req = "$c = () -> (1 + count(1) over ());\n"
                   "select $c();";
        ExpectFailWithError(req,
            "<main>:1:26: Error: Window and aggregation functions are not allowed in this context\n");

        req = "$c = () -> (1 + lead(1) over (rows between unbounded preceding and current row));\n"
              "select $c();";
        ExpectFailWithError(req,
            "<main>:1:25: Error: Window and aggregation functions are not allowed in this context\n");

        req = "select 1 + count(1) over ();";
        ExpectFailWithError(req,
            "<main>:1:1: Error: Window and aggregation functions are not allowed without FROM\n"
            "<main>:1:12: Error: Failed to use aggregation function Count without window specification or in wrong place\n");

        req = "select 1 + lead(1) over (rows between current row and unbounded following);";
        ExpectFailWithError(req,
            "<main>:1:12: Error: Window functions are not allowed without FROM\n"
            "<main>:1:12: Error: Failed to use window function Lead without window specification or in wrong place\n");
    }

    Y_UNIT_TEST(ErrDistinctInWrongPlace) {
        auto req = "select Some::Udf(distinct key) from plato.Input;";
        ExpectFailWithError(req,
            "<main>:1:18: Error: DISTINCT can only be used in aggregation functions\n");
        req = "select sum(key)(distinct foo) from plato.Input;";
        ExpectFailWithError(req,
            "<main>:1:17: Error: DISTINCT can only be used in aggregation functions\n");

        req = "select len(distinct foo) from plato.Input;";
        ExpectFailWithError(req,
            "<main>:1:8: Error: DISTINCT can only be used in aggregation functions\n");

        req = "$foo = ($x) -> ($x); select $foo(distinct key) from plato.Input;";
        ExpectFailWithError(req,
            "<main>:1:34: Error: DISTINCT can only be used in aggregation functions\n");
    }

    Y_UNIT_TEST(ErrForNotSingleChildInInlineAST) {
        ExpectFailWithError("select YQL::\"\"",
            "<main>:1:8: Error: Failed to parse YQL: expecting AST root node with single child, but got 0\n");
        ExpectFailWithError("select YQL::@@  \t@@",
            "<main>:1:8: Error: Failed to parse YQL: expecting AST root node with single child, but got 0\n");
        auto req = "$lambda = YQL::@@(lambda '(x)(+ x x)) (lambda '(y)(+ y y))@@;\n"
                   "select ListMap([1, 2, 3], $lambda);";
        ExpectFailWithError(req,
            "<main>:1:11: Error: Failed to parse YQL: expecting AST root node with single child, but got 2\n");
    }

    Y_UNIT_TEST(ErrEmptyColumnName) {
        ExpectFailWithError("select * without \"\" from plato.Input",
            "<main>:1:18: Error: String literal can not be used here\n");

        ExpectFailWithError("select * without `` from plato.Input;",
            "<main>:1:18: Error: Empty column name is not allowed\n");

        ExpectFailWithErrorForAnsiLexer("select * without \"\" from plato.Input",
            "<main>:1:18: Error: Empty column name is not allowed\n");

        ExpectFailWithErrorForAnsiLexer("select * without `` from plato.Input;",
            "<main>:1:18: Error: Empty column name is not allowed\n");
    }

    Y_UNIT_TEST(ErrOnNonZeroArgumentsForTableRows) {
        ExpectFailWithError("$udf=\"\";process plato.Input using $udf(TableRows(k))",
            "<main>:1:40: Error: TableRows requires exactly 0 arguments\n");
    }

    Y_UNIT_TEST(ErrGroupByWithAggregationFunctionAndDistinctExpr) {
        ExpectFailWithError("select * from plato.Input group by count(distinct key|key)",
            "<main>:1:36: Error: Unable to GROUP BY aggregated values\n");
    }

    // FIXME: check if we can get old behaviour
#if 0
    Y_UNIT_TEST(ErrWithSchemaWithColumnsWithoutType) {
        ExpectFailWithError("select * from plato.Input with COLUMNs",
            "<main>:1:32: Error: Expected type after COLUMNS\n"
            "<main>:1:32: Error: Failed to parse table hints\n");

        ExpectFailWithError("select * from plato.Input with scheMa",
            "<main>:1:32: Error: Expected type after SCHEMA\n"
            "<main>:1:32: Error: Failed to parse table hints\n");
    }
#endif

    Y_UNIT_TEST(ErrCollectPreaggregatedInListLiteralWithoutFrom) {
        ExpectFailWithError("SELECT([VARIANCE(DISTINCT[])])",
            "<main>:1:1: Error: Column references are not allowed without FROM\n"
            "<main>:1:9: Error: Column reference '_yql_preagg_Variance0'\n");
    }

    Y_UNIT_TEST(ErrGroupBySmartParenAsTuple) {
        ExpectFailWithError("SELECT * FROM plato.Input GROUP BY (k, v,)",
            "<main>:1:41: Error: Unexpected trailing comma in grouping elements list\n");
    }

    Y_UNIT_TEST(HandleNestedSmartParensInGroupBy) {
        ExpectFailWithError("SELECT * FROM plato.Input GROUP BY (+() as k)",
            "<main>:1:37: Error: Unable to GROUP BY constant expression\n");
    }

    Y_UNIT_TEST(ErrRenameWithAddColumn) {
        ExpectFailWithError("USE plato; ALTER TABLE table RENAME TO moved, ADD COLUMN addc uint64",
                            "<main>:1:40: Error: RENAME TO can not be used together with another table action\n");
    }

    Y_UNIT_TEST(ErrAddColumnAndRename) {
        // FIXME: fix positions in ALTER TABLE
        ExpectFailWithError("USE plato; ALTER TABLE table ADD COLUMN addc uint64, RENAME TO moved",
                            "<main>:1:46: Error: RENAME TO can not be used together with another table action\n");
    }

    Y_UNIT_TEST(InvalidUuidValue) {
        ExpectFailWithError("SELECT Uuid('123e4567ae89ba12d3aa456a426614174ab0')",
                            "<main>:1:8: Error: Invalid value \"123e4567ae89ba12d3aa456a426614174ab0\" for type Uuid\n");
        ExpectFailWithError("SELECT Uuid('123e4567ae89b-12d3-a456-426614174000')",
                            "<main>:1:8: Error: Invalid value \"123e4567ae89b-12d3-a456-426614174000\" for type Uuid\n");
    }

    Y_UNIT_TEST(WindowFunctionWithoutOver) {
        ExpectFailWithError("SELECT LAST_VALUE(foo) FROM plato.Input",
                            "<main>:1:8: Error: Can't use window function LastValue without window specification (OVER keyword is missing)\n");
        ExpectFailWithError("SELECT LAST_VALUE(foo) FROM plato.Input GROUP BY key",
                            "<main>:1:8: Error: Can't use window function LastValue without window specification (OVER keyword is missing)\n");
    }

    Y_UNIT_TEST(CreateAlterUserWithoutCluster) {
        ExpectFailWithError("\n CREATE USER user ENCRYPTED PASSWORD 'foobar';", "<main>:2:2: Error: USE statement is missing - no default cluster is selected\n");
        ExpectFailWithError("ALTER USER CURRENT_USER RENAME TO $foo;", "<main>:1:1: Error: USE statement is missing - no default cluster is selected\n");
    }

    Y_UNIT_TEST(ModifyPermissionsWithoutCluster) {
        ExpectFailWithError("\n GRANT CONNECT ON `/Root` TO user;", "<main>:2:2: Error: USE statement is missing - no default cluster is selected\n");
        ExpectFailWithError("\n REVOKE MANAGE ON `/Root` FROM user;", "<main>:2:2: Error: USE statement is missing - no default cluster is selected\n");
    }

    Y_UNIT_TEST(ReservedRoleNames) {
        ExpectFailWithError("USE plato; CREATE USER current_User;", "<main>:1:24: Error: System role CURRENT_USER can not be used here\n");
        ExpectFailWithError("USE plato; ALTER USER current_User RENAME TO Current_role", "<main>:1:46: Error: System role CURRENT_ROLE can not be used here\n");
        UNIT_ASSERT(SqlToYql("USE plato; DROP GROUP IF EXISTS a, b, c, current_User;").IsOk());
    }

    Y_UNIT_TEST(DisableClassicDivisionWithError) {
        ExpectFailWithError("pragma ClassicDivision = 'false'; select $foo / 30;", "<main>:1:42: Error: Unknown name: $foo\n");
    }

    Y_UNIT_TEST(AggregationOfAgrregatedDistinctExpr) {
        ExpectFailWithError("select sum(sum(distinct x + 1)) from plato.Input", "<main>:1:12: Error: Aggregation of aggregated values is forbidden\n");
    }

    Y_UNIT_TEST(WarnForUnusedSqlHint) {
        NYql::TAstParseResult res = SqlToYql("select * from plato.Input1 as a join /*+ merge() */ plato.Input2 as b using(key);\n"
                                             "select --+            foo(bar)\n"
                                             "       1;");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:23: Warning: Hint foo will not be used, code: 4534\n");
    }

    Y_UNIT_TEST(WarnForDeprecatedSchema) {
        NSQLTranslation::TTranslationSettings settings;
        settings.ClusterMapping["s3bucket"] = NYql::S3ProviderName;
        NYql::TAstParseResult res = SqlToYql("select * from s3bucket.`foo` with schema (col1 Int32, String as col2, Int64 as col3);", settings);
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_STRING_CONTAINS(res.Issues.ToString(), "Warning: Deprecated syntax for positional schema: please use 'column type' instead of 'type AS column', code: 4535\n");
    }

    Y_UNIT_TEST(ErrorOnColumnNameInMaxByLimit) {
        ExpectFailWithError(
            "SELECT AGGREGATE_BY(AsTuple(value, key), AggregationFactory(\"MAX_BY\", subkey)) FROM plato.Input;",
            "<main>:1:42: Error: Source does not allow column references\n"
            "<main>:1:71: Error: Column reference 'subkey'\n");
    }

    Y_UNIT_TEST(ErrorInLibraryWithTopLevelNamedSubquery) {
        TString withUnusedSubq = "$unused = select max(key) from plato.Input;\n"
                                 "\n"
                                 "define subquery $foo() as\n"
                                 "  $count = select count(*) from plato.Input;\n"
                                 "  select * from plato.Input limit $count / 2;\n"
                                 "end define;\n"
                                 "export $foo;\n";
        UNIT_ASSERT(SqlToYqlWithMode(withUnusedSubq, NSQLTranslation::ESqlMode::LIBRARY).IsOk());

        TString withTopLevelSubq = "$count = select count(*) from plato.Input;\n"
                                   "\n"
                                   "define subquery $foo() as\n"
                                   "  select * from plato.Input limit $count / 2;\n"
                                   "end define;\n"
                                   "export $foo;\n";
        auto res = SqlToYqlWithMode(withTopLevelSubq, NSQLTranslation::ESqlMode::LIBRARY);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:17: Error: Named subquery can not be used as a top level statement in libraries\n");
    }

    Y_UNIT_TEST(SessionStartAndSessionStateShouldSurviveSessionWindowArgsError){
        TString query = R"(
            $init = ($_row) -> (min(1, 2)); -- error: aggregation func min() can not be used here
            $calculate = ($_row, $_state) -> (1);
            $update = ($_row, $_state) -> (2);
            SELECT
                SessionStart() over w as session_start,
                SessionState() over w as session_state,
            FROM plato.Input as t
            WINDOW w AS (
                PARTITION BY user, SessionWindow(ts + 1, $init, $update, $calculate)
            )
        )";
        ExpectFailWithError(query, "<main>:2:33: Error: Aggregation function Min requires exactly 1 argument(s), given: 2\n");
    }
}

void CheckUnused(const TString& req, const TString& symbol, unsigned row, unsigned col) {
    auto res = SqlToYql(req);

    UNIT_ASSERT(res.Root);
    UNIT_ASSERT_NO_DIFF(Err2Str(res), TStringBuilder() << "<main>:" << row << ":" << col << ": Warning: Symbol " << symbol << " is not used, code: 4527\n");
}

Y_UNIT_TEST_SUITE(WarnUnused) {
    Y_UNIT_TEST(ActionOrSubquery) {
        TString req = "  $a()\n"
                      "as select 1;\n"
                      "end define;\n"
                      "\n"
                      "select 1;";
        CheckUnused("define action\n" + req, "$a", 2, 3);
        CheckUnused("define subquery\n" + req, "$a", 2, 3);
    }

    Y_UNIT_TEST(Import) {
        TString req = "import lib1 symbols\n"
                      "  $sqr;\n"
                      "select 1;";
        CheckUnused(req, "$sqr", 2, 3);

        req = "import lib1 symbols\n"
              "  $sqr as\n"
              "    $sq;\n"
              "select 1;";
        CheckUnused(req, "$sq", 3, 5);
    }

    Y_UNIT_TEST(NamedNodeStatement) {
        TString req = " $a, $a = AsTuple(1, 2);\n"
                      "select $a;";
        CheckUnused(req, "$a", 1, 2);
        req = "$a,  $b = AsTuple(1, 2);\n"
              "select $a;";
        CheckUnused(req, "$b", 1, 6);
        CheckUnused(" $a = 1; $a = 2; select $a;", "$a", 1, 2);
    }

    Y_UNIT_TEST(Declare) {
        CheckUnused("declare $a as String;select 1;", "$a", 1, 9);
    }

    Y_UNIT_TEST(ActionParams) {
        TString req = "define action $a($x, $y) as\n"
                      "  select $x;\n"
                      "end define;\n"
                      "\n"
                      "do $a(1,2);";
        CheckUnused(req, "$y", 1, 22);
    }

    Y_UNIT_TEST(SubqueryParams) {
        TString req = "use plato;\n"
                      "define subquery $q($name, $x) as\n"
                      "  select * from $name;\n"
                      "end define;\n"
                      "\n"
                      "select * from $q(\"Input\", 1);";
        CheckUnused(req, "$x", 2, 27);
    }

    Y_UNIT_TEST(For) {
        TString req = "define action $a() as\n"
                      "  select 1;\n"
                      "end define;\n"
                      "\n"
                      "for $i in ListFromRange(1, 10)\n"
                      "do $a();";
        CheckUnused(req, "$i", 5, 5);
    }

    Y_UNIT_TEST(LambdaParams) {
        TString req = "$lambda = ($x, $y) -> ($x);\n"
                      "select $lambda(1, 2);";
        CheckUnused(req, "$y", 1, 16);
    }

    Y_UNIT_TEST(InsideLambdaBody) {
        TString req = "$lambda = () -> {\n"
                      "  $x = 1; return 1;\n"
                      "};\n"
                      "select $lambda();";
        CheckUnused(req, "$x", 2, 3);
        req = "$lambda = () -> {\n"
              "  $x = 1; $x = 2; return $x;\n"
              "};\n"
              "select $lambda();";
        CheckUnused(req, "$x", 2, 3);
    }

    Y_UNIT_TEST(InsideAction) {
        TString req = "define action $a() as\n"
                      "  $x = 1; select 1;\n"
                      "end define;\n"
                      "\n"
                      "do $a();";
        CheckUnused(req, "$x", 2, 3);
        req = "define action $a() as\n"
              "  $x = 1; $x = 2; select $x;\n"
              "end define;\n"
              "\n"
              "do $a();";
        CheckUnused(req, "$x", 2, 3);
    }

    Y_UNIT_TEST(NoWarnOnNestedActions) {
        auto req = "pragma warning(\"error\", \"4527\");\n"
                   "define action $action($b) as\n"
                   "    define action $aaa() as\n"
                   "        select $b;\n"
                   "    end define;\n"
                   "    do $aaa();\n"
                   "end define;\n"
                   "\n"
                   "do $action(1);";
        UNIT_ASSERT(SqlToYql(req).IsOk());
    }

    Y_UNIT_TEST(NoWarnForUsageAfterSubquery) {
        auto req = "use plato;\n"
                   "pragma warning(\"error\", \"4527\");\n"
                   "\n"
                   "$a = 1;\n"
                   "\n"
                   "define subquery $q($table) as\n"
                   "  select * from $table;\n"
                   "end define;\n"
                   "\n"
                   "select * from $q(\"Input\");\n"
                   "select $a;";
        UNIT_ASSERT(SqlToYql(req).IsOk());
    }
}

Y_UNIT_TEST_SUITE(AnonymousNames) {
    Y_UNIT_TEST(ReferenceAnonymousVariableIsForbidden) {
        auto req = "$_ = 1; select $_;";

        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:16: Error: Unable to reference anonymous name $_\n");

        req = "$`_` = 1; select $`_`;";
        res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:18: Error: Unable to reference anonymous name $_\n");
    }

    Y_UNIT_TEST(Declare) {
        auto req = "declare $_ as String;";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:9: Error: Can not use anonymous name '$_' in DECLARE statement\n");
    }

    Y_UNIT_TEST(ActionSubquery) {
        auto req = "define action $_() as select 1; end define;";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:15: Error: Can not use anonymous name '$_' as ACTION name\n");

        req = "define subquery $_() as select 1; end define;";
        res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:17: Error: Can not use anonymous name '$_' as SUBQUERY name\n");
    }

    Y_UNIT_TEST(Import) {
        auto req = "import lib symbols $sqr as $_;";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:28: Error: Can not import anonymous name $_\n");
    }

    Y_UNIT_TEST(Export) {
        auto req = "export $_;";
        auto res = SqlToYqlWithMode(req, NSQLTranslation::ESqlMode::LIBRARY);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: Can not export anonymous name $_\n");
    }

    Y_UNIT_TEST(AnonymousInActionArgs) {
        auto req = "pragma warning(\"error\", \"4527\");\n"
                   "define action $a($_, $y, $_) as\n"
                   "  select $y;\n"
                   "end define;\n"
                   "\n"
                   "do $a(1,2,3);";
        UNIT_ASSERT(SqlToYql(req).IsOk());
    }

    Y_UNIT_TEST(AnonymousInSubqueryArgs) {
        auto req = "use plato;\n"
                   "pragma warning(\"error\", \"4527\");\n"
                   "define subquery $q($_, $y, $_) as\n"
                   "  select * from $y;\n"
                   "end define;\n"
                   "\n"
                   "select * from $q(1,\"Input\",3);";
        UNIT_ASSERT(SqlToYql(req).IsOk());
    }

    Y_UNIT_TEST(AnonymousInLambdaArgs) {
        auto req = "pragma warning(\"error\", \"4527\");\n"
                   "$lambda = ($_, $x, $_) -> ($x);\n"
                   "select $lambda(1,2,3);";
        UNIT_ASSERT(SqlToYql(req).IsOk());
    }

    Y_UNIT_TEST(AnonymousInFor) {
        auto req = "pragma warning(\"error\", \"4527\");\n"
                   "evaluate for $_ in ListFromRange(1, 10) do begin select 1; end do;";
        UNIT_ASSERT(SqlToYql(req).IsOk());
    }

    Y_UNIT_TEST(Assignment) {
        auto req = "pragma warning(\"error\", \"4527\");\n"
                   "$_ = 1;\n"
                   "$_, $x, $_ = AsTuple(1,2,3);\n"
                   "select $x;";
        UNIT_ASSERT(SqlToYql(req).IsOk());
    }
}

Y_UNIT_TEST_SUITE(JsonValue) {
    Y_UNIT_TEST(JsonValueArgumentCount) {
        NYql::TAstParseResult res = SqlToYql("select JSON_VALUE(CAST(@@{\"key\": 1238}@@ as Json));");

        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:49: Error: Unexpected token ')' : syntax error...\n\n");
    }

    Y_UNIT_TEST(JsonValueJsonPathMustBeLiteralString) {
        NYql::TAstParseResult res = SqlToYql("$jsonPath = \"strict $.key\"; select JSON_VALUE(CAST(@@{\"key\": 1238}@@ as Json), $jsonPath);");

        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:79: Error: Unexpected token absence : Missing STRING_VALUE \n\n");
    }

    Y_UNIT_TEST(JsonValueTranslation) {
        NYql::TAstParseResult res = SqlToYql("select JSON_VALUE(CAST(@@{\"key\": 1238}@@ as Json), \"strict $.key\");");

        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            Y_UNUSED(word);
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'\"strict $.key\""));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("SafeCast"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("DataType 'Json"));
        };

        TWordCountHive elementStat({"JsonValue"});
        VerifyProgram(res, elementStat, verifyLine);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["JsonValue"]);
    }

    Y_UNIT_TEST(JsonValueReturningSection) {
        for (const auto& typeName : {"Bool", "Int64", "Double", "String"}) {
            NYql::TAstParseResult res = SqlToYql(
                TStringBuilder() << "select JSON_VALUE(CAST(@@{\"key\": 1238}@@ as Json), \"strict $.key\" RETURNING " << typeName << ");"
            );

            UNIT_ASSERT(res.Root);

            TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
                Y_UNUSED(word);
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'\"strict $.key\""));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("SafeCast"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("DataType 'Json"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(TStringBuilder() << "DataType '" << typeName));
            };

            TWordCountHive elementStat({typeName});
            VerifyProgram(res, elementStat, verifyLine);
            UNIT_ASSERT(elementStat[typeName] > 0);
        }
    }

    Y_UNIT_TEST(JsonValueInvalidReturningType) {
        NYql::TAstParseResult res = SqlToYql("select JSON_VALUE(CAST(@@{'key': 1238}@@ as Json), 'strict $.key' RETURNING invalid);");

        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:77: Error: Unknown simple type 'invalid'\n");
    }

    Y_UNIT_TEST(JsonValueAndReturningInExpressions) {
        NYql::TAstParseResult res = SqlToYql(
            "USE plato\n;"
            "$json_value = \"some string\";\n"
            "SELECT $json_value;\n"
            "SELECT 1 as json_value;\n"
            "SELECT $json_value as json_value;\n"
            "$returning = \"another string\";\n"
            "SELECT $returning;\n"
            "SELECT 1 as returning;\n"
            "SELECT $returning as returning;\n"
        );

        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(JsonValueValidCaseHandlers) {
        const TVector<std::pair<TString, TString>> testCases = {
            {"", "'DefaultValue (Null)"},
            {"NULL", "'DefaultValue (Null)"},
            {"ERROR", "'Error (Null)"},
            {"DEFAULT 123", "'DefaultValue (Int32 '\"123\")"},
        };

        for (const auto& onEmpty : testCases) {
            for (const auto& onError : testCases) {
                TStringBuilder query;
                query << "$json = CAST(@@{\"key\": 1238}@@ as Json);\n"
                    << "SELECT JSON_VALUE($json, \"strict $.key\"";
                if (!onEmpty.first.Empty()) {
                    query << " " << onEmpty.first << " ON EMPTY";
                }
                if (!onError.first.Empty()) {
                    query << " " << onError.first << " ON ERROR";
                }
                query << ");\n";

                NYql::TAstParseResult res = SqlToYql(query);

                UNIT_ASSERT(res.Root);

                TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
                    Y_UNUSED(word);
                    UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(onEmpty.second + " " + onError.second));
                };

                TWordCountHive elementStat({"JsonValue"});
                VerifyProgram(res, elementStat, verifyLine);
                UNIT_ASSERT(elementStat["JsonValue"] > 0);
            }
        }
    }

    Y_UNIT_TEST(JsonValueTooManyCaseHandlers) {
        NYql::TAstParseResult res = SqlToYql(
            "select JSON_VALUE(CAST(@@{\"key\": 1238}@@ as Json), \"strict $.key\" NULL ON EMPTY NULL ON ERROR NULL ON EMPTY);\n"
        );

        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(
            Err2Str(res),
            "<main>:1:52: Error: Only 1 ON EMPTY and/or 1 ON ERROR clause is expected\n"
        );
    }

    Y_UNIT_TEST(JsonValueTooManyOnEmpty) {
        NYql::TAstParseResult res = SqlToYql(
            "select JSON_VALUE(CAST(@@{\"key\": 1238}@@ as Json), \"strict $.key\" NULL ON EMPTY NULL ON EMPTY);\n"
        );

        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(
            Err2Str(res),
            "<main>:1:52: Error: Only 1 ON EMPTY clause is expected\n"
        );
    }

    Y_UNIT_TEST(JsonValueTooManyOnError) {
        NYql::TAstParseResult res = SqlToYql(
            "select JSON_VALUE(CAST(@@{\"key\": 1238}@@ as Json), \"strict $.key\" NULL ON ERROR NULL ON ERROR);\n"
        );

        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(
            Err2Str(res),
            "<main>:1:52: Error: Only 1 ON ERROR clause is expected\n"
        );
    }

    Y_UNIT_TEST(JsonValueOnEmptyAfterOnError) {
        NYql::TAstParseResult res = SqlToYql(
            "select JSON_VALUE(CAST(@@{\"key\": 1238}@@ as Json), \"strict $.key\" NULL ON ERROR NULL ON EMPTY);\n"
        );

        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(
            Err2Str(res),
            "<main>:1:52: Error: ON EMPTY clause must be before ON ERROR clause\n"
        );
    }

    Y_UNIT_TEST(JsonValueNullInput) {
        NYql::TAstParseResult res = SqlToYql(R"(SELECT JSON_VALUE(NULL, "strict $.key");)");

        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
            Y_UNUSED(word);
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("(Nothing (OptionalType (DataType 'Json)))"));
        };

        TWordCountHive elementStat({"JsonValue"});
        VerifyProgram(res, elementStat, verifyLine);
        UNIT_ASSERT(elementStat["JsonValue"] > 0);
    }
}

Y_UNIT_TEST_SUITE(JsonExists) {
    Y_UNIT_TEST(JsonExistsValidHandlers) {
        const TVector<std::pair<TString, TString>> testCases = {
            {"", "(Just (Bool '\"false\"))"},
            {"TRUE ON ERROR", "(Just (Bool '\"true\"))"},
            {"FALSE ON ERROR", "(Just (Bool '\"false\"))"},
            {"UNKNOWN ON ERROR", "(Nothing (OptionalType (DataType 'Bool)))"},
            // NOTE: in this case we expect arguments of JsonExists callable to end immediately
            // after variables. This parenthesis at the end of the expression is left on purpose
            {"ERROR ON ERROR", "(Utf8 '\"strict $.key\") (JsonVariables))"},
        };

        for (const auto& item : testCases) {
            NYql::TAstParseResult res = SqlToYql(
                TStringBuilder() << R"(
                $json = CAST(@@{"key": 1238}@@ as Json);
                SELECT JSON_EXISTS($json, "strict $.key" )" << item.first << ");\n"
            );

            UNIT_ASSERT(res.Root);

            TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
                Y_UNUSED(word);
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(item.second));
            };

            TWordCountHive elementStat({"JsonExists"});
            VerifyProgram(res, elementStat, verifyLine);
            UNIT_ASSERT(elementStat["JsonExists"] > 0);
        }
    }

    Y_UNIT_TEST(JsonExistsInvalidHandler) {
        NYql::TAstParseResult res = SqlToYql(R"(
            $json = CAST(@@{"key": 1238}@@ as Json);
            $default = false;
            SELECT JSON_EXISTS($json, "strict $.key" $default ON ERROR);
        )");

        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:4:53: Error: Unexpected token absence : Missing RPAREN \n\n");
    }

    Y_UNIT_TEST(JsonExistsNullInput) {
        NYql::TAstParseResult res = SqlToYql(R"(SELECT JSON_EXISTS(NULL, "strict $.key");)");

        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
            Y_UNUSED(word);
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("(Nothing (OptionalType (DataType 'Json)))"));
        };

        TWordCountHive elementStat({"JsonExists"});
        VerifyProgram(res, elementStat, verifyLine);
        UNIT_ASSERT(elementStat["JsonExists"] > 0);
    }
}

Y_UNIT_TEST_SUITE(JsonQuery) {
    Y_UNIT_TEST(JsonQueryValidHandlers) {
        using TTestSuite = const TVector<std::pair<TString, TString>>;
        TTestSuite wrapCases = {
            {"", "'NoWrap"},
            {"WITHOUT WRAPPER", "'NoWrap"},
            {"WITHOUT ARRAY WRAPPER", "'NoWrap"},
            {"WITH WRAPPER", "'Wrap"},
            {"WITH ARRAY WRAPPER", "'Wrap"},
            {"WITH UNCONDITIONAL WRAPPER", "'Wrap"},
            {"WITH UNCONDITIONAL ARRAY WRAPPER", "'Wrap"},
            {"WITH CONDITIONAL WRAPPER", "'ConditionalWrap"},
            {"WITH CONDITIONAL ARRAY WRAPPER", "'ConditionalWrap"},
        };
        TTestSuite handlerCases = {
            {"", "'Null"},
            {"ERROR", "'Error"},
            {"NULL", "'Null"},
            {"EMPTY ARRAY", "'EmptyArray"},
            {"EMPTY OBJECT", "'EmptyObject"},
        };

        for (const auto& wrap : wrapCases) {
            for (const auto& onError : handlerCases) {
                for (const auto& onEmpty : handlerCases) {
                    TStringBuilder query;
                    query << R"($json = CAST(@@{"key": [123]}@@ as Json);
                    SELECT JSON_QUERY($json, "strict $.key" )" << wrap.first;
                    if (!onEmpty.first.Empty()) {
                        if (wrap.first.StartsWith("WITH ")) {
                            continue;
                        }
                        query << " " << onEmpty.first << " ON EMPTY";
                    }
                    if (!onError.first.Empty()) {
                        query << " " << onError.first << " ON ERROR";
                    }
                    query << ");\n";

                    NYql::TAstParseResult res = SqlToYql(query);

                    UNIT_ASSERT(res.Root);

                    TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
                        Y_UNUSED(word);
                        const TString args = TStringBuilder() << wrap.second << " " << onEmpty.second << " " << onError.second;
                        UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(args));
                    };

                    Cout << wrap.first << " " << onEmpty.first << " " << onError.first << Endl;

                    TWordCountHive elementStat({"JsonQuery"});
                    VerifyProgram(res, elementStat, verifyLine);
                    UNIT_ASSERT(elementStat["JsonQuery"] > 0);
                }
            }
        }
    }

    Y_UNIT_TEST(JsonQueryOnEmptyWithWrapper) {
        NYql::TAstParseResult res = SqlToYql(R"(
            $json = CAST(@@{"key": 1238}@@ as Json);
            SELECT JSON_QUERY($json, "strict $" WITH ARRAY WRAPPER EMPTY ARRAY ON EMPTY);
        )");

        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:38: Error: ON EMPTY is prohibited because WRAPPER clause is specified\n");
    }

    Y_UNIT_TEST(JsonQueryNullInput) {
        NYql::TAstParseResult res = SqlToYql(R"(SELECT JSON_QUERY(NULL, "strict $.key");)");

        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
            Y_UNUSED(word);
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("(Nothing (OptionalType (DataType 'Json)))"));
        };

        TWordCountHive elementStat({"JsonQuery"});
        VerifyProgram(res, elementStat, verifyLine);
        UNIT_ASSERT(elementStat["JsonQuery"] > 0);
    }
}

Y_UNIT_TEST_SUITE(JsonPassing) {
    Y_UNIT_TEST(SupportedVariableTypes) {
        const TVector<TString> functions = {"JSON_EXISTS", "JSON_VALUE", "JSON_QUERY"};

        for (const auto& function : functions) {
            const auto query = Sprintf(R"(
                pragma CompactNamedExprs;
                $json = CAST(@@{"key": 1238}@@ as Json);
                SELECT %s(
                    $json,
                    "strict $.key"
                    PASSING
                        "string" as var1,
                        1.234 as var2,
                        CAST(1 as Int64) as var3,
                        true as var4,
                        $json as var5
                ))",
                function.data()
            );
            NYql::TAstParseResult res = SqlToYql(query);

            UNIT_ASSERT(res.Root);

            TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
                Y_UNUSED(word);
                UNIT_ASSERT_VALUES_UNEQUAL_C(TString::npos, line.find(R"('('"var1" (String '"string")))"), "Cannot find `var1`");
                UNIT_ASSERT_VALUES_UNEQUAL_C(TString::npos, line.find(R"('('"var2" (Double '"1.234")))"), "Cannot find `var2`");
                UNIT_ASSERT_VALUES_UNEQUAL_C(TString::npos, line.find(R"('('"var3" (SafeCast (Int32 '"1") (DataType 'Int64))))"), "Cannot find `var3`");
                UNIT_ASSERT_VALUES_UNEQUAL_C(TString::npos, line.find(R"('('"var4" (Bool '"true")))"), "Cannot find `var4`");
                UNIT_ASSERT_VALUES_UNEQUAL_C(TString::npos, line.find(R"('('"var5" namedexprnode0))"), "Cannot find `var5`");
            };

            TWordCountHive elementStat({"JsonVariables"});
            VerifyProgram(res, elementStat, verifyLine);
            UNIT_ASSERT(elementStat["JsonVariables"] > 0);
        }
    }

    Y_UNIT_TEST(ValidVariableNames) {
        const TVector<TString> functions = {"JSON_EXISTS", "JSON_VALUE", "JSON_QUERY"};

        for (const auto& function : functions) {
            const auto query = Sprintf(R"(
                $json = CAST(@@{"key": 1238}@@ as Json);
                SELECT %s(
                    $json,
                    "strict $.key"
                    PASSING
                        "one" as var1,
                        "two" as "VaR2",
                        "three" as `var3`,
                        "four" as VaR4
                ))",
                function.data()
            );
            NYql::TAstParseResult res = SqlToYql(query);

            UNIT_ASSERT(res.Root);

            TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
                Y_UNUSED(word);
                UNIT_ASSERT_VALUES_UNEQUAL_C(TString::npos, line.find(R"('('"var1" (String '"one")))"), "Cannot find `var1`");
                UNIT_ASSERT_VALUES_UNEQUAL_C(TString::npos, line.find(R"('('"VaR2" (String '"two")))"), "Cannot find `VaR2`");
                UNIT_ASSERT_VALUES_UNEQUAL_C(TString::npos, line.find(R"('('"var3" (String '"three")))"), "Cannot find `var3`");
                UNIT_ASSERT_VALUES_UNEQUAL_C(TString::npos, line.find(R"('('"VaR4" (String '"four")))"), "Cannot find `VaR4`");
            };

            TWordCountHive elementStat({"JsonVariables"});
            VerifyProgram(res, elementStat, verifyLine);
            UNIT_ASSERT(elementStat["JsonVariables"] > 0);
        }
    }
}

Y_UNIT_TEST_SUITE(MigrationToJsonApi) {
    Y_UNIT_TEST(WarningOnDeprecatedJsonUdf) {
        NYql::TAstParseResult res = SqlToYql(R"(
            $json = CAST(@@{"key": 1234}@@ as Json);
            SELECT Json::Parse($json);
        )");

        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:26: Warning: Json UDF is deprecated. Please use JSON API instead, code: 4506\n");
    }
}

Y_UNIT_TEST_SUITE(AnsiIdentsNegative) {
    Y_UNIT_TEST(EnableAnsiLexerFromRequestSpecialComments) {
        auto req = "\n"
                   "\t --!ansi_lexer \n"
                   "-- Some comment\n"
                   "-- another comment\n"
                   "pragma SimpleColumns;\n"
                   "\n"
                   "select 1, '''' as empty;";

        auto res = SqlToYql(req);
        UNIT_ASSERT(res.IsOk());
        UNIT_ASSERT(res.Issues.Size() == 0);
    }

    Y_UNIT_TEST(AnsiLexerShouldNotBeEnabledHere) {
        auto req = "$str = '\n"
                   "--!ansi_lexer\n"
                   "--!syntax_v1\n"
                   "';\n"
                   "\n"
                   "select 1, $str, \"\" as empty;";
        auto res = SqlToYql(req);
        UNIT_ASSERT(res.IsOk());
        UNIT_ASSERT(res.Issues.Size() == 0);
    }

    Y_UNIT_TEST(DoubleQuotesInDictsTuplesOrLists) {
        auto req = "$d = { 'a': 1, \"b\": 2, 'c': 3,};";

        auto res = SqlToYqlWithAnsiLexer(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:16: Error: Column reference \"b\" is not allowed in current scope\n");

        req = "$t = (1, 2, \"a\");";

        res = SqlToYqlWithAnsiLexer(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:13: Error: Column reference \"a\" is not allowed in current scope\n");

        req = "$l = ['a', 'b', \"c\"];";

        res = SqlToYqlWithAnsiLexer(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:17: Error: Column reference \"c\" is not allowed in current scope\n");
    }

    Y_UNIT_TEST(MultilineComments) {
        auto req = "/*/**/ select 1;";
        auto res = SqlToYql(req);
        UNIT_ASSERT(res.Root);
        res = SqlToYqlWithAnsiLexer(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:16: Error: Unexpected character : syntax error...\n\n");

        req = "/*\n"
              "--/*\n"
              "*/ select 1;";
        res = SqlToYql(req);
        UNIT_ASSERT(res.Root);
        res = SqlToYqlWithAnsiLexer(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:12: Error: Unexpected character : syntax error...\n\n");

        req = "/*\n"
              "/*\n"
              "--*/\n"
              "*/ select 1;";
        res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:4:0: Error: Unexpected token '*' : cannot match to any predicted input...\n\n");
        res = SqlToYqlWithAnsiLexer(req);
        UNIT_ASSERT(res.Root);
    }
}

Y_UNIT_TEST_SUITE(AnsiOptionalAs) {
    Y_UNIT_TEST(OptionalAsInProjection) {
        UNIT_ASSERT(SqlToYql("PRAGMA AnsiOptionalAs; SELECT a b, c FROM plato.Input;").IsOk());
        ExpectFailWithError("PRAGMA DisableAnsiOptionalAs;\n"
                            "SELECT a b, c FROM plato.Input;",
                            "<main>:2:10: Error: Expecting mandatory AS here. Did you miss comma? Please add PRAGMA AnsiOptionalAs; for ANSI compatibility\n");
    }

    Y_UNIT_TEST(OptionalAsWithKeywords) {
        UNIT_ASSERT(SqlToYql("PRAGMA AnsiOptionalAs; SELECT a type, b data, c source FROM plato.Input;").IsOk());
    }
}

Y_UNIT_TEST_SUITE(SessionWindowNegative) {
    Y_UNIT_TEST(SessionWindowWithoutSource) {
        ExpectFailWithError("SELECT 1 + SessionWindow(ts, 32);",
            "<main>:1:12: Error: SessionWindow requires data source\n");
    }

    Y_UNIT_TEST(SessionWindowInProjection) {
        ExpectFailWithError("SELECT 1 + SessionWindow(ts, 32) from plato.Input;",
            "<main>:1:12: Error: SessionWindow can only be used as a top-level GROUP BY / PARTITION BY expression\n");
    }

    Y_UNIT_TEST(SessionWindowWithNonConstSecondArg) {
        ExpectFailWithError(
            "SELECT key, session_start FROM plato.Input\n"
            "GROUP BY SessionWindow(ts, 32 + subkey) as session_start, key;",

            "<main>:2:10: Error: Source does not allow column references\n"
            "<main>:2:33: Error: Column reference 'subkey'\n");
    }

    Y_UNIT_TEST(SessionWindowWithWrongNumberOfArgs) {
        ExpectFailWithError("SELECT * FROM plato.Input GROUP BY SessionWindow()",
            "<main>:1:36: Error: SessionWindow requires either two or four arguments\n");
        ExpectFailWithError("SELECT * FROM plato.Input GROUP BY SessionWindow(key, subkey, 100)",
            "<main>:1:36: Error: SessionWindow requires either two or four arguments\n");
    }

    Y_UNIT_TEST(DuplicateSessionWindow) {
        ExpectFailWithError(
            "SELECT\n"
            "    *\n"
            "FROM plato.Input\n"
            "GROUP BY\n"
            "    SessionWindow(ts, 10),\n"
            "    user,\n"
            "    SessionWindow(ts, 20)\n"
            ";",

            "<main>:7:5: Error: Duplicate session window specification:\n"
            "<main>:5:5: Error: Previous session window is declared here\n");

        ExpectFailWithError(
            "SELECT\n"
            "    MIN(key) over w\n"
            "FROM plato.Input\n"
            "WINDOW w AS (\n"
            "    PARTITION BY SessionWindow(ts, 10), user,\n"
            "    SessionWindow(ts, 20)\n"
            ");",

            "<main>:6:5: Error: Duplicate session window specification:\n"
            "<main>:5:18: Error: Previous session window is declared here\n");
    }

    Y_UNIT_TEST(SessionStartStateWithoutSource) {
        ExpectFailWithError("SELECT 1 + SessionStart();",
            "<main>:1:12: Error: SessionStart requires data source\n");
        ExpectFailWithError("SELECT 1 + SessionState();",
            "<main>:1:12: Error: SessionState requires data source\n");
    }

    Y_UNIT_TEST(SessionStartStateWithoutGroupByOrWindow) {
        ExpectFailWithError("SELECT 1 + SessionStart() from plato.Input;",
            "<main>:1:12: Error: SessionStart can not be used without aggregation by SessionWindow\n");
        ExpectFailWithError("SELECT 1 + SessionState() from plato.Input;",
            "<main>:1:12: Error: SessionState can not be used without aggregation by SessionWindow\n");
    }

    Y_UNIT_TEST(SessionStartStateWithGroupByWithoutSession) {
        ExpectFailWithError("SELECT 1 + SessionStart() from plato.Input group by user;",
            "<main>:1:12: Error: SessionStart can not be used here: SessionWindow specification is missing in GROUP BY\n");
        ExpectFailWithError("SELECT 1 + SessionState() from plato.Input group by user;",
            "<main>:1:12: Error: SessionState can not be used here: SessionWindow specification is missing in GROUP BY\n");
    }

    Y_UNIT_TEST(SessionStartStateWithoutOverWithWindowWithoutSession) {
        ExpectFailWithError("SELECT 1 + SessionStart(), MIN(key) over w from plato.Input window w as ()",
            "<main>:1:12: Error: SessionStart can not be used without aggregation by SessionWindow. Maybe you forgot to add OVER `window_name`?\n");
        ExpectFailWithError("SELECT 1 + SessionState(), MIN(key) over w from plato.Input window w as ()",
            "<main>:1:12: Error: SessionState can not be used without aggregation by SessionWindow. Maybe you forgot to add OVER `window_name`?\n");
    }

    Y_UNIT_TEST(SessionStartStateWithWindowWithoutSession) {
        ExpectFailWithError("SELECT 1 + SessionStart() over w, MIN(key) over w from plato.Input window w as ()",
            "<main>:1:12: Error: SessionStart can not be used with window w: SessionWindow specification is missing in PARTITION BY\n");
        ExpectFailWithError("SELECT 1 + SessionState() over w, MIN(key) over w from plato.Input window w as ()",
            "<main>:1:12: Error: SessionState can not be used with window w: SessionWindow specification is missing in PARTITION BY\n");
    }

    Y_UNIT_TEST(SessionStartStateWithSessionedWindow) {
        ExpectFailWithError("SELECT 1 + SessionStart(), MIN(key) over w from plato.Input group by key window w as (partition by SessionWindow(ts, 1)) ",
            "<main>:1:12: Error: SessionStart can not be used here: SessionWindow specification is missing in GROUP BY. Maybe you forgot to add OVER `window_name`?\n");
        ExpectFailWithError("SELECT 1 + SessionState(), MIN(key) over w from plato.Input group by key window w as (partition by SessionWindow(ts, 1)) ",
            "<main>:1:12: Error: SessionState can not be used here: SessionWindow specification is missing in GROUP BY. Maybe you forgot to add OVER `window_name`?\n");
    }

    Y_UNIT_TEST(AggregationBySessionStateIsNotSupportedYet) {
        ExpectFailWithError("SELECT SOME(1 + SessionState()), key from plato.Input group by key, SessionWindow(ts, 1);",
            "<main>:1:17: Error: SessionState with GROUP BY is not supported yet\n");
    }

    Y_UNIT_TEST(SessionWindowInRtmr) {
        NYql::TAstParseResult res = SqlToYql(
            "SELECT * FROM plato.Input GROUP BY SessionWindow(ts, 10);",
            10, TString(NYql::RtmrProviderName));
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:54: Error: Streaming group by query must have a hopping window specification.\n");

        res = SqlToYql(R"(
            SELECT key, SUM(value) AS value FROM plato.Input
            GROUP BY key, HOP(subkey, "PT10S", "PT30S", "PT20S"), SessionWindow(ts, 10);
        )", 10, TString(NYql::RtmrProviderName));

        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:13: Error: SessionWindow is unsupported for streaming sources\n");
    }
}

Y_UNIT_TEST_SUITE(LibraSqlSugar) {
    auto makeResult = [](TStringBuf settings) {
        return SqlToYql(
            TStringBuilder()
                << settings
                << "\n$udf1 = MyLibra::MakeLibraPreprocessor($settings);"
                << "\n$udf2 = CustomLibra::MakeLibraPreprocessor($settings);"
                << "\nPROCESS plato.Input USING $udf1(TableRow())"
                << "\nUNION ALL"
                << "\nPROCESS plato.Input USING $udf2(TableRow());"
        );
    };

    Y_UNIT_TEST(EmptySettings) {
        auto res = makeResult(R"(
            $settings = AsStruct();
        )");
        UNIT_ASSERT(res.IsOk());
    }

    Y_UNIT_TEST(OnlyEntities) {
        auto res = makeResult(R"(
            $settings = AsStruct(
                AsList("A", "B", "C") AS Entities
            );
        )");
        UNIT_ASSERT(res.IsOk());
    }

    Y_UNIT_TEST(EntitiesWithStrategy) {
        auto res = makeResult(R"(
            $settings = AsStruct(
                AsList("A", "B", "C") AS Entities,
                "blacklist" AS EntitiesStrategy
            );
        )");
        UNIT_ASSERT(res.IsOk());
    }

    Y_UNIT_TEST(AllSettings) {
        auto res = makeResult(R"(
            $settings = AsStruct(
                AsList("A", "B", "C") AS Entities,
                "whitelist" AS EntitiesStrategy,
                "path" AS BlockstatDict,
                false AS ParseWithFat,
                "map" AS Mode
            );
        )");
        UNIT_ASSERT(res.IsOk());
    }

    Y_UNIT_TEST(BadStrategy) {
        auto res = makeResult(R"(
            $settings = AsStruct("bad" AS EntitiesStrategy);
        )");
        UNIT_ASSERT_STRING_CONTAINS(
            Err2Str(res),
            "Error: MakeLibraPreprocessor got invalid entities strategy: expected 'whitelist' or 'blacklist'"
        );
    }

    Y_UNIT_TEST(BadEntities) {
        auto res = makeResult(R"(
            $settings = AsStruct(AsList("A", 1) AS Entities);
        )");
        UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "Error: MakeLibraPreprocessor entity must be string literal");
    }
}

Y_UNIT_TEST_SUITE(TrailingQuestionsNegative) {
    Y_UNIT_TEST(Basic) {
        ExpectFailWithError("SELECT 1?;", "<main>:1:9: Error: Unexpected token '?' at the end of expression\n");
        ExpectFailWithError("SELECT 1? + 1;", "<main>:1:10: Error: Unexpected token '+' : cannot match to any predicted input...\n\n");
        ExpectFailWithError("SELECT 1 + 1??? < 2", "<main>:1:13: Error: Unexpected token '?' at the end of expression\n");
        ExpectFailWithError("SELECT   1? > 2? > 3?",
            "<main>:1:11: Error: Unexpected token '?' at the end of expression\n"
            "<main>:1:16: Error: Unexpected token '?' at the end of expression\n"
            "<main>:1:21: Error: Unexpected token '?' at the end of expression\n");
    }

    Y_UNIT_TEST(SmartParen) {
        ExpectFailWithError("$x = 1; SELECT (Int32?, $x?)", "<main>:1:27: Error: Unexpected token '?' at the end of expression\n");
        ExpectFailWithError("SELECT (Int32, foo?)", "<main>:1:19: Error: Unexpected token '?' at the end of expression\n");
    }

    Y_UNIT_TEST(LambdaOptArgs) {
        ExpectFailWithError("$l = ($x, $y?, $z??, $t?) -> ($x);", "<main>:1:18: Error: Expecting at most one '?' token here (for optional lambda parameters), but got 2\n");
    }
}

Y_UNIT_TEST_SUITE(FlexibleTypes) {
    Y_UNIT_TEST(AssumeOrderByType) {
        UNIT_ASSERT(SqlToYql("PRAGMA FlexibleTypes; SELECT 1 AS int32 ASSUME ORDER BY int32").IsOk());
    }

    Y_UNIT_TEST(GroupingSets) {
        UNIT_ASSERT(SqlToYql("PRAGMA FlexibleTypes; SELECT COUNT(*) AS cnt, text, uuid FROM plato.Input GROUP BY GROUPING SETS((uuid), (uuid, text));").IsOk());
    }

    Y_UNIT_TEST(WeakField) {
        UNIT_ASSERT(SqlToYql("PRAGMA FlexibleTypes; SELECT WeakField(text, string) as text FROM plato.Input").IsOk());
    }

    Y_UNIT_TEST(Aggregation1) {
        TString q =
            "PRAGMA FlexibleTypes;\n"
            "$foo = ($x, $const, $type) -> ($x || $const || FormatType($type));\n"
            "SELECT $foo(SOME(x), 'aaa', String) FROM plato.Input GROUP BY y;";
        UNIT_ASSERT(SqlToYql(q).IsOk());
    }

    Y_UNIT_TEST(Aggregation2) {
        TString q =
            "PRAGMA FlexibleTypes;\n"
            "SELECT 1 + String + MAX(key) FROM plato.Input;";
        UNIT_ASSERT(SqlToYql(q).IsOk());
    }
}

Y_UNIT_TEST_SUITE(ExternalDeclares) {
    Y_UNIT_TEST(BasicUsage) {
        NSQLTranslation::TTranslationSettings settings;
        settings.DeclaredNamedExprs["foo"] = "String";
        auto res = SqlToYqlWithSettings("select $foo;", settings);
        UNIT_ASSERT(res.IsOk());
        UNIT_ASSERT(res.Issues.Size() == 0);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "declare") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(R"__((declare "$foo" (DataType 'String)))__"));
            }
        };

        TWordCountHive elementStat = {{TString("declare"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["declare"]);
    }

    Y_UNIT_TEST(DeclareOverrides) {
        NSQLTranslation::TTranslationSettings settings;
        settings.DeclaredNamedExprs["foo"] = "String";
        auto res = SqlToYqlWithSettings("declare $foo as Int32; select $foo;", settings);
        UNIT_ASSERT(res.IsOk());
        UNIT_ASSERT(res.Issues.Size() == 0);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "declare") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(R"__((declare "$foo" (DataType 'Int32)))__"));
            }
        };

        TWordCountHive elementStat = {{TString("declare"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["declare"]);
    }

    Y_UNIT_TEST(UnusedDeclareDoesNotProduceWarning) {
        NSQLTranslation::TTranslationSettings settings;
        settings.DeclaredNamedExprs["foo"] = "String";
        auto res = SqlToYqlWithSettings("select 1;", settings);
        UNIT_ASSERT(res.IsOk());
        UNIT_ASSERT(res.Issues.Size() == 0);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "declare") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(R"__((declare "$foo" (DataType 'String)))__"));
            }
        };

        TWordCountHive elementStat = {{TString("declare"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["declare"]);
    }

    Y_UNIT_TEST(DeclaresWithInvalidTypesFails) {
        NSQLTranslation::TTranslationSettings settings;
        settings.DeclaredNamedExprs["foo"] = "List<BadType>";
        auto res = SqlToYqlWithSettings("select 1;", settings);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res),
            "<main>:0:5: Error: Unknown type: 'BadType'\n"
            "<main>: Error: Failed to parse type for externally declared name 'foo'\n");
    }
}

Y_UNIT_TEST_SUITE(ExternalDataSource) {
    Y_UNIT_TEST(CreateExternalDataSourceWithAuthNone) {
        NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="my-bucket",
                    AUTH_METHOD="NONE"
                );
            )sql");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_STRING_CONTAINS(line, R"#('('('"auth_method" '"NONE") '('"location" '"my-bucket") '('"source_type" '"ObjectStorage"))#");
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("createObject"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(CreateExternalDataSourceWithAuthServiceAccount) {
        NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="my-bucket",
                    AUTH_METHOD="SERVICE_ACCOUNT",
                    SERVICE_ACCOUNT_ID="sa",
                    SERVICE_ACCOUNT_SECRET_NAME="sa_secret_name"
                );
            )sql");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_STRING_CONTAINS(line, R"#('('('"auth_method" '"SERVICE_ACCOUNT") '('"location" '"my-bucket") '('"service_account_id" '"sa") '('"service_account_secret_name" '"sa_secret_name") '('"source_type" '"ObjectStorage"))#");
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("createObject"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(CreateExternalDataSourceWithBasic) {
        NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="PostgreSQL",
                    LOCATION="protocol://host:port/",
                    AUTH_METHOD="BASIC",
                    LOGIN="admin",
                    PASSWORD_SECRET_NAME="secret_name"
                );
            )sql");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_STRING_CONTAINS(line, R"#('('('"auth_method" '"BASIC") '('"location" '"protocol://host:port/") '('"login" '"admin") '('"password_secret_name" '"secret_name") '('"source_type" '"PostgreSQL"))#");
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("createObject"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(CreateExternalDataSourceWithMdbBasic) {
        NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="PostgreSQL",
                    LOCATION="protocol://host:port/",
                    AUTH_METHOD="MDB_BASIC",
                    SERVICE_ACCOUNT_ID="sa",
                    SERVICE_ACCOUNT_SECRET_NAME="sa_secret_name",
                    LOGIN="admin",
                    PASSWORD_SECRET_NAME="secret_name"
                );
            )sql");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_STRING_CONTAINS(line, R"#('('('"auth_method" '"MDB_BASIC") '('"location" '"protocol://host:port/") '('"login" '"admin") '('"password_secret_name" '"secret_name") '('"service_account_id" '"sa") '('"service_account_secret_name" '"sa_secret_name") '('"source_type" '"PostgreSQL"))#");
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("createObject"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(CreateExternalDataSourceWithAws) {
        NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="PostgreSQL",
                    LOCATION="protocol://host:port/",
                    AUTH_METHOD="AWS",
                    AWS_ACCESS_KEY_ID_SECRET_NAME="secred_id_name",
                    AWS_SECRET_ACCESS_KEY_SECRET_NAME="secret_key_name",
                    AWS_REGION="ru-central-1"
                );
            )sql");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_STRING_CONTAINS(line, R"#('('('"auth_method" '"AWS") '('"aws_access_key_id_secret_name" '"secred_id_name") '('"aws_region" '"ru-central-1") '('"aws_secret_access_key_secret_name" '"secret_key_name") '('"location" '"protocol://host:port/") '('"source_type" '"PostgreSQL"))#");
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("createObject"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(CreateExternalDataSourceWithToken) {
        NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="YT",
                    LOCATION="protocol://host:port/",
                    AUTH_METHOD="TOKEN",
                    TOKEN_SECRET_NAME="token_name"
                );
            )sql");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_STRING_CONTAINS(line, R"#('('('"auth_method" '"TOKEN") '('"location" '"protocol://host:port/") '('"source_type" '"YT") '('"token_secret_name" '"token_name"))#");
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("createObject"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(CreateExternalDataSourceWithTablePrefix) {
        NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                pragma TablePathPrefix='/aba';
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="my-bucket",
                    AUTH_METHOD="NONE"
                );
            )sql");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_STRING_CONTAINS(line, "/aba/MyDataSource");
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("createObject"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(CreateExternalDataSourceIfNotExists) {
        NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE IF NOT EXISTS MyDataSource WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="my-bucket",
                    AUTH_METHOD="NONE"
                );
            )sql");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_STRING_CONTAINS(line, R"#('('('"auth_method" '"NONE") '('"location" '"my-bucket") '('"source_type" '"ObjectStorage"))#");
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("createObjectIfNotExists"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(AlterExternalDataSource) {
        NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                ALTER EXTERNAL DATA SOURCE MyDataSource
                    SET (SOURCE_TYPE = "ObjectStorage", Login = "Admin"),
                    SET Location "bucket",
                    RESET (Auth_Method, Service_Account_Id, Service_Account_Secret_Name);
            )sql");
        UNIT_ASSERT_C(res.Root, res.Issues.ToString());

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_STRING_CONTAINS(line, R"#(('mode 'alterObject))#");
                UNIT_ASSERT_STRING_CONTAINS(line, R"#('('features '('('"location" '"bucket") '('"login" '"Admin") '('"source_type" '"ObjectStorage"))))#");
                UNIT_ASSERT_STRING_CONTAINS(line, R"#('('resetFeatures '('"auth_method" '"service_account_id" '"service_account_secret_name")))#");
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(CreateExternalDataSourceOrReplace) {
        NYql::TAstParseResult res = SqlToYql(R"(
                USE plato;
                CREATE OR REPLACE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="my-bucket",
                    AUTH_METHOD="NONE"
                );
            )");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_STRING_CONTAINS(line, R"#('('('"auth_method" '"NONE") '('"location" '"my-bucket") '('"source_type" '"ObjectStorage"))#");
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("createObjectOrReplace"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(CreateOrReplaceForUnsupportedTableTypesShouldFail) {
        ExpectFailWithError(R"sql(
                USE plato;
                CREATE OR REPLACE TABLE t (a int32 not null, primary key(a, a));
            )sql" , "<main>:3:23: Error: OR REPLACE feature is supported only for EXTERNAL DATA SOURCE and EXTERNAL TABLE\n");

        ExpectFailWithError(R"sql(
                USE plato;
                CREATE OR REPLACE TABLE t (
                    Key Uint64,
                    Value1 String,
                    PRIMARY KEY (Key)
                )
                WITH (
                    STORE = COLUMN,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10
                );
            )sql" , "<main>:3:23: Error: OR REPLACE feature is supported only for EXTERNAL DATA SOURCE and EXTERNAL TABLE\n");
    }

    Y_UNIT_TEST(CreateExternalDataSourceWithBadArguments) {
        ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource;
            )sql" , "<main>:3:56: Error: Unexpected token ';' : syntax error...\n\n");

        ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    LOCATION="my-bucket",
                    AUTH_METHOD="NONE"
                );
            )sql" , "<main>:5:33: Error: SOURCE_TYPE requires key\n");

        ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="my-bucket"
                );
            )sql" , "<main>:5:30: Error: AUTH_METHOD requires key\n");


        ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="my-bucket",
                    AUTH_METHOD="NONE1"
                );
            )sql" , "<main>:6:33: Error: Unknown AUTH_METHOD = NONE1\n");

        ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="my-bucket",
                    AUTH_METHOD="SERVICE_ACCOUNT"
                );
            )sql" , "<main>:6:33: Error: SERVICE_ACCOUNT_ID requires key\n");

        ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="my-bucket",
                    AUTH_METHOD="SERVICE_ACCOUNT",
                    SERVICE_ACCOUNT_ID="s1"
                );
            )sql" , "<main>:7:40: Error: SERVICE_ACCOUNT_SECRET_NAME requires key\n");

        ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="my-bucket",
                    AUTH_METHOD="SERVICE_ACCOUNT",
                    SERVICE_ACCOUNT_SECRET_NAME="s1"
                );
            )sql" , "<main>:7:49: Error: SERVICE_ACCOUNT_ID requires key\n");

        ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="PostgreSQL",
                    LOCATION="protocol://host:port/",
                    AUTH_METHOD="BASIC",
                    LOGIN="admin"
                );
            )sql" , "<main>:7:27: Error: PASSWORD_SECRET_NAME requires key\n");

        ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="PostgreSQL",
                    LOCATION="protocol://host:port/",
                    AUTH_METHOD="BASIC",
                    PASSWORD_SECRET_NAME="secret_name"
                );
            )sql" , "<main>:7:42: Error: LOGIN requires key\n");

        ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="PostgreSQL",
                    LOCATION="protocol://host:port/",
                    AUTH_METHOD="MDB_BASIC",
                    SERVICE_ACCOUNT_SECRET_NAME="sa_secret_name",
                    LOGIN="admin",
                    PASSWORD_SECRET_NAME="secret_name"
                );
            )sql" , "<main>:9:42: Error: SERVICE_ACCOUNT_ID requires key\n");

        ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="PostgreSQL",
                    LOCATION="protocol://host:port/",
                    AUTH_METHOD="MDB_BASIC",
                    SERVICE_ACCOUNT_ID="sa",
                    LOGIN="admin",
                    PASSWORD_SECRET_NAME="secret_name"
                );
            )sql" , "<main>:9:42: Error: SERVICE_ACCOUNT_SECRET_NAME requires key\n");

        ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="PostgreSQL",
                    LOCATION="protocol://host:port/",
                    AUTH_METHOD="MDB_BASIC",
                    SERVICE_ACCOUNT_ID="sa",
                    SERVICE_ACCOUNT_SECRET_NAME="sa_secret_name",
                    PASSWORD_SECRET_NAME="secret_name"
                );
            )sql" , "<main>:9:42: Error: LOGIN requires key\n");

        ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="PostgreSQL",
                    LOCATION="protocol://host:port/",
                    AUTH_METHOD="MDB_BASIC",
                    SERVICE_ACCOUNT_ID="sa",
                    SERVICE_ACCOUNT_SECRET_NAME="sa_secret_name",
                    LOGIN="admin"
                );
            )sql" , "<main>:9:27: Error: PASSWORD_SECRET_NAME requires key\n");

        ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="PostgreSQL",
                    LOCATION="protocol://host:port/",
                    AUTH_METHOD="AWS",
                    AWS_SECRET_ACCESS_KEY_SECRET_NAME="secret_key_name",
                    AWS_REGION="ru-central-1"
                );
            )sql" , "<main>:8:32: Error: AWS_ACCESS_KEY_ID_SECRET_NAME requires key\n");

        ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="PostgreSQL",
                    LOCATION="protocol://host:port/",
                    AUTH_METHOD="AWS",
                    AWS_ACCESS_KEY_ID_SECRET_NAME="secred_id_name",
                    AWS_REGION="ru-central-1"
                );
            )sql" , "<main>:8:32: Error: AWS_SECRET_ACCESS_KEY_SECRET_NAME requires key\n");

        ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL DATA SOURCE MyDataSource WITH (
                    SOURCE_TYPE="PostgreSQL",
                    LOCATION="protocol://host:port/",
                    AUTH_METHOD="AWS",
                    AWS_SECRET_ACCESS_KEY_SECRET_NAME="secret_key_name",
                    AWS_ACCESS_KEY_ID_SECRET_NAME="secred_id_name"
                );
            )sql" , "<main>:8:51: Error: AWS_REGION requires key\n");
    }

    Y_UNIT_TEST(DropExternalDataSourceWithTablePrefix) {
        NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                DROP EXTERNAL DATA SOURCE MyDataSource;
            )sql");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("'features"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("dropObject"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(DropExternalDataSource) {
        NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                pragma TablePathPrefix='/aba';
                DROP EXTERNAL DATA SOURCE MyDataSource;
            )sql");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_STRING_CONTAINS(line, "/aba/MyDataSource");
                UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("'features"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("dropObject"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(DropExternalDataSourceIfExists) {
        NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                DROP EXTERNAL DATA SOURCE IF EXISTS MyDataSource;
            )sql");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_STRING_CONTAINS(line, "MyDataSource");
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("dropObjectIfExists"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }
}

Y_UNIT_TEST_SUITE(ExternalTable) {
    Y_UNIT_TEST(CreateExternalTable) {
        NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            CREATE EXTERNAL TABLE mytable (
                a int
            ) WITH (
                DATA_SOURCE="/Root/mydatasource",
                LOCATION="/folder1/*"
            );
        )sql");
        UNIT_ASSERT_C(res.Root, res.Issues.ToOneLineString());

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_STRING_CONTAINS(line, R"#('('('data_source_path (String '"/Root/mydatasource")) '('location (String '"/folder1/*")))) '('tableType 'externalTable)))))#");
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("tablescheme"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(CreateExternalTableWithTablePrefix) {
        NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            pragma TablePathPrefix='/aba';
            CREATE EXTERNAL TABLE mytable (
                a int
            ) WITH (
                DATA_SOURCE="mydatasource",
                LOCATION="/folder1/*"
            );
        )sql");
        UNIT_ASSERT_C(res.Root, res.Issues.ToOneLineString());

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_STRING_CONTAINS(line, "/aba/mydatasource");
                UNIT_ASSERT_STRING_CONTAINS(line, "/aba/mytable");
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("tablescheme"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(CreateExternalTableObjectStorage) {
        auto res = SqlToYql(R"sql(
            USE plato;
            CREATE EXTERNAL TABLE mytable (
                a int,
                year Int
            ) WITH (
                DATA_SOURCE="/Root/mydatasource",
                LOCATION="/folder1/*",
                FORMAT="json_as_string",
                `projection.enabled`="true",
                `projection.year.type`="integer",
                `projection.year.min`="2010",
                `projection.year.max`="2022",
                `projection.year.interval`="1",
                `projection.month.type`="integer",
                `projection.month.min`="1",
                `projection.month.max`="12",
                `projection.month.interval`="1",
                `projection.month.digits`="2",
                `storage.location.template`="${year}/${month}",
                PARTITONED_BY = "[year, month]"
            );
        )sql");
        UNIT_ASSERT_C(res.IsOk(), res.Issues.ToString());
    }

    Y_UNIT_TEST(CreateExternalTableIfNotExists) {
        NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            CREATE EXTERNAL TABLE IF NOT EXISTS mytable (
                a int
            ) WITH (
                DATA_SOURCE="/Root/mydatasource",
                LOCATION="/folder1/*"
            );
        )sql");
        UNIT_ASSERT_C(res.Root, res.Issues.ToOneLineString());

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_STRING_CONTAINS(line, R"#('('('data_source_path (String '"/Root/mydatasource")) '('location (String '"/folder1/*")))) '('tableType 'externalTable)))))#");
                UNIT_ASSERT_STRING_CONTAINS(line, "create_if_not_exists");
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(CreateExternalTableOrReplace) {
        NYql::TAstParseResult res = SqlToYql(R"(
            USE plato;
            CREATE OR REPLACE EXTERNAL TABLE mytable (
                a int
            ) WITH (
                DATA_SOURCE="/Root/mydatasource",
                LOCATION="/folder1/*"
            );
        )");
        UNIT_ASSERT_C(res.Root, res.Issues.ToOneLineString());

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_STRING_CONTAINS(line, R"#('('('data_source_path (String '"/Root/mydatasource")) '('location (String '"/folder1/*")))) '('tableType 'externalTable)))))#");
                UNIT_ASSERT_STRING_CONTAINS(line, "create_or_replace");
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(AlterExternalTableAddColumn) {
        NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            ALTER EXTERNAL TABLE mytable
                ADD COLUMN my_column int32,
                RESET (LOCATION);
        )sql");
        UNIT_ASSERT_C(res.Root, res.Issues.ToOneLineString());

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_STRING_CONTAINS(line, R"#('actions '('('addColumns '('('"my_column" (AsOptionalType (DataType 'Int32))#");
                UNIT_ASSERT_STRING_CONTAINS(line, R"#(('setTableSettings '('('location)))#");
                UNIT_ASSERT_STRING_CONTAINS(line, R"#(('tableType 'externalTable))#");
                UNIT_ASSERT_STRING_CONTAINS(line, R"#(('mode 'alter))#");
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(AlterExternalTableDropColumn) {
        NYql::TAstParseResult res = SqlToYql(R"sql(
            USE plato;
            ALTER EXTERNAL TABLE mytable
                DROP COLUMN my_column,
                SET (Location = "abc", Other_Prop = "42"),
                SET x 'y';
        )sql");
        UNIT_ASSERT_C(res.Root, res.Issues.ToOneLineString());

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_STRING_CONTAINS(line, R"#('actions '('('dropColumns '('"my_column")#");
                UNIT_ASSERT_STRING_CONTAINS(line, R"#(('setTableSettings '('('location (String '"abc")) '('Other_Prop (String '"42")) '('x (String '"y")))))#");
                UNIT_ASSERT_STRING_CONTAINS(line, R"#(('tableType 'externalTable))#");
                UNIT_ASSERT_STRING_CONTAINS(line, R"#(('mode 'alter))#");
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(CreateExternalTableWithBadArguments) {
        ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL TABLE mytable;
            )sql" , "<main>:3:45: Error: Unexpected token ';' : syntax error...\n\n");

        ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL TABLE mytable (
                    a int
                );
            )sql" , "<main>:4:23: Error: DATA_SOURCE requires key\n");

        ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL TABLE mytable (
                    a int
                ) WITH (
                    DATA_SOURCE="/Root/mydatasource"
                );
            )sql" , "<main>:6:33: Error: LOCATION requires key\n");

        ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL TABLE mytable (
                    a int
                ) WITH (
                    LOCATION="/folder1/*"
                );
            )sql" , "<main>:6:30: Error: DATA_SOURCE requires key\n");

        ExpectFailWithError(R"sql(
                USE plato;
                CREATE EXTERNAL TABLE mytable (
                    a int,
                    PRIMARY KEY(a)
                ) WITH (
                    DATA_SOURCE="/Root/mydatasource",
                    LOCATION="/folder1/*"
                );
            )sql" , "<main>:8:30: Error: PRIMARY KEY is not supported for external table\n");
    }

    Y_UNIT_TEST(DropExternalTable) {
        NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                DROP EXTERNAL TABLE MyExternalTable;
            )sql");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("tablescheme"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(DropExternalTableWithTablePrefix) {
        NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                pragma TablePathPrefix='/aba';
                DROP EXTERNAL TABLE MyExternalTable;
            )sql");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_STRING_CONTAINS(line, "/aba/MyExternalTable");
                UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("'tablescheme"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(DropExternalTableIfExists) {
        NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                DROP EXTERNAL TABLE IF EXISTS MyExternalTable;
            )sql");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("tablescheme"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("drop_if_exists"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }
}

Y_UNIT_TEST_SUITE(TopicsDDL) {
    void TestQuery(const TString& query, bool expectOk = true) {
        TStringBuilder finalQuery;

        finalQuery << "use plato;" << Endl << query;
        auto res = SqlToYql(finalQuery, 10, "kikimr");
        if (expectOk) {
            UNIT_ASSERT_C(res.IsOk(), res.Issues.ToString());
        } else {
            UNIT_ASSERT(!res.IsOk());
        }
    }

    Y_UNIT_TEST(CreateTopicSimple) {
        TestQuery(R"(
            CREATE TOPIC topic1;
        )");
        TestQuery(R"(
            CREATE TOPIC `cluster1.topic1`;
        )");
        TestQuery(R"(
            CREATE TOPIC topic1 WITH (metering_mode = "str_value", partition_count_limit = 123, retention_period = Interval('PT1H'));
        )");
    }

    Y_UNIT_TEST(CreateTopicConsumer) {
        TestQuery(R"(
            CREATE TOPIC topic1 (CONSUMER cons1);
        )");
        TestQuery(R"(
            CREATE TOPIC topic1 (CONSUMER cons1, CONSUMER cons2 WITH (important = false));
        )");
        TestQuery(R"(
            CREATE TOPIC topic1 (CONSUMER cons1, CONSUMER cons2 WITH (important = false)) WITH (supported_codecs = "1,2,3");
        )");
    }

    Y_UNIT_TEST(AlterTopicSimple) {
        TestQuery(R"(
            ALTER TOPIC topic1 SET (retention_period = Interval('PT1H'));
        )");
        TestQuery(R"(
            ALTER TOPIC topic1 SET (retention_storage_mb = 3, partition_count_limit = 50);
        )");
        TestQuery(R"(
            ALTER TOPIC topic1 RESET (supported_codecs, retention_period);
        )");
        TestQuery(R"(
            ALTER TOPIC topic1 RESET (partition_write_speed_bytes_per_second),
                 SET (partition_write_burst_bytes = 11111, min_active_partitions = 1);
        )");
    }
    Y_UNIT_TEST(AlterTopicConsumer) {
        TestQuery(R"(
            ALTER TOPIC topic1 ADD CONSUMER consumer1,
                ADD CONSUMER consumer2 WITH (important = false, supported_codecs = "RAW"),
                ALTER CONSUMER consumer3 SET (important = false, read_from = 1),
                ALTER CONSUMER consumer3 RESET (supported_codecs),
                DROP CONSUMER consumer4,
                SET (partition_count_limit = 11, retention_period = Interval('PT1H')),
                RESET(metering_mode)
        )");
    }
    Y_UNIT_TEST(DropTopic) {
        TestQuery(R"(
            DROP TOPIC topic1;
        )");
    }

    Y_UNIT_TEST(TopicBadRequests) {
        TestQuery(R"(
            CREATE TOPIC topic1();
        )", false);
        TestQuery(R"(
            CREATE TOPIC topic1 SET setting1 = value1;
        )", false);
        TestQuery(R"(
            ALTER TOPIC topic1 SET setting1 value1;
        )", false);
        TestQuery(R"(
            ALTER TOPIC topic1 RESET setting1;
        )", false);

        TestQuery(R"(
            ALTER TOPIC topic1 DROP CONSUMER consumer4 WITH (k1 = v1);
        )", false);

        TestQuery(R"(
            CREATE TOPIC topic1 WITH (retention_period = 123);
        )", false);
        TestQuery(R"(
            CREATE TOPIC topic1 (CONSUMER cons1, CONSUMER cons1 WITH (important = false));
        )", false);
        TestQuery(R"(
            CREATE TOPIC topic1 (CONSUMER cons1 WITH (bad_option = false));
        )", false);
        TestQuery(R"(
            ALTER TOPIC topic1 ADD CONSUMER cons1, ALTER CONSUMER cons1 RESET (important);
        )", false);
        TestQuery(R"(
            ALTER TOPIC topic1 ADD CONSUMER consumer1,
                ALTER CONSUMER consumer3 SET (supported_codecs = "RAW", read_from = 1),
                ALTER CONSUMER consumer3 RESET (supported_codecs);
        )", false);
        TestQuery(R"(
            ALTER TOPIC topic1 ADD CONSUMER consumer1,
                ALTER CONSUMER consumer3 SET (supported_codecs = "RAW", read_from = 1),
                ALTER CONSUMER consumer3 SET (read_from = 2);
        )", false);
    }

    Y_UNIT_TEST(TopicWithPrefix) {
        NYql::TAstParseResult res = SqlToYql(R"(
            USE plato;
            PRAGMA TablePathPrefix = '/database/path/to/tables';
            ALTER TOPIC `my_table/my_feed` ADD CONSUMER `my_consumer`;
        )");
        UNIT_ASSERT(res.Root);

        TWordCountHive elementStat = {{TString("/database/path/to/tables/my_table/my_feed"), 0}, {"topic", 0}};
        VerifyProgram(res, elementStat);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["topic"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["/database/path/to/tables/my_table/my_feed"]);
    }
}

Y_UNIT_TEST_SUITE(BlockEnginePragma) {
    Y_UNIT_TEST(Basic) {
        const TVector<TString> values = {"auto", "force", "disable"};
        for (const auto& value : values) {
            const auto query = TStringBuilder() << "pragma Blockengine='" << value << "'; select 1;";
            NYql::TAstParseResult res = SqlToYql(query);
            UNIT_ASSERT(res.Root);

            TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
                Y_UNUSED(word);
                UNIT_ASSERT_STRING_CONTAINS(line, TStringBuilder() << R"(Configure! world (DataSource '"config") '"BlockEngine" '")" << value << "\"");
            };

            TWordCountHive elementStat({"BlockEngine"});
            VerifyProgram(res, elementStat, verifyLine);
            UNIT_ASSERT(elementStat["BlockEngine"] == ((value == "disable") ? 0 : 1));
        }
    }

    Y_UNIT_TEST(UnknownSetting) {
        ExpectFailWithError("use plato; pragma BlockEngine='foo';",
            "<main>:1:31: Error: Expected `disable|auto|force' argument for: BlockEngine\n");
    }
}

Y_UNIT_TEST_SUITE(TViewSyntaxTest) {
    Y_UNIT_TEST(CreateViewSimple) {
        NYql::TAstParseResult res = SqlToYql(R"(
                USE plato;
                CREATE VIEW TheView WITH (security_invoker = TRUE) AS SELECT 1;
            )"
        );
        UNIT_ASSERT_C(res.Root, res.Issues.ToString());
    }

    Y_UNIT_TEST(CreateViewNoSecurityInvoker) {
        NYql::TAstParseResult res = SqlToYql(R"(
                USE plato;
                CREATE VIEW TheView AS SELECT 1;
            )"
        );
        UNIT_ASSERT_STRING_CONTAINS(res.Issues.ToString(), "Unexpected token 'AS' : syntax error");
    }

    Y_UNIT_TEST(CreateViewSecurityInvokerTurnedOff) {
        NYql::TAstParseResult res = SqlToYql(R"(
                USE plato;
                CREATE VIEW TheView WITH (security_invoker = FALSE) AS SELECT 1;
            )"
        );
        UNIT_ASSERT_STRING_CONTAINS(res.Issues.ToString(), "SECURITY_INVOKER option must be explicitly enabled");
    }

    Y_UNIT_TEST(CreateViewFromTable) {
        constexpr const char* path = "/PathPrefix/TheView";
        constexpr const char* query = R"(
            SELECT * FROM SomeTable
        )";

        NYql::TAstParseResult res = SqlToYql(std::format(R"(
                    USE plato;
                    CREATE VIEW `{}` WITH (security_invoker = TRUE) AS {};
                )",
                path,
                query
            )
        );
        UNIT_ASSERT_C(res.Root, res.Issues.ToString());

        TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
            if (word == "Write!") {
                UNIT_ASSERT_STRING_CONTAINS(line, path);
                UNIT_ASSERT_STRING_CONTAINS(line, "createObject");
            }
        };
        TWordCountHive elementStat = { {"Write!"} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(elementStat["Write!"], 1);
    }

    Y_UNIT_TEST(CheckReconstructedQuery) {
        constexpr const char* path = "/PathPrefix/TheView";
        constexpr const char* query = R"(
            SELECT * FROM FirstTable JOIN SecondTable ON FirstTable.key == SecondTable.key
        )";

        NYql::TAstParseResult res = SqlToYql(std::format(R"(
                    USE plato;
                    CREATE VIEW `{}` WITH (security_invoker = TRUE) AS {};
                )",
                path,
                query
            )
        );
        UNIT_ASSERT_C(res.Root, res.Issues.ToString());

        TString reconstructedQuery = ToString(Tokenize(query));
        TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
            if (word == "query_text") {
                UNIT_ASSERT_STRING_CONTAINS(line, reconstructedQuery);
            }
        };
        TWordCountHive elementStat = { {"Write!"} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(elementStat["Write!"], 1);
    }

    Y_UNIT_TEST(DropView) {
        constexpr const char* path = "/PathPrefix/TheView";
        NYql::TAstParseResult res = SqlToYql(std::format(R"(
                    USE plato;
                    DROP VIEW `{}`;
                )",
                path
            )
        );
        UNIT_ASSERT_C(res.Root, res.Issues.ToString());

        TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
            if (word == "Write!") {
                UNIT_ASSERT_STRING_CONTAINS(line, path);
                UNIT_ASSERT_STRING_CONTAINS(line, "dropObject");
            }
        };
        TWordCountHive elementStat = { {"Write!"} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(elementStat["Write!"], 1);
    }

    Y_UNIT_TEST(CreateViewWithTablePrefix) {
        NYql::TAstParseResult res = SqlToYql(R"(
                USE plato;
                PRAGMA TablePathPrefix='/PathPrefix';
                CREATE VIEW TheView WITH (security_invoker = TRUE) AS SELECT 1;
            )"
        );
        UNIT_ASSERT_C(res.Root, res.Issues.ToString());

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write!") {
                UNIT_ASSERT_STRING_CONTAINS(line, "/PathPrefix/TheView");
                UNIT_ASSERT_STRING_CONTAINS(line, "createObject");
            }
        };

        TWordCountHive elementStat = { {"Write!"} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(elementStat["Write!"], 1);
    }

    Y_UNIT_TEST(DropViewWithTablePrefix) {
        NYql::TAstParseResult res = SqlToYql(R"(
                USE plato;
                PRAGMA TablePathPrefix='/PathPrefix';
                DROP VIEW TheView;
            )"
        );
        UNIT_ASSERT_C(res.Root, res.Issues.ToString());

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_STRING_CONTAINS(line, "/PathPrefix/TheView");
                UNIT_ASSERT_STRING_CONTAINS(line, "dropObject");
            }
        };

        TWordCountHive elementStat = { {"Write!"} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(elementStat["Write!"], 1);
    }
    
    Y_UNIT_TEST(YtAlternativeSchemaSyntax) {
        NYql::TAstParseResult res = SqlToYql(R"(
            SELECT * FROM plato.Input WITH schema(y Int32, x String not null);
        )");
        UNIT_ASSERT_C(res.Root, res.Issues.ToString());

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "userschema") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos,
                    line.find(R"__('('('"userschema" (StructType '('"y" (AsOptionalType (DataType 'Int32))) '('"x" (DataType 'String))))))__"));
            }
        };

        TWordCountHive elementStat = {{TString("userschema"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["userschema"]);
    }

    Y_UNIT_TEST(UseViewAndFullColumnId) {
        NYql::TAstParseResult res = SqlToYql("USE plato; SELECT Input.x FROM Input VIEW uitzicht;");
        UNIT_ASSERT(res.Root);

        TWordCountHive elementStat = {{TString("SqlAccess"), 0}, {"SqlProjectItem", 0}, {"Read!", 0}};
        VerifyProgram(res, elementStat);
        UNIT_ASSERT_VALUES_EQUAL(0, elementStat["SqlAccess"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SqlProjectItem"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Read!"]);
    }
}

Y_UNIT_TEST_SUITE(CompactNamedExprs) {
    Y_UNIT_TEST(SourceCallablesInWrongContext) {
        TString query = R"(
            pragma CompactNamedExprs;
            $foo = %s();
            select $foo from plato.Input;
        )";

        THashMap<TString, TString> errs = {
            {"TableRow", "<main>:3:20: Error: TableRow requires data source\n"},
            {"JoinTableRow", "<main>:3:20: Error: JoinTableRow requires data source\n"},
            {"TableRecordIndex", "<main>:3:20: Error: Unable to use function: TableRecord without source\n"},
            {"TablePath", "<main>:3:20: Error: Unable to use function: TablePath without source\n"},
            {"SystemMetadata", "<main>:3:20: Error: Unable to use function: SystemMetadata without source\n"},
        };

        for (TString callable : { "TableRow", "JoinTableRow", "TableRecordIndex", "TablePath", "SystemMetadata"}) {
            auto req = Sprintf(query.c_str(), callable.c_str());
            ExpectFailWithError(req, errs[callable]);
        }
    }

    Y_UNIT_TEST(ValidateUnusedExprs) {
        TString query = R"(
            pragma warning("disable", "4527");
            pragma CompactNamedExprs;
            pragma ValidateUnusedExprs;

            $foo = count(1);
            select 1;
        )";
        ExpectFailWithError(query, "<main>:6:20: Error: Aggregation is not allowed in this context\n");
        query = R"(
            pragma warning("disable", "4527");
            pragma CompactNamedExprs;
            pragma ValidateUnusedExprs;

            define subquery $x() as 
                select count(1, 2);
            end define;
            select 1;
        )";
        ExpectFailWithError(query, "<main>:7:24: Error: Aggregation function Count requires exactly 1 argument(s), given: 2\n");
    }

    Y_UNIT_TEST(DisableValidateUnusedExprs) {
        TString query = R"(
            pragma warning("disable", "4527");
            pragma CompactNamedExprs;
            pragma DisableValidateUnusedExprs;

            $foo = count(1);
            select 1;
        )";
        SqlToYql(query).IsOk();
        query = R"(
            pragma warning("disable", "4527");
            pragma CompactNamedExprs;
            pragma DisableValidateUnusedExprs;

            define subquery $x() as 
                select count(1, 2);
            end define;
            select 1;
        )";
        SqlToYql(query).IsOk();
    }
}

Y_UNIT_TEST_SUITE(ResourcePool) {
    Y_UNIT_TEST(CreateResourcePool) {
        NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                CREATE RESOURCE POOL MyResourcePool WITH (
                    CONCURRENT_QUERY_LIMIT=20,
                    QUERY_CANCEL_AFTER_SECONDS=86400,
                    QUEUE_TYPE="FIFO"
                );
            )sql");
        UNIT_ASSERT_C(res.Root, res.Issues.ToString());

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_STRING_CONTAINS(line, R"#('('('"concurrent_query_limit" (Int32 '"20")) '('"query_cancel_after_seconds" (Int32 '"86400")) '('"queue_type" '"FIFO"))#");
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("createObject"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(CreateResourcePoolWithBadArguments) {
        ExpectFailWithError(R"sql(
                USE plato;
                CREATE RESOURCE POOL MyResourcePool;
            )sql" , "<main>:3:51: Error: Unexpected token ';' : syntax error...\n\n");

        ExpectFailWithError(R"sql(
                USE plato;
                CREATE RESOURCE POOL MyResourcePool WITH (
                    DUPLICATE_SETTING="first_value",
                    DUPLICATE_SETTING="second_value"
                );
            )sql" , "<main>:5:21: Error: DUPLICATE_SETTING duplicate keys\n");
    }

    Y_UNIT_TEST(AlterResourcePool) {
        NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                ALTER RESOURCE POOL MyResourcePool
                    SET (CONCURRENT_QUERY_LIMIT = 30, Weight = 5),
                    SET QUEUE_TYPE "UNORDERED",
                    RESET (Query_Cancel_After_Seconds, Query_Count_Limit);
            )sql");
        UNIT_ASSERT_C(res.Root, res.Issues.ToString());

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_STRING_CONTAINS(line, R"#(('mode 'alterObject))#");
                UNIT_ASSERT_STRING_CONTAINS(line, R"#('('features '('('"concurrent_query_limit" (Int32 '"30")) '('"queue_type" '"UNORDERED") '('"weight" (Int32 '"5")))))#");
                UNIT_ASSERT_STRING_CONTAINS(line, R"#('('resetFeatures '('"query_cancel_after_seconds" '"query_count_limit")))#");
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(DropResourcePool) {
        NYql::TAstParseResult res = SqlToYql(R"sql(
                USE plato;
                DROP RESOURCE POOL MyResourcePool;
            )sql");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("'features"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("dropObject"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }
}

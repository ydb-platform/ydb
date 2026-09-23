#include "sql_ut.h"

#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/translation/sql.h>

using namespace NSQLTranslationV1;

namespace {

void AssertPaths(const NYql::TAstParseResult& result, std::initializer_list<const char*> expected,
                 std::initializer_list<const char*> unexpected = {}) {
    UNIT_ASSERT_C(result.IsOk(), Err2Str(result));
    const auto program = GetPrettyPrint(result);
    for (const auto* path : expected) {
        UNIT_ASSERT_STRING_CONTAINS(program, Quote(path));
    }
    for (const auto* path : unexpected) {
        UNIT_ASSERT_C(!program.Contains(Quote(path)), program);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TablePathPrefixScope) {

Y_UNIT_TEST(FollowingStatementsOnly) {
    for (const TString target : {"", "'plato', ", "'kikimr', "}) {
        const TString query = TStringBuilder()
            << "USE plato; SELECT * FROM Before;"
            << "PRAGMA TablePathPrefix(" << target << "'/first'); SELECT * FROM Input;"
            << "PRAGMA TablePathPrefix(" << target << "'/second'); SELECT * FROM Input;"
            << "SELECT * FROM `/absolute/Input`;"
            << "PRAGMA TablePathPrefix(" << target << "'/unused');";
        AssertPaths(SqlToYql(query, 10, NYql::KikimrProviderName),
                    {"Before", "/first/Input", "/second/Input", "/absolute/Input"},
                    {"/unused/Before", "/unused/Input", "/second/Before"});
    }
}

Y_UNIT_TEST(NamedSourcesKeepDefinitionPrefix) {
    AssertPaths(SqlToYql(R"sql(
        USE plato;
        PRAGMA TablePathPrefix = '/first';
        $query = SELECT * FROM Input;
        PRAGMA TablePathPrefix = '/second';
        SELECT * FROM $query;
        SELECT * FROM $query;
        SELECT * FROM Other;
    )sql"), {"/first/Input", "/second/Other"}, {"/second/Input"});
}

Y_UNIT_TEST(TableNamesFromExpressions) {
    const auto result = SqlToYql(R"sql(
        USE plato;
        $literal = 'Input';
        $expression = 'In' || 'put';
        PRAGMA TablePathPrefix = '/first';
        SELECT * FROM $literal;
        SELECT * FROM $expression;
        PRAGMA TablePathPrefix = '/second';
        SELECT * FROM $literal;
        SELECT * FROM $expression;
        PRAGMA TablePathPrefix = '/unused';
    )sql");
    AssertPaths(result, {"/first/Input", "/second/Input", "/first", "/second"}, {"/unused/Input", "/unused"});
    UNIT_ASSERT_STRING_CONTAINS(GetPrettyPrint(result), "BuildTablePath");
}

Y_UNIT_TEST(ClonedExpressionSourcesKeepDefinitionPrefix) {
    const auto result = SqlToYql(R"sql(
        USE plato;
        $table = 'In' || 'put';
        PRAGMA TablePathPrefix = '/first';
        $query = SELECT * FROM $table;
        PRAGMA TablePathPrefix = '/second';
        SELECT * FROM $query;
        SELECT * FROM $query;
    )sql");
    AssertPaths(result, {"/first"}, {"/second"});
    UNIT_ASSERT_STRING_CONTAINS(GetPrettyPrint(result), "BuildTablePath");
}

Y_UNIT_TEST(WritesAndTableDdl) {
    for (const TString statement : {
            "INSERT INTO Input (key) VALUES (1);",
            "UPSERT INTO Input (key) VALUES (1);",
            "REPLACE INTO Input (key) VALUES (1);",
            "UPDATE Input SET value = 1 WHERE key = 1;",
            "DELETE FROM Input WHERE key = 1;",
            "CREATE TABLE Input (key Uint64, PRIMARY KEY (key));",
            "ALTER TABLE Input ADD COLUMN value String;",
            "DROP TABLE Input;"}) {
        const TString query = TStringBuilder()
            << "USE plato; PRAGMA TablePathPrefix = '/first';" << statement
            << "PRAGMA TablePathPrefix = '/second';" << statement;
        AssertPaths(SqlToYql(query, 10, NYql::KikimrProviderName), {"/first/Input", "/second/Input"});
    }
}

Y_UNIT_TEST(RenameDestination) {
    AssertPaths(SqlToYql(R"sql(
        USE plato;
        PRAGMA TablePathPrefix = '/first';
        ALTER TABLE Input RENAME TO Output;
        PRAGMA TablePathPrefix = '/second';
        ALTER TABLE Input RENAME TO Output;
    )sql", 10, NYql::KikimrProviderName), {"/first/Input", "/first/Output", "/second/Input", "/second/Output"});
}

Y_UNIT_TEST(TopicsAndBackupTables) {
    AssertPaths(SqlToYql(R"sql(
        USE plato;
        PRAGMA TablePathPrefix = '/first';
        CREATE TOPIC Input;
        CREATE BACKUP COLLECTION Backup (TABLE Included) WITH (STORAGE = 'local');
        ALTER BACKUP COLLECTION Backup ADD TABLE Added, DROP TABLE Removed;
        PRAGMA TablePathPrefix = '/second';
        DROP TOPIC Input;
        CREATE BACKUP COLLECTION Backup (TABLE Included) WITH (STORAGE = 'local');
        ALTER BACKUP COLLECTION Backup ADD TABLE Added, DROP TABLE Removed;
    )sql", 10, NYql::KikimrProviderName),
        {"/first/Input", "/second/Input", "/first/Included", "/second/Included",
         "/first/Added", "/second/Added", "/first/Removed", "/second/Removed"});
}

Y_UNIT_TEST(TableFunctions) {
    for (const TString source : {"CONCAT('Input')", "CONCAT_STRICT('Input')", "RANGE('Input')"}) {
        const TString query = TStringBuilder()
            << "USE plato; PRAGMA TablePathPrefix = '/first'; SELECT * FROM " << source << ";"
            << "PRAGMA TablePathPrefix = '/second'; SELECT * FROM " << source << ";";
        AssertPaths(SqlToYql(query), {"/first/Input", "/second/Input"});
    }
    AssertPaths(SqlToYql(R"sql(
        USE plato;
        PRAGMA UseTablePrefixForEach;
        PRAGMA TablePathPrefix = '/first';
        SELECT * FROM EACH(AsList('Input'));
        PRAGMA TablePathPrefix = '/second';
        SELECT * FROM EACH(AsList('Input'));
    )sql"), {"/first", "/second"});
}

Y_UNIT_TEST(YqlSelectSources) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;
    const auto result = SqlToYqlWithSettings(R"sql(
        USE plato;
        PRAGMA YqlSelect = 'force';
        $table = 'In' || 'put';
        PRAGMA TablePathPrefix = '/first';
        $query = SELECT key FROM Input;
        SELECT key FROM $table;
        PRAGMA TablePathPrefix = '/second';
        SELECT $query;
        SELECT key FROM Input;
        SELECT key FROM $table;
        PRAGMA TablePathPrefix = '/unused';
    )sql", settings);
    AssertPaths(result, {"/first/Input", "/second/Input", "/first", "/second"}, {"/unused/Input", "/unused"});
}

Y_UNIT_TEST(SplitStatementsKeepNamedSourcePrefix) {
    google::protobuf::Arena arena;
    NSQLTranslation::TTranslationSettings settings;
    settings.Arena = &arena;
    settings.ClusterMapping["plato"] = NYql::KikimrProviderName;
    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
    NSQLTranslationV1::TParsers parsers;
    parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory();
    const auto results = NSQLTranslationV1::SqlToAstStatements(lexers, parsers, R"sql(
        USE plato;
        PRAGMA TablePathPrefix = '/first';
        $query = SELECT * FROM Input;
        SELECT * FROM $query;
        PRAGMA TablePathPrefix = '/second';
        SELECT * FROM $query;
        SELECT * FROM Other;
    )sql", settings, nullptr);
    UNIT_ASSERT_VALUES_EQUAL(results.size(), 3);
    AssertPaths(results[0], {"/first/Input"}, {"/second/Input"});
    AssertPaths(results[1], {"/first/Input"}, {"/second/Input"});
    AssertPaths(results[2], {"/second/Other"});
}

} // Y_UNIT_TEST_SUITE(TablePathPrefixScope)

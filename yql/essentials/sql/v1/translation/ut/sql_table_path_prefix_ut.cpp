#include "sql_ut.h"

#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/translation/context.h>
#include <yql/essentials/sql/v1/translation/select_yql.h>
#include <yql/essentials/sql/v1/translation/sql.h>

using namespace NSQLTranslationV1;

namespace {

NYql::TAstParseResult SqlToYqlWithMultiScopes(const TString& query, size_t maxErrors = 10, const TString& provider = {}) {
    NSQLTranslation::TTranslationSettings settings;
    settings.EnableTablePathPrefixMultiScopes = true;
    return SqlToYqlWithMode(query, NSQLTranslation::ESqlMode::QUERY, maxErrors, provider,
                          EDebugOutput::None, false, settings);
}

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

Y_UNIT_TEST(GenericTranslationKeepsLegacyDefault) {
    const NSQLTranslation::TTranslationSettings settings;
    UNIT_ASSERT(!settings.EnableTablePathPrefixMultiScopes);
    AssertPaths(SqlToYql(R"sql(
        USE plato;
        PRAGMA TablePathPrefix = '/first';
        SELECT * FROM Input;
        PRAGMA TablePathPrefix = '/second';
    )sql"), {"/second/Input"}, {"/first/Input"});
}

Y_UNIT_TEST(LegacyFactorySignaturesRemainAvailable) {
    TStringBuf (TContext::*getPrefix)(const TString&, const TDeferredAtom&) const = &TContext::GetPrefixPath;
    TNodePtr (*tableKey)(TPosition, const TString&, const TDeferredAtom&, const TDeferredAtom&, const TViewDescription&) = &BuildTableKey;
    TNodePtr (*tableKeys)(TPosition, const TString&, const TDeferredAtom&, const TString&, const TVector<TTableArg>&) = &BuildTableKeys;
    TNodePtr (*topicKey)(TPosition, const TDeferredAtom&, const TDeferredAtom&) = &BuildTopicKey;
    TSourcePtr (*innerSource)(TPosition, TNodePtr, const TString&, const TDeferredAtom&, const TString&) = &BuildInnerSource;
    TSourcePtr (*expressionSource)(TPosition, TContext&, const TString&, const TDeferredAtom&, TNodePtr, const TString&) = &TryMakeSourceFromExpression;
    TNodePtr (*alterTable)(TPosition, const TTableRef&, const TAlterTableParameters&, TScopedStatePtr) = &BuildAlterTable;
    TNodePtr (*yqlTableRef)(TPosition, TYqlTableRefArgs&&) = &BuildYqlTableRef;
    UNIT_ASSERT(getPrefix && tableKey && tableKeys && topicKey && innerSource && expressionSource && alterTable && yqlTableRef);
}

Y_UNIT_TEST(LegacyKeyFactoriesKeepDeferredLookupWithOptInContext) {
    NSQLTranslation::TTranslationSettings settings;
    settings.EnableTablePathPrefixMultiScopes = true;
    settings.DefaultCluster = "plato";
    settings.ClusterMapping["plato"] = "kikimr";
    NYql::TIssues issues;
    TContext ctx({}, {}, settings, {}, issues);
    const TDeferredAtom table(ctx.Pos(), "Input");
    const auto capturedEmpty = BuildTableKey(ctx.Pos(), ctx.Scoped->CurrService, ctx.Scoped->CurrCluster, table, {},
        TTablePathPrefix(ctx, ctx.Scoped->CurrService, ctx.Scoped->CurrCluster));
    UNIT_ASSERT(ctx.SetPathPrefix("/first"));
    const auto legacyTable = BuildTableKey(ctx.Pos(), ctx.Scoped->CurrService, ctx.Scoped->CurrCluster, table, {});
    const auto legacyTopic = BuildTopicKey(ctx.Pos(), ctx.Scoped->CurrCluster, table);
    const auto capturedTable = BuildTableKey(ctx.Pos(), ctx.Scoped->CurrService, ctx.Scoped->CurrCluster, table, {},
        TTablePathPrefix(ctx, ctx.Scoped->CurrService, ctx.Scoped->CurrCluster));
    UNIT_ASSERT(ctx.SetPathPrefix("/later"));
    for (const auto& node : {legacyTable, legacyTopic, capturedTable, capturedEmpty}) {
        const auto keys = node->GetTableKeys()->BuildKeys(ctx, ITableKeys::EBuildKeysMode::INPUT);
        UNIT_ASSERT_C(keys && keys->Init(ctx, nullptr), issues.ToString());
        const auto* ast = keys->Translate(ctx);
        UNIT_ASSERT_C(ast, issues.ToString());
        const TString expected = node == capturedEmpty ? "Input" : node == capturedTable ? "/first/Input" : "/later/Input";
        UNIT_ASSERT_STRING_CONTAINS(ast->ToString(), Quote(expected.c_str()));
    }
}

Y_UNIT_TEST(KeyFactorySnapshotsKeepClusterAndProviderSeparate) {
    for (const bool enabled : {false, true}) {
        NSQLTranslation::TTranslationSettings settings;
        settings.EnableTablePathPrefixMultiScopes = enabled;
        settings.DefaultCluster = "plato";
        for (const auto* cluster : {"plato", "target", "providerOnly"}) {
            settings.ClusterMapping[cluster] = "kikimr";
        }
        NYql::TIssues issues;
        TContext ctx({}, {}, settings, {}, issues);
        UNIT_ASSERT(ctx.SetPathPrefix("/global_first"));
        UNIT_ASSERT(ctx.SetPathPrefix("/current", TString("plato")));
        UNIT_ASSERT(ctx.SetPathPrefix("/provider_first", TString("kikimr")));
        UNIT_ASSERT(ctx.SetPathPrefix("/cluster_first", TString("target")));
        const TDeferredAtom table(ctx.Pos(), "Input");
        TVector<std::pair<TNodePtr, TString>> cases;
        for (const auto* clusterName : {"target", "providerOnly"}) {
            const TDeferredAtom cluster(ctx.Pos(), clusterName);
            const TTablePathPrefix prefix(ctx, "kikimr", cluster);
            const TString base = TString(clusterName) == "target" ? "/cluster_" : "/provider_";
            const TString expected = base + (enabled ? "first/Input" : "later/Input");
            cases.emplace_back(BuildTableKey(ctx.Pos(), "kikimr", cluster, table, {}, prefix), expected);
            cases.emplace_back(BuildTableKeys(ctx.Pos(), "kikimr", cluster, "concat",
                {{.Expr = BuildLiteralRawString(ctx.Pos(), "Input")}}, prefix), expected);
            cases.emplace_back(BuildTopicKey(ctx.Pos(), cluster, table, prefix),
                !enabled && TString(clusterName) == "providerOnly" ? TString("/global_later/Input") : expected);
        }
        UNIT_ASSERT(ctx.SetPathPrefix("/global_later"));
        UNIT_ASSERT(ctx.SetPathPrefix("/provider_later", TString("kikimr")));
        UNIT_ASSERT(ctx.SetPathPrefix("/cluster_later", TString("target")));
        for (const auto& [node, expected] : cases) {
            const auto keys = node->GetTableKeys()->BuildKeys(ctx, ITableKeys::EBuildKeysMode::INPUT);
            UNIT_ASSERT_C(keys && keys->Init(ctx, nullptr), issues.ToString());
            const auto* ast = keys->Translate(ctx);
            UNIT_ASSERT_C(ast, issues.ToString());
            UNIT_ASSERT_STRING_CONTAINS(ast->ToString(), Quote(expected.c_str()));
        }
    }
}

Y_UNIT_TEST(FollowingStatementsOnly) {
    for (const TString target : {"", "'plato', ", "'kikimr', "}) {
        const TString query = TStringBuilder()
            << "USE plato; SELECT * FROM Before;"
            << "PRAGMA TablePathPrefix(" << target << "'/first'); SELECT * FROM Input;"
            << "PRAGMA TablePathPrefix(" << target << "'/second'); SELECT * FROM Input;"
            << "SELECT * FROM `/absolute/Input`;"
            << "PRAGMA TablePathPrefix(" << target << "'/unused');";
        AssertPaths(SqlToYqlWithMultiScopes(query, 10, TString(NYql::KikimrProviderName)),
                    {"Before", "/first/Input", "/second/Input", "/absolute/Input"},
                    {"/unused/Before", "/unused/Input", "/second/Before"});
    }
}

Y_UNIT_TEST(NamedSourcesKeepDefinitionPrefix) {
    AssertPaths(SqlToYqlWithMultiScopes(R"sql(
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
    const auto result = SqlToYqlWithMultiScopes(R"sql(
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
    const auto result = SqlToYqlWithMultiScopes(R"sql(
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
        AssertPaths(SqlToYqlWithMultiScopes(query, 10, TString(NYql::KikimrProviderName)), {"/first/Input", "/second/Input"});
    }
}

Y_UNIT_TEST(RenameDestination) {
    AssertPaths(SqlToYqlWithMultiScopes(R"sql(
        USE plato;
        PRAGMA TablePathPrefix = '/first';
        ALTER TABLE Input RENAME TO Output;
        PRAGMA TablePathPrefix = '/second';
        ALTER TABLE Input RENAME TO Output;
    )sql", 10, TString(NYql::KikimrProviderName)), {"/first/Input", "/first/Output", "/second/Input", "/second/Output"});
}

Y_UNIT_TEST(TopicsAndBackupTables) {
    AssertPaths(SqlToYqlWithMultiScopes(R"sql(
        USE plato;
        PRAGMA TablePathPrefix = '/first';
        CREATE TOPIC Input;
        CREATE BACKUP COLLECTION Backup (TABLE Included) WITH (STORAGE = 'local');
        ALTER BACKUP COLLECTION Backup ADD TABLE Added, DROP TABLE Removed;
        PRAGMA TablePathPrefix = '/second';
        DROP TOPIC Input;
        CREATE BACKUP COLLECTION Backup (TABLE Included) WITH (STORAGE = 'local');
        ALTER BACKUP COLLECTION Backup ADD TABLE Added, DROP TABLE Removed;
    )sql", 10, TString(NYql::KikimrProviderName)),
        {"/first/Input", "/second/Input", "/first/Included", "/second/Included",
         "/first/Added", "/second/Added", "/first/Removed", "/second/Removed"});
}

Y_UNIT_TEST(TopicsUseProviderPrefixInSourceOrder) {
    AssertPaths(SqlToYqlWithMultiScopes(R"sql(
        USE plato;
        PRAGMA TablePathPrefix('kikimr', '/provider_first');
        CREATE TOPIC Created;
        PRAGMA TablePathPrefix = '/global';
        DROP TOPIC Dropped;
        PRAGMA TablePathPrefix('kikimr', '/provider_second');
        CREATE TOPIC Later;
        PRAGMA TablePathPrefix = '/unused';
    )sql", 10, TString(NYql::KikimrProviderName)),
        {"/provider_first/Created", "/provider_first/Dropped", "/provider_second/Later"},
        {"/global/Dropped", "/unused/Created", "/unused/Dropped", "/unused/Later"});
}

Y_UNIT_TEST(TopicClusterPrefixOverridesProviderAndGlobal) {
    AssertPaths(SqlToYqlWithMultiScopes(R"sql(
        USE plato;
        PRAGMA TablePathPrefix('kikimr', '/provider');
        CREATE TOPIC BeforeCluster;
        PRAGMA TablePathPrefix('plato', '/cluster_first');
        CREATE TOPIC Created;
        PRAGMA TablePathPrefix('kikimr', '/provider_later');
        PRAGMA TablePathPrefix = '/global_later';
        DROP TOPIC Dropped;
        PRAGMA TablePathPrefix('plato', '/cluster_second');
        CREATE TOPIC Later;
        PRAGMA TablePathPrefix('plato', '/unused');
    )sql", 10, TString(NYql::KikimrProviderName)),
        {"/provider/BeforeCluster", "/cluster_first/Created", "/cluster_first/Dropped", "/cluster_second/Later"},
        {"/provider_later/Dropped", "/global_later/Dropped", "/unused/BeforeCluster", "/unused/Created", "/unused/Dropped", "/unused/Later"});
}

Y_UNIT_TEST(TableFunctions) {
    for (const TString source : {"CONCAT('Input')", "CONCAT_STRICT('Input')", "RANGE('Input')"}) {
        const TString query = TStringBuilder()
            << "USE plato; PRAGMA TablePathPrefix = '/first'; SELECT * FROM " << source << ";"
            << "PRAGMA TablePathPrefix = '/second'; SELECT * FROM " << source << ";";
        AssertPaths(SqlToYqlWithMultiScopes(query), {"/first/Input", "/second/Input"});
    }
    AssertPaths(SqlToYqlWithMultiScopes(R"sql(
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
    settings.EnableTablePathPrefixMultiScopes = true;
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
    settings.EnableTablePathPrefixMultiScopes = true;
    settings.Arena = &arena;
    settings.ClusterMapping["plato"] = TString(NYql::KikimrProviderName);
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

Y_UNIT_TEST(LegacyModeUsesFinalPrefix) {
    NSQLTranslation::TTranslationSettings settings;
    settings.EnableTablePathPrefixMultiScopes = false;
    for (const TString statement : {
            "SELECT * FROM Input;",
            "INSERT INTO Input (key) VALUES (1);",
            "CREATE TABLE Input (key Uint64, PRIMARY KEY (key));",
            "ALTER TABLE Input RENAME TO Output;",
            "DROP TABLE Input;",
            "CREATE TOPIC Input;"}) {
        const TString query = TStringBuilder()
            << "USE plato; PRAGMA TablePathPrefix = '/first';" << statement
            << "PRAGMA TablePathPrefix = '/second';" << statement;
        const auto result = SqlToYqlWithMode(query, NSQLTranslation::ESqlMode::QUERY, 10,
            TString(NYql::KikimrProviderName), EDebugOutput::None, false, settings);
        AssertPaths(result, {"/second/Input"}, {"/first/Input", "/first/Output"});
        if (statement.Contains("RENAME")) {
            AssertPaths(result, {"/second/Output"});
        }
    }
}

Y_UNIT_TEST(LegacyModeKeepsDeferredExpressionAndNamedSourceLookup) {
    NSQLTranslation::TTranslationSettings settings;
    settings.EnableTablePathPrefixMultiScopes = false;
    AssertPaths(SqlToYqlWithSettings(R"sql(
        USE plato;
        $literal = 'Input';
        $expression = 'In' || 'put';
        PRAGMA TablePathPrefix = '/first';
        $query = SELECT * FROM $expression;
        SELECT * FROM $literal;
        SELECT * FROM $expression;
        PRAGMA TablePathPrefix = '/second';
        SELECT * FROM $query;
        SELECT * FROM $query;
    )sql", settings), {"/second/Input", "/second"}, {"/first/Input", "/first"});
}

Y_UNIT_TEST(LegacyModeKeepsProviderAndClusterPrecedence) {
    NSQLTranslation::TTranslationSettings settings;
    settings.EnableTablePathPrefixMultiScopes = false;
    AssertPaths(SqlToYqlWithSettings(R"sql(
        USE plato;
        PRAGMA TablePathPrefix('yt', '/provider_first');
        SELECT * FROM BeforeCluster;
        PRAGMA TablePathPrefix('plato', '/cluster_first');
        SELECT * FROM AfterCluster;
        PRAGMA TablePathPrefix('yt', '/provider_last');
        PRAGMA TablePathPrefix = '/global_last';
        PRAGMA TablePathPrefix('plato', '/cluster_last');
    )sql", settings), {"/cluster_last/BeforeCluster", "/cluster_last/AfterCluster"},
        {"/provider_first/BeforeCluster", "/cluster_first/AfterCluster", "/provider_last/AfterCluster", "/global_last/AfterCluster"});
}

Y_UNIT_TEST(LegacyModeKeepsTableFunctionsAndBackupLookup) {
    NSQLTranslation::TTranslationSettings settings;
    settings.EnableTablePathPrefixMultiScopes = false;
    AssertPaths(SqlToYqlWithSettings(R"sql(
        USE plato;
        PRAGMA UseTablePrefixForEach;
        PRAGMA TablePathPrefix = '/first';
        SELECT * FROM CONCAT('Concatenated');
        SELECT * FROM RANGE('Range');
        SELECT * FROM EACH(AsList('Input'));
        CREATE BACKUP COLLECTION Backup (TABLE Included) WITH (STORAGE = 'local');
        ALTER BACKUP COLLECTION Backup ADD TABLE Added, DROP TABLE Removed;
        PRAGMA TablePathPrefix = '/second';
        SELECT * FROM Input;
    )sql", settings), {"/second/Concatenated", "/second/Range", "/second", "/second/Included", "/second/Added", "/second/Removed"},
        {"/first/Concatenated", "/first/Range", "/first/Included", "/first/Added", "/first/Removed"});
}

Y_UNIT_TEST(LegacyModeKeepsTopicProviderBehavior) {
    NSQLTranslation::TTranslationSettings settings;
    settings.EnableTablePathPrefixMultiScopes = false;
    const auto result = SqlToYqlWithMode(R"sql(
        USE plato;
        PRAGMA TablePathPrefix('kikimr', '/provider');
        CREATE TOPIC Input;
        PRAGMA TablePathPrefix = '/global';
        DROP TOPIC Input;
    )sql", NSQLTranslation::ESqlMode::QUERY, 10, TString(NYql::KikimrProviderName), EDebugOutput::None, false, settings);
    AssertPaths(result, {"/global/Input"}, {"/provider/Input"});
}

Y_UNIT_TEST(LegacyModeKeepsYqlSelectLookup) {
    NSQLTranslation::TTranslationSettings settings;
    settings.EnableTablePathPrefixMultiScopes = false;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;
    AssertPaths(SqlToYqlWithSettings(R"sql(
        USE plato;
        PRAGMA YqlSelect = 'force';
        $table = 'In' || 'put';
        PRAGMA TablePathPrefix = '/first';
        $query = SELECT key FROM Input;
        SELECT key FROM $table;
        PRAGMA TablePathPrefix = '/second';
        SELECT $query;
    )sql", settings), {"/second/Input", "/second"}, {"/first/Input", "/first"});
}

Y_UNIT_TEST(ScopeSwitchDiagnosticCountsChangedAssignmentsAfterReferences) {
    for (const bool enabled : {false, true}) {
        const auto check = [enabled](const TString& query, size_t expected) {
            size_t switches = 0;
            NSQLTranslation::TTranslationSettings settings;
            settings.EnableTablePathPrefixMultiScopes = enabled;
            settings.IncrementCounter = [&](const TString& group, const TString& name) {
                if (group == "TablePathPrefix" && name == "SwitchedScopeToGlobal") {
                    ++switches;
                }
            };
            const auto result = SqlToYqlWithSettings(query, settings);
            UNIT_ASSERT_C(result.IsOk(), Err2Str(result));
            UNIT_ASSERT_VALUES_EQUAL_C(switches, expected, query);
        };
        check("USE plato; PRAGMA TablePathPrefix='/first'; SELECT * FROM Input;", 0);
        check("USE plato; SELECT * FROM Input; PRAGMA TablePathPrefix='/first';", 1);
        check("USE plato; PRAGMA TablePathPrefix='/first'; SELECT * FROM Input; PRAGMA TablePathPrefix='/first';", 0);
        check(R"sql(
            USE plato;
            PRAGMA TablePathPrefix = '/first';
            SELECT * FROM Input;
            PRAGMA TablePathPrefix('yt', '/provider');
            PRAGMA TablePathPrefix('yt', '/provider');
            PRAGMA TablePathPrefix('plato', '/cluster');
            PRAGMA TablePathPrefix('plato', '/cluster');
            PRAGMA TablePathPrefix = '/second';
        )sql", 3);
    }
}

} // Y_UNIT_TEST_SUITE(TablePathPrefixScope)

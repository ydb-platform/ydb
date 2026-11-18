#include "sql_ut.h"

#include <yql/essentials/sql/v1/translation/sql.h>
#include <yql/essentials/sql/v1/translation/sql_query.h>

using namespace NSQLTranslationV1;

Y_UNIT_TEST_SUITE(SqlProjectSettings) {
Y_UNIT_TEST(LibrarySubsetRetainsScopedPragmas) {
    NYql::TProjectSettings settings;
    for (const TString& name : {"ClassicDivision", "CheckedOps", "StrictJoinKeyTypes", "DisableStrictJoinKeyTypes",
                                "UnicodeLiterals", "DisableUnicodeLiterals", "WarnUntypedStringLiterals",
                                "DisableWarnUntypedStringLiterals", "SimplePg", "DisableSimplePg"}) {
        settings.AddPragma(name, TVector<TString>{});
    }
    settings.AddPragma("CLASSIC_DIVISION", "false");
    settings.AddPragma("Checked_Ops", TVector<TString>{"invalid", "extra"});
    settings.AddPragma("ClassicDivision", "true");
    const auto subset = FilterScopedProjectSettings(settings);
    UNIT_ASSERT_VALUES_EQUAL(subset.Pragmas.size(), settings.Pragmas.size());
    for (size_t index = 0; index < settings.Pragmas.size(); ++index) {
        UNIT_ASSERT_VALUES_EQUAL(subset.Pragmas[index].Name, settings.Pragmas[index].Name);
        UNIT_ASSERT_VALUES_EQUAL(subset.Pragmas[index].Value, settings.Pragmas[index].Value);
    }
}

Y_UNIT_TEST(LibrarySubsetDropsUnscopedAndInvalidPragmas) {
    NYql::TProjectSettings settings;
    UNIT_ASSERT(FilterScopedProjectSettings(settings).Pragmas.empty());
    for (const TString& name : {"", "Unknown", "yt.Pool", "config.flags", "yson.Strict", "DqEngine", "TablePathPrefix",
                                "ErrorMsg", "Classic__Division", "_ClassicDivision", "yson.ClassicDivision"}) {
        settings.AddPragma(name, "value");
    }
    UNIT_ASSERT(FilterScopedProjectSettings(settings).Pragmas.empty());
    settings.AddPragma("Unicode_Literals", TVector<TString>{});
    const auto pragmaCount = settings.Pragmas.size();
    const auto subset = FilterScopedProjectSettings(settings);
    UNIT_ASSERT_VALUES_EQUAL(subset.Pragmas.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(subset.Pragmas.front().Name, "Unicode_Literals");
    UNIT_ASSERT(subset.Pragmas.front().Value.empty());
    UNIT_ASSERT_VALUES_EQUAL(settings.Pragmas.size(), pragmaCount);
}

Y_UNIT_TEST(AllowedPragmasAreAcceptedForProjects) {
    const auto translator = MakeTranslator({}, {});
    for (const TString& name : {"ClassicDivision", "AllowDotInAlias", "AnsiRankForNullableKeys", "DisableAnsiRankForNullableKeys",
                                "DqEngine", "BlockEngine", "yson.Strict"}) {
        UNIT_ASSERT_C(translator->AcceptsProjectSetting({.Name = name}), name);
    }
}

Y_UNIT_TEST(DisallowedPragmasAreRejectedForProjects) {
    const auto translator = MakeTranslator({}, {});
    for (const TString& name : {"UdfBridge", "ErrorMsg", "RefSelect", "CompactGroupBy", "DisableCompactGroupBy",
                                "EquiJoin", "DisableUnordered", "AnsiOrderByLimitInUnionAll", "DisableAnsiOrderByLimitInUnionAll", "ERROR_MSG", "Disable_Compact_Group_By"}) {
        UNIT_ASSERT_C(!translator->AcceptsProjectSetting({.Name = name}), name);
    }
}

Y_UNIT_TEST(DisallowedProjectPragmasAreIgnored) {
    const auto expected = SqlToYql("SELECT 1;");
    UNIT_ASSERT_C(expected.IsOk(), expected.Issues.ToString());
    for (bool strict : {false, true}) {
        NSQLTranslation::TTranslationSettings settings;
        settings.StrictConfigValidation = strict;
        settings.ProjectSettings.AddPragma("ERROR_MSG", "must not be evaluated");
        settings.ProjectSettings.AddPragma("RefSelect", TVector<TString>{});
        settings.ProjectSettings.AddPragma("CompactGroupBy", TVector<TString>{});
        settings.ProjectSettings.AddPragma("Disable_Compact_Group_By", TVector<TString>{});
        const auto result = SqlToYqlWithSettings("SELECT 1;", settings);
        UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
        UNIT_ASSERT_C(result.Issues.Empty(), result.Issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.Root->ToString(), expected.Root->ToString());
    }
}

Y_UNIT_TEST(LibraryAndViewModesApplyOnlyScopedProjectSettings) {
    for (const auto mode : {NSQLTranslation::ESqlMode::LIBRARY, NSQLTranslation::ESqlMode::LIMITED_VIEW}) {
        for (bool strict : {false, true}) {
            NSQLTranslation::TTranslationSettings settings;
            settings.Mode = mode;
            settings.StrictConfigValidation = strict;
            const TString query = mode == NSQLTranslation::ESqlMode::LIBRARY ? "$value = 'text'; EXPORT $value;" : "SELECT 'text' AS value;";
            const auto expected = SqlToYqlWithMode("PRAGMA UnicodeLiterals; " + query, settings.Mode, 10, {}, EDebugOutput::None, /*ansiLexer=*/false, settings);
            settings.ProjectSettings.AddPragma("Unicode_Literals", TVector<TString>{});
            settings.ProjectSettings.AddPragma("RelativePathPrefix", "project/");
            settings.ProjectSettings.AddPragma("TablePathPrefix", TVector<TString>{"arnold", "//project"});
            settings.ProjectSettings.AddPragma("DqEngine", "invalid");
            settings.ProjectSettings.AddPragma("Unknown", "value");
            const auto result = SqlToYqlWithMode(query, settings.Mode, 10, {}, EDebugOutput::None, /*ansiLexer=*/false, settings);
            UNIT_ASSERT_C(expected.IsOk(), expected.Issues.ToString());
            UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
            UNIT_ASSERT_C(result.Issues.Empty(), result.Issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.Root->ToString(), expected.Root->ToString());
        }
    }
}

Y_UNIT_TEST(AcceptanceOnlyChecksNames) {
    const auto translator = MakeTranslator({}, {});
    for (const TVector<TString>& values : {TVector<TString>{}, TVector<TString>{"invalid"}, TVector<TString>{"true", "false"}}) {
        UNIT_ASSERT(translator->AcceptsProjectSetting({.Name = "ClassicDivision", .Value = values}));
        UNIT_ASSERT(!translator->AcceptsProjectSetting({.Name = "Unknown", .Value = values}));
    }
}

Y_UNIT_TEST(ProjectPragmaNamesAreNormalized) {
    const auto translator = MakeTranslator({}, {});
    for (bool strict : {false, true}) {
        for (const TString& name : {"Classic_Division", "CLASSIC_DIVISION", "yson.St_rict"}) {
            NSQLTranslation::TTranslationSettings settings;
            settings.StrictConfigValidation = strict;
            settings.ProjectSettings.AddPragma(name, "false");
            const auto result = SqlToYqlWithSettings("SELECT 1 / 2;", settings);
            const auto expected = SqlToYql("PRAGMA " + name + "='false'; SELECT 1 / 2;");
            UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
            UNIT_ASSERT_C(expected.IsOk(), expected.Issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.Root->ToString(), expected.Root->ToString());
            UNIT_ASSERT(translator->AcceptsProjectSetting(settings.ProjectSettings.Pragmas.front()));
        }
    }
}

Y_UNIT_TEST(InvalidProjectPragmaNamesRespectStrictValidation) {
    const auto translator = MakeTranslator({}, {});
    for (const TString& name : {"", "Classic__Division", "_ClassicDivision", "yson._Strict"}) {
        for (bool strict : {false, true}) {
            NSQLTranslation::TTranslationSettings settings;
            settings.StrictConfigValidation = strict;
            settings.ProjectSettings.AddPragma(name, "false");
            settings.ProjectSettings.AddPragma("Classic_Division", "false");
            UNIT_ASSERT(!translator->AcceptsProjectSetting(settings.ProjectSettings.Pragmas.front()));
            const auto result = SqlToYqlWithSettings("SELECT 1 / 2;", settings);
            UNIT_ASSERT_VALUES_EQUAL(result.IsOk(), !strict);
            if (!strict) {
                const auto expected = SqlToYql("PRAGMA ClassicDivision='false'; SELECT 1 / 2;");
                UNIT_ASSERT_C(result.Issues.Empty(), result.Issues.ToString());
                UNIT_ASSERT_C(expected.IsOk(), expected.Issues.ToString());
                UNIT_ASSERT_VALUES_EQUAL(result.Root->ToString(), expected.Root->ToString());
            }
        }
    }
}

Y_UNIT_TEST(YsonProjectPragmaMatchesQueryPragma) {
    for (bool strict : {false, true}) {
        for (const TString& name : {"yson.Strict", "YSON.Strict", "YsOn.Strict", "yson.AutoConvert", "YSON.AutoConvert", "YsOn.AutoConvert"}) {
            NSQLTranslation::TTranslationSettings settings;
            settings.StrictConfigValidation = strict;
            settings.ProjectSettings.AddPragma(name, "false");
            const TString query = "SELECT Yson::ConvertToInt64(Yson::Parse('1'));";
            const auto actual = SqlToYqlWithSettings(query, settings);
            const auto expected = SqlToYql("PRAGMA " + name + "='false'; " + query);
            UNIT_ASSERT_C(actual.IsOk(), actual.Issues.ToString());
            UNIT_ASSERT_C(expected.IsOk(), expected.Issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(actual.Root->ToString(), expected.Root->ToString());
            UNIT_ASSERT(MakeTranslator({}, {})->AcceptsProjectSetting(settings.ProjectSettings.Pragmas.front()));
        }
    }
}

Y_UNIT_TEST(RelativePathPrefixAppliesToDdl) {
    for (const TString& query : {"ALTER SEQUENCE sequence INCREMENT 2;",
                                 "ALTER DATABASE users SET (topics_metrics_level = 'topic');",
                                 "CREATE OBJECT secretId (TYPE SECRET);"}) {
        NSQLTranslation::TTranslationSettings settings;
        settings.PathPrefix = "/Root/database";
        settings.DefaultCluster = "plato";
        const auto expected = SqlToYqlWithMode("USE plato; PRAGMA RelativePathPrefix='folder'; " + query,
                                               NSQLTranslation::ESqlMode::QUERY, 10, "kikimr", EDebugOutput::None, /*ansiLexer=*/false, settings);
        settings.ProjectSettings.AddPragma("RelativePathPrefix", "folder");
        const auto result = SqlToYqlWithMode("USE plato; " + query,
                                             NSQLTranslation::ESqlMode::QUERY, 10, "kikimr", EDebugOutput::None, /*ansiLexer=*/false, settings);
        UNIT_ASSERT_C(expected.IsOk(), expected.Issues.ToString());
        UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.Root->ToString(), expected.Root->ToString());
    }
}

Y_UNIT_TEST(ProjectPragmaAndQueryOverride) {
    NSQLTranslation::TTranslationSettings settings;
    settings.ProjectSettings.AddPragma("ClassicDivision", "false");
    const auto project = SqlToYqlWithSettings("SELECT 1 / 2;", settings);
    const auto explicitPragma = SqlToYql("PRAGMA ClassicDivision='false'; SELECT 1 / 2;");
    UNIT_ASSERT_C(project.IsOk(), project.Issues.ToString());
    UNIT_ASSERT(explicitPragma.IsOk());
    UNIT_ASSERT_VALUES_EQUAL(project.Root->ToString(), explicitPragma.Root->ToString());
    const auto override = SqlToYqlWithSettings("PRAGMA ClassicDivision='true'; SELECT 1 / 2;", settings);
    const auto expected = SqlToYql("PRAGMA ClassicDivision='true'; SELECT 1 / 2;");
    UNIT_ASSERT_C(override.IsOk(), override.Issues.ToString());
    UNIT_ASSERT_VALUES_EQUAL(override.Root->ToString(), expected.Root->ToString());
}

Y_UNIT_TEST(QueryOverridesProjectRelativePathPrefix) {
    NSQLTranslation::TTranslationSettings settings;
    settings.PathPrefix = "/Root/database";
    settings.DefaultCluster = "plato";
    settings.ClusterPathPrefixes["plato"] = "/Cluster/root";
    const TString query = "PRAGMA RelativePathPrefix='query'; ALTER SEQUENCE sequence INCREMENT 2;";
    const auto expected = SqlToYqlWithMode(query, NSQLTranslation::ESqlMode::QUERY,
                                           10, "kikimr", EDebugOutput::None, /*ansiLexer=*/false, settings);
    settings.ProjectSettings.AddPragma("RelativePathPrefix", "project");
    const auto result = SqlToYqlWithMode(query, NSQLTranslation::ESqlMode::QUERY,
                                         10, "kikimr", EDebugOutput::None, /*ansiLexer=*/false, settings);
    UNIT_ASSERT_C(expected.IsOk(), expected.Issues.ToString());
    UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
    UNIT_ASSERT_VALUES_EQUAL(result.Root->ToString(), expected.Root->ToString());
}

Y_UNIT_TEST(QueryTablePathPrefixDisablesProjectRelativePathPrefix) {
    for (const TString& statement : {"ALTER SEQUENCE sequence INCREMENT 2;",
                                     "ALTER DATABASE users SET (topics_metrics_level = 'topic');",
                                     "CREATE OBJECT secretId (TYPE SECRET);"}) {
        for (const TString& prefix : {"/query", ""}) {
            NSQLTranslation::TTranslationSettings settings;
            settings.PathPrefix = "/Root/database";
            settings.DefaultCluster = "plato";
            const TString query = "PRAGMA TablePathPrefix='" + prefix + "'; " + statement;
            const auto expected = SqlToYqlWithMode(query, NSQLTranslation::ESqlMode::QUERY,
                                                   10, "kikimr", EDebugOutput::None, /*ansiLexer=*/false, settings);
            settings.ProjectSettings.AddPragma("RelativePathPrefix", "project");
            const auto result = SqlToYqlWithMode(query, NSQLTranslation::ESqlMode::QUERY,
                                                 10, "kikimr", EDebugOutput::None, /*ansiLexer=*/false, settings);
            UNIT_ASSERT_C(expected.IsOk(), expected.Issues.ToString());
            UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.Root->ToString(), expected.Root->ToString());
        }
    }
}

Y_UNIT_TEST(QueryRelativePathPrefixDisablesProjectTablePathPrefixes) {
    for (const TString& clusterPrefix : {"", "/Cluster/root"}) {
        for (const TString& relativePrefix : {"", "query"}) {
            NSQLTranslation::TTranslationSettings settings;
            settings.PathPrefix = "/Root";
            settings.DefaultCluster = "plato";
            settings.ClusterPathPrefixes["plato"] = clusterPrefix;
            const TString query = "PRAGMA RelativePathPrefix='" + relativePrefix + "'; SELECT * FROM plato.input; SELECT * FROM hahn.input;";
            const auto expected = SqlToYqlWithMode(query, NSQLTranslation::ESqlMode::QUERY, 10, "kikimr",
                                                   EDebugOutput::None, /*ansiLexer=*/false, settings);
            for (const TVector<TString>& values : {TVector<TString>{"/project"}, TVector<TString>{"plato", "/project"},
                                                   TVector<TString>{"kikimr", "/project"}}) {
                settings.ProjectSettings.AddPragma("TablePathPrefix", values);
                const auto actual = SqlToYqlWithMode(query, NSQLTranslation::ESqlMode::QUERY, 10, "kikimr",
                                                     EDebugOutput::None, /*ansiLexer=*/false, settings);
                UNIT_ASSERT_C(expected.IsOk(), expected.Issues.ToString());
                UNIT_ASSERT_C(actual.IsOk(), actual.Issues.ToString());
                UNIT_ASSERT_VALUES_EQUAL(actual.Root->ToString(), expected.Root->ToString());
            }
        }
    }
}

Y_UNIT_TEST(QueryTablePathPrefixRestoresProjectRelativePaths) {
    for (const TString& pragmas : {"PRAGMA TablePathPrefix='/query';",
                                   "PRAGMA TablePathPrefix='';",
                                   "PRAGMA TablePathPrefix('plato', '/query');",
                                   "PRAGMA TablePathPrefix('kikimr', '/query');",
                                   "PRAGMA TablePathPrefix('plato', '/query'); PRAGMA TablePathPrefix='/global';"}) {
        for (const TString& clusterPrefix : {"/Cluster/root", ""}) {
            NSQLTranslation::TTranslationSettings settings;
            settings.PathPrefix = "/Root/database";
            settings.DefaultCluster = "plato";
            settings.ClusterPathPrefixes["plato"] = clusterPrefix;
            const TString query = pragmas + " SELECT * FROM plato.input; SELECT * FROM hahn.input;";
            const auto expected = SqlToYqlWithMode(query, NSQLTranslation::ESqlMode::QUERY,
                                                   10, "kikimr", EDebugOutput::None, /*ansiLexer=*/false, settings);
            settings.ProjectSettings.AddPragma("RelativePathPrefix", "project");
            const auto result = SqlToYqlWithMode(query, NSQLTranslation::ESqlMode::QUERY,
                                                 10, "kikimr", EDebugOutput::None, /*ansiLexer=*/false, settings);
            UNIT_ASSERT_C(expected.IsOk(), expected.Issues.ToString());
            UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.Root->ToString(), expected.Root->ToString());
        }
    }
}

Y_UNIT_TEST(UnknownProjectPragmasRespectStrictValidation) {
    for (bool strict : {false, true}) {
        NSQLTranslation::TTranslationSettings settings;
        settings.StrictConfigValidation = strict;
        settings.ProjectSettings.AddPragma("Unknown", "value");
        settings.ProjectSettings.AddPragma("yson.Unknown", "value");
        size_t errors = 0;
        settings.IncrementCounter = [&](const TString& group, const TString& name) {
            if (group == "sql_errors" && name != "AstToYqlError") {
                ++errors;
            }
        };
        const auto result = SqlToYqlWithSettings("SELECT 1;", settings);
        UNIT_ASSERT_VALUES_EQUAL(result.IsOk(), !strict);
        if (strict) {
            UNIT_ASSERT_STRING_CONTAINS(result.Issues.ToString(), "Unknown pragma");
        } else {
            UNIT_ASSERT_C(result.Issues.Empty(), result.Issues.ToString());
        }
        UNIT_ASSERT_VALUES_EQUAL(errors, strict ? 2 : 0);
    }
}

Y_UNIT_TEST(KnownProjectPragmasValidateValues) {
    for (const auto& name : {"ClassicDivision", "yson.Strict"}) {
        NSQLTranslation::TTranslationSettings settings;
        settings.ProjectSettings.AddPragma(name, "invalid");
        const auto result = SqlToYqlWithSettings("SELECT 1;", settings);
        UNIT_ASSERT(!result.IsOk());
        UNIT_ASSERT_STRING_CONTAINS(result.Issues.ToString(), "Expected");
    }
}

Y_UNIT_TEST(RecognizedPragmaOutsideFormerWhitelist) {
    NSQLTranslation::TTranslationSettings settings;
    settings.StrictConfigValidation = true;
    settings.ProjectSettings.AddPragma("PositionalUnionAll", TVector<TString>{});
    const auto result = SqlToYqlWithSettings("SELECT 1;", settings);
    UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
}

Y_UNIT_TEST(SupportedSettingsAfterUnknownAreApplied) {
    NSQLTranslation::TTranslationSettings settings;
    settings.ProjectSettings.AddPragma("Unknown", "value");
    settings.ProjectSettings.AddPragma("ClassicDivision", "false");
    settings.ProjectSettings.AddPragma("yson.Unknown", "value");
    const auto result = SqlToYqlWithSettings("SELECT 1 / 2;", settings);
    const auto expected = SqlToYql("PRAGMA ClassicDivision='false'; SELECT 1 / 2;");
    UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
    UNIT_ASSERT(expected.IsOk());
    UNIT_ASSERT_VALUES_EQUAL(result.Root->ToString(), expected.Root->ToString());
}

Y_UNIT_TEST(AllSupportedSettingsAreValidated) {
    NSQLTranslation::TTranslationSettings settings;
    settings.StrictConfigValidation = true;
    settings.ProjectSettings.AddPragma("ClassicDivision", "invalid");
    settings.ProjectSettings.AddPragma("Unknown", "value");
    settings.ProjectSettings.AddPragma("yson.Strict", "invalid");
    const auto result = SqlToYqlWithSettings("SELECT 1;", settings);
    UNIT_ASSERT(!result.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(result.Issues.ToString(), "ClassicDivision");
    UNIT_ASSERT_STRING_CONTAINS(result.Issues.ToString(), "yson.Strict");
}
} // Y_UNIT_TEST_SUITE(SqlProjectSettings)

Y_UNIT_TEST_SUITE(RelativePathPrefix) {

NYql::TAstParseResult Translate(const TString& query, const TString& clusterPrefix = {}) {
    NSQLTranslation::TTranslationSettings settings;
    settings.PathPrefix = "/Root/database";
    if (clusterPrefix) {
        settings.ClusterPathPrefixes["plato"] = clusterPrefix;
    }
    return SqlToYqlWithMode("USE plato; " + query, NSQLTranslation::ESqlMode::QUERY,
                            10, "kikimr", EDebugOutput::None, /*ansiLexer=*/false, settings);
}

void AssertPath(const TString& query, const TString& expected, const TString& clusterPrefix = {}) {
    const auto result = Translate(query, clusterPrefix);
    UNIT_ASSERT_C(result.IsOk(), Err2Str(result));
    UNIT_ASSERT_STRING_CONTAINS(GetPrettyPrint(result), TStringBuilder() << "(String '\"" << expected << "\")");
}

void AssertQuotedPath(const TString& query, const TString& expected) {
    const auto result = Translate(query);
    UNIT_ASSERT_C(result.IsOk(), Err2Str(result));
    UNIT_ASSERT_STRING_CONTAINS(GetPrettyPrint(result), TStringBuilder() << "'\"" << expected << "\"");
}

void AssertError(const TString& query, const TString& expected) {
    const auto result = Translate(query);
    UNIT_ASSERT(!result.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(result), expected);
}

Y_UNIT_TEST(ResolvesPathsFromBase) {
    AssertPath("PRAGMA RelativePathPrefix = 'folder'; SELECT * FROM users;", "/Root/database/folder/users");
    AssertPath("PRAGMA RelativePathPrefix = './folder'; SELECT * FROM users;", "/Root/database/folder/users");
    AssertPath("PRAGMA RelativePathPrefix = '...'; SELECT * FROM users;", "/Root/database/.../users");
    AssertPath("PRAGMA RelativePathPrefix = './.'; SELECT * FROM users;", "/Root/database/users");
    AssertPath("PRAGMA RelativePathPrefix = '../sibling'; SELECT * FROM users;", "/Root/sibling/users");
    AssertPath("PRAGMA RelativePathPrefix = '../..'; SELECT * FROM users;", "/users");
    AssertPath("PRAGMA RelativePathPrefix = 'folder'; SELECT * FROM `/Other/users`;", "/Other/users");
    AssertPath("PRAGMA RelativePathPrefix = ''; SELECT * FROM users;", "/Root/database/users");
    AssertPath("PRAGMA RelativePathPrefix = 'folder'; SELECT * FROM users;",
               "/Cluster/root/folder/users", "/Cluster/root");
}

Y_UNIT_TEST(OtherSchemeObjects) {
    AssertPath("PRAGMA RelativePathPrefix = 'folder'; CREATE TABLE users (id Uint64, PRIMARY KEY (id));",
               "/Root/database/folder/users");
    AssertPath("PRAGMA RelativePathPrefix = 'folder'; CREATE TOPIC users;", "/Root/database/folder/users");
    AssertPath("PRAGMA RelativePathPrefix = 'folder'; CREATE OBJECT secretId (TYPE SECRET);",
               "/Root/database/folder/secretId");
    AssertQuotedPath("PRAGMA RelativePathPrefix = 'folder'; GRANT CONNECT ON users TO user;",
                     "/Root/database/folder/users");
    AssertQuotedPath("PRAGMA RelativePathPrefix = 'folder'; REVOKE CONNECT ON users FROM user;",
                     "/Root/database/folder/users");
    AssertPath("PRAGMA RelativePathPrefix = 'folder'; ALTER SEQUENCE sequence INCREMENT 2;",
               "/Root/database/folder/sequence");
    AssertPath("PRAGMA RelativePathPrefix = 'folder'; "
               "ALTER TABLE users ADD CHANGEFEED feed WITH (MODE = 'UPDATES', FORMAT = 'json');",
               "/Root/database/folder/users");
    AssertPath("PRAGMA RelativePathPrefix = 'folder'; ALTER DATABASE users SET (topics_metrics_level = 'topic');",
               "/Root/database/folder/users");
}

Y_UNIT_TEST(TransferPaths) {
    const auto result = Translate("PRAGMA RelativePathPrefix = 'folder';"
                                  "CREATE TRANSFER `TransferName` FROM `TopicName` TO `TableName`"
                                  "USING ($x) -> { return $x; };");
    UNIT_ASSERT_C(result.IsOk(), Err2Str(result));
    const auto ast = GetPrettyPrint(result);
    UNIT_ASSERT_STRING_CONTAINS(ast, "/Root/database/folder/TopicName");
    UNIT_ASSERT_STRING_CONTAINS(ast, "/Root/database/folder/TableName");
}

Y_UNIT_TEST(OnlyKikimrProvider) {
    const auto result = SqlToYqlWithMode("USE plato; PRAGMA RelativePathPrefix = 'folder'; SELECT * FROM users;",
                                         NSQLTranslation::ESqlMode::QUERY, 10, "yt");
    UNIT_ASSERT(!result.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(result), "RelativePathPrefix is supported only for the kikimr provider");
}

Y_UNIT_TEST(RejectsAbsoluteAndRepeatedPrefixes) {
    AssertError("PRAGMA RelativePathPrefix = '/Other'; SELECT * FROM users;", "Expected a relative path");
    AssertError("PRAGMA RelativePathPrefix = '/'; SELECT * FROM users;", "Expected a relative path");
    AssertError("PRAGMA RelativePathPrefix = '//'; SELECT * FROM users;", "Expected a relative path");
    AssertError("PRAGMA RelativePathPrefix = '///'; SELECT * FROM users;", "Expected a relative path");
    AssertError("DECLARE $prefix AS String; PRAGMA RelativePathPrefix = $prefix; SELECT * FROM users;",
                "Expected string");
    AssertError("$prefix = 'folder'; PRAGMA RelativePathPrefix = $prefix; SELECT * FROM users;",
                "Expected string");
    AssertError("PRAGMA RelativePathPrefix = 'folder'; PRAGMA RelativePathPrefix = 'other'; SELECT * FROM users;",
                "RelativePathPrefix must be specified only once");
    AssertError("PRAGMA TablePathPrefix = 'folder'; PRAGMA RelativePathPrefix = 'other'; SELECT * FROM users;",
                "RelativePathPrefix cannot be combined with TablePathPrefix");
    AssertError("PRAGMA RelativePathPrefix = 'folder'; PRAGMA TablePathPrefix = 'other'; SELECT * FROM users;",
                "RelativePathPrefix cannot be combined with TablePathPrefix");
    AssertError("PRAGMA TablePathPrefix('yt', 'folder'); PRAGMA RelativePathPrefix = 'other'; SELECT * FROM users;",
                "RelativePathPrefix cannot be combined with TablePathPrefix");
    AssertError("PRAGMA RelativePathPrefix = 'folder'; PRAGMA TablePathPrefix('yt', 'other'); SELECT * FROM users;",
                "RelativePathPrefix cannot be combined with TablePathPrefix");
}

Y_UNIT_TEST(RejectsPrefixAfterSqlStatement) {
    AssertError("SELECT * FROM users; PRAGMA RelativePathPrefix = 'folder';",
                "RelativePathPrefix must be specified before SQL statements");
    AssertError("CREATE TABLE users (id Uint64, PRIMARY KEY (id)); PRAGMA RelativePathPrefix = 'folder';",
                "RelativePathPrefix must be specified before SQL statements");
    AssertError("CREATE OBJECT secretId (TYPE SECRET); PRAGMA RelativePathPrefix = 'folder';",
                "RelativePathPrefix must be specified before SQL statements");
    AssertError("CREATE STREAMING QUERY MyQuery AS DO BEGIN PRAGMA RelativePathPrefix = 'folder';"
                "USE plato; $source = SELECT * FROM Input; INSERT INTO Output SELECT * FROM $source; END DO;",
                "RelativePathPrefix is allowed only at the top level");
}

Y_UNIT_TEST(ExistingTablePathPrefixIsUnchanged) {
    AssertPath("PRAGMA TablePathPrefix = 'folder'; SELECT * FROM users;", "folder/users");
    AssertPath("PRAGMA TablePathPrefix = 'folder'; SELECT * FROM `/Other/users`;", "/Other/users");
    AssertPath("PRAGMA TablePathPrefix = 'folder1'; PRAGMA TablePathPrefix = 'folder2'; SELECT * FROM users;", "folder2/users");
    AssertPath("PRAGMA TablePathPrefix = 'folder'; CREATE OBJECT secretId (TYPE SECRET);", "secretId");
    AssertPath("PRAGMA TablePathPrefix = 'folder'; ALTER DATABASE users SET (topics_metrics_level = 'topic');", "users");
}

} // Y_UNIT_TEST_SUITE(RelativePathPrefix)

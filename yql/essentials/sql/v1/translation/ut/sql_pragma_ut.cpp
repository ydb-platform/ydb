#include "sql_ut.h"

#include <yql/essentials/sql/v1/translation/sql.h>

using namespace NSQLTranslationV1;

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

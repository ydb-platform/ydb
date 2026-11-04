#include <ydb/core/kqp/common/simple/database_config_audit.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpDatabaseConfigAudit) {
    Y_UNIT_TEST(SqlTranslationBlock) {
        // This is the block shape emitted by TAlterDatabase::DoInit.
        const auto ast = NYql::ParseAst(R"yql(
(
  (let world (block '(
    (let sink (DataSink 'kikimr 'db))
    (let world (Write! world sink (Key '('databasePath (String '"/Root/db"))) (Void)
      '('('mode 'alterDatabase) '('ENABLE_CLIENT_MODIFICATIONS (String '"false")))))
    (return world)
  )))
  (return world)
)
)yql");
        UNIT_ASSERT_C(ast.IsOk(), ast.Issues.ToString());
        const auto changes = CollectDatabaseConfigAuditSettings(ast.Root);
        UNIT_ASSERT_VALUES_EQUAL(changes.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(changes.front().Database, "/Root/db");
        UNIT_ASSERT_STRING_CONTAINS(changes.front().Changes, "ENABLE_CLIENT_MODIFICATIONS=(String");
    }

    Y_UNIT_TEST(SetAndReset) {
        const auto ast = NYql::ParseAst(R"yql(
(
  (let world (Write! world sink (Key '('databasePath (String '"/Root/a"))) (Void)
    '('('mode 'alterDatabase) '('ENABLE_CLIENT_MODIFICATIONS (Bool 'false)))))
  (let world (Write! world sink (Key '('databasePath (String '"/Root/b"))) (Void)
    '('('mode 'alterDatabase) '('ENABLE_CLIENT_MODIFICATIONS (Void)))))
)
)yql");
        UNIT_ASSERT_C(ast.IsOk(), ast.Issues.ToString());
        const auto changes = CollectDatabaseConfigAuditSettings(ast.Root);
        UNIT_ASSERT_VALUES_EQUAL(changes.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(changes[0].Database, "/Root/a");
        UNIT_ASSERT_VALUES_EQUAL(changes[0].Changes, "ENABLE_CLIENT_MODIFICATIONS=false");
        UNIT_ASSERT_VALUES_EQUAL(changes[1].Database, "/Root/b");
        UNIT_ASSERT_VALUES_EQUAL(changes[1].Changes, "ENABLE_CLIENT_MODIFICATIONS=INHERIT");
    }

    Y_UNIT_TEST(UnknownNamesAndInvalidTypes) {
        const auto ast = NYql::ParseAst(R"yql(
(let world (Write! world sink (Key '('databasePath (String '"/Root/db"))) (Void)
  '('('mode 'alterDatabase)
    '('ENABLE_CLIENT_MODIFICATIONS (String '"false"))
    '('UNKNOWN_FLAG (Bool 'true))
    '('MAX_SHARDS (Uint64 '"10")))))
)yql");
        UNIT_ASSERT_C(ast.IsOk(), ast.Issues.ToString());
        const auto changes = CollectDatabaseConfigAuditSettings(ast.Root);
        UNIT_ASSERT_VALUES_EQUAL(changes.size(), 1);
        UNIT_ASSERT_STRING_CONTAINS(changes.front().Changes, "ENABLE_CLIENT_MODIFICATIONS=(String");
        UNIT_ASSERT_STRING_CONTAINS(changes.front().Changes, "UNKNOWN_FLAG=true");
        UNIT_ASSERT(!changes.front().Changes.Contains("MAX_SHARDS"));
    }

    Y_UNIT_TEST(ExcludesLegacySettingsAndQuotedCode) {
        const auto ast = NYql::ParseAst(R"yql(
(
  (let world (Write! world sink (Key '('databasePath (String '"/Root/db"))) (Void)
    '('('mode 'alterDatabase) '('owner 'admin))))
  (let world (Write! world sink (Key '('databasePath (String '"/Root/db"))) (Void)
    '('('mode 'alterDatabase) '('MAX_PATHS (Uint64 '"10")))))
  '(Write! world sink (Key '('databasePath (String '"/Root/db"))) (Void)
    '('('mode 'alterDatabase) '('ENABLE_CLIENT_MODIFICATIONS (Bool 'false))))
  '(block '(
    (let world (Write! world sink (Key '('databasePath (String '"/Root/db"))) (Void)
      '('('mode 'alterDatabase) '('ENABLE_CLIENT_MODIFICATIONS (Bool 'false)))))
    (return world)
  ))
)
)yql");
        UNIT_ASSERT_C(ast.IsOk(), ast.Issues.ToString());
        UNIT_ASSERT(CollectDatabaseConfigAuditSettings(ast.Root).empty());
        UNIT_ASSERT(CollectDatabaseConfigAuditSettings(nullptr).empty());
    }

    Y_UNIT_TEST(ExcludesOtherWriteOperations) {
        const auto ast = NYql::ParseAst(R"yql(
(
  (let world (Write! world sink (Key '('table (String '"/Root/db"))) (Void)
    '('('mode 'alterDatabase) '('ENABLE_CLIENT_MODIFICATIONS (Bool 'false)))))
  (let world (Write! world sink (Key '('databasePath (String '"/Root/db"))) (Void)
    '('('mode 'otherOperation) '('ENABLE_CLIENT_MODIFICATIONS (Bool 'false)))))
)
)yql");
        UNIT_ASSERT_C(ast.IsOk(), ast.Issues.ToString());
        UNIT_ASSERT(CollectDatabaseConfigAuditSettings(ast.Root).empty());
    }
}

} // namespace NKikimr::NKqp

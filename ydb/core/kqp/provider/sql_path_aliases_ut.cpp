#include "sql_path_aliases.h"

#include <ydb/core/kqp/common/kqp_yql.h>

#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/sql/v1/sql.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>

#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/arena.h>

namespace NYql {
namespace {

TString RewriteSql(TStringBuf sql, TStringBuf pathPrefix = {}, bool dynamicCluster = false, bool withAliases = true,
    bool expectUnchanged = false) {
    google::protobuf::Arena arena;
    NSQLTranslation::TTranslationSettings settings;
    settings.DefaultCluster = "plato";
    settings.ClusterMapping = {{"plato", TString(KikimrProviderName)}};
    settings.SyntaxVersion = 1;
    settings.Mode = NSQLTranslation::ESqlMode::QUERY;
    settings.Arena = &arena;
    settings.PathPrefix = TString(pathPrefix);
    if (dynamicCluster) {
        settings.DynamicClusterProvider = TString(KikimrProviderName);
    }

    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
    NSQLTranslationV1::TParsers parsers;
    parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory(false);

    auto ast = NSQLTranslationV1::SqlToYql(lexers, parsers, TString(sql), settings);
    UNIT_ASSERT_C(ast.IsOk(), ast.Issues.ToString());

    TExprContext ctx;
    TExprNode::TPtr query;
    UNIT_ASSERT_C(CompileExpr(*ast.Root, query, ctx, nullptr, nullptr), ctx.IssueManager.GetIssues().ToString());
    const auto original = query.Get();
    std::function<TString(TStringBuf)> normalizePath;
    if (withAliases) {
        normalizePath = [](TStringBuf path) {
            if (path == "/alias") {
                return TString("/canonical");
            }
            return path.StartsWith("/alias/") ? TString("/canonical") + TString(path.SubStr(6)) : TString(path);
        };
    }
    UNIT_ASSERT_C(RewriteSqlPathAliases(query, ctx, "plato", normalizePath), ctx.IssueManager.GetIssues().ToString());
    if (!withAliases || expectUnchanged) {
        UNIT_ASSERT_VALUES_EQUAL(query.Get(), original);
    }
    const auto rewritten = query.Get();
    UNIT_ASSERT_C(RewriteSqlPathAliases(query, ctx, "plato", normalizePath), ctx.IssueManager.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(query.Get(), rewritten);
    return KqpExprToPrettyString(*query, ctx);
}

}

Y_UNIT_TEST_SUITE(SqlPathAliases) {
    Y_UNIT_TEST(EmptyConfigLeavesSqlGraphUnchanged) {
        for (const TString sql : {
            "SELECT * FROM `/alias/table`;",
            "CREATE TABLE `/alias/table` (key Uint64, PRIMARY KEY (key));",
        }) {
            const auto unchanged = RewriteSql(sql, {}, false, false);
            UNIT_ASSERT_STRING_CONTAINS_C(unchanged, "/alias/table", sql << '\n' << unchanged);
        }
    }

    Y_UNIT_TEST(ConfiguredAliasesLeaveUnmatchedPathUnchanged) {
        const auto unchanged = RewriteSql("SELECT * FROM `/other/table`;", {}, false, true, true);
        UNIT_ASSERT_STRING_CONTAINS(unchanged, "/other/table");
    }

    Y_UNIT_TEST(LiteralPathsFromSql) {
        for (const TString sql : {
            "SELECT * FROM `/alias/table`;",
            "$p = '/alias/table'; SELECT * FROM $p;",
            "DROP VIEW `/alias/table`;",
            "DROP EXTERNAL DATA SOURCE `/alias/table`;",
            "DROP ASYNC REPLICATION `/alias/table`;",
            "DROP TRANSFER `/alias/table`;",
            "DROP SECRET `/alias/table`;",
        }) {
            const auto rewritten = RewriteSql(sql);
            UNIT_ASSERT_STRING_CONTAINS_C(rewritten, "/canonical/table", sql << '\n' << rewritten);
        }
    }

    Y_UNIT_TEST(ReplicationOnlyRewritesLocalTarget) {
        const auto rewritten = RewriteSql("CREATE ASYNC REPLICATION replication FOR `/alias/remote` AS `/alias/table` WITH (ENDPOINT = 'localhost:2135', DATABASE = '/Root');");
        UNIT_ASSERT_STRING_CONTAINS(rewritten, "/canonical/table");
        UNIT_ASSERT_STRING_CONTAINS(rewritten, "/alias/remote");
        UNIT_ASSERT_VALUES_EQUAL(rewritten.find("/canonical/remote"), TString::npos);
    }

    Y_UNIT_TEST(PathPrefixIsRewritten) {
        const auto rewritten = RewriteSql("SELECT * FROM table;", "/alias");
        UNIT_ASSERT_STRING_CONTAINS(rewritten, "/canonical/table");
    }

    Y_UNIT_TEST(TableRenameTargetIsRewritten) {
        const auto rewritten = RewriteSql("ALTER TABLE `/alias/old` RENAME TO `/alias/new`;");
        UNIT_ASSERT_STRING_CONTAINS(rewritten, "/canonical/old");
        UNIT_ASSERT_STRING_CONTAINS(rewritten, "/canonical/new");
        UNIT_ASSERT_VALUES_EQUAL(rewritten.find("/alias/"), TString::npos);
    }

    Y_UNIT_TEST(PrefixedDmlPathsAreRewrittenInOnePass) {
        for (const TString sql : {
            "PRAGMA TablePathPrefix = '/alias'; REPLACE INTO table (key) VALUES (1);",
            "PRAGMA TablePathPrefix = '/alias'; UPDATE table SET value = 'updated' WHERE key = 1;",
            "PRAGMA TablePathPrefix = '/alias'; DELETE FROM table WHERE key = 1;",
        }) {
            const auto rewritten = RewriteSql(sql);
            UNIT_ASSERT_STRING_CONTAINS_C(rewritten, "/canonical/table", sql << '\n' << rewritten);
            UNIT_ASSERT_VALUES_EQUAL_C(rewritten.find("/alias/table"), TString::npos, sql << '\n' << rewritten);
        }
    }

    Y_UNIT_TEST(PrefixedDropThenCreatePathsAreRewrittenInOnePass) {
        const auto rewritten = RewriteSql("PRAGMA TablePathPrefix = '/alias'; DROP TABLE IF EXISTS table; CREATE TABLE table (key Uint64, PRIMARY KEY (key));");
        UNIT_ASSERT_STRING_CONTAINS(rewritten, "/canonical/table");
        UNIT_ASSERT_VALUES_EQUAL(rewritten.find("/alias/table"), TString::npos);
    }

    Y_UNIT_TEST(PermissionAndExternalTableOptions) {
        const auto permission = RewriteSql("GRANT SELECT ON `/alias/table` TO user;");
        UNIT_ASSERT_STRING_CONTAINS(permission, "/canonical/table");

        const auto externalTable = RewriteSql("CREATE EXTERNAL TABLE `/alias/table` (key Uint64) WITH (DATA_SOURCE = '/alias/ds', LOCATION = '/');");
        UNIT_ASSERT_STRING_CONTAINS(externalTable, "/canonical/table");
        UNIT_ASSERT_STRING_CONTAINS(externalTable, "/canonical/ds");
    }

    Y_UNIT_TEST(BackupCollectionPrefixAndEntry) {
        const auto rewritten = RewriteSql("PRAGMA TablePathPrefix = '/alias'; CREATE BACKUP COLLECTION collection (TABLE table) WITH (STORAGE = 'cluster');");
        UNIT_ASSERT_STRING_CONTAINS(rewritten, "/canonical/table");
        UNIT_ASSERT_VALUES_EQUAL(rewritten.find("/alias"), TString::npos);
    }

    Y_UNIT_TEST(OtherClusterIsNotRewritten) {
        const auto rewritten = RewriteSql("SELECT * FROM `/remote/source`.`/alias/table`;", {}, true);
        UNIT_ASSERT_STRING_CONTAINS(rewritten, "/alias/table");
        UNIT_ASSERT_VALUES_EQUAL(rewritten.find("/canonical/table"), TString::npos);
    }
}

}

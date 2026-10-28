#include "yql_kikimr_provider_impl.h"
#include "yql_kikimr_settings.h"

#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/core/kqp/common/kqp_yql.h>

#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/core/type_ann/type_ann_core.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/sql_types/yql_callable_names.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/providers/common/provider/yql_data_provider_impl.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/sql/v1/translation/sql.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/time_provider/time_provider.h>
#include <library/cpp/random_provider/random_provider.h>

#include <google/protobuf/arena.h>

namespace NYql {
namespace {

class TTestProvider : public TDataProviderBase {
public:
    TTestProvider(TStringBuf callable, TAutoPtr<IGraphTransformer> intents)
        : Callable(callable)
        , Intents(std::move(intents))
    {}

    TStringBuf GetName() const override {
        return KikimrProviderName;
    }

    bool CanParse(const TExprNode& node) override {
        return node.IsCallable(Callable);
    }

    IGraphTransformer& GetIntentDeterminationTransformer() override {
        return *Intents;
    }

private:
    TStringBuf Callable;
    TAutoPtr<IGraphTransformer> Intents;
};

struct TAliasRewriteFixture {
    TIntrusivePtr<NKikimr::NMiniKQL::IFunctionRegistry> Registry =
        NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
    TExprContext Ctx;
    TTypeAnnotationContext Types;
    TIntrusivePtr<TKikimrConfiguration> Config = MakeIntrusive<TKikimrConfiguration>();
    TIntrusivePtr<TKikimrSessionContext> Session = MakeIntrusive<TKikimrSessionContext>(Registry.Get(), Config,
        CreateDefaultTimeProvider(), CreateDeterministicRandomProvider(1), nullptr);
    TAutoPtr<IGraphTransformer> Intents;

    TAliasRewriteFixture(std::function<TString(TStringBuf)> normalizePath, bool registerWrites = false) {
        Config->NormalizePath = std::move(normalizePath);
        Session->SetCluster("plato");
        Session->SetDatabase("/canonical");
        Types.AddDataSource(KikimrProviderName, new TTestProvider(ReadName,
            CreateSqlPathAliasesTransformer(Session, new TNullTransformer)));
        Types.AddDataSink(KikimrProviderName, new TTestProvider(WriteName, registerWrites
            ? CreateKiSinkIntentDeterminationTransformer(Session)
            : CreateSqlPathAliasesTransformer(Session, new TNullTransformer)));
        Intents = CreateIntentDeterminationTransformer(Types);
    }

    TExprNode::TPtr CompileSql(TStringBuf sql, TStringBuf pathPrefix = {}, bool dynamicCluster = false) {
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
        TExprNode::TPtr query;
        UNIT_ASSERT_C(CompileExpr(*ast.Root, query, Ctx, nullptr, nullptr), Ctx.IssueManager.GetIssues().ToString());
        return query;
    }

    TExprNode::TPtr CompileAst(TStringBuf text) {
        auto ast = ParseAst(TString(text));
        UNIT_ASSERT_C(ast.IsOk(), ast.Issues.ToString());
        TExprNode::TPtr query;
        UNIT_ASSERT_C(CompileExpr(*ast.Root, query, Ctx, nullptr, nullptr), Ctx.IssueManager.GetIssues().ToString());
        return query;
    }

    void Rewrite(TExprNode::TPtr& query) {
        Ctx.Step.Repeat(TExprStep::Intents);
        UNIT_ASSERT_VALUES_EQUAL_C(SyncTransform(*Intents, query, Ctx), IGraphTransformer::TStatus::Ok,
            Ctx.IssueManager.GetIssues().ToString());
    }
};

TString NormalizePath(TStringBuf path) {
    TStringBuf normalizedPath = path;
    while (normalizedPath.StartsWith("//")) {
        normalizedPath = normalizedPath.SubStr(1);
    }
    if (normalizedPath == "/alias") {
        return TString("/canonical");
    }
    return normalizedPath.StartsWith("/alias/") ? TString("/canonical") + TString(normalizedPath.SubStr(6)) : TString(path);
}

TString RewriteSql(TStringBuf sql, TStringBuf pathPrefix = {}, bool dynamicCluster = false, bool withAliases = true,
    bool expectUnchanged = false) {
    std::function<TString(TStringBuf)> normalizePath;
    if (withAliases) {
        normalizePath = NormalizePath;
    }
    TAliasRewriteFixture fixture(std::move(normalizePath));
    auto query = fixture.CompileSql(sql, pathPrefix, dynamicCluster);
    const auto original = query.Get();
    fixture.Rewrite(query);
    if (!withAliases || expectUnchanged) {
        UNIT_ASSERT_VALUES_EQUAL(query.Get(), original);
    }
    const auto rewritten = query.Get();
    fixture.Rewrite(query);
    UNIT_ASSERT_VALUES_EQUAL(query.Get(), rewritten);
    return KqpExprToPrettyString(*query, fixture.Ctx);
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
        const auto repeatedSlash = RewriteSql("GRANT ALL ON `//alias` TO user;", {}, false, false);
        UNIT_ASSERT_STRING_CONTAINS(repeatedSlash, "//alias");
    }

    Y_UNIT_TEST(ConfiguredAliasesLeaveUnmatchedPathUnchanged) {
        const auto unchanged = RewriteSql("SELECT * FROM `/other/table`;", {}, false, true, true);
        UNIT_ASSERT_STRING_CONTAINS(unchanged, "/other/table");
        const auto repeatedSlash = RewriteSql("SELECT * FROM `//other/table`;", {}, false, true, true);
        UNIT_ASSERT_STRING_CONTAINS(repeatedSlash, "//other/table");
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

        const auto relative = RewriteSql("CREATE ASYNC REPLICATION replication FOR `/alias/remote` AS `f/alias` WITH (ENDPOINT = 'localhost:2135', DATABASE = '/Root');");
        UNIT_ASSERT_STRING_CONTAINS(relative, "f/alias");
        UNIT_ASSERT_VALUES_EQUAL(relative.find("/canonical"), TString::npos);
    }

    Y_UNIT_TEST(TransferOnlyRewritesLocalTarget) {
        const auto rewritten = RewriteSql("CREATE TRANSFER `/alias/transfer` FROM `/alias/remote` TO `/alias/table` USING ($x) -> { RETURN $x; } WITH (ENDPOINT = 'localhost:2135', DATABASE = '/Root');");
        UNIT_ASSERT_STRING_CONTAINS(rewritten, "/canonical/transfer");
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

        const auto repeatedSlash = RewriteSql("GRANT ALL ON `//alias` TO user;");
        UNIT_ASSERT_STRING_CONTAINS(repeatedSlash, "/canonical");
        UNIT_ASSERT_VALUES_EQUAL(repeatedSlash.find("//alias"), TString::npos);

        const auto externalTable = RewriteSql("CREATE EXTERNAL TABLE `/alias/table` (key Uint64) WITH (DATA_SOURCE = '/alias/ds', LOCATION = '/');");
        UNIT_ASSERT_STRING_CONTAINS(externalTable, "/canonical/table");
        UNIT_ASSERT_STRING_CONTAINS(externalTable, "/canonical/ds");
    }

    Y_UNIT_TEST(BackupCollectionPrefixAndEntry) {
        const auto rewritten = RewriteSql("PRAGMA TablePathPrefix = '/alias'; CREATE BACKUP COLLECTION collection (TABLE table) WITH (STORAGE = 'cluster');");
        UNIT_ASSERT_STRING_CONTAINS(rewritten, "/canonical/table");
        UNIT_ASSERT_VALUES_EQUAL(rewritten.find("/alias"), TString::npos);
    }

    Y_UNIT_TEST(PhysicalInputIsUnchanged) {
        const auto unchanged = RewriteSql("SELECT * FROM `/canonical/table`;", {}, false, true, true);
        UNIT_ASSERT_STRING_CONTAINS(unchanged, "/canonical/table");
    }

    Y_UNIT_TEST(NewIoFromViewExpansionIsRewritten) {
        TAliasRewriteFixture fixture(NormalizePath);
        auto query = fixture.CompileSql("SELECT * FROM `/alias/view`;");
        fixture.Rewrite(query);
        auto body = fixture.CompileSql("SELECT * FROM `/alias/table`;");
        query = fixture.Ctx.NewList(query->Pos(), {query, body});
        fixture.Rewrite(query);
        const auto rewritten = KqpExprToPrettyString(*query, fixture.Ctx);
        UNIT_ASSERT_STRING_CONTAINS(rewritten, "/canonical/view");
        UNIT_ASSERT_STRING_CONTAINS(rewritten, "/canonical/table");
        UNIT_ASSERT_VALUES_EQUAL(rewritten.find("/alias/"), TString::npos);
    }

    Y_UNIT_TEST(CopiedGraphKeepsPhysicalPathsAfterRewind) {
        TAliasRewriteFixture fixture(NormalizePath);
        auto query = fixture.CompileSql("SELECT * FROM `/alias/table`;");
        fixture.Rewrite(query);
        TNodeOnNodeOwnedMap clones;
        auto copy = fixture.Ctx.DeepCopy(*query, fixture.Ctx, clones, true, false);
        UNIT_ASSERT(copy->UniqueId() != query->UniqueId());
        const auto original = copy.Get();
        fixture.Intents->Rewind();
        fixture.Rewrite(copy);
        UNIT_ASSERT_VALUES_EQUAL(copy.Get(), original);
        UNIT_ASSERT_STRING_CONTAINS(KqpExprToPrettyString(*copy, fixture.Ctx), "/canonical/table");
    }

    Y_UNIT_TEST(LiteralPathsFromAst) {
        for (const bool withAliases : {true, false}) {
            std::function<TString(TStringBuf)> normalizePath;
            if (withAliases) {
                normalizePath = NormalizePath;
            }
            TAliasRewriteFixture fixture(std::move(normalizePath));
            auto query = fixture.CompileAst(R"(
                (
                    (let read (Read! world (DataSource 'kikimr 'plato)
                        (Key '('table (String '"/alias/input"))) (Void) '()))
                    (return (Write! (Left! read) (DataSink 'kikimr 'plato)
                        (Key '('table (String '"/alias/output"))) (Right! read) '('('mode 'upsert))))
                )
            )");
            const auto original = query.Get();
            fixture.Rewrite(query);
            const auto rewritten = KqpExprToPrettyString(*query, fixture.Ctx);
            if (withAliases) {
                UNIT_ASSERT_STRING_CONTAINS(rewritten, "/canonical/input");
                UNIT_ASSERT_STRING_CONTAINS(rewritten, "/canonical/output");
                UNIT_ASSERT_VALUES_EQUAL(rewritten.find("/alias/"), TString::npos);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(query.Get(), original);
                UNIT_ASSERT_STRING_CONTAINS(rewritten, "/alias/input");
                UNIT_ASSERT_STRING_CONTAINS(rewritten, "/alias/output");
            }
            const auto rewrittenRoot = query.Get();
            fixture.Rewrite(query);
            UNIT_ASSERT_VALUES_EQUAL(query.Get(), rewrittenRoot);
        }
    }

    Y_UNIT_TEST(LoweredIoFromAstIsUnchanged) {
        for (const TStringBuf text : {
            R"((
                (return (KiReadTable! world (DataSource 'kikimr 'plato)
                    (Key '('table (String '"/alias/table"))) (Void) '()))
            ))",
            R"((
                (return (KiWriteTable! world (DataSink 'kikimr 'plato)
                    '"/alias/table" (AsList (AsStruct '('key (Uint64 '1)))) 'upsert '() '()))
            ))",
        }) {
            ui32 calls = 0;
            TAliasRewriteFixture fixture([&calls](TStringBuf path) {
                ++calls;
                return NormalizePath(path);
            });
            auto query = fixture.CompileAst(text);
            UNIT_ASSERT(query->IsCallable("KiReadTable!") || query->IsCallable("KiWriteTable!"));
            const auto original = query.Get();
            auto transformer = CreateSqlPathAliasesTransformer(fixture.Session, new TNullTransformer);
            UNIT_ASSERT_VALUES_EQUAL_C(SyncTransform(*transformer, query, fixture.Ctx), IGraphTransformer::TStatus::Ok,
                fixture.Ctx.IssueManager.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(query.Get(), original);
            UNIT_ASSERT_VALUES_EQUAL(calls, 0);
            UNIT_ASSERT_STRING_CONTAINS(KqpExprToPrettyString(*query, fixture.Ctx), "/alias/table");
        }
    }

    Y_UNIT_TEST(EmptyConfigKeepsOriginalIntentTransformer) {
        TAliasRewriteFixture fixture({});
        TAutoPtr<IGraphTransformer> original = new TNullTransformer;
        const auto* ptr = original.Get();
        auto transformer = CreateSqlPathAliasesTransformer(fixture.Session, std::move(original));
        UNIT_ASSERT_VALUES_EQUAL(transformer.Get(), ptr);
    }

    Y_UNIT_TEST(SinkRegistersPhysicalPathBeforeMetadata) {
        TAliasRewriteFixture fixture([](TStringBuf path) { return NormalizePath(path); }, true);
        auto query = fixture.CompileSql("PRAGMA TablePathPrefix = '/alias'; REPLACE INTO table (key) VALUES (1);");
        fixture.Rewrite(query);
        const auto& tables = fixture.Session->Tables().GetTables();
        UNIT_ASSERT(tables.contains(std::make_pair(TString("plato"), TString("/canonical/table"))));
        UNIT_ASSERT(!tables.contains(std::make_pair(TString("plato"), TString("/alias/table"))));
    }

    Y_UNIT_TEST(OtherClusterIsNotRewritten) {
        const auto rewritten = RewriteSql("SELECT * FROM `/remote/source`.`/alias/table`;", {}, true);
        UNIT_ASSERT_STRING_CONTAINS(rewritten, "/alias/table");
        UNIT_ASSERT_VALUES_EQUAL(rewritten.find("/canonical/table"), TString::npos);
    }
}

}

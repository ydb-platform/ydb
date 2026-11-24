#include <ydb/core/kqp/sql/translator.h>

#include <yql/essentials/sql/sql.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {
namespace {

NSQLTranslation::TTranslationSettings MakeSettings() {
    NSQLTranslation::TTranslationSettings settings;
    settings.SyntaxVersion = 1;
    settings.DefaultCluster = "db";
    settings.ClusterMapping["db"] = "kikimr";
    return settings;
}

} // namespace

Y_UNIT_TEST_SUITE(KqpSqlTranslator) {
    Y_UNIT_TEST(ScalarRemoteEmbedderAliases) {
        NSQLTranslation::TTranslators translators(nullptr, MakeKqpSqlV1Translator(false), nullptr);
        const auto settings = MakeSettings();
        for (const auto* query : {
            "SELECT ApplyRemoteEmbedder('model', 'url', 'secret', 'текст');",
            "SELECT applyremoteembedder /* comment */ ('model', 'url', 'secret', 'text');",
            "SELECT Apply_Remote_Embedder('model', 'url', 'secret', 'text');",
            "SELECT `ApplyRemoteEmbedder`('model', 'url', 'secret', 'text');",
            "SELECT `\\x41pplyRemoteEmbedder`('model', 'url', 'secret', 'text');",
            "--!ansi_lexer\nSELECT \"ApplyRemoteEmbedder\"('model', 'url', 'secret', 'text');",
            "SELECT 1 IN ApplyRemoteEmbedder('model', 'url', 'secret', 'text');",
        }) {
            const auto result = NSQLTranslation::SqlToYql(translators, query, settings);
            UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.Root->ToString(), "Knn.ApplyRemoteEmbedder");
        }
    }

    Y_UNIT_TEST(StreamRemoteEmbedderAlias) {
        NSQLTranslation::TTranslators translators(nullptr, MakeKqpSqlV1Translator(false), nullptr);
        const auto settings = MakeSettings();
        const TString query = R"(
            $requests = AsList(AsStruct('текст' AS Text, 1 AS PassThrough));
            SELECT Embedding, Status, PassThrough FROM (
                PROCESS AS_TABLE($requests)
                USING applyremoteembedderstream('model', 'url', 'secret', TableRows())
            );
            SELECT ApplyRemoteEmbedder('model', 'url', 'secret', 'text');
        )";

        const auto result = NSQLTranslation::SqlToYql(translators, query, settings);
        UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.Root->ToString(), "Knn.ApplyRemoteEmbedderStream");

        TVector<NYql::TStmtParseInfo> statements;
        const auto parts = NSQLTranslation::SqlToAstStatements(translators, query, settings, nullptr, nullptr, &statements);
        UNIT_ASSERT_VALUES_EQUAL(parts.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(statements.size(), parts.size());
        UNIT_ASSERT_C(parts[0].IsOk(), parts[0].Issues.ToString());
        UNIT_ASSERT_C(parts[1].IsOk(), parts[1].Issues.ToString());
        UNIT_ASSERT_STRING_CONTAINS(parts[0].Root->ToString(), "Knn.ApplyRemoteEmbedderStream");
        UNIT_ASSERT_STRING_CONTAINS(parts[1].Root->ToString(), "Knn.ApplyRemoteEmbedder");

        const auto ansi = NSQLTranslation::SqlToYql(translators, R"(--!ansi_lexer
            PROCESS AS_TABLE(AsList(AsStruct('text' AS Text, 1 AS PassThrough)))
            USING "ApplyRemoteEmbedderStream"('model', 'url', 'secret', TableRows());
        )", settings);
        UNIT_ASSERT_C(ansi.IsOk(), ansi.Issues.ToString());
        UNIT_ASSERT_STRING_CONTAINS(ansi.Root->ToString(), "Knn.ApplyRemoteEmbedderStream");
    }

    Y_UNIT_TEST(RemoteEmbedderNonAliasOccurrences) {
        NSQLTranslation::TTranslators translators(nullptr, MakeKqpSqlV1Translator(false), nullptr);
        const auto settings = MakeSettings();
        const auto result = NSQLTranslation::SqlToYql(translators, R"(
            CREATE TABLE ApplyRemoteEmbedder (id Int64, PRIMARY KEY (id));
            $ApplyRemoteEmbedder = ($x) -> { RETURN $x; };
            SELECT $ApplyRemoteEmbedder('ApplyRemoteEmbedder()'), Other::ApplyRemoteEmbedder(1);
        )", settings);
        UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
        const auto ast = result.Root->ToString();
        UNIT_ASSERT_STRING_CONTAINS(ast, "ApplyRemoteEmbedder()");
        UNIT_ASSERT_STRING_CONTAINS(ast, "Other.ApplyRemoteEmbedder");
        UNIT_ASSERT(!TStringBuf(ast).Contains("Knn.ApplyRemoteEmbedder"));
    }
}

} // namespace NKikimr::NKqp

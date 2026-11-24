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
            "SELECT ApplyRemoteEmbedder('текст', 'model', 'url', 'secret');",
            "SELECT applyremoteembedder /* comment */ ('text', 'model', 'url', 'secret');",
            "SELECT Apply_Remote_Embedder('text', 'model', 'url', 'secret');",
            "SELECT `ApplyRemoteEmbedder`('text', 'model', 'url', 'secret');",
            "SELECT `\\x41pplyRemoteEmbedder`('text', 'model', 'url', 'secret');",
            "--!ansi_lexer\nSELECT \"ApplyRemoteEmbedder\"('text', 'model', 'url', 'secret');",
            "SELECT 1 IN ApplyRemoteEmbedder('text', 'model', 'url', 'secret');",
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
                USING applyremoteembedder(TableRows(), 'model', 'url', 'secret', 32, 10240, 5)
            );
            SELECT ApplyRemoteEmbedder('text', 'model', 'url', 'secret');
        )";

        const auto result = NSQLTranslation::SqlToYql(translators, query, settings);
        UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.Root->ToString(), "Knn.ApplyRemoteEmbedder");

        TVector<NYql::TStmtParseInfo> statements;
        const auto parts = NSQLTranslation::SqlToAstStatements(translators, query, settings, nullptr, nullptr, &statements);
        UNIT_ASSERT_VALUES_EQUAL(parts.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(statements.size(), parts.size());
        UNIT_ASSERT_C(parts[0].IsOk(), parts[0].Issues.ToString());
        UNIT_ASSERT_C(parts[1].IsOk(), parts[1].Issues.ToString());
        UNIT_ASSERT_STRING_CONTAINS(parts[0].Root->ToString(), "Knn.ApplyRemoteEmbedder");
        UNIT_ASSERT_STRING_CONTAINS(parts[1].Root->ToString(), "Knn.ApplyRemoteEmbedder");

        const auto ansi = NSQLTranslation::SqlToYql(translators, R"(--!ansi_lexer
            PROCESS AS_TABLE(AsList(AsStruct('text' AS Text, 1 AS PassThrough)))
            USING "ApplyRemoteEmbedder"(TableRows(), 'model', 'url', 'secret');
        )", settings);
        UNIT_ASSERT_C(ansi.IsOk(), ansi.Issues.ToString());
        UNIT_ASSERT_STRING_CONTAINS(ansi.Root->ToString(), "Knn.ApplyRemoteEmbedder");
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

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/sql/sql.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NSQLTranslation;

enum class EDebugOutput {
    None,
    ToCerr,
};

TString Err2Str(NYql::TAstParseResult& res, EDebugOutput debug = EDebugOutput::None) {
    TStringStream s;
    res.Issues.PrintTo(s);

    if (debug == EDebugOutput::ToCerr) {
        Cerr << s.Str() << Endl;
    }
    return s.Str();
}

NYql::TAstParseResult SqlToYqlWithMode(const TString& query, NSQLTranslation::ESqlMode mode = NSQLTranslation::ESqlMode::QUERY, size_t maxErrors = 10, const TString& provider = {},
    EDebugOutput debug = EDebugOutput::None, bool ansiLexer = false, NSQLTranslation::TTranslationSettings settings = {})
{
    google::protobuf::Arena arena;
    const auto service = provider ? provider : TString(NYql::YtProviderName);
    const TString cluster = "plato";
    settings.ClusterMapping[cluster] = service;
    settings.ClusterMapping["hahn"] = NYql::YtProviderName;
    settings.ClusterMapping["mon"] = NYql::SolomonProviderName;
    settings.MaxErrors = maxErrors;
    settings.Mode = mode;
    settings.Arena = &arena;
    settings.AnsiLexer = ansiLexer;
    settings.SyntaxVersion = 1;
    auto q = TStringBuilder() << "--!syntax_pg\n" << query;
    auto res = SqlToYql(q, settings);
    if (debug == EDebugOutput::ToCerr) {
        Err2Str(res, debug);
    }
    return res;
}

NYql::TAstParseResult PgSqlToYql(const TString& query, size_t maxErrors = 10, const TString& provider = {}, EDebugOutput debug = EDebugOutput::None) {
    return SqlToYqlWithMode(query, NSQLTranslation::ESqlMode::QUERY, maxErrors, provider, debug);
}

Y_UNIT_TEST_SUITE(PgSqlParsingOnly) {
    Y_UNIT_TEST(InsertStmt) {
        auto res = PgSqlToYql("INSERT INTO plato.Input VALUES (1, 1)");
        UNIT_ASSERT(res.Root);
        res.Root->PrintTo(Cerr);
    }

    Y_UNIT_TEST(DeleteStmt) {
        auto res = PgSqlToYql("DELETE FROM plato.Input");
        UNIT_ASSERT(res.Root);
        TString program = R"(
        (
            (let world (Configure! world (DataSource 'config) 'OrderedColumns))
            (let read0 (Read! world (DataSource '"yt" '"plato") (Key '('table (String '"input"))) (Void) '()))
            (let world (Left! read0))
            (let world (Write! world (DataSink '"yt" '"plato") (Key '('table (String '"input"))) (Void) '('('pg_delete (PgSelect '('('set_items '((PgSetItem '('('result '((PgResultItem '"" (Void) (lambda '() (PgStar))))) '('from '('((Right! read0) '"input" '()))) '('join_ops '('())))))) '('set_ops '('push))))) '('mode 'delete))))
            (let world (CommitAll! world))
            (return world)
        )
        )";
        const auto expectedAst = NYql::ParseAst(program);
        UNIT_ASSERT_STRINGS_EQUAL(res.Root->ToString(), expectedAst.Root->ToString());
    }
}

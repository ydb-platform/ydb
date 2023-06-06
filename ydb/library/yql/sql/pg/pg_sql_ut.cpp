#include <contrib/libs/fmt/include/fmt/format.h>

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/sql/sql.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/config.h>

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
    settings.ClusterMapping[""] = NYql::KikimrProviderName;
    settings.MaxErrors = maxErrors;
    settings.Mode = mode;
    settings.Arena = &arena;
    settings.AnsiLexer = ansiLexer;
    settings.SyntaxVersion = 1;
    settings.PgParser = true;
    auto res = SqlToYql(query, settings);
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

    Y_UNIT_TEST(CreateTableStmt_Basic) {
        auto res = PgSqlToYql("CREATE TABLE t (a int, b text)");
        UNIT_ASSERT(res.Root);

        TString program = R"(
        (
            (let world (Configure! world (DataSource 'config) 'OrderedColumns))
            (let world (Write! world (DataSink '"kikimr" '"") (Key '('tablescheme (String '"t"))) (Void) '('('mode 'create) '('columns '('('a (PgType 'int4)) '('b (PgType 'text)))))))
            (let world (CommitAll! world))
            (return world)
        )
        )";
        const auto expectedAst = NYql::ParseAst(program);
        UNIT_ASSERT_STRINGS_EQUAL(res.Root->ToString(), expectedAst.Root->ToString());
    }

    Y_UNIT_TEST(CreateTableStmt_NotNull) {
        auto res = PgSqlToYql("CREATE TABLE t (a int NOT NULL, b text)");
        UNIT_ASSERT(res.Root);

        TString program = R"(
        (
            (let world (Configure! world (DataSource 'config) 'OrderedColumns))
            (let world (Write! world (DataSink '"kikimr" '"") (Key '('tablescheme (String '"t"))) (Void) '('('mode 'create) '('columns '('('a (PgType 'int4)) '('b (PgType 'text)))) '('notnull '('a)))))
            (let world (CommitAll! world))
            (return world)
        )
        )";
        const auto expectedAst = NYql::ParseAst(program);
        UNIT_ASSERT_STRINGS_EQUAL(res.Root->ToString(), expectedAst.Root->ToString());
    }

    Y_UNIT_TEST(CreateTableStmt_JustPK) {
        auto res = PgSqlToYql("CREATE TABLE t (a int PRIMARY KEY, b text)");
        UNIT_ASSERT(res.Root);

        TString program = R"(
        (
            (let world (Configure! world (DataSource 'config) 'OrderedColumns))
            (let world (Write! world (DataSink '"kikimr" '"") (Key '('tablescheme (String '"t"))) (Void) '('('mode 'create) '('columns '('('a (PgType 'int4)) '('b (PgType 'text)))) '('primarykey '('a)) '('notnull '('a)))))
            (let world (CommitAll! world))
            (return world)
        )
        )";
        const auto expectedAst = NYql::ParseAst(program);
        UNIT_ASSERT_STRINGS_EQUAL(res.Root->ToString(), expectedAst.Root->ToString());
    }

    Y_UNIT_TEST(CreateTableStmt_PKAndNotNull) {
        auto res = PgSqlToYql("CREATE TABLE t (a int PRIMARY KEY NOT NULL, b text)");
        UNIT_ASSERT(res.Root);

        TString program = R"(
        (
            (let world (Configure! world (DataSource 'config) 'OrderedColumns))
            (let world (Write! world (DataSink '"kikimr" '"") (Key '('tablescheme (String '"t"))) (Void) '('('mode 'create) '('columns '('('a (PgType 'int4)) '('b (PgType 'text)))) '('primarykey '('a)) '('notnull '('a)))))
            (let world (CommitAll! world))
            (return world)
        )
        )";
        const auto expectedAst = NYql::ParseAst(program);
        UNIT_ASSERT_STRINGS_EQUAL(res.Root->ToString(), expectedAst.Root->ToString());
    }

    Y_UNIT_TEST(CreateTableStmt_PKAndOtherNotNull) {
        auto res = PgSqlToYql("CREATE TABLE t (a int PRIMARY KEY, b text NOT NULL)");
        UNIT_ASSERT(res.Root);

        TString program = R"(
        (
            (let world (Configure! world (DataSource 'config) 'OrderedColumns))
            (let world (Write! world (DataSink '"kikimr" '"") (Key '('tablescheme (String '"t"))) (Void) '('('mode 'create) '('columns '('('a (PgType 'int4)) '('b (PgType 'text)))) '('primarykey '('a)) '('notnull '('a 'b)))))
            (let world (CommitAll! world))
            (return world)
        )
        )";
        const auto expectedAst = NYql::ParseAst(program);
        UNIT_ASSERT_STRINGS_EQUAL(res.Root->ToString(), expectedAst.Root->ToString());
    }

    Y_UNIT_TEST(CreateTableStmt_TableLevelPK) {
        auto res = PgSqlToYql("CREATE TABLE t (a int, b text NOT NULL, PRIMARY KEY (a, b))");
        UNIT_ASSERT(res.Root);

        TString program = R"(
        (
            (let world (Configure! world (DataSource 'config) 'OrderedColumns))
            (let world (Write! world (DataSink '"kikimr" '"") (Key '('tablescheme (String '"t"))) (Void) '('('mode 'create) '('columns '('('a (PgType 'int4)) '('b (PgType 'text)))) '('primarykey '('a 'b)) '('notnull '('b 'a)))))
            (let world (CommitAll! world))
            (return world)
        )
        )";
        const auto expectedAst = NYql::ParseAst(program);
        UNIT_ASSERT_STRINGS_EQUAL(res.Root->ToString(), expectedAst.Root->ToString());
    }

    Y_UNIT_TEST(CreateTableStmt_RepeatingColumnNames) {
        auto res = PgSqlToYql("CREATE TABLE t (a int, a text)");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_EQUAL(res.Issues.Size(), 1);

        auto issue = *(res.Issues.begin());
        UNIT_ASSERT(issue.GetMessage().find("duplicate") != TString::npos);
    }

    Y_UNIT_TEST(CreateTableStmt_PKHasColumnsNotBelongingToTable_Fails) {
        auto res = PgSqlToYql("CREATE TABLE t (a int, primary key(b))");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_EQUAL(res.Issues.Size(), 1);

        auto issue = *(res.Issues.begin());
        UNIT_ASSERT(issue.GetMessage().find("PK column does not belong to table") != TString::npos);
    }
    
    Y_UNIT_TEST(CreateTableStmt_AliasSerialToIntType) {
        auto res = PgSqlToYql("CREATE TABLE t (a SerIAL)");
        UNIT_ASSERT(res.Root);
        
        TString program = R"(
            (
                (let world (Configure! world (DataSource 'config) 'OrderedColumns))
                (let world (Write! world (DataSink '"kikimr" '"") (Key '('tablescheme (String '"t"))) (Void) '('('mode 'create) '('columns '('('a (PgType 'int4))))))) 
                (let world (CommitAll! world)) 
                (return world))
        )";
        const auto expectedAst = NYql::ParseAst(program);
        UNIT_ASSERT_STRINGS_EQUAL(res.Root->ToString(), expectedAst.Root->ToString());
    }

    Y_UNIT_TEST(VariableShowStmt) {
        auto res = PgSqlToYql("Show server_version_num");
        UNIT_ASSERT(res.Root);

        TString program = fmt::format(R"(
            (
                (let world (Configure! world (DataSource 'config) 'OrderedColumns))
                (let output (PgSelect '('('set_items '((PgSetItem '('('result '((PgResultItem '"server_version_num" (Void) (lambda '() (PgConst '"{}" (PgType 'text)))))))))) '('set_ops '('push))))) 
                (let result_sink (DataSink 'result))
                (let world (Write! world result_sink (Key) output '('('type) '('autoref)))) 
                (let world (Commit! world result_sink)) 
                (let world (CommitAll! world)) 
                (return world)
            )
        )", NYql::GetPostgresServerVersionNum());
        const auto expectedAst = NYql::ParseAst(program);
        UNIT_ASSERT_STRINGS_EQUAL(res.Root->ToString(), expectedAst.Root->ToString());
    }
}

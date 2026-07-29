#include "sql_ut.h"

#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/translation/sql.h>

using namespace NSQLTranslationV1;

namespace {

NSQLTranslation::TParsedTokenList Tokenize(const TString& query) {
    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();

    auto lexer = NSQLTranslationV1::MakeLexer(lexers, /*ansi=*/false);
    NSQLTranslation::TParsedTokenList tokens;
    NYql::TIssues issues;
    UNIT_ASSERT_C(Tokenize(*lexer, query, "Query", tokens, issues,
                           NSQLTranslation::SQL_MAX_PARSER_ERRORS),
                  issues.ToString());

    return tokens;
}

} // namespace

Y_UNIT_TEST_SUITE(TViewSyntaxTest) {
Y_UNIT_TEST(CreateViewSimple) {
    NYql::TAstParseResult res = SqlToYql(R"(
                    USE ydb;
                    CREATE VIEW TheView WITH (security_invoker = TRUE) AS SELECT 1;
                )");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(CreateViewWithUdfs) {
    NYql::TAstParseResult res = SqlToYql(R"(
                    USE ydb;
                    CREATE VIEW TheView WITH (security_invoker = TRUE) AS SELECT "bbb" LIKE Unwrap("aaa");
                )");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(CreateViewIfNotExists) {
    constexpr const char* name = "TheView";
    NYql::TAstParseResult res = SqlToYql(std::format(R"(
                    USE ydb;
                    CREATE VIEW IF NOT EXISTS {} AS SELECT 1;
                )", name));
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
        if (word == "Write!") {
            UNIT_ASSERT_STRING_CONTAINS(line, name);
            UNIT_ASSERT_STRING_CONTAINS(line, "createObjectIfNotExists");
        }
    };

    TWordCountHive elementStat = {{"Write!"}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(elementStat["Write!"], 1);
}

Y_UNIT_TEST(CreateViewFromTable) {
    constexpr const char* path = "/PathPrefix/TheView";
    constexpr const char* query = R"(
                SELECT * FROM SomeTable
            )";

    NYql::TAstParseResult res = SqlToYql(std::format(R"(
                        USE ydb;
                        CREATE VIEW `{}` WITH (security_invoker = TRUE) AS {};
                    )",
                                                     path,
                                                     query));
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
        if (word == "Write!") {
            UNIT_ASSERT_STRING_CONTAINS(line, path);
            UNIT_ASSERT_STRING_CONTAINS(line, "createObject");
        }
    };
    TWordCountHive elementStat = {{"Write!"}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(elementStat["Write!"], 1);
}

Y_UNIT_TEST(CheckReconstructedQuery) {
    constexpr const char* path = "/PathPrefix/TheView";
    constexpr const char* query = R"(
                SELECT * FROM FirstTable JOIN SecondTable ON FirstTable.key == SecondTable.key
            )";

    NYql::TAstParseResult res = SqlToYql(std::format(R"(
                        USE ydb;
                        CREATE VIEW `{}` WITH (security_invoker = TRUE) AS {};
                    )",
                                                     path,
                                                     query));
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TString reconstructedQuery = ToString(Tokenize(query));
    TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
        if (word == "query_text") {
            UNIT_ASSERT_STRING_CONTAINS(line, reconstructedQuery);
        }
    };
    TWordCountHive elementStat = {{"Write!"}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(elementStat["Write!"], 1);
}

Y_UNIT_TEST(DropView) {
    constexpr const char* path = "/PathPrefix/TheView";
    NYql::TAstParseResult res = SqlToYql(std::format(R"(
                        USE ydb;
                        DROP VIEW `{}`;
                    )",
                                                     path));
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
        if (word == "Write!") {
            UNIT_ASSERT_STRING_CONTAINS(line, path);
            UNIT_ASSERT_STRING_CONTAINS(line, "dropObject");
        }
    };
    TWordCountHive elementStat = {{"Write!"}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(elementStat["Write!"], 1);
}

Y_UNIT_TEST(DropViewIfExists) {
    constexpr const char* name = "TheView";
    NYql::TAstParseResult res = SqlToYql(std::format(R"(
                    USE ydb;
                    DROP VIEW IF EXISTS {};
                )", name));
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
        if (word == "Write!") {
            UNIT_ASSERT_STRING_CONTAINS(line, name);
            UNIT_ASSERT_STRING_CONTAINS(line, "dropObjectIfExists");
        }
    };

    TWordCountHive elementStat = {{"Write!"}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(elementStat["Write!"], 1);
}

Y_UNIT_TEST(DropViewIfExistsYt) {
    constexpr const char* name = "TheView";
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YtDropView.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(std::format(R"(
                    USE plato;
                    DROP VIEW IF EXISTS {};
                )", name), settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
        if (word == "Write!") {
            UNIT_ASSERT_STRING_CONTAINS(line, name);
            UNIT_ASSERT_STRING_CONTAINS(line, "dropObjectIfExists");
        }
    };

    TWordCountHive elementStat = {{"Write!"}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(elementStat["Write!"], 1);
}

Y_UNIT_TEST(CreateViewWithTablePrefix) {
    NYql::TAstParseResult res = SqlToYql(R"(
                    USE ydb;
                    PRAGMA TablePathPrefix='/PathPrefix';
                    CREATE VIEW TheView WITH (security_invoker = TRUE) AS SELECT 1;
                )");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write!") {
            UNIT_ASSERT_STRING_CONTAINS(line, "/PathPrefix/TheView");
            UNIT_ASSERT_STRING_CONTAINS(line, "createObject");
        }
    };

    TWordCountHive elementStat = {{"Write!"}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(elementStat["Write!"], 1);
}

Y_UNIT_TEST(DropViewWithTablePrefix) {
    NYql::TAstParseResult res = SqlToYql(R"(
                    USE ydb;
                    PRAGMA TablePathPrefix='/PathPrefix';
                    DROP VIEW TheView;
                )");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "/PathPrefix/TheView");
            UNIT_ASSERT_STRING_CONTAINS(line, "dropObject");
        }
    };

    TWordCountHive elementStat = {{"Write!"}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(elementStat["Write!"], 1);
}

Y_UNIT_TEST(CreateDropViewDoesntWorkWithBinding) {
    NSQLTranslation::TTranslationSettings settings;
    ExpectFailWithError("use ydb; $path = 'foo'; create view $path as select 1",
                        "<main>:1:37: Error: Bind parameter is not supported\n", settings);
    ExpectFailWithError("use ydb; $path = 'foo'; drop view $path",
                        "<main>:1:35: Error: Bind parameter is not supported\n", settings);
}

Y_UNIT_TEST(YtAlternativeSchemaSyntax) {
    NYql::TAstParseResult res = SqlToYql(R"(
                SELECT * FROM plato.Input WITH schema(y Int32, x String not null);
            )");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "userschema") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos,
                                       line.find(R"__('('('"userschema" (StructType '('"y" (AsOptionalType (DataType 'Int32))) '('"x" (DataType 'String))))))__"));
        }
    };

    TWordCountHive elementStat = {{TString("userschema"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["userschema"]);
}

Y_UNIT_TEST(UseViewAndFullColumnId) {
    NYql::TAstParseResult res = SqlToYql("USE plato; SELECT Input.x FROM Input VIEW uitzicht;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{TString("SqlAccess"), 0}, {"SqlProjectItem", 0}, {"Read!", 0}};
    VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL(0, elementStat["SqlAccess"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SqlProjectItem"]);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Read!"]);
}
} // Y_UNIT_TEST_SUITE(TViewSyntaxTest)

Y_UNIT_TEST_SUITE(CreateViewNewSyntax) {

Y_UNIT_TEST(DoesntWorkOnOldLangVersion) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::MakeLangVersion(2025, 4);
    ExpectFailWithError("create view plato.foo as select 1;",
                        "<main>:1:1: Error: CREATE VIEW is not available before language version 2025.05\n",
                        settings);
    ExpectFailWithError("create view plato.foo as do begin select 1; end do;",
                        "<main>:1:1: Error: CREATE VIEW is not available before language version 2025.05\n",
                        settings);
}

Y_UNIT_TEST(Basic) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YtCreateView.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
            CREATE VIEW plato.foo AS
            DO BEGIN $foo = 1; select /* some hint */  $foo + 123; END DO;
        )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {
        {TString("__query_text"), 0},
        {TString("__query_ast"), 0},
    };
    VerifyProgram(res, stat, [](const TString& word, const TString& line) {
        if (word == "__query_text") {
            UNIT_ASSERT_STRING_CONTAINS(line, " $foo = 1; select /* some hint */  $foo + 123; ");
        }
    });
    UNIT_ASSERT_VALUES_EQUAL(stat["__query_text"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["__query_ast"], 1);
}

Y_UNIT_TEST(NamedNodesAreNotVisibleInView) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YtCreateView.MinLangVer;

    auto query = R"sql(
        $foo = 1;
        CREATE VIEW plato.foo AS
        DO BEGIN
            $bar = 2;
            select $bar + $foo;
        END DO;
    )sql";

    ExpectFailWithError(query, "<main>:6:27: Error: Unknown name: $foo\n", settings);
}

Y_UNIT_TEST(ScopedPragmasDoNotAffectView) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YtCreateView.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
            pragma CheckedOps = 'true';
            CREATE VIEW plato.foo AS
            DO BEGIN
                select 1 + 1;
            END DO;
        )sql", settings);

    TWordCountHive stat = {
        {TString("+MayWarn"), 0},
        {TString("CheckedAdd"), 0},
    };
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["+MayWarn"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["CheckedAdd"], 0);
}

Y_UNIT_TEST(NewSyntaxDoesntWorkOnYdb) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YtCreateView.MinLangVer;
    ExpectFailWithError("create view ydb.foo as do begin select 1; end do",
                        "<main>:1:1: Error: CREATE VIEW ... AS DO BEGIN ... END DO syntax is not supported for ydb provider. Please use CREATE VIEW ... AS SELECT\n",
                        settings);
}

Y_UNIT_TEST(OldSyntaxDoesntWorkOnYt) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YtCreateView.MinLangVer;
    ExpectFailWithError("create view plato.foo as select 1;",
                        "<main>:1:1: Error: CREATE VIEW ... AS SELECT syntax is not supported for yt provider. Please use CREATE VIEW ... AS DO BEGIN ... END DO\n",
                        settings);
}

Y_UNIT_TEST(EmptyViewBody) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YtCreateView.MinLangVer;
    ExpectFailWithError("create view plato.foo as do begin end do", "<main>:1:29: Error: Empty view body is not allowed\n", settings);
    ExpectFailWithError("create view plato.foo as do begin /*comment*/;; end do", "<main>:1:29: Error: Empty view body is not allowed\n", settings);
}

Y_UNIT_TEST(MultiSelectsInViewOrStatementAfterSelect) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YtCreateView.MinLangVer;
    ExpectFailWithError("create view plato.foo as do begin select 1; select 2; end do",
                        "<main>:1:52: Error: Strictly one select/process/reduce statement is expected at the end of subquery\n", settings);
    ExpectFailWithError("create view plato.foo as do begin select 1;   $foo = 2; end do",
                        "<main>:1:54: Error: Strictly one select/process/reduce statement is expected at the end of subquery\n", settings);
}

Y_UNIT_TEST(ErrorOnMissingCluster) {
    NSQLTranslation::TTranslationSettings settings;
    ExpectFailWithError("create view foo as do begin select 1; end do", "<main>:1:13: Error: No cluster name given and no default cluster is selected\n", settings);
    ExpectFailWithError("create view foo as select 1", "<main>:1:13: Error: No cluster name given and no default cluster is selected\n", settings);
    ExpectFailWithError("drop view foo", "<main>:1:11: Error: No cluster name given and no default cluster is selected\n", settings);
}

Y_UNIT_TEST(ErrorOnTempView) {
    NSQLTranslation::TTranslationSettings settings;
    ExpectFailWithError("create view plato.@foo as do begin select 1; end do", "<main>:1:20: Error: Temporary object is not supported\n", settings);
    ExpectFailWithError("$path = 'foo'; create view plato.@$path as do begin select 1; end do", "<main>:1:35: Error: Temporary object is not supported\n", settings);
    ExpectFailWithError("drop view plato.@foo", "<main>:1:18: Error: Temporary object is not supported\n", settings);
}

Y_UNIT_TEST(CreateViewIfNotExists) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YtCreateView.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings("create view if not exists plato.foo as do begin select 1; end do", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
        if (word == "Write!") {
            UNIT_ASSERT_STRING_CONTAINS(line, "createObjectIfNotExists");
        }
    };

    TWordCountHive elementStat = {{"Write!"}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(elementStat["Write!"], 1);
}

Y_UNIT_TEST(CreateDropViewWithBind) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YtCreateView.MinLangVer;

    auto verify = [&](const TString& q) {
        TString header = R"sql(
                USE plato;
                PRAGMA TablePathPrefix = '//prefix';
                $path = 'tab' || 'le';
            )sql";
        NYql::TAstParseResult res = SqlToYqlWithSettings(header + q, settings);
        UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Concat") {
                UNIT_ASSERT_STRING_CONTAINS(line, R"((let namedexprnode0 (Concat (String '"tab") (String '"le"))))");
            }
            if (word == "Write!") {
                UNIT_ASSERT_STRING_CONTAINS(line, R"('('objectId (String (EvaluateAtom (BuildTablePath (String '"//prefix") (String (EvaluateAtom namedexprnode0)))))))");
            }
        };
        TWordCountHive elementStat = {{"Write!"}, {"Concat"}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(elementStat["Write!"], 1);
        UNIT_ASSERT_VALUES_EQUAL(elementStat["Concat"], 1);
    };

    verify("create view $path as do begin select 1; end do");
    verify("drop view $path");
}

} // Y_UNIT_TEST_SUITE(CreateViewNewSyntax)

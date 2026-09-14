#include "sql_ut.h"

#include <yql/essentials/sql/v1/translation/sql.h>

using namespace NSQLTranslationV1;

Y_UNIT_TEST_SUITE(ExternalFunction) {
Y_UNIT_TEST(ValidUseFunctions) {
    UNIT_ASSERT(SqlToYql(
                    "PROCESS plato.Input"
                    " USING EXTERNAL FUNCTION('YANDEX-CLOUD', 'foo', <|a: 123, b: a + 641|>)"
                    " WITH INPUT_TYPE=Struct<a:Int32>, OUTPUT_TYPE=Struct<b:Int32>,"
                    " CONCURRENCY=3, OPTIMIZE_FOR='CALLS'")
                    .IsOk());

    // use CALLS without quotes, as keyword
    UNIT_ASSERT(SqlToYql(
                    "PROCESS plato.Input"
                    " USING EXTERNAL FUNCTION('YANDEX-CLOUD', 'foo')"
                    " WITH INPUT_TYPE=Struct<a:Int32>, OUTPUT_TYPE=Struct<b:Int32>,"
                    " OPTIMIZE_FOR=CALLS")
                    .IsOk());

    UNIT_ASSERT(SqlToYql(
                    "PROCESS plato.Input"
                    " USING EXTERNAL FUNCTION('YANDEX-CLOUD', 'foo', TableRow())"
                    " WITH INPUT_TYPE=Struct<a:Int32>, OUTPUT_TYPE=Struct<b:Int32>,"
                    " CONCURRENCY=3")
                    .IsOk());

    UNIT_ASSERT(SqlToYql(
                    "PROCESS plato.Input"
                    " USING EXTERNAL FUNCTION('YANDEX-CLOUD', 'foo')"
                    " WITH INPUT_TYPE=Struct<a:Int32>, OUTPUT_TYPE=Struct<b:Int32>,"
                    " CONCURRENCY=3, BATCH_SIZE=1000000, CONNECTION='yc-folder34fse-con',"
                    " INIT=[0, 900]")
                    .IsOk());

    UNIT_ASSERT(SqlToYql(
                    "PROCESS plato.Input"
                    " USING EXTERNAL FUNCTION('YANDEX-CLOUD', 'bar', TableRow())"
                    " WITH UNKNOWN_PARAM_1='837747712', UNKNOWN_PARAM_2=Tuple<Uint16, Utf8>,"
                    " INPUT_TYPE=Struct<a:Int32>, OUTPUT_TYPE=Struct<b:Int32>")
                    .IsOk());
}

Y_UNIT_TEST(InValidUseFunctions) {
    ExpectFailWithError("PROCESS plato.Input USING some::udf(*) WITH INPUT_TYPE=Struct<a:Int32>",
                        "<main>:1:33: Error: PROCESS without USING EXTERNAL FUNCTION doesn't allow WITH block\n");

    ExpectFailWithError("PROCESS plato.Input USING EXTERNAL FUNCTION('YANDEX-CLOUD', 'jhhjfh88134d')"
                        " WITH INPUT_TYPE=Struct<a:Int32>, OUTPUT_TYPE=Struct<b:Int32>"
                        " ASSUME ORDER BY key",
                        "<main>:1:129: Error: PROCESS with USING EXTERNAL FUNCTION doesn't allow ASSUME block\n");

    ExpectFailWithError("PROCESS plato.Input USING EXTERNAL FUNCTION('YANDEX-CLOUD', 'foo', 'bar', 'baz')",
                        "<main>:1:15: Error: EXTERNAL FUNCTION requires from 2 to 3 arguments, but got: 4\n");

    ExpectFailWithError("PROCESS plato.Input\n"
                        " USING EXTERNAL FUNCTION('YANDEX-CLOUD', 'foo', <|field_1: a1, field_b: b1|>)\n"
                        " WITH INPUT_TYPE=Struct<a:Int32>, OUTPUT_TYPE=Struct<b:Int32>,\n"
                        " CONCURRENCY=3, BATCH_SIZE=1000000, CONNECTION='yc-folder34fse-con',\n"
                        " CONCURRENCY=5, INPUT_TYPE=Struct<b:Bool>,\n"
                        " INIT=[0, 900]\n",
                        "<main>:5:2: Error: WITH \"CONCURRENCY\" clause should be specified only once\n"
                        "<main>:5:17: Error: WITH \"INPUT_TYPE\" clause should be specified only once\n");
}

} // Y_UNIT_TEST_SUITE(ExternalFunction)

Y_UNIT_TEST_SUITE(WarnUnused) {

void CheckUnused(const TString& req, const TString& symbol, unsigned row, unsigned col) {
    auto res = SqlToYql(req);

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_NO_DIFF(Err2Str(res), TStringBuilder() << "<main>:" << row << ":" << col << ": Warning: Symbol " << symbol << " is not used, code: 4527\n");
}

Y_UNIT_TEST(ActionOrSubquery) {
    TString req = "  $a()\n"
                  "as select 1;\n"
                  "end define;\n"
                  "\n"
                  "select 1;";
    CheckUnused("define action\n" + req, "$a", 2, 3);
    CheckUnused("define subquery\n" + req, "$a", 2, 3);
}

Y_UNIT_TEST(Import) {
    TString req = "import lib1 symbols\n"
                  "  $sqr;\n"
                  "select 1;";
    CheckUnused(req, "$sqr", 2, 3);

    req = "import lib1 symbols\n"
          "  $sqr as\n"
          "    $sq;\n"
          "select 1;";
    CheckUnused(req, "$sq", 3, 5);
}

Y_UNIT_TEST(NamedNodeStatement) {
    TString req = " $a, $a = AsTuple(1, 2);\n"
                  "select $a;";
    CheckUnused(req, "$a", 1, 2);
    req = "$a,  $b = AsTuple(1, 2);\n"
          "select $a;";
    CheckUnused(req, "$b", 1, 6);
    CheckUnused(" $a = 1; $a = 2; select $a;", "$a", 1, 2);
}

Y_UNIT_TEST(Declare) {
    CheckUnused("declare $a as String;select 1;", "$a", 1, 9);
}

Y_UNIT_TEST(ActionParams) {
    TString req = "define action $a($x, $y) as\n"
                  "  select $x;\n"
                  "end define;\n"
                  "\n"
                  "do $a(1,2);";
    CheckUnused(req, "$y", 1, 22);
}

Y_UNIT_TEST(SubqueryParams) {
    TString req = "use plato;\n"
                  "define subquery $q($name, $x) as\n"
                  "  select * from $name;\n"
                  "end define;\n"
                  "\n"
                  "select * from $q(\"Input\", 1);";
    CheckUnused(req, "$x", 2, 27);
}

Y_UNIT_TEST(For) {
    TString req = "define action $a() as\n"
                  "  select 1;\n"
                  "end define;\n"
                  "\n"
                  "evaluate for $i in ListFromRange(1, 10)\n"
                  "do $a();";
    CheckUnused(req, "$i", 5, 14);
}

Y_UNIT_TEST(LambdaParams) {
    TString req = "$lambda = ($x, $y) -> ($x);\n"
                  "select $lambda(1, 2);";
    CheckUnused(req, "$y", 1, 16);
}

Y_UNIT_TEST(InsideLambdaBody) {
    TString req = "$lambda = () -> {\n"
                  "  $x = 1; return 1;\n"
                  "};\n"
                  "select $lambda();";
    CheckUnused(req, "$x", 2, 3);
    req = "$lambda = () -> {\n"
          "  $x = 1; $x = 2; return $x;\n"
          "};\n"
          "select $lambda();";
    CheckUnused(req, "$x", 2, 3);
}

Y_UNIT_TEST(InsideAction) {
    TString req = "define action $a() as\n"
                  "  $x = 1; select 1;\n"
                  "end define;\n"
                  "\n"
                  "do $a();";
    CheckUnused(req, "$x", 2, 3);
    req = "define action $a() as\n"
          "  $x = 1; $x = 2; select $x;\n"
          "end define;\n"
          "\n"
          "do $a();";
    CheckUnused(req, "$x", 2, 3);
}

Y_UNIT_TEST(NoWarnOnNestedActions) {
    auto req = "pragma warning(\"error\", \"4527\");\n"
               "define action $action($b) as\n"
               "    define action $aaa() as\n"
               "        select $b;\n"
               "    end define;\n"
               "    do $aaa();\n"
               "end define;\n"
               "\n"
               "do $action(1);";
    UNIT_ASSERT(SqlToYql(req).IsOk());
}

Y_UNIT_TEST(NoWarnForUsageAfterSubquery) {
    auto req = "use plato;\n"
               "pragma warning(\"error\", \"4527\");\n"
               "\n"
               "$a = 1;\n"
               "\n"
               "define subquery $q($table) as\n"
               "  select * from $table;\n"
               "end define;\n"
               "\n"
               "select * from $q(\"Input\");\n"
               "select $a;";
    UNIT_ASSERT(SqlToYql(req).IsOk());
}

} // Y_UNIT_TEST_SUITE(WarnUnused)

Y_UNIT_TEST_SUITE(AnonymousNames) {
Y_UNIT_TEST(ReferenceAnonymousVariableIsForbidden) {
    auto req = "$_ = 1; select $_;";

    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:16: Error: Unable to reference anonymous name $_\n");

    req = "$`_` = 1; select $`_`;";
    res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:18: Error: Unable to reference anonymous name $_\n");
}

Y_UNIT_TEST(Declare) {
    auto req = "declare $_ as String;";
    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:9: Error: Can not use anonymous name '$_' in DECLARE statement\n");
}

Y_UNIT_TEST(ActionSubquery) {
    auto req = "define action $_() as select 1; end define;";
    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:15: Error: Can not use anonymous name '$_' as ACTION name\n");

    req = "define subquery $_() as select 1; end define;";
    res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:17: Error: Can not use anonymous name '$_' as SUBQUERY name\n");
}

Y_UNIT_TEST(Import) {
    auto req = "import lib symbols $sqr as $_;";
    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:28: Error: Can not import anonymous name $_\n");
}

Y_UNIT_TEST(Export) {
    auto req = "export $_;";
    auto res = SqlToYqlWithMode(req, NSQLTranslation::ESqlMode::LIBRARY);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: Can not export anonymous name $_\n");
}

Y_UNIT_TEST(AnonymousInActionArgs) {
    auto req = "pragma warning(\"error\", \"4527\");\n"
               "define action $a($_, $y, $_) as\n"
               "  select $y;\n"
               "end define;\n"
               "\n"
               "do $a(1,2,3);";
    UNIT_ASSERT(SqlToYql(req).IsOk());
}

Y_UNIT_TEST(AnonymousInSubqueryArgs) {
    auto req = "use plato;\n"
               "pragma warning(\"error\", \"4527\");\n"
               "define subquery $q($_, $y, $_) as\n"
               "  select * from $y;\n"
               "end define;\n"
               "\n"
               "select * from $q(1,\"Input\",3);";
    UNIT_ASSERT(SqlToYql(req).IsOk());
}

Y_UNIT_TEST(AnonymousInLambdaArgs) {
    auto req = "pragma warning(\"error\", \"4527\");\n"
               "$lambda = ($_, $x, $_) -> ($x);\n"
               "select $lambda(1,2,3);";
    UNIT_ASSERT(SqlToYql(req).IsOk());
}

Y_UNIT_TEST(AnonymousInFor) {
    auto req = "pragma warning(\"error\", \"4527\");\n"
               "evaluate for $_ in ListFromRange(1, 10) do begin select 1; end do;";
    UNIT_ASSERT(SqlToYql(req).IsOk());
}

Y_UNIT_TEST(Assignment) {
    auto req = "pragma warning(\"error\", \"4527\");\n"
               "$_ = 1;\n"
               "$_, $x, $_ = AsTuple(1,2,3);\n"
               "select $x;";
    UNIT_ASSERT(SqlToYql(req).IsOk());
}

} // Y_UNIT_TEST_SUITE(AnonymousNames)

Y_UNIT_TEST_SUITE(LibraSqlSugar) {
auto makeResult = [](TStringBuf settings) {
    return SqlToYql(
        TStringBuilder()
        << settings
        << "\n$udf1 = MyLibra::MakeLibraPreprocessor($settings);"
        << "\n$udf2 = CustomLibra::MakeLibraPreprocessor($settings);"
        << "\nPROCESS plato.Input USING $udf1(TableRow())"
        << "\nUNION ALL"
        << "\nPROCESS plato.Input USING $udf2(TableRow());");
};

Y_UNIT_TEST(EmptySettings) {
    auto res = makeResult(R"(
                $settings = AsStruct();
            )");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(OnlyEntities) {
    auto res = makeResult(R"(
                $settings = AsStruct(
                    AsList("A", "B", "C") AS Entities
                );
            )");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(EntitiesWithStrategy) {
    auto res = makeResult(R"(
                $settings = AsStruct(
                    AsList("A", "B", "C") AS Entities,
                    "blacklist" AS EntitiesStrategy
                );
            )");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(AllSettings) {
    auto res = makeResult(R"(
                $settings = AsStruct(
                    AsList("A", "B", "C") AS Entities,
                    "whitelist" AS EntitiesStrategy,
                    "path" AS BlockstatDict,
                    false AS ParseWithFat,
                    "map" AS Mode
                );
            )");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(BadStrategy) {
    auto res = makeResult(R"(
                $settings = AsStruct("bad" AS EntitiesStrategy);
            )");
    UNIT_ASSERT_STRING_CONTAINS(
        Err2Str(res),
        "Error: MakeLibraPreprocessor got invalid entities strategy: expected 'whitelist' or 'blacklist'");
}

Y_UNIT_TEST(BadEntities) {
    auto res = makeResult(R"(
                $settings = AsStruct(AsList("A", 1) AS Entities);
            )");
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "Error: MakeLibraPreprocessor entity must be string literal");
}

} // Y_UNIT_TEST_SUITE(LibraSqlSugar)

Y_UNIT_TEST_SUITE(TrailingQuestionsNegative) {
Y_UNIT_TEST(Basic) {
    ExpectFailWithError("SELECT 1?;", "<main>:1:9: Error: Unexpected token '?' at the end of expression\n");
#if ANTLR_VER == 3
    ExpectFailWithError("SELECT 1? + 1;", "<main>:1:10: Error: Unexpected token '+' : cannot match to any predicted input...\n\n");
#else
    ExpectFailWithError("SELECT 1? + 1;", "<main>:1:10: Error: mismatched input '+' expecting {<EOF>, ';'}\n");
#endif
    ExpectFailWithError("SELECT 1 + 1??? < 2", "<main>:1:13: Error: Unexpected token '?' at the end of expression\n");
    ExpectFailWithError("SELECT   1? > 2? > 3?",
                        "<main>:1:11: Error: Unexpected token '?' at the end of expression\n"
                        "<main>:1:16: Error: Unexpected token '?' at the end of expression\n"
                        "<main>:1:21: Error: Unexpected token '?' at the end of expression\n");
}

Y_UNIT_TEST(SmartParen) {
    ExpectFailWithError("$x = 1; SELECT (Int32?, $x?)", "<main>:1:27: Error: Unexpected token '?' at the end of expression\n");
    ExpectFailWithError("SELECT (Int32, foo?)", "<main>:1:19: Error: Unexpected token '?' at the end of expression\n");
}

Y_UNIT_TEST(LambdaOptArgs) {
    ExpectFailWithError("$l = ($x, $y?, $z??, $t?) -> ($x);", "<main>:1:18: Error: Expecting at most one '?' token here (for optional lambda parameters), but got 2\n");
}
} // Y_UNIT_TEST_SUITE(TrailingQuestionsNegative)

Y_UNIT_TEST_SUITE(FlexibleTypes) {
Y_UNIT_TEST(AssumeOrderByType) {
    UNIT_ASSERT(SqlToYql("PRAGMA FlexibleTypes; SELECT 1 AS int32 ASSUME ORDER BY int32").IsOk());
}

Y_UNIT_TEST(GroupingSets) {
    UNIT_ASSERT(SqlToYql("PRAGMA FlexibleTypes; SELECT COUNT(*) AS cnt, text, uuid FROM plato.Input GROUP BY GROUPING SETS((uuid), (uuid, text));").IsOk());
}

Y_UNIT_TEST(WeakField) {
    UNIT_ASSERT(SqlToYql("PRAGMA FlexibleTypes; SELECT WeakField(text, string) as text FROM plato.Input").IsOk());
}

Y_UNIT_TEST(Aggregation1) {
    TString q =
        "PRAGMA FlexibleTypes;\n"
        "$foo = ($x, $const, $type) -> ($x || $const || FormatType($type));\n"
        "SELECT $foo(SOME(x), 'aaa', String) FROM plato.Input GROUP BY y;";
    UNIT_ASSERT(SqlToYql(q).IsOk());
}

Y_UNIT_TEST(Aggregation2) {
    TString q =
        "PRAGMA FlexibleTypes;\n"
        "SELECT 1 + String + MAX(key) FROM plato.Input;";
    UNIT_ASSERT(SqlToYql(q).IsOk());
}

} // Y_UNIT_TEST_SUITE(FlexibleTypes)

Y_UNIT_TEST_SUITE(ExternalDeclares) {
Y_UNIT_TEST(BasicUsage) {
    NSQLTranslation::TTranslationSettings settings;
    settings.DeclaredNamedExprs["foo"] = "String";
    auto res = SqlToYqlWithSettings("select $foo;", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT(res.Issues.Size() == 0);

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "declare") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(R"__((declare "$foo" (DataType 'String)))__"));
        }
    };

    TWordCountHive elementStat = {{TString("declare"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["declare"]);
}

Y_UNIT_TEST(DeclareOverrides) {
    NSQLTranslation::TTranslationSettings settings;
    settings.DeclaredNamedExprs["foo"] = "String";
    auto res = SqlToYqlWithSettings("declare $foo as Int32; select $foo;", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT(res.Issues.Size() == 0);

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "declare") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(R"__((declare "$foo" (DataType 'Int32)))__"));
        }
    };

    TWordCountHive elementStat = {{TString("declare"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["declare"]);
}

Y_UNIT_TEST(UnusedDeclareDoesNotProduceWarning) {
    NSQLTranslation::TTranslationSettings settings;
    settings.DeclaredNamedExprs["foo"] = "String";
    auto res = SqlToYqlWithSettings("select 1;", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT(res.Issues.Size() == 0);

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "declare") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(R"__((declare "$foo" (DataType 'String)))__"));
        }
    };

    TWordCountHive elementStat = {{TString("declare"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["declare"]);
}

Y_UNIT_TEST(DeclaresWithInvalidTypesFails) {
    NSQLTranslation::TTranslationSettings settings;
    settings.DeclaredNamedExprs["foo"] = "List<BadType>";
    auto res = SqlToYqlWithSettings("select 1;", settings);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res),
                        "<main>:0:5: Error: Unknown type: 'BadType'\n"
                        "<main>: Error: Failed to parse type for externally declared name 'foo'\n");
}

} // Y_UNIT_TEST_SUITE(ExternalDeclares)

Y_UNIT_TEST_SUITE(BlockEnginePragma) {
Y_UNIT_TEST(Basic) {
    const TVector<TString> values = {"auto", "force", "disable"};
    for (const auto& value : values) {
        const auto query = TStringBuilder() << "pragma Blockengine='" << value << "'; select 1;";
        NYql::TAstParseResult res = SqlToYql(query);
        UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

        TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
            Y_UNUSED(word);
            UNIT_ASSERT_STRING_CONTAINS(line, TStringBuilder() << R"(Configure! world (DataSource '"config") '"BlockEngine" '")" << value << "\"");
        };

        TWordCountHive elementStat({"BlockEngine"});
        VerifyProgram(res, elementStat, verifyLine);
        UNIT_ASSERT(elementStat["BlockEngine"] == ((value == "disable") ? 0 : 1));
    }
}

Y_UNIT_TEST(UnknownSetting) {
    ExpectFailWithError("use plato; pragma BlockEngine='foo';",
                        "<main>:1:31: Error: Expected `disable|auto|force' argument for: BlockEngine\n");
}

} // Y_UNIT_TEST_SUITE(BlockEnginePragma)

Y_UNIT_TEST_SUITE(CompactNamedExprs) {
Y_UNIT_TEST(SourceCallablesInWrongContext) {
    TString query = R"(
            pragma CompactNamedExprs;
            $foo = %s();
            select $foo from plato.Input;
        )";

    THashMap<TString, TString> errs = {
        {"TableRow", "<main>:3:20: Error: TableRow requires data source\n"},
        {"JoinTableRow", "<main>:3:20: Error: JoinTableRow requires data source\n"},
        {"TableRecordIndex", "<main>:3:20: Error: Unable to use function: TableRecord without source\n"},
        {"TablePath", "<main>:3:20: Error: Unable to use function: TablePath without source\n"},
        {"SystemMetadata", "<main>:3:20: Error: Unable to use function: SystemMetadata without source\n"},
    };

    for (TString callable : {"TableRow", "JoinTableRow", "TableRecordIndex", "TablePath", "SystemMetadata"}) {
        auto req = Sprintf(query.c_str(), callable.c_str());
        ExpectFailWithError(req, errs[callable]);
    }
}

Y_UNIT_TEST(ValidateUnusedExprs) {
    TString query = R"(
            pragma warning("disable", "4527");
            pragma CompactNamedExprs;
            pragma ValidateUnusedExprs;

            $foo = count(1);
            select 1;
        )";
    ExpectFailWithError(query, "<main>:6:20: Error: Aggregation is not allowed in this context\n");
    query = R"(
            pragma warning("disable", "4527");
            pragma CompactNamedExprs;
            pragma ValidateUnusedExprs;

            define subquery $x() as
                select count(1, 2);
            end define;
            select 1;
        )";
    ExpectFailWithError(query, "<main>:7:24: Error: Aggregation function Count requires exactly 1 argument(s), given: 2\n");
}

Y_UNIT_TEST(DisableValidateUnusedExprs) {
    TString query = R"(
                pragma warning("disable", "4527");
                pragma CompactNamedExprs;
                pragma DisableValidateUnusedExprs;

                $foo = count(1);
                select 1;
            )";
    SqlToYql(query).IsOk();
    query = R"(
                pragma warning("disable", "4527");
                pragma CompactNamedExprs;
                pragma DisableValidateUnusedExprs;

                define subquery $x() as
                    select count(1, 2);
                end define;
                select 1;
            )";
    SqlToYql(query).IsOk();
}

} // Y_UNIT_TEST_SUITE(CompactNamedExprs)

Y_UNIT_TEST_SUITE(NullAsTypeName) {

Y_UNIT_TEST(ListOfNull) {
    NYql::TAstParseResult res = SqlToYql("select List<NULL>;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    TWordCountHive stat = {"ListType", "NullType"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["ListType"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["NullType"], 1);
}

Y_UNIT_TEST(CastAsNull) {
    NYql::TAstParseResult res = SqlToYql("select cast(1 as NULL);");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    TWordCountHive stat = {"NullType"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["NullType"], 1);
}

Y_UNIT_TEST(OptionalNull) {
    NYql::TAstParseResult res = SqlToYql("select cast(1 as NULL?);");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    TWordCountHive stat = {"NullType", "OptionalType"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["NullType"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["OptionalType"], 1);
}

} // Y_UNIT_TEST_SUITE(NullAsTypeName)

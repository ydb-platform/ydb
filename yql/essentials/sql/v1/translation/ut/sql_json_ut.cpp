#include "sql_ut.h"

#include <yql/essentials/sql/v1/translation/sql.h>

using namespace NSQLTranslationV1;

Y_UNIT_TEST_SUITE(JsonValue) {

Y_UNIT_TEST(JsonValueArgumentCount) {
    NYql::TAstParseResult res = SqlToYql("select JSON_VALUE(CAST(@@{\"key\": 1238}@@ as Json));");

    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:49: Error: mismatched input ')' expecting ','\n");
}

Y_UNIT_TEST(JsonValueJsonPathMustBeLiteralString) {
    NYql::TAstParseResult res = SqlToYql(R"($jsonPath = "strict $.key"; select JSON_VALUE(CAST(@@{"key": 1238}@@ as Json), $jsonPath);)");

    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:79: Error: mismatched input '$' expecting STRING_VALUE\n");
}

Y_UNIT_TEST(JsonValueTranslation) {
    NYql::TAstParseResult res = SqlToYql(R"(select JSON_VALUE(CAST(@@{"key": 1238}@@ as Json), "strict $.key");)");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        Y_UNUSED(word);
        UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'\"strict $.key\""));
        UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("SafeCast"));
        UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("DataType 'Json"));
    };

    TWordCountHive elementStat({"JsonValue"});
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["JsonValue"]);
}

Y_UNIT_TEST(JsonValueReturningSection) {
    for (const auto& typeName : {"Bool", "Int64", "Double", "String"}) {
        NYql::TAstParseResult res = SqlToYql(
            TStringBuilder() << R"(select JSON_VALUE(CAST(@@{"key": 1238}@@ as Json), "strict $.key" RETURNING )" << typeName << ");");

        UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

        TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
            Y_UNUSED(word);
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'\"strict $.key\""));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("SafeCast"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("DataType 'Json"));
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(TStringBuilder() << "DataType '" << typeName));
        };

        TWordCountHive elementStat({typeName});
        VerifyProgram(res, elementStat, verifyLine);
        UNIT_ASSERT(elementStat[typeName] > 0);
    }
}

Y_UNIT_TEST(JsonValueInvalidReturningType) {
    NYql::TAstParseResult res = SqlToYql("select JSON_VALUE(CAST(@@{'key': 1238}@@ as Json), 'strict $.key' RETURNING invalid);");

    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:77: Error: Unknown simple type 'invalid'\n");
}

Y_UNIT_TEST(JsonValueAndReturningInExpressions) {
    NYql::TAstParseResult res = SqlToYql(
        "USE plato\n;"
        "$json_value = \"some string\";\n"
        "SELECT $json_value;\n"
        "SELECT 1 as json_value;\n"
        "SELECT $json_value as json_value;\n"
        "$returning = \"another string\";\n"
        "SELECT $returning;\n"
        "SELECT 1 as returning;\n"
        "SELECT $returning as returning;\n");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(JsonValueValidCaseHandlers) {
    const TVector<std::pair<TString, TString>> testCases = {
        {"", "'DefaultValue (Null)"},
        {"NULL", "'DefaultValue (Null)"},
        {"ERROR", "'Error (Null)"},
        {"DEFAULT 123", "'DefaultValue (Int32 '\"123\")"},
    };

    for (const auto& onEmpty : testCases) {
        for (const auto& onError : testCases) {
            TStringBuilder query;
            query << "$json = CAST(@@{\"key\": 1238}@@ as Json);\n"
                  << "SELECT JSON_VALUE($json, \"strict $.key\"";
            if (!onEmpty.first.empty()) {
                query << " " << onEmpty.first << " ON EMPTY";
            }
            if (!onError.first.empty()) {
                query << " " << onError.first << " ON ERROR";
            }
            query << ");\n";

            NYql::TAstParseResult res = SqlToYql(query);

            UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

            TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
                Y_UNUSED(word);
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(onEmpty.second + " " + onError.second));
            };

            TWordCountHive elementStat({"JsonValue"});
            VerifyProgram(res, elementStat, verifyLine);
            UNIT_ASSERT(elementStat["JsonValue"] > 0);
        }
    }
}

Y_UNIT_TEST(JsonValueTooManyCaseHandlers) {
    NYql::TAstParseResult res = SqlToYql(
        "select JSON_VALUE(CAST(@@{\"key\": 1238}@@ as Json), \"strict $.key\" NULL ON EMPTY NULL ON ERROR NULL ON EMPTY);\n");

    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(
        Err2Str(res),
        "<main>:1:52: Error: Only 1 ON EMPTY and/or 1 ON ERROR clause is expected\n");
}

Y_UNIT_TEST(JsonValueTooManyOnEmpty) {
    NYql::TAstParseResult res = SqlToYql(
        "select JSON_VALUE(CAST(@@{\"key\": 1238}@@ as Json), \"strict $.key\" NULL ON EMPTY NULL ON EMPTY);\n");

    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(
        Err2Str(res),
        "<main>:1:52: Error: Only 1 ON EMPTY clause is expected\n");
}

Y_UNIT_TEST(JsonValueTooManyOnError) {
    NYql::TAstParseResult res = SqlToYql(
        "select JSON_VALUE(CAST(@@{\"key\": 1238}@@ as Json), \"strict $.key\" NULL ON ERROR NULL ON ERROR);\n");

    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(
        Err2Str(res),
        "<main>:1:52: Error: Only 1 ON ERROR clause is expected\n");
}

Y_UNIT_TEST(JsonValueOnEmptyAfterOnError) {
    NYql::TAstParseResult res = SqlToYql(
        "select JSON_VALUE(CAST(@@{\"key\": 1238}@@ as Json), \"strict $.key\" NULL ON ERROR NULL ON EMPTY);\n");

    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(
        Err2Str(res),
        "<main>:1:52: Error: ON EMPTY clause must be before ON ERROR clause\n");
}

Y_UNIT_TEST(JsonValueNullInput) {
    NYql::TAstParseResult res = SqlToYql(R"(SELECT JSON_VALUE(NULL, "strict $.key");)");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
        Y_UNUSED(word);
        UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("(Nothing (OptionalType (DataType 'Json)))"));
    };

    TWordCountHive elementStat({"JsonValue"});
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT(elementStat["JsonValue"] > 0);
}
} // Y_UNIT_TEST_SUITE(JsonValue)

Y_UNIT_TEST_SUITE(JsonExists) {

Y_UNIT_TEST(JsonExistsValidHandlers) {
    const TVector<std::pair<TString, TString>> testCases = {
        {"", "(Just (Bool '\"false\"))"},
        {"TRUE ON ERROR", "(Just (Bool '\"true\"))"},
        {"FALSE ON ERROR", "(Just (Bool '\"false\"))"},
        {"UNKNOWN ON ERROR", "(Nothing (OptionalType (DataType 'Bool)))"},
        // NOTE: in this case we expect arguments of JsonExists callable to end immediately
        // after variables. This parenthesis at the end of the expression is left on purpose
        {"ERROR ON ERROR", "(Utf8 '\"strict $.key\") (JsonVariables))"},
    };

    for (const auto& item : testCases) {
        NYql::TAstParseResult res = SqlToYql(
            TStringBuilder() << R"(
                    $json = CAST(@@{"key": 1238}@@ as Json);
                    SELECT JSON_EXISTS($json, "strict $.key" )"
                             << item.first << ");\n");

        UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

        TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
            Y_UNUSED(word);
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(item.second));
        };

        TWordCountHive elementStat({"JsonExists"});
        VerifyProgram(res, elementStat, verifyLine);
        UNIT_ASSERT(elementStat["JsonExists"] > 0);
    }
}

Y_UNIT_TEST(JsonExistsInvalidHandler) {
    NYql::TAstParseResult res = SqlToYql(R"(
            $json = CAST(@@{"key": 1238}@@ as Json);
            $default = false;
            SELECT JSON_EXISTS($json, "strict $.key" $default ON ERROR);
        )");

    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:4:53: Error: mismatched input '$' expecting {')', ERROR, FALSE, TRUE, UNKNOWN}\n");
}

Y_UNIT_TEST(JsonExistsNullInput) {
    NYql::TAstParseResult res = SqlToYql(R"(SELECT JSON_EXISTS(NULL, "strict $.key");)");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
        Y_UNUSED(word);
        UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("(Nothing (OptionalType (DataType 'Json)))"));
    };

    TWordCountHive elementStat({"JsonExists"});
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT(elementStat["JsonExists"] > 0);
}

} // Y_UNIT_TEST_SUITE(JsonExists)

Y_UNIT_TEST_SUITE(JsonQuery) {

Y_UNIT_TEST(JsonQueryValidHandlers) {
    using TTestSuite = const TVector<std::pair<TString, TString>>;
    TTestSuite wrapCases = {
        {"", "'NoWrap"},
        {"WITHOUT WRAPPER", "'NoWrap"},
        {"WITHOUT ARRAY WRAPPER", "'NoWrap"},
        {"WITH WRAPPER", "'Wrap"},
        {"WITH ARRAY WRAPPER", "'Wrap"},
        {"WITH UNCONDITIONAL WRAPPER", "'Wrap"},
        {"WITH UNCONDITIONAL ARRAY WRAPPER", "'Wrap"},
        {"WITH CONDITIONAL WRAPPER", "'ConditionalWrap"},
        {"WITH CONDITIONAL ARRAY WRAPPER", "'ConditionalWrap"},
    };
    TTestSuite handlerCases = {
        {"", "'Null"},
        {"ERROR", "'Error"},
        {"NULL", "'Null"},
        {"EMPTY ARRAY", "'EmptyArray"},
        {"EMPTY OBJECT", "'EmptyObject"},
    };

    for (const auto& wrap : wrapCases) {
        for (const auto& onError : handlerCases) {
            for (const auto& onEmpty : handlerCases) {
                TStringBuilder query;
                query << R"($json = CAST(@@{"key": [123]}@@ as Json);
                        SELECT JSON_QUERY($json, "strict $.key" )"
                      << wrap.first;
                if (!onEmpty.first.empty()) {
                    if (wrap.first.StartsWith("WITH ")) {
                        continue;
                    }
                    query << " " << onEmpty.first << " ON EMPTY";
                }
                if (!onError.first.empty()) {
                    query << " " << onError.first << " ON ERROR";
                }
                query << ");\n";

                NYql::TAstParseResult res = SqlToYql(query);

                UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

                TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
                    Y_UNUSED(word);
                    const TString args = TStringBuilder() << wrap.second << " " << onEmpty.second << " " << onError.second;
                    UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(args));
                };

                Cout << wrap.first << " " << onEmpty.first << " " << onError.first << Endl;

                TWordCountHive elementStat({"JsonQuery"});
                VerifyProgram(res, elementStat, verifyLine);
                UNIT_ASSERT(elementStat["JsonQuery"] > 0);
            }
        }
    }
}

Y_UNIT_TEST(JsonQueryOnEmptyWithWrapper) {
    NYql::TAstParseResult res = SqlToYql(R"(
            $json = CAST(@@{"key": 1238}@@ as Json);
            SELECT JSON_QUERY($json, "strict $" WITH ARRAY WRAPPER EMPTY ARRAY ON EMPTY);
        )");

    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:38: Error: ON EMPTY is prohibited because WRAPPER clause is specified\n");
}

Y_UNIT_TEST(JsonQueryNullInput) {
    NYql::TAstParseResult res = SqlToYql(R"(SELECT JSON_QUERY(NULL, "strict $.key");)");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
        Y_UNUSED(word);
        UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("(Nothing (OptionalType (DataType 'Json)))"));
    };

    TWordCountHive elementStat({"JsonQuery"});
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT(elementStat["JsonQuery"] > 0);
}

Y_UNIT_TEST(YQL_20777) {
    ExpectFailWithError(
        R"sql(SELECT JSON_QUERY ('{}'j, '\1');)sql",
        "<main>:1:30: Error: Failed to parse string literal: Invalid octal value near byte 3\n");
}

} // Y_UNIT_TEST_SUITE(JsonQuery)

Y_UNIT_TEST_SUITE(JsonPassing) {

Y_UNIT_TEST(SupportedVariableTypes) {
    const TVector<TString> functions = {"JSON_EXISTS", "JSON_VALUE", "JSON_QUERY"};

    for (const auto& function : functions) {
        const auto query = Sprintf(R"(
                    pragma CompactNamedExprs;
                    $json = CAST(@@{"key": 1238}@@ as Json);
                    SELECT %s(
                        $json,
                        "strict $.key"
                        PASSING
                            "string" as var1,
                            1.234 as var2,
                            CAST(1 as Int64) as var3,
                            true as var4,
                            $json as var5
                    ))",
                                   function.data());
        NYql::TAstParseResult res = SqlToYql(query);

        UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

        TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
            Y_UNUSED(word);
            UNIT_ASSERT_VALUES_UNEQUAL_C(TString::npos, line.find(R"('('"var1" (String '"string")))"), "Cannot find `var1`");
            UNIT_ASSERT_VALUES_UNEQUAL_C(TString::npos, line.find(R"('('"var2" (Double '"1.234")))"), "Cannot find `var2`");
            UNIT_ASSERT_VALUES_UNEQUAL_C(TString::npos, line.find(R"('('"var3" (SafeCast (Int32 '"1") (DataType 'Int64))))"), "Cannot find `var3`");
            UNIT_ASSERT_VALUES_UNEQUAL_C(TString::npos, line.find(R"('('"var4" (Bool '"true")))"), "Cannot find `var4`");
            UNIT_ASSERT_VALUES_UNEQUAL_C(TString::npos, line.find(R"('('"var5" namedexprnode0))"), "Cannot find `var5`");
        };

        TWordCountHive elementStat({"JsonVariables"});
        VerifyProgram(res, elementStat, verifyLine);
        UNIT_ASSERT(elementStat["JsonVariables"] > 0);
    }
}

Y_UNIT_TEST(ValidVariableNames) {
    const TVector<TString> functions = {"JSON_EXISTS", "JSON_VALUE", "JSON_QUERY"};

    for (const auto& function : functions) {
        const auto query = Sprintf(R"(
                    $json = CAST(@@{"key": 1238}@@ as Json);
                    SELECT %s(
                        $json,
                        "strict $.key"
                        PASSING
                            "one" as var1,
                            "two" as "VaR2",
                            "three" as `var3`,
                            "four" as VaR4
                    ))",
                                   function.data());
        NYql::TAstParseResult res = SqlToYql(query);

        UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

        TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
            Y_UNUSED(word);
            UNIT_ASSERT_VALUES_UNEQUAL_C(TString::npos, line.find(R"('('"var1" (String '"one")))"), "Cannot find `var1`");
            UNIT_ASSERT_VALUES_UNEQUAL_C(TString::npos, line.find(R"('('"VaR2" (String '"two")))"), "Cannot find `VaR2`");
            UNIT_ASSERT_VALUES_UNEQUAL_C(TString::npos, line.find(R"('('"var3" (String '"three")))"), "Cannot find `var3`");
            UNIT_ASSERT_VALUES_UNEQUAL_C(TString::npos, line.find(R"('('"VaR4" (String '"four")))"), "Cannot find `VaR4`");
        };

        TWordCountHive elementStat({"JsonVariables"});
        VerifyProgram(res, elementStat, verifyLine);
        UNIT_ASSERT(elementStat["JsonVariables"] > 0);
    }
}

} // Y_UNIT_TEST_SUITE(JsonPassing)

Y_UNIT_TEST_SUITE(MigrationToJsonApi) {

Y_UNIT_TEST(WarningOnDeprecatedJsonUdf) {
    NYql::TAstParseResult res = SqlToYql(R"(
            $json = CAST(@@{"key": 1234}@@ as Json);
            SELECT Json::Parse($json);
        )");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:26: Warning: Json UDF is deprecated. Please use JSON API instead, code: 4506\n");
}

} // Y_UNIT_TEST_SUITE(MigrationToJsonApi)


#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/sql/sql.h>
#include <util/generic/map.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/split.h>

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
    settings.MaxErrors = maxErrors;
    settings.Mode = mode;
    settings.Arena = &arena;
    settings.AnsiLexer = ansiLexer;
    settings.SyntaxVersion = 1;
    auto res = SqlToYql(query, settings);
    if (debug == EDebugOutput::ToCerr) {
        Err2Str(res, debug);
    }
    return res;
}

NYql::TAstParseResult SqlToYql(const TString& query, size_t maxErrors = 10, const TString& provider = {}, EDebugOutput debug = EDebugOutput::None) {
    return SqlToYqlWithMode(query, NSQLTranslation::ESqlMode::QUERY, maxErrors, provider, debug);
}

NYql::TAstParseResult SqlToYqlWithSettings(const TString& query, const NSQLTranslation::TTranslationSettings& settings) {
    return SqlToYqlWithMode(query, NSQLTranslation::ESqlMode::QUERY, 10, {}, EDebugOutput::None, false, settings);
}

void ExpectFailWithError(const TString& query, const TString& error) {
    NYql::TAstParseResult res = SqlToYql(query);

    UNIT_ASSERT(!res.Root);
    UNIT_ASSERT_NO_DIFF(Err2Str(res), error);
}

NYql::TAstParseResult SqlToYqlWithAnsiLexer(const TString& query, size_t maxErrors = 10, const TString& provider = {}, EDebugOutput debug = EDebugOutput::None) {
    bool ansiLexer = true;
    return SqlToYqlWithMode(query, NSQLTranslation::ESqlMode::QUERY, maxErrors, provider, debug, ansiLexer);
}

void ExpectFailWithErrorForAnsiLexer(const TString& query, const TString& error) {
    NYql::TAstParseResult res = SqlToYqlWithAnsiLexer(query);

    UNIT_ASSERT(!res.Root);
    UNIT_ASSERT_NO_DIFF(Err2Str(res), error);
}

TString GetPrettyPrint(const NYql::TAstParseResult& res) {
    TStringStream yqlProgram;
    res.Root->PrettyPrintTo(yqlProgram, NYql::TAstPrintFlags::PerLine | NYql::TAstPrintFlags::ShortQuote);
    return yqlProgram.Str();
}

TString Quote(const char* str) {
    return TStringBuilder() << "'\"" << str << "\"";
}

class TWordCountHive: public TMap<TString, unsigned> {
public:
    TWordCountHive(std::initializer_list<TString> strings) {
        for (auto& str: strings) {
            emplace(str, 0);
        }
    }

    TWordCountHive(std::initializer_list<std::pair<const TString, unsigned>> list)
        : TMap(list)
    {
    }
};

typedef std::function<void (const TString& word, const TString& line)> TVerifyLineFunc;

TString VerifyProgram(const NYql::TAstParseResult& res, TWordCountHive& wordCounter, TVerifyLineFunc verifyLine = TVerifyLineFunc()) {
    const auto programm = GetPrettyPrint(res);
    TVector<TString> yqlProgram;
    Split(programm, "\n", yqlProgram);
    for (const auto& line: yqlProgram) {
        for (auto& counterIter: wordCounter) {
            const auto& word = counterIter.first;
            auto pos = line.find(word);
            while (pos != TString::npos) {
                ++counterIter.second;
                if (verifyLine) {
                    verifyLine(word, line);
                }
                pos = line.find(word, pos + word.length());
            }
        }
    }
    return programm;
}

void VerifySqlInHints(const TString& query, const THashSet<TString>& expectedHints, TMaybe<bool> ansi) {
    TString pragma;
    if (ansi.Defined()) {
        pragma = *ansi ? "PRAGMA AnsiInForEmptyOrNullableItemsCollections;" :
                         "PRAGMA DisableAnsiInForEmptyOrNullableItemsCollections;";
    }

    NYql::TAstParseResult res = SqlToYql(pragma + query);
    UNIT_ASSERT(res.Root);

    TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
        Y_UNUSED(word);
        if (!ansi.Defined()) {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'('warnNoAnsi)"));
        } else if (*ansi) {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'('ansi)"));
        }
        for (auto& hint : expectedHints)  {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(hint));
        }
    };
    TWordCountHive elementStat = {{TString("SqlIn"), 0}};
    VerifyProgram(res, elementStat, verifyLine);
}

void VerifySqlInHints(const TString& query, const THashSet<TString>& expectedHints) {
    VerifySqlInHints(query, expectedHints, false);
    VerifySqlInHints(query, expectedHints, true);
}

NSQLTranslation::TTranslationSettings GetSettingsWithS3Binding(const TString& name) {
    NSQLTranslation::TTranslationSettings settings;
    NSQLTranslation::TTableBindingSettings bindSettings;
    bindSettings.ClusterType = "s3";
    bindSettings.Settings["cluster"] = "cluster";
    bindSettings.Settings["path"] = "path";
    bindSettings.Settings["format"] = "format";
    bindSettings.Settings["compression"] = "ccompression";
    bindSettings.Settings["bar"] = "1";
    // schema is not validated in this test but should be valid YSON text
    bindSettings.Settings["schema"] = R"__("[
                        "StructType";
                        [
                            [
                                "key";
                                [
                                    "DataType";
                                    "String"
                                ]
                            ];
                            [
                                "subkey";
                                [
                                    "DataType";
                                    "String"
                                ]
                            ];
                            [
                                "value";
                                [
                                    "DataType";
                                    "String"
                                ]
                            ]
    ]])__";
    bindSettings.Settings["partitioned_by"] = "[\"key\", \"subkey\"]";
    settings.PrivateBindings[name] = bindSettings;
    return settings;
}

Y_UNIT_TEST_SUITE(AnsiMode) {
    Y_UNIT_TEST(PragmaAnsi) {
        UNIT_ASSERT(SqlToYql("PRAGMA ANSI 2016;").IsOk());
    }
}

Y_UNIT_TEST_SUITE(SqlParsingOnly) {
    Y_UNIT_TEST(CoverColumnName) {
        UNIT_ASSERT(SqlToYql("SELECT cover FROM plato.Input").IsOk());
    }

    Y_UNIT_TEST(TableHints) {
        UNIT_ASSERT(SqlToYql("SELECT * FROM plato.Input WITH INFER_SCHEMA").IsOk());
        UNIT_ASSERT(SqlToYql("SELECT * FROM plato.Input WITH (INFER_SCHEMA)").IsOk());
    }

    Y_UNIT_TEST(InNoHints) {
        TString query = "SELECT * FROM plato.Input WHERE key IN (1,2,3)";

        VerifySqlInHints(query, { "'('('warnNoAnsi))" }, {});
        VerifySqlInHints(query, { "'()" }, false);
        VerifySqlInHints(query, { "'('('ansi))" }, true);
    }

    Y_UNIT_TEST(InHintCompact) {
        // should parse COMPACT as hint
        TString query = "SELECT * FROM plato.Input WHERE key IN COMPACT(1, 2, 3)";

        VerifySqlInHints(query, { "'('isCompact)" });
    }

    Y_UNIT_TEST(InHintSubquery) {
        // should parse tableSource as hint
        TString query = "$subq = (SELECT key FROM plato.Input); SELECT * FROM plato.Input WHERE key IN $subq";

        VerifySqlInHints(query, { "'('tableSource)" });
    }

    Y_UNIT_TEST(InHintCompactSubquery) {
        TString query = "$subq = (SELECT key FROM plato.Input); SELECT * FROM plato.Input WHERE key IN COMPACT $subq";

        VerifySqlInHints(query, { "'('isCompact)", "'('tableSource)" });
    }

    Y_UNIT_TEST(CompactKeywordNotReservedForNames) {
        UNIT_ASSERT(SqlToYql("SELECT COMPACT FROM plato.Input WHERE COMPACT IN COMPACT(1, 2, 3)").IsOk());
        UNIT_ASSERT(SqlToYql("USE plato; SELECT * FROM COMPACT").IsOk());
    }

    Y_UNIT_TEST(FamilyKeywordNotReservedForNames) {
        // FIXME: check if we can get old behaviour
        //UNIT_ASSERT(SqlToYql("USE plato; CREATE TABLE FAMILY (FAMILY Uint32, PRIMARY KEY (FAMILY));").IsOk());
        //UNIT_ASSERT(SqlToYql("USE plato; SELECT FAMILY FROM FAMILY").IsOk());
        UNIT_ASSERT(SqlToYql("USE plato; SELECT FAMILY FROM Input").IsOk());
    }

    Y_UNIT_TEST(ResetKeywordNotReservedForNames) {
        UNIT_ASSERT(SqlToYql("USE plato; CREATE TABLE RESET (RESET Uint32, PRIMARY KEY (RESET));").IsOk());
        UNIT_ASSERT(SqlToYql("USE plato; SELECT RESET FROM RESET").IsOk());
    }

    Y_UNIT_TEST(SyncKeywordNotReservedForNames) {
        UNIT_ASSERT(SqlToYql("USE plato; CREATE TABLE SYNC (SYNC Uint32, PRIMARY KEY (SYNC));").IsOk());
        UNIT_ASSERT(SqlToYql("USE plato; SELECT SYNC FROM SYNC").IsOk());
    }

    Y_UNIT_TEST(AsyncKeywordNotReservedForNames) {
        UNIT_ASSERT(SqlToYql("USE plato; CREATE TABLE ASYNC (ASYNC Uint32, PRIMARY KEY (ASYNC));").IsOk());
        UNIT_ASSERT(SqlToYql("USE plato; SELECT ASYNC FROM ASYNC").IsOk());
    }

    Y_UNIT_TEST(DisableKeywordNotReservedForNames) {
        UNIT_ASSERT(SqlToYql("USE plato; CREATE TABLE DISABLE (DISABLE Uint32, PRIMARY KEY (DISABLE));").IsOk());
        UNIT_ASSERT(SqlToYql("USE plato; SELECT DISABLE FROM DISABLE").IsOk());
    }

    Y_UNIT_TEST(ChangefeedKeywordNotReservedForNames) {
        UNIT_ASSERT(SqlToYql("USE plato; CREATE TABLE CHANGEFEED (CHANGEFEED Uint32, PRIMARY KEY (CHANGEFEED));").IsOk());
        UNIT_ASSERT(SqlToYql("USE plato; SELECT CHANGEFEED FROM CHANGEFEED").IsOk());
    }

    Y_UNIT_TEST(Jubilee) {
        NYql::TAstParseResult res = SqlToYql("USE plato; INSERT INTO Arcadia (r2000000) VALUES (\"2M GET!!!\");");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(QualifiedAsteriskBefore) {
        NYql::TAstParseResult res = SqlToYql(
            "PRAGMA DisableSimpleColumns;"
            "select interested_table.*, LENGTH(value) AS megahelpful_len  from plato.Input as interested_table;"
        );
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            static bool seenStar = false;
            if (word == "FlattenMembers") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("interested_table."));
            } else if (word == "SqlProjectItem") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("megahelpful_len")));
                UNIT_ASSERT_VALUES_EQUAL(seenStar, true);
            } else if (word == "SqlProjectStarItem") {
                seenStar = true;
            }
        };
        TWordCountHive elementStat = {{TString("FlattenMembers"), 0}, {TString("SqlProjectItem"), 0}, {TString("SqlProjectStarItem"), 0}};
        VerifyProgram(res, elementStat, verifyLine);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["FlattenMembers"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SqlProjectItem"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SqlProjectStarItem"]);
    }

    Y_UNIT_TEST(QualifiedAsteriskAfter) {
        NYql::TAstParseResult res = SqlToYql(
            "PRAGMA DisableSimpleColumns;"
            "select LENGTH(value) AS megahelpful_len, interested_table.*  from plato.Input as interested_table;"
        );
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            static bool seenStar = false;
            if (word == "FlattenMembers") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("interested_table."));
            } else if (word == "SqlProjectItem") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("megahelpful_len")));
                UNIT_ASSERT_VALUES_EQUAL(seenStar, false);
            } else if (word == "SqlProjectStarItem") {
                seenStar = true;
            }
        };
        TWordCountHive elementStat = {{TString("FlattenMembers"), 0}, {TString("SqlProjectItem"), 0}, {TString("SqlProjectStarItem"), 0}};
        VerifyProgram(res, elementStat, verifyLine);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["FlattenMembers"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SqlProjectItem"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SqlProjectStarItem"]);
    }

    Y_UNIT_TEST(QualifiedMembers) {
        NYql::TAstParseResult res = SqlToYql("select interested_table.key, interested_table.value from plato.Input as interested_table;");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            const bool fieldKey = TString::npos != line.find(Quote("key"));
            const bool fieldValue = TString::npos != line.find(Quote("value"));
            const bool refOnTable = TString::npos != line.find("interested_table.");
            if (word == "SqlProjectItem") {
                UNIT_ASSERT(fieldKey || fieldValue);
                UNIT_ASSERT(!refOnTable);
            } else if (word == "Write!") {
                UNIT_ASSERT(fieldKey && fieldValue && !refOnTable);
            }
        };
        TWordCountHive elementStat = {{TString("SqlProjectStarItem"), 0}, {TString("SqlProjectItem"), 0}, {TString("Write!"), 0}};
        VerifyProgram(res, elementStat, verifyLine);
        UNIT_ASSERT_VALUES_EQUAL(0, elementStat["SqlProjectStarItem"]);
        UNIT_ASSERT_VALUES_EQUAL(2, elementStat["SqlProjectItem"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write!"]);
    }

    Y_UNIT_TEST(JoinParseCorrect) {
        NYql::TAstParseResult res = SqlToYql(
            "PRAGMA DisableSimpleColumns;"
            " SELECT table_bb.*, table_aa.key as megakey"
            " FROM plato.Input AS table_aa"
            " JOIN plato.Input AS table_bb"
            " ON table_aa.value == table_bb.value;"
        );
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "SelectMembers") {
                UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("table_aa."));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("table_bb."));
            } else if (word == "SqlProjectItem") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("megakey")));
            } else if (word == "SqlColumn") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("table_aa")));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("key")));
            }
        };
        TWordCountHive elementStat = {{TString("SqlProjectItem"), 0}, {TString("SqlProjectStarItem"), 0}, {TString("SelectMembers"), 0}, {TString("SqlColumn"), 0}};
        VerifyProgram(res, elementStat, verifyLine);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SqlProjectItem"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SqlProjectStarItem"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SelectMembers"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SqlColumn"]);
    }

    Y_UNIT_TEST(Join3Table) {
        NYql::TAstParseResult res = SqlToYql(
            " PRAGMA DisableSimpleColumns;"
            " SELECT table_bb.*, table_aa.key as gigakey, table_cc.* "
            " FROM plato.Input AS table_aa"
            " JOIN plato.Input AS table_bb ON table_aa.key == table_bb.key"
            " JOIN plato.Input AS table_cc ON table_aa.subkey == table_cc.subkey;"
        );
        Err2Str(res);
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "SelectMembers") {
                UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("table_aa."));
                UNIT_ASSERT(line.find("table_bb.") != TString::npos || line.find("table_cc.") != TString::npos);
            } else if (word == "SqlProjectItem") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("gigakey")));
            } else if (word == "SqlColumn") {
                const auto posTableAA = line.find(Quote("table_aa"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, posTableAA);
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("key")));
                UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("table_aa", posTableAA + 3));
            }
        };
        TWordCountHive elementStat = {{TString("SqlProjectItem"), 0}, {TString("SqlProjectStarItem"), 0}, {TString("SelectMembers"), 0}, {TString("SqlColumn"), 0}};
        VerifyProgram(res, elementStat, verifyLine);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SqlProjectItem"]);
        UNIT_ASSERT_VALUES_EQUAL(2, elementStat["SqlProjectStarItem"]);
        UNIT_ASSERT_VALUES_EQUAL(2, elementStat["SelectMembers"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SqlColumn"]);
    }

    Y_UNIT_TEST(JoinWithoutConcreteColumns) {
        NYql::TAstParseResult res = SqlToYql(
            " use plato;"
            " SELECT a.v, b.value"
            "     FROM `Input1` VIEW `ksv` AS a"
            "     JOIN `Input2` AS b"
            "     ON a.k == b.key;"
        );
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "SqlProjectItem") {
                UNIT_ASSERT(line.find(Quote("a.v")) != TString::npos || line.find(Quote("b.value")) != TString::npos);
            } else if (word == "SqlColumn") {
                const auto posTableA = line.find(Quote("a"));
                const auto posTableB = line.find(Quote("b"));
                if (posTableA != TString::npos) {
                    UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("v")));
                } else {
                    UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, posTableB);
                    UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("value")));
                }
            }
        };
        TWordCountHive elementStat = {{TString("SqlProjectStarItem"), 0}, {TString("SqlProjectItem"), 0}, {TString("SqlColumn"), 0}};
        VerifyProgram(res, elementStat, verifyLine);
        UNIT_ASSERT_VALUES_EQUAL(0, elementStat["SqlProjectStarItem"]);
        UNIT_ASSERT_VALUES_EQUAL(2, elementStat["SqlProjectItem"]);
        UNIT_ASSERT_VALUES_EQUAL(2, elementStat["SqlColumn"]);
    }

    Y_UNIT_TEST(JoinWithSameValues) {
        NYql::TAstParseResult res = SqlToYql("SELECT a.value, b.value FROM plato.Input AS a JOIN plato.Input as b ON a.key == b.key;");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "SqlProjectItem") {
                const bool isValueFromA = TString::npos != line.find(Quote("a.value"));
                const bool isValueFromB = TString::npos != line.find(Quote("b.value"));
                UNIT_ASSERT(isValueFromA || isValueFromB);
            } if (word == "Write!") {
                UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("a.a."));
                UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("b.b."));
            }
        };
        TWordCountHive elementStat = {{TString("SqlProjectStarItem"), 0}, {TString("SqlProjectItem"), 0}, {"Write!", 0}};
        VerifyProgram(res, elementStat, verifyLine);
        UNIT_ASSERT_VALUES_EQUAL(0, elementStat["SqlProjectStarItem"]);
        UNIT_ASSERT_VALUES_EQUAL(2, elementStat["SqlProjectItem"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write!"]);
    }

    Y_UNIT_TEST(SameColumnsForDifferentTables) {
        NYql::TAstParseResult res = SqlToYql("SELECT a.key, b.key FROM plato.Input as a JOIN plato.Input as b on a.key==b.key;");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(SameColumnsForDifferentTablesFullJoin) {
        NYql::TAstParseResult res = SqlToYql("SELECT a.key, b.key, a.value, b.value FROM plato.Input AS a FULL JOIN plato.Input AS b USING(key);");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(ReverseLabels) {
        NYql::TAstParseResult res = SqlToYql("select in.key as subkey, subkey as key from plato.Input as in;");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(AutogenerationAliasWithoutCollisionConflict1) {
        NYql::TAstParseResult res = SqlToYql("select LENGTH(Value), key as column1 from plato.Input;");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(AutogenerationAliasWithoutCollision2Conflict2) {
        NYql::TAstParseResult res = SqlToYql("select key as column0, LENGTH(Value) from plato.Input;");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(InputAliasForQualifiedAsterisk) {
        NYql::TAstParseResult res = SqlToYql("use plato; select zyuzya.*, key from plato.Input as zyuzya;");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(SelectSupportsResultColumnsWithTrailingComma) {
        NYql::TAstParseResult res = SqlToYql("select a, b, c, from plato.Input;");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(SelectOrderByLabeledColumn) {
        NYql::TAstParseResult res = SqlToYql("pragma DisableOrderedColumns; select key as goal from plato.Input order by goal");
        UNIT_ASSERT(res.Root);
        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "DataSource") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("plato"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("Input"));

                UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("goal"));
            } else if (word == "Sort") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("goal"));

                UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("key"));
            }
        };
        TWordCountHive elementStat = {{TString("DataSource"), 0}, {TString("Sort"), 0}};
        VerifyProgram(res, elementStat, verifyLine);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["DataSource"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Sort"]);
    }

    Y_UNIT_TEST(SelectOrderBySimpleExpr) {
        NYql::TAstParseResult res = SqlToYql("select a from plato.Input order by a + a");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(SelectOrderByDuplicateLabels) {
        NYql::TAstParseResult res = SqlToYql("select a from plato.Input order by a, a");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(SelectOrderByExpression) {
        NYql::TAstParseResult res = SqlToYql("select * from plato.Input as i order by cast(key as uint32) + cast(subkey as uint32)");
        UNIT_ASSERT(res.Root);
        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Sort") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("\"+\""));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("key"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("subkey"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("(Bool 'true)"));

                UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("i.key"));
                UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("i.subkey"));
            }
        };
        TWordCountHive elementStat = {{TString("Sort"), 0}};
        VerifyProgram(res, elementStat, verifyLine);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Sort"]);
    }

    Y_UNIT_TEST(SelectOrderByExpressionDesc) {
        NYql::TAstParseResult res = SqlToYql("pragma disablesimplecolumns; select i.*, key, subkey from plato.Input as i order by cast(i.key as uint32) - cast(i.subkey as uint32) desc");
        UNIT_ASSERT(res.Root);
        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Sort") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("\"-\""));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("\"key\""));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("\"subkey\""));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("(Bool 'false)"));
            } else if (word == "Write!") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'columns"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("\"key\""));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("\"subkey\""));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("prefix"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("\"i.\""));
            }
        };
        TWordCountHive elementStat = {{TString("Sort"), 0}, {TString("Write!"), 0}};
        VerifyProgram(res, elementStat, verifyLine);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Sort"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write!"]);
    }

    Y_UNIT_TEST(SelectOrderByExpressionAsc) {
        NYql::TAstParseResult res = SqlToYql("select i.key, i.subkey from plato.Input as i order by cast(key as uint32) % cast(i.subkey as uint32) asc");
        UNIT_ASSERT(res.Root);
        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Sort") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("\"%\""));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("\"key\""));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("\"subkey\""));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("(Bool 'true)"));
            } else if (word == "Write!") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'columns"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("\"key\""));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("\"subkey\""));
                UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("i."));
            }
        };
        TWordCountHive elementStat = {{TString("Sort"), 0}, {TString("Write!"), 0}};
        VerifyProgram(res, elementStat, verifyLine);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Sort"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write!"]);
    }

    Y_UNIT_TEST(ReferenceToKeyInSubselect) {
        NYql::TAstParseResult res = SqlToYql("select b.key from (select a.key from plato.Input as a) as b;");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(OrderByCastValue) {
        NYql::TAstParseResult res = SqlToYql("select i.key, i.subkey from plato.Input as i order by cast(key as uint32) desc;");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(GroupByCastValue) {
        NYql::TAstParseResult res = SqlToYql("select count(1) from plato.Input as i group by cast(key as uint8);");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(KeywordInSelectColumns) {
        NYql::TAstParseResult res = SqlToYql("select in, s.check from (select 1 as in, \"test\" as check) as s;");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(SelectAllGroupBy) {
        NYql::TAstParseResult res = SqlToYql("select * from plato.Input group by subkey;");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(PrimaryKeyParseCorrect) {
        NYql::TAstParseResult res = SqlToYql("USE plato; CREATE TABLE tableName (Key Uint32, Subkey Int64, Value String, PRIMARY KEY (Key, Subkey));");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("\"Key\""));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("\"Subkey\""));
            }
        };

        TWordCountHive elementStat = {{TString("Write"), 0}, {TString("primarykey"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["primarykey"]);
    }

    Y_UNIT_TEST(DeleteFromTableByKey) {
        NYql::TAstParseResult res = SqlToYql("delete from plato.Input where key = 200;", 10, "kikimr");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("('mode 'delete)"));
            }
        };

        TWordCountHive elementStat = {{TString("Write"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(DeleteFromTable) {
        NYql::TAstParseResult res = SqlToYql("delete from plato.Input;", 10, "kikimr");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("('mode 'delete)"));
            }
        };

        TWordCountHive elementStat = {{TString("Write"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(DeleteFromTableOnValues) {
        NYql::TAstParseResult res = SqlToYql("delete from plato.Input on (key) values (1);",
            10, "kikimr");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("('mode 'delete_on)"));
            }
        };

        TWordCountHive elementStat = {{TString("Write"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(DeleteFromTableOnSelect) {
        NYql::TAstParseResult res = SqlToYql(
            "delete from plato.Input on select key from plato.Input where value > 0;", 10, "kikimr");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("('mode 'delete_on)"));
            }
        };

        TWordCountHive elementStat = {{TString("Write"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(UpdateByValues) {
        NYql::TAstParseResult res = SqlToYql("update plato.Input set key = 777, value = 'cool' where key = 200;", 10, "kikimr");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("('mode 'update)"));
            } else if (word == "AsStruct") {
                const bool isKey = line.find("key") != TString::npos;
                const bool isValue = line.find("value") != TString::npos;
                UNIT_ASSERT(isKey || isValue);
                if (isKey) {
                    UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("777")));
                } else if (isValue) {
                    UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("cool")));
                }
            }
        };

        TWordCountHive elementStat = {{TString("Write"), 0}, {TString("AsStruct"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["AsStruct"]);
    }

    Y_UNIT_TEST(UpdateByMultiValues) {
        NYql::TAstParseResult res = SqlToYql("update plato.Input set (key, value, subkey) = ('2','ddd',':') where key = 200;", 10, "kikimr");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("('mode 'update)"));
            } else if (word == "AsStruct") {
                const bool isKey = line.find("key") != TString::npos;
                const bool isSubkey = line.find("subkey") != TString::npos;
                const bool isValue = line.find("value") != TString::npos;
                UNIT_ASSERT(isKey || isSubkey || isValue);
                if (isKey && !isSubkey) {
                    UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("2")));
                } else if (isSubkey) {
                    UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote(":")));
                } else if (isValue) {
                    UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("ddd")));
                }
            }
        };

        TWordCountHive elementStat = {{TString("Write"), 0}, {TString("AsStruct"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["AsStruct"]);
    }

    Y_UNIT_TEST(UpdateBySelect) {
        NYql::TAstParseResult res = SqlToYql("update plato.Input set (key, value, subkey) = (select key, value, subkey from plato.Input where key = 911) where key = 200;", 10, "kikimr");
        UNIT_ASSERT(res.Root);

        int lineIndex = 0;
        int writeLineIndex = -1;
        bool found = false;

        TVerifyLineFunc verifyLine = [&lineIndex, &writeLineIndex, &found](const TString& word, const TString& line) {
            if (word == "Write") {
                writeLineIndex = lineIndex;
                found = line.find("('mode 'update)") != TString::npos;
            } else if (word == "mode") {
                found |= lineIndex == writeLineIndex + 1 && line.find("('mode 'update)") != TString::npos;
                UNIT_ASSERT(found);
            }

            ++lineIndex;
        };

        TWordCountHive elementStat = {{TString("Write"), 0}, {TString("mode"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(UpdateSelfModifyAll) {
        NYql::TAstParseResult res = SqlToYql("update plato.Input set subkey = subkey + 's';", 10, "kikimr");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("('mode 'update)"));
            } else if (word == "AsStruct") {
                const bool isSubkey = line.find("subkey") != TString::npos;
                UNIT_ASSERT(isSubkey);
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("subkey")));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("s")));
            }
        };

        TWordCountHive elementStat = {{TString("Write"), 0}, {TString("AsStruct"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["AsStruct"]);
    }

    Y_UNIT_TEST(UpdateOnValues) {
        NYql::TAstParseResult res = SqlToYql("update plato.Input on (key, value) values (5, 'cool')", 10, "kikimr");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("('mode 'update_on)"));
            }
        };

        TWordCountHive elementStat = {{TString("Write"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(UpdateOnSelect) {
        NYql::TAstParseResult res = SqlToYql(
            "update plato.Input on select key, value + 1 as value from plato.Input", 10, "kikimr");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("('mode 'update_on)"));
            }
        };

        TWordCountHive elementStat = {{TString("Write"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(UnionAllTest) {
        NYql::TAstParseResult res = SqlToYql("SELECT key FROM plato.Input UNION ALL select subkey FROM plato.Input;");
        UNIT_ASSERT(res.Root);

        TWordCountHive elementStat = {{TString("UnionAll"), 0}};
        VerifyProgram(res, elementStat, {});
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["UnionAll"]);
    }

    Y_UNIT_TEST(DeclareDecimalParameter) {
        NYql::TAstParseResult res = SqlToYql("declare $value as Decimal(22,9); select $value as cnt;");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(SimpleGroupBy) {
        NYql::TAstParseResult res = SqlToYql("select count(1),z from plato.Input group by key as z order by z;");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(EmptyColumnName0) {
        /// Now it's parsed well and error occur on validate step like "4:31:Empty struct member name is not allowed" in "4:31:Function: AddMember"
        NYql::TAstParseResult res = SqlToYql("insert into plato.Output (``, list1) values (0, AsList(0, 1, 2));");
        /// Verify that parsed well without crash
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(KikimrRollback) {
        NYql::TAstParseResult res = SqlToYql("use plato; select * from Input; rollback;", 10, "kikimr");
        UNIT_ASSERT(res.Root);

        TWordCountHive elementStat = {{TString("rollback"), 0}};
        VerifyProgram(res, elementStat);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["rollback"]);
    }

    Y_UNIT_TEST(PragmaFile) {
        NYql::TAstParseResult res = SqlToYql(R"(pragma file("HW", "sbr:181041334");)");
        UNIT_ASSERT(res.Root);

        TWordCountHive elementStat = {{TString(R"((let world (Configure! world (DataSource '"config") '"AddFileByUrl" '"HW" '"sbr:181041334")))"), 0}};
        VerifyProgram(res, elementStat);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat.cbegin()->second);
    }

    Y_UNIT_TEST(DoNotCrashOnNamedInFilter) {
        NYql::TAstParseResult res = SqlToYql("USE plato; $all = ($table_name) -> { return true; }; SELECT * FROM FILTER(Input, $all)");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(PragmasFileAndUdfOrder) {
        NYql::TAstParseResult res = SqlToYql(R"(
            PRAGMA file("libvideoplayers_udf.so", "https://proxy.sandbox.yandex-team.ru/235185290");
            PRAGMA udf("libvideoplayers_udf.so");
        )");
        UNIT_ASSERT(res.Root);

        const auto programm = GetPrettyPrint(res);
        const auto file = programm.find("AddFileByUrl");
        const auto udfs = programm.find("ImportUdfs");
        UNIT_ASSERT(file < udfs);
    }

    Y_UNIT_TEST(ProcessUserType) {
        NYql::TAstParseResult res = SqlToYql("process plato.Input using Kikimr::PushData(TableRows());", 1, TString(NYql::KikimrProviderName));
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Kikimr.PushData") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("TupleType"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("TypeOf"));
            }
        };

        TWordCountHive elementStat = {{TString("Kikimr.PushData"), 0}};
        VerifyProgram(res, elementStat, verifyLine);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Kikimr.PushData"]);
    }

    Y_UNIT_TEST(ProcessUserTypeAuth) {
        NYql::TAstParseResult res = SqlToYql("process plato.Input using YDB::PushData(TableRows(), AsTuple('oauth', SecureParam('api:oauth')));", 1, TString(NYql::KikimrProviderName));
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "YDB.PushData") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("TupleType"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("TypeOf"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("api:oauth"));
            }
        };

        TWordCountHive elementStat = {{TString("YDB.PushData"), 0}};
        VerifyProgram(res, elementStat, verifyLine);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["YDB.PushData"]);
    }

    Y_UNIT_TEST(SelectStreamRtmr) {
        NYql::TAstParseResult res = SqlToYql(
            "USE plato; INSERT INTO Output SELECT STREAM key FROM Input;",
            10, TString(NYql::RtmrProviderName));
        UNIT_ASSERT(res.Root);

        res = SqlToYql(
            "USE plato; INSERT INTO Output SELECT key FROM Input;",
            10, TString(NYql::RtmrProviderName));
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(SelectStreamRtmrJoinWithYt) {
        NYql::TAstParseResult res = SqlToYql(
            "USE plato; INSERT INTO Output SELECT STREAM key FROM Input LEFT JOIN hahn.ttt as t ON Input.key = t.Name;",
            10, TString(NYql::RtmrProviderName));
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(SelectStreamNonRtmr) {
        NYql::TAstParseResult res = SqlToYql(
            "USE plato; INSERT INTO Output SELECT STREAM key FROM Input;",
            10);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:31: Error: SELECT STREAM is unsupported for non-streaming sources\n");
    }

    Y_UNIT_TEST(GroupByHopRtmr) {
        NYql::TAstParseResult res = SqlToYql(R"(
            USE plato; INSERT INTO Output SELECT key, SUM(value) AS value FROM Input
            GROUP BY key, HOP(subkey, "PT10S", "PT30S", "PT20S");
        )", 10, TString(NYql::RtmrProviderName));
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(GroupByHopRtmrSubquery) {
        // 'use plato' intentially avoided
        NYql::TAstParseResult res = SqlToYql(R"(
            SELECT COUNT(*) AS value FROM (SELECT * FROM plato.Input)
            GROUP BY HOP(Data, "PT10S", "PT30S", "PT20S")
        )", 10, TString(NYql::RtmrProviderName));
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(GroupByHopRtmrSubqueryBinding) {
        NYql::TAstParseResult res = SqlToYql(R"(
            USE plato;
            $q = SELECT * FROM Input;
            INSERT INTO Output SELECT STREAM * FROM (
                SELECT COUNT(*) AS value FROM $q
                GROUP BY HOP(Data, "PT10S", "PT30S", "PT20S")
            );
        )", 10, TString(NYql::RtmrProviderName));
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(GroupByNoHopRtmr) {
        NYql::TAstParseResult res = SqlToYql(R"(
            USE plato; INSERT INTO Output SELECT STREAM key, SUM(value) AS value FROM Input
            GROUP BY key;
        )", 10, TString(NYql::RtmrProviderName));
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:22: Error: Streaming group by query must have a hopping window specification.\n");
    }

    Y_UNIT_TEST(KikimrInserts) {
         NYql::TAstParseResult res = SqlToYql(R"(
            USE plato;
            INSERT INTO Output SELECT key, value FROM Input;
            INSERT OR ABORT INTO Output SELECT key, value FROM Input;
            INSERT OR IGNORE INTO Output SELECT key, value FROM Input;
            INSERT OR REVERT INTO Output SELECT key, value FROM Input;
        )", 10, TString(NYql::KikimrProviderName));
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(WarnMissingIsBeforeNotNull) {
        NYql::TAstParseResult res = SqlToYql("select 1 NOT NULL");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Warning: Missing IS keyword before NOT NULL, code: 4507\n");
    }

    Y_UNIT_TEST(Subqueries) {
        NYql::TAstParseResult res = SqlToYql(R"(
            USE plato;
            $sq1 = (SELECT * FROM plato.Input);

            $sq2 = SELECT * FROM plato.Input;

            $squ1 = (
                SELECT * FROM plato.Input
                UNION ALL
                SELECT * FROM plato.Input
            );

            $squ2 =
                SELECT * FROM plato.Input
                UNION ALL
                SELECT * FROM plato.Input;

            $squ3 = (
                (SELECT * FROM plato.Input)
                UNION ALL
                (SELECT * FROM plato.Input)
            );

            SELECT * FROM $sq1;
            SELECT * FROM $sq2;
            SELECT * FROM $squ1;
            SELECT * FROM $squ2;
            SELECT * FROM $squ3;
        )");

        Cerr << Err2Str(res) << Endl;
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(SubqueriesJoin) {
        NYql::TAstParseResult res = SqlToYql(R"(
            USE plato;

            $left = SELECT * FROM plato.Input1 WHERE value != "BadValue";
            $right = SELECT * FROM plato.Input2;

            SELECT * FROM $left AS l
            JOIN $right AS r
            ON l.key == r.key;
        )");

        Cerr << Err2Str(res) << Endl;
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(AnyInBackticksAsTableName) {
        NYql::TAstParseResult res = SqlToYql("use plato; select * from `any`;");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(AnyJoinForTableAndSubQuery) {
        NYql::TAstParseResult res = SqlToYql(R"(
            USE plato;

            $r = SELECT * FROM plato.Input2;

            SELECT * FROM ANY plato.Input1 AS l
            LEFT JOIN ANY $r AS r
            USING (key);
        )");

        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "EquiJoin") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'('left 'any)"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'('right 'any)"));
            }
        };

        TWordCountHive elementStat = {{TString("left"), 0}, {TString("right"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["left"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["right"]);
    }

    Y_UNIT_TEST(AnyJoinForTableAndTableSource) {
        NYql::TAstParseResult res = SqlToYql(R"(
            USE plato;

            $r = AsList(
                AsStruct("aaa" as key, "bbb" as subkey, "ccc" as value)
            );

            SELECT * FROM ANY plato.Input1 AS l
            LEFT JOIN ANY AS_TABLE($r) AS r
            USING (key);
        )");

        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "EquiJoin") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'('left 'any)"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'('right 'any)"));
            }
        };

        TWordCountHive elementStat = {{TString("left"), 0}, {TString("right"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["left"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["right"]);
    }

    Y_UNIT_TEST(AnyJoinNested) {
        NYql::TAstParseResult res = SqlToYql(R"(
            USE plato;

            FROM ANY Input1 as a
                JOIN Input2 as b ON a.key = b.key
                LEFT JOIN ANY Input3 as c ON a.key = c.key
                RIGHT JOIN ANY Input4 as d ON d.key = b.key
                CROSS JOIN Input5
            SELECT *;
        )");

        UNIT_ASSERT(res.Root);

        TWordCountHive elementStat = {{TString("left"), 0}, {TString("right"), 0}};
        VerifyProgram(res, elementStat);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["left"]);
        UNIT_ASSERT_VALUES_EQUAL(2, elementStat["right"]);
    }

    Y_UNIT_TEST(InlineAction) {
        NYql::TAstParseResult res = SqlToYql(
            "do begin\n"
            "  select 1\n"
            "; end do\n");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "");
    }

    Y_UNIT_TEST(FlattenByCorrelationName) {
        UNIT_ASSERT(SqlToYql("select * from plato.Input as t flatten by t.x").IsOk());
        UNIT_ASSERT(SqlToYql("select * from plato.Input as t flatten by t -- same as flatten by t.t").IsOk());
    }

    Y_UNIT_TEST(DiscoveryMode) {
        UNIT_ASSERT(SqlToYqlWithMode("insert into plato.Output select * from plato.Input", NSQLTranslation::ESqlMode::DISCOVERY).IsOk());
        UNIT_ASSERT(SqlToYqlWithMode("select * from plato.concat(Input1, Input2)", NSQLTranslation::ESqlMode::DISCOVERY).IsOk());
        UNIT_ASSERT(SqlToYqlWithMode("select * from plato.each(AsList(\"Input1\", \"Input2\"))", NSQLTranslation::ESqlMode::DISCOVERY).IsOk());
    }

    Y_UNIT_TEST(CubeWithAutoGeneratedLikeColumnName) {
        UNIT_ASSERT(SqlToYql("select key,subkey,group from plato.Input group by cube(key,subkey,group)").IsOk());
    }

    Y_UNIT_TEST(CubeWithAutoGeneratedLikeAlias) {
        UNIT_ASSERT(SqlToYql("select key,subkey,group from plato.Input group by cube(key,subkey,value as group)").IsOk());
    }

    Y_UNIT_TEST(FilterCanBeUsedAsColumnIdOrBind) {
        UNIT_ASSERT(SqlToYql("select filter from plato.Input").IsOk());
        UNIT_ASSERT(SqlToYql("select 1 as filter").IsOk());
        UNIT_ASSERT(SqlToYql("$filter = 1; select $filter").IsOk());
    }

    Y_UNIT_TEST(DuplicateSemicolonsAreAllowedBetweenTopLevelStatements) {
        UNIT_ASSERT(SqlToYql(";;select 1; ; select 2;/*comment*/;select 3;;--comment\n;select 4;;").IsOk());
    }

    Y_UNIT_TEST(DuplicateAndMissingTrailingSemicolonsAreAllowedBetweenActionStatements) {
        TString req =
            "define action $action($b,$c) as\n"
            "    ;;$d = $b + $c;\n"
            "    select $b;\n"
            "    select $c;;\n"
            "    select $d,\n"
            "end define;\n"
            "\n"
            "do $action(1,2);";
        UNIT_ASSERT(SqlToYql(req).IsOk());
    }

    Y_UNIT_TEST(DuplicateAndMissingTrailingSemicolonsAreAllowedBetweenInlineActionStatements) {
        TString req =
            "do begin\n"
            "    ;select 1,\n"
            "end do;\n"
            "evaluate for $i in AsList(1,2,3) do begin\n"
            "    select $i;;\n"
            "    select $i + $i;;\n"
            "end do;";
        UNIT_ASSERT(SqlToYql(req).IsOk());
    }

    Y_UNIT_TEST(DuplicateSemicolonsAreAllowedBetweenLambdaStatements) {
        TString req =
            "$x=1;\n"
            "$foo = ($a, $b)->{\n"
            "   ;;$v = $a + $b;\n"
            "   $bar = ($c) -> {; return $c << $x};;\n"
            "   return $bar($v);;\n"
            "};\n"
            "select $foo(1,2);";
        UNIT_ASSERT(SqlToYql(req).IsOk());
    }

    Y_UNIT_TEST(StringLiteralWithEscapedBackslash) {
        NYql::TAstParseResult res1 = SqlToYql(R"foo(SELECT 'a\\';)foo");
        NYql::TAstParseResult res2 = SqlToYql(R"foo(SELECT "a\\";)foo");
        UNIT_ASSERT(res1.Root);
        UNIT_ASSERT(res2.Root);

        TWordCountHive elementStat = {{TString("a\\"), 0}};

        VerifyProgram(res1, elementStat);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["a\\"]);

        VerifyProgram(res2, elementStat);
        UNIT_ASSERT_VALUES_EQUAL(2, elementStat["a\\"]);
    }

    Y_UNIT_TEST(StringMultiLineLiteralWithEscapes) {
        UNIT_ASSERT(SqlToYql("SELECT @@@foo@@@@bar@@@").IsOk());
        UNIT_ASSERT(SqlToYql("SELECT @@@@@@@@@").IsOk());
    }

    Y_UNIT_TEST(StringMultiLineLiteralConsequitiveAt) {
        UNIT_ASSERT(!SqlToYql("SELECT @").IsOk());
        UNIT_ASSERT(!SqlToYql("SELECT @@").IsOk());
        UNIT_ASSERT(!SqlToYql("SELECT @@@").IsOk());
        UNIT_ASSERT( SqlToYql("SELECT @@@@").IsOk());
        UNIT_ASSERT( SqlToYql("SELECT @@@@@").IsOk());

        UNIT_ASSERT(!SqlToYql("SELECT @@@@@@").IsOk());
        UNIT_ASSERT(!SqlToYql("SELECT @@@@@@@").IsOk());

        UNIT_ASSERT( SqlToYql("SELECT @@@@@@@@").IsOk());
        UNIT_ASSERT( SqlToYql("SELECT @@@@@@@@@").IsOk());
        UNIT_ASSERT(!SqlToYql("SELECT @@@@@@@@@@").IsOk());
    }

    Y_UNIT_TEST(ConstnessForListDictSetCreate) {
        auto req = "$foo = ($x, $y) -> (\"aaaa\");\n"
                   "\n"
                   "select\n"
                   "    $foo(sum(key), ListCreate(String)),\n"
                   "    $foo(sum(key), DictCreate(String, String)),\n"
                   "    $foo(sum(key), SetCreate(String)),\n"
                   "from (select 1 as key);";
        UNIT_ASSERT(SqlToYql(req).IsOk());
    }

    Y_UNIT_TEST(CanUseEmptyTupleInWindowPartitionBy) {
        auto req = "select sum(key) over w\n"
                   "from plato.Input\n"
                   "window w as (partition compact by (), (subkey), (), value || value as dvalue);";
        UNIT_ASSERT(SqlToYql(req).IsOk());
    }

    Y_UNIT_TEST(NoWarnUnionAllWithOrderByWithExplicitLegacyMode) {
        auto req = "pragma DisableAnsiOrderByLimitInUnionAll;\n"
                   "use plato;\n"
                   "\n"
                   "select * from Input order by key limit 10\n"
                   "union all\n"
                   "select * from Input order by key limit 1;";

        auto res = SqlToYql(req);
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Warning: Use of deprecated DisableAnsiOrderByLimitInUnionAll pragma. It will be dropped soon, code: 4518\n");
    }

    Y_UNIT_TEST(WarnUnionAllWithDiscardIntoResultWithExplicitLegacyMode) {
        auto req = "use plato;\n"
                   "pragma DisableAnsiOrderByLimitInUnionAll;\n"
                   "\n"
                   "select * from Input into result aaa\n"
                   "union all\n"
                   "discard select * from Input;";

        auto res = SqlToYql(req);
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:8: Warning: Use of deprecated DisableAnsiOrderByLimitInUnionAll pragma. It will be dropped soon, code: 4518\n"
                                          "<main>:4:21: Warning: INTO RESULT will be ignored here. Please use INTO RESULT after last subquery in UNION ALL if you want label entire UNION ALL result, code: 4522\n"
                                          "<main>:6:1: Warning: DISCARD will be ignored here. Please use DISCARD before first subquery in UNION ALL if you want to discard entire UNION ALL result, code: 4522\n");
    }

    Y_UNIT_TEST(WarnUnionAllWithIgnoredOrderByLegacyMode) {
        auto req = "use plato;\n"
                   "pragma DisableAnsiOrderByLimitInUnionAll;\n"
                   "\n"
                   "SELECT * FROM (\n"
                   "  SELECT * FROM Input\n"
                   "  UNION ALL\n"
                   "  SELECT t.* FROM Input AS t ORDER BY t.key\n"
                   ");";
        auto res = SqlToYql(req);
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:8: Warning: Use of deprecated DisableAnsiOrderByLimitInUnionAll pragma. It will be dropped soon, code: 4518\n"
                                          "<main>:7:3: Warning: ORDER BY without LIMIT in subquery will be ignored, code: 4504\n");
    }

    Y_UNIT_TEST(ReduceUsingUdfWithShortcutsWorks) {
        auto req = "use plato;\n"
                   "\n"
                   "$arg = 'foo';\n"
                   "$func = XXX::YYY($arg);\n"
                   "\n"
                   "REDUCE Input ON key using $func(subkey);\n"
                   "REDUCE Input ON key using $func(UUU::VVV(TableRow()));\n";
        UNIT_ASSERT(SqlToYql(req).IsOk());
        req = "use plato;\n"
              "\n"
              "$arg = 'foo';\n"
              "$func = XXX::YYY($arg);\n"
              "\n"
              "REDUCE Input ON key using all $func(subkey);\n"
              "REDUCE Input ON key using all $func(UUU::VVV(TableRow()));";
        UNIT_ASSERT(SqlToYql(req).IsOk());
    }

    Y_UNIT_TEST(YsonDisableStrict) {
        UNIT_ASSERT(SqlToYql("pragma yson.DisableStrict = \"false\";").IsOk());
        UNIT_ASSERT(SqlToYql("pragma yson.DisableStrict;").IsOk());
    }

    Y_UNIT_TEST(YsonStrict) {
        UNIT_ASSERT(SqlToYql("pragma yson.Strict = \"false\";").IsOk());
        UNIT_ASSERT(SqlToYql("pragma yson.Strict;").IsOk());
    }

    Y_UNIT_TEST(JoinByTuple) {
        auto req = "use plato;\n"
                   "\n"
                   "select * from T1 as a\n"
                   "join T2 as b\n"
                   "on AsTuple(a.key, a.subkey) = AsTuple(b.key, b.subkey);";
        UNIT_ASSERT(SqlToYql(req).IsOk());
    }

    Y_UNIT_TEST(JoinByStruct) {
        auto req = "use plato;\n"
                   "\n"
                   "select * from T1 as a\n"
                   "join T2 as b\n"
                   "on AsStruct(a.key as k, a.subkey as sk) = AsStruct(b.key as k, b.subkey as sk);";
        UNIT_ASSERT(SqlToYql(req).IsOk());
    }

    Y_UNIT_TEST(JoinByUdf) {
        auto req = "use plato;\n"
                   "\n"
                   "select a.align\n"
                   "from T1 as a\n"
                   "join T2 as b\n"
                   "on Yson::SerializeJsonEncodeUtf8(a.align)=b.align;";
        UNIT_ASSERT(SqlToYql(req).IsOk());
    }

    Y_UNIT_TEST(EscapedIdentifierAsLambdaArg) {
        auto req = "$f = ($`foo bar`, $x) -> { return $`foo bar` + $x; };\n"
                   "\n"
                   "select $f(1, 2);";
        auto res = SqlToYql(req);
        UNIT_ASSERT(res.Root);
        const auto programm = GetPrettyPrint(res);
        auto expected = "(Apply (lambda '(\"$foo bar\" \"$x\")";
        UNIT_ASSERT(programm.find(expected) != TString::npos);
    }

    Y_UNIT_TEST(CompactionPolicyParseCorrect) {
        NYql::TAstParseResult res = SqlToYql(
            R"( USE plato;
                CREATE TABLE tableName (Key Uint32, Value String, PRIMARY KEY (Key))
                WITH ( COMPACTION_POLICY = "SomeCompactionPreset" );)"
        );
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("compactionPolicy"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("SomeCompactionPreset"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(AutoPartitioningBySizeParseCorrect) {
        NYql::TAstParseResult res = SqlToYql(
            R"( USE plato;
                CREATE TABLE tableName (Key Uint32, Value String, PRIMARY KEY (Key))
                WITH ( AUTO_PARTITIONING_BY_SIZE = ENABLED );)"
        );
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("autoPartitioningBySize"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("ENABLED"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(UniformPartitionsParseCorrect) {
        NYql::TAstParseResult res = SqlToYql(
            R"( USE plato;
                CREATE TABLE tableName (Key Uint32, Value String, PRIMARY KEY (Key))
                WITH ( UNIFORM_PARTITIONS = 16 );)"
        );
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("uniformPartitions"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("16"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(TtlParseCorrect) {
        NYql::TAstParseResult res = SqlToYql(
            R"( USE plato;
                CREATE TABLE tableName (Key Uint32, CreatedAt Timestamp, PRIMARY KEY (Key))
                WITH ( TTL = Interval("P1D") On CreatedAt);)"
        );
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("setTtlSettings"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("expireAfter"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("86400000"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(ChangefeedParseCorrect) {
        auto res = SqlToYql(R"( USE plato;
            CREATE TABLE tableName (
                Key Uint32, PRIMARY KEY (Key),
                CHANGEFEED feedName WITH (MODE = 'KEYS_ONLY', FORMAT = 'json')
            );
        )");
        UNIT_ASSERT_C(res.Root, Err2Str(res));

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Write") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("changefeed"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("mode"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("KEYS_ONLY"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("format"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("json"));
            }
        };

        TWordCountHive elementStat = { {TString("Write"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
    }

    Y_UNIT_TEST(CloneForAsTableWorksWithCube) {
        UNIT_ASSERT(SqlToYql("SELECT * FROM AS_TABLE([<|k1:1, k2:1|>]) GROUP BY CUBE(k1, k2);").IsOk());
    }

    Y_UNIT_TEST(WindowPartitionByColumnProperlyEscaped) {
        NYql::TAstParseResult res = SqlToYql("SELECT SUM(key) OVER w FROM plato.Input WINDOW w AS (PARTITION BY `column with space`);");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "CalcOverWindow") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("\"column with space\""));
            }
        };

        TWordCountHive elementStat = { {TString("CalcOverWindow"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["CalcOverWindow"]);
    }

    Y_UNIT_TEST(WindowPartitionByExpressionWithoutAliasesAreAllowed) {
        NYql::TAstParseResult res = SqlToYql("SELECT SUM(key) OVER w FROM plato.Input as i WINDOW w AS (PARTITION BY ii.subkey);");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "AddMember") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("AddMember row 'group_w_0 (SqlAccess 'struct (Member row '\"ii\")"));
            }
            if (word == "CalcOverWindow") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("CalcOverWindow core '('\"group_w_0\")"));
            }
        };

        TWordCountHive elementStat = { {TString("CalcOverWindow"), 0}, {TString("AddMember"), 0} };
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["CalcOverWindow"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["AddMember"]);
    }

    Y_UNIT_TEST(PqReadByAfterUse) {
        ExpectFailWithError("use plato; pragma PqReadBy='plato2';",
            "<main>:1:28: Error: Cluster in PqReadPqBy pragma differs from cluster specified in USE statement: plato2 != plato\n");

        UNIT_ASSERT(SqlToYql("pragma PqReadBy='plato2';").IsOk());
        UNIT_ASSERT(SqlToYql("pragma PqReadBy='plato2'; use plato;").IsOk());
        UNIT_ASSERT(SqlToYql("$x='plato'; use rtmr:$x; pragma PqReadBy='plato2';").IsOk());
        UNIT_ASSERT(SqlToYql("use plato; pragma PqReadBy='dq';").IsOk());
    }

    Y_UNIT_TEST(MrObject) {
        NYql::TAstParseResult res = SqlToYql(
            "declare $path as String;\n"
            "select * from plato.object($path, `format`, \"comp\" || \"ression\" as compression, 1 as bar) with schema (Int32 as y, String as x)"
        );
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "MrObject") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos,
                    line.find(R"__((MrObject (EvaluateAtom "$path") '"format" '('('"compression" (Concat (String '"comp") (String '"ression"))) '('"bar" (Int32 '"1")))))__"));
            } else if (word == "userschema") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos,
                    line.find(R"__('('('"userschema" (StructType '('"y" (DataType 'Int32)) '('"x" (DataType 'String))) '('"y" '"x"))))__"));
            }
        };

        TWordCountHive elementStat = {{TString("MrObject"), 0}, {TString("userschema"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["MrObject"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["userschema"]);
    }

    Y_UNIT_TEST(TableBindings) {
        NSQLTranslation::TTranslationSettings settings = GetSettingsWithS3Binding("foo");
        NYql::TAstParseResult res = SqlToYqlWithSettings(
            "select * from bindings.foo",
            settings
        );
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "MrObject") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos,
                    line.find(R"__((MrObject '"path" '"format" '('('"bar" (String '"1")) '('"compression" (String '"ccompression")))))__"));
            } else if (word == "userschema") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos,
                    line.find(R"__('('('"partitionedby" '"key" '"subkey") '('"userschema" (SqlTypeFromYson)__"));
            }
        };

        TWordCountHive elementStat = {{TString("MrObject"), 0}, {TString("userschema"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["MrObject"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["userschema"]);
    }

    Y_UNIT_TEST(TableBindingsV2) {
        NSQLTranslation::TTranslationSettings settings = GetSettingsWithS3Binding("foo");
        NYql::TAstParseResult res = SqlToYqlWithSettings(
            "pragma S3BindingsAsTableHints; select * from bindings.foo",
            settings
        );
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "MrObject") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos,
                                           line.find(R"__((MrTableConcat (Key '('table (String '"path")))) (Void) '('('"bar" '"1") '('"compression" '"ccompression") '('"format" '"format") '('"partitionedby" '"key" '"subkey") '('"userschema" (SqlTypeFromYson)__"));
            }
        };

        TWordCountHive elementStat = {{TString("MrTableConcat"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["MrTableConcat"]);
    }

    Y_UNIT_TEST(TrailingCommaInWithout) {
        UNIT_ASSERT(SqlToYql("SELECT * WITHOUT stream, FROM plato.Input").IsOk());
        UNIT_ASSERT(SqlToYql("SELECT a.* WITHOUT a.intersect, FROM plato.Input AS a").IsOk());
        UNIT_ASSERT(SqlToYql("SELECT a.* WITHOUT col1, col2, a.col3, FROM plato.Input AS a").IsOk());
    }

    Y_UNIT_TEST(NoStackOverflowOnBigCaseStatement) {
        TStringBuilder req;
        req << "select case 1 + 123";
        for (size_t i = 0; i < 20000; ++i)  {
            req << " when " << i << " then " << i + 1;
        }
        req << " else 100500 end;";
        UNIT_ASSERT(SqlToYql(req).IsOk());
    }

    Y_UNIT_TEST(CollectPreaggregatedInListLiteral) {
        UNIT_ASSERT(SqlToYql("SELECT [COUNT(DISTINCT a+b)] FROM plato.Input").IsOk());
    }

    Y_UNIT_TEST(SmartParenInGroupByClause) {
        UNIT_ASSERT(SqlToYql("SELECT * FROM plato.Input GROUP BY (k, v)").IsOk());
    }

    Y_UNIT_TEST(AlterTableRenameToIsCorrect) {
        UNIT_ASSERT(SqlToYql("USE plato; ALTER TABLE table RENAME TO moved").IsOk());
    }

    Y_UNIT_TEST(AlterTableAddDropColumnIsCorrect) {
        UNIT_ASSERT(SqlToYql("USE plato; ALTER TABLE table ADD COLUMN addc uint64, DROP COLUMN dropc, ADD addagain uint64").IsOk());
    }

    Y_UNIT_TEST(AlterTableSetTTLIsCorrect) {
        UNIT_ASSERT(SqlToYql("USE plato; ALTER TABLE table SET (TTL = Interval(\"PT3H\") ON column)").IsOk());
    }

    Y_UNIT_TEST(AlterTableAddChangefeedIsCorrect) {
        UNIT_ASSERT(SqlToYql("USE plato; ALTER TABLE table ADD CHANGEFEED feed WITH (MODE = 'UPDATES', FORMAT = 'json')").IsOk());
    }

    Y_UNIT_TEST(AlterTableAlterChangefeedIsCorrect) {
        UNIT_ASSERT(SqlToYql("USE plato; ALTER TABLE table ALTER CHANGEFEED feed DISABLE").IsOk());
        ExpectFailWithError("USE plato; ALTER TABLE table ALTER CHANGEFEED feed SET (FORMAT = 'proto');",
            "<main>:1:66: Error: FORMAT alter is not supported\n");
    }

    Y_UNIT_TEST(AlterTableDropChangefeedIsCorrect) {
        UNIT_ASSERT(SqlToYql("USE plato; ALTER TABLE table DROP CHANGEFEED feed").IsOk());
    }

    Y_UNIT_TEST(AlterTableSetPartitioningIsCorrect) {
        UNIT_ASSERT(SqlToYql("USE plato; ALTER TABLE table SET (AUTO_PARTITIONING_BY_SIZE = DISABLED)").IsOk());
    }

    Y_UNIT_TEST(OptionalAliases) {
        UNIT_ASSERT(SqlToYql("USE plato; SELECT foo FROM (SELECT key foo FROM Input);").IsOk());
        UNIT_ASSERT(SqlToYql("USE plato; SELECT a.x FROM Input1 a JOIN Input2 b ON a.key = b.key;").IsOk());
        UNIT_ASSERT(SqlToYql("USE plato; SELECT a.x FROM (VALUES (1,2), (3,4)) a(x,key) JOIN Input b ON a.key = b.key;").IsOk());
    }

    Y_UNIT_TEST(TableNameConstness) {
        UNIT_ASSERT(SqlToYql("USE plato; $path = 'foo'; SELECT TableName($path), count(*) FROM Input;").IsOk());
        UNIT_ASSERT(SqlToYql("$path = 'foo'; SELECT TableName($path, 'yt'), count(*) FROM plato.Input;").IsOk());
        ExpectFailWithError("USE plato; SELECT TableName(), count(*) FROM plato.Input;",
            "<main>:1:19: Error: Expression has to be an aggregation function or key column, because aggregation is used elsewhere in this subquery\n");
    }

    Y_UNIT_TEST(UseShouldWorkAsColumnName) {
        UNIT_ASSERT(SqlToYql("select use from (select 1 as use);").IsOk());
    }

    Y_UNIT_TEST(TrueFalseWorkAfterDollar) {
        UNIT_ASSERT(SqlToYql("$ true = false; SELECT $ true or false;").IsOk());
        UNIT_ASSERT(SqlToYql("$False = 0; SELECT $False;").IsOk());
    }
}

Y_UNIT_TEST_SUITE(ExternalFunction) {
    Y_UNIT_TEST(ValidUseFunctions) {

        UNIT_ASSERT(SqlToYql(
                "PROCESS plato.Input"
                " USING EXTERNAL FUNCTION('YANDEX-CLOUD', 'foo', <|a: 123, b: a + 641|>)"
                " WITH INPUT_TYPE=Struct<a:Int32>, OUTPUT_TYPE=Struct<b:Int32>,"
                " CONCURRENCY=3, OPTIMIZE_FOR='CALLS'").IsOk());

        // use CALLS without quotes, as keyword
        UNIT_ASSERT(SqlToYql(
                "PROCESS plato.Input"
                " USING EXTERNAL FUNCTION('YANDEX-CLOUD', 'foo')"
                " WITH INPUT_TYPE=Struct<a:Int32>, OUTPUT_TYPE=Struct<b:Int32>,"
                " OPTIMIZE_FOR=CALLS").IsOk());

        UNIT_ASSERT(SqlToYql(
                "PROCESS plato.Input"
                " USING EXTERNAL FUNCTION('YANDEX-CLOUD', 'foo', TableRow())"
                " WITH INPUT_TYPE=Struct<a:Int32>, OUTPUT_TYPE=Struct<b:Int32>,"
                " CONCURRENCY=3").IsOk());

        UNIT_ASSERT(SqlToYql(
                "PROCESS plato.Input"
                " USING EXTERNAL FUNCTION('YANDEX-CLOUD', 'foo')"
                " WITH INPUT_TYPE=Struct<a:Int32>, OUTPUT_TYPE=Struct<b:Int32>,"
                " CONCURRENCY=3, BATCH_SIZE=1000000, CONNECTION='yc-folder34fse-con',"
                " INIT=[0, 900]").IsOk());

        UNIT_ASSERT(SqlToYql(
                "PROCESS plato.Input"
                " USING EXTERNAL FUNCTION('YANDEX-CLOUD', 'bar', TableRow())"
                " WITH UNKNOWN_PARAM_1='837747712', UNKNOWN_PARAM_2=Tuple<Uint16, Utf8>,"
                " INPUT_TYPE=Struct<a:Int32>, OUTPUT_TYPE=Struct<b:Int32>").IsOk());
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
}

Y_UNIT_TEST_SUITE(SqlToYQLErrors) {
    Y_UNIT_TEST(StrayUTF8) {
        /// 'c' in plato is russian here
        NYql::TAstParseResult res = SqlToYql("select * from сedar.Input");
        UNIT_ASSERT(!res.Root);

        TString a1 = Err2Str(res);
        TString a2(R"foo(<main>:1:14: Error: Unexpected character 'с' (Unicode character <1089>) : cannot match to any predicted input...

<main>:1:15: Error: Unexpected character : cannot match to any predicted input...

)foo");

        UNIT_ASSERT_NO_DIFF(a1, a2);
    }

    Y_UNIT_TEST(IvalidStringLiteralWithEscapedBackslash) {
        NYql::TAstParseResult res1 = SqlToYql(R"foo($bar = 'a\\'b';)foo");
        NYql::TAstParseResult res2 = SqlToYql(R"foo($bar = "a\\"b";)foo");
        UNIT_ASSERT(!res1.Root);
        UNIT_ASSERT(!res2.Root);

        UNIT_ASSERT_NO_DIFF(Err2Str(res1), "<main>:1:15: Error: Unexpected character : syntax error...\n\n");
        UNIT_ASSERT_NO_DIFF(Err2Str(res2), "<main>:1:15: Error: Unexpected character : syntax error...\n\n");
    }

    Y_UNIT_TEST(InvalidHexInStringLiteral) {
        NYql::TAstParseResult res = SqlToYql("select \"foo\\x1\\xfe\"");
        UNIT_ASSERT(!res.Root);
        TString a1 = Err2Str(res);
        TString a2 = "<main>:1:15: Error: Failed to parse string literal: Invalid hexadecimal value\n";

        UNIT_ASSERT_NO_DIFF(a1, a2);
    }

    Y_UNIT_TEST(InvalidOctalInMultilineStringLiteral) {
        NYql::TAstParseResult res = SqlToYql("select \"foo\n"
                                             "bar\n"
                                             "\\01\"");
        UNIT_ASSERT(!res.Root);
        TString a1 = Err2Str(res);
        TString a2 = "<main>:3:4: Error: Failed to parse string literal: Invalid octal value\n";

        UNIT_ASSERT_NO_DIFF(a1, a2);
    }

    Y_UNIT_TEST(InvalidDoubleAtString) {
        NYql::TAstParseResult res = SqlToYql("select @@@@@@");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:13: Error: Unexpected character : syntax error...\n\n");
    }

    Y_UNIT_TEST(InvalidDoubleAtStringWhichWasAcceptedEarlier) {
        NYql::TAstParseResult res = SqlToYql("SELECT @@foo@@ @ @@bar@@");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:7: Error: Unexpected token '@@foo@@' : cannot match to any predicted input...\n\n");
    }

    Y_UNIT_TEST(InvalidStringFromTable) {
        NYql::TAstParseResult res = SqlToYql("select \"FOO\"\"BAR from plato.foo");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:31: Error: Unexpected character : syntax error...\n\n");
    }

    Y_UNIT_TEST(InvalidDoubleAtStringFromTable) {
        NYql::TAstParseResult res = SqlToYql("select @@@@@@ from plato.foo");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:28: Error: Unexpected character : syntax error...\n\n");
    }

    Y_UNIT_TEST(SelectInvalidSyntax) {
        NYql::TAstParseResult res = SqlToYql("select 1 form Wat");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:14: Error: Unexpected token 'Wat' : cannot match to any predicted input...\n\n");
    }

    Y_UNIT_TEST(SelectNoCluster) {
        NYql::TAstParseResult res = SqlToYql("select foo from bar");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: No cluster name given and no default cluster is selected\n");
    }

    Y_UNIT_TEST(SelectDuplicateColumns) {
        NYql::TAstParseResult res = SqlToYql("select a, a from plato.Input");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:11: Error: Unable to use duplicate column names. Collision in name: a\n");
    }

    Y_UNIT_TEST(SelectDuplicateLabels) {
        NYql::TAstParseResult res = SqlToYql("select a as foo, b as foo from plato.Input");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:18: Error: Unable to use duplicate column names. Collision in name: foo\n");
    }

    Y_UNIT_TEST(SelectCaseWithoutThen) {
        NYql::TAstParseResult res = SqlToYql("select case when true 1;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res),
            "<main>:1:22: Error: Unexpected token absence : Missing THEN \n\n"
            "<main>:1:23: Error: Unexpected token absence : Missing END \n\n"
        );
    }

    Y_UNIT_TEST(SelectComplexCaseWithoutThen) {
        NYql::TAstParseResult res = SqlToYql(
            "SELECT *\n"
            "FROM plato.Input AS a\n"
            "WHERE CASE WHEN a.key = \"foo\" a.subkey ELSE a.value END\n"
        );
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:30: Error: Unexpected token absence : Missing THEN \n\n");
    }

    Y_UNIT_TEST(SelectCaseWithoutEnd) {
        NYql::TAstParseResult res = SqlToYql("select case a when b then c end from plato.Input");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: ELSE is required\n");
    }

    Y_UNIT_TEST(SelectWithBadAggregationNoInput) {
        NYql::TAstParseResult res = SqlToYql("select a, Min(b), c");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res),
           "<main>:1:1: Error: Column references are not allowed without FROM\n"
           "<main>:1:8: Error: Column reference 'a'\n"
           "<main>:1:1: Error: Column references are not allowed without FROM\n"
           "<main>:1:15: Error: Column reference 'b'\n"
           "<main>:1:1: Error: Column references are not allowed without FROM\n"
           "<main>:1:19: Error: Column reference 'c'\n"
        );
    }

    Y_UNIT_TEST(SelectWithBadAggregation) {
        ExpectFailWithError("select count(*), 1 + key from plato.Input",
            "<main>:1:22: Error: Column `key` must either be a key column in GROUP BY or it should be used in aggregation function\n");
    }

    Y_UNIT_TEST(SelectWithBadAggregatedTerms) {
        ExpectFailWithError("select key, 2 * subkey from plato.Input group by key",
            "<main>:1:17: Error: Column `subkey` must either be a key column in GROUP BY or it should be used in aggregation function\n");
    }

    Y_UNIT_TEST(SelectWithBadAggregationInHaving) {
        ExpectFailWithError("select key from plato.Input group by key\n"
                            "having \"f\" || value == \"foo\"",
            "<main>:2:15: Error: Column `value` must either be a key column in GROUP BY or it should be used in aggregation function\n");
    }

    Y_UNIT_TEST(JoinWithNonAggregatedColumnInProjection) {
        ExpectFailWithError("select a.key, 1 + b.subkey\n"
                            "from plato.Input1 as a join plato.Input2 as b using(key)\n"
                            "group by a.key;",
            "<main>:1:19: Error: Column `b.subkey` must either be a key column in GROUP BY or it should be used in aggregation function\n");

        ExpectFailWithError("select a.key, 1 + b.subkey.x\n"
                            "from plato.Input1 as a join plato.Input2 as b using(key)\n"
                            "group by a.key;",
            "<main>:1:19: Error: Column must either be a key column in GROUP BY or it should be used in aggregation function\n");
    }

    Y_UNIT_TEST(SelectWithBadAggregatedTermsWithSources) {
        ExpectFailWithError("select key, 1 + a.subkey\n"
                            "from plato.Input1 as a\n"
                            "group by a.key;",
            "<main>:1:17: Error: Column `a.subkey` must either be a key column in GROUP BY or it should be used in aggregation function\n");
        ExpectFailWithError("select key, 1 + a.subkey.x\n"
                            "from plato.Input1 as a\n"
                            "group by a.key;",
            "<main>:1:17: Error: Column must either be a key column in GROUP BY or it should be used in aggregation function\n");
    }

    Y_UNIT_TEST(WarnForAggregationBySelectAlias) {
        NYql::TAstParseResult res = SqlToYql("select c + 1 as c from plato.Input\n"
                                             "group by  c");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res),
            "<main>:2:11: Warning: GROUP BY will aggregate by column `c` instead of aggregating by SELECT expression with same alias, code: 4532\n"
            "<main>:1:10: Warning: You should probably use alias in GROUP BY instead of using it here. Please consult documentation for more details, code: 4532\n");

        res = SqlToYql("select c + 1 as c from plato.Input\n"
                       "group by Math::Floor(c + 2) as c;");

        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res),
            "<main>:2:22: Warning: GROUP BY will aggregate by column `c` instead of aggregating by SELECT expression with same alias, code: 4532\n"
            "<main>:1:10: Warning: You should probably use alias in GROUP BY instead of using it here. Please consult documentation for more details, code: 4532\n");
    }

    Y_UNIT_TEST(NoWarnForAggregationBySelectAliasWhenAggrFunctionsAreUsedInAlias) {
        NYql::TAstParseResult res = SqlToYql("select\n"
                                             "    cast(avg(val) as int) as value,\n"
                                             "    value as key\n"
                                             "from\n"
                                             "    plato.Input\n"
                                             "group by value");

        UNIT_ASSERT(res.Root);
        UNIT_ASSERT(res.Issues.Size() == 0);

        res = SqlToYql("select\n"
                       "    cast(avg(val) over w as int) as value,\n"
                       "    value as key\n"
                       "from\n"
                       "    plato.Input\n"
                       "group by value\n"
                       "window w as ()");

        UNIT_ASSERT(res.Root);
        UNIT_ASSERT(res.Issues.Size() == 0);
    }

    Y_UNIT_TEST(NoWarnForAggregationBySelectAliasWhenQualifiedNameIsUsed) {
        NYql::TAstParseResult res = SqlToYql("select\n"
                                             "  Unwrap(a.key) as key\n"
                                             "from plato.Input as a\n"
                                             "join plato.Input2 as b using(k)\n"
                                             "group by a.key;");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT(res.Issues.Size() == 0);

        res = SqlToYql("select Unwrap(a.key) as key\n"
                       "from plato.Input as a\n"
                       "group by a.key;");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT(res.Issues.Size() == 0);
    }

    Y_UNIT_TEST(NoWarnForAggregationBySelectAliasWhenTrivialRenamingIsUsed) {
        NYql::TAstParseResult res = SqlToYql("select a.key as key\n"
                                             "from plato.Input as a\n"
                                             "group by key;");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT(res.Issues.Size() == 0);

        res = SqlToYql("select key as key\n"
                       "from plato.Input\n"
                       "group by key;");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT(res.Issues.Size() == 0);
    }

    Y_UNIT_TEST(ErrorByAggregatingByExpressionWithSameExpressionInSelect) {
        ExpectFailWithError("select k * 2 from plato.Input group by k * 2",
            "<main>:1:8: Error: Column `k` must either be a key column in GROUP BY or it should be used in aggregation function\n");
    }

    Y_UNIT_TEST(ErrorForAggregationBySelectAlias) {
        ExpectFailWithError("select key, Math::Floor(1.1 + a.subkey) as foo\n"
                            "from plato.Input as a\n"
                            "group by a.key, foo;",
            "<main>:3:17: Warning: GROUP BY will aggregate by column `foo` instead of aggregating by SELECT expression with same alias, code: 4532\n"
            "<main>:1:19: Warning: You should probably use alias in GROUP BY instead of using it here. Please consult documentation for more details, code: 4532\n"
            "<main>:1:31: Error: Column `a.subkey` must either be a key column in GROUP BY or it should be used in aggregation function\n");

        ExpectFailWithError("select c + 1 as c from plato.Input\n"
                            "group by Math::Floor(c + 2);",
            "<main>:2:22: Warning: GROUP BY will aggregate by column `c` instead of aggregating by SELECT expression with same alias, code: 4532\n"
            "<main>:1:10: Warning: You should probably use alias in GROUP BY instead of using it here. Please consult documentation for more details, code: 4532\n"
            "<main>:1:8: Error: Column `c` must either be a key column in GROUP BY or it should be used in aggregation function\n");
    }

    Y_UNIT_TEST(SelectWithDuplicateGroupingColumns) {
        NYql::TAstParseResult res = SqlToYql("select c from plato.Input group by c, c");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: Duplicate grouping column: c\n");
    }

    Y_UNIT_TEST(SelectWithBadAggregationInGrouping) {
        NYql::TAstParseResult res = SqlToYql("select a, Min(b), c group by c");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: Column references are not allowed without FROM\n"
            "<main>:1:30: Error: Column reference 'c'\n");
    }

    Y_UNIT_TEST(SelectWithOpOnBadAggregation) {
        ExpectFailWithError("select 1 + a + Min(b) from plato.Input",
            "<main>:1:12: Error: Column `a` must either be a key column in GROUP BY or it should be used in aggregation function\n");
    }

    Y_UNIT_TEST(SelectOrderByConstantNum) {
        NYql::TAstParseResult res = SqlToYql("select a from plato.Input order by 1");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:36: Error: Unable to ORDER BY constant expression\n");
    }

    Y_UNIT_TEST(SelectOrderByConstantExpr) {
        NYql::TAstParseResult res = SqlToYql("select a from plato.Input order by 1 * 42");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:38: Error: Unable to ORDER BY constant expression\n");
    }

    Y_UNIT_TEST(SelectOrderByConstantString) {
        NYql::TAstParseResult res = SqlToYql("select a from plato.Input order by \"nest\"");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:36: Error: Unable to ORDER BY constant expression\n");
    }

    Y_UNIT_TEST(SelectOrderByAggregated) {
        NYql::TAstParseResult res = SqlToYql("select a from plato.Input order by min(a)");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:36: Error: Unable to ORDER BY aggregated values\n");
    }

    Y_UNIT_TEST(ErrorInOrderByExpresison) {
        NYql::TAstParseResult res = SqlToYql("select key, value from plato.Input order by (key as zey)");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:45: Error: You should use in ORDER BY column name, qualified field, callable function or expression\n");
    }

    Y_UNIT_TEST(SelectAggregatedWhere) {
        NYql::TAstParseResult res = SqlToYql("select * from plato.Input where count(key)");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:33: Error: Can not use aggregated values in filtering\n");
    }

    Y_UNIT_TEST(DoubleFrom) {
        NYql::TAstParseResult res = SqlToYql("from plato.Input select * from plato.Input");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:27: Error: Only one FROM clause is allowed\n");
    }

    Y_UNIT_TEST(SelectJoinMissingCorrName) {
        NYql::TAstParseResult res = SqlToYql("select * from plato.Input1 as a join plato.Input2 as b on a.key == key");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:65: Error: JOIN: column requires correlation name\n");
    }

    Y_UNIT_TEST(SelectJoinMissingCorrName1) {
        NYql::TAstParseResult res = SqlToYql(
            "use plato;\n"
            "$foo = select * from Input1;\n"
            "select * from Input2 join $foo USING(key);\n"
        );
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:27: Error: JOIN: missing correlation name for source\n");
    }

    Y_UNIT_TEST(SelectJoinMissingCorrName2) {
        NYql::TAstParseResult res = SqlToYql(
            "use plato;\n"
            "$foo = select * from Input1;\n"
            "select * from Input2 cross join $foo;\n"
        );
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:33: Error: JOIN: missing correlation name for source\n");
    }

    Y_UNIT_TEST(SelectJoinEmptyCorrNames) {
        NYql::TAstParseResult res = SqlToYql(
            "$left = (SELECT * FROM plato.Input1 LIMIT 2);\n"
            "$right = (SELECT * FROM plato.Input2 LIMIT 2);\n"
            "SELECT * FROM $left FULL JOIN $right USING (key);\n"
        );
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:45: Error: At least one correlation name is required in join\n");
    }

    Y_UNIT_TEST(SelectJoinSameCorrNames) {
        NYql::TAstParseResult res = SqlToYql("SELECT Input.key FROM plato.Input JOIN plato.Input1 ON Input.key == Input.subkey\n");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:66: Error: JOIN: different correlation names are required for joined tables\n");
    }

    Y_UNIT_TEST(SelectJoinConstPredicateArg) {
        NYql::TAstParseResult res = SqlToYql("SELECT * FROM plato.Input1 as A JOIN plato.Input2 as B ON A.key == B.key AND A.subkey == \"wtf\"\n");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:87: Error: JOIN: each equality predicate argument must depend on exactly one JOIN input\n");
    }

    Y_UNIT_TEST(SelectJoinNonEqualityPredicate) {
        NYql::TAstParseResult res = SqlToYql("SELECT * FROM plato.Input1 as A JOIN plato.Input2 as B ON A.key == B.key AND A.subkey > B.subkey\n");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:87: Error: JOIN ON expression must be a conjunction of equality predicates\n");
    }

    Y_UNIT_TEST(SelectEquiJoinCorrNameOutOfScope) {
        NYql::TAstParseResult res = SqlToYql(
            "PRAGMA equijoin;\n"
            "SELECT * FROM plato.A JOIN plato.B ON A.key == C.key JOIN plato.C ON A.subkey == C.subkey;\n"
        );
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:45: Error: JOIN: can not use source: C in equality predicate, it is out of current join scope\n");
    }

    Y_UNIT_TEST(SelectEquiJoinNoRightSource) {
        NYql::TAstParseResult res = SqlToYql(
            "PRAGMA equijoin;\n"
            "SELECT * FROM plato.A JOIN plato.B ON A.key == B.key JOIN plato.C ON A.subkey == B.subkey;\n"
        );
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:79: Error: JOIN ON equality predicate must have one of its arguments from the rightmost source\n");
    }

    Y_UNIT_TEST(SelectEquiJoinOuterWithoutType) {
        NYql::TAstParseResult res = SqlToYql(
            "SELECT * FROM plato.A Outer JOIN plato.B ON A.key == B.key;\n"
        );
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:23: Error: Invalid join type: OUTER JOIN. OUTER keyword is optional and can only be used after LEFT, RIGHT or FULL\n");
    }

    Y_UNIT_TEST(SelectEquiJoinOuterWithWrongType) {
        NYql::TAstParseResult res = SqlToYql(
            "SELECT * FROM plato.A LEFT semi OUTER JOIN plato.B ON A.key == B.key;\n"
            );
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:33: Error: Invalid join type: LEFT SEMI OUTER JOIN. OUTER keyword is optional and can only be used after LEFT, RIGHT or FULL\n");
    }

    Y_UNIT_TEST(InsertNoCluster) {
        NYql::TAstParseResult res = SqlToYql("insert into Output (foo) values (1)");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: No cluster name given and no default cluster is selected\n");
    }

    Y_UNIT_TEST(InsertValuesNoLabels) {
        NYql::TAstParseResult res = SqlToYql("insert into plato.Output values (1)");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:19: Error: INSERT INTO ... VALUES requires specification of table columns\n");
    }

    Y_UNIT_TEST(UpsertValuesNoLabelsKikimr) {
        NYql::TAstParseResult res = SqlToYql("upsert into plato.Output values (1)", 10, TString(NYql::KikimrProviderName));
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:19: Error: UPSERT INTO ... VALUES requires specification of table columns\n");
    }

    Y_UNIT_TEST(ReplaceValuesNoLabelsKikimr) {
        NYql::TAstParseResult res = SqlToYql("replace into plato.Output values (1)", 10, TString(NYql::KikimrProviderName));
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:20: Error: REPLACE INTO ... VALUES requires specification of table columns\n");
    }

    Y_UNIT_TEST(InsertValuesInvalidLabels) {
        NYql::TAstParseResult res = SqlToYql("insert into plato.Output (foo) values (1, 2)");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:27: Error: VALUES have 2 columns, INSERT INTO expects: 1\n");
    }

    Y_UNIT_TEST(BuiltinFileOpNoArgs) {
        NYql::TAstParseResult res = SqlToYql("select FilePath()");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: FilePath() requires exactly 1 arguments, given: 0\n");
    }

    Y_UNIT_TEST(ProcessWithHaving) {
        NYql::TAstParseResult res = SqlToYql("process plato.Input using some::udf(value) having value == 1");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:15: Error: PROCESS does not allow HAVING yet! You may request it on yql@ maillist.\n");
    }

    Y_UNIT_TEST(ReduceNoBy) {
        NYql::TAstParseResult res = SqlToYql("reduce plato.Input using some::udf(value)");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:19: Error: Unexpected token absence : Missing ON \n\n<main>:1:25: Error: Unexpected token absence : Missing USING \n\n");
    }

    Y_UNIT_TEST(ReduceDistinct) {
        NYql::TAstParseResult res = SqlToYql("reduce plato.Input on key using some::udf(distinct value)");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:43: Error: DISTINCT can not be used in PROCESS/REDUCE\n");
    }

    Y_UNIT_TEST(CreateTableWithView) {
        NYql::TAstParseResult res = SqlToYql("CREATE TABLE plato.foo:bar (key INT);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:22: Error: Unexpected token ':' : syntax error...\n\n");
    }

    Y_UNIT_TEST(AsteriskWithSomethingAfter) {
        NYql::TAstParseResult res = SqlToYql("select *, LENGTH(value) from plato.Input;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: Unable to use plain '*' with other projection items. Please use qualified asterisk instead: '<table>.*' (<table> can be either table name or table alias).\n");
    }

    Y_UNIT_TEST(AsteriskWithSomethingBefore) {
        NYql::TAstParseResult res = SqlToYql("select LENGTH(value), * from plato.Input;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:23: Error: Unable to use plain '*' with other projection items. Please use qualified asterisk instead: '<table>.*' (<table> can be either table name or table alias).\n");
    }

    Y_UNIT_TEST(DuplicatedQualifiedAsterisk) {
        NYql::TAstParseResult res = SqlToYql("select in.*, key, in.* from plato.Input as in;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:19: Error: Unable to use twice same quialified asterisk. Invalid source: in\n");
    }

    Y_UNIT_TEST(BrokenLabel) {
        NYql::TAstParseResult res = SqlToYql("select in.*, key as `funny.label` from plato.Input as in;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:14: Error: Unable to use '.' in column name. Invalid column name: funny.label\n");
    }

    Y_UNIT_TEST(KeyConflictDetect0) {
        NYql::TAstParseResult res = SqlToYql("select key, in.key as key from plato.Input as in;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:13: Error: Unable to use duplicate column names. Collision in name: key\n");
    }

    Y_UNIT_TEST(KeyConflictDetect1) {
        NYql::TAstParseResult res = SqlToYql("select length(key) as key, key from plato.Input;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:28: Error: Unable to use duplicate column names. Collision in name: key\n");
    }

    Y_UNIT_TEST(KeyConflictDetect2) {
        NYql::TAstParseResult res = SqlToYql("select key, in.key from plato.Input as in;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: Duplicate column: key\n");
    }

    Y_UNIT_TEST(AutogenerationAliasWithCollisionConflict1) {
        NYql::TAstParseResult res = SqlToYql("select LENGTH(Value), key as column0 from plato.Input;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: Duplicate column: column0\n");
    }

    Y_UNIT_TEST(AutogenerationAliasWithCollisionConflict2) {
        NYql::TAstParseResult res = SqlToYql("select key as column1, LENGTH(Value) from plato.Input;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: Duplicate column: column1\n");
    }

    Y_UNIT_TEST(MissedSourceTableForQualifiedAsteriskOnSimpleSelect) {
        NYql::TAstParseResult res = SqlToYql("use plato; select Intop.*, Input.key from plato.Input;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:19: Error: Unknown correlation name: Intop\n");
    }

    Y_UNIT_TEST(MissedSourceTableForQualifiedAsteriskOnJoin) {
        NYql::TAstParseResult res = SqlToYql("use plato; select tmissed.*, t2.*, t1.key from plato.Input as t1 join plato.Input as t2 on t1.key==t2.key;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:19: Error: Unknown correlation name for asterisk: tmissed\n");
    }

    Y_UNIT_TEST(UnableToReferenceOnNotExistSubcolumn) {
        NYql::TAstParseResult res = SqlToYql("select b.subkey from (select key from plato.Input as a) as b;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: Column subkey is not in source column set\n");
    }

    Y_UNIT_TEST(ConflictOnSameNameWithQualify0) {
        NYql::TAstParseResult res = SqlToYql("select in.key, in.key as key from plato.Input as in;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: Duplicate column: key\n");
    }

    Y_UNIT_TEST(ConflictOnSameNameWithQualify1) {
        NYql::TAstParseResult res = SqlToYql("select in.key, length(key) as key from plato.Input as in;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: Duplicate column: key\n");
    }

    Y_UNIT_TEST(ConflictOnSameNameWithQualify2) {
        NYql::TAstParseResult res = SqlToYql("select key, in.key from plato.Input as in;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: Duplicate column: key\n");
    }

    Y_UNIT_TEST(ConflictOnSameNameWithQualify3) {
        NYql::TAstParseResult res = SqlToYql("select in.key, subkey as key from plato.Input as in;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: Duplicate column: key\n");
    }

    Y_UNIT_TEST(SelectOrderByUnknownLabel) {
        NYql::TAstParseResult res = SqlToYql("select a from plato.Input order by b");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:36: Error: Column b is not in source column set. Did you mean a?\n");
    }

    Y_UNIT_TEST(SelectFlattenBySameColumns) {
        NYql::TAstParseResult res = SqlToYql("select key from plato.Input flatten by (key, key as kk)");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:46: Error: Duplicate column name found: key in FlattenBy section\n");
    }

    Y_UNIT_TEST(SelectFlattenBySameAliases) {
        NYql::TAstParseResult res = SqlToYql("select key from plato.Input flatten by (key as kk, subkey as kk);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:52: Error: Duplicate alias found: kk in FlattenBy section\n");
    }

    Y_UNIT_TEST(SelectFlattenByExprSameAliases) {
        NYql::TAstParseResult res = SqlToYql("select key from plato.Input flatten by (key as kk, ListSkip(subkey,1) as kk);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:52: Error: Collision between alias and column name: kk in FlattenBy section\n");
    }

    Y_UNIT_TEST(SelectFlattenByConflictNameAndAlias0) {
        NYql::TAstParseResult res = SqlToYql("select key from plato.Input flatten by (key, subkey as key);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:46: Error: Collision between alias and column name: key in FlattenBy section\n");
    }

    Y_UNIT_TEST(SelectFlattenByConflictNameAndAlias1) {
        NYql::TAstParseResult res = SqlToYql("select key from plato.Input flatten by (key as kk, subkey as key);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:52: Error: Collision between alias and column name: key in FlattenBy section\n");
    }

    Y_UNIT_TEST(SelectFlattenByExprConflictNameAndAlias1) {
        NYql::TAstParseResult res = SqlToYql("select key from plato.Input flatten by (key as kk, ListSkip(subkey,1) as key);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:52: Error: Duplicate column name found: key in FlattenBy section\n");
    }

    Y_UNIT_TEST(SelectFlattenByUnnamedExpr) {
        NYql::TAstParseResult res = SqlToYql("select key from plato.Input flatten by (key, ListSkip(key, 1))");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:46: Error: Unnamed expression after FLATTEN BY is not allowed\n");
    }

    Y_UNIT_TEST(UseInOnStrings) {
        NYql::TAstParseResult res = SqlToYql("select * from plato.Input where \"foo\" in \"foovalue\";");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:42: Error: Unable to use IN predicate with string argument, it won't search substring - "
                                          "expecting tuple, list, dict or single column table source\n");
    }

    Y_UNIT_TEST(UseSubqueryInScalarContextInsideIn) {
        NYql::TAstParseResult res = SqlToYql("$q = (select key from plato.Input); select * from plato.Input where subkey in ($q);");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:79: Warning: Using subrequest in scalar context after IN, "
                                          "perhaps you should remove parenthesis here, code: 4501\n");
    }

    Y_UNIT_TEST(InHintsWithKeywordClash) {
        NYql::TAstParseResult res = SqlToYql("SELECT COMPACT FROM plato.Input WHERE COMPACT IN COMPACT `COMPACT`(1,2,3)");
        UNIT_ASSERT(!res.Root);
        // should try to parse last compact as call expression
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:58: Error: Unknown builtin: COMPACT\n");
    }

    Y_UNIT_TEST(ErrorColumnPosition) {
        NYql::TAstParseResult res = SqlToYql(
            "USE plato;\n"
            "SELECT \n"
            "value FROM (\n"
            "select key from Input\n"
            ");\n"
        );
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:1: Error: Column value is not in source column set\n");
    }

    Y_UNIT_TEST(InsertAbortMapReduce) {
        NYql::TAstParseResult res = SqlToYql("INSERT OR ABORT INTO plato.Output SELECT key FROM plato.Input");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:0: Error: INSERT OR ABORT INTO is not supported for yt tables\n");
    }

    Y_UNIT_TEST(ReplaceIntoMapReduce) {
        NYql::TAstParseResult res = SqlToYql("REPLACE INTO plato.Output SELECT key FROM plato.Input");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:0: Error: Meaning of REPLACE INTO has been changed, now you should use INSERT INTO <table> WITH TRUNCATE ... for yt\n");
    }

    Y_UNIT_TEST(UpsertIntoMapReduce) {
        NYql::TAstParseResult res = SqlToYql("UPSERT INTO plato.Output SELECT key FROM plato.Input");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:0: Error: UPSERT INTO is not supported for yt tables\n");
    }

    Y_UNIT_TEST(UpdateMapReduce) {
        NYql::TAstParseResult res = SqlToYql("UPDATE plato.Output SET value = value + 1 WHERE key < 1");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:0: Error: UPDATE is unsupported for yt\n");
    }

    Y_UNIT_TEST(DeleteMapReduce) {
        NYql::TAstParseResult res = SqlToYql("DELETE FROM plato.Output WHERE key < 1");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:0: Error: DELETE is unsupported for yt\n");
    }

    Y_UNIT_TEST(ReplaceIntoWithTruncate) {
        NYql::TAstParseResult res = SqlToYql("REPLACE INTO plato.Output WITH TRUNCATE SELECT key FROM plato.Input");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:32: Error: Unable REPLACE INTO with truncate mode\n");
    }

    Y_UNIT_TEST(UpsertIntoWithTruncate) {
        NYql::TAstParseResult res = SqlToYql("UPSERT INTO plato.Output WITH TRUNCATE SELECT key FROM plato.Input");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:31: Error: Unable UPSERT INTO with truncate mode\n");
    }

    Y_UNIT_TEST(InsertIntoWithTruncateKikimr) {
        NYql::TAstParseResult res = SqlToYql("INSERT INTO plato.Output WITH TRUNCATE SELECT key FROM plato.Input", 10, TString(NYql::KikimrProviderName));
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:0: Error: INSERT INTO WITH TRUNCATE is not supported for kikimr tables\n");
    }

    Y_UNIT_TEST(InsertIntoWithWrongArgumentCount) {
        NYql::TAstParseResult res = SqlToYql("insert into plato.Output with truncate (key, value, subkey) values (5, '1', '2', '3');");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:53: Error: VALUES have 4 columns, INSERT INTO ... WITH TRUNCATE expects: 3\n");
    }

    Y_UNIT_TEST(UpsertWithWrongArgumentCount) {
        NYql::TAstParseResult res = SqlToYql("upsert into plato.Output (key, value, subkey) values (2, '3');", 10, TString(NYql::KikimrProviderName));
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:39: Error: VALUES have 2 columns, UPSERT INTO expects: 3\n");
    }

    Y_UNIT_TEST(UnionNotSupported) {
        NYql::TAstParseResult res = SqlToYql("SELECT key FROM plato.Input UNION select subkey FROM plato.Input;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:29: Error: UNION without quantifier ALL is not supported yet. Did you mean UNION ALL?\n");
    }

    Y_UNIT_TEST(GroupingSetByExprWithoutAlias) {
        NYql::TAstParseResult res = SqlToYql("SELECT key FROM plato.Input GROUP BY GROUPING SETS (cast(key as uint32), subkey);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:53: Error: Unnamed expressions are not supported in GROUPING SETS. Please use '<expr> AS <name>'.\n");
    }

    Y_UNIT_TEST(GroupingSetByExprWithoutAlias2) {
        NYql::TAstParseResult res = SqlToYql("SELECT key FROM plato.Input GROUP BY subkey || subkey, GROUPING SETS (\n"
                                             "cast(key as uint32), subkey);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:1: Error: Unnamed expressions are not supported in GROUPING SETS. Please use '<expr> AS <name>'.\n");
    }

    Y_UNIT_TEST(CubeByExprWithoutAlias) {
        NYql::TAstParseResult res = SqlToYql("SELECT key FROM plato.Input GROUP BY CUBE (key, subkey / key);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:56: Error: Unnamed expressions are not supported in CUBE. Please use '<expr> AS <name>'.\n");
    }

    Y_UNIT_TEST(RollupByExprWithoutAlias) {
        NYql::TAstParseResult res = SqlToYql("SELECT key FROM plato.Input GROUP BY ROLLUP (subkey / key);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:53: Error: Unnamed expressions are not supported in ROLLUP. Please use '<expr> AS <name>'.\n");
    }

    Y_UNIT_TEST(GroupByHugeCubeDeniedNoPragma) {
        NYql::TAstParseResult res = SqlToYql("SELECT key FROM plato.Input GROUP BY CUBE (key, subkey, value, key + subkey as sum, key - subkey as sub, key + val as keyval);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:119: Error: GROUP BY CUBE is allowed only for 5 columns, but you use 6\n");
    }

    Y_UNIT_TEST(GroupByInvalidPragma) {
        NYql::TAstParseResult res = SqlToYql("PRAGMA GroupByCubeLimit = '-4';");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:27: Error: Expected unsigned integer literal as a single argument for: GroupByCubeLimit\n");
    }

    Y_UNIT_TEST(GroupByHugeCubeDeniedPragme) {
        NYql::TAstParseResult res = SqlToYql("PRAGMA GroupByCubeLimit = '4'; SELECT key FROM plato.Input GROUP BY CUBE (key, subkey, value, key + subkey as sum, key - subkey as sub);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:132: Error: GROUP BY CUBE is allowed only for 4 columns, but you use 5\n");
    }

    Y_UNIT_TEST(GroupByFewBigCubes) {
        NYql::TAstParseResult res = SqlToYql("SELECT key FROM plato.Input GROUP BY CUBE(key, subkey, key + subkey as sum), CUBE(value, value + key + subkey as total);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: Unable to GROUP BY more than 32 groups, you try use 80 groups\n");
    }

    Y_UNIT_TEST(GroupByFewBigCubesWithPragmaLimit) {
        NYql::TAstParseResult res = SqlToYql("PRAGMA GroupByLimit = '16'; SELECT key FROM plato.Input GROUP BY GROUPING SETS(key, subkey, key + subkey as sum), ROLLUP(value, value + key + subkey as total);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:29: Error: Unable to GROUP BY more than 16 groups, you try use 18 groups\n");
    }

    Y_UNIT_TEST(NoGroupingColumn0) {
        NYql::TAstParseResult res = SqlToYql(
            "select count(1), key_first, val_first, grouping(key_first, val_first, nomind) as group\n"
            "from plato.Input group by grouping sets (cast(key as uint32) /100 as key_first, Substring(value, 1, 1) as val_first);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:71: Error: Column 'nomind' is not a grouping column\n");
    }

    Y_UNIT_TEST(NoGroupingColumn1) {
        NYql::TAstParseResult res = SqlToYql("select count(1), grouping(key, value) as group_duo from plato.Input group by cube (key, subkey);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:32: Error: Column 'value' is not a grouping column\n");
    }

    Y_UNIT_TEST(EmptyAccess0) {
        NYql::TAstParseResult res = SqlToYql("insert into plato.Output (list0, list1) values (AsList(0, 1, 2), AsList(``));");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:73: Error: Column reference \"\" is not allowed in current scope\n");
    }

    Y_UNIT_TEST(EmptyAccess1) {
        NYql::TAstParseResult res = SqlToYql("insert into plato.Output (list0, list1) values (AsList(0, 1, 2), ``);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:66: Error: Column reference \"\" is not allowed in current scope\n");
    }

    Y_UNIT_TEST(UseUnknownColumnInInsert) {
        NYql::TAstParseResult res = SqlToYql("insert into plato.Output (list0, list1) values (AsList(0, 1, 2), AsList(`test`));");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:73: Error: Column reference \"test\" is not allowed in current scope\n");
    }

    Y_UNIT_TEST(GroupByEmptyColumn) {
        NYql::TAstParseResult res = SqlToYql("select count(1) from plato.Input group by ``;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:43: Error: Column name can not be empty\n");
    }

    Y_UNIT_TEST(ConvertNumberOutOfBase) {
        NYql::TAstParseResult res = SqlToYql("select 0o80l;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: Failed to parse number from string: 0o80l, char: '8' is out of base: 8\n");
    }

    Y_UNIT_TEST(ConvertNumberOutOfRangeForInt64ButFitsInUint64) {
        NYql::TAstParseResult res = SqlToYql("select 0xc000000000000000l;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: Failed to parse 13835058055282163712 as integer literal of Int64 type: value out of range for Int64\n");
    }

    Y_UNIT_TEST(ConvertNumberOutOfRangeUint64) {
        NYql::TAstParseResult res = SqlToYql("select 0xc0000000000000000l;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: Failed to parse number from string: 0xc0000000000000000l, number limit overflow\n");

        res = SqlToYql("select 1234234543563435151456;\n");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: Failed to parse number from string: 1234234543563435151456, number limit overflow\n");
    }

    Y_UNIT_TEST(ConvertNumberNegativeOutOfRange) {
        NYql::TAstParseResult res = SqlToYql("select -9223372036854775808;\n"
                                             "select -9223372036854775809;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:8: Error: Failed to parse negative integer: -9223372036854775809, number limit overflow\n");
    }

    Y_UNIT_TEST(InvaildUsageReal0) {
        NYql::TAstParseResult res = SqlToYql("select .0;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:7: Error: Unexpected token '.' : cannot match to any predicted input...\n\n");
    }

    Y_UNIT_TEST(InvaildUsageReal1) {
        NYql::TAstParseResult res = SqlToYql("select .0f;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:7: Error: Unexpected token '.' : cannot match to any predicted input...\n\n");
    }

    Y_UNIT_TEST(InvaildUsageWinFunctionWithoutWindow) {
        NYql::TAstParseResult res = SqlToYql("select lead(key, 2) from plato.Input;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: Failed to use window function Lead without window specification\n");
    }

    Y_UNIT_TEST(DropTableWithIfExists) {
        NYql::TAstParseResult res = SqlToYql("DROP TABLE IF EXISTS plato.foo;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:0: Error: IF EXISTS in DROP TABLE is not supported.\n");
    }

    Y_UNIT_TEST(TooManyErrors) {
        const char* q = R"(
        USE plato;
        select A, B, C, D, E, F, G, H, I, J, K, L, M, N from (select b from `abc`);
)";

        NYql::TAstParseResult res = SqlToYql(q, 10);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res),
            R"(<main>:3:16: Error: Column A is not in source column set. Did you mean b?
<main>:3:19: Error: Column B is not in source column set. Did you mean b?
<main>:3:22: Error: Column C is not in source column set. Did you mean b?
<main>:3:25: Error: Column D is not in source column set. Did you mean b?
<main>:3:28: Error: Column E is not in source column set. Did you mean b?
<main>:3:31: Error: Column F is not in source column set. Did you mean b?
<main>:3:34: Error: Column G is not in source column set. Did you mean b?
<main>:3:37: Error: Column H is not in source column set. Did you mean b?
<main>:3:40: Error: Column I is not in source column set. Did you mean b?
<main>: Error: Too many issues, code: 1
)");
    };

    Y_UNIT_TEST(ShouldCloneBindingForNamedParameter) {
        NYql::TAstParseResult res = SqlToYql(R"($f = () -> {
    $value_type = TypeOf(1);
    $pair_type = StructType(
        TypeOf("2") AS key,
        $value_type AS value
    );

    RETURN TupleType(
        ListType($value_type),
        $pair_type);
};

select FormatType($f());
)");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(BlockedInvalidFrameBounds) {
        auto check = [](const TString& frame, const TString& err) {
            const TString prefix = "SELECT SUM(x) OVER w FROM plato.Input WINDOW w AS (PARTITION BY key ORDER BY subkey\n";
            NYql::TAstParseResult res = SqlToYql(prefix + frame + ")");
            UNIT_ASSERT(!res.Root);
            UNIT_ASSERT_NO_DIFF(Err2Str(res), err);
        };

        check("ROWS UNBOUNDED FOLLOWING", "<main>:2:5: Error: Frame cannot start from UNBOUNDED FOLLOWING\n");
        check("ROWS BETWEEN 5 PRECEDING AND UNBOUNDED PRECEDING", "<main>:2:29: Error: Frame cannot end with UNBOUNDED PRECEDING\n");
        check("ROWS BETWEEN CURRENT ROW AND 5 PRECEDING", "<main>:2:13: Error: Frame cannot start from CURRENT ROW and end with PRECEDING\n");
        check("ROWS BETWEEN 5 FOLLOWING AND CURRENT ROW", "<main>:2:14: Error: Frame cannot start from FOLLOWING and end with CURRENT ROW\n");
        check("ROWS BETWEEN 5 FOLLOWING AND 5 PRECEDING", "<main>:2:14: Error: Frame cannot start from FOLLOWING and end with PRECEDING\n");
    }

    Y_UNIT_TEST(BlockedRangeValueWithoutSingleOrderBy) {
        UNIT_ASSERT(SqlToYql("SELECT COUNT(*) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM plato.Input").IsOk());
        UNIT_ASSERT(SqlToYql("SELECT COUNT(*) OVER (RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM plato.Input").IsOk());

        auto res = SqlToYql("SELECT COUNT(*) OVER (RANGE 5 PRECEDING) FROM plato.Input");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:29: Error: RANGE with <offset> PRECEDING/FOLLOWING requires exactly one expression in ORDER BY partition clause\n");

        res = SqlToYql("SELECT COUNT(*) OVER (ORDER BY key, value RANGE 5 PRECEDING) FROM plato.Input");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:49: Error: RANGE with <offset> PRECEDING/FOLLOWING requires exactly one expression in ORDER BY partition clause\n");
    }

    Y_UNIT_TEST(NoColumnsInFrameBounds) {
        NYql::TAstParseResult res = SqlToYql(
            "SELECT SUM(x) OVER w FROM plato.Input WINDOW w AS (ROWS BETWEEN\n"
            " 1 + key PRECEDING AND 2 + key FOLLOWING);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:6: Error: Column reference \"key\" is not allowed in current scope\n");
    }

    Y_UNIT_TEST(WarnOnEmptyFrameBounds) {
        NYql::TAstParseResult res = SqlToYql(
            "SELECT SUM(x) OVER w FROM plato.Input WINDOW w AS (PARTITION BY key ORDER BY subkey\n"
            "ROWS BETWEEN 10 FOLLOWING AND 5 FOLLOWING)");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:14: Warning: Used frame specification implies empty window frame, code: 4520\n");
    }

    Y_UNIT_TEST(WarnOnRankWithUnorderedWindow) {
        NYql::TAstParseResult res = SqlToYql("SELECT RANK() OVER w FROM plato.Input WINDOW w AS ()");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Warning: Rank() is used with unordered window - all rows will be considered equal to each other, code: 4521\n");
    }

    Y_UNIT_TEST(WarnOnRankExprWithUnorderedWindow) {
        NYql::TAstParseResult res = SqlToYql("SELECT RANK(key) OVER w FROM plato.Input WINDOW w AS ()");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Warning: Rank(<expression>) is used with unordered window - the result is likely to be undefined, code: 4521\n");
    }

    Y_UNIT_TEST(AnyAsTableName) {
        NYql::TAstParseResult res = SqlToYql("use plato; select * from any;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:28: Error: Unexpected token ';' : syntax error...\n\n");
    }

    Y_UNIT_TEST(IncorrectOrderOfLambdaOptionalArgs) {
        NYql::TAstParseResult res = SqlToYql("$f = ($x?, $y)->($x + $y); select $f(1);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:12: Error: Non-optional argument can not follow optional one\n");
    }

    Y_UNIT_TEST(IncorrectOrderOfActionOptionalArgs) {
        NYql::TAstParseResult res = SqlToYql("define action $f($x?, $y) as select $x,$y; end define; do $f(1);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:23: Error: Non-optional argument can not follow optional one\n");
    }

    Y_UNIT_TEST(NotAllowedQuestionOnNamedNode) {
        NYql::TAstParseResult res = SqlToYql("$f = 1; select $f?;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:18: Error: Unexpected token '?' at the end of expression\n");
    }

    Y_UNIT_TEST(AnyAndCrossJoin) {
        NYql::TAstParseResult res = SqlToYql("use plato; select * from any Input1 cross join Input2");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:26: Error: ANY should not be used with Cross JOIN\n");

        res = SqlToYql("use plato; select * from Input1 cross join any Input2");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:44: Error: ANY should not be used with Cross JOIN\n");
    }

    Y_UNIT_TEST(ErrorPlainEndAsInlineActionTerminator) {
        NYql::TAstParseResult res = SqlToYql(
            "do begin\n"
            "  select 1\n"
            "; end\n");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:4:0: Error: Unexpected token absence : Missing DO \n\n");
    }

    Y_UNIT_TEST(ErrorMultiWayJoinWithUsing) {
        NYql::TAstParseResult res = SqlToYql(
            "USE plato;\n"
            "PRAGMA DisableSimpleColumns;\n"
            "SELECT *\n"
            "FROM Input1 AS a\n"
            "JOIN Input2 AS b USING(key)\n"
            "JOIN Input3 AS c ON a.key = c.key;\n"
        );
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res),
            "<main>:5:24: Error: Multi-way JOINs should be connected with ON clause instead of USING clause\n"
        );
    }

    Y_UNIT_TEST(RequireLabelInFlattenByWithDot) {
        NYql::TAstParseResult res = SqlToYql("select * from plato.Input flatten by x.y");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res),
            "<main>:1:40: Error: Unnamed expression after FLATTEN BY is not allowed\n"
        );
    }

    Y_UNIT_TEST(WarnUnnamedColumns) {
        NYql::TAstParseResult res = SqlToYql(
            "PRAGMA WarnUnnamedColumns;\n"
            "\n"
            "SELECT key, subkey, key || subkey FROM plato.Input ORDER BY subkey;\n");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:28: Warning: Autogenerated column name column2 will be used for expression, code: 4516\n");
    }

    Y_UNIT_TEST(WarnSourceColumnMismatch) {
        NYql::TAstParseResult res = SqlToYql(
            "insert into plato.Output (key, subkey, new_value, one_more_value) select key as Key, subkey, value, \"x\" from plato.Input;");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:51: Warning: Column names in SELECT don't match column specification in parenthesis. \"key\" doesn't match \"Key\". \"new_value\" doesn't match \"value\", code: 4517\n");
    }

    Y_UNIT_TEST(YtCaseInsensitive) {
        NYql::TAstParseResult res = SqlToYql("select * from PlatO.foo;");
        UNIT_ASSERT(res.Root);

        res = SqlToYql("use PlatO; select * from foo;");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(KikimrCaseSensitive) {
        NYql::TAstParseResult res = SqlToYql("select * from PlatO.foo;", 10, "kikimr");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:15: Error: Unknown cluster: PlatO\n");

        res = SqlToYql("use PlatO; select * from foo;", 10, "kikimr");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:5: Error: Unknown cluster: PlatO\n");
    }

    Y_UNIT_TEST(DiscoveryModeForbidden) {
        NYql::TAstParseResult res = SqlToYqlWithMode("insert into plato.Output select * from plato.range(\"\", Input1, Input4)", NSQLTranslation::ESqlMode::DISCOVERY);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:40: Error: range is not allowed in Discovery mode, code: 4600\n");

        res = SqlToYqlWithMode("insert into plato.Output select * from plato.like(\"\", \"Input%\")", NSQLTranslation::ESqlMode::DISCOVERY);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:40: Error: like is not allowed in Discovery mode, code: 4600\n");

        res = SqlToYqlWithMode("insert into plato.Output select * from plato.regexp(\"\", \"Input.\")", NSQLTranslation::ESqlMode::DISCOVERY);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:40: Error: regexp is not allowed in Discovery mode, code: 4600\n");

        res = SqlToYqlWithMode("insert into plato.Output select * from plato.filter(\"\", ($name) -> { return find($name, \"Input\") is not null; })", NSQLTranslation::ESqlMode::DISCOVERY);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:40: Error: filter is not allowed in Discovery mode, code: 4600\n");

        res = SqlToYqlWithMode("select Path from plato.folder(\"\") where Type == \"table\"", NSQLTranslation::ESqlMode::DISCOVERY);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:18: Error: folder is not allowed in Discovery mode, code: 4600\n");
    }

    Y_UNIT_TEST(YsonFuncWithoutArgs) {
        UNIT_ASSERT(SqlToYql("SELECT Yson::SerializeText(Yson::From());").IsOk());
    }

    Y_UNIT_TEST(CanNotUseOrderByInNonLastSelectInUnionAllChain) {
        auto req = "pragma AnsiOrderByLimitInUnionAll;\n"
                   "use plato;\n"
                   "\n"
                   "select * from Input order by key\n"
                   "union all\n"
                   "select * from Input order by key limit 1;";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:4:21: Error: ORDER BY within UNION ALL is only allowed after last subquery\n");
    }

    Y_UNIT_TEST(CanNotUseLimitInNonLastSelectInUnionAllChain) {
        auto req = "pragma AnsiOrderByLimitInUnionAll;\n"
                   "use plato;\n"
                   "\n"
                   "select * from Input limit 1\n"
                   "union all\n"
                   "select * from Input order by key limit 1;";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:4:21: Error: LIMIT within UNION ALL is only allowed after last subquery\n");
    }

    Y_UNIT_TEST(CanNotUseDiscardInNonFirstSelectInUnionAllChain) {
        auto req = "pragma AnsiOrderByLimitInUnionAll;\n"
                   "use plato;\n"
                   "\n"
                   "select * from Input\n"
                   "union all\n"
                   "discard select * from Input;";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:6:1: Error: DISCARD within UNION ALL is only allowed before first subquery\n");
    }

    Y_UNIT_TEST(CanNotUseIntoResultInNonLastSelectInUnionAllChain) {
        auto req = "use plato;\n"
                   "pragma AnsiOrderByLimitInUnionAll;\n"
                   "\n"
                   "select * from Input\n"
                   "union all\n"
                   "discard select * from Input;";

        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:6:1: Error: DISCARD within UNION ALL is only allowed before first subquery\n");
    }

    Y_UNIT_TEST(YsonStrictInvalidPragma) {
        auto res = SqlToYql("pragma yson.Strict = \"wrong\";");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:22: Error: Expected 'true', 'false' or no parameter for: Strict\n");
    }

    Y_UNIT_TEST(WarnTableNameInSomeContexts) {
        UNIT_ASSERT(SqlToYql("use plato; select TableName() from Input;").IsOk());
        UNIT_ASSERT(SqlToYql("use plato; select TableName(\"aaaa\");").IsOk());
        UNIT_ASSERT(SqlToYql("select TableName(\"aaaa\", \"yt\");").IsOk());

        auto res = SqlToYql("select TableName() from plato.Input;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: TableName requires either one of \"yt\"/\"kikimr\"/\"rtmr\" as second argument or current cluster name\n");

        res = SqlToYql("use plato;\n"
                       "select TableName() from Input1 as a join Input2 as b using(key);");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:8: Warning: TableName() may produce empty result when used in ambiguous context (with JOIN), code: 4525\n");

        res = SqlToYql("use plato;\n"
                       "select SOME(TableName()), key from Input group by key;");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:13: Warning: TableName() will produce empty result when used with aggregation.\n"
                                          "Please consult https://yql.yandex-team.ru/docs/yt/builtins/basic/#tablepath for possible workaround, code: 4525\n");
    }

    Y_UNIT_TEST(WarnOnDistincWithHavingWithoutAggregations) {
        auto res = SqlToYql("select distinct key from plato.Input having key != '0';");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:49: Warning: The usage of HAVING without aggregations with SELECT DISTINCT is non-standard and will stop working soon. Please use WHERE instead., code: 4526\n");
    }

    Y_UNIT_TEST(FlattenByExprWithNestedNull) {
        auto res = SqlToYql("USE plato;\n"
                            "\n"
                            "SELECT * FROM (SELECT 1 AS region_id)\n"
                            "FLATTEN BY (\n"
                            "    CAST($unknown(region_id) AS List<String>) AS region\n"
                            ")");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:5:10: Error: Unknown name: $unknown\n");
    }

    Y_UNIT_TEST(EmptySymbolNameIsForbidden) {
        auto req = "    $`` = 1; select $``;";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:5: Error: Empty symbol name is not allowed\n");
    }

    Y_UNIT_TEST(WarnOnBinaryOpWithNullArg) {
        auto req = "select * from plato.Input where cast(key as Int32) != NULL";
        auto res = SqlToYql(req);
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:52: Warning: Binary operation != will return NULL here, code: 4529\n");

        req = "select 1 or null";
        res = SqlToYql(req);
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "");
    }

    Y_UNIT_TEST(ErrorIfTableSampleArgUsesColumns) {
        auto req = "SELECT key FROM plato.Input TABLESAMPLE BERNOULLI(MIN_OF(100.0, CAST(subkey as Int32)));";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:70: Error: Column reference \"subkey\" is not allowed in current scope\n");
    }

    Y_UNIT_TEST(DerivedColumnListForSelectIsNotSupportedYet) {
        auto req = "SELECT a,b,c FROM plato.Input as t(x,y,z);";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:35: Error: Derived column list is only supported for VALUES\n");
    }

    Y_UNIT_TEST(ErrorIfValuesHasDifferentCountOfColumns) {
        auto req = "VALUES (1,2,3), (4,5);";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:17: Error: All VALUES items should have same size: expecting 3, got 2\n");
    }

    Y_UNIT_TEST(ErrorIfDerivedColumnSizeExceedValuesColumnCount) {
        auto req = "SELECT * FROM(VALUES (1,2), (3,4)) as t(x,y,z);";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:40: Error: Derived column list size exceeds column count in VALUES\n");
    }

    Y_UNIT_TEST(WarnoOnAutogeneratedNamesForValues) {
        auto req = "PRAGMA WarnUnnamedColumns;\n"
                   "SELECT * FROM (VALUES (1,2,3,4), (5,6,7,8)) as t(x,y);";
        auto res = SqlToYql(req);
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:16: Warning: Autogenerated column names column2...column3 will be used here, code: 4516\n");
    }

    Y_UNIT_TEST(ErrUnionAllWithOrderByWithoutExplicitLegacyMode) {
        auto req = "use plato;\n"
                   "\n"
                   "select * from Input order by key\n"
                   "union all\n"
                   "select * from Input order by key;";

        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:21: Error: ORDER BY within UNION ALL is only allowed after last subquery\n");
    }

    Y_UNIT_TEST(ErrUnionAllWithLimitWithoutExplicitLegacyMode) {
        auto req = "use plato;\n"
                   "\n"
                   "select * from Input limit 10\n"
                   "union all\n"
                   "select * from Input limit 1;";

        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:21: Error: LIMIT within UNION ALL is only allowed after last subquery\n");
    }

    Y_UNIT_TEST(ErrUnionAllWithIntoResultWithoutExplicitLegacyMode) {
        auto req = "use plato;\n"
                   "\n"
                   "select * from Input into result aaa\n"
                   "union all\n"
                   "select * from Input;";

        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:21: Error: INTO RESULT within UNION ALL is only allowed after last subquery\n");
    }

    Y_UNIT_TEST(ErrUnionAllWithDiscardWithoutExplicitLegacyMode) {
        auto req = "use plato;\n"
                   "\n"
                   "select * from Input\n"
                   "union all\n"
                   "discard select * from Input;";

        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:5:1: Error: DISCARD within UNION ALL is only allowed before first subquery\n");
    }

    Y_UNIT_TEST(ErrUnionAllKeepsIgnoredOrderByWarning) {
        auto req = "use plato;\n"
                   "\n"
                   "SELECT * FROM (\n"
                   "  SELECT * FROM Input\n"
                   "  UNION ALL\n"
                   "  SELECT t.* FROM Input AS t ORDER BY t.key\n"
                   ");";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:4:3: Warning: ORDER BY without LIMIT in subquery will be ignored, code: 4504\n"
                                          "<main>:6:39: Error: Unknown correlation name: t\n");
    }

    Y_UNIT_TEST(InvalidTtl) {
        auto req = R"(
            USE plato;
            CREATE TABLE tableName (Key Uint32, CreatedAt Timestamp, PRIMARY KEY (Key))
            WITH ( TTL = 1 On ExpireAt );
        )";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:4:26: Error: Literal of Interval type is expected for TTL\n"
                                          "<main>:4:26: Error: Invalid TTL settings\n");
    }

    Y_UNIT_TEST(InvalidChangefeedSink) {
        auto req = R"(
            USE plato;
            CREATE TABLE tableName (
                Key Uint32, PRIMARY KEY (Key),
                CHANGEFEED feedName WITH (SINK_TYPE = "S3", MODE = "KEYS_ONLY", FORMAT = "json")
            );
        )";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:5:55: Error: Unknown changefeed sink type: S3\n");
    }

    Y_UNIT_TEST(InvalidChangefeedSettings) {
        auto req = R"(
            USE plato;
            CREATE TABLE tableName (
                Key Uint32, PRIMARY KEY (Key),
                CHANGEFEED feedName WITH (SINK_TYPE = "local", FOO = "bar")
            );
        )";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:5:70: Error: Unknown changefeed setting: FOO\n");
    }

    Y_UNIT_TEST(ErrJoinWithGroupingSetsWithoutCorrelationName) {
        auto req = "USE plato;\n"
                   "\n"
                   "SELECT k1, k2, subkey\n"
                   "FROM T1 AS a JOIN T2 AS b USING (key)\n"
                   "GROUP BY GROUPING SETS(\n"
                   "  (a.key as k1, b.subkey as k2),\n"
                   "  (k1),\n"
                   "  (subkey)\n"
                   ");";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:8:4: Error: Columns in grouping sets should have correlation name, error in key: subkey\n");
    }

    Y_UNIT_TEST(ErrJoinWithGroupByWithoutCorrelationName) {
        auto req = "USE plato;\n"
                   "\n"
                   "SELECT k1, k2,\n"
                   "    value\n"
                   "FROM T1 AS a JOIN T2 AS b USING (key)\n"
                   "GROUP BY a.key as k1, b.subkey as k2,\n"
                   "    value;";
        ExpectFailWithError(req,
            "<main>:7:5: Error: Columns in GROUP BY should have correlation name, error in key: value\n");
    }

    Y_UNIT_TEST(ErrWithMissingFrom) {
        auto req = "select 1 as key where 1 > 1;";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:25: Error: Filtering is not allowed without FROM\n");

        req = "select 1 + count(*);";
        res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:12: Error: Aggregation is not allowed without FROM\n");

        req = "select 1 as key, subkey + value;";
        res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: Column references are not allowed without FROM\n"
                                          "<main>:1:18: Error: Column reference 'subkey'\n"
                                          "<main>:1:1: Error: Column references are not allowed without FROM\n"
                                          "<main>:1:27: Error: Column reference 'value'\n");

        req = "select count(1) group by key;";
        res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: Column references are not allowed without FROM\n"
                                          "<main>:1:26: Error: Column reference 'key'\n");
    }

    Y_UNIT_TEST(ErrWithMissingFromForWindow) {
        auto req = "$c = () -> (1 + count(1) over w);\n"
                   "select $c();";
        ExpectFailWithError(req,
            "<main>:1:9: Error: Window and aggregation functions are not allowed in this context\n"
            "<main>:1:17: Error: Failed to use aggregation function Count without window specification or in wrong place\n");

        req = "$c = () -> (1 + lead(1) over w);\n"
              "select $c();";
        ExpectFailWithError(req,
            "<main>:1:17: Error: Window functions are not allowed in this context\n"
            "<main>:1:17: Error: Failed to use window function Lead without window specification or in wrong place\n");

        req = "select 1 + count(1) over w window w as ();";
        ExpectFailWithError(req,
            "<main>:1:1: Error: Window and aggregation functions are not allowed without FROM\n"
            "<main>:1:12: Error: Failed to use aggregation function Count without window specification or in wrong place\n");

        req = "select 1 + lead(1) over w window w as ();";
        ExpectFailWithError(req,
            "<main>:1:12: Error: Window functions are not allowed without FROM\n"
            "<main>:1:12: Error: Failed to use window function Lead without window specification or in wrong place\n");
    }

    Y_UNIT_TEST(ErrWithMissingFromForInplaceWindow) {
        auto req = "$c = () -> (1 + count(1) over ());\n"
                   "select $c();";
        ExpectFailWithError(req,
            "<main>:1:26: Error: Window and aggregation functions are not allowed in this context\n");

        req = "$c = () -> (1 + lead(1) over (rows between unbounded preceding and current row));\n"
              "select $c();";
        ExpectFailWithError(req,
            "<main>:1:25: Error: Window and aggregation functions are not allowed in this context\n");

        req = "select 1 + count(1) over ();";
        ExpectFailWithError(req,
            "<main>:1:1: Error: Window and aggregation functions are not allowed without FROM\n"
            "<main>:1:12: Error: Failed to use aggregation function Count without window specification or in wrong place\n");

        req = "select 1 + lead(1) over (rows between current row and unbounded following);";
        ExpectFailWithError(req,
            "<main>:1:12: Error: Window functions are not allowed without FROM\n"
            "<main>:1:12: Error: Failed to use window function Lead without window specification or in wrong place\n");
    }

    Y_UNIT_TEST(ErrDistinctInWrongPlace) {
        auto req = "select Some::Udf(distinct key) from plato.Input;";
        ExpectFailWithError(req,
            "<main>:1:18: Error: DISTINCT can only be used in aggregation functions\n");
        req = "select sum(key)(distinct foo) from plato.Input;";
        ExpectFailWithError(req,
            "<main>:1:17: Error: DISTINCT can only be used in aggregation functions\n");

        req = "select len(distinct foo) from plato.Input;";
        ExpectFailWithError(req,
            "<main>:1:8: Error: DISTINCT can only be used in aggregation functions\n");

        req = "$foo = ($x) -> ($x); select $foo(distinct key) from plato.Input;";
        ExpectFailWithError(req,
            "<main>:1:34: Error: DISTINCT can only be used in aggregation functions\n");
    }

    Y_UNIT_TEST(ErrForNotSingleChildInInlineAST) {
        ExpectFailWithError("select YQL::\"\"",
            "<main>:1:8: Error: Failed to parse YQL: expecting AST root node with single child, but got 0\n");
        ExpectFailWithError("select YQL::@@  \t@@",
            "<main>:1:8: Error: Failed to parse YQL: expecting AST root node with single child, but got 0\n");
        auto req = "$lambda = YQL::@@(lambda '(x)(+ x x)) (lambda '(y)(+ y y))@@;\n"
                   "select ListMap([1, 2, 3], $lambda);";
        ExpectFailWithError(req,
            "<main>:1:11: Error: Failed to parse YQL: expecting AST root node with single child, but got 2\n");
    }

    Y_UNIT_TEST(ErrEmptyColumnName) {
        ExpectFailWithError("select * without \"\" from plato.Input",
            "<main>:1:18: Error: String literal can not be used here\n");

        ExpectFailWithError("select * without `` from plato.Input;",
            "<main>:1:18: Error: Empty column name is not allowed\n");

        ExpectFailWithErrorForAnsiLexer("select * without \"\" from plato.Input",
            "<main>:1:18: Error: Empty column name is not allowed\n");

        ExpectFailWithErrorForAnsiLexer("select * without `` from plato.Input;",
            "<main>:1:18: Error: Empty column name is not allowed\n");
    }

    Y_UNIT_TEST(ErrOnNonZeroArgumentsForTableRows) {
        ExpectFailWithError("$udf=\"\";process plato.Input using $udf(TableRows(k))",
            "<main>:1:40: Error: TableRows requires exactly 0 arguments\n");
    }

    Y_UNIT_TEST(ErrGroupByWithAggregationFunctionAndDistinctExpr) {
        ExpectFailWithError("select * from plato.Input group by count(distinct key|key)",
            "<main>:1:36: Error: Unable to GROUP BY aggregated values\n");
    }

    // FIXME: check if we can get old behaviour
#if 0
    Y_UNIT_TEST(ErrWithSchemaWithColumnsWithoutType) {
        ExpectFailWithError("select * from plato.Input with COLUMNs",
            "<main>:1:32: Error: Expected type after COLUMNS\n"
            "<main>:1:32: Error: Failed to parse table hints\n");

        ExpectFailWithError("select * from plato.Input with scheMa",
            "<main>:1:32: Error: Expected type after SCHEMA\n"
            "<main>:1:32: Error: Failed to parse table hints\n");
    }
#endif

    Y_UNIT_TEST(ErrCollectPreaggregatedInListLiteralWithoutFrom) {
        ExpectFailWithError("SELECT([VARIANCE(DISTINCT[])])",
            "<main>:1:1: Error: Column references are not allowed without FROM\n"
            "<main>:1:9: Error: Column reference '_yql_preagg_Variance0'\n");
    }

    Y_UNIT_TEST(ErrGroupBySmartParenAsTuple) {
        ExpectFailWithError("SELECT * FROM plato.Input GROUP BY (k, v,)",
            "<main>:1:36: Error: Unable to use tuple in group by clause\n");
    }

    Y_UNIT_TEST(HandleNestedSmartParensInGroupBy) {
        ExpectFailWithError("SELECT * FROM plato.Input GROUP BY (+() as k)",
            "<main>:1:37: Error: Unable to GROUP BY constant expression\n");
    }

    Y_UNIT_TEST(ErrRenameWithAddColumn) {
        ExpectFailWithError("USE plato; ALTER TABLE table RENAME TO moved, ADD COLUMN addc uint64",
                            "<main>:1:40: Error: RENAME TO can not be used together with another table action\n");
    }

    Y_UNIT_TEST(ErrAddColumnAndRename) {
        // FIXME: fix positions in ALTER TABLE
        ExpectFailWithError("USE plato; ALTER TABLE table ADD COLUMN addc uint64, RENAME TO moved",
                            "<main>:1:46: Error: RENAME TO can not be used together with another table action\n");
    }

    Y_UNIT_TEST(InvalidUuidValue) {
        ExpectFailWithError("SELECT Uuid('123e4567ae89ba12d3aa456a426614174ab0')",
                            "<main>:1:8: Error: Invalid value \"123e4567ae89ba12d3aa456a426614174ab0\" for type Uuid\n");
        ExpectFailWithError("SELECT Uuid('123e4567ae89b-12d3-a456-426614174000')",
                            "<main>:1:8: Error: Invalid value \"123e4567ae89b-12d3-a456-426614174000\" for type Uuid\n");
    }

    Y_UNIT_TEST(WindowFunctionWithoutOver) {
        ExpectFailWithError("SELECT LAST_VALUE(foo) FROM plato.Input",
                            "<main>:1:8: Error: Can't use window function LastValue without window specification (OVER keyword is missing)\n");
        ExpectFailWithError("SELECT LAST_VALUE(foo) FROM plato.Input GROUP BY key",
                            "<main>:1:8: Error: Can't use window function LastValue without window specification (OVER keyword is missing)\n");
    }

    Y_UNIT_TEST(CreateAlterUserWithoutCluster) {
        ExpectFailWithError("\n CREATE USER user ENCRYPTED PASSWORD 'foobar';", "<main>:2:2: Error: USE statement is missing - no default cluster is selected\n");
        ExpectFailWithError("ALTER USER CURRENT_USER RENAME TO $foo;", "<main>:1:1: Error: USE statement is missing - no default cluster is selected\n");
    }

    Y_UNIT_TEST(ReservedRoleNames) {
        ExpectFailWithError("USE plato; CREATE USER current_User;", "<main>:1:24: Error: System role CURRENT_USER can not be used here\n");
        ExpectFailWithError("USE plato; ALTER USER current_User RENAME TO Current_role", "<main>:1:46: Error: System role CURRENT_ROLE can not be used here\n");
        UNIT_ASSERT(SqlToYql("USE plato; DROP GROUP IF EXISTS a, b, c, current_User;").IsOk());
    }

    Y_UNIT_TEST(DisableClassicDivisionWithError) {
        ExpectFailWithError("pragma ClassicDivision = 'false'; select $foo / 30;", "<main>:1:42: Error: Unknown name: $foo\n");
    }

    Y_UNIT_TEST(AggregationOfAgrregatedDistinctExpr) {
        ExpectFailWithError("select sum(sum(distinct x + 1)) from plato.Input", "<main>:1:12: Error: Aggregation of aggregated values is forbidden\n");
    }

    Y_UNIT_TEST(WarnForUnusedSqlHint) {
        NYql::TAstParseResult res = SqlToYql("select * from plato.Input1 as a join /*+ merge() */ plato.Input2 as b using(key);\n"
                                             "select --+            foo(bar)\n"
                                             "       1;");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:23: Warning: Hint foo will not be used, code: 4534\n");
    }

    Y_UNIT_TEST(WarnForDeprecatedSchema) {
        NYql::TAstParseResult res = SqlToYql("select * from plato.T with schema (col1 Int32, String as col2, Int64 as col3);");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:48: Warning: Deprecated syntax for positional schema: please use 'column type' instead of 'type AS column', code: 4535\n");
    }

    Y_UNIT_TEST(ErrorOnColumnNameInMaxByLimit) {
        ExpectFailWithError(
            "SELECT AGGREGATE_BY(AsTuple(value, key), AggregationFactory(\"MAX_BY\", subkey)) FROM plato.Input;",
            "<main>:1:42: Error: Source does not allow column references\n"
            "<main>:1:71: Error: Column reference 'subkey'\n");
    }
}

void CheckUnused(const TString& req, const TString& symbol, unsigned row, unsigned col) {
    auto res = SqlToYql(req);

    UNIT_ASSERT(res.Root);
    UNIT_ASSERT_NO_DIFF(Err2Str(res), TStringBuilder() << "<main>:" << row << ":" << col << ": Warning: Symbol " << symbol << " is not used, code: 4527\n");
}

Y_UNIT_TEST_SUITE(WarnUnused) {
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
                      "for $i in ListFromRange(1, 10)\n"
                      "do $a();";
        CheckUnused(req, "$i", 5, 5);
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
}

Y_UNIT_TEST_SUITE(AnonymousNames) {
    Y_UNIT_TEST(ReferenceAnonymousVariableIsForbidden) {
        auto req = "$_ = 1; select $_;";

        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:16: Error: Unable to reference anonymous name $_\n");

        req = "$`_` = 1; select $`_`;";
        res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:18: Error: Unable to reference anonymous name $_\n");
    }

    Y_UNIT_TEST(Declare) {
        auto req = "declare $_ as String;";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:9: Error: Can not use anonymous name '$_' in DECLARE statement\n");
    }

    Y_UNIT_TEST(ActionSubquery) {
        auto req = "define action $_() as select 1; end define;";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:15: Error: Can not use anonymous name '$_' as ACTION name\n");

        req = "define subquery $_() as select 1; end define;";
        res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:17: Error: Can not use anonymous name '$_' as SUBQUERY name\n");
    }

    Y_UNIT_TEST(Import) {
        auto req = "import lib symbols $sqr as $_;";
        auto res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:28: Error: Can not import anonymous name $_\n");
    }

    Y_UNIT_TEST(Export) {
        auto req = "export $_;";
        auto res = SqlToYqlWithMode(req, NSQLTranslation::ESqlMode::LIBRARY);
        UNIT_ASSERT(!res.Root);
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
}

Y_UNIT_TEST_SUITE(JsonValue) {
    Y_UNIT_TEST(JsonValueArgumentCount) {
        NYql::TAstParseResult res = SqlToYql("select JSON_VALUE(CAST(@@{\"key\": 1238}@@ as Json));");

        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:49: Error: Unexpected token ')' : syntax error...\n\n");
    }

    Y_UNIT_TEST(JsonValueJsonPathMustBeLiteralString) {
        NYql::TAstParseResult res = SqlToYql("$jsonPath = \"strict $.key\"; select JSON_VALUE(CAST(@@{\"key\": 1238}@@ as Json), $jsonPath);");

        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:79: Error: Unexpected token absence : Missing STRING_VALUE \n\n");
    }

    Y_UNIT_TEST(JsonValueTranslation) {
        NYql::TAstParseResult res = SqlToYql("select JSON_VALUE(CAST(@@{\"key\": 1238}@@ as Json), \"strict $.key\");");

        UNIT_ASSERT(res.Root);

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
                TStringBuilder() << "select JSON_VALUE(CAST(@@{\"key\": 1238}@@ as Json), \"strict $.key\" RETURNING " << typeName << ");"
            );

            UNIT_ASSERT(res.Root);

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

        UNIT_ASSERT(!res.Root);
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
            "SELECT $returning as returning;\n"
            "SELECT returning FROM InputSyntax;\n"
            "SELECT returning, count(*) FROM InputSyntax GROUP BY returning;\n"
        );

        UNIT_ASSERT(res.Root);
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
                if (!onEmpty.first.Empty()) {
                    query << " " << onEmpty.first << " ON EMPTY";
                }
                if (!onError.first.Empty()) {
                    query << " " << onError.first << " ON ERROR";
                }
                query << ");\n";

                NYql::TAstParseResult res = SqlToYql(query);

                UNIT_ASSERT(res.Root);

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
            "select JSON_VALUE(CAST(@@{\"key\": 1238}@@ as Json), \"strict $.key\" NULL ON EMPTY NULL ON ERROR NULL ON EMPTY);\n"
        );

        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(
            Err2Str(res),
            "<main>:1:52: Error: Only 1 ON EMPTY and/or 1 ON ERROR clause is expected\n"
        );
    }

    Y_UNIT_TEST(JsonValueTooManyOnEmpty) {
        NYql::TAstParseResult res = SqlToYql(
            "select JSON_VALUE(CAST(@@{\"key\": 1238}@@ as Json), \"strict $.key\" NULL ON EMPTY NULL ON EMPTY);\n"
        );

        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(
            Err2Str(res),
            "<main>:1:52: Error: Only 1 ON EMPTY clause is expected\n"
        );
    }

    Y_UNIT_TEST(JsonValueTooManyOnError) {
        NYql::TAstParseResult res = SqlToYql(
            "select JSON_VALUE(CAST(@@{\"key\": 1238}@@ as Json), \"strict $.key\" NULL ON ERROR NULL ON ERROR);\n"
        );

        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(
            Err2Str(res),
            "<main>:1:52: Error: Only 1 ON ERROR clause is expected\n"
        );
    }

    Y_UNIT_TEST(JsonValueOnEmptyAfterOnError) {
        NYql::TAstParseResult res = SqlToYql(
            "select JSON_VALUE(CAST(@@{\"key\": 1238}@@ as Json), \"strict $.key\" NULL ON ERROR NULL ON EMPTY);\n"
        );

        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(
            Err2Str(res),
            "<main>:1:52: Error: ON EMPTY clause must be before ON ERROR clause\n"
        );
    }

    Y_UNIT_TEST(JsonValueNullInput) {
        NYql::TAstParseResult res = SqlToYql(R"(SELECT JSON_VALUE(NULL, "strict $.key");)");

        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
            Y_UNUSED(word);
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("(Nothing (OptionalType (DataType 'Json)))"));
        };

        TWordCountHive elementStat({"JsonValue"});
        VerifyProgram(res, elementStat, verifyLine);
        UNIT_ASSERT(elementStat["JsonValue"] > 0);
    }
}

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
                SELECT JSON_EXISTS($json, "strict $.key" )" << item.first << ");\n"
            );

            UNIT_ASSERT(res.Root);

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

        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:4:53: Error: Unexpected token absence : Missing RPAREN \n\n");
    }

    Y_UNIT_TEST(JsonExistsNullInput) {
        NYql::TAstParseResult res = SqlToYql(R"(SELECT JSON_EXISTS(NULL, "strict $.key");)");

        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
            Y_UNUSED(word);
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("(Nothing (OptionalType (DataType 'Json)))"));
        };

        TWordCountHive elementStat({"JsonExists"});
        VerifyProgram(res, elementStat, verifyLine);
        UNIT_ASSERT(elementStat["JsonExists"] > 0);
    }
}

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
                    SELECT JSON_QUERY($json, "strict $.key" )" << wrap.first;
                    if (!onEmpty.first.Empty()) {
                        if (wrap.first.StartsWith("WITH ")) {
                            continue;
                        }
                        query << " " << onEmpty.first << " ON EMPTY";
                    }
                    if (!onError.first.Empty()) {
                        query << " " << onError.first << " ON ERROR";
                    }
                    query << ");\n";

                    NYql::TAstParseResult res = SqlToYql(query);

                    UNIT_ASSERT(res.Root);

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

        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:38: Error: ON EMPTY is prohibited because WRAPPER clause is specified\n");
    }

    Y_UNIT_TEST(JsonQueryNullInput) {
        NYql::TAstParseResult res = SqlToYql(R"(SELECT JSON_QUERY(NULL, "strict $.key");)");

        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
            Y_UNUSED(word);
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("(Nothing (OptionalType (DataType 'Json)))"));
        };

        TWordCountHive elementStat({"JsonQuery"});
        VerifyProgram(res, elementStat, verifyLine);
        UNIT_ASSERT(elementStat["JsonQuery"] > 0);
    }
}

Y_UNIT_TEST_SUITE(JsonPassing) {
    Y_UNIT_TEST(SupportedVariableTypes) {
        const TVector<TString> functions = {"JSON_EXISTS", "JSON_VALUE", "JSON_QUERY"};

        for (const auto& function : functions) {
            const auto query = Sprintf(R"(
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
                function.data()
            );
            NYql::TAstParseResult res = SqlToYql(query);

            UNIT_ASSERT(res.Root);

            TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
                Y_UNUSED(word);
                UNIT_ASSERT_VALUES_UNEQUAL_C(TString::npos, line.find(R"('('"var1" (String '"string")))"), "Cannot find `var1`");
                UNIT_ASSERT_VALUES_UNEQUAL_C(TString::npos, line.find(R"('('"var2" (Double '"1.234")))"), "Cannot find `var2`");
                UNIT_ASSERT_VALUES_UNEQUAL_C(TString::npos, line.find(R"('('"var3" (SafeCast (Int32 '"1") (DataType 'Int64))))"), "Cannot find `var3`");
                UNIT_ASSERT_VALUES_UNEQUAL_C(TString::npos, line.find(R"('('"var4" (Bool '"true")))"), "Cannot find `var4`");
                UNIT_ASSERT_VALUES_UNEQUAL_C(TString::npos, line.find(R"('('"var5" (SafeCast (String '@@{"key": 1238}@@) (DataType 'Json))))"), "Cannot find `var5`");
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
                function.data()
            );
            NYql::TAstParseResult res = SqlToYql(query);

            UNIT_ASSERT(res.Root);

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
}

Y_UNIT_TEST_SUITE(MigrationToJsonApi) {
    Y_UNIT_TEST(WarningOnDeprecatedJsonUdf) {
        NYql::TAstParseResult res = SqlToYql(R"(
            $json = CAST(@@{"key": 1234}@@ as Json);
            SELECT Json::Parse($json);
        )");

        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:26: Warning: Json UDF is deprecated. Please use JSON API instead, code: 4506\n");
    }
}

Y_UNIT_TEST_SUITE(AnsiIdentsNegative) {
    Y_UNIT_TEST(EnableAnsiLexerFromRequestSpecialComments) {
        auto req = "\n"
                   "\t --!ansi_lexer \n"
                   "-- Some comment\n"
                   "-- another comment\n"
                   "pragma SimpleColumns;\n"
                   "\n"
                   "select 1, '''' as empty;";

        auto res = SqlToYql(req);
        UNIT_ASSERT(res.IsOk());
        UNIT_ASSERT(res.Issues.Size() == 0);
    }

    Y_UNIT_TEST(AnsiLexerShouldNotBeEnabledHere) {
        auto req = "$str = '\n"
                   "--!ansi_lexer\n"
                   "--!syntax_v1\n"
                   "';\n"
                   "\n"
                   "select 1, $str, \"\" as empty;";
        auto res = SqlToYql(req);
        UNIT_ASSERT(res.IsOk());
        UNIT_ASSERT(res.Issues.Size() == 0);
    }

    Y_UNIT_TEST(DoubleQuotesInDictsTuplesOrLists) {
        auto req = "$d = { 'a': 1, \"b\": 2, 'c': 3,};";

        auto res = SqlToYqlWithAnsiLexer(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:16: Error: Column reference \"b\" is not allowed in current scope\n");

        req = "$t = (1, 2, \"a\");";

        res = SqlToYqlWithAnsiLexer(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:13: Error: Column reference \"a\" is not allowed in current scope\n");

        req = "$l = ['a', 'b', \"c\"];";

        res = SqlToYqlWithAnsiLexer(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:17: Error: Column reference \"c\" is not allowed in current scope\n");
    }

    Y_UNIT_TEST(MultilineComments) {
        auto req = "/*/**/ select 1;";
        auto res = SqlToYql(req);
        UNIT_ASSERT(res.Root);
        res = SqlToYqlWithAnsiLexer(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:16: Error: Unexpected character : syntax error...\n\n");

        req = "/*\n"
              "--/*\n"
              "*/ select 1;";
        res = SqlToYql(req);
        UNIT_ASSERT(res.Root);
        res = SqlToYqlWithAnsiLexer(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:12: Error: Unexpected character : syntax error...\n\n");

        req = "/*\n"
              "/*\n"
              "--*/\n"
              "*/ select 1;";
        res = SqlToYql(req);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:4:0: Error: Unexpected token '*' : cannot match to any predicted input...\n\n");
        res = SqlToYqlWithAnsiLexer(req);
        UNIT_ASSERT(res.Root);
    }
}

Y_UNIT_TEST_SUITE(AnsiOptionalAs) {
    Y_UNIT_TEST(OptionalAsInProjection) {
        UNIT_ASSERT(SqlToYql("PRAGMA AnsiOptionalAs; SELECT a b, c FROM plato.Input;").IsOk());
        ExpectFailWithError("PRAGMA DisableAnsiOptionalAs;\n"
                            "SELECT a b, c FROM plato.Input;",
                            "<main>:2:10: Error: Expecting mandatory AS here. Did you miss comma? Please add PRAGMA AnsiOptionalAs; for ANSI compatibility\n");
    }
}

Y_UNIT_TEST_SUITE(SessionWindowNegative) {
    Y_UNIT_TEST(SessionWindowWithoutSource) {
        ExpectFailWithError("SELECT 1 + SessionWindow(ts, 32);",
            "<main>:1:12: Error: SessionWindow requires data source\n");
    }

    Y_UNIT_TEST(SessionWindowInProjection) {
        ExpectFailWithError("SELECT 1 + SessionWindow(ts, 32) from plato.Input;",
            "<main>:1:12: Error: SessionWindow can only be used as a top-level GROUP BY / PARTITION BY expression\n");
    }

    Y_UNIT_TEST(SessionWindowWithNonConstSecondArg) {
        ExpectFailWithError(
            "SELECT key, session_start FROM plato.Input\n"
            "GROUP BY SessionWindow(ts, 32 + subkey) as session_start, key;",

            "<main>:2:10: Error: Source does not allow column references\n"
            "<main>:2:33: Error: Column reference 'subkey'\n");
    }

    Y_UNIT_TEST(SessionWindowWithWrongNumberOfArgs) {
        ExpectFailWithError("SELECT * FROM plato.Input GROUP BY SessionWindow()",
            "<main>:1:36: Error: SessionWindow requires either two or four arguments\n");
        ExpectFailWithError("SELECT * FROM plato.Input GROUP BY SessionWindow(key, subkey, 100)",
            "<main>:1:36: Error: SessionWindow requires either two or four arguments\n");
    }

    Y_UNIT_TEST(DuplicateSessionWindow) {
        ExpectFailWithError(
            "SELECT\n"
            "    *\n"
            "FROM plato.Input\n"
            "GROUP BY\n"
            "    SessionWindow(ts, 10),\n"
            "    user,\n"
            "    SessionWindow(ts, 20)\n"
            ";",

            "<main>:7:5: Error: Duplicate session window specification:\n"
            "<main>:5:5: Error: Previous session window is declared here\n");

        ExpectFailWithError(
            "SELECT\n"
            "    MIN(key) over w\n"
            "FROM plato.Input\n"
            "WINDOW w AS (\n"
            "    PARTITION BY SessionWindow(ts, 10), user,\n"
            "    SessionWindow(ts, 20)\n"
            ");",

            "<main>:6:5: Error: Duplicate session window specification:\n"
            "<main>:5:18: Error: Previous session window is declared here\n");
    }

    Y_UNIT_TEST(SessionStartStateWithoutSource) {
        ExpectFailWithError("SELECT 1 + SessionStart();",
            "<main>:1:12: Error: SessionStart requires data source\n");
        ExpectFailWithError("SELECT 1 + SessionState();",
            "<main>:1:12: Error: SessionState requires data source\n");
    }

    Y_UNIT_TEST(SessionStartStateWithoutGroupByOrWindow) {
        ExpectFailWithError("SELECT 1 + SessionStart() from plato.Input;",
            "<main>:1:12: Error: SessionStart can not be used without aggregation by SessionWindow\n");
        ExpectFailWithError("SELECT 1 + SessionState() from plato.Input;",
            "<main>:1:12: Error: SessionState can not be used without aggregation by SessionWindow\n");
    }

    Y_UNIT_TEST(SessionStartStateWithGroupByWithoutSession) {
        ExpectFailWithError("SELECT 1 + SessionStart() from plato.Input group by user;",
            "<main>:1:12: Error: SessionStart can not be used here: SessionWindow specification is missing in GROUP BY\n");
        ExpectFailWithError("SELECT 1 + SessionState() from plato.Input group by user;",
            "<main>:1:12: Error: SessionState can not be used here: SessionWindow specification is missing in GROUP BY\n");
    }

    Y_UNIT_TEST(SessionStartStateWithoutOverWithWindowWithoutSession) {
        ExpectFailWithError("SELECT 1 + SessionStart(), MIN(key) over w from plato.Input window w as ()",
            "<main>:1:12: Error: SessionStart can not be used without aggregation by SessionWindow. Maybe you forgot to add OVER `window_name`?\n");
        ExpectFailWithError("SELECT 1 + SessionState(), MIN(key) over w from plato.Input window w as ()",
            "<main>:1:12: Error: SessionState can not be used without aggregation by SessionWindow. Maybe you forgot to add OVER `window_name`?\n");
    }

    Y_UNIT_TEST(SessionStartStateWithWindowWithoutSession) {
        ExpectFailWithError("SELECT 1 + SessionStart() over w, MIN(key) over w from plato.Input window w as ()",
            "<main>:1:12: Error: SessionStart can not be used with window w: SessionWindow specification is missing in PARTITION BY\n");
        ExpectFailWithError("SELECT 1 + SessionState() over w, MIN(key) over w from plato.Input window w as ()",
            "<main>:1:12: Error: SessionState can not be used with window w: SessionWindow specification is missing in PARTITION BY\n");
    }

    Y_UNIT_TEST(SessionStartStateWithSessionedWindow) {
        ExpectFailWithError("SELECT 1 + SessionStart(), MIN(key) over w from plato.Input group by key window w as (partition by SessionWindow(ts, 1)) ",
            "<main>:1:12: Error: SessionStart can not be used here: SessionWindow specification is missing in GROUP BY. Maybe you forgot to add OVER `window_name`?\n");
        ExpectFailWithError("SELECT 1 + SessionState(), MIN(key) over w from plato.Input group by key window w as (partition by SessionWindow(ts, 1)) ",
            "<main>:1:12: Error: SessionState can not be used here: SessionWindow specification is missing in GROUP BY. Maybe you forgot to add OVER `window_name`?\n");
    }

    Y_UNIT_TEST(AggregationBySessionStateIsNotSupportedYet) {
        ExpectFailWithError("SELECT SOME(1 + SessionState()), key from plato.Input group by key, SessionWindow(ts, 1);",
            "<main>:1:17: Error: SessionState with GROUP BY is not supported yet\n");
    }

    Y_UNIT_TEST(SessionWindowInRtmr) {
        NYql::TAstParseResult res = SqlToYql(
            "SELECT * FROM plato.Input GROUP BY SessionWindow(ts, 10);",
            10, TString(NYql::RtmrProviderName));
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:54: Error: Streaming group by query must have a hopping window specification.\n");

        res = SqlToYql(R"(
            SELECT key, SUM(value) AS value FROM plato.Input
            GROUP BY key, HOP(subkey, "PT10S", "PT30S", "PT20S"), SessionWindow(ts, 10);
        )", 10, TString(NYql::RtmrProviderName));

        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:13: Error: SessionWindow is unsupported for streaming sources\n");
    }
}

Y_UNIT_TEST_SUITE(LibraSqlSugar) {
    auto makeResult = [](TStringBuf settings) {
        return SqlToYql(
            TStringBuilder()
                << settings
                << "\n$udf1 = MyLibra::MakeLibraPreprocessor($settings);"
                << "\n$udf2 = CustomLibra::MakeLibraPreprocessor($settings);"
                << "\nPROCESS plato.Input USING $udf1(TableRow())"
                << "\nUNION ALL"
                << "\nPROCESS plato.Input USING $udf2(TableRow());"
        );
    };

    Y_UNIT_TEST(EmptySettings) {
        auto res = makeResult(R"(
            $settings = AsStruct();
        )");
        UNIT_ASSERT(res.IsOk());
    }

    Y_UNIT_TEST(OnlyEntities) {
        auto res = makeResult(R"(
            $settings = AsStruct(
                AsList("A", "B", "C") AS Entities
            );
        )");
        UNIT_ASSERT(res.IsOk());
    }

    Y_UNIT_TEST(EntitiesWithStrategy) {
        auto res = makeResult(R"(
            $settings = AsStruct(
                AsList("A", "B", "C") AS Entities,
                "blacklist" AS EntitiesStrategy
            );
        )");
        UNIT_ASSERT(res.IsOk());
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
        UNIT_ASSERT(res.IsOk());
    }

    Y_UNIT_TEST(BadStrategy) {
        auto res = makeResult(R"(
            $settings = AsStruct("bad" AS EntitiesStrategy);
        )");
        UNIT_ASSERT_STRING_CONTAINS(
            Err2Str(res),
            "Error: MakeLibraPreprocessor got invalid entities strategy: expected 'whitelist' or 'blacklist'"
        );
    }

    Y_UNIT_TEST(BadEntities) {
        auto res = makeResult(R"(
            $settings = AsStruct(AsList("A", 1) AS Entities);
        )");
        UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "Error: MakeLibraPreprocessor entity must be string literal");
    }
}

Y_UNIT_TEST_SUITE(TrailingQuestionsNegative) {
    Y_UNIT_TEST(Basic) {
        ExpectFailWithError("SELECT 1?;", "<main>:1:9: Error: Unexpected token '?' at the end of expression\n");
        ExpectFailWithError("SELECT 1? + 1;", "<main>:1:10: Error: Unexpected token '+' : cannot match to any predicted input...\n\n");
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
}

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
}

Y_UNIT_TEST_SUITE(ExternalDeclares) {
    Y_UNIT_TEST(BasicUsage) {
        NSQLTranslation::TTranslationSettings settings;
        settings.DeclaredNamedExprs["foo"] = "String";
        auto res = SqlToYqlWithSettings("select $foo;", settings);
        UNIT_ASSERT(res.IsOk());
        UNIT_ASSERT(res.Issues.Size() == 0);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "declare") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(R"__((declare $foo (DataType 'String)))__"));
            }
        };

        TWordCountHive elementStat = {{TString("declare"), 0}};
        VerifyProgram(res, elementStat, verifyLine);

        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["declare"]);
    }

    Y_UNIT_TEST(NoDeclareOverrides) {
        NSQLTranslation::TTranslationSettings settings;
        settings.DeclaredNamedExprs["foo"] = "String";
        auto res = SqlToYqlWithSettings("declare $foo as Int32; select $foo;", settings);
        UNIT_ASSERT(res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:9: Warning: Duplicate declaration of '$foo' will be ignored, code: 4536\n");

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "declare") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(R"__((declare $foo (DataType 'String)))__"));
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
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res),
            "<main>:0:5: Error: Unknown type: 'BadType'\n"
            "<main>: Error: Failed to parse type for externally declared name 'foo'\n");
    }
}

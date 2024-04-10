#include "sql.h"

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <util/generic/map.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/split.h>

using namespace NSQLTranslationV0;

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

NYql::TAstParseResult SqlToYqlWithMode(const TString& query, NSQLTranslation::ESqlMode mode = NSQLTranslation::ESqlMode::QUERY, size_t maxErrors = 10, const TString& provider = {}, EDebugOutput debug = EDebugOutput::None) {
    google::protobuf::Arena arena;
    const auto service = provider ? provider : TString(NYql::YtProviderName);
    const TString cluster = "plato";
    NSQLTranslation::TTranslationSettings settings;
    settings.ClusterMapping[cluster] = service;
    settings.MaxErrors = maxErrors;
    settings.Mode = mode;
    settings.Arena = &arena;
    settings.V0Behavior = NSQLTranslation::EV0Behavior::Report;
    settings.WarnOnV0 = false;
    auto res = SqlToYql(query, settings);
    if (debug == EDebugOutput::ToCerr) {
        Err2Str(res, debug);
    }
    return res;
}

NYql::TAstParseResult SqlToYql(const TString& query, size_t maxErrors = 10, const TString& provider = {}, EDebugOutput debug = EDebugOutput::None) {
    return SqlToYqlWithMode(query, NSQLTranslation::ESqlMode::QUERY, maxErrors, provider, debug);
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
            if (line.find(word) != TString::npos) {
                ++counterIter.second;
                if (verifyLine) {
                    verifyLine(word, line);
                }
            }
        }
    }
    return programm;
}

Y_UNIT_TEST_SUITE(SqlParsingOnly) {
    Y_UNIT_TEST(V0) {
        NYql::TAstParseResult res = SqlToYql("select null;");
        UNIT_ASSERT(res.Root);
        const auto programm = GetPrettyPrint(res);
        UNIT_ASSERT(TString::npos != programm.find("(Configure! world (DataSource '\"config\") '\"SQL\" '\"0\"))"));
    }

    Y_UNIT_TEST(TableHints) {
        UNIT_ASSERT(SqlToYql("SELECT * FROM plato.Input WITH INFER_SCHEME").IsOk());
        UNIT_ASSERT(SqlToYql("SELECT * FROM plato.Input WITH (INFER_SCHEME)").IsOk());
    }

    Y_UNIT_TEST(InNoHints) {
        NYql::TAstParseResult res = SqlToYql("SELECT * FROM plato.Input WHERE key IN (1,2,3)");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            Y_UNUSED(word);
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'()"));
        };
        TWordCountHive elementStat = {{TString("SqlIn"), 0}};
        VerifyProgram(res, elementStat, verifyLine);
    }

    Y_UNIT_TEST(InHintCompact) {
        // should parse COMPACT as hint
        NYql::TAstParseResult res = SqlToYql("SELECT * FROM plato.Input WHERE key IN COMPACT(1, 2, 3)");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            Y_UNUSED(word);
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'('('isCompact))"));
        };
        TWordCountHive elementStat = {{TString("SqlIn"), 0}};
        VerifyProgram(res, elementStat, verifyLine);
    }

    Y_UNIT_TEST(InHintTableSource) {
        // should add tableSource as a hint
        NYql::TAstParseResult res = SqlToYql("$subq = (SELECT key FROM plato.Input); SELECT * FROM plato.Input WHERE key IN $subq");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            Y_UNUSED(word);
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'('('tableSource))"));
        };
        TWordCountHive elementStat = {{TString("SqlIn"), 0}};
        VerifyProgram(res, elementStat, verifyLine);
    }

    Y_UNIT_TEST(InHintCompactAndTableSource) {
        NYql::TAstParseResult res = SqlToYql("$subq = (SELECT key FROM plato.Input); SELECT * FROM plato.Input WHERE key IN COMPACT $subq");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            Y_UNUSED(word);
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'('('isCompact) '('tableSource))"));
        };
        TWordCountHive elementStat = {{TString("SqlIn"), 0}};
        VerifyProgram(res, elementStat, verifyLine);
    }

    Y_UNIT_TEST(CompactKeywordNotReservedForNames) {
        UNIT_ASSERT(SqlToYql("SELECT COMPACT FROM plato.Input WHERE COMPACT IN COMPACT(1, 2, 3)").IsOk());
        UNIT_ASSERT(SqlToYql("USE plato; SELECT * FROM COMPACT").IsOk());
    }

    Y_UNIT_TEST(Jubilee) {
        NYql::TAstParseResult res = SqlToYql("USE plato; INSERT INTO Arcadia (r2000000) VALUES (\"2M GET!!!\");");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(QualifiedAsteriskBefore) {
        NYql::TAstParseResult res = SqlToYql("select interested_table.*, LENGTH(value) AS megahelpful_len  from plato.Input as interested_table;");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "FlattenMembers") {
                static ui32 count1 = 0;
                if (++count1 == 1) {
                    UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("interested_table."));
                }
            } else if (word == "AsStruct") {
                static ui32 count2 = 0;
                if (++count2 == 2) {
                    UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("megahelpful_len"));
                }
            }
        };
        TWordCountHive elementStat = {{TString("FlattenMembers"), 0}, {TString("AsStruct"), 0}};
        VerifyProgram(res, elementStat, verifyLine);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["FlattenMembers"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["AsStruct"]);
    }

    Y_UNIT_TEST(QualifiedAsteriskAfter) {
        NYql::TAstParseResult res = SqlToYql("select LENGTH(value) AS megahelpful_len, interested_table.*  from plato.Input as interested_table;");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "FlattenMembers") {
                static ui32 count1 = 0;
                if (++count1 == 1) {
                    UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("interested_table."));
                }
            } else if (word == "AsStruct") {
                static ui32 count2 = 0;
                if (++count2 == 2) {
                    UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("megahelpful_len"));
                }
            }
        };
        TWordCountHive elementStat = {{TString("FlattenMembers"), 0}, {TString("AsStruct"), 0}};
        VerifyProgram(res, elementStat, verifyLine);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["FlattenMembers"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["AsStruct"]);
    }

    Y_UNIT_TEST(QualifiedMembers) {
        NYql::TAstParseResult res = SqlToYql("select interested_table.key, interested_table.value from plato.Input as interested_table;");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            const bool fieldKey = TString::npos != line.find(Quote("key"));
            const bool fieldValue = TString::npos != line.find(Quote("value"));
            const bool refOnTable = TString::npos != line.find("interested_table.");
            if (word == "AsStruct") {
                UNIT_ASSERT(fieldKey || fieldValue);
                UNIT_ASSERT(!refOnTable);
            } else if (word == "Write!") {
                UNIT_ASSERT(fieldKey && fieldValue && !refOnTable);
            }
        };
        TWordCountHive elementStat = {{TString("FlattenMembers"), 0}, {TString("AsStruct"), 0}, {TString("Write!"), 0}};
        VerifyProgram(res, elementStat, verifyLine);
        UNIT_ASSERT_VALUES_EQUAL(0, elementStat["FlattenMembers"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["AsStruct"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write!"]);
    }

    Y_UNIT_TEST(JoinParseCorrect) {
        NYql::TAstParseResult res = SqlToYql(
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
            } else if (word == "AsStruct") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("SqlColumn"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("table_aa")));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("key")));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("megakey")));
            }
        };
        TWordCountHive elementStat = {{TString("FlattenMembers"), 0}, {TString("SelectMembers"), 0}, {TString("AsStruct"), 0}};
        VerifyProgram(res, elementStat, verifyLine);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["FlattenMembers"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SelectMembers"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["AsStruct"]);
    }

    Y_UNIT_TEST(Join3Table) {
        NYql::TAstParseResult res = SqlToYql(
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
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("table_bb."));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("table_cc."));
            } else if (word == "AsStruct") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("SqlColumn"));
                const auto posTableAA = line.find(Quote("table_aa"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, posTableAA);
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("key")));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("gigakey")));
                UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("table_aa", posTableAA + 3));
            }
        };
        TWordCountHive elementStat = {{TString("FlattenMembers"), 0}, {TString("SelectMembers"), 0}, {TString("AsStruct"), 0}};
        VerifyProgram(res, elementStat, verifyLine);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["FlattenMembers"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["SelectMembers"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["AsStruct"]);
    }

    Y_UNIT_TEST(JoinWithoutConcreteColumns) {
        NYql::TAstParseResult res = SqlToYql(
            " use plato;"
            " SELECT a.v, b.value"
            "     FROM [Input1]:[ksv] AS a"
            "     JOIN [Input2] AS b"
            "     ON a.k == b.key;"
        );
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "AsStruct") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("SqlColumn"));
                const auto posTableA = line.find(Quote("a"));
                const auto posTableB = line.find(Quote("b"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, posTableA);
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, posTableB);
                UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find(Quote("a"), posTableA + 1));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("v")));
                UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find(Quote("b"), posTableB + 1));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(Quote("value")));
            }
        };
        TWordCountHive elementStat = {{TString("FlattenMembers"), 0}, {TString("AsStruct"), 0}};
        VerifyProgram(res, elementStat, verifyLine);
        UNIT_ASSERT_VALUES_EQUAL(0, elementStat["FlattenMembers"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["AsStruct"]);
    }

    Y_UNIT_TEST(JoinWithSameValues) {
        NYql::TAstParseResult res = SqlToYql("SELECT a.value, b.value FROM plato.Input AS a JOIN plato.Input as b ON a.key == b.key;");
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "AsStruct") {
                const bool isValueFromA = TString::npos != line.find(Quote("a.value"));
                const bool isValueFromB = TString::npos != line.find(Quote("b.value"));
                UNIT_ASSERT(isValueFromA || isValueFromB);
            } if (word == "Write!") {
                const bool noDuplicateSourceInA = TString::npos == line.find("a.a.");
                const bool noDuplicateSourceInB = TString::npos == line.find("b.b.");
                UNIT_ASSERT(noDuplicateSourceInA || noDuplicateSourceInB);
            }
        };
        TWordCountHive elementStat = {{TString("FlattenMembers"), 0}, {TString("AsStruct"), 0}, {"Write!", 0}};
        VerifyProgram(res, elementStat, verifyLine);
        UNIT_ASSERT_VALUES_EQUAL(0, elementStat["FlattenMembers"]);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["AsStruct"]);
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

    Y_UNIT_TEST(SelectOrderByLabeledColumn) {
        NYql::TAstParseResult res = SqlToYql("select key as goal from plato.Input order by goal");
        UNIT_ASSERT(res.Root);
        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "DataSource" && TString::npos == line.find("SQL")) {
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
        UNIT_ASSERT_VALUES_EQUAL(2, elementStat["DataSource"]);
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
        NYql::TAstParseResult res = SqlToYql("select i.*, key, subkey from plato.Input as i order by cast(i.key as uint32) - cast(i.subkey as uint32) desc");
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
        NYql::TAstParseResult res = SqlToYql("insert into plato.Output ([], list1) values (0, AsList(0, 1, 2));");
        /// Verify that parsed well without crash
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(KikimrRollback) {
        NYql::TAstParseResult res = SqlToYql("use plato; rollback;", 10, "kikimr");
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
        NYql::TAstParseResult res = SqlToYql("USE plato; $all = YQL::@@(lambda '(table_name) (Bool 'true))@@; SELECT * FROM FILTER(Input, $all)");
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
        NYql::TAstParseResult res = SqlToYql("process plato.Input using Kikimr::PushData($ROWS);", 1, TString(NYql::KikimrProviderName));
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "Kikimr.PushData") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("TupleType"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("TypeOf"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("inputRowsList"));
            }
        };

        TWordCountHive elementStat = {{TString("Kikimr.PushData"), 0}};
        VerifyProgram(res, elementStat, verifyLine);
        UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Kikimr.PushData"]);
    }

    Y_UNIT_TEST(ProcessUserTypeAuth) {
        NYql::TAstParseResult res = SqlToYql("process plato.Input using YDB::PushData($ROWS, AsTuple('oauth', SecureParam('api:oauth')));", 1, TString(NYql::KikimrProviderName));
        UNIT_ASSERT(res.Root);

        TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
            if (word == "YDB.PushData") {
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("TupleType"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("TypeOf"));
                UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("inputRowsList"));
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
    }

    Y_UNIT_TEST(GroupByHopRtmr) {
        NYql::TAstParseResult res = SqlToYql(R"(
            USE plato; INSERT INTO Output SELECT STREAM key, SUM(value) AS value FROM Input
            GROUP BY key, HOP(subkey, "PT10S", "PT30S", "PT20S");
        )", 10, TString(NYql::RtmrProviderName));
        UNIT_ASSERT(res.Root);
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

    Y_UNIT_TEST(WarnLegacySuffixForTinyIntLiteral) {
        NYql::TAstParseResult res = SqlToYql("select 1b, 0ub, 0b0b;");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res),
            "<main>:1:8: Warning: Deprecated suffix 'b' - please use 't' suffix for 8-bit integers, code: 4508\n"
            "<main>:1:12: Warning: Deprecated suffix 'b' - please use 't' suffix for 8-bit integers, code: 4508\n"
            "<main>:1:17: Warning: Deprecated suffix 'b' - please use 't' suffix for 8-bit integers, code: 4508\n"
        );
    }

    Y_UNIT_TEST(WarnMultiWayJoinWithUsing) {
        NYql::TAstParseResult res = SqlToYql(
            "USE plato;\n"
            "PRAGMA DisableSimpleColumns;\n"
            "SELECT *\n"
            "FROM Input1 AS a\n"
            "JOIN Input2 AS b USING(key)\n"
            "JOIN Input3 AS c ON a.key = c.key;\n"
        );
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res),
            "<main>:5:24: Warning: Multi-way JOINs should be connected with ON clause instead of USING clause, code: 4514\n"
        );
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

        TString header = "<main>:1:15: Error: Unexpected character : syntax error...\n\n";
        TString err1 = "<main>:1:7: Error: Unexpected token '\'a\\\\\'' : cannot match to any predicted input...\n\n";
        TString err2 = "<main>:1:7: Error: Unexpected token '\"a\\\\\"' : cannot match to any predicted input...\n\n";
        UNIT_ASSERT_NO_DIFF(Err2Str(res1), header + err1);
        UNIT_ASSERT_NO_DIFF(Err2Str(res2), header + err2);
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

    Y_UNIT_TEST(InvalidOctalInMultilineStringIdentifier) {
        NYql::TAstParseResult res = SqlToYql("use plato; select * from `In\\0123\n\n"
                                             "\\`\\01put`");
        UNIT_ASSERT(!res.Root);
        TString a1 = Err2Str(res);
        TString a2 = "<main>:3:6: Error: Cannot parse broken identifier: Invalid octal value\n";

        UNIT_ASSERT_NO_DIFF(a1, a2);
    }

    Y_UNIT_TEST(WrongTokenTrivial) {
        NYql::TAstParseResult res = SqlToYql("foo");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:0: Error: Unexpected token 'foo' : cannot match to any predicted input...\n\n");
    }

    Y_UNIT_TEST(InvalidDoubleAtString) {
        NYql::TAstParseResult res = SqlToYql("select @@@@@@");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:13: Error: Unexpected character : syntax error...\n\n"
                                          "<main>:1:0: Error: Unexpected token absence : cannot match to any predicted input...\n\n"    );
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
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:28: Error: Unexpected character : syntax error...\n\n"
                                          "<main>:1:0: Error: Unexpected token absence : cannot match to any predicted input...\n\n");
    }

    Y_UNIT_TEST(SelectInvalidSyntax) {
        NYql::TAstParseResult res = SqlToYql("select 1 form Wat");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:9: Error: Unexpected token 'form' : syntax error...\n\n");
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
        UNIT_ASSERT_NO_DIFF(Err2Str(res),
            "<main>:3:24: Error: Unexpected token '\"foo\"' : cannot match to any predicted input...\n\n"
            "<main>:3:39: Error: Unexpected token absence : Missing THEN \n\n"
        );
    }

    Y_UNIT_TEST(SelectCaseWithoutEnd) {
        NYql::TAstParseResult res = SqlToYql("select case a when b then c end from plato.Input");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: ELSE is required\n");
    }

    Y_UNIT_TEST(SelectWithBadAggregationNoInput) {
        NYql::TAstParseResult res = SqlToYql("select a, Min(b), c");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: Source does not allow column references\n"
           "<main>:1:1: Error: Source does not allow column references\n"
           "<main>:1:1: Error: Source does not allow column references\n");
    }

    Y_UNIT_TEST(SelectWithBadAggregation) {
        NYql::TAstParseResult res = SqlToYql("select count(*), key from plato.Input");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:18: Error: Expression has to be an aggregation function or key column, because aggregation is used elsewhere in this subquery\n");
    }

    Y_UNIT_TEST(SelectWithBadAggregatedTerms) {
        NYql::TAstParseResult res = SqlToYql("select key, subkey from plato.Input group by key");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:13: Error: Expression has to be an aggregation function or key column, because aggregation is used elsewhere in this subquery\n");
    }

    Y_UNIT_TEST(SelectWithBadAggregationInHaving) {
        NYql::TAstParseResult res = SqlToYql("select key from plato.Input group by key having value == \"foo\"");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:55: Error: Expression has to be an aggregation function or key column, because aggregation is used elsewhere in this subquery\n");
    }

    Y_UNIT_TEST(SelectWithDuplicateGroupingColumns) {
        NYql::TAstParseResult res = SqlToYql("select c from plato.Input group by c, c");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: Duplicate grouping column: c\n");
    }

    Y_UNIT_TEST(SelectWithBadAggregationInGrouping) {
        NYql::TAstParseResult res = SqlToYql("select a, Min(b), c group by c");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: Source does not allow column references\n"
            "<main>:1:1: Error: Source does not allow column references\n"
            "<main>:1:1: Error: Source does not allow column references\n"
            "<main>:1:1: Error: Source does not allow column references\n"
            "<main>:1:1: Error: No aggregations were specified\n");
    }

    Y_UNIT_TEST(SelectWithOpOnBadAggregation) {
        NYql::TAstParseResult res = SqlToYql("select a + Min(b) from plato.Input");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:10: Error: Expression has to be an aggregation function or key column, because aggregation is used elsewhere in this subquery\n");
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
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:87: Error: JOIN: equality predicate arguments must not be constant\n");
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

    Y_UNIT_TEST(InsertNoCluster) {
        NYql::TAstParseResult res = SqlToYql("insert into Output (foo) values (1)");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:34: Error: No cluster name given and no default cluster is selected\n");
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
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:52: Error: REDUCE does not allow DISTINCT arguments\n");
    }

    Y_UNIT_TEST(ProcessMultipleRowsPlaceholders) {
        NYql::TAstParseResult res = SqlToYql("process plato.Input using some::udf($ROWS, $ROWS)");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:15: Error: Only single instance of $ROWS is allowed.\n");
    }

    Y_UNIT_TEST(ProcessRowInExpression) {
        NYql::TAstParseResult res = SqlToYql("process plato.Input using some::udf($ROW + 1)");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:15: Error: $ROW can't be used as a part of expression.\n");
    }

    Y_UNIT_TEST(ProcessRowsInExpression) {
        NYql::TAstParseResult res = SqlToYql("process plato.Input using some::udf($ROWS + 1)");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:15: Error: $ROWS can't be used as a part of expression.\n");
    }

    Y_UNIT_TEST(ProcessRowsWithColumnAccess) {
        NYql::TAstParseResult res = SqlToYql("process plato.Input using some::udf(key, $ROWS)");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:15: Error: Source does not allow column references\n");
    }

    Y_UNIT_TEST(ProcessRowsWithRowAccess) {
        NYql::TAstParseResult res = SqlToYql("process plato.Input using some::udf($ROWS, $ROW)");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:15: Error: Source does not allow column references\n");
    }

    Y_UNIT_TEST(ProcessRowsPlaceholderOutOfScope) {
        NYql::TAstParseResult res = SqlToYql(
            "$data = (process plato.Input using some::udf($ROWS));\n"
            "SELECT * FROM $ROWS;\n"
        );
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:15: Error: Unknown name: $ROWS\n");
    }

    Y_UNIT_TEST(ProcessWithInvalidSource) {
        NYql::TAstParseResult res = SqlToYql("PROCESS $input USING YQL::AsList($ROWS);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:9: Error: Unknown name: $input\n");
    }

    Y_UNIT_TEST(SelectWithUnknownBind) {
        NYql::TAstParseResult res = SqlToYql("select * from plato.Input where $value like 'Hell!';");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:33: Error: Unknown name: $value\n");
    }

    Y_UNIT_TEST(CreateTableWithView) {
        NYql::TAstParseResult res = SqlToYql("CREATE TABLE plato.foo:bar (key INT);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:22: Error: Unexpected token ':' : syntax error...\n\n");
    }

    Y_UNIT_TEST(CreateTableWithOptColumn) {
        NYql::TAstParseResult res = SqlToYql("CREATE TABLE plato.foo (key \"Int32?\");");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:25: Error: CREATE TABLE clause requires non-optional column types in scheme\n");
    }

    Y_UNIT_TEST(AsteriskWithSomethingAfter) {
        NYql::TAstParseResult res = SqlToYql("select *, LENGTH(value) from plato.Input;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: Unable to use general '*' with other columns, either specify concrete table like '<table>.*', either specify concrete columns.\n");
    }

    Y_UNIT_TEST(AsteriskWithSomethingBefore) {
        NYql::TAstParseResult res = SqlToYql("select LENGTH(value), * from plato.Input;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:23: Error: Unable to use general '*' with other columns, either specify concrete table like '<table>.*', either specify concrete columns.\n");
    }

    Y_UNIT_TEST(DuplicatedQualifiedAsterisk) {
        NYql::TAstParseResult res = SqlToYql("select in.*, key, in.* from plato.Input as in;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:19: Error: Unable to use twice same quialified asterisk. Invalid source: in\n");
    }

    Y_UNIT_TEST(BrokenLabel) {
        NYql::TAstParseResult res = SqlToYql("select in.*, key as [funny.label] from plato.Input as in;");
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

    Y_UNIT_TEST(UseInOnStrings) {
        NYql::TAstParseResult res = SqlToYql("select * from plato.Input where \"foo\" in \"foovalue\";");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:42: Error: Unable to use IN predicate with string argument, it won't search substring - "
                                          "expecting tuple, list, dict or single column table source\n");
    }

    Y_UNIT_TEST(UseSubqueryInScalarContextInsideIn) {
        NYql::TAstParseResult res = SqlToYql("$q = (select key from plato.Input); select * from plato.Input where subkey in ($q);");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:79: Warning: Using subrequest in scalar context after IN, perhaps you should remove parenthesis here, code: 4501\n");
    }

    Y_UNIT_TEST(InHintsWithKeywordClash) {
        NYql::TAstParseResult res = SqlToYql("SELECT COMPACT FROM plato.Input WHERE COMPACT IN COMPACT [COMPACT](1,2,3)");
        UNIT_ASSERT(!res.Root);
        // should try to parse last compact as call expression
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:58: Error: Unknown builtin: COMPACT, to use YQL functions, try YQL::COMPACT\n");
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
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:74: Error: You should use in GROUPING SETS either expression with required alias either column name or used alias.\n");
    }

    Y_UNIT_TEST(CubeByExprWithoutAlias) {
        NYql::TAstParseResult res = SqlToYql("SELECT key FROM plato.Input GROUP BY CUBE (key, subkey / key);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:58: Error: You should use in CUBE either expression with required alias either column name or used alias.\n");
    }

    Y_UNIT_TEST(RollupByExprWithoutAlias) {
        NYql::TAstParseResult res = SqlToYql("SELECT key FROM plato.Input GROUP BY ROLLUP (subkey / key);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:55: Error: You should use in ROLLUP either expression with required alias either column name or used alias.\n");
    }

    Y_UNIT_TEST(GroupByHugeCubeDeniedNoPragma) {
        NYql::TAstParseResult res = SqlToYql("SELECT key FROM plato.Input GROUP BY CUBE (key, subkey, value, key + subkey as sum, key - subkey as sub, key + val as keyval);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:119: Error: GROUP BY CUBE is allowed only for 5 columns, but you use 6\n");
    }

    Y_UNIT_TEST(GroupByInvalidPragma) {
        NYql::TAstParseResult res = SqlToYql("PRAGMA GroupByCubeLimit = '-4';");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: Expected single unsigned integer argument for: GroupByCubeLimit\n");
    }

    Y_UNIT_TEST(GroupByHugeCubeDeniedPragme) {
        NYql::TAstParseResult res = SqlToYql("PRAGMA GroupByCubeLimit = '4'; SELECT key FROM plato.Input GROUP BY CUBE (key, subkey, value, key + subkey as sum, key - subkey as sub);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:132: Error: GROUP BY CUBE is allowed only for 4 columns, but you use 5\n");
    }

    Y_UNIT_TEST(GroupByFewBigCubes) {
        NYql::TAstParseResult res = SqlToYql("SELECT key FROM plato.Input GROUP BY CUBE(key, subkey, key + subkey as sum), CUBE(value, value + key + subkey as total);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: Unable to GROUP BY more than 64 groups, you try use 80 groups\n");
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
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:71: Error: Column 'nomind' not used as grouping column\n");
    }

    Y_UNIT_TEST(NoGroupingColumn1) {
        NYql::TAstParseResult res = SqlToYql("select count(1), grouping(key, value) as group_duo from plato.Input group by cube (key, subkey);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:32: Error: Column 'value' not used as grouping column\n");
    }

    Y_UNIT_TEST(EmptyAccess0) {
        NYql::TAstParseResult res = SqlToYql("insert into plato.Output (list0, list1) values (AsList(0, 1, 2), AsList([]));");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:34: Error: Source does not allow column references\n");
    }

    Y_UNIT_TEST(EmptyAccess1) {
        NYql::TAstParseResult res = SqlToYql("insert into plato.Output (list0, list1) values (AsList(0, 1, 2), []);");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:34: Error: Source does not allow column references\n");
    }

    Y_UNIT_TEST(UseUnknownColumnInInsert) {
        NYql::TAstParseResult res = SqlToYql("insert into plato.Output (list0, list1) values (AsList(0, 1, 2), AsList([test]));");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:34: Error: Source does not allow column references\n");
    }

    Y_UNIT_TEST(GroupByEmptyColumn) {
        NYql::TAstParseResult res = SqlToYql("select count(1) from plato.Input group by [];");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:43: Error: Column name can not be empty\n");
    }

    Y_UNIT_TEST(RollbackUnsupported) {
        NYql::TAstParseResult res = SqlToYql("insert into plato.Output (v0, v1) values (0, 1); rollback;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:50: Error: ROLLBACK isn't supported for provider: yt\n");
    }

    Y_UNIT_TEST(ShouldFailAsTruncatedBinaryNotSucceedAsLegacyTinyZeroInt) {
        NYql::TAstParseResult res = SqlToYql("select 0b;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res),
            "<main>:1:9: Error: Unexpected character ';' (Unicode character <59>) : missing elements...\n\n"
            "<main>:1:0: Error: Unexpected token absence : cannot match to any predicted input...\n\n"
        );
    }

    Y_UNIT_TEST(ConvertNumberFailed0) {
        NYql::TAstParseResult res = SqlToYql("select 0o80l;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: Failed to parse number from string: 0o80l, char: '8' is out of base: 8\n");
    }

    Y_UNIT_TEST(ConvertNumberFailed1) {
        NYql::TAstParseResult res = SqlToYql("select 0xc000000000000000l;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: Failed to convert string: 13835058055282163712 to Int64 value\n");
    }

    Y_UNIT_TEST(ConvertNumberFailed2) {
        NYql::TAstParseResult res = SqlToYql("select 0xc0000000000000000l;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: Failed to parse number from string: 0xc0000000000000000l, number limit overflow\n");
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
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: Failed to use window function: Lead without window specification\n");
    }

    Y_UNIT_TEST(DropTableWithIfExists) {
        NYql::TAstParseResult res = SqlToYql("DROP TABLE IF EXISTS plato.foo;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:0: Error: IF EXISTS in DROP TABLE is not supported.\n");
    }

    Y_UNIT_TEST(TooManyErrors) {
        const char* q = R"(
        USE plato;
        $a1 = A;
        $a2 = DateTime::ToDate($a1, $a1);
        $a3 = DateTime::ToDate($a2, $a2);
        $a4 = DateTime::ToDate($a3, $a3);

        $s = (select b from plato.abc);

        select $a4 from $s;
)";

        NYql::TAstParseResult res = SqlToYql(q, 10);
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res),
            R"(<main>:3:15: Error: Column A is not in source column set. Did you mean b?
<main>:3:15: Error: Column A is not in source column set. Did you mean b?
<main>:3:15: Error: Column A is not in source column set. Did you mean b?
<main>:3:15: Error: Column A is not in source column set. Did you mean b?
<main>:3:15: Error: Column A is not in source column set. Did you mean b?
<main>:3:15: Error: Column A is not in source column set. Did you mean b?
<main>:3:15: Error: Column A is not in source column set. Did you mean b?
<main>:3:15: Error: Column A is not in source column set. Did you mean b?
<main>:3:15: Error: Column A is not in source column set. Did you mean b?
<main>: Error: Too many issues, code: 1
)");
    }

    Y_UNIT_TEST(ShouldCloneBindingForNamedParameter) {
        NYql::TAstParseResult res = SqlToYql(R"(
$f = () -> {
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
        UNIT_ASSERT_NO_DIFF(Err2Str(res), R"(<main>:1:15: Error: Unknown cluster: PlatO
<main>:1:15: Error: No cluster name given and no default cluster is selected
)");

        res = SqlToYql("use PlatO; select * from foo;", 10, "kikimr");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:5: Error: Unknown cluster: PlatO\n");
    }
}

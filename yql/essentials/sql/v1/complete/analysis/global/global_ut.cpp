#include "global.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NSQLComplete;

Y_UNIT_TEST_SUITE(GlobalAnalysisTests) {

    Y_UNIT_TEST(TopLevelNamesCollected) {
        IGlobalAnalysis::TPtr global = MakeGlobalAnalysis();

        TString query = R"(
            DECLARE $cluster_name AS String;

            IMPORT math SYMBOLS $sqrt, $pow;

            $sqrt = 0;

            DEFINE ACTION $hello_world($name, $suffix?) AS
                $name = $name ?? ($suffix ?? "world");
                SELECT "Hello, " || $name || "!";
            END DEFINE;

            $first, $second, $_ = AsTuple(1, 2, 3);
        )";

        TGlobalContext ctx = global->Analyze({query}, {});
        Sort(ctx.Names);

        TVector<TString> expected = {
            "cluster_name",
            "first",
            "hello_world",
            "pow",
            "second",
            "sqrt",
        };
        UNIT_ASSERT_VALUES_EQUAL(ctx.Names, expected);
    }

    Y_UNIT_TEST(LocalNamesCollected) {
        IGlobalAnalysis::TPtr global = MakeGlobalAnalysis();

        TString query = R"(
            DEFINE ACTION $sum($x, $y) AS
                $acc = 0;
                EVALUATE FOR $i IN AsList($x, $y) DO BEGIN
                    $plus = ($a, $b) -> (#);
                    $acc = $plus($acc, $i);
                END DO;
            END DEFINE;
        )";

        TCompletionInput input = SharpedInput(query);

        TGlobalContext ctx = global->Analyze(input, {});
        Sort(ctx.Names);

        TVector<TString> expected = {
            "a",
            "acc",
            "b",
            "i",
            "plus",
            "sum",
            "x",
            "y",
        };
        UNIT_ASSERT_VALUES_EQUAL(ctx.Names, expected);
    }

    Y_UNIT_TEST(RecursiveName) {
        IGlobalAnalysis::TPtr global = MakeGlobalAnalysis();

        TString query = R"(
            $x = $x;
            #
        )";

        TGlobalContext ctx = global->Analyze(SharpedInput(query), {});
        UNIT_ASSERT_VALUES_EQUAL(ctx.Names, TVector<TString>{"x"});
    }

    Y_UNIT_TEST(EnclosingFunctionName) {
        IGlobalAnalysis::TPtr global = MakeGlobalAnalysis();
        {
            TString query = "SELECT * FROM Concat(#)";
            TGlobalContext ctx = global->Analyze(SharpedInput(query), {});
            TFunctionContext expected = {"Concat", 0};
            UNIT_ASSERT_VALUES_EQUAL(ctx.EnclosingFunction, expected);
        }
        {
            TString query = "SELECT * FROM Concat(a, #)";
            TGlobalContext ctx = global->Analyze(SharpedInput(query), {});
            TFunctionContext expected = {"Concat", 1, "a"};
            UNIT_ASSERT_VALUES_EQUAL(ctx.EnclosingFunction, expected);
        }
        {
            TString query = "SELECT * FROM Concat(a#)";
            TGlobalContext ctx = global->Analyze(SharpedInput(query), {});
            TFunctionContext expected = {"Concat", 0};
            UNIT_ASSERT_VALUES_EQUAL(ctx.EnclosingFunction, expected);
        }
        {
            TString query = "SELECT * FROM Concat(#";
            TGlobalContext ctx = global->Analyze(SharpedInput(query), {});
            TFunctionContext expected = {"Concat", 0};
            UNIT_ASSERT_VALUES_EQUAL(ctx.EnclosingFunction, expected);
        }
        {
            TString query = "SELECT * FROM (#)";
            TGlobalContext ctx = global->Analyze(SharpedInput(query), {});
            UNIT_ASSERT_VALUES_EQUAL(ctx.EnclosingFunction, Nothing());
        }
        {
            TString query = "SELECT * FROM plato.Concat(#)";
            TGlobalContext ctx = global->Analyze(SharpedInput(query), {});
            TClusterContext expected = {"", "plato"};
            UNIT_ASSERT_VALUES_EQUAL(ctx.EnclosingFunction->Cluster, expected);
        }
    }

    Y_UNIT_TEST(SimpleSelectFrom) {
        IGlobalAnalysis::TPtr global = MakeGlobalAnalysis();
        {
            TString query = "SELECT # FROM plato.Input";

            TGlobalContext ctx = global->Analyze(SharpedInput(query), {});

            TColumnContext expected = {.Tables = {TTableId{"plato", "Input"}}};
            UNIT_ASSERT_VALUES_EQUAL(ctx.Column, expected);
        }
        {
            TString query = "SELECT # FROM plato.`//home/input`";

            TGlobalContext ctx = global->Analyze(SharpedInput(query), {});

            TColumnContext expected = {.Tables = {TTableId{"plato", "//home/input"}}};
            UNIT_ASSERT_VALUES_EQUAL(ctx.Column, expected);
        }
        {
            TString query = "SELECT # FROM plato.Input AS x";

            TGlobalContext ctx = global->Analyze(SharpedInput(query), {});

            TColumnContext expected = {.Tables = {TAliased<TTableId>("x", TTableId{"plato", "Input"})}};
            UNIT_ASSERT_VALUES_EQUAL(ctx.Column, expected);
        }
    }

    Y_UNIT_TEST(Join) {
        IGlobalAnalysis::TPtr global = MakeGlobalAnalysis();
        {
            TString query = R"(
                SELECT #
                FROM q.a AS x, p.b, c
                JOIN p.d AS y ON x.key = y.key
            )";

            TGlobalContext ctx = global->Analyze(SharpedInput(query), {});

            TColumnContext expected = {
                .Tables = {
                    TAliased<TTableId>("", {"", "c"}),
                    TAliased<TTableId>("", {"p", "b"}),
                    TAliased<TTableId>("x", {"q", "a"}),
                    TAliased<TTableId>("y", {"p", "d"}),
                },
            };
            UNIT_ASSERT_VALUES_EQUAL(ctx.Column, expected);
        }
    }

    Y_UNIT_TEST(Subquery) {
        IGlobalAnalysis::TPtr global = MakeGlobalAnalysis();
        {
            TString query = "SELECT # FROM (SELECT * FROM x)";

            TGlobalContext ctx = global->Analyze(SharpedInput(query), {});

            TColumnContext expected = {.Tables = {TAliased<TTableId>("", {"", "x"})}};
            UNIT_ASSERT_VALUES_EQUAL(ctx.Column, expected);
        }
        {
            TString query = "SELECT # FROM (SELECT a, b FROM x)";

            TGlobalContext ctx = global->Analyze(SharpedInput(query), {});

            TColumnContext expected = {.Columns = {{.Name = "a"}, {.Name = "b"}}};
            UNIT_ASSERT_VALUES_EQUAL(ctx.Column, expected);
        }
        {
            TString query = "SELECT # FROM (SELECT 1 AS a, 2 AS b)";

            TGlobalContext ctx = global->Analyze(SharpedInput(query), {});

            TColumnContext expected = {.Columns = {{.Name = "a"}, {.Name = "b"}}};
            UNIT_ASSERT_VALUES_EQUAL(ctx.Column, expected);
        }
        {
            TString query = "SELECT # FROM (SELECT 1 AS a, 2 AS b) AS x";

            TGlobalContext ctx = global->Analyze(SharpedInput(query), {});

            TColumnContext expected = {.Columns = {{"x", "a"}, {"x", "b"}}};
            UNIT_ASSERT_VALUES_EQUAL(ctx.Column, expected);
        }
        {
            TString query = R"(
                SELECT #
                FROM (SELECT * FROM example.`/people`) AS ep
                JOIN (SELECT room AS Room, time FROM example.`/yql/tutorial`) AS et ON 1 = 1
            )";

            TGlobalContext ctx = global->Analyze(SharpedInput(query), {});

            TColumnContext expected = {
                .Tables = {
                    TAliased<TTableId>("ep", {"example", "/people"}),
                },
                .Columns = {
                    {.TableAlias = "et", .Name = "Room"},
                    {.TableAlias = "et", .Name = "time"},
                },
            };
            UNIT_ASSERT_VALUES_EQUAL(ctx.Column, expected);
        }
        {
            TString query = R"(
                SELECT # FROM (
                    SELECT x.*, y.name, e
                    FROM (SELECT a.*, d FROM a AS a JOIN c AS c ON TRUE) AS x
                    JOIN b AS y
                )
            )";

            TGlobalContext ctx = global->Analyze(SharpedInput(query), {});

            TColumnContext expected = {
                .Tables = {
                    TAliased<TTableId>("", {"", "a"}),
                },
                .Columns = {
                    {.Name = "d"},
                    {.Name = "e"},
                },
            };
            UNIT_ASSERT_VALUES_EQUAL(ctx.Column, expected);
        }
        {
            TString query = "SELECT # FROM (SELECT 1, *, 2 FROM t)";

            TGlobalContext ctx = global->Analyze(SharpedInput(query), {});

            TColumnContext expected = {
                .Tables = {
                    TAliased<TTableId>("", {"", "t"}),
                },
            };
            UNIT_ASSERT_VALUES_EQUAL(ctx.Column, expected);
        }
    }

    Y_UNIT_TEST(SubqueryWithout) {
        IGlobalAnalysis::TPtr global = MakeGlobalAnalysis();
        {
            TString query = "SELECT # FROM (SELECT * WITHOUT a FROM x)";

            TGlobalContext ctx = global->Analyze(SharpedInput(query), {});

            TColumnContext expected = {
                .Tables = {
                    TAliased<TTableId>("", {"", "x"}),
                },
                .WithoutByTableAlias = {
                    {"", {"a"}},
                },
            };
            UNIT_ASSERT_VALUES_EQUAL(ctx.Column, expected);
        }
        {
            TString query = R"(
                SELECT #
                FROM (
                    SELECT * WITHOUT Age, eqt.course
                    FROM example.`/people` AS epp
                    JOIN example.`/yql/tutorial` AS eqt ON TRUE
                    JOIN testing ON TRUE
                ) AS ep
            )";

            TGlobalContext ctx = global->Analyze(SharpedInput(query), {});

            TColumnContext expected = {
                .Tables = {
                    TAliased<TTableId>("ep", {"", "testing"}),
                    TAliased<TTableId>("ep", {"example", "/people"}),
                    TAliased<TTableId>("ep", {"example", "/yql/tutorial"}),
                },
                .WithoutByTableAlias = {
                    {"ep", {"course", "Age"}},
                },
            };
            UNIT_ASSERT_VALUES_EQUAL(ctx.Column, expected);
        }
    }

    Y_UNIT_TEST(Projection) {
        IGlobalAnalysis::TPtr global = MakeGlobalAnalysis();
        {
            TString query = "SELECT a, b, # FROM x";

            TGlobalContext ctx = global->Analyze(SharpedInput(query), {});

            TColumnContext expected = {.Tables = {TAliased<TTableId>("", {"", "x"})}};
            UNIT_ASSERT_VALUES_EQUAL(ctx.Column, expected);
        }
    }

    Y_UNIT_TEST(NamedSubquery) {
        IGlobalAnalysis::TPtr global = MakeGlobalAnalysis();
        {
            TString query = R"(
                $subquery = (SELECT * FROM x);
                SELECT # FROM $subquery;
            )";

            TGlobalContext ctx = global->Analyze(SharpedInput(query), {});

            TColumnContext expected = {.Tables = {TAliased<TTableId>("", {"", "x"})}};
            UNIT_ASSERT_VALUES_EQUAL(ctx.Column, expected);
        }
        {
            TString query = R"(
                SELECT # FROM $subquery;
            )";

            TGlobalContext ctx = global->Analyze(SharpedInput(query), {});

            TColumnContext expected = {};
            UNIT_ASSERT_VALUES_EQUAL(ctx.Column, expected);
        }
        {
            TString query = R"(
                $subquery1 = (SELECT * FROM $subquery1);
                SELECT # FROM $subquery1;
            )";

            TGlobalContext ctx = global->Analyze(SharpedInput(query), {});

            TColumnContext expected = {};
            UNIT_ASSERT_VALUES_EQUAL(ctx.Column, expected);
        }
        {
            TString query = R"(
                $subquery1 = (SELECT * FROM $subquery2);
                $subquery2 = (SELECT * FROM $subquery1);
                SELECT # FROM $subquery2;
            )";

            TGlobalContext ctx = global->Analyze(SharpedInput(query), {});

            TColumnContext expected = {};
            UNIT_ASSERT_VALUES_EQUAL(ctx.Column, expected);
        }
    }

    Y_UNIT_TEST(EvaluationAssignment) {
        IGlobalAnalysis::TPtr global = MakeGlobalAnalysis();

        TString query = R"sql(
            DECLARE $t1 AS String;
            DECLARE $t2 AS String;

            $x1 = $t1;
            $x2 = $t2;

            $y1 = $x1;
            $y2 = $x2;

            $q1 = (SELECT * FROM $y1);
            $q2 = (SELECT * FROM $y2);

            SELECT # FROM $q1 JOIN $q2
        )sql";

        TEnvironment env = {
            .Parameters = {
                {"$t1", "table1"},
                {"$t2", "table2"},
            },
        };

        TGlobalContext ctx = global->Analyze(SharpedInput(query), env);

        TColumnContext expected = {
            .Tables = {
                TAliased<TTableId>("", {"", "table1"}),
                TAliased<TTableId>("", {"", "table2"}),
            },
        };
        UNIT_ASSERT_VALUES_EQUAL(ctx.Column, expected);
    }

    Y_UNIT_TEST(EvaluationRecursion) {
        IGlobalAnalysis::TPtr global = MakeGlobalAnalysis();

        TVector<TString> queries = {
            R"sql($x = $x; SELECT # FROM $x)sql",
            R"sql($x = $y; $y = $x; SELECT # FROM $x)sql",
        };

        for (TString& query : queries) {
            TGlobalContext ctx = global->Analyze(SharpedInput(query), {});
            Y_DO_NOT_OPTIMIZE_AWAY(ctx);
        }
    }

    Y_UNIT_TEST(EvaluationSubquery) {
        IGlobalAnalysis::TPtr global = MakeGlobalAnalysis();

        TString query = R"sql(
            $x = (SELECT * FROM $y);
            $y = (SELECT * FROM $x);
            $z = $x || $y;
            SELECT # FROM $z;
        )sql";

        TGlobalContext ctx = global->Analyze(SharpedInput(query), {});

        TColumnContext expected = {};
        UNIT_ASSERT_VALUES_EQUAL(ctx.Column, expected);
    }

    Y_UNIT_TEST(EvaluationStringConcat) {
        IGlobalAnalysis::TPtr global = MakeGlobalAnalysis();

        TString query = R"sql(
            $cluster = 'ex' || 'am' || "ple";
            $product = "yql";
            $seq = "1";
            $source = "/home/" || $product || "/" || $seq;
            SELECT # FROM $cluster.$source;
        )sql";

        TGlobalContext ctx = global->Analyze(SharpedInput(query), {});

        TColumnContext expected = {
            .Tables = {TAliased<TTableId>("", {"example", "/home/yql/1"})},
        };
        UNIT_ASSERT_VALUES_EQUAL(ctx.Column, expected);
    }

} // Y_UNIT_TEST_SUITE(GlobalAnalysisTests)

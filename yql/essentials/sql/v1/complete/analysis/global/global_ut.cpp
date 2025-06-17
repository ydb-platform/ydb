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

    Y_UNIT_TEST(EnclosingFunctionName) {
        IGlobalAnalysis::TPtr global = MakeGlobalAnalysis();
        {
            TString query = "SELECT * FROM Concat(#)";
            TGlobalContext ctx = global->Analyze(SharpedInput(query), {});
            UNIT_ASSERT_VALUES_EQUAL(ctx.EnclosingFunction, "Concat");
        }
        {
            TString query = "SELECT * FROM Concat(a, #)";
            TGlobalContext ctx = global->Analyze(SharpedInput(query), {});
            UNIT_ASSERT_VALUES_EQUAL(ctx.EnclosingFunction, "Concat");
        }
        {
            TString query = "SELECT * FROM Concat(a#)";
            TGlobalContext ctx = global->Analyze(SharpedInput(query), {});
            UNIT_ASSERT_VALUES_EQUAL(ctx.EnclosingFunction, "Concat");
        }
        {
            TString query = "SELECT * FROM Concat(#";
            TGlobalContext ctx = global->Analyze(SharpedInput(query), {});
            UNIT_ASSERT_VALUES_EQUAL(ctx.EnclosingFunction, Nothing());
        }
        {
            TString query = "SELECT * FROM (#)";
            TGlobalContext ctx = global->Analyze(SharpedInput(query), {});
            UNIT_ASSERT_VALUES_EQUAL(ctx.EnclosingFunction, Nothing());
        }
    }

} // Y_UNIT_TEST_SUITE(GlobalAnalysisTests)

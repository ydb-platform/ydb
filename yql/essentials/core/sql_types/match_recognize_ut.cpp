#include "match_recognize.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYql::NMatchRecognize;

Y_UNIT_TEST_SUITE(MatchRecognizePattern){
    auto factorVar = [](const TString& v) { return TRowPatternFactor{v, 0, 0, false, false, false};};
    auto factorExpr = [](const TRowPattern& p) { return TRowPatternFactor{p, 0, 0, false, false, false};};
    auto expected = [](std::initializer_list<TString> list) { return THashSet<TString>(list); };
    Y_UNIT_TEST(SingleVarPattern) {
        const TRowPattern pattern = {{factorVar("A")}};
        UNIT_ASSERT_VALUES_EQUAL(expected({"A"}), GetPatternVars(pattern));
    }
    Y_UNIT_TEST(DistinctVarsPattern) {
        const TRowPattern pattern = {{factorVar("A"), factorVar("B"), factorVar("C")}};
        UNIT_ASSERT_VALUES_EQUAL(expected({"A", "B", "C"}), GetPatternVars(pattern));
    }
    Y_UNIT_TEST(RepeatedVarsPattern) {
        const TRowPattern pattern = {{factorVar("A"), factorVar("B"), factorVar("B"), factorVar("C")}};
        UNIT_ASSERT_VALUES_EQUAL(expected({"A", "B", "C"}), GetPatternVars(pattern));
    }
    Y_UNIT_TEST(NestedPattern) {
        const TRowPattern pattern = {
                {factorVar("A"), factorVar("B"), factorVar("B"), factorVar("C")},
                {factorVar("B"), factorVar("C"), factorVar("D"), factorExpr(
                                                                    {{factorExpr({
                                                                         {factorVar("C"), factorVar("D"), factorVar("E")},
                                                                         {factorVar("F")}
                                                                    })}}
                                                                  )
                },
                {factorVar("C")},
                {factorVar("D")},
                {factorVar("E")}
        };
        UNIT_ASSERT_VALUES_EQUAL(expected({"A", "B", "C", "D", "E", "F"}), GetPatternVars(pattern));
    }
}

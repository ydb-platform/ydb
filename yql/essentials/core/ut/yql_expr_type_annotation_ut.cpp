#include "yql_expr_type_annotation.h"

#include <library/cpp/testing/unittest/registar.h>


namespace NYql {

Y_UNIT_TEST_SUITE(Misc) {
    Y_UNIT_TEST(NormalizeName) {
        auto CheckNoIssues = [](TString name, const TString& expected) {
            UNIT_ASSERT_C(!NormalizeName(TPosition(), name), name);
            UNIT_ASSERT_VALUES_EQUAL(name, expected);
        };

        auto CheckIssues = [](TString name, const TString& expected) {
            UNIT_ASSERT_C(NormalizeName(TPosition(), name), name);
            UNIT_ASSERT_VALUES_EQUAL(name, expected);
        };

        CheckNoIssues("", "");
        CheckNoIssues("_", "_");
        CheckNoIssues("__", "__");
        CheckNoIssues("a", "a");
        CheckNoIssues("abc", "abc");
        CheckNoIssues("aBc", "abc");
        CheckNoIssues("_aBc", "_abc");
        CheckNoIssues("__aBc", "__abc");
        CheckNoIssues("___aBc", "___abc");
        CheckNoIssues("______aBc", "______abc");
        CheckNoIssues("_a_Bc", "_abc");
        CheckIssues("_a__Bc", "_a__Bc");
        CheckNoIssues("_aBc_", "_abc");
        CheckIssues("_aBc__", "_aBc__");
        CheckNoIssues("aBc_", "abc");
        CheckIssues("aBc__", "aBc__");
        CheckIssues("aBc___", "aBc___");
        CheckNoIssues("aB_c", "abc");
        CheckIssues("aB__c", "aB__c");
        CheckNoIssues("a_B_c", "abc");
        CheckNoIssues("_a_B_c", "_abc");
        CheckNoIssues("__a_B_c", "__abc");
        CheckNoIssues("a_B_c_", "abc");
        CheckNoIssues("a_B_c_d", "abcd");
        CheckNoIssues("a_B_c_d_", "abcd");
        CheckNoIssues("a_B_c_d_e", "abcde");
        CheckNoIssues("_a_B_c_d_e", "_abcde");
        CheckIssues("a_B_c_d_e_", "a_B_c_d_e_");
        CheckIssues("a_B_c_d_e_f", "a_B_c_d_e_f");
        CheckIssues("_a_B_c_d_e_f", "_a_B_c_d_e_f");
    }
}

} // namespace NYql

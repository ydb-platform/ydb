#include <library/cpp/testing/unittest/registar.h>

#include "yql_warning.h"

using namespace NYql;

Y_UNIT_TEST_SUITE(TWarningRuleTest) {

    Y_UNIT_TEST(AllValidActionsAndPatternsAreParsed) {
        TWarningRule rule;
        TString errorMessage;
        UNIT_ASSERT_VALUES_EQUAL(TWarningRule::ParseFrom("1234", "disable", rule, errorMessage), TWarningRule::EParseResult::PARSE_OK);
        UNIT_ASSERT_VALUES_EQUAL(TWarningRule::ParseFrom("*",    "error",   rule, errorMessage), TWarningRule::EParseResult::PARSE_OK);
        UNIT_ASSERT_VALUES_EQUAL(TWarningRule::ParseFrom("0",    "default", rule, errorMessage), TWarningRule::EParseResult::PARSE_OK);
    }

    Y_UNIT_TEST(InvalidActionIsDetected) {
        TWarningRule rule;
        TString errorMessage;
        UNIT_ASSERT_VALUES_EQUAL(TWarningRule::ParseFrom("1234", "wrong", rule, errorMessage), TWarningRule::EParseResult::PARSE_ACTION_FAIL);
        UNIT_ASSERT_VALUES_EQUAL(TWarningRule::ParseFrom("1234", "",      rule, errorMessage), TWarningRule::EParseResult::PARSE_ACTION_FAIL);
    }

    Y_UNIT_TEST(InvalidPatternIsDetected) {
        TWarningRule rule;
        TString errorMessage;
        UNIT_ASSERT_VALUES_EQUAL(TWarningRule::ParseFrom("",        "default", rule, errorMessage), TWarningRule::EParseResult::PARSE_PATTERN_FAIL);
        UNIT_ASSERT_VALUES_EQUAL(TWarningRule::ParseFrom("-1",      "default", rule, errorMessage), TWarningRule::EParseResult::PARSE_PATTERN_FAIL);
        UNIT_ASSERT_VALUES_EQUAL(TWarningRule::ParseFrom("*1",      "default", rule, errorMessage), TWarningRule::EParseResult::PARSE_PATTERN_FAIL);
        UNIT_ASSERT_VALUES_EQUAL(TWarningRule::ParseFrom("default", "default", rule, errorMessage), TWarningRule::EParseResult::PARSE_PATTERN_FAIL);
    }
}


void AddRule(TWarningPolicy& policy, const TString& codePattern, const TString& action)
{
    TWarningRule rule;
    TString errorMessage;
    UNIT_ASSERT_VALUES_EQUAL(TWarningRule::ParseFrom(codePattern, action, rule, errorMessage), TWarningRule::EParseResult::PARSE_OK);
    policy.AddRule(rule);
}


Y_UNIT_TEST_SUITE(TWarningPolicyTest) {

    Y_UNIT_TEST(BasicIntegerRules) {
        TWarningPolicy policy;
        AddRule(policy, "123", "error");
        AddRule(policy, "456", "error");
        AddRule(policy, "456", "disable");

        UNIT_ASSERT_VALUES_EQUAL(policy.GetRules().size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(policy.GetAction(123), EWarningAction::ERROR);
        UNIT_ASSERT_VALUES_EQUAL(policy.GetAction(456), EWarningAction::DISABLE);
        UNIT_ASSERT_VALUES_EQUAL(policy.GetAction(111), EWarningAction::DEFAULT);
    }


    Y_UNIT_TEST(AsteriskRules) {
        TWarningPolicy policy;
        AddRule(policy, "*", "error");
        AddRule(policy, "456", "disable");

        UNIT_ASSERT_VALUES_EQUAL(policy.GetAction(999), EWarningAction::ERROR);
        UNIT_ASSERT_VALUES_EQUAL(policy.GetAction(456), EWarningAction::DISABLE);

        AddRule(policy, "*", "default");

        UNIT_ASSERT_VALUES_EQUAL(policy.GetRules().size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(policy.GetAction(999), EWarningAction::DEFAULT);
        UNIT_ASSERT_VALUES_EQUAL(policy.GetAction(456), EWarningAction::DEFAULT);
        UNIT_ASSERT_VALUES_EQUAL(policy.GetAction(123), EWarningAction::DEFAULT);

    }

    Y_UNIT_TEST(ReactionOnPull) {
        TWarningPolicy policy;
        AddRule(policy, "*", "error");
        AddRule(policy, "456", "default");
        AddRule(policy, "999", "disable");

        UNIT_ASSERT_VALUES_EQUAL(policy.GetAction(999), EWarningAction::DISABLE);
        UNIT_ASSERT_VALUES_EQUAL(policy.GetAction(456), EWarningAction::DEFAULT);
        UNIT_ASSERT_VALUES_EQUAL(policy.GetAction(123), EWarningAction::ERROR);

        UNIT_ASSERT_VALUES_EQUAL(policy.GetRules().size(), 3);
        policy.Clear();
        UNIT_ASSERT_VALUES_EQUAL(policy.GetRules().size(), 0);

        UNIT_ASSERT_VALUES_EQUAL(policy.GetAction(999), EWarningAction::DEFAULT);
        UNIT_ASSERT_VALUES_EQUAL(policy.GetAction(456), EWarningAction::DEFAULT);
        UNIT_ASSERT_VALUES_EQUAL(policy.GetAction(123), EWarningAction::DEFAULT);
    }
}

#include <library/cpp/testing/unittest/registar.h>

#include "yql_issue.h"

using namespace NYql;

Y_UNIT_TEST_SUITE(TIssuesDataTest) {
    Y_UNIT_TEST(SeverityMapTest) {
        auto severity = GetSeverity(TIssuesIds::UNEXPECTED);
        auto severityName = SeverityToString(severity);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(severity),
            static_cast<ui32>(TSeverityIds::S_FATAL));
        UNIT_ASSERT_VALUES_EQUAL(severityName, "Fatal");
    }

    Y_UNIT_TEST(UnknownSeverityNameTest) {
        auto severityName = SeverityToString(static_cast<ESeverity>(999));
        UNIT_ASSERT_VALUES_EQUAL(severityName, "Unknown");
    }

    Y_UNIT_TEST(IssueNameTest) {
        auto issueName = IssueCodeToString(TIssuesIds::DEFAULT_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(issueName, "Default error");
    }

    Y_UNIT_TEST(IssueDefaultErrorNumber) {
        UNIT_ASSERT_VALUES_EQUAL((ui32)TIssuesIds::DEFAULT_ERROR, 0);
    }

}

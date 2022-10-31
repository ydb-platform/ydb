#include <ydb/library/yql/dq/actors/compute/dq_compute_issues_buffer.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NDq {

Y_UNIT_TEST_SUITE(TIssuesBufferTest) {
    Y_UNIT_TEST(TestEmpty) {
        TIssuesBuffer buffer(5);

        UNIT_ASSERT_VALUES_EQUAL(buffer.GetAllAddedIssuesCount(), 0);

        const auto dumps = buffer.Dump();
        UNIT_ASSERT_VALUES_EQUAL(dumps.size(), 0);
    }

    Y_UNIT_TEST(TestSimplePush) {
        TIssuesBuffer buffer(5);

        auto issues = TIssues({ TIssue("issue1") });
        buffer.Push(issues);

        UNIT_ASSERT_VALUES_EQUAL(buffer.GetAllAddedIssuesCount(), 1);

        const auto dumps = buffer.Dump();
        UNIT_ASSERT_VALUES_EQUAL(dumps.size(), 1);
        UNIT_ASSERT_STRINGS_EQUAL(dumps.at(0).Issues.begin()->GetMessage(), "issue1");
    }

    Y_UNIT_TEST(TestPushWithOverflow) {
        TIssuesBuffer buffer(2);

        auto issues1 = TIssues({ TIssue("issue1") });
        auto issues2 = TIssues({ TIssue("issue2") });
        auto issues3 = TIssues({ TIssue("issue3") });
        buffer.Push(issues1);
        buffer.Push(issues2);
        buffer.Push(issues3);

        UNIT_ASSERT_VALUES_EQUAL(buffer.GetAllAddedIssuesCount(), 3);

        const auto dumps = buffer.Dump();
        UNIT_ASSERT_VALUES_EQUAL(dumps.size(), 2);
        UNIT_ASSERT_STRINGS_EQUAL(dumps.at(0).Issues.begin()->GetMessage(), "issue2");
        UNIT_ASSERT_STRINGS_EQUAL(dumps.at(1).Issues.begin()->GetMessage(), "issue3");
    }

    Y_UNIT_TEST(TestSmallBuffer) {
        TIssuesBuffer buffer(1);

        auto issues1 = TIssues({ TIssue("issue1") });
        auto issues2 = TIssues({ TIssue("issue2") });
        auto issues3 = TIssues({ TIssue("issue3") });
        buffer.Push(issues1);
        buffer.Push(issues2);
        buffer.Push(issues3);

        UNIT_ASSERT_VALUES_EQUAL(buffer.GetAllAddedIssuesCount(), 3);

        const auto dumps = buffer.Dump();
        UNIT_ASSERT_VALUES_EQUAL(dumps.size(), 1);
        UNIT_ASSERT_STRINGS_EQUAL(dumps.at(0).Issues.begin()->GetMessage(), "issue3");
    }

    Y_UNIT_TEST(TestUseAfterDump) {
        TIssuesBuffer buffer(2);

        auto issues1 = TIssues({ TIssue("issue1") });
        buffer.Push(issues1);
        buffer.Dump();

        auto issues2 = TIssues({ TIssue("issue2") });
        auto issues3 = TIssues({ TIssue("issue3") });
        auto issues4 = TIssues({ TIssue("issue4") });
        buffer.Push(issues2);
        buffer.Push(issues3);
        buffer.Push(issues4);

        UNIT_ASSERT_VALUES_EQUAL(buffer.GetAllAddedIssuesCount(), 4);
        const auto dumps = buffer.Dump();
        UNIT_ASSERT_VALUES_EQUAL(dumps.size(), 2);
        UNIT_ASSERT_STRINGS_EQUAL(dumps.at(0).Issues.begin()->GetMessage(), "issue3");
        UNIT_ASSERT_STRINGS_EQUAL(dumps.at(1).Issues.begin()->GetMessage(), "issue4");
    }
}

} // namespace NYql::NDq

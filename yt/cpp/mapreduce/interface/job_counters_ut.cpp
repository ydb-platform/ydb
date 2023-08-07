#include <yt/cpp/mapreduce/interface/job_counters.h>
#include <yt/cpp/mapreduce/interface/operation.h>

#include <library/cpp/yson/node/node_io.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYT;

Y_UNIT_TEST_SUITE(JobCounters)
{
    Y_UNIT_TEST(Full)
    {
        const TString input = R"""(
            {
                "completed" = {
                    "total" = 6;
                    "non-interrupted" = 1;
                    "interrupted" = {
                        "whatever_interrupted" = 2;
                        "whatever_else_interrupted" = 3;
                    };
                };
                "aborted" = {
                    "non_scheduled" = {
                        "whatever_non_scheduled" = 4;
                        "whatever_else_non_scheduled" = 5;
                    };
                    "scheduled" = {
                        "whatever_scheduled" = 6;
                        "whatever_else_scheduled" = 7;
                    };
                    "total" = 22;
                };
                "lost" = 8;
                "invalidated" = 9;
                "failed" = 10;
                "running" = 11;
                "suspended" = 12;
                "pending" = 13;
                "blocked" = 14;
                "total" = 105;
            })""";

        TJobCounters counters(NodeFromYsonString(input));

        UNIT_ASSERT_VALUES_EQUAL(counters.GetTotal(), 105);

        UNIT_ASSERT_VALUES_EQUAL(counters.GetCompleted().GetTotal(), 6);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetCompletedNonInterrupted().GetTotal(), 1);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetCompletedInterrupted().GetTotal(), 5);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetAborted().GetTotal(), 22);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetAbortedNonScheduled().GetTotal(), 9);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetAbortedScheduled().GetTotal(), 13);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetLost().GetTotal(), 8);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetInvalidated().GetTotal(), 9);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetFailed().GetTotal(), 10);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetRunning().GetTotal(), 11);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetSuspended().GetTotal(), 12);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetPending().GetTotal(), 13);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetBlocked().GetTotal(), 14);

        UNIT_ASSERT_VALUES_EQUAL(counters.GetCompletedInterrupted().GetValue("whatever_interrupted"), 2);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetCompletedInterrupted().GetValue("whatever_else_interrupted"), 3);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetAbortedNonScheduled().GetValue("whatever_non_scheduled"), 4);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetAbortedNonScheduled().GetValue("whatever_else_non_scheduled"), 5);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetAbortedScheduled().GetValue("whatever_scheduled"), 6);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetAbortedScheduled().GetValue("whatever_else_scheduled"), 7);

        UNIT_ASSERT_EXCEPTION(counters.GetCompletedInterrupted().GetValue("Nothingness"), yexception);
    }

    Y_UNIT_TEST(Empty)
    {
        const TString input = "{}";

        TJobCounters counters(NodeFromYsonString(input));

        UNIT_ASSERT_VALUES_EQUAL(counters.GetTotal(), 0);

        UNIT_ASSERT_VALUES_EQUAL(counters.GetCompleted().GetTotal(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetCompletedNonInterrupted().GetTotal(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetCompletedInterrupted().GetTotal(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetAborted().GetTotal(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetAbortedNonScheduled().GetTotal(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetAbortedScheduled().GetTotal(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetLost().GetTotal(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetInvalidated().GetTotal(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetFailed().GetTotal(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetRunning().GetTotal(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetSuspended().GetTotal(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetPending().GetTotal(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetBlocked().GetTotal(), 0);
    }

    Y_UNIT_TEST(Broken)
    {
        UNIT_ASSERT_EXCEPTION_CONTAINS(TJobCounters(TNode()), yexception, "TJobCounters");
        UNIT_ASSERT_EXCEPTION_CONTAINS(TJobCounters(TNode(1)), yexception, "TJobCounters");
        UNIT_ASSERT_EXCEPTION_CONTAINS(TJobCounters(TNode(1.0)), yexception, "TJobCounters");
        UNIT_ASSERT_EXCEPTION_CONTAINS(TJobCounters(TNode("Whatever")), yexception, "TJobCounters");
    }
}

#include <yt/cpp/mapreduce/interface/job_counters.h>
#include <yt/cpp/mapreduce/interface/operation.h>

#include <library/cpp/yson/node/node_io.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYT;

TEST(TJobCountersTest, Full)
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

    EXPECT_EQ(counters.GetTotal(), 105);

    EXPECT_EQ(counters.GetCompleted().GetTotal(), 6);
    EXPECT_EQ(counters.GetCompletedNonInterrupted().GetTotal(), 1);
    EXPECT_EQ(counters.GetCompletedInterrupted().GetTotal(), 5);
    EXPECT_EQ(counters.GetAborted().GetTotal(), 22);
    EXPECT_EQ(counters.GetAbortedNonScheduled().GetTotal(), 9);
    EXPECT_EQ(counters.GetAbortedScheduled().GetTotal(), 13);
    EXPECT_EQ(counters.GetLost().GetTotal(), 8);
    EXPECT_EQ(counters.GetInvalidated().GetTotal(), 9);
    EXPECT_EQ(counters.GetFailed().GetTotal(), 10);
    EXPECT_EQ(counters.GetRunning().GetTotal(), 11);
    EXPECT_EQ(counters.GetSuspended().GetTotal(), 12);
    EXPECT_EQ(counters.GetPending().GetTotal(), 13);
    EXPECT_EQ(counters.GetBlocked().GetTotal(), 14);

    EXPECT_EQ(counters.GetCompletedInterrupted().GetValue("whatever_interrupted"), 2);
    EXPECT_EQ(counters.GetCompletedInterrupted().GetValue("whatever_else_interrupted"), 3);
    EXPECT_EQ(counters.GetAbortedNonScheduled().GetValue("whatever_non_scheduled"), 4);
    EXPECT_EQ(counters.GetAbortedNonScheduled().GetValue("whatever_else_non_scheduled"), 5);
    EXPECT_EQ(counters.GetAbortedScheduled().GetValue("whatever_scheduled"), 6);
    EXPECT_EQ(counters.GetAbortedScheduled().GetValue("whatever_else_scheduled"), 7);

    EXPECT_THROW(counters.GetCompletedInterrupted().GetValue("Nothingness"), yexception);
}

TEST(TJobCountersTest, Empty)
{
    const TString input = "{}";

    TJobCounters counters(NodeFromYsonString(input));

    EXPECT_EQ(counters.GetTotal(), 0);

    EXPECT_EQ(counters.GetCompleted().GetTotal(), 0);
    EXPECT_EQ(counters.GetCompletedNonInterrupted().GetTotal(), 0);
    EXPECT_EQ(counters.GetCompletedInterrupted().GetTotal(), 0);
    EXPECT_EQ(counters.GetAborted().GetTotal(), 0);
    EXPECT_EQ(counters.GetAbortedNonScheduled().GetTotal(), 0);
    EXPECT_EQ(counters.GetAbortedScheduled().GetTotal(), 0);
    EXPECT_EQ(counters.GetLost().GetTotal(), 0);
    EXPECT_EQ(counters.GetInvalidated().GetTotal(), 0);
    EXPECT_EQ(counters.GetFailed().GetTotal(), 0);
    EXPECT_EQ(counters.GetRunning().GetTotal(), 0);
    EXPECT_EQ(counters.GetSuspended().GetTotal(), 0);
    EXPECT_EQ(counters.GetPending().GetTotal(), 0);
    EXPECT_EQ(counters.GetBlocked().GetTotal(), 0);
}

TEST(TJobCountersTest, Broken)
{
    EXPECT_THROW_MESSAGE_HAS_SUBSTR((TJobCounters(TNode())), yexception, "TJobCounters");
    EXPECT_THROW_MESSAGE_HAS_SUBSTR((TJobCounters(TNode(1))), yexception, "TJobCounters");
    EXPECT_THROW_MESSAGE_HAS_SUBSTR((TJobCounters(TNode(1.0))), yexception, "TJobCounters");
    EXPECT_THROW_MESSAGE_HAS_SUBSTR((TJobCounters(TNode("Whatever"))), yexception, "TJobCounters");
}

#include <yt/cpp/mapreduce/raw_client/raw_requests.h>

#include <library/cpp/yson/node/node_io.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYT;
using namespace NYT::NDetail;
using namespace NYT::NDetail::NRawClient;

TEST(TOperationsApiParsingTest, ParseOperationAttributes)
{
    auto response = TStringBuf(R"""({
        "id" = "1-2-3-4";
        "authenticated_user" = "some-user";
        "start_time" = "2018-01-01T00:00:00.0Z";
        "weight" = 1.;
        "state" = "completed";
        "suspended" = %false;
        "finish_time" = "2018-01-02T00:00:00.0Z";
        "brief_progress" = {
            "jobs" = {
                "lost" = 0;
                "pending" = 0;
                "failed" = 1;
                "aborted" = 0;
                "total" = 84;
                "running" = 0;
                "completed" = 84;
            };
        };
        "result" = {
            "error" = {
                "attributes" = {};
                "code" = 0;
                "message" = "";
            };
        };
        "brief_spec" = {
            "input_table_paths" = <
                "count" = 1;
            > [
                "//some-input";
            ];
            "pool" = "some-pool";
            "scheduling_info_per_pool_tree" = {
                "physical" = {
                    "pool" = "some-pool";
                };
            };
            "title" = "some-title";
            "output_table_paths" = <
                "count" = 1;
            > [
                "//some-output";
            ];
            "mapper" = {
                "command" = "some-command";
            };
        };
        "type" = "map";
        "pool" = "some-pool";
        "progress" = {
            "build_time" = "2018-01-01T00:00:00.000000Z";
            "job_statistics" = {
                "data" = {
                    "input" = {
                        "row_count" = {
                            "$" = {
                                "failed" = {
                                    "map" = {
                                        "max" = 1;
                                        "count" = 1;
                                        "sum" = 1;
                                        "min" = 1;
                                    };
                                };
                                "completed" = {
                                    "map" = {
                                        "max" = 1;
                                        "count" = 84;
                                        "sum" = 84;
                                        "min" = 1;
                                    };
                                };
                            };
                        };
                    };
                };
            };
            "total_job_counter" = {
                "completed" = {
                    "total" = 3;
                    "non-interrupted" = 1;
                    "interrupted" = {
                        "whatever_interrupted" = 2;
                    };
                };
                "aborted" = {
                    "non_scheduled" = {
                        "whatever_non_scheduled" = 3;
                    };
                    "scheduled" = {
                        "whatever_scheduled" = 4;
                    };
                    "total" = 7;
                };
                "lost" = 5;
                "invalidated" = 6;
                "failed" = 7;
                "running" = 8;
                "suspended" = 9;
                "pending" = 10;
                "blocked" = 11;
                "total" = 66;
            };
        };
        "events" = [
            {"state" = "pending"; "time" = "2018-01-01T00:00:00.000000Z";};
            {"state" = "materializing"; "time" = "2018-01-02T00:00:00.000000Z";};
            {"state" = "running"; "time" = "2018-01-03T00:00:00.000000Z";};
        ];
    })""");
    auto attrs = ParseOperationAttributes(NodeFromYsonString(response));

    EXPECT_TRUE(attrs.Id);
    EXPECT_EQ(GetGuidAsString(*attrs.Id), "1-2-3-4");

    EXPECT_TRUE(attrs.Type);
    EXPECT_EQ(*attrs.Type, EOperationType::Map);

    EXPECT_TRUE(attrs.State);
    EXPECT_EQ(*attrs.State, "completed");

    EXPECT_TRUE(attrs.BriefState);
    EXPECT_EQ(*attrs.BriefState, EOperationBriefState::Completed);

    EXPECT_TRUE(attrs.AuthenticatedUser);
    EXPECT_EQ(*attrs.AuthenticatedUser, "some-user");

    EXPECT_TRUE(attrs.StartTime);
    EXPECT_TRUE(attrs.FinishTime);
    EXPECT_EQ(*attrs.FinishTime - *attrs.StartTime, TDuration::Days(1));

    EXPECT_TRUE(attrs.BriefProgress);
    EXPECT_EQ(attrs.BriefProgress->Completed, 84u);
    EXPECT_EQ(attrs.BriefProgress->Failed, 1u);

    EXPECT_TRUE(attrs.BriefSpec);
    EXPECT_EQ((*attrs.BriefSpec)["title"].AsString(), "some-title");

    EXPECT_TRUE(attrs.Suspended);
    EXPECT_EQ(*attrs.Suspended, false);

    EXPECT_TRUE(attrs.Result);
    EXPECT_TRUE(!attrs.Result->Error);

    EXPECT_TRUE(attrs.Progress);
    EXPECT_EQ(attrs.Progress->JobStatistics.JobState({}).GetStatistics("data/input/row_count").Sum(), 85u);
    EXPECT_EQ(attrs.Progress->JobCounters.GetCompletedInterrupted().GetTotal(), 2u);
    EXPECT_EQ(attrs.Progress->JobCounters.GetAbortedNonScheduled().GetTotal(), 3u);
    EXPECT_EQ(attrs.Progress->JobCounters.GetAbortedScheduled().GetTotal(), 4u);
    EXPECT_EQ(attrs.Progress->JobCounters.GetAborted().GetTotal(), 7u);
    EXPECT_EQ(attrs.Progress->JobCounters.GetFailed().GetTotal(), 7u);
    EXPECT_EQ(attrs.Progress->JobCounters.GetTotal(), 66u);
    EXPECT_EQ(*attrs.Progress->BuildTime, TInstant::ParseIso8601("2018-01-01T00:00:00.000000Z"));

    EXPECT_TRUE(attrs.Events);
    EXPECT_EQ((*attrs.Events)[1].State, "materializing");
    EXPECT_EQ((*attrs.Events)[1].Time, TInstant::ParseIso8601("2018-01-02T00:00:00.000000Z"));
}

TEST(TOperationsApiParsingTest, EmptyProgress)
{
    auto response = TStringBuf(R"""({
        "id" = "1-2-3-4";
        "brief_progress" = {};
        "progress" = {};
    })""");
    auto attrs = ParseOperationAttributes(NodeFromYsonString(response));

    EXPECT_TRUE(attrs.Id);
    EXPECT_EQ(GetGuidAsString(*attrs.Id), "1-2-3-4");

    EXPECT_TRUE(!attrs.BriefProgress);

    EXPECT_TRUE(attrs.Progress);
    EXPECT_EQ(attrs.Progress->JobStatistics.JobState({}).GetStatisticsNames(), TVector<TString>{});
    EXPECT_EQ(attrs.Progress->JobCounters.GetTotal(), 0u);
    EXPECT_TRUE(!attrs.Progress->BuildTime);
}

#include <yt/cpp/mapreduce/interface/job_statistics.h>
#include <yt/cpp/mapreduce/interface/operation.h>

#include <library/cpp/yson/node/node_io.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYT;

TEST(TJobStatisticsTest, Simple)
{
    const TString input = R"""(
        {
            "data" = {
                "output" = {
                    "0" = {
                        "uncompressed_data_size" = {
                            "$" = {
                                "completed" = {
                                    "simple_sort" = {
                                        "max" = 130;
                                        "count" = 1;
                                        "min" = 130;
                                        "sum" = 130;
                                    };
                                    "map" = {
                                        "max" = 42;
                                        "count" = 1;
                                        "min" = 42;
                                        "sum" = 42;
                                    };
                                };
                                "aborted" = {
                                    "simple_sort" = {
                                        "max" = 24;
                                        "count" = 1;
                                        "min" = 24;
                                        "sum" = 24;
                                    };
                                };
                            };
                        };
                    };
                };
            };
        })""";

    TJobStatistics stat(NodeFromYsonString(input));

    EXPECT_TRUE(stat.HasStatistics("data/output/0/uncompressed_data_size"));
    EXPECT_TRUE(!stat.HasStatistics("nonexistent-statistics"));
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(stat.GetStatistics("BLAH-BLAH"), yexception, "Statistics");

    EXPECT_EQ(stat.GetStatisticsNames(), TVector<TString>{"data/output/0/uncompressed_data_size"});

    EXPECT_EQ(stat.GetStatistics("data/output/0/uncompressed_data_size").Max(), 130);
    EXPECT_EQ(stat.GetStatistics("data/output/0/uncompressed_data_size").Count(), 2);
    EXPECT_EQ(stat.GetStatistics("data/output/0/uncompressed_data_size").Min(), 42);
    EXPECT_EQ(stat.GetStatistics("data/output/0/uncompressed_data_size").Sum(), 172);
    EXPECT_EQ(stat.GetStatistics("data/output/0/uncompressed_data_size").Avg(), 172 / 2);

    EXPECT_EQ(stat.JobState({EJobState::Aborted}).GetStatistics("data/output/0/uncompressed_data_size").Sum(), 24);
    EXPECT_EQ(stat.JobType({EJobType::Map}).JobState({EJobState::Aborted}).GetStatistics("data/output/0/uncompressed_data_size").Sum(), TMaybe<i64>());
}

TEST(TJobStatisticsTest, OtherTypes)
{
    const TString input = R"""(
    {
        "time" = {
            "exec" = {
                "$" = {
                    "completed" = {
                        "map" = {
                            "max" = 2482468;
                            "count" = 38;
                            "min" = 578976;
                            "sum" = 47987270;
                        };
                    };
                };
            };
        };
    })""";

    TJobStatistics stat(NodeFromYsonString(input));

    EXPECT_EQ(stat.GetStatisticsAs<TDuration>("time/exec").Max(), TDuration::MilliSeconds(2482468));
}

TEST(TJobStatisticsTest, Custom)
{
    const TString input = R"""(
        {
            "custom" = {
                "some" = {
                    "path" = {
                        "$" = {
                            "completed" = {
                                "map" = {
                                    "max" = -1;
                                    "count" = 1;
                                    "min" = -1;
                                    "sum" = -1;
                                };
                            };
                        };
                    };
                };
                "another" = {
                    "path" = {
                        "$" = {
                            "completed" = {
                                "map" = {
                                    "max" = 1001;
                                    "count" = 2;
                                    "min" = 1001;
                                    "sum" = 2002;
                                };
                            };
                        };
                    };
                };
            };
        })""";

    TJobStatistics stat(NodeFromYsonString(input));

    EXPECT_TRUE(stat.HasCustomStatistics("some/path"));
    EXPECT_TRUE(!stat.HasCustomStatistics("nonexistent-statistics"));
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(stat.GetCustomStatistics("BLAH-BLAH"), yexception, "Statistics");

    const auto names = stat.GetCustomStatisticsNames();
    const THashSet<TString> expected = {"some/path", "another/path"};
    EXPECT_EQ(THashSet<TString>(names.begin(), names.end()), expected);

    EXPECT_EQ(stat.GetCustomStatistics("some/path").Max(), -1);
    EXPECT_EQ(stat.GetCustomStatistics("another/path").Avg(), 1001);
}

TEST(TJobStatisticsTest, TaskNames)
{
    const TString input = R"""(
        {
            "data" = {
                "output" = {
                    "0" = {
                        "uncompressed_data_size" = {
                            "$" = {
                                "completed" = {
                                    "partition_map" = {
                                        "max" = 130;
                                        "count" = 1;
                                        "min" = 130;
                                        "sum" = 130;
                                    };
                                    "partition(0)" = {
                                        "max" = 42;
                                        "count" = 1;
                                        "min" = 42;
                                        "sum" = 42;
                                    };
                                };
                                "aborted" = {
                                    "simple_sort" = {
                                        "max" = 24;
                                        "count" = 1;
                                        "min" = 24;
                                        "sum" = 24;
                                    };
                                };
                            };
                        };
                    };
                };
            };
        })""";

    TJobStatistics stat(NodeFromYsonString(input));

    EXPECT_TRUE(stat.HasStatistics("data/output/0/uncompressed_data_size"));
    EXPECT_TRUE(!stat.HasStatistics("nonexistent-statistics"));
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(stat.GetStatistics("BLAH-BLAH"), yexception, "Statistics");

    EXPECT_EQ(stat.GetStatisticsNames(), TVector<TString>{"data/output/0/uncompressed_data_size"});

    EXPECT_EQ(stat.GetStatistics("data/output/0/uncompressed_data_size").Max(), 130);
    EXPECT_EQ(stat.GetStatistics("data/output/0/uncompressed_data_size").Count(), 2);
    EXPECT_EQ(stat.GetStatistics("data/output/0/uncompressed_data_size").Min(), 42);
    EXPECT_EQ(stat.GetStatistics("data/output/0/uncompressed_data_size").Sum(), 172);
    EXPECT_EQ(stat.GetStatistics("data/output/0/uncompressed_data_size").Avg(), 172 / 2);

    EXPECT_EQ(
        stat
            .JobState({EJobState::Aborted})
            .GetStatistics("data/output/0/uncompressed_data_size")
            .Sum(),
        24);
    EXPECT_EQ(
        stat
            .JobType({EJobType::Partition})
            .JobState({EJobState::Aborted})
            .GetStatistics("data/output/0/uncompressed_data_size")
            .Sum(),
        TMaybe<i64>());
    EXPECT_EQ(
        stat
            .TaskName({"partition(0)"})
            .GetStatistics("data/output/0/uncompressed_data_size")
            .Sum(),
        42);
    EXPECT_EQ(
        stat
            .TaskName({"partition"})
            .GetStatistics("data/output/0/uncompressed_data_size")
            .Sum(),
        TMaybe<i64>());
    EXPECT_EQ(
        stat
            .TaskName({"partition_map(0)"})
            .GetStatistics("data/output/0/uncompressed_data_size")
            .Sum(),
        130);
    EXPECT_EQ(
        stat
            .JobType({EJobType::Partition})
            .GetStatistics("data/output/0/uncompressed_data_size")
            .Sum(),
        42);
    EXPECT_EQ(
        stat
            .JobType({EJobType::PartitionMap})
            .GetStatistics("data/output/0/uncompressed_data_size")
            .Sum(),
        130);
    EXPECT_EQ(
        stat
            .TaskName({ETaskName::Partition0})
            .GetStatistics("data/output/0/uncompressed_data_size")
            .Sum(),
        42);
    EXPECT_EQ(
        stat
            .TaskName({ETaskName::Partition1})
            .GetStatistics("data/output/0/uncompressed_data_size")
            .Sum(),
        TMaybe<i64>());
    EXPECT_EQ(
        stat
            .TaskName({ETaskName::PartitionMap0})
            .GetStatistics("data/output/0/uncompressed_data_size")
            .Sum(),
        130);
}

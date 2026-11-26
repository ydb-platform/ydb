#include <yt/cpp/mapreduce/interface/job_statistics.h>
#include <yt/cpp/mapreduce/interface/operation.h>

#include <library/cpp/yson/node/node_io.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYT;

TEST(TJobStatisticsTest, TaggedStatistics)
{
    TJobStatistics stat(NodeFromYsonString(R"""({
        data = { input = { data_weight = [
            {tags={job_state=running;job_type=map;pool_tree=physical};summary={sum=30;count=2;min=10;max=20}};
            {tags={job_state=running;job_type=map;pool_tree=cloud};summary={sum=40;count=1;min=40;max=40}};
            {tags={job_state=completed;job_type=map;pool_tree=physical};summary={sum=100;count=1;min=100;max=100}};
            {tags={job_state=failed;job_type=map;pool_tree=physical};summary={sum=500;count=1;min=500;max=500}};
            {tags={job_state=unknown;job_type=map;pool_tree=physical};summary={sum=1000;count=1;min=1000;max=1000}};
        ]; row_count = [
            {tags={job_state=running;job_type=partition_map;pool_tree=physical};summary={sum=7;count=1;min=7;max=7}};
        ]}};
        custom = {"path/with/slashes" = [
            {tags={job_state=completed;job_type=map;pool_tree=physical};summary={sum=-3;count=1;min=-3;max=-3}};
            {tags={job_state=completed;job_type=map;pool_tree=cloud};summary={sum=-5;count=1;min=-5;max=-5}};
        ]};
    })"""));

    EXPECT_EQ(stat.GetStatistics("data/input/data_weight").Sum(), 100);
    EXPECT_EQ(stat.GetStatistics("data/input/row_count").Sum(), TMaybe<i64>());
    auto running = stat.JobState({EJobState::Running}).GetStatistics("data/input/data_weight");
    EXPECT_EQ(running.Sum(), 70);
    EXPECT_EQ(running.Count(), 3);
    EXPECT_EQ(running.Min(), 10);
    EXPECT_EQ(running.Max(), 40);
    EXPECT_EQ(running.Avg(), 70 / 3);
    EXPECT_EQ(stat.JobState({EJobState::Running, EJobState::Completed}).GetStatistics("data/input/data_weight").Sum(), 170);
    EXPECT_EQ(stat.JobState({}).GetStatistics("data/input/data_weight").Sum(), 670);
    EXPECT_EQ(stat.JobState({EJobState::Running}).TaskName({ETaskName::PartitionMap0}).GetStatistics("data/input/row_count").Sum(), 7);
    EXPECT_EQ(stat.JobState({EJobState::Running}).TaskName({ETaskName::PartitionMap0}).GetStatistics("data/input/data_weight").Sum(), TMaybe<i64>());
    EXPECT_EQ(stat.GetCustomStatistics("path\\/with\\/slashes").Sum(), -8);
    EXPECT_EQ(stat.GetCustomStatistics("path\\/with\\/slashes").Min(), -5);
    EXPECT_EQ(stat.GetCustomStatistics("path\\/with\\/slashes").Max(), -3);
}

TEST(TJobStatisticsTest, Simple)
{
    const TStringBuf input = R"""(
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
    const TStringBuf input = R"""(
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
    const TStringBuf input = R"""(
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
    const TStringBuf input = R"""(
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

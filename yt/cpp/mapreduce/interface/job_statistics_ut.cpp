#include <yt/cpp/mapreduce/interface/job_statistics.h>
#include <yt/cpp/mapreduce/interface/operation.h>

#include <library/cpp/yson/node/node_io.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYT;

Y_UNIT_TEST_SUITE(JobStatistics)
{
    Y_UNIT_TEST(Simple)
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

        UNIT_ASSERT(stat.HasStatistics("data/output/0/uncompressed_data_size"));
        UNIT_ASSERT(!stat.HasStatistics("nonexistent-statistics"));
        UNIT_ASSERT_EXCEPTION_CONTAINS(stat.GetStatistics("BLAH-BLAH"), yexception, "Statistics");

        UNIT_ASSERT_VALUES_EQUAL(stat.GetStatisticsNames(), TVector<TString>{"data/output/0/uncompressed_data_size"});

        UNIT_ASSERT_VALUES_EQUAL(stat.GetStatistics("data/output/0/uncompressed_data_size").Max(), 130);
        UNIT_ASSERT_VALUES_EQUAL(stat.GetStatistics("data/output/0/uncompressed_data_size").Count(), 2);
        UNIT_ASSERT_VALUES_EQUAL(stat.GetStatistics("data/output/0/uncompressed_data_size").Min(), 42);
        UNIT_ASSERT_VALUES_EQUAL(stat.GetStatistics("data/output/0/uncompressed_data_size").Sum(), 172);
        UNIT_ASSERT_VALUES_EQUAL(stat.GetStatistics("data/output/0/uncompressed_data_size").Avg(), 172 / 2);

        UNIT_ASSERT_VALUES_EQUAL(stat.JobState({EJobState::Aborted}).GetStatistics("data/output/0/uncompressed_data_size").Sum(), 24);
        UNIT_ASSERT_VALUES_EQUAL(stat.JobType({EJobType::Map}).JobState({EJobState::Aborted}).GetStatistics("data/output/0/uncompressed_data_size").Sum(), TMaybe<i64>());
    }

    Y_UNIT_TEST(TestOtherTypes)
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

        UNIT_ASSERT_VALUES_EQUAL(stat.GetStatisticsAs<TDuration>("time/exec").Max(), TDuration::MilliSeconds(2482468));
    }

    Y_UNIT_TEST(Custom)
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

        UNIT_ASSERT(stat.HasCustomStatistics("some/path"));
        UNIT_ASSERT(!stat.HasCustomStatistics("nonexistent-statistics"));
        UNIT_ASSERT_EXCEPTION_CONTAINS(stat.GetCustomStatistics("BLAH-BLAH"), yexception, "Statistics");

        const auto names = stat.GetCustomStatisticsNames();
        const THashSet<TString> expected = {"some/path", "another/path"};
        UNIT_ASSERT_VALUES_EQUAL(THashSet<TString>(names.begin(), names.end()), expected);

        UNIT_ASSERT_VALUES_EQUAL(stat.GetCustomStatistics("some/path").Max(), -1);
        UNIT_ASSERT_VALUES_EQUAL(stat.GetCustomStatistics("another/path").Avg(), 1001);
    }

    Y_UNIT_TEST(TaskNames)
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

        UNIT_ASSERT(stat.HasStatistics("data/output/0/uncompressed_data_size"));
        UNIT_ASSERT(!stat.HasStatistics("nonexistent-statistics"));
        UNIT_ASSERT_EXCEPTION_CONTAINS(stat.GetStatistics("BLAH-BLAH"), yexception, "Statistics");

        UNIT_ASSERT_VALUES_EQUAL(stat.GetStatisticsNames(), TVector<TString>{"data/output/0/uncompressed_data_size"});

        UNIT_ASSERT_VALUES_EQUAL(stat.GetStatistics("data/output/0/uncompressed_data_size").Max(), 130);
        UNIT_ASSERT_VALUES_EQUAL(stat.GetStatistics("data/output/0/uncompressed_data_size").Count(), 2);
        UNIT_ASSERT_VALUES_EQUAL(stat.GetStatistics("data/output/0/uncompressed_data_size").Min(), 42);
        UNIT_ASSERT_VALUES_EQUAL(stat.GetStatistics("data/output/0/uncompressed_data_size").Sum(), 172);
        UNIT_ASSERT_VALUES_EQUAL(stat.GetStatistics("data/output/0/uncompressed_data_size").Avg(), 172 / 2);

        UNIT_ASSERT_VALUES_EQUAL(
            stat
                .JobState({EJobState::Aborted})
                .GetStatistics("data/output/0/uncompressed_data_size")
                .Sum(),
            24);
        UNIT_ASSERT_VALUES_EQUAL(
            stat
                .JobType({EJobType::Partition})
                .JobState({EJobState::Aborted})
                .GetStatistics("data/output/0/uncompressed_data_size")
                .Sum(),
            TMaybe<i64>());
        UNIT_ASSERT_VALUES_EQUAL(
            stat
                .TaskName({"partition(0)"})
                .GetStatistics("data/output/0/uncompressed_data_size")
                .Sum(),
            42);
        UNIT_ASSERT_VALUES_EQUAL(
            stat
                .TaskName({"partition"})
                .GetStatistics("data/output/0/uncompressed_data_size")
                .Sum(),
            TMaybe<i64>());
        UNIT_ASSERT_VALUES_EQUAL(
            stat
                .TaskName({"partition_map(0)"})
                .GetStatistics("data/output/0/uncompressed_data_size")
                .Sum(),
            130);
        UNIT_ASSERT_VALUES_EQUAL(
            stat
                .JobType({EJobType::Partition})
                .GetStatistics("data/output/0/uncompressed_data_size")
                .Sum(),
            42);
        UNIT_ASSERT_VALUES_EQUAL(
            stat
                .JobType({EJobType::PartitionMap})
                .GetStatistics("data/output/0/uncompressed_data_size")
                .Sum(),
            130);
        UNIT_ASSERT_VALUES_EQUAL(
            stat
                .TaskName({ETaskName::Partition0})
                .GetStatistics("data/output/0/uncompressed_data_size")
                .Sum(),
            42);
        UNIT_ASSERT_VALUES_EQUAL(
            stat
                .TaskName({ETaskName::Partition1})
                .GetStatistics("data/output/0/uncompressed_data_size")
                .Sum(),
            TMaybe<i64>());
        UNIT_ASSERT_VALUES_EQUAL(
            stat
                .TaskName({ETaskName::PartitionMap0})
                .GetStatistics("data/output/0/uncompressed_data_size")
                .Sum(),
            130);
    }
}

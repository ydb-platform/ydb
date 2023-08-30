#include "run_ydb.h"

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

#include <util/string/cast.h>
#include <util/string/split.h>
#include <util/system/env.h>
#include <util/system/shellcommand.h>

Y_UNIT_TEST_SUITE(YdbWorkloadTopic) {

TString ExecYdb(const TList<TString>& args)
{
    //
    // ydb -e grpc://${YDB_ENDPOINT} -d /${YDB_DATABASE} workload topic ${args}
    //
    return RunYdb({"workload", "topic"}, args);
}

struct TTopicConfigurationMatcher {
    TString Topic = "workload-topic";
    ui32 Partitions = 128;
    ui32 Consumers = 1;
};

void ExpectTopic(const TTopicConfigurationMatcher& matcher)
{
    NYdb::TDriverConfig config;
    config.SetEndpoint(GetEnv("YDB_ENDPOINT"));
    config.SetDatabase(GetEnv("YDB_DATABASE"));

    NYdb::TDriver driver(config);
    NYdb::NTopic::TTopicClient client(driver);

    auto result =
        client.DescribeTopic(matcher.Topic).GetValueSync();
    auto& description = result.GetTopicDescription();

    UNIT_ASSERT_VALUES_EQUAL(description.GetPartitions().size(), matcher.Partitions);
    UNIT_ASSERT_VALUES_EQUAL(description.GetConsumers().size(), matcher.Consumers);
}

Y_UNIT_TEST(Default_RunFull) {
    ExecYdb({"init"});
    auto output = ExecYdb({"run", "full", "-s", "10"});
    ExecYdb({"clean"});

    TVector<TString> lines, columns;

    ui64 fullTime = GetFullTimeValue(output);

    UNIT_ASSERT_GE(fullTime, 0);
    UNIT_ASSERT_LT(fullTime, 10'000);
}

Y_UNIT_TEST(Init_Clean)
{
    //
    // default `init` + `clean`
    //
    ExecYdb({"init"});
    ExpectTopic({.Topic="workload-topic", .Partitions=128, .Consumers=1});

    ExecYdb({"clean"});
    ExpectTopic({.Topic="workload-topic", .Partitions=0, .Consumers=0});

    //
    // specific `init` + `clean`
    //
    ExecYdb({"init", "--topic", "qqqq", "-p", "3", "-c", "5"});
    ExpectTopic({.Topic="qqqq", .Partitions=3, .Consumers=5});

    UNIT_ASSERT_EXCEPTION(ExecYdb({"clean"}), yexception);

    ExecYdb({"clean", "--topic", "qqqq"});
    ExpectTopic({.Topic="qqqq", .Partitions=0, .Consumers=0});
}

Y_UNIT_TEST(Clean_Without_Init)
{
    UNIT_ASSERT_EXCEPTION(ExecYdb({"clean"}), yexception);
}

Y_UNIT_TEST(Double_Init)
{
    ExecYdb({"init"});
    UNIT_ASSERT_EXCEPTION(ExecYdb({"init"}), yexception);
}

}

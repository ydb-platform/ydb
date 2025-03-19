#include "run_ydb.h"

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/lib/ydb_cli/commands/topic_workload/topic_workload_defines.h>

#include <util/string/cast.h>
#include <util/string/split.h>
#include <util/system/env.h>
#include <util/system/shellcommand.h>

namespace {
class TFixture : public NUnitTest::TBaseFixture {

    public:
        void TearDown(NUnitTest::TTestContext&) override {
            try {
                ExecYdb({"clean"});
            } catch (yexception) {
                // ignore errors
            }
        }

        TString ExecYdb(const TList<TString>& args, bool checkExitCode = true)
        {
            //
            // ydb -e grpc://${YDB_ENDPOINT} -d /${YDB_DATABASE} workload topic ${args}
            //
            return RunYdb({"workload", "topic"}, args, checkExitCode);
        }

        ui32 CountMessagesInPartition(ui32 partition) {
            auto output = RunYdb({},
                            {"topic", "read", NYdb::NConsoleClient::TOPIC,
                            "--consumer", NYdb::NConsoleClient::CONSUMER_PREFIX + "-0",
                            "--partition-ids", ToString(partition),
                            "--commit", "0",
                            "--format", "newline-delimited",
                            "--limit", "200"});
            TVector<TString> lines;
            Split(output, "\n", lines);

            return lines.size();
        }

        void EnsureStatisticsColumns(const TList<TString>& args,
                                    const TList<TVector<TString>>& columns)
        {
            ExecYdb({"init"});
            auto output = ExecYdb(args, false);

            TVector<TString> lines;
            Split(output, "\n", lines);

            UNIT_ASSERT_GE(lines.size(), columns.size());

            auto iter = columns.begin();
            for (ui32 i = 0; i < columns.size(); i++) {
                UnitAssertColumnsOrder(lines[i], *iter);
                iter++;
            }
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

        TVector<TString> ParseStatisticsLine(const TString& line) {
            TVector<TString> values;
            Split(line, "\t", values);
            return values;
        }
};

Y_UNIT_TEST_SUITE_F(YdbWorkloadTopic, TFixture) {

Y_UNIT_TEST(Default_RunFull) {
    ExecYdb({"init"});
    auto output = ExecYdb({"run", "full", "-s", "10"});

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

Y_UNIT_TEST(Read_Statistics)
{
    EnsureStatisticsColumns({"run", "read", "-s", "1", "--warmup", "0"},
                            {{"Window", "Lag", "Lag time", "Read speed", "Full time"},
                            {"#", "percentile,msg", "percentile,ms", "msg/s", "MB/s", "percentile,ms"}});
}

Y_UNIT_TEST(Write_Statistics)
{
    EnsureStatisticsColumns({"run", "write", "-s", "1", "--warmup", "0"},
                            {{"Window", "Write speed", "Write time", "Inflight"},
                            {"#", "msg/s", "MB/s", "percentile,ms", "percentile,msg"}});
}

Y_UNIT_TEST(ReadWrite_Statistics)
{
    EnsureStatisticsColumns({"run", "full", "-s", "1", "--warmup", "0"},
                            {{"Window", "Write speed", "Write time", "Inflight", "Lag", "Lag time", "Read speed", "Full time"},
                            {"#", "msg/s", "MB/s", "percentile,ms", "percentile,msg", "percentile,msg", "percentile,ms", "msg/s", "MB/s", "percentile,ms"}});
}

Y_UNIT_TEST(Write_Statistics_UseTx)
{
    EnsureStatisticsColumns({"run", "write", "-s", "1", "--warmup", "0", "--use-tx"},
                            {{"Window", "Write speed", "Write time", "Inflight", "Select time", "Upsert time", "Commit time"},
                            {"#", "msg/s", "MB/s", "percentile,ms", "percentile,msg", "percentile,ms", "percentile,ms", "percentile,ms"}});
}

Y_UNIT_TEST(Full_Statistics_UseTx)
{
    ExecYdb({"init"});
    auto output = ExecYdb(
        {"run", "full", "-s", "1", "--warmup", "0", "--use-tx", "--message-rate", "5", "--tx-commit-interval", "500"}, false);
    TVector<TString> lines;
    Split(output, "\n", lines);
     
     // at least 3 lines are expeted: first - headers (e.g. Write speed), second - subheaders (e.g. msg./s, MB/s), third - values
    UNIT_ASSERT_GE(lines.size(), 3); 

    // assert headers are correct
    UnitAssertColumnsOrder(
        lines[0], 
        {"Window", "Write speed", "Write time", "Inflight", "Read speed", "Topic time", "Select time", "Upsert time", "Commit time"}
    );
    const TVector<TString> expectedSubheaders = {"#", "msg/s", "MB/s", "percentile,ms", "percentile,msg", "msg/s", "MB/s", "percentile,ms", "percentile,ms", "percentile,ms", "percentile,ms"};
    TVector<TString> values = ParseStatisticsLine(lines[2]);
    UnitAssertColumnsOrder(lines[1], expectedSubheaders);
    // assert there are correct values in output messages per second
    UNIT_ASSERT_GT(std::stoi(values[1]), 1);
    UNIT_ASSERT_LE(std::stoi(values[1]), 5);
}

Y_UNIT_TEST(WriteInTx)
{
    // In the test, 6 writers write messages within 10 seconds.
    // Then the number of recorded messages is checked. Commit transactions every second.
    // It is expected that at least 60 messages will be written.

    ExecYdb({"init",
            "--partitions", "3"});

    auto output = ExecYdb({"run", "write",
                          "--threads", "6",
                          "--byte-rate", "102400",
                          "--message-size", "10240",
                          "--use-tx",
                          "--commit-messages", "10",
                          "--warmup", "2",
                          "--seconds", "10"});
    ui64 commitTimeValue = GetCommitTimeValue(output);

    output = RunYdb({},
                    {"topic", "read", "workload-topic",
                    "--consumer", "workload-consumer-0",
                    "--commit", "0",
                    "--format", "newline-delimited",
                    "--limit", "200"});
    TVector<TString> lines;
    Split(output, "\n", lines);

    // The value in the 'Commit time` column is greater than 0
    UNIT_ASSERT_GT(commitTimeValue, 0);

    UNIT_ASSERT_GE(lines.size(), 60);
}

Y_UNIT_TEST(WriteProducesToAllPartitionsEvenly)
{
    // In the test 1 writer writes messages to three partitions.
    // Then it is checked that there are even number of messages in all partitions.
    ExecYdb({"init", "--partitions", "3"});

    auto output = ExecYdb({"run", "write",
                          "--threads", "1",
                          "--message-rate", "2", // 2 messages per second
                          "--use-tx",
                          "--tx-commit-interval", "1000",
                          "--seconds", "6"});

    UNIT_ASSERT_GE(CountMessagesInPartition(0), 4);
    UNIT_ASSERT_GE(CountMessagesInPartition(1), 4);
    UNIT_ASSERT_GE(CountMessagesInPartition(2), 4);
}

}
} // anonymous namespace

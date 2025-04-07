#include "run_ydb.h"
#include <iostream>

#include <util/string/split.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

namespace {
class TFixture : public NUnitTest::TBaseFixture {

   
    public:
        TString TopicName;
        TString TableName;

    public:
        void SetUp(NUnitTest::TTestContext&) override {
            // we need to generate random topic name and table name, cause in CI tests are executed parallely against one common database
            TopicName = GenerateTopicName();;
            TableName = GenerateTableName();

            try {
                ExecYdb({"init", "--topic", TopicName, "--table", TableName});
            } catch (const yexception) {
                // ignore errors
            }
        }

        void TearDown(NUnitTest::TTestContext&) override {
            try {
                ExecYdb({"clean", "--topic", TopicName, "--table", TableName});
            } catch (const yexception) {
                // ignore errors
            }
        }

        struct TTopicConfigMatcher {
            TString Name;
            ui32 Partitions;
            ui32 Consumers;
        };

        struct TTableConfigMatcher {
            TString Name;
            ui32 Partitions;
        };

        NYdb::NTable::TSession GetSession(NYdb::NTable::TTableClient& client)
        {
            auto result = client.GetSession().GetValueSync();
            return result.GetSession();
        }

        void ExpectTopic(const TTopicConfigMatcher& matcher)
        {
            NYdb::TDriverConfig config;
            config.SetEndpoint(GetYdbEndpoint());
            config.SetDatabase(GetYdbDatabase());

            NYdb::TDriver driver(config);
            NYdb::NTopic::TTopicClient client(driver);

            auto result = client.DescribeTopic(matcher.Name).GetValueSync();
            if (result.GetStatus() == NYdb::EStatus::SCHEME_ERROR) {
                UNIT_ASSERT_VALUES_EQUAL(0, matcher.Partitions);
                UNIT_ASSERT_VALUES_EQUAL(0, matcher.Consumers);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);

                auto& description = result.GetTopicDescription();

                UNIT_ASSERT_VALUES_EQUAL(description.GetPartitions().size(), matcher.Partitions);
                UNIT_ASSERT_VALUES_EQUAL(description.GetConsumers().size(), matcher.Consumers);
            }
        }

        void ExpectTable(const TTableConfigMatcher& matcher)
        {
            NYdb::TDriverConfig config;
            config.SetEndpoint(GetYdbEndpoint());
            config.SetDatabase(GetYdbDatabase());

            NYdb::TDriver driver(config);
            NYdb::NTable::TTableClient client(driver);
            auto session = GetSession(client);
            
            NYdb::NTable::TDescribeTableSettings options;
            options.WithTableStatistics(true);

            auto result = session.DescribeTable("/" + GetYdbDatabase() + "/" + matcher.Name,
                                                options).GetValueSync();
            if (result.GetStatus() == NYdb::EStatus::SCHEME_ERROR) {
                UNIT_ASSERT_VALUES_EQUAL(0, matcher.Partitions);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);

                auto description = result.GetTableDescription();

                UNIT_ASSERT_VALUES_EQUAL(description.GetPartitionsCount(), matcher.Partitions);
            }
        }

        TString ExecYdb(const TList<TString>& args, bool checkExitCode = true)
        {
            //
            // ydb -e grpc://${YDB_ENDPOINT} -d /${YDB_DATABASE} workload transfer topic-to-table ${args}
            //
            return RunYdb({"workload", "transfer", "topic-to-table"}, args, checkExitCode);
        }

        void RunYdbAndAssertTopicAndTableProps(const TList<TString>& args,
                    const TString& topic, ui32 topicPartitions, ui32 consumers,
                    const TString& table, ui32 tablePartitions)
        {
            ExecYdb(args);
            ExpectTopic({.Name=topic, .Partitions=topicPartitions, .Consumers=consumers});
            ExpectTable({.Name=table, .Partitions=tablePartitions});
        }

        void EnsureStatisticsColumns(const TList<TString>& args,
                             const TVector<TString>& columns1,
                             const TVector<TString>& columns2)
        {
            ExecYdb({"init"});
            auto output = ExecYdb(args, false);

            TVector<TString> lines;
            Split(output, "\n", lines);

            UnitAssertColumnsOrder(lines[0], columns1);
            UnitAssertColumnsOrder(lines[1], columns2);

            ExecYdb({"clean"});
        }

        TString GenerateTopicName()
        {
            return "transfer-topic-" + GetRandomString(4);
        }

        TString GenerateTableName()
        {
            return "transfer-table-" + GetRandomString(4);
        }
    private:

        TString GetRandomString(int len)
        {
            TString str;
            for (int i = 0; i < len; ++i) {
                str.push_back('a' + rand() % 26);
            }
            return str;
        }
};

Y_UNIT_TEST_SUITE_F(YdbWorkloadTransferTopicToTable, TFixture) {

Y_UNIT_TEST(Default_Run)
{
    auto output = ExecYdb({"run", "-s", "10", "--topic", TopicName});

    ui64 fullTime = GetFullTimeValue(output);

    UNIT_ASSERT_GE(fullTime, 0);
    UNIT_ASSERT_LT(fullTime, 10'000);
}

Y_UNIT_TEST(Default_Init_Clean)
{
    // setip and tear down also executed but ignored
    TopicName = GenerateTopicName();;
    TableName = GenerateTableName();

    ExecYdb({"init", "--topic", TopicName, "--table", TableName});
    ExecYdb({"clean", "--topic", TopicName, "--table", TableName});
}

Y_UNIT_TEST(Specific_Init_Clean)
{
    const TString topic = GenerateTopicName();
    const TString table = GenerateTableName();

    RunYdbAndAssertTopicAndTableProps({"init",
           "--topic", topic, "--topic-partitions", "3", "--consumers", "5",
           "--table", table, "--table-partitions", "8"},
           topic, 3, 5,
           table, 8);
    RunYdbAndAssertTopicAndTableProps({"clean",
            "--topic", topic,
            "--table", table},
            topic, 0, 0,
            table, 0);
}

Y_UNIT_TEST(Clean_Without_Init)
{
    UNIT_ASSERT_EXCEPTION(ExecYdb({"clean", "--topic", GenerateTopicName(), "--table", GenerateTableName()}), yexception);
}

Y_UNIT_TEST(Double_Init)
{
    UNIT_ASSERT_EXCEPTION(ExecYdb({"init", "--topic", TopicName, "--table", TableName}), yexception);
}

Y_UNIT_TEST(Statistics)
{
    EnsureStatisticsColumns({"run", "-s", "1", "--warmup", "0", "--topic", TopicName, "--table", TableName},
                            {"Window", "Write speed", "Write time", "Inflight", "Read speed", "Topic time", "Select time", "Upsert time", "Commit time"},
                            {"#", "msg/s", "MB/s", "percentile,ms", "percentile,msg", "msg/s", "MB/s", "percentile,ms", "percentile,ms", "percentile,ms", "percentile,ms"});
}

}
} // anonymous namespace

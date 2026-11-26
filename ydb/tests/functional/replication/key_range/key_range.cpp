#include <ydb/core/transfer/ut/common/utils.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <util/generic/algorithm.h>

using namespace NReplicationTest;

Y_UNIT_TEST_SUITE(ReplicationKeyRange)
{
    TString GetSourceTablePath(const MainTestCase& testCase) {
        return TStringBuilder() << "/" << GetEnv("YDB_DATABASE") << "/" << testCase.SourceTableName;
    }

    TString WaitReplicationTopicPath(MainTestCase& testCase) {
        NYdb::NTable::TTableClient client(testCase.Driver);
        for (size_t attempt = 0; attempt < 60; ++attempt) {
            auto session = client.GetSession().GetValueSync();
            UNIT_ASSERT_C(session.IsSuccess(), session.GetIssues().ToString());
            auto result = session.GetSession().DescribeTable(GetSourceTablePath(testCase)).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            const auto changefeeds = result.GetTableDescription().GetChangefeedDescriptions();
            if (!changefeeds.empty()) {
                return GetSourceTablePath(testCase) + "/" + TString(changefeeds.front().GetName());
            }
            Sleep(TDuration::Seconds(1));
        }
        UNIT_FAIL("Replication has not created a changefeed");
        return {};
    }

    size_t CountActivePartitions(MainTestCase& testCase, const TString& topicPath) {
        auto result = testCase.TopicClient.DescribeTopic(topicPath).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
        return CountIf(result.GetTopicDescription().GetPartitions(), [](const auto& partition) {
            return partition.GetActive();
        });
    }

    std::pair<ui64, ui64> ReadCountAndChecksum(MainTestCase& testCase, const std::string& table) {
        auto result = testCase.ExecuteQuery(Sprintf(
            "SELECT COUNT(*) AS count, SUM(Key %% 1000003ul) AS checksum FROM `%s`;", table.data()), false);
        if (!result.IsSuccess()) {
            return {Max<ui64>(), Max<ui64>()};
        }
        auto parser = result.GetResultSetParser(0);
        UNIT_ASSERT(parser.TryNextRow());
        return {parser.ColumnParser("count").GetUint64(), parser.ColumnParser("checksum").GetOptionalUint64().value_or(0)};
    }

    void UpsertTransaction(MainTestCase& testCase, size_t rows) {
        TStringBuilder query;
        query << "$value = ListConcat(ListReplicate(\"x\", 20000));\n";
        query << "UPSERT INTO `" << testCase.SourceTableName << "` (Key, Value) VALUES ";
        for (size_t i = 0; i < rows; ++i) {
            query << (i ? ", " : "") << "(" << RandomNumber<ui64>() << "ul, $value)";
        }
        query << ";";
        auto result = testCase.ExecuteQuery(query);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    size_t CountTablePartitions(MainTestCase& testCase) {
        NYdb::NTable::TTableClient client(testCase.Driver);
        auto session = client.GetSession().GetValueSync();
        UNIT_ASSERT_C(session.IsSuccess(), session.GetIssues().ToString());
        auto result = session.GetSession().DescribeTable(GetSourceTablePath(testCase),
            NYdb::NTable::TDescribeTableSettings().WithKeyShardBoundary(true)).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        return result.GetTableDescription().GetKeyRanges().size();
    }

    void CheckGlobalTransactionsAcrossTopicPartitionSplit(const TString& tablePartitioning, bool singleTableShard)
    {
        // given
        constexpr size_t rowsPerTransaction = 40;
        constexpr size_t transactionsAfterSplit = 5;
        MainTestCase testCase;
        const TString sourceTableDdl = TStringBuilder() << R"(
            CREATE TABLE `%s` (
                Key Uint64 NOT NULL,
                Value String,
                PRIMARY KEY (Key)
            ) WITH ()" << tablePartitioning << R"();
        )";
        testCase.CreateSourceTable(std::string(sourceTableDdl.data(), sourceTableDdl.size()));
        testCase.ExecuteDDL(Sprintf(R"(
            CREATE ASYNC REPLICATION `%s` FOR `%s` AS `%s`
            WITH (CONNECTION_STRING = "grpc://%s", CONSISTENCY_LEVEL = "GLOBAL");
        )", testCase.ReplicationName.data(), testCase.SourceTableName.data(), testCase.TableName.data(),
                testCase.ConnectionString.data()));
        const auto topicPath = WaitReplicationTopicPath(testCase);
        UNIT_ASSERT_VALUES_EQUAL(CountActivePartitions(testCase, topicPath), 1);

        // when
        size_t attempt = 0;
        for (; attempt < 240 && CountActivePartitions(testCase, topicPath) < 2; ++attempt) {
            UpsertTransaction(testCase, rowsPerTransaction);
            const auto [count, checksum] = ReadCountAndChecksum(testCase, testCase.TableName);
            Y_UNUSED(checksum);
            UNIT_ASSERT_C(count == Max<ui64>() || count % rowsPerTransaction == 0,
                "Replica shows a partial transaction before split: " << count);
        }
        UNIT_ASSERT_C(attempt < 240, "Topic partition has not split under load");
        if (singleTableShard) {
            UNIT_ASSERT_VALUES_EQUAL(CountTablePartitions(testCase), 1);
        }
        for (size_t i = 0; i < transactionsAfterSplit; ++i) {
            UpsertTransaction(testCase, rowsPerTransaction);
        }

        // then
        const auto expected = ReadCountAndChecksum(testCase, testCase.SourceTableName);
        UNIT_ASSERT_VALUES_EQUAL(expected.first, (attempt + transactionsAfterSplit) * rowsPerTransaction);
        std::pair<ui64, ui64> actual;
        for (size_t wait = 0; wait < 300 && actual != expected; ++wait) {
            actual = ReadCountAndChecksum(testCase, testCase.TableName);
            UNIT_ASSERT_C(actual.first == Max<ui64>() || actual.first % rowsPerTransaction == 0,
                "Replica shows a partial transaction after split: " << actual.first);
            if (actual != expected) {
                Sleep(TDuration::Seconds(1));
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(actual.first, expected.first);
        UNIT_ASSERT_VALUES_EQUAL(actual.second, expected.second);
        testCase.CheckReplicationState(TReplicationDescription::EState::Running);

        testCase.DropReplication();
        testCase.DropSourceTable();
        testCase.ExecuteDDL(Sprintf("DROP TABLE `%s`;", testCase.TableName.data()));
    }

    Y_UNIT_TEST(GlobalKeyRangeKeepsTransactionsWholeAcrossTopicPartitionSplit)
    {
        CheckGlobalTransactionsAcrossTopicPartitionSplit(R"(
                AUTO_PARTITIONING_BY_SIZE = ENABLED,
                AUTO_PARTITIONING_PARTITION_SIZE_MB = 1
            )", false);
    }

    Y_UNIT_TEST(GlobalKeyRangeSplitsTopicPartitionOfSingleShardTable)
    {
        CheckGlobalTransactionsAcrossTopicPartitionSplit(R"(
                AUTO_PARTITIONING_BY_SIZE = DISABLED,
                AUTO_PARTITIONING_BY_LOAD = DISABLED
            )", true);
    }
}

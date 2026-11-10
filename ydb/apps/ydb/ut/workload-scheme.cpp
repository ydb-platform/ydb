#include "run_ydb.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/folder/tempdir.h>
#include <util/generic/guid.h>
#include <util/stream/file.h>
#include <util/string/builder.h>

namespace {

class TSchemeWorkloadFixture : public NUnitTest::TBaseFixture {
public:
    void SetUp(NUnitTest::TTestContext&) override {
        Path = "scheme_load_" + CreateGuidAsString();
        ConfigPath = Temporary.Name() + "/scheme.yaml";
    }

    void TearDown(NUnitTest::TTestContext&) override {
        RunYdb({"scheme", "rmdir"}, {"--recursive", "--force", Path}, false);
    }

    void WriteConfig(const TString& groups) {
        TOFStream out(ConfigPath);
        out << "path: " << Path << "\ncreate_concurrency: 3\nalter_concurrency: 2\n" << groups << Endl;
    }

    TString Init() {
        return RunYdb({"workload", "scheme", "init"}, {"--config", ConfigPath});
    }

    TString Run() {
        return RunYdb({"workload", "scheme", "run"}, {"--config", ConfigPath, "--seconds", "1"});
    }

    NYdb::TDriver Driver() {
        return NYdb::TDriver(NYdb::TDriverConfig().SetEndpoint(GetYdbEndpoint()).SetDatabase(std::string(Database())));
    }

    TString Database() const {
        const auto database = GetYdbDatabase();
        return database.StartsWith('/') ? database : "/" + database;
    }

    TString FullPath(const TString& name) const {
        return Database() + "/" + Path + "/" + name;
    }

    void CheckTable(NYdb::NTable::TSession session, ui32 index, bool altered) {
        const TString path = FullPath(TStringBuilder() << "table_" << index);
        const auto result = session.DescribeTable(std::string(path), NYdb::NTable::TDescribeTableSettings().WithKeyShardBoundary(true)).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        const auto& description = result.GetTableDescription();
        UNIT_ASSERT_VALUES_EQUAL(description.GetPartitioningSettings().GetMinPartitionsCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(description.GetPartitioningSettings().GetMaxPartitionsCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(description.GetKeyRanges().size(), 2);
        UNIT_ASSERT(description.GetPartitioningSettings().GetPartitioningBySize() == false);
        UNIT_ASSERT(description.GetPartitioningSettings().GetPartitioningByLoad() == false);
        const auto columns = description.GetColumns();
        UNIT_ASSERT(columns.size() == 1 || (altered && columns.size() == 4));
        for (const auto& column : columns) {
            if (column.Name != "id") {
                UNIT_ASSERT(column.Name == "extra_0" || column.Name == "extra_1" || column.Name == "extra_2");
                UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatType(column.Type), "Utf8?");
            }
        }
    }

    void CheckTopic(NYdb::NTopic::TTopicClient& client, ui32 index, bool altered) {
        const TString path = FullPath(TStringBuilder() << "topic_" << index);
        const auto result = client.DescribeTopic(std::string(path)).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        const auto& description = result.GetTopicDescription();
        UNIT_ASSERT_VALUES_EQUAL(description.GetPartitions().size(), 3);
        UNIT_ASSERT(description.GetPartitioningSettings().GetAutoPartitioningSettings().GetStrategy()
            == NYdb::NTopic::EAutoPartitioningStrategy::Disabled);
        const auto& consumers = description.GetConsumers();
        UNIT_ASSERT(consumers.empty() || (altered && consumers.size() == 2));
        for (const auto& consumer : consumers) {
            UNIT_ASSERT(consumer.GetConsumerName() == "reader_0" || consumer.GetConsumerName() == "reader_1");
        }
    }

    TTempDir Temporary;
    TString Path;
    TString ConfigPath;
};

} // namespace

Y_UNIT_TEST_SUITE_F(YdbWorkloadScheme, TSchemeWorkloadFixture) {
    Y_UNIT_TEST(MixedCreateAlterAndRestart) {
        WriteConfig("tables: {count: 2, partitions: 2, alter_batch_size: 3}\ntopics: {count: 2, partitions: 3, alter_batch_size: 2}");
        const auto created = Init();
        UNIT_ASSERT_STRING_CONTAINS(created, "CreateTable\t2");
        UNIT_ASSERT_STRING_CONTAINS(created, "CreateTopic\t2");
        auto driver = Driver();
        NYdb::NTable::TTableClient tables(driver);
        NYdb::NTopic::TTopicClient topics(driver);
        const auto session = tables.GetSession().GetValueSync().GetSession();
        for (ui32 i = 0; i < 2; ++i) {
            CheckTable(session, i, false);
            CheckTopic(topics, i, false);
        }
        for (ui32 iteration = 0; iteration < 2; ++iteration) {
            const auto output = Run();
            UNIT_ASSERT_STRING_CONTAINS(output, "AddColumns\t");
            UNIT_ASSERT_STRING_CONTAINS(output, "AddConsumers\t");
            UNIT_ASSERT(!output.Contains("AddColumns\t0\n"));
            UNIT_ASSERT(!output.Contains("AddConsumers\t0\n"));
            for (ui32 i = 0; i < 2; ++i) {
                CheckTable(session, i, true);
                CheckTopic(topics, i, true);
            }
        }
        UNIT_ASSERT_EXCEPTION_CONTAINS(Init(), yexception, "Object already exists");
    }

    Y_UNIT_TEST(TablesOnly) {
        WriteConfig("tables: {count: 1, partitions: 2, alter_batch_size: 3}");
        Init();
        Run();
        auto driver = Driver();
        NYdb::NTable::TTableClient client(driver);
        CheckTable(client.GetSession().GetValueSync().GetSession(), 0, true);
    }

    Y_UNIT_TEST(TopicsOnly) {
        WriteConfig("topics: {count: 1, partitions: 3, alter_batch_size: 2}");
        Init();
        Run();
        auto driver = Driver();
        NYdb::NTopic::TTopicClient client(driver);
        CheckTopic(client, 0, true);
    }

    Y_UNIT_TEST(MissingObjectsFailRun) {
        WriteConfig("tables: {count: 1, partitions: 1}");
        UNIT_ASSERT_EXCEPTION_CONTAINS(Run(), yexception, "DescribeTable");
    }

    Y_UNIT_TEST(PartialAlterBatchFailsBeforeMutation) {
        WriteConfig("tables: {count: 1, partitions: 2, alter_batch_size: 3}");
        Init();
        auto driver = Driver();
        NYdb::NTable::TTableClient client(driver);
        auto session = client.GetSession().GetValueSync().GetSession();
        const auto path = std::string(FullPath("table_0"));
        const auto status = session.AlterTable(path, NYdb::NTable::TAlterTableSettings().AppendAddColumns(
            NYdb::NTable::TTableColumn("extra_0", NYdb::TTypeBuilder().BeginOptional().Primitive(NYdb::EPrimitiveType::Utf8).EndOptional().Build()))).GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        UNIT_ASSERT_EXCEPTION_CONTAINS(Run(), yexception, "Unexpected or partial ALTER batch");
        const auto result = session.DescribeTable(path).GetValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(result.GetTableDescription().GetColumns().size(), 2);
    }

    Y_UNIT_TEST(InvalidConfigurationFails) {
        WriteConfig("tables: {count: 1, partitions: 0}");
        UNIT_ASSERT_EXCEPTION_CONTAINS(Init(), yexception, "tables.partitions");
        WriteConfig("tables: {count: 1, partitions: 1}");
        UNIT_ASSERT_EXCEPTION_CONTAINS(RunYdb({"workload", "scheme", "run"}, {"--config", ConfigPath, "--seconds", "0"}), yexception, "--seconds must be positive");
    }
}

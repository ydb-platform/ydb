#include "../hive_metastore_client.h"

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/common/env.h>
#include <util/string/split.h>

namespace {

TString Exec(const TString& cmd) {
    std::array<char, 128> buffer;
    TString result;
    std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"), pclose);
    if (!pipe) {
        throw std::runtime_error("popen() failed!");
    }
    while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe.get()) != nullptr) {
        result += buffer.data();
    }
    return result;
}

TString GetExternalHiveMetastorePort(const TString& service, const TString& port) {
    auto dockerComposeBin = BinaryPath("library/recipes/docker_compose/bin/docker-compose");
    auto composeFileYml = ArcadiaSourceRoot() + "/ydb/core/external_sources/hive_metastore/ut/docker-compose.yml";
    auto result = StringSplitter(Exec(dockerComposeBin + " -f " + composeFileYml + " port " + service + " " + port)).Split(':').ToList<TString>();
    return result ? result.back() : TString{};
}

void WaitHiveMetastore(const TString& host, int32_t port) {
    NKikimr::NExternalSource::THiveMetastoreClient client(host, port);
    for (int i = 0; i < 30; i++) {
        try {
            client.GetDatabase("default").GetValue(TDuration::Seconds(10));
            return;
        } catch (...) {
            Sleep(TDuration::Seconds(1));
        }
    }
    ythrow yexception() << "Hive metastore isn't ready, host: " << host << " port: " << port;
}

}

Y_UNIT_TEST_SUITE(HiveMetastoreClient) {
    Y_UNIT_TEST(SuccessRequest) {
        const TString host = "0.0.0.0";
        const int32_t port = stoi(GetExternalHiveMetastorePort("hive-metastore", "9083"));
        WaitHiveMetastore(host, port);
        
        NKikimr::NExternalSource::THiveMetastoreClient client("0.0.0.0", stoi(GetExternalHiveMetastorePort("hive-metastore", "9083")));
        {
            auto future = client.GetDatabase("default");
            UNIT_ASSERT_VALUES_EQUAL(future.GetValue(TDuration::Seconds(10)).name, "default");
        }

        {
            std::vector<Apache::Hadoop::Hive::FieldSchema> columns;
            {
                Apache::Hadoop::Hive::FieldSchema schema;
                schema.__set_name("id");
                schema.__set_type("string");
                schema.__set_comment("col comment");
                columns.push_back(schema);
            }

            {
                Apache::Hadoop::Hive::FieldSchema schema;
                schema.__set_name("client_name");
                schema.__set_type("string");
                columns.push_back(schema);
            }

            {
                Apache::Hadoop::Hive::FieldSchema schema;
                schema.__set_name("amount");
                schema.__set_type("string");
                columns.push_back(schema);
            }

            {
                Apache::Hadoop::Hive::FieldSchema schema;
                schema.__set_name("year");
                schema.__set_type("string");
                columns.push_back(schema);
            }

            {
                Apache::Hadoop::Hive::FieldSchema schema;
                schema.__set_name("month");
                schema.__set_type("string");
                columns.push_back(schema);
            }

            {
                Apache::Hadoop::Hive::FieldSchema schema;
                schema.__set_name("day");
                schema.__set_type("string");
                columns.push_back(schema);
            }

            std::vector<Apache::Hadoop::Hive::FieldSchema> partitionKeys;
            {
                Apache::Hadoop::Hive::FieldSchema schema;
                schema.__set_name("year");
                schema.__set_type("string");
                partitionKeys.push_back(schema);
            }

            {
                Apache::Hadoop::Hive::FieldSchema schema;
                schema.__set_name("month");
                schema.__set_type("string");
                partitionKeys.push_back(schema);
            }

            {
                Apache::Hadoop::Hive::FieldSchema schema;
                schema.__set_name("day");
                schema.__set_type("string");
                partitionKeys.push_back(schema);
            }

            Apache::Hadoop::Hive::SerDeInfo sreDeInfo;
            sreDeInfo.__set_serializationLib("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");

            Apache::Hadoop::Hive::StorageDescriptor descriptor;
            descriptor.__set_cols(columns);
            descriptor.__set_location("s3a://datalake/");
            descriptor.__set_inputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
            descriptor.__set_outputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat");
            descriptor.__set_serdeInfo(sreDeInfo);

            Apache::Hadoop::Hive::Table table;
            table.__set_tableName("my_table");
            table.__set_dbName("default");
            table.__set_owner("hcpp");
            table.__set_sd(descriptor);
            table.__set_partitionKeys(partitionKeys);

            auto future = client.CreateTable(table);
            UNIT_ASSERT_NO_EXCEPTION(future.GetValue(TDuration::Seconds(10)));
        }

        {
            auto table = client.GetTable("default", "my_table").GetValue(TDuration::Seconds(10));
            UNIT_ASSERT_VALUES_EQUAL(table.tableName, "my_table");
            UNIT_ASSERT_VALUES_EQUAL(table.dbName, "default");
            UNIT_ASSERT_VALUES_EQUAL(table.owner, "hcpp");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.location, "s3a://datalake/");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.inputFormat, "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.outputFormat, "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.serdeInfo.serializationLib, "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols.size(), 6);
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[0].name, "id");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[0].type, "string");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[0].comment, "col comment");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[1].name, "client_name");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[1].type, "string");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[2].name, "amount");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[2].type, "string");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[3].name, "year");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[3].type, "string");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[4].name, "month");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[4].type, "string");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[5].name, "day");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[5].type, "string");
            UNIT_ASSERT_VALUES_EQUAL(table.partitionKeys.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(table.partitionKeys[0].name, "year");
            UNIT_ASSERT_VALUES_EQUAL(table.partitionKeys[0].type, "string");
            UNIT_ASSERT_VALUES_EQUAL(table.partitionKeys[1].name, "month");
            UNIT_ASSERT_VALUES_EQUAL(table.partitionKeys[1].type, "string");
            UNIT_ASSERT_VALUES_EQUAL(table.partitionKeys[2].name, "day");
            UNIT_ASSERT_VALUES_EQUAL(table.partitionKeys[2].type, "string");
        }

        {
            std::vector<Apache::Hadoop::Hive::ColumnStatisticsObj> columnStatisticsObj;
            {
                Apache::Hadoop::Hive::StringColumnStatsData stringStats;
                stringStats.__set_avgColLen(30);
                Apache::Hadoop::Hive::ColumnStatisticsData data;
                data.__set_stringStats(stringStats);
                Apache::Hadoop::Hive::ColumnStatisticsObj obj;
                obj.__set_colName("id");
                obj.__set_colType("string");
                obj.__set_statsData(data);
                columnStatisticsObj.push_back(obj);
            }
            Apache::Hadoop::Hive::ColumnStatisticsDesc columnStatisticsDesc;
            columnStatisticsDesc.__set_dbName("default");
            columnStatisticsDesc.__set_tableName("my_table");
            Apache::Hadoop::Hive::ColumnStatistics columnStatistics;
            columnStatistics.__set_statsDesc(columnStatisticsDesc);
            columnStatistics.__set_statsObj(columnStatisticsObj);

            auto future = client.UpdateTableColumnStatistics(columnStatistics);
            UNIT_ASSERT_NO_EXCEPTION(future.GetValue(TDuration::Seconds(10)));
        }

        {
            Apache::Hadoop::Hive::TableStatsRequest request;
            request.__set_dbName("default");
            request.__set_tblName("my_table");
            request.__set_colNames({"id"});
            auto result = client.GetTableStatistics(request).GetValue(TDuration::Seconds(10));
            UNIT_ASSERT_VALUES_EQUAL(result.tableStats.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.tableStats[0].colName, "id");
            UNIT_ASSERT_VALUES_EQUAL(result.tableStats[0].colType, "string");
            UNIT_ASSERT_VALUES_EQUAL(result.tableStats[0].statsData.stringStats.avgColLen, 30);
        }

        {
            Apache::Hadoop::Hive::StorageDescriptor descriptor;
            descriptor.__set_location("s3a://datalake/year=1970/month=1/day=1");

            Apache::Hadoop::Hive::Partition partition;
            partition.__set_dbName("default");
            partition.__set_tableName("my_table");
            partition.__set_sd(descriptor);
            partition.__set_values({"1970", "1", "1"});

            auto future = client.AddPartition(partition);
            UNIT_ASSERT_NO_EXCEPTION(future.GetValue(TDuration::Seconds(10)));
        }

        {
            auto partitions = client.GetPartitionsByFilter("default", "my_table", "").GetValue(TDuration::Seconds(10));
            UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].dbName, "default");
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].tableName, "my_table");
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].values.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].values[0], "1970");
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].values[1], "1");
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].values[2], "1");
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].sd.location, "s3a://datalake/year=1970/month=1/day=1");
        }

        {
            auto configValue = client.GetConfigValue("hive.metastore.port").GetValue(TDuration::Seconds(10));
            UNIT_ASSERT_VALUES_EQUAL(configValue, "9083");
        }
    }
}
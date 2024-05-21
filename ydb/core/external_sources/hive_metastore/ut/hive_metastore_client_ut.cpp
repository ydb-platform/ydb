#include <ydb/core/external_sources/hive_metastore/hive_metastore_client.h>
#include <ydb/core/external_sources/hive_metastore/ut/common.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/common/env.h>
#include <util/string/split.h>
#include <util/string/strip.h>

namespace NKikimr::NExternalSource {

Y_UNIT_TEST_SUITE(HiveMetastoreClient) {
    Y_UNIT_TEST(SuccessRequest) {
        const TString host = "0.0.0.0";
        const int32_t port = stoi(GetExternalPort("hive-metastore", "9083"));
        WaitHiveMetastore(host, port, "default");

        NKikimr::NExternalSource::THiveMetastoreClient client("0.0.0.0", port);
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

    Y_UNIT_TEST(SuccessTrinoRequest) {
        const TString host = "0.0.0.0";
        const int32_t port = stoi(GetExternalPort("hive-metastore", "9083"));
        WaitHiveMetastore(host, port, "final");

        NKikimr::NExternalSource::THiveMetastoreClient client("0.0.0.0", port);

        // iceberg
        {
            auto tables = client.GetAllTables("iceberg").GetValue(TDuration::Seconds(10));
            UNIT_ASSERT_VALUES_EQUAL(tables.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(tables[0], "request_logs");

            auto table = client.GetTable("iceberg", "request_logs").GetValue(TDuration::Seconds(10));
            UNIT_ASSERT_VALUES_EQUAL(table.tableName, "request_logs");
            UNIT_ASSERT_VALUES_EQUAL(table.dbName, "iceberg");
            UNIT_ASSERT_VALUES_EQUAL(table.owner, "trino");
            UNIT_ASSERT_VALUES_UNEQUAL(table.createTime, 0);
            UNIT_ASSERT_VALUES_EQUAL(table.lastAccessTime, 0);
            UNIT_ASSERT_VALUES_EQUAL(table.retention, 0);
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols.size(), 7);
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[0].name, "request_time");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[0].type, "timestamp");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[1].name, "url");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[1].type, "string");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[2].name, "ip");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[2].type, "string");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[3].name, "user_agent");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[3].type, "string");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[4].name, "year");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[4].type, "string");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[5].name, "month");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[5].type, "string");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[6].name, "day");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[6].type, "string");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.location, "s3://datalake/data/logs");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.inputFormat, "org.apache.hadoop.mapred.FileInputFormat");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.outputFormat, "org.apache.hadoop.mapred.FileOutputFormat");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.compressed, 0);
            UNIT_ASSERT_VALUES_EQUAL(table.sd.numBuckets, 0);
            UNIT_ASSERT_VALUES_EQUAL(table.sd.serdeInfo.name, "request_logs");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.serdeInfo.serializationLib, "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.serdeInfo.parameters.size(), 0);
            UNIT_ASSERT(!table.sd.serdeInfo.__isset.description);
            UNIT_ASSERT(!table.sd.serdeInfo.__isset.serializerClass);
            UNIT_ASSERT(!table.sd.serdeInfo.__isset.deserializerClass);
            UNIT_ASSERT(!table.sd.serdeInfo.__isset.serdeType);
            UNIT_ASSERT_VALUES_EQUAL(table.sd.bucketCols.size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(table.sd.sortCols.size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(table.sd.parameters.size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(table.sd.skewedInfo.skewedColNames.size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(table.sd.skewedInfo.skewedColValues.size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(table.sd.skewedInfo.skewedColValueLocationMaps.size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(table.sd.storedAsSubDirectories, 0);
            UNIT_ASSERT_VALUES_EQUAL(table.partitionKeys.size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(table.parameters.size(), 7);
            UNIT_ASSERT(table.parameters.contains("EXTERNAL"));
            UNIT_ASSERT(table.parameters.contains("metadata_location"));
            UNIT_ASSERT(table.parameters.contains("numFiles"));
            UNIT_ASSERT(table.parameters.contains("previous_metadata_location"));
            UNIT_ASSERT(table.parameters.contains("table_type"));
            UNIT_ASSERT(table.parameters.contains("totalSize"));
            UNIT_ASSERT(table.parameters.contains("transient_lastDdlTime"));
            UNIT_ASSERT_VALUES_EQUAL(table.parameters["EXTERNAL"], "TRUE");
            UNIT_ASSERT_STRING_CONTAINS(table.parameters["metadata_location"], "s3://datalake/data/logs/metadata/");
            UNIT_ASSERT_VALUES_EQUAL(table.parameters["numFiles"], "2");
            UNIT_ASSERT_STRING_CONTAINS(table.parameters["previous_metadata_location"], "s3://datalake/data/logs/metadata/");
            UNIT_ASSERT_VALUES_EQUAL(table.parameters["table_type"], "ICEBERG");
            UNIT_ASSERT_C(table.parameters["totalSize"] == "6760" || table.parameters["totalSize"] == "6754", table.parameters["totalSize"]);
            UNIT_ASSERT_VALUES_UNEQUAL(table.parameters["transient_lastDdlTime"], TString());
            UNIT_ASSERT_VALUES_EQUAL(table.viewOriginalText, TString());
            UNIT_ASSERT_VALUES_EQUAL(table.viewExpandedText, TString());
            UNIT_ASSERT_VALUES_EQUAL(table.tableType, "EXTERNAL_TABLE");
            UNIT_ASSERT(!table.__isset.privileges);
            UNIT_ASSERT_VALUES_EQUAL(table.temporary, 0);
            UNIT_ASSERT_VALUES_EQUAL(table.rewriteEnabled, 0);
            UNIT_ASSERT(!table.__isset.creationMetadata);
            UNIT_ASSERT_VALUES_EQUAL(table.catName, "hive");
            UNIT_ASSERT_EQUAL(table.ownerType, Apache::Hadoop::Hive::PrincipalType::USER);
            UNIT_ASSERT_VALUES_EQUAL(table.writeId, -1);
            UNIT_ASSERT(!table.__isset.isStatsCompliant);
            UNIT_ASSERT(!table.__isset.colStats);
            UNIT_ASSERT(!table.__isset.accessType);
            UNIT_ASSERT(!table.__isset.requiredReadCapabilities);
            UNIT_ASSERT(!table.__isset.requiredWriteCapabilities);
            UNIT_ASSERT(!table.__isset.id);
            UNIT_ASSERT(!table.__isset.fileMetadata);
            UNIT_ASSERT(!table.__isset.dictionary);
            UNIT_ASSERT(!table.__isset.txnId);

            auto partitions = client.GetPartitionsByFilter("iceberg", "request_logs", "").GetValue(TDuration::Seconds(10));
            UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 0);

            Apache::Hadoop::Hive::TableStatsRequest request;
            request.__set_dbName("iceberg");
            request.__set_tblName("request_logs");
            request.__set_colNames({"request_time", "url", "ip", "user_agent"});
            auto statisctics = client.GetTableStatistics(request).GetValue(TDuration::Seconds(10));
            UNIT_ASSERT_VALUES_EQUAL(statisctics.tableStats.size(), 0);
            UNIT_ASSERT(!statisctics.__isset.isStatsCompliant);
        }

        // hive
        {
            auto tables = client.GetAllTables("hive").GetValue(TDuration::Seconds(10));
            UNIT_ASSERT_VALUES_EQUAL(tables.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(tables[0], "request_logs");

            auto table = client.GetTable("hive", "request_logs").GetValue(TDuration::Seconds(10));
            UNIT_ASSERT_VALUES_EQUAL(table.tableName, "request_logs");
            UNIT_ASSERT_VALUES_EQUAL(table.dbName, "hive");
            UNIT_ASSERT_VALUES_EQUAL(table.owner, "trino");
            UNIT_ASSERT_VALUES_UNEQUAL(table.createTime, 0);
            UNIT_ASSERT_VALUES_EQUAL(table.lastAccessTime, 0);
            UNIT_ASSERT_VALUES_EQUAL(table.retention, 0);
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[0].name, "request_time");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[0].type, "timestamp");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[1].name, "url");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[1].type, "string");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[2].name, "ip");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[2].type, "string");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[3].name, "user_agent");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.cols[3].type, "string");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.location, "s3://datalake/data/logs/hive/request_logs");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.inputFormat, "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.outputFormat, "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.compressed, 0);
            UNIT_ASSERT_VALUES_EQUAL(table.sd.numBuckets, 0);
            UNIT_ASSERT_VALUES_EQUAL(table.sd.serdeInfo.name, "request_logs");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.serdeInfo.serializationLib, "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
            UNIT_ASSERT_VALUES_EQUAL(table.sd.serdeInfo.parameters.size(), 0);
            UNIT_ASSERT(!table.sd.serdeInfo.__isset.description);
            UNIT_ASSERT(!table.sd.serdeInfo.__isset.serializerClass);
            UNIT_ASSERT(!table.sd.serdeInfo.__isset.deserializerClass);
            UNIT_ASSERT(!table.sd.serdeInfo.__isset.serdeType);
            UNIT_ASSERT_VALUES_EQUAL(table.sd.bucketCols.size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(table.sd.sortCols.size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(table.sd.parameters.size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(table.sd.skewedInfo.skewedColNames.size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(table.sd.skewedInfo.skewedColValues.size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(table.sd.skewedInfo.skewedColValueLocationMaps.size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(table.sd.storedAsSubDirectories, 0);
            UNIT_ASSERT_VALUES_EQUAL(table.partitionKeys.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(table.partitionKeys[0].name, "year");
            UNIT_ASSERT_VALUES_EQUAL(table.partitionKeys[0].type, "string");
            UNIT_ASSERT_VALUES_EQUAL(table.partitionKeys[1].name, "month");
            UNIT_ASSERT_VALUES_EQUAL(table.partitionKeys[1].type, "string");
            UNIT_ASSERT_VALUES_EQUAL(table.partitionKeys[2].name, "day");
            UNIT_ASSERT_VALUES_EQUAL(table.partitionKeys[2].type, "string");
            UNIT_ASSERT_VALUES_EQUAL(table.parameters.size(), 5);
            UNIT_ASSERT(table.parameters.contains("STATS_GENERATED_VIA_STATS_TASK"));
            UNIT_ASSERT(table.parameters.contains("auto.purge"));
            UNIT_ASSERT(table.parameters.contains("trino_query_id"));
            UNIT_ASSERT(table.parameters.contains("trino_version"));
            UNIT_ASSERT(table.parameters.contains("transient_lastDdlTime"));
            UNIT_ASSERT_VALUES_EQUAL(table.parameters["STATS_GENERATED_VIA_STATS_TASK"], "workaround for potential lack of HIVE-12730");
            UNIT_ASSERT_STRING_CONTAINS(table.parameters["auto.purge"], "false");
            UNIT_ASSERT_VALUES_UNEQUAL(table.parameters["trino_query_id"], TString());
            UNIT_ASSERT_STRING_CONTAINS(table.parameters["trino_version"], "447");
            UNIT_ASSERT_VALUES_UNEQUAL(table.parameters["transient_lastDdlTime"], TString());
            UNIT_ASSERT_VALUES_EQUAL(table.viewOriginalText, TString());
            UNIT_ASSERT_VALUES_EQUAL(table.viewExpandedText, TString());
            UNIT_ASSERT_VALUES_EQUAL(table.tableType, "MANAGED_TABLE");
            UNIT_ASSERT(!table.__isset.privileges);
            UNIT_ASSERT_VALUES_EQUAL(table.temporary, 0);
            UNIT_ASSERT_VALUES_EQUAL(table.rewriteEnabled, 0);
            UNIT_ASSERT(!table.__isset.creationMetadata);
            UNIT_ASSERT_VALUES_EQUAL(table.catName, "hive");
            UNIT_ASSERT_EQUAL(table.ownerType, Apache::Hadoop::Hive::PrincipalType::USER);
            UNIT_ASSERT_VALUES_EQUAL(table.writeId, -1);
            UNIT_ASSERT(!table.__isset.isStatsCompliant);
            UNIT_ASSERT(!table.__isset.colStats);
            UNIT_ASSERT(!table.__isset.accessType);
            UNIT_ASSERT(!table.__isset.requiredReadCapabilities);
            UNIT_ASSERT(!table.__isset.requiredWriteCapabilities);
            UNIT_ASSERT(!table.__isset.id);
            UNIT_ASSERT(!table.__isset.fileMetadata);
            UNIT_ASSERT(!table.__isset.dictionary);
            UNIT_ASSERT(!table.__isset.txnId);

            auto partitions = client.GetPartitionsByFilter("hive", "request_logs", "").GetValue(TDuration::Seconds(10));
            UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].values.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].values[0], "2024");
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].values[1], "05");
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].values[2], "01");
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].dbName, "hive");
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].tableName, "request_logs");
            UNIT_ASSERT_VALUES_UNEQUAL(partitions[0].createTime, 0);
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].lastAccessTime, 0);
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].sd.cols.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].sd.cols[0].name, "request_time");
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].sd.cols[0].type, "timestamp");
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].sd.cols[1].name, "url");
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].sd.cols[1].type, "string");
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].sd.cols[2].name, "ip");
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].sd.cols[2].type, "string");
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].sd.cols[3].name, "user_agent");
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].sd.cols[3].type, "string");
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].sd.location, "s3://datalake/data/logs/hive/request_logs/year=2024/month=05/day=01");
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].sd.inputFormat, "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].sd.outputFormat, "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat");
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].sd.compressed, 0);
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].sd.numBuckets, 0);
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].sd.serdeInfo.name, "request_logs");
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].sd.serdeInfo.serializationLib, "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].sd.serdeInfo.parameters.size(), 0);
            UNIT_ASSERT(!partitions[0].sd.serdeInfo.__isset.description);
            UNIT_ASSERT(!partitions[0].sd.serdeInfo.__isset.serializerClass);
            UNIT_ASSERT(!partitions[0].sd.serdeInfo.__isset.deserializerClass);
            UNIT_ASSERT(!partitions[0].sd.serdeInfo.__isset.serdeType);
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].sd.bucketCols.size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].sd.sortCols.size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].sd.parameters.size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].sd.skewedInfo.skewedColNames.size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].sd.skewedInfo.skewedColValues.size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].sd.skewedInfo.skewedColValueLocationMaps.size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].sd.storedAsSubDirectories, 0);
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].parameters.size(), 8);
            UNIT_ASSERT(partitions[0].parameters.contains("COLUMN_STATS_ACCURATE"));
            UNIT_ASSERT(partitions[0].parameters.contains("STATS_GENERATED_VIA_STATS_TASK"));
            UNIT_ASSERT(partitions[0].parameters.contains("numFiles"));
            UNIT_ASSERT(partitions[0].parameters.contains("numRows"));
            UNIT_ASSERT(partitions[0].parameters.contains("totalSize"));
            UNIT_ASSERT(partitions[0].parameters.contains("transient_lastDdlTime"));
            UNIT_ASSERT(partitions[0].parameters.contains("trino_query_id"));
            UNIT_ASSERT(partitions[0].parameters.contains("trino_version"));    
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].parameters["COLUMN_STATS_ACCURATE"], R"({"COLUMN_STATS":{"ip":"true","request_time":"true","url":"true","user_agent":"true"}})");
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].parameters["STATS_GENERATED_VIA_STATS_TASK"], "workaround for potential lack of HIVE-12730");
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].parameters["numFiles"], "3");
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].parameters["numRows"], "3");
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].parameters["totalSize"], "2367");
            UNIT_ASSERT_VALUES_UNEQUAL(partitions[0].parameters["transient_lastDdlTime"], TString());
            UNIT_ASSERT_VALUES_UNEQUAL(partitions[0].parameters["trino_query_id"], TString());
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].parameters["trino_version"], "447");
            UNIT_ASSERT(!partitions[0].__isset.privileges);
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].catName, "hive");
            UNIT_ASSERT_VALUES_EQUAL(partitions[0].writeId, -1);
            UNIT_ASSERT(!partitions[0].__isset.isStatsCompliant);
            UNIT_ASSERT(!partitions[0].__isset.colStats);
            UNIT_ASSERT(!partitions[0].__isset.fileMetadata);

            Apache::Hadoop::Hive::TableStatsRequest request;
            request.__set_dbName("hive");
            request.__set_tblName("request_logs");
            request.__set_colNames({"request_time", "url", "ip", "user_agent"});

            auto statisctics = client.GetTableStatistics(request).GetValue(TDuration::Seconds(10));
            UNIT_ASSERT_VALUES_EQUAL(statisctics.tableStats.size(), 0);
            UNIT_ASSERT(!statisctics.__isset.isStatsCompliant);
        }
    }
}

}

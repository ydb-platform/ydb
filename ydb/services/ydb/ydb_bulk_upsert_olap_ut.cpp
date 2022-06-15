#include "ydb_common_ut.h"

#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/draft/ydb_long_tx.h>

#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

using namespace NYdb;

namespace {

ui32 ScanQuerySelect(NYdb::NTable::TTableClient client, const TString& tablePath,
                     const std::vector<std::pair<TString, NYdb::EPrimitiveType>>& ydbSchema = TTestOlap::PublicSchema()) {
    auto query = Sprintf("SELECT * FROM `%s`", tablePath.c_str());

    // Executes scan query
    auto result = client.StreamExecuteScanQuery(query).GetValueSync();
    if (!result.IsSuccess()) {
        Cerr << "ScanQuery execution failure: " << result.GetIssues().ToString() << Endl;
        return 0;
    }

    ui32 numRows = 0;
    bool eos = false;
    Cout << "ScanQuery:" << Endl;
    while (!eos) {
        auto streamPart = result.ReadNext().ExtractValueSync();
        if (!streamPart.IsSuccess()) {
            eos = true;
            if (!streamPart.EOS()) {
                Cerr << "ScanQuery execution failure: " << streamPart.GetIssues().ToString() << Endl;
                return 0;
            }
            continue;
        }

        if (streamPart.HasResultSet()) {
            auto rs = streamPart.ExtractResultSet();
            auto columns = rs.GetColumnsMeta();

            TResultSetParser parser(rs);
            while (parser.TryNextRow()) {
                for (auto& [colName, colType] : ydbSchema) {
                    switch (colType) {
                        case NYdb::EPrimitiveType::Timestamp:
                            Cout << parser.ColumnParser(colName).GetOptionalTimestamp() << ", ";
                            break;
                        case NYdb::EPrimitiveType::Utf8:
                            Cout << parser.ColumnParser(colName).GetOptionalUtf8() << ", ";
                            break;
                        case NYdb::EPrimitiveType::Int32:
                            Cout << parser.ColumnParser(colName).GetOptionalInt32() << ", ";
                            break;
                        case NYdb::EPrimitiveType::JsonDocument:
                            Cout << parser.ColumnParser(colName).GetOptionalJsonDocument() << ", ";
                            break;
                        default:
                            Cout << "<other>, ";
                            break;
                    }
                }
                Cout << Endl;
                ++numRows;
            }
        }
    }
    return numRows;
}

}

Y_UNIT_TEST_SUITE(YdbTableBulkUpsertOlap) {

    Y_UNIT_TEST(UpsertArrowBatch) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetEnableOlapSchemaOperations(true);
        TKikimrWithGrpcAndRootSchema server(appConfig);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);

        TTestOlap::CreateTable(*server.ServerSettings);

        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));

        NYdb::NTable::TTableClient client(connection);
        auto session = client.GetSession().ExtractValueSync().GetSession();
        TString tablePath = TTestOlap::TablePath;

        auto srcBatch = TTestOlap::SampleBatch();
        auto schema = srcBatch->schema();
        TString strSchema = NArrow::SerializeSchema(*schema);
        TString strBatch = NArrow::SerializeBatchNoCompression(srcBatch);

        TInstant start = TInstant::Now();
        {
            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::ApacheArrow, strBatch, strSchema).GetValueSync();

            Cerr << res.GetStatus() << Endl;
            UNIT_ASSERT_EQUAL_C(res.GetStatus(), EStatus::SUCCESS, res.GetIssues().ToString());
        }
        Cerr << "Upsert done: " << TInstant::Now() - start << Endl;

        { // Read all
            ui32 numRows = ScanQuerySelect(client, tablePath);
            UNIT_ASSERT_GT(numRows, 0);
        }

        // Negatives

        std::vector<TString> columns;
        for (auto& [name, type] : TTestOlap::PublicSchema()) {
            columns.push_back(name);
        }

        { // Wrong fromat
            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::CSV, strBatch, strSchema).GetValueSync();

            Cerr << "Negative (wrong format): " << res.GetStatus() << Endl;
            UNIT_ASSERT(res.GetStatus() != EStatus::SUCCESS);
        }

        { // Wrong data
            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::ApacheArrow, "").GetValueSync();

            Cerr << "Negative (wrong data): " << res.GetStatus() << Endl;
            UNIT_ASSERT(res.GetStatus() != EStatus::SUCCESS);
        }

        { // Less columns
            std::vector<TString> wrongColumns = columns;
            wrongColumns.resize(columns.size() - 1);
            auto wrongBatch = NArrow::ExtractColumns(srcBatch, wrongColumns);
            strBatch = NArrow::SerializeBatchNoCompression(wrongBatch);

            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::ApacheArrow, strBatch, strSchema).GetValueSync();

            Cerr << "Negative (less columns): " << res.GetStatus() << Endl;
            UNIT_ASSERT(res.GetStatus() != EStatus::SUCCESS);
        }

        { // Reordered columns (it leads to wrong types)
            std::vector<TString> wrongColumns = columns;
            std::sort(wrongColumns.begin(), wrongColumns.end());
            auto wrongBatch = NArrow::ExtractColumns(srcBatch, wrongColumns);
            strBatch = NArrow::SerializeBatchNoCompression(wrongBatch);

            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::ApacheArrow, strBatch, strSchema).GetValueSync();

            Cerr << "Negative (reordered columns): " << res.GetStatus() << Endl;
            UNIT_ASSERT(res.GetStatus() != EStatus::SUCCESS);
        }
    }

    Y_UNIT_TEST(UpsertCSV) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetEnableOlapSchemaOperations(true);
        TKikimrWithGrpcAndRootSchema server(appConfig);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);

        TTestOlap::CreateTable(*server.ServerSettings); // 2 shards

        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));

        NYdb::NTable::TTableClient client(connection);
        auto session = client.GetSession().ExtractValueSync().GetSession();
        TString tablePath = TTestOlap::TablePath;

        auto schema = TTestOlap::ArrowSchema();
        auto sampleBatch = TTestOlap::SampleBatch(true);
        TString csv = TTestOlap::ToCSV(sampleBatch);

        // send it with CSV fromat and client/server parsing flag

        TInstant start = TInstant::Now();
        {
            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::CSV, csv).GetValueSync();

            Cerr << res.GetStatus() << Endl;
            UNIT_ASSERT_EQUAL_C(res.GetStatus(), EStatus::SUCCESS, res.GetIssues().ToString());
        }
        Cerr << "Upsert done: " << TInstant::Now() - start << Endl;

        { // Read all
            ui32 numRows = ScanQuerySelect(client, tablePath);
            UNIT_ASSERT_GT(numRows, 0);
        }

        // Negatives

        std::vector<TString> columns;
        for (auto& [name, type] : TTestOlap::PublicSchema()) {
            columns.push_back(name);
        }

        { // Wrong format
            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::ApacheArrow, csv).GetValueSync();

            Cerr << "Negative (wrong format): " << res.GetStatus() << Endl;
            UNIT_ASSERT(res.GetStatus() != EStatus::SUCCESS);
        }

        { // Wrong data
            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::CSV, "abc").GetValueSync();

            Cerr << "Negative (wrong data): " << res.GetStatus() << Endl;
            UNIT_ASSERT(res.GetStatus() != EStatus::SUCCESS);
        }

        { // Less columns
            std::vector<TString> wrongColumns = columns;
            wrongColumns.resize(columns.size() - 1);
            csv = TTestOlap::ToCSV(NArrow::ExtractColumns(sampleBatch, wrongColumns));

            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::CSV, csv).GetValueSync();

            Cerr << "Negative (less columns): " << res.GetStatus() << Endl;
            UNIT_ASSERT(res.GetStatus() != EStatus::SUCCESS);
        }

        { // Reordered columns (it leads to wrong types)
            std::vector<TString> wrongColumns = columns;
            std::sort(wrongColumns.begin(), wrongColumns.end());
            csv = TTestOlap::ToCSV(NArrow::ExtractColumns(sampleBatch, wrongColumns));

            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::CSV, csv).GetValueSync();

            Cerr << "Negative (reordered columns): " << res.GetStatus() << Endl;
            UNIT_ASSERT(res.GetStatus() != EStatus::SUCCESS);
        }

        { // Reordered columns with header
            std::vector<TString> wrongColumns = columns;
            std::sort(wrongColumns.begin(), wrongColumns.end());
            csv = TTestOlap::ToCSV(NArrow::ExtractColumns(sampleBatch, wrongColumns), true);

            NYdb::NTable::TBulkUpsertSettings upsertSettings;
            {
                Ydb::Formats::CsvSettings csvSettings;
                csvSettings.set_header(true);

                TString formatSettings;
                Y_PROTOBUF_SUPPRESS_NODISCARD csvSettings.SerializeToString(&formatSettings);
                upsertSettings.FormatSettings(formatSettings);
            }

            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::CSV, csv, {}, upsertSettings).GetValueSync();

            Cerr << "Reordered columns (with header): " << res.GetStatus() << Endl;
            UNIT_ASSERT(res.GetStatus() == EStatus::SUCCESS);
        }

        { // Big CSV batch
            auto bigBatch = TTestOlap::SampleBatch(true, 130000);
            ui32 batchSize = NArrow::GetBatchDataSize(bigBatch);
            Cerr << "rows: " << bigBatch->num_rows() << " batch size: " << batchSize << Endl;
            UNIT_ASSERT(batchSize > 15 * 1024 * 1024);
            UNIT_ASSERT(batchSize < 17 * 1024 * 1024);

            TString bigCsv = TTestOlap::ToCSV(bigBatch);

            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::CSV, bigCsv).GetValueSync();

            Cerr << "Big batch: " << res.GetStatus() << Endl;
            UNIT_ASSERT(res.GetStatus() == EStatus::SUCCESS);
        }

        { // Too big CSV batch
            auto bigBatch = TTestOlap::SampleBatch(true, 150000); // 2 shards, greater then 8 Mb per shard
            ui32 batchSize = NArrow::GetBatchDataSize(bigBatch);
            Cerr << "rows: " << bigBatch->num_rows() << " batch size: " << batchSize << Endl;
            UNIT_ASSERT(batchSize > 16 * 1024 * 1024);
            UNIT_ASSERT(batchSize < 20 * 1024 * 1024);

            TString bigCsv = TTestOlap::ToCSV(bigBatch);

            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::CSV, bigCsv).GetValueSync();

            Cerr << "Negative (big batch): " << res.GetStatus() << Endl;
            UNIT_ASSERT(res.GetStatus() != EStatus::SUCCESS);
        }
    }

    Y_UNIT_TEST(UpsertArrowBatch_DataShard) {
        TKikimrWithGrpcAndRootSchema server;
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);

        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));

        NYdb::NTable::TTableClient client(connection);
        auto session = client.GetSession().ExtractValueSync().GetSession();
        TString tablePath = "/Root/LogsX";

        { // CREATE TABLE /Root/Logs (timestamp Timestamp, ... PK timestamp)
            auto tableBuilder = client.GetTableBuilder();
            for (auto& [name, type] : TTestOlap::PublicSchema()) {
                tableBuilder.AddNullableColumn(name, type);
            }
            tableBuilder.SetPrimaryKeyColumns({"timestamp"});
            NYdb::NTable::TCreateTableSettings tableSettings;
            //tableSettings.PartitioningPolicy(NYdb::NTable::TPartitioningPolicy().UniformPartitions(2));
            auto result = session.CreateTable(tablePath, tableBuilder.Build(), tableSettings).ExtractValueSync();

            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        auto srcBatch = TTestOlap::SampleBatch();
        auto schema = srcBatch->schema();
        TString strSchema = NArrow::SerializeSchema(*schema);
        TString strBatch = NArrow::SerializeBatchNoCompression(srcBatch);
        ui32 numRows = srcBatch->num_rows();

        TInstant start = TInstant::Now();
        {
            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::ApacheArrow, strBatch, strSchema).GetValueSync();

            Cerr << res.GetStatus() << Endl;
            UNIT_ASSERT_EQUAL_C(res.GetStatus(), EStatus::SUCCESS, res.GetIssues().ToString());
        }
        Cerr << "Upsert done: " << TInstant::Now() - start << Endl;

        { // Read all
            ui32 numRows = ScanQuerySelect(client, tablePath);
            UNIT_ASSERT_GT(numRows, 0);
        }

        // Read
        auto res = session.ExecuteDataQuery("SELECT count(*) AS _cnt FROM [/Root/LogsX];",
                                            NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        Cerr << res.GetStatus() << Endl;
        UNIT_ASSERT_EQUAL(res.GetStatus(), EStatus::SUCCESS);

        auto rs = NYdb::TResultSetParser(res.GetResultSet(0));
        UNIT_ASSERT(rs.TryNextRow());
        ui64 count = rs.ColumnParser("_cnt").GetUint64();
        Cerr << "count returned " << count << " rows" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(count, numRows);

        // Negatives

        std::vector<TString> columns;
        for (auto& [name, type] : TTestOlap::PublicSchema()) {
            columns.push_back(name);
        }

        { // Wrong format
            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::CSV, strBatch, strSchema).GetValueSync();

            Cerr << "Negative (wrong format): " << res.GetStatus() << Endl;
            UNIT_ASSERT(res.GetStatus() != EStatus::SUCCESS);
        }

        { // Wrong data
            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::ApacheArrow, "").GetValueSync();

            Cerr << "Negative (wrong data): " << res.GetStatus() << Endl;
            UNIT_ASSERT(res.GetStatus() != EStatus::SUCCESS);
        }

        { // Less columns
            std::vector<TString> wrongColumns = columns;
            wrongColumns.resize(columns.size() - 1);
            auto wrongBatch = NArrow::ExtractColumns(srcBatch, wrongColumns);
            strBatch = NArrow::SerializeBatchNoCompression(wrongBatch);

            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::ApacheArrow, strBatch, strSchema).GetValueSync();

            Cerr << "Negative (less columns): " << res.GetStatus() << Endl;
            UNIT_ASSERT(res.GetStatus() != EStatus::SUCCESS);
        }

        { // Reordered columns (it leads to wrong types)
            std::vector<TString> wrongColumns = columns;
            std::sort(wrongColumns.begin(), wrongColumns.end());
            auto wrongBatch = NArrow::ExtractColumns(srcBatch, wrongColumns);
            strBatch = NArrow::SerializeBatchNoCompression(wrongBatch);

            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::ApacheArrow, strBatch, strSchema).GetValueSync();

            Cerr << "Negative (reordered columns): " << res.GetStatus() << Endl;
            UNIT_ASSERT(res.GetStatus() != EStatus::SUCCESS);
        }
    }

    Y_UNIT_TEST(UpsertCSV_DataShard) {
        TKikimrWithGrpcAndRootSchema server;
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);

        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));

        NYdb::NTable::TTableClient client(connection);
        auto session = client.GetSession().ExtractValueSync().GetSession();
        TString tablePath = "/Root/LogsX";

        { // CREATE TABLE /Root/Logs (timestamp Timestamp, ... PK timestamp)
            auto tableBuilder = client.GetTableBuilder();
            for (auto& [name, type] : TTestOlap::PublicSchema()) {
                tableBuilder.AddNullableColumn(name, type);
            }
            tableBuilder.SetPrimaryKeyColumns({"timestamp"});
            NYdb::NTable::TCreateTableSettings tableSettings;
            //tableSettings.PartitioningPolicy(NYdb::NTable::TPartitioningPolicy().UniformPartitions(2));
            auto result = session.CreateTable(tablePath, tableBuilder.Build(), tableSettings).ExtractValueSync();

            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        ui32 numRows = 100;
        auto schema = TTestOlap::ArrowSchema();
        auto sampleBatch = TTestOlap::SampleBatch(true, numRows);
        TString csv = TTestOlap::ToCSV(sampleBatch);

        TInstant start = TInstant::Now();
        {
            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::CSV, csv).GetValueSync();

            Cerr << res.GetStatus() << Endl;
            UNIT_ASSERT_EQUAL_C(res.GetStatus(), EStatus::SUCCESS, res.GetIssues().ToString());
        }
        Cerr << "Upsert done: " << TInstant::Now() - start << Endl;

        // Read
        auto res = session.ExecuteDataQuery("SELECT count(*) AS _cnt FROM [/Root/LogsX];",
                                            NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        Cerr << res.GetStatus() << Endl;
        UNIT_ASSERT_EQUAL(res.GetStatus(), EStatus::SUCCESS);

        auto rs = NYdb::TResultSetParser(res.GetResultSet(0));
        UNIT_ASSERT(rs.TryNextRow());
        ui64 count = rs.ColumnParser("_cnt").GetUint64();
        Cerr << "count returned " << count << " rows" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(count, numRows);

        // Negatives

        std::vector<TString> columns;
        for (auto& [name, type] : TTestOlap::PublicSchema()) {
            columns.push_back(name);
        }

        { // Wrong format
            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::ApacheArrow, csv).GetValueSync();

            Cerr << "Negative (format is not CSV): " << res.GetStatus() << Endl;
            UNIT_ASSERT(res.GetStatus() != EStatus::SUCCESS);
        }

        { // Wrong data
            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::CSV, "abc").GetValueSync();

            Cerr << "Negative (format is not CSV): " << res.GetStatus() << Endl;
            UNIT_ASSERT(res.GetStatus() != EStatus::SUCCESS);
        }

        { // Less columns
            std::vector<TString> wrongColumns = columns;
            wrongColumns.resize(columns.size() - 1);
            csv = TTestOlap::ToCSV(NArrow::ExtractColumns(sampleBatch, wrongColumns));

            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::CSV, csv).GetValueSync();

            Cerr << "Negative (less columns): " << res.GetStatus() << Endl;
            UNIT_ASSERT(res.GetStatus() != EStatus::SUCCESS);
        }

        { // Reordered columns (it leads to wrong types)
            std::vector<TString> wrongColumns = columns;
            std::sort(wrongColumns.begin(), wrongColumns.end());
            csv = TTestOlap::ToCSV(NArrow::ExtractColumns(sampleBatch, wrongColumns));

            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::CSV, csv).GetValueSync();

            Cerr << "Negative (reordered columns): " << res.GetStatus() << Endl;
            UNIT_ASSERT(res.GetStatus() != EStatus::SUCCESS);
        }

        { // Reordered columns with header
            std::vector<TString> wrongColumns = columns;
            std::sort(wrongColumns.begin(), wrongColumns.end());
            csv = TTestOlap::ToCSV(NArrow::ExtractColumns(sampleBatch, wrongColumns), true);

            NYdb::NTable::TBulkUpsertSettings upsertSettings;
            {
                Ydb::Formats::CsvSettings csvSettings;
                csvSettings.set_header(true);

                TString formatSettings;
                Y_PROTOBUF_SUPPRESS_NODISCARD csvSettings.SerializeToString(&formatSettings);
                upsertSettings.FormatSettings(formatSettings);
            }

            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::CSV, csv, {}, upsertSettings).GetValueSync();

            Cerr << "Reordered columns (with header): " << res.GetStatus() << Endl;
            UNIT_ASSERT(res.GetStatus() == EStatus::SUCCESS);
        }
    }
}

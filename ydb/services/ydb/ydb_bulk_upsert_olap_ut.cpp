#include "ydb_common_ut.h"

#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/size_calcer.h>

#include <ydb/public/api/protos/ydb_formats.pb.h>

using namespace NYdb;

namespace {

std::vector<TString> ScanQuerySelectSimple(
    NYdb::NTable::TTableClient client, const TString& tablePath,
    const std::vector<std::pair<TString, NYdb::EPrimitiveType>>& ydbSchema = TTestOlap::PublicSchema())
{
    auto query = Sprintf("SELECT * FROM `%s`", tablePath.c_str());

    // Executes scan query
    auto result = client.StreamExecuteScanQuery(query).GetValueSync();
    if (!result.IsSuccess()) {
        Cerr << "ScanQuery execution failure: " << result.GetIssues().ToString() << Endl;
        return {};
    }

    std::vector<TString> out;
    bool eos = false;
    Cout << "ScanQuery:" << Endl;
    while (!eos) {
        auto streamPart = result.ReadNext().ExtractValueSync();
        if (!streamPart.IsSuccess()) {
            eos = true;
            if (!streamPart.EOS()) {
                Cerr << "ScanQuery execution failure: " << streamPart.GetIssues().ToString() << Endl;
                return {};
            }
            continue;
        }

        if (streamPart.HasResultSet()) {
            auto rs = streamPart.ExtractResultSet();
            auto columns = rs.GetColumnsMeta();

            TResultSetParser parser(rs);
            while (parser.TryNextRow()) {
                TStringBuilder ss;

                for (auto& [colName, colType] : ydbSchema) {
                    switch (colType) {
                        case NYdb::EPrimitiveType::Timestamp:
                            if (colName == "timestamp") {
                                ss << parser.ColumnParser(colName).GetTimestamp() << ",";
                            } else {
                                ss << parser.ColumnParser(colName).GetOptionalTimestamp() << ",";
                            }
                            break;
                        case NYdb::EPrimitiveType::Datetime:
                            ss << parser.ColumnParser(colName).GetOptionalDatetime() << ",";
                            break;
                        case NYdb::EPrimitiveType::String: {
                            auto& col = parser.ColumnParser(colName);
                            if (col.GetKind() == TTypeParser::ETypeKind::Optional) {
                                ss << col.GetOptionalString() << ",";
                            } else {
                                ss << col.GetString() << ",";
                            }
                            break;
                        }
                        case NYdb::EPrimitiveType::Utf8:
                            if (colName == "uid") {
                                ss << parser.ColumnParser(colName).GetUtf8() << ",";
                            } else {
                                ss << parser.ColumnParser(colName).GetOptionalUtf8() << ",";
                            }
                            break;
                        case NYdb::EPrimitiveType::Int32:
                            ss << parser.ColumnParser(colName).GetOptionalInt32() << ",";
                            break;
                        case NYdb::EPrimitiveType::JsonDocument:
                            ss << parser.ColumnParser(colName).GetOptionalJsonDocument() << ",";
                            break;
                        default:
                            ss << "<other>,";
                            break;
                    }
                }

                out.emplace_back(TString(ss));
                auto& str = out.back();
                if (str.size()) {
                    str.resize(str.size() - 1);
                }
                Cout << str << Endl;
            }
        }
    }
    return out;
}

std::vector<TString> ScanQuerySelect(
    NYdb::NTable::TTableClient client, const TString& tablePath,
    const std::vector<std::pair<TString, NYdb::EPrimitiveType>>& ydbSchema = TTestOlap::PublicSchema())
{
    for(ui32 iter = 0; iter < 3; iter++) {
        try {
            return ScanQuerySelectSimple(client, tablePath, ydbSchema);
        } catch(...) {
            /// o_O
        }
    }

    return ScanQuerySelectSimple(client, tablePath, ydbSchema);
}

}

Y_UNIT_TEST_SUITE(YdbTableBulkUpsertOlap) {

    Y_UNIT_TEST(UpsertArrowBatch) {
        NKikimrConfig::TAppConfig appConfig;
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
            auto rows = ScanQuerySelect(client, tablePath);
            UNIT_ASSERT_GT(rows.size(), 0);
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
            auto wrongBatch = NArrow::TColumnOperator().VerifyIfAbsent().Extract(srcBatch, wrongColumns);
            strBatch = NArrow::SerializeBatchNoCompression(wrongBatch);

            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::ApacheArrow, strBatch, strSchema).GetValueSync();

            Cerr << "Negative (less columns): " << res.GetStatus() << Endl;
            UNIT_ASSERT(res.GetStatus() != EStatus::SUCCESS);
        }

        { // Reordered columns (it leads to wrong types)
            std::vector<TString> wrongColumns = columns;
            std::sort(wrongColumns.begin(), wrongColumns.end());
            auto wrongBatch = NArrow::TColumnOperator().VerifyIfAbsent().Extract(srcBatch, wrongColumns);
            strBatch = NArrow::SerializeBatchNoCompression(wrongBatch);

            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::ApacheArrow, strBatch, strSchema).GetValueSync();

            Cerr << "Negative (reordered columns): " << res.GetStatus() << Endl;
            UNIT_ASSERT(res.GetStatus() != EStatus::SUCCESS);
        }
    }

    Y_UNIT_TEST(UpsertArrowDupField) {
        NKikimrConfig::TAppConfig appConfig;
        TKikimrWithGrpcAndRootSchema server(appConfig);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);

        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));

        NYdb::NTable::TTableClient client(connection);
        auto session = client.GetSession().ExtractValueSync().GetSession();
        TString tablePath = TTestOlap::TablePath;

        // KIKIMR-18866
        for (ui32 i = 0; i < 2; ++i) {
            // CREATE TABLE import1(a Text NOT NULL, b Text, c Text, PRIMARY KEY(a))
            std::vector<std::pair<TString, NYdb::EPrimitiveType>> ydbSchema = {
                { "a", NYdb::EPrimitiveType::Utf8 },
                { "b", NYdb::EPrimitiveType::Utf8 },
                { "c", NYdb::EPrimitiveType::Utf8 }
            };

            auto tableBuilder = client.GetTableBuilder();
            for (auto& [name, type] : ydbSchema) {
                if (name == "a") {
                    tableBuilder.AddNonNullableColumn(name, type);
                } else {
                    tableBuilder.AddNullableColumn(name, type);
                }
            }
            tableBuilder.SetPrimaryKeyColumns({"a"});
            auto result = session.CreateTable(tablePath, tableBuilder.Build(), {}).ExtractValueSync();

            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            std::vector<std::shared_ptr<arrow::Field>> fields;
            fields.push_back(std::make_shared<arrow::Field>("a", arrow::utf8()));
            fields.push_back(std::make_shared<arrow::Field>("b", arrow::utf8()));
            fields.push_back(std::make_shared<arrow::Field>("b", arrow::utf8())); // not unique column
            if (i) {
                fields.push_back(std::make_shared<arrow::Field>("c", arrow::utf8()));
            }

            auto schema = std::make_shared<arrow::Schema>(fields);
            std::unique_ptr<arrow::RecordBatchBuilder> builder;
            arrow::RecordBatchBuilder::Make(schema, arrow::default_memory_pool(), 1, &builder).ok();
            for (i64 i = 0; i < schema->num_fields(); ++i) {
                builder->GetFieldAs<arrow::StringBuilder>(i)->Append("test").ok();
            }
            std::shared_ptr<arrow::RecordBatch> batch;
            builder->Flush(false, &batch).ok();

            TString strSchema = NArrow::SerializeSchema(*schema);
            TString strBatch = NArrow::SerializeBatchNoCompression(batch);

            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::ApacheArrow, strBatch, strSchema).GetValueSync();

            Cerr << res.GetStatus() << Endl;
            UNIT_ASSERT_EQUAL_C(res.GetStatus(), EStatus::BAD_REQUEST, res.GetIssues().ToString());

            result = session.DropTable(tablePath).GetValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
    }

    void ParquetImportBug(bool columnTable) {
        NKikimrConfig::TAppConfig appConfig;
        TKikimrWithGrpcAndRootSchema server(appConfig);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);

        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));

        NYdb::NTable::TTableClient client(connection);
        auto session = client.GetSession().ExtractValueSync().GetSession();
        TString tablePath = TTestOlap::TablePath;

        std::vector<std::pair<TString, NYdb::EPrimitiveType>> schema = {
                { "id", NYdb::EPrimitiveType::Uint32 },
                { "timestamp", NYdb::EPrimitiveType::Timestamp },
                { "dateTimeS", NYdb::EPrimitiveType::Datetime },
                { "dateTimeU", NYdb::EPrimitiveType::Datetime },
                { "date", NYdb::EPrimitiveType::Date },
                { "utf8ToString", NYdb::EPrimitiveType::String },
                { "stringToString", NYdb::EPrimitiveType::String },
            };

        auto tableBuilder = client.GetTableBuilder();
        auto tableType = columnTable ? NYdb::NTable::EStoreType::Column : NYdb::NTable::EStoreType::Row;
        tableBuilder.SetStoreType(tableType);
        for (auto& [name, type] : schema) {
            if (name == "id" || name == "timestamp") {
                tableBuilder.AddNonNullableColumn(name, type);
            } else {
                tableBuilder.AddNullableColumn(name, type);
            }
        }
        tableBuilder.SetPrimaryKeyColumns({"id"});
        if (columnTable) {
            NYdb::NTable::TTablePartitioningSettingsBuilder partsBuilder(tableBuilder);
            partsBuilder.SetMinPartitionsCount(1);
            partsBuilder.EndPartitioningSettings();
        }
        auto result = session.CreateTable(tablePath, tableBuilder.Build(), {}).ExtractValueSync();

        UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        {
            auto descResult = session.DescribeTable(tablePath, {}).ExtractValueSync();
            UNIT_ASSERT_EQUAL(descResult.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(descResult.GetStatus(), EStatus::SUCCESS);

            Cerr << "Table type: " << (ui32) descResult.GetTableDescription().GetStoreType() << Endl;
            UNIT_ASSERT_EQUAL(descResult.GetTableDescription().GetStoreType(), tableType);
        }

        auto batchSchema = std::make_shared<arrow::Schema>(
            std::vector<std::shared_ptr<arrow::Field>>{
                arrow::field("id", arrow::uint32(), false),
                arrow::field("timestamp", arrow::int64(), false),
                arrow::field("dateTimeS", arrow::int32()),
                arrow::field("dateTimeU", arrow::uint32()),
                arrow::field("date", arrow::uint16()),
                arrow::field("utf8ToString", arrow::utf8()),
                arrow::field("stringToString", arrow::binary()),
            });

        size_t rowsCount = 100;
        auto builders = NArrow::MakeBuilders(batchSchema, rowsCount);

        for (size_t i = 0; i < rowsCount; ++i) {
            Y_ABORT_UNLESS(NArrow::Append<arrow::UInt32Type>(*builders[0], i));
            Y_ABORT_UNLESS(NArrow::Append<arrow::Int64Type>(*builders[1], i));
            Y_ABORT_UNLESS(NArrow::Append<arrow::Int32Type>(*builders[2], i));
            Y_ABORT_UNLESS(NArrow::Append<arrow::Int32Type>(*builders[3], i));
            Y_ABORT_UNLESS(NArrow::Append<arrow::UInt16Type>(*builders[4], i));
            Y_ABORT_UNLESS(NArrow::Append<arrow::StringType>(*builders[5], std::to_string(i)));
            Y_ABORT_UNLESS(NArrow::Append<arrow::BinaryType>(*builders[6], std::to_string(i)));
        }

        auto srcBatch = arrow::RecordBatch::Make(batchSchema, rowsCount, NArrow::Finish(std::move(builders)));
        TString strSchema = NArrow::SerializeSchema(*batchSchema);
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
            auto rows = ScanQuerySelect(client, tablePath, schema);
            UNIT_ASSERT_EQUAL(rows.size(), 100);
        }
    }

    Y_UNIT_TEST(ParquetImportBug) {
        ParquetImportBug(true);
    }

    Y_UNIT_TEST(ParquetImportBug_Datashard) {
        ParquetImportBug(false);
    }

    Y_UNIT_TEST(UpsertCsvBug) {
        NKikimrConfig::TAppConfig appConfig;
        TKikimrWithGrpcAndRootSchema server(appConfig);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);

        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));

        NYdb::NTable::TTableClient client(connection);
        auto session = client.GetSession().ExtractValueSync().GetSession();
        TString tablePath = TTestOlap::TablePath;

        { // KIKIMR-16411
//          CREATE TABLE subscriber (
//              id String NOT NULL,
//              email Utf8,
//              status String,
//              subscribed_at Datetime,
//              confirmed_at Datetime,
//              unsubscribed_at Datetime,
//              referrer Utf8,
//              language Utf8,
//              timezone Utf8,
//              ip_address String,
//              fields JsonDocument,
//              PRIMARY KEY (id)
//          );
            std::vector<std::pair<TString, NYdb::EPrimitiveType>> schema = {
                { "id", NYdb::EPrimitiveType::String },
                { "email", NYdb::EPrimitiveType::Utf8 },
                { "status", NYdb::EPrimitiveType::String },
                { "subscribed_at", NYdb::EPrimitiveType::Datetime },
                { "confirmed_at", NYdb::EPrimitiveType::Datetime },
                { "unsubscribed_at", NYdb::EPrimitiveType::Datetime },
                { "referrer", NYdb::EPrimitiveType::Utf8 },
                { "language", NYdb::EPrimitiveType::Utf8 },
                { "timezone", NYdb::EPrimitiveType::Utf8 },
                { "ip_address", NYdb::EPrimitiveType::String },
                { "fields", NYdb::EPrimitiveType::JsonDocument }
            };

            auto tableBuilder = client.GetTableBuilder();
            for (auto& [name, type] : schema) {
                if (name == "id") {
                    tableBuilder.AddNonNullableColumn(name, type);
                } else {
                    tableBuilder.AddNullableColumn(name, type);
                }
            }
            tableBuilder.SetPrimaryKeyColumns({"id"});
            auto result = session.CreateTable(tablePath, tableBuilder.Build(), {}).ExtractValueSync();

            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            TString csv =
            "id|email|status|subscribed_at|confirmed_at|unsubscribed_at|referrer|language|timezone|ip_address|fields\n"
            "123123bs|testd|subscr|1579301930|123213123||http|ru|AsiaNovo|hello|\"{}\"\n";

            Ydb::Formats::CsvSettings csvSettings;
            csvSettings.set_header(true);
            csvSettings.set_delimiter("|");

            TString formatSettings;
            Y_PROTOBUF_SUPPRESS_NODISCARD csvSettings.SerializeToString(&formatSettings);

            NYdb::NTable::TBulkUpsertSettings upsertSettings;
            upsertSettings.FormatSettings(formatSettings);

            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::CSV, csv, {}, upsertSettings).GetValueSync();

            Cerr << res.GetStatus() << Endl;
            UNIT_ASSERT_EQUAL_C(res.GetStatus(), EStatus::SUCCESS, res.GetIssues().ToString());

            auto rows = ScanQuerySelect(client, tablePath, schema);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(rows[0],
                "123123bs,testd,subscr,2020-01-17T22:58:50.000000Z,1973-11-27T01:52:03.000000Z,(empty maybe),http,ru,AsiaNovo,hello,{}");

            result = session.DropTable(tablePath).GetValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        // KIKIMR-18859
        {
            // CREATE TABLE import1(a Text NOT NULL, b Text, c Text, PRIMARY KEY(a))
            std::vector<std::pair<TString, NYdb::EPrimitiveType>> schema = {
                { "a", NYdb::EPrimitiveType::Utf8 },
                { "b", NYdb::EPrimitiveType::Utf8 },
                { "c", NYdb::EPrimitiveType::Utf8 }
            };

            auto tableBuilder = client.GetTableBuilder();
            for (auto& [name, type] : schema) {
                if (name == "a") {
                    tableBuilder.AddNonNullableColumn(name, type);
                } else {
                    tableBuilder.AddNullableColumn(name, type);
                }
            }
            tableBuilder.SetPrimaryKeyColumns({"a"});
            auto result = session.CreateTable(tablePath, tableBuilder.Build(), {}).ExtractValueSync();

            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            // for i in `seq 1 9999`; do echo 'aaa'"$i"',bbb,ccc'; done | ydb import file csv -p import1 --columns "a,b,b"
            TString csv;
            csv += "a,b,b\n"; // wrong header
            for (size_t i = 0; i < 10000; ++i) {
                csv += TString("aaa") + ToString(i) + ",bbb,ccc\n";
            }

            Ydb::Formats::CsvSettings csvSettings;
            csvSettings.set_header(true);
            csvSettings.set_delimiter(",");

            TString formatSettings;
            Y_PROTOBUF_SUPPRESS_NODISCARD csvSettings.SerializeToString(&formatSettings);

            NYdb::NTable::TBulkUpsertSettings upsertSettings;
            upsertSettings.FormatSettings(formatSettings);

            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::CSV, csv, {}, upsertSettings).GetValueSync();

            Cerr << res.GetStatus() << Endl;
            UNIT_ASSERT_EQUAL_C(res.GetStatus(), EStatus::BAD_REQUEST, res.GetIssues().ToString());

            result = session.DropTable(tablePath).GetValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        // Excess columns
        {
            // CREATE TABLE import1(a Text NOT NULL, b Text, PRIMARY KEY(a))
            std::vector<std::pair<TString, NYdb::EPrimitiveType>> schema = {
                { "a", NYdb::EPrimitiveType::Utf8 },
                { "b", NYdb::EPrimitiveType::Utf8 }
            };

            auto tableBuilder = client.GetTableBuilder();
            for (auto& [name, type] : schema) {
                if (name == "a") {
                    tableBuilder.AddNonNullableColumn(name, type);
                } else {
                    tableBuilder.AddNullableColumn(name, type);
                }
            }
            tableBuilder.SetPrimaryKeyColumns({"a"});
            auto result = session.CreateTable(tablePath, tableBuilder.Build(), {}).ExtractValueSync();

            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            TString csv;
            csv += "a,b,c\n"; // header have more columns than table
            for (size_t i = 0; i < 100; ++i) {
                csv += TString("aaa") + ToString(i) + ",bbb,ccc\n";
            }

            Ydb::Formats::CsvSettings csvSettings;
            csvSettings.set_header(true);
            csvSettings.set_delimiter(",");

            TString formatSettings;
            Y_PROTOBUF_SUPPRESS_NODISCARD csvSettings.SerializeToString(&formatSettings);

            NYdb::NTable::TBulkUpsertSettings upsertSettings;
            upsertSettings.FormatSettings(formatSettings);

            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::CSV, csv, {}, upsertSettings).GetValueSync();

            Cerr << res.GetStatus() << Endl;
            UNIT_ASSERT_EQUAL_C(res.GetStatus(), EStatus::SUCCESS, res.GetIssues().ToString());

            result = session.DropTable(tablePath).GetValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
    }

    Y_UNIT_TEST(UpsertCSV) {
        NKikimrConfig::TAppConfig appConfig;
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
            auto rows = ScanQuerySelect(client, tablePath);
            UNIT_ASSERT_GT(rows.size(), 0);
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
            csv = TTestOlap::ToCSV(NArrow::TColumnOperator().VerifyIfAbsent().Extract(sampleBatch, wrongColumns));

            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::CSV, csv).GetValueSync();

            Cerr << "Negative (less columns): " << res.GetStatus() << Endl;
            UNIT_ASSERT(res.GetStatus() != EStatus::SUCCESS);
        }

        { // Reordered columns (it leads to wrong types)
            std::vector<TString> wrongColumns = columns;
            std::sort(wrongColumns.begin(), wrongColumns.end());
            csv = TTestOlap::ToCSV(NArrow::TColumnOperator().VerifyIfAbsent().Extract(sampleBatch, wrongColumns));

            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::CSV, csv).GetValueSync();

            Cerr << "Negative (reordered columns): " << res.GetStatus() << Endl;
            UNIT_ASSERT(res.GetStatus() != EStatus::SUCCESS);
        }

        { // Reordered columns with header
            std::vector<TString> wrongColumns = columns;
            std::sort(wrongColumns.begin(), wrongColumns.end());
            csv = TTestOlap::ToCSV(NArrow::TColumnOperator().VerifyIfAbsent().Extract(sampleBatch, wrongColumns), true);

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
            UNIT_ASSERT(batchSize > 15 * 1024 * 1024 + 130000 * 4 * (ui32)bigBatch->num_columns());
            UNIT_ASSERT(batchSize < 17 * 1024 * 1024 + 130000 * 4 * (ui32)bigBatch->num_columns());

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
            UNIT_ASSERT(batchSize > 16 * 1024 * 1024 + 150000 * 4 * (ui32)bigBatch->num_columns());
            UNIT_ASSERT(batchSize < 20 * 1024 * 1024 + 150000 * 4 * (ui32)bigBatch->num_columns());

            TString bigCsv = TTestOlap::ToCSV(bigBatch);

            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::CSV, bigCsv).GetValueSync();

            Cerr << "big batch: " << res.GetStatus() << Endl;
            UNIT_ASSERT(res.GetStatus() == EStatus::SUCCESS);
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
                if (name == "uid" || name == "timestamp") {
                    tableBuilder.AddNonNullableColumn(name, type);
                } else {
                    tableBuilder.AddNullableColumn(name, type);
                }
            }
            tableBuilder.SetPrimaryKeyColumns({"uid", "timestamp"});
            auto result = session.CreateTable(tablePath, tableBuilder.Build(), {}).ExtractValueSync();

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
            auto rows = ScanQuerySelect(client, tablePath);
            UNIT_ASSERT_GT(rows.size(), 0);
        }

        // Read
        auto res = session.ExecuteDataQuery("SELECT count(*) AS _cnt FROM `/Root/LogsX`;",
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
            auto wrongBatch = NArrow::TColumnOperator().VerifyIfAbsent().Extract(srcBatch, wrongColumns);
            strBatch = NArrow::SerializeBatchNoCompression(wrongBatch);

            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::ApacheArrow, strBatch, strSchema).GetValueSync();

            Cerr << "Negative (less columns): " << res.GetStatus() << Endl;
            UNIT_ASSERT(res.GetStatus() != EStatus::SUCCESS);
        }

        { // Reordered columns (it leads to wrong types)
            std::vector<TString> wrongColumns = columns;
            std::sort(wrongColumns.begin(), wrongColumns.end());
            auto wrongBatch = NArrow::TColumnOperator().VerifyIfAbsent().Extract(srcBatch, wrongColumns);
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
                if (name == "uid" || name == "timestamp") {
                    tableBuilder.AddNonNullableColumn(name, type);
                } else {
                    tableBuilder.AddNullableColumn(name, type);
                }
            }
            tableBuilder.SetPrimaryKeyColumns({"uid", "timestamp"});
            auto result = session.CreateTable(tablePath, tableBuilder.Build(), {}).ExtractValueSync();

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
        auto res = session.ExecuteDataQuery("SELECT count(*) AS _cnt FROM `/Root/LogsX`;",
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
            csv = TTestOlap::ToCSV(NArrow::TColumnOperator().VerifyIfAbsent().Extract(sampleBatch, wrongColumns));

            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::CSV, csv).GetValueSync();

            Cerr << "Negative (less columns): " << res.GetStatus() << Endl;
            UNIT_ASSERT(res.GetStatus() != EStatus::SUCCESS);
        }

        { // Reordered columns (it leads to wrong types)
            std::vector<TString> wrongColumns = columns;
            std::sort(wrongColumns.begin(), wrongColumns.end());
            csv = TTestOlap::ToCSV(NArrow::TColumnOperator().VerifyIfAbsent().Extract(sampleBatch, wrongColumns));

            auto res = client.BulkUpsert(tablePath,
                NYdb::NTable::EDataFormat::CSV, csv).GetValueSync();

            Cerr << "Negative (reordered columns): " << res.GetStatus() << Endl;
            UNIT_ASSERT(res.GetStatus() != EStatus::SUCCESS);
        }

        { // Reordered columns with header
            std::vector<TString> wrongColumns = columns;
            std::sort(wrongColumns.begin(), wrongColumns.end());
            csv = TTestOlap::ToCSV(NArrow::TColumnOperator().VerifyIfAbsent().Extract(sampleBatch, wrongColumns), true);

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

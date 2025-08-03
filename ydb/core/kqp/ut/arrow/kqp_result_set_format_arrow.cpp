#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

TKikimrRunner CreateKikimrRunner() {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
    return TKikimrRunner(appConfig);
}

void FillOltpTypesTable(TQueryClient& client) {
    auto createResult = client.ExecuteQuery(R"(
        CREATE TABLE `/Root/OltpTable` (
            Key Uint64,
            BoolValue Bool,
            Int8Value Int8,
            Uint8Value Uint8,
            Int16Value Int16,
            Uint16Value Uint16,
            Int32Value Int32,
            Uint32Value Uint32,
            Int64Value Int64,
            Uint64Value Uint64,
            FloatValue Float,
            DoubleValue Double,
            StringValue String,
            Utf8Value Utf8,
            DateValue Date,
            DatetimeValue Datetime,
            TimestampValue Timestamp,
            IntervalValue Interval,
            DecimalValue Decimal(22,9),
            JsonValue Json,
            YsonValue Yson,
            JsonDocumentValue JsonDocument,
            DyNumberValue DyNumber,
            Int32NotNullValue Int32 NOT NULL,
            PRIMARY KEY (Key)
        );
    )", TTxControl::NoTx()).GetValueSync();
    UNIT_ASSERT_C(createResult.IsSuccess(), createResult.GetIssues().ToString());

    auto insertResult = client.ExecuteQuery(R"(
        INSERT INTO `/Root/OltpTable` (Key, BoolValue, Int8Value, Uint8Value, Int16Value, Uint16Value, Int32Value, Uint32Value, Int64Value, Uint64Value, FloatValue, DoubleValue, StringValue, Utf8Value, DateValue, DatetimeValue, TimestampValue, IntervalValue, DecimalValue, JsonValue, YsonValue, JsonDocumentValue, DyNumberValue, Int32NotNullValue) VALUES
        (42, true, -1, 1, -2, 2, -3, 3, -4, 4, CAST(3.0 AS Float), 4.0, "five", Utf8("six"), Date("2007-07-07"), Datetime("2008-08-08T08:08:08Z"), Timestamp("2009-09-09T09:09:09.09Z"), Interval("P10D"), CAST("11.11" AS Decimal(22, 9)), "[12]", "[13]", JsonDocument("[14]"), DyNumber("15.15"), 123);
    )", TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(insertResult.IsSuccess(), insertResult.GetIssues().ToString());
}

void FillOlapTypesTable(TQueryClient& client) {
    auto createResult = client.ExecuteQuery(R"(
        CREATE TABLE `/Root/OlapTable` (
            Key Uint64 NOT NULL,
            Int8Value Int8,
            Uint8Value Uint8,
            Int16Value Int16,
            Uint16Value Uint16,
            Int32Value Int32,
            Uint32Value Uint32,
            Int64Value Int64,
            Uint64Value Uint64,
            FloatValue Float,
            DoubleValue Double,
            StringValue String,
            Utf8Value Utf8,
            DateValue Date,
            DatetimeValue Datetime,
            TimestampValue Timestamp,
            JsonValue Json,
            YsonValue Yson,
            JsonDocumentValue JsonDocument,
            PRIMARY KEY (Key)
        ) WITH (
            STORE = COLUMN
        );
    )", TTxControl::NoTx()).GetValueSync();
    UNIT_ASSERT_C(createResult.IsSuccess(), createResult.GetIssues().ToString());

    auto insertResult = client.ExecuteQuery(R"(
        INSERT INTO `/Root/OlapTable` (Key, Int8Value, Uint8Value, Int16Value, Uint16Value, Int32Value, Uint32Value, Int64Value, Uint64Value, FloatValue, DoubleValue, StringValue, Utf8Value, DateValue, DatetimeValue, TimestampValue, JsonValue, YsonValue, JsonDocumentValue) VALUES
        (42, -1, 1, -2, 2, -3, 3, -4, 4, CAST(3.0 AS Float), 4.0, "five", Utf8("six"), Date("2007-07-07"), Datetime("2008-08-08T08:08:08Z"), Timestamp("2009-09-09T09:09:09.09Z"), "[12]", "[13]", JsonDocument("[14]"));
    )", TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(insertResult.IsSuccess(), insertResult.GetIssues().ToString());
}

void AssertRecordBatchSize(const TResultSet& messageResultSet, const TResultSet& arrowResultSet) {
    UNIT_ASSERT_VALUES_EQUAL_C(messageResultSet.ColumnsCount(), arrowResultSet.ColumnsCount(), "Columns count mismatch");

    auto [schema, recordBatches] = arrowResultSet.GetArrowResult();

    if (messageResultSet.RowsCount() == 0) {
        UNIT_ASSERT_C(schema.empty(), "Schema should be empty");
        UNIT_ASSERT_C(recordBatches.empty(), "Record batches should be empty");
        return;
    }

    auto arrowSchema = NArrow::DeserializeSchema(TString(schema));
    size_t arrowRowsCount = 0;

    for (const auto& recordBatch : recordBatches) {
        auto batch = NArrow::DeserializeBatch(TString(recordBatch), arrowSchema);
        arrowRowsCount += batch->num_rows();

        UNIT_ASSERT_VALUES_EQUAL_C(batch->num_columns(), messageResultSet.ColumnsCount(), "Columns count mismatch");
    }

    UNIT_ASSERT_VALUES_EQUAL_C(messageResultSet.RowsCount(), arrowRowsCount, "Rows count mismatch");
}

} // namespace

Y_UNIT_TEST_SUITE(KqpResultSetFormatArrow) {
    Y_UNIT_TEST_TWIN(AllTypes, isOlap) {
        auto kikimr = CreateKikimrRunner();
        auto client = kikimr.GetQueryClient();

        if (isOlap) {
            FillOlapTypesTable(client);
        } else {
            FillOltpTypesTable(client);
        }

        const TString query = Sprintf(R"(
            SELECT * FROM `/Root/%s`;
        )", (isOlap) ? "OlapTable" : "OltpTable");

        auto messageResponse = client.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
            TExecuteQuerySettings().Format(EResultSetFormat::Value)).GetValueSync();

        auto arrowResponse = client.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
            TExecuteQuerySettings().Format(EResultSetFormat::Arrow)).GetValueSync();

        UNIT_ASSERT_C(messageResponse.IsSuccess(), messageResponse.GetIssues().ToString());
        UNIT_ASSERT_C(arrowResponse.IsSuccess(), arrowResponse.GetIssues().ToString());

        AssertRecordBatchSize(messageResponse.GetResultSet(0), arrowResponse.GetResultSet(0));
    }

    Y_UNIT_TEST(LargeTable) {
        auto kikimr = CreateKikimrRunner();
        auto client = kikimr.GetQueryClient();

        CreateLargeTable(kikimr, 10000, 4, 10, 5000, 10);

        const TString query = Sprintf(R"(
            SELECT * FROM `/Root/LargeTable`;
        )");

        auto messageResponse = client.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
            TExecuteQuerySettings().Format(EResultSetFormat::Value)).GetValueSync();

        auto arrowResponse = client.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
            TExecuteQuerySettings().Format(EResultSetFormat::Arrow)).GetValueSync();

        UNIT_ASSERT_C(messageResponse.IsSuccess(), messageResponse.GetIssues().ToString());
        UNIT_ASSERT_C(arrowResponse.IsSuccess(), arrowResponse.GetIssues().ToString());

        AssertRecordBatchSize(messageResponse.GetResultSet(0), arrowResponse.GetResultSet(0));
    }

    Y_UNIT_TEST(LargeLimitTable) {
        auto kikimr = CreateKikimrRunner();
        auto client = kikimr.GetQueryClient();

        CreateLargeTable(kikimr, 10000, 4, 10, 5000, 10);

        const TString query = Sprintf(R"(
            SELECT * FROM `/Root/LargeTable` LIMIT 70000;
        )");

        auto messageResponse = client.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
            TExecuteQuerySettings().Format(EResultSetFormat::Value)).GetValueSync();

        auto arrowResponse = client.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
            TExecuteQuerySettings().Format(EResultSetFormat::Arrow)).GetValueSync();

        UNIT_ASSERT_C(messageResponse.IsSuccess(), messageResponse.GetIssues().ToString());
        UNIT_ASSERT_C(arrowResponse.IsSuccess(), arrowResponse.GetIssues().ToString());

        AssertRecordBatchSize(messageResponse.GetResultSet(0), arrowResponse.GetResultSet(0));
    }

    Y_UNIT_TEST_TWIN(Returning, isOlap) {
        auto kikimr = CreateKikimrRunner();
        auto client = kikimr.GetQueryClient();

        TString query;

        if (isOlap) {
            FillOlapTypesTable(client);
            query = R"(
                    UPSERT INTO `/Root/OlapTable` (Key, Int8Value, Uint8Value, Int16Value, Uint16Value, Int32Value, Uint32Value, Int64Value, Uint64Value, FloatValue, DoubleValue, StringValue, Utf8Value, DateValue, DatetimeValue, TimestampValue, JsonValue, YsonValue, JsonDocumentValue) VALUES
                (43, -1, 1, -2, 2, -3, 3, -4, 4, CAST(3.0 AS Float), 4.0, "five", Utf8("six"), Date("2007-07-07"), Datetime("2008-08-08T08:08:08Z"), Timestamp("2009-09-09T09:09:09.09Z"), "[12]", "[13]", JsonDocument("[14]")),
                (44, -1, 1, -2, 2, -3, 3, -4, 4, CAST(3.0 AS Float), 4.0, "five", Utf8("six"), Date("2007-07-07"), Datetime("2008-08-08T08:08:08Z"), Timestamp("2009-09-09T09:09:09.09Z"), "[12]", "[13]", JsonDocument("[14]")),
                (45, -1, 1, -2, 2, -3, 3, -4, 4, CAST(3.0 AS Float), 4.0, "five", Utf8("six"), Date("2007-07-07"), Datetime("2008-08-08T08:08:08Z"), Timestamp("2009-09-09T09:09:09.09Z"), "[12]", "[13]", JsonDocument("[14]"))
                RETURNING *;
            )";
        } else {
            FillOltpTypesTable(client);
            query = R"(
                    UPSERT INTO `/Root/OltpTable` (Key, BoolValue, Int8Value, Uint8Value, Int16Value, Uint16Value, Int32Value, Uint32Value, Int64Value, Uint64Value, FloatValue, DoubleValue, StringValue, Utf8Value, DateValue, DatetimeValue, TimestampValue, IntervalValue, DecimalValue, JsonValue, YsonValue, JsonDocumentValue, DyNumberValue, Int32NotNullValue) VALUES
                (43, true, -1, 1, -2, 2, -3, 3, -4, 4, CAST(3.0 AS Float), 4.0, "five", Utf8("six"), Date("2007-07-07"), Datetime("2008-08-08T08:08:08Z"), Timestamp("2009-09-09T09:09:09.09Z"), Interval("P10D"), CAST("11.11" AS Decimal(22, 9)), "[12]", "[13]", JsonDocument("[14]"), DyNumber("15.15"), 123),
                (44, true, -1, 1, -2, 2, -3, 3, -4, 4, CAST(3.0 AS Float), 4.0, "five", Utf8("six"), Date("2007-07-07"), Datetime("2008-08-08T08:08:08Z"), Timestamp("2009-09-09T09:09:09.09Z"), Interval("P10D"), CAST("11.11" AS Decimal(22, 9)), "[12]", "[13]", JsonDocument("[14]"), DyNumber("15.15"), 123),
                (45, true, -1, 1, -2, 2, -3, 3, -4, 4, CAST(3.0 AS Float), 4.0, "five", Utf8("six"), Date("2007-07-07"), Datetime("2008-08-08T08:08:08Z"), Timestamp("2009-09-09T09:09:09.09Z"), Interval("P10D"), CAST("11.11" AS Decimal(22, 9)), "[12]", "[13]", JsonDocument("[14]"), DyNumber("15.15"), 123)
                RETURNING *;
            )";
        }

        auto messageResponse = client.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
            TExecuteQuerySettings().Format(EResultSetFormat::Value)).GetValueSync();

        auto arrowResponse = client.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
            TExecuteQuerySettings().Format(EResultSetFormat::Arrow)).GetValueSync();

        UNIT_ASSERT_C(messageResponse.IsSuccess(), messageResponse.GetIssues().ToString());
        UNIT_ASSERT_C(arrowResponse.IsSuccess(), arrowResponse.GetIssues().ToString());

        AssertRecordBatchSize(messageResponse.GetResultSet(0), arrowResponse.GetResultSet(0));
    }

    Y_UNIT_TEST(ColumnOrder_1) {
        auto kikimr = CreateKikimrRunner();
        auto client = kikimr.GetQueryClient();

        {
            auto result = client.ExecuteQuery(R"(
                SELECT Name, Amount FROM Test WHERE Group = 2;
            )", TTxControl::BeginTx().CommitTx(), TExecuteQuerySettings().Format(EResultSetFormat::Arrow)).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto [schema, recordBatches] = result.GetResultSet(0).GetArrowResult();

            UNIT_ASSERT(!schema.empty());
            UNIT_ASSERT(!recordBatches.empty());

            auto arrowSchema = NArrow::DeserializeSchema(TString(schema));
            auto batch = NArrow::DeserializeBatch(TString(recordBatches[0]), arrowSchema);

            UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 1);
            UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 2);

            std::vector<std::tuple<std::string, ui64>> expected = {{"Tony", 7200}};

            auto nameColumn = std::static_pointer_cast<arrow::BinaryArray>(batch->column(0));
            auto amountColumn = std::static_pointer_cast<arrow::UInt64Array>(batch->column(1));

            for (int64_t i = 0; i < batch->num_rows(); ++i) {
                const auto& row = expected[i];

                UNIT_ASSERT(!nameColumn->IsNull(i));
                UNIT_ASSERT_VALUES_EQUAL(nameColumn->GetString(i), std::get<0>(row));

                UNIT_ASSERT(!amountColumn->IsNull(i));
                UNIT_ASSERT_VALUES_EQUAL(amountColumn->Value(i), std::get<1>(row));

            }
        }
        {
            auto result = client.ExecuteQuery(R"(
                SELECT Amount, Name FROM Test WHERE Group = 2;
            )", TTxControl::BeginTx().CommitTx(), TExecuteQuerySettings().Format(EResultSetFormat::Arrow)).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto [schema, recordBatches] = result.GetResultSet(0).GetArrowResult();

            UNIT_ASSERT(!schema.empty());
            UNIT_ASSERT(!recordBatches.empty());

            auto arrowSchema = NArrow::DeserializeSchema(TString(schema));
            auto batch = NArrow::DeserializeBatch(TString(recordBatches[0]), arrowSchema);

            UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 1);
            UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 2);

            std::vector<std::tuple<ui64, std::string>> expected = {{7200, "Tony"}};

            auto amountColumn = std::static_pointer_cast<arrow::UInt64Array>(batch->column(0));
            auto nameColumn = std::static_pointer_cast<arrow::BinaryArray>(batch->column(1));

            for (int64_t i = 0; i < batch->num_rows(); ++i) {
                const auto& row = expected[i];

                UNIT_ASSERT(!amountColumn->IsNull(i));
                UNIT_ASSERT_VALUES_EQUAL(amountColumn->Value(i), std::get<0>(row));

                UNIT_ASSERT(!nameColumn->IsNull(i));
                UNIT_ASSERT_VALUES_EQUAL(nameColumn->GetString(i), std::get<1>(row));
            }
        }
        {
            auto result = client.ExecuteQuery(R"(
                SELECT Comment, Amount, Name FROM Test ORDER BY Amount DESC;
            )", TTxControl::BeginTx().CommitTx(), TExecuteQuerySettings().Format(EResultSetFormat::Arrow)).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto [schema, recordBatches] = result.GetResultSet(0).GetArrowResult();

            UNIT_ASSERT(!schema.empty());
            UNIT_ASSERT(!recordBatches.empty());

            auto arrowSchema = NArrow::DeserializeSchema(TString(schema));
            auto batch = NArrow::DeserializeBatch(TString(recordBatches[0]), arrowSchema);

            UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 3);
            UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 3);

            std::vector<std::tuple<std::string, ui64, std::string>> expected = {{"None", 7200, "Tony"}, {"None", 3500, "Anna"}, {"None", 300, "Paul"}};

            auto commentColumn = std::static_pointer_cast<arrow::BinaryArray>(batch->column(0));
            auto amountColumn = std::static_pointer_cast<arrow::UInt64Array>(batch->column(1));
            auto nameColumn = std::static_pointer_cast<arrow::BinaryArray>(batch->column(2));

            for (int64_t i = 0; i < batch->num_rows(); ++i) {
                const auto& row = expected[i];

                UNIT_ASSERT(!commentColumn->IsNull(i));
                UNIT_ASSERT_VALUES_EQUAL(commentColumn->GetString(i), std::get<0>(row));

                UNIT_ASSERT(!amountColumn->IsNull(i));
                UNIT_ASSERT_VALUES_EQUAL(amountColumn->Value(i), std::get<1>(row));

                UNIT_ASSERT(!nameColumn->IsNull(i));
                UNIT_ASSERT_VALUES_EQUAL(nameColumn->GetString(i), std::get<2>(row));
            }
        }
    }

    Y_UNIT_TEST(ColumnOrder_2) {
        auto kikimr = CreateKikimrRunner();
        auto client = kikimr.GetQueryClient();

        {
            auto result = client.ExecuteQuery(R"(
                CREATE TABLE TestUtf8 (
                    id Int32 NOT NULL,
                    name Utf8,
                    is_valid Bool,
                    PRIMARY KEY(id, name)
                );
            )", TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = client.ExecuteQuery(R"(
                INSERT INTO TestUtf8 (id, name, is_valid) VALUES (1, "John", true), (2, "John", false);
            )", TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = client.ExecuteQuery(R"(
                SELECT name, is_valid FROM TestUtf8 WHERE is_valid = true;
            )", TTxControl::BeginTx().CommitTx(), TExecuteQuerySettings().Format(EResultSetFormat::Arrow)).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto [schema, recordBatches] = result.GetResultSet(0).GetArrowResult();

            UNIT_ASSERT(!schema.empty());
            UNIT_ASSERT(!recordBatches.empty());

            auto arrowSchema = NArrow::DeserializeSchema(TString(schema));
            auto batch = NArrow::DeserializeBatch(TString(recordBatches[0]), arrowSchema);

            UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 1);
            UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 2);

            std::vector<std::tuple<std::string, bool>> expected = {{"John", true}};

            auto nameColumn = std::static_pointer_cast<arrow::StringArray>(batch->column(0));
            auto isValidColumn = std::static_pointer_cast<arrow::BooleanArray>(batch->column(1));

            for (int64_t i = 0; i < batch->num_rows(); ++i) {
                const auto& row = expected[i];

                UNIT_ASSERT(!nameColumn->IsNull(i));
                UNIT_ASSERT_VALUES_EQUAL(nameColumn->GetString(i), std::get<0>(row));

                UNIT_ASSERT(!isValidColumn->IsNull(i));
                UNIT_ASSERT_VALUES_EQUAL(isValidColumn->Value(i), std::get<1>(row));
            }
        }
        {
            auto result = client.ExecuteQuery(R"(
                SELECT is_valid, name FROM TestUtf8 WHERE is_valid = false;
            )", TTxControl::BeginTx().CommitTx(), TExecuteQuerySettings().Format(EResultSetFormat::Arrow)).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto [schema, recordBatches] = result.GetResultSet(0).GetArrowResult();

            UNIT_ASSERT(!schema.empty());
            UNIT_ASSERT(!recordBatches.empty());

            auto arrowSchema = NArrow::DeserializeSchema(TString(schema));
            auto batch = NArrow::DeserializeBatch(TString(recordBatches[0]), arrowSchema);

            UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 1);
            UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 2);

            std::vector<std::tuple<bool, std::string>> expected = {{false, "John"}};

            auto isValidColumn = std::static_pointer_cast<arrow::BooleanArray>(batch->column(0));
            auto nameColumn = std::static_pointer_cast<arrow::StringArray>(batch->column(1));

            for (int64_t i = 0; i < batch->num_rows(); ++i) {
                const auto& row = expected[i];

                UNIT_ASSERT(!isValidColumn->IsNull(i));
                UNIT_ASSERT_VALUES_EQUAL(isValidColumn->Value(i), std::get<0>(row));

                UNIT_ASSERT(!nameColumn->IsNull(i));
                UNIT_ASSERT_VALUES_EQUAL(nameColumn->GetString(i), std::get<1>(row));
            }
        }
    }

    Y_UNIT_TEST(Compression_ZSTD) {
        auto kikimr = CreateKikimrRunner();
        auto client = kikimr.GetQueryClient();

        {
            auto settings = TExecuteQuerySettings()
                .Format(EResultSetFormat::Arrow)
                .ArrowFormatSettings(TArrowFormatSettings()
                    .CompressionCodec(TArrowFormatSettings::TCompressionCodec()
                        .Type(TArrowFormatSettings::TCompressionCodec::EType::Zstd)));

            auto result = client.ExecuteQuery(R"(
                SELECT Comment, Amount, Name FROM Test ORDER BY Amount DESC;
            )", TTxControl::BeginTx().CommitTx(), settings).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto [schema, recordBatches] = result.GetResultSet(0).GetArrowResult();

            UNIT_ASSERT(!schema.empty());
            UNIT_ASSERT(!recordBatches.empty());

            auto arrowSchema = NArrow::DeserializeSchema(TString(schema));
            auto batch = NArrow::DeserializeBatch(TString(recordBatches[0]), arrowSchema);

            UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 3);
            UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 3);

            std::vector<std::tuple<std::string, ui64, std::string>> expected = {{"None", 7200, "Tony"}, {"None", 3500, "Anna"}, {"None", 300, "Paul"}};

            auto commentColumn = std::static_pointer_cast<arrow::BinaryArray>(batch->column(0));
            auto amountColumn = std::static_pointer_cast<arrow::UInt64Array>(batch->column(1));
            auto nameColumn = std::static_pointer_cast<arrow::BinaryArray>(batch->column(2));

            for (int64_t i = 0; i < batch->num_rows(); ++i) {
                const auto& row = expected[i];

                UNIT_ASSERT(!commentColumn->IsNull(i));
                UNIT_ASSERT_VALUES_EQUAL(commentColumn->GetString(i), std::get<0>(row));

                UNIT_ASSERT(!amountColumn->IsNull(i));
                UNIT_ASSERT_VALUES_EQUAL(amountColumn->Value(i), std::get<1>(row));

                UNIT_ASSERT(!nameColumn->IsNull(i));
                UNIT_ASSERT_VALUES_EQUAL(nameColumn->GetString(i), std::get<2>(row));

            }
        }
    }

    Y_UNIT_TEST(Compression_LZ4_FRAME) {
        auto kikimr = CreateKikimrRunner();
        auto client = kikimr.GetQueryClient();

        {
            auto settings = TExecuteQuerySettings()
                .Format(EResultSetFormat::Arrow)
                .ArrowFormatSettings(TArrowFormatSettings()
                    .CompressionCodec(TArrowFormatSettings::TCompressionCodec()
                        .Type(TArrowFormatSettings::TCompressionCodec::EType::Lz4Frame)));

            auto result = client.ExecuteQuery(R"(
                SELECT Comment, Amount, Name FROM Test ORDER BY Amount DESC;
            )", TTxControl::BeginTx().CommitTx(), settings).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto [schema, recordBatches] = result.GetResultSet(0).GetArrowResult();

            UNIT_ASSERT(!schema.empty());
            UNIT_ASSERT(!recordBatches.empty());

            auto arrowSchema = NArrow::DeserializeSchema(TString(schema));
            auto batch = NArrow::DeserializeBatch(TString(recordBatches[0]), arrowSchema);

            UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 3);
            UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 3);

            std::vector<std::tuple<std::string, ui64, std::string>> expected = {{"None", 7200, "Tony"}, {"None", 3500, "Anna"}, {"None", 300, "Paul"}};

            auto commentColumn = std::static_pointer_cast<arrow::BinaryArray>(batch->column(0));
            auto amountColumn = std::static_pointer_cast<arrow::UInt64Array>(batch->column(1));
            auto nameColumn = std::static_pointer_cast<arrow::BinaryArray>(batch->column(2));

            for (int64_t i = 0; i < batch->num_rows(); ++i) {
                const auto& row = expected[i];

                UNIT_ASSERT(!commentColumn->IsNull(i));
                UNIT_ASSERT_VALUES_EQUAL(commentColumn->GetString(i), std::get<0>(row));

                UNIT_ASSERT(!amountColumn->IsNull(i));
                UNIT_ASSERT_VALUES_EQUAL(amountColumn->Value(i), std::get<1>(row));

                UNIT_ASSERT(!nameColumn->IsNull(i));
                UNIT_ASSERT_VALUES_EQUAL(nameColumn->GetString(i), std::get<2>(row));
            }
        }
    }
}

} // namespace NKqp
} // namespace NKikimr

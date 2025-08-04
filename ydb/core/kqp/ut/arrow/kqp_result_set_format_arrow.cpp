#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/library/formats/arrow/arrow_helpers.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;
using TTypeInfo = NScheme::TTypeInfo;
namespace NTypeIds = NScheme::NTypeIds;

namespace {

    TKikimrRunner CreateKikimrRunner(bool withSampleTables) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        auto settings = TKikimrSettings(appConfig).SetWithSampleTables(withSampleTables);
        return TKikimrRunner(settings);
    }

void CreateAllTypesRowTable(TQueryClient& client) {
    auto createResult = client.ExecuteQuery(R"(
        CREATE TABLE `/Root/RowTable` (
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
        INSERT INTO `/Root/RowTable` (Key, BoolValue, Int8Value, Uint8Value, Int16Value, Uint16Value, Int32Value, Uint32Value, Int64Value, Uint64Value, FloatValue, DoubleValue, StringValue, Utf8Value, DateValue, DatetimeValue, TimestampValue, IntervalValue, DecimalValue, JsonValue, YsonValue, JsonDocumentValue, DyNumberValue, Int32NotNullValue) VALUES
        (42, true, -1, 1, -2, 2, -3, 3, -4, 4, CAST(3.0 AS Float), 4.0, "five", Utf8("six"), Date("2007-07-07"), Datetime("2008-08-08T08:08:08Z"), Timestamp("2009-09-09T09:09:09.09Z"), Interval("P10D"), CAST("11.11" AS Decimal(22, 9)), "[12]", "[13]", JsonDocument("[14]"), DyNumber("15.15"), 123);
    )", TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(insertResult.IsSuccess(), insertResult.GetIssues().ToString());
}

void CreateAllTypesColumnTable(TQueryClient& client) {
    auto createResult = client.ExecuteQuery(R"(
        CREATE TABLE `/Root/ColumnTable` (
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
        INSERT INTO `/Root/ColumnTable` (Key, Int8Value, Uint8Value, Int16Value, Uint16Value, Int32Value, Uint32Value, Int64Value, Uint64Value, FloatValue, DoubleValue, StringValue, Utf8Value, DateValue, DatetimeValue, TimestampValue, JsonValue, YsonValue, JsonDocumentValue) VALUES
        (42, -1, 1, -2, 2, -3, 3, -4, 4, CAST(3.0 AS Float), 4.0, "five", Utf8("six"), Date("2007-07-07"), Datetime("2008-08-08T08:08:08Z"), Timestamp("2009-09-09T09:09:09.09Z"), "[12]", "[13]", JsonDocument("[14]"));
    )", TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(insertResult.IsSuccess(), insertResult.GetIssues().ToString());
}

void AssertArrowValueResultsSize(const std::vector<TResultSet>& arrowResultSets, const std::vector<TResultSet>& valueResultSets) {
    size_t valueRowsCount = 0;
    size_t valueColumnsCount = 0;

    for (const auto& resultSet : valueResultSets) {
        UNIT_ASSERT_VALUES_EQUAL_C(resultSet.Format(), TResultSet::EFormat::Value, "Result set format mismatch");

        valueRowsCount += resultSet.RowsCount();
        if (!valueColumnsCount) {
            valueColumnsCount = resultSet.ColumnsCount();
        }
    }

    size_t arrowRowsCount = 0;

    std::shared_ptr<arrow::Schema> arrowSchema;
    for (const auto& resultSet : arrowResultSets) {
        UNIT_ASSERT_VALUES_EQUAL_C(resultSet.Format(), TResultSet::EFormat::Arrow, "Result set format mismatch");

        const auto& [schema, recordBatches] = resultSet.GetArrowResult();
        UNIT_ASSERT_C(!schema.empty() || arrowSchema, "Schema must be empty only if it's not the first result set");

        if (!arrowSchema) {
            arrowSchema = NArrow::DeserializeSchema(TString(schema));
            UNIT_ASSERT_VALUES_EQUAL_C(arrowSchema->num_fields(), valueColumnsCount, "Columns count mismatch");
        }

        for (const auto& recordBatch : recordBatches) {
            auto batch = NArrow::DeserializeBatch(TString(recordBatch), arrowSchema);
            UNIT_ASSERT_C(batch->ValidateFull().ok(), "Batch validation failed");
            arrowRowsCount += batch->num_rows();

            UNIT_ASSERT_VALUES_EQUAL_C(batch->num_columns(), valueColumnsCount, "Columns count mismatch");
        }
    }

    UNIT_ASSERT_VALUES_EQUAL_C(valueRowsCount, arrowRowsCount, "Rows count mismatch");
}

std::shared_ptr<arrow::RecordBatch> ExecuteAndCombineBatches(TQueryClient& client, const TString& query, bool assertSize = false) {
    auto arrowResponse = client.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(), TExecuteQuerySettings().Format(TResultSet::EFormat::Arrow)).GetValueSync();
    UNIT_ASSERT_C(arrowResponse.IsSuccess(), arrowResponse.GetIssues().ToString());

    if (assertSize) {
        auto valueResponse = client.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(), TExecuteQuerySettings().Format(TResultSet::EFormat::Value)).GetValueSync();
        UNIT_ASSERT_C(valueResponse.IsSuccess(), valueResponse.GetIssues().ToString());
        AssertArrowValueResultsSize(arrowResponse.GetResultSets(), valueResponse.GetResultSets());
    }

    const auto& [schema, batches] = arrowResponse.GetResultSet(0).GetArrowResult();

    std::vector<std::shared_ptr<arrow::RecordBatch>> recordBatches;
    std::shared_ptr<arrow::Schema> arrowSchema = NArrow::DeserializeSchema(TString(schema));

    for (const auto& batch : batches) {
        auto arrowBatch = NArrow::DeserializeBatch(TString(batch), arrowSchema);
        UNIT_ASSERT_C(arrowBatch->ValidateFull().ok(), "Batch validation failed");
        recordBatches.push_back(arrowBatch);
    }

    auto resultBatch = NArrow::CombineBatches(recordBatches);
    UNIT_ASSERT_C(resultBatch->ValidateFull().ok(), "Batch combine validation failed");

    return resultBatch;
}

} // namespace

Y_UNIT_TEST_SUITE(KqpResultSetFormat) {
    Y_UNIT_TEST_TWIN(Arrow_AllTypes, isOlap) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ false);
        auto client = kikimr.GetQueryClient();

        if (isOlap) {
            CreateAllTypesColumnTable(client);
        } else {
            CreateAllTypesRowTable(client);
        }

        const TString query = Sprintf(R"(
            SELECT * FROM `/Root/%s`;
        )", (isOlap) ? "ColumnTable" : "RowTable");

        Y_UNUSED(ExecuteAndCombineBatches(client, query, /* assertSize */ true));
    }

    Y_UNIT_TEST(ArrowFormat_LargeTable) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ false);
        auto client = kikimr.GetQueryClient();

        CreateLargeTable(kikimr, 10000, 4, 10, 5000, 10);

        const TString query = Sprintf(R"(
            SELECT * FROM `/Root/LargeTable`;
        )");

        Y_UNUSED(ExecuteAndCombineBatches(client, query, /* assertSize */ true));
    }

    Y_UNIT_TEST(ArrowFormat_LargeTableLimit) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ false);
        auto client = kikimr.GetQueryClient();

        CreateLargeTable(kikimr, 10000, 4, 10, 5000, 10);

        const TString query = Sprintf(R"(
            SELECT * FROM `/Root/LargeTable` LIMIT 70000;
        )");

        Y_UNUSED(ExecuteAndCombineBatches(client, query, /* assertSize */ true));
    }

    Y_UNIT_TEST_TWIN(Arrow_Returning, isOlap) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ false);
        auto client = kikimr.GetQueryClient();

        TString query;

        if (isOlap) {
            CreateAllTypesColumnTable(client);
            query = R"(
                UPSERT INTO `/Root/ColumnTable` (Key, Int8Value, Uint8Value, Int16Value, Uint16Value, Int32Value, Uint32Value, Int64Value, Uint64Value, FloatValue, DoubleValue, StringValue, Utf8Value, DateValue, DatetimeValue, TimestampValue, JsonValue, YsonValue, JsonDocumentValue) VALUES
                (43, -1, 1, -2, 2, -3, 3, -4, 4, CAST(3.0 AS Float), 4.0, "five", Utf8("six"), Date("2007-07-07"), Datetime("2008-08-08T08:08:08Z"), Timestamp("2009-09-09T09:09:09.09Z"), "[12]", "[13]", JsonDocument("[14]")),
                (44, -1, 1, -2, 2, -3, 3, -4, 4, CAST(3.0 AS Float), 4.0, "five", Utf8("six"), Date("2007-07-07"), Datetime("2008-08-08T08:08:08Z"), Timestamp("2009-09-09T09:09:09.09Z"), "[12]", "[13]", JsonDocument("[14]")),
                (45, -1, 1, -2, 2, -3, 3, -4, 4, CAST(3.0 AS Float), 4.0, "five", Utf8("six"), Date("2007-07-07"), Datetime("2008-08-08T08:08:08Z"), Timestamp("2009-09-09T09:09:09.09Z"), "[12]", "[13]", JsonDocument("[14]"))
                RETURNING *;
            )";
        } else {
            CreateAllTypesRowTable(client);
            query = R"(
                UPSERT INTO `/Root/RowTable` (Key, BoolValue, Int8Value, Uint8Value, Int16Value, Uint16Value, Int32Value, Uint32Value, Int64Value, Uint64Value, FloatValue, DoubleValue, StringValue, Utf8Value, DateValue, DatetimeValue, TimestampValue, IntervalValue, DecimalValue, JsonValue, YsonValue, JsonDocumentValue, DyNumberValue, Int32NotNullValue) VALUES
                (43, true, -1, 1, -2, 2, -3, 3, -4, 4, CAST(3.0 AS Float), 4.0, "five", Utf8("six"), Date("2007-07-07"), Datetime("2008-08-08T08:08:08Z"), Timestamp("2009-09-09T09:09:09.09Z"), Interval("P10D"), CAST("11.11" AS Decimal(22, 9)), "[12]", "[13]", JsonDocument("[14]"), DyNumber("15.15"), 123),
                (44, true, -1, 1, -2, 2, -3, 3, -4, 4, CAST(3.0 AS Float), 4.0, "five", Utf8("six"), Date("2007-07-07"), Datetime("2008-08-08T08:08:08Z"), Timestamp("2009-09-09T09:09:09.09Z"), Interval("P10D"), CAST("11.11" AS Decimal(22, 9)), "[12]", "[13]", JsonDocument("[14]"), DyNumber("15.15"), 123),
                (45, true, -1, 1, -2, 2, -3, 3, -4, 4, CAST(3.0 AS Float), 4.0, "five", Utf8("six"), Date("2007-07-07"), Datetime("2008-08-08T08:08:08Z"), Timestamp("2009-09-09T09:09:09.09Z"), Interval("P10D"), CAST("11.11" AS Decimal(22, 9)), "[12]", "[13]", JsonDocument("[14]"), DyNumber("15.15"), 123)
                RETURNING *;
            )";
        }

        Y_UNUSED(ExecuteAndCombineBatches(client, query, /* assertSize */ true));
    }

    Y_UNIT_TEST(ArrowFormat_ColumnOrder) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ true);
        auto client = kikimr.GetQueryClient();

        {
            auto batch = ExecuteAndCombineBatches(client, R"(
                SELECT Name, Amount FROM Test WHERE Group = 2;
            )", /* assertSize */ true);

            NColumnShard::TTableUpdatesBuilder builder(NArrow::MakeArrowSchema({
                std::make_pair("Name", TTypeInfo(NTypeIds::String)),
                std::make_pair("Amount", TTypeInfo(NTypeIds::Uint64))
            }));

            builder.AddRow().Add<std::string>("Tony").Add<ui64>(7200);

            auto expected = builder.BuildArrow();
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
        }
        {
            auto batch = ExecuteAndCombineBatches(client, R"(
                SELECT Amount, Name FROM Test WHERE Group = 2;
            )", /* assertSize */ true);

            NColumnShard::TTableUpdatesBuilder builder(NArrow::MakeArrowSchema({
                std::make_pair("Amount", TTypeInfo(NTypeIds::Uint64)),
                std::make_pair("Name", TTypeInfo(NTypeIds::String))
            }));

            builder.AddRow().Add<ui64>(7200).Add<std::string>("Tony");

            auto expected = builder.BuildArrow();
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
        }
        {
            auto batch = ExecuteAndCombineBatches(client, R"(
                SELECT Comment, Amount, Name FROM Test ORDER BY Amount DESC;
            )", /* assertSize */ true);

            NColumnShard::TTableUpdatesBuilder builder(NArrow::MakeArrowSchema({
                std::make_pair("Comment", TTypeInfo(NTypeIds::String)),
                std::make_pair("Amount", TTypeInfo(NTypeIds::Uint64)),
                std::make_pair("Name", TTypeInfo(NTypeIds::String))
            }));

            builder.AddRow().Add<std::string>("None").Add<ui64>(7200).Add<std::string>("Tony");
            builder.AddRow().Add<std::string>("None").Add<ui64>(3500).Add<std::string>("Anna");
            builder.AddRow().Add<std::string>("None").Add<ui64>(300).Add<std::string>("Paul");

            auto expected = builder.BuildArrow();
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
        }
    }

    Y_UNIT_TEST(ArrowFormat_Types_String) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ false);
        auto client = kikimr.GetQueryClient();

        {
            auto result = client.ExecuteQuery(R"(
                CREATE TABLE StringTypesTable (
                    Utf8Value Utf8,
                    JsonValue Json,
                    Utf8NotNullValue Utf8 NOT NULL,
                    JsonNotNullValue Json NOT NULL,
                    PRIMARY KEY (Utf8Value)
                );
            )", TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = client.ExecuteQuery(R"(
                INSERT INTO StringTypesTable (Utf8Value, JsonValue, Utf8NotNullValue, JsonNotNullValue) VALUES
                ("John", "[1]", "John", "[2]"),
                (NULL, "[]", "Maria", "[3]"),
                ("Leo", NULL, "Leo", "[4]"),
                ("Michael", "[5]", "Michael", "[6]");
            )", TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto batch = ExecuteAndCombineBatches(client, R"(
                SELECT Utf8Value, JsonValue, Utf8NotNullValue, JsonNotNullValue
                FROM StringTypesTable ORDER BY Utf8Value;
            )", /* assertSize */ true);

            NColumnShard::TTableUpdatesBuilder builder(NArrow::MakeArrowSchema({
                std::make_pair("Utf8Value", TTypeInfo(NTypeIds::Utf8)),
                std::make_pair("JsonValue", TTypeInfo(NTypeIds::Json)),
                std::make_pair("Utf8NotNullValue", TTypeInfo(NTypeIds::Utf8)),
                std::make_pair("JsonNotNullValue", TTypeInfo(NTypeIds::Json))
            }));

            builder.AddRow().AddNull().Add<std::string>("[]").Add<std::string>("Maria").Add<std::string>("[3]");
            builder.AddRow().Add<std::string>("John").Add<std::string>("[1]").Add<std::string>("John").Add<std::string>("[2]");
            builder.AddRow().Add<std::string>("Leo").AddNull().Add<std::string>("Leo").Add<std::string>("[4]");
            builder.AddRow().Add<std::string>("Michael").Add<std::string>("[5]").Add<std::string>("Michael").Add<std::string>("[6]");

            auto expected = builder.BuildArrow();
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
        }
    }

    Y_UNIT_TEST(ArrowFormat_Types_Binary) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ false);
        auto client = kikimr.GetQueryClient();

        {
            auto result = client.ExecuteQuery(R"(
                CREATE TABLE BinaryTypesTable (
                    StringValue String,
                    YsonValue Yson,
                    DyNumberValue DyNumber,
                    JsonDocumentValue JsonDocument,
                    StringNotNullValue String NOT NULL,
                    YsonNotNullValue Yson NOT NULL,
                    JsonDocumentNotNullValue JsonDocument NOT NULL,
                    DyNumberNotNullValue DyNumber NOT NULL,
                    PRIMARY KEY (StringValue)
                );
            )", TTxControl::NoTx())
                              .GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = client.ExecuteQuery(R"(
                INSERT INTO BinaryTypesTable (StringValue, YsonValue, DyNumberValue, JsonDocumentValue, StringNotNullValue, YsonNotNullValue, JsonDocumentNotNullValue, DyNumberNotNullValue) VALUES
                ("John", "[1]", DyNumber("1.0"), JsonDocument("[1]"), "Mark", "[2]", JsonDocument("[3]"), DyNumber("4.0")),
                (NULL, "[4]", NULL, NULL, "Maria", "[5]", JsonDocument("[6]"), DyNumber("7.0")),
                ("Mark", NULL, NULL, NULL, "Michael", "[7]", JsonDocument("[8]"), DyNumber("9.0")),
                ("Leo", "[10]", DyNumber("11.0"), JsonDocument("[12]"), "Maria", "[13]", JsonDocument("[14]"), DyNumber("15.0"));
            )", TTxControl::BeginTx().CommitTx())
                              .GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto batch = ExecuteAndCombineBatches(client, R"(
                SELECT StringValue, YsonValue, DyNumberValue, JsonDocumentValue, StringNotNullValue, YsonNotNullValue, JsonDocumentNotNullValue, DyNumberNotNullValue
                FROM BinaryTypesTable ORDER BY StringValue;
            )", /* assertSize */ true);

            NColumnShard::TTableUpdatesBuilder builder(NArrow::MakeArrowSchema({
                std::make_pair("StringValue", TTypeInfo(NTypeIds::String)),
                std::make_pair("YsonValue", TTypeInfo(NTypeIds::Yson)),
                std::make_pair("DyNumberValue", TTypeInfo(NTypeIds::DyNumber)),
                std::make_pair("JsonDocumentValue", TTypeInfo(NTypeIds::JsonDocument)),
                std::make_pair("StringNotNullValue", TTypeInfo(NTypeIds::String)),
                std::make_pair("YsonNotNullValue", TTypeInfo(NTypeIds::Yson)),
                std::make_pair("JsonDocumentNotNullValue", TTypeInfo(NTypeIds::JsonDocument)),
                std::make_pair("DyNumberNotNullValue", TTypeInfo(NTypeIds::DyNumber))
            }));

            builder.AddRow().AddNull().Add<std::string>("[4]").AddNull().AddNull().Add<std::string>("Maria").Add<std::string>("[5]").Add<std::string>("[6]").Add<std::string>("7.0");
            builder.AddRow().Add<std::string>("John").Add<std::string>("[1]").Add<std::string>("1.0").Add<std::string>("[1]").Add<std::string>("Mark").Add<std::string>("[2]").Add<std::string>("[3]").Add<std::string>("4.0");
            builder.AddRow().Add<std::string>("Leo").Add<std::string>("[10]").Add<std::string>("11.0").Add<std::string>("[12]").Add<std::string>("Maria").Add<std::string>("[13]").Add<std::string>("[14]").Add<std::string>("15.0");
            builder.AddRow().Add<std::string>("Mark").AddNull().AddNull().AddNull().Add<std::string>("Michael").Add<std::string>("[7]").Add<std::string>("[8]").Add<std::string>("9.0");

            auto expected = builder.BuildArrow();
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
        }
    }

    Y_UNIT_TEST(ArrowFormat_Compression_ZSTD) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ true);
        auto client = kikimr.GetQueryClient();

        std::shared_ptr<arrow::Schema> firstSchema;
        TString firstRecordBatch;

        std::shared_ptr<arrow::Schema> secondSchema;
        TString secondRecordBatch;

        {
            auto settings = TExecuteQuerySettings()
                .Format(TResultSet::EFormat::Arrow)
                .ArrowFormatSettings(TArrowFormatSettings()
                    .CompressionCodec(TArrowFormatSettings::TCompressionCodec()
                        .Type(TArrowFormatSettings::TCompressionCodec::EType::Zstd)));

            auto result = client.ExecuteQuery(R"(
                SELECT Comment, Amount, Name FROM Test ORDER BY Amount DESC;
            )", TTxControl::BeginTx().CommitTx(), settings).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto [schema, recordBatches] = result.GetResultSet(0).GetArrowResult();

            firstSchema = NArrow::DeserializeSchema(TString(schema));
            firstRecordBatch = std::move(recordBatches[0]);
        }
        {
            auto settings = TExecuteQuerySettings().Format(TResultSet::EFormat::Arrow);

            auto result = client.ExecuteQuery(R"(
                SELECT Comment, Amount, Name FROM Test ORDER BY Amount DESC;
            )", TTxControl::BeginTx().CommitTx(), settings).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto [schema, recordBatches] = result.GetResultSet(0).GetArrowResult();

            secondSchema = NArrow::DeserializeSchema(TString(schema));
            secondRecordBatch = std::move(recordBatches[0]);
        }

        UNIT_ASSERT_VALUES_EQUAL(firstSchema->ToString(), secondSchema->ToString());

        auto firstArrowBatch = NArrow::DeserializeBatch(firstRecordBatch, firstSchema);
        auto secondArrowBatch = NArrow::DeserializeBatch(secondRecordBatch, secondSchema);

        UNIT_ASSERT_C(firstArrowBatch->ValidateFull().ok(), "Batch validation failed");
        UNIT_ASSERT_C(secondArrowBatch->ValidateFull().ok(), "Batch validation failed");

        UNIT_ASSERT_VALUES_UNEQUAL(firstRecordBatch, secondRecordBatch);
        UNIT_ASSERT_VALUES_EQUAL(firstArrowBatch->ToString(), secondArrowBatch->ToString());

        UNIT_ASSERT_C(firstArrowBatch->num_rows() > 0, "Arrow batch must not be empty");
    }

    Y_UNIT_TEST(ArrowFormat_Compression_LZ4_FRAME) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ true);
        auto client = kikimr.GetQueryClient();

        std::shared_ptr<arrow::Schema> firstSchema;
        TString firstRecordBatch;

        std::shared_ptr<arrow::Schema> secondSchema;
        TString secondRecordBatch;

        {
            auto settings = TExecuteQuerySettings()
                .Format(TResultSet::EFormat::Arrow)
                .ArrowFormatSettings(TArrowFormatSettings()
                    .CompressionCodec(TArrowFormatSettings::TCompressionCodec()
                        .Type(TArrowFormatSettings::TCompressionCodec::EType::Lz4Frame)));

            auto result = client.ExecuteQuery(R"(
                SELECT Comment, Amount, Name FROM Test ORDER BY Amount DESC;
            )", TTxControl::BeginTx().CommitTx(), settings).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto [schema, recordBatches] = result.GetResultSet(0).GetArrowResult();

            firstSchema = NArrow::DeserializeSchema(TString(schema));
            firstRecordBatch = std::move(recordBatches[0]);
        }
        {
            auto settings = TExecuteQuerySettings().Format(TResultSet::EFormat::Arrow);

            auto result = client.ExecuteQuery(R"(
                SELECT Comment, Amount, Name FROM Test ORDER BY Amount DESC;
            )", TTxControl::BeginTx().CommitTx(), settings).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto [schema, recordBatches] = result.GetResultSet(0).GetArrowResult();

            secondSchema = NArrow::DeserializeSchema(TString(schema));
            secondRecordBatch = std::move(recordBatches[0]);
        }

        UNIT_ASSERT_VALUES_EQUAL(firstSchema->ToString(), secondSchema->ToString());

        auto firstArrowBatch = NArrow::DeserializeBatch(firstRecordBatch, firstSchema);
        auto secondArrowBatch = NArrow::DeserializeBatch(secondRecordBatch, secondSchema);

        UNIT_ASSERT_C(firstArrowBatch->ValidateFull().ok(), "Batch validation failed");
        UNIT_ASSERT_C(secondArrowBatch->ValidateFull().ok(), "Batch validation failed");

        UNIT_ASSERT_VALUES_UNEQUAL(firstRecordBatch, secondRecordBatch);
        UNIT_ASSERT_VALUES_EQUAL(firstArrowBatch->ToString(), secondArrowBatch->ToString());

        UNIT_ASSERT_C(firstArrowBatch->num_rows() > 0, "Arrow batch must not be empty");
    }
}

} // namespace NKikimr::NKqp

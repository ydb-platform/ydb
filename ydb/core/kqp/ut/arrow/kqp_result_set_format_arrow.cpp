#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/converter.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/library/formats/arrow/arrow_helpers.h>
#include <yql/essentials/types/binary_json/write.h>
#include <yql/essentials/types/dynumber/dynumber.h>

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

std::string SerializeToBinaryJsonString(const TStringBuf json) {
    const auto binaryJson = std::get<NBinaryJson::TBinaryJson>(NBinaryJson::SerializeToBinaryJson(json));
    const TStringBuf buffer(binaryJson.Data(), binaryJson.Size());
    return TString(buffer);
}

} // namespace

Y_UNIT_TEST_SUITE(KqpResultSetFormat) {
    Y_UNIT_TEST(DefaultFormat) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ true);
        auto client = kikimr.GetQueryClient();

        auto result = client.ExecuteQuery(R"(
            SELECT Comment, Amount, Name FROM Test ORDER BY Amount DESC;
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto resultSet = result.GetResultSet(0);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.Format(), TResultSet::EFormat::Value);

        CompareYson(R"([
            [["None"];[7200u];["Tony"]];
            [["None"];[3500u];["Anna"]];
            [["None"];[300u];["Paul"]]
        ])", FormatResultSetYson(resultSet));
    }

    Y_UNIT_TEST(ValueFormat_Simple) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ true);
        auto client = kikimr.GetQueryClient();

        auto settings = TExecuteQuerySettings().Format(TResultSet::EFormat::Value);

        auto result = client.ExecuteQuery(R"(
            SELECT Comment, Amount, Name FROM Test ORDER BY Amount DESC;
        )", TTxControl::BeginTx().CommitTx(), settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto resultSet = result.GetResultSet(0);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.Format(), TResultSet::EFormat::Value);

        CompareYson(R"([
            [["None"];[7200u];["Tony"]];
            [["None"];[3500u];["Anna"]];
            [["None"];[300u];["Paul"]]
        ])", FormatResultSetYson(resultSet));
    }

    Y_UNIT_TEST(ArrowFormat_Simple) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ true);
        auto client = kikimr.GetQueryClient();

        auto settings = TExecuteQuerySettings().Format(TResultSet::EFormat::Arrow);

        auto result = client.ExecuteQuery(R"(
            SELECT Comment, Amount, Name FROM Test ORDER BY Amount DESC;
        )", TTxControl::BeginTx().CommitTx(), settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto resultSet = result.GetResultSet(0);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.Format(), TResultSet::EFormat::Arrow);

        const auto& [schema, batches] = resultSet.GetArrowResult();

        std::shared_ptr<arrow::Schema> arrowSchema = NArrow::DeserializeSchema(TString(schema));
        std::shared_ptr<arrow::RecordBatch> arrowBatch = NArrow::DeserializeBatch(TString(batches[0]), arrowSchema);

        NColumnShard::TTableUpdatesBuilder builder(NArrow::MakeArrowSchema({
            std::make_pair("Comment", TTypeInfo(NTypeIds::String)),
            std::make_pair("Amount", TTypeInfo(NTypeIds::Uint64)),
            std::make_pair("Name", TTypeInfo(NTypeIds::String))
        }));

        builder.AddRow().Add<std::string>("None").Add<ui64>(7200).Add<std::string>("Tony");
        builder.AddRow().Add<std::string>("None").Add<ui64>(3500).Add<std::string>("Anna");
        builder.AddRow().Add<std::string>("None").Add<ui64>(300).Add<std::string>("Paul");

        auto expected = builder.BuildArrow();
        UNIT_ASSERT_VALUES_EQUAL(arrowBatch->ToString(), expected->ToString());
    }

    Y_UNIT_TEST_TWIN(ArrowFormat_AllTypes, isOlap) {
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

    Y_UNIT_TEST(ArrowFormat_Types_Arithmetic) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ false);
        auto client = kikimr.GetQueryClient();

        {
            auto result = client.ExecuteQuery(R"(
                CREATE TABLE ArithmeticTypesTable (
                    BoolValue Bool,
                    Int8Value Int8,
                    Uint8Value Uint8 NOT NULL,
                    Int16Value Int16,
                    Uint16Value Uint16 NOT NULL,
                    Int32Value Int32,
                    Uint32Value Uint32 NOT NULL,
                    Int64Value Int64,
                    Uint64Value Uint64 NOT NULL,
                    FloatValue Float,
                    DoubleValue Double NOT NULL,
                    PRIMARY KEY (BoolValue)
                );
            )", TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = client.ExecuteQuery(R"(
                INSERT INTO ArithmeticTypesTable (BoolValue, Int8Value, Uint8Value, Int16Value, Uint16Value, Int32Value, Uint32Value, Int64Value, Uint64Value, FloatValue, DoubleValue) VALUES
                (NULL, -1, 1, -2, 2, -3, 3, -4, 4, CAST(5.0 AS Float), 6.0),
                (true, -1, 1, -2, 2, -3, 3, -4, 4, CAST(5.0 AS Float), 6.0),
                (false, -1, 1, -2, 2, -3, 3, -4, 4, CAST(5.0 AS Float), 6.0);
            )", TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto batch = ExecuteAndCombineBatches(client, R"(
                    SELECT BoolValue, Int8Value, Uint8Value, Int16Value, Uint16Value, Int32Value, Uint32Value, Int64Value, Uint64Value, FloatValue, DoubleValue
                    FROM ArithmeticTypesTable ORDER BY BoolValue;
            )", /* assertSize */ true);

            NColumnShard::TTableUpdatesBuilder builder(NArrow::MakeArrowSchema({
                std::make_pair("BoolValue", TTypeInfo(NTypeIds::Bool)),
                std::make_pair("Int8Value", TTypeInfo(NTypeIds::Int8)),
                std::make_pair("Uint8Value", TTypeInfo(NTypeIds::Uint8)),
                std::make_pair("Int16Value", TTypeInfo(NTypeIds::Int16)),
                std::make_pair("Uint16Value", TTypeInfo(NTypeIds::Uint16)),
                std::make_pair("Int32Value", TTypeInfo(NTypeIds::Int32)),
                std::make_pair("Uint32Value", TTypeInfo(NTypeIds::Uint32)),
                std::make_pair("Int64Value", TTypeInfo(NTypeIds::Int64)),
                std::make_pair("Uint64Value", TTypeInfo(NTypeIds::Uint64)),
                std::make_pair("FloatValue", TTypeInfo(NTypeIds::Float)),
                std::make_pair("DoubleValue", TTypeInfo(NTypeIds::Double)),
                // std::make_pair("DecimalValue", TTypeInfo(NTypeIds::Decimal)) // ydb/core/scheme_types/scheme_type_info.h:32
            }));

            builder.AddRow().AddNull().Add<i8>(-1).Add<ui8>(1).Add<i16>(-2).Add<ui16>(2).Add<i32>(-3).Add<ui32>(3).Add<i64>(-4).Add<ui64>(4).Add<float>(5.0).Add<double>(6.0);
            builder.AddRow().Add<bool>(false).Add<i8>(-1).Add<ui8>(1).Add<i16>(-2).Add<ui16>(2).Add<i32>(-3).Add<ui32>(3).Add<i64>(-4).Add<ui64>(4).Add<float>(5.0).Add<double>(6.0);
            builder.AddRow().Add<bool>(true).Add<i8>(-1).Add<ui8>(1).Add<i16>(-2).Add<ui16>(2).Add<i32>(-3).Add<ui32>(3).Add<i64>(-4).Add<ui64>(4).Add<float>(5.0).Add<double>(6.0);

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
                ("John", "[1]", DyNumber("1.0"), JsonDocument("{\"a\": 1}"), "Mark", "[2]", JsonDocument("{\"b\": 2}"), DyNumber("4.0")),
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

            builder.AddRow().AddNull().Add("[4]").AddNull().AddNull().Add("Maria").Add("[5]").Add(SerializeToBinaryJsonString("[6]")).Add(NDyNumber::ParseDyNumberString("7.0")->c_str());
            builder.AddRow().Add("John").Add("[1]").Add(NDyNumber::ParseDyNumberString("1.0")->c_str()).Add(SerializeToBinaryJsonString("{\"a\": 1}")).Add("Mark").Add("[2]").Add(SerializeToBinaryJsonString("{\"b\": 2}")).Add(NDyNumber::ParseDyNumberString("4.0")->c_str());
            builder.AddRow().Add("Leo").Add("[10]").Add(NDyNumber::ParseDyNumberString("11.0")->c_str()).Add(SerializeToBinaryJsonString("[12]")).Add("Maria").Add("[13]").Add(SerializeToBinaryJsonString("[14]")).Add(NDyNumber::ParseDyNumberString("15.0")->c_str());
            builder.AddRow().Add("Mark").AddNull().AddNull().AddNull().Add("Michael").Add("[7]").Add(SerializeToBinaryJsonString("[8]")).Add(NDyNumber::ParseDyNumberString("9.0")->c_str());


            auto expected = builder.BuildArrow();
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
        }
    }

    Y_UNIT_TEST(ArrowFormat_Types_Time) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ false);
        auto client = kikimr.GetQueryClient();

        {
            auto result = client.ExecuteQuery(R"(
                CREATE TABLE TimeTypesTable (
                    DateValue Date,
                    DatetimeValue Datetime,
                    TimestampValue Timestamp,
                    IntervalValue Interval,
                    PRIMARY KEY (DateValue)
                );
            )", TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = client.ExecuteQuery(R"(
                INSERT INTO TimeTypesTable (DateValue, DatetimeValue, TimestampValue, IntervalValue) VALUES
                (Date("2001-01-01"), Datetime("2002-02-02T02:02:02Z"), Timestamp("2003-03-03T03:03:03Z"), Interval("P7D"));
            )", TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto batch = ExecuteAndCombineBatches(client, R"(
                SELECT DateValue, DatetimeValue, TimestampValue, IntervalValue
                FROM TimeTypesTable ORDER BY DateValue;
            )", /* assertSize */ true);

            NColumnShard::TTableUpdatesBuilder builder(NArrow::MakeArrowSchema({
                std::make_pair("DateValue", TTypeInfo(NTypeIds::Date)),
                std::make_pair("DatetimeValue", TTypeInfo(NTypeIds::Datetime)),
                std::make_pair("TimestampValue", TTypeInfo(NTypeIds::Timestamp)),
                std::make_pair("IntervalValue", TTypeInfo(NTypeIds::Interval))
            }));

            builder.AddRow().Add<ui16>(11323).Add<ui32>(1012615322).Add<ui64>(1046660583000000).Add<i64>(604800000000);

            auto expected = builder.BuildArrow();
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
        }
    }

    Y_UNIT_TEST(ArrowFormat_Compression_None) {
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
                        .Type(TArrowFormatSettings::TCompressionCodec::EType::None)));

            auto result = client.ExecuteQuery(R"(
                SELECT Comment, Amount, Name FROM Test ORDER BY Amount DESC;
            )", TTxControl::BeginTx().CommitTx(), settings).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto [schema, recordBatches] = result.GetResultSet(0).GetArrowResult();

            firstSchema = NArrow::DeserializeSchema(TString(schema));
            firstRecordBatch = std::move(recordBatches[0]);
        }
        {
            // Default compression is None
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

        UNIT_ASSERT_VALUES_EQUAL(firstRecordBatch, secondRecordBatch);
        UNIT_ASSERT_VALUES_EQUAL(firstArrowBatch->ToString(), secondArrowBatch->ToString());

        UNIT_ASSERT_C(firstArrowBatch->num_rows() > 0, "Arrow batch must not be empty");
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
                        .Type(TArrowFormatSettings::TCompressionCodec::EType::Zstd)
                        .Level(12)));

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

        {
            auto settings = TExecuteQuerySettings()
                .Format(TResultSet::EFormat::Arrow)
                .ArrowFormatSettings(TArrowFormatSettings()
                    .CompressionCodec(TArrowFormatSettings::TCompressionCodec()
                        .Type(TArrowFormatSettings::TCompressionCodec::EType::Lz4Frame)
                        .Level(12)));

            auto result = client.ExecuteQuery(R"(
                SELECT Comment, Amount, Name FROM Test ORDER BY Amount DESC;
            )", TTxControl::BeginTx().CommitTx(), settings).GetValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::INTERNAL_ERROR);
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Codec 'lz4' doesn't support setting a compression level");
        }
    }
}

} // namespace NKikimr::NKqp

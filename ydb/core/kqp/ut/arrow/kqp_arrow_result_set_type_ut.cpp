#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

TKikimrRunner CreateOlapKikimrRunner() {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);

    auto settings = TKikimrSettings().SetAppConfig(appConfig);
    return TKikimrRunner(settings);
}

std::shared_ptr<arrow::RecordBatch> DeserializeArrowResultSet(const TResultSet& resultSet) {
    UNIT_ASSERT_VALUES_EQUAL(resultSet.GetType(), NYdb::TResultSet::EType::Arrow);

    const TString& serializedSchema = resultSet.GetArrowSchema();
    const TString& serializedBatch = resultSet.GetArrowBatch();

    auto schema = NArrow::DeserializeSchema(serializedSchema);
    UNIT_ASSERT(schema);

    return NArrow::DeserializeBatch(serializedBatch, schema);
}

void FillAllTypesOltp(TQueryClient& client) {
    auto createResult = client.ExecuteQuery(R"(
        --!syntax_v1
        CREATE TABLE `/Root/OltpTable` (
            Key Uint64,
            BoolValue Bool,
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
        --!syntax_v1
        INSERT INTO `/Root/OltpTable` (Key, BoolValue, Int32Value, Uint32Value, Int64Value, Uint64Value, FloatValue, DoubleValue, StringValue, Utf8Value, DateValue, DatetimeValue, TimestampValue, IntervalValue, DecimalValue, JsonValue, YsonValue, JsonDocumentValue, DyNumberValue, Int32NotNullValue) VALUES
        (42, true, -1, 1, -2, 2, CAST(3.0 AS Float), 4.0, "five", Utf8("six"), Date("2007-07-07"), Datetime("2008-08-08T08:08:08Z"), Timestamp("2009-09-09T09:09:09.09Z"), Interval("P10D"), CAST("11.11" AS Decimal(22, 9)), "[12]", "[13]", JsonDocument("[14]"), DyNumber("15.15"), 123);
    )", TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(insertResult.IsSuccess(), insertResult.GetIssues().ToString());
}

void FillAllTypesOlap(TQueryClient& client) {
    auto createResult = client.ExecuteQuery(R"(
        --!syntax_v1
        CREATE TABLE `/Root/OlapTable` (
            Key Uint64 NOT NULL,
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
        --!syntax_v1
        INSERT INTO `/Root/OlapTable` (Key, Int32Value, Uint32Value, Int64Value, Uint64Value, FloatValue, DoubleValue, StringValue, Utf8Value, DateValue, DatetimeValue, TimestampValue, JsonValue, YsonValue, JsonDocumentValue) VALUES
        (42, -1, 1, -2, 2, CAST(3.0 AS Float), 4.0, "five", Utf8("six"), Date("2007-07-07"), Datetime("2008-08-08T08:08:08Z"), Timestamp("2009-09-09T09:09:09.09Z"), "[12]", "[13]", JsonDocument("[14]"));
    )", TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(insertResult.IsSuccess(), insertResult.GetIssues().ToString());
}

} // namespace

Y_UNIT_TEST_SUITE(KqpArrowResultSetType) {
    Y_UNIT_TEST(OltpAllTypes) {
        auto kikimr = CreateOlapKikimrRunner();
        auto client = kikimr.GetQueryClient();

        FillAllTypesOltp(client);

        auto messageResponse = client.ExecuteQuery(R"(
            --!syntax_v1
            SELECT * FROM `/Root/OltpTable`;
        )", TTxControl::BeginTx().CommitTx(), TExecuteQuerySettings().ResultSetType(TResultSet::EType::Message)).GetValueSync();
        UNIT_ASSERT_C(messageResponse.IsSuccess(), messageResponse.GetIssues().ToString());

        auto arrowResponse = client.ExecuteQuery(R"(
            --!syntax_v1
            SELECT * FROM `/Root/OltpTable`;
        )", TTxControl::BeginTx().CommitTx(), TExecuteQuerySettings().ResultSetType(TResultSet::EType::Arrow)).GetValueSync();
        UNIT_ASSERT_C(arrowResponse.IsSuccess(), arrowResponse.GetIssues().ToString());

        const auto& messageResultSet = messageResponse.GetResultSet(0);
        const auto& arrowResultSet = arrowResponse.GetResultSet(0);

        std::shared_ptr<arrow::RecordBatch> batch = DeserializeArrowResultSet(arrowResultSet);

        UNIT_ASSERT_VALUES_EQUAL(messageResultSet.RowsCount(), static_cast<size_t>(batch->num_rows()));
        UNIT_ASSERT_VALUES_EQUAL(messageResultSet.ColumnsCount(), static_cast<size_t>(batch->num_columns()));
    }

    Y_UNIT_TEST(OlapAllTypes) {
        auto kikimr = CreateOlapKikimrRunner();
        auto client = kikimr.GetQueryClient();

        FillAllTypesOlap(client);

        auto messageResponse = client.ExecuteQuery(R"(
            --!syntax_v1
            SELECT * FROM `/Root/OlapTable`;
        )", TTxControl::BeginTx().CommitTx(), TExecuteQuerySettings().ResultSetType(TResultSet::EType::Message)).GetValueSync();
        UNIT_ASSERT_C(messageResponse.IsSuccess(), messageResponse.GetIssues().ToString());

        auto arrowResponse = client.ExecuteQuery(R"(
            --!syntax_v1
            SELECT * FROM `/Root/OlapTable`;
        )", TTxControl::BeginTx().CommitTx(), TExecuteQuerySettings().ResultSetType(TResultSet::EType::Arrow)).GetValueSync();
        UNIT_ASSERT_C(arrowResponse.IsSuccess(), arrowResponse.GetIssues().ToString());

        const auto& messageResultSet = messageResponse.GetResultSet(0);
        const auto& arrowResultSet = arrowResponse.GetResultSet(0);

        std::shared_ptr<arrow::RecordBatch> batch = DeserializeArrowResultSet(arrowResultSet);

        UNIT_ASSERT_VALUES_EQUAL(messageResultSet.RowsCount(), static_cast<size_t>(batch->num_rows()));
        UNIT_ASSERT_VALUES_EQUAL(messageResultSet.ColumnsCount(), static_cast<size_t>(batch->num_columns()));
    }
}

} // namespace NKqp
} // namespace NKikimr

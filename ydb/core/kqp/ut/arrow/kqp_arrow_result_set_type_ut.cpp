#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

TKikimrRunner CreateKikimrRunner(bool isOlap = false) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableOlapSink(isOlap);

    auto settings = TKikimrSettings().SetAppConfig(appConfig);
    return TKikimrRunner(settings);
}

void CreateAllColumnsOltp(TQueryClient& client) {
    auto createResult = client.ExecuteQuery(R"(
        --!syntax_v1
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
        --!syntax_v1
        INSERT INTO `/Root/OltpTable` (Key, BoolValue, Int8Value, Uint8Value, Int16Value, Uint16Value, Int32Value, Uint32Value, Int64Value, Uint64Value, FloatValue, DoubleValue, StringValue, Utf8Value, DateValue, DatetimeValue, TimestampValue, IntervalValue, DecimalValue, JsonValue, YsonValue, JsonDocumentValue, DyNumberValue, Int32NotNullValue) VALUES
        (42, true, -1, 1, -2, 2, -3, 3, -4, 4, CAST(3.0 AS Float), 4.0, "five", Utf8("six"), Date("2007-07-07"), Datetime("2008-08-08T08:08:08Z"), Timestamp("2009-09-09T09:09:09.09Z"), Interval("P10D"), CAST("11.11" AS Decimal(22, 9)), "[12]", "[13]", JsonDocument("[14]"), DyNumber("15.15"), 123);
    )", TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(insertResult.IsSuccess(), insertResult.GetIssues().ToString());
}

void CreateAllColumnsOlap(TQueryClient& client) {
    auto createResult = client.ExecuteQuery(R"(
        --!syntax_v1
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
        --!syntax_v1
        INSERT INTO `/Root/OlapTable` (Key, Int8Value, Uint8Value, Int16Value, Uint16Value, Int32Value, Uint32Value, Int64Value, Uint64Value, FloatValue, DoubleValue, StringValue, Utf8Value, DateValue, DatetimeValue, TimestampValue, JsonValue, YsonValue, JsonDocumentValue) VALUES
        (42, -1, 1, -2, 2, -3, 3, -4, 4, CAST(3.0 AS Float), 4.0, "five", Utf8("six"), Date("2007-07-07"), Datetime("2008-08-08T08:08:08Z"), Timestamp("2009-09-09T09:09:09.09Z"), "[12]", "[13]", JsonDocument("[14]"));
    )", TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(insertResult.IsSuccess(), insertResult.GetIssues().ToString());
}

void CompareResultSets(const TResultSet& messageResultSet, const TResultSet& arrowResultSet) {
    UNIT_ASSERT_VALUES_EQUAL(messageResultSet.RowsCount(), arrowResultSet.RowsCount());
    UNIT_ASSERT_VALUES_EQUAL(messageResultSet.ColumnsCount(), arrowResultSet.ColumnsCount());

    std::shared_ptr<arrow::RecordBatch> batch = arrowResultSet.GetArrowBatch();

    UNIT_ASSERT_VALUES_EQUAL(messageResultSet.RowsCount(), static_cast<size_t>(batch->num_rows()));
    UNIT_ASSERT_VALUES_EQUAL(messageResultSet.ColumnsCount(), static_cast<size_t>(batch->num_columns()));
}

} // namespace

Y_UNIT_TEST_SUITE(KqpArrowResultSetType) {
    Y_UNIT_TEST_TWIN(AllTypes, isOlap) {
        auto kikimr = CreateKikimrRunner(isOlap);
        auto client = kikimr.GetQueryClient();

        if (isOlap) {
            CreateAllColumnsOlap(client);
        } else {
            CreateAllColumnsOltp(client);
        }

        const TString query = Sprintf(R"(
            --!syntax_v1
            SELECT * FROM `/Root/%s`;
        )", (isOlap) ? "OlapTable" : "OltpTable");

        auto messageResponse = client.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
            TExecuteQuerySettings().ResultSetType(TResultSet::EType::Message)).GetValueSync();

        auto arrowResponse = client.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
            TExecuteQuerySettings().ResultSetType(TResultSet::EType::Arrow)).GetValueSync();

        UNIT_ASSERT_C(messageResponse.IsSuccess(), messageResponse.GetIssues().ToString());
        UNIT_ASSERT_C(arrowResponse.IsSuccess(), arrowResponse.GetIssues().ToString());

        CompareResultSets(messageResponse.GetResultSet(0), arrowResponse.GetResultSet(0));
    }

    Y_UNIT_TEST(LargeTable) {
        auto kikimr = CreateKikimrRunner();
        auto client = kikimr.GetQueryClient();

        CreateLargeTable(kikimr, 10000, 4, 10, 5000, 10);

        const TString query = Sprintf(R"(
            --!syntax_v1
            SELECT * FROM `/Root/LargeTable`;
        )");

        auto messageResponse = client.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
            TExecuteQuerySettings().ResultSetType(TResultSet::EType::Message)).GetValueSync();

        auto arrowResponse = client.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
            TExecuteQuerySettings().ResultSetType(TResultSet::EType::Arrow)).GetValueSync();

        UNIT_ASSERT_C(messageResponse.IsSuccess(), messageResponse.GetIssues().ToString());
        UNIT_ASSERT_C(arrowResponse.IsSuccess(), arrowResponse.GetIssues().ToString());

        CompareResultSets(messageResponse.GetResultSet(0), arrowResponse.GetResultSet(0));
    }

    Y_UNIT_TEST(LargeLimitTable) {
        auto kikimr = CreateKikimrRunner();
        auto client = kikimr.GetQueryClient();

        CreateLargeTable(kikimr, 10000, 4, 10, 5000, 10);

        const TString query = Sprintf(R"(
            --!syntax_v1
            SELECT * FROM `/Root/LargeTable` LIMIT 70000;
        )");

        auto messageResponse = client.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
            TExecuteQuerySettings().ResultSetType(TResultSet::EType::Message)).GetValueSync();

        auto arrowResponse = client.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
            TExecuteQuerySettings().ResultSetType(TResultSet::EType::Arrow)).GetValueSync();

        UNIT_ASSERT_C(messageResponse.IsSuccess(), messageResponse.GetIssues().ToString());
        UNIT_ASSERT_C(arrowResponse.IsSuccess(), arrowResponse.GetIssues().ToString());

        CompareResultSets(messageResponse.GetResultSet(0), arrowResponse.GetResultSet(0));
    }

    Y_UNIT_TEST_TWIN(Returning, isOlap) {
        auto kikimr = CreateKikimrRunner(isOlap);
        auto client = kikimr.GetQueryClient();

        TString query;

        if (isOlap) {
            CreateAllColumnsOlap(client);
            query = R"(
                --!syntax_v1
                UPSERT INTO `/Root/OlapTable` (Key, Int8Value, Uint8Value, Int16Value, Uint16Value, Int32Value, Uint32Value, Int64Value, Uint64Value, FloatValue, DoubleValue, StringValue, Utf8Value, DateValue, DatetimeValue, TimestampValue, JsonValue, YsonValue, JsonDocumentValue) VALUES
                (43, -1, 1, -2, 2, -3, 3, -4, 4, CAST(3.0 AS Float), 4.0, "five", Utf8("six"), Date("2007-07-07"), Datetime("2008-08-08T08:08:08Z"), Timestamp("2009-09-09T09:09:09.09Z"), "[12]", "[13]", JsonDocument("[14]")),
                (44, -1, 1, -2, 2, -3, 3, -4, 4, CAST(3.0 AS Float), 4.0, "five", Utf8("six"), Date("2007-07-07"), Datetime("2008-08-08T08:08:08Z"), Timestamp("2009-09-09T09:09:09.09Z"), "[12]", "[13]", JsonDocument("[14]")),
                (45, -1, 1, -2, 2, -3, 3, -4, 4, CAST(3.0 AS Float), 4.0, "five", Utf8("six"), Date("2007-07-07"), Datetime("2008-08-08T08:08:08Z"), Timestamp("2009-09-09T09:09:09.09Z"), "[12]", "[13]", JsonDocument("[14]"))
                RETURNING *;
            )";
        } else {
            CreateAllColumnsOltp(client);
            query = R"(
                --!syntax_v1
                UPSERT INTO `/Root/OltpTable` (Key, BoolValue, Int8Value, Uint8Value, Int16Value, Uint16Value, Int32Value, Uint32Value, Int64Value, Uint64Value, FloatValue, DoubleValue, StringValue, Utf8Value, DateValue, DatetimeValue, TimestampValue, IntervalValue, DecimalValue, JsonValue, YsonValue, JsonDocumentValue, DyNumberValue, Int32NotNullValue) VALUES
                (43, true, -1, 1, -2, 2, -3, 3, -4, 4, CAST(3.0 AS Float), 4.0, "five", Utf8("six"), Date("2007-07-07"), Datetime("2008-08-08T08:08:08Z"), Timestamp("2009-09-09T09:09:09.09Z"), Interval("P10D"), CAST("11.11" AS Decimal(22, 9)), "[12]", "[13]", JsonDocument("[14]"), DyNumber("15.15"), 123),
                (44, true, -1, 1, -2, 2, -3, 3, -4, 4, CAST(3.0 AS Float), 4.0, "five", Utf8("six"), Date("2007-07-07"), Datetime("2008-08-08T08:08:08Z"), Timestamp("2009-09-09T09:09:09.09Z"), Interval("P10D"), CAST("11.11" AS Decimal(22, 9)), "[12]", "[13]", JsonDocument("[14]"), DyNumber("15.15"), 123),
                (45, true, -1, 1, -2, 2, -3, 3, -4, 4, CAST(3.0 AS Float), 4.0, "five", Utf8("six"), Date("2007-07-07"), Datetime("2008-08-08T08:08:08Z"), Timestamp("2009-09-09T09:09:09.09Z"), Interval("P10D"), CAST("11.11" AS Decimal(22, 9)), "[12]", "[13]", JsonDocument("[14]"), DyNumber("15.15"), 123)
                RETURNING *;
            )";
        }

        auto messageResponse = client.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
            TExecuteQuerySettings().ResultSetType(TResultSet::EType::Message)).GetValueSync();

        auto arrowResponse = client.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
            TExecuteQuerySettings().ResultSetType(TResultSet::EType::Arrow)).GetValueSync();

        UNIT_ASSERT_C(messageResponse.IsSuccess(), messageResponse.GetIssues().ToString());
        UNIT_ASSERT_C(arrowResponse.IsSuccess(), arrowResponse.GetIssues().ToString());

        CompareResultSets(messageResponse.GetResultSet(0), arrowResponse.GetResultSet(0));
    }
}

} // namespace NKqp
} // namespace NKikimr

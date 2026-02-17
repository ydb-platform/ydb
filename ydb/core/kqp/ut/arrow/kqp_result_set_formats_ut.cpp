#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/converter.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>

#include <ydb/library/formats/arrow/arrow_helpers.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/arrow/accessor.h>

#include <yql/essentials/types/binary_json/write.h>
#include <yql/essentials/types/dynumber/dynumber.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;
using TTypeInfo = NScheme::TTypeInfo;
namespace NTypeIds = NScheme::NTypeIds;

namespace {

TKikimrRunner CreateKikimrRunner(bool withSampleTables, ui64 channelBufferSize = 8_MB) {
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableArrowResultSetFormat(true);

    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
    appConfig.MutableTableServiceConfig()->MutableResourceManager()->SetChannelBufferSize(channelBufferSize);
    appConfig.MutableTableServiceConfig()->MutableResourceManager()->SetMinChannelBufferSize(std::min(2_KB, channelBufferSize));
    appConfig.MutableTableServiceConfig()->MutableResourceManager()->SetChannelChunkSizeLimit(channelBufferSize);

    auto settings = TKikimrSettings(appConfig).SetFeatureFlags(featureFlags).SetWithSampleTables(withSampleTables);
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
            UuidValue Uuid,
            Int32NotNullValue Int32 NOT NULL,
            PRIMARY KEY (Key)
        );
    )", TTxControl::NoTx()).GetValueSync();
    UNIT_ASSERT_C(createResult.IsSuccess(), createResult.GetIssues().ToString());

    auto insertResult = client.ExecuteQuery(R"(
        INSERT INTO `/Root/RowTable` (Key, BoolValue, Int8Value, Uint8Value, Int16Value, Uint16Value, Int32Value, Uint32Value, Int64Value, Uint64Value, FloatValue, DoubleValue, StringValue, Utf8Value, DateValue, DatetimeValue, TimestampValue, IntervalValue, DecimalValue, JsonValue, YsonValue, JsonDocumentValue, DyNumberValue, UuidValue, Int32NotNullValue) VALUES
        (42, true, -1, 1, -2, 2, -3, 3, -4, 4, CAST(3.0 AS Float), 4.0, "five", Utf8("six"), Date("2007-07-07"), Datetime("2008-08-08T08:08:08Z"), Timestamp("2009-09-09T09:09:09.09Z"), Interval("P10D"), CAST("11.11" AS Decimal(22, 9)), "[12]", "[13]", JsonDocument("[14]"), DyNumber("15.15"), Uuid("5b99a330-04ef-4f1a-9b64-ba6d5f44eafe"), 123);
    )", TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(insertResult.IsSuccess(), insertResult.GetIssues().ToString());
}

void CreateAllTypesColumnTable(TQueryClient& client) {
    auto createResult = client.ExecuteQuery(R"(
        CREATE TABLE `/Root/ColumnTable` (
            Key Uint64 NOT NULL,
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
        INSERT INTO `/Root/ColumnTable` (Key, BoolValue, Int8Value, Uint8Value, Int16Value, Uint16Value, Int32Value, Uint32Value, Int64Value, Uint64Value, FloatValue, DoubleValue, StringValue, Utf8Value, DateValue, DatetimeValue, TimestampValue, JsonValue, YsonValue, JsonDocumentValue) VALUES
        (42, true, -1, 1, -2, 2, -3, 3, -4, 4, CAST(3.0 AS Float), 4.0, "five", Utf8("six"), Date("2007-07-07"), Datetime("2008-08-08T08:08:08Z"), Timestamp("2009-09-09T09:09:09.09Z"), "[12]", "[13]", JsonDocument("[14]"));
    )", TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(insertResult.IsSuccess(), insertResult.GetIssues().ToString());
}

void AssertArrowValueResultsSize(const std::vector<TResultSet>& arrowResultSets, const std::vector<TResultSet>& valueResultSets) {
    UNIT_ASSERT_VALUES_EQUAL_C(arrowResultSets.size(), valueResultSets.size(), "Result sets count mismatch");

    for (size_t i = 0; i < arrowResultSets.size(); ++i) {
        const auto& arrowResultSet = arrowResultSets[i];
        const auto& valueResultSet = valueResultSets[i];

        UNIT_ASSERT_VALUES_EQUAL_C(TArrowAccessor::Format(arrowResultSet), TResultSet::EFormat::Arrow, "Result set format mismatch");
        UNIT_ASSERT_VALUES_EQUAL_C(TArrowAccessor::Format(valueResultSet), TResultSet::EFormat::Value, "Result set format mismatch");

        UNIT_ASSERT_VALUES_EQUAL_C(arrowResultSet.RowsCount(), 0, "Rows must be empty for Arrow format of the result set");

        size_t arrowRowsCount = 0;

        const auto& schema = TArrowAccessor::GetArrowSchema(arrowResultSet);
        const auto& batches = TArrowAccessor::GetArrowBatches(arrowResultSet);

        UNIT_ASSERT_C(!schema.empty(), "Schema must not be empty");

        std::shared_ptr<arrow::Schema> arrowSchema = NArrow::DeserializeSchema(TString(schema));

        for (const auto& batch : batches) {
            auto arrowBatch = NArrow::DeserializeBatch(TString(batch), arrowSchema);
            UNIT_ASSERT_C(arrowBatch, "Batch must be deserialized");
            UNIT_ASSERT_C(arrowBatch->ValidateFull().ok(), "Batch validation failed");

            arrowRowsCount += arrowBatch->num_rows();

            UNIT_ASSERT_VALUES_EQUAL_C(arrowBatch->num_columns(), valueResultSet.ColumnsCount(), "Columns count mismatch");
        }

        UNIT_ASSERT_VALUES_EQUAL_C(arrowRowsCount, valueResultSet.RowsCount(), "Rows count mismatch");
    }
}

std::vector<std::shared_ptr<arrow::RecordBatch>> ExecuteAndCombineBatches(TQueryClient& client, const TString& query, bool assertSize = false, ui64 minBatchesCount = 1, TParams params = TParamsBuilder().Build()) {
    auto arrowSettings = TExecuteQuerySettings().Format(TResultSet::EFormat::Arrow);
    auto arrowResponse = client.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(), params, arrowSettings).GetValueSync();
    UNIT_ASSERT_C(arrowResponse.IsSuccess(), arrowResponse.GetIssues().ToString());

    if (assertSize) {
        auto valueSettings = TExecuteQuerySettings().Format(TResultSet::EFormat::Value);
        auto valueResponse = client.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(), params, valueSettings).GetValueSync();
        UNIT_ASSERT_C(valueResponse.IsSuccess(), valueResponse.GetIssues().ToString());
        AssertArrowValueResultsSize(arrowResponse.GetResultSets(), valueResponse.GetResultSets());
    }

    std::vector<std::shared_ptr<arrow::RecordBatch>> resultBatches;

    for (const auto& resultSet : arrowResponse.GetResultSets()) {
        const auto& schema = TArrowAccessor::GetArrowSchema(resultSet);
        const auto& batches = TArrowAccessor::GetArrowBatches(resultSet);

        UNIT_ASSERT_C(!schema.empty(), "Schema must not be empty");
        UNIT_ASSERT_GE_C(batches.size(), minBatchesCount, "Batches count must be greater than or equal to " + ToString(minBatchesCount));

        std::vector<std::shared_ptr<arrow::RecordBatch>> arrowBatches;
        auto arrowSchema = NArrow::DeserializeSchema(TString(schema));

        for (const auto& batch : batches) {
            auto arrowBatch = NArrow::DeserializeBatch(TString(batch), arrowSchema);
            UNIT_ASSERT_C(arrowBatch, "Batch must be deserialized");
            UNIT_ASSERT_C(arrowBatch->ValidateFull().ok(), "Batch validation failed");

            arrowBatches.push_back(std::move(arrowBatch));
        }

        auto resultBatch = NArrow::CombineBatches(arrowBatches);
        UNIT_ASSERT_C(resultBatch->ValidateFull().ok(), "Batch combine validation failed");

        resultBatches.push_back(std::move(resultBatch));
    }

    return resultBatches;
}

void CompareCompressedAndDefaultBatches(TQueryClient& client, std::optional<TArrowFormatSettings::TCompressionCodec> codec, bool assertEqual = false) {
    std::shared_ptr<arrow::Schema> schemaCompressedBatch;
    TString compressedBatch;

    std::shared_ptr<arrow::Schema> schemaDefaultBatch;
    TString defaultBatch;

    {
        auto settings = TExecuteQuerySettings()
            .Format(TResultSet::EFormat::Arrow)
            .ArrowFormatSettings(TArrowFormatSettings()
                 .CompressionCodec(std::move(codec)));

        auto result = client.ExecuteQuery(R"(
            SELECT Comment, Amount, Name FROM Test ORDER BY Amount DESC;
        )", TTxControl::BeginTx().CommitTx(), settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        const auto& schema = TArrowAccessor::GetArrowSchema(result.GetResultSet(0));
        const auto& batches = TArrowAccessor::GetArrowBatches(result.GetResultSet(0));

        UNIT_ASSERT_C(!batches.empty(), "Batches must not be empty");

        schemaCompressedBatch = NArrow::DeserializeSchema(TString(schema));
        compressedBatch = std::move(batches[0]);
    }
    {
        auto settings = TExecuteQuerySettings().Format(TResultSet::EFormat::Arrow);

        auto result = client.ExecuteQuery(R"(
            SELECT Comment, Amount, Name FROM Test ORDER BY Amount DESC;
        )", TTxControl::BeginTx().CommitTx(), settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        const auto& schema = TArrowAccessor::GetArrowSchema(result.GetResultSet(0));
        const auto& batches = TArrowAccessor::GetArrowBatches(result.GetResultSet(0));

        UNIT_ASSERT_C(!batches.empty(), "Batches must not be empty");

        schemaDefaultBatch = NArrow::DeserializeSchema(TString(schema));
        defaultBatch = std::move(batches[0]);
    }

    UNIT_ASSERT_VALUES_EQUAL(schemaCompressedBatch->ToString(), schemaDefaultBatch->ToString());

    // TODO [ditimizhev@]: Assert arrow::Codec compression types instead of strings
    if (assertEqual) {
        UNIT_ASSERT_VALUES_EQUAL(compressedBatch, defaultBatch);
    } else {
        UNIT_ASSERT_VALUES_UNEQUAL(compressedBatch, defaultBatch);
    }

    auto firstArrowBatch = NArrow::DeserializeBatch(compressedBatch, schemaCompressedBatch);
    auto secondArrowBatch = NArrow::DeserializeBatch(defaultBatch, schemaDefaultBatch);

    UNIT_ASSERT_C(firstArrowBatch, "First arrow batch must be deserialized");
    UNIT_ASSERT_C(secondArrowBatch, "Second arrow batch must be deserialized");

    UNIT_ASSERT_C(firstArrowBatch->num_rows() > 0, "Arrow batch must not be empty");

    UNIT_ASSERT_C(firstArrowBatch->ValidateFull().ok(), "Batch validation failed");
    UNIT_ASSERT_C(secondArrowBatch->ValidateFull().ok(), "Batch validation failed");

    UNIT_ASSERT_VALUES_EQUAL(firstArrowBatch->ToString(), secondArrowBatch->ToString());
}

void ValidateOptionalColumn(const std::shared_ptr<arrow::Array>& array, int depth, bool isVariant) {
    if (depth == 0 && isVariant) {
        UNIT_ASSERT_C(array->type()->id() == arrow::Type::DENSE_UNION, "Column type must be arrow::Type::DENSE_UNION");
        return;
    }

    if (depth == 1 && !isVariant) {
        return;
    }

    UNIT_ASSERT_C(array->type()->id() == arrow::Type::STRUCT, "Column type must be arrow::Type::STRUCT");

    auto structArray = static_pointer_cast<arrow::StructArray>(array);
    UNIT_ASSERT_C(structArray->num_fields() == 1, "Struct array must have 1 field");

    auto innerArray = structArray->field(0);
    ValidateOptionalColumn(innerArray, depth - 1, isVariant);
}

} // namespace

Y_UNIT_TEST_SUITE(KqpResultSetFormats) {
    /**
     * By default, unspecified format is Value for compatibility with previous versions.
     */
    Y_UNIT_TEST(DefaultFormat) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ true);
        auto client = kikimr.GetQueryClient();

        auto result = client.ExecuteQuery(R"(
            SELECT Comment, Amount, Name FROM Test ORDER BY Amount DESC;
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto resultSet = result.GetResultSet(0);
        UNIT_ASSERT_VALUES_EQUAL(TArrowAccessor::Format(resultSet), TResultSet::EFormat::Value);

        CompareYson(R"([
            [["None"];[7200u];["Tony"]];
            [["None"];[3500u];["Anna"]];
            [["None"];[300u];["Paul"]]
        ])", FormatResultSetYson(resultSet));
    }

    /**
     * Set Value format explicitly in TExecuteQuerySettings.
     */
    Y_UNIT_TEST(ValueFormat_Simple) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ true);
        auto client = kikimr.GetQueryClient();

        auto settings = TExecuteQuerySettings().Format(TResultSet::EFormat::Value);

        auto result = client.ExecuteQuery(R"(
            SELECT Comment, Amount, Name FROM Test ORDER BY Amount DESC;
        )", TTxControl::BeginTx().CommitTx(), settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto resultSet = result.GetResultSet(0);
        UNIT_ASSERT_VALUES_EQUAL(TArrowAccessor::Format(resultSet), TResultSet::EFormat::Value);

        CompareYson(R"([
            [["None"];[7200u];["Tony"]];
            [["None"];[3500u];["Anna"]];
            [["None"];[300u];["Paul"]]
        ])", FormatResultSetYson(resultSet));
    }

    /**
     * Small channel buffer size, rows from many ExecuteQueryResponePart parts are filled into a single ResultSet.
     */
    Y_UNIT_TEST(ValueFormat_SmallChannelBufferSize) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ false, 1_KB);
        auto client = kikimr.GetQueryClient();

        CreateLargeTable(kikimr, 100, 2, 2, 10, 2);

        auto settings = TExecuteQuerySettings().Format(TResultSet::EFormat::Value);

        auto result = client.ExecuteQuery(R"(
            SELECT * FROM LargeTable;
        )", TTxControl::BeginTx().CommitTx(), settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto resultSet = result.GetResultSet(0);
        UNIT_ASSERT_VALUES_EQUAL(TArrowAccessor::Format(resultSet), TResultSet::EFormat::Value);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 200);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 4);
    }

    /**
     * By default, SchemaInclusionMode is ALWAYS for Value format.
     */
    Y_UNIT_TEST(ValueFormat_SchemaInclusionMode_Unspecified) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ false, 1_KB);
        auto client = kikimr.GetQueryClient();

        CreateLargeTable(kikimr, 100, 2, 2, 10, 2);

        auto settings = TExecuteQuerySettings().Format(TResultSet::EFormat::Value);

        auto it = client.StreamExecuteQuery(R"(
            SELECT * FROM LargeTable;
        )", TTxControl::BeginTx().CommitTx(), settings).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        size_t count = 0;
        for (;;) {
            auto part = it.ReadNext().GetValueSync();
            if (!part.IsSuccess()) {
                UNIT_ASSERT_C(part.EOS(), part.GetIssues().ToString());
                break;
            }

            if (part.HasResultSet()) {
                auto resultSet = part.ExtractResultSet();
                UNIT_ASSERT_VALUES_EQUAL(TArrowAccessor::Format(resultSet), TResultSet::EFormat::Value);
                UNIT_ASSERT_VALUES_UNEQUAL(resultSet.ColumnsCount(), 0);

                ++count;
            }
        }

        UNIT_ASSERT_GT_C(count, 1, "Expected at least 2 result sets");
    }

    /**
     * Set SchemaInclusionMode ALWAYS for Value format explicitly in TExecuteQuerySettings.
     */
    Y_UNIT_TEST(ValueFormat_SchemaInclusionMode_Always) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ false, 1_KB);
        auto client = kikimr.GetQueryClient();

        CreateLargeTable(kikimr, 100, 2, 2, 10, 2);

        auto settings = TExecuteQuerySettings().Format(TResultSet::EFormat::Value).SchemaInclusionMode(ESchemaInclusionMode::Always);

        auto it = client.StreamExecuteQuery(R"(
            SELECT * FROM LargeTable;
        )", TTxControl::BeginTx().CommitTx(), settings).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        size_t count = 0;
        for (;;) {
            auto part = it.ReadNext().GetValueSync();
            if (!part.IsSuccess()) {
                UNIT_ASSERT_C(part.EOS(), part.GetIssues().ToString());
                break;
            }

            if (part.HasResultSet()) {
                auto resultSet = part.ExtractResultSet();
                UNIT_ASSERT_VALUES_EQUAL(TArrowAccessor::Format(resultSet), TResultSet::EFormat::Value);
                UNIT_ASSERT_VALUES_UNEQUAL(resultSet.ColumnsCount(), 0);

                ++count;
            }
        }

        UNIT_ASSERT_GT_C(count, 1, "Expected at least 2 result sets");
    }

    /**
     * Set SchemaInclusionMode FIRST_ONLY for Value format explicitly in TExecuteQuerySettings.
     */
    Y_UNIT_TEST(ValueFormat_SchemaInclusionMode_FirstOnly) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ false, 1_KB);
        auto client = kikimr.GetQueryClient();

        CreateLargeTable(kikimr, 100, 2, 2, 10, 2);

        auto settings = TExecuteQuerySettings().Format(TResultSet::EFormat::Value).SchemaInclusionMode(ESchemaInclusionMode::FirstOnly);

        auto it = client.StreamExecuteQuery(R"(
            SELECT * FROM LargeTable;
        )", TTxControl::BeginTx().CommitTx(), settings).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        size_t count = 0;
        for (;;) {
            auto part = it.ReadNext().GetValueSync();
            if (!part.IsSuccess()) {
                UNIT_ASSERT_C(part.EOS(), part.GetIssues().ToString());
                break;
            }

            if (part.HasResultSet()) {
                auto resultSet = part.ExtractResultSet();
                UNIT_ASSERT_VALUES_EQUAL(TArrowAccessor::Format(resultSet), TResultSet::EFormat::Value);

                if (count == 0) {
                    UNIT_ASSERT_VALUES_UNEQUAL(resultSet.ColumnsCount(), 0);
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 0);
                }

                ++count;
            }
        }

        UNIT_ASSERT_GT_C(count, 1, "Expected at least 2 result sets");
    }

    /**
     * For Value format, FirstOnly schema inclusion mode is supported for multistatement queries.
     */
    Y_UNIT_TEST(ValueFormat_SchemaInclusionMode_FirstOnly_Multistatement) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ false, 1_KB);
        auto client = kikimr.GetQueryClient();

        CreateLargeTable(kikimr, 200, 2, 2, 10, 2);

        auto settings = TExecuteQuerySettings().Format(TResultSet::EFormat::Value).SchemaInclusionMode(ESchemaInclusionMode::FirstOnly);

        auto it = client.StreamExecuteQuery(R"(
            SELECT * FROM LargeTable;
            SELECT Key, Data FROM LargeTable;
        )", TTxControl::BeginTx().CommitTx(), settings).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        std::unordered_map<size_t, ui64> counts;

        for (;;) {
            auto part = it.ReadNext().GetValueSync();
            if (!part.IsSuccess()) {
                UNIT_ASSERT_C(part.EOS(), part.GetIssues().ToString());
                break;
            }

            if (part.HasResultSet()) {
                auto resultSet = part.ExtractResultSet();
                UNIT_ASSERT_VALUES_EQUAL(TArrowAccessor::Format(resultSet), TResultSet::EFormat::Value);

                auto idx = part.GetResultSetIndex();

                if (counts.find(idx) == counts.end()) {
                    UNIT_ASSERT_VALUES_UNEQUAL(resultSet.ColumnsCount(), 0);
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 0);
                }

                ++counts[idx];
            }
        }

        UNIT_ASSERT_C(counts.size() == 2, "Expected 2 result set indexes");

        for (const auto& [idx, count] : counts) {
            UNIT_ASSERT_GT_C(count, 1, "Expected at least 2 result sets for statement with ResultSetIndex = " << idx);
        }
    }

    /**
     * Set Arrow format explicitly in TExecuteQuerySettings.
     */
    Y_UNIT_TEST(ArrowFormat_Simple) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ true);
        auto client = kikimr.GetQueryClient();

        auto settings = TExecuteQuerySettings().Format(TResultSet::EFormat::Arrow);

        auto result = client.ExecuteQuery(R"(
            SELECT Comment, Amount, Name FROM Test ORDER BY Amount DESC;
        )", TTxControl::BeginTx().CommitTx(), settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto resultSet = result.GetResultSet(0);
        UNIT_ASSERT_VALUES_EQUAL(TArrowAccessor::Format(resultSet), TResultSet::EFormat::Arrow);

        const auto& schema = TArrowAccessor::GetArrowSchema(resultSet);
        const auto& batches = TArrowAccessor::GetArrowBatches(resultSet);

        UNIT_ASSERT_C(!batches.empty(), "Batches must not be empty");

        auto arrowSchema = NArrow::DeserializeSchema(TString(schema));
        auto arrowBatch = NArrow::DeserializeBatch(TString(batches[0]), arrowSchema);

        UNIT_ASSERT_C(arrowSchema, "Schema must be deserialized");
        UNIT_ASSERT_C(arrowBatch, "Batch must be deserialized");
        UNIT_ASSERT_C(arrowBatch->ValidateFull().ok(), "Batch validation failed");

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

    Y_UNIT_TEST(ArrowFormat_EmptyBatch) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ true);
        auto client = kikimr.GetQueryClient();

        auto settings = TExecuteQuerySettings().Format(TResultSet::EFormat::Arrow);

        auto result = client.ExecuteQuery(R"(
            SELECT Comment, Amount, Name FROM Test WHERE Amount >= 999999;
        )", TTxControl::BeginTx().CommitTx(), settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto resultSet = result.GetResultSet(0);
        UNIT_ASSERT_VALUES_EQUAL(TArrowAccessor::Format(resultSet), TResultSet::EFormat::Arrow);

        const auto& schema = TArrowAccessor::GetArrowSchema(resultSet);
        const auto& batches = TArrowAccessor::GetArrowBatches(resultSet);

        UNIT_ASSERT_C(!batches.empty(), "Expected at least one empty batch");

        auto arrowSchema = NArrow::DeserializeSchema(TString(schema));
        auto arrowBatch = NArrow::DeserializeBatch(TString(batches[0]), arrowSchema);

        UNIT_ASSERT_C(arrowSchema, "Schema must be deserialized");
        UNIT_ASSERT_C(arrowBatch, "Batch must be deserialized");
        UNIT_ASSERT_C(arrowBatch->ValidateFull().ok(), "Batch validation failed");

        NColumnShard::TTableUpdatesBuilder builder(NArrow::MakeArrowSchema({
            std::make_pair("Comment", TTypeInfo(NTypeIds::String)),
            std::make_pair("Amount", TTypeInfo(NTypeIds::Uint64)),
            std::make_pair("Name", TTypeInfo(NTypeIds::String))
        }));

        UNIT_ASSERT_C(arrowBatch->num_rows() == 0, "Batch must have 0 rows");

        auto expected = builder.BuildArrow();
        UNIT_ASSERT_VALUES_EQUAL(arrowBatch->ToString(), expected->ToString());
    }

    /**
     * Arrow format is supported for all types of columns.
     */
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

    /**
     * Arrow format is supported for large batches.
     */
    Y_UNIT_TEST(ArrowFormat_LargeTable) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ false);
        auto client = kikimr.GetQueryClient();

        CreateLargeTable(kikimr, 10000, 4, 10, 5000, 10);

        const TString query = Sprintf(R"(
            SELECT * FROM `/Root/LargeTable`;
        )");

        Y_UNUSED(ExecuteAndCombineBatches(client, query, /* assertSize */ true));
    }

    /**
     * Arrow format is supported for large batches with LIMIT.
     */
    Y_UNIT_TEST(ArrowFormat_LargeTable_Limit) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ false);
        auto client = kikimr.GetQueryClient();

        CreateLargeTable(kikimr, 10000, 4, 10, 5000, 10);

        const TString query = Sprintf(R"(
            SELECT * FROM `/Root/LargeTable` LIMIT 70000;
        )");

        Y_UNUSED(ExecuteAndCombineBatches(client, query, /* assertSize */ true));
    }

    /**
     * Arrow format is supported for returning.
     */
    Y_UNIT_TEST_TWIN(ArrowFormat_Returning, isOlap) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ false);
        auto client = kikimr.GetQueryClient();

        TString query;

        if (isOlap) {
            CreateAllTypesColumnTable(client);
            query = R"(
                UPSERT INTO `/Root/ColumnTable` (Key, BoolValue, Int8Value, Uint8Value, Int16Value, Uint16Value, Int32Value, Uint32Value, Int64Value, Uint64Value, FloatValue, DoubleValue, StringValue, Utf8Value, DateValue, DatetimeValue, TimestampValue, JsonValue, YsonValue, JsonDocumentValue) VALUES
                (43, false, -1, 1, -2, 2, -3, 3, -4, 4, CAST(3.0 AS Float), 4.0, "five", Utf8("six"), Date("2007-07-07"), Datetime("2008-08-08T08:08:08Z"), Timestamp("2009-09-09T09:09:09.09Z"), "[12]", "[13]", JsonDocument("[14]")),
                (44, true, -1, 1, -2, 2, -3, 3, -4, 4, CAST(3.0 AS Float), 4.0, "five", Utf8("six"), Date("2007-07-07"), Datetime("2008-08-08T08:08:08Z"), Timestamp("2009-09-09T09:09:09.09Z"), "[12]", "[13]", JsonDocument("[14]")),
                (45, false, -1, 1, -2, 2, -3, 3, -4, 4, CAST(3.0 AS Float), 4.0, "five", Utf8("six"), Date("2007-07-07"), Datetime("2008-08-08T08:08:08Z"), Timestamp("2009-09-09T09:09:09.09Z"), "[12]", "[13]", JsonDocument("[14]"))
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

    /**
     * Check different orders of columns in SELECT with Arrow format.
     */
    Y_UNIT_TEST(ArrowFormat_ColumnOrder) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ true);
        auto client = kikimr.GetQueryClient();

        {
            auto batches = ExecuteAndCombineBatches(client, R"(
                SELECT Name, Amount FROM Test WHERE Group = 2;
            )", /* assertSize */ true);

            UNIT_ASSERT_C(!batches.empty(), "Batches must not be empty");

            NColumnShard::TTableUpdatesBuilder builder(NArrow::MakeArrowSchema({
                std::make_pair("Name", TTypeInfo(NTypeIds::String)),
                std::make_pair("Amount", TTypeInfo(NTypeIds::Uint64))
            }));

            builder.AddRow().Add<std::string>("Tony").Add<ui64>(7200);

            auto expected = builder.BuildArrow();
            UNIT_ASSERT_VALUES_EQUAL(batches.front()->ToString(), expected->ToString());
        }
        {
            auto batches = ExecuteAndCombineBatches(client, R"(
                SELECT Amount, Name FROM Test WHERE Group = 2;
            )", /* assertSize */ true);

            UNIT_ASSERT_C(!batches.empty(), "Batches must not be empty");

            NColumnShard::TTableUpdatesBuilder builder(NArrow::MakeArrowSchema({
                std::make_pair("Amount", TTypeInfo(NTypeIds::Uint64)),
                std::make_pair("Name", TTypeInfo(NTypeIds::String))
            }));

            builder.AddRow().Add<ui64>(7200).Add<std::string>("Tony");

            auto expected = builder.BuildArrow();
            UNIT_ASSERT_VALUES_EQUAL(batches.front()->ToString(), expected->ToString());
        }
        {
            auto batches = ExecuteAndCombineBatches(client, R"(
                SELECT Comment, Amount, Name FROM Test ORDER BY Amount DESC;
            )", /* assertSize */ true);

            UNIT_ASSERT_C(!batches.empty(), "Batches must not be empty");

            NColumnShard::TTableUpdatesBuilder builder(NArrow::MakeArrowSchema({
                std::make_pair("Comment", TTypeInfo(NTypeIds::String)),
                std::make_pair("Amount", TTypeInfo(NTypeIds::Uint64)),
                std::make_pair("Name", TTypeInfo(NTypeIds::String))
            }));

            builder.AddRow().Add<std::string>("None").Add<ui64>(7200).Add<std::string>("Tony");
            builder.AddRow().Add<std::string>("None").Add<ui64>(3500).Add<std::string>("Anna");
            builder.AddRow().Add<std::string>("None").Add<ui64>(300).Add<std::string>("Paul");

            auto expected = builder.BuildArrow();
            UNIT_ASSERT_VALUES_EQUAL(batches.front()->ToString(), expected->ToString());
        }
    }

    /**
     * Small channel buffer size, data bytes and schema from many ExecuteQueryResponePart parts are filled into a single ResultSet as a std::vector with a single schema.
     */
    Y_UNIT_TEST(ArrowFormat_SmallChannelBufferSize) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ false, 1_KB);
        auto client = kikimr.GetQueryClient();

        CreateLargeTable(kikimr, 100, 2, 2, 10, 2);

        auto batches = ExecuteAndCombineBatches(client, R"(
            SELECT * FROM LargeTable;
        )", /* assertSize */ true, /* minBatchesCount */ 2);

        UNIT_ASSERT_C(!batches.empty(), "Batches must not be empty");

        UNIT_ASSERT_VALUES_EQUAL(batches.front()->num_rows(), 200);
        UNIT_ASSERT_VALUES_EQUAL(batches.front()->num_columns(), 4);
    }

    /**
     * These YQL types are supported for Arrow format as arithmetic types:
     */
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
                    DecimalValue Decimal(22, 2),
                    PRIMARY KEY (BoolValue)
                );
            )", TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = client.ExecuteQuery(R"(
                INSERT INTO ArithmeticTypesTable (BoolValue, Int8Value, Uint8Value, Int16Value, Uint16Value, Int32Value, Uint32Value, Int64Value, Uint64Value, FloatValue, DoubleValue, DecimalValue) VALUES
                (NULL, -1, 1, -2, 2, -3, 3, -4, 4, CAST(5.0 AS Float), 6.0, CAST("7.77" AS Decimal(22, 2))),
                (true, -1, 1, -2, 2, -3, 3, -4, 4, CAST(5.0 AS Float), 6.0, CAST("7.77" AS Decimal(22, 2))),
                (false, -1, 1, -2, 2, -3, 3, -4, 4, CAST(5.0 AS Float), 6.0, CAST("7.77" AS Decimal(22, 2)));
            )", TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto batches = ExecuteAndCombineBatches(client, R"(
                    SELECT BoolValue, Int8Value, Uint8Value, Int16Value, Uint16Value, Int32Value, Uint32Value, Int64Value, Uint64Value, FloatValue, DoubleValue, DecimalValue
                    FROM ArithmeticTypesTable ORDER BY BoolValue;
            )", /* assertSize */ true);

            UNIT_ASSERT_C(!batches.empty(), "Batches must not be empty");

            NColumnShard::TTableUpdatesBuilder builder(NArrow::MakeArrowSchema({
                std::make_pair("BoolValue", TTypeInfo(NTypeIds::Uint8)),
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
                std::make_pair("DecimalValue", TTypeInfo(NScheme::TDecimalType(22, 2)))
            }));

            builder.AddRow().AddNull().Add<i8>(-1).Add<ui8>(1).Add<i16>(-2).Add<ui16>(2).Add<i32>(-3).Add<ui32>(3).Add<i64>(-4).Add<ui64>(4).Add<float>(5.0).Add<double>(6.0).Add(TDecimalValue("7.77", 22, 2));
            builder.AddRow().Add<ui8>(false).Add<i8>(-1).Add<ui8>(1).Add<i16>(-2).Add<ui16>(2).Add<i32>(-3).Add<ui32>(3).Add<i64>(-4).Add<ui64>(4).Add<float>(5.0).Add<double>(6.0).Add(TDecimalValue("7.77", 22, 2));
            builder.AddRow().Add<ui8>(true).Add<i8>(-1).Add<ui8>(1).Add<i16>(-2).Add<ui16>(2).Add<i32>(-3).Add<ui32>(3).Add<i64>(-4).Add<ui64>(4).Add<float>(5.0).Add<double>(6.0).Add(TDecimalValue("7.77", 22, 2));

            auto expected = builder.BuildArrow();
            UNIT_ASSERT_VALUES_EQUAL(batches.front()->ToString(), expected->ToString());
        }
    }

    /**
     * These YQL types are supported for Arrow format as string types:
     */
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
            auto batches = ExecuteAndCombineBatches(client, R"(
                SELECT Utf8Value, JsonValue, Utf8NotNullValue, JsonNotNullValue
                FROM StringTypesTable ORDER BY Utf8Value;
            )", /* assertSize */ true);

            UNIT_ASSERT_C(!batches.empty(), "Batches must not be empty");

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
            UNIT_ASSERT_VALUES_EQUAL(batches.front()->ToString(), expected->ToString());
        }
    }

    /**
     * These YQL types are supported for Arrow format as binary types:
     */
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
                    UuidValue Uuid,
                    StringNotNullValue String NOT NULL,
                    YsonNotNullValue Yson NOT NULL,
                    JsonDocumentNotNullValue JsonDocument NOT NULL,
                    DyNumberNotNullValue DyNumber NOT NULL,
                    UuidNotNullValue Uuid NOT NULL,
                    PRIMARY KEY (StringValue)
                );
            )", TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = client.ExecuteQuery(R"(
                INSERT INTO BinaryTypesTable (StringValue, YsonValue, DyNumberValue, JsonDocumentValue, UuidValue, StringNotNullValue, YsonNotNullValue, JsonDocumentNotNullValue, DyNumberNotNullValue, UuidNotNullValue) VALUES
                ("John", "[1]", DyNumber("1.0"), JsonDocument("{\"a\": 1}"), Uuid("5b99a330-04ef-4f1a-9b64-ba6d5f44eafe"), "Mark", "[2]", JsonDocument("{\"b\": 2}"), DyNumber("4.0"), Uuid("5b99a330-04ef-4f1a-9b64-ba6d5f44eafe")),
                (NULL, "[4]", NULL, NULL, NULL, "Maria", "[5]", JsonDocument("[6]"), DyNumber("7.0"), Uuid("5b99a330-04ef-4f1a-9b64-ba6d5f44eafe")),
                ("Mark", NULL, NULL, NULL, NULL, "Michael", "[7]", JsonDocument("[8]"), DyNumber("9.0"), Uuid("5b99a330-04ef-4f1a-9b64-ba6d5f44eafe")),
                ("Leo", "[10]", DyNumber("11.0"), JsonDocument("[12]"), Uuid("5b99a330-04ef-4f1a-9b64-ba6d5f44eafe"), "Maria", "[13]", JsonDocument("[14]"), DyNumber("15.0"), Uuid("5b99a330-04ef-4f1a-9b64-ba6d5f44eafe"));
            )", TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto batches = ExecuteAndCombineBatches(client, R"(
                SELECT StringValue, YsonValue, DyNumberValue, JsonDocumentValue, UuidValue, StringNotNullValue, YsonNotNullValue, JsonDocumentNotNullValue, DyNumberNotNullValue, UuidNotNullValue
                FROM BinaryTypesTable ORDER BY StringValue;
            )", /* assertSize */ true);

            UNIT_ASSERT_C(!batches.empty(), "Batches must not be empty");

            const TString expected =
R"(StringValue:   [
    null,
    4A6F686E,
    4C656F,
    4D61726B
  ]
YsonValue:   [
    5B345D,
    5B315D,
    5B31305D,
    null
  ]
DyNumberValue:   [
    null,
    ".1e1",
    ".11e2",
    null
  ]
JsonDocumentValue:   [
    null,
    "{"a":1}",
    "[12]",
    null
  ]
UuidValue:   [
    null,
    30A3995BEF041A4F9B64BA6D5F44EAFE,
    30A3995BEF041A4F9B64BA6D5F44EAFE,
    null
  ]
StringNotNullValue:   [
    4D61726961,
    4D61726B,
    4D61726961,
    4D69636861656C
  ]
YsonNotNullValue:   [
    5B355D,
    5B325D,
    5B31335D,
    5B375D
  ]
JsonDocumentNotNullValue:   [
    "[6]",
    "{"b":2}",
    "[14]",
    "[8]"
  ]
DyNumberNotNullValue:   [
    ".7e1",
    ".4e1",
    ".15e2",
    ".9e1"
  ]
UuidNotNullValue:   [
    30A3995BEF041A4F9B64BA6D5F44EAFE,
    30A3995BEF041A4F9B64BA6D5F44EAFE,
    30A3995BEF041A4F9B64BA6D5F44EAFE,
    30A3995BEF041A4F9B64BA6D5F44EAFE
  ]
)";
            UNIT_ASSERT_VALUES_EQUAL(batches.front()->ToString(), expected);
        }
    }

    /**
     * These YQL types are supported for Arrow format as integer and time types:
     */
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
            auto batches = ExecuteAndCombineBatches(client, R"(
                SELECT DateValue, DatetimeValue, TimestampValue, IntervalValue
                FROM TimeTypesTable ORDER BY DateValue;
            )", /* assertSize */ true);

            UNIT_ASSERT_C(!batches.empty(), "Batches must not be empty");

            NColumnShard::TTableUpdatesBuilder builder(NArrow::MakeArrowSchema({
                std::make_pair("DateValue", TTypeInfo(NTypeIds::Uint16)),
                std::make_pair("DatetimeValue", TTypeInfo(NTypeIds::Uint32)),
                std::make_pair("TimestampValue", TTypeInfo(NTypeIds::Uint64)),
                std::make_pair("IntervalValue", TTypeInfo(NTypeIds::Int64))
            }));

            builder.AddRow().Add<ui16>(11323).Add<ui32>(1012615322).Add<ui64>(1046660583000000).Add<i64>(604800000000);

            auto expected = builder.BuildArrow();
            UNIT_ASSERT_VALUES_EQUAL(batches.front()->ToString(), expected->ToString());
        }
    }

    /**
     * Arrow format is supported for compression.
     * By default, unspecified compression codec is None (without compression).
     */
    Y_UNIT_TEST(ArrowFormat_Compression_None) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ true);
        auto client = kikimr.GetQueryClient();

        CompareCompressedAndDefaultBatches(client, std::nullopt, /* assertEqual */ true);

        CompareCompressedAndDefaultBatches(client, TArrowFormatSettings::TCompressionCodec().Type(TArrowFormatSettings::TCompressionCodec::EType::None), /* assertEqual */ true);
    }

    /**
     * Arrow format is supported for compression by ZSTD codec.
     * Compression level is supported.
     */
    Y_UNIT_TEST(ArrowFormat_Compression_ZSTD) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ true);
        auto client = kikimr.GetQueryClient();

        CompareCompressedAndDefaultBatches(client, TArrowFormatSettings::TCompressionCodec().Type(TArrowFormatSettings::TCompressionCodec::EType::Zstd).Level(12), /* assertEqual */ false);
    }

    /**
     * Arrow format is supported for compression by LZ4_FRAME codec.
     * Compression level is not supported.
     */
    Y_UNIT_TEST(ArrowFormat_Compression_LZ4_FRAME) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ true);
        auto client = kikimr.GetQueryClient();

        CompareCompressedAndDefaultBatches(client, TArrowFormatSettings::TCompressionCodec().Type(TArrowFormatSettings::TCompressionCodec::EType::Lz4Frame), /* assertEqual */ false);

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

    /**
     * Arrow batches are returned for different result set indexes.
     */
    Y_UNIT_TEST(ArrowFormat_Multistatement) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ true);
        auto client = kikimr.GetQueryClient();

        auto settings = TExecuteQuerySettings().Format(TResultSet::EFormat::Arrow);

        auto result = client.ExecuteQuery(R"(
            SELECT Comment, Amount, Name FROM Test ORDER BY Amount DESC;
            SELECT Key, Value FROM KeyValue ORDER BY Key;
        )", TTxControl::BeginTx().CommitTx(), settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 2);

        {
            auto resultSet = result.GetResultSet(0);
            UNIT_ASSERT_VALUES_EQUAL(TArrowAccessor::Format(resultSet), TResultSet::EFormat::Arrow);

            const auto& schema = TArrowAccessor::GetArrowSchema(resultSet);
            const auto& batches = TArrowAccessor::GetArrowBatches(resultSet);

            UNIT_ASSERT_C(!batches.empty(), "Batches must not be empty");

            auto arrowSchema = NArrow::DeserializeSchema(TString(schema));
            auto arrowBatch = NArrow::DeserializeBatch(TString(batches[0]), arrowSchema);

            UNIT_ASSERT_C(arrowSchema, "Schema must be deserialized");
            UNIT_ASSERT_C(arrowBatch, "Batch must be deserialized");
            UNIT_ASSERT_C(arrowBatch->ValidateFull().ok(), "Batch validation failed");

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
        {
            auto resultSet = result.GetResultSet(1);
            UNIT_ASSERT_VALUES_EQUAL(TArrowAccessor::Format(resultSet), TResultSet::EFormat::Arrow);

            const auto& schema = TArrowAccessor::GetArrowSchema(resultSet);
            const auto& batches = TArrowAccessor::GetArrowBatches(resultSet);

            UNIT_ASSERT_C(!batches.empty(), "Batches must not be empty");

            auto arrowSchema = NArrow::DeserializeSchema(TString(schema));
            auto arrowBatch = NArrow::DeserializeBatch(TString(batches[0]), arrowSchema);

            UNIT_ASSERT_C(arrowSchema, "Schema must be deserialized");
            UNIT_ASSERT_C(arrowBatch, "Batch must be deserialized");
            UNIT_ASSERT_C(arrowBatch->ValidateFull().ok(), "Batch validation failed");

            NColumnShard::TTableUpdatesBuilder builder(NArrow::MakeArrowSchema({
                std::make_pair("Key", TTypeInfo(NTypeIds::Uint64)),
                std::make_pair("Value", TTypeInfo(NTypeIds::String))
            }));

            builder.AddRow().Add<ui64>(1).Add<std::string>("One");
            builder.AddRow().Add<ui64>(2).Add<std::string>("Two");

            auto expected = builder.BuildArrow();
            UNIT_ASSERT_VALUES_EQUAL(arrowBatch->ToString(), expected->ToString());
        }
    }

    /**
     * By default, SchemaInclusionMode is ALWAYS for Arrow format.
     */
    Y_UNIT_TEST(ArrowFormat_SchemaInclusionMode_Unspecified) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ false, 1_KB);
        auto client = kikimr.GetQueryClient();

        CreateLargeTable(kikimr, 100, 2, 2, 10, 2);

        auto settings = TExecuteQuerySettings().Format(TResultSet::EFormat::Arrow);

        auto it = client.StreamExecuteQuery(R"(
            SELECT * FROM LargeTable;
        )", TTxControl::BeginTx().CommitTx(), settings).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        std::shared_ptr<arrow::Schema> arrowSchema;

        size_t count = 0;
        for (;;) {
            auto part = it.ReadNext().GetValueSync();
            if (!part.IsSuccess()) {
                UNIT_ASSERT_C(part.EOS(), part.GetIssues().ToString());
                break;
            }

            if (part.HasResultSet()) {
                auto resultSet = part.ExtractResultSet();
                UNIT_ASSERT_VALUES_EQUAL(TArrowAccessor::Format(resultSet), TResultSet::EFormat::Arrow);

                const auto& schema = TArrowAccessor::GetArrowSchema(resultSet);
                const auto& batches = TArrowAccessor::GetArrowBatches(resultSet);

                UNIT_ASSERT_C(!batches.empty(), "Batches must not be empty");

                // With Arrow format, the result set contains a YQL schema and an Arrow record batch schema
                UNIT_ASSERT_C(!schema.empty(), "Schema must not be empty");
                UNIT_ASSERT_VALUES_UNEQUAL_C(resultSet.ColumnsCount(), 0, "Columns must not be empty for the first result set");

                auto curSchema = NArrow::DeserializeSchema(TString(schema));
                UNIT_ASSERT_C(curSchema, "Schema must be deserialized");

                if (arrowSchema) {
                    UNIT_ASSERT_VALUES_EQUAL(arrowSchema->ToString(), curSchema->ToString());
                } else {
                    arrowSchema = curSchema;
                }

                auto arrowBatch = NArrow::DeserializeBatch(TString(batches[0]), arrowSchema);
                UNIT_ASSERT_C(arrowBatch, "Batch must be deserialized");
                UNIT_ASSERT_C(arrowBatch->ValidateFull().ok(), "Batch validation failed");
                UNIT_ASSERT_GT_C(arrowBatch->num_rows(), 0, "Batch must have at least 1 row");

                ++count;
            }
        }

        UNIT_ASSERT_GT_C(count, 1, "Expected at least 2 result sets");
    }

    /**
     * Set SchemaInclusionMode ALWAYS for Arrow format explicitly in TExecuteQuerySettings.
     */
    Y_UNIT_TEST(ArrowFormat_SchemaInclusionMode_Always) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ false, 1_KB);
        auto client = kikimr.GetQueryClient();

        CreateLargeTable(kikimr, 100, 2, 2, 10, 2);

        auto settings = TExecuteQuerySettings().Format(TResultSet::EFormat::Arrow).SchemaInclusionMode(ESchemaInclusionMode::Always);

        auto it = client.StreamExecuteQuery(R"(
            SELECT * FROM LargeTable;
        )", TTxControl::BeginTx().CommitTx(), settings).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        std::shared_ptr<arrow::Schema> arrowSchema;

        size_t count = 0;
        for (;;) {
            auto part = it.ReadNext().GetValueSync();
            if (!part.IsSuccess()) {
                UNIT_ASSERT_C(part.EOS(), part.GetIssues().ToString());
                break;
            }

            if (part.HasResultSet()) {
                auto resultSet = part.ExtractResultSet();
                UNIT_ASSERT_VALUES_EQUAL(TArrowAccessor::Format(resultSet), TResultSet::EFormat::Arrow);

                const auto& schema = TArrowAccessor::GetArrowSchema(resultSet);
                const auto& batches = TArrowAccessor::GetArrowBatches(resultSet);

                UNIT_ASSERT_C(!batches.empty(), "Batches must not be empty");

                // With Arrow format, the result set contains a YQL schema and an Arrow record batch schema
                UNIT_ASSERT_C(!schema.empty(), "Schema must not be empty");
                UNIT_ASSERT_VALUES_UNEQUAL_C(resultSet.ColumnsCount(), 0, "Columns must not be empty for the first result set");

                auto curSchema = NArrow::DeserializeSchema(TString(schema));
                UNIT_ASSERT_C(curSchema, "Schema must be deserialized");

                if (arrowSchema) {
                    UNIT_ASSERT_VALUES_EQUAL(arrowSchema->ToString(), curSchema->ToString());
                } else {
                    arrowSchema = curSchema;
                }

                auto arrowBatch = NArrow::DeserializeBatch(TString(batches[0]), arrowSchema);
                UNIT_ASSERT_C(arrowBatch, "Batch must be deserialized");
                UNIT_ASSERT_C(arrowBatch->ValidateFull().ok(), "Batch validation failed");
                UNIT_ASSERT_GT_C(arrowBatch->num_rows(), 0, "Batch must have at least 1 row");

                ++count;
            }
        }

        UNIT_ASSERT_GT_C(count, 1, "Expected at least 2 result sets");
    }

    /**
     * Set SchemaInclusionMode FIRST_ONLY for Arrow format explicitly in TExecuteQuerySettings.
     */
    Y_UNIT_TEST(ArrowFormat_SchemaInclusionMode_FirstOnly) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ false, 1_KB);
        auto client = kikimr.GetQueryClient();

        CreateLargeTable(kikimr, 100, 2, 2, 10, 2);

        auto settings = TExecuteQuerySettings().Format(TResultSet::EFormat::Arrow).SchemaInclusionMode(ESchemaInclusionMode::FirstOnly);

        auto it = client.StreamExecuteQuery(R"(
            SELECT * FROM LargeTable;
        )", TTxControl::BeginTx().CommitTx(), settings).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        std::shared_ptr<arrow::Schema> arrowSchema;

        size_t count = 0;
        for (;;) {
            auto part = it.ReadNext().GetValueSync();
            if (!part.IsSuccess()) {
                UNIT_ASSERT_C(part.EOS(), part.GetIssues().ToString());
                break;
            }

            if (part.HasResultSet()) {
                auto resultSet = part.ExtractResultSet();
                UNIT_ASSERT_VALUES_EQUAL(TArrowAccessor::Format(resultSet), TResultSet::EFormat::Arrow);

                const auto& schema = TArrowAccessor::GetArrowSchema(resultSet);
                const auto& batches = TArrowAccessor::GetArrowBatches(resultSet);

                UNIT_ASSERT_C(!batches.empty(), "Batches must not be empty");

                if (count == 0) {
                    // With Arrow format, the result set contains a YQL schema and an Arrow record batch schema
                    UNIT_ASSERT_VALUES_UNEQUAL_C(resultSet.ColumnsCount(), 0, "Columns must not be empty for the first result set");
                    UNIT_ASSERT_C(!schema.empty(), "Schema must not be empty for the first result set");

                    arrowSchema = NArrow::DeserializeSchema(TString(schema));

                    UNIT_ASSERT_C(arrowSchema, "Schema must be deserialized");
                } else {
                    UNIT_ASSERT_VALUES_EQUAL_C(resultSet.ColumnsCount(), 0, "Columns count must be empty for the rest result sets");
                    UNIT_ASSERT_C(schema.empty(), "Schema must be empty for the rest result sets");
                }

                auto arrowBatch = NArrow::DeserializeBatch(TString(batches[0]), arrowSchema);
                UNIT_ASSERT_C(arrowBatch, "Batch must be deserialized");
                UNIT_ASSERT_C(arrowBatch->ValidateFull().ok(), "Batch validation failed");
                UNIT_ASSERT_GT_C(arrowBatch->num_rows(), 0, "Batch must have at least 1 row");

                ++count;
            }
        }

        UNIT_ASSERT_GT_C(count, 1, "Expected at least 2 result sets");
    }

    /**
     * For Arrow format, FirstOnly schema inclusion mode is supported for multistatement queries.
     */
    Y_UNIT_TEST(ArrowFormat_SchemaInclusionMode_FirstOnly_Multistatement) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ false, 1_KB);
        auto client = kikimr.GetQueryClient();

        CreateLargeTable(kikimr, 200, 2, 2, 10, 2);

        auto settings = TExecuteQuerySettings().Format(TResultSet::EFormat::Arrow).SchemaInclusionMode(ESchemaInclusionMode::FirstOnly);

        auto it = client.StreamExecuteQuery(R"(
            SELECT * FROM LargeTable;
            SELECT Key, Data FROM LargeTable;
        )", TTxControl::BeginTx().CommitTx(), settings).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        std::unordered_map<size_t, std::shared_ptr<arrow::Schema>> arrowSchemas;
        std::unordered_map<size_t, ui64> counts;

        for (;;) {
            auto part = it.ReadNext().GetValueSync();
            if (!part.IsSuccess()) {
                UNIT_ASSERT_C(part.EOS(), part.GetIssues().ToString());
                break;
            }

            if (part.HasResultSet()) {
                auto resultSet = part.ExtractResultSet();
                UNIT_ASSERT_VALUES_EQUAL(TArrowAccessor::Format(resultSet), TResultSet::EFormat::Arrow);

                const auto& schema = TArrowAccessor::GetArrowSchema(resultSet);
                const auto& batches = TArrowAccessor::GetArrowBatches(resultSet);

                UNIT_ASSERT_C(!batches.empty(), "Batches must not be empty");

                auto idx = part.GetResultSetIndex();

                if (arrowSchemas.find(idx) == arrowSchemas.end()) {
                    // The first result set of each statement contains schemas.
                    UNIT_ASSERT_VALUES_UNEQUAL_C(resultSet.ColumnsCount(), 0, "Columns must not be empty for the first result set of the statement");
                    UNIT_ASSERT_C(!schema.empty(), "Schema must not be empty for the first result set of the statement");

                    arrowSchemas[idx] = NArrow::DeserializeSchema(TString(schema));

                    UNIT_ASSERT_C(arrowSchemas[idx], "Schema must be deserialized");
                } else {
                    UNIT_ASSERT_VALUES_EQUAL_C(resultSet.ColumnsCount(), 0, "Columns count must be empty for the rest result sets of the statement");
                    UNIT_ASSERT_C(schema.empty(), "Schema must be empty for the rest result sets of the statement");
                }

                auto arrowBatch = NArrow::DeserializeBatch(TString(batches[0]), arrowSchemas[idx]);
                UNIT_ASSERT_C(arrowBatch, "Batch must be deserialized");
                UNIT_ASSERT_C(arrowBatch->ValidateFull().ok(), "Batch validation failed");
                UNIT_ASSERT_GT_C(arrowBatch->num_rows(), 0, "Batch must have at least 1 row");

                ++counts[idx];
            }
        }

        UNIT_ASSERT_C(counts.size() == 2, "Expected 2 result set indexes");

        for (const auto& [idx, count] : counts) {
            UNIT_ASSERT_GT_C(count, 1, "Expected at least 2 result sets for statement with ResultSetIndex = " << idx);
        }
    }

    /**
     * Small stress test for Arrow format:
     * - 3 statements
     * - 10 shards, 1000 rows per shard
     * - ZSTD compression with level 10
     * - SchemaInclusionMode is FIRST_ONLY
     * - ChannelBufferSize is 1KB
     */
    Y_UNIT_TEST(ArrowFormat_Stress) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ false, 1_KB);
        auto client = kikimr.GetQueryClient();

        std::unordered_map<size_t, std::shared_ptr<arrow::Schema>> arrowSchemas;
        std::unordered_map<size_t, size_t> arrowResultSetsCount;
        std::unordered_map<size_t, size_t> arrowRowsPerStatement;

        std::unordered_map<size_t, size_t> valueRowsPerStatement;

        CreateLargeTable(kikimr, 1000, 4, 4, 100, 10);

        const size_t statementCount = 3;
        const auto query = R"(
            SELECT * FROM LargeTable;
            UPDATE LargeTable SET Data = Data + 1 WHERE Key % 2 = 1 RETURNING Data;
            SELECT DataText FROM LargeTable WHERE Key % 2 = 0;
        )";

        // Get rows count for each statement from Value format to compare with Arrow format
        const auto valueResult = client.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(valueResult.IsSuccess(), valueResult.GetIssues().ToString());

        for (size_t stmt = 0; stmt < statementCount; ++stmt) {
            const auto resultSet = valueResult.GetResultSet(stmt);
            valueRowsPerStatement[stmt] = resultSet.RowsCount();
        }

        auto settings = TExecuteQuerySettings()
            .Format(TResultSet::EFormat::Arrow)
            .SchemaInclusionMode(ESchemaInclusionMode::FirstOnly)
            .ArrowFormatSettings(TArrowFormatSettings()
                .CompressionCodec(TArrowFormatSettings::TCompressionCodec()
                    .Type(TArrowFormatSettings::TCompressionCodec::EType::Zstd)
                    .Level(10)));

        auto it = client.StreamExecuteQuery(query, TTxControl::BeginTx().CommitTx(), settings).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        for (;;) {
            auto part = it.ReadNext().GetValueSync();
            if (!part.IsSuccess()) {
                UNIT_ASSERT_C(part.EOS(), part.GetIssues().ToString());
                break;
            }

            if (part.HasResultSet()) {
                auto resultSet = part.ExtractResultSet();
                UNIT_ASSERT_VALUES_EQUAL(TArrowAccessor::Format(resultSet), TResultSet::EFormat::Arrow);

                const auto& schema = TArrowAccessor::GetArrowSchema(resultSet);
                const auto& batches = TArrowAccessor::GetArrowBatches(resultSet);

                UNIT_ASSERT_C(!batches.empty(), "Batches must not be empty");

                auto idx = part.GetResultSetIndex();

                if (arrowSchemas.find(idx) == arrowSchemas.end()) {
                    // The first result set of each statement contains schemas.
                    UNIT_ASSERT_VALUES_UNEQUAL_C(resultSet.ColumnsCount(), 0, "Columns must not be empty for the first result set of the statement");
                    UNIT_ASSERT_C(!schema.empty(), "Schema must not be empty for the first result set of the statement");

                    arrowSchemas[idx] = NArrow::DeserializeSchema(TString(schema));

                    UNIT_ASSERT_C(arrowSchemas[idx], "Schema must be deserialized");
                } else {
                    UNIT_ASSERT_VALUES_EQUAL_C(resultSet.ColumnsCount(), 0, "Columns count must be empty for the rest result sets of the statement");
                    UNIT_ASSERT_C(schema.empty(), "Schema must be empty for the rest result sets of the statement");
                }

                auto arrowBatch = NArrow::DeserializeBatch(TString(batches[0]), arrowSchemas[idx]);
                UNIT_ASSERT_C(arrowBatch, "Batch must be deserialized");
                UNIT_ASSERT_C(arrowBatch->ValidateFull().ok(), "Batch validation failed");

                arrowRowsPerStatement[idx] += arrowBatch->num_rows();
                ++arrowResultSetsCount[idx];
            }
        }

        UNIT_ASSERT_C(arrowResultSetsCount.size() == 3, "Expected 3 result set indexes");

        for (const auto& [idx, count] : arrowResultSetsCount) {
            UNIT_ASSERT_GT_C(count, 1, "Expected at least 2 result sets for statement with ResultSetIndex = " << idx);
        }

        for (const auto& [idx, count] : arrowRowsPerStatement) {
            UNIT_ASSERT_VALUES_EQUAL_C(count, valueRowsPerStatement[idx], "Rows count mismatch for statement with ResultSetIndex = " << idx);
        }
    }

    /**
     * More tests for different types with correctness and convertations between Arrow and UV :
     * ydb/core/kqp/common/result_set_format/ut/kqp_formats_arrow_ut.cpp
    */

    // Optional<T>
    Y_UNIT_TEST(ArrowFormat_Types_Optional_1) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ true);
        auto client = kikimr.GetQueryClient();

        {
            auto batches = ExecuteAndCombineBatches(client, R"(
                SELECT Key1, Name FROM Join2
                WHERE Key1 IN [104, 106, 108]
                ORDER BY Key1;
            )", /* assertSize */ false, 1);

            UNIT_ASSERT_C(!batches.empty(), "Batches must not be empty");

            const auto& batch = batches.front();

            UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 3);
            UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 2);

            ValidateOptionalColumn(batch->column(0), 1, false);
            ValidateOptionalColumn(batch->column(1), 1, false);

            const TString expected =
R"(Key1:   [
    104,
    106,
    108
  ]
Name:   [
    4E616D6533,
    4E616D6533,
    null
  ]
)";
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected);
        }
    }

    // Optional<Optional<T>>
    Y_UNIT_TEST(ArrowFormat_Types_Optional_2) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ true);
        auto client = kikimr.GetQueryClient();

        {
            auto batches = ExecuteAndCombineBatches(client, R"(
                SELECT Just(Key1), Just(Name) FROM Join2
                WHERE Key1 IN [104, 106, 108]
                ORDER BY Key1;
            )", /* assertSize */ false, 1);

            UNIT_ASSERT_C(!batches.empty(), "Batches must not be empty");

            const auto& batch = batches.front();

            UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 3);
            UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 2);

            ValidateOptionalColumn(batch->column(0), 2, false);
            ValidateOptionalColumn(batch->column(1), 2, false);

            const TString expected =
R"(column0:   -- is_valid: all not null
  -- child 0 type: uint32
    [
      104,
      106,
      108
    ]
column1:   -- is_valid: all not null
  -- child 0 type: binary
    [
      4E616D6533,
      4E616D6533,
      null
    ]
)";
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected);
        }
    }

    // Optional<Variant<T, F>>
    Y_UNIT_TEST(ArrowFormat_Types_Optional_3) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ false);
        auto client = kikimr.GetQueryClient();

        {
            auto batches = ExecuteAndCombineBatches(client, R"(
                SELECT Just(Variant(1, "foo", Variant<foo: Int32, bar: Bool>));
            )", /* assertSize */ false);

            UNIT_ASSERT_C(!batches.empty(), "Batches must not be empty");

            const auto& batch = batches.front();

            UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 1);
            UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 1);

            ValidateOptionalColumn(batch->column(0), 1, /* isVariant */ true);

            const TString expected =
R"(column0:   -- is_valid: all not null
  -- child 0 type: dense_union<bar: uint8 not null=0, foo: int32 not null=1>
    -- is_valid: all not null
    -- type_ids:       [
        1
      ]
    -- value_offsets:       [
        0
      ]
    -- child 0 type: uint8
      []
    -- child 1 type: int32
      [
        1
      ]
)";
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected);
        }
    }

    // Optional<Optional<Variant<T, F, G>>>
    Y_UNIT_TEST(ArrowFormat_Types_Optional_4) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ false);
        auto client = kikimr.GetQueryClient();

        {
            auto batches = ExecuteAndCombineBatches(client, R"(
                SELECT Just(Just(Variant(1, "foo", Variant<foo: Int32, bar: Bool, foobar: String>)));
            )", /* assertSize */ false);

            UNIT_ASSERT_C(!batches.empty(), "Batches must not be empty");

            const auto& batch = batches.front();

            UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 1);
            UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 1);

            ValidateOptionalColumn(batch->column(0), 2, /* isVariant */ true);

            const TString expected =
R"(column0:   -- is_valid: all not null
  -- child 0 type: struct<opt: dense_union<bar: uint8 not null=0, foo: int32 not null=1, foobar: binary not null=2>>
    -- is_valid: all not null
    -- child 0 type: dense_union<bar: uint8 not null=0, foo: int32 not null=1, foobar: binary not null=2>
      -- is_valid: all not null
      -- type_ids:         [
          1
        ]
      -- value_offsets:         [
          0
        ]
      -- child 0 type: uint8
        []
      -- child 1 type: int32
        [
          1
        ]
      -- child 2 type: binary
        []
)";
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected);
        }
    }

    // List<T>
    Y_UNIT_TEST(ArrowFormat_Types_List_1) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ false);
        auto client = kikimr.GetQueryClient();

        {
            auto batches = ExecuteAndCombineBatches(client, R"(
                SELECT CAST([1, 2, 3] AS List<Int32>);
            )", /* assertSize */ false);

            UNIT_ASSERT_C(!batches.empty(), "Batches must not be empty");

            const auto& batch = batches.front();

            UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 1);
            UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 1);
            UNIT_ASSERT_C(batch->column(0)->type()->id() == arrow::Type::LIST, "Column type must be arrow::Type::LIST");

            const TString expected =
R"(column0:   [
    [
      1,
      2,
      3
    ]
  ]
)";
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected);
        }
    }

    // List<Optional<T>>
    Y_UNIT_TEST(ArrowFormat_Types_List_2) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ false);
        auto client = kikimr.GetQueryClient();

        {
            auto batches = ExecuteAndCombineBatches(client, R"(
                SELECT CAST([1, NULL, 3, null] AS List<Optional<Int32>>);
            )", /* assertSize */ false);

            UNIT_ASSERT_C(!batches.empty(), "Batches must not be empty");

            const auto& batch = batches.front();

            UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 1);
            UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 1);
            UNIT_ASSERT_C(batch->column(0)->type()->id() == arrow::Type::LIST, "Column type must be arrow::Type::LIST");

            const TString expected =
R"(column0:   [
    [
      1,
      null,
      3,
      null
    ]
  ]
)";
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected);
        }
    }

    // List<Optional<T>> of columns
    Y_UNIT_TEST(ArrowFormat_Types_List_3) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ true);
        auto client = kikimr.GetQueryClient();

        {
            auto batches = ExecuteAndCombineBatches(client, R"(
                SELECT [App, Host] FROM Logs
                ORDER BY App;
            )", /* assertSize */ false);

            UNIT_ASSERT_C(!batches.empty(), "Batches must not be empty");

            const auto& batch = batches.front();

            UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 9);
            UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 1);
            UNIT_ASSERT_C(batch->column(0)->type()->id() == arrow::Type::LIST, "Column type must be arrow::Type::LIST");

            const TString expected =
R"(column0:   [
    [
      "apache",
      "front-42"
    ],
    [
      "kikimr-db",
      "kikimr-db-10"
    ],
    [
      "kikimr-db",
      "kikimr-db-21"
    ],
    [
      "kikimr-db",
      "kikimr-db-21"
    ],
    [
      "kikimr-db",
      "kikimr-db-53"
    ],
    [
      "nginx",
      "nginx-10"
    ],
    [
      "nginx",
      "nginx-23"
    ],
    [
      "nginx",
      "nginx-23"
    ],
    [
      "ydb",
      "ydb-1000"
    ]
  ]
)";
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected);
        }
    }

    // List<>
    Y_UNIT_TEST(ArrowFormat_Types_EmptyList) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ false);
        auto client = kikimr.GetQueryClient();

        {
            auto batches = ExecuteAndCombineBatches(client, R"(
                SELECT [];
            )", /* assertSize */ false);

            UNIT_ASSERT_C(!batches.empty(), "Batches must not be empty");

            const auto& batch = batches.front();

            UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 1);
            UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 1);
            UNIT_ASSERT_C(batch->column(0)->type()->id() == arrow::Type::STRUCT, "Column type must be arrow::Type::STRUCT");

            const TString expected =
R"(column0:   -- is_valid: all not null
)";
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected);
        }
    }

    // Tuple<T, F>
    Y_UNIT_TEST(ArrowFormat_Types_Tuple) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ false);
        auto client = kikimr.GetQueryClient();

        {
            auto batches = ExecuteAndCombineBatches(client, R"(
                SELECT CAST((1, 2.5) AS Tuple<Uint32, Double>);
            )", /* assertSize */ false);

            UNIT_ASSERT_C(!batches.empty(), "Batches must not be empty");

            const auto& batch = batches.front();

            UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 1);
            UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 1);
            UNIT_ASSERT_C(batch->column(0)->type()->id() == arrow::Type::STRUCT, "Column type must be arrow::Type::STRUCT");

            const TString expected =
R"(column0:   -- is_valid: all not null
  -- child 0 type: uint32
    [
      1
    ]
  -- child 1 type: double
    [
      2.5
    ]
)";
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected);
        }
    }

    // Dict<K, V>
    Y_UNIT_TEST(ArrowFormat_Types_Dict_1) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ false);
        auto client = kikimr.GetQueryClient();

        {
            auto batches = ExecuteAndCombineBatches(client, R"(
                SELECT CAST({"a": 1, "b": 2, "c": 3} AS Dict<String,Int32>);
            )", /* assertSize */ false);

            UNIT_ASSERT_C(!batches.empty(), "Batches must not be empty");

            const auto& batch = batches.front();

            UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 1);
            UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 1);
            UNIT_ASSERT_C(batch->column(0)->type()->id() == arrow::Type::LIST, "Column type must be arrow::Type::LIST");

            const TString expected =
R"(column0:   [
    -- is_valid: all not null
    -- child 0 type: binary
      [
        61,
        63,
        62
      ]
    -- child 1 type: int32
      [
        1,
        3,
        2
      ]
  ]
)";

            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected);
        }
    }

    // Dict<Optional<K>, V>
    Y_UNIT_TEST(ArrowFormat_Types_Dict_2) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ false);
        auto client = kikimr.GetQueryClient();

        {
            auto batches = ExecuteAndCombineBatches(client, R"(
                SELECT CAST({"a": 1, "b": 2, NULL: 3} AS Dict<Optional<String>,Int32>);
            )", /* assertSize */ false);

            UNIT_ASSERT_C(!batches.empty(), "Batches must not be empty");

            const auto& batch = batches.front();

            UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 1);
            UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 1);
            UNIT_ASSERT_C(batch->column(0)->type()->id() == arrow::Type::LIST, "Column type must be arrow::Type::LIST");

            const TString expected =
R"(column0:   [
    -- is_valid: all not null
    -- child 0 type: binary
      [
        61,
        62,
        null
      ]
    -- child 1 type: int32
      [
        1,
        2,
        3
      ]
  ]
)";

            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected);
        }
    }

    // Dict<>
    Y_UNIT_TEST(ArrowFormat_Types_EmptyDict) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ false);
        auto client = kikimr.GetQueryClient();

        {
            auto batches = ExecuteAndCombineBatches(client, R"(
                SELECT {};
            )", /* assertSize */ false);

            UNIT_ASSERT_C(!batches.empty(), "Batches must not be empty");

            const auto& batch = batches.front();

            UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 1);
            UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 1);
            UNIT_ASSERT_C(batch->column(0)->type()->id() == arrow::Type::STRUCT, "Column type must be arrow::Type::STRUCT");

            const TString expected =
R"(column0:   -- is_valid: all not null
)";
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected);
        }
    }

    // Struct<first:T, second:F>
    Y_UNIT_TEST(ArrowFormat_Types_Struct) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ false);
        auto client = kikimr.GetQueryClient();

        {
            auto batches = ExecuteAndCombineBatches(client, R"(
                SELECT CAST(<|first: 1, second: "2"|> AS Struct<first:Int32,second:Utf8>);
            )", /* assertSize */ false);

            UNIT_ASSERT_C(!batches.empty(), "Batches must not be empty");

            const auto& batch = batches.front();

            UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 1);
            UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 1);
            UNIT_ASSERT_C(batch->column(0)->type()->id() == arrow::Type::STRUCT, "Column type must be arrow::Type::STRUCT");

            const TString expected =
R"(column0:   -- is_valid: all not null
  -- child 0 type: int32
    [
      1
    ]
  -- child 1 type: string
    [
      "2"
    ]
)";
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected);
        }
    }

    // Variant<T, F>
    Y_UNIT_TEST(ArrowFormat_Types_Variant) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ false);
        auto client = kikimr.GetQueryClient();

        {
            auto batches = ExecuteAndCombineBatches(client, R"(
                SELECT Variant(1, "foo", Variant<foo: Int32, bar: Bool>);
            )", /* assertSize */ false);

            UNIT_ASSERT_C(!batches.empty(), "Batches must not be empty");

            const auto& batch = batches.front();

            UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 1);
            UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 1);
            UNIT_ASSERT_C(batch->column(0)->type()->id() == arrow::Type::DENSE_UNION, "Column type must be arrow::Type::DENSE_UNION");

            const TString expected =
R"(column0:   -- is_valid: all not null
  -- type_ids:     [
      1
    ]
  -- value_offsets:     [
      0
    ]
  -- child 0 type: uint8
    []
  -- child 1 type: int32
    [
      1
    ]
)";
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected);
        }
    }

    Y_UNIT_TEST_TWIN(ArrowFormat_BulkUpsert, IsOlap) {
        auto kikimr = CreateKikimrRunner(/* withSampleTables */ false);
        auto queryClient = kikimr.GetQueryClient();
        auto tableClient = kikimr.GetTableClient();

        std::vector<std::pair<std::string, std::string>> types = {
            {"Bool", "Bool"},
            {"Uint8", "Uint8"},
            {"Int8", "Int8"},
            {"Uint16", "Uint16"},
            {"Int16", "Int16"},
            {"Uint32", "Uint32"},
            {"Int32", "Int32"},
            {"Uint64", "Uint64"},
            {"Int64", "Int64"},
            {"Float", "Float"},
            {"Double", "Double"},
            {"Date", "Date"},
            {"Datetime", "Datetime"},
            {"Timestamp", "Timestamp"},
            {"String", "String"},
            {"Utf8", "Utf8"},
            {"Date32", "Date32"},
            {"Datetime64", "Datetime64"},
            {"Timestamp64", "Timestamp64"},
            {"Interval64", "Interval64"},
            {"Json", "Json"},
            {"JsonDocument", "JsonDocument"},
            {"Yson", "Yson"},
            {"Decimal", "Decimal(22, 9)"}
        };

        if (!IsOlap) {
            types.push_back({"Interval", "Interval"});
            types.push_back({"DyNumber", "DyNumber"});
            types.push_back({"Uuid", "Uuid"});
        }

        {
            std::string query = "CREATE TABLE SomeTypes (";
            query += "Key Uint64 NOT NULL";
            for (const auto& [name, type] : types) {
                query += std::format(", {}Value {}", name, type);
            }
            query += ", PRIMARY KEY (Key))";

            if (IsOlap) {
                query += " WITH (STORE = COLUMN)";
            }

            auto result = queryClient.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            std::string query = "INSERT INTO SomeTypes (";
            query += "Key";

            for (const auto& [name, type] : types) {
                query += std::format(", {}Value", name);
            }
            query += ") VALUES ";
            query += "(1, true, 1, -1, 1, -1, 1, -1, 1, -1, 1.0f, 1.0, Date('2025-01-01'), Datetime('2025-01-01T00:00:00Z'), Timestamp('2025-01-01T00:00:00Z'), '1', '1'u, Date32('2025-01-01'), Datetime64('2025-01-01T00:00:00Z'), Timestamp64('2025-01-01T00:00:00Z'), Interval64('P1D'), Json(@@[1]@@), JsonDocument('[1]'), Yson('[1]'), Decimal('1', 22, 9)";

            if (!IsOlap) {
                query += ", Interval('P1D'), DyNumber('1'), Uuid('f9d5cc3f-f1dc-4d9c-b97e-766e57ca4cc1')";
            }
            query += "),";

            query += "(2, false, 2, -2, 2, -2, 2, -2, 2, -2, 2.0f, 2.0, Date('2025-02-02'), Datetime('2025-02-02T00:00:00Z'), Timestamp('2025-02-02T00:00:00Z'), '2', '2'u, Date32('2025-02-02'), Datetime64('2025-02-02T00:00:00Z'), Timestamp64('2025-02-02T00:00:00Z'), Interval64('P2D'), Json(@@[2]@@), JsonDocument('[2]'), Yson('[2]'), Decimal('2', 22, 9)";

            if (!IsOlap) {
                query += ", Interval('P2D'), DyNumber('2'), Uuid('f9d5cc3f-f1dc-4d9c-b97e-766e57ca4cc2')";
            }
            query += "),";

            query += "(3, true, 3, -3, 3, -3, 3, -3, 3, -3, 3.0f, 3.0, Date('2025-03-03'), Datetime('2025-03-03T00:00:00Z'), Timestamp('2025-03-03T00:00:00Z'), '3', '3'u, Date32('2025-03-03'), Datetime64('2025-03-03T00:00:00Z'), Timestamp64('2025-03-03T00:00:00Z'), Interval64('P3D'), Json(@@[3]@@), JsonDocument('[3]'), Yson('[3]'), Decimal('3', 22, 9)";

            if (!IsOlap) {
                query += ", Interval('P3D'), DyNumber('3'), Uuid('f9d5cc3f-f1dc-4d9c-b97e-766e57ca4cc3')";
            }
            query += ")";

            auto result = queryClient.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            TString query = "SELECT ";
            query += "Key + 3 AS Key";
            for (const auto& [name, type] : types) {
                query += std::format(", {}Value", name);
            }
            query += " FROM SomeTypes ORDER BY Key;";

            auto batches = ExecuteAndCombineBatches(queryClient, query);

            UNIT_ASSERT_C(batches.size() == 1, "Batches must be exactly one");

            const auto& batch = batches.front();
            const auto& schema = batch->schema();

            UNIT_ASSERT_C(schema->field(0)->name() == "Key", "Key column must be present");
            for (size_t i = 0; i < types.size(); ++i) {
                UNIT_ASSERT_C(schema->field(i + 1)->name() == std::format("{}Value", types[i].first), "Column " << types[i].first << "Value must be present");
            }

            auto serializedSchema = NArrow::SerializeSchema(*schema);
            auto serializedBatch = NArrow::SerializeBatchNoCompression(batch);

            auto bulkResult = tableClient.BulkUpsert("/Root/SomeTypes", NYdb::NTable::EDataFormat::ApacheArrow, serializedBatch, serializedSchema).GetValueSync();
            UNIT_ASSERT_C(bulkResult.IsSuccess(), bulkResult.GetIssues().ToString());

            std::string selectQuery = "SELECT ";
            selectQuery += "Key";
            for (const auto& [name, type] : types) {
                selectQuery += std::format(", {}Value", name);
            }
            selectQuery += " FROM SomeTypes ORDER BY Key;";


            auto result = queryClient.ExecuteQuery(selectQuery, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            TString expected = "[";

            expected += R"([1u;[%true];[1u];[-1];[1u];[-1];[1u];[-1];[1u];[-1];[1.];[1.];[20089u];[1735689600u];[1735689600000000u];["1"];["1"];[20089];[1735689600];[1735689600000000];[86400000000];["[1]"];["[1]"];["[1]"];["1"])";
            if (!IsOlap) {
                expected += R"(;[86400000000];[".1e1"];["f9d5cc3f-f1dc-4d9c-b97e-766e57ca4cc1"])";
            }
            expected += "];";

            expected += R"([2u;[%false];[2u];[-2];[2u];[-2];[2u];[-2];[2u];[-2];[2.];[2.];[20121u];[1738454400u];[1738454400000000u];["2"];["2"];[20121];[1738454400];[1738454400000000];[172800000000];["[2]"];["[2]"];["[2]"];["2"])";
            if (!IsOlap) {
                expected += R"(;[172800000000];[".2e1"];["f9d5cc3f-f1dc-4d9c-b97e-766e57ca4cc2"])";
            }
            expected += "];";

            expected += R"([3u;[%true];[3u];[-3];[3u];[-3];[3u];[-3];[3u];[-3];[3.];[3.];[20150u];[1740960000u];[1740960000000000u];["3"];["3"];[20150];[1740960000];[1740960000000000];[259200000000];["[3]"];["[3]"];["[3]"];["3"])";
            if (!IsOlap) {
                expected += R"(;[259200000000];[".3e1"];["f9d5cc3f-f1dc-4d9c-b97e-766e57ca4cc3"])";
            }
            expected += "];";

            expected += R"([4u;[%true];[1u];[-1];[1u];[-1];[1u];[-1];[1u];[-1];[1.];[1.];[20089u];[1735689600u];[1735689600000000u];["1"];["1"];[20089];[1735689600];[1735689600000000];[86400000000];["[1]"];["[1]"];["[1]"];["1"])";
            if (!IsOlap) {
                expected += R"(;[86400000000];[".1e1"];["f9d5cc3f-f1dc-4d9c-b97e-766e57ca4cc1"])";
            }
            expected += "];";

            expected += R"([5u;[%false];[2u];[-2];[2u];[-2];[2u];[-2];[2u];[-2];[2.];[2.];[20121u];[1738454400u];[1738454400000000u];["2"];["2"];[20121];[1738454400];[1738454400000000];[172800000000];["[2]"];["[2]"];["[2]"];["2"])";
            if (!IsOlap) {
                expected += R"(;[172800000000];[".2e1"];["f9d5cc3f-f1dc-4d9c-b97e-766e57ca4cc2"])";
            }
            expected += "];";

            expected += R"([6u;[%true];[3u];[-3];[3u];[-3];[3u];[-3];[3u];[-3];[3.];[3.];[20150u];[1740960000u];[1740960000000000u];["3"];["3"];[20150];[1740960000];[1740960000000000];[259200000000];["[3]"];["[3]"];["[3]"];["3"])";
            if (!IsOlap) {
                expected += R"(;[259200000000];[".3e1"];["f9d5cc3f-f1dc-4d9c-b97e-766e57ca4cc3"])";
            }
            expected += "]]";

            CompareYson(expected, FormatResultSetYson(result.GetResultSet(0)));
        }
    }
}

} // namespace NKikimr::NKqp

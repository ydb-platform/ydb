#include <ydb/library/yql/providers/s3/actors/yql_arrow_column_converters.h>

#include <ydb/library/yql/udfs/common/clickhouse/client/src/Formats/FormatSettings.h>

#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/public/udf/arrow/block_builder.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/deque.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/exception.h>

namespace NYql::NDq {

using namespace NKikimr::NMiniKQL;

namespace {

struct TTestFixture {
    TScopedAlloc Alloc{__LOCATION__};
    TTypeEnvironment Env{Alloc};
    TDeque<TString> Names;
    std::unordered_map<TStringBuf, TType*, THash<TStringBuf>> RowTypes;
    NDB::FormatSettings Settings;
    std::vector<int> ColumnIndices;
    std::vector<TColumnConverter> ColumnConverters;
    TMissingColumns MissingColumns;

    void AddColumn(const TString& name, NUdf::TDataTypeId typeId) {
        RowTypes.emplace(Names.emplace_back(name), TDataType::Create(typeId, Env));
    }

    void AddOptionalColumn(const TString& name, NUdf::TDataTypeId typeId) {
        RowTypes.emplace(Names.emplace_back(name), TOptionalType::Create(TDataType::Create(typeId, Env), Env));
    }

    void Build(const std::shared_ptr<arrow::Schema>& outputSchema, const std::shared_ptr<arrow::Schema>& dataSchema) {
        BuildColumnConverters(outputSchema, dataSchema, ColumnIndices, ColumnConverters, MissingColumns, RowTypes, Settings);
    }

    std::shared_ptr<arrow::RecordBatch> Convert(const std::shared_ptr<arrow::RecordBatch>& batch) {
        return ConvertArrowColumns(batch, ColumnConverters, MissingColumns);
    }
};

template <typename TArrayType, typename TValue>
std::shared_ptr<arrow::Array> MakeArray(const std::vector<TValue>& values) {
    using TBuilder = typename arrow::TypeTraits<TArrayType>::BuilderType;
    // i64 is `long` on darwin while arrow's int64_t is `long long` there, so std::vector<i64> is not a std::vector<value_type>
    const std::vector<typename TBuilder::value_type> converted(values.begin(), values.end());
    TBuilder builder;
    UNIT_ASSERT(builder.AppendValues(converted).ok());
    std::shared_ptr<arrow::Array> array;
    UNIT_ASSERT(builder.Finish(&array).ok());
    return array;
}

template <bool Nullable, typename TArrowType>
void CheckTemporalToString(const std::shared_ptr<arrow::DataType>& sourceType, bool utf8, size_t length,
    const std::vector<typename arrow::TypeTraits<TArrowType>::BuilderType::value_type>& values,
    const std::vector<TString>& strings, bool withNulls = false)
{
    TTestFixture f;
    const auto typeId = utf8 ? NUdf::TDataType<NUdf::TUtf8>::Id : NUdf::TDataType<char*>::Id;
    if constexpr (Nullable) {
        f.AddOptionalColumn("ts", typeId);
    } else {
        f.AddColumn("ts", typeId);
    }
    const auto targetType = utf8 ? arrow::utf8() : arrow::binary();
    typename arrow::TypeTraits<TArrowType>::BuilderType inputBuilder(sourceType, arrow::system_memory_pool());
    TTypeInfoHelper typeInfo;
    NUdf::TStringArrayBuilder<arrow::BinaryType, Nullable> chunkBuilder(typeInfo, targetType, *arrow::system_memory_pool(), length);
    const size_t valuesPerChunk = typeInfo.GetMaxBlockBytes() / strings.front().size();
    size_t nullCount = 0;
    auto isNull = [&](size_t i) {
        // Nulls immediately before and after the payload split, and in a later chunk.
        return withNulls && (i == valuesPerChunk - 1 || i == valuesPerChunk || i == valuesPerChunk + 2 || i == valuesPerChunk + 4 || i == 2 * valuesPerChunk + 3);
    };
    for (size_t i = 0; i < length; ++i) {
        if (isNull(i)) {
            UNIT_ASSERT(inputBuilder.AppendNull().ok());
            chunkBuilder.Add(NUdf::TBlockItem{});
            ++nullCount;
        } else {
            UNIT_ASSERT(inputBuilder.Append(values[i % values.size()]).ok());
            const auto& text = strings[i % strings.size()];
            UNIT_ASSERT_VALUES_EQUAL(text.size(), strings.front().size());
            chunkBuilder.Add(NUdf::TBlockItem(NUdf::TStringRef(text.data(), text.size())));
        }
    }
    // Verify the fixture actually crosses the real string builder's payload limit.
    const auto datum = chunkBuilder.Build(true);
    const size_t chunkCount = Max<size_t>(1, (length - nullCount + valuesPerChunk - 1) / valuesPerChunk);
    UNIT_ASSERT_VALUES_EQUAL(datum.is_array(), chunkCount == 1);
    if (chunkCount > 1) {
        UNIT_ASSERT_VALUES_EQUAL(datum.chunked_array()->num_chunks(), chunkCount);
    }

    std::shared_ptr<arrow::Array> input;
    UNIT_ASSERT(inputBuilder.Finish(&input).ok());
    auto converter = BuildColumnConverter("ts", sourceType, targetType, f.RowTypes.at("ts"), f.Settings);
    const auto output = converter(input);
    UNIT_ASSERT_C(output->ValidateFull().ok(), output->ValidateFull().ToString());
    UNIT_ASSERT(output->type()->Equals(targetType));
    UNIT_ASSERT_VALUES_EQUAL(output->length(), length);
    UNIT_ASSERT_VALUES_EQUAL(output->null_count(), nullCount);
    const auto& binary = static_cast<const arrow::BinaryArray&>(*output);
    for (size_t i = 0; i < length; ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(output->IsNull(i), isNull(i), i);
        if (!isNull(i)) {
            UNIT_ASSERT_VALUES_EQUAL_C(binary.GetString(i), strings[i % strings.size()], i);
        }
    }
    // Consumers require one array per column in a RecordBatch.
    const auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("ts", targetType, Nullable)}), length, {output});
    UNIT_ASSERT(batch->ValidateFull().ok());
}

template <bool Nullable>
void CheckTimestampToString(bool utf8, size_t length, bool withNulls = false) {
    for (const auto unit : {arrow::TimeUnit::SECOND, arrow::TimeUnit::MILLI, arrow::TimeUnit::MICRO}) {
        const i64 scale = unit == arrow::TimeUnit::SECOND ? 1 : unit == arrow::TimeUnit::MILLI ? 1000 : 1000000;
        const i64 fraction = unit == arrow::TimeUnit::SECOND ? 0 : unit == arrow::TimeUnit::MILLI ? 123 : 123456;
        const TString fractionalText = unit == arrow::TimeUnit::SECOND ? "000000" : unit == arrow::TimeUnit::MILLI ? "123000" : "123456";
        CheckTemporalToString<Nullable, arrow::TimestampType>(arrow::timestamp(unit), utf8, length,
            {1712059260 * scale, 1712059261 * scale + fraction, 1712059320 * scale},
            {"2024-04-02T12:01:00.000000Z", "2024-04-02T12:01:01." + fractionalText + "Z", "2024-04-02T12:02:00.000000Z"}, withNulls);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TArrowColumnConvertersTest) {
    Y_UNIT_TEST(TimestampToStringChunkBoundary) {
        for (const size_t length : {1, 9102, 9103}) {
            CheckTimestampToString<false>(false, length);
            CheckTimestampToString<true>(false, length);
        }
    }

    Y_UNIT_TEST(TimestampToUtf8ChunkBoundary) {
        for (const size_t length : {1, 9102, 9103}) {
            CheckTimestampToString<false>(true, length);
            CheckTimestampToString<true>(true, length);
        }
    }

    Y_UNIT_TEST(TimestampToStringMultipleChunks) {
        CheckTimestampToString<false>(false, 30000);
        CheckTimestampToString<true>(false, 30000);
        CheckTimestampToString<true>(false, 30000, true);
    }

    Y_UNIT_TEST(TimestampToUtf8MultipleChunks) {
        CheckTimestampToString<false>(true, 30000);
        CheckTimestampToString<true>(true, 30000);
        CheckTimestampToString<true>(true, 30000, true);
    }

    Y_UNIT_TEST(Date32ToStringChunkBoundary) {
        for (const bool utf8 : {false, true}) {
            for (const size_t length : {24576, 24577, 60000}) {
                CheckTemporalToString<false, arrow::Date32Type>(arrow::date32(), utf8, length,
                    {19815, 19816, 19817}, {"2024-04-02", "2024-04-03", "2024-04-04"});
                CheckTemporalToString<true, arrow::Date32Type>(arrow::date32(), utf8, length,
                    {19815, 19816, 19817}, {"2024-04-02", "2024-04-03", "2024-04-04"});
            }
            CheckTemporalToString<true, arrow::Date32Type>(arrow::date32(), utf8, 60000,
                {19815, 19816, 19817}, {"2024-04-02", "2024-04-03", "2024-04-04"}, true);
        }
    }

    Y_UNIT_TEST(MissingOptionalColumnIsFilledWithNulls) {
        TTestFixture f;
        f.AddColumn("a", NUdf::TDataType<i32>::Id);
        f.AddOptionalColumn("b", NUdf::TDataType<char*>::Id);
        f.AddOptionalColumn("c", NUdf::TDataType<i64>::Id);

        auto outputSchema = arrow::schema({
            arrow::field("a", arrow::int32(), false),
            arrow::field("b", arrow::binary(), true),
            arrow::field("c", arrow::int64(), true),
        });
        auto dataSchema = arrow::schema({
            arrow::field("c", arrow::int64(), true),
            arrow::field("a", arrow::int32(), false),
        });
        f.Build(outputSchema, dataSchema);

        UNIT_ASSERT_VALUES_EQUAL(f.ColumnIndices, (std::vector<int>{1, 0}));
        UNIT_ASSERT_VALUES_EQUAL(f.ColumnConverters.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(f.MissingColumns.Columns.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(f.MissingColumns.Columns[0].OutputIndex, 1u);
        UNIT_ASSERT_VALUES_EQUAL(f.MissingColumns.Columns[0].Field->name(), "b");
        UNIT_ASSERT(f.MissingColumns.Schema->Equals(*outputSchema));

        auto batch = arrow::RecordBatch::Make(
            arrow::schema({outputSchema->field(0), outputSchema->field(2)}), 3,
            {MakeArray<arrow::Int32Type>(std::vector<i32>{1, 2, 3}), MakeArray<arrow::Int64Type>(std::vector<i64>{10, 20, 30})});

        auto converted = f.Convert(batch);
        UNIT_ASSERT(converted->Validate().ok());
        UNIT_ASSERT_VALUES_EQUAL(converted->num_rows(), 3);
        UNIT_ASSERT_VALUES_EQUAL(converted->num_columns(), 3);
        UNIT_ASSERT(converted->schema()->Equals(*outputSchema));

        UNIT_ASSERT(converted->column(0)->Equals(batch->column(0)));
        UNIT_ASSERT(converted->column(2)->Equals(batch->column(1)));

        const auto& nullColumn = converted->column(1);
        UNIT_ASSERT(nullColumn->type()->Equals(arrow::binary()));
        UNIT_ASSERT_VALUES_EQUAL(nullColumn->length(), 3);
        UNIT_ASSERT_VALUES_EQUAL(nullColumn->null_count(), 3);
    }

    Y_UNIT_TEST(MissingColumnsAtBothEnds) {
        TTestFixture f;
        f.AddOptionalColumn("a", NUdf::TDataType<i32>::Id);
        f.AddColumn("b", NUdf::TDataType<i64>::Id);
        f.AddOptionalColumn("c", NUdf::TDataType<double>::Id);

        auto outputSchema = arrow::schema({
            arrow::field("a", arrow::int32(), true),
            arrow::field("b", arrow::int64(), false),
            arrow::field("c", arrow::float64(), true),
        });
        auto dataSchema = arrow::schema({arrow::field("b", arrow::int64(), false)});
        f.Build(outputSchema, dataSchema);

        UNIT_ASSERT_VALUES_EQUAL(f.ColumnIndices, (std::vector<int>{0}));
        UNIT_ASSERT_VALUES_EQUAL(f.MissingColumns.Columns.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(f.MissingColumns.Columns[0].OutputIndex, 0u);
        UNIT_ASSERT_VALUES_EQUAL(f.MissingColumns.Columns[1].OutputIndex, 2u);

        auto batch = arrow::RecordBatch::Make(
            arrow::schema({outputSchema->field(1)}), 2, {MakeArray<arrow::Int64Type>(std::vector<i64>{7, 8})});

        auto converted = f.Convert(batch);
        UNIT_ASSERT(converted->Validate().ok());
        UNIT_ASSERT_VALUES_EQUAL(converted->num_columns(), 3);
        UNIT_ASSERT(converted->schema()->Equals(*outputSchema));
        UNIT_ASSERT(converted->column(0)->type()->Equals(arrow::int32()));
        UNIT_ASSERT_VALUES_EQUAL(converted->column(0)->null_count(), 2);
        UNIT_ASSERT(converted->column(1)->Equals(batch->column(0)));
        UNIT_ASSERT(converted->column(2)->type()->Equals(arrow::float64()));
        UNIT_ASSERT_VALUES_EQUAL(converted->column(2)->null_count(), 2);
    }

    Y_UNIT_TEST(AllColumnsMissing) {
        TTestFixture f;
        f.AddOptionalColumn("a", NUdf::TDataType<i32>::Id);
        f.AddOptionalColumn("b", NUdf::TDataType<char*>::Id);

        auto outputSchema = arrow::schema({
            arrow::field("a", arrow::int32(), true),
            arrow::field("b", arrow::binary(), true),
        });
        auto dataSchema = arrow::schema({arrow::field("x", arrow::int64(), false)});
        f.Build(outputSchema, dataSchema);

        UNIT_ASSERT(f.ColumnIndices.empty());
        UNIT_ASSERT(f.ColumnConverters.empty());
        UNIT_ASSERT_VALUES_EQUAL(f.MissingColumns.Columns.size(), 2);

        auto batch = arrow::RecordBatch::Make(arrow::schema({}), 5, std::vector<std::shared_ptr<arrow::Array>>{});
        auto converted = f.Convert(batch);
        UNIT_ASSERT(converted->Validate().ok());
        UNIT_ASSERT_VALUES_EQUAL(converted->num_rows(), 5);
        UNIT_ASSERT_VALUES_EQUAL(converted->num_columns(), 2);
        UNIT_ASSERT_VALUES_EQUAL(converted->column(0)->null_count(), 5);
        UNIT_ASSERT_VALUES_EQUAL(converted->column(1)->null_count(), 5);
    }

    Y_UNIT_TEST(WideSchemaWithManyMissingColumns) {
        constexpr size_t numColumns = 10000;
        TTestFixture f;
        arrow::FieldVector outputFields;
        arrow::FieldVector dataFields;
        std::vector<std::shared_ptr<arrow::Array>> dataColumns;
        for (size_t i = 0; i < numColumns; ++i) {
            const TString name = TStringBuilder() << "c" << i;
            f.AddOptionalColumn(name, NUdf::TDataType<i32>::Id);
            outputFields.push_back(arrow::field(name, arrow::int32(), true));
            // first half of the columns and every other column of the second half are absent in the file
            if (i >= numColumns / 2 && i % 2 == 0) {
                dataFields.push_back(outputFields.back());
                dataColumns.push_back(MakeArray<arrow::Int32Type>(std::vector<i32>{static_cast<i32>(i), -static_cast<i32>(i)}));
            }
        }
        auto outputSchema = arrow::schema(outputFields);
        auto dataSchema = arrow::schema(dataFields);
        f.Build(outputSchema, dataSchema);

        UNIT_ASSERT_VALUES_EQUAL(f.ColumnIndices.size(), dataColumns.size());
        UNIT_ASSERT_VALUES_EQUAL(f.MissingColumns.Columns.size(), numColumns - dataColumns.size());

        auto batch = arrow::RecordBatch::Make(dataSchema, 2, dataColumns);
        auto converted = f.Convert(batch);
        UNIT_ASSERT(converted->Validate().ok());
        UNIT_ASSERT_VALUES_EQUAL(converted->num_rows(), 2);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(converted->num_columns()), numColumns);
        UNIT_ASSERT(converted->schema()->Equals(*outputSchema));
        for (size_t i = 0; i < numColumns; ++i) {
            const auto& column = converted->column(i);
            UNIT_ASSERT_VALUES_EQUAL_C(column->length(), 2, i);
            if (i >= numColumns / 2 && i % 2 == 0) {
                UNIT_ASSERT_VALUES_EQUAL_C(column->null_count(), 0, i);
                UNIT_ASSERT_VALUES_EQUAL_C(static_cast<const arrow::Int32Array&>(*column).Value(0), static_cast<i32>(i), i);
            } else {
                UNIT_ASSERT_VALUES_EQUAL_C(column->null_count(), 2, i);
            }
        }
    }

    Y_UNIT_TEST(MissingNonOptionalColumnFails) {
        TTestFixture f;
        f.AddColumn("a", NUdf::TDataType<i32>::Id);
        f.AddColumn("b", NUdf::TDataType<char*>::Id);

        auto outputSchema = arrow::schema({
            arrow::field("a", arrow::int32(), false),
            arrow::field("b", arrow::binary(), false),
        });
        auto dataSchema = arrow::schema({arrow::field("a", arrow::int32(), false)});

        UNIT_ASSERT_EXCEPTION_CONTAINS(f.Build(outputSchema, dataSchema), parquet::ParquetException, "Missing field: b");
    }

    Y_UNIT_TEST(NoMissingColumnsKeepsBatch) {
        TTestFixture f;
        f.AddOptionalColumn("a", NUdf::TDataType<i32>::Id);

        auto outputSchema = arrow::schema({arrow::field("a", arrow::int32(), true)});
        auto dataSchema = arrow::schema({arrow::field("a", arrow::int32(), true)});
        f.Build(outputSchema, dataSchema);
        UNIT_ASSERT(f.MissingColumns.Columns.empty());
        UNIT_ASSERT_VALUES_EQUAL(f.ColumnIndices, (std::vector<int>{0}));

        auto batch = arrow::RecordBatch::Make(dataSchema, 2, {MakeArray<arrow::Int32Type>(std::vector<i32>{1, 2})});
        auto converted = f.Convert(batch);
        UNIT_ASSERT(converted->Equals(*batch));
    }
}

} // namespace NYql::NDq

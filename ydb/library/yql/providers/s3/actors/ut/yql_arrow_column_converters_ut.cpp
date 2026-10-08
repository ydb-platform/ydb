#include <ydb/library/yql/providers/s3/actors/yql_arrow_column_converters.h>

#include <ydb/library/yql/udfs/common/clickhouse/client/src/Formats/FormatSettings.h>

#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/public/decimal/yql_decimal.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/deque.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/api.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/reader.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/writer.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/exception.h>

#include <functional>
#include <optional>
#include <tuple>

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

    TType* DecimalType(ui8 precision, ui8 scale, bool optional = true) {
        auto type = TDataDecimalType::Create(precision, scale, Env);
        return optional ? static_cast<TType*>(TOptionalType::Create(type, Env)) : type;
    }

    std::shared_ptr<arrow::Array> ConvertDecimal(const std::shared_ptr<arrow::Array>& input, ui8 precision, ui8 scale, bool optional = true) {
        auto type = DecimalType(precision, scale, optional);
        std::shared_ptr<arrow::DataType> targetType;
        UNIT_ASSERT(ConvertArrowType(type, targetType));
        auto converter = BuildColumnConverter("amount", input->type(), targetType, type, Settings);
        UNIT_ASSERT(converter);
        return converter(input);
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

// Values are unscaled integers, so the fixture does not perform the conversion under test.
std::shared_ptr<arrow::Array> MakeDecimalArray(int precision, int scale, const std::vector<std::optional<std::string>>& values) {
    arrow::Decimal128Builder builder(arrow::decimal128(precision, scale));
    for (const auto& value : values) {
        UNIT_ASSERT(value ? builder.Append(arrow::Decimal128(*value)).ok() : builder.AppendNull().ok());
    }
    std::shared_ptr<arrow::Array> array;
    UNIT_ASSERT(builder.Finish(&array).ok());
    UNIT_ASSERT(array->ValidateFull().ok());
    return array;
}

std::vector<TString> GetDecimalValues(const std::shared_ptr<arrow::Array>& array, ui8 precision, ui8 scale) {
    UNIT_ASSERT(array->ValidateFull().ok());
    const auto& typed = static_cast<const arrow::FixedSizeBinaryArray&>(*array);
    std::vector<TString> result;
    for (i64 i = 0; i < typed.length(); ++i) {
        if (typed.IsNull(i)) {
            result.push_back("NULL");
        } else {
            NDecimal::TInt128 value;
            memcpy(&value, typed.GetValue(i), sizeof(value));
            result.push_back(NDecimal::ToString(value, precision, scale));
        }
    }
    return result;
}

// Exercise real Parquet serialization and decoding, including row groups and chunks.
std::shared_ptr<arrow::Table> ReadDecimalParquet(const arrow::ArrayVector& chunks) {
    const auto type = chunks.front()->type();
    const auto table = arrow::Table::Make(arrow::schema({arrow::field("amount", type)}),
        {std::make_shared<arrow::ChunkedArray>(chunks, type)});
    const auto stream = arrow::io::BufferOutputStream::Create();
    UNIT_ASSERT_C(stream.ok(), stream.status().ToString());
    const auto status = parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), *stream, 2);
    UNIT_ASSERT_C(status.ok(), status.ToString());
    const auto buffer = (*stream)->Finish();
    UNIT_ASSERT_C(buffer.ok(), buffer.status().ToString());
    std::unique_ptr<parquet::arrow::FileReader> reader;
    UNIT_ASSERT(parquet::arrow::OpenFile(std::make_shared<arrow::io::BufferReader>(*buffer),
        arrow::default_memory_pool(), &reader).ok());
    reader->set_batch_size(2);
    std::vector<std::shared_ptr<arrow::Table>> rowGroups;
    for (int i = 0; i < reader->num_row_groups(); ++i) {
        std::shared_ptr<arrow::Table> rowGroup;
        UNIT_ASSERT(reader->RowGroup(i)->ReadTable(&rowGroup).ok());
        rowGroups.push_back(rowGroup);
    }
    const auto decoded = arrow::ConcatenateTables(rowGroups);
    UNIT_ASSERT_C(decoded.ok(), decoded.status().ToString());
    UNIT_ASSERT((*decoded)->ValidateFull().ok());
    UNIT_ASSERT((*decoded)->field(0)->type()->Equals(type));
    return *decoded;
}

std::vector<TString> ConvertDecimalParquet(const std::shared_ptr<arrow::Table>& table, ui8 precision, ui8 scale) {
    TTestFixture f;
    f.RowTypes.emplace("amount", f.DecimalType(precision, scale));
    f.Build(arrow::schema({arrow::field("amount", arrow::fixed_size_binary(16))}), table->schema());
    arrow::TableBatchReader reader(*table);
    reader.set_chunksize(2);
    std::vector<TString> values;
    std::shared_ptr<arrow::RecordBatch> batch;
    while (true) {
        UNIT_ASSERT(reader.ReadNext(&batch).ok());
        if (!batch) {
            break;
        }
        const auto converted = f.Convert(batch);
        const auto chunk = GetDecimalValues(converted->column(0), precision, scale);
        values.insert(values.end(), chunk.begin(), chunk.end());
    }
    return values;
}

void AssertDecimalConversionError(const std::function<void()>& convert, TStringBuf source, TStringBuf target, TStringBuf reason) {
    try {
        convert();
    } catch (const parquet::ParquetException& e) {
        const TString message(e.what());
        UNIT_ASSERT_STRING_CONTAINS(message, "amount");
        UNIT_ASSERT_STRING_CONTAINS(message, source);
        UNIT_ASSERT_STRING_CONTAINS(message, target);
        UNIT_ASSERT_STRING_CONTAINS(message, reason);
        return;
    }
    UNIT_FAIL("Expected a Decimal conversion error");
}

} // namespace

Y_UNIT_TEST_SUITE(TArrowColumnConvertersTest) {
    Y_UNIT_TEST(DecimalMatchingNonDefaultControl) {
        TTestFixture f;
        const auto input = MakeDecimalArray(10, 2, {"12345", "-1", "0", std::nullopt});
        UNIT_ASSERT_VALUES_EQUAL(GetDecimalValues(f.ConvertDecimal(input, 10, 2), 10, 2),
            (std::vector<TString>{"123.45", "-0.01", "0", "NULL"}));
    }

    Y_UNIT_TEST(DecimalScaleIncreasePreservesValue) {
        TTestFixture f;
        for (const auto precision : {10, 12}) {
            const auto input = MakeDecimalArray(precision, 2, {"999", "12345", "-1", "0", std::nullopt, "67890"})->Slice(1);
            const auto output = f.ConvertDecimal(input, 22, 9);
            UNIT_ASSERT_VALUES_EQUAL(GetDecimalValues(output, 22, 9),
                (std::vector<TString>{"123.45", "-0.01", "0", "NULL", "678.9"}));
        }
    }

    Y_UNIT_TEST(DecimalMatchingNonDefaultAndSlicedInput) {
        TTestFixture f;
        const auto input = MakeDecimalArray(10, 2, {"100", "200", std::nullopt, "-300", "0"});
        for (const auto& array : {input, input->Slice(1, 3), input->Slice(3, 0)}) {
            const auto output = f.ConvertDecimal(array, 10, 2);
            UNIT_ASSERT_VALUES_EQUAL(output->offset(), array->offset());
            UNIT_ASSERT_VALUES_EQUAL(output->null_count(), array->null_count());
            UNIT_ASSERT(output->data()->buffers[1] == array->data()->buffers[1]);
        }
        UNIT_ASSERT_VALUES_EQUAL(GetDecimalValues(f.ConvertDecimal(input->Slice(1, 3), 10, 2), 10, 2),
            (std::vector<TString>{"2", "NULL", "-3"}));
        const auto required = MakeDecimalArray(10, 2, {"100", "-200", "300"})->Slice(2);
        UNIT_ASSERT_VALUES_EQUAL(GetDecimalValues(f.ConvertDecimal(required, 10, 2, false), 10, 2),
            (std::vector<TString>{"3"}));
    }

    Y_UNIT_TEST(DecimalWiderPrecisionPreservesOffsetAndNulls) {
        TTestFixture f;
        const auto input = MakeDecimalArray(5, 2,
            {"1", std::nullopt, "2", std::nullopt, "3", "4", "5", std::nullopt, "6", "-99999", std::nullopt, "0"})->Slice(9);
        const auto output = f.ConvertDecimal(input, 22, 2);
        UNIT_ASSERT_VALUES_EQUAL(output->offset(), 9);
        UNIT_ASSERT_VALUES_EQUAL(GetDecimalValues(output, 22, 2),
            (std::vector<TString>{"-999.99", "NULL", "0"}));
    }

    Y_UNIT_TEST(DecimalExactScaleDecreasePreservesValue) {
        TTestFixture f;
        const auto input = MakeDecimalArray(12, 4, {"1", "1234500", "-100", std::nullopt, "0"})->Slice(1);
        UNIT_ASSERT_VALUES_EQUAL(GetDecimalValues(f.ConvertDecimal(input, 5, 2), 5, 2),
            (std::vector<TString>{"123.45", "-0.01", "NULL", "0"}));
    }

    Y_UNIT_TEST(DecimalNonExactScaleDecreaseIsRejected) {
        TTestFixture f;
        for (const auto& value : {"1234501", "-1234501"}) {
            AssertDecimalConversionError([&] { f.ConvertDecimal(MakeDecimalArray(12, 4, {value}), 22, 2); },
                "Decimal(12, 4)", "Decimal(22, 2)", "lose data");
        }
    }

    Y_UNIT_TEST(DecimalNarrowerPrecisionChecksValues) {
        TTestFixture f;
        const auto input = MakeDecimalArray(38, 2, {"100000", "99999", "-99999", std::nullopt, "0"})->Slice(1);
        UNIT_ASSERT_VALUES_EQUAL(GetDecimalValues(f.ConvertDecimal(input, 5, 2), 5, 2),
            (std::vector<TString>{"999.99", "-999.99", "NULL", "0"}));
        for (const auto& value : {"100000", "-100000"}) {
            AssertDecimalConversionError([&] { f.ConvertDecimal(MakeDecimalArray(38, 2, {value}), 5, 2); },
                "Decimal(38, 2)", "Decimal(5, 2)", "precision");
        }
    }

    Y_UNIT_TEST(DecimalScaleIncreaseChecksTargetRange) {
        TTestFixture f;
        const auto input = MakeDecimalArray(16, 2, {"999999999999999", "-999999999999999", "0"});
        UNIT_ASSERT_VALUES_EQUAL(GetDecimalValues(f.ConvertDecimal(input, 22, 9), 22, 9),
            (std::vector<TString>{"9999999999999.99", "-9999999999999.99", "0"}));
        for (const auto& value : {"1000000000000000", "-1000000000000000"}) {
            AssertDecimalConversionError([&] { f.ConvertDecimal(MakeDecimalArray(16, 2, {value}), 22, 9); },
                "Decimal(16, 2)", "Decimal(22, 9)", "precision");
        }
    }

    Y_UNIT_TEST(DecimalRescaleCannotWrapInt128) {
        TTestFixture f;
        // (2^94 + 1) * 10^34 wraps to 10^34 in 128 bits, which fits precision 35.
        for (const auto& value : {"19807040628566084398385987585", "-19807040628566084398385987585"}) {
            AssertDecimalConversionError([&] { f.ConvertDecimal(MakeDecimalArray(29, 0, {value}), 35, 34); },
                "Decimal(29, 0)", "Decimal(35, 34)", "precision");
        }
    }

    Y_UNIT_TEST(DecimalScaleIncreaseWithNoIntegerDigits) {
        TTestFixture f;
        const auto input = MakeDecimalArray(38, 0, {"0", std::nullopt});
        UNIT_ASSERT_VALUES_EQUAL(GetDecimalValues(f.ConvertDecimal(input, 35, 35), 35, 35),
            (std::vector<TString>{"0", "NULL"}));
        for (const auto& value : {"1", "-1"}) {
            AssertDecimalConversionError([&] { f.ConvertDecimal(MakeDecimalArray(38, 0, {value}), 35, 35); },
                "Decimal(38, 0)", "Decimal(35, 35)", "precision");
        }
    }

    Y_UNIT_TEST(DecimalScaleDeltaBeyondMultiplierTable) {
        TTestFixture f;
        // Arrow permits scales outside Parquet's [0, 38] range. Neither direction
        // may index Rescale's multiplier table beyond 38, even for zero.
        for (const auto scale : {39, -39}) {
            const auto zeros = MakeDecimalArray(38, scale, {"1", "0", std::nullopt})->Slice(1);
            UNIT_ASSERT_VALUES_EQUAL(GetDecimalValues(f.ConvertDecimal(zeros, 35, 0), 35, 0),
                (std::vector<TString>{"0", "NULL"}));
            for (const auto& input : {MakeDecimalArray(38, scale, {std::nullopt}), MakeDecimalArray(38, scale, {})}) {
                const auto output = f.ConvertDecimal(input, 35, 0);
                UNIT_ASSERT(output->ValidateFull().ok());
                UNIT_ASSERT_VALUES_EQUAL(output->length(), input->length());
                UNIT_ASSERT_VALUES_EQUAL(output->null_count(), input->null_count());
            }
            const TString source = TStringBuilder() << "Decimal(38, " << scale << ")";
            for (const auto& value : {"1", "-1"}) {
                AssertDecimalConversionError([&] { f.ConvertDecimal(MakeDecimalArray(38, scale, {value}), 35, 0); },
                    source, "Decimal(35, 0)", scale > 0 ? "lose data" : "precision");
            }
        }
    }

    Y_UNIT_TEST(DecimalMaximumPrecisionBoundary) {
        TTestFixture f;
        const auto input = MakeDecimalArray(38, 0,
            {"99999999999999999999999999999999999", "-99999999999999999999999999999999999"});
        UNIT_ASSERT_VALUES_EQUAL(GetDecimalValues(f.ConvertDecimal(input, 35, 0), 35, 0),
            (std::vector<TString>{"99999999999999999999999999999999999", "-99999999999999999999999999999999999"}));
        AssertDecimalConversionError([&] { f.ConvertDecimal(MakeDecimalArray(38, 0,
            {"100000000000000000000000000000000000"}), 35, 0); },
            "Decimal(38, 0)", "Decimal(35, 0)", "precision");
    }

    Y_UNIT_TEST(DecimalAllNullAndEmptyArrays) {
        TTestFixture f;
        for (const auto& input : {MakeDecimalArray(38, 38, {std::nullopt, std::nullopt})->Slice(1), MakeDecimalArray(38, 38, {})}) {
            const auto output = f.ConvertDecimal(input, 5, 2);
            UNIT_ASSERT(output->ValidateFull().ok());
            UNIT_ASSERT_VALUES_EQUAL(output->length(), input->length());
            UNIT_ASSERT_VALUES_EQUAL(output->null_count(), input->length());
        }
    }

    Y_UNIT_TEST(DecimalParquetScaleIncreaseAndChunks) {
        const auto table = ReadDecimalParquet({
            MakeDecimalArray(12, 2, {"0", "12345", "-1"})->Slice(1),
            MakeDecimalArray(12, 2, {std::nullopt, "0", "67890"})});
        UNIT_ASSERT(table->column(0)->num_chunks() > 1);
        UNIT_ASSERT_VALUES_EQUAL(ConvertDecimalParquet(table, 22, 9),
            (std::vector<TString>{"123.45", "-0.01", "NULL", "0", "678.9"}));
    }

    Y_UNIT_TEST(DecimalParquetExactScaleDecrease) {
        const auto table = ReadDecimalParquet({MakeDecimalArray(12, 4, {"1234500", "-100", "0", std::nullopt})});
        UNIT_ASSERT_VALUES_EQUAL(ConvertDecimalParquet(table, 5, 2),
            (std::vector<TString>{"123.45", "-0.01", "0", "NULL"}));
    }

    Y_UNIT_TEST(DecimalParquetLossyScaleDecreaseIsRejected) {
        const auto table = ReadDecimalParquet({MakeDecimalArray(12, 4, {"1234500", std::nullopt, "-1234501"})});
        AssertDecimalConversionError([&] { ConvertDecimalParquet(table, 22, 2); },
            "Decimal(12, 4)", "Decimal(22, 2)", "lose data");
    }

    Y_UNIT_TEST(DecimalParquetNarrowerPrecision) {
        const auto table = ReadDecimalParquet({MakeDecimalArray(12, 2, {"99999", "-99999", "0", std::nullopt})});
        UNIT_ASSERT_VALUES_EQUAL(ConvertDecimalParquet(table, 5, 2),
            (std::vector<TString>{"999.99", "-999.99", "0", "NULL"}));
        const auto overflow = ReadDecimalParquet({MakeDecimalArray(12, 2, {"99999", std::nullopt, "-100000"})});
        AssertDecimalConversionError([&] { ConvertDecimalParquet(overflow, 5, 2); },
            "Decimal(12, 2)", "Decimal(5, 2)", "precision");
    }

    Y_UNIT_TEST(DecimalParquetRescaleOverflowIsRejected) {
        const auto table = ReadDecimalParquet({MakeDecimalArray(29, 0, {"19807040628566084398385987585"})});
        AssertDecimalConversionError([&] { ConvertDecimalParquet(table, 35, 34); },
            "Decimal(29, 0)", "Decimal(35, 34)", "precision");
    }

    Y_UNIT_TEST(DecimalParquetDifferentFileSchemas) {
        const std::vector<std::tuple<int, int, std::string>> schemas = {
            {12, 2, "12345"}, {22, 9, "123450000000"}, {14, 4, "1234500"}};
        for (const auto& [precision, scale, value] : schemas) {
            const auto table = ReadDecimalParquet({MakeDecimalArray(precision, scale, {value, std::nullopt})});
            UNIT_ASSERT_VALUES_EQUAL(ConvertDecimalParquet(table, 22, 9), (std::vector<TString>{"123.45", "NULL"}));
        }
    }

    Y_UNIT_TEST(DecimalParquetMatchingNonDefaultWriteRead) {
        TTestFixture f;
        const auto input = MakeDecimalArray(10, 2, {"12345", "-9999999999", "0", std::nullopt});
        const auto block = f.ConvertDecimal(input, 10, 2);
        const auto writer = BuildOutputColumnConverter("amount", f.DecimalType(10, 2));
        const auto fileArray = writer(block);
        UNIT_ASSERT(fileArray->type()->Equals(arrow::decimal128(10, 2)));
        const auto table = ReadDecimalParquet({fileArray});
        UNIT_ASSERT_VALUES_EQUAL(ConvertDecimalParquet(table, 10, 2),
            (std::vector<TString>{"123.45", "-99999999.99", "0", "NULL"}));
    }

    Y_UNIT_TEST(Decimal256IsExplicitlyRejected) {
        TTestFixture f;
        arrow::Decimal256Builder builder(arrow::decimal256(40, 2));
        UNIT_ASSERT(builder.Append(arrow::Decimal256("12345")).ok());
        std::shared_ptr<arrow::Array> input;
        UNIT_ASSERT(builder.Finish(&input).ok());
        AssertDecimalConversionError([&] { f.ConvertDecimal(input, 22, 9); },
            "Decimal256(40, 2)", "Decimal(22, 9)", "Unsupported");
        const auto table = ReadDecimalParquet({input});
        AssertDecimalConversionError([&] { ConvertDecimalParquet(table, 22, 9); },
            "Decimal256(40, 2)", "Decimal(22, 9)", "Unsupported");
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

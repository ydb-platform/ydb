#include <ydb/library/yql/providers/s3/actors/yql_arrow_column_converters.h>

#include <ydb/library/yql/udfs/common/clickhouse/client/src/Formats/FormatSettings.h>

#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_node.h>

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
    typename arrow::TypeTraits<TArrayType>::BuilderType builder;
    UNIT_ASSERT(builder.AppendValues(values).ok());
    std::shared_ptr<arrow::Array> array;
    UNIT_ASSERT(builder.Finish(&array).ok());
    return array;
}

} // namespace

Y_UNIT_TEST_SUITE(TArrowColumnConvertersTest) {
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

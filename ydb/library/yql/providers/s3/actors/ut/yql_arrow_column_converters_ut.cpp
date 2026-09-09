#include <ydb/library/yql/providers/s3/actors/yql_arrow_column_converters.h>

#include <ydb/library/yql/udfs/common/clickhouse/client/src/Formats/FormatSettings.h>

#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_node.h>

#include <library/cpp/testing/unittest/registar.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/exception.h>

namespace NYql::NDq {

using namespace NKikimr::NMiniKQL;

namespace {

struct TTestFixture {
    TScopedAlloc Alloc{__LOCATION__};
    TTypeEnvironment Env{Alloc};
    std::unordered_map<TStringBuf, TType*, THash<TStringBuf>> RowTypes;
    NDB::FormatSettings Settings;

    void AddColumn(TStringBuf name, NUdf::TDataTypeId typeId) {
        RowTypes.emplace(name, TDataType::Create(typeId, Env));
    }

    void AddOptionalColumn(TStringBuf name, NUdf::TDataTypeId typeId) {
        RowTypes.emplace(name, TOptionalType::Create(TDataType::Create(typeId, Env), Env));
    }
};

template <typename TArrayType, typename TValue>
std::shared_ptr<arrow::Array> MakeArray(const std::vector<TValue>& values) {
    typename arrow::TypeTraits<TArrayType>::BuilderType builder;
    ARROW_UNUSED(builder.AppendValues(values));
    std::shared_ptr<arrow::Array> array;
    ARROW_UNUSED(builder.Finish(&array));
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

        std::vector<int> columnIndices;
        std::vector<TColumnConverter> columnConverters;
        std::vector<TMissingColumn> missingColumns;
        BuildColumnConverters(outputSchema, dataSchema, columnIndices, columnConverters, missingColumns, f.RowTypes, f.Settings);

        UNIT_ASSERT_VALUES_EQUAL(columnIndices, (std::vector<int>{1, 0}));
        UNIT_ASSERT_VALUES_EQUAL(columnConverters.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(missingColumns.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(missingColumns[0].OutputIndex, 1);
        UNIT_ASSERT_VALUES_EQUAL(missingColumns[0].Field->name(), "b");

        auto batch = arrow::RecordBatch::Make(
            arrow::schema({outputSchema->field(0), outputSchema->field(2)}), 3,
            {MakeArray<arrow::Int32Type>(std::vector<i32>{1, 2, 3}), MakeArray<arrow::Int64Type>(std::vector<i64>{10, 20, 30})});

        auto converted = ConvertArrowColumns(batch, columnConverters, missingColumns);
        UNIT_ASSERT(converted->Validate().ok());
        UNIT_ASSERT_VALUES_EQUAL(converted->num_rows(), 3);
        UNIT_ASSERT_VALUES_EQUAL(converted->num_columns(), 3);
        UNIT_ASSERT_VALUES_EQUAL(converted->schema()->field_names(), (std::vector<std::string>{"a", "b", "c"}));

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

        std::vector<int> columnIndices;
        std::vector<TColumnConverter> columnConverters;
        std::vector<TMissingColumn> missingColumns;
        BuildColumnConverters(outputSchema, dataSchema, columnIndices, columnConverters, missingColumns, f.RowTypes, f.Settings);

        UNIT_ASSERT_VALUES_EQUAL(columnIndices, (std::vector<int>{0}));
        UNIT_ASSERT_VALUES_EQUAL(missingColumns.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(missingColumns[0].OutputIndex, 0);
        UNIT_ASSERT_VALUES_EQUAL(missingColumns[1].OutputIndex, 2);

        auto batch = arrow::RecordBatch::Make(
            arrow::schema({outputSchema->field(1)}), 2, {MakeArray<arrow::Int64Type>(std::vector<i64>{7, 8})});

        auto converted = ConvertArrowColumns(batch, columnConverters, missingColumns);
        UNIT_ASSERT(converted->Validate().ok());
        UNIT_ASSERT_VALUES_EQUAL(converted->num_columns(), 3);
        UNIT_ASSERT_VALUES_EQUAL(converted->schema()->field_names(), (std::vector<std::string>{"a", "b", "c"}));
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

        std::vector<int> columnIndices;
        std::vector<TColumnConverter> columnConverters;
        std::vector<TMissingColumn> missingColumns;
        BuildColumnConverters(outputSchema, dataSchema, columnIndices, columnConverters, missingColumns, f.RowTypes, f.Settings);

        UNIT_ASSERT(columnIndices.empty());
        UNIT_ASSERT(columnConverters.empty());
        UNIT_ASSERT_VALUES_EQUAL(missingColumns.size(), 2);

        auto batch = arrow::RecordBatch::Make(arrow::schema({}), 5, std::vector<std::shared_ptr<arrow::Array>>{});
        auto converted = ConvertArrowColumns(batch, columnConverters, missingColumns);
        UNIT_ASSERT(converted->Validate().ok());
        UNIT_ASSERT_VALUES_EQUAL(converted->num_rows(), 5);
        UNIT_ASSERT_VALUES_EQUAL(converted->num_columns(), 2);
        UNIT_ASSERT_VALUES_EQUAL(converted->column(0)->null_count(), 5);
        UNIT_ASSERT_VALUES_EQUAL(converted->column(1)->null_count(), 5);
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

        std::vector<int> columnIndices;
        std::vector<TColumnConverter> columnConverters;
        std::vector<TMissingColumn> missingColumns;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            BuildColumnConverters(outputSchema, dataSchema, columnIndices, columnConverters, missingColumns, f.RowTypes, f.Settings),
            parquet::ParquetException, "Missing field: b");
    }

    Y_UNIT_TEST(NoMissingColumnsKeepsBatch) {
        TTestFixture f;
        f.AddOptionalColumn("a", NUdf::TDataType<i32>::Id);

        auto outputSchema = arrow::schema({arrow::field("a", arrow::int32(), true)});
        auto dataSchema = arrow::schema({arrow::field("a", arrow::int32(), true)});

        std::vector<int> columnIndices;
        std::vector<TColumnConverter> columnConverters;
        std::vector<TMissingColumn> missingColumns;
        BuildColumnConverters(outputSchema, dataSchema, columnIndices, columnConverters, missingColumns, f.RowTypes, f.Settings);
        UNIT_ASSERT(missingColumns.empty());
        UNIT_ASSERT_VALUES_EQUAL(columnIndices, (std::vector<int>{0}));

        auto batch = arrow::RecordBatch::Make(dataSchema, 2, {MakeArray<arrow::Int32Type>(std::vector<i32>{1, 2})});
        auto converted = ConvertArrowColumns(batch, columnConverters, missingColumns);
        UNIT_ASSERT(converted->Equals(*batch));
    }
}

} // namespace NYql::NDq

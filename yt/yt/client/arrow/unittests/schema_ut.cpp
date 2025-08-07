#include <yt/yt/client/arrow/schema.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/schema.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NArrow {
namespace {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TEST(TArrowSchemaConverterTest, ConvertSimpleTypes)
{
    const auto arrowSchema = arrow::schema({
        arrow::field("f_null", arrow::null(), false),
        arrow::field("f_boolean", arrow::boolean(), false),
        arrow::field("f_int8", arrow::int8(), false),
        arrow::field("f_int16", arrow::int16(), false),
        arrow::field("f_int32", arrow::int32(), false),
        arrow::field("f_int64", arrow::int64(), false),
        arrow::field("f_uint8", arrow::uint8(), false),
        arrow::field("f_uint16", arrow::uint16(), false),
        arrow::field("f_uint32", arrow::uint32(), false),
        arrow::field("f_uint64", arrow::uint64(), false),
        arrow::field("f_float16", arrow::float16(), false),
        arrow::field("f_float32", arrow::float32(), false),
        arrow::field("f_float64", arrow::float64(), false),
        arrow::field("f_utf8", arrow::utf8(), false),
        arrow::field("f_large_utf8", arrow::large_utf8(), false),
        arrow::field("f_binary", arrow::binary(), false),
        arrow::field("f_large_binary", arrow::large_binary(), false),
        arrow::field("f_decimal", arrow::decimal(35, 18), false),
        arrow::field("f_date32", arrow::date32(), false),
        arrow::field("f_date64", arrow::date64(), false),
        arrow::field("f_timestamp", arrow::timestamp(arrow::TimeUnit::MICRO), false),
    });

    const auto result = CreateYTTableSchemaFromArrowSchema(arrowSchema);

    const auto expected = TTableSchema({
        TColumnSchema("f_null", SimpleLogicalType(ESimpleLogicalValueType::Null)),
        TColumnSchema("f_boolean", SimpleLogicalType(ESimpleLogicalValueType::Boolean)),
        TColumnSchema("f_int8", SimpleLogicalType(ESimpleLogicalValueType::Int8)),
        TColumnSchema("f_int16", SimpleLogicalType(ESimpleLogicalValueType::Int16)),
        TColumnSchema("f_int32", SimpleLogicalType(ESimpleLogicalValueType::Int32)),
        TColumnSchema("f_int64", SimpleLogicalType(ESimpleLogicalValueType::Int64)),
        TColumnSchema("f_uint8", SimpleLogicalType(ESimpleLogicalValueType::Uint8)),
        TColumnSchema("f_uint16", SimpleLogicalType(ESimpleLogicalValueType::Uint16)),
        TColumnSchema("f_uint32", SimpleLogicalType(ESimpleLogicalValueType::Uint32)),
        TColumnSchema("f_uint64", SimpleLogicalType(ESimpleLogicalValueType::Uint64)),
        TColumnSchema("f_float16", SimpleLogicalType(ESimpleLogicalValueType::Float)),
        TColumnSchema("f_float32", SimpleLogicalType(ESimpleLogicalValueType::Float)),
        TColumnSchema("f_float64", SimpleLogicalType(ESimpleLogicalValueType::Double)),
        TColumnSchema("f_utf8", SimpleLogicalType(ESimpleLogicalValueType::String)),
        TColumnSchema("f_large_utf8", SimpleLogicalType(ESimpleLogicalValueType::String)),
        TColumnSchema("f_binary", SimpleLogicalType(ESimpleLogicalValueType::Any)),
        TColumnSchema("f_large_binary", SimpleLogicalType(ESimpleLogicalValueType::Any)),
        TColumnSchema("f_decimal", DecimalLogicalType(35,18)),
        TColumnSchema("f_date32", SimpleLogicalType(ESimpleLogicalValueType::Int32)),
        TColumnSchema("f_date64", SimpleLogicalType(ESimpleLogicalValueType::Int64)),
        TColumnSchema("f_timestamp", SimpleLogicalType(ESimpleLogicalValueType::Int64)),
    });

    EXPECT_EQ(expected, *result);
}

TEST(TArrowSchemaConverterTest, ConvertComplexTypes)
{
    const auto complexSchema = arrow::schema({
        arrow::field("f_array", arrow::list(arrow::field("item", arrow::int32(), false)), false),
        arrow::field("f_struct", arrow::struct_({
            arrow::field("nested_num", arrow::int32(), false),
            arrow::field("nested_struct", arrow::struct_({
                arrow::field("inner_list", arrow::list(arrow::field("item", arrow::int32(), false)), false),
                arrow::field("inner_num", arrow::int32(), false)
            }), false)
        }), false),
        arrow::field("f_map", arrow::map(arrow::utf8(), arrow::uint64()), false),
        arrow::field("f_opt", arrow::int32())
    });

    const auto result = CreateYTTableSchemaFromArrowSchema(complexSchema);
    const auto expected = TTableSchema({
        TColumnSchema("f_array", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int32))),
        TColumnSchema("f_struct", StructLogicalType({
            TStructField("nested_num", SimpleLogicalType(ESimpleLogicalValueType::Int32)),
            TStructField("nested_struct", StructLogicalType({
                TStructField("inner_list", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int32))),
                TStructField("inner_num", SimpleLogicalType(ESimpleLogicalValueType::Int32))
            })),
        })),
        TColumnSchema("f_map", DictLogicalType(
            SimpleLogicalType(ESimpleLogicalValueType::String),
            OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Uint64))
        )),
        TColumnSchema("f_opt", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int32)))
    });

    EXPECT_EQ(expected, *result);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NArrow

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
    const auto arrowSchema = arrow20::schema({
        arrow20::field("f_null", arrow20::null(), false),
        arrow20::field("f_boolean", arrow20::boolean(), false),
        arrow20::field("f_int8", arrow20::int8(), false),
        arrow20::field("f_int16", arrow20::int16(), false),
        arrow20::field("f_int32", arrow20::int32(), false),
        arrow20::field("f_int64", arrow20::int64(), false),
        arrow20::field("f_uint8", arrow20::uint8(), false),
        arrow20::field("f_uint16", arrow20::uint16(), false),
        arrow20::field("f_uint32", arrow20::uint32(), false),
        arrow20::field("f_uint64", arrow20::uint64(), false),
        arrow20::field("f_float16", arrow20::float16(), false),
        arrow20::field("f_float32", arrow20::float32(), false),
        arrow20::field("f_float64", arrow20::float64(), false),
        arrow20::field("f_utf8", arrow20::utf8(), false),
        arrow20::field("f_large_utf8", arrow20::large_utf8(), false),
        arrow20::field("f_binary", arrow20::binary(), false),
        arrow20::field("f_large_binary", arrow20::large_binary(), false),
        arrow20::field("f_decimal", arrow20::smallest_decimal(35, 18), false),
        arrow20::field("f_date32", arrow20::date32(), false),
        arrow20::field("f_date64", arrow20::date64(), false),
        arrow20::field("f_timestamp", arrow20::timestamp(arrow20::TimeUnit::MICRO), false),
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
    const auto complexSchema = arrow20::schema({
        arrow20::field("f_array", arrow20::list(arrow20::field("item", arrow20::int32(), false)), false),
        arrow20::field("f_struct", arrow20::struct_({
            arrow20::field("nested_num", arrow20::int32(), false),
            arrow20::field("nested_struct", arrow20::struct_({
                arrow20::field("inner_list", arrow20::list(arrow20::field("item", arrow20::int32(), false)), false),
                arrow20::field("inner_num", arrow20::int32(), false)
            }), false)
        }), false),
        arrow20::field("f_map", arrow20::map(arrow20::utf8(), arrow20::uint64()), false),
        arrow20::field("f_opt", arrow20::int32())
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

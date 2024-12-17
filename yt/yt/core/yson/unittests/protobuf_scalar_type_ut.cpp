#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/yson/protobuf_interop.h>

#include <yt/yt/core/yson/unittests/proto/protobuf_scalar_type_ut.pb.h>

#include <google/protobuf/descriptor.h>

namespace NYT::NYson {
namespace {

using namespace google::protobuf;

////////////////////////////////////////////////////////////////////////////////

#define EXPECT_SCALAR_TYPE(messageType, path, expectedType) \
    do { \
        auto type = ReflectProtobufMessageType<NProto::messageType>(); \
        auto element = ResolveProtobufElementByYPath(type, path).Element; \
        EXPECT_TRUE(std::holds_alternative<std::unique_ptr<TProtobufScalarElement>>(element)); \
        int fieldType = static_cast<int>( \
            std::get<std::unique_ptr<TProtobufScalarElement>>(element)->Type); \
        EXPECT_EQ(fieldType, expectedType); \
    } while (false);

TEST(TProtobufScalarTypeTest, Types)
{
    EXPECT_SCALAR_TYPE(TExampleMessage, "/int32_field", FieldDescriptor::TYPE_INT32);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/uint32_field", FieldDescriptor::TYPE_UINT32);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/sint32_field", FieldDescriptor::TYPE_SINT32);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/fixed32_field", FieldDescriptor::TYPE_FIXED32);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/sfixed32_field", FieldDescriptor::TYPE_SFIXED32);

    EXPECT_SCALAR_TYPE(TExampleMessage, "/int64_field", FieldDescriptor::TYPE_INT64);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/uint64_field", FieldDescriptor::TYPE_UINT64);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/sint64_field", FieldDescriptor::TYPE_SINT64);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/fixed64_field", FieldDescriptor::TYPE_FIXED64);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/sfixed64_field", FieldDescriptor::TYPE_SFIXED64);

    EXPECT_SCALAR_TYPE(TExampleMessage, "/bool_field", FieldDescriptor::TYPE_BOOL);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/string_field", FieldDescriptor::TYPE_STRING);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/float_field", FieldDescriptor::TYPE_FLOAT);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/double_field", FieldDescriptor::TYPE_DOUBLE);
}

TEST(TProtobufScalarTypeTest, RepeatedNestedTypes)
{
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_nested_message/0/int32_field", FieldDescriptor::TYPE_INT32);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_nested_message/0/uint32_field", FieldDescriptor::TYPE_UINT32);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_nested_message/0/sint32_field", FieldDescriptor::TYPE_SINT32);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_nested_message/0/fixed32_field", FieldDescriptor::TYPE_FIXED32);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_nested_message/0/sfixed32_field", FieldDescriptor::TYPE_SFIXED32);

    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_nested_message/0/int64_field", FieldDescriptor::TYPE_INT64);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_nested_message/0/uint64_field", FieldDescriptor::TYPE_UINT64);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_nested_message/0/sint64_field", FieldDescriptor::TYPE_SINT64);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_nested_message/0/fixed64_field", FieldDescriptor::TYPE_FIXED64);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_nested_message/0/sfixed64_field", FieldDescriptor::TYPE_SFIXED64);

    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_nested_message/0/bool_field", FieldDescriptor::TYPE_BOOL);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_nested_message/0/string_field", FieldDescriptor::TYPE_STRING);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_nested_message/0/float_field", FieldDescriptor::TYPE_FLOAT);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_nested_message/0/double_field", FieldDescriptor::TYPE_DOUBLE);

    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_nested_message/1/int32_field", FieldDescriptor::TYPE_INT32);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_nested_message/1/uint32_field", FieldDescriptor::TYPE_UINT32);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_nested_message/1/sint32_field", FieldDescriptor::TYPE_SINT32);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_nested_message/1/fixed32_field", FieldDescriptor::TYPE_FIXED32);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_nested_message/1/sfixed32_field", FieldDescriptor::TYPE_SFIXED32);

    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_nested_message/1/int64_field", FieldDescriptor::TYPE_INT64);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_nested_message/1/uint64_field", FieldDescriptor::TYPE_UINT64);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_nested_message/1/sint64_field", FieldDescriptor::TYPE_SINT64);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_nested_message/1/fixed64_field", FieldDescriptor::TYPE_FIXED64);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_nested_message/1/sfixed64_field", FieldDescriptor::TYPE_SFIXED64);

    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_nested_message/1/bool_field", FieldDescriptor::TYPE_BOOL);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_nested_message/1/string_field", FieldDescriptor::TYPE_STRING);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_nested_message/1/float_field", FieldDescriptor::TYPE_FLOAT);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_nested_message/1/double_field", FieldDescriptor::TYPE_DOUBLE);
}

TEST(TProtobufScalarTypeTest, NestedTypes)
{
    EXPECT_SCALAR_TYPE(TExampleMessage, "/nested_message1/int32_field", FieldDescriptor::TYPE_INT32);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/nested_message1/uint32_field", FieldDescriptor::TYPE_UINT32);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/nested_message1/sint32_field", FieldDescriptor::TYPE_SINT32);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/nested_message1/fixed32_field", FieldDescriptor::TYPE_FIXED32);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/nested_message1/sfixed32_field", FieldDescriptor::TYPE_SFIXED32);

    EXPECT_SCALAR_TYPE(TExampleMessage, "/nested_message1/int64_field", FieldDescriptor::TYPE_INT64);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/nested_message1/uint64_field", FieldDescriptor::TYPE_UINT64);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/nested_message1/sint64_field", FieldDescriptor::TYPE_SINT64);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/nested_message1/fixed64_field", FieldDescriptor::TYPE_FIXED64);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/nested_message1/sfixed64_field", FieldDescriptor::TYPE_SFIXED64);

    EXPECT_SCALAR_TYPE(TExampleMessage, "/nested_message1/bool_field", FieldDescriptor::TYPE_BOOL);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/nested_message1/string_field", FieldDescriptor::TYPE_STRING);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/nested_message1/float_field", FieldDescriptor::TYPE_FLOAT);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/nested_message1/double_field", FieldDescriptor::TYPE_DOUBLE);

    EXPECT_SCALAR_TYPE(TExampleMessage, "/nested_message2/int32_field", FieldDescriptor::TYPE_INT32);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/nested_message2/uint32_field", FieldDescriptor::TYPE_UINT32);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/nested_message2/sint32_field", FieldDescriptor::TYPE_SINT32);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/nested_message2/fixed32_field", FieldDescriptor::TYPE_FIXED32);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/nested_message2/sfixed32_field", FieldDescriptor::TYPE_SFIXED32);

    EXPECT_SCALAR_TYPE(TExampleMessage, "/nested_message2/int64_field", FieldDescriptor::TYPE_INT64);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/nested_message2/uint64_field", FieldDescriptor::TYPE_UINT64);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/nested_message2/sint64_field", FieldDescriptor::TYPE_SINT64);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/nested_message2/fixed64_field", FieldDescriptor::TYPE_FIXED64);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/nested_message2/sfixed64_field", FieldDescriptor::TYPE_SFIXED64);

    EXPECT_SCALAR_TYPE(TExampleMessage, "/nested_message2/bool_field", FieldDescriptor::TYPE_BOOL);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/nested_message2/string_field", FieldDescriptor::TYPE_STRING);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/nested_message2/float_field", FieldDescriptor::TYPE_FLOAT);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/nested_message2/double_field", FieldDescriptor::TYPE_DOUBLE);
}

TEST(TProtobufScalarTypeTest, RepeatedTypes)
{
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_int32_field/0", FieldDescriptor::TYPE_INT32);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_uint32_field/0", FieldDescriptor::TYPE_UINT32);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_sint32_field/0", FieldDescriptor::TYPE_SINT32);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_fixed32_field/0", FieldDescriptor::TYPE_FIXED32);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_sfixed32_field/0", FieldDescriptor::TYPE_SFIXED32);

    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_int64_field/0", FieldDescriptor::TYPE_INT64);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_uint64_field/0", FieldDescriptor::TYPE_UINT64);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_sint64_field/0", FieldDescriptor::TYPE_SINT64);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_fixed64_field/0", FieldDescriptor::TYPE_FIXED64);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_sfixed64_field/0", FieldDescriptor::TYPE_SFIXED64);

    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_bool_field/0", FieldDescriptor::TYPE_BOOL);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_string_field/0", FieldDescriptor::TYPE_STRING);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_float_field/0", FieldDescriptor::TYPE_FLOAT);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_double_field/0", FieldDescriptor::TYPE_DOUBLE);

    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_int32_field/1", FieldDescriptor::TYPE_INT32);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_uint32_field/1", FieldDescriptor::TYPE_UINT32);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_sint32_field/1", FieldDescriptor::TYPE_SINT32);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_fixed32_field/1", FieldDescriptor::TYPE_FIXED32);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_sfixed32_field/1", FieldDescriptor::TYPE_SFIXED32);

    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_int64_field/1", FieldDescriptor::TYPE_INT64);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_uint64_field/1", FieldDescriptor::TYPE_UINT64);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_sint64_field/1", FieldDescriptor::TYPE_SINT64);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_fixed64_field/1", FieldDescriptor::TYPE_FIXED64);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_sfixed64_field/1", FieldDescriptor::TYPE_SFIXED64);

    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_bool_field/1", FieldDescriptor::TYPE_BOOL);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_string_field/1", FieldDescriptor::TYPE_STRING);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_float_field/1", FieldDescriptor::TYPE_FLOAT);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/repeated_double_field/1", FieldDescriptor::TYPE_DOUBLE);
}

TEST(TProtobufScalarTypeTest, MapTypes)
{
    EXPECT_SCALAR_TYPE(TExampleMessage, "/string_to_int32_map/hello",
        FieldDescriptor::TYPE_INT32);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/string_to_int32_map/world",
        FieldDescriptor::TYPE_INT32);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/string_to_string_map/hello",
        FieldDescriptor::TYPE_STRING);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/string_to_string_map/world",
        FieldDescriptor::TYPE_STRING);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/int32_to_int32_map/0",
        FieldDescriptor::TYPE_INT32);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/int32_to_int32_map/1",
        FieldDescriptor::TYPE_INT32);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/int32_to_string_map/0",
        FieldDescriptor::TYPE_STRING);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/int32_to_string_map/1",
        FieldDescriptor::TYPE_STRING);
}

TEST(TProtobufScalarTypeTest, MapNestedTypes)
{
    EXPECT_SCALAR_TYPE(TExampleMessage, "/nested_message_map/hello/int32_field",
        FieldDescriptor::TYPE_INT32);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/nested_message_map/world/int32_field",
        FieldDescriptor::TYPE_INT32);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/nested_message_map/hello/string_field",
        FieldDescriptor::TYPE_STRING);
    EXPECT_SCALAR_TYPE(TExampleMessage, "/nested_message_map/world/string_field",
        FieldDescriptor::TYPE_STRING);
}

#define EXPECT_ENUM_STORAGE_TYPE(messageType, path, expectedEnumType) \
    do { \
        auto type = ReflectProtobufMessageType<NProto::messageType>(); \
        auto element = ResolveProtobufElementByYPath(type, path).Element; \
        EXPECT_TRUE(std::holds_alternative<std::unique_ptr<TProtobufScalarElement>>(element)); \
        auto elementPtr = std::get<std::unique_ptr<TProtobufScalarElement>>(element).get(); \
        int fieldType = static_cast<int>(elementPtr->Type); \
        EXPECT_EQ(fieldType, FieldDescriptor::TYPE_ENUM); \
        EXPECT_EQ(elementPtr->EnumStorageType, expectedEnumType); \
    } while (false);

TEST(TProtobufScalarTypeTest, EnumTypes)
{
    EXPECT_ENUM_STORAGE_TYPE(TExampleMessage, "/enum_int", EEnumYsonStorageType::Int);
    EXPECT_ENUM_STORAGE_TYPE(TExampleMessage, "/enum_int_repeated/0", EEnumYsonStorageType::Int);

    EXPECT_ENUM_STORAGE_TYPE(TExampleMessage, "/enum_string", EEnumYsonStorageType::String);
    EXPECT_ENUM_STORAGE_TYPE(TExampleMessage, "/enum_string_repeated/0", EEnumYsonStorageType::String);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYson

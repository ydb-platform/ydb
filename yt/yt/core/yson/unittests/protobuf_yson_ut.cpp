#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/yson/unittests/proto/protobuf_yson_ut.pb.h>
#include <yt/yt/core/yson/unittests/proto/protobuf_yson_casing_ut.pb.h>
#include <yt/yt/core/yson/unittests/proto/protobuf_yson_casing_ext_ut.pb.h>

#include <yt/yt/core/yson/config.h>
#include <yt/yt/core/yson/protobuf_interop.h>
#include <yt/yt/core/yson/null_consumer.h>
#include <yt/yt/core/yson/string_merger.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include <google/protobuf/util/message_differencer.h>

#include <google/protobuf/wire_format.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

DECLARE_PROTO_EXTENSION(NYT::NYson::NProto::TMessageExt, 12345)
REGISTER_PROTO_EXTENSION(NYT::NYson::NProto::TNestedMessageWithCustomConverter, 12345, ext)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

namespace NYT::NYson::NProto {

////////////////////////////////////////////////////////////////////////////////

using TError = NYT::NProto::TError;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

namespace NYT::NYson {
namespace {

using namespace NYTree;
using namespace NYPath;
using namespace ::google::protobuf::io;
using namespace ::google::protobuf::internal;

using FieldDescriptor = google::protobuf::FieldDescriptor;

////////////////////////////////////////////////////////////////////////////////

struct TNestedMessageWithCustomConverter
{
    int X;
    int Y;
};

void FromProto(TNestedMessageWithCustomConverter* message, const NYT::NYson::NProto::TNestedMessageWithCustomConverter& protoMessage)
{
    message->X = protoMessage.x();
    message->Y = protoMessage.y();
}

void ToProto(NYT::NYson::NProto::TNestedMessageWithCustomConverter* protoMessage, const TNestedMessageWithCustomConverter& message)
{
    protoMessage->set_x(message.X);
    protoMessage->set_y(message.Y);
}

void Serialize(const TNestedMessageWithCustomConverter& message, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).BeginMap()
        .Item("x").Value(message.X + 1)
        .Item("y").Value(message.Y + 1)
    .EndMap();
}

void Deserialize(TNestedMessageWithCustomConverter& message, NYTree::INodePtr node)
{
    auto mapNode = node->AsMap();
    message.X = mapNode->GetChildOrThrow("x")->AsInt64()->GetValue() - 1;
    message.Y = mapNode->GetChildOrThrow("y")->AsInt64()->GetValue() - 1;
}

REGISTER_INTERMEDIATE_PROTO_INTEROP_REPRESENTATION(NYT::NYson::NProto::TNestedMessageWithCustomConverter, TNestedMessageWithCustomConverter)

////////////////////////////////////////////////////////////////////////////////

struct TBytesIntermediateRepresentation
{
    int X;
};

void FromBytes(TBytesIntermediateRepresentation* value, TStringBuf bytes)
{
    value->X = FromString<int>(bytes);
}

void ToBytes(TString* bytes, const TBytesIntermediateRepresentation& value)
{
    *bytes = ToString(value.X);
}

void Serialize(const TBytesIntermediateRepresentation& value, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).BeginMap()
        .Item("x").Value(value.X + 1)
    .EndMap();
}

void Deserialize(TBytesIntermediateRepresentation& value, NYTree::INodePtr node)
{
    auto mapNode = node->AsMap();
    value.X = mapNode->GetChildOrThrow("x")->AsInt64()->GetValue() - 1;
}

REGISTER_INTERMEDIATE_PROTO_INTEROP_BYTES_FIELD_REPRESENTATION(NYT::NYson::NProto::TMessage, 29, TBytesIntermediateRepresentation)
REGISTER_INTERMEDIATE_PROTO_INTEROP_BYTES_FIELD_REPRESENTATION(NYT::NYson::NProto::TMessage, 30, TBytesIntermediateRepresentation)

////////////////////////////////////////////////////////////////////////////////

TString ToHex(const TString& data)
{
    TStringBuilder builder;
    for (char ch : data) {
        builder.AppendFormat("%x%x ",
            static_cast<unsigned char>(ch) >> 4,
            static_cast<unsigned char>(ch) & 0xf);
    }
    return builder.Flush();
}

#define EXPECT_YPATH(body, ypath) \
    do { \
        bool thrown = false; \
        try body \
        catch (const TErrorException& ex) { \
            thrown = true; \
            Cerr << ToString(ex.Error()) << Endl; \
            EXPECT_EQ(ypath, ex.Error().Attributes().Get<TYPath>("ypath")); \
        } \
        EXPECT_TRUE(thrown); \
    } while (false);

#define TEST_PROLOGUE_WITH_OPTIONS(type, options) \
    TString str; \
    StringOutputStream output(&str); \
    auto protobufWriter = CreateProtobufWriter(&output, ReflectProtobufMessageType<NYT::NYson::NProto::type>(), options); \
    BuildYsonFluently(protobufWriter.get())

#define TEST_PROLOGUE(type) \
    TEST_PROLOGUE_WITH_OPTIONS(type, TProtobufWriterOptions())

#define TEST_EPILOGUE(type) \
    Cerr << ToHex(str) << Endl; \
    NYT::NYson::NProto::type message; \
    EXPECT_TRUE(message.ParseFromArray(str.data(), str.length()));

TEST(TYsonToProtobufYsonTest, Success)
{
    TEST_PROLOGUE(TMessage)
        .BeginMap()
            .Item("int32_field").Value(10000)
            .Item("uint32_field").Value(10000U)
            .Item("sint32_field").Value(10000)
            .Item("int64_field").Value(10000)
            .Item("uint64_field").Value(10000U)
            .Item("fixed32_field").Value(10000U)
            .Item("fixed64_field").Value(10000U)
            .Item("sfixed32_field").Value(-10000)
            .Item("sfixed64_field").Value(10000)
            .Item("bool_field").Value(true)
            .Item("repeated_int32_field").BeginList()
                .Item().Value(1)
                .Item().Value(2)
                .Item().Value(3)
            .EndList()
            .Item("nested_message1").BeginMap()
                .Item("int32_field").Value(123)
                .Item("color").Value("blue")
                .Item("flag").Value("yes")
                .Item("nested_message").BeginMap()
                    .Item("color").Value("green")
                    .Item("flag").Value("true")
                    .Item("nested_message").BeginMap()
                    .EndMap()
                .EndMap()
            .EndMap()
            .Item("nested_message2").BeginMap()
            .EndMap()
            .Item("string_field").Value("hello")
            .Item("repeated_nested_message1").BeginList()
                .Item().BeginMap()
                    .Item("int32_field").Value(456)
                .EndMap()
                .Item().BeginMap()
                    .Item("int32_field").Value(654)
                .EndMap()
            .EndList()
            .Item("float_field").Value(3.14)
            .Item("double_field").Value(3.14)
            .Item("attributes").BeginMap()
                .Item("k1").Value(1)
                .Item("k2").Value("test")
                .Item("k3").BeginList()
                    .Item().Value(1)
                    .Item().Value(2)
                    .Item().Value(3)
                .EndList()
            .EndMap()
            .Item("yson_field").BeginMap()
                .Item("a").Value(1)
                .Item("b").BeginList()
                    .Item().Value("foobar")
                .EndList()
            .EndMap()
            .Item("string_to_int32_map").BeginMap()
                .Item("hello").Value(0)
                .Item("world").Value(1)
            .EndMap()
            .Item("int32_to_int32_map").BeginMap()
                .Item("100").Value(0)
                .Item("-200").Value(1)
            .EndMap()
            .Item("nested_message_map").BeginMap()
                .Item("hello").BeginMap()
                    .Item("int32_field").Value(123)
                .EndMap()
                .Item("world").BeginMap()
                    .Item("color").Value("blue")
                    .Item("nested_message_map").BeginMap()
                        .Item("test").BeginMap()
                            .Item("repeated_int32_field").BeginList()
                                .Item().Value(1)
                                .Item().Value(2)
                                .Item().Value(3)
                            .EndList()
                        .EndMap()
                    .EndMap()
                .EndMap()
            .EndMap()
            .Item("extension").BeginMap()
                .Item("extension_extension").BeginMap()
                    .Item("extension_extension_field").Value(23U)
                .EndMap()
                .Item("extension_field").Value(12U)
            .EndMap()
            .Item("nested_message_with_custom_converter").BeginMap()
                .Item("x").Value(43)
                .Item("y").Value(101)
            .EndMap()
            .Item("repeated_nested_message_with_custom_converter").BeginList()
                .Item().BeginMap()
                    .Item("x").Value(13)
                    .Item("y").Value(24)
                .EndMap()
            .EndList()
            .Item("int32_to_nested_message_with_custom_converter_map").BeginMap()
                .Item("123").BeginMap()
                    .Item("x").Value(7)
                    .Item("y").Value(8)
                .EndMap()
            .EndMap()
            .Item("guid").Value("0-deadbeef-0-abacaba")
            .Item("bytes_with_custom_converter").BeginMap()
                .Item("x").Value(43)
            .EndMap()
            .Item("repeated_bytes_with_custom_converter").BeginList()
                .Item().BeginMap()
                    .Item("x").Value(124)
                .EndMap()
            .EndList()
            .Item("extensions").BeginMap()
                .Item("ext").BeginMap()
                    .Item("x").Value(42)
                .EndMap()
                .Item("smth").BeginMap()
                    .Item("foo").Value("bar")
                .EndMap()
            .EndMap()
        .EndMap();


    TEST_EPILOGUE(TMessage)
    EXPECT_EQ(10000, message.int32_field_xxx());
    EXPECT_EQ(10000U, message.uint32_field());
    EXPECT_EQ(10000, message.sint32_field());
    EXPECT_EQ(10000, message.int64_field());
    EXPECT_EQ(10000U, message.uint64_field());
    EXPECT_EQ(10000U, message.fixed32_field());
    EXPECT_EQ(10000U, message.fixed64_field());
    EXPECT_EQ(-10000, message.sfixed32_field());
    EXPECT_EQ(10000, message.sfixed64_field());
    EXPECT_TRUE(message.bool_field());
    EXPECT_EQ("hello", message.string_field());
    EXPECT_FLOAT_EQ(3.14, message.float_field());
    EXPECT_DOUBLE_EQ(3.14, message.double_field());

    EXPECT_TRUE(message.has_nested_message1());
    EXPECT_EQ(123, message.nested_message1().int32_field());
    EXPECT_EQ(NYT::NYson::NProto::EColor::Color_Blue, message.nested_message1().color());
    EXPECT_EQ(NYT::NYson::NProto::EFlag::Flag_True, message.nested_message1().flag());
    EXPECT_TRUE(message.nested_message1().has_nested_message());
    EXPECT_FALSE(message.nested_message1().nested_message().has_int32_field());
    EXPECT_EQ(NYT::NYson::NProto::EColor::Color_Green, message.nested_message1().nested_message().color());
    EXPECT_EQ(NYT::NYson::NProto::EFlag::Flag_True, message.nested_message1().nested_message().flag());
    EXPECT_TRUE(message.nested_message1().nested_message().has_nested_message());
    EXPECT_FALSE(message.nested_message1().nested_message().nested_message().has_nested_message());
    EXPECT_FALSE(message.nested_message1().nested_message().nested_message().has_int32_field());

    EXPECT_TRUE(message.has_nested_message2());
    EXPECT_FALSE(message.nested_message2().has_int32_field());
    EXPECT_FALSE(message.nested_message2().has_nested_message());

    EXPECT_EQ(3, message.repeated_int32_field().size());
    EXPECT_EQ(1, message.repeated_int32_field().Get(0));
    EXPECT_EQ(2, message.repeated_int32_field().Get(1));
    EXPECT_EQ(3, message.repeated_int32_field().Get(2));

    EXPECT_EQ(2, message.repeated_nested_message1().size());
    EXPECT_EQ(456, message.repeated_nested_message1().Get(0).int32_field());
    EXPECT_EQ(654, message.repeated_nested_message1().Get(1).int32_field());

    EXPECT_EQ(3, message.attributes().attributes_size());
    EXPECT_EQ("k1", message.attributes().attributes(0).key());
    EXPECT_EQ(ConvertToYsonString(1).ToString(), message.attributes().attributes(0).value());
    EXPECT_EQ("k2", message.attributes().attributes(1).key());
    EXPECT_EQ(ConvertToYsonString("test").ToString(), message.attributes().attributes(1).value());
    EXPECT_EQ("k3", message.attributes().attributes(2).key());
    EXPECT_EQ(ConvertToYsonString(std::vector<int>{1, 2, 3}).ToString(), message.attributes().attributes(2).value());

    auto node = BuildYsonNodeFluently().BeginMap()
            .Item("a").Value(1)
            .Item("b").BeginList()
                .Item().Value("foobar")
            .EndList()
        .EndMap();

    EXPECT_EQ(ConvertToYsonString(node).ToString(), message.yson_field());

    EXPECT_EQ(2, message.string_to_int32_map_size());
    EXPECT_EQ(0, message.string_to_int32_map().at("hello"));
    EXPECT_EQ(1, message.string_to_int32_map().at("world"));

    EXPECT_EQ(2, message.int32_to_int32_map_size());
    EXPECT_EQ(0, message.int32_to_int32_map().at(100));
    EXPECT_EQ(1, message.int32_to_int32_map().at(-200));

    EXPECT_EQ(2, message.nested_message_map_size());
    EXPECT_EQ(123, message.nested_message_map().at("hello").int32_field());
    EXPECT_EQ(NYT::NYson::NProto::Color_Blue, message.nested_message_map().at("world").color());
    EXPECT_EQ(1, message.nested_message_map().at("world").nested_message_map_size());
    EXPECT_EQ(3, message.nested_message_map().at("world").nested_message_map().at("test").repeated_int32_field_size());
    EXPECT_EQ(1, message.nested_message_map().at("world").nested_message_map().at("test").repeated_int32_field(0));
    EXPECT_EQ(2, message.nested_message_map().at("world").nested_message_map().at("test").repeated_int32_field(1));
    EXPECT_EQ(3, message.nested_message_map().at("world").nested_message_map().at("test").repeated_int32_field(2));

    EXPECT_EQ(42, message.nested_message_with_custom_converter().x());
    EXPECT_EQ(100, message.nested_message_with_custom_converter().y());

    EXPECT_EQ(1, message.repeated_nested_message_with_custom_converter_size());
    EXPECT_EQ(12, message.repeated_nested_message_with_custom_converter().Get(0).x());
    EXPECT_EQ(23, message.repeated_nested_message_with_custom_converter().Get(0).y());

    EXPECT_EQ(1, message.int32_to_nested_message_with_custom_converter_map_size());
    EXPECT_EQ(6, message.int32_to_nested_message_with_custom_converter_map().at(123).x());
    EXPECT_EQ(7, message.int32_to_nested_message_with_custom_converter_map().at(123).y());

    EXPECT_EQ(12U, message.GetExtension(NYT::NYson::NProto::TMessageExtension::extension).extension_field());
    EXPECT_EQ(23U, message
        .GetExtension(NYT::NYson::NProto::TMessageExtension::extension)
        .GetExtension(NYT::NYson::NProto::TMessageExtensionExtension::extension_extension)
        .extension_extension_field());

    EXPECT_EQ(0xabacabau, message.guid().first());
    EXPECT_EQ(0xdeadbeefu, message.guid().second());

    EXPECT_EQ("42", message.bytes_with_custom_converter());

    EXPECT_EQ(1, message.repeated_bytes_with_custom_converter_size());
    EXPECT_EQ("123", message.repeated_bytes_with_custom_converter(0));

    EXPECT_EQ(GetProtoExtension<NYT::NYson::NProto::TMessageExt>(message.extensions()).x(), 42);
}

TEST(TYsonToProtobufYsonTest, ParseMapFromList)
{
    TEST_PROLOGUE(TMessage)
        .BeginMap()
            .Item("string_to_int32_map").BeginList()
                .Item().BeginMap()
                    .Item("key").Value("hello")
                    .Item("value").Value(0)
                .EndMap()
                .Item().BeginMap()
                    .Item("key").Value("world")
                    .Item("value").Value(1)
                .EndMap()
            .EndList()
            .Item("int32_to_int32_map").BeginList()
                .Item().BeginMap()
                    .Item("key").Value(100)
                    .Item("value").Value(0)
                .EndMap()
                .Item().BeginMap()
                    .Item("key").Value(-200)
                    .Item("value").Value(1)
                .EndMap()
            .EndList()
        .EndMap();

    TEST_EPILOGUE(TMessage)
    EXPECT_EQ(2, message.string_to_int32_map_size());
    EXPECT_EQ(0, message.string_to_int32_map().at("hello"));
    EXPECT_EQ(1, message.string_to_int32_map().at("world"));

    EXPECT_EQ(2, message.int32_to_int32_map_size());
    EXPECT_EQ(0, message.int32_to_int32_map().at(100));
    EXPECT_EQ(1, message.int32_to_int32_map().at(-200));
}

TEST(TYsonToProtobufYsonTest, Aliases)
{
    TEST_PROLOGUE(TMessage)
        .BeginMap()
            .Item("int32_field_alias1").Value(10000)
        .EndMap();


    TEST_EPILOGUE(TMessage)
    EXPECT_EQ(10000, message.int32_field_xxx());
}

TEST(TYsonToProtobufTest, TypeConversions)
{
    TEST_PROLOGUE(TMessage)
        .BeginMap()
            .Item("int32_field").Value(10000U)
            .Item("uint32_field").Value(10000)
            .Item("sint32_field").Value(10000U)
            .Item("int64_field").Value(10000U)
            .Item("uint64_field").Value(10000)
            .Item("fixed32_field").Value(10000)
            .Item("fixed64_field").Value(10000)
            .Item("sfixed32_field").Value(10000U)
            .Item("sfixed64_field").Value(10000U)
            .Item("float_field").Value(0)
            .Item("double_field").Value(1)
        .EndMap();

    TEST_EPILOGUE(TMessage)
    EXPECT_EQ(10000, message.int32_field_xxx());
    EXPECT_EQ(10000U, message.uint32_field());
    EXPECT_EQ(10000, message.sint32_field());
    EXPECT_EQ(10000, message.int64_field());
    EXPECT_EQ(10000U, message.uint64_field());
    EXPECT_EQ(10000U, message.fixed32_field());
    EXPECT_EQ(10000U, message.fixed64_field());
    EXPECT_EQ(10000, message.sfixed32_field());
    EXPECT_EQ(10000, message.sfixed64_field());
    EXPECT_EQ(0.0, message.float_field());
    EXPECT_EQ(1.0, message.double_field());
}

TEST(TYsonToProtobufYsonTest, Entities)
{
    TEST_PROLOGUE(TMessage)
        .BeginMap()
            .Item("int32_field").Entity()
            .Item("repeated_int32_field").Entity()
            .Item("nested_message1").Entity()
            .Item("repeated_nested_message1").Entity()
            .Item("attributes").Entity()
            .Item("yson_field").Entity()
            .Item("string_to_int32_map").Entity()
            .Item("int32_to_int32_map").Entity()
        .EndMap();

    TEST_EPILOGUE(TMessage)
    EXPECT_FALSE(message.has_int32_field_xxx());
    EXPECT_TRUE(message.repeated_int32_field().empty());
    EXPECT_FALSE(message.has_nested_message1());
    EXPECT_TRUE(message.repeated_nested_message1().empty());
    EXPECT_FALSE(message.has_attributes());
    EXPECT_EQ("#", message.yson_field());
    EXPECT_TRUE(message.string_to_int32_map().empty());
    EXPECT_TRUE(message.int32_to_int32_map().empty());
}

TEST(TYsonToProtobufTest, RootEntity)
{
    TEST_PROLOGUE(TMessage)
        .Entity();

    TEST_EPILOGUE(TMessage)
    EXPECT_FALSE(message.has_int32_field_xxx());
}

TEST(TYsonToProtobufTest, Failure)
{
    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .Value(0);
    }, "");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginList()
            .EndList();
    }, "");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("int32_field").Value(true)
            .EndMap();
    }, "/int32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("int32_field").Value(static_cast<i64>(std::numeric_limits<i32>::max()) + 1)
            .EndMap();
    }, "/int32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("int32_field").Value(static_cast<i64>(std::numeric_limits<i32>::min()) - 1)
            .EndMap();
    }, "/int32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("uint32_field").Value(static_cast<ui64>(std::numeric_limits<ui32>::max()) + 1)
            .EndMap();
    }, "/uint32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("nested_message1").BeginMap()
                    .Item("int32_field").Value("test")
                .EndMap()
            .EndMap();
    }, "/nested_message1/int32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("nested_message1").BeginMap()
                    .Item("int32_field").BeginAttributes().EndAttributes().Value(123)
                .EndMap()
            .EndMap();
    }, "/nested_message1/int32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("nested_message1").BeginMap()
                    .Item("color").Value("white")
                .EndMap()
            .EndMap();
    }, "/nested_message1/color");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("nested_message1").Value(123)
            .EndMap();
    }, "/nested_message1");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("repeated_nested_message1").BeginList()
                    .Item().BeginMap()
                        .Item("color").Value("blue")
                    .EndMap()
                    .Item().BeginMap()
                        .Item("color").Value("black")
                    .EndMap()
                .EndList()
            .EndMap();
    }, "/repeated_nested_message1/1/color");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("repeated_nested_message1").BeginList()
                    .Item().BeginList()
                    .EndList()
                .EndList()
            .EndMap();
    }, "/repeated_nested_message1/0");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("repeated_nested_message1").BeginList()
                    .Item().BeginMap()
                        .Item("color").Value("black")
                    .EndMap()
                .EndList()
            .EndMap();
    }, "/repeated_nested_message1/0/color");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("int32_field").Value(0)
                .Item("int32_field").Value(1)
            .EndMap();
    }, "/int32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("int32_field").Value(0)
                .Item("int32_field").Value(1)
            .EndMap();
    }, "/int32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessageWithRequiredFields)
            .BeginMap()
                .Item("required_field").Value(0)
                .Item("required_field").Value(1)
            .EndMap();
    }, "/required_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessageWithRequiredFields)
            .BeginMap()
            .EndMap();
    }, "/required_field");

    // int32
    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("int32_field").Value(10000000000)
            .EndMap();
    }, "/int32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("int32_field").Value(10000000000U)
            .EndMap();
    }, "/int32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("int32_field").Value(-10000000000)
            .EndMap();
    }, "/int32_field");

    // sint32
    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("sint32_field").Value(10000000000)
            .EndMap();
    }, "/sint32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("sint32_field").Value(10000000000U)
            .EndMap();
    }, "/sint32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("sint32_field").Value(-10000000000)
            .EndMap();
    }, "/sint32_field");

    // uint32
    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("uint32_field").Value(10000000000)
            .EndMap();
    }, "/uint32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("uint32_field").Value(10000000000U)
            .EndMap();
    }, "/uint32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("uint32_field").Value(-1)
            .EndMap();
    }, "/uint32_field");

    // int32
    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("int64_field").Value(std::numeric_limits<ui64>::max())
            .EndMap();
    }, "/int64_field");

    // uint64
    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("uint64_field").Value(-1)
            .EndMap();
    }, "/uint64_field");

    // fixed32
    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("fixed32_field").Value(10000000000)
            .EndMap();
    }, "/fixed32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("fixed32_field").Value(10000000000U)
            .EndMap();
    }, "/fixed32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("fixed32_field").Value(-10000000000)
            .EndMap();
    }, "/fixed32_field");

    // sfixed32
    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("sfixed32_field").Value(10000000000)
            .EndMap();
    }, "/sfixed32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("sfixed32_field").Value(10000000000U)
            .EndMap();
    }, "/sfixed32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("sfixed32_field").Value(-10000000000)
            .EndMap();
    }, "/sfixed32_field");

    // fixed64
    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("fixed64_field").Value(-1)
            .EndMap();
    }, "/fixed64_field");

    // YT-9094
    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("repeated_int32_field").BeginList()
                .EndList()
                .Item("repeated_nested_message1").BeginList()
                    .Item().BeginMap()
                    .EndMap()
                    .Item().BeginMap()
                        .Item("int32_field").Value(1)
                    .EndMap()
                    .Item().BeginMap()
                        .Item("int32_field").Value(1)
                    .EndMap()
                    .Item().BeginMap()
                        .Item("int32_field").Value(1)
                    .EndMap()
                .EndList()
                .Item("repeated_nested_message2").BeginList()
                    .Item().BeginMap()
                        .Item("int32_field").Value(1)
                    .EndMap()
                .EndList()
            .Item("attributes").BeginMap()
                .Item("host").Value("localhost")
            .EndMap()
            .Item("nested_message1").BeginList()
                .EndList()
            .EndMap();
    }, "/nested_message1");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("nested_message_map").BeginList()
                    .Item().Value(123)
                .EndList()
            .EndMap();
    }, "/nested_message_map/0");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("nested_message_map").Value(123)
            .EndMap();
    }, "/nested_message_map");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("string_to_int32_map").BeginMap()
                    .Item("a").Value("b")
                .EndMap()
            .EndMap();
    }, "/string_to_int32_map/a");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("int32_to_int32_map").BeginMap()
                    .Item("a").Value(100)
                .EndMap()
            .EndMap();
    }, "/int32_to_int32_map");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("int32_to_int32_map").BeginMap()
                    .Item("100").Value("b")
                .EndMap()
            .EndMap();
    }, "/int32_to_int32_map/100");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("nested_message_map").BeginMap()
                    .Item("a").BeginMap()
                        .Item("nested_message_map").Value(123)
                    .EndMap()
                .EndMap()
            .EndMap();
    }, "/nested_message_map/a/nested_message_map");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessageWithRequiredAnnotation)
            .BeginMap()
            .EndMap();
    }, "/required_string");
}

TEST(TYsonToProtobufTest, ErrorProto)
{
    TEST_PROLOGUE(TError)
        .BeginMap()
            .Item("message").Value("Hello world")
            .Item("code").Value(1)
            .Item("attributes").BeginMap()
                .Item("host").Value("localhost")
            .EndMap()
        .EndMap();

    TEST_EPILOGUE(TError);

    EXPECT_EQ("Hello world", message.message());
    EXPECT_EQ(1, message.code());

    auto attribute = message.attributes().attributes()[0];
    EXPECT_EQ(attribute.key(), "host");
    EXPECT_EQ(ConvertTo<TString>(TYsonString(attribute.value())), "localhost");
}

TEST(TYsonToProtobufTest, SkipUnknownFields)
{
    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("unknown_field").Value(1)
            .EndMap();
    }, "");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("repeated_nested_message1").BeginList()
                    .Item().BeginMap()
                        .Item("unknown_field").Value(1)
                    .EndMap()
                .EndList()
            .EndMap();
    }, "/repeated_nested_message1/0");

    {
        TProtobufWriterOptions options;
        options.UnknownYsonFieldModeResolver = TProtobufWriterOptions::CreateConstantUnknownYsonFieldModeResolver(EUnknownYsonFieldsMode::Keep);

        TEST_PROLOGUE_WITH_OPTIONS(TMessage, options)
            .BeginMap()
                .Item("int32_field").Value(10000)
                .Item("unknown_field").Value(1)
                .Item("nested_message1").BeginMap()
                    .Item("int32_field").Value(123)
                    .Item("nested_message").BeginMap()
                        .Item("unknown_map").BeginMap()
                        .EndMap()
                    .EndMap()
                .EndMap()
                .Item("repeated_nested_message1").BeginList()
                    .Item().BeginMap()
                        .Item("int32_field").Value(456)
                        .Item("unknown_list").BeginList()
                        .EndList()
                    .EndMap()
                .EndList()
            .EndMap();

        TEST_EPILOGUE(TMessage)
        EXPECT_EQ(10000, message.int32_field_xxx());

        EXPECT_TRUE(message.has_nested_message1());
        EXPECT_EQ(123, message.nested_message1().int32_field());
        EXPECT_TRUE(message.nested_message1().has_nested_message());

        EXPECT_EQ(1, message.repeated_nested_message1().size());
        EXPECT_EQ(456, message.repeated_nested_message1().Get(0).int32_field());
    }
}

TEST(TYsonToProtobufTest, KeepUnknownFields)
{
    auto ysonString = BuildYsonStringFluently()
        .BeginMap()
            .Item("known_string").Value("hello")
            .Item("unknown_int").Value(123)
            .Item("unknown_map").BeginMap()
                .Item("a").Value(1)
                .Item("b").Value("test")
            .EndMap()
            .Item("known_submessage").BeginMap()
                .Item("known_int").Value(555)
                .Item("unknown_list").BeginList()
                    .Item().Value(1)
                    .Item().Value(2)
                    .Item().Value(3)
                .EndList()
            .EndMap()
            .Item("known_submessages").BeginList()
                .Item().BeginMap()
                    .Item("known_string").Value("first")
                    .Item("unknown_int").Value(10)
                .EndMap()
                .Item().BeginMap()
                    .Item("known_string").Value("second")
                    .Item("unknown_int").Value(20)
                .EndMap()
            .EndList()
            .Item("another_unknown_int").Value(777)
        .EndMap();

    TString protobufString;
    StringOutputStream protobufOutput(&protobufString);
    TProtobufWriterOptions options;
    options.UnknownYsonFieldModeResolver = TProtobufWriterOptions::CreateConstantUnknownYsonFieldModeResolver(EUnknownYsonFieldsMode::Keep);
    auto protobufWriter = CreateProtobufWriter(&protobufOutput, ReflectProtobufMessageType<NYT::NYson::NProto::TExtensibleMessage>(), options);
    ParseYsonStringBuffer(ysonString.ToString(), EYsonType::Node, protobufWriter.get());

    NYT::NYson::NProto::TExtensibleMessage message;
    EXPECT_TRUE(message.ParseFromArray(protobufString.data(), protobufString.length()));

    EXPECT_EQ("hello", message.known_string());
    EXPECT_EQ(555, message.known_submessage().known_int());
    EXPECT_EQ(2, message.known_submessages_size());
    EXPECT_EQ("first", message.known_submessages(0).known_string());
    EXPECT_EQ("second", message.known_submessages(1).known_string());

    TString newYsonString;
    TStringOutput newYsonOutputStream(newYsonString);
    TYsonWriter ysonWriter(&newYsonOutputStream, EYsonFormat::Pretty);
    ArrayInputStream protobufInput(protobufString.data(), protobufString.length());
    ParseProtobuf(&ysonWriter, &protobufInput, ReflectProtobufMessageType<NYT::NYson::NProto::TExtensibleMessage>());

    Cerr << newYsonString << Endl;

    EXPECT_TRUE(AreNodesEqual(ConvertToNode(TYsonString(newYsonString)), ConvertToNode(ysonString)));
}

TEST(TYsonToProtobufTest, Entities)
{
    TProtobufWriterOptions options;

    TEST_PROLOGUE_WITH_OPTIONS(TMessage, options)
        .BeginMap()
            .Item("nested_message1").Entity()
        .EndMap();
    TEST_EPILOGUE(TMessage)

    EXPECT_FALSE(message.has_nested_message1());
}

TEST(TYsonToProtobufTest, ValidUtf8StringCheck)
{
    for (auto option: {EUtf8Check::Disable, EUtf8Check::LogOnFail, EUtf8Check::ThrowOnFail}) {
        auto config = New<TProtobufInteropConfig>();
        config->Utf8Check = option;
        SetProtobufInteropConfig(config);

        TString invalidUtf8 = "\xc3\x28";

        auto check = [&] {
            TEST_PROLOGUE_WITH_OPTIONS(TMessage, {})
                .BeginMap()
                    .Item("string_field").Value(invalidUtf8)
                .EndMap();
        };
        if (option == EUtf8Check::ThrowOnFail) {
            EXPECT_THROW_WITH_SUBSTRING(check(), "Non UTF-8 value in string field");
        } else {
            EXPECT_NO_THROW(check());
        }

        NProto::TMessage message;
        message.set_string_field(invalidUtf8);
        TString newYsonString;
        TStringOutput newYsonOutputStream(newYsonString);
        TYsonWriter ysonWriter(&newYsonOutputStream, EYsonFormat::Pretty);
        if (option == EUtf8Check::ThrowOnFail) {
            EXPECT_THROW_WITH_SUBSTRING(
                WriteProtobufMessage(&ysonWriter, message), "Non UTF-8 value in string field");
        } else {
            EXPECT_NO_THROW(WriteProtobufMessage(&ysonWriter, message));
        }
    }
}

TEST(TYsonToProtobufTest, CustomUnknownFieldsModeResolver)
{
    {
        // Basic usage of custom resolver with state.
        TProtobufWriterOptions options;
        int unknownKeyCount = 0;
        options.UnknownYsonFieldModeResolver = [&unknownKeyCount] (const NYPath::TYPath& path) -> NYson::EUnknownYsonFieldsMode {
            if (path == "/nested_message1/nested_message/unknown_map") {
                return NYson::EUnknownYsonFieldsMode::Forward;
            } else if (path == "/nested_message1/nested_message/unknown_map/first_unknown_key" || path == "/nested_message1/nested_message/unknown_map/second_unknown_key") {
                ++unknownKeyCount;
                return NYson::EUnknownYsonFieldsMode::Keep;
            }
            return NYson::EUnknownYsonFieldsMode::Keep;
        };

        TEST_PROLOGUE_WITH_OPTIONS(TMessage, options)
            .BeginMap()
                .Item("int32_field").Value(10000)
                .Item("unknown_field").Value(1)
                .Item("nested_message1").BeginMap()
                    .Item("int32_field").Value(123)
                    .Item("nested_message").BeginMap()
                        .Item("unknown_map").BeginMap()
                            .Item("first_unknown_key").Value(11)
                            .Item("second_unknown_key").Value(22)
                        .EndMap()
                    .EndMap()
                .EndMap()
                .Item("repeated_nested_message1").BeginList()
                    .Item().BeginMap()
                        .Item("int32_field").Value(456)
                        .Item("unknown_list").BeginList()
                        .EndList()
                    .EndMap()
                .EndList()
            .EndMap();

        TEST_EPILOGUE(TMessage)
        EXPECT_EQ(10000, message.int32_field_xxx());

        EXPECT_TRUE(message.has_nested_message1());
        EXPECT_EQ(123, message.nested_message1().int32_field());
        EXPECT_TRUE(message.nested_message1().has_nested_message());

        EXPECT_EQ(1, message.repeated_nested_message1().size());
        EXPECT_EQ(456, message.repeated_nested_message1().Get(0).int32_field());
        EXPECT_EQ(2, unknownKeyCount);
    }
    {
        auto ysonStringBeforeSerialization = BuildYsonStringFluently()
        .BeginMap()
            .Item("known_string").Value("hello")
            .Item("unknown_skipped_int1").Value(1)
            .Item("unknown_skipped_int2").Value(2)
            .Item("unknown_map1").BeginMap()
                .Item("a").Value(1)
                .Item("b").Value("test")
                .Item("c").Entity()
                .Item("unknown_map2").BeginMap()
                    .Item("x").Value(10)
                    .Item("y").Entity()
                    .Item("z").Value(1.12)
                .EndMap()
            .EndMap()
            .Item("known_submessage").BeginMap()
                .Item("known_int").Value(555)
                .Item("unknown_list1").BeginList()
                    .Item().Value(1)
                    .Item().Value(2)
                    .Item().Value(3)
                .EndList()
            .EndMap()
        .EndMap();


        TString protobufString;
        StringOutputStream protobufOutput(&protobufString);
        TProtobufWriterOptions options;
        options.UnknownYsonFieldModeResolver = [] (const NYPath::TYPath& path) -> NYson::EUnknownYsonFieldsMode {
            if (path == "/unknown_map1") {
                return NYson::EUnknownYsonFieldsMode::Forward;
            }
            if (path.StartsWith("/unknown_map1/")) {
                return NYson::EUnknownYsonFieldsMode::Keep;
            }
            if (path == "/unknown_skipped_int1" || path == "/unknown_skipped_int2") {
                return NYson::EUnknownYsonFieldsMode::Skip;
            }
            if (path == "/known_submessage/unknown_list1") {
                return NYson::EUnknownYsonFieldsMode::Forward;
            }
            if (path.StartsWith("/known_submessage/unknown_list1/")) {
                return NYson::EUnknownYsonFieldsMode::Keep;
            }
            return NYson::EUnknownYsonFieldsMode::Fail;
        };
        auto protobufWriter = CreateProtobufWriter(&protobufOutput, ReflectProtobufMessageType<NYT::NYson::NProto::TExtensibleMessage>(), options);
        ParseYsonStringBuffer(ysonStringBeforeSerialization.ToString(), EYsonType::Node, protobufWriter.get());

        auto expectedYsonStringAfterSerialization = BuildYsonStringFluently()
            .BeginMap()
                .Item("known_string").Value("hello")
                .Item("unknown_map1").BeginMap()
                    .Item("a").Value(1)
                    .Item("b").Value("test")
                    .Item("c").Entity()
                    .Item("unknown_map2").BeginMap()
                        .Item("x").Value(10)
                        .Item("y").Entity()
                        .Item("z").Value(1.12)
                    .EndMap()
                .EndMap()
                .Item("known_submessage").BeginMap()
                    .Item("known_int").Value(555)
                    .Item("unknown_list1").BeginList()
                        .Item().Value(1)
                        .Item().Value(2)
                        .Item().Value(3)
                    .EndList()
                .EndMap()
            .EndMap();

        NYT::NYson::NProto::TExtensibleMessage message;
        EXPECT_TRUE(message.ParseFromArray(protobufString.data(), protobufString.length()));

        TString newYsonString;
        TStringOutput newYsonOutputStream(newYsonString);
        TYsonWriter ysonWriter(&newYsonOutputStream, EYsonFormat::Pretty);
        ArrayInputStream protobufInput(protobufString.data(), protobufString.length());
        ParseProtobuf(&ysonWriter, &protobufInput, ReflectProtobufMessageType<NYT::NYson::NProto::TExtensibleMessage>());
        EXPECT_TRUE(AreNodesEqual(ConvertToNode(TYsonString(newYsonString)), ConvertToNode(expectedYsonStringAfterSerialization)));
    }
    {
        TProtobufWriterOptions options;
        options.UnknownYsonFieldModeResolver = [] (const NYPath::TYPath& path) -> NYson::EUnknownYsonFieldsMode {
            if (path == "/nested_message1/unknown_map") {
                return NYson::EUnknownYsonFieldsMode::Forward;
            }
            if (path == "/nested_message1/unknown_map/ok_value") {
                return NYson::EUnknownYsonFieldsMode::Keep;
            }
            if (path == "/nested_message1/unknown_map/fail_value") {
                return NYson::EUnknownYsonFieldsMode::Fail;
            }
            return NYson::EUnknownYsonFieldsMode::Keep;
        };
        EXPECT_YPATH({
            TEST_PROLOGUE_WITH_OPTIONS(TMessage, options)
                .BeginMap()
                    .Item("nested_message1").BeginMap()
                        .Item("int32_field").Value(11)
                        .Item("unknown_map").BeginMap()
                            .Item("ok_value").Value(0)
                            .Item("fail_value").Value(1)
                        .EndMap()
                    .EndMap()
                .EndMap();
        }, "/nested_message1/unknown_map/fail_value");
    }
    {
        // Don't fail if Forward is returned on empty map or empty list.
        TProtobufWriterOptions options;
        options.UnknownYsonFieldModeResolver = [] (const NYPath::TYPath& path) -> NYson::EUnknownYsonFieldsMode {
            if (path == "/nested_message1/unknown_map") {
                return NYson::EUnknownYsonFieldsMode::Forward;
            }
            if (path == "/nested_message1/unknown_map/ok_value") {
                return NYson::EUnknownYsonFieldsMode::Keep;
            }
            if (path == "/nested_message1/unknown_map/forwarded_empty_map") {
                return NYson::EUnknownYsonFieldsMode::Forward;
            }
            if (path == "/nested_message1/unknown_map/forwarded_empty_list") {
                return NYson::EUnknownYsonFieldsMode::Forward;
            }
            return NYson::EUnknownYsonFieldsMode::Keep;
        };
        TEST_PROLOGUE_WITH_OPTIONS(TMessage, options)
            .BeginMap()
                .Item("nested_message1").BeginMap()
                    .Item("int32_field").Value(11)
                    .Item("unknown_map").BeginMap()
                        .Item("ok_value").Value(0)
                        .Item("forwarded_empty_map").BeginMap()
                        .EndMap()
                        .Item("forwarded_empty_list").BeginList()
                        .EndList()
                    .EndMap()
                .EndMap()
            .EndMap();
    }
    {
        // Fail if leaf is scalar and returns Forward.
        {
            TProtobufWriterOptions options;
            options.UnknownYsonFieldModeResolver = [] (const NYPath::TYPath& path) -> NYson::EUnknownYsonFieldsMode {
                if (path == "/nested_message1/unknown_map") {
                    return NYson::EUnknownYsonFieldsMode::Forward;
                }
                if (path == "/nested_message1/unknown_map/forwarded_scalar_value") {
                    return NYson::EUnknownYsonFieldsMode::Forward;
                }
                return NYson::EUnknownYsonFieldsMode::Keep;
            };
            EXPECT_YPATH({
                TEST_PROLOGUE_WITH_OPTIONS(TMessage, options)
                    .BeginMap()
                        .Item("nested_message1").BeginMap()
                            .Item("int32_field").Value(11)
                            .Item("unknown_map").BeginMap()
                                .Item("forwarded_scalar_value").Value(0)
                            .EndMap()
                        .EndMap()
                    .EndMap();
            }, "/nested_message1/unknown_map/forwarded_scalar_value");
        }
        {
            // Fail on forwarded scalar value on the top level.
            TProtobufWriterOptions options;
            options.UnknownYsonFieldModeResolver = [] (const NYPath::TYPath& path) -> NYson::EUnknownYsonFieldsMode {
                if (path == "/nested_message1/unknown_map") {
                    return NYson::EUnknownYsonFieldsMode::Forward;
                }
                if (path == "/nested_message1/unknown_map/kept_scalar_value") {
                    return NYson::EUnknownYsonFieldsMode::Keep;
                }
                if (path == "/nested_message1/forwarded_scalar_value") {
                    return NYson::EUnknownYsonFieldsMode::Forward;
                }
                return NYson::EUnknownYsonFieldsMode::Keep;
            };
            EXPECT_YPATH({
                TEST_PROLOGUE_WITH_OPTIONS(TMessage, options)
                    .BeginMap()
                        .Item("nested_message1").BeginMap()
                            .Item("int32_field").Value(11)
                            .Item("unknown_map").BeginMap()
                                .Item("kept_scalar_value").Value("val")
                            .EndMap()
                            .Item("forwarded_scalar_value").Value(0)
                        .EndMap()
                    .EndMap();
            }, "/nested_message1/forwarded_scalar_value");
        }
    }
    {
        TProtobufWriterOptions options;
        options.UnknownYsonFieldModeResolver = [] (const NYPath::TYPath& path) -> NYson::EUnknownYsonFieldsMode {
            if (path == "/nested_message1/unknown_map") {
                return NYson::EUnknownYsonFieldsMode::Forward;
            }
            if (path == "/nested_message1/unknown_map/ok_value") {
                return NYson::EUnknownYsonFieldsMode::Keep;
            }
            if (path == "/nested_message1/unknown_map/forwarded_empty_list") {
                return NYson::EUnknownYsonFieldsMode::Forward;
            }
            if (path == "/nested_message1/unknown_map/forwarded_empty_list/2") {
                return NYson::EUnknownYsonFieldsMode::Fail;
            }
            return NYson::EUnknownYsonFieldsMode::Keep;
        };
        EXPECT_YPATH({
            TEST_PROLOGUE_WITH_OPTIONS(TMessage, options)
                .BeginMap()
                    .Item("nested_message1").BeginMap()
                        .Item("int32_field").Value(11)
                        .Item("unknown_map").BeginMap()
                            .Item("ok_value").Value(0)
                            .Item("forwarded_empty_list").BeginList()
                                .Item().Value("0")
                                .Item().Value("1")
                                .Item().Value("2")
                            .EndList()
                        .EndMap()
                    .EndMap()
                .EndMap();
        }, "/nested_message1/unknown_map/forwarded_empty_list/2");
        TEST_PROLOGUE_WITH_OPTIONS(TMessage, options)
            .BeginMap()
                .Item("nested_message1").BeginMap()
                    .Item("int32_field").Value(11)
                    .Item("unknown_map").BeginMap()
                        .Item("ok_value").Value(0)
                        .Item("forwarded_empty_list").BeginList()
                            .Item().BeginList()
                            .EndList()
                        .EndList()
                    .EndMap()
                .EndMap()
            .EndMap();
    }
    {
        auto ysonStringBeforeSerialization = BuildYsonStringFluently()
            .BeginMap()
                .Item("nested_message1").BeginMap()
                    .Item("int32_field").Value(11)
                    .Item("unknown_map1").BeginMap()
                        .Item("ok_value").Value(0)
                        .Item("skip_value1").Value("aa")
                        .Item("unknown_map2").BeginMap()
                            .Item("skip_value2").Value(1)
                        .EndMap()
                        .Item("skipped_map").BeginMap()
                            .Item("fail_value").Value(12)
                        .EndMap()
                        .Item("kept_map").BeginMap()
                            .Item("fail_value").Value(12)
                        .EndMap()
                    .EndMap()
                .EndMap()
            .EndMap();
        TString protobufString;
        StringOutputStream protobufOutput(&protobufString);
        TProtobufWriterOptions options;
        options.UnknownYsonFieldModeResolver = [] (const NYPath::TYPath& path) -> NYson::EUnknownYsonFieldsMode {
            if (path == "/nested_message1/unknown_map1") {
                return NYson::EUnknownYsonFieldsMode::Forward;
            }
            if (path == "/nested_message1/unknown_map1/ok_value") {
                return NYson::EUnknownYsonFieldsMode::Keep;
            }
            if (path == "/nested_message1/unknown_map1/skip_value1") {
                return NYson::EUnknownYsonFieldsMode::Skip;
            }
            if (path == "/nested_message1/unknown_map1/unknown_map2") {
                return NYson::EUnknownYsonFieldsMode::Forward;
            }
            if (path == "/nested_message1/unknown_map1/unknown_map2/skip_value2") {
                return NYson::EUnknownYsonFieldsMode::Skip;
            }
            if (path == "/nested_message1/unknown_map1/skipped_map") {
                return NYson::EUnknownYsonFieldsMode::Skip;
            }
            if (path == "/nested_message1/unknown_map1/kept_map") {
                return NYson::EUnknownYsonFieldsMode::Keep;
            }
            // Shall not be invoked.
            return NYson::EUnknownYsonFieldsMode::Fail;
        };

        auto protobufWriter = CreateProtobufWriter(&protobufOutput, ReflectProtobufMessageType<NYT::NYson::NProto::TMessage>(), options);
        ParseYsonStringBuffer(ysonStringBeforeSerialization.ToString(), EYsonType::Node, protobufWriter.get());

        auto expectedYsonStringAfterSerialization = BuildYsonStringFluently()
            .BeginMap()
                .Item("nested_message1").BeginMap()
                    .Item("int32_field").Value(11)
                    .Item("unknown_map1").BeginMap()
                        .Item("ok_value").Value(0)
                        .Item("unknown_map2").BeginMap()
                        .EndMap()
                        .Item("kept_map").BeginMap()
                            .Item("fail_value").Value(12)
                        .EndMap()
                    .EndMap()
                .EndMap()
            .EndMap();

        NYT::NYson::NProto::TMessage message;
        EXPECT_TRUE(message.ParseFromArray(protobufString.data(), protobufString.length()));

        TString newYsonString;
        TStringOutput newYsonOutputStream(newYsonString);
        TYsonWriter ysonWriter(&newYsonOutputStream, EYsonFormat::Pretty);
        ArrayInputStream protobufInput(protobufString.data(), protobufString.length());
        ParseProtobuf(&ysonWriter, &protobufInput, ReflectProtobufMessageType<NYT::NYson::NProto::TMessage>());

        Cerr << newYsonString << Endl;

        EXPECT_TRUE(AreNodesEqual(ConvertToNode(TYsonString(newYsonString)), ConvertToNode(expectedYsonStringAfterSerialization)));
    }
    {
        TProtobufWriterOptions options;
        options.UnknownYsonFieldModeResolver = [] (const NYPath::TYPath& path) {
            if (path == "/forwarded_attr") {
                return NYson::EUnknownYsonFieldsMode::Forward;
            }
            if (path == "/kept_attr") {
                return NYson::EUnknownYsonFieldsMode::Keep;
            }
            return NYson::EUnknownYsonFieldsMode::Fail;
        };
        auto ysonStringBeforeSerialization = BuildYsonStringFluently()
            .BeginMap()
                .Item("forwarded_attr").BeginAttributes()
                    .Item("key1").BeginList()
                        .Item().Value("list_item1")
                    .EndList()
                .EndAttributes().BeginMap()
                .EndMap()
                .Item("kept_attr").BeginAttributes()
                    .Item("key1").BeginAttributes()
                        .Item("key2").Value(true)
                    .EndAttributes().Value("11212")
                .EndAttributes().Value(111)
            .EndMap();

        TString protobufString;
        StringOutputStream protobufOutput(&protobufString);

        auto protobufWriter = CreateProtobufWriter(&protobufOutput, ReflectProtobufMessageType<NYT::NYson::NProto::TMessage>(), options);
        ParseYsonStringBuffer(ysonStringBeforeSerialization.ToString(), EYsonType::Node, protobufWriter.get());

        auto expectedYsonStringAfterSerialization = ysonStringBeforeSerialization;

        TString newYsonString;
        TStringOutput newYsonOutputStream(newYsonString);
        TYsonWriter ysonWriter(&newYsonOutputStream, EYsonFormat::Pretty);
        ArrayInputStream protobufInput(protobufString.data(), protobufString.length());
        ParseProtobuf(&ysonWriter, &protobufInput, ReflectProtobufMessageType<NYT::NYson::NProto::TMessage>());

        Cerr << newYsonString << Endl;

        EXPECT_TRUE(AreNodesEqual(ConvertToNode(TYsonString(newYsonString)), ConvertToNode(expectedYsonStringAfterSerialization)));
    }
    {
        TProtobufWriterOptions options;
        options.UnknownYsonFieldModeResolver = TProtobufWriterOptions::CreateConstantUnknownYsonFieldModeResolver(EUnknownYsonFieldsMode::Forward);
        EXPECT_YPATH({
            TEST_PROLOGUE_WITH_OPTIONS(TMessage, options)
                .BeginMap()
                    .Item("nested_message1").BeginMap()
                        .Item("int32_field").Value(11)
                        .Item("unknown_field\1").Value("val")
                    .EndMap()
                .EndMap();
        }, R"(/nested_message1/unknown_field\x01)");
    }
    {
        TProtobufWriterOptions options;
        options.UnknownYsonFieldModeResolver = [] (const NYPath::TYPath& path) {
            if (path == R"(/nested_message1/unknown_field\x01)") {
                return EUnknownYsonFieldsMode::Forward;
            }
            return EUnknownYsonFieldsMode::Keep;
        };

        EXPECT_YPATH({
            TEST_PROLOGUE_WITH_OPTIONS(TMessage, options)
                .BeginMap()
                    .Item("nested_message1").BeginMap()
                        .Item("int32_field").Value(11)
                        .Item("unknown_field\1").Value("val")
                    .EndMap()
                .EndMap();
        }, R"(/nested_message1/unknown_field\x01)");
    }
    {
        TProtobufWriterOptions options;
        options.UnknownYsonFieldModeResolver = TProtobufWriterOptions::CreateConstantUnknownYsonFieldModeResolver(EUnknownYsonFieldsMode::Fail);

        EXPECT_YPATH({
            TEST_PROLOGUE_WITH_OPTIONS(TMessage, options)
                .BeginMap()
                    .Item("nested_message1").BeginMap()
                        .Item("int32_field").Value(11)
                        .Item("unknown_field1").Value("val")
                    .EndMap()
                .EndMap();
        }, "/nested_message1");
    }
}

TEST(TYsonToProtobufTest, ReservedFields)
{
    TProtobufWriterOptions options;

    TEST_PROLOGUE_WITH_OPTIONS(TMessageWithReservedFields, options)
        .BeginMap()
            .Item("reserved_field1").Value(1)
            .Item("reserved_field1").Entity()
            .Item("reserved_field3").BeginMap()
                .Item("key").Value("value")
            .EndMap()
        .EndMap();
    TEST_EPILOGUE(TMessage)
}

#undef TEST_PROLOGUE
#undef TEST_PROLOGUE_WITH_OPTIONS
#undef TEST_EPILOGUE

////////////////////////////////////////////////////////////////////////////////

#define TEST_PROLOGUE() \
    TString protobuf; \
    StringOutputStream protobufOutputStream(&protobuf); \
    CodedOutputStream codedStream(&protobufOutputStream);

#define TEST_EPILOGUE_WITH_OPTIONS(type, options) \
    codedStream.Trim(); \
    Cerr << ToHex(protobuf) << Endl; \
    ArrayInputStream protobufInputStream(protobuf.data(), protobuf.length()); \
    TString yson; \
    TStringOutput ysonOutputStream(yson); \
    TYsonWriter writer(&ysonOutputStream, EYsonFormat::Pretty); \
    ParseProtobuf(&writer, &protobufInputStream, ReflectProtobufMessageType<NYT::NYson::NProto::type>(), options); \
    Cerr << ConvertToYsonString(TYsonString(yson), EYsonFormat::Pretty).ToString() << Endl;

#define TEST_EPILOGUE(type) \
    TEST_EPILOGUE_WITH_OPTIONS(type, TProtobufParserOptions())

TEST(TProtobufToYsonTest, Success)
{
    NYT::NYson::NProto::TMessage message;
    message.set_int32_field_xxx(10000);
    message.set_uint32_field(10000U);
    message.set_sint32_field(10000);
    message.set_int64_field(10000);
    message.set_uint64_field(10000U);
    message.set_fixed32_field(10000U);
    message.set_fixed64_field(10000U);
    message.set_sfixed32_field(-10000);
    message.set_sfixed64_field(10000);
    message.set_bool_field(true);
    message.set_string_field("hello");
    message.set_float_field(3.14);
    message.set_double_field(3.14);

    message.add_repeated_int32_field(1);
    message.add_repeated_int32_field(2);
    message.add_repeated_int32_field(3);

    message.mutable_nested_message1()->set_int32_field(123);
    message.mutable_nested_message1()->set_color(NYT::NYson::NProto::Color_Blue);

    message.mutable_nested_message1()->mutable_nested_message()->set_color(NYT::NYson::NProto::Color_Green);

    message.MutableExtension(NYT::NYson::NProto::TMessageExtension::extension)->set_extension_field(12);
    message
        .MutableExtension(NYT::NYson::NProto::TMessageExtension::extension)
        ->MutableExtension(NYT::NYson::NProto::TMessageExtensionExtension::extension_extension)
        ->set_extension_extension_field(23);

    {
        auto* proto = message.add_repeated_nested_message1();
        proto->set_int32_field(456);
        proto->add_repeated_int32_field(1);
        proto->add_repeated_int32_field(2);
        proto->add_repeated_int32_field(3);
    }
    {
        auto* proto = message.add_repeated_nested_message1();
        proto->set_int32_field(654);
    }
    {
        auto* proto = message.mutable_attributes();
        {
            auto* entry = proto->add_attributes();
            entry->set_key("k1");
            entry->set_value(ConvertToYsonString(1).ToString());
        }
        {
            auto* entry = proto->add_attributes();
            entry->set_key("k2");
            entry->set_value(ConvertToYsonString("test").ToString());
        }
        {
            auto* entry = proto->add_attributes();
            entry->set_key("k3");
            entry->set_value(ConvertToYsonString(std::vector<int>{1, 2, 3}).ToString());
        }
    }

    message.set_yson_field("{a=1;b=[\"foobar\";];}");

    {
        auto& map = *message.mutable_string_to_int32_map();
        map["hello"] = 0;
        map["world"] = 1;
    }

    {
        auto& map = *message.mutable_int32_to_int32_map();
        map[100] = 0;
        map[-200] = 1;
    }

    {
        auto& map = *message.mutable_nested_message_map();
        {
            NYT::NYson::NProto::TNestedMessage value;
            value.set_int32_field(123);
            map["hello"] = value;
        }
        {
            NYT::NYson::NProto::TNestedMessage value;
            value.set_color(NYT::NYson::NProto::Color_Blue);
            {
                auto& submap = *value.mutable_nested_message_map();
                {
                    NYT::NYson::NProto::TNestedMessage subvalue;
                    subvalue.add_repeated_int32_field(1);
                    subvalue.add_repeated_int32_field(2);
                    subvalue.add_repeated_int32_field(3);
                    submap["test"] = subvalue;
                }
            }
            map["world"] = value;
        }
    }

    message.mutable_nested_message_with_custom_converter()->set_x(42);
    message.mutable_nested_message_with_custom_converter()->set_y(100);

    {
        auto* nestedMessage = message.add_repeated_nested_message_with_custom_converter();
        nestedMessage->set_x(12);
        nestedMessage->set_y(23);
    }

    {
        auto& map = *message.mutable_int32_to_nested_message_with_custom_converter_map();
        {
            NYT::NYson::NProto::TNestedMessageWithCustomConverter value;
            value.set_x(6);
            value.set_y(7);
            map[123] = value;
        }
    }

    {
        auto* guid = message.mutable_guid();
        guid->set_first(0xabacaba);
        guid->set_second(0xdeadbeef);
    }

    message.set_bytes_with_custom_converter("42");
    message.add_repeated_bytes_with_custom_converter("123");

    {
        NYT::NYson::NProto::TMessageExt messageExt;
        messageExt.set_x(42);
        SetProtoExtension(message.mutable_extensions(), messageExt);
    }

    TEST_PROLOGUE()
    Y_UNUSED(message.SerializeToCodedStream(&codedStream));
    TEST_EPILOGUE(TMessage)

    auto writtenNode = ConvertToNode(TYsonString(yson));
    auto expectedNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("int32_field").Value(10000)
            .Item("uint32_field").Value(10000U)
            .Item("sint32_field").Value(10000)
            .Item("int64_field").Value(10000)
            .Item("uint64_field").Value(10000U)
            .Item("fixed32_field").Value(10000U)
            .Item("fixed64_field").Value(10000U)
            .Item("sfixed32_field").Value(-10000)
            .Item("sfixed64_field").Value(10000)
            .Item("bool_field").Value(true)
            .Item("string_field").Value("hello")
            .Item("float_field").Value(3.14)
            .Item("double_field").Value(3.14)
            .Item("repeated_int32_field").BeginList()
                .Item().Value(1)
                .Item().Value(2)
                .Item().Value(3)
            .EndList()
            .Item("nested_message1").BeginMap()
                .Item("int32_field").Value(123)
                .Item("color").Value("blue")
                .Item("nested_message").BeginMap()
                    .Item("color").Value("green")
                .EndMap()
            .EndMap()
            .Item("repeated_nested_message1").BeginList()
                .Item().BeginMap()
                    .Item("int32_field").Value(456)
                    .Item("repeated_int32_field").BeginList()
                        .Item().Value(1)
                        .Item().Value(2)
                        .Item().Value(3)
                    .EndList()
                .EndMap()
                .Item().BeginMap()
                    .Item("int32_field").Value(654)
                .EndMap()
            .EndList()
            .Item("attributes").BeginMap()
                .Item("k1").Value(1)
                .Item("k2").Value("test")
                .Item("k3").BeginList()
                    .Item().Value(1)
                    .Item().Value(2)
                    .Item().Value(3)
                .EndList()
            .EndMap()
            .Item("yson_field").BeginMap()
                .Item("a").Value(1)
                .Item("b").BeginList()
                    .Item().Value("foobar")
                .EndList()
            .EndMap()
            .Item("string_to_int32_map").BeginMap()
                .Item("hello").Value(0)
                .Item("world").Value(1)
            .EndMap()
            .Item("int32_to_int32_map").BeginMap()
                .Item("100").Value(0)
                .Item("-200").Value(1)
            .EndMap()
            .Item("nested_message_map").BeginMap()
                .Item("hello").BeginMap()
                    .Item("int32_field").Value(123)
                .EndMap()
                .Item("world").BeginMap()
                    .Item("color").Value("blue")
                    .Item("nested_message_map").BeginMap()
                        .Item("test").BeginMap()
                            .Item("repeated_int32_field").BeginList()
                                .Item().Value(1)
                                .Item().Value(2)
                                .Item().Value(3)
                            .EndList()
                        .EndMap()
                    .EndMap()
                .EndMap()
            .EndMap()
            .Item("extension").BeginMap()
                .Item("extension_extension").BeginMap()
                    .Item("extension_extension_field").Value(23U)
                .EndMap()
                .Item("extension_field").Value(12U)
            .EndMap()
            .Item("nested_message_with_custom_converter").BeginMap()
                .Item("x").Value(43)
                .Item("y").Value(101)
            .EndMap()
            .Item("repeated_nested_message_with_custom_converter").BeginList()
                .Item().BeginMap()
                    .Item("x").Value(13)
                    .Item("y").Value(24)
                .EndMap()
            .EndList()
            .Item("int32_to_nested_message_with_custom_converter_map").BeginMap()
                .Item("123").BeginMap()
                    .Item("x").Value(7)
                    .Item("y").Value(8)
                .EndMap()
            .EndMap()
            .Item("guid").Value("0-deadbeef-0-abacaba")
            .Item("bytes_with_custom_converter").BeginMap()
                .Item("x").Value(43)
            .EndMap()
            .Item("repeated_bytes_with_custom_converter").BeginList()
                .Item().BeginMap()
                    .Item("x").Value(124)
                .EndMap()
            .EndList()
            .Item("extensions").BeginMap()
                .Item("ext").BeginMap()
                    .Item("x").Value(42)
                .EndMap()
            .EndMap()
        .EndMap();

    EXPECT_TRUE(AreNodesEqual(writtenNode, expectedNode));
}

TEST(TProtobufToYsonTest, Casing)
{
    NYT::NYson::NProto::TCamelCaseStyleMessage message;
    message.set_somefield(1);
    message.set_anotherfield123(2);
    message.set_crazy_field(3);
    message.set_enumfield(NYT::NYson::NProto::TCamelCaseStyleMessage::VALUE_FIRST);

    TEST_PROLOGUE()
    Y_UNUSED(message.SerializeToCodedStream(&codedStream));
    TEST_EPILOGUE(TCamelCaseStyleMessage)

    auto writtenNode = ConvertToNode(TYsonString(yson));
    auto expectedNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("some_field").Value(1)
            .Item("another_field123").Value(2)
            .Item("crazy_field").Value(3)
            .Item("enum_field").Value("value_first")
        .EndMap();
    EXPECT_TRUE(AreNodesEqual(writtenNode, expectedNode));
}

TEST(TProtobufToYsonTest, ErrorProto)
{
    NYT::NProto::TError errorProto;
    errorProto.set_message("Hello world");
    errorProto.set_code(1);
    auto attributeProto = errorProto.mutable_attributes()->add_attributes();
    attributeProto->set_key("host");
    attributeProto->set_value(ConvertToYsonString("localhost").ToString());

    auto serialized = SerializeProtoToRef(errorProto);

    ArrayInputStream inputStream(serialized.Begin(), serialized.Size());
    TString yson;
    TStringOutput outputStream(yson);
    TYsonWriter writer(&outputStream, EYsonFormat::Pretty);
    ParseProtobuf(&writer, &inputStream, ReflectProtobufMessageType<NYT::NProto::TError>());

    auto writtenNode = ConvertToNode(TYsonString(yson));
    auto expectedNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("message").Value("Hello world")
            .Item("code").Value(1)
            .Item("attributes").BeginMap()
                .Item("host").Value("localhost")
            .EndMap()
        .EndMap();
    EXPECT_TRUE(AreNodesEqual(writtenNode, expectedNode));
}

TEST(TProtobufToYsonTest, Failure)
{
    EXPECT_YPATH({
        TEST_PROLOGUE()
        codedStream.WriteTag(WireFormatLite::MakeTag(1 /*int32_field_xxx*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        TEST_EPILOGUE(TMessage)
    }, "/int32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE()
        codedStream.WriteTag(WireFormatLite::MakeTag(15 /*nested_message1*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(3);
        codedStream.WriteTag(WireFormatLite::MakeTag(19 /*color*/, WireFormatLite::WIRETYPE_VARINT));
        codedStream.WriteVarint64(100);
        TEST_EPILOGUE(TMessage)
    }, "/nested_message1/color");

    EXPECT_YPATH({
        TEST_PROLOGUE()
        codedStream.WriteTag(WireFormatLite::MakeTag(17 /*repeated_int32_field*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        TEST_EPILOGUE(TMessage)
    }, "/repeated_int32_field/0");

    EXPECT_YPATH({
        TEST_PROLOGUE()
        codedStream.WriteTag(WireFormatLite::MakeTag(17 /*repeated_int32_field*/, WireFormatLite::WIRETYPE_VARINT));
        codedStream.WriteVarint64(1);
        codedStream.WriteTag(WireFormatLite::MakeTag(17 /*repeated_int32_field*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        TEST_EPILOGUE(TMessage)
    }, "/repeated_int32_field/1");

    EXPECT_YPATH({
        TEST_PROLOGUE()
        codedStream.WriteTag(WireFormatLite::MakeTag(18 /*repeated_nested_message1*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(3);
        codedStream.WriteTag(WireFormatLite::MakeTag(19 /*color*/, WireFormatLite::WIRETYPE_VARINT));
        codedStream.WriteVarint64(2);
        codedStream.WriteTag(WireFormatLite::MakeTag(18 /*repeated_nested_message1*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(3);
        codedStream.WriteTag(WireFormatLite::MakeTag(19 /*color*/, WireFormatLite::WIRETYPE_VARINT));
        codedStream.WriteVarint64(4);
        TEST_EPILOGUE(TMessage)
    }, "/repeated_nested_message1/1/color");

    EXPECT_YPATH({
        TEST_PROLOGUE()
        codedStream.WriteTag(WireFormatLite::MakeTag(18 /*repeated_nested_message1*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(3);
        codedStream.WriteTag(WireFormatLite::MakeTag(19 /*color*/, WireFormatLite::WIRETYPE_VARINT));
        codedStream.WriteVarint64(2);
        codedStream.WriteTag(WireFormatLite::MakeTag(18 /*repeated_nested_message1*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(6);
        codedStream.WriteTag(WireFormatLite::MakeTag(100 /*repeated_int32_field*/, WireFormatLite::WIRETYPE_VARINT));
        codedStream.WriteVarint64(0);
        codedStream.WriteTag(WireFormatLite::MakeTag(100 /*repeated_int32_field*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        TEST_EPILOGUE(TMessage)
    }, "/repeated_nested_message1/1/repeated_int32_field/1");

    EXPECT_YPATH({
        TEST_PROLOGUE()
        TEST_EPILOGUE(TMessageWithRequiredFields)
    }, "/required_field");

    EXPECT_YPATH({
        TEST_PROLOGUE()
        codedStream.WriteTag(WireFormatLite::MakeTag(3 /*nested_messages*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(0);
        TEST_EPILOGUE(TMessageWithRequiredFields)
    }, "/nested_messages/0/required_field");

    EXPECT_YPATH({
        TEST_PROLOGUE()
        codedStream.WriteTag(WireFormatLite::MakeTag(3 /*nested_messages*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(4);
        codedStream.WriteTag(WireFormatLite::MakeTag(2 /*required_field*/, WireFormatLite::WIRETYPE_VARINT));
        codedStream.WriteVarint64(0);
        codedStream.WriteTag(WireFormatLite::MakeTag(2 /*required_field*/, WireFormatLite::WIRETYPE_VARINT));
        codedStream.WriteVarint64(0);
        TEST_EPILOGUE(TMessageWithRequiredFields)
    }, "/nested_messages/0/required_field");

    EXPECT_YPATH({
        TEST_PROLOGUE()
        codedStream.WriteTag(WireFormatLite::MakeTag(1 /*int32_field_xxx*/, WireFormatLite::WIRETYPE_VARINT));
        codedStream.WriteVarint64(0);
        codedStream.WriteTag(WireFormatLite::MakeTag(1 /*int32_field_xxx*/, WireFormatLite::WIRETYPE_VARINT));
        codedStream.WriteVarint64(0);
        TEST_EPILOGUE(TMessage)
    }, "/int32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE()
        codedStream.WriteTag(WireFormatLite::MakeTag(19 /*attributes*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(2);
        codedStream.WriteTag(WireFormatLite::MakeTag(1 /*attribute*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(0);
        TEST_EPILOGUE(TMessage)
    }, "/attributes");

    EXPECT_YPATH({
        TEST_PROLOGUE()
        codedStream.WriteTag(WireFormatLite::MakeTag(19 /*attributes*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(4);
        codedStream.WriteTag(WireFormatLite::MakeTag(1 /*attribute*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(2);
        codedStream.WriteTag(WireFormatLite::MakeTag(1 /*key*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(0);
        TEST_EPILOGUE(TMessage)
    }, "/attributes");

    EXPECT_YPATH({
        TEST_PROLOGUE()
        codedStream.WriteTag(WireFormatLite::MakeTag(19 /*attributes*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(4);
        codedStream.WriteTag(WireFormatLite::MakeTag(1 /*attribute*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(2);
        codedStream.WriteTag(WireFormatLite::MakeTag(2 /*value*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(0);
        TEST_EPILOGUE(TMessage)
    }, "/attributes");

    EXPECT_YPATH({
        TEST_PROLOGUE()
        codedStream.WriteTag(WireFormatLite::MakeTag(19 /*attributes*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(6);
        codedStream.WriteTag(WireFormatLite::MakeTag(1 /*attribute*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(4);
        codedStream.WriteTag(WireFormatLite::MakeTag(1 /*key*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(0);
        codedStream.WriteTag(WireFormatLite::MakeTag(1 /*key*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(0);
        TEST_EPILOGUE(TMessage)
    }, "/attributes");
}

TEST(TProtobufToYsonTest, UnknownFields)
{
    EXPECT_YPATH({
        TEST_PROLOGUE()
        codedStream.WriteTag(WireFormatLite::MakeTag(100 /*unknown*/, WireFormatLite::WIRETYPE_FIXED32));
        TEST_EPILOGUE(TMessage)
    }, "");

    {
        TProtobufParserOptions options;
        options.SkipUnknownFields = true;

        TEST_PROLOGUE()
        codedStream.WriteTag(WireFormatLite::MakeTag(100 /*unknown*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(9);
        codedStream.WriteRaw("blablabla", 9);
        codedStream.WriteTag(WireFormatLite::MakeTag(15 /*nested_message1*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(3);
        codedStream.WriteTag(WireFormatLite::MakeTag(19 /*color*/, WireFormatLite::WIRETYPE_VARINT));
        codedStream.WriteVarint64(2 /*red*/);
        TEST_EPILOGUE_WITH_OPTIONS(TMessage, options)

        auto writtenNode = ConvertToNode(TYsonString(yson));
        auto expectedNode = BuildYsonNodeFluently()
            .BeginMap()
                .Item("nested_message1").BeginMap()
                    .Item("color").Value("red")
                .EndMap()
            .EndMap();
        EXPECT_TRUE(AreNodesEqual(writtenNode, expectedNode));
    }
}

TEST(TProtobufToYsonTest, ReservedFields)
{
    TEST_PROLOGUE()
    codedStream.WriteTag(WireFormatLite::MakeTag(100 /*unknown*/, WireFormatLite::WIRETYPE_VARINT));
    codedStream.WriteVarint64(0);
    TEST_EPILOGUE(TMessageWithReservedFields)
}

#undef TEST_PROLOGUE
#undef TEST_EPILOGUE
#undef TEST_EPILOGUE_WITH_OPTIONS

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TestMessageByYPath(const TYPath& path)
{
    auto result = ResolveProtobufElementByYPath(ReflectProtobufMessageType<NYT::NYson::NProto::TMessage>(), path);
    EXPECT_TRUE(std::holds_alternative<std::unique_ptr<TProtobufMessageElement>>(result.Element));
    const auto& messageElement = std::get<std::unique_ptr<TProtobufMessageElement>>(result.Element);
    EXPECT_EQ(ReflectProtobufMessageType<T>(), messageElement->Type);
    EXPECT_EQ(path, result.HeadPath);
    EXPECT_EQ("", result.TailPath);
}

TEST(TResolveProtobufElementByYPath, Message)
{
    TestMessageByYPath<NYT::NYson::NProto::TMessage>("");
    TestMessageByYPath<NYT::NYson::NProto::TNestedMessage>("/nested_message1");
    TestMessageByYPath<NYT::NYson::NProto::TNestedMessage>("/repeated_nested_message1/0/nested_message");
    TestMessageByYPath<NYT::NYson::NProto::TNestedMessage>("/nested_message_map/k");
    TestMessageByYPath<NYT::NYson::NProto::TNestedMessage>("/nested_message_map/k/nested_message");
    TestMessageByYPath<NYT::NYson::NProto::TNestedMessage>("/nested_message_map/k/nested_message/nested_message_map/k");
}

////////////////////////////////////////////////////////////////////////////////

void TestScalarByYPath(const TYPath& path, FieldDescriptor::Type type)
{
    auto result = ResolveProtobufElementByYPath(ReflectProtobufMessageType<NYT::NYson::NProto::TMessage>(), path);
    EXPECT_TRUE(std::holds_alternative<std::unique_ptr<TProtobufScalarElement>>(result.Element));
    EXPECT_EQ(path, result.HeadPath);
    EXPECT_EQ("", result.TailPath);
    EXPECT_EQ(
        static_cast<TProtobufScalarElement::TType>(type),
        std::get<std::unique_ptr<TProtobufScalarElement>>(result.Element)->Type);
}

TEST(TResolveProtobufElementByYPath, Scalar)
{
    TestScalarByYPath("/uint32_field", FieldDescriptor::TYPE_UINT32);
    TestScalarByYPath("/repeated_int32_field/123", FieldDescriptor::TYPE_INT32);
    TestScalarByYPath("/repeated_nested_message1/0/color", FieldDescriptor::TYPE_ENUM);
    TestScalarByYPath("/nested_message_map/abc/int32_field", FieldDescriptor::TYPE_INT32);
    TestScalarByYPath("/string_to_int32_map/abc", FieldDescriptor::TYPE_INT32);
    TestScalarByYPath("/int32_to_int32_map/100", FieldDescriptor::TYPE_INT32);
}

////////////////////////////////////////////////////////////////////////////////

void TestAttributeDictionaryByYPath(const TYPath& path, const TYPath& headPath)
{
    auto result = ResolveProtobufElementByYPath(ReflectProtobufMessageType<NYT::NYson::NProto::TMessage>(), path);
    EXPECT_TRUE(std::holds_alternative<std::unique_ptr<TProtobufAttributeDictionaryElement>>(result.Element));
    EXPECT_EQ(headPath, result.HeadPath);
    EXPECT_EQ(path.substr(headPath.length()), result.TailPath);
}

TEST(TResolveProtobufElementByYPath, AttributeDictionary)
{
    TestAttributeDictionaryByYPath("/attributes", "/attributes");
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TestAnyByYPath(const TYPath& path, const TYPath& headPath)
{
    TResolveProtobufElementByYPathOptions options;
    options.AllowUnknownYsonFields = true;
    auto result = ResolveProtobufElementByYPath(ReflectProtobufMessageType<T>(), path, options);
    EXPECT_TRUE(std::holds_alternative<std::unique_ptr<TProtobufAnyElement>>(result.Element));
    EXPECT_EQ(headPath, result.HeadPath);
    EXPECT_EQ(path.substr(headPath.length()), result.TailPath);
}

TEST(TResolveProtobufElementByYPath, Any)
{
    TestAnyByYPath<NYT::NYson::NProto::TMessage>("/yson_field", "/yson_field");
    TestAnyByYPath<NYT::NYson::NProto::TMessage>("/yson_field/abc", "/yson_field");
    TestAnyByYPath<NYT::NYson::NProto::TMessage>("/attributes/abc", "/attributes/abc");
    TestAnyByYPath<NYT::NYson::NProto::TMessage>("/attributes/abc/xyz", "/attributes/abc");
    TestAnyByYPath<NYT::NYson::NProto::TExtensibleMessage>("/hello", "/hello");
    TestAnyByYPath<NYT::NYson::NProto::TExtensibleMessage>("/hello/world", "/hello");
    TestAnyByYPath<NYT::NYson::NProto::TExtensibleMessage>("/known_submessage/hello", "/known_submessage/hello");
    TestAnyByYPath<NYT::NYson::NProto::TExtensibleMessage>("/known_submessage/hello/world", "/known_submessage/hello");
    TestAnyByYPath<NYT::NYson::NProto::TExtensibleMessage>("/known_submessages/123/hello", "/known_submessages/123/hello");
    TestAnyByYPath<NYT::NYson::NProto::TExtensibleMessage>("/known_submessages/123/hello/world", "/known_submessages/123/hello");
}

////////////////////////////////////////////////////////////////////////////////

void TestRepeatedByYPath(const TYPath& path)
{
    auto result = ResolveProtobufElementByYPath(ReflectProtobufMessageType<NYT::NYson::NProto::TMessage>(), path);
    EXPECT_TRUE(std::holds_alternative<std::unique_ptr<TProtobufRepeatedElement>>(result.Element));
    EXPECT_EQ(path, result.HeadPath);
    EXPECT_EQ("", result.TailPath);
}

TEST(TResolveProtobufElementByYPath, Repeated)
{
    TestRepeatedByYPath("/repeated_int32_field");
    TestRepeatedByYPath("/nested_message1/repeated_int32_field");
}

////////////////////////////////////////////////////////////////////////////////

template <typename ValueElementType>
void TestMapByYPath(const TYPath& path, int expectedUnderlyingKeyProtoType)
{
    auto result = ResolveProtobufElementByYPath(ReflectProtobufMessageType<NYT::NYson::NProto::TMessage>(), path);

    EXPECT_EQ(path, result.HeadPath);
    EXPECT_EQ("", result.TailPath);

    auto* map = std::get_if<std::unique_ptr<TProtobufMapElement>>(&result.Element);
    ASSERT_TRUE(map);
    EXPECT_EQ(static_cast<int>((*map)->KeyElement.Type), expectedUnderlyingKeyProtoType);
    EXPECT_TRUE(std::holds_alternative<std::unique_ptr<ValueElementType>>((*map)->Element));
}

TEST(TResolveProtobufElementByYPath, Map)
{
    TestMapByYPath<TProtobufScalarElement>("/string_to_int32_map", FieldDescriptor::TYPE_STRING);
    TestMapByYPath<TProtobufScalarElement>("/int32_to_int32_map", FieldDescriptor::TYPE_INT32);
    TestMapByYPath<TProtobufMessageElement>("/nested_message_map", FieldDescriptor::TYPE_STRING);
    TestMapByYPath<TProtobufMessageElement>("/nested_message_map/abc/nested_message_map", FieldDescriptor::TYPE_STRING);
    TestMapByYPath<TProtobufMessageElement>("/nested_message1/nested_message_map", FieldDescriptor::TYPE_STRING);
}

////////////////////////////////////////////////////////////////////////////////

#define DO(path, errorPath) \
    EXPECT_YPATH({ResolveProtobufElementByYPath(ReflectProtobufMessageType<NYT::NYson::NProto::TMessage>(), path);}, errorPath);

TEST(TResolveProtobufElementByYPath, Failure)
{
    DO("/repeated_int32_field/1/2", "/repeated_int32_field/1")
    DO("/repeated_int32_field/ 1/2", "/repeated_int32_field/ 1")
    DO("/repeated_int32_field/1 /2", "/repeated_int32_field/1 ")
    DO("/repeated_int32_field/-1/2", "/repeated_int32_field/-1")
    DO("/repeated_int32_field/abc/xyz", "/repeated_int32_field/abc")
    DO("/missing", "/missing")
    DO("/repeated_nested_message1/1/xyz/abc", "/repeated_nested_message1/1/xyz")
    DO("/repeated_nested_message1/-1/xyz/abc", "/repeated_nested_message1/-1/xyz")
    DO("/repeated_nested_message1/ 1/xyz/abc", "/repeated_nested_message1/ 1")
    DO("/repeated_nested_message1/1 /xyz/abc", "/repeated_nested_message1/1 ")
    DO("/repeated_nested_message1/xyz/abc", "/repeated_nested_message1/xyz")
}

#undef DO

////////////////////////////////////////////////////////////////////////////////

TEST(TProtobufEnums, FindValueByLiteral)
{
    static const auto* type = ReflectProtobufEnumType(NYT::NYson::NProto::EColor_descriptor());
    ASSERT_EQ(std::nullopt, FindProtobufEnumValueByLiteral<NYT::NYson::NProto::EColor>(type, "zzz"));
    ASSERT_EQ(NYT::NYson::NProto::Color_Red, FindProtobufEnumValueByLiteral<NYT::NYson::NProto::EColor>(type, "red"));
}

TEST(TProtobufEnums, FindLiteralByValue)
{
    static const auto* type = ReflectProtobufEnumType(NYT::NYson::NProto::EColor_descriptor());
    ASSERT_EQ("red", FindProtobufEnumLiteralByValue(type, NYT::NYson::NProto::Color_Red));
    ASSERT_EQ(TStringBuf(), FindProtobufEnumLiteralByValue(type, NYT::NYson::NProto::EColor(666)));
}

TEST(TProtobufEnums, FindValueByLiteralWithAlias)
{
    static const auto* type = ReflectProtobufEnumType(NYT::NYson::NProto::EFlag_descriptor());
    ASSERT_EQ(NYT::NYson::NProto::Flag_True, FindProtobufEnumValueByLiteral<NYT::NYson::NProto::EFlag>(type, "true"));
    ASSERT_EQ(NYT::NYson::NProto::Flag_True, FindProtobufEnumValueByLiteral<NYT::NYson::NProto::EFlag>(type, "yes"));
}

TEST(TProtobufEnums, FindLiteralByValueWithAlias)
{
    static const auto* type = ReflectProtobufEnumType(NYT::NYson::NProto::EFlag_descriptor());
    ASSERT_EQ("true", FindProtobufEnumLiteralByValue(type, NYT::NYson::NProto::Flag_True));
    ASSERT_EQ("true", FindProtobufEnumLiteralByValue(type, NYT::NYson::NProto::Flag_Yes));
    ASSERT_EQ("true", FindProtobufEnumLiteralByValue(type, NYT::NYson::NProto::Flag_AnotherYes));
}

TEST(TProtobufEnums, ConvertToProtobufEnumValueUntyped)
{
    static const auto* type = ReflectProtobufEnumType(NYT::NYson::NProto::EColor_descriptor());
    EXPECT_EQ(
        NYT::NYson::NProto::Color_Red,
        ConvertToProtobufEnumValue<NYT::NYson::NProto::EColor>(type, BuildYsonNodeFluently().Value(2)));
    EXPECT_EQ(
        NYT::NYson::NProto::Color_Red,
        ConvertToProtobufEnumValue<NYT::NYson::NProto::EColor>(type, BuildYsonNodeFluently().Value(2u)));
    EXPECT_EQ(
        NYT::NYson::NProto::Color_Red,
        ConvertToProtobufEnumValue<NYT::NYson::NProto::EColor>(type, BuildYsonNodeFluently().Value("red")));
    EXPECT_THROW_WITH_SUBSTRING(
        ConvertToProtobufEnumValue<NYT::NYson::NProto::EColor>(type, BuildYsonNodeFluently().Value(100500)),
        "Unknown value");
    EXPECT_THROW_WITH_SUBSTRING(
        ConvertToProtobufEnumValue<NYT::NYson::NProto::EColor>(type, BuildYsonNodeFluently().Value("kizil")),
        "Unknown value");
    EXPECT_THROW_WITH_SUBSTRING(
        ConvertToProtobufEnumValue<NYT::NYson::NProto::EColor>(type, BuildYsonNodeFluently().BeginMap().EndMap()),
        "Expected integral or string");
    EXPECT_THROW_WITH_SUBSTRING(
        ConvertToProtobufEnumValue<NYT::NYson::NProto::EColor>(type, BuildYsonNodeFluently().Value(1ull << 52)),
        "out of expected range");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYsonToProtobufTest, ConvertToYsonString)
{
    auto expectedNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("int32_field").Value(10000)
            .Item("uint32_field").Value(10000U)
            .Item("sint32_field").Value(10000)
            .Item("int64_field").Value(10000)
            .Item("uint64_field").Value(10000U)
            .Item("fixed32_field").Value(10000U)
            .Item("fixed64_field").Value(10000U)
            .Item("sfixed32_field").Value(10000)
            .Item("sfixed64_field").Value(-10000)
            .Item("bool_field").Value(true)
            .Item("string_field").Value("hello")
            .Item("float_field").Value(3.14)
            .Item("double_field").Value(3.14)
            .Item("repeated_int32_field").BeginList()
                .Item().Value(1)
                .Item().Value(2)
                .Item().Value(3)
            .EndList()
            .Item("nested_message1").BeginMap()
                .Item("int32_field").Value(123)
                .Item("color").Value("blue")
                .Item("nested_message").BeginMap()
                    .Item("color").Value("green")
                .EndMap()
            .EndMap()
            .Item("repeated_nested_message1").BeginList()
                .Item().BeginMap()
                    .Item("int32_field").Value(456)
                    .Item("repeated_int32_field").BeginList()
                        .Item().Value(1)
                        .Item().Value(2)
                        .Item().Value(3)
                    .EndList()
                .EndMap()
                .Item().BeginMap()
                    .Item("int32_field").Value(654)
                .EndMap()
            .EndList()
            .Item("attributes").BeginMap()
                .Item("k1").Value(1)
                .Item("k2").Value("test")
                .Item("k3").BeginList()
                    .Item().Value(1)
                    .Item().Value(2)
                    .Item().Value(3)
                .EndList()
            .EndMap()
            .Item("yson_field").BeginMap()
                .Item("a").Value(1)
                .Item("b").BeginList()
                    .Item().Value("foobar")
                .EndList()
            .EndMap()
            .Item("string_to_int32_map").BeginMap()
                .Item("hello").Value(0)
                .Item("world").Value(1)
            .EndMap()
            .Item("int32_to_int32_map").BeginMap()
                .Item("100").Value(0)
                .Item("-200").Value(1)
            .EndMap()
            .Item("nested_message_map").BeginMap()
                .Item("hello").BeginMap()
                    .Item("int32_field").Value(123)
                .EndMap()
                .Item("world").BeginMap()
                    .Item("color").Value("blue")
                    .Item("nested_message_map").BeginMap()
                        .Item("test").BeginMap()
                            .Item("repeated_int32_field").BeginList()
                                .Item().Value(1)
                                .Item().Value(2)
                                .Item().Value(3)
                            .EndList()
                        .EndMap()
                    .EndMap()
                .EndMap()
            .EndMap()
            .Item("extension").BeginMap()
                .Item("extension_extension").BeginMap()
                    .Item("extension_extension_field").Value(23U)
                .EndMap()
                .Item("extension_field").Value(12U)
            .EndMap()
            .Item("nested_message_with_custom_converter").BeginMap()
                .Item("x").Value(43)
                .Item("y").Value(101)
            .EndMap()
            .Item("repeated_nested_message_with_custom_converter").BeginList()
                .Item().BeginMap()
                    .Item("x").Value(13)
                    .Item("y").Value(24)
                .EndMap()
            .EndList()
            .Item("int32_to_nested_message_with_custom_converter_map").BeginMap()
                .Item("123").BeginMap()
                    .Item("x").Value(7)
                    .Item("y").Value(8)
                .EndMap()
            .EndMap()
            .Item("guid").Value("0-deadbeef-0-abacaba")
            .Item("bytes_with_custom_converter").BeginMap()
                .Item("x").Value(43)
            .EndMap()
            .Item("repeated_bytes_with_custom_converter").BeginList()
                .Item().BeginMap()
                    .Item("x").Value(124)
                .EndMap()
            .EndList()
            .Item("extensions").BeginMap()
                .Item("ext").BeginMap()
                    .Item("x").Value(42)
                .EndMap()
            .EndMap()
        .EndMap();

    auto unknownFieldsMode = NYT::NYson::EUnknownYsonFieldsMode::Skip;
    auto ysonStringBinary = ConvertToYsonString(expectedNode, NYT::NYson::EYsonFormat::Binary);
    auto ysonStringText = ConvertToYsonString(expectedNode, NYT::NYson::EYsonFormat::Text);
    auto rootType = ReflectProtobufMessageType<NYT::NYson::NProto::TMessage>();
    auto protobufStringBinary = YsonStringToProto(ysonStringBinary, rootType, unknownFieldsMode);
    auto protobufStringText = YsonStringToProto(ysonStringText, rootType, unknownFieldsMode);
    ASSERT_EQ(protobufStringBinary, protobufStringText);
}

TEST(TYsonToProtobufTest, YsonStringMerger)
{
    auto expectedNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("uint32_field").Value(324234)
            .Item("repeated_nested_message1").BeginList()
                .Item().BeginMap()
                    .Item("int32_field").Value(456)
                    .Item("repeated_int32_field").BeginList()
                        .Item().Value(1)
                        .Item().Value(2)
                        .Item().Value(3)
                    .EndList()
                .EndMap()
                .Item().BeginMap()
                    .Item("int32_field").Value(654)
                .EndMap()
            .EndList()
            .Item("nested_message_map").BeginMap()
                .Item("hello").BeginMap()
                    .Item("int32_field").Value(123)
                .EndMap()
                .Item("world").BeginMap()
                    .Item("color").Value("blue")
                    .Item("nested_message_map").BeginMap()
                        .Item("test").BeginMap()
                            .Item("repeated_int32_field").BeginList()
                                .Item().Value(1)
                                .Item().Value(2)
                                .Item().Value(3)
                            .EndList()
                        .EndMap()
                    .EndMap()
                .EndMap()
            .EndMap()
        .EndMap();

    NYPath::TYPath repeatedNestedMessage1Path = "/repeated_nested_message1";
    auto repeatedNestedMessage1 = BuildYsonNodeFluently()
        .BeginList()
            .Item().BeginMap()
                .Item("int32_field").Value(456)
                .Item("repeated_int32_field").BeginList()
                    .Item().Value(1)
                    .Item().Value(2)
                    .Item().Value(3)
                .EndList()
            .EndMap()
            .Item().BeginMap()
                .Item("int32_field").Value(654)
            .EndMap()
        .EndList();

    NYPath::TYPath nestedMessageMapHelloPath = "/nested_message_map/hello";
    auto nestedMessageMapHello = BuildYsonNodeFluently()
        .BeginMap()
            .Item("int32_field").Value(123)
        .EndMap();

    NYPath::TYPath nestedMessageMapWorldPath = "/nested_message_map/world";
    auto nestedMessageMapWorld = BuildYsonNodeFluently()
        .BeginMap()
            .Item("color").Value("blue")
            .Item("nested_message_map").BeginMap()
                .Item("test").BeginMap()
                    .Item("repeated_int32_field").BeginList()
                        .Item().Value(1)
                        .Item().Value(2)
                        .Item().Value(3)
                    .EndList()
                .EndMap()
            .EndMap()
        .EndMap();

    NYPath::TYPath uint32FieldPath = "/uint32_field";
    auto uint32Field = BuildYsonNodeFluently()
        .Value(324234);

    auto paths = std::vector<NYPath::TYPath>{
        repeatedNestedMessage1Path,
        nestedMessageMapHelloPath,
        nestedMessageMapWorldPath,
        uint32FieldPath
    };

    // Yson strings of different types can be merged.
    std::vector<TYsonString> ysonStrings{
        ConvertToYsonString(repeatedNestedMessage1, EYsonFormat::Binary),
        ConvertToYsonString(nestedMessageMapHello, EYsonFormat::Text),
        ConvertToYsonString(nestedMessageMapWorld, EYsonFormat::Binary),
        ConvertToYsonString(uint32Field, EYsonFormat::Text)
    };

    std::vector<TYsonStringBuf> ysonStringBufs;
    for (const auto& ysonString : ysonStrings) {
        ysonStringBufs.push_back(ysonString);
    }
    auto ysonStringMerged = MergeYsonStrings(paths, ysonStringBufs);
    auto unknownFieldsMode = NYT::NYson::EUnknownYsonFieldsMode::Skip;
    auto ysonStringBinary = ConvertToYsonString(expectedNode, NYT::NYson::EYsonFormat::Binary);
    auto ysonStringText = ConvertToYsonString(expectedNode, NYT::NYson::EYsonFormat::Text);
    auto rootType = ReflectProtobufMessageType<NYT::NYson::NProto::TMessage>();
    auto protobufStringBinary = YsonStringToProto(ysonStringBinary, rootType, unknownFieldsMode);
    auto protobufStringText = YsonStringToProto(ysonStringText, rootType, unknownFieldsMode);
    auto protobufStringMerged = YsonStringToProto(ysonStringMerged, rootType, unknownFieldsMode);
    ASSERT_EQ(protobufStringBinary, protobufStringText);
    ASSERT_EQ(protobufStringBinary, protobufStringMerged);
}

template <class T, class TNodeList, class TRepeated>
void CopyToProto(const TNodeList& from, TRepeated& rep)
{
    for (auto child : from->GetChildren()) {
        rep.Add(child->template GetValue<T>());
    }
}

TEST(TPackedRepeatedProtobufTest, TestSerializeDeserialize)
{
    auto node = BuildYsonNodeFluently()
        .BeginMap()
            .Item("int32_rep").BeginList()
                .Item().Value(0)
                .Item().Value(42)
                .Item().Value(-100)
                .Item().Value(Max<i32>())
                .Item().Value(Min<i32>())
            .EndList()
            .Item("int64_rep").BeginList()
                .Item().Value(0)
                .Item().Value(42)
                .Item().Value(-100)
                .Item().Value(Min<i64>())
                .Item().Value(Max<i64>())
            .EndList()
            .Item("uint32_rep").BeginList()
                .Item().Value(0U)
                .Item().Value(42U)
                .Item().Value(Min<ui32>())
                .Item().Value(Max<ui32>())
            .EndList()
            .Item("uint64_rep").BeginList()
                .Item().Value(0U)
                .Item().Value(42U)
                .Item().Value(Min<ui64>())
                .Item().Value(Max<ui64>())
            .EndList()
            .Item("float_rep").BeginList()
                .Item().Value(0.F)
                .Item().Value(42.F)
                .Item().Value(-100.F)
                .Item().Value(Min<float>())
                .Item().Value(Max<float>())
            .EndList()
            .Item("double_rep").BeginList()
                .Item().Value(0.)
                .Item().Value(42.)
                .Item().Value(-100.)
                .Item().Value(Min<double>())
                .Item().Value(Max<double>())
            .EndList()
            .Item("fixed32_rep").BeginList()
                .Item().Value(0U)
                .Item().Value(42U)
                .Item().Value(Min<ui32>())
                .Item().Value(Max<ui32>())
            .EndList()
            .Item("fixed64_rep").BeginList()
                .Item().Value(0U)
                .Item().Value(42U)
                .Item().Value(Min<ui64>())
                .Item().Value(Max<ui64>())
            .EndList()
            .Item("sfixed32_rep").BeginList()
                .Item().Value(0)
                .Item().Value(42)
                .Item().Value(-100)
                .Item().Value(Max<i32>())
                .Item().Value(Min<i32>())
            .EndList()
            .Item("sfixed64_rep").BeginList()
                .Item().Value(0)
                .Item().Value(42)
                .Item().Value(-100)
                .Item().Value(Max<i64>())
                .Item().Value(Min<i64>())
            .EndList()
            .Item("enum_rep").BeginList()
                .Item().Value(0)
                .Item().Value(1)
            .EndList()
        .EndMap();


    NProto::TPackedRepeatedMessage serializedMessage;
    TProtobufWriterOptions options;
    DeserializeProtobufMessage(
        serializedMessage,
        ReflectProtobufMessageType<NProto::TPackedRepeatedMessage>(),
        node,
        options);

    NProto::TPackedRepeatedMessage manuallyBuildMessage;
    {
        auto map = node->AsMap();
        CopyToProto<i64>(map->FindChild("int32_rep")->AsList(), *manuallyBuildMessage.mutable_int32_rep());
        CopyToProto<i64>(map->FindChild("int64_rep")->AsList(), *manuallyBuildMessage.mutable_int64_rep());
        CopyToProto<ui64>(map->FindChild("uint32_rep")->AsList(), *manuallyBuildMessage.mutable_uint32_rep());
        CopyToProto<ui64>(map->FindChild("uint64_rep")->AsList(), *manuallyBuildMessage.mutable_uint64_rep());
        CopyToProto<double>(map->FindChild("float_rep")->AsList(), *manuallyBuildMessage.mutable_float_rep());
        CopyToProto<double>(map->FindChild("double_rep")->AsList(), *manuallyBuildMessage.mutable_double_rep());
        CopyToProto<ui64>(map->FindChild("fixed32_rep")->AsList(), *manuallyBuildMessage.mutable_fixed32_rep());
        CopyToProto<ui64>(map->FindChild("fixed64_rep")->AsList(), *manuallyBuildMessage.mutable_fixed64_rep());
        CopyToProto<i64>(map->FindChild("sfixed32_rep")->AsList(), *manuallyBuildMessage.mutable_sfixed32_rep());
        CopyToProto<i64>(map->FindChild("sfixed64_rep")->AsList(), *manuallyBuildMessage.mutable_sfixed64_rep());
        CopyToProto<i64>(map->FindChild("enum_rep")->AsList(), *manuallyBuildMessage.mutable_enum_rep());
    }

    // Check manuallyBuildMessage
    {
        TString protobufString;
        Y_UNUSED(manuallyBuildMessage.SerializeToString(&protobufString));

        TString newYsonString;
        TStringOutput newYsonOutputStream(newYsonString);
        TYsonWriter ysonWriter(&newYsonOutputStream, EYsonFormat::Pretty);
        ArrayInputStream protobufInput(protobufString.data(), protobufString.length());
        EXPECT_NO_THROW(ParseProtobuf(&ysonWriter, &protobufInput, ReflectProtobufMessageType<NYT::NYson::NProto::TPackedRepeatedMessage>()));

        EXPECT_TRUE(AreNodesEqual(ConvertToNode(TYsonString(newYsonString)), node));
    }

    // Check serializedMessage
    {
        TString protobufString;
        Y_UNUSED(serializedMessage.SerializeToString(&protobufString));

        TString newYsonString;
        TStringOutput newYsonOutputStream(newYsonString);
        TYsonWriter ysonWriter(&newYsonOutputStream, EYsonFormat::Pretty);
        ArrayInputStream protobufInput(protobufString.data(), protobufString.length());
        EXPECT_NO_THROW(ParseProtobuf(&ysonWriter, &protobufInput, ReflectProtobufMessageType<NYT::NYson::NProto::TPackedRepeatedMessage>()));

        EXPECT_TRUE(AreNodesEqual(ConvertToNode(TYsonString(newYsonString)), node));
    }
}

TEST(TEnumYsonStorageTypeTest, TestDeserializeSerialize)
{
    for (auto storageType: {EEnumYsonStorageType::String, EEnumYsonStorageType::Int}) {
        auto config = New<TProtobufInteropConfig>();
        config->DefaultEnumYsonStorageType = storageType;
        SetProtobufInteropConfig(config);

        NProto::TMessageWithEnums message;
        {
            auto zero = NProto::TMessageWithEnums_EEnum::TMessageWithEnums_EEnum_VALUE0;
            auto one = NProto::TMessageWithEnums_EEnum::TMessageWithEnums_EEnum_VALUE1;

            message.set_enum_int(zero);
            message.add_enum_rep_not_packed_int(zero);
            message.add_enum_rep_not_packed_int(one);
            message.add_enum_rep_packed_int(zero);
            message.add_enum_rep_packed_int(one);

            message.set_enum_string(one);
            message.add_enum_rep_not_packed_string(one);
            message.add_enum_rep_not_packed_string(zero);
            message.add_enum_rep_packed_string(one);
            message.add_enum_rep_packed_string(zero);

            message.set_enum_without_option(zero);
            message.add_enum_rep_not_packed_without_option(zero);
            message.add_enum_rep_not_packed_without_option(one);
            message.add_enum_rep_packed_without_option(zero);
            message.add_enum_rep_packed_without_option(one);
        }

        // Proto message to yson.
        TString stringWithYson;
        {
            TString protobufString;
            Y_UNUSED(message.SerializeToString(&protobufString));
            TStringOutput ysonOutputStream(stringWithYson);
            TYsonWriter ysonWriter(&ysonOutputStream, EYsonFormat::Pretty);
            ArrayInputStream protobufInput(protobufString.data(), protobufString.length());

            EXPECT_NO_THROW(ParseProtobuf(&ysonWriter, &protobufInput, ReflectProtobufMessageType<NProto::TMessageWithEnums>()));
        }

        // Check enum representation in yson.
        auto resultedNode = ConvertToNode(TYsonString(stringWithYson));
        {
            auto expectedNode = BuildYsonNodeFluently()
                .BeginMap()
                    .Item("enum_int").Value(0)
                    .Item("enum_rep_not_packed_int").BeginList()
                        .Item().Value(0)
                        .Item().Value(1)
                    .EndList()
                    .Item("enum_rep_packed_int").BeginList()
                        .Item().Value(0)
                        .Item().Value(1)
                    .EndList()

                    .Item("enum_string").Value("VALUE1")
                    .Item("enum_rep_not_packed_string").BeginList()
                        .Item().Value("VALUE1")
                        .Item().Value("VALUE0")
                    .EndList()
                    .Item("enum_rep_packed_string").BeginList()
                        .Item().Value("VALUE1")
                        .Item().Value("VALUE0")
                    .EndList()
                .EndMap();

                auto map = expectedNode->AsMap();
                switch (storageType) {
                    case EEnumYsonStorageType::String:
                        map->AsMap()->AddChild("enum_without_option", BuildYsonNodeFluently().Value("VALUE0"));
                        map->AsMap()->AddChild("enum_rep_not_packed_without_option", BuildYsonNodeFluently()
                            .BeginList()
                                .Item().Value("VALUE0")
                                .Item().Value("VALUE1")
                            .EndList());
                        map->AsMap()->AddChild("enum_rep_packed_without_option", BuildYsonNodeFluently()
                            .BeginList()
                                .Item().Value("VALUE0")
                                .Item().Value("VALUE1")
                            .EndList());
                        break;
                    case EEnumYsonStorageType::Int:
                        map->AsMap()->AddChild("enum_without_option", BuildYsonNodeFluently().Value(0));
                        map->AsMap()->AddChild("enum_rep_not_packed_without_option", BuildYsonNodeFluently()
                            .BeginList()
                                .Item().Value(0)
                                .Item().Value(1)
                            .EndList());
                        map->AsMap()->AddChild("enum_rep_packed_without_option", BuildYsonNodeFluently()
                            .BeginList()
                                .Item().Value(0)
                                .Item().Value(1)
                            .EndList());
                        break;
                }

            EXPECT_TRUE(AreNodesEqual(resultedNode, expectedNode));
        }

        // Yson to proto message.
        NProto::TMessageWithEnums resultedMessage;
        DeserializeProtobufMessage(
            resultedMessage,
            ReflectProtobufMessageType<NProto::TMessageWithEnums>(),
            resultedNode,
            TProtobufWriterOptions{});

        // Check that original message is equal to its deserialized + serialized version
        EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(message, resultedMessage));
    }
}

TEST(TYsonToProtobufTest, Casing)
{
    auto ysonNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("some_field").Value(1)
            .Item("another_field123").Value(2)
        .EndMap();
    auto ysonString = ConvertToYsonString(ysonNode);

    NYson::TProtobufWriterOptions protobufWriterOptions;
    protobufWriterOptions.ConvertSnakeToCamelCase = true;

    NProto::TExternalProtobuf message;
    message.ParseFromStringOrThrow(NYson::YsonStringToProto(
        ysonString,
        NYson::ReflectProtobufMessageType<NProto::TExternalProtobuf>(),
        protobufWriterOptions));

    EXPECT_EQ(message.somefield(), 1);
    EXPECT_EQ(message.anotherfield123(), 2);
}

} // namespace
} // namespace NYT::NYson

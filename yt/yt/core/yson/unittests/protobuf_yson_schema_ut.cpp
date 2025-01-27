#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/yson/unittests/proto/protobuf_yson_schema_ut.pb.h>

#include <yt/yt/core/yson/config.h>
#include <yt/yt/core/yson/protobuf_interop.h>
#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <util/generic/scope.h>
#include <util/stream/str.h>

namespace NYT::NYson {
namespace {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TProtobufYsonSchemaTest, WriteMessageSchema)
{
    TStringStream outputStream;
    TYsonWriter ysonWriter(&outputStream, EYsonFormat::Text);

    WriteSchema(ReflectProtobufMessageType<NProto::TTestSchemaMessage>(), &ysonWriter);
    TStringBuf expected = R"({
        type_name="struct";
        members=[
            {name="int32_field";type="int32";};
            {name="uint32_field";type="uint32";};
            {name="sint32_field";type="int32";};
            {name="int64_field";type="int64";};
            {name="uint64_field";type="uint64";};
            {name="sint64_field";type="int64";};
            {name="fixed32_field";type="uint32";};
            {name="fixed64_field";type="uint64";};
            {name="sfixed32_field";type="int32";};
            {name="sfixed64_field";type="int64";};
            {name="bool_field";type="bool";};
            {name="string_field";type="utf8";};
            {name="bytes_field";type="string";};
            {name="float_field";type="float";};
            {name="double_field";type="double";};
            {name="enum_field";type={type_name="enum";enum_name="EEnum";"values"=["value0";"value1";];};};
            {name="required_int32_field";type="int32";required=%true;};
            {name="repeated_int32_field";type={type_name="list";item="int32";};};
            {name="string_to_int32_map";type={type_name="dict";key="utf8";value="int32";};};
            {name="int32_to_string_map";type={type_name="dict";key="int32";value="utf8";};};
        ];
    })";

    auto expectedNode = ConvertToNode(TYsonStringBuf(expected), GetEphemeralNodeFactory());
    auto actualNode = ConvertToNode(TYsonStringBuf(outputStream.Str()), GetEphemeralNodeFactory());

    EXPECT_TRUE(AreNodesEqual(actualNode, expectedNode))
        << "Expected: " << ConvertToYsonString(expectedNode, EYsonFormat::Pretty).AsStringBuf() << "\n\n"
        << "Actual: " << ConvertToYsonString(actualNode, EYsonFormat::Text).AsStringBuf() << "\n\n";
}

TEST(TProtobufYsonSchemaTest, CamelCaseNames)
{
    TStringStream outputStream;
    TYsonWriter ysonWriter(&outputStream, EYsonFormat::Text);

    WriteSchema(ReflectProtobufMessageType<NProto::TCamelCaseStyleTestSchemaMessage1>(), &ysonWriter);
    TStringBuf expected = R"({
        type_name="struct";
        members=[
            {name="SomeField";type="int32";};
            {name="EnumField";type={type_name="enum";enum_name="EEnum";"values"=["VALUE_NONE";"VALUE_FIRST";];};};
        ];
    })";

    auto expectedNode = ConvertToNode(TYsonStringBuf(expected), GetEphemeralNodeFactory());
    auto actualNode = ConvertToNode(TYsonStringBuf(outputStream.Str()), GetEphemeralNodeFactory());

    EXPECT_TRUE(AreNodesEqual(actualNode, expectedNode))
        << "Expected: " << ConvertToYsonString(expectedNode, EYsonFormat::Pretty).AsStringBuf() << "\n\n"
        << "Actual: " << ConvertToYsonString(actualNode, EYsonFormat::Pretty).AsStringBuf() << "\n\n";
}

TEST(TProtobufYsonSchemaTest, ForceSnakeCaseNames)
{
    auto oldConfig = GetProtobufInteropConfig();
    Y_DEFER {
        SetProtobufInteropConfig(oldConfig);
    };

    auto newConfig = CloneYsonStruct(oldConfig);
    newConfig->ForceSnakeCaseNames = true;
    SetProtobufInteropConfig(newConfig);

    TStringStream outputStream;
    TYsonWriter ysonWriter(&outputStream, EYsonFormat::Text);

    WriteSchema(ReflectProtobufMessageType<NProto::TCamelCaseStyleTestSchemaMessage2>(), &ysonWriter);
    TStringBuf expected = R"({
        type_name="struct";
        members=[
            {name="some_field";type="int32";};
            {name="enum_field";type={type_name="enum";enum_name="EEnum";"values"=["value_none";"value_first";];};};
        ];
    })";

    auto expectedNode = ConvertToNode(TYsonStringBuf(expected), GetEphemeralNodeFactory());
    auto actualNode = ConvertToNode(TYsonStringBuf(outputStream.Str()), GetEphemeralNodeFactory());

    EXPECT_TRUE(AreNodesEqual(actualNode, expectedNode))
        << "Expected: " << ConvertToYsonString(expectedNode, EYsonFormat::Pretty).AsStringBuf() << "\n\n"
        << "Actual: " << ConvertToYsonString(actualNode, EYsonFormat::Pretty).AsStringBuf() << "\n\n";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYson

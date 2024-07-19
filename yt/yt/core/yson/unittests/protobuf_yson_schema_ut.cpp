#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/yson/unittests/proto/protobuf_yson_schema_ut.pb.h>

#include <yt/yt/core/yson/protobuf_interop.h>
#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <util/stream/str.h>


namespace NYT::NYson {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TProtobufYsonSchemaTest, GetMessageSchema)
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

    auto expectedNode = NYTree::ConvertToNode(TYsonStringBuf(expected), NYTree::GetEphemeralNodeFactory());
    auto actualNode = NYTree::ConvertToNode(TYsonStringBuf(outputStream.Str()), NYTree::GetEphemeralNodeFactory());

    EXPECT_TRUE(NYTree::AreNodesEqual(actualNode, expectedNode))
        << "Expected: " << ConvertToYsonString(expectedNode, EYsonFormat::Text, 4).AsStringBuf() << "\n\n"
        << "Actual: " << ConvertToYsonString(actualNode, EYsonFormat::Text, 4).AsStringBuf() << "\n\n";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYson

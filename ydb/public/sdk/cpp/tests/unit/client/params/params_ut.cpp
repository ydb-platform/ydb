#include <gtest/gtest.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/params/params.h>
#include <google/protobuf/text_format.h>
#include <ydb/public/api/protos/ydb_value.pb.h>

using namespace NYdb;

using TExpectedErrorException = yexception;

void CheckProtoValue(const Ydb::Value& value, const TString& expected) {
    TStringType protoStr;
    google::protobuf::TextFormat::PrintToString(value, &protoStr);
    ASSERT_EQ(protoStr, expected);
}

TEST(ParamsBuilder, Build) {
    auto params = TParamsBuilder()
        .AddParam("$param1")
            .BeginList()
            .AddListItem()
                .BeginOptional()
                    .Uint64(10)
                .EndOptional()
            .AddListItem()
                .EmptyOptional()
            .EndList()
            .Build()
        .AddParam("$param2")
            .String("test")
            .Build()
        .Build();

    ASSERT_EQ(FormatType(params.GetValue("$param1")->GetType()), "List<Uint64?>");

    CheckProtoValue(params.GetValue("$param1")->GetProto(),
        "items {\n"
        "  uint64_value: 10\n"
        "}\n"
        "items {\n"
        "  null_flag_value: NULL_VALUE\n"
        "}\n");

    ASSERT_EQ(FormatType(params.GetValue("$param2")->GetType()), "String");
    CheckProtoValue(params.GetValue("$param2")->GetProto(), "bytes_value: \"test\"\n");
}

TEST(ParamsBuilder, BuildFromValue) {
    auto value2 = TValueBuilder()
        .BeginList()
        .AddListItem()
            .BeginOptional()
                .BeginTuple()
                .AddElement()
                    .Int32(-11)
                .AddElement()
                    .String("test2")
                .EndTuple()
            .EndOptional()
        .AddListItem()
            .EmptyOptional()
        .EndList()
        .Build();

    auto params = TParamsBuilder()
        .AddParam("$param1")
            .Utf8("test1")
            .Build()
        .AddParam("$param2", value2)
        .Build();

    ASSERT_EQ(FormatType(params.GetValue("$param1")->GetType()), "Utf8");

    CheckProtoValue(params.GetValue("$param1")->GetProto(), "text_value: \"test1\"\n");

    ASSERT_EQ(FormatType(params.GetValue("$param2")->GetType()), "List<Tuple<Int32,String>?>");
    CheckProtoValue(params.GetValue("$param2")->GetProto(),
        "items {\n"
        "  items {\n"
        "    int32_value: -11\n"
        "  }\n"
        "  items {\n"
        "    bytes_value: \"test2\"\n"
        "  }\n"
        "}\n"
        "items {\n"
        "  null_flag_value: NULL_VALUE\n"
        "}\n");
}

TEST(ParamsBuilder, BuildWithTypeInfo) {
    auto param1Type = TTypeBuilder()
        .BeginList()
            .Primitive(EPrimitiveType::String)
        .EndList()
        .Build();

    auto param2Type = TTypeBuilder()
        .BeginOptional()
            .Primitive(EPrimitiveType::Uint32)
        .EndOptional()
        .Build();

    std::map<std::string, TType> paramsMap;
    paramsMap.emplace("$param1", param1Type);
    paramsMap.emplace("$param2", param2Type);

    auto value1 = TValueBuilder()
        .BeginList()
        .AddListItem()
            .String("str1")
        .AddListItem()
            .String("str2")
        .EndList()
        .Build();

    auto params = TParamsBuilder(paramsMap)
        .AddParam("$param1", value1)
        .AddParam("$param2")
            .EmptyOptional()
            .Build()
        .Build();

    ASSERT_EQ(FormatType(params.GetValue("$param1")->GetType()), "List<String>");
    CheckProtoValue(params.GetValue("$param1")->GetProto(),
        "items {\n"
        "  bytes_value: \"str1\"\n"
        "}\n"
        "items {\n"
        "  bytes_value: \"str2\"\n"
        "}\n");

    ASSERT_EQ(FormatType(params.GetValue("$param2")->GetType()), "Uint32?");
    CheckProtoValue(params.GetValue("$param2")->GetProto(), "null_flag_value: NULL_VALUE\n");
}

TEST(ParamsBuilder, MissingParam) {
    auto param1Type = TTypeBuilder()
        .BeginList()
            .Primitive(EPrimitiveType::String)
        .EndList()
        .Build();

    auto param2Type = TTypeBuilder()
        .BeginOptional()
            .Primitive(EPrimitiveType::Uint32)
        .EndOptional()
        .Build();

    std::map<std::string, TType> paramsMap;
    paramsMap.emplace("$param1", param1Type);
    paramsMap.emplace("$param2", param2Type);

    ASSERT_THROW({
        auto params = TParamsBuilder(paramsMap)
            .AddParam("$param3")
                .EmptyOptional()
                .Build()
            .Build();
    }, TExpectedErrorException);
}

TEST(ParamsBuilder, IncompleteParam) {
    auto paramsBuilder = TParamsBuilder();

    auto& param1Builder = paramsBuilder.AddParam("$param1");
    auto& param2Builder = paramsBuilder.AddParam("$param2");

    param1Builder
        .BeginList()
        .AddListItem()
            .BeginOptional()
                .Uint64(10)
            .EndOptional()
        .AddListItem()
            .EmptyOptional()
        .EndList()
        .Build();

    param2Builder.String("test");

    ASSERT_THROW(paramsBuilder.Build(), TExpectedErrorException);
}

TEST(ParamsBuilder, TypeMismatch) {
    auto param1Type = TTypeBuilder()
        .BeginList()
            .Primitive(EPrimitiveType::String)
        .EndList()
        .Build();

    auto param2Type = TTypeBuilder()
        .BeginOptional()
            .Primitive(EPrimitiveType::Uint32)
        .EndOptional()
        .Build();

    std::map<std::string, TType> paramsMap;
    paramsMap.emplace("$param1", param1Type);
    paramsMap.emplace("$param2", param2Type);

    ASSERT_THROW({
        auto params = TParamsBuilder(paramsMap)
            .AddParam("$param1")
                .EmptyOptional()
                .Build()
            .Build();
    }, TExpectedErrorException);
}

TEST(ParamsBuilder, TypeMismatchFromValue) {
    auto param1Type = TTypeBuilder()
        .BeginList()
            .Primitive(EPrimitiveType::String)
        .EndList()
        .Build();

    auto param2Type = TTypeBuilder()
        .BeginOptional()
            .Primitive(EPrimitiveType::Uint32)
        .EndOptional()
        .Build();

    std::map<std::string, TType> paramsMap;
    paramsMap.emplace("$param1", param1Type);
    paramsMap.emplace("$param2", param2Type);

    auto value1 = TValueBuilder()
        .BeginList()
        .AddListItem()
            .String("str1")
        .AddListItem()
            .String("str2")
        .EndList()
        .Build();

    ASSERT_THROW({
        auto params = TParamsBuilder(paramsMap)
            .AddParam("$param2", value1)
            .Build();
    }, TExpectedErrorException);
}

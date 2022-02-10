#include <ydb/public/lib/yson_value/ydb_yson_value.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

using namespace NYdb;

using TExpectedErrorException = yexception;

Y_UNIT_TEST_SUITE(ParamsBuilder) {
    Y_UNIT_TEST(Build) {
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

        UNIT_ASSERT_NO_DIFF(FormatType(params.GetValue("$param1")->GetType()),
            R"(List<Uint64?>)");
        UNIT_ASSERT_NO_DIFF(FormatValueYson(*params.GetValue("$param1")),
            R"([[10u];#])");

        UNIT_ASSERT_NO_DIFF(FormatType(params.GetValue("$param2")->GetType()),
            R"(String)");
        UNIT_ASSERT_NO_DIFF(FormatValueYson(*params.GetValue("$param2")),
            R"("test")");
    }

    Y_UNIT_TEST(BuildFromValue) {
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

        UNIT_ASSERT_NO_DIFF(FormatType(params.GetValue("$param1")->GetType()),
            R"(Utf8)");
        UNIT_ASSERT_NO_DIFF(FormatValueYson(*params.GetValue("$param1")),
            R"("test1")");

        UNIT_ASSERT_NO_DIFF(FormatType(params.GetValue("$param2")->GetType()),
            R"(List<Tuple<Int32,String>?>)");
        UNIT_ASSERT_NO_DIFF(FormatValueYson(*params.GetValue("$param2")),
            R"([[[-11;"test2"]];#])");
    }

    Y_UNIT_TEST(BuildWithTypeInfo) {
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

        TMap<TString, TType> paramsMap;
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

        UNIT_ASSERT_NO_DIFF(FormatType(params.GetValue("$param1")->GetType()),
            R"(List<String>)");
        UNIT_ASSERT_NO_DIFF(FormatValueYson(*params.GetValue("$param1")),
            R"(["str1";"str2"])");

        UNIT_ASSERT_NO_DIFF(FormatType(params.GetValue("$param2")->GetType()),
            R"(Uint32?)");
        UNIT_ASSERT_NO_DIFF(FormatValueYson(*params.GetValue("$param2")),
            R"(#)");
    }

    Y_UNIT_TEST(MissingParam) {
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

        TMap<TString, TType> paramsMap;
        paramsMap.emplace("$param1", param1Type);
        paramsMap.emplace("$param2", param2Type);

        try {
            auto params = TParamsBuilder(paramsMap)
                .AddParam("$param3")
                    .EmptyOptional()
                    .Build()
                .Build();
        } catch (const TExpectedErrorException& e) {
            return;
        }

        UNIT_ASSERT(false);
    }

    Y_UNIT_TEST(IncompleteParam) {
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

        try {
            paramsBuilder.Build();
        } catch (const TExpectedErrorException& e) {
            return;
        }

        UNIT_ASSERT(false);
    }

    Y_UNIT_TEST(TypeMismatch) {
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

        TMap<TString, TType> paramsMap;
        paramsMap.emplace("$param1", param1Type);
        paramsMap.emplace("$param2", param2Type);

        try {
            auto params = TParamsBuilder(paramsMap)
                .AddParam("$param1")
                    .EmptyOptional()
                    .Build()
                .Build();
        } catch (const TExpectedErrorException& e) {
            return;
        }

        UNIT_ASSERT(false);
    }

    Y_UNIT_TEST(TypeMismatchFromValue) {
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

        TMap<TString, TType> paramsMap;
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

        try {
            auto params = TParamsBuilder(paramsMap)
                .AddParam("$param2", value1)
                .Build();
        } catch (const TExpectedErrorException& e) {
            return;
        }

        UNIT_ASSERT(false);
    }
}

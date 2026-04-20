#include <ydb/core/util/struct_log/create_message.h>
#include <ydb/core/util/struct_log/json_writer.h>
#include <ydb/core/util/struct_log/key_name.h>
#include <ydb/core/util/struct_log/log_stack.h>
#include <ydb/core/util/struct_log/native_types_mapping.h>
#include <ydb/core/util/struct_log/native_types_support.h>
#include <ydb/core/util/struct_log/structured_message.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NStructLog {

template <typename T>
void TestType(const std::vector<T>& values) {
    auto typeCode = TTypesMapping::GetCode<T>();

    for(auto  value: values) {
        auto originalStr = TTypesMapping::ToString(value);

        TBinaryData data;
        TTypesMapping::Serialize<T>(value, data);

        T readValue;
        auto hasRead = TTypesMapping::Deserialize<T>(readValue, typeCode, data.data(), data.size());
        UNIT_ASSERT(hasRead);

        TString recoveredStr;
        if (hasRead) {
            recoveredStr = TTypesMapping::ToString(readValue);
        }

        UNIT_ASSERT(value==readValue);
    }
}

Y_UNIT_TEST_SUITE(StructLog) {
    Y_UNIT_TEST(NativeTypes) {
        TestType<ui8>({0, 1, 2, 3, 4, 5});
        TestType<i8>({0, 1, 2, 3, 4, 5});
        TestType<ui16>({0, 1, 2, 3, 4, 5});
        TestType<i16>({0, 1, 2, 3, 4, 5});
        TestType<ui32>({0, 1, 2, 3, 4, 5});
        TestType<i32>({0, 1, 2, 3, 4, 5});
        TestType<ui64>({0, 1, 2, 3, 4, 5});
        TestType<i64>({0, 1, 2, 3, 4, 5});
        TestType<bool>({true, false});
        TestType<TString>({"", "a", "ab", "abc"});

        TestType<float>({1, 1.2, 1.23, 1.234, 1.2345});
    }

    Y_UNIT_TEST(TestKeyName) {
        // compile vs compile
        UNIT_ASSERT(TKeyName("aaa") < TKeyName("bbb"));
        UNIT_ASSERT(TKeyName("aa") < TKeyName("aaa"));
        UNIT_ASSERT(TKeyName("aaa") == TKeyName("aaa"));
        UNIT_ASSERT(TKeyName("aaa") > TKeyName("aa"));
        UNIT_ASSERT(TKeyName("bbb") > TKeyName("aaa"));

        // compile vs string
        UNIT_ASSERT(TKeyName("aaa") < TKeyName(TString("bbb")));
        UNIT_ASSERT(TKeyName("aa") < TKeyName(TString("aaa")));
        UNIT_ASSERT(TKeyName("aaa") == TKeyName(TString("aaa")));
        UNIT_ASSERT(TKeyName("aaa") > TKeyName(TString("aa")));
        UNIT_ASSERT(TKeyName("bbb") > TKeyName(TString("aaa")));

        // string vs compile
        UNIT_ASSERT(TKeyName(TString("aaa")) < TKeyName("bbb"));
        UNIT_ASSERT(TKeyName(TString("aa")) < TKeyName("aaa"));
        UNIT_ASSERT(TKeyName(TString("aaa")) == TKeyName("aaa"));
        UNIT_ASSERT(TKeyName(TString("aaa")) > TKeyName("aa"));
        UNIT_ASSERT(TKeyName(TString("bbb")) > TKeyName("aaa"));

        // string vs string
        UNIT_ASSERT(TKeyName(TString("aaa")) < TKeyName(TString("bbb")));
        UNIT_ASSERT(TKeyName(TString("aa")) < TKeyName(TString("aaa")));
        UNIT_ASSERT(TKeyName(TString("aaa")) == TKeyName(TString("aaa")));
        UNIT_ASSERT(TKeyName(TString("aaa")) > TKeyName(TString("aa")));
        UNIT_ASSERT(TKeyName(TString("bbb")) > TKeyName(TString("aaa")));
    }

    template <typename T>
    void TestCreateMessageTypedValueRead(const std::vector<T>& values) {
        for(auto value: values) {
            auto message = YDBLOG_CREATE_MESSAGE({"value", value});

            auto index = message.GetValueIndex("value");
            UNIT_ASSERT(index.has_value());

            UNIT_ASSERT(message.template CheckValueType<T>(index.value()));

            UNIT_ASSERT(message.template GetValue<T>("value") == value);
        }
    }

    Y_UNIT_TEST(CreateMessageValueRead) {
        TestCreateMessageTypedValueRead<std::uint8_t>({0, 1, 2, 3, 4, 5});
        TestCreateMessageTypedValueRead<std::int8_t>({0, 1, 2, 3, 4, 5});
        TestCreateMessageTypedValueRead<std::uint16_t>({0, 1, 2, 3, 4, 5});
        TestCreateMessageTypedValueRead<std::int16_t>({0, 1, 2, 3, 4, 5});
        TestCreateMessageTypedValueRead<std::uint32_t>({0, 1, 2, 3, 4, 5});
        TestCreateMessageTypedValueRead<std::int32_t>({0, 1, 2, 3, 4, 5});
        TestCreateMessageTypedValueRead<std::uint64_t>({0, 1, 2, 3, 4, 5});
        TestCreateMessageTypedValueRead<std::int64_t>({0, 1, 2, 3, 4, 5});
        TestCreateMessageTypedValueRead<bool>({true, false});
        TestCreateMessageTypedValueRead<TString>({"", "a", "ab", "abc"});
    }

    Y_UNIT_TEST(SortValues) {
        {
            auto message = YDBLOG_CREATE_MESSAGE({"v1", 2});
            UNIT_ASSERT(message.GetValuesCount() == 1);
            UNIT_ASSERT(message.GetValueIndex("v1") == 0);
            UNIT_ASSERT(!message.GetValueIndex("v2").has_value());

            UNIT_ASSERT(message.GetValue<int>("v1") == 2);
        }
        {
            auto message = YDBLOG_CREATE_MESSAGE({"v3", 3}, {"v2", 1}, {"v1", 2});
            UNIT_ASSERT(message.GetValuesCount() == 3);
            UNIT_ASSERT(message.GetValueIndex("v1") == 0);
            UNIT_ASSERT(message.GetValueIndex("v2") == 1);
            UNIT_ASSERT(message.GetValueIndex("v3") == 2);
            UNIT_ASSERT(!message.GetValueIndex("v4").has_value());

            UNIT_ASSERT(message.GetValue<int>("v1") == 2);
            UNIT_ASSERT(message.GetValue<int>("v2") == 1);
            UNIT_ASSERT(message.GetValue<int>("v3") == 3);
        }
        {
            auto message = YDBLOG_CREATE_MESSAGE({"v0", 1}, {"v2", 1}, {"v1", 1}, {"v1", 2}, {"v1", 3});
            UNIT_ASSERT(message.GetValuesCount() == 3);
            UNIT_ASSERT(message.GetValueIndex("v0") == 0);
            UNIT_ASSERT(message.GetValueIndex("v1") == 1);
            UNIT_ASSERT(message.GetValueIndex("v2") == 2);

            UNIT_ASSERT(message.GetValue<int>("v0") == 1);
            UNIT_ASSERT(message.GetValue<int>("v1") == 3);
            UNIT_ASSERT(message.GetValue<int>("v2") == 1);
        }
        {
            auto message = YDBLOG_CREATE_MESSAGE({"v0", 1}, {"v2", 1}, {"v1", 3}, {"v1", 2}, {"v1", 1});
            UNIT_ASSERT(message.GetValuesCount() == 3);
            UNIT_ASSERT(message.GetValueIndex("v0") == 0);
            UNIT_ASSERT(message.GetValueIndex("v1") == 1);
            UNIT_ASSERT(message.GetValueIndex("v2") == 2);

            UNIT_ASSERT(message.GetValue<int>("v0") == 1);
            UNIT_ASSERT(message.GetValue<int>("v1") == 1);
            UNIT_ASSERT(message.GetValue<int>("v2") == 1);
        }
    }

    Y_UNIT_TEST(RemoveValues) {
        auto message = YDBLOG_CREATE_MESSAGE({"v0", 1}, {"v1", 1}, {"v2", 3});

        UNIT_ASSERT(message.HasValue("v1"));
        message.RemoveValue(1);
        UNIT_ASSERT(!message.HasValue("v1"));

        UNIT_ASSERT(message.HasValue("v0"));
        message.RemoveValue("v0");
        UNIT_ASSERT(!message.HasValue("v0"));
    }

    Y_UNIT_TEST(ScanValues) {
        auto message = YDBLOG_CREATE_MESSAGE(
            {"uint8", static_cast<ui8>(1)},
            {"int8", static_cast<i8>(2)},
            {"uint16", static_cast<ui16>(3)},
            {"int16", static_cast<i16>(4)},
            {"uint32", static_cast<ui32>(5)},
            {"int32", static_cast<i32>(6)},
            {"uint64", static_cast<ui64>(7)},
            {"int64", static_cast<i64>(8)},
            {"bool", static_cast<bool>(true)},
            {"string", static_cast<TString>("abc")},
            {"float", static_cast<float>(1.123)},
            {"double", static_cast<double>(1.123)},
            {"long double", static_cast<long double>(1.123)}
        );

        message.ForEach(
            MakeOverloaded(
                [](const std::vector<TKeyName>& name, const std::uint8_t& value) { UNIT_ASSERT(name.size() == 1 && name[0].ToString() == "uint8" && value == 1); },
                [](const std::vector<TKeyName>& name, const std::int8_t& value) { UNIT_ASSERT(name.size() == 1 && name[0].ToString() == "int8" && value == 2); },
                [](const std::vector<TKeyName>& name, const std::uint16_t& value) { UNIT_ASSERT(name.size() == 1 && name[0].ToString() == "uint16" && value == 3); },
                [](const std::vector<TKeyName>& name, const std::int16_t& value) { UNIT_ASSERT(name.size() == 1 && name[0].ToString() == "int16" && value == 4); },
                [](const std::vector<TKeyName>& name, const std::uint32_t& value) { UNIT_ASSERT(name.size() == 1 && name[0].ToString() == "uint32" && value == 5); },
                [](const std::vector<TKeyName>& name, const std::int32_t& value) { UNIT_ASSERT(name.size() == 1 && name[0].ToString() == "int32" && value == 6); },
                [](const std::vector<TKeyName>& name, const std::uint64_t& value) { UNIT_ASSERT(name.size() == 1 && name[0].ToString() == "uint64" && value == 7); },
                [](const std::vector<TKeyName>& name, const std::int64_t& value) { UNIT_ASSERT(name.size() == 1 && name[0].ToString() == "int64" && value == 8); },
                [](const std::vector<TKeyName>& name, const bool& value) { UNIT_ASSERT(name.size() == 1 && name[0].ToString() == "bool" && value == true); },
                [](const std::vector<TKeyName>& name, const TString& value) { UNIT_ASSERT(name.size() == 1 && name[0].ToString() == "string" && value == "abc"); },
                [](const std::vector<TKeyName>& name, const float& value) { UNIT_ASSERT(name.size() == 1 && name[0].ToString() == "float" && std::abs(value - 1.123) < 0.001); },
                [](const std::vector<TKeyName>& name, const double& value) { UNIT_ASSERT(name.size() == 1 && name[0].ToString() == "double" && std::abs(value - 1.123) < 0.001); },
                [](const std::vector<TKeyName>& name, const long double& value) { UNIT_ASSERT(name.size() == 1 && name[0].ToString()=="long double" && std::abs(value - 1.123) < 0.001); }
            ));
    }

    TString GetMessageString(const TStructuredMessage& message) {
        TString result;
        auto append = [&result](const std::vector<TKeyName>& name, const TString& typeName, const auto& value)
        {
            if (!result.empty()) result +=", ";

            bool addDot = false;
            for(auto& nameItem: name) {
                if (addDot) {
                    result += ".";
                } else {
                    addDot = true;
                }

                result += nameItem.ToString();
            }
            result += ":";
            result += typeName;
            result += "=";
            result += TTypesMapping::ToString(value);
        };

        message.ForEach(
            MakeOverloaded(
                [&](const std::vector<TKeyName>& name, const std::uint8_t& value) { append(name, "uint8", value); },
                [&](const std::vector<TKeyName>& name, const std::int8_t& value) { append(name, "int8", value); },
                [&](const std::vector<TKeyName>& name, const std::uint16_t& value) { append(name, "uint16", value); },
                [&](const std::vector<TKeyName>& name, const std::int16_t& value) { append(name, "int16", value); },
                [&](const std::vector<TKeyName>& name, const std::uint32_t& value) { append(name, "uint32", value); },
                [&](const std::vector<TKeyName>& name, const std::int32_t& value) { append(name, "int32", value); },
                [&](const std::vector<TKeyName>& name, const std::uint64_t& value) { append(name, "uint64", value); },
                [&](const std::vector<TKeyName>& name, const std::int64_t& value) { append(name, "int64", value); },
                [&](const std::vector<TKeyName>& name, const bool& value) { append(name, "bool", value); },
                [&](const std::vector<TKeyName>& name, const TString& value) { append(name, "string", value); },
                [&](const std::vector<TKeyName>& name, const float& value) { append(name, "float", value); },
                [&](const std::vector<TKeyName>& name, const double& value) { append(name, "double", value); },
                [&](const std::vector<TKeyName>& name, const long double& value) { append(name, "long double", value); }
            )
        );
        return result;
    }

    #define TEST_MESSAGE(M, S) { auto m = M; auto str = GetMessageString(m); \
        UNIT_ASSERT_STRINGS_EQUAL(str, S); }

    Y_UNIT_TEST(CreateMessageVaryValuesCount) {
        TEST_MESSAGE(YDBLOG_CREATE_MESSAGE(), "");
        TEST_MESSAGE(YDBLOG_CREATE_MESSAGE({"v1", 1}), "v1:int32=1");
        TEST_MESSAGE(YDBLOG_CREATE_MESSAGE({"v1", 1}, {"v2", 2}), "v1:int32=1, v2:int32=2");
        TEST_MESSAGE(YDBLOG_CREATE_MESSAGE({"v1", 1}, {"v2", 2}, {"v3", 3}), "v1:int32=1, v2:int32=2, v3:int32=3");
    }

    Y_UNIT_TEST(CreateMessageVaryTypes) {
        // Empty pairs
        TEST_MESSAGE(YDBLOG_CREATE_MESSAGE({"v1", 1}, {}), "v1:int32=1");

        // Support types
        TEST_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", static_cast<std::uint8_t>(1)}), "value:uint8=1");
        TEST_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", static_cast<std::int8_t>(2)}), "value:int8=2");
        TEST_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", static_cast<std::uint16_t>(3)}), "value:uint16=3");
        TEST_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", static_cast<std::int16_t>(4)}), "value:int16=4");
        TEST_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", static_cast<std::uint32_t>(5)}), "value:uint32=5");
        TEST_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", static_cast<std::int32_t>(6)}), "value:int32=6");
        TEST_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", static_cast<std::uint64_t>(7)}), "value:uint64=7");
        TEST_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", static_cast<std::int64_t>(8)}), "value:int64=8");
        TEST_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", true}), "value:bool=true");
        TEST_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", TString("abc")}), "value:string=abc");
        TEST_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", "abc"}), "value:string=abc");
        TEST_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", static_cast<float>(1.123)}), "value:float=1.123000");
        TEST_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", static_cast<double>(1.123)}), "value:double=1.123000");
        TEST_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", static_cast<long double>(1.123)}), "value:long double=1.123000");

        // optional values
        TEST_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", TMaybe<ui8>{}}), "");
        TEST_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", TMaybe<ui8>{1}}), "value:uint8=1");
    }

    Y_UNIT_TEST(CreateMessageWithReusage) {
        // reuse message and sub message
        auto subMessage = YDBLOG_CREATE_MESSAGE({"subValue1", 1}, {"subValue2", 2});
        TEST_MESSAGE(YDBLOG_CREATE_MESSAGE(subMessage), "subValue1:int32=1, subValue2:int32=2");
        TEST_MESSAGE(YDBLOG_CREATE_MESSAGE(subMessage, subMessage), "subValue1:int32=1, subValue2:int32=2");
        TEST_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", subMessage}), "value.subValue1:int32=1, value.subValue2:int32=2");
        TEST_MESSAGE(YDBLOG_CREATE_MESSAGE(subMessage, {"value", subMessage}), "subValue1:int32=1, subValue2:int32=2, value.subValue1:int32=1, value.subValue2:int32=2");

        // optional subMessages
        TEST_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", TMaybe<TStructuredMessage>{}}), "");
        TEST_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", TMaybe<TStructuredMessage>{subMessage}}), "value.subValue1:int32=1, value.subValue2:int32=2");
        TEST_MESSAGE(YDBLOG_CREATE_MESSAGE(TMaybe<TStructuredMessage>{}), "");
        TEST_MESSAGE(YDBLOG_CREATE_MESSAGE(TMaybe<TStructuredMessage>{subMessage}), "subValue1:int32=1, subValue2:int32=2");
    }

    Y_UNIT_TEST(UpdateMessage) {
        {
            auto message = YDBLOG_CREATE_MESSAGE({"v1", 1}, {"v2", 2}, {"v3", 3});
            TEST_MESSAGE(message, "v1:int32=1, v2:int32=2, v3:int32=3");

            YDBLOG_UPDATE_MESSAGE(message, TStructuredMessage{});
            TEST_MESSAGE(message, "v1:int32=1, v2:int32=2, v3:int32=3");
        }
        {
            auto message = YDBLOG_CREATE_MESSAGE({"v1", 1}, {"v2", 2});
            TEST_MESSAGE(message, "v1:int32=1, v2:int32=2");

            YDBLOG_UPDATE_MESSAGE(message, {"v3", 3});
            TEST_MESSAGE(message, "v1:int32=1, v2:int32=2, v3:int32=3");
        }
        {
            auto message = YDBLOG_CREATE_MESSAGE({"v1", 1});
            TEST_MESSAGE(message, "v1:int32=1");

            YDBLOG_UPDATE_MESSAGE(message, {"v2", 2}, {"v3", 3});
            TEST_MESSAGE(message, "v1:int32=1, v2:int32=2, v3:int32=3");
        }
        {
            auto message = YDBLOG_CREATE_MESSAGE();
            TEST_MESSAGE(message, "");

            YDBLOG_UPDATE_MESSAGE(message, {"v1", 1}, {"v2", 2}, {"v3", 3});
            TEST_MESSAGE(message, "v1:int32=1, v2:int32=2, v3:int32=3");
        }
    }

    Y_UNIT_TEST(LogStack) {
        TLogStack::TLogGuard guard;

        TEST_MESSAGE(TLogStack::GetTop(), "");

        YDBLOG_UPDATE_CONTEXT({"v1", 1});
        TEST_MESSAGE(TLogStack::GetTop(), "v1:int32=1");

        TLogStack::Push();
        TEST_MESSAGE(TLogStack::GetTop(), "v1:int32=1");
        YDBLOG_UPDATE_CONTEXT({"v2", 2});
        TEST_MESSAGE(TLogStack::GetTop(), "v1:int32=1, v2:int32=2");

        YDBLOG_UPDATE_CONTEXT({"v3", 3});
        TEST_MESSAGE(TLogStack::GetTop(), "v1:int32=1, v2:int32=2, v3:int32=3");

        YDBLOG_REMOVE_CONTEXT("v1", "v2");
        TEST_MESSAGE(TLogStack::GetTop(), "v3:int32=3");

        TLogStack::Pop();
        TEST_MESSAGE(TLogStack::GetTop(), "v1:int32=1");

        TLogStack::Pop();
        TEST_MESSAGE(TLogStack::GetTop(), "");
    }

    Y_UNIT_TEST(LogStackGuard) {
        TEST_MESSAGE(TLogStack::GetTop(), "");
        {
            TLogStack::TLogGuard guard1;
            YDBLOG_UPDATE_CONTEXT({"v1", 1});
            TEST_MESSAGE(TLogStack::GetTop(), "v1:int32=1");

            {
                TLogStack::TLogGuard guard2;
                TEST_MESSAGE(TLogStack::GetTop(), "v1:int32=1");
                YDBLOG_UPDATE_CONTEXT({"v2", 2});
                TEST_MESSAGE(TLogStack::GetTop(), "v1:int32=1, v2:int32=2");
            }

            TEST_MESSAGE(TLogStack::GetTop(), "v1:int32=1");
        }
        TEST_MESSAGE(TLogStack::GetTop(), "");
    }

    TString GetMessageJsonString(const TStructuredMessage& message) {
        NJsonWriter::TBuf jsonWriter;
        TJsonWriter().Write(jsonWriter, message);
        return jsonWriter.Str();
    }

    #define TEST_JSON_MESSAGE(M, S) { auto m = M; auto str = GetMessageJsonString(m); \
        UNIT_ASSERT_STRINGS_EQUAL(str, S); }

    Y_UNIT_TEST(GenerateJson) {
        TEST_JSON_MESSAGE(YDBLOG_CREATE_MESSAGE(), R"({})");
        TEST_JSON_MESSAGE(YDBLOG_CREATE_MESSAGE({"v1", 1}), R"({"v1":1})");
        TEST_JSON_MESSAGE(YDBLOG_CREATE_MESSAGE({"v1", 1}, {"v2", 2}), R"({"v1":1,"v2":2})");
        TEST_JSON_MESSAGE(YDBLOG_CREATE_MESSAGE({"v1", 1}, {"v2", 2}, {"v3", 3}), R"({"v1":1,"v2":2,"v3":3})");

        // Empty pairs
        TEST_JSON_MESSAGE(YDBLOG_CREATE_MESSAGE({"v1", 1}, {}), R"({"v1":1})");

        // Support types
        TEST_JSON_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", static_cast<ui8>(1)}), R"({"value":1})");
        TEST_JSON_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", static_cast<i8>(2)}), R"({"value":2})");
        TEST_JSON_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", static_cast<ui16>(3)}), R"({"value":3})");
        TEST_JSON_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", static_cast<i16>(4)}), R"({"value":4})");
        TEST_JSON_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", static_cast<ui32>(5)}), R"({"value":5})");
        TEST_JSON_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", static_cast<i32>(6)}), R"({"value":6})");
        TEST_JSON_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", static_cast<ui64>(7)}), R"({"value":7})");
        TEST_JSON_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", static_cast<i64>(8)}), R"({"value":8})");
        TEST_JSON_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", true}), R"({"value":true})");
        TEST_JSON_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", TString("abc")}), R"({"value":"abc"})");
        TEST_JSON_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", "abc"}), R"({"value":"abc"})");
        TEST_JSON_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", static_cast<float>(1.123)}), R"({"value":1.123})");
        TEST_JSON_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", static_cast<double>(1.123)}), R"({"value":1.123})");
        TEST_JSON_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", static_cast<long double>(1.123)}), R"({"value":1.123})");

        // reuse message and sub message
        auto subMessage = YDBLOG_CREATE_MESSAGE({"subValue1", 1}, {"subValue2", 2});

        TEST_JSON_MESSAGE(YDBLOG_CREATE_MESSAGE(subMessage), R"({"subValue1":1,"subValue2":2})");
        TEST_JSON_MESSAGE(YDBLOG_CREATE_MESSAGE(subMessage, subMessage), R"({"subValue1":1,"subValue2":2})");
        TEST_JSON_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", subMessage}), R"({"value":{"subValue1":1,"subValue2":2}})");
        TEST_JSON_MESSAGE(YDBLOG_CREATE_MESSAGE(subMessage, {"value", subMessage}), R"({"subValue1":1,"subValue2":2,"value":{"subValue1":1,"subValue2":2}})");

        // optional values
        TEST_JSON_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", TMaybe<ui8>{}}), R"({})");
        TEST_JSON_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", TMaybe<ui8>{1}}), R"({"value":1})");

        // optional subMessages
        TEST_JSON_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", TMaybe<TStructuredMessage>{}}), R"({})");
        TEST_JSON_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", TMaybe<TStructuredMessage>{subMessage}}), R"({"value":{"subValue1":1,"subValue2":2}})");
        TEST_JSON_MESSAGE(YDBLOG_CREATE_MESSAGE(TMaybe<TStructuredMessage>{}), R"({})");
        TEST_JSON_MESSAGE(YDBLOG_CREATE_MESSAGE(TMaybe<TStructuredMessage>{subMessage}), R"({"subValue1":1,"subValue2":2})");

        // subMessage name conflict
        TEST_JSON_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", 1}, {"value", YDBLOG_CREATE_MESSAGE({"value", 1})}),
            R"({"value":1,"_value":{"value":1}})");
        TEST_JSON_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", 1}, {"value", YDBLOG_CREATE_MESSAGE({"value", 1}, {"value2", 2})}),
            R"({"value":1,"_value":{"value":1,"value2":2}})");
        TEST_JSON_MESSAGE(YDBLOG_CREATE_MESSAGE({"value", 1}, {"value", YDBLOG_CREATE_MESSAGE({"value", 1})}, {"xvalue", YDBLOG_CREATE_MESSAGE({"value", 10})}),
            R"({"value":1,"_value":{"value":1},"xvalue":{"value":10}})");
    }
}
}

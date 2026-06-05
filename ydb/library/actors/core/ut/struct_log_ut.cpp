#include <ydb/library/actors/struct_log/create_message.h>
#include <ydb/library/actors/struct_log/json_writer.h>
#include <ydb/library/actors/struct_log/key_name.h>
#include <ydb/library/actors/struct_log/log_stack.h>
#include <ydb/library/actors/struct_log/native_types_mapping.h>
#include <ydb/library/actors/struct_log/native_types_support.h>
#include <ydb/library/actors/struct_log/structured_message.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/overloaded.h>

namespace NActors::NStructuredLog {

template <typename T>
void TestType(const std::vector<T>& values) {
    auto typeCode = TTypesMapping::GetCode<T>();

    for (auto value : values) {
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

        UNIT_ASSERT(value == readValue);
    }
}

Y_UNIT_TEST_SUITE(StructLog) {
    Y_UNIT_TEST(NativeTypes) { TestType<TString>({"", "a", "ab", "abc"}); }

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
        for (auto value : values) {
            auto message = YDB_LOG_CREATE_MESSAGE({"value", value});

            auto index = message.GetValueIndex("value");
            UNIT_ASSERT(index.has_value());

            UNIT_ASSERT(message.template CheckValueType<T>(index.value()));

            UNIT_ASSERT(message.template GetValue<T>("value") == value);
        }
    }

    Y_UNIT_TEST(CreateMessageValueRead) {
        TestCreateMessageTypedValueRead<TString>({"", "a", "ab", "abc"});
    }

    Y_UNIT_TEST(SortValues) {
        {
            auto message = YDB_LOG_CREATE_MESSAGE({"v1", 2});
            UNIT_ASSERT(message.GetValuesCount() == 1);
            UNIT_ASSERT(message.GetValueIndex("v1") == 0);
            UNIT_ASSERT(!message.GetValueIndex("v2").has_value());

            UNIT_ASSERT(message.GetValue<TString>("v1") == "2");
        }
        {
            auto message = YDB_LOG_CREATE_MESSAGE({"v3", 3}, {"v2", 1}, {"v1", 2});
            UNIT_ASSERT(message.GetValuesCount() == 3);
            UNIT_ASSERT(message.GetValueIndex("v1") == 0);
            UNIT_ASSERT(message.GetValueIndex("v2") == 1);
            UNIT_ASSERT(message.GetValueIndex("v3") == 2);
            UNIT_ASSERT(!message.GetValueIndex("v4").has_value());

            UNIT_ASSERT(message.GetValue<TString>("v1") == "2");
            UNIT_ASSERT(message.GetValue<TString>("v2") == "1");
            UNIT_ASSERT(message.GetValue<TString>("v3") == "3");
        }
        {
            auto message = YDB_LOG_CREATE_MESSAGE({"v0", 1}, {"v2", 1}, {"v1", 1}, {"v1", 2}, {"v1", 3});
            UNIT_ASSERT(message.GetValuesCount() == 3);
            UNIT_ASSERT(message.GetValueIndex("v0") == 0);
            UNIT_ASSERT(message.GetValueIndex("v1") == 1);
            UNIT_ASSERT(message.GetValueIndex("v2") == 2);

            UNIT_ASSERT(message.GetValue<TString>("v0") == "1");
            UNIT_ASSERT(message.GetValue<TString>("v1") == "3");
            UNIT_ASSERT(message.GetValue<TString>("v2") == "1");
        }
        {
            auto message = YDB_LOG_CREATE_MESSAGE({"v0", 1}, {"v2", 1}, {"v1", 3}, {"v1", 2}, {"v1", 1});
            UNIT_ASSERT(message.GetValuesCount() == 3);
            UNIT_ASSERT(message.GetValueIndex("v0") == 0);
            UNIT_ASSERT(message.GetValueIndex("v1") == 1);
            UNIT_ASSERT(message.GetValueIndex("v2") == 2);

            UNIT_ASSERT(message.GetValue<TString>("v0") == "1");
            UNIT_ASSERT(message.GetValue<TString>("v1") == "1");
            UNIT_ASSERT(message.GetValue<TString>("v2") == "1");
        }
    }

    Y_UNIT_TEST(RemoveValues) {
        auto message = YDB_LOG_CREATE_MESSAGE({"v0", 1}, {"v1", 1}, {"v2", 3});

        UNIT_ASSERT(message.HasValue("v1"));
        message.RemoveValue(1);
        UNIT_ASSERT(!message.HasValue("v1"));

        UNIT_ASSERT(message.HasValue("v0"));
        message.RemoveValue("v0");
        UNIT_ASSERT(!message.HasValue("v0"));
    }

    Y_UNIT_TEST(ScanValues) {
        auto message = YDB_LOG_CREATE_MESSAGE({"string", static_cast<TString>("abc")});

        message.ForEachTyped(TOverloaded{[](const std::vector<TKeyName>& name, const TString& value) {
            UNIT_ASSERT(name.size() == 1 && name[0].ToString() == "string" && value == "abc");
        }});
    }

    TString GetMessageString(const TStructuredMessage& message) {
        TString result;
        auto append = [&result](const std::vector<TKeyName>& name, const auto& value) {
            if (!result.empty()) result += ", ";

            bool addDot = false;
            for (auto& nameItem : name) {
                if (addDot) {
                    result += ".";
                } else {
                    addDot = true;
                }

                result += nameItem.ToString();
            }
            result += "=";
            result += TTypesMapping::ToString(value);
        };

        message.ForEachTyped(TOverloaded{[&](const std::vector<TKeyName>& name, const TString& value) {
            append(name, value);
        }});
        return result;
    }

#define TEST_MESSAGE(M, S)                 \
    {                                      \
        auto m = M;                        \
        auto str = GetMessageString(m);    \
        UNIT_ASSERT_STRINGS_EQUAL(str, S); \
    }

    Y_UNIT_TEST(CreateMessageVaryValuesCount) {
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE(), "");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"v1", 1}), "v1=1");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"v1", 1}, {}), "v1=1");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"v1", 1}, {}, {}), "v1=1");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"v1", 1}, {}, {}, {}), "v1=1");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"v1", 1}, {"v2", 2}), "v1=1, v2=2");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"v1", 1}, {"v2", 2}, {"v3", 3}), "v1=1, v2=2, v3=3");
    }

    Y_UNIT_TEST(CreateMessageNativeTypes) {
        // Native type values
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", static_cast<ui8>('a')}), "value=a");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", static_cast<i8>('a')}), "value=a");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", static_cast<ui16>(3)}), "value=3");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", static_cast<i16>(4)}), "value=4");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", static_cast<ui32>(5)}), "value=5");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", static_cast<i32>(6)}), "value=6");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", static_cast<ui64>(7)}), "value=7");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", static_cast<i64>(8)}), "value=8");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", true}), "value=true");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", TString("abc")}), "value=abc");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", "abc"}), "value=abc");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", static_cast<float>(1.123)}), "value=1.123");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", static_cast<double>(1.123)}), "value=1.123");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", static_cast<long double>(1.123)}), "value=1.123");

        int i = 0;
        auto ptr = static_cast<void*>(&i);

        UNIT_ASSERT_STRINGS_EQUAL(TStringBuilder() << "value=" << ptr, GetMessageString(YDB_LOG_CREATE_MESSAGE({"value", ptr})));

        ptr = nullptr;
        UNIT_ASSERT_STRINGS_EQUAL(TStringBuilder() << "value=" << ptr, GetMessageString(YDB_LOG_CREATE_MESSAGE({"value", ptr})));
        UNIT_ASSERT_STRINGS_EQUAL(TStringBuilder() << "value=" << nullptr, GetMessageString(YDB_LOG_CREATE_MESSAGE({"value", nullptr})));
    }

    Y_UNIT_TEST(CreateMessageOptionalTypes) {
        // TMaybe values
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", TMaybe<ui32>{}}), "value=<null>");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", TMaybe<ui32>{1}}), "value=1");

        // std::optional values
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", std::optional<ui32>{}}), "value=<null>");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", std::optional<ui32>{1}}), "value=1");
    }

    Y_UNIT_TEST(CreateMessagePointerTypes) {
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", nullptr}), "value=nullptr");

        ui64 value = 1;
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", &value}), "value=1");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", std::make_shared<ui64>(1)}), "value=1");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", std::make_unique<ui64>(1)}), "value=1");
    }

    struct TTestTypeToString {
        TString ToString() const {
            return "some value";
        }
    };

    struct TTestTypeToStructuredMessage {
        TStructuredMessage ToStructuredMessage() const {
            return YDB_LOG_CREATE_MESSAGE({"value1", 1}, {"value2", 2});
        }
    };

    struct TTestTypeToBoth {
        TString ToString() const {
            return "some value";
        }

        TStructuredMessage ToStructuredMessage() const {
            return YDB_LOG_CREATE_MESSAGE({"value1", 1}, {"value2", 2});
        }
    };

    Y_UNIT_TEST(CreateMessageToMethods) {
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", TTestTypeToString{}}), "value=some value");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", TTestTypeToStructuredMessage{}}), "value.value1=1, value.value2=2");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", TTestTypeToBoth{}}), "value.value1=1, value.value2=2");
    }

    Y_UNIT_TEST(CreateMessageIterable) {
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", std::vector<TString>{"a", "b", "c"}}), "value=[a, b, c]");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", std::vector<TString>{"a", "b", "c", "d"}}), "value=[a, b, c, d]");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", std::vector<TString>{"a", "b", "c", "d", "e"}}), "value=[a, b, c, d, e]");
    }

    Y_UNIT_TEST(CreateMessageIterableKV) {
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", TMap<ui64, TString>{{1, "a"}}}), "value={1:a}");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", TMap<ui64, TString>{{1, "a"}, {2, "b"}}}), "value={1:a, 2:b}");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", TMap<ui64, TString>{{1, "a"}, {2, "b"}, {3, "c"}}}), "value={1:a, 2:b, 3:c}");

        /* @todo IterableKV*/
    }

    Y_UNIT_TEST(CreateMessageVariant) {
        using TTestType = std::variant<ui64, bool, TString>;

        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", TTestType{TString{"abcde"}}}), "value=abcde");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", TTestType{true}}), "value=true");
    }

    Y_UNIT_TEST(CreateMessageTuple) {
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", std::tuple{1}}), "value=(1)");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", std::tuple{1,2}}), "value=(1:2)");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", std::tuple{1,2,3}}), "value=(1:2:3)");
    }

    Y_UNIT_TEST(CreateMessageWithReusage) {
        // reuse message and sub message
        auto subMessage = YDB_LOG_CREATE_MESSAGE({"subValue1", 1}, {"subValue2", 2});
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE(subMessage), "subValue1=1, subValue2=2");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE(subMessage, subMessage), "subValue1=1, subValue2=2");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", subMessage}), "value.subValue1=1, value.subValue2=2");
        TEST_MESSAGE(
            YDB_LOG_CREATE_MESSAGE(subMessage, {"value", subMessage}),
            "subValue1=1, subValue2=2, value.subValue1=1, value.subValue2=2"
        );

        // optional subMessages
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", TMaybe<TStructuredMessage>{}}), "");
        TEST_MESSAGE(
            YDB_LOG_CREATE_MESSAGE({"value", TMaybe<TStructuredMessage>{subMessage}}),
            "value.subValue1=1, value.subValue2=2"
        );
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE(TMaybe<TStructuredMessage>{}), "");
        TEST_MESSAGE(YDB_LOG_CREATE_MESSAGE(TMaybe<TStructuredMessage>{subMessage}), "subValue1=1, subValue2=2");
    }

    Y_UNIT_TEST(UpdateMessage) {
        {
            auto message = YDB_LOG_CREATE_MESSAGE({"v1", 1}, {"v2", 2}, {"v3", 3});
            TEST_MESSAGE(message, "v1=1, v2=2, v3=3");

            YDB_LOG_UPDATE_MESSAGE(message, TStructuredMessage{});
            TEST_MESSAGE(message, "v1=1, v2=2, v3=3");
        }
        {
            auto message = YDB_LOG_CREATE_MESSAGE({"v1", 1}, {"v2", 2});
            TEST_MESSAGE(message, "v1=1, v2=2");

            YDB_LOG_UPDATE_MESSAGE(message, {"v3", 3});
            TEST_MESSAGE(message, "v1=1, v2=2, v3=3");
        }
        {
            auto message = YDB_LOG_CREATE_MESSAGE({"v1", 1});
            TEST_MESSAGE(message, "v1=1");

            YDB_LOG_UPDATE_MESSAGE(message, {"v2", 2}, {"v3", 3});
            TEST_MESSAGE(message, "v1=1, v2=2, v3=3");
        }
        {
            auto message = YDB_LOG_CREATE_MESSAGE();
            TEST_MESSAGE(message, "");

            YDB_LOG_UPDATE_MESSAGE(message, {"v1", 1}, {"v2", 2}, {"v3", 3});
            TEST_MESSAGE(message, "v1=1, v2=2, v3=3");
        }
    }

    Y_UNIT_TEST(LogStack) {
        TLogStack::TLogGuard guard;

        TEST_MESSAGE(TLogStack::GetTop(), "");

        YDB_LOG_UPDATE_CONTEXT({"v1", 1});
        TEST_MESSAGE(TLogStack::GetTop(), "v1=1");

        TLogStack::Push();
        TEST_MESSAGE(TLogStack::GetTop(), "v1=1");
        YDB_LOG_UPDATE_CONTEXT({"v2", 2});
        TEST_MESSAGE(TLogStack::GetTop(), "v1=1, v2=2");

        YDB_LOG_UPDATE_CONTEXT({"v3", 3});
        TEST_MESSAGE(TLogStack::GetTop(), "v1=1, v2=2, v3=3");

        YDB_LOG_REMOVE_CONTEXT("v1", "v2");
        TEST_MESSAGE(TLogStack::GetTop(), "v3=3");

        TLogStack::Pop();
        TEST_MESSAGE(TLogStack::GetTop(), "v1=1");

        TLogStack::Pop();
        TEST_MESSAGE(TLogStack::GetTop(), "");
    }

    Y_UNIT_TEST(LogStackGuard) {
        TEST_MESSAGE(TLogStack::GetTop(), "");
        {
            TLogStack::TLogGuard guard1;
            YDB_LOG_UPDATE_CONTEXT({"v1", 1});
            TEST_MESSAGE(TLogStack::GetTop(), "v1=1");

            {
                TLogStack::TLogGuard guard2;
                TEST_MESSAGE(TLogStack::GetTop(), "v1=1");
                YDB_LOG_UPDATE_CONTEXT({"v2", 2});
                TEST_MESSAGE(TLogStack::GetTop(), "v1=1, v2=2");
            }

            TEST_MESSAGE(TLogStack::GetTop(), "v1=1");
        }
        TEST_MESSAGE(TLogStack::GetTop(), "");
    }

    TString GetMessageJsonString(const TStructuredMessage& message) {
        NJsonWriter::TBuf jsonWriter;
        TJsonWriter().Write(jsonWriter, message);
        return jsonWriter.Str();
    }

#define TEST_JSON_MESSAGE(M, S)             \
    {                                       \
        auto m = M;                         \
        auto str = GetMessageJsonString(m); \
        UNIT_ASSERT_STRINGS_EQUAL(str, S);  \
    }

    Y_UNIT_TEST(GenerateJson) {
        TEST_JSON_MESSAGE(YDB_LOG_CREATE_MESSAGE(), R"({})");
        TEST_JSON_MESSAGE(YDB_LOG_CREATE_MESSAGE({"v1", 1}), R"({"v1":"1"})");
        TEST_JSON_MESSAGE(YDB_LOG_CREATE_MESSAGE({"v1", 1}, {"v2", 2}), R"({"v1":"1","v2":"2"})");
        TEST_JSON_MESSAGE(YDB_LOG_CREATE_MESSAGE({"v1", 1}, {"v2", 2}, {"v3", 3}), R"({"v1":"1","v2":"2","v3":"3"})");

        // Empty pairs
        TEST_JSON_MESSAGE(YDB_LOG_CREATE_MESSAGE({"v1", 1}, {}), R"({"v1":"1"})");

        // Support types
        TEST_JSON_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", static_cast<ui8>('a')}), R"({"value":"a"})");
        TEST_JSON_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", static_cast<i8>('a')}), R"({"value":"a"})");
        TEST_JSON_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", static_cast<ui16>(3)}), R"({"value":"3"})");
        TEST_JSON_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", static_cast<i16>(4)}), R"({"value":"4"})");
        TEST_JSON_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", static_cast<ui32>(5)}), R"({"value":"5"})");
        TEST_JSON_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", static_cast<i32>(6)}), R"({"value":"6"})");
        TEST_JSON_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", static_cast<ui64>(7)}), R"({"value":"7"})");
        TEST_JSON_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", static_cast<i64>(8)}), R"({"value":"8"})");
        TEST_JSON_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", true}), R"({"value":"true"})");
        TEST_JSON_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", TString("abc")}), R"({"value":"abc"})");
        TEST_JSON_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", "abc"}), R"({"value":"abc"})");
        TEST_JSON_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", static_cast<float>(1.123)}), R"({"value":"1.123"})");
        TEST_JSON_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", static_cast<double>(1.123)}), R"({"value":"1.123"})");
        TEST_JSON_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", static_cast<long double>(1.123)}), R"({"value":"1.123"})");
        TEST_JSON_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", NActors::TActorId(1, 2)}), R"({"value":"[0:1:2]"})");

        // reuse message and sub message
        auto subMessage = YDB_LOG_CREATE_MESSAGE({"subValue1", 1}, {"subValue2", 2});

        TEST_JSON_MESSAGE(YDB_LOG_CREATE_MESSAGE(subMessage), R"({"subValue1":"1","subValue2":"2"})");
        TEST_JSON_MESSAGE(YDB_LOG_CREATE_MESSAGE(subMessage, subMessage), R"({"subValue1":"1","subValue2":"2"})");
        TEST_JSON_MESSAGE(
            YDB_LOG_CREATE_MESSAGE({"value", subMessage}), R"({"value":{"subValue1":"1","subValue2":"2"}})"
        );
        TEST_JSON_MESSAGE(
            YDB_LOG_CREATE_MESSAGE(subMessage, {"value", subMessage}),
            R"({"subValue1":"1","subValue2":"2","value":{"subValue1":"1","subValue2":"2"}})"
        );

        // optional values
        TEST_JSON_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", TMaybe<ui16>{}}), R"({"value":"\u003Cnull\u003E"})");
        TEST_JSON_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", TMaybe<ui16>{1}}), R"({"value":"1"})");

        // optional subMessages
        TEST_JSON_MESSAGE(YDB_LOG_CREATE_MESSAGE({"value", TMaybe<TStructuredMessage>{}}), R"({})");
        TEST_JSON_MESSAGE(
            YDB_LOG_CREATE_MESSAGE({"value", TMaybe<TStructuredMessage>{subMessage}}),
            R"({"value":{"subValue1":"1","subValue2":"2"}})"
        );
        TEST_JSON_MESSAGE(YDB_LOG_CREATE_MESSAGE(TMaybe<TStructuredMessage>{}), R"({})");
        TEST_JSON_MESSAGE(
            YDB_LOG_CREATE_MESSAGE(TMaybe<TStructuredMessage>{subMessage}), R"({"subValue1":"1","subValue2":"2"})"
        );

        // subMessage name conflict
        TEST_JSON_MESSAGE(
            YDB_LOG_CREATE_MESSAGE({"value", 1}, {"value", YDB_LOG_CREATE_MESSAGE({"value", 1})}),
            R"({"value":"1","_value":{"value":"1"}})"
        );
        TEST_JSON_MESSAGE(
            YDB_LOG_CREATE_MESSAGE({"value", 1}, {"value", YDB_LOG_CREATE_MESSAGE({"value", 1}, {"value2", 2})}),
            R"({"value":"1","_value":{"value":"1","value2":"2"}})"
        );
        TEST_JSON_MESSAGE(
            YDB_LOG_CREATE_MESSAGE(
                {"value", 1},
                {"value", YDB_LOG_CREATE_MESSAGE({"value", 1})},
                {"xvalue", YDB_LOG_CREATE_MESSAGE({"value", 10})}
            ),
            R"({"value":"1","_value":{"value":"1"},"xvalue":{"value":"10"}})"
        );
    }
}
}  // namespace NActors::NStructuredLog

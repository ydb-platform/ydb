#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/events/events.h>

#include <ydb/core/fq/libs/row_dispatcher/json_parser.h>

#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/actor_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <contrib/libs/simdjson/include/simdjson.h>

namespace {

using namespace NKikimr;
using namespace NFq;

class TFixture : public NUnitTest::TBaseFixture {

public:
    TFixture()
    : Runtime(true) {}

    void SetUp(NUnitTest::TTestContext&) override {
        TAutoPtr<TAppPrepare> app = new TAppPrepare();
        Runtime.SetLogBackend(CreateStderrBackend());
        Runtime.SetLogPriority(NKikimrServices::FQ_ROW_DISPATCHER, NLog::PRI_TRACE);
        Runtime.Initialize(app->Unwrap());
    }

    void TearDown(NUnitTest::TTestContext& /* context */) override {
        if (Parser) {
            Parser.reset();
        }
    }

    void MakeParser(TVector<TString> columns, TVector<TString> types) {
        Parser = NFq::NewJsonParser(columns, types, 0, TDuration::Zero());
    }

    void MakeParser(TVector<TString> columns) {
        MakeParser(columns, TVector<TString>(columns.size(), "[DataType; String]"));
    }

    const TVector<NMiniKQL::TUnboxedValueVector>& PushToParser(ui64 offset, const TString& data) {
        Parser->AddMessages({GetMessage(offset, data)});

        const auto& parsedValues = Parser->Parse();
        ResultNumberValues = parsedValues ? parsedValues.front().size() : 0;
        return parsedValues;
    }

    static NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage GetMessage(ui64 offset, const TString& data) {
        NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessageInformation info(offset, "", 0, TInstant::Zero(), TInstant::Zero(), nullptr, nullptr, 0, "");
        return NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage(data, nullptr, info, nullptr);
    }

    TActorSystemStub actorSystemStub;
    NActors::TTestActorRuntime Runtime;
    std::unique_ptr<NFq::TJsonParser> Parser;
    ui64 ResultNumberValues = 0;
};

Y_UNIT_TEST_SUITE(TJsonParserTests) {
    Y_UNIT_TEST_F(Simple1, TFixture) {
        MakeParser({"a1", "a2"}, {"[DataType; String]", "[OptionalType; [DataType; Uint64]]"});
        const auto& result = PushToParser(42,R"({"a1": "hello1", "a2": 101, "event": "event1"})");
        UNIT_ASSERT_VALUES_EQUAL(1, ResultNumberValues);

        UNIT_ASSERT_VALUES_EQUAL(2, result.size());
        UNIT_ASSERT_VALUES_EQUAL("hello1", TString(result[0][0].AsStringRef()));
        UNIT_ASSERT_VALUES_EQUAL(101, result[1][0].GetOptionalValue().Get<ui64>());
    }

    Y_UNIT_TEST_F(Simple2, TFixture) {
        MakeParser({"a2", "a1"});
        const auto& result = PushToParser(42,R"({"a1": "hello1", "a2": "101", "event": "event1"})");
        UNIT_ASSERT_VALUES_EQUAL(1, ResultNumberValues);

        UNIT_ASSERT_VALUES_EQUAL(2, result.size());
        UNIT_ASSERT_VALUES_EQUAL("101", TString(result[0][0].AsStringRef()));
        UNIT_ASSERT_VALUES_EQUAL("hello1", TString(result[1][0].AsStringRef()));
    }

    Y_UNIT_TEST_F(Simple3, TFixture) {
        MakeParser({"a1", "a2"});
        const auto& result = PushToParser(42,R"({"a2": "hello1", "a1": "101", "event": "event1"})");
        UNIT_ASSERT_VALUES_EQUAL(1, ResultNumberValues);

        UNIT_ASSERT_VALUES_EQUAL(2, result.size());
        UNIT_ASSERT_VALUES_EQUAL("101", TString(result[0][0].AsStringRef()));
        UNIT_ASSERT_VALUES_EQUAL("hello1", TString(result[1][0].AsStringRef()));
    }

    Y_UNIT_TEST_F(Simple4, TFixture) {
        MakeParser({"a2", "a1"});
        const auto& result = PushToParser(42, R"({"a2": "hello1", "a1": "101", "event": "event1"})");
        UNIT_ASSERT_VALUES_EQUAL(1, ResultNumberValues);

        UNIT_ASSERT_VALUES_EQUAL(2, result.size());
        UNIT_ASSERT_VALUES_EQUAL("hello1", TString(result[0][0].AsStringRef()));
        UNIT_ASSERT_VALUES_EQUAL("101", TString(result[1][0].AsStringRef()));
    }

    Y_UNIT_TEST_F(LargeStrings, TFixture) {
        MakeParser({"col"});

        const TString largeString = "abcdefghjkl1234567890+abcdefghjkl1234567890";
        const TString jsonString = TStringBuilder() << "{\"col\": \"" << largeString << "\"}";
        Parser->AddMessages({
            GetMessage(42, jsonString),
            GetMessage(43, jsonString)
        });

        const auto& result = Parser->Parse();
        ResultNumberValues = result.front().size();
        UNIT_ASSERT_VALUES_EQUAL(2, ResultNumberValues);

        UNIT_ASSERT_VALUES_EQUAL(1, result.size());
        UNIT_ASSERT_VALUES_EQUAL(largeString, TString(result[0][0].AsStringRef()));
        UNIT_ASSERT_VALUES_EQUAL(largeString, TString(result[0][1].AsStringRef()));
    }

    Y_UNIT_TEST_F(ManyValues, TFixture) {
        MakeParser({"a1", "a2"});

        Parser->AddMessages({
            GetMessage(42, R"({"a1": "hello1", "a2": "101", "event": "event1"})"),
            GetMessage(43, R"({"a1": "hello1", "a2": "101", "event": "event2"})"),
            GetMessage(44, R"({"a2": "101", "a1": "hello1", "event": "event3"})")
        });

        const auto& result = Parser->Parse();
        ResultNumberValues = result.front().size();
        UNIT_ASSERT_VALUES_EQUAL(3, ResultNumberValues);
        UNIT_ASSERT_VALUES_EQUAL(2, result.size());
        for (size_t i = 0; i < ResultNumberValues; ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C("hello1", TString(result[0][i].AsStringRef()), i);
            UNIT_ASSERT_VALUES_EQUAL_C("101", TString(result[1][i].AsStringRef()), i);
        }
    }

    Y_UNIT_TEST_F(MissingFields, TFixture) {
        MakeParser({"a1", "a2"}, {"[OptionalType; [DataType; String]]", "[OptionalType; [DataType; Uint64]]"});

        Parser->AddMessages({
            GetMessage(42, R"({"a1": "hello1", "a2": 101  , "event": "event1"})"),
            GetMessage(43, R"({"a1": "hello1", "event": "event2"})"),
            GetMessage(44, R"({"a2": "101", "a1": null, "event": "event3"})")
        });

        const auto& result = Parser->Parse();
        ResultNumberValues = result.front().size();
        UNIT_ASSERT_VALUES_EQUAL(3, ResultNumberValues);
        UNIT_ASSERT_VALUES_EQUAL(2, result.size());
        for (size_t i = 0; i < ResultNumberValues; ++i) {
            if (i == 2) {
                UNIT_ASSERT_C(!result[0][i], i);
            } else {
                NYql::NUdf::TUnboxedValue value = result[0][i].GetOptionalValue();
                UNIT_ASSERT_VALUES_EQUAL_C("hello1", TString(value.AsStringRef()), i);
            }
            if (i == 1) {
                UNIT_ASSERT_C(!result[1][i], i);
            } else {
                UNIT_ASSERT_VALUES_EQUAL_C(101, result[1][i].GetOptionalValue().Get<ui64>(), i);
            }
        }
    }

    Y_UNIT_TEST_F(NestedTypes, TFixture) {
        MakeParser({"nested", "a1"}, {"[OptionalType; [DataType; Json]]", "[DataType; String]"});

        Parser->AddMessages({
            GetMessage(42, R"({"a1": "hello1", "nested": {"key": "value"}})"),
            GetMessage(43, R"({"a1": "hello2", "nested": ["key1", "key2"]})")
        });

        const auto& result = Parser->Parse();
        ResultNumberValues = result.front().size();
        UNIT_ASSERT_VALUES_EQUAL(2, ResultNumberValues);

        UNIT_ASSERT_VALUES_EQUAL(2, result.size());
        UNIT_ASSERT_VALUES_EQUAL("{\"key\": \"value\"}", TString(result[0][0].AsStringRef()));
        UNIT_ASSERT_VALUES_EQUAL("hello1", TString(result[1][0].AsStringRef()));

        UNIT_ASSERT_VALUES_EQUAL("[\"key1\", \"key2\"]", TString(result[0][1].AsStringRef()));
        UNIT_ASSERT_VALUES_EQUAL("hello2", TString(result[1][1].AsStringRef()));
    }

    Y_UNIT_TEST_F(SimpleBooleans, TFixture) {
        MakeParser({"a"}, {"[DataType; Bool]"});
        Parser->AddMessages({
            GetMessage(42, R"({"a": true})"),
            GetMessage(43, R"({"a": false})")
        });

        const auto& result = Parser->Parse();
        ResultNumberValues = result.front().size();
        UNIT_ASSERT_VALUES_EQUAL(2, ResultNumberValues);

        UNIT_ASSERT_VALUES_EQUAL(1, result.size());
        UNIT_ASSERT_VALUES_EQUAL(true, result[0][0].Get<bool>());
        UNIT_ASSERT_VALUES_EQUAL(false, result[0][1].Get<bool>());
    }

    Y_UNIT_TEST_F(MissingFieldsValidation, TFixture) {
        MakeParser({"a1", "a2"}, {"[DataType; String]", "[DataType; Uint64]"});
        UNIT_ASSERT_EXCEPTION_CONTAINS(PushToParser(42, R"({"a1": "hello1", "a2": null, "event": "event1"})"), yexception, "Failed to parse json string at offset 42, got parsing error for column 'a2' with type [DataType; Uint64], description: (yexception) found unexpected null value, expected non optional data type Uint64");
        UNIT_ASSERT_EXCEPTION_CONTAINS(PushToParser(42, R"({"a2": 105, "event": "event1"})"), yexception, "Failed to parse json messages, found 1 missing values from offset 42 in non optional column 'a1' with type [DataType; String]");
    }

    Y_UNIT_TEST_F(TypeKindsValidation, TFixture) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            MakeParser({"a2", "a1"}, {"[OptionalType; [DataType; String]]", "[ListType; [DataType; String]]"}),
            yexception,
            "Failed to create parser for column 'a1' with type [ListType; [DataType; String]], description: (yexception) unsupported type kind List"
        );
    }

    Y_UNIT_TEST_F(NumbersValidation, TFixture) {
        MakeParser({"a1", "a2"}, {"[OptionalType; [DataType; String]]", "[DataType; Uint8]"});
        UNIT_ASSERT_EXCEPTION_CONTAINS(PushToParser(42, R"({"a1": 456, "a2": 42})"), yexception, "Failed to parse json string at offset 42, got parsing error for column 'a1' with type [OptionalType; [DataType; String]], description: (yexception) failed to parse data type String from json number (raw: '456'), error: (yexception) number value is not expected for data type String");
        UNIT_ASSERT_EXCEPTION_CONTAINS(PushToParser(42, R"({"a1": "456", "a2": -42})"), yexception, "Failed to parse json string at offset 42, got parsing error for column 'a2' with type [DataType; Uint8], description: (yexception) failed to parse data type Uint8 from json number (raw: '-42'), error: (simdjson::simdjson_error) INCORRECT_TYPE: The JSON element does not have the requested type.");
        UNIT_ASSERT_EXCEPTION_CONTAINS(PushToParser(42, R"({"a1": "str", "a2": 99999})"), yexception, "Failed to parse json string at offset 42, got parsing error for column 'a2' with type [DataType; Uint8], description: (yexception) failed to parse data type Uint8 from json number (raw: '99999'), error: (yexception) number is out of range");
    }

    Y_UNIT_TEST_F(NestedJsonValidation, TFixture) {
        MakeParser({"a1", "a2"}, {"[OptionalType; [DataType; Json]]", "[OptionalType; [DataType; String]]"});
        UNIT_ASSERT_EXCEPTION_CONTAINS(PushToParser(42, R"({"a1": {"key": "value"}, "a2": {"key2": "value2"}})"), yexception, "Failed to parse json string at offset 42, got parsing error for column 'a2' with type [OptionalType; [DataType; String]], description: (yexception) found unexpected nested value (raw: '{\"key2\": \"value2\"}'), expected data type String, please use Json type for nested values");
        UNIT_ASSERT_EXCEPTION_CONTAINS(PushToParser(42, R"({"a1": {"key" "value"}, "a2": "str"})"), yexception, "Failed to parse json string at offset 42, got parsing error for column 'a1' with type [OptionalType; [DataType; Json]], description: (simdjson::simdjson_error) TAPE_ERROR: The JSON document has an improper structure: missing or superfluous commas, braces, missing keys, etc.");
    }

    Y_UNIT_TEST_F(BoolsValidation, TFixture) {
        MakeParser({"a1", "a2"}, {"[OptionalType; [DataType; String]]", "[DataType; Bool]"});
        UNIT_ASSERT_EXCEPTION_CONTAINS(PushToParser(42, R"({"a1": true, "a2": false})"), yexception, "Failed to parse json string at offset 42, got parsing error for column 'a1' with type [OptionalType; [DataType; String]], description: (yexception) found unexpected bool value, expected data type String");
    }

    Y_UNIT_TEST_F(ThrowExceptionByError, TFixture) {
        MakeParser({"a"});
        UNIT_ASSERT_EXCEPTION_CONTAINS(PushToParser(42, R"(ydb)"), simdjson::simdjson_error, "INCORRECT_TYPE: The JSON element does not have the requested type.");
        UNIT_ASSERT_EXCEPTION_CONTAINS(PushToParser(42, R"({"a": "value1"} {"a": "value2"})"), yexception, "Failed to parse json messages, expected 1 json rows from offset 42 but got 2");
    }
}

}

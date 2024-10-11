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
        Parser = NFq::NewJsonParser(columns, types);
    }

    void MakeParser(TVector<TString> columns) {
        MakeParser(columns, TVector<TString>(columns.size(), "String"));
    }

    void PushToParser(const TString& data) {
        TJsonParserBuffer& buffer = Parser->GetBuffer();
        buffer.Reserve(data.size());
        buffer.AddValue(data);

        ParsedValues = Parser->Parse();
        ResultNumberValues = ParsedValues ? ParsedValues.front().size() : 0;
    }

    TVector<TString> GetParsedRow(size_t id) const {
        TVector<TString> result;
        result.reserve(ParsedValues.size());
        for (const auto& columnResult : ParsedValues) {
            result.emplace_back(columnResult[id]);
        }
        return result;
    }

    TActorSystemStub actorSystemStub;
    NActors::TTestActorRuntime Runtime;
    std::unique_ptr<NFq::TJsonParser> Parser;

    ui64 ResultNumberValues = 0;
    TVector<TVector<std::string_view>> ParsedValues;
};

Y_UNIT_TEST_SUITE(TJsonParserTests) {
    Y_UNIT_TEST_F(Simple1, TFixture) {
        MakeParser({"a1", "a2"}, {"String", "Optional<Uint64>"});
        PushToParser(R"({"a1": "hello1", "a2": 101, "event": "event1"})");
        UNIT_ASSERT_VALUES_EQUAL(1, ResultNumberValues);

        const auto& result = GetParsedRow(0);
        UNIT_ASSERT_VALUES_EQUAL(2, result.size());
        UNIT_ASSERT_VALUES_EQUAL("hello1", result.front());
        UNIT_ASSERT_VALUES_EQUAL("101", result.back());
    }

    Y_UNIT_TEST_F(Simple2, TFixture) {
        MakeParser({"a2", "a1"});
        PushToParser(R"({"a1": "hello1", "a2": "101", "event": "event1"})");
        UNIT_ASSERT_VALUES_EQUAL(1, ResultNumberValues);

        const auto& result = GetParsedRow(0);
        UNIT_ASSERT_VALUES_EQUAL(2, result.size());
        UNIT_ASSERT_VALUES_EQUAL("101", result.front());
        UNIT_ASSERT_VALUES_EQUAL("hello1", result.back());
    }

    Y_UNIT_TEST_F(Simple3, TFixture) {
        MakeParser({"a1", "a2"});
        PushToParser(R"({"a2": "hello1", "a1": "101", "event": "event1"})");
        UNIT_ASSERT_VALUES_EQUAL(1, ResultNumberValues);

        const auto& result = GetParsedRow(0);
        UNIT_ASSERT_VALUES_EQUAL(2, result.size());
        UNIT_ASSERT_VALUES_EQUAL("101", result.front());
        UNIT_ASSERT_VALUES_EQUAL("hello1", result.back());
    }

    Y_UNIT_TEST_F(Simple4, TFixture) {
        MakeParser({"a2", "a1"});
        PushToParser(R"({"a2": "hello1", "a1": "101", "event": "event1"})");
        UNIT_ASSERT_VALUES_EQUAL(1, ResultNumberValues);

        const auto& result = GetParsedRow(0);
        UNIT_ASSERT_VALUES_EQUAL(2, result.size());
        UNIT_ASSERT_VALUES_EQUAL("hello1", result.front());
        UNIT_ASSERT_VALUES_EQUAL("101", result.back());
    }

    Y_UNIT_TEST_F(ManyValues, TFixture) {
        MakeParser({"a1", "a2"});

        TJsonParserBuffer& buffer = Parser->GetBuffer();
        buffer.AddValue(R"({"a1": "hello1", "a2": "101", "event": "event1"})");
        buffer.AddValue(R"({"a1": "hello1", "a2": "101", "event": "event2"})");
        buffer.AddValue(R"({"a2": "101", "a1": "hello1", "event": "event3"})");

        ParsedValues = Parser->Parse();
        ResultNumberValues = ParsedValues.front().size();
        UNIT_ASSERT_VALUES_EQUAL(3, ResultNumberValues);
        for (size_t i = 0; i < ResultNumberValues; ++i) {
            const auto& result = GetParsedRow(i);
            UNIT_ASSERT_VALUES_EQUAL_C(2, result.size(), i);
            UNIT_ASSERT_VALUES_EQUAL_C("hello1", result.front(), i);
            UNIT_ASSERT_VALUES_EQUAL_C("101", result.back(), i);
        }
    }

    Y_UNIT_TEST_F(MissingFields, TFixture) {
        MakeParser({"a1", "a2"});

        TJsonParserBuffer& buffer = Parser->GetBuffer();
        buffer.AddValue(R"({"a1": "hello1", "a2": "101", "event": "event1"})");
        buffer.AddValue(R"({"a1": "hello1", "event": "event2"})");
        buffer.AddValue(R"({"a2": "101", "a1": null, "event": "event3"})");

        ParsedValues = Parser->Parse();
        ResultNumberValues = ParsedValues.front().size();
        UNIT_ASSERT_VALUES_EQUAL(3, ResultNumberValues);
        for (size_t i = 0; i < ResultNumberValues; ++i) {
            const auto& result = GetParsedRow(i);
            UNIT_ASSERT_VALUES_EQUAL_C(2, result.size(), i);
            UNIT_ASSERT_VALUES_EQUAL_C(i != 2 ? "hello1" : "", result.front(), i);
            UNIT_ASSERT_VALUES_EQUAL_C(i != 1 ? "101" : "", result.back(), i);
        }
    }

    Y_UNIT_TEST_F(NestedTypes, TFixture) {
        MakeParser({"nested", "a1"}, {"Optional<Json>", "String"});

        TJsonParserBuffer& buffer = Parser->GetBuffer();
        buffer.AddValue(R"({"a1": "hello1", "nested": {"key": "value"}})");
        buffer.AddValue(R"({"a1": "hello1", "nested": ["key1", "key2"]})");

        ParsedValues = Parser->Parse();
        ResultNumberValues = ParsedValues.front().size();
        UNIT_ASSERT_VALUES_EQUAL(2, ResultNumberValues);

        const auto& nestedJson = GetParsedRow(0);
        UNIT_ASSERT_VALUES_EQUAL(2, nestedJson.size());
        UNIT_ASSERT_VALUES_EQUAL("{\"key\": \"value\"}", nestedJson.front());
        UNIT_ASSERT_VALUES_EQUAL("hello1", nestedJson.back());

        const auto& nestedList = GetParsedRow(1);
        UNIT_ASSERT_VALUES_EQUAL(2, nestedList.size());
        UNIT_ASSERT_VALUES_EQUAL("[\"key1\", \"key2\"]", nestedList.front());
        UNIT_ASSERT_VALUES_EQUAL("hello1", nestedList.back());
    }

    Y_UNIT_TEST_F(StringTypeValidation, TFixture) {
        MakeParser({"a1"}, {"String"});
        UNIT_ASSERT_EXCEPTION_CONTAINS(PushToParser(R"({"a1": 1234})"), simdjson::simdjson_error, "INCORRECT_TYPE: The JSON element does not have the requested type.");
    }

    Y_UNIT_TEST_F(JsonTypeValidation, TFixture) {
        MakeParser({"a1"}, {"Int32"});
        UNIT_ASSERT_EXCEPTION_CONTAINS(PushToParser(R"({"a1": {"key": "value"}})"), yexception, "Failed to parse json string, expected scalar type for column 'a1' with type Int32 but got nested json, please change column type to Json.");
    }

    Y_UNIT_TEST_F(ThrowExceptionByError, TFixture) {
        MakeParser({"a2", "a1"});
        UNIT_ASSERT_EXCEPTION_CONTAINS(PushToParser(R"(ydb)"), simdjson::simdjson_error, "INCORRECT_TYPE: The JSON element does not have the requested type.");
    }
}

}

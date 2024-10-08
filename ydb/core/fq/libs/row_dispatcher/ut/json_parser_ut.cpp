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
        Parser = NFq::NewJsonParser(columns, types, [this](TVector<TVector<std::string_view>>&& parsedValues, TJsonParserBuffer::TPtr buffer) {
            ResultOffset = buffer->GetOffset();
            ParsedValues = std::move(parsedValues);
            ResultNumberValues = ParsedValues.empty() ? 0 : ParsedValues.front().size();
        });
    }

    void MakeParser(TVector<TString> columns) {
        MakeParser(columns, TVector<TString>(columns.size(), "String"));
    }

    void PushToParser(ui64 offset, const TString& data) const {
        TJsonParserBuffer& buffer = Parser->GetBuffer(offset);
        buffer.Reserve(data.size());
        buffer.AddValue(data);
        Parser->Parse();
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

    ui64 ResultOffset;
    ui64 ResultNumberValues;
    TVector<TVector<std::string_view>> ParsedValues;
};

Y_UNIT_TEST_SUITE(TJsonParserTests) {
    Y_UNIT_TEST_F(Simple1, TFixture) {
        MakeParser({"a1", "a2"}, {"String", "Optional<Uint64>"});
        PushToParser(5, R"({"a1": "hello1", "a2": 101, "event": "event1"})");
        UNIT_ASSERT_VALUES_EQUAL(5, ResultOffset);
        UNIT_ASSERT_VALUES_EQUAL(1, ResultNumberValues);

        const auto& result = GetParsedRow(0);
        UNIT_ASSERT_VALUES_EQUAL(2, result.size());
        UNIT_ASSERT_VALUES_EQUAL("hello1", result.front());
        UNIT_ASSERT_VALUES_EQUAL("101", result.back());
    }

    Y_UNIT_TEST_F(Simple2, TFixture) {
        MakeParser({"a2", "a1"});
        PushToParser(5, R"({"a1": "hello1", "a2": "101", "event": "event1"})");
        UNIT_ASSERT_VALUES_EQUAL(5, ResultOffset);
        UNIT_ASSERT_VALUES_EQUAL(1, ResultNumberValues);

        const auto& result = GetParsedRow(0);
        UNIT_ASSERT_VALUES_EQUAL(2, result.size());
        UNIT_ASSERT_VALUES_EQUAL("101", result.front());
        UNIT_ASSERT_VALUES_EQUAL("hello1", result.back());
    }

    Y_UNIT_TEST_F(Simple3, TFixture) {
        MakeParser({"a1", "a2"});
        PushToParser(5, R"({"a2": "hello1", "a1": "101", "event": "event1"})");
        UNIT_ASSERT_VALUES_EQUAL(5, ResultOffset);
        UNIT_ASSERT_VALUES_EQUAL(1, ResultNumberValues);

        const auto& result = GetParsedRow(0);
        UNIT_ASSERT_VALUES_EQUAL(2, result.size());
        UNIT_ASSERT_VALUES_EQUAL("101", result.front());
        UNIT_ASSERT_VALUES_EQUAL("hello1", result.back());
    }

    Y_UNIT_TEST_F(Simple4, TFixture) {
        MakeParser({"a2", "a1"});
        PushToParser(5, R"({"a2": "hello1", "a1": "101", "event": "event1"})");
        UNIT_ASSERT_VALUES_EQUAL(5, ResultOffset);
        UNIT_ASSERT_VALUES_EQUAL(1, ResultNumberValues);

        const auto& result = GetParsedRow(0);
        UNIT_ASSERT_VALUES_EQUAL(2, result.size());
        UNIT_ASSERT_VALUES_EQUAL("hello1", result.front());
        UNIT_ASSERT_VALUES_EQUAL("101", result.back());
    }

    Y_UNIT_TEST_F(ManyValues, TFixture) {
        MakeParser({"a1", "a2"});

        TJsonParserBuffer& buffer = Parser->GetBuffer(42);
        buffer.AddValue(R"({"a1": "hello1", "a2": 101, "event": "event1"})");
        buffer.AddValue(R"({"a1": "hello1", "a2": "101", "event": "event2"})");
        buffer.AddValue(R"({"a2": "101", "a1": "hello1", "event": "event3"})");

        Parser->Parse();
        UNIT_ASSERT_VALUES_EQUAL(42, ResultOffset);
        UNIT_ASSERT_VALUES_EQUAL(3, ResultNumberValues);

        for (size_t i = 0; i < ResultNumberValues; ++i) {
            const auto& result = GetParsedRow(i);
            UNIT_ASSERT_VALUES_EQUAL_C(2, result.size(), i);
            UNIT_ASSERT_VALUES_EQUAL_C("hello1", result.front(), i);
            UNIT_ASSERT_VALUES_EQUAL_C("101", result.back(), i);
        }
    }

    Y_UNIT_TEST_F(ThrowExceptionByError, TFixture) {
        MakeParser({"a2", "a1"});
        UNIT_ASSERT_EXCEPTION_CONTAINS(PushToParser(5, R"(ydb)"), simdjson::simdjson_error, "INCORRECT_TYPE: The JSON element does not have the requested type.");
    }
}

}

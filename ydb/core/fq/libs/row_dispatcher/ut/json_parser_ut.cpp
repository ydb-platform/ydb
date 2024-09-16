#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/events/events.h>

#include <ydb/core/fq/libs/row_dispatcher/json_parser.h>

#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/actor_helpers.h>
#include <library/cpp/testing/unittest/registar.h>

namespace {

using namespace NKikimr;
using namespace NFq;

class TFixture : public NUnitTest::TBaseFixture {

public:
    TFixture()
    : Runtime(true) {}

    void SetUp(NUnitTest::TTestContext&) override {
        TAutoPtr<TAppPrepare> app = new TAppPrepare();
        Runtime.Initialize(app->Unwrap());
        Runtime.SetLogPriority(NKikimrServices::FQ_ROW_DISPATCHER, NLog::PRI_DEBUG);
    }

    void TearDown(NUnitTest::TTestContext& /* context */) override {
        if (Parser) {
            Parser.reset();
        }
    }

    void MakeParser(TVector<TString> columns, NFq::TJsonParser::TCallback callback) {
        Parser = NFq::NewJsonParser(
            columns,
            callback);
    }

    TActorSystemStub actorSystemStub;
    NActors::TTestActorRuntime Runtime;
    std::unique_ptr<NFq::TJsonParser> Parser;
};

Y_UNIT_TEST_SUITE(TJsonParserTests) {
    Y_UNIT_TEST_F(Simple1, TFixture) { 
        TList<TString> result;
        ui64 resultOffset;
        MakeParser({"a1", "a2"}, [&](ui64 offset, TList<TString>&& value){
                resultOffset = offset;
                result = std::move(value);
            });
        Parser->Push(5, R"({"a1": "hello1", "a2": "101",  "event": "event1"})");
        UNIT_ASSERT_VALUES_EQUAL(5, resultOffset);
        UNIT_ASSERT_VALUES_EQUAL(2, result.size());
        UNIT_ASSERT_VALUES_EQUAL("hello1", result.front());
        UNIT_ASSERT_VALUES_EQUAL("101", result.back());
    }

    Y_UNIT_TEST_F(Simple2, TFixture) { 
        TList<TString> result;
        ui64 resultOffset;
        MakeParser({"a2", "a1"}, [&](ui64 offset, TList<TString>&& value){
                resultOffset = offset;
                result = std::move(value);
            });
        Parser->Push(5, R"({"a1": "hello1", "a2": "101",  "event": "event1"})");
        UNIT_ASSERT_VALUES_EQUAL(5, resultOffset);
        UNIT_ASSERT_VALUES_EQUAL(2, result.size());
        UNIT_ASSERT_VALUES_EQUAL("101", result.front());
        UNIT_ASSERT_VALUES_EQUAL("hello1", result.back());
    }

    Y_UNIT_TEST_F(Simple3, TFixture) { 
        TList<TString> result;
        ui64 resultOffset;
        MakeParser({"a1", "a2"}, [&](ui64 offset, TList<TString>&& value){
                resultOffset = offset;
                result = std::move(value);
            });
        Parser->Push(5, R"({"a2": "hello1", "a1": "101",  "event": "event1"})");
        UNIT_ASSERT_VALUES_EQUAL(5, resultOffset);
        UNIT_ASSERT_VALUES_EQUAL(2, result.size());
        UNIT_ASSERT_VALUES_EQUAL("101", result.front());
        UNIT_ASSERT_VALUES_EQUAL("hello1", result.back());
    }

    Y_UNIT_TEST_F(Simple4, TFixture) { 
        TList<TString> result;
        ui64 resultOffset;
        MakeParser({"a2", "a1"}, [&](ui64 offset, TList<TString>&& value){
                resultOffset = offset;
                result = std::move(value);
            });
        Parser->Push(5, R"({"a2": "hello1", "a1": "101",  "event": "event1"})");
        UNIT_ASSERT_VALUES_EQUAL(5, resultOffset);
        UNIT_ASSERT_VALUES_EQUAL(2, result.size());
        UNIT_ASSERT_VALUES_EQUAL("hello1", result.front());
        UNIT_ASSERT_VALUES_EQUAL("101", result.back());
    }

    Y_UNIT_TEST_F(ThrowExceptionByError, TFixture) { 

        MakeParser({"a2", "a1"}, [&](ui64, TList<TString>&&){ });
        UNIT_ASSERT_EXCEPTION_CONTAINS(Parser->Push(5, R"(ydb)"), yexception, " Failed to unwrap empty optional");
    }
}

}


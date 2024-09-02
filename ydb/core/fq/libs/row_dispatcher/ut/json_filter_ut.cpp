#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/events/events.h>

#include <ydb/core/fq/libs/row_dispatcher/json_filter.h>

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
        Runtime.SetLogPriority(NKikimrServices::YQ_ROW_DISPATCHER, NLog::PRI_DEBUG);
    }

    void TearDown(NUnitTest::TTestContext& /* context */) override {
        if (Filter) {
            Filter.reset();
        }
    }

    void MakeFilter(
        TVector<TString> columns,
        TVector<TString> types,
        const TString& whereFilter,
        NFq::TJsonFilter::TCallback callback) {
        Filter = NFq::NewJsonFilter(
            columns,
            types,
            whereFilter,
            callback);
    }

    TActorSystemStub actorSystemStub;
    NActors::TTestActorRuntime Runtime;
    std::unique_ptr<NFq::TJsonFilter> Filter;
};

Y_UNIT_TEST_SUITE(TJsonFilterTests) {
    Y_UNIT_TEST_F(Simple1, TFixture) {
        TMap<ui64, TString> result;
        MakeFilter(
            {"a1", "a2"},
            {"String", "UInt64"},
            "where a2 > 100",
            [&](ui64 offset, const TString& json) {
                result[offset] = json;
            });
        Filter->Push(5, {"hello1", "99"});
        Filter->Push(6, {"hello2", "101"});
        UNIT_ASSERT_VALUES_EQUAL(1, result.size());
        UNIT_ASSERT_VALUES_EQUAL(R"({"a1":"hello2","a2":101})", result[6]);
    }

    Y_UNIT_TEST_F(Simple2, TFixture) {
        TMap<ui64, TString> result;
        MakeFilter(
            {"a2", "a1"},
            {"UInt64", "String"},
            "where a2 > 100",
            [&](ui64 offset, const TString& json) {
                result[offset] = json;
            });
        Filter->Push(5, {"99", "hello1"});
        Filter->Push(6, {"101", "hello2"});
        UNIT_ASSERT_VALUES_EQUAL(1, result.size());
        UNIT_ASSERT_VALUES_EQUAL(R"({"a1":"hello2","a2":101})", result[6]);
    }

     Y_UNIT_TEST_F(ThrowExceptionByError, TFixture) { 
        MakeFilter(
            {"a1", "a2"},
            {"String", "UInt64"},
            "where Unwrap(a2) = 1",
            [&](ui64, const TString&) { });
        UNIT_ASSERT_EXCEPTION_CONTAINS(Filter->Push(5, {"99", "hello1"}), yexception, "Failed to unwrap empty optional");
     }
}

}


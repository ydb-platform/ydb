#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/events/events.h>

#include <ydb/core/fq/libs/row_dispatcher/json_parser.h>
#include <ydb/core/fq/libs/row_dispatcher/json_filter.h>

#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/actor_helpers.h>
//#include <ydb/library/actors/testlib/test_runtime.h>
#include <library/cpp/testing/unittest/registar.h>

namespace {

using namespace NKikimr;
using namespace NFq;

struct TTestBootstrap : public NActors::TTestActorRuntime {
    explicit TTestBootstrap()
        : TTestActorRuntime(true)
    {
        TAutoPtr<TAppPrepare> app = new TAppPrepare();
        Initialize(app->Unwrap());
        SetLogPriority(NKikimrServices::YQ_ROW_DISPATCHER, NLog::PRI_DEBUG);
    }
    TActorSystemStub actorSystemStub;
};

Y_UNIT_TEST_SUITE(TRetryEventsQueueTest) {
    Y_UNIT_TEST(Empty) { 

        TTestBootstrap bootstrap; 
        TVector<TString> columns{"time", "data"};

        auto Parser = NFq::NewJsonParser(
            "/home/kardymon-d/arcadia2/contrib/ydb/library/yql/udfs/",
            columns,
            [&](ui64 offset, TList<TString>&& value){
                std::cerr << "offset " << offset << std::endl;
                for (auto v: value) {
                    std::cerr << "v " << v << std::endl;
                }

            });
        std::cerr << "push " << std::endl;

        Parser->Push(5, R"({"time": 101, "data": "hello1", "event": "event1"})");
        std::cerr << "push end" << std::endl;

    }
}

}


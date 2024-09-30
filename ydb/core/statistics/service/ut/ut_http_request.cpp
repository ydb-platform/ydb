#include <ydb/core/statistics/service/http_request.h>
#include <ydb/core/statistics/ut_common/ut_common.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/util/ulid.h>

namespace NKikimr {
namespace NStat {

Y_UNIT_TEST_SUITE(HttpRequest) {
    Y_UNIT_TEST(AnalyzeTable) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 10)[0];

        auto sender = runtime.AllocateEdgeActor();
        runtime.Register(new THttpRequest(THttpRequest::ERequestType::ANALYZE, {
            { THttpRequest::EParamType::PATH, tableInfo.Path }
        }, sender));

        auto res = runtime.GrabEdgeEvent<NMon::TEvHttpInfoRes>(sender);
        auto msg = static_cast<NMon::TEvHttpInfoRes*>(res->Get());

        Cerr << "Answer: '" << msg->Answer << "'" << Endl;
        UNIT_ASSERT(msg->Answer.find("Analyze sent. OperationId:") != TString::npos);
    }

    Y_UNIT_TEST(GetStatus) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 10)[0];

        auto sender = runtime.AllocateEdgeActor();
        auto operationId = TULIDGenerator().Next(TInstant::Now()).ToString();
        runtime.Register(new THttpRequest(THttpRequest::ERequestType::STATUS, {
            { THttpRequest::EParamType::PATH, tableInfo.Path },
            { THttpRequest::EParamType::OPERATION_ID, operationId }
        }, sender));

        auto res = runtime.GrabEdgeEvent<NMon::TEvHttpInfoRes>(sender);
        auto msg = static_cast<NMon::TEvHttpInfoRes*>(res->Get());

        Cerr << "Answer: '" << msg->Answer << "'" << Endl;
        UNIT_ASSERT_EQUAL("No analyze operation", msg->Answer);
    }

    Y_UNIT_TEST(Probe) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 10)[0];
        TString columnName = "Value";

        auto sender = runtime.AllocateEdgeActor();
        auto operationId = TULIDGenerator().Next(TInstant::Now()).ToBinary();
        auto analyzeRequest = MakeAnalyzeRequest({{tableInfo.PathId, {1, 2}}}, operationId);
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest.release());
        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);

        runtime.Register(new THttpRequest(THttpRequest::ERequestType::COUNT_MIN_SKETCH_PROBE, {
                { THttpRequest::EParamType::PATH, tableInfo.Path },
                { THttpRequest::EParamType::COLUMN_NAME, columnName },
                { THttpRequest::EParamType::CELL_VALUE, "1" }
            }, sender));

        auto res = runtime.GrabEdgeEvent<NMon::TEvHttpInfoRes>(sender);
        auto msg = static_cast<NMon::TEvHttpInfoRes*>(res->Get());

        Cerr << "Answer: '" << msg->Answer << "'" << Endl;
        UNIT_ASSERT(msg->Answer.find(tableInfo.Path + "[" + columnName + "]=") != TString::npos);
    }
}

} // NStat
} // NKikimr
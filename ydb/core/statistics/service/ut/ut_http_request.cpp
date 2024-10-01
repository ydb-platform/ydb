#include <ydb/core/statistics/service/http_request.h>
#include <ydb/core/statistics/ut_common/ut_common.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/util/ulid.h>

namespace NKikimr {
namespace NStat {

void AnalyzeTest(bool isServerless) {
    TTestEnv env(1, 1);
    auto& runtime = *env.GetServer().GetRuntime();
    const auto databaseInfo = isServerless
        ? CreateServerlessDatabaseColumnTables(env, 1, 10)
        : CreateDatabaseColumnTables(env, 1, 10);
    const auto& tableInfo = databaseInfo.Tables[0];
    const auto sender = runtime.AllocateEdgeActor();

    runtime.Register(new THttpRequest(THttpRequest::ERequestType::ANALYZE, {
        { THttpRequest::EParamType::PATH, tableInfo.Path }
    }, sender));

    auto res = runtime.GrabEdgeEvent<NMon::TEvHttpInfoRes>(sender);
    auto msg = static_cast<NMon::TEvHttpInfoRes*>(res->Get());

    Cerr << "Answer: '" << msg->Answer << "'" << Endl;
    const TString expected = "Analyze sent. OperationId:";
    UNIT_ASSERT_STRING_CONTAINS(msg->Answer, expected);
}

void ProbeTest(bool isServerless) {
    TTestEnv env(1, 1);
    auto& runtime = *env.GetServer().GetRuntime();
    const auto databaseInfo = isServerless
        ? CreateServerlessDatabaseColumnTables(env, 1, 10)
        : CreateDatabaseColumnTables(env, 1, 10);
    const auto& tableInfo = databaseInfo.Tables[0];
    TString columnName = "Value";
    const auto sender = runtime.AllocateEdgeActor();

    const auto operationId = TULIDGenerator().Next(TInstant::Now()).ToBinary();
    auto analyzeRequest = MakeAnalyzeRequest({{tableInfo.PathId, {1, 2}}}, operationId);
    runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest.release());
    runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);

    runtime.Register(new THttpRequest(THttpRequest::ERequestType::COUNT_MIN_SKETCH_PROBE, {
            { THttpRequest::EParamType::DATABASE, databaseInfo.FullDatabaseName},
            { THttpRequest::EParamType::PATH, tableInfo.Path },
            { THttpRequest::EParamType::COLUMN_NAME, columnName },
            { THttpRequest::EParamType::CELL_VALUE, "1" }
        }, sender));
    auto res = runtime.GrabEdgeEvent<NMon::TEvHttpInfoRes>(sender);
    auto msg = static_cast<NMon::TEvHttpInfoRes*>(res->Get());

    Cerr << "Answer: '" << msg->Answer << "'" << Endl;
    const TString expected = tableInfo.Path + "[" + columnName + "]=";
    UNIT_ASSERT_STRING_CONTAINS(msg->Answer, expected);
}

Y_UNIT_TEST_SUITE(HttpRequest) {
    Y_UNIT_TEST(Analyze) {
        AnalyzeTest(false);
    }

    Y_UNIT_TEST(AnalyzeServerless) {
        AnalyzeTest(true);
    }

    Y_UNIT_TEST(Status) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        const auto databaseInfo = CreateDatabaseColumnTables(env, 1, 10);
        const auto& tableInfo = databaseInfo.Tables[0];

        const auto sender = runtime.AllocateEdgeActor();
        const auto operationId = TULIDGenerator().Next(TInstant::Now()).ToString();
        runtime.Register(new THttpRequest(THttpRequest::ERequestType::STATUS, {
            { THttpRequest::EParamType::PATH, tableInfo.Path },
            { THttpRequest::EParamType::OPERATION_ID, operationId }
        }, sender));

        auto res = runtime.GrabEdgeEvent<NMon::TEvHttpInfoRes>(sender);
        auto msg = static_cast<NMon::TEvHttpInfoRes*>(res->Get());

        Cerr << "Answer: '" << msg->Answer << "'" << Endl;
        UNIT_ASSERT_EQUAL(msg->Answer, "No analyze operation");
    }

    Y_UNIT_TEST(Probe) {
        ProbeTest(false);
    }

    Y_UNIT_TEST(ProbeServerless) {
        ProbeTest(true);
    }
}

} // NStat
} // NKikimr
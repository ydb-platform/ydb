#include <ydb/core/statistics/service/http_request.h>
#include <ydb/core/statistics/ut_common/ut_common.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/util/ulid.h>
#include <library/cpp/json/json_reader.h>

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
        },
        THttpRequest::EResponseContentType::HTML,
        sender));

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

    bool firstStatsToSA = false;
    auto statsObserver1 = runtime.AddObserver<TEvStatistics::TEvSchemeShardStats>([&](auto& /* ev */){
        firstStatsToSA = true;
    });
    runtime.WaitFor("TEvSchemeShardStats 1", [&]{ return firstStatsToSA; });

    bool secondStatsToSA = false;
    auto statsObserver2 = runtime.AddObserver<TEvStatistics::TEvSchemeShardStats>([&](auto& /* ev */){
        secondStatsToSA = true;
    });
    runtime.WaitFor("TEvSchemeShardStats 2", [&]{ return secondStatsToSA; });

    const auto operationId = TULIDGenerator().Next(TInstant::Now()).ToBinary();
    auto analyzeRequest = MakeAnalyzeRequest({{tableInfo.PathId, {1, 2}}}, operationId);
    runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest.release());
    runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);

    runtime.Register(new THttpRequest(THttpRequest::ERequestType::PROBE_COUNT_MIN_SKETCH, {
            { THttpRequest::EParamType::PATH, tableInfo.Path },
            { THttpRequest::EParamType::COLUMN_NAME, columnName },
            { THttpRequest::EParamType::CELL_VALUE, "1" }
        },
        THttpRequest::EResponseContentType::HTML,
        sender));
    auto res = runtime.GrabEdgeEvent<NMon::TEvHttpInfoRes>(sender);
    auto msg = static_cast<NMon::TEvHttpInfoRes*>(res->Get());

    Cerr << "Answer: '" << msg->Answer << "'" << Endl;
    const TString expected = tableInfo.Path + "[" + columnName + "]=";
    UNIT_ASSERT_STRING_CONTAINS(msg->Answer, expected);
}

void ProbeBaseStatsTest(bool isServerless) {
    TTestEnv env(1, 1);

    auto& runtime = *env.GetServer().GetRuntime();

    // Create a database and a table
    if (isServerless) {
        CreateDatabase(env, "Shared", 1, true);
        CreateServerlessDatabase(env, "Database", "/Root/Shared");
    } else {
        CreateDatabase(env, "Database");
    }
    CreateColumnStoreTable(env, "Database", "Table", 5);
    const TString path = "/Root/Database/Table";
    const TPathId pathId = ResolvePathId(runtime, path);
    const ui32 nodeIdx = 1;

    // Wait until correct base statistics gets reported.
    ValidateRowCount(runtime, nodeIdx, pathId, ColumnTableRowsNumber);

    // Issue the probe_base_stats request and verify that the result makes sense.
    const auto sender = runtime.AllocateEdgeActor(nodeIdx);
    runtime.Register(
        new THttpRequest(THttpRequest::ERequestType::PROBE_BASE_STATS, {
                { THttpRequest::EParamType::PATH, path },
            },
            THttpRequest::EResponseContentType::JSON,
            sender),
        nodeIdx);
    auto res = runtime.GrabEdgeEvent<NMon::TEvHttpInfoRes>(sender);
    auto msg = static_cast<NMon::TEvHttpInfoRes*>(res->Get());

    TStringBuf answer(msg->Answer);
    Cerr << "Answer: '" << answer << "'" << Endl;
    auto jsonStart = answer.find('{');
    UNIT_ASSERT(jsonStart != TStringBuf::npos);
    TStringBuf jsonStr = answer.SubStr(jsonStart);
    NJson::TJsonValue json;
    UNIT_ASSERT(NJson::ReadJsonTree(jsonStr, &json));
    UNIT_ASSERT_VALUES_EQUAL(json["row_count"].GetIntegerSafe(), ColumnTableRowsNumber);
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
            },
            THttpRequest::EResponseContentType::HTML,
            sender));

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

    Y_UNIT_TEST(ProbeBaseStats) {
        ProbeBaseStatsTest(false);
    }

    Y_UNIT_TEST(ProbeBaseStatsServerless) {
        ProbeBaseStatsTest(true);
    }
}

} // NStat
} // NKikimr

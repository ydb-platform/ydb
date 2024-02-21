#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include "datashard_ut_common_kqp.h"

#include <library/cpp/json/json_reader.h>

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;

Y_UNIT_TEST_SUITE(DataShardDebugInfo) {

    TVector<NJson::TJsonValue> ExtractDebugInfo(const Ydb::Table::ExecuteQueryResult& result) {
        TVector<NJson::TJsonValue> out;
        for (const TString& debugInfo : result.query_stats().debug_info()) {
            NJson::ReadJsonTree(debugInfo, &out.emplace_back(), /* throwOnError */ true);
        }
        return out;
    }

    TVector<NJson::TJsonValue> ExtractDebugInfo(const Ydb::Table::ExecuteDataQueryResponse& response) {
        Ydb::Table::ExecuteQueryResult result;
        response.operation().result().UnpackTo(&result);
        return ExtractDebugInfo(result);
    }

    Y_UNIT_TEST(ReadDebugInfo) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 10), (2, 20), (3, 30);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (4, 40), (5, 50), (6, 60);");

        runtime.GetAppData().FeatureFlags.SetEnableQueryDebugInfo(true);

        TString readSessionId = CreateSessionRPC(runtime);
        auto readFuture = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            UNION ALL
            SELECT key, value FROM `/Root/table-2`
            ORDER BY key;
            )"), readSessionId, "", /* commit */ true));

        auto readResponse = AwaitResponse(runtime, std::move(readFuture));
        UNIT_ASSERT_VALUES_EQUAL(readResponse.operation().status(), Ydb::StatusIds::SUCCESS);

        int reads = 0;
        for (auto& debugInfo : ExtractDebugInfo(readResponse)) {
            if (debugInfo.Has("op") && debugInfo["op"].GetString() == "read") {
                ++reads;
            }
        }

        UNIT_ASSERT_C(reads >= 2, "Found only " << reads << " reads");
    }

} // Y_UNIT_TEST_SUITE(DataShardDebugInfo)

} // namespace NKikimr

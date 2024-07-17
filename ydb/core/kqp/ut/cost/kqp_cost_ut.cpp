#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <library/cpp/json/json_reader.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

static NKikimrConfig::TAppConfig GetAppConfig(bool scanSourceRead = false, bool streamLookup = true) {
    auto app = NKikimrConfig::TAppConfig();
    app.MutableTableServiceConfig()->SetEnableKqpScanQuerySourceRead(scanSourceRead);
    app.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamLookup(streamLookup);
    return app;
}

static NYdb::NTable::TExecDataQuerySettings GetDataQuerySettings() {
    NYdb::NTable::TExecDataQuerySettings execSettings;
    execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);
    return execSettings;
}

Y_UNIT_TEST_SUITE(KqpCost) {
    void EnableDebugLogging(NActors::TTestActorRuntime * runtime) {
        //runtime->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        // runtime->SetLogPriority(NKikimrServices::TX_PROXY_SCHEME_CACHE, NActors::NLog::PRI_DEBUG);
        // runtime->SetLogPriority(NKikimrServices::SCHEME_BOARD_REPLICA, NActors::NLog::PRI_DEBUG);
        // runtime->SetLogPriority(NKikimrServices::TX_PROXY, NActors::NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::KQP_COMPUTE, NActors::NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::KQP_GATEWAY, NActors::NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::KQP_RESOURCE_MANAGER, NActors::NLog::PRI_DEBUG);
        //runtime->SetLogPriority(NKikimrServices::LONG_TX_SERVICE, NActors::NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_TRACE);
        runtime->SetLogPriority(NKikimrServices::TX_COLUMNSHARD_SCAN, NActors::NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::TX_CONVEYOR, NActors::NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);
        //runtime->SetLogPriority(NKikimrServices::BLOB_CACHE, NActors::NLog::PRI_DEBUG);
        //runtime->SetLogPriority(NKikimrServices::GRPC_SERVER, NActors::NLog::PRI_DEBUG);
    }

    Y_UNIT_TEST(PointLookup) {
        TKikimrRunner kikimr(GetAppConfig(false, false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = Q_(R"(
            SELECT * FROM `/Root/Test` WHERE Group = 1u AND Name = "Anna";
        )");

        auto txControl = TTxControl::BeginTx().CommitTx();

        auto result = session.ExecuteDataQuery(query, txControl, GetDataQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        CompareYson(R"(
            [
                [[3500u];["None"];
                [1u];["Anna"]]
            ]
        )", NYdb::FormatResultSetYson(result.GetResultSet(0)));

        auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().bytes(), 20);
    }

    Y_UNIT_TEST(Range) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = Q_(R"(
            SELECT * FROM `/Root/Test` WHERE Group < 2u ORDER BY Group;
        )");

        auto txControl = TTxControl::BeginTx().CommitTx();

        auto result = session.ExecuteDataQuery(query, txControl, GetDataQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        CompareYson(R"(
            [
                [[3500u];["None"];[1u];["Anna"]];
                [[300u];["None"];[1u];["Paul"]]
            ]
        )", NYdb::FormatResultSetYson(result.GetResultSet(0)));

        auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        size_t phase = stats.query_phases_size() - 1;
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).reads().rows(), 2);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(phase).table_access(0).reads().bytes(), 40);
    }

    Y_UNIT_TEST(RangeFullScan) {
        TKikimrRunner kikimr(GetAppConfig());

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = Q_(R"(
            SELECT * FROM `/Root/Test` WHERE Amount < 5000ul ORDER BY Group LIMIT 1;
        )");

        auto txControl = TTxControl::BeginTx().CommitTx();

        auto result = session.ExecuteDataQuery(query, txControl, GetDataQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        CompareYson(R"(
            [
                [[3500u];["None"];[1u];["Anna"]]
            ]
        )", NYdb::FormatResultSetYson(result.GetResultSet(0)));

        auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

        Cerr << stats.DebugString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().bytes(), 20);
    }

    const static TString Query = R"(SELECT * FROM `/Root/Test` WHERE Amount < 5000ul ORDER BY Group LIMIT 1;)";
    const static TString Expected = R"([[[3500u];["None"];[1u];["Anna"]]])";

    Y_UNIT_TEST_TWIN(ScanQueryRangeFullScan, SourceRead) {
        TKikimrRunner kikimr(GetAppConfig(SourceRead));

        auto db = kikimr.GetTableClient();
        EnableDebugLogging(kikimr.GetTestServer().GetRuntime());
        auto query = Q_(Query);

        NYdb::NTable::TStreamExecScanQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto it = db.StreamExecuteScanQuery(query, execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(it.GetStatus(), EStatus::SUCCESS);
        auto res = CollectStreamResult(it);

        UNIT_ASSERT(res.ConsumedRuFromHeader > 0);

        CompareYson(Expected, res.ResultSetYson);
/*
        const auto& stats = *res.QueryStats;

        Cerr << stats.DebugString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().bytes(), 20);
*/
    }

    Y_UNIT_TEST_TWIN(ScanScriptingRangeFullScan, SourceRead) {
        TKikimrRunner kikimr(GetAppConfig(SourceRead));

        NYdb::NScripting::TScriptingClient client(kikimr.GetDriver());
        auto query = Q_(Query);

        NYdb::NScripting::TExecuteYqlRequestSettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto it = client.StreamExecuteYqlScript(query, execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(it.GetStatus(), EStatus::SUCCESS);
        auto res = CollectStreamResult(it);

        UNIT_ASSERT(res.ConsumedRuFromHeader > 0);

        CompareYson(Expected, res.ResultSetYson);
    }

    Y_UNIT_TEST(QuerySeviceRangeFullScan) {
        TKikimrRunner kikimr(GetAppConfig());

        NYdb::NQuery::TQueryClient client(kikimr.GetDriver());
        auto query = Q_(Query);

        NYdb::NQuery::TExecuteQuerySettings execSettings;

        auto it = client.StreamExecuteQuery(
            query,
            NYdb::NQuery::TTxControl::BeginTx().CommitTx(),
            execSettings
        ).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(it.GetStatus(), EStatus::SUCCESS);
        auto res = CollectStreamResult(it);

        UNIT_ASSERT(res.ConsumedRuFromHeader > 0);

        CompareYson(Expected, res.ResultSetYson);
    }

}

}
}

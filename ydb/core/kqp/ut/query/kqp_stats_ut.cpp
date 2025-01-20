#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/resources/ydb_resources.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/public/sdk/cpp/client/draft/ydb_scripting.h>

#include <cstdlib>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpStats) {

auto GetYqlStreamIterator(
        TKikimrRunner& kikimr,
        ECollectQueryStatsMode mode,
        const TString& query) {
    NYdb::NScripting::TExecuteYqlRequestSettings settings;
    settings.CollectQueryStats(mode);

    NYdb::NScripting::TScriptingClient client(kikimr.GetDriver());

    auto it = client.StreamExecuteYqlScript(query, settings).GetValueSync();
    return it;
}

auto GetScanStreamIterator(
        TKikimrRunner& kikimr,
        ECollectQueryStatsMode mode,
        const TString& query) {
    auto db = kikimr.GetTableClient();

    TStreamExecScanQuerySettings settings;
    settings.CollectQueryStats(mode);

    auto it = db.StreamExecuteScanQuery(query, settings).GetValueSync();
    return it;
}

template <typename Iterator>
void MultiTxStatsFullExp(
        std::function<Iterator(TKikimrRunner&, ECollectQueryStatsMode, const TString&)> getIter) {
    auto kikimr = DefaultKikimrRunner();
    auto it = getIter(kikimr, ECollectQueryStatsMode::Profile, R"(
        SELECT * FROM `/Root/EightShard` WHERE Key BETWEEN 150 AND 266 ORDER BY Data LIMIT 4;
    )");
    auto res = CollectStreamResult(it);
    CompareYson(R"([
        [[1];[202u];["Value2"]];
        [[2];[201u];["Value1"]];
        [[3];[203u];["Value3"]]
    ])", res.ResultSetYson);

    UNIT_ASSERT(res.PlanJson);
    NJson::TJsonValue plan;
    NJson::ReadJsonTree(*res.PlanJson, &plan, true);
    auto node = FindPlanNodeByKv(plan, "Node Type", "TopSort-TableRangeScan");
    if (!node.IsDefined()) {
        node = FindPlanNodeByKv(plan, "Node Type", "TopSort-Filter-TableRangeScan");
    }
    UNIT_ASSERT_EQUAL(node.GetMap().at("Stats").GetMapSafe().at("Tasks").GetIntegerSafe(), 2);
}

Y_UNIT_TEST(MultiTxStatsFullExpYql) {
    MultiTxStatsFullExp<NYdb::NScripting::TYqlResultPartIterator>(GetYqlStreamIterator);
}

Y_UNIT_TEST(MultiTxStatsFullExpScan) {
    MultiTxStatsFullExp<NYdb::NTable::TScanQueryPartIterator>(GetScanStreamIterator);
}

template <typename Iterator>
void JoinNoStats(
        std::function<Iterator(TKikimrRunner&, ECollectQueryStatsMode, const TString&)> getIter) {
    auto kikimr = DefaultKikimrRunner();
    auto it = getIter(kikimr, ECollectQueryStatsMode::None, R"(
        SELECT count(*) FROM `/Root/EightShard` AS t JOIN `/Root/KeyValue` AS kv ON t.Data = kv.Key;
    )");
    auto res = CollectStreamResult(it);
    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(res.ResultSetYson, "[[16u]]");

    UNIT_ASSERT(!res.QueryStats);
    UNIT_ASSERT(!res.PlanJson);
}

Y_UNIT_TEST(JoinNoStatsYql) {
    JoinNoStats<NYdb::NScripting::TYqlResultPartIterator>(GetYqlStreamIterator);
}

Y_UNIT_TEST(JoinNoStatsScan) {
    JoinNoStats<NYdb::NTable::TScanQueryPartIterator>(GetScanStreamIterator);
}

template <typename Iterator>
TCollectedStreamResult JoinStatsBasic(
        std::function<Iterator(TKikimrRunner&, ECollectQueryStatsMode, const TString&)> getIter, bool StreamLookupJoin = false) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(StreamLookupJoin);
    appConfig.MutableTableServiceConfig()->SetEnableKqpScanQueryStreamLookup(false);
    appConfig.MutableTableServiceConfig()->SetEnableKqpScanQuerySourceRead(true);
    appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamLookup(true);

    auto settings = TKikimrSettings()
        .SetAppConfig(appConfig);
    TKikimrRunner kikimr(settings);

    auto it = getIter(kikimr, ECollectQueryStatsMode::Basic, R"(
        SELECT count(*) FROM `/Root/EightShard` AS t JOIN `/Root/KeyValue` AS kv ON t.Data = kv.Key;
    )");
    auto res = CollectStreamResult(it);
    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

    UNIT_ASSERT_VALUES_EQUAL(res.ResultSetYson, "[[16u]]");

    UNIT_ASSERT(res.QueryStats);
    return res;
}

Y_UNIT_TEST_TWIN(JoinStatsBasicYql, StreamLookupJoin) {
    auto res = JoinStatsBasic<NYdb::NScripting::TYqlResultPartIterator>(GetYqlStreamIterator, StreamLookupJoin);

    if (StreamLookupJoin) {
        UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases(0).table_access().size(), 2);
    } else {
        UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases(0).table_access().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases(0).table_access(0).name(), "/Root/EightShard");
        UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases(1).table_access().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases(1).table_access(0).name(), "/Root/KeyValue");
    }
}

Y_UNIT_TEST(JoinStatsBasicScan) {
    auto res = JoinStatsBasic<NYdb::NTable::TScanQueryPartIterator>(GetScanStreamIterator);

    UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases().size(), 2);
    if (res.QueryStats->query_phases(0).table_access(0).name() == "/Root/KeyValue") {
        UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases(0).table_access(0).name(), "/Root/KeyValue");
        UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases(0).table_access(0).partitions_count(), 1);
        UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases(0).table_access(1).name(), "/Root/EightShard");
        UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases(0).table_access(1).partitions_count(), 8);
    } else {
        UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases(0).table_access(0).name(), "/Root/EightShard");
        UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases(0).table_access(0).partitions_count(), 8);
        UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases(0).table_access(1).name(), "/Root/KeyValue");
        UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases(0).table_access(1).partitions_count(), 1);
    }

    UNIT_ASSERT(!res.PlanJson);
}

template <typename Iterator>
void MultiTxStatsFull(
        std::function<Iterator(TKikimrRunner&, ECollectQueryStatsMode, const TString&)> getResult) {
    auto app = NKikimrConfig::TAppConfig();
    app.MutableTableServiceConfig()->SetEnableKqpScanQuerySourceRead(true);
    TKikimrRunner kikimr(app);
    auto it = getResult(kikimr, ECollectQueryStatsMode::Full, R"(
        SELECT * FROM `/Root/EightShard` WHERE Key BETWEEN 150 AND 266 ORDER BY Data LIMIT 4;
    )");
    auto res = CollectStreamResult(it);

    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(
        res.ResultSetYson,
        R"([[[1];[202u];["Value2"]];[[2];[201u];["Value1"]];[[3];[203u];["Value3"]]])"
    );

    UNIT_ASSERT(res.QueryStats);
    UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases().size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases(0).table_access(0).name(), "/Root/EightShard");
    UNIT_ASSERT_VALUES_EQUAL(res.QueryStats->query_phases(0).table_access(0).partitions_count(), 2);

    UNIT_ASSERT(res.PlanJson);
    NJson::TJsonValue plan;
    NJson::ReadJsonTree(*res.PlanJson, &plan, true);
    Cout << plan;
    auto node = FindPlanNodeByKv(plan, "Node Type", "TopSort");
    UNIT_ASSERT_EQUAL(node.GetMap().at("Stats").GetMapSafe().at("Tasks").GetIntegerSafe(), 2);
}

Y_UNIT_TEST(MultiTxStatsFullYql) {
    MultiTxStatsFull<NYdb::NScripting::TYqlResultPartIterator>(GetYqlStreamIterator);
}

Y_UNIT_TEST(MultiTxStatsFullScan) {
    MultiTxStatsFull<NYdb::NTable::TScanQueryPartIterator>(GetScanStreamIterator);
}

Y_UNIT_TEST(DeferredEffects) {
    auto kikimr = DefaultKikimrRunner();
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();
    TString planJson;
    NJson::TJsonValue plan;

    TExecDataQuerySettings settings;
    settings.CollectQueryStats(ECollectQueryStatsMode::Full);

    auto result = session.ExecuteDataQuery(R"(
        UPSERT INTO `/Root/TwoShard`
        SELECT Key + 100u AS Key, Value1 FROM `/Root/TwoShard` WHERE Key in (1,2,3,4,5);
    )", TTxControl::BeginTx(), settings).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

    // TODO(sk): do proper phase dependency tracking
    //
    // NJson::ReadJsonTree(result.GetQueryPlan(), &plan, true);
    // auto node = FindPlanNodeByKv(plan, "Node Type", "TablePointLookup");
    // UNIT_ASSERT_EQUAL(node.GetMap().at("Stats").GetMapSafe().at("Tasks").GetIntegerSafe(), 1);

    auto tx = result.GetTransaction();
    UNIT_ASSERT(tx);

    auto params = db.GetParamsBuilder()
        .AddParam("$key")
            .Uint32(100)
            .Build()
        .AddParam("$value")
            .String("New")
            .Build()
        .Build();

    result = session.ExecuteDataQuery(R"(

        DECLARE $key AS Uint32;
        DECLARE $value AS String;

        UPSERT INTO `/Root/TwoShard` (Key, Value1) VALUES
            ($key, $value);
    )", TTxControl::Tx(*tx).CommitTx(), std::move(params), settings).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

    NJson::ReadJsonTree(result.GetQueryPlan(), &plan, true);
    UNIT_ASSERT_VALUES_EQUAL(plan.GetMapSafe().at("Plan").GetMapSafe().at("Plans").GetArraySafe().size(), 3);

    result = session.ExecuteDataQuery(R"(
        SELECT * FROM `/Root/TwoShard`;
        UPDATE `/Root/TwoShard` SET Value1 = "XXX" WHERE Key in (3,600);
    )", TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
    result.GetIssues().PrintTo(Cerr);
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

    NJson::ReadJsonTree(result.GetQueryPlan(), &plan, true);
    UNIT_ASSERT_VALUES_EQUAL(plan.GetMapSafe().at("Plan").GetMapSafe().at("Plans").GetArraySafe().size(), 3);

    auto ru = result.GetResponseMetadata().find(NYdb::YDB_CONSUMED_UNITS_HEADER);
    UNIT_ASSERT(ru != result.GetResponseMetadata().end());

    UNIT_ASSERT(std::atoi(ru->second.c_str()) > 1);
}

Y_UNIT_TEST(DataQueryWithEffects) {
    auto kikimr = DefaultKikimrRunner();
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    TExecDataQuerySettings settings;
    settings.CollectQueryStats(ECollectQueryStatsMode::Full);

    auto result = session.ExecuteDataQuery(R"(
        UPSERT INTO `/Root/TwoShard`
        SELECT Key + 1u AS Key, Value1 FROM `/Root/TwoShard`;
    )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), settings).ExtractValueSync();
    result.GetIssues().PrintTo(Cerr);
    AssertSuccessResult(result);

    NJson::TJsonValue plan;
    NJson::ReadJsonTree(result.GetQueryPlan(), &plan, true);

    auto node = FindPlanNodeByKv(plan, "Node Type", "Upsert-ConstantExpr");
    UNIT_ASSERT_EQUAL(node.GetMap().at("Stats").GetMapSafe().at("Tasks").GetIntegerSafe(), 2);
}

Y_UNIT_TEST(DataQueryMulti) {
    auto kikimr = DefaultKikimrRunner();
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    TExecDataQuerySettings settings;
    settings.CollectQueryStats(ECollectQueryStatsMode::Full);

    auto result = session.ExecuteDataQuery(R"(
        SELECT 1;
        SELECT 2;
        SELECT 3;
        SELECT 4;
    )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), settings).ExtractValueSync();
    result.GetIssues().PrintTo(Cerr);
    AssertSuccessResult(result);

    NJson::TJsonValue plan;
    NJson::ReadJsonTree(result.GetQueryPlan(), &plan, true);
    UNIT_ASSERT_EQUAL_C(plan.GetMapSafe().at("Plan").GetMapSafe().at("Plans").GetArraySafe().size(), 0, result.GetQueryPlan());
}

Y_UNIT_TEST(RequestUnitForBadRequestExecute) {
    auto kikimr = DefaultKikimrRunner();
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    auto result = session.ExecuteDataQuery(Q_(R"(
            INCORRECT_STMT
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), TExecDataQuerySettings().ReportCostInfo(true))
        .ExtractValueSync();
    result.GetIssues().PrintTo(Cerr);

    auto ru = result.GetResponseMetadata().find(NYdb::YDB_CONSUMED_UNITS_HEADER);
    UNIT_ASSERT(ru != result.GetResponseMetadata().end());
    UNIT_ASSERT_VALUES_EQUAL(ru->second, "1");
    UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
    UNIT_ASSERT(result.GetConsumedRu() > 0);
}

Y_UNIT_TEST(RequestUnitForBadRequestExplicitPrepare) {
    auto kikimr = DefaultKikimrRunner();
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    auto result = session.PrepareDataQuery(Q_(R"(
        INCORRECT_STMT
    )"), TPrepareDataQuerySettings().ReportCostInfo(true)).ExtractValueSync();
    result.GetIssues().PrintTo(Cerr);

    auto ru = result.GetResponseMetadata().find(NYdb::YDB_CONSUMED_UNITS_HEADER);
    UNIT_ASSERT(ru != result.GetResponseMetadata().end());
    UNIT_ASSERT_VALUES_EQUAL(ru->second, "1");
    UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
    UNIT_ASSERT(result.GetConsumedRu() > 0);
}

Y_UNIT_TEST(RequestUnitForSuccessExplicitPrepare) {
    auto kikimr = DefaultKikimrRunner();
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    auto result = session.PrepareDataQuery(Q_(R"(
        SELECT 0; SELECT 1; SELECT 2; SELECT 3; SELECT 4;
        SELECT 5; SELECT 6; SELECT 7; SELECT 8; SELECT 9;
    )"), TPrepareDataQuerySettings().ReportCostInfo(true)).ExtractValueSync();
    result.GetIssues().PrintTo(Cerr);

    auto ru = result.GetResponseMetadata().find(NYdb::YDB_CONSUMED_UNITS_HEADER);
    UNIT_ASSERT(ru != result.GetResponseMetadata().end());
    UNIT_ASSERT(atoi(ru->second.c_str()) > 1);
    UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
    UNIT_ASSERT(result.GetConsumedRu() > 1);
}

Y_UNIT_TEST(RequestUnitForExecute) {
    auto kikimr = DefaultKikimrRunner();
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    auto query = Q1_(R"(
        SELECT COUNT(*) FROM TwoShard;
    )");

    auto settings = TExecDataQuerySettings()
        .KeepInQueryCache(true)
        .ReportCostInfo(true);

    // Cached/uncached executions
    for (ui32 i = 0; i < 2; ++i) {
        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        Cerr << "Consumed units: " << result.GetConsumedRu() << Endl;
        UNIT_ASSERT(result.GetConsumedRu() > 1);

        auto ru = result.GetResponseMetadata().find(NYdb::YDB_CONSUMED_UNITS_HEADER);
        UNIT_ASSERT(ru != result.GetResponseMetadata().end());
        UNIT_ASSERT(atoi(ru->second.c_str()) > 1);
    }
}

Y_UNIT_TEST(StatsProfile) {
    auto kikimr = DefaultKikimrRunner();
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    TExecDataQuerySettings settings;
    settings.CollectQueryStats(ECollectQueryStatsMode::Profile);

    auto result = session.ExecuteDataQuery(R"(
        SELECT COUNT(*) FROM TwoShard;
    )", TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

    Cerr << result.GetQueryPlan() << Endl;

    NJson::TJsonValue plan;
    NJson::ReadJsonTree(result.GetQueryPlan(), &plan, true);

    auto node1 = FindPlanNodeByKv(plan, "Node Type", "Aggregate");
    UNIT_ASSERT_EQUAL(node1.GetMap().at("Stats").GetMapSafe().at("ComputeNodes").GetArraySafe().size(), 2);

    //auto node2 = FindPlanNodeByKv(plan, "Node Type", "Aggregate");
    //UNIT_ASSERT_EQUAL(node2.GetMap().at("Stats").GetMapSafe().at("ComputeNodes").GetArraySafe().size(), 1);
}

Y_UNIT_TEST_TWIN(StreamLookupStats, StreamLookupJoin) {
    NKikimrConfig::TAppConfig app;
    app.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(StreamLookupJoin);
    app.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamLookup(true);

    TKikimrRunner kikimr(TKikimrSettings().SetAppConfig(app));
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    TExecDataQuerySettings settings;
    settings.CollectQueryStats(ECollectQueryStatsMode::Full);

    auto result = session.ExecuteDataQuery(R"(
        $keys = SELECT Key FROM `/Root/KeyValue`;
        SELECT * FROM `/Root/TwoShard` WHERE Key in $keys;
    )", TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

    Cerr << result.GetQueryPlan() << Endl;

    NJson::TJsonValue plan;
    NJson::ReadJsonTree(result.GetQueryPlan(), &plan, true);
    auto streamLookup = FindPlanNodeByKv(plan, "Node Type", "TableLookup");
    UNIT_ASSERT(streamLookup.IsDefined());

    auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

    if (StreamLookupJoin) {
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).affected_shards(), 2);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).partitions_count(), 1);
    } else {
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).affected_shards(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).name(), "/Root/TwoShard");
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).partitions_count(), 1);
    }

    AssertTableStats(result, "/Root/TwoShard", {
        .ExpectedReads = 2,
    });
}

Y_UNIT_TEST(SysViewClientLost) {
    TKikimrRunner kikimr;
    CreateLargeTable(kikimr, 500000, 10, 100, 5000, 1);

    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    {
        TStringStream request;
        request << "SELECT * FROM `/Root/.sys/top_queries_by_read_bytes_one_hour` ORDER BY Duration";

        auto it = db.StreamExecuteScanQuery(request.Str()).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        ui64 rowsCount = 0;
        for (;;) {
            auto streamPart = it.ReadNext().GetValueSync();
            if (!streamPart.IsSuccess()) {
                UNIT_ASSERT_C(streamPart.EOS(), streamPart.GetIssues().ToString());
                break;
            }

            if (streamPart.HasResultSet()) {
                auto resultSet = streamPart.ExtractResultSet();

                NYdb::TResultSetParser parser(resultSet);
                while (parser.TryNextRow()) {
                    auto value = parser.ColumnParser("QueryText").GetOptionalUtf8();
                    UNIT_ASSERT(value);
                    rowsCount++;
                }
            }
        }
        UNIT_ASSERT(rowsCount == 1);
    }

    auto settings = TStreamExecScanQuerySettings();
    settings.ClientTimeout(TDuration::MilliSeconds(50));

    TStringStream timeoutedRequestStream;
    timeoutedRequestStream << R"(
        SELECT COUNT(*) FROM `/Root/LargeTable` WHERE SUBSTRING(DataText, 50, 5) = "22222";
    )";
    TString timeoutedRequest = timeoutedRequestStream.Str();

    auto result = db.StreamExecuteScanQuery(timeoutedRequest, settings).GetValueSync();

    if (result.IsSuccess()) {
        try {
            auto yson = StreamResultToYson(result, true);
            UNIT_ASSERT(false);
        } catch (const TStreamReadError& ex) {
            UNIT_ASSERT_VALUES_EQUAL(ex.Status, NYdb::EStatus::CLIENT_DEADLINE_EXCEEDED);
        } catch (const std::exception& ex) {
            UNIT_ASSERT_C(false, "unknown exception during the test");
        }
    } else {
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::CLIENT_DEADLINE_EXCEEDED);
    }

    ui32 timeoutedCount = 0;
    ui32 iterations = 10;
    while (timeoutedCount == 0 && iterations > 0)
    {
        iterations--;
        Sleep(TDuration::Seconds(1));

        TStringStream request;
        request << "SELECT * FROM `/Root/.sys/top_queries_by_read_bytes_one_hour` ORDER BY Duration";

        auto it = db.StreamExecuteScanQuery(request.Str()).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        ui64 queryCount = 0;
        for (;;) {
            auto streamPart = it.ReadNext().GetValueSync();
            if (!streamPart.IsSuccess()) {
                UNIT_ASSERT_C(streamPart.EOS(), streamPart.GetIssues().ToString());
                break;
            }

            if (streamPart.HasResultSet()) {
                auto resultSet = streamPart.ExtractResultSet();

                NYdb::TResultSetParser parser(resultSet);
                while (parser.TryNextRow()) {
                    auto value = parser.ColumnParser("QueryText").GetOptionalUtf8();
                    UNIT_ASSERT(value);
                    if (*value == timeoutedRequest) {
                        queryCount++;
                    }
                }
            }
        }
        timeoutedCount = queryCount;
    }

    UNIT_ASSERT(timeoutedCount == 1);
}

Y_UNIT_TEST(SysViewCancelled) {
    TKikimrRunner kikimr;
    CreateLargeTable(kikimr, 500000, 10, 100, 5000, 1);

    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    {
        TStringStream request;
        request << "SELECT * FROM `/Root/.sys/top_queries_by_read_bytes_one_hour` ORDER BY Duration";

        auto it = db.StreamExecuteScanQuery(request.Str()).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        ui64 rowsCount = 0;
        for (;;) {
            auto streamPart = it.ReadNext().GetValueSync();
            if (!streamPart.IsSuccess()) {
                UNIT_ASSERT_C(streamPart.EOS(), streamPart.GetIssues().ToString());
                break;
            }

            if (streamPart.HasResultSet()) {
                auto resultSet = streamPart.ExtractResultSet();

                NYdb::TResultSetParser parser(resultSet);
                while (parser.TryNextRow()) {
                    auto value = parser.ColumnParser("QueryText").GetOptionalUtf8();
                    UNIT_ASSERT(value);
                    rowsCount++;
                }
            }
        }
        UNIT_ASSERT(rowsCount == 1);
    }

    TStringStream cancelledRequest;
    cancelledRequest << "SELECT COUNT(*) FROM `/Root/LargeTable` WHERE SUBSTRING(DataText, 50, 5) = \"33333\"";
    auto prepareResult = session.PrepareDataQuery(cancelledRequest.Str()).GetValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(prepareResult.GetStatus(), NYdb::EStatus::SUCCESS, prepareResult.GetIssues().ToString());
    auto dataQuery = prepareResult.GetQuery();

    auto settings = TExecDataQuerySettings();
    settings.CancelAfter(TDuration::MilliSeconds(100));

    auto result = dataQuery.Execute(TTxControl::BeginTx().CommitTx(), settings).GetValueSync();

    result.GetIssues().PrintTo(Cerr);
    UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(),  NYdb::EStatus::CANCELLED);

    {
        TStringStream request;
        request << "SELECT * FROM `/Root/.sys/top_queries_by_read_bytes_one_hour` ORDER BY Duration";

        auto it = db.StreamExecuteScanQuery(request.Str()).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        ui64 queryCount = 0;
        ui64 rowsCount = 0;
        for (;;) {
            auto streamPart = it.ReadNext().GetValueSync();
            if (!streamPart.IsSuccess()) {
                UNIT_ASSERT_C(streamPart.EOS(), streamPart.GetIssues().ToString());
                break;
            }

            if (streamPart.HasResultSet()) {
                auto resultSet = streamPart.ExtractResultSet();

                NYdb::TResultSetParser parser(resultSet);
                while (parser.TryNextRow()) {
                    auto value = parser.ColumnParser("QueryText").GetOptionalUtf8();
                    UNIT_ASSERT(value);
                    if (*value == cancelledRequest.Str()) {
                        queryCount++;
                    }
                    rowsCount++;
                }
            }
        }

        UNIT_ASSERT(queryCount == 1);
        UNIT_ASSERT(rowsCount == 3);
    }
}

Y_UNIT_TEST(OneShardLocalExec) {
    TKikimrRunner kikimr;
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);
    {
        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/KeyValue` WHERE Key = 1;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), 2);
    }
    {
        auto result = session.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES (1, "1");
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), 3);
    }
    {
        auto result = kikimr.GetQueryClient().ExecuteQuery(R"(
            SELECT * FROM `/Root/KeyValue` WHERE Key = 1;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), 4);
    }
    {
        auto result = kikimr.GetQueryClient().ExecuteQuery(R"(
            UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES (1, "1");
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), 5);
    }
    UNIT_ASSERT_VALUES_EQUAL(counters.NonLocalSingleNodeReqCount->Val(), 0);
}

Y_UNIT_TEST(OneShardNonLocalExec) {
    TKikimrRunner kikimr(TKikimrSettings().SetNodeCount(2));
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();
    auto monPort = kikimr.GetTestServer().GetRuntime()->GetMonPort();

    auto firstNodeId = kikimr.GetTestServer().GetRuntime()->GetFirstNodeId();

    TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);

    auto expectedTotalSingleNodeReqCount = counters.TotalSingleNodeReqCount->Val();
    auto expectedNonLocalSingleNodeReqCount = counters.NonLocalSingleNodeReqCount->Val();

    auto drainNode = [monPort](size_t nodeId, bool undrain = false) {
        TNetworkAddress addr("localhost", monPort);
        TSocket s(addr);
        TString url;
        if (undrain) {
            url = "/tablets/app?TabletID=72057594037968897&node=" + std::to_string(nodeId) + "&page=SetDown&down=0";
        } else {
            url = "/tablets/app?TabletID=72057594037968897&node=" + std::to_string(nodeId) + "&page=DrainNode";
        }
        SendMinimalHttpRequest(s, "localhost", url);
        TSocketInput si(s);
        THttpInput input(&si);
        TString firstLine = input.FirstLine();

        const auto httpCode = ParseHttpRetCode(firstLine);
        UNIT_ASSERT_VALUES_EQUAL(httpCode, 200);
    };

    auto waitTablets = [&session](size_t nodeId) mutable {
        TDescribeTableSettings describeTableSettings =
            TDescribeTableSettings()
                .WithTableStatistics(true)
                .WithPartitionStatistics(true)
                .WithShardNodesInfo(true);

        bool done = false;
        for (int i = 0; i < 10; i++) {
            std::unordered_set<ui32> nodeIds;
            auto res = session.DescribeTable("Root/EightShard", describeTableSettings)
                .ExtractValueSync();

            UNIT_ASSERT_EQUAL(res.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(res.GetStatus(), EStatus::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(res.GetTableDescription().GetPartitionsCount(), 8);
            UNIT_ASSERT_VALUES_EQUAL(res.GetTableDescription().GetPartitionStats().size(), 8);
            for (const auto& s : res.GetTableDescription().GetPartitionStats()) {
                nodeIds.emplace(s.LeaderNodeId);
            }
            if (nodeIds.size() == 1 && *nodeIds.begin() == nodeId) {
                done = true;
                break;
            }
            Sleep(TDuration::Seconds(1));
        }
        UNIT_ASSERT_C(done, "unable to wait tablets move on specific node");
    };

    // Move all tablets on the node2, we have a grpc connection to node 1
    // so all sessions will be created on the node 1
    drainNode(firstNodeId);
    waitTablets(firstNodeId + 1);

    {
        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/EightShard` WHERE Key = 1;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), ++expectedTotalSingleNodeReqCount);
    }
    {
        auto result = session.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/EightShard` (Key, Data) VALUES (1, 1);
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), ++expectedTotalSingleNodeReqCount);
    }
    {
        auto result = kikimr.GetQueryClient().ExecuteQuery(R"(
            SELECT * FROM `/Root/EightShard` WHERE Key = 1;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), ++expectedTotalSingleNodeReqCount);
    }
    {
        auto result = kikimr.GetQueryClient().ExecuteQuery(R"(
            UPSERT INTO `/Root/EightShard` (Key, Data) VALUES (1, 1);
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), ++expectedTotalSingleNodeReqCount);
    }
    {
        auto result = session.ExecuteDataQuery(R"(
            UPDATE `/Root/EightShard` SET Data = 111 WHERE Key = 1;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), ++expectedTotalSingleNodeReqCount);
    }
    {
        auto result = kikimr.GetQueryClient().ExecuteQuery(R"(
            UPDATE `/Root/EightShard` SET Data = 111 WHERE Key = 1;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), ++expectedTotalSingleNodeReqCount);
    }
    expectedNonLocalSingleNodeReqCount += 6;
    UNIT_ASSERT_VALUES_EQUAL(counters.NonLocalSingleNodeReqCount->Val(), expectedNonLocalSingleNodeReqCount);

    // Now resume node 1 and move all tablets on the node1
    // so all tablets will be on the same node with session
    drainNode(firstNodeId, true);
    drainNode(firstNodeId + 1);
    waitTablets(firstNodeId);

    {
        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/EightShard` WHERE Key = 1;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), ++expectedTotalSingleNodeReqCount);
    }
    {
        auto result = session.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/EightShard` (Key, Data) VALUES (1, 1);
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), ++expectedTotalSingleNodeReqCount);
    }
    {
        auto result = kikimr.GetQueryClient().ExecuteQuery(R"(
            SELECT * FROM `/Root/EightShard` WHERE Key = 1;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), ++expectedTotalSingleNodeReqCount);
    }
    {
        auto result = kikimr.GetQueryClient().ExecuteQuery(R"(
            UPSERT INTO `/Root/EightShard` (Key, Data) VALUES (1, 1);
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), ++expectedTotalSingleNodeReqCount);
    }
    {
        auto result = session.ExecuteDataQuery(R"(
            UPDATE `/Root/EightShard` SET Data = 111 WHERE Key = 1;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), ++expectedTotalSingleNodeReqCount);
    }
    {
        auto result = kikimr.GetQueryClient().ExecuteQuery(R"(
            UPDATE `/Root/EightShard` SET Data = 111 WHERE Key = 1;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), ++expectedTotalSingleNodeReqCount);
    }
        {
        auto result = session.ExecuteDataQuery(R"(
            UPDATE `/Root/EightShard` SET Data = 111 WHERE Key = 1;
            SELECT * FROM `/Root/EightShard` WHERE Key = 1;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), ++expectedTotalSingleNodeReqCount);
    }
    {
        auto result = kikimr.GetQueryClient().ExecuteQuery(R"(
            UPDATE `/Root/EightShard` SET Data = 111 WHERE Key = 1;
            SELECT * FROM `/Root/EightShard` WHERE Key = 1;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(counters.TotalSingleNodeReqCount->Val(), ++expectedTotalSingleNodeReqCount);
    }
    // All executions are local - same value of counter
    UNIT_ASSERT_VALUES_EQUAL(counters.NonLocalSingleNodeReqCount->Val(), expectedNonLocalSingleNodeReqCount);
}

} // suite

} // namespace NKqp
} // namespace NKikimr

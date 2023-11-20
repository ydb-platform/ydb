#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/kqp/counters/kqp_counters.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

void CreateSampleTables(TKikimrRunner& kikimr) {
    kikimr.GetTestClient().CreateTable("/Root", R"(
        Name: "FourShard"
        Columns { Name: "Key", Type: "Uint64" }
        Columns { Name: "Value1", Type: "String" }
        Columns { Name: "Value2", Type: "String" }
        KeyColumnNames: ["Key"],
        SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 100 } } } }
        SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 200 } } } }
        SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 300 } } } }
    )");

    TTableClient tableClient{kikimr.GetDriver()};
    auto session = tableClient.CreateSession().GetValueSync().GetSession();

    auto result = session.ExecuteDataQuery(R"(
        REPLACE INTO `/Root/FourShard` (Key, Value1, Value2) VALUES
            (1u,   "Value-001",  "1"),
            (2u,   "Value-002",  "2"),
            (101u, "Value-101",  "101"),
            (102u, "Value-102",  "102"),
            (201u, "Value-201",  "201"),
            (202u, "Value-202",  "202"),
            (301u, "Value-301",  "301"),
            (302u, "Value-302",  "302")
    )", TTxControl::BeginTx().CommitTx()).GetValueSync();

    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    session.Close();
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(KqpFlowControl) {

void DoFlowControlTest(ui64 limit, bool hasBlockedByCapacity) {
    NKikimrConfig::TAppConfig appCfg;
    appCfg.MutableTableServiceConfig()->MutableResourceManager()->SetChannelBufferSize(limit);
    appCfg.MutableTableServiceConfig()->MutableResourceManager()->SetMinChannelBufferSize(limit);
    appCfg.MutableTableServiceConfig()->MutableResourceManager()->SetMkqlHeavyProgramMemoryLimit(200ul << 20);
    appCfg.MutableTableServiceConfig()->MutableResourceManager()->SetQueryMemoryLimit(20ul << 30);
    appCfg.MutableTableServiceConfig()->SetEnableKqpScanQueryStreamLookup(false);

    // TODO: KIKIMR-14294
    auto kikimrSettings = TKikimrSettings()
        .SetAppConfig(appCfg)
        .SetKqpSettings({});

    TKikimrRunner kikimr{kikimrSettings};
    // kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
    // kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPUTE, NActors::NLog::PRI_DEBUG);
    // kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_GATEWAY, NActors::NLog::PRI_DEBUG);
    // kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_RESOURCE_MANAGER, NActors::NLog::PRI_DEBUG);

    CreateSampleTables(kikimr);
    auto db = kikimr.GetTableClient();

    Y_DEFER {
        NYql::NDq::GetDqExecutionSettingsForTests().Reset();
    };

    NYql::NDq::GetDqExecutionSettingsForTests().FlowControl.MaxOutputChunkSize = limit;
    NYql::NDq::GetDqExecutionSettingsForTests().FlowControl.InFlightBytesOvercommit = 1.0f;

    TStreamExecScanQuerySettings settings;
    settings.CollectQueryStats(ECollectQueryStatsMode::Profile);

    auto it = db.StreamExecuteScanQuery(R"(
            $r = (select * from `/Root/FourShard` where Key > 201);

            SELECT l.Key as key, l.Text as text, r.Value1 as value
            FROM `/Root/EightShard` AS l JOIN $r AS r ON l.Key = r.Key
            ORDER BY key, text, value
        )", settings).GetValueSync();

    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

    auto res = CollectStreamResult(it);

    CompareYson(R"([
            [[202u];["Value2"];["Value-202"]];
            [[301u];["Value1"];["Value-301"]];
            [[302u];["Value2"];["Value-302"]]
        ])", res.ResultSetYson);

    NJson::TJsonValue plan;
    NJson::ReadJsonTree(*res.PlanJson, &plan, true);

    ui32 writesBlockedNoSpace = 0;
    auto nodes = FindPlanNodes(plan, "Pop.WaitTimeUs.Sum");
    for (auto& node : nodes) {
        writesBlockedNoSpace += node.GetIntegerSafe();
    }

    UNIT_ASSERT_EQUAL_C(hasBlockedByCapacity, writesBlockedNoSpace > 0, *res.PlanJson);
}

Y_UNIT_TEST(FlowControl_Unlimited) {
    DoFlowControlTest(100ul << 20, false);
}

Y_UNIT_TEST(FlowControl_BigLimit) {
    DoFlowControlTest(1ul << 10, false);
}

Y_UNIT_TEST(FlowControl_SmallLimit) {
    DoFlowControlTest(1ul, true);
}

//Y_UNIT_TEST(SlowClient) {
void SlowClient() {
    NKikimrConfig::TAppConfig appCfg;
    appCfg.MutableTableServiceConfig()->MutableResourceManager()->SetChannelBufferSize(1);

    TKikimrRunner kikimr(appCfg);

    {
        TTableClient tableClient{kikimr.GetDriver()};
        auto session = tableClient.CreateSession().GetValueSync().GetSession();
        auto value = std::string(1000, 'a');

        for (int q = 0; q < 100; ++q) {
            TStringBuilder query;
            query << "REPLACE INTO [/Root/KeyValue] (Key, Value) VALUES (" << q << ", \"" << value << "\")";

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
    }

    auto db = kikimr.GetTableClient();
    auto it = db.StreamExecuteScanQuery("SELECT Key, Value FROM `/Root/KeyValue`").GetValueSync();
    auto part = it.ReadNext().GetValueSync();

    auto counters = kikimr.GetTestServer().GetRuntime()->GetAppData(0).Counters;
    TKqpCounters kqpCounters(counters);

    UNIT_ASSERT_EQUAL(kqpCounters.RmComputeActors->Val(), 2);

    Cerr << "-- got value and go sleep...\n";
    ::Sleep(TDuration::Seconds(3));
    Cerr << "-- go on...\n";

    UNIT_ASSERT_EQUAL(kqpCounters.RmComputeActors->Val(), 2);

    // consume 990 elements
    int remains = 990;
    while (remains > 0) {
        if (part.HasResultSet()) {
            part.ExtractResultSet();
            --remains;
            ::Sleep(TDuration::MilliSeconds(10));
            Cerr << "-- remains: " << remains << Endl;
        }
        part = it.ReadNext().GetValueSync();
        UNIT_ASSERT(!part.EOS());
    }

    UNIT_ASSERT_EQUAL(kqpCounters.RmComputeActors->Val(), 2);

    while (!part.EOS()) {
        part = it.ReadNext().GetValueSync();
    }

    UNIT_ASSERT_EQUAL(kqpCounters.RmComputeActors->Val(), 0);
    UNIT_ASSERT_EQUAL(kqpCounters.RmMemory->Val(), 0);
}

} // suite

} // namespace NKikimr::NKqp

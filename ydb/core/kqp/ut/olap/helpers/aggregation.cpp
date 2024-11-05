#include "aggregation.h"
#include <ydb/core/kqp/common/simple/kqp_event_ids.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/datashard/datashard_ut_common_kqp.h>

namespace NKikimr::NKqp {

void TestAggregationsBase(const std::vector<TAggregationTestCase>& cases) {
    auto settings = TKikimrSettings()
        .SetWithSampleTables(false);
    TKikimrRunner kikimr(settings);

    TLocalHelper(kikimr).CreateTestOlapTable();
    auto tableClient = kikimr.GetTableClient();
    Tests::NCommon::TLoggerInit(kikimr).SetComponents({ NKikimrServices::GROUPED_MEMORY_LIMITER, NKikimrServices::TX_COLUMNSHARD_SCAN }, "CS").Initialize();

    {
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 1000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 11000, 3001000, 1000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 12000, 3002000, 1000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 13000, 3003000, 1000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 14000, 3004000, 1000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 20000, 2000000, 7000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 30000, 1000000, 11000);
    }

    for (auto&& i : cases) {
        const TString queryFixed = i.GetFixedQuery();
        {
            auto it = tableClient.StreamExecuteScanQuery(queryFixed).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            if (!i.GetExpectedReply().empty()) {
                CompareYson(result, i.GetExpectedReply());
            }
        }
        CheckPlanForAggregatePushdown(queryFixed, tableClient, i.GetExpectedPlanOptions(), i.GetExpectedReadNodeType());
    }
}

void TestAggregationsInternal(const std::vector<TAggregationTestCase>& cases) {
    TPortManager tp;
    ui16 mbusport = tp.GetPort(2134);
    auto settings = Tests::TServerSettings(mbusport)
        .SetDomainName("Root")
        .SetUseRealThreads(false)
        .SetNodeCount(2);

    Tests::TServer::TPtr server = new Tests::TServer(settings);

    auto runtime = server->GetRuntime();
    Tests::NCommon::TLoggerInit(runtime).Initialize();
    Tests::NCommon::TLoggerInit(runtime).SetComponents({ NKikimrServices::GROUPED_MEMORY_LIMITER }, "CS").Initialize();
    auto sender = runtime->AllocateEdgeActor();

    InitRoot(server, sender);

    ui32 numShards = 1;
    ui32 numIterations = 10;
    TLocalHelper(*server).CreateTestOlapTable("olapTable", "olapStore", numShards, numShards);
    const ui32 iterationPackSize = 2000;
    for (ui64 i = 0; i < numIterations; ++i) {
        TLocalHelper(*server).SendDataViaActorSystem("/Root/olapStore/olapTable", 0, 1000000 + i * 1000000, iterationPackSize);
    }

    TAggregationTestCase currentTest;
    auto captureEvents = [&](TAutoPtr<IEventHandle>& ev) -> auto {
        switch (ev->GetTypeRewrite()) {
            case NKqp::TKqpComputeEvents::EvScanData:
            {
                auto* msg = ev->Get<NKqp::TEvKqpCompute::TEvScanData>();
                Y_ABORT_UNLESS(currentTest.MutableLimitChecker().CheckExpectedLimitOnScanData(msg->ArrowBatch ? msg->ArrowBatch->num_rows() : 0));
                Y_ABORT_UNLESS(currentTest.MutableRecordChecker().CheckExpectedOnScanData(msg->ArrowBatch ? msg->ArrowBatch->num_columns() : 0));
                break;
            }
            case TEvDataShard::EvKqpScan:
            {
                auto* msg = ev->Get<TEvDataShard::TEvKqpScan>();
                Y_ABORT_UNLESS(currentTest.MutableLimitChecker().CheckExpectedLimitOnScanTask(msg->Record.GetItemsLimit()));
                break;
            }
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    runtime->SetObserverFunc(captureEvents);

    for (auto&& i : cases) {
        const TString queryFixed = i.GetFixedQuery();
        currentTest = i;
        auto streamSender = runtime->AllocateEdgeActor();
        NDataShard::NKqpHelpers::SendRequest(*runtime, streamSender, NDataShard::NKqpHelpers::MakeStreamRequest(streamSender, queryFixed, false));
        auto ev = runtime->GrabEdgeEventRethrow<NKqp::TEvKqpCompute::TEvScanData>(streamSender, TDuration::Seconds(10));
        Y_ABORT_UNLESS(currentTest.CheckFinished());
    }
}

void WriteTestDataForTableWithNulls(TKikimrRunner& kikimr, TString testTable) {
    UNIT_ASSERT(testTable == "/Root/tableWithNulls"); // TODO: check schema instead
    Tests::NCS::TTableWithNullsHelper lHelper(kikimr.GetTestServer());
    auto batch = lHelper.TestArrowBatch();
    lHelper.SendDataViaActorSystem(testTable, batch);
}

void TestTableWithNulls(const std::vector<TAggregationTestCase>& cases, const bool genericQuery /*= false*/) {
    auto settings = TKikimrSettings()
        .SetWithSampleTables(false);
    TKikimrRunner kikimr(settings);

    Tests::NCommon::TLoggerInit(kikimr).Initialize();
    TTableWithNullsHelper(kikimr).CreateTableWithNulls();
    auto tableClient = kikimr.GetTableClient();

    {
        WriteTestDataForTableWithNulls(kikimr, "/Root/tableWithNulls");
    }

    if (!genericQuery) {
        auto tableClient = kikimr.GetTableClient();
        for (auto&& i : cases) {
            RunTestCaseWithClient(i, tableClient);
            CheckPlanForAggregatePushdown(i.GetFixedQuery(), tableClient,
                i.GetExpectedPlanOptions(), i.GetExpectedReadNodeType());
        }
    } else {
        auto queryClient = kikimr.GetQueryClient();
        for (auto&& i : cases) {
            RunTestCaseWithClient(i, queryClient);
            CheckPlanForAggregatePushdown(i.GetFixedQuery(), queryClient,
                i.GetExpectedPlanOptions(), i.GetExpectedReadNodeType());
        }
    }
}

void TestAggregations(const std::vector<TAggregationTestCase>& cases) {
    TestAggregationsBase(cases);
    TestAggregationsInternal(cases);
}

}
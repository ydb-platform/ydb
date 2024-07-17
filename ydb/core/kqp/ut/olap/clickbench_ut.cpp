#include "helpers/aggregation.h"

#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/testlib/cs_helper.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/datashard/datashard_ut_common_kqp.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpOlapClickbench) {

    class TClickHelper : public Tests::NCS::TCickBenchHelper {
    private:
        using TBase = Tests::NCS::TCickBenchHelper;
    public:
        using TBase::TBase;

        TClickHelper(TKikimrRunner& runner)
            : TBase(runner.GetTestServer())
        {}

        void CreateClickBenchTable(TString tableName = "benchTable", ui32 shardsCount = 4) {
            TActorId sender = Server.GetRuntime()->AllocateEdgeActor();

            TBase::CreateTestOlapTable(sender, "", Sprintf(R"(
                Name: "%s"
                ColumnShardCount: %d
                Schema {
                    %s
                }
                Sharding {
                    HashSharding {
                        Function: HASH_FUNCTION_CONSISTENCY_64
                        Columns: "EventTime"
                    }
                })", tableName.c_str(), shardsCount, PROTO_SCHEMA));
        }
    };

    void WriteTestDataForClickBench(TKikimrRunner& kikimr, TString testTable, ui64 pathIdBegin, ui64 tsBegin, size_t rowCount) {
        UNIT_ASSERT(testTable == "/Root/benchTable"); // TODO: check schema instead
        TClickHelper lHelper(kikimr.GetTestServer());
        auto batch = lHelper.TestArrowBatch(pathIdBegin, tsBegin, rowCount);
        lHelper.SendDataViaActorSystem(testTable, batch);
    }

    void TestClickBenchBase(const std::vector<TAggregationTestCase>& cases, const bool genericQuery) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TClickHelper(kikimr).CreateClickBenchTable();
        auto tableClient = kikimr.GetTableClient();


        ui32 numIterations = 10;
        const ui32 iterationPackSize = NSan::PlainOrUnderSanitizer(2000, 20);
        for (ui64 i = 0; i < numIterations; ++i) {
            WriteTestDataForClickBench(kikimr, "/Root/benchTable", 0, 1000000 + i * 1000000, iterationPackSize);
        }

        if (!genericQuery) {
            auto tableClient = kikimr.GetTableClient();
            for (auto&& i : cases) {
                const TString queryFixed = i.GetFixedQuery();
                RunTestCaseWithClient(i, tableClient);
                CheckPlanForAggregatePushdown(queryFixed, tableClient, i.GetExpectedPlanOptions(), i.GetExpectedReadNodeType());
            }
        } else {
            auto queryClient = kikimr.GetQueryClient();
            for (auto&& i : cases) {
                const TString queryFixed = i.GetFixedQuery();
                RunTestCaseWithClient(i, queryClient);
                CheckPlanForAggregatePushdown(queryFixed, queryClient, i.GetExpectedPlanOptions(), i.GetExpectedReadNodeType());
            }
        }
    }

    void TestClickBenchInternal(const std::vector<TAggregationTestCase>& cases) {
        TPortManager tp;
        ui16 mbusport = tp.GetPort(2134);
        auto settings = Tests::TServerSettings(mbusport)
            .SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetNodeCount(2);

        Tests::TServer::TPtr server = new Tests::TServer(settings);

        auto runtime = server->GetRuntime();
        auto sender = runtime->AllocateEdgeActor();

        InitRoot(server, sender);

        TClickHelper(*server).CreateClickBenchTable();

        // write data

        ui32 numIterations = 10;
        const ui32 iterationPackSize = NSan::PlainOrUnderSanitizer(2000, 20);
        for (ui64 i = 0; i < numIterations; ++i) {
            TClickHelper(*server).SendDataViaActorSystem("/Root/benchTable", 0, 1000000 + i * 1000000,
                                                         iterationPackSize);
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

        // selects

        for (auto&& i : cases) {
            const TString queryFixed = i.GetFixedQuery();
            currentTest = i;
            auto streamSender = runtime->AllocateEdgeActor();
            NDataShard::NKqpHelpers::SendRequest(*runtime, streamSender, NDataShard::NKqpHelpers::MakeStreamRequest(streamSender, queryFixed, false));
            auto ev = runtime->GrabEdgeEventRethrow<NKqp::TEvKqpCompute::TEvScanData>(streamSender, TDuration::Seconds(10));
            Y_ABORT_UNLESS(currentTest.CheckFinished());
        }
    }

    void TestClickBench(const std::vector<TAggregationTestCase>& cases, const bool genericQuery = false) {
        TestClickBenchBase(cases, genericQuery);
        if (!genericQuery) {
            TestClickBenchInternal(cases);
        }
    }

    Y_UNIT_TEST(ClickBenchSmoke) {
        TAggregationTestCase q7;
        q7.SetQuery(R"(
                SELECT
                    AdvEngineID, COUNT(*) as c
                FROM `/Root/benchTable`
                WHERE AdvEngineID != 0
                GROUP BY AdvEngineID
                ORDER BY c DESC
            )")
            //.SetExpectedReply("[[[\"40999\"];[4];1u];[[\"40998\"];[3];1u];[[\"40997\"];[2];1u]]")
            // Should be fixed in https://st.yandex-team.ru/KIKIMR-17009
            // .SetExpectedReadNodeType("TableFullScan");
            .SetExpectedReadNodeType("Aggregate-TableFullScan");
        q7.FillExpectedAggregationGroupByPlanOptions();

        TAggregationTestCase q9;
        q9.SetQuery(R"(
                SELECT
                    RegionID, SUM(AdvEngineID), COUNT(*) AS c, avg(ResolutionWidth), COUNT(DISTINCT UserID)
                FROM `/Root/benchTable`
                GROUP BY RegionID
                ORDER BY c DESC
                LIMIT 10
            )")
            //.SetExpectedReply("[[[\"40999\"];[4];1u];[[\"40998\"];[3];1u];[[\"40997\"];[2];1u]]")
            .SetExpectedReadNodeType("TableFullScan");
            // .SetExpectedReadNodeType("Aggregate-TableFullScan");
        q9.FillExpectedAggregationGroupByPlanOptions();

        TAggregationTestCase q12;
        q12.SetQuery(R"(
                SELECT
                    SearchPhrase, count(*) AS c
                FROM `/Root/benchTable`
                WHERE SearchPhrase != ''
                GROUP BY SearchPhrase
                ORDER BY c DESC
                LIMIT 10;
            )")
            //.SetExpectedReply("[[[\"40999\"];[4];1u];[[\"40998\"];[3];1u];[[\"40997\"];[2];1u]]")
            // Should be fixed in https://st.yandex-team.ru/KIKIMR-17009
            // .SetExpectedReadNodeType("TableFullScan");
            .SetExpectedReadNodeType("Aggregate-TableFullScan");
        q12.FillExpectedAggregationGroupByPlanOptions();

        TAggregationTestCase q14;
        q14.SetQuery(R"(
                SELECT
                    SearchEngineID, SearchPhrase, count(*) AS c
                FROM `/Root/benchTable`
                WHERE SearchPhrase != ''
                GROUP BY SearchEngineID, SearchPhrase
                ORDER BY c DESC
                LIMIT 10;
            )")
            //.SetExpectedReply("[[[\"40999\"];[4];1u];[[\"40998\"];[3];1u];[[\"40997\"];[2];1u]]")
            // Should be fixed in https://st.yandex-team.ru/KIKIMR-17009
            // .SetExpectedReadNodeType("TableFullScan");
            .SetExpectedReadNodeType("Aggregate-TableFullScan");
        q14.FillExpectedAggregationGroupByPlanOptions();

        TAggregationTestCase q22;
        q22.SetQuery(R"(
                SELECT
                    SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID)
                FROM `/Root/benchTable`
                WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> ''
                GROUP BY SearchPhrase
                ORDER BY c DESC
                LIMIT 10;
            )")
            .AddExpectedPlanOptions("KqpOlapFilter")
            .SetExpectedReadNodeType("TableFullScan");
        q22.FillExpectedAggregationGroupByPlanOptions();

        TAggregationTestCase q39;
        q39.SetQuery(R"(
                SELECT TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst, COUNT(*) AS PageViews
                FROM `/Root/benchTable`
                WHERE CounterID = 62 AND EventDate >= Date('2013-07-01') AND EventDate <= Date('2013-07-31') AND IsRefresh == 0
                GROUP BY
                    TraficSourceID, SearchEngineID, AdvEngineID, IF (SearchEngineID = 0 AND AdvEngineID = 0, Referer, '') AS Src,
                    URL AS Dst
                ORDER BY PageViews DESC
                LIMIT 10;
            )")
            .AddExpectedPlanOptions("KqpOlapFilter")
            .SetExpectedReadNodeType("Aggregate-Filter-TableFullScan");
        q39.FillExpectedAggregationGroupByPlanOptions();

        std::vector<TAggregationTestCase> cases = {q7, q9, q12, q14, q22, q39};
        for (auto&& c : cases) {
            c.SetUseLlvm(NSan::PlainOrUnderSanitizer(true, false));
        }

        TestClickBench(cases);
    }
}

}

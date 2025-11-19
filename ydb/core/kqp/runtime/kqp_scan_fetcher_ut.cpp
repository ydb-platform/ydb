#include <ydb/core/kqp/compute_actor/kqp_scan_fetcher_actor.h>

#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/actors/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/accessor/positive_integer.h>

Y_UNIT_TEST_SUITE(TKqpScanFetcher) {

    Y_UNIT_TEST(ScanDelayedRetry) {

        constexpr ui64 TABLET_ID = 1001001;

        NActors::TTestActorRuntime runtime;
        runtime.Initialize(NKikimr::TAppPrepare().Unwrap());

        auto pipeCache = runtime.AllocateEdgeActor();
        runtime.RegisterService(NKikimr::MakePipePerNodeCacheID(false), pipeCache);
        auto scan = runtime.AllocateEdgeActor();
        auto compute = runtime.AllocateEdgeActor();

        NKikimrKqp::TKqpSnapshot snapshot;
        NYql::NDq::TComputeRuntimeSettings settings;
        NKikimrTxDataShard::TKqpTransaction_TScanTaskMeta meta;
        auto& read = *meta.AddReads();
        read.SetShardId(TABLET_ID);
        read.AddKeyRanges();
        NKikimrConfig::TTableServiceConfig::TShardsScanningPolicy protoPolicy;
        NKikimr::NKqp::TShardsScanningPolicy shardsScanningPolicy(protoPolicy);
        NWilson::TTraceId traceId(0);
        NKikimr::NKqp::TCPULimits cpuLimits;
        NMonitoring::TDynamicCounterPtr counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto scanFetcher = runtime.Register(CreateKqpScanFetcher(snapshot, { compute }, meta, settings,
            0, TMaybe<ui64>(), 0, TMaybe<NKikimrDataEvents::ELockMode>(), shardsScanningPolicy,
            MakeIntrusive<NKikimr::NKqp::TKqpCounters>(counters), 0, cpuLimits)
        );
        runtime.EnableScheduleForActor(scanFetcher, true);

        NKikimr::TPositiveIncreasingControlInteger controlGeneration;
        // 1. Simulate fail
        {
            auto event = runtime.GrabEdgeEvent<NKikimr::TEvPipeCache::TEvForward>(TSet<NActors::TActorId>{pipeCache});
            NKikimr::TEvDataShard::TEvKqpScan* evScan = dynamic_cast<NKikimr::TEvDataShard::TEvKqpScan*>(event->Get()->Ev.get());
            controlGeneration = NKikimr::TPositiveIncreasingControlInteger(evScan->Record.GetGeneration());
        }
        runtime.Send(scanFetcher, pipeCache, new NKikimr::TEvPipeCache::TEvDeliveryProblem(TABLET_ID, false));

        // 2. First fail is retried instantly, so fail again
        {
            auto event = runtime.GrabEdgeEvent<NKikimr::TEvPipeCache::TEvForward>(TSet<NActors::TActorId>{pipeCache});
            NKikimr::TEvDataShard::TEvKqpScan* evScan = dynamic_cast<NKikimr::TEvDataShard::TEvKqpScan*>(event->Get()->Ev.get());
            controlGeneration = NKikimr::TPositiveIncreasingControlInteger(evScan->Record.GetGeneration());
        }
        runtime.Send(scanFetcher, pipeCache, new NKikimr::TEvPipeCache::TEvDeliveryProblem(TABLET_ID, false));

        // 3. Now we have 250ms until next retry, send late reply
        runtime.Send(scanFetcher, scan, new NKikimr::NKqp::TEvKqpCompute::TEvScanInitActor(0, scan, 2, TABLET_ID, true));

        // 4. Check for Fetcher failure
        {
            auto event = runtime.GrabEdgeEvent<NKikimr::NKqp::NScanPrivate::TEvScanExchange::TEvTerminateFromFetcher>(TSet<NActors::TActorId>{compute}, TDuration::Seconds(1));
            UNIT_ASSERT_C(!event, "Unexpected TEvTerminateFromFetcher");
        }

        // 5. Yet another retry
        {
            auto event = runtime.GrabEdgeEvent<NKikimr::TEvPipeCache::TEvForward>(TSet<NActors::TActorId>{pipeCache});
            NKikimr::TEvDataShard::TEvKqpScan* evScan = dynamic_cast<NKikimr::TEvDataShard::TEvKqpScan*>(event->Get()->Ev.get());
            controlGeneration = NKikimr::TPositiveIncreasingControlInteger(evScan->Record.GetGeneration());
        }
        runtime.Send(scanFetcher, scan, new NKikimr::NKqp::TEvKqpCompute::TEvScanInitActor(0, scan, 3, TABLET_ID, true));
        {
            auto event = runtime.GrabEdgeEvent<NKikimr::NKqp::TEvKqpCompute::TEvScanDataAck>(TSet<NActors::TActorId>{scan});
            Y_UNUSED(event);
        }
    }
}

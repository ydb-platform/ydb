#include <ydb/core/kqp/compute_actor/kqp_scan_fetcher_actor.h>

#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/actors/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/accessor/positive_integer.h>

Y_UNIT_TEST_SUITE(TKqpScanFetcher) {

    Y_UNIT_TEST(NodeDisconnectAbortsScannersAndPassAway) {
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
        auto scanFetcher = runtime.Register(CreateKqpScanFetcher(snapshot, { compute }, meta, settings, "/Root",
            0, TMaybe<ui64>(), 0, TMaybe<NKikimrDataEvents::ELockMode>(), shardsScanningPolicy,
            MakeIntrusive<NKikimr::NKqp::TKqpCounters>(counters), 0, cpuLimits)
        );
        runtime.EnableScheduleForActor(scanFetcher, true);

        // 1. Wait for the scan request to be forwarded via pipe cache
        {
            auto event = runtime.GrabEdgeEvent<NKikimr::TEvPipeCache::TEvForward>(TSet<NActors::TActorId>{pipeCache}, TDuration::Seconds(1));
            UNIT_ASSERT(event);
        }

        // 2. Simulate scan init from datashard
        runtime.Send(scanFetcher, scan, new NKikimr::NKqp::TEvKqpCompute::TEvScanInitActor(0, scan, 1, TABLET_ID, true));

        // 3. Simulate node disconnect
        runtime.Send(scanFetcher, runtime.AllocateEdgeActor(),
            new NActors::TEvInterconnect::TEvNodeDisconnected(scan.NodeId()));

        // 4. Verify that fetcher sent TEvTerminateFromFetcher to compute actor
        {
            auto event = runtime.GrabEdgeEvent<NKikimr::NKqp::NScanPrivate::TEvScanExchange::TEvTerminateFromFetcher>(
                TSet<NActors::TActorId>{compute}, TDuration::Seconds(1));
            UNIT_ASSERT_C(event, "Expected TEvTerminateFromFetcher to be sent to compute actor on node disconnect");
        }

        // 5. Verify that fetcher sent TEvAbortExecution to the scanner (from AbortAllScanners)
        {
            auto event = runtime.GrabEdgeEvent<NKikimr::NKqp::TEvKqp::TEvAbortExecution>(
                TSet<NActors::TActorId>{scan}, TDuration::Seconds(1));
            UNIT_ASSERT_C(event, "Expected TEvAbortExecution to be sent to scanner on node disconnect");
        }

        // 6. Verify that the fetcher actor is dead (PassAway was called)
        UNIT_ASSERT_C(runtime.FindActor(scanFetcher) == nullptr,
            "Expected scan fetcher actor to be dead after node disconnect");
    }

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
        NKikimr::NKqp::TShardsScanningPolicy shardsScanningPolicy;
        NWilson::TTraceId traceId(0);
        NKikimr::NKqp::TCPULimits cpuLimits;
        NMonitoring::TDynamicCounterPtr counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto scanFetcher = runtime.Register(CreateKqpScanFetcher(snapshot, { compute }, meta, settings, "/Root",
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

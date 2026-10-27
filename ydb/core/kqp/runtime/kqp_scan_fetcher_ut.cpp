#include <ydb/core/kqp/compute_actor/kqp_scan_fetcher_actor.h>

#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/actors/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/accessor/positive_integer.h>
#include <ydb/library/testlib/helpers.h>

Y_UNIT_TEST_SUITE(TKqpScanFetcher) {

    Y_UNIT_TEST(SchemeCacheUsesDatabasePathNotSchedulerDatabaseId) {
        using namespace NKikimr;
        using namespace NKikimr::NKqp;
        NActors::TTestActorRuntime runtime;
        runtime.Initialize(TAppPrepare().Unwrap());
        const auto pipeCache = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakePipePerNodeCacheID(false), pipeCache);
        const auto schemeCache = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeSchemeCacheID(), schemeCache);
        const auto compute = runtime.AllocateEdgeActor();
        NKikimrKqp::TKqpSnapshot snapshot;
        NYql::NDq::TComputeRuntimeSettings settings;
        NKikimrTxDataShard::TKqpTransaction_TScanTaskMeta meta;
        auto& read = *meta.AddReads();
        read.SetShardId(1001001);
        read.AddKeyRanges();
        TShardsScanningPolicy policy(256, 1024, 3, false, 5, 20, 0);
        const auto fetcher = runtime.Register(CreateKqpScanFetcher(snapshot, {compute}, meta, settings,
            "/Root/database-path", NScheduler::NHdrf::TFullPoolId{"scheduler-id", "pool"},
            42, {}, 0, {}, policy, MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()), {}, TCPULimits()));
        runtime.EnableScheduleForActor(fetcher, true);
        runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(pipeCache);
        runtime.Send(fetcher, pipeCache, new TEvPipeCache::TEvDeliveryProblem(1001001, false));
        const auto resolve = runtime.GrabEdgeEvent<TEvTxProxySchemeCache::TEvResolveKeySet>(schemeCache);
        UNIT_ASSERT_VALUES_EQUAL(resolve->Get()->Request->DatabaseName, "/Root/database-path");
    }

    Y_UNIT_TEST_TWIN(ScanDelayedRetry, Managed) {

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
        using namespace NKikimr::NKqp;
        std::optional<NScheduler::NHdrf::TFullPoolId> schedulerPool;
        if (Managed) {
            schedulerPool = NScheduler::NHdrf::TFullPoolId{"actual-database-id", "actual-pool"};
        }
        const auto checkCredentials = [&](const auto& record) {
            UNIT_ASSERT(record.HasTxId());
            UNIT_ASSERT_VALUES_EQUAL(record.GetTxId(), 0);
            UNIT_ASSERT_VALUES_EQUAL(record.HasDatabaseId(), Managed);
            UNIT_ASSERT_VALUES_EQUAL(record.HasPoolId(), Managed);
            if (Managed) {
                UNIT_ASSERT_VALUES_EQUAL(record.GetDatabaseId(), "actual-database-id");
                UNIT_ASSERT_VALUES_EQUAL(record.GetPoolId(), "actual-pool");
            }
        };
        auto scanFetcher = runtime.Register(CreateKqpScanFetcher(snapshot, { compute }, meta, settings, "/Root", schedulerPool,
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
            checkCredentials(evScan->Record);
        }
        runtime.Send(scanFetcher, pipeCache, new NKikimr::TEvPipeCache::TEvDeliveryProblem(TABLET_ID, false));

        // 2. First fail is retried instantly, so fail again
        {
            auto event = runtime.GrabEdgeEvent<NKikimr::TEvPipeCache::TEvForward>(TSet<NActors::TActorId>{pipeCache});
            NKikimr::TEvDataShard::TEvKqpScan* evScan = dynamic_cast<NKikimr::TEvDataShard::TEvKqpScan*>(event->Get()->Ev.get());
            controlGeneration = NKikimr::TPositiveIncreasingControlInteger(evScan->Record.GetGeneration());
            checkCredentials(evScan->Record);
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
            checkCredentials(evScan->Record);
        }
        runtime.Send(scanFetcher, scan, new NKikimr::NKqp::TEvKqpCompute::TEvScanInitActor(0, scan, 3, TABLET_ID, true));
        {
            auto event = runtime.GrabEdgeEvent<NKikimr::NKqp::TEvKqpCompute::TEvScanDataAck>(TSet<NActors::TActorId>{scan});
            Y_UNUSED(event);
        }
    }
}

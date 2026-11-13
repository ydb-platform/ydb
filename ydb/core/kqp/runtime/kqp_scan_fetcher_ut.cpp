#include <ydb/core/kqp/compute_actor/kqp_scan_fetcher_actor.h>

#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/actors/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/accessor/positive_integer.h>
#include <ydb/library/testlib/helpers.h>

#include <arrow/api.h>

namespace {

std::shared_ptr<const NKikimr::NColumnShardStats::TReport> MakeColumnShardReport() {
    auto report = std::make_shared<NKikimr::NColumnShardStats::TReport>();
    report->MutableIdentity()->SetTabletId(1001001);
    report->MutableIdentity()->SetScanAttemptId("scan-attempt");
    auto* operation = report->AddOperations();
    operation->SetKey("Filter");
    operation->SetName("Filter");
    operation->SetCallsCount(1);
    operation->MutableStatistics()->MutableOutputRows()->SetValue(0);
    operation->MutableUnfinished()->SetCallsCount(2);
    auto* source = report->AddDataSources();
    source->SetStorageId("default");
    source->SetReads(1);
    return report;
}

}

Y_UNIT_TEST_SUITE(TKqpScanFetcher) {

    Y_UNIT_TEST_TWIN(EmptyFinalBatchSerialization, WithColumnShardStats) {
        NKikimr::NKqp::TEvKqpCompute::TEvScanData data(7, 3);
        data.Finished = true;
        if (WithColumnShardStats) {
            data.ColumnShardStats = MakeColumnShardReport();
        }
        auto serializer = MakeHolder<NActors::TAllocChunkSerializer>();
        UNIT_ASSERT(data.SerializeToArcadiaStream(serializer.Get()));
        auto buffers = serializer->Release(data.CreateSerializationInfo(false));
        UNIT_ASSERT_VALUES_EQUAL(buffers->GetSize(), data.CalculateSerializedSize());
        const auto restored = THolder<NKikimr::NKqp::TEvKqpCompute::TEvScanData>(
            NKikimr::NKqp::TEvKqpCompute::TEvScanData::Load(buffers.Get()));
        UNIT_ASSERT(restored->Finished);
        UNIT_ASSERT(restored->IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(restored->ScanId, data.ScanId);
        UNIT_ASSERT_VALUES_EQUAL(restored->Generation, data.Generation);
        UNIT_ASSERT_VALUES_EQUAL(bool(restored->ColumnShardStats), WithColumnShardStats);
        if (WithColumnShardStats) {
            UNIT_ASSERT_VALUES_EQUAL(restored->ColumnShardStats->SerializeAsString(),
                data.ColumnShardStats->SerializeAsString());
        }
    }

    Y_UNIT_TEST_TWIN(ColumnShardReportForwardedOnce, SplitBatch) {
        using namespace NKikimr;
        using namespace NKikimr::NKqp;
        using namespace NKikimr::NKqp::NScanPrivate;
        NActors::TTestActorRuntime runtime;
        runtime.Initialize(TAppPrepare().Unwrap());
        const auto pipeCache = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakePipePerNodeCacheID(false), pipeCache);
        const auto scan = runtime.AllocateEdgeActor();
        const std::vector<NActors::TActorId> computes = {
            runtime.AllocateEdgeActor(), runtime.AllocateEdgeActor()};
        NKikimrKqp::TKqpSnapshot snapshot;
        NYql::NDq::TComputeRuntimeSettings settings;
        NKikimrTxDataShard::TKqpTransaction_TScanTaskMeta meta;
        auto& read = *meta.AddReads();
        read.SetShardId(1001001);
        read.AddKeyRanges();
        const auto fetcher = runtime.Register(CreateKqpScanFetcher(snapshot, {computes[0], computes[1]}, meta, settings,
            "/Root", {}, 42, {}, 0, {}, TShardsScanningPolicy(),
            MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()), {}, TCPULimits()));
        runtime.EnableScheduleForActor(fetcher, true);
        const auto forwarded = runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(pipeCache);
        const auto* request = dynamic_cast<TEvDataShard::TEvKqpScan*>(forwarded->Get()->Ev.get());
        UNIT_ASSERT(request);
        const auto scanId = request->Record.GetScanId();
        const auto generation = request->Record.GetGeneration();
        for (const auto& compute : computes) {
            runtime.Send(fetcher, compute, new TEvScanExchange::TEvAckData(1 << 20));
        }
        runtime.Send(fetcher, scan, new TEvKqpCompute::TEvScanInitActor(scanId, scan, generation, 1001001));
        runtime.GrabEdgeEvent<TEvKqpCompute::TEvScanDataAck>(scan);

        auto data = MakeHolder<TEvKqpCompute::TEvScanData>(scanId, generation);
        data->Finished = true;
        const auto report = MakeColumnShardReport();
        data->ColumnShardStats = report;
        if (SplitBatch) {
            data->ArrowBatch = arrow::Table::Make(arrow::schema({arrow::field("value", arrow::null())}),
                std::vector<std::shared_ptr<arrow::Array>>{std::make_shared<arrow::NullArray>(2)});
            data->SplittedBatches = {{0}, {1}};
        }
        runtime.Send(fetcher, scan, data.Release());
        ui32 reports = 0;
        ui32 rows = 0;
        const TSet<NActors::TActorId> recipients(computes.begin(), computes.end());
        for (ui32 part = 0; part < (SplitBatch ? 2u : 1u); ++part) {
            const auto event = runtime.GrabEdgeEvent<TEvScanExchange::TEvSendData>(recipients);
            const auto& message = *event->Get();
            UNIT_ASSERT(message.GetFinished());
            UNIT_ASSERT_VALUES_EQUAL(message.GetTabletId(), 1001001);
            rows += SplitBatch ? message.GetDataIndexes().size() : message.GetRowsCount();
            if (message.GetColumnShardStats()) {
                ++reports;
                UNIT_ASSERT_VALUES_EQUAL(message.GetColumnShardStats()->SerializeAsString(), report->SerializeAsString());
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(reports, 1);
        UNIT_ASSERT_VALUES_EQUAL(rows, SplitBatch ? 2u : 0u);
    }

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

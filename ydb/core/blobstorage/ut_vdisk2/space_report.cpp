#include "env.h"

#include <ydb/core/blobstorage/vdisk/huge/blobstorage_hullhuge.h>

using namespace NKikimr;

namespace {

    ui64 SumBreakdown(const NKikimrVDisk::TVDiskSpaceBreakdown& breakdown) {
        return breakdown.GetUsefulBlobDataBytes()
            + breakdown.GetLiveMetadataBytes()
            + breakdown.GetLiveAuxiliaryDataBytes()
            + breakdown.GetGcDeadBlobDataBytes()
            + breakdown.GetGcDeadMetadataBytes()
            + breakdown.GetMergeRedundantBlobDataBytes()
            + breakdown.GetMergeRedundantMetadataBytes()
            + breakdown.GetWritePaddingBytes()
            + breakdown.GetSlotInternalFragmentationBytes()
            + breakdown.GetFreeSlotBytes()
            + breakdown.GetChunkTailBytes()
            + breakdown.GetFreeChunkReserveBytes()
            + breakdown.GetLockedOrQuarantinedBytes()
            + breakdown.GetUnclassifiedBytes()
            + breakdown.GetFreeStripeBytes();
    }

    void AssertComponentAllocation(
            const NKikimrVDisk::TVDiskSpaceComponent& component,
            ui64 chunkSize) {
        UNIT_ASSERT_VALUES_EQUAL(
            component.GetAllocatedBytes(),
            component.GetChunkCount() * chunkSize + component.GetStripedBytes());
    }

    void AssertReportIsConsistent(const NKikimrVDisk::TVDiskSpaceReport& report) {
        UNIT_ASSERT_VALUES_EQUAL(
            report.GetPDiskAllocatedBytes(),
            report.GetPDiskAllocatedChunks() * report.GetChunkSizeBytes());
        UNIT_ASSERT_VALUES_EQUAL(report.GetAccountedBytes(), SumBreakdown(report.GetTotal()));

        AssertComponentAllocation(report.GetLogoBlobs(), report.GetChunkSizeBytes());
        AssertComponentAllocation(report.GetBlocks(), report.GetChunkSizeBytes());
        AssertComponentAllocation(report.GetBarriers(), report.GetChunkSizeBytes());
        AssertComponentAllocation(report.GetHuge().GetTotal(), report.GetChunkSizeBytes());
        AssertComponentAllocation(report.GetSyncLog(), report.GetChunkSizeBytes());
        for (const auto& chunkKeeper : report.GetChunkKeeper()) {
            AssertComponentAllocation(chunkKeeper.GetTotal(), report.GetChunkSizeBytes());
        }
        AssertComponentAllocation(report.GetUnattributed(), report.GetChunkSizeBytes());

        ui64 componentBytes = SumBreakdown(report.GetLogoBlobs().GetBreakdown())
            + SumBreakdown(report.GetBlocks().GetBreakdown())
            + SumBreakdown(report.GetBarriers().GetBreakdown())
            + SumBreakdown(report.GetHuge().GetTotal().GetBreakdown())
            + SumBreakdown(report.GetSyncLog().GetBreakdown())
            + SumBreakdown(report.GetUnattributed().GetBreakdown());
        for (const auto& chunkKeeper : report.GetChunkKeeper()) {
            componentBytes += SumBreakdown(chunkKeeper.GetTotal().GetBreakdown());
        }
        UNIT_ASSERT_VALUES_EQUAL(report.GetAccountedBytes(), componentBytes);

        const i64 expectedDelta = static_cast<i64>(report.GetPDiskAllocatedBytes())
            - static_cast<i64>(report.GetAccountedBytes());
        UNIT_ASSERT_VALUES_EQUAL(report.GetReconciliationDeltaBytes(), expectedDelta);

        UNIT_ASSERT_VALUES_EQUAL(
            report.GetStripeHeap().GetAllocatedBytes(),
            report.GetStripeHeap().GetChunkCount() * report.GetChunkSizeBytes());
        UNIT_ASSERT_VALUES_EQUAL(
            report.GetStripeHeap().GetAllocatedBytes(),
            report.GetStripeHeap().GetUsedBytes()
                + report.GetStripeHeap().GetFreeBytes()
                + report.GetStripeHeap().GetLockedFreeBytes());

        for (const auto& sizeClass : report.GetHuge().GetSizeClasses()) {
            UNIT_ASSERT_VALUES_EQUAL(
                SumBreakdown(sizeClass.GetBreakdown()),
                sizeClass.GetChunkCount() * report.GetChunkSizeBytes());
        }
    }

    void SendSpaceReportRequest(TTestEnv& env, const TActorId& edge) {
        env.GetRuntime()->Send(new IEventHandle(
            env.GetVDiskServiceId(), edge, new TEvGetVDiskSpaceReportRequest), 1);
    }

    const NKikimrVDisk::TGetVDiskSpaceReportResponse& WaitForSpaceReport(
            TTestEnv& env,
            const TActorId& edge,
            std::unique_ptr<TEventHandle<TEvGetVDiskSpaceReportResponse>>& handle) {
        handle = env.GetRuntime()->WaitForEdgeActorEvent<TEvGetVDiskSpaceReportResponse>(edge);
        return handle->Get()->Record;
    }

    void AssertCompletedReport(const NKikimrVDisk::TGetVDiskSpaceReportResponse& response) {
        UNIT_ASSERT_VALUES_EQUAL(response.GetStatus(), NKikimrProto::EReplyStatus_Name(NKikimrProto::OK));
        UNIT_ASSERT(response.HasReport());
        AssertReportIsConsistent(response.GetReport());
        UNIT_ASSERT(response.GetReport().GetCollectionCompletedAtUnixMs()
            >= response.GetReport().GetCollectionStartedAtUnixMs());
    }

    ::NMonitoring::TDynamicCounterPtr GetSpaceReportCounters(TTestEnv& env) {
        return env.GetCounters()
            ->GetSubgroup("subsystem", "vdisk")
            ->GetSubgroup("counters", "vdisks")
            ->GetSubgroup("storagePool", "static")
            ->GetSubgroup("group", "000000000")
            ->GetSubgroup("orderNumber", "00")
            ->GetSubgroup("pdisk", "000000001")
            ->GetSubgroup("media", "ssd")
            ->GetSubgroup("subsystem", "vdisk_space_report");
    }

    void WaitForSuccessfulRefresh(TTestEnv& env, ui64 previousSuccesses = 0) {
        const auto counter = GetSpaceReportCounters(env)->GetCounter("RefreshSuccesses", true);
        env.GetRuntime()->Sim([&] {
            return static_cast<ui64>(counter->Val()) <= previousSuccesses;
        });
    }

    void WaitForFailedRefresh(TTestEnv& env, ui64 previousFailures = 0) {
        const auto counter = GetSpaceReportCounters(env)->GetCounter("RefreshFailures", true);
        env.GetRuntime()->Sim([&] {
            return static_cast<ui64>(counter->Val()) <= previousFailures;
        });
    }

    void AssertReportCounters(
            TTestEnv& env,
            const NKikimrVDisk::TVDiskSpaceReport& report) {
        const auto counters = GetSpaceReportCounters(env);
        UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter("ChunkSizeBytes")->Val(), report.GetChunkSizeBytes());
        UNIT_ASSERT_VALUES_EQUAL(
            counters->GetCounter("PDiskAllocatedChunks")->Val(), report.GetPDiskAllocatedChunks());
        UNIT_ASSERT_VALUES_EQUAL(
            counters->GetCounter("PDiskAllocatedBytes")->Val(), report.GetPDiskAllocatedBytes());
        UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter("AccountedBytes")->Val(), report.GetAccountedBytes());
        UNIT_ASSERT_VALUES_EQUAL(
            counters->GetCounter("ReconciliationDeltaBytes")->Val(),
            report.GetReconciliationDeltaBytes());

        const auto total = counters->GetSubgroup("scope", "total");
        UNIT_ASSERT_VALUES_EQUAL(
            total->GetCounter("UsefulBlobDataBytes")->Val(),
            report.GetTotal().GetUsefulBlobDataBytes());
        const auto inplaceBlobs = counters->GetSubgroup("component", "inplace_blobs");
        UNIT_ASSERT_VALUES_EQUAL(
            inplaceBlobs->GetCounter("ChunkCount")->Val(), report.GetLogoBlobs().GetChunkCount());
        UNIT_ASSERT_VALUES_EQUAL(
            inplaceBlobs->GetCounter("UsefulBlobDataBytes")->Val(),
            report.GetLogoBlobs().GetBreakdown().GetUsefulBlobDataBytes());
    }

} // anonymous namespace

Y_UNIT_TEST_SUITE(VDiskSpaceReportTests) {

    Y_UNIT_TEST(ReportsConsistentSpaceBreakdown) {
        TTestEnv env;
        const TString data(100, 'x');
        const TLogoBlobID id(1, 1, 1, 0, data.size(), 0, 1);
        UNIT_ASSERT_VALUES_EQUAL(env.Put(id, data).GetStatus(), NKikimrProto::OK);
        env.Compact();

        const TActorId edge = env.GetRuntime()->AllocateEdgeActor(1);

        SendSpaceReportRequest(env, edge);

        std::unique_ptr<TEventHandle<TEvGetVDiskSpaceReportResponse>> coldHandle;
        const auto& cold = WaitForSpaceReport(env, edge, coldHandle);
        UNIT_ASSERT_VALUES_EQUAL(cold.GetStatus(), NKikimrProto::EReplyStatus_Name(NKikimrProto::NOTREADY));
        UNIT_ASSERT(!cold.HasReport());

        WaitForSuccessfulRefresh(env);
        const TActorId reportEdge = env.GetRuntime()->AllocateEdgeActor(1);
        SendSpaceReportRequest(env, reportEdge);

        std::unique_ptr<TEventHandle<TEvGetVDiskSpaceReportResponse>> reportHandle;
        const auto& response = WaitForSpaceReport(env, reportEdge, reportHandle);
        AssertCompletedReport(response);
        UNIT_ASSERT(response.GetReport().GetLogoBlobs().GetBreakdown().GetUsefulBlobDataBytes() > 0);
        AssertReportCounters(env, response.GetReport());

        const ui64 completedAt = response.GetReport().GetCollectionCompletedAtUnixMs();
        const TActorId cachedEdge = env.GetRuntime()->AllocateEdgeActor(1);
        SendSpaceReportRequest(env, cachedEdge);
        std::unique_ptr<TEventHandle<TEvGetVDiskSpaceReportResponse>> cachedHandle;
        const auto& cached = WaitForSpaceReport(env, cachedEdge, cachedHandle);
        AssertCompletedReport(cached);
        UNIT_ASSERT_VALUES_EQUAL(cached.GetReport().GetCollectionCompletedAtUnixMs(), completedAt);
    }

    Y_UNIT_TEST(ReportsSharedStripedChunk) {
        TTestEnv env(nullptr, true);
        env.ChangeMinHugeBlobSize(32_KB);

        const TString data(64_KB, 'x');
        const TLogoBlobID id(1, 1, 1, 0, data.size(), 0, 1);
        UNIT_ASSERT_VALUES_EQUAL(env.Put(id, data).GetStatus(), NKikimrProto::OK);
        env.Compact();
        UNIT_ASSERT_VALUES_EQUAL(env.Block(2, 1).GetStatus(), NKikimrProto::OK);
        env.Compact(EHullDbType::Blocks, true);

        const TActorId edge = env.GetRuntime()->AllocateEdgeActor(1);
        SendSpaceReportRequest(env, edge);

        std::unique_ptr<TEventHandle<TEvGetVDiskSpaceReportResponse>> coldHandle;
        const auto& cold = WaitForSpaceReport(env, edge, coldHandle);
        UNIT_ASSERT_VALUES_EQUAL(cold.GetStatus(), NKikimrProto::EReplyStatus_Name(NKikimrProto::NOTREADY));

        WaitForSuccessfulRefresh(env);
        const TActorId reportEdge = env.GetRuntime()->AllocateEdgeActor(1);
        SendSpaceReportRequest(env, reportEdge);
        std::unique_ptr<TEventHandle<TEvGetVDiskSpaceReportResponse>> reportHandle;
        const auto& response = WaitForSpaceReport(env, reportEdge, reportHandle);
        AssertCompletedReport(response);

        const auto& report = response.GetReport();
        UNIT_ASSERT_VALUES_EQUAL(report.GetStripeHeap().GetChunkCount(), 1);
        UNIT_ASSERT(report.GetStripeHeap().GetUsedBytes() > 0);
        UNIT_ASSERT(report.GetHuge().GetTotal().GetStripedBytes() > 0);
        UNIT_ASSERT(report.GetBlocks().GetStripedBytes() > 0);
        UNIT_ASSERT(report.GetHuge().GetTotal().GetBreakdown().GetUsefulBlobDataBytes() > 0);
        UNIT_ASSERT_VALUES_EQUAL(
            report.GetLogoBlobs().GetStripedBytes()
                + report.GetBlocks().GetStripedBytes()
                + report.GetBarriers().GetStripedBytes()
                + report.GetHuge().GetTotal().GetStripedBytes(),
            report.GetStripeHeap().GetAllocatedBytes());
        UNIT_ASSERT_VALUES_EQUAL(report.GetReconciliationDeltaBytes(), 0);
    }

    Y_UNIT_TEST(ReportsHugeStatsTimeoutOnColdCache) {
        TTestEnv env(nullptr, true);
        env.ChangeMinHugeBlobSize(32_KB);

        const TString data(64_KB, 'x');
        const TLogoBlobID id(1, 1, 1, 0, data.size(), 0, 1);
        UNIT_ASSERT_VALUES_EQUAL(env.Put(id, data).GetStatus(), NKikimrProto::OK);
        env.Compact();
        UNIT_ASSERT_VALUES_EQUAL(env.Block(2, 1).GetStatus(), NKikimrProto::OK);
        env.Compact(EHullDbType::Blocks, true);

        TTestActorSystem* const runtime = env.GetRuntime();
        runtime->FilterFunction = [](ui32, std::unique_ptr<IEventHandle>& ev) {
            return ev->GetTypeRewrite() != TEvHugeSpaceStatResult::EventType;
        };

        const TActorId edge = runtime->AllocateEdgeActor(1);
        SendSpaceReportRequest(env, edge);

        std::unique_ptr<TEventHandle<TEvGetVDiskSpaceReportResponse>> coldHandle;
        const auto& cold = WaitForSpaceReport(env, edge, coldHandle);
        UNIT_ASSERT_VALUES_EQUAL(cold.GetStatus(), NKikimrProto::EReplyStatus_Name(NKikimrProto::NOTREADY));

        WaitForFailedRefresh(env);
        runtime->FilterFunction = {};

        const TActorId retryEdge = runtime->AllocateEdgeActor(1);
        SendSpaceReportRequest(env, retryEdge);
        std::unique_ptr<TEventHandle<TEvGetVDiskSpaceReportResponse>> retryHandle;
        const auto& retry = WaitForSpaceReport(env, retryEdge, retryHandle);
        UNIT_ASSERT_VALUES_EQUAL(retry.GetStatus(), NKikimrProto::EReplyStatus_Name(NKikimrProto::NOTREADY));
        UNIT_ASSERT_STRING_CONTAINS(retry.GetErrorReason(), "HugeKeeper space counters timed out");
        UNIT_ASSERT(!retry.HasReport());
    }

    Y_UNIT_TEST(CoalescesConcurrentColdRequests) {
        TTestEnv env;
        TTestActorSystem* const runtime = env.GetRuntime();
        std::unique_ptr<IEventHandle> detainedHugeStat;
        ui32 detainedNodeId = 0;
        runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (!detainedHugeStat && ev->GetTypeRewrite() == TEvHugeSpaceStatResult::EventType) {
                detainedNodeId = nodeId;
                detainedHugeStat = std::move(ev);
                return false;
            }
            return true;
        };

        const TActorId firstEdge = runtime->AllocateEdgeActor(1);
        SendSpaceReportRequest(env, firstEdge);
        std::unique_ptr<TEventHandle<TEvGetVDiskSpaceReportResponse>> firstColdHandle;
        const auto& firstCold = WaitForSpaceReport(env, firstEdge, firstColdHandle);
        UNIT_ASSERT_VALUES_EQUAL(
            firstCold.GetStatus(), NKikimrProto::EReplyStatus_Name(NKikimrProto::NOTREADY));
        runtime->Sim([&] { return !detainedHugeStat; });
        UNIT_ASSERT(detainedHugeStat);

        const TActorId secondEdge = runtime->AllocateEdgeActor(1);
        SendSpaceReportRequest(env, secondEdge);
        std::unique_ptr<TEventHandle<TEvGetVDiskSpaceReportResponse>> secondHandle;
        const auto& second = WaitForSpaceReport(env, secondEdge, secondHandle);
        UNIT_ASSERT_VALUES_EQUAL(second.GetStatus(), NKikimrProto::EReplyStatus_Name(NKikimrProto::NOTREADY));
        UNIT_ASSERT(!second.HasReport());
        UNIT_ASSERT_VALUES_EQUAL(
            GetSpaceReportCounters(env)->GetCounter("RefreshInProgress")->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            GetSpaceReportCounters(env)->GetCounter("RefreshSuccesses", true)->Val(), 0);

        runtime->FilterFunction = {};
        runtime->Send(detainedHugeStat.release(), detainedNodeId);
        WaitForSuccessfulRefresh(env);

        const TActorId cachedEdge = runtime->AllocateEdgeActor(1);
        SendSpaceReportRequest(env, cachedEdge);
        std::unique_ptr<TEventHandle<TEvGetVDiskSpaceReportResponse>> cachedHandle;
        const auto& cached = WaitForSpaceReport(env, cachedEdge, cachedHandle);
        AssertCompletedReport(cached);
        UNIT_ASSERT_VALUES_EQUAL(
            GetSpaceReportCounters(env)->GetCounter("RefreshSuccesses", true)->Val(), 1);
    }

    Y_UNIT_TEST(PeriodicRefreshPopulatesCache) {
        TTestEnv env;
        env.SetSpaceReportPeriodSeconds(1);

        WaitForSuccessfulRefresh(env);
        UNIT_ASSERT_VALUES_EQUAL(
            GetSpaceReportCounters(env)->GetCounter("ColdCacheRequests", true)->Val(), 0);

        const TActorId edge = env.GetRuntime()->AllocateEdgeActor(1);
        SendSpaceReportRequest(env, edge);
        std::unique_ptr<TEventHandle<TEvGetVDiskSpaceReportResponse>> handle;
        const auto& response = WaitForSpaceReport(env, edge, handle);
        AssertCompletedReport(response);
        AssertReportCounters(env, response.GetReport());
    }

}

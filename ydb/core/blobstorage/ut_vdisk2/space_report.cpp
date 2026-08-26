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
            + breakdown.GetUnclassifiedBytes();
    }

    void AssertComponentAllocation(
            const NKikimrVDisk::TVDiskSpaceComponent& component,
            ui64 chunkSize) {
        UNIT_ASSERT_VALUES_EQUAL(component.GetAllocatedBytes(), component.GetChunkCount() * chunkSize);
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

        std::unique_ptr<TEventHandle<TEvGetVDiskSpaceReportResponse>> handle;
        const auto& response = WaitForSpaceReport(env, edge, handle);
        AssertCompletedReport(response);
        UNIT_ASSERT(response.GetReport().GetLogoBlobs().GetBreakdown().GetUsefulBlobDataBytes() > 0);
    }

    Y_UNIT_TEST(RejectsConcurrentRequest) {
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
        runtime->Sim([&] { return !detainedHugeStat; });
        UNIT_ASSERT(detainedHugeStat);

        const TActorId secondEdge = runtime->AllocateEdgeActor(1);
        SendSpaceReportRequest(env, secondEdge);
        std::unique_ptr<TEventHandle<TEvGetVDiskSpaceReportResponse>> secondHandle;
        const auto& second = WaitForSpaceReport(env, secondEdge, secondHandle);
        UNIT_ASSERT_VALUES_EQUAL(second.GetStatus(), NKikimrProto::EReplyStatus_Name(NKikimrProto::TRYLATER));
        UNIT_ASSERT(!second.HasReport());

        runtime->FilterFunction = {};
        runtime->Send(detainedHugeStat.release(), detainedNodeId);

        std::unique_ptr<TEventHandle<TEvGetVDiskSpaceReportResponse>> firstHandle;
        const auto& first = WaitForSpaceReport(env, firstEdge, firstHandle);
        AssertCompletedReport(first);
    }

}

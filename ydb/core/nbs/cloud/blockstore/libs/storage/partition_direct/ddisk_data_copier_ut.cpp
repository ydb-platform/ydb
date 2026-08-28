#include "ddisk_data_copier.h"

#include "base_test_fixture.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/testlib/range_locker_access.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NThreading;

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

namespace {

THostMask MakePrimariesMask()
{
    THostMask result;
    result.Set(0);
    result.Set(1);
    result.Set(2);
    return result;
}

void FinishFlushes(TBlocksDirtyMap& dirtyMap, const TFlushHints& hints)
{
    for (const auto& [route, flush]: hints.GetAllHints()) {
        dirtyMap.FlushFinished(route, {MakePBufferKeys(flush.Segments)}, {});
    }
}

struct TFixture
    : public TBaseFixture
    , public IRangeSyncClient
{
    TDDiskDataCopierPtr Copier;
    TVector<ui64> CopyProgressNotifications;

    std::optional<TBlockRange64> GetFreshRange(THostIndex host) const override
    {
        return DirtyMap->GetFreshRange(host);
    }

    TReadHint MakeReadHint(TBlockRange64 range) override
    {
        return DirtyMap->MakeReadHint(range);
    }

    TRangeLock MakeDDiskRangeLock(TBlockRange64 range, THostMask mask) override
    {
        return TRangeLockAccess::Make(DirtyMap, range, mask);
    }

    TSyncHint BeginRangeSync(THostIndex host, TBlockRange64 range) override
    {
        return DirtyMap->BeginRangeSync(host, range);
    }

    void EndRangeSync(ui64 syncId, bool success) override
    {
        DirtyMap->EndRangeSync(syncId, success);
    }

    void OnCopyProgress(ui64 totalBytes) override
    {
        CopyProgressNotifications.push_back(totalBytes);
    }

    void Init() override
    {
        TBaseFixture::Init();

        VChunkConfig.PromoteHost(3);
        VChunkConfig.SetWatermark(3, BlockSize * VChunkBlockCount);
        DirtyMap->UpdateConfig(VChunkConfig);

        Copier = std::make_shared<TDDiskDataCopier>(
            Runtime->GetActorSystem(0),
            TraceService.get(),
            PartitionDirectService.get(),
            DiskDescription,
            VChunkConfig,
            DirectBlockGroup,
            this,
            FreshDDisk);
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TDDiskDataCopierTest)
{
    Y_UNIT_TEST_F(ShouldCopyDDisk, TFixture)
    {
        Init();

        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Operational,32768};"
            "H1*{Operational,32768};"
            "H2*{Operational,32768};"
            "H3*{Operational,32768};"
            "H4+{Disabled,0};",
            DirtyMap->DebugPrintDDiskState());

        // Mark DDisk#1 completely fresh.
        DirtyMap->UpdateWatermarkDebugOnly(FreshDDisk, 0);
        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Operational,32768};"
            "H1*{Fresh+,0};"   // Watermarks
            "H2*{Operational,32768};"
            "H3*{Operational,32768};"
            "H4+{Disabled,0};",
            DirtyMap->DebugPrintDDiskState());

        // No ranges locked.
        UNIT_ASSERT_VALUES_EQUAL("", DirtyMap->DebugPrintLockedDDiskRanges());

        // Start data copy
        ExpectedRange = TBlockRange64::WithLength(0, BlocksPerCopy);
        UNIT_ASSERT_VALUES_EQUAL(0, Copier->GetBytesCopied());
        auto complete = Copier->Start();

        // Should transfer all ranges. One-by-one.
        for (size_t i = 0; i < DefaultVChunkSize / CopyRangeSize; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(false, complete.IsReady());

            // expectedRange should be locked for reading and copying.
            UNIT_ASSERT_VALUES_EQUAL(
                ExpectedRange.Print() + "[H1];" + ExpectedRange.Print() +
                    "[H0,H2,H3];",
                DirtyMap->DebugPrintLockedDDiskRanges());

            // Complete reading and re-arm promise.
            SetReadResult({.Error = MakeError(S_OK)}, false);

            // expectedRange should be locked for copying.
            UNIT_ASSERT_VALUES_EQUAL(
                ExpectedRange.Print() + "[H1];",
                DirtyMap->DebugPrintLockedDDiskRanges());

            // Set next expected range right before completing write.
            auto nextExpectedRange = TBlockRange64::WithLength(
                (i + 1) * BlocksPerCopy,
                BlocksPerCopy);
            ExpectedRange = nextExpectedRange;

            // Complete writing and rea-arm promise
            SetWriteResult(
                TDBGWriteBlocksResponse{.Error = MakeError(S_OK)},
                false);
            UNIT_ASSERT_VALUES_EQUAL(
                (i + 1) * CopyRangeSize,
                Copier->GetBytesCopied());
            UNIT_ASSERT_VALUES_EQUAL(
                Copier->GetBytesCopied() / CopyProgressSaveInterval,
                CopyProgressNotifications.size());
            if (!CopyProgressNotifications.empty()) {
                UNIT_ASSERT_VALUES_EQUAL(
                    CopyProgressNotifications.size() * CopyProgressSaveInterval,
                    CopyProgressNotifications.back());
            }

            if (i == 5) {
                // Check state on 5th iteration
                UNIT_ASSERT_VALUES_EQUAL(
                    "H0*{Operational,32768};"
                    "H1*{Fresh+,1536};"   // Watermarks for reading
                                          // and writing raised
                    "H2*{Operational,32768};"
                    "H3*{Operational,32768};"
                    "H4+{Disabled,0};",
                    DirtyMap->DebugPrintDDiskState());
            }
        }

        // Data copying should be completed.
        UNIT_ASSERT_VALUES_EQUAL(true, complete.IsReady());
        UNIT_ASSERT_VALUES_EQUAL(
            TDDiskDataCopier::EResult::Ok,
            complete.GetValue());
        UNIT_ASSERT_VALUES_EQUAL(DefaultVChunkSize, Copier->GetBytesCopied());

        // All DDisk fully operational
        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Operational,32768};"
            "H1*{Operational,32768};"
            "H2*{Operational,32768};"
            "H3*{Operational,32768};"
            "H4+{Disabled,0};",
            DirtyMap->DebugPrintDDiskState());
    }

    Y_UNIT_TEST_F(ShouldAccumulatePartialRangesForCopyProgress, TFixture)
    {
        Init();

        const ui64 firstCopyBytes = CopyProgressSaveInterval - BlockSize;
        const ui64 firstCopyBlocks = firstCopyBytes / BlockSize;
        ui64 rangeStart = VChunkBlockCount - firstCopyBlocks;

        DirtyMap->UpdateWatermarkDebugOnly(FreshDDisk, rangeStart * BlockSize);

        ExpectedRange = TBlockRange64::WithLength(
            rangeStart,
            Min<ui64>(BlocksPerCopy, VChunkBlockCount - rangeStart));
        auto complete = Copier->Start();

        while (!complete.IsReady()) {
            const ui64 rangeSize = ExpectedRange.Size();
            SetReadResult({.Error = MakeError(S_OK)}, false);

            rangeStart += rangeSize;
            if (rangeStart < VChunkBlockCount) {
                ExpectedRange = TBlockRange64::WithLength(
                    rangeStart,
                    Min<ui64>(BlocksPerCopy, VChunkBlockCount - rangeStart));
            }
            SetWriteResult(
                TDBGWriteBlocksResponse{.Error = MakeError(S_OK)},
                false);
        }

        UNIT_ASSERT_VALUES_EQUAL(firstCopyBytes, Copier->GetBytesCopied());
        UNIT_ASSERT_VALUES_EQUAL(0, CopyProgressNotifications.size());

        DirtyMap->UpdateWatermarkDebugOnly(
            FreshDDisk,
            (VChunkBlockCount - 1) * BlockSize);
        ExpectedRange = TBlockRange64::WithLength(VChunkBlockCount - 1, 1);
        complete = Copier->Start();

        SetReadResult({.Error = MakeError(S_OK)}, false);
        SetWriteResult(
            TDBGWriteBlocksResponse{.Error = MakeError(S_OK)},
            false);

        UNIT_ASSERT_VALUES_EQUAL(true, complete.IsReady());
        UNIT_ASSERT_VALUES_EQUAL(
            CopyProgressSaveInterval,
            Copier->GetBytesCopied());
        UNIT_ASSERT_VALUES_EQUAL(1, CopyProgressNotifications.size());
        UNIT_ASSERT_VALUES_EQUAL(
            CopyProgressSaveInterval,
            CopyProgressNotifications.front());
    }

    Y_UNIT_TEST_F(ShouldRetryOnReadError, TFixture)
    {
        Init();

        size_t readsCount = 0;
        DirectBlockGroup->ReadBlocksFromDDiskHandler = [&]   //
            (ui32 vChunkIndex,
             THostIndex hostIndex,
             TBlockRange64 range,
             const TGuardedSgList& guardedSglist,
             const NWilson::TTraceId& traceId)
        {
            Y_UNUSED(vChunkIndex);
            Y_UNUSED(hostIndex);
            Y_UNUSED(range);
            Y_UNUSED(guardedSglist);
            Y_UNUSED(traceId);

            ++readsCount;

            // ReadExecutor will respond with E_REJECTED even if all replicas
            // returned a non-retriable error.
            return MakeFuture<TDBGReadBlocksResponse>(
                {.Error = MakeError(E_IO)});
        };

        // Mark DDisk#1 completely fresh.
        DirtyMap->UpdateWatermarkDebugOnly(FreshDDisk, 0);

        // Start data copy
        ExpectedRange = TBlockRange64::WithLength(0, BlocksPerCopy);
        auto complete = Copier->Start();

        // Wait for read retry scheduled.
        WaitScheduledTasks(1, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(3, readsCount);

        // Data copying should not be advanced.
        UNIT_ASSERT_VALUES_EQUAL(false, complete.IsReady());
        UNIT_ASSERT_VALUES_EQUAL(
            TBlockRange64::WithLength(0, 32768),
            *DirtyMap->GetFreshRange(FreshDDisk));

        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Operational,32768};"
            "H1*{Fresh+,0};"   // Watermarks
            "H2*{Operational,32768};"
            "H3*{Operational,32768};"
            "H4+{Disabled,0};",
            DirtyMap->DebugPrintDDiskState());
    }

    Y_UNIT_TEST_F(ShouldStopOnNonRetriableWriteError, TFixture)
    {
        Init();

        // Will response with error for write requests.
        DirectBlockGroup->WriteBlocksToDDiskHandler = [&]   //
            (ui32 vChunkIndex,
             THostIndex hostIndex,
             TBlockRange64 range,
             const TGuardedSgList& guardedSglist,
             const NWilson::TTraceId& traceId)
        {
            Y_UNUSED(vChunkIndex);
            Y_UNUSED(hostIndex);
            Y_UNUSED(range);
            Y_UNUSED(guardedSglist);
            Y_UNUSED(traceId);

            return MakeFuture<TDBGWriteBlocksResponse>(
                {.Error = MakeError(E_IO_SILENT)});
        };

        // Mark DDisk#1 completely fresh.
        DirtyMap->UpdateWatermarkDebugOnly(FreshDDisk, 0);

        // Start data copy
        ExpectedRange = TBlockRange64::WithLength(0, BlocksPerCopy);
        auto complete = Copier->Start();

        // Read range - OK.
        SetReadResult({.Error = MakeError(S_OK)}, false);

        // Data copying should be completed with error.
        UNIT_ASSERT_VALUES_EQUAL(true, complete.IsReady());
        UNIT_ASSERT_VALUES_EQUAL(
            TDDiskDataCopier::EResult::Error,
            complete.GetValue());
        UNIT_ASSERT_VALUES_EQUAL(0, Copier->GetBytesCopied());
        UNIT_ASSERT_VALUES_EQUAL(0, CopyProgressNotifications.size());

        UNIT_ASSERT_VALUES_EQUAL(
            TBlockRange64::WithLength(0, 32768),
            *DirtyMap->GetFreshRange(FreshDDisk));

        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Operational,32768};"
            "H1*{Fresh+,0};"   // Watermarks
            "H2*{Operational,32768};"
            "H3*{Operational,32768};"
            "H4+{Disabled,0};",
            DirtyMap->DebugPrintDDiskState());
    }

    Y_UNIT_TEST_F(ShouldClearInflightSyncOnNonRetriableWriteError, TFixture)
    {
        Init();

        // Will respond with a non-retriable error for write requests.
        DirectBlockGroup->WriteBlocksToDDiskHandler = [&]   //
            (ui32 vChunkIndex,
             THostIndex hostIndex,
             TBlockRange64 range,
             const TGuardedSgList& guardedSglist,
             const NWilson::TTraceId& traceId)
        {
            Y_UNUSED(vChunkIndex);
            Y_UNUSED(hostIndex);
            Y_UNUSED(range);
            Y_UNUSED(guardedSglist);
            Y_UNUSED(traceId);

            return MakeFuture<TDBGWriteBlocksResponse>(
                {.Error = MakeError(E_IO_SILENT)});
        };

        // Mark DDisk#1 completely fresh.
        DirtyMap->UpdateWatermarkDebugOnly(FreshDDisk, 0);

        // Start data copy
        ExpectedRange = TBlockRange64::WithLength(0, BlocksPerCopy);
        auto complete = Copier->Start();

        // The range sync is registered as in-flight while the copy is running.
        UNIT_ASSERT_VALUES_EQUAL(
            "H1[0..255]ready;",
            DirtyMap->DebugPrintInflightSync());

        // Read range - OK.
        SetReadResult({.Error = MakeError(S_OK)}, false);

        // Data copying should be completed with error.
        UNIT_ASSERT_VALUES_EQUAL(true, complete.IsReady());
        UNIT_ASSERT_VALUES_EQUAL(
            TDDiskDataCopier::EResult::Error,
            complete.GetValue());

        // The failed sync must be removed from the in-flight sync map
        // (EndRangeSync(syncId, false)), so it does not leak.
        UNIT_ASSERT_VALUES_EQUAL("", DirtyMap->DebugPrintInflightSync());

        // The fresh range must NOT advance after a failed sync.
        UNIT_ASSERT_VALUES_EQUAL(
            TBlockRange64::WithLength(0, 32768),
            *DirtyMap->GetFreshRange(FreshDDisk));

        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Operational,32768};"
            "H1*{Fresh+,0};"   // Watermarks unchanged
            "H2*{Operational,32768};"
            "H3*{Operational,32768};"
            "H4+{Disabled,0};",
            DirtyMap->DebugPrintDDiskState());
    }

    Y_UNIT_TEST_F(ShouldClearInflightSyncOnReadError, TFixture)
    {
        Init();

        size_t readsCount = 0;
        DirectBlockGroup->ReadBlocksFromDDiskHandler = [&]   //
            (ui32 vChunkIndex,
             THostIndex hostIndex,
             TBlockRange64 range,
             const TGuardedSgList& guardedSglist,
             const NWilson::TTraceId& traceId)
        {
            Y_UNUSED(vChunkIndex);
            Y_UNUSED(hostIndex);
            Y_UNUSED(range);
            Y_UNUSED(guardedSglist);
            Y_UNUSED(traceId);

            ++readsCount;

            // ReadExecutor will respond with E_REJECTED even if all replicas
            // returned a non-retriable error.
            return MakeFuture<TDBGReadBlocksResponse>(
                {.Error = MakeError(E_IO)});
        };

        // Mark DDisk#1 completely fresh.
        DirtyMap->UpdateWatermarkDebugOnly(FreshDDisk, 0);

        // Start data copy
        ExpectedRange = TBlockRange64::WithLength(0, BlocksPerCopy);
        auto complete = Copier->Start();

        // Wait for read retry scheduled.
        WaitScheduledTasks(1, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(3, readsCount);

        // The failed sync must be removed from the in-flight sync map
        // (EndRangeSync(syncId, false)) even on a read error, so it does not
        // leak across retries.
        UNIT_ASSERT_VALUES_EQUAL("", DirtyMap->DebugPrintInflightSync());

        // Data copying should not be advanced.
        UNIT_ASSERT_VALUES_EQUAL(false, complete.IsReady());

        // The fresh range must NOT advance after a failed sync.
        UNIT_ASSERT_VALUES_EQUAL(
            TBlockRange64::WithLength(0, 32768),
            *DirtyMap->GetFreshRange(FreshDDisk));

        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Operational,32768};"
            "H1*{Fresh+,0};"   // Watermarks unchanged
            "H2*{Operational,32768};"
            "H3*{Operational,32768};"
            "H4+{Disabled,0};",
            DirtyMap->DebugPrintDDiskState());
    }

    Y_UNIT_TEST_F(ShouldClearInflightSyncOnRetriableWriteError, TFixture)
    {
        Init();

        // Will respond with a retriable error for write requests.
        DirectBlockGroup->WriteBlocksToDDiskHandler = [&]   //
            (ui32 vChunkIndex,
             THostIndex hostIndex,
             TBlockRange64 range,
             const TGuardedSgList& guardedSglist,
             const NWilson::TTraceId& traceId)
        {
            Y_UNUSED(vChunkIndex);
            Y_UNUSED(hostIndex);
            Y_UNUSED(range);
            Y_UNUSED(guardedSglist);
            Y_UNUSED(traceId);

            return MakeFuture<TDBGWriteBlocksResponse>(
                {.Error = MakeError(E_REJECTED)});
        };

        // Mark DDisk#1 completely fresh.
        DirtyMap->UpdateWatermarkDebugOnly(FreshDDisk, 0);

        // Start data copy
        ExpectedRange = TBlockRange64::WithLength(0, BlocksPerCopy);
        auto complete = Copier->Start();

        // The range sync is registered as in-flight while the copy is running.
        UNIT_ASSERT_VALUES_EQUAL(
            "H1[0..255]ready;",
            DirtyMap->DebugPrintInflightSync());

        // Read range - OK. The subsequent write fails with a retriable error.
        SetReadResult({.Error = MakeError(S_OK)}, false);

        // The retriable error must schedule a retry, not complete the copy.
        UNIT_ASSERT_VALUES_EQUAL(false, complete.IsReady());

        // Even on a retriable write error the failed sync must be removed from
        // the in-flight sync map (EndRangeSync(syncId, false)), so it does not
        // leak across retries.
        UNIT_ASSERT_VALUES_EQUAL("", DirtyMap->DebugPrintInflightSync());

        // The fresh range must NOT advance after a failed sync.
        UNIT_ASSERT_VALUES_EQUAL(
            TBlockRange64::WithLength(0, 32768),
            *DirtyMap->GetFreshRange(FreshDDisk));

        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Operational,32768};"
            "H1*{Fresh+,0};"   // Watermarks unchanged
            "H2*{Operational,32768};"
            "H3*{Operational,32768};"
            "H4+{Disabled,0};",
            DirtyMap->DebugPrintDDiskState());
    }

    Y_UNIT_TEST_F(ShouldClearInflightSyncAfterSuccessfulRange, TFixture)
    {
        Init();

        // Mark DDisk#1 completely fresh.
        DirtyMap->UpdateWatermarkDebugOnly(FreshDDisk, 0);

        // Start data copy
        ExpectedRange = TBlockRange64::WithLength(0, BlocksPerCopy);
        auto complete = Copier->Start();

        // The first range sync is registered as in-flight.
        UNIT_ASSERT_VALUES_EQUAL(
            "H1[0..255]ready;",
            DirtyMap->DebugPrintInflightSync());

        // Read range #0 - OK.
        SetReadResult({.Error = MakeError(S_OK)}, false);

        // The next range starts right after writing range #0.
        ExpectedRange = TBlockRange64::WithLength(BlocksPerCopy, BlocksPerCopy);

        // Stop the copier so it finishes after the current range's write.
        Copier->Stop();

        // Write range #0 - OK. EndRangeSync(syncId, true) must remove the
        // completed entry from the in-flight sync map and advance the fresh
        // range.
        SetWriteResult(
            TDBGWriteBlocksResponse{.Error = MakeError(S_OK)},
            false);

        // Data copying should be interrupted right after the successful range.
        UNIT_ASSERT_VALUES_EQUAL(true, complete.IsReady());
        UNIT_ASSERT_VALUES_EQUAL(
            TDDiskDataCopier::EResult::Interrupted,
            complete.GetValue());

        // The successfully synced range must be removed from the in-flight sync
        // map; nothing must leak.
        UNIT_ASSERT_VALUES_EQUAL("", DirtyMap->DebugPrintInflightSync());

        // The fresh range advanced by exactly one copy range.
        UNIT_ASSERT_VALUES_EQUAL(
            TBlockRange64::MakeClosedInterval(256, 32767),
            *DirtyMap->GetFreshRange(FreshDDisk));
        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Operational,32768};"
            "H1*{Fresh+,256};"   // Watermark advanced by one copy range
            "H2*{Operational,32768};"
            "H3*{Operational,32768};"
            "H4+{Disabled,0};",
            DirtyMap->DebugPrintDDiskState());
    }

    Y_UNIT_TEST_F(ShouldRetryOnRetriableWriteError, TFixture)
    {
        Init();

        size_t readsCount = 0;
        DirectBlockGroup->ReadBlocksFromDDiskHandler = [&]   //
            (ui32 vChunkIndex,
             THostIndex hostIndex,
             TBlockRange64 range,
             const TGuardedSgList& guardedSglist,
             const NWilson::TTraceId& traceId)
        {
            Y_UNUSED(vChunkIndex);
            Y_UNUSED(hostIndex);
            Y_UNUSED(range);
            Y_UNUSED(guardedSglist);
            Y_UNUSED(traceId);

            ++readsCount;

            return MakeFuture<TDBGReadBlocksResponse>(
                {.Error = MakeError(S_OK)});
        };

        size_t writesCount = 0;
        // Will response with error for write requests.
        DirectBlockGroup->WriteBlocksToDDiskHandler = [&]   //
            (ui32 vChunkIndex,
             THostIndex hostIndex,
             TBlockRange64 range,
             const TGuardedSgList& guardedSglist,
             const NWilson::TTraceId& traceId)
        {
            Y_UNUSED(vChunkIndex);
            Y_UNUSED(hostIndex);
            Y_UNUSED(range);
            Y_UNUSED(guardedSglist);
            Y_UNUSED(traceId);

            ++writesCount;

            return MakeFuture<TDBGWriteBlocksResponse>(
                {.Error = MakeError(E_REJECTED)});
        };

        // Mark DDisk#1 completely fresh.
        DirtyMap->UpdateWatermarkDebugOnly(FreshDDisk, 0);

        // Start data copy
        ExpectedRange = TBlockRange64::WithLength(0, BlocksPerCopy);
        auto complete = Copier->Start();

        // Wait for copy range retry scheduled.
        WaitScheduledTasks(1, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(1, readsCount);
        UNIT_ASSERT_VALUES_EQUAL(1, writesCount);

        // Data copying should not be advanced.
        UNIT_ASSERT_VALUES_EQUAL(false, complete.IsReady());
        UNIT_ASSERT_VALUES_EQUAL(
            TBlockRange64::WithLength(0, 32768),
            *DirtyMap->GetFreshRange(FreshDDisk));
        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Operational,32768};"
            "H1*{Fresh+,0};"   // Watermarks
            "H2*{Operational,32768};"
            "H3*{Operational,32768};"
            "H4+{Disabled,0};",
            DirtyMap->DebugPrintDDiskState());
    }

    Y_UNIT_TEST_F(ShouldStartAfterStop, TFixture)
    {
        Init();

        // Mark DDisk#1 completely fresh.
        DirtyMap->UpdateWatermarkDebugOnly(FreshDDisk, 0);

        // Start data copying
        ExpectedRange = TBlockRange64::WithLength(0, BlocksPerCopy);
        auto complete = Copier->Start();
        UNIT_ASSERT_VALUES_EQUAL(false, complete.IsReady());

        // Read range #0 - OK.
        SetReadResult({.Error = MakeError(S_OK)}, false);

        // Stop data copy
        auto stopped = Copier->Stop();
        UNIT_ASSERT_VALUES_EQUAL(false, stopped.IsReady());

        // Write range #0 - OK.
        SetWriteResult(
            TDBGWriteBlocksResponse{.Error = MakeError(S_OK)},
            false);

        // Coping should be stoped with "Interrupted" status.
        UNIT_ASSERT_VALUES_EQUAL(true, stopped.IsReady());
        UNIT_ASSERT_VALUES_EQUAL(
            TDDiskDataCopier::EResult::Interrupted,
            stopped.GetValue());
        UNIT_ASSERT_VALUES_EQUAL(true, complete.IsReady());
        UNIT_ASSERT_VALUES_EQUAL(
            TDDiskDataCopier::EResult::Interrupted,
            complete.GetValue());

        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Operational,32768};"
            "H1*{Fresh+,256};"   // Watermarks
            "H2*{Operational,32768};"
            "H3*{Operational,32768};"
            "H4+{Disabled,0};",
            DirtyMap->DebugPrintDDiskState());

        // Start data copying again
        ExpectedRange = TBlockRange64::WithLength(256, BlocksPerCopy);
        complete = Copier->Start();
        UNIT_ASSERT_VALUES_EQUAL(false, complete.IsReady());

        // Read range #1 - OK.
        SetReadResult({.Error = MakeError(S_OK)}, false);

        // Stop data copy
        stopped = Copier->Stop();
        UNIT_ASSERT_VALUES_EQUAL(false, stopped.IsReady());

        // Write range #1 - OK.
        SetWriteResult(
            TDBGWriteBlocksResponse{.Error = MakeError(S_OK)},
            false);

        // Coping should be stoped with "Interrupted" status.
        UNIT_ASSERT_VALUES_EQUAL(true, stopped.IsReady());
        UNIT_ASSERT_VALUES_EQUAL(
            TDDiskDataCopier::EResult::Interrupted,
            stopped.GetValue());
        UNIT_ASSERT_VALUES_EQUAL(true, complete.IsReady());
        UNIT_ASSERT_VALUES_EQUAL(
            TDDiskDataCopier::EResult::Interrupted,
            complete.GetValue());

        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Operational,32768};"
            "H1*{Fresh+,512};"   // Watermarks
            "H2*{Operational,32768};"
            "H3*{Operational,32768};"
            "H4+{Disabled,0};",
            DirtyMap->DebugPrintDDiskState());
    }

    Y_UNIT_TEST_F(ShouldStartFromWaterline, TFixture)
    {
        Init();

        // Mark DDisk#1 partially fresh.
        DirtyMap->UpdateWatermarkDebugOnly(FreshDDisk, CopyRangeSize);

        // Start data copy
        ExpectedRange = TBlockRange64::WithLength(256, BlocksPerCopy);
        auto complete = Copier->Start();

        // Read range - OK.
        SetReadResult({.Error = MakeError(S_OK)}, false);

        // Stop after one range
        Copier->Stop();

        SetWriteResult(
            TDBGWriteBlocksResponse{.Error = MakeError(S_OK)},
            false);

        // Data copying should be completed with error.
        UNIT_ASSERT_VALUES_EQUAL(true, complete.IsReady());
        UNIT_ASSERT_VALUES_EQUAL(
            TDDiskDataCopier::EResult::Interrupted,
            complete.GetValue());

        UNIT_ASSERT_VALUES_EQUAL(
            TBlockRange64::MakeClosedInterval(512, 32767),
            *DirtyMap->GetFreshRange(FreshDDisk));

        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Operational,32768};"
            "H1*{Fresh+,512};"   // Watermarks
            "H2*{Operational,32768};"
            "H3*{Operational,32768};"
            "H4+{Disabled,0};",
            DirtyMap->DebugPrintDDiskState());
    }

    Y_UNIT_TEST_F(ShouldCopyWithWrites, TFixture)
    {
        const auto overlapped_0 = TBlockRange64::WithLength(
            10,
            10);   // overlapped with #0 sync range
        const auto overlapped_1 = TBlockRange64::WithLength(
            260,
            10);   // overlapped with #1 sync range
        const auto overlapped_01 = TBlockRange64::WithLength(
            250,
            10);   // overlapped with #0 + #1 sync range

        Init();
        RangeData = GenerateRandomString(CopyRangeSize * 3);

        // Mark DDisk#1 completely fresh.
        DirtyMap->UpdateWatermarkDebugOnly(FreshDDisk, 0);

        DirtyMap->RegisterInflightWrite(
            MakeKey(123),
            TBlockRange64::WithLength(10, 10));
        DirtyMap->WriteFinished(
            MakeKey(123),
            overlapped_0,
            MakePrimariesMask(),
            MakePrimariesMask());
        DirtyMap->RegisterInflightWrite(
            MakeKey(124),
            TBlockRange64::WithLength(250, 10));
        DirtyMap->WriteFinished(
            MakeKey(124),
            overlapped_01,
            MakePrimariesMask(),
            MakePrimariesMask());
        DirtyMap->RegisterInflightWrite(
            MakeKey(125),
            TBlockRange64::WithLength(260, 10));
        DirtyMap->WriteFinished(
            MakeKey(125),
            overlapped_1,
            MakePrimariesMask(),
            MakePrimariesMask());

        // Start data copy
        ExpectedRange = TBlockRange64::WithLength(0, BlocksPerCopy);
        auto complete = Copier->Start();

        // Coping range #0 in progress.
        UNIT_ASSERT_VALUES_EQUAL(
            "H1[0..255]ready;",
            DirtyMap->DebugPrintInflightSync());

        // Flush hints should not contains writes overlapped with copied
        // range #0
        auto flushHints = DirtyMap->MakeFlushHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "H0->H0:1:125[260..269];"
            "H0->H3:1:125[260..269];"
            "H1->H1:1:125[260..269];"
            "H2->H2:1:125[260..269];",
            flushHints.DebugPrint());

        // Read range #0 - OK.
        SetReadResult({.Error = MakeError(S_OK)}, false);

        // The reading of range #1 will begin immediately after writing to range
        // #0.
        ExpectedRange = TBlockRange64::WithLength(256, BlocksPerCopy);

        // Write range #0 - OK.
        SetWriteResult(
            TDBGWriteBlocksResponse{.Error = MakeError(S_OK)},
            false);

        // Coping range #1 waiting for flushes completed.
        UNIT_ASSERT_VALUES_EQUAL(
            "H1[256..511]wait;",
            DirtyMap->DebugPrintInflightSync());

        // Complete flushes to start copying range #1.
        FinishFlushes(*DirtyMap, flushHints);

        // Coping range #1 in progress.
        UNIT_ASSERT_VALUES_EQUAL(
            "H1[256..511]ready;",
            DirtyMap->DebugPrintInflightSync());

        // Flush hints should not contains writes overlapped with range #1,
        // but contains #0
        flushHints = DirtyMap->MakeFlushHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "H0->H0:1:123[10..19];"
            "H0->H3:1:123[10..19];"
            "H1->H1:1:123[10..19];"
            "H2->H2:1:123[10..19];",
            flushHints.DebugPrint());

        // Read range #1 - OK.
        SetReadResult({.Error = MakeError(S_OK)}, false);

        // The reading of range #2 will begin immediately after writing to range
        // #1.
        ExpectedRange = TBlockRange64::WithLength(512, BlocksPerCopy);

        // Write range #1 - OK.
        SetWriteResult(
            TDBGWriteBlocksResponse{.Error = MakeError(S_OK)},
            false);

        //  Coping range #2 in progress.
        UNIT_ASSERT_VALUES_EQUAL(
            "H1[512..767]ready;",
            DirtyMap->DebugPrintInflightSync());

        // Flush hints should contains writes overlapped with range #1
        flushHints = DirtyMap->MakeFlushHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "H0->H0:1:124[250..259];"
            "H0->H3:1:124[250..259];"
            "H1->H1:1:124[250..259];"
            "H2->H2:1:124[250..259];",
            flushHints.DebugPrint());

        // Read range #2 - OK.
        SetReadResult({.Error = MakeError(S_OK)}, false);

        // Will stop after writing range #2.
        Copier->Stop();
        SetWriteResult(
            TDBGWriteBlocksResponse{.Error = MakeError(S_OK)},
            false);
        UNIT_ASSERT_VALUES_EQUAL("", DirtyMap->DebugPrintInflightSync());

        // Data copying should be completed with error.
        UNIT_ASSERT_VALUES_EQUAL(true, complete.IsReady());
        UNIT_ASSERT_VALUES_EQUAL(
            TDDiskDataCopier::EResult::Interrupted,
            complete.GetValue());

        UNIT_ASSERT_VALUES_EQUAL(
            TBlockRange64::MakeClosedInterval(768, 32767),
            *DirtyMap->GetFreshRange(FreshDDisk));
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

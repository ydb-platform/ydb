#include "vchunk.h"

#include "base_test_fixture.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/dirty_map.h>

#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor_ut.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NThreading;

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

namespace {

// GetSafeBarrierForErase asserts it runs on the vchunk's executor thread, so
// hop onto the executor and bring the value back.
std::optional<TPBufferKey> GetSafeBarrierOnExecutor(
    const TExecutorPtr& executor,
    TVChunk& vchunk)
{
    auto promise = NThreading::NewPromise<std::optional<TPBufferKey>>();
    auto future = promise.GetFuture();
    executor->ExecuteSimple(
        [promise = std::move(promise), &vchunk]() mutable
        { promise.SetValue(vchunk.GetSafeBarrierForErase()); });
    return future.GetValue(TDuration::Seconds(10));
}

// Drives dirtyMap into a state where NeedPersist() is true and the persist
// generation has advanced. Mirrors the flush choreography used in dirty_map_ut:
// a write above the fresh DDisk's watermark populates its Ahead field, which
// bumps the behind/ahead generation on flush. Must run on the executor thread.
void MakeDirtyMapNeedPersist(TBlocksDirtyMap& dirtyMap)
{
    THostMask requested;
    requested.Set(0);
    requested.Set(1);
    requested.Set(2);
    requested.Set(3);

    dirtyMap.RegisterInflightWrite(
        MakeKey(100),
        TBlockRange64::WithLength(10, 10));
    dirtyMap.WriteFinished(
        MakeKey(100),
        TBlockRange64::WithLength(10, 10),
        requested,
        requested);

    auto flushHint = dirtyMap.MakeFlushHint(1);
    for (const auto& [route, hint]: flushHint.GetAllHints()) {
        dirtyMap.FlushFinished(route, MakePBufferKeys(hint.Segments), {});
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVChunkTest)
{
    Y_UNIT_TEST_F(ShouldScheduleCleanup, TBaseFixture)
    {
        Init();

        const TBlockRange64 range = TBlockRange64::WithLength(10, 1);
        ExpectedRange = range;
        RangeData = GenerateRandomString(BlockSize * range.Size());

        auto callContext = MakeIntrusive<TCallContext>(static_cast<ui64>(0));
        auto request =
            std::make_shared<TWriteBlocksLocalRequest>(TRequestHeaders{
                .VolumeConfig = PartitionDirectService->GetVolumeConfig(),
                .RequestId = 1,
                .Range = range});
        request->Sglist = MakeSgList();

        auto vchunk = std::make_shared<TVChunk>(
            Runtime->GetActorSystem(0),
            TraceService.get(),
            PartitionDirectService.get(),
            DiskDescription,
            VChunkConfig,
            DirtyMapStateProto,
            DirectBlockGroup,
            3,   // syncRequestsBatchSize
            DefaultVChunkSize);
        vchunk->Start();

        // Run write request
        auto future =
            vchunk->WriteBlocksLocal(callContext, request, NWilson::TTraceId());

        // Wait for three PBuffers write requests.
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            WaitWriteRequests(3, TDuration::Seconds(10)));

        // Finish write to PBuffers requests with success.
        SetWriteResult(TDBGWriteBlocksResponse{.Error = MakeError(S_OK)}, true);

        // Wait for write blocks response.
        const auto& result = future.GetValue(TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result.Error.GetCode(),
            FormatError(result.Error));

        // Wait for VChunk scheduled cleaning up (flushes).
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            WaitScheduledTasks(1, TDuration::Seconds(10)));

        // Should not run flushes
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            WaitFlushRequests(3, TDuration::MilliSeconds(100)));

        // Run tasks with cleanup (flushes).
        RunScheduledTasks();

        // Wait for three PBuffers flush requests.
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            WaitFlushRequests(3, TDuration::Seconds(10)));

        // Finish flush PBuffers requests with success.
        SetFlushResult(TDBGFlushResponse{.Errors{MakeError(S_OK)}}, true);

        // Wait for VChunk scheduled cleaning up (erase).
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            WaitScheduledTasks(1, TDuration::Seconds(10)));

        // Run tasks with cleanup (erases).
        RunScheduledTasks();

        UNIT_ASSERT_VALUES_EQUAL(
            true,
            WaitEraseRequests(3, TDuration::Seconds(10)));

        // Finish erase requests with success.
        SetEraseResult(TDBGEraseResponse{.Error = MakeError(S_OK)}, true);

        // Should get scheduled tasks.
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            WaitScheduledTasks(1, TDuration::MilliSeconds(100)));

        auto onStop = vchunk->Stop();
        onStop.GetValue(TDuration::Seconds(10));
    }

    Y_UNIT_TEST_F(ShouldHoldSafeBarrierForInflightWrite, TBaseFixture)
    {
        Init();

        const TBlockRange64 range = TBlockRange64::WithLength(10, 1);
        ExpectedRange = range;
        RangeData = GenerateRandomString(BlockSize * range.Size());

        // Force the next generated lsn to be 123 (LsnGenerator pre-increments).
        PartitionDirectService->LsnGenerator = 122;

        auto vchunk = std::make_shared<TVChunk>(
            Runtime->GetActorSystem(0),
            TraceService.get(),
            PartitionDirectService.get(),
            DiskDescription,
            VChunkConfig,
            DirtyMapStateProto,
            DirectBlockGroup,
            3,   // syncRequestsBatchSize
            DefaultVChunkSize);
        vchunk->Start();

        // No write yet -> no safe barrier.
        UNIT_ASSERT(
            !GetSafeBarrierOnExecutor(DirectBlockGroup->GetExecutor(), *vchunk)
                 .has_value());

        auto callContext = MakeIntrusive<TCallContext>(static_cast<ui64>(0));
        auto request =
            std::make_shared<TWriteBlocksLocalRequest>(TRequestHeaders{
                .VolumeConfig = PartitionDirectService->GetVolumeConfig(),
                .RequestId = 1,
                .Range = range});
        request->Sglist = MakeSgList();

        auto future =
            vchunk->WriteBlocksLocal(callContext, request, NWilson::TTraceId());

        // The record id is minted and registered as the write is dispatched,
        // so the safe barrier is held at the minted record id right away.
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            WaitWriteRequests(3, TDuration::Seconds(10)));
        UNIT_ASSERT_VALUES_EQUAL(
            MakeKey(123).Print(),
            GetSafeBarrierOnExecutor(DirectBlockGroup->GetExecutor(), *vchunk)
                ->Print());

        // Acknowledging the PBuffer writes does not release the barrier: the
        // entry stays inflight until it is flushed and erased.
        SetWriteResult(TDBGWriteBlocksResponse{.Error = MakeError(S_OK)}, true);
        const auto& result = future.GetValue(TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result.Error.GetCode(),
            FormatError(result.Error));
        UNIT_ASSERT_VALUES_EQUAL(
            MakeKey(123).Print(),
            GetSafeBarrierOnExecutor(DirectBlockGroup->GetExecutor(), *vchunk)
                ->Print());

        vchunk->Stop().GetValue(TDuration::Seconds(10));
    }

    // Until the vchunk finishes restoring its dirty map from the PBuffers,
    // its pre-flush records exist only in the PBuffers and are not inflight.
    // Reporting "no constraint" (nullopt) in that window is indistinguishable
    // from an idle vchunk, so FinishPBufferCleanup would skip it and a
    // tablet-wide barrier erase could wipe the very records the restore is
    // about to return. An un-restored vchunk must report the zero record id
    // (the blocking bound) instead; cleanup skips its tick on it.
    Y_UNIT_TEST_F(
        ShouldConstrainCleanupBarrierUntilRestoreCompletes,
        TBaseFixture)
    {
        Init();

        // Keep the restore pending: the vchunk stays not-ready.
        auto neverResolvePromise =
            NThreading::NewPromise<TDBGRestoreResponse>();
        DirectBlockGroup->RestoreDBGPBuffersHandler =
            [neverResolvePromise](const auto& vChunkIndex) mutable
        {
            Y_UNUSED(vChunkIndex);
            return neverResolvePromise.GetFuture();
        };

        auto vchunk = std::make_shared<TVChunk>(
            Runtime->GetActorSystem(0),
            TraceService.get(),
            PartitionDirectService.get(),
            DiskDescription,
            VChunkConfig,
            DirtyMapStateProto,
            DirectBlockGroup,
            3,   // syncRequestsBatchSize
            DefaultVChunkSize);
        vchunk->Start();

        DrainExecutor(DirectBlockGroup->GetExecutor());
        UNIT_ASSERT_EQUAL(false, IsDirtyMapReady(*vchunk));

        const auto barrierWhileRestoring =
            GetSafeBarrierOnExecutor(DirectBlockGroup->GetExecutor(), *vchunk);

        // Resolve the restore; with an empty dirty map and restore complete
        // the vchunk stops constraining the cleanup.
        RunOnExecutor(
            DirectBlockGroup->GetExecutor(),
            [&]() -> bool
            {
                InvokeUpdateDirtyMap(
                    *vchunk,
                    TDBGRestoreResponse{.Error = MakeError(S_OK)});
                return true;
            });
        const auto barrierAfterRestore =
            GetSafeBarrierOnExecutor(DirectBlockGroup->GetExecutor(), *vchunk);

        vchunk->Stop().GetValue(TDuration::Seconds(10));

        UNIT_ASSERT_C(
            barrierWhileRestoring.has_value(),
            "vchunk with a pending restore reported 'no constraint' to the "
            "cleanup barrier gather");
        UNIT_ASSERT_VALUES_EQUAL(
            TPBufferKey{}.Print(),
            barrierWhileRestoring->Print());
        UNIT_ASSERT(!barrierAfterRestore.has_value());
    }

    Y_UNIT_TEST_F(ShouldSwitchHostToTemporaryOfflineAndBack, TBaseFixture)
    {
        Init();

        auto vchunk = std::make_shared<TVChunk>(
            Runtime->GetActorSystem(0),
            TraceService.get(),
            PartitionDirectService.get(),
            DiskDescription,
            VChunkConfig,
            DirtyMapStateProto,
            DirectBlockGroup,
            3,   // syncRequestsBatchSize
            DefaultVChunkSize);
        vchunk->Start();

        // Call SetHostState(TemporaryOffline)
        {
            TPromise<void> ready = NewPromise();
            auto wait = ready.GetFuture();
            DirectBlockGroup->GetExecutor()->ExecuteSimple(
                [vchunk,
                 ready = std::move(ready)]   //
                () mutable
                {
                    vchunk->SetHostState(0, EHostState::TemporaryOffline);
                    ready.SetValue();
                });
            wait.GetValue(TDuration::Seconds(10));
        }

        // Config should stay the same since new config is not persisted yet.
        UNIT_ASSERT_VALUES_EQUAL(
            "[DBG0/V100]{Primary,Primary,Primary,HandOff,HandOff}",
            AccessConfig(*vchunk).DebugPrint());

        // DirtyMap config should stay the same too.
        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Operational,32768};"
            "H1*{Operational,32768};"
            "H2*{Operational,32768};"
            "H3+{Disabled,0};"
            "H4+{Disabled,0};",
            AccessBlocksDirtyMap(*vchunk).DebugPrintDDiskState());

        // Reply UpdateConfig request.
        {
            UNIT_ASSERT_VALUES_EQUAL(1, ReplyUpdateRequests());
            DrainExecutor(DirectBlockGroup->GetExecutor());
        }

        // Config should be updated.
        UNIT_ASSERT_VALUES_EQUAL(
            "[DBG0/V100]{Rotten,Primary,Primary,HandOff,HandOff}",
            AccessConfig(*vchunk).DebugPrint());

        // DirtyMap config should be updated.
        UNIT_ASSERT_VALUES_EQUAL(
            "H0-{Operational,32768};"
            "H1*{Operational,32768};"
            "H2*{Operational,32768};"
            "H3+{Disabled,0};"
            "H4+{Disabled,0};",
            AccessBlocksDirtyMap(*vchunk).DebugPrintDDiskState());

        // Call SetHostState(Online)
        {
            TPromise<void> ready = NewPromise();
            auto wait = ready.GetFuture();
            DirectBlockGroup->GetExecutor()->ExecuteSimple(
                [vchunk,
                 ready = std::move(ready)]   //
                () mutable
                {
                    vchunk->SetHostState(0, EHostState::Online);
                    ready.SetValue();
                });
            wait.GetValue(TDuration::Seconds(10));
        }

        // Reply UpdateConfig request.
        {
            UNIT_ASSERT_VALUES_EQUAL(1, ReplyUpdateRequests());
            DrainExecutor(DirectBlockGroup->GetExecutor());
        }

        // Config should be updated.
        UNIT_ASSERT_VALUES_EQUAL(
            "[DBG0/V100]{Primary,Primary,Primary,HandOff,HandOff}",
            AccessConfig(*vchunk).DebugPrint());

        // DirtyMap config should be updated.
        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Operational,32768};"
            "H1*{Operational,32768};"
            "H2*{Operational,32768};"
            "H3+{Disabled,0};"
            "H4+{Disabled,0};",
            AccessBlocksDirtyMap(*vchunk).DebugPrintDDiskState());

        auto onStop = vchunk->Stop();
        onStop.GetValue(TDuration::Seconds(10));
    }

    Y_UNIT_TEST_F(ShouldAppendHost, TBaseFixture)
    {
        Init();

        auto vchunk = std::make_shared<TVChunk>(
            Runtime->GetActorSystem(0),
            TraceService.get(),
            PartitionDirectService.get(),
            DiskDescription,
            VChunkConfig,
            DirtyMapStateProto,
            DirectBlockGroup,
            3,
            DefaultVChunkSize);
        vchunk->Start();

        UNIT_ASSERT_VALUES_EQUAL(
            DirectBlockGroupHostCount,
            AccessConfig(*vchunk).GetHostCount());

        {
            TPromise<void> ready = NewPromise();
            auto wait = ready.GetFuture();
            DirectBlockGroup->GetExecutor()->ExecuteSimple(
                [vchunk,
                 ready = std::move(ready)]   //
                () mutable
                {
                    vchunk->UpdateHostCount(DirectBlockGroupHostCount + 1);
                    ready.SetValue();
                });
            wait.GetValue(TDuration::Seconds(10));
        }

        UNIT_ASSERT_VALUES_EQUAL(
            DirectBlockGroupHostCount,
            AccessConfig(*vchunk).GetHostCount());

        // Reply UpdateConfig request.
        {
            UNIT_ASSERT_VALUES_EQUAL(1, ReplyUpdateRequests());
            DrainExecutor(DirectBlockGroup->GetExecutor());
        }

        UNIT_ASSERT_VALUES_EQUAL(
            DirectBlockGroupHostCount + 1,
            AccessConfig(*vchunk).GetHostCount());
        UNIT_ASSERT_VALUES_EQUAL(
            "[DBG0/V100]{Primary,Primary,Primary,HandOff,HandOff,HandOff}",
            AccessConfig(*vchunk).DebugPrint());
        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Operational,32768};"
            "H1*{Operational,32768};"
            "H2*{Operational,32768};"
            "H3+{Disabled,0};"
            "H4+{Disabled,0};"
            "H5+{Disabled,0};",
            AccessBlocksDirtyMap(*vchunk).DebugPrintDDiskState());

        auto onStop = vchunk->Stop();
        onStop.GetValue(TDuration::Seconds(10));
    }

    Y_UNIT_TEST_F(
        ShouldDemoteDisabledDDiskWhenHealthyQuorumExists,
        TBaseFixture)
    {
        Init();

        VChunkConfig.PromoteHost(3);
        VChunkConfig.SetWatermark(3, std::nullopt);

        auto vchunk = std::make_shared<TVChunk>(
            Runtime->GetActorSystem(0),
            TraceService.get(),
            PartitionDirectService.get(),
            DiskDescription,
            VChunkConfig,
            DirtyMapStateProto,
            DirectBlockGroup,
            3,
            DefaultVChunkSize);
        vchunk->Start();

        RunOnExecutor(
            DirectBlockGroup->GetExecutor(),
            [&]
            {
                vchunk->SetHostState(0, EHostState::TemporaryOffline);
                return true;
            })
            .GetValue(TDuration::Seconds(10));

        // First persist only disables H0. H1-H3 already form a healthy quorum.
        UNIT_ASSERT_VALUES_EQUAL(1, ReplyUpdateRequests());
        DrainExecutor(DirectBlockGroup->GetExecutor());
        UNIT_ASSERT_VALUES_EQUAL(
            EHostRole::Primary,
            AccessConfig(*vchunk).GetDDiskRole(0));

        // Applying that config schedules a second persist which removes the
        // now redundant disabled DDisk.
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            PartitionDirectService->UpdateConfigRequests.size());
        UNIT_ASSERT_VALUES_EQUAL(
            EHostRole::None,
            PartitionDirectService->UpdateConfigRequests.front()
                .Config.GetDDiskRole(0));

        UNIT_ASSERT_VALUES_EQUAL(1, ReplyUpdateRequests());
        DrainExecutor(DirectBlockGroup->GetExecutor());
        UNIT_ASSERT_VALUES_EQUAL(
            EHostRole::None,
            AccessConfig(*vchunk).GetDDiskRole(0));
        UNIT_ASSERT_VALUES_EQUAL(
            QuorumDirectBlockGroupHostCount,
            AccessConfig(*vchunk).GetDDisks().Count());

        vchunk->Stop().GetValue(TDuration::Seconds(10));
    }

    Y_UNIT_TEST_F(ShouldKeepWatermarkWhenCopyFails, TBaseFixture)
    {
        Init();

        VChunkConfig.PromoteHost(3);
        VChunkConfig.DisableHost(0);

        auto vchunk = std::make_shared<TVChunk>(
            Runtime->GetActorSystem(0),
            TraceService.get(),
            PartitionDirectService.get(),
            DiskDescription,
            VChunkConfig,
            DirtyMapStateProto,
            DirectBlockGroup,
            3,
            DefaultVChunkSize);
        vchunk->Start();
        DrainExecutor(DirectBlockGroup->GetExecutor());

        RunOnExecutor(
            DirectBlockGroup->GetExecutor(),
            [&]
            {
                InvokeOnCopyComplete(
                    *vchunk,
                    3,
                    TDDiskDataCopier::EResult::Error);
                return true;
            })
            .GetValue(TDuration::Seconds(10));

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            PartitionDirectService->UpdateConfigRequests.size());
        UNIT_ASSERT_VALUES_EQUAL(0, *AccessConfig(*vchunk).GetWatermark(3));

        vchunk->Stop().GetValue(TDuration::Seconds(10));
    }

    Y_UNIT_TEST_F(ShouldSwitchHostToOfflineAndBack, TBaseFixture)
    {
        Init();

        bool isHostOffline = false;
        DirectBlockGroup->ReadBlocksFromDDiskHandler = [&]   //
            (ui32 vChunkIndex,
             THostIndex hostIndex,
             TBlockRange64 range,
             const TGuardedSgList& guardedSglist,
             const NWilson::TTraceId& traceId)
        {
            Y_UNUSED(vChunkIndex);

            Y_UNUSED(range);
            Y_UNUSED(guardedSglist);
            Y_UNUSED(traceId);

            // Should not read from offline host when host disabled.
            if (isHostOffline) {
                UNIT_ASSERT_VALUES_UNEQUAL(0, hostIndex);
            }

            auto promise = NewPromise<TDBGReadBlocksResponse>();
            auto future = promise.GetFuture();
            auto guard = TGuard(PromisesGuard);
            ReadPromises.push_back(std::move(promise));
            return future;
        };

        DirectBlockGroup->WriteBlocksToDDiskHandler = [&]   //
            (ui32 vChunkIndex,
             THostIndex hostIndex,
             TBlockRange64 range,
             const TGuardedSgList& guardedSglist,
             const NWilson::TTraceId& traceId)
        {
            Y_UNUSED(vChunkIndex);

            Y_UNUSED(range);
            Y_UNUSED(guardedSglist);
            Y_UNUSED(traceId);

            // Should write to fresh host.
            UNIT_ASSERT_VALUES_EQUAL(3, hostIndex);

            auto promise = NewPromise<TDBGWriteBlocksResponse>();
            auto future = promise.GetFuture();
            auto guard = TGuard(PromisesGuard);
            WritePromises.push_back(std::move(promise));
            return future;
        };

        auto vchunk = std::make_shared<TVChunk>(
            Runtime->GetActorSystem(0),
            TraceService.get(),
            PartitionDirectService.get(),
            DiskDescription,
            VChunkConfig,
            DirtyMapStateProto,
            DirectBlockGroup,
            3,   // syncRequestsBatchSize
            DefaultVChunkSize);
        vchunk->Start();

        // Call SetHostState(Offline)
        {
            TPromise<void> ready = NewPromise();
            auto wait = ready.GetFuture();
            DirectBlockGroup->GetExecutor()->ExecuteSimple(
                [vchunk,
                 ready = std::move(ready)]   //
                () mutable
                {
                    vchunk->SetHostState(0, EHostState::Offline);
                    ready.SetValue();
                });
            wait.GetValue(TDuration::Seconds(10));
            isHostOffline = true;
        }

        // Config should stay the same since new config is not persisted yet.
        UNIT_ASSERT_VALUES_EQUAL(
            "[DBG0/V100]{Primary,Primary,Primary,HandOff,HandOff}",
            AccessConfig(*vchunk).DebugPrint());

        // DirtyMap config should stay the same too.
        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Operational,32768};"
            "H1*{Operational,32768};"
            "H2*{Operational,32768};"
            "H3+{Disabled,0};"
            "H4+{Disabled,0};",
            AccessBlocksDirtyMap(*vchunk).DebugPrintDDiskState());

        // Reply UpdateConfig request.
        {
            UNIT_ASSERT_VALUES_EQUAL(1, ReplyUpdateRequests());
            DrainExecutor(DirectBlockGroup->GetExecutor());
        }

        // Config should be updated.
        UNIT_ASSERT_VALUES_EQUAL(
            "[DBG0/V100]{Rotten,Primary,Primary,Fresh,HandOff}",
            AccessConfig(*vchunk).DebugPrint());

        // DirtyMap config should be updated.
        UNIT_ASSERT_VALUES_EQUAL(
            "H0-{Operational,32768};"
            "H1*{Operational,32768};"
            "H2*{Operational,32768};"
            "H3*{Fresh+,0};"
            "H4+{Disabled,0};",
            AccessBlocksDirtyMap(*vchunk).DebugPrintDDiskState());

        // Call SetHostState(Online)
        {
            TPromise<void> ready = NewPromise();
            auto wait = ready.GetFuture();
            DirectBlockGroup->GetExecutor()->ExecuteSimple(
                [vchunk,
                 ready = std::move(ready)]   //
                () mutable
                {
                    vchunk->SetHostState(0, EHostState::Online);
                    ready.SetValue();
                });
            wait.GetValue(TDuration::Seconds(10));
            isHostOffline = false;
        }

        // Reply UpdateConfig request.
        {
            UNIT_ASSERT_VALUES_EQUAL(1, ReplyUpdateRequests());
            DrainExecutor(DirectBlockGroup->GetExecutor());
        }

        // Config should be updated.
        UNIT_ASSERT_VALUES_EQUAL(
            "[DBG0/V100]{Primary,Primary,Primary,Fresh,HandOff}",
            AccessConfig(*vchunk).DebugPrint());

        // DirtyMap config should be updated.
        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Operational,32768};"
            "H1*{Operational,32768};"
            "H2*{Operational,32768};"
            "H3*{Fresh+,0};"
            "H4+{Disabled,0};",
            AccessBlocksDirtyMap(*vchunk).DebugPrintDDiskState());

        // Execute copier reads and writes.
        for (size_t i = 0; i < VChunkBlockCount / BlocksPerCopy; ++i) {
            WaitReadRequests(1, TDuration::Seconds(10));
            SetReadResult({.Error = MakeError(S_OK)}, true);

            WaitWriteRequests(1, TDuration::Seconds(10));
            SetWriteResult({.Error = MakeError(S_OK)}, true);
        }

        // Waiting for the copying to be completed.
        {
            DrainExecutor(DirectBlockGroup->GetExecutor());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                PartitionDirectService->UpdateConfigRequests.size());
            UNIT_ASSERT_VALUES_EQUAL(
                CopyProgressSaveInterval,
                *PartitionDirectService->UpdateConfigRequests.front()
                     .Config.GetWatermark(3));
            UNIT_ASSERT_VALUES_EQUAL(1, ReplyUpdateRequests());
            DrainExecutor(DirectBlockGroup->GetExecutor());

            UNIT_ASSERT_VALUES_EQUAL(
                1,
                PartitionDirectService->UpdateConfigRequests.size());
            UNIT_ASSERT(!PartitionDirectService->UpdateConfigRequests.front()
                             .Config.GetWatermark(3)
                             .has_value());
            UNIT_ASSERT_VALUES_EQUAL(1, ReplyUpdateRequests());
            DrainExecutor(DirectBlockGroup->GetExecutor());
        }

        // Config should be updated.
        UNIT_ASSERT_VALUES_EQUAL(
            "[DBG0/V100]{Primary,Primary,Primary,Primary,HandOff}",
            AccessConfig(*vchunk).DebugPrint());

        // DirtyMap config should be updated.
        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Operational,32768};"
            "H1*{Operational,32768};"
            "H2*{Operational,32768};"
            "H3*{Operational,32768};"
            "H4+{Disabled,0};",
            AccessBlocksDirtyMap(*vchunk).DebugPrintDDiskState());

        auto onStop = vchunk->Stop();
        onStop.GetValue(TDuration::Seconds(10));
    }

    // ReadBlocksLocal / WriteBlocksLocal are blocked while DirtyMapReady
    // is false and resume after UpdateDirtyMap fires on the executor thread.
    Y_UNIT_TEST_F(ShouldBlockLocalIoUntilDirtyMapReady, TBaseFixture)
    {
        Init();

        // Override the restore handler to keep DirtyMapReady == false: the
        // future is never resolved, so the vchunk subscription callback never
        // fires during the "before" phase of the test.
        auto neverResolvePromise =
            NThreading::NewPromise<TDBGRestoreResponse>();
        DirectBlockGroup->RestoreDBGPBuffersHandler =
            [neverResolvePromise](const auto& vChunkIndex) mutable
        {
            Y_UNUSED(vChunkIndex);
            return neverResolvePromise.GetFuture();
        };

        const TBlockRange64 range = TBlockRange64::WithLength(0, 1);
        ExpectedRange = range;
        RangeData = GenerateRandomString(BlockSize * range.Size());

        auto vchunk = std::make_shared<TVChunk>(
            Runtime->GetActorSystem(0),
            TraceService.get(),
            PartitionDirectService.get(),
            DiskDescription,
            VChunkConfig,
            DirtyMapStateProto,
            DirectBlockGroup,
            3,   // syncRequestsBatchSize
            DefaultVChunkSize);
        vchunk->Start();

        // Drain executor: DoStart has subscribed to the restore future; since
        // that future is pending, DirtyMapReady stays false.
        DrainExecutor(DirectBlockGroup->GetExecutor());
        UNIT_ASSERT_EQUAL(false, IsDirtyMapReady(*vchunk));

        // Submit write - coroutine suspends on WaitFor(DirtyMapReadyFuture).
        auto callContext = MakeIntrusive<TCallContext>(static_cast<ui64>(0));
        auto writeRequest =
            std::make_shared<TWriteBlocksLocalRequest>(TRequestHeaders{
                .VolumeConfig = PartitionDirectService->GetVolumeConfig(),
                .RequestId = 1,
                .Range = range});
        writeRequest->Sglist = MakeSgList();
        auto writeFuture = vchunk->WriteBlocksLocal(
            callContext,
            writeRequest,
            NWilson::TTraceId());

        // Submit read - also suspends.
        TString readBuffer(BlockSize * range.Size(), '\0');
        auto readRequest =
            std::make_shared<TReadBlocksLocalRequest>(TRequestHeaders{
                .VolumeConfig = PartitionDirectService->GetVolumeConfig(),
                .RequestId = 2,
                .Range = range});
        readRequest->Sglist = TGuardedSgList(
            TSgList{TBlockDataRef{readBuffer.data(), readBuffer.size()}});
        auto readFuture = vchunk->ReadBlocksLocal(
            callContext,
            readRequest,
            NWilson::TTraceId());

        // Drain: both coroutines are now suspended inside WaitFor
        DrainExecutor(DirectBlockGroup->GetExecutor());
        UNIT_ASSERT(!writeFuture.HasValue());
        UNIT_ASSERT(!readFuture.HasValue());

        // resolves DirtyMapReady promise and unblocks both suspended coroutines
        RunOnExecutor(
            DirectBlockGroup->GetExecutor(),
            [&]() -> bool
            {
                InvokeUpdateDirtyMap(
                    *vchunk,
                    TDBGRestoreResponse{.Error = MakeError(S_OK)});
                return true;
            });

        // Write resumed: wait for three PBuffer write requests and complete
        // them.
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            WaitWriteRequests(3, TDuration::Seconds(10)));
        SetWriteResult(TDBGWriteBlocksResponse{.Error = MakeError(S_OK)}, true);

        const auto& writeResult = writeFuture.GetValue(TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            writeResult.Error.GetCode(),
            FormatError(writeResult.Error));

        // Read resumed: wait for one DDisk read and complete it.
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            WaitReadRequests(1, TDuration::Seconds(10)));
        SetReadResult(TDBGReadBlocksResponse{.Error = MakeError(S_OK)}, true);

        const auto& readResult = readFuture.GetValue(TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            readResult.Error.GetCode(),
            FormatError(readResult.Error));

        auto onStop = vchunk->Stop();
        onStop.GetValue(TDuration::Seconds(10));
    }

    // A second UpdateDirtyMap call (resync path) must
    // not try to SetValue on an already-resolved DirtyMapReady promise (which
    // would raise an exception), and operations issued afterwards must complete
    // immediately.
    Y_UNIT_TEST_F(ShouldNotRecreateDirtyMapPromiseOnResync, TBaseFixture)
    {
        Init();

        // Default handler returns an immediately-resolved future, so
        // DirtyMapReady becomes true inside DoStart.

        const TBlockRange64 range = TBlockRange64::WithLength(0, 1);
        ExpectedRange = range;
        RangeData = GenerateRandomString(BlockSize * range.Size());

        auto vchunk = std::make_shared<TVChunk>(
            Runtime->GetActorSystem(0),
            TraceService.get(),
            PartitionDirectService.get(),
            DiskDescription,
            VChunkConfig,
            DirtyMapStateProto,
            DirectBlockGroup,
            3,   // syncRequestsBatchSize
            DefaultVChunkSize);
        vchunk->Start();

        // Drain: the restore callback fires synchronously (future was already
        // resolved) and sets DirtyMapReady = true
        DrainExecutor(DirectBlockGroup->GetExecutor());
        UNIT_ASSERT_EQUAL(true, IsDirtyMapReady(*vchunk));

        // This must NOT call SetValue on the already-resolved one-shot promise
        RunOnExecutor(
            DirectBlockGroup->GetExecutor(),
            [&]() -> bool
            {
                InvokeUpdateDirtyMap(
                    *vchunk,
                    TDBGRestoreResponse{.Error = MakeError(S_OK)});
                return true;
            });

        // Operations issued after the second update must not block
        auto callContext = MakeIntrusive<TCallContext>(static_cast<ui64>(0));
        auto writeRequest =
            std::make_shared<TWriteBlocksLocalRequest>(TRequestHeaders{
                .VolumeConfig = PartitionDirectService->GetVolumeConfig(),
                .RequestId = 1,
                .Range = range});
        writeRequest->Sglist = MakeSgList();
        auto writeFuture = vchunk->WriteBlocksLocal(
            callContext,
            writeRequest,
            NWilson::TTraceId());

        UNIT_ASSERT_VALUES_EQUAL(
            true,
            WaitWriteRequests(3, TDuration::Seconds(10)));
        SetWriteResult(TDBGWriteBlocksResponse{.Error = MakeError(S_OK)}, true);

        const auto& writeResult = writeFuture.GetValue(TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            writeResult.Error.GetCode(),
            FormatError(writeResult.Error));

        auto onStop = vchunk->Stop();
        onStop.GetValue(TDuration::Seconds(10));
    }

    // DoPersistDirtyMap must forward the dirty map state to
    // IPartitionDirectService::UpdateDirtyMapState (carrying the vchunk index
    // and the current state generation) and, once that future resolves, run
    // OnDirtyMapPersisted which clears the in-flight flag and acknowledges the
    // generation to the dirty map (NeedPersist() becomes false).
    Y_UNIT_TEST_F(ShouldPersistDirtyMapState, TBaseFixture)
    {
        // Host 3 is fresh; a write above its watermark populates its Ahead
        // field so a flush bumps the persist generation.
        VChunkConfig.PromoteHost(3);
        VChunkConfig.SetWatermark(3, BlockSize * 5);

        Init();

        auto vchunk = std::make_shared<TVChunk>(
            Runtime->GetActorSystem(0),
            TraceService.get(),
            PartitionDirectService.get(),
            DiskDescription,
            VChunkConfig,
            DirtyMapStateProto,
            DirectBlockGroup,
            3,   // syncRequestsBatchSize
            DefaultVChunkSize);
        vchunk->Start();
        DrainExecutor(DirectBlockGroup->GetExecutor());

        // Drive the dirty map into a "need persist" state and trigger persist,
        // all on the executor thread the vchunk state is confined to.
        ui32 expectedGeneration = 0;
        RunOnExecutor(
            DirectBlockGroup->GetExecutor(),
            [&]() -> bool
            {
                auto& dirtyMap = AccessBlocksDirtyMap(*vchunk);
                MakeDirtyMapNeedPersist(dirtyMap);
                UNIT_ASSERT_VALUES_EQUAL(true, dirtyMap.NeedPersist());
                expectedGeneration = dirtyMap.GetCurrentGeneration();

                InvokePersistDirtyMap(*vchunk);
                return true;
            })
            .GetValue(TDuration::Seconds(10));

        // A single UpdateDirtyMapState request must have been issued with the
        // vchunk index and captured generation; the vchunk marks itself busy.
        UNIT_ASSERT_VALUES_EQUAL(
            1u,
            PartitionDirectService->UpdateDirtyMapStateRequests.size());
        const auto& request =
            PartitionDirectService->UpdateDirtyMapStateRequests.front();
        UNIT_ASSERT_VALUES_EQUAL(FixtureVChunkIndex, request.VChunkIndex);
        UNIT_ASSERT_VALUES_EQUAL(
            expectedGeneration,
            request.Proto.GetStateGeneration());
        UNIT_ASSERT_VALUES_EQUAL(true, IsDirtyMapStatePersisting(*vchunk));

        // Complete the persist; OnDirtyMapPersisted runs on the callback.
        UNIT_ASSERT_VALUES_EQUAL(1, ReplyUpdateDirtyMapStateRequests());
        DrainExecutor(DirectBlockGroup->GetExecutor());

        // Flag cleared and the generation acknowledged to the dirty map.
        UNIT_ASSERT_VALUES_EQUAL(false, IsDirtyMapStatePersisting(*vchunk));
        RunOnExecutor(
            DirectBlockGroup->GetExecutor(),
            [&]() -> bool
            {
                UNIT_ASSERT_VALUES_EQUAL(
                    false,
                    AccessBlocksDirtyMap(*vchunk).NeedPersist());
                return true;
            })
            .GetValue(TDuration::Seconds(10));

        auto onStop = vchunk->Stop();
        onStop.GetValue(TDuration::Seconds(10));
    }

    // A second DoPersistDirtyMap call while a persist is already in flight must
    // be a no-op: no duplicate UpdateDirtyMapState request is issued.
    Y_UNIT_TEST_F(
        ShouldNotPersistDirtyMapStateWhileAlreadyPersisting,
        TBaseFixture)
    {
        VChunkConfig.PromoteHost(3);
        VChunkConfig.SetWatermark(3, BlockSize * 5);

        Init();

        auto vchunk = std::make_shared<TVChunk>(
            Runtime->GetActorSystem(0),
            TraceService.get(),
            PartitionDirectService.get(),
            DiskDescription,
            VChunkConfig,
            DirtyMapStateProto,
            DirectBlockGroup,
            3,   // syncRequestsBatchSize
            DefaultVChunkSize);
        vchunk->Start();
        DrainExecutor(DirectBlockGroup->GetExecutor());

        RunOnExecutor(
            DirectBlockGroup->GetExecutor(),
            [&]() -> bool
            {
                auto& dirtyMap = AccessBlocksDirtyMap(*vchunk);
                MakeDirtyMapNeedPersist(dirtyMap);

                // First call starts a persist; second call must be ignored
                // while it is still in flight.
                InvokePersistDirtyMap(*vchunk);
                InvokePersistDirtyMap(*vchunk);
                return true;
            })
            .GetValue(TDuration::Seconds(10));

        UNIT_ASSERT_VALUES_EQUAL(
            1u,
            PartitionDirectService->UpdateDirtyMapStateRequests.size());

        UNIT_ASSERT_VALUES_EQUAL(1, ReplyUpdateDirtyMapStateRequests());
        DrainExecutor(DirectBlockGroup->GetExecutor());
        UNIT_ASSERT_VALUES_EQUAL(false, IsDirtyMapStatePersisting(*vchunk));

        auto onStop = vchunk->Stop();
        onStop.GetValue(TDuration::Seconds(10));
    }

    // With no dirty map changes NeedPersist() is false, so DoPersistDirtyMap
    // must not issue any UpdateDirtyMapState request.
    Y_UNIT_TEST_F(ShouldNotPersistDirtyMapStateWhenNothingChanged, TBaseFixture)
    {
        Init();

        auto vchunk = std::make_shared<TVChunk>(
            Runtime->GetActorSystem(0),
            TraceService.get(),
            PartitionDirectService.get(),
            DiskDescription,
            VChunkConfig,
            DirtyMapStateProto,
            DirectBlockGroup,
            3,   // syncRequestsBatchSize
            DefaultVChunkSize);
        vchunk->Start();
        DrainExecutor(DirectBlockGroup->GetExecutor());

        RunOnExecutor(
            DirectBlockGroup->GetExecutor(),
            [&]() -> bool
            {
                UNIT_ASSERT_VALUES_EQUAL(
                    false,
                    AccessBlocksDirtyMap(*vchunk).NeedPersist());
                InvokePersistDirtyMap(*vchunk);
                return true;
            })
            .GetValue(TDuration::Seconds(10));

        UNIT_ASSERT_VALUES_EQUAL(
            0u,
            PartitionDirectService->UpdateDirtyMapStateRequests.size());
        UNIT_ASSERT_VALUES_EQUAL(false, IsDirtyMapStatePersisting(*vchunk));

        auto onStop = vchunk->Stop();
        onStop.GetValue(TDuration::Seconds(10));
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

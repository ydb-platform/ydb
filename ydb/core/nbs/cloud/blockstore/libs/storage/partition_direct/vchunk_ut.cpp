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

// Drives dirtyMap into a state where its Behind state needs persistence.
// Must run on the executor thread.
void MakeDirtyMapNeedPersist(TBlocksDirtyMap& dirtyMap)
{
    THostMask requested;
    requested.Set(0);
    requested.Set(1);
    requested.Set(2);
    requested.Set(3);

    const auto pBufferKey = MakeKey(100);
    const auto range = TBlockRange16::WithLength(10, 10);
    dirtyMap.RegisterInflightWrite(pBufferKey, range);
    dirtyMap.WriteFinished(pBufferKey, range, requested, requested);

    auto flushHints = dirtyMap.MakeFlushHint(1);
    Y_ABORT_UNLESS(!flushHints.Empty());
    for (const auto& [route, hint]: flushHints.GetAllHints()) {
        dirtyMap.FlushFinished(route, MakePBufferKeys(hint.Segments), {});
    }
}

// Adds a successful write that is ready to be flushed. Must run on the
// executor thread.
void MakeDirtyMapNeedFlush(TBlocksDirtyMap& dirtyMap)
{
    THostMask requested;
    requested.Set(0);
    requested.Set(1);
    requested.Set(2);
    requested.Set(3);

    dirtyMap.RegisterInflightWrite(
        MakeKey(100),
        TBlockRange16::WithLength(10, 10));
    dirtyMap.WriteFinished(
        MakeKey(100),
        TBlockRange16::WithLength(10, 10),
        requested,
        requested);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVChunkTest)
{
    Y_UNIT_TEST_F(ShouldScheduleCleanup, TBaseFixture)
    {
        Init();

        const auto range = TBlockRange16::WithLength(10, 1);
        ExpectedRange = range;
        RangeData = GenerateRandomString(BlockSize * range.Size());

        auto callContext = MakeIntrusive<TCallContext>(static_cast<ui64>(0));
        auto request =
            std::make_shared<TWriteBlocksLocalRequest>(TRequestHeaders{
                .VolumeConfig = PartitionDirectService->GetVolumeConfig(),
                .RequestId = 1,
                .Range = ConvertRangeSafe<TBlockRange64>(range)});
        request->Sglist = MakeSgList();

        auto vchunk = std::make_shared<TVChunk>(
            Runtime->GetActorSystem(0),
            TraceService.get(),
            PartitionDirectService.get(),
            DiskDescription,
            VChunkConfig,
            true,
            DirtyMapStateProto,
            DirectBlockGroup,
            3,   // syncRequestsBatchSize
            DefaultBlockSize,
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

    Y_UNIT_TEST_F(ShouldPersistTouchedBeforeFirstFlush, TBaseFixture)
    {
        Init();

        auto vchunk = std::make_shared<TVChunk>(
            Runtime->GetActorSystem(0),
            TraceService.get(),
            PartitionDirectService.get(),
            DiskDescription,
            VChunkConfig,
            false,
            DirtyMapStateProto,
            DirectBlockGroup,
            1,   // syncRequestsBatchSize
            DefaultBlockSize,
            DefaultVChunkSize);
        vchunk->Start();
        DrainExecutor(DirectBlockGroup->GetExecutor());

        RunOnExecutor(
            DirectBlockGroup->GetExecutor(),
            [&]() -> bool
            {
                MakeDirtyMapNeedFlush(AccessBlocksDirtyMap(*vchunk));
                InvokeFlush(*vchunk);
                return true;
            })
            .GetValue(TDuration::Seconds(10));

        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui32>{FixtureVChunkIndex},
            PartitionDirectService->TouchedVChunkIndices);
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            WaitFlushRequests(1, TDuration::MilliSeconds(100)));

        DrainExecutor(DirectBlockGroup->GetExecutor());
        RunOnExecutor(
            DirectBlockGroup->GetExecutor(),
            [&]() -> bool
            {
                InvokeFlush(*vchunk);
                return true;
            })
            .GetValue(TDuration::Seconds(10));

        UNIT_ASSERT_VALUES_EQUAL(
            true,
            WaitFlushRequests(1, TDuration::Seconds(10)));

        SetFlushResult(TDBGFlushResponse{.Errors{MakeError(S_OK)}}, true);
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            WaitEraseRequests(1, TDuration::Seconds(10)));
        SetEraseResult(TDBGEraseResponse{.Error = MakeError(S_OK)}, true);

        auto onStop = vchunk->Stop();
        onStop.GetValue(TDuration::Seconds(10));
    }

    Y_UNIT_TEST_F(ShouldHoldSafeBarrierForInflightWrite, TBaseFixture)
    {
        Init();

        const auto range = TBlockRange16::WithLength(10, 1);
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
            false,
            DirtyMapStateProto,
            DirectBlockGroup,
            3,   // syncRequestsBatchSize
            DefaultBlockSize,
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
                .Range = ConvertRangeSafe<TBlockRange64>(range)});
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

        // Acknowledging the PBuffer writes does not release the restore
        // barrier: the entry stays inflight until it is flushed and erased.
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

    Y_UNIT_TEST_F(ShouldTrackDiskWideInflightWriteCount, TBaseFixture)
    {
        Init();

        const auto range = TBlockRange16::WithLength(10, 1);
        ExpectedRange = range;
        RangeData = GenerateRandomString(BlockSize * range.Size());

        auto vchunk = std::make_shared<TVChunk>(
            Runtime->GetActorSystem(0),
            TraceService.get(),
            PartitionDirectService.get(),
            DiskDescription,
            VChunkConfig,
            true,
            DirtyMapStateProto,
            DirectBlockGroup,
            // Keep the batch above the three overlapping writes so completing
            // them does not start flush executors. Those would abort when the
            // fixture is destroyed with pending TDBGFlushResponse promises.
            100,   // syncRequestsBatchSize
            DefaultBlockSize,
            DefaultVChunkSize);
        vchunk->Start();

        auto startWrite = [&]
        {
            auto callContext =
                MakeIntrusive<TCallContext>(static_cast<ui64>(0));
            auto request =
                std::make_shared<TWriteBlocksLocalRequest>(TRequestHeaders{
                    .VolumeConfig = PartitionDirectService->GetVolumeConfig(),
                    .RequestId = 1,
                    .Range = ConvertRangeSafe<TBlockRange64>(range)});
            request->Sglist = MakeSgList();
            return vchunk->WriteBlocksLocal(
                callContext,
                request,
                NWilson::TTraceId());
        };

        const auto first = startWrite();
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            WaitWriteRequests(3, TDuration::Seconds(10)));
        UNIT_ASSERT_VALUES_EQUAL(
            1u,
            PartitionDirectService->InflightWriteCount);

        const auto second = startWrite();
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            WaitWriteRequests(6, TDuration::Seconds(10)));
        UNIT_ASSERT_VALUES_EQUAL(
            2u,
            PartitionDirectService->InflightWriteCount);

        const auto third = startWrite();
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            WaitWriteRequests(9, TDuration::Seconds(10)));
        UNIT_ASSERT_VALUES_EQUAL(
            3u,
            PartitionDirectService->InflightWriteCount);

        SetWriteResult(TDBGWriteBlocksResponse{.Error = MakeError(S_OK)}, true);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            first.GetValue(TDuration::Seconds(10)).Error.GetCode(),
            "first write");
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            second.GetValue(TDuration::Seconds(10)).Error.GetCode(),
            "second write");
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            third.GetValue(TDuration::Seconds(10)).Error.GetCode(),
            "third write");
        UNIT_ASSERT_VALUES_EQUAL(
            0u,
            PartitionDirectService->InflightWriteCount);

        vchunk->Stop().GetValue(TDuration::Seconds(10));
    }

    // Until the vchunk finishes restoring its dirty map from the PBuffers,
    // its pre-flush records exist only in the PBuffers and are not inflight.
    // Reporting "no constraint" (nullopt) in that window is indistinguishable
    // from an idle vchunk, so the DBG cleanup would skip it and a barrier
    // erase could wipe the very records the restore is about to return. An
    // un-restored vchunk must report the zero record id (the blocking bound)
    // instead; cleanup skips its tick on it.
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
            false,
            DirtyMapStateProto,
            DirectBlockGroup,
            3,   // syncRequestsBatchSize
            DefaultBlockSize,
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

    Y_UNIT_TEST_F(ShouldSerializeQueuedConfigUpdates, TBaseFixture)
    {
        Init();

        auto vchunk = std::make_shared<TVChunk>(
            Runtime->GetActorSystem(0),
            TraceService.get(),
            PartitionDirectService.get(),
            DiskDescription,
            VChunkConfig,
            false,
            DirtyMapStateProto,
            DirectBlockGroup,
            3,
            DefaultBlockSize,
            DefaultVChunkSize);
        vchunk->Start();

        RunOnExecutor(
            DirectBlockGroup->GetExecutor(),
            [&]
            {
                // A no-op update must not block the following requests.
                vchunk->SetHostState(1, EHostState::Online);
                vchunk->SetHostState(0, EHostState::TemporaryOffline);
                vchunk->SetHostState(0, EHostState::Online);
                return true;
            })
            .GetValue(TDuration::Seconds(10));

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            PartitionDirectService->UpdateConfigRequests.size());
        UNIT_ASSERT_VALUES_EQUAL(1, ReplyUpdateRequests());
        DrainExecutor(DirectBlockGroup->GetExecutor());

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            PartitionDirectService->UpdateConfigRequests.size());
        UNIT_ASSERT_VALUES_EQUAL(1, ReplyUpdateRequests());
        DrainExecutor(DirectBlockGroup->GetExecutor());

        UNIT_ASSERT(AccessConfig(*vchunk).GetEnabledDDisks().Get(0));
        vchunk->Stop().GetValue(TDuration::Seconds(10));
    }

    Y_UNIT_TEST_F(ShouldStartQueuedConfigAfterDirtyMapPersist, TBaseFixture)
    {
        VChunkConfig.PromoteHost(3);
        Init();

        auto vchunk = std::make_shared<TVChunk>(
            Runtime->GetActorSystem(0),
            TraceService.get(),
            PartitionDirectService.get(),
            DiskDescription,
            VChunkConfig,
            true,
            DirtyMapStateProto,
            DirectBlockGroup,
            3,
            DefaultBlockSize,
            DefaultVChunkSize);
        vchunk->Start();
        DrainExecutor(DirectBlockGroup->GetExecutor());

        RunOnExecutor(
            DirectBlockGroup->GetExecutor(),
            [&]
            {
                auto& dirtyMap = AccessBlocksDirtyMap(*vchunk);
                dirtyMap.SetReadablePrefixDebugOnly(3, BlockSize * 5);
                MakeDirtyMapNeedPersist(dirtyMap);
                InvokeStartPersist(*vchunk);

                // The config must wait for the in-flight dirty map persist.
                vchunk->SetHostState(3, EHostState::TemporaryOffline);
                return true;
            })
            .GetValue(TDuration::Seconds(10));

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            PartitionDirectService->UpdateDirtyMapStateRequests.size());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            PartitionDirectService->UpdateConfigRequests.size());

        UNIT_ASSERT_VALUES_EQUAL(1, ReplyUpdateDirtyMapStateRequests());
        DrainExecutor(DirectBlockGroup->GetExecutor());

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            PartitionDirectService->UpdateConfigRequests.size());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            PartitionDirectService->UpdateDirtyMapStateRequests.size());

        UNIT_ASSERT_VALUES_EQUAL(1, ReplyUpdateRequests());
        DrainExecutor(DirectBlockGroup->GetExecutor());

        UNIT_ASSERT(!AccessConfig(*vchunk).GetEnabledDDisks().Get(3));
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            PartitionDirectService->UpdateConfigRequests.size());
        UNIT_ASSERT_VALUES_EQUAL(1, ReplyUpdateRequests());
        DrainExecutor(DirectBlockGroup->GetExecutor());

        UNIT_ASSERT_VALUES_EQUAL(false, IsPersisting(*vchunk));
        vchunk->Stop().GetValue(TDuration::Seconds(10));
    }

    Y_UNIT_TEST_F(ShouldPersistBehindChangedDuringConfigPersist, TBaseFixture)
    {
        VChunkConfig.PromoteHost(3);
        Init();

        auto vchunk = std::make_shared<TVChunk>(
            Runtime->GetActorSystem(0),
            TraceService.get(),
            PartitionDirectService.get(),
            DiskDescription,
            VChunkConfig,
            true,
            DirtyMapStateProto,
            DirectBlockGroup,
            3,
            DefaultBlockSize,
            DefaultVChunkSize);
        vchunk->Start();
        DrainExecutor(DirectBlockGroup->GetExecutor());

        RunOnExecutor(
            DirectBlockGroup->GetExecutor(),
            [&]
            {
                auto& dirtyMap = AccessBlocksDirtyMap(*vchunk);
                dirtyMap.SetReadablePrefixDebugOnly(3, BlockSize * 5);

                // The config snapshot contains H3's original Behind range.
                vchunk->SetHostState(3, EHostState::TemporaryOffline);
                UNIT_ASSERT_VALUES_EQUAL(0u, dirtyMap.GetCurrentGeneration());

                // A completed range sync changes Behind while that snapshot
                // is still being persisted.
                const auto sync =
                    dirtyMap.BeginRangeSync(3, TBlockRange16::WithLength(5, 5));
                dirtyMap.EndRangeSync(sync.SyncId, true);
                UNIT_ASSERT_VALUES_EQUAL(true, dirtyMap.NeedPersist());
                InvokeStartPersist(*vchunk);
                return true;
            })
            .GetValue(TDuration::Seconds(10));

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            PartitionDirectService->UpdateConfigRequests.size());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            PartitionDirectService->UpdateDirtyMapStateRequests.size());
        const TString configState =
            PartitionDirectService->UpdateConfigRequests.front()
                .Proto.SerializeAsString();

        UNIT_ASSERT_VALUES_EQUAL(1, ReplyUpdateRequests());
        DrainExecutor(DirectBlockGroup->GetExecutor());

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            PartitionDirectService->UpdateDirtyMapStateRequests.size());
        const TString dirtyMapState =
            PartitionDirectService->UpdateDirtyMapStateRequests.front()
                .Proto.SerializeAsString();
        UNIT_ASSERT(configState != dirtyMapState);

        const TString currentState =
            RunOnExecutor(
                DirectBlockGroup->GetExecutor(),
                [&]
                {
                    return AccessBlocksDirtyMap(*vchunk)
                        .GetStateForPersist()
                        .SerializeAsString();
                })
                .GetValue(TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(currentState, dirtyMapState);

        UNIT_ASSERT_VALUES_EQUAL(1, ReplyUpdateDirtyMapStateRequests());
        DrainExecutor(DirectBlockGroup->GetExecutor());

        // Disabling H3 schedules its demotion after the dirty map is saved.
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            PartitionDirectService->UpdateConfigRequests.size());
        UNIT_ASSERT_VALUES_EQUAL(1, ReplyUpdateRequests());
        DrainExecutor(DirectBlockGroup->GetExecutor());

        UNIT_ASSERT_VALUES_EQUAL(false, IsPersisting(*vchunk));
        vchunk->Stop().GetValue(TDuration::Seconds(10));
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
            false,
            DirtyMapStateProto,
            DirectBlockGroup,
            3,   // syncRequestsBatchSize
            DefaultBlockSize,
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
            false,
            DirtyMapStateProto,
            DirectBlockGroup,
            3,
            DefaultBlockSize,
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

        auto vchunk = std::make_shared<TVChunk>(
            Runtime->GetActorSystem(0),
            TraceService.get(),
            PartitionDirectService.get(),
            DiskDescription,
            VChunkConfig,
            false,
            DirtyMapStateProto,
            DirectBlockGroup,
            3,
            DefaultBlockSize,
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

    Y_UNIT_TEST_F(ShouldPromoteTargetDDiskForBalance, TBaseFixture)
    {
        Init();

        auto vchunk = std::make_shared<TVChunk>(
            Runtime->GetActorSystem(0),
            TraceService.get(),
            PartitionDirectService.get(),
            DiskDescription,
            VChunkConfig,
            false,
            DirtyMapStateProto,
            DirectBlockGroup,
            3,
            DefaultBlockSize,
            DefaultVChunkSize);
        vchunk->Start();

        RunOnExecutor(
            DirectBlockGroup->GetExecutor(),
            [&]
            {
                vchunk->BalanceDDisks(2, 3);
                return true;
            })
            .GetValue(TDuration::Seconds(10));

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            PartitionDirectService->UpdateConfigRequests.size());
        const auto* pendingHost =
            DirectBlockGroup->PendingDDiskAllocations.FindPtr(
                VChunkConfig.GetVChunkIndex());
        UNIT_ASSERT(pendingHost);
        UNIT_ASSERT_VALUES_EQUAL(THostIndex(3), *pendingHost);
        const auto& requestedConfig =
            PartitionDirectService->UpdateConfigRequests.front().Config;
        UNIT_ASSERT_VALUES_EQUAL(
            EHostRole::Primary,
            requestedConfig.GetDDiskRole(3));
        UNIT_ASSERT_VALUES_EQUAL(4, requestedConfig.GetDDisks().Count());

        UNIT_ASSERT_VALUES_EQUAL(1, ReplyUpdateRequests());
        DrainExecutor(DirectBlockGroup->GetExecutor());
        UNIT_ASSERT_VALUES_EQUAL(
            EHostRole::Primary,
            AccessConfig(*vchunk).GetDDiskRole(3));
        UNIT_ASSERT_VALUES_EQUAL(4, AccessConfig(*vchunk).GetDDisks().Count());
        UNIT_ASSERT(DirectBlockGroup->PendingDDiskAllocations.empty());

        // With no data to copy, the target is already healthy and H2 can be
        // demoted immediately.
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            PartitionDirectService->UpdateConfigRequests.size());
        const auto& demoteConfig =
            PartitionDirectService->UpdateConfigRequests.front().Config;
        UNIT_ASSERT_VALUES_EQUAL(
            EHostRole::Primary,
            demoteConfig.GetDDiskRole(0));
        UNIT_ASSERT_VALUES_EQUAL(EHostRole::None, demoteConfig.GetDDiskRole(2));
        UNIT_ASSERT_VALUES_EQUAL(
            EHostRole::Primary,
            demoteConfig.GetDDiskRole(3));

        UNIT_ASSERT_VALUES_EQUAL(1, ReplyUpdateRequests());
        DrainExecutor(DirectBlockGroup->GetExecutor());
        UNIT_ASSERT_VALUES_EQUAL(3, AccessConfig(*vchunk).GetDDisks().Count());

        vchunk->Stop().GetValue(TDuration::Seconds(10));
    }

    Y_UNIT_TEST_F(ShouldNotPersistConfigWhenCopyFails, TBaseFixture)
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
            false,
            DirtyMapStateProto,
            DirectBlockGroup,
            3,
            DefaultBlockSize,
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

        vchunk->Stop().GetValue(TDuration::Seconds(10));
    }

    Y_UNIT_TEST_F(ShouldNotAllocateDDiskWhenQuorumRemains, TBaseFixture)
    {
        Init();

        auto vchunk = std::make_shared<TVChunk>(
            Runtime->GetActorSystem(0),
            TraceService.get(),
            PartitionDirectService.get(),
            DiskDescription,
            VChunkConfig,
            false,
            DirtyMapStateProto,
            DirectBlockGroup,
            3,
            DefaultBlockSize,
            DefaultVChunkSize);
        vchunk->Start();

        RunOnExecutor(
            DirectBlockGroup->GetExecutor(),
            [&]
            {
                vchunk->SetHostState(3, EHostState::Offline);
                return true;
            })
            .GetValue(TDuration::Seconds(10));

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            PartitionDirectService->UpdateConfigRequests.size());
        const auto& config =
            PartitionDirectService->UpdateConfigRequests.front().Config;
        UNIT_ASSERT_VALUES_EQUAL(
            QuorumDirectBlockGroupHostCount,
            config.GetEnabledDDisks().Count());
        UNIT_ASSERT_VALUES_EQUAL(EHostRole::None, config.GetDDiskRole(4));

        UNIT_ASSERT_VALUES_EQUAL(1, ReplyUpdateRequests());
        DrainExecutor(DirectBlockGroup->GetExecutor());
        vchunk->Stop().GetValue(TDuration::Seconds(10));
    }

    Y_UNIT_TEST_F(ShouldPromoteOperationalHostWhenDDiskUntouched, TBaseFixture)
    {
        Init();

        auto vchunk = std::make_shared<TVChunk>(
            Runtime->GetActorSystem(0),
            TraceService.get(),
            PartitionDirectService.get(),
            DiskDescription,
            VChunkConfig,
            false,
            DirtyMapStateProto,
            DirectBlockGroup,
            3,
            DefaultBlockSize,
            DefaultVChunkSize);
        vchunk->Start();

        RunOnExecutor(
            DirectBlockGroup->GetExecutor(),
            [&]
            {
                vchunk->SetHostState(0, EHostState::Offline);
                return true;
            })
            .GetValue(TDuration::Seconds(10));

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            PartitionDirectService->UpdateConfigRequests.size());
        const auto& config =
            PartitionDirectService->UpdateConfigRequests.front().Config;
        UNIT_ASSERT(config.GetDDiskRole(3) == EHostRole::Primary);

        UNIT_ASSERT_VALUES_EQUAL(1, ReplyUpdateRequests());
        DrainExecutor(DirectBlockGroup->GetExecutor());
        UNIT_ASSERT_VALUES_EQUAL(
            "H0-{Operational,32768};"
            "H1*{Operational,32768};"
            "H2*{Operational,32768};"
            "H3*{Operational,32768};"
            "H4+{Disabled,0};",
            AccessBlocksDirtyMap(*vchunk).DebugPrintDDiskState());

        vchunk->Stop().GetValue(TDuration::Seconds(10));
    }

    Y_UNIT_TEST_F(ShouldSwitchHostToOfflineAndBack, TBaseFixture)
    {
        Init();

        bool isHostOffline = false;
        DirectBlockGroup->ReadBlocksFromDDiskHandler = [&]   //
            (ui32 vChunkIndex,
             THostIndex hostIndex,
             TBlockRange16 range,
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
             TBlockRange16 range,
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
            true,
            DirtyMapStateProto,
            DirectBlockGroup,
            3,   // syncRequestsBatchSize
            DefaultBlockSize,
            DefaultVChunkSize);
        vchunk->Start();

        auto getHealthyDDisks = [&]
        {
            return RunOnExecutor(
                       DirectBlockGroup->GetExecutor(),
                       [&] { return vchunk->GetHealthyDDisks().Print(); })
                .GetValue(TDuration::Seconds(10));
        };

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
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                PartitionDirectService->UpdateConfigRequests.size());
            const auto& request =
                PartitionDirectService->UpdateConfigRequests.front();
            auto persistedDirtyMap = std::make_shared<TBlocksDirtyMap>(
                CreateArenaAllocatorPool(),
                request.Config,
                true,
                request.Proto,
                DefaultBlockSize,
                VChunkBlockCount);
            UNIT_ASSERT_VALUES_EQUAL(
                "  H3: [0..32767]\n",
                persistedDirtyMap->DebugPrintBehind());

            UNIT_ASSERT_VALUES_EQUAL(1, ReplyUpdateRequests());
            DrainExecutor(DirectBlockGroup->GetExecutor());
        }

        // Config should be updated.
        UNIT_ASSERT_VALUES_EQUAL(
            "[DBG0/V100]{Rotten,Primary,Primary,Primary,HandOff}",
            AccessConfig(*vchunk).DebugPrint());

        // DirtyMap config should be updated.
        UNIT_ASSERT_VALUES_EQUAL(
            "H0-{Operational,32768};"
            "H1*{Operational,32768};"
            "H2*{Operational,32768};"
            "H3*{Fresh+,0};"
            "H4+{Disabled,0};",
            AccessBlocksDirtyMap(*vchunk).DebugPrintDDiskState());
        UNIT_ASSERT_VALUES_EQUAL("[H1,H2]", getHealthyDDisks());

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
            "[DBG0/V100]{Primary,Primary,Primary,Primary,HandOff}",
            AccessConfig(*vchunk).DebugPrint());

        // DirtyMap config should be updated.
        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Operational,32768};"
            "H1*{Operational,32768};"
            "H2*{Operational,32768};"
            "H3*{Fresh+,0};"
            "H4+{Disabled,0};",
            AccessBlocksDirtyMap(*vchunk).DebugPrintDDiskState());
        UNIT_ASSERT_VALUES_EQUAL("[H0,H1,H2]", getHealthyDDisks());
        UNIT_ASSERT(PartitionDirectService->UpdateConfigRequests.empty());

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
            UNIT_ASSERT_VALUES_EQUAL("[H0,H1,H2]", getHealthyDDisks());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                PartitionDirectService->UpdateDirtyMapStateRequests.size());
            UNIT_ASSERT_VALUES_EQUAL(1, ReplyUpdateDirtyMapStateRequests());
            DrainExecutor(DirectBlockGroup->GetExecutor());
            UNIT_ASSERT_VALUES_EQUAL("[H0,H1,H2]", getHealthyDDisks());

            UNIT_ASSERT_VALUES_EQUAL(
                1,
                PartitionDirectService->UpdateDirtyMapStateRequests.size());
            UNIT_ASSERT_VALUES_EQUAL(1, ReplyUpdateDirtyMapStateRequests());
            DrainExecutor(DirectBlockGroup->GetExecutor());
            UNIT_ASSERT_VALUES_EQUAL("[H0,H1,H2,H3]", getHealthyDDisks());
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

        // Once copying finishes, the fourth healthy DDisk is unnecessary.
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            PartitionDirectService->UpdateConfigRequests.size());
        const auto& demoteConfig =
            PartitionDirectService->UpdateConfigRequests.front().Config;
        UNIT_ASSERT_VALUES_EQUAL(EHostRole::None, demoteConfig.GetDDiskRole(0));
        UNIT_ASSERT_VALUES_EQUAL(3, demoteConfig.GetDDisks().Count());

        UNIT_ASSERT_VALUES_EQUAL(1, ReplyUpdateRequests());
        DrainExecutor(DirectBlockGroup->GetExecutor());
        UNIT_ASSERT_VALUES_EQUAL(
            EHostRole::None,
            AccessConfig(*vchunk).GetDDiskRole(0));
        UNIT_ASSERT_VALUES_EQUAL(3, AccessConfig(*vchunk).GetDDisks().Count());

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

        const auto range = TBlockRange16::WithLength(0, 1);
        ExpectedRange = range;
        RangeData = GenerateRandomString(BlockSize * range.Size());

        auto vchunk = std::make_shared<TVChunk>(
            Runtime->GetActorSystem(0),
            TraceService.get(),
            PartitionDirectService.get(),
            DiskDescription,
            VChunkConfig,
            false,
            DirtyMapStateProto,
            DirectBlockGroup,
            3,   // syncRequestsBatchSize
            DefaultBlockSize,
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
                .Range = ConvertRangeSafe<TBlockRange64>(range)});
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
                .Range = ConvertRangeSafe<TBlockRange64>(range)});
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

        const auto range = TBlockRange16::WithLength(0, 1);
        ExpectedRange = range;
        RangeData = GenerateRandomString(BlockSize * range.Size());

        auto vchunk = std::make_shared<TVChunk>(
            Runtime->GetActorSystem(0),
            TraceService.get(),
            PartitionDirectService.get(),
            DiskDescription,
            VChunkConfig,
            false,
            DirtyMapStateProto,
            DirectBlockGroup,
            3,   // syncRequestsBatchSize
            DefaultBlockSize,
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
                .Range = ConvertRangeSafe<TBlockRange64>(range)});
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
    // and the DDiskTouched flag) and, once that future resolves, run
    // OnDirtyMapPersisted which clears the in-flight flag and acknowledges the
    // generation to the dirty map (NeedPersist() becomes false).
    Y_UNIT_TEST_F(ShouldPersistDirtyMapState, TBaseFixture)
    {
        VChunkConfig.PromoteHost(3);
        Init();

        auto vchunk = std::make_shared<TVChunk>(
            Runtime->GetActorSystem(0),
            TraceService.get(),
            PartitionDirectService.get(),
            DiskDescription,
            VChunkConfig,
            true,
            DirtyMapStateProto,
            DirectBlockGroup,
            3,   // syncRequestsBatchSize
            DefaultBlockSize,
            DefaultVChunkSize);
        vchunk->Start();
        DrainExecutor(DirectBlockGroup->GetExecutor());

        // Drive the dirty map into a "need persist" state and trigger persist,
        // all on the executor thread the vchunk state is confined to.
        RunOnExecutor(
            DirectBlockGroup->GetExecutor(),
            [&]() -> bool
            {
                auto& dirtyMap = AccessBlocksDirtyMap(*vchunk);
                dirtyMap.SetReadablePrefixDebugOnly(3, BlockSize * 5);
                MakeDirtyMapNeedPersist(dirtyMap);
                UNIT_ASSERT_VALUES_EQUAL(true, dirtyMap.NeedPersist());

                InvokeStartPersist(*vchunk);
                return true;
            })
            .GetValue(TDuration::Seconds(10));

        // A single UpdateDirtyMapState request must have been issued with the
        // vchunk index; the vchunk marks itself busy.
        UNIT_ASSERT_VALUES_EQUAL(
            1u,
            PartitionDirectService->UpdateDirtyMapStateRequests.size());
        const auto& request =
            PartitionDirectService->UpdateDirtyMapStateRequests.front();
        UNIT_ASSERT_VALUES_EQUAL(FixtureVChunkIndex, request.VChunkIndex);
        UNIT_ASSERT_VALUES_EQUAL(true, IsPersisting(*vchunk));

        // Complete the persist; OnDirtyMapPersisted runs on the callback.
        UNIT_ASSERT_VALUES_EQUAL(1, ReplyUpdateDirtyMapStateRequests());
        DrainExecutor(DirectBlockGroup->GetExecutor());

        // Flag cleared and the generation acknowledged to the dirty map.
        UNIT_ASSERT_VALUES_EQUAL(false, IsPersisting(*vchunk));
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

    // A host goes offline while the erase to it is in flight: the record
    // cannot be erased there, so the vchunk persists a barrier that covers
    // it and forgets it once the restore barrier is committed.
    Y_UNIT_TEST_F(ShouldPersistRestoreBarrierForDisabledHost, TBaseFixture)
    {
        Init();

        auto vchunk = std::make_shared<TVChunk>(
            Runtime->GetActorSystem(0),
            TraceService.get(),
            PartitionDirectService.get(),
            DiskDescription,
            VChunkConfig,
            true,
            DirtyMapStateProto,
            DirectBlockGroup,
            3,   // syncRequestsBatchSize
            DefaultBlockSize,
            DefaultVChunkSize);
        vchunk->Start();
        DrainExecutor(DirectBlockGroup->GetExecutor());

        RunOnExecutor(
            DirectBlockGroup->GetExecutor(),
            [&]() -> bool
            {
                auto& dirtyMap = AccessBlocksDirtyMap(*vchunk);
                const auto range = TBlockRange16::WithLength(10, 10);
                const auto hosts = THostMask::MakeAll(3);

                dirtyMap.RegisterInflightWrite(MakeKey(100), range);
                dirtyMap.WriteFinished(MakeKey(100), range, hosts, hosts);
                for (const auto& [route, hint]:
                     dirtyMap.MakeFlushHint(1).GetAllHints())
                {
                    dirtyMap.FlushFinished(
                        route,
                        MakePBufferKeys(hint.Segments),
                        {});
                }
                for (const auto& [host, hint]:
                     dirtyMap.MakeEraseHint(1).GetAllHints())
                {
                    if (host != THostIndex{2}) {
                        dirtyMap.EraseFinished(
                            host,
                            MakePBufferKeys(hint.Segments),
                            {});
                    }
                }
                UNIT_ASSERT_VALUES_EQUAL(1, dirtyMap.GetInflightCount());

                // Host 2 goes offline with its erase in flight.
                vchunk->SetHostState(2, EHostState::TemporaryOffline);
                return true;
            })
            .GetValue(TDuration::Seconds(10));

        // The config with the disabled host is persisted, then the dirty map
        // state with the restore barrier over the record.
        bool barrierPersisted = false;
        for (int i = 0; i < 4 && !barrierPersisted; ++i) {
            ReplyUpdateRequests();
            DrainExecutor(DirectBlockGroup->GetExecutor());
            for (const auto& request:
                 PartitionDirectService->UpdateDirtyMapStateRequests)
            {
                if (request.Proto.GetRestoreBarrierLsn() == MakeKey(100).Lsn) {
                    UNIT_ASSERT_VALUES_EQUAL(
                        MakeKey(100).Generation,
                        request.Proto.GetRestoreBarrierGeneration());
                    barrierPersisted = true;
                }
            }
            ReplyUpdateDirtyMapStateRequests();
            DrainExecutor(DirectBlockGroup->GetExecutor());
        }
        UNIT_ASSERT_VALUES_EQUAL(true, barrierPersisted);

        // The barrier is committed: the record left the dirty map.
        RunOnExecutor(
            DirectBlockGroup->GetExecutor(),
            [&]() -> bool
            {
                UNIT_ASSERT_VALUES_EQUAL(
                    0,
                    AccessBlocksDirtyMap(*vchunk).GetInflightCount());
                return true;
            })
            .GetValue(TDuration::Seconds(10));

        vchunk->Stop().GetValue(TDuration::Seconds(10));
    }

    // A second StartPersist call while a persist is already in flight must
    // be a no-op: no duplicate UpdateDirtyMapState request is issued.
    Y_UNIT_TEST_F(
        ShouldNotPersistDirtyMapStateWhileAlreadyPersisting,
        TBaseFixture)
    {
        VChunkConfig.PromoteHost(3);

        Init();

        auto vchunk = std::make_shared<TVChunk>(
            Runtime->GetActorSystem(0),
            TraceService.get(),
            PartitionDirectService.get(),
            DiskDescription,
            VChunkConfig,
            true,
            DirtyMapStateProto,
            DirectBlockGroup,
            3,   // syncRequestsBatchSize
            DefaultBlockSize,
            DefaultVChunkSize);
        vchunk->Start();
        DrainExecutor(DirectBlockGroup->GetExecutor());

        RunOnExecutor(
            DirectBlockGroup->GetExecutor(),
            [&]() -> bool
            {
                auto& dirtyMap = AccessBlocksDirtyMap(*vchunk);
                dirtyMap.SetReadablePrefixDebugOnly(3, BlockSize * 5);
                MakeDirtyMapNeedPersist(dirtyMap);

                // First call starts a persist; second call must be ignored
                // while it is still in flight.
                InvokeStartPersist(*vchunk);
                InvokeStartPersist(*vchunk);
                return true;
            })
            .GetValue(TDuration::Seconds(10));

        UNIT_ASSERT_VALUES_EQUAL(
            1u,
            PartitionDirectService->UpdateDirtyMapStateRequests.size());

        UNIT_ASSERT_VALUES_EQUAL(1, ReplyUpdateDirtyMapStateRequests());
        DrainExecutor(DirectBlockGroup->GetExecutor());
        UNIT_ASSERT_VALUES_EQUAL(false, IsPersisting(*vchunk));

        auto onStop = vchunk->Stop();
        onStop.GetValue(TDuration::Seconds(10));
    }

    // With no dirty map changes NeedPersist() is false, so StartPersist
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
            false,
            DirtyMapStateProto,
            DirectBlockGroup,
            3,   // syncRequestsBatchSize
            DefaultBlockSize,
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
                InvokeStartPersist(*vchunk);
                return true;
            })
            .GetValue(TDuration::Seconds(10));

        UNIT_ASSERT_VALUES_EQUAL(
            0u,
            PartitionDirectService->UpdateDirtyMapStateRequests.size());
        UNIT_ASSERT_VALUES_EQUAL(false, IsPersisting(*vchunk));

        auto onStop = vchunk->Stop();
        onStop.GetValue(TDuration::Seconds(10));
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

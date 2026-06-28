#include "vchunk.h"

#include "base_test_fixture.h"

#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor_ut.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NThreading;

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

namespace {

// GetSafeBarrierForErase asserts it runs on the vchunk's executor thread, so
// hop onto the executor and bring the value back.
std::optional<ui64> GetSafeBarrierOnExecutor(
    const TExecutorPtr& executor,
    TVChunk& vchunk)
{
    auto promise = NThreading::NewPromise<std::optional<ui64>>();
    auto future = promise.GetFuture();
    executor->ExecuteSimple(
        [promise = std::move(promise), &vchunk]() mutable
        { promise.SetValue(vchunk.GetSafeBarrierForErase()); });
    return future.GetValue(TDuration::Seconds(10));
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
            PartitionDirectService.get(),
            VChunkConfig,
            DirectBlockGroup,
            3,   // syncRequestsBatchSize
            DefaultVChunkSize,
            Counters);
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
            PartitionDirectService.get(),
            VChunkConfig,
            DirectBlockGroup,
            3,   // syncRequestsBatchSize
            DefaultVChunkSize,
            Counters);
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

        // GenerateLsn + RegisterInflightWrite happen as the write is
        // dispatched, so the safe barrier is held at the generated lsn (123)
        // right away.
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            WaitWriteRequests(3, TDuration::Seconds(10)));
        UNIT_ASSERT_VALUES_EQUAL(
            123,
            *GetSafeBarrierOnExecutor(
                DirectBlockGroup->GetExecutor(),
                *vchunk));

        // Acknowledging the PBuffer writes does not release the barrier: the
        // entry stays inflight until it is flushed and erased.
        SetWriteResult(TDBGWriteBlocksResponse{.Error = MakeError(S_OK)}, true);
        const auto& result = future.GetValue(TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result.Error.GetCode(),
            FormatError(result.Error));
        UNIT_ASSERT_VALUES_EQUAL(
            123,
            *GetSafeBarrierOnExecutor(
                DirectBlockGroup->GetExecutor(),
                *vchunk));

        vchunk->Stop().GetValue(TDuration::Seconds(10));
    }

    Y_UNIT_TEST_F(ShouldSwitchHostToTemporaryOfflineAndBack, TBaseFixture)
    {
        Init();

        auto vchunk = std::make_shared<TVChunk>(
            Runtime->GetActorSystem(0),
            PartitionDirectService.get(),
            VChunkConfig,
            DirectBlockGroup,
            3,   // syncRequestsBatchSize
            DefaultVChunkSize,
            Counters);
        vchunk->Start();

        // Call SetHostState(TemporaryOffline)
        {
            TPromise<void> ready = NewPromise();
            auto wait = ready.GetFuture();
            DirectBlockGroup->GetExecutor()->ExecuteSimple(
                [&]()
                {
                    vchunk->SetHostState(0, EHostState::TemporaryOffline);
                    ready.SetValue();
                });
            wait.GetValue(TDuration::Seconds(10));
        }

        // Config should stay the same since new config is not persisted yet.
        UNIT_ASSERT_VALUES_EQUAL(
            "[100] "
            "PBuffer{Primary;Primary;Primary;HandOff;HandOff} "
            "DDisk{Primary;Primary;Primary;None;None} "
            "Enabled{+++++}",
            AccessConfig(*vchunk).DebugPrint());

        // DirtyMap config should stay the same too.
        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Operational,32768,32768};"
            "H1*{Operational,32768,32768};"
            "H2*{Operational,32768,32768};"
            "H3+{Disabled,0,0};"
            "H4+{Disabled,0,0};",
            AccessBlocksDirtyMap(*vchunk).DebugPrintDDiskState());

        // Scheduled DBResponse.
        // TODO replace with real DB response.
        {
            UNIT_ASSERT_VALUES_EQUAL(1, ScheduledTasks.size());
            auto task = RunScheduledTasks();
            task.Wait(TDuration::Seconds(10));
        }

        // Config should be updated.
        UNIT_ASSERT_VALUES_EQUAL(
            "[100] "
            "PBuffer{Primary;Primary;Primary;HandOff;HandOff} "
            "DDisk{Primary;Primary;Primary;None;None} "
            "Enabled{-++++}",
            AccessConfig(*vchunk).DebugPrint());

        // DirtyMap config should be updated.
        UNIT_ASSERT_VALUES_EQUAL(
            "H0-{Operational,32768,32768};"
            "H1*{Operational,32768,32768};"
            "H2*{Operational,32768,32768};"
            "H3+{Disabled,0,0};"
            "H4+{Disabled,0,0};",
            AccessBlocksDirtyMap(*vchunk).DebugPrintDDiskState());

        // Call SetHostState(Online)
        {
            TPromise<void> ready = NewPromise();
            auto wait = ready.GetFuture();
            DirectBlockGroup->GetExecutor()->ExecuteSimple(
                [&]()
                {
                    vchunk->SetHostState(0, EHostState::Online);
                    ready.SetValue();
                });
            wait.GetValue(TDuration::Seconds(10));
        }

        // Scheduled DBResponse.
        // TODO replace with real DB response.
        {
            UNIT_ASSERT_VALUES_EQUAL(1, ScheduledTasks.size());
            auto task = RunScheduledTasks();
            task.Wait(TDuration::Seconds(10));
        }

        // Config should be updated.
        UNIT_ASSERT_VALUES_EQUAL(
            "[100] "
            "PBuffer{Primary;Primary;Primary;HandOff;HandOff} "
            "DDisk{Primary;Primary;Primary;None;None} "
            "Enabled{+++++}",
            AccessConfig(*vchunk).DebugPrint());

        // DirtyMap config should be updated.
        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Operational,32768,32768};"
            "H1*{Operational,32768,32768};"
            "H2*{Operational,32768,32768};"
            "H3+{Disabled,0,0};"
            "H4+{Disabled,0,0};",
            AccessBlocksDirtyMap(*vchunk).DebugPrintDDiskState());

        auto onStop = vchunk->Stop();
        onStop.GetValue(TDuration::Seconds(10));
    }

    Y_UNIT_TEST_F(ShouldAppendHostAndGrowDirtyMap, TBaseFixture)
    {
        Init();

        auto vchunk = std::make_shared<TVChunk>(
            Runtime->GetActorSystem(0),
            PartitionDirectService.get(),
            VChunkConfig,
            DirectBlockGroup,
            3,
            DefaultVChunkSize,
            Counters);
        vchunk->Start();

        UNIT_ASSERT_VALUES_EQUAL(
            DirectBlockGroupHostCount,
            AccessConfig(*vchunk).GetHostCount());

        {
            TPromise<void> ready = NewPromise();
            auto wait = ready.GetFuture();
            DirectBlockGroup->GetExecutor()->ExecuteSimple(
                [&]()
                {
                    vchunk->OnHostAppended(DirectBlockGroupHostCount + 1);
                    ready.SetValue();
                });
            wait.GetValue(TDuration::Seconds(10));
        }

        UNIT_ASSERT_VALUES_EQUAL(
            DirectBlockGroupHostCount,
            AccessConfig(*vchunk).GetHostCount());

        UNIT_ASSERT_STRING_CONTAINS(
            AccessBlocksDirtyMap(*vchunk).DebugPrintDDiskState(),
            "H5-{Disabled,0,0}");

        {
            UNIT_ASSERT_VALUES_EQUAL(1, ScheduledTasks.size());
            auto task = RunScheduledTasks();
            task.Wait(TDuration::Seconds(10));
        }

        UNIT_ASSERT_VALUES_EQUAL(
            DirectBlockGroupHostCount + 1,
            AccessConfig(*vchunk).GetHostCount());
        UNIT_ASSERT_STRING_CONTAINS(
            AccessConfig(*vchunk).DebugPrint(),
            "PBuffer{Primary;Primary;Primary;HandOff;HandOff;None}");
        UNIT_ASSERT_STRING_CONTAINS(
            AccessConfig(*vchunk).DebugPrint(),
            "DDisk{Primary;Primary;Primary;None;None;None}");
        UNIT_ASSERT_STRING_CONTAINS(
            AccessConfig(*vchunk).DebugPrint(),
            "Enabled{+++++-}");

        UNIT_ASSERT_STRING_CONTAINS(
            AccessBlocksDirtyMap(*vchunk).DebugPrintDDiskState(),
            "H5-{Disabled,0,0}");

        auto onStop = vchunk->Stop();
        onStop.GetValue(TDuration::Seconds(10));
    }

    // On recovery the DBG comes up with N+1 connections (a committed AddHost)
    // while a vchunk's persisted config can still lag at N. The vchunk catches
    // itself up to the DBG's host count when it starts.
    Y_UNIT_TEST_F(ShouldCatchUpToDbgHostCountOnStart, TBaseFixture)
    {
        Init();

        // The DBG already has one host more than the vchunk's (lagging) config.
        DirectBlockGroup->HostCount = DirectBlockGroupHostCount + 1;

        auto vchunk = std::make_shared<TVChunk>(
            Runtime->GetActorSystem(0),
            PartitionDirectService.get(),
            VChunkConfig,
            DirectBlockGroup,
            3,   // syncRequestsBatchSize
            DefaultVChunkSize,
            Counters);
        vchunk->Start();
        DrainExecutor(DirectBlockGroup->GetExecutor());

        // DoStart grew the dirty map to the DBG's host count.
        UNIT_ASSERT_STRING_CONTAINS(
            AccessBlocksDirtyMap(*vchunk).DebugPrintDDiskState(),
            "H5-{Disabled,0,0}");

        // The config catch-up is persisted via a scheduled task.
        {
            UNIT_ASSERT_VALUES_EQUAL(1, ScheduledTasks.size());
            auto task = RunScheduledTasks();
            task.Wait(TDuration::Seconds(10));
        }

        UNIT_ASSERT_VALUES_EQUAL(
            DirectBlockGroupHostCount + 1,
            AccessConfig(*vchunk).GetHostCount());

        auto onStop = vchunk->Stop();
        onStop.GetValue(TDuration::Seconds(10));
    }

    Y_UNIT_TEST_F(ShouldSwitchHostToOfflineAndBack, TBaseFixture)
    {
        Init();

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

            // Should not read from offline host.
            UNIT_ASSERT_VALUES_UNEQUAL(0, hostIndex);

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
            PartitionDirectService.get(),
            VChunkConfig,
            DirectBlockGroup,
            3,   // syncRequestsBatchSize
            DefaultVChunkSize,
            Counters);
        vchunk->Start();

        // Call SetHostState(Offline)
        {
            TPromise<void> ready = NewPromise();
            auto wait = ready.GetFuture();
            DirectBlockGroup->GetExecutor()->ExecuteSimple(
                [&]()
                {
                    vchunk->SetHostState(0, EHostState::Offline);
                    ready.SetValue();
                });
            wait.GetValue(TDuration::Seconds(10));
        }

        // Config should stay the same since new config is not persisted yet.
        UNIT_ASSERT_VALUES_EQUAL(
            "[100] "
            "PBuffer{Primary;Primary;Primary;HandOff;HandOff} "
            "DDisk{Primary;Primary;Primary;None;None} "
            "Enabled{+++++}",
            AccessConfig(*vchunk).DebugPrint());

        // DirtyMap config should stay the same too.
        UNIT_ASSERT_VALUES_EQUAL(
            "H0*{Operational,32768,32768};"
            "H1*{Operational,32768,32768};"
            "H2*{Operational,32768,32768};"
            "H3+{Disabled,0,0};"
            "H4+{Disabled,0,0};",
            AccessBlocksDirtyMap(*vchunk).DebugPrintDDiskState());

        // Scheduled DBResponse.
        // TODO replace with real DB response.
        {
            WaitScheduledTasks(1, TDuration::Seconds(10));
            UNIT_ASSERT_VALUES_EQUAL(1, ScheduledTasks.size());
            auto task = RunScheduledTasks();
            task.Wait(TDuration::Seconds(10));
        }

        // Config should be updated.
        UNIT_ASSERT_VALUES_EQUAL(
            "[100] "
            "PBuffer{HandOff;Primary;Primary;Primary;HandOff} "
            "DDisk{None;Primary;Primary;Primary;None} "
            "Enabled{-+++[0]+}",
            AccessConfig(*vchunk).DebugPrint());

        // DirtyMap config should be updated.
        UNIT_ASSERT_VALUES_EQUAL(
            "H0-{Disabled,0,0};"
            "H1*{Operational,32768,32768};"
            "H2*{Operational,32768,32768};"
            "H3*{Fresh,0,256};"
            "H4+{Disabled,0,0};",
            AccessBlocksDirtyMap(*vchunk).DebugPrintDDiskState());

        // Call SetHostState(Online)
        {
            TPromise<void> ready = NewPromise();
            auto wait = ready.GetFuture();
            DirectBlockGroup->GetExecutor()->ExecuteSimple(
                [&]()
                {
                    vchunk->SetHostState(0, EHostState::Online);
                    ready.SetValue();
                });
            wait.GetValue(TDuration::Seconds(10));
        }

        // Scheduled DBResponse.
        // TODO replace with real DB response.
        {
            WaitScheduledTasks(1, TDuration::Seconds(10));
            UNIT_ASSERT_VALUES_EQUAL(1, ScheduledTasks.size());
            auto task = RunScheduledTasks();
            task.Wait(TDuration::Seconds(10));
        }

        // Config should be updated.
        UNIT_ASSERT_VALUES_EQUAL(
            "[100] "
            "PBuffer{HandOff;Primary;Primary;Primary;HandOff} "
            "DDisk{None;Primary;Primary;Primary;None} "
            "Enabled{++++[0]+}",
            AccessConfig(*vchunk).DebugPrint());

        // DirtyMap config should be updated.
        UNIT_ASSERT_VALUES_EQUAL(
            "H0+{Disabled,0,0};"
            "H1*{Operational,32768,32768};"
            "H2*{Operational,32768,32768};"
            "H3*{Fresh,0,256};"
            "H4+{Disabled,0,0};",
            AccessBlocksDirtyMap(*vchunk).DebugPrintDDiskState());

        // Execute copier reads and writes.
        for (size_t i = 0; i < VChunkBlockCount / BlocksPerCopy; ++i) {
            WaitReadRequests(1, TDuration::Seconds(10));
            SetReadResult({.Error = MakeError(S_OK)}, true);

            WaitWriteRequests(1, TDuration::Seconds(10));
            SetWriteResult({.Error = MakeError(S_OK)}, true);
        }

        // Waiting for the coping to be completed.
        {
            WaitScheduledTasks(1, TDuration::Seconds(10));
            UNIT_ASSERT_VALUES_EQUAL(1, ScheduledTasks.size());
            auto task = RunScheduledTasks();
            task.Wait(TDuration::Seconds(10));
        }

        // Config should be updated.
        UNIT_ASSERT_VALUES_EQUAL(
            "[100] "
            "PBuffer{HandOff;Primary;Primary;Primary;HandOff} "
            "DDisk{None;Primary;Primary;Primary;None} "
            "Enabled{+++++}",
            AccessConfig(*vchunk).DebugPrint());

        // DirtyMap config should be updated.
        UNIT_ASSERT_VALUES_EQUAL(
            "H0+{Disabled,0,0};"
            "H1*{Operational,32768,32768};"
            "H2*{Operational,32768,32768};"
            "H3*{Operational,32768,32768};"
            "H4+{Disabled,0,0};",
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
            PartitionDirectService.get(),
            VChunkConfig,
            DirectBlockGroup,
            3,   // syncRequestsBatchSize
            DefaultVChunkSize,
            Counters);
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
            PartitionDirectService.get(),
            VChunkConfig,
            DirectBlockGroup,
            3,   // syncRequestsBatchSize
            DefaultVChunkSize,
            Counters);
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
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

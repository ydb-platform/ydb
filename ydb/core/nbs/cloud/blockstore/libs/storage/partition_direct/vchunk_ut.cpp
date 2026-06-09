#include "vchunk.h"

#include "base_test_fixture.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NThreading;

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

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

        // Should not get more scheduled tasks.
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            WaitScheduledTasks(1, TDuration::MilliSeconds(100)));

        auto onStop = vchunk->Stop();
        onStop.GetValue(TDuration::Seconds(10));
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
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

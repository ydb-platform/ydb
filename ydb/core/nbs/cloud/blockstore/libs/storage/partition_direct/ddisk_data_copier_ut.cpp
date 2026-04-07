#include "ddisk_data_copier.h"

#include "base_test_fixture.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NThreading;

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TFixture: public TBaseFixture
{
    TDDiskDataCopierPtr Copier;

    void Init() override
    {
        TBaseFixture::Init();

        Copier = std::make_shared<TDDiskDataCopier>(
            Runtime->GetActorSystem(0),
            VChunkConfig,
            PartitionDirectService,
            DirectBlockGroup,
            &DirtyMap,
            FreshDDisk);
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TDDiskDataCopierTest)
{
    Y_UNIT_TEST_F(ShouldCopyDDisk, TFixture)
    {
        Init();

        // Mark DDisk#1 completely fresh.
        DirtyMap.MarkFresh(FreshDDisk, 0);
        UNIT_ASSERT_VALUES_EQUAL(
            "DDisk0{Operational,32768,32768};"
            "DDisk1{Fresh,0,0};"   // Watermarks
            "DDisk2{Operational,32768,32768};"
            "HODDisk0{Operational,32768,32768};"
            "HODDisk1{Operational,32768,32768};",
            DirtyMap.DebugPrintDDiskState());

        // No ranges locked.
        UNIT_ASSERT_VALUES_EQUAL("", DirtyMap.DebugPrintLockedDDiskRanges());

        // Start data copy
        ExpectedRange = TBlockRange64::WithLength(0, BlocksPerCopy);
        auto complete = Copier->Start();

        // Should transfer all ranges. One-by-one.
        for (size_t i = 0; i < VChunkSize / CopyRangeSize; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(false, complete.IsReady());

            // expectedRange should be locked for reading and copying.
            UNIT_ASSERT_VALUES_EQUAL(
                ExpectedRange.Print(),
                DirtyMap.DebugPrintLockedDDiskRanges());

            // Complete reading and rea-arm.
            ReadPromise.SetValue(
                TDBGReadBlocksResponse{.Error = MakeError(S_OK)});
            ReadPromise = NewPromise<TDBGReadBlocksResponse>();

            // expectedRange should be locked for copying.
            UNIT_ASSERT_VALUES_EQUAL(
                ExpectedRange.Print(),
                DirtyMap.DebugPrintLockedDDiskRanges());

            // Set next expected range right before completing write.
            auto nextExpectedRange = TBlockRange64::WithLength(
                (i + 1) * BlocksPerCopy,
                BlocksPerCopy);
            ExpectedRange = nextExpectedRange;

            // Complete writing and rea-arm promise
            WritePromise.SetValue(
                TDBGWriteBlocksResponse{.Error = MakeError(S_OK)});
            WritePromise = NewPromise<TDBGWriteBlocksResponse>();

            if (i == 5) {
                // Check state on 5th iteration
                UNIT_ASSERT_VALUES_EQUAL(
                    "DDisk0{Operational,32768,32768};"
                    "DDisk1{Fresh,1536,1792};"   // Watermarks for reading
                                                 // and writing raised
                    "DDisk2{Operational,32768,32768};"
                    "HODDisk0{Operational,32768,32768};"
                    "HODDisk1{Operational,32768,32768};",
                    DirtyMap.DebugPrintDDiskState());
            }
        }

        // Data copying should be completed.
        UNIT_ASSERT_VALUES_EQUAL(true, complete.IsReady());
        UNIT_ASSERT_VALUES_EQUAL(
            TDDiskDataCopier::EResult::Ok,
            complete.GetValue());

        // All DDisk fully operational
        UNIT_ASSERT_VALUES_EQUAL(
            "DDisk0{Operational,32768,32768};"
            "DDisk1{Operational,32768,32768};"
            "DDisk2{Operational,32768,32768};"
            "HODDisk0{Operational,32768,32768};"
            "HODDisk1{Operational,32768,32768};",
            DirtyMap.DebugPrintDDiskState());
    }

    Y_UNIT_TEST_F(ShouldStopOnReadError, TFixture)
    {
        Init();

        DirectBlockGroup->ReadBlocksFromDDiskHandler = [&]   //
            (ui32 vChunkIndex,
             ui8 hostIndex,
             TBlockRange64 range,
             const TGuardedSgList& guardedSglist,
             NWilson::TTraceId traceId)
        {
            Y_UNUSED(vChunkIndex);
            Y_UNUSED(hostIndex);
            Y_UNUSED(range);
            Y_UNUSED(guardedSglist);
            Y_UNUSED(traceId);

            return MakeFuture<TDBGReadBlocksResponse>(
                {.Error = MakeError(E_REJECTED)});
        };

        // Mark DDisk#1 completely fresh.
        DirtyMap.MarkFresh(FreshDDisk, 0);

        // Start data copy
        ExpectedRange = TBlockRange64::WithLength(0, BlocksPerCopy);
        auto complete = Copier->Start();

        // Data copying should be completed with error.
        UNIT_ASSERT_VALUES_EQUAL(true, complete.IsReady());
        UNIT_ASSERT_VALUES_EQUAL(
            TDDiskDataCopier::EResult::Error,
            complete.GetValue());

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            *DirtyMap.GetFreshWatermark(ELocation::DDisk1));

        UNIT_ASSERT_VALUES_EQUAL(
            "DDisk0{Operational,32768,32768};"
            "DDisk1{Fresh,0,256};"   // Watermarks
            "DDisk2{Operational,32768,32768};"
            "HODDisk0{Operational,32768,32768};"
            "HODDisk1{Operational,32768,32768};",
            DirtyMap.DebugPrintDDiskState());
    }

    Y_UNIT_TEST_F(ShouldStopOnWriteError, TFixture)
    {
        Init();

        // Will response with error for write requests.
        DirectBlockGroup->WriteBlocksToDDiskHandler = [&]   //
            (ui32 vChunkIndex,
             ui8 hostIndex,
             TBlockRange64 range,
             const TGuardedSgList& guardedSglist,
             NWilson::TTraceId traceId)
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
        DirtyMap.MarkFresh(FreshDDisk, 0);

        // Start data copy
        ExpectedRange = TBlockRange64::WithLength(0, BlocksPerCopy);
        auto complete = Copier->Start();

        // Read range - OK.
        ReadPromise.SetValue({.Error = MakeError(S_OK)});

        // Data copying should be completed with error.
        UNIT_ASSERT_VALUES_EQUAL(true, complete.IsReady());
        UNIT_ASSERT_VALUES_EQUAL(
            TDDiskDataCopier::EResult::Error,
            complete.GetValue());

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            *DirtyMap.GetFreshWatermark(ELocation::DDisk1));

        UNIT_ASSERT_VALUES_EQUAL(
            "DDisk0{Operational,32768,32768};"
            "DDisk1{Fresh,0,256};"   // Watermarks
            "DDisk2{Operational,32768,32768};"
            "HODDisk0{Operational,32768,32768};"
            "HODDisk1{Operational,32768,32768};",
            DirtyMap.DebugPrintDDiskState());
    }

    Y_UNIT_TEST_F(ShouldStartAfterStop, TFixture)
    {
        Init();

        // Mark DDisk#1 completely fresh.
        DirtyMap.MarkFresh(FreshDDisk, 0);

        // Start data coping
        ExpectedRange = TBlockRange64::WithLength(0, BlocksPerCopy);
        auto complete = Copier->Start();
        UNIT_ASSERT_VALUES_EQUAL(false, complete.IsReady());

        // Read range #0 - OK.
        ReadPromise.SetValue({.Error = MakeError(S_OK)});
        ReadPromise = NewPromise<TDBGReadBlocksResponse>();

        // Stop data copy
        auto stopped = Copier->Stop();
        UNIT_ASSERT_VALUES_EQUAL(false, stopped.IsReady());

        // Write range #0 - OK.
        WritePromise.SetValue({.Error = MakeError(S_OK)});
        WritePromise = NewPromise<TDBGWriteBlocksResponse>();

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
            "DDisk0{Operational,32768,32768};"
            "DDisk1{Fresh,256,256};"   // Watermarks
            "DDisk2{Operational,32768,32768};"
            "HODDisk0{Operational,32768,32768};"
            "HODDisk1{Operational,32768,32768};",
            DirtyMap.DebugPrintDDiskState());

        // Start data coping again
        ExpectedRange = TBlockRange64::WithLength(256, BlocksPerCopy);
        complete = Copier->Start();
        UNIT_ASSERT_VALUES_EQUAL(false, complete.IsReady());

        // Read range #1 - OK.
        ReadPromise.SetValue({.Error = MakeError(S_OK)});
        ReadPromise = NewPromise<TDBGReadBlocksResponse>();

        // Stop data copy
        stopped = Copier->Stop();
        UNIT_ASSERT_VALUES_EQUAL(false, stopped.IsReady());

        // Write range #1 - OK.
        WritePromise.SetValue({.Error = MakeError(S_OK)});
        WritePromise = NewPromise<TDBGWriteBlocksResponse>();

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
            "DDisk0{Operational,32768,32768};"
            "DDisk1{Fresh,512,512};"   // Watermarks
            "DDisk2{Operational,32768,32768};"
            "HODDisk0{Operational,32768,32768};"
            "HODDisk1{Operational,32768,32768};",
            DirtyMap.DebugPrintDDiskState());
    }

    Y_UNIT_TEST_F(ShouldStartFromWaterline, TFixture)
    {
        Init();

        // Mark DDisk#1 partially fresh.
        DirtyMap.MarkFresh(FreshDDisk, CopyRangeSize);

        // Start data copy
        ExpectedRange = TBlockRange64::WithLength(256, BlocksPerCopy);
        auto complete = Copier->Start();

        // Read range - OK.
        ReadPromise.SetValue({.Error = MakeError(S_OK)});

        // Stop after one range
        Copier->Stop();

        WritePromise.SetValue({.Error = MakeError(S_OK)});

        // Data copying should be completed with error.
        UNIT_ASSERT_VALUES_EQUAL(true, complete.IsReady());
        UNIT_ASSERT_VALUES_EQUAL(
            TDDiskDataCopier::EResult::Interrupted,
            complete.GetValue());

        UNIT_ASSERT_VALUES_EQUAL(
            CopyRangeSize * 2,
            *DirtyMap.GetFreshWatermark(ELocation::DDisk1));

        UNIT_ASSERT_VALUES_EQUAL(
            "DDisk0{Operational,32768,32768};"
            "DDisk1{Fresh,512,512};"   // Watermarks
            "DDisk2{Operational,32768,32768};"
            "HODDisk0{Operational,32768,32768};"
            "HODDisk1{Operational,32768,32768};",
            DirtyMap.DebugPrintDDiskState());
    }

    Y_UNIT_TEST_F(ShouldCopyWithWrites, TFixture)
    {
        Init();

        // Mark DDisk#1 completely fresh.
        DirtyMap.MarkFresh(FreshDDisk, 0);

        DirtyMap.WriteFinished(
            123,
            TBlockRange64::WithLength(10, 10),   // #0
            PBuffersMask,
            PBuffersMask);
        DirtyMap.WriteFinished(
            124,
            TBlockRange64::WithLength(250, 10),   // #0 + #1
            PBuffersMask,
            PBuffersMask);
        DirtyMap.WriteFinished(
            125,
            TBlockRange64::WithLength(260, 10),   // #1
            PBuffersMask,
            PBuffersMask);

        // Start data copy
        ExpectedRange = TBlockRange64::WithLength(0, BlocksPerCopy);
        auto complete = Copier->Start();

        // Coping range #0 in progress.

        // Flush hints should not contains writes overlapped with copied range
        // #0
        auto flushHints = DirtyMap.MakeFlushHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "PBuffer0->DDisk0:125[260..269];"
            "PBuffer0->HODDisk0:125[260..269];"
            "PBuffer2->DDisk2:125[260..269];",
            flushHints.DebugPrint());

        // Read range #0 - OK.
        ReadPromise.SetValue({.Error = MakeError(S_OK)});
        ReadPromise = NewPromise<TDBGReadBlocksResponse>();

        // The reading of range #1 will begin immediately after writing to range
        // #0.
        ExpectedRange = TBlockRange64::WithLength(256, BlocksPerCopy);

        // Write range #0 - OK.
        WritePromise.SetValue({.Error = MakeError(S_OK)});
        WritePromise = NewPromise<TDBGWriteBlocksResponse>();

        // Coping range #1 in progress.

        // Flush hints should not contains writes overlapped with range #1, but
        // contains #0
        flushHints = DirtyMap.MakeFlushHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "PBuffer0->DDisk0:123[10..19];"
            "PBuffer0->HODDisk0:123[10..19];"
            "PBuffer1->DDisk1:123[10..19];"
            "PBuffer2->DDisk2:123[10..19];",
            flushHints.DebugPrint());

        // Read range #1 - OK.
        ReadPromise.SetValue({.Error = MakeError(S_OK)});
        ReadPromise = NewPromise<TDBGReadBlocksResponse>();

        // The reading of range #2 will begin immediately after writing to range
        // #1.
        ExpectedRange = TBlockRange64::WithLength(512, BlocksPerCopy);

        // Write range #1 - OK.
        WritePromise.SetValue({.Error = MakeError(S_OK)});
        WritePromise = NewPromise<TDBGWriteBlocksResponse>();

        // Coping range #2 in progress.

        // Flush hints should contains writes overlapped with range #1
        flushHints = DirtyMap.MakeFlushHint(1);
        UNIT_ASSERT_VALUES_EQUAL(
            "PBuffer0->DDisk0:124[250..259];"
            "PBuffer0->HODDisk0:124[250..259];"
            "PBuffer1->DDisk1:124[250..259];"
            "PBuffer2->DDisk2:124[250..259];",
            flushHints.DebugPrint());

        // Read range #2 - OK.
        ReadPromise.SetValue({.Error = MakeError(S_OK)});
        ReadPromise = NewPromise<TDBGReadBlocksResponse>();

        // Will stop after writing range #2.
        Copier->Stop();
        WritePromise.SetValue({.Error = MakeError(S_OK)});

        // Data copying should be completed with error.
        UNIT_ASSERT_VALUES_EQUAL(true, complete.IsReady());
        UNIT_ASSERT_VALUES_EQUAL(
            TDDiskDataCopier::EResult::Interrupted,
            complete.GetValue());

        UNIT_ASSERT_VALUES_EQUAL(
            CopyRangeSize * 3,
            *DirtyMap.GetFreshWatermark(ELocation::DDisk1));
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

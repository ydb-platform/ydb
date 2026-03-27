#include "ddisk_data_copier.h"

#include "direct_block_group_mock.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/partition_direct_service_mock.h>

#include <ydb/core/testlib/actors/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/fast.h>

using namespace NKikimr;

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

namespace {

TString GenerateRandomString(size_t size)
{
    TReallyFastRng32 rng(42);
    TString result;
    result.reserve(size + sizeof(ui64));
    while (result.size() < size) {
        ui64 value = rng.GenRand64();
        result +=
            TStringBuf(reinterpret_cast<const char*>(&value), sizeof(value));
    }
    result.resize(size);
    return result;
}

struct TFixture: public NUnitTest::TBaseFixture
{
    const ui64 BlocksPerCopy = CopyRangeSize / DefaultBlockSize;
    const ELocation FreshDDisk = ELocation::DDisk1;
    const TLocationMask DDiskMask =
        TLocationMask::MakeDDisk(true, true, true, true, false);
    const TLocationMask PBuffersMask = TLocationMask::MakePrimaryPBuffers();
    const TVChunkConfig VChunkConfig{
        .VChunkIndex = 100,
        .PrimaryHost0 = 0,
        .PrimaryHost1 = 1,
        .PrimaryHost2 = 2,
        .HandOffHost0 = 3,
        .HandOffHost1 = 4};

    std::unique_ptr<NActors::TTestActorRuntime> Runtime;
    TPartitionDirectServiceMockPtr PartitionDirectService;
    TDirectBlockGroupMockPtr DirectBlockGroup;
    TBlocksDirtyMap DirtyMap{DefaultBlockSize, VChunkSize / DefaultBlockSize};
    TDDiskDataCopierPtr Copier;

    TBlockRange64 ExpectedRange;
    TString RangeData;
    NThreading::TPromise<TDBGReadBlocksResponse> ReadPromise =
        NThreading::NewPromise<TDBGReadBlocksResponse>();
    NThreading::TPromise<TDBGWriteBlocksResponse> WritePromise =
        NThreading::NewPromise<TDBGWriteBlocksResponse>();

    void Init()
    {
        Runtime = std::make_unique<NActors::TTestActorRuntime>();
        Runtime->Initialize(TTestActorRuntime::TEgg{
            .App0 = new TAppData(
                0,
                0,
                0,
                0,
                {},
                nullptr,
                nullptr,
                nullptr,
                nullptr),
            .Opaque = nullptr,
            .KeyConfigGenerator = nullptr,
            .Icb = {},
            .Dcb = {}});

        PartitionDirectService =
            std::make_shared<TPartitionDirectServiceMock>();
        PartitionDirectService->VolumeConfig = std::make_shared<TVolumeConfig>(
            "disk-1",
            DefaultBlockSize,
            65536,
            1024);
        DirtyMap.UpdateConfig(DDiskMask.Include(PBuffersMask), {});

        DirectBlockGroup = std::make_shared<TDirectBlockGroupMock>();
        DirectBlockGroup->ReadBlocksFromDDiskHandler = [&]   //
            (ui32 vChunkIndex,
             ui8 hostIndex,
             TBlockRange64 range,
             const TGuardedSgList& guardedSglist,
             NWilson::TTraceId traceId)
        {
            Y_UNUSED(traceId);

            UNIT_ASSERT_VALUES_EQUAL(VChunkConfig.VChunkIndex, vChunkIndex);
            UNIT_ASSERT_VALUES_EQUAL(VChunkConfig.PrimaryHost0, hostIndex);
            UNIT_ASSERT_VALUES_EQUAL(ExpectedRange, range);

            RangeData = GenerateRandomString(CopyRangeSize);
            SgListCopy(
                TBlockDataRef{RangeData.data(), RangeData.size()},
                guardedSglist.Acquire().Get());

            return ReadPromise.GetFuture();
        };

        DirectBlockGroup->WriteBlocksToDDiskHandler = [&]   //
            (ui32 vChunkIndex,
             ui8 hostIndex,
             TBlockRange64 range,
             const TGuardedSgList& guardedSglist,
             NWilson::TTraceId traceId)
        {
            Y_UNUSED(traceId);

            UNIT_ASSERT_VALUES_EQUAL(VChunkConfig.VChunkIndex, vChunkIndex);
            UNIT_ASSERT_VALUES_EQUAL(VChunkConfig.PrimaryHost1, hostIndex);
            UNIT_ASSERT_VALUES_EQUAL(ExpectedRange, range);

            TString copiedData;
            copiedData.resize(CopyRangeSize);
            SgListCopy(
                guardedSglist.Acquire().Get(),
                TBlockDataRef{copiedData.data(), copiedData.size()});

            UNIT_ASSERT_VALUES_EQUAL(RangeData, copiedData);

            return WritePromise.GetFuture();
        };

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
            ReadPromise = NThreading::NewPromise<TDBGReadBlocksResponse>();

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
            WritePromise = NThreading::NewPromise<TDBGWriteBlocksResponse>();

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

            return NThreading::MakeFuture<TDBGReadBlocksResponse>(
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
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

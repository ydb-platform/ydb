#include "base_test_fixture.h"

#include <util/random/fast.h>

using namespace NKikimr;
using namespace NThreading;

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

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

void TBaseFixture::Init()
{
    Runtime = std::make_unique<NActors::TTestActorRuntime>();
    Runtime->Initialize(TTestActorRuntime::TEgg{
        .App0 =
            new TAppData(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr),
        .Opaque = nullptr,
        .KeyConfigGenerator = nullptr,
        .Icb = {},
        .Dcb = {}});

    PartitionDirectService = std::make_shared<TPartitionDirectServiceMock>();
    PartitionDirectService->VolumeConfig = std::make_shared<TVolumeConfig>(
        "disk-1",
        DefaultBlockSize,
        65536,
        1024,
        DefaultVChunkSize);
    DirtyMap.UpdateConfig(DDiskMask.Include(PBuffersMask), {});

    DirectBlockGroup = std::make_shared<TDirectBlockGroupMock>();
    DirectBlockGroup->ReadBlocksFromDDiskHandler = [&]   //
        (ui32 vChunkIndex,
         ui8 hostIndex,
         TBlockRange64 range,
         const TGuardedSgList& guardedSglist,
         const NWilson::TTraceId& traceId)
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
         const NWilson::TTraceId& traceId)
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
}

TGuardedSgList TBaseFixture::MakeSgList() const
{
    return TGuardedSgList(
        TSgList{TBlockDataRef{RangeData.data(), RangeData.size()}});
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

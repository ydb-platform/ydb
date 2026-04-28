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

void TBaseFixture::AddReadPromise(
    NThreading::TPromise<TDBGReadBlocksResponse> promise)
{
    TGuard<TMutex> guard(ReadMutex);
    ReadPromises.push_back(std::move(promise));
}

void TBaseFixture::SetReadResult(TDBGReadBlocksResponse response)
{
    TGuard<TMutex> guard(ReadMutex);
    for (auto& promise: ReadPromises) {
        promise.SetValue(std::move(response));
    }
}

void TBaseFixture::ClearReadPromises()
{
    TGuard<TMutex> guard(ReadMutex);
    ReadPromises.clear();
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

        if (RangeData.empty()) {
            RangeData = GenerateRandomString(CopyRangeSize);
        }

        const ui64 offsetBlocks = range.Start - ExpectedRange.Start;
        const ui64 offsetBytes = offsetBlocks * BlockSize;
        const ui64 sizeBytes = range.Size() * BlockSize;
        SgListCopy(
            TBlockDataRef{RangeData.data() + offsetBytes, sizeBytes},
            guardedSglist.Acquire().Get());

        auto readPromise = NewPromise<TDBGReadBlocksResponse>();
        auto res = readPromise.GetFuture();
        AddReadPromise(std::move(readPromise));
        return res;
    };

    DirectBlockGroup->ReadBlocksFromPBufferHandler = [&]   //
        (ui32 vChunkIndex,
         ui8 hostIndex,
         ui64 lsn,
         TBlockRange64 range,
         const TGuardedSgList& guardedSglist,
         const NWilson::TTraceId& traceId)
    {
        Y_UNUSED(lsn);
        Y_UNUSED(traceId);
        UNIT_ASSERT_VALUES_EQUAL(VChunkConfig.VChunkIndex, vChunkIndex);
        UNIT_ASSERT_VALUES_EQUAL(VChunkConfig.PrimaryHost0, hostIndex);

        const ui64 offsetBlocks = range.Start - ExpectedRange.Start;
        const ui64 offsetBytes = offsetBlocks * BlockSize;
        const ui64 sizeBytes = range.Size() * BlockSize;

        SgListCopy(
            TBlockDataRef{RangeData.data() + offsetBytes, sizeBytes},
            guardedSglist.Acquire().Get());

        auto readPromise = NewPromise<TDBGReadBlocksResponse>();
        auto res = readPromise.GetFuture();
        AddReadPromise(std::move(readPromise));
        return res;
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

        const ui64 offsetBlocks = range.Start - ExpectedRange.Start;
        const ui64 offsetBytes = offsetBlocks * BlockSize;
        const ui64 sizeBytes = range.Size() * BlockSize;
        TString expectedData =
            TString(RangeData.data() + offsetBytes, sizeBytes);
        UNIT_ASSERT_VALUES_EQUAL(expectedData, copiedData);

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

#include "base_test_fixture.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/volume_config.h>

#include <ydb/core/base/appdata_fwd.h>

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

    Runtime->SetLogPriority(NKikimrServices::NBS_PARTITION, NLog::PRI_DEBUG);

    PartitionDirectService = std::make_shared<TPartitionDirectServiceMock>();
    PartitionDirectService->VolumeConfig = std::make_shared<TVolumeConfig>(
        "disk-1",
        DefaultBlockSize,
        65536,
        1024,
        DefaultVChunkSize);

    DirectBlockGroup = std::make_shared<TDirectBlockGroupMock>();

    DirectBlockGroup->ScheduleHandler = [&](TDuration delay, TCallback callback)
    {
        auto guard = TGuard(PromisesGuard);
        ScheduledTasks.push_back(
            TScheduledTask{.Delay = delay, .Callback = std::move(callback)});
    };

    DirectBlockGroup->ReadBlocksFromDDiskHandler = [&]   //
        (ui32 vChunkIndex,
         THostIndex hostIndex,
         TBlockRange64 range,
         const TGuardedSgList& guardedSglist,
         const NWilson::TTraceId& traceId)
    {
        Y_UNUSED(traceId);
        UNIT_ASSERT_VALUES_EQUAL(VChunkConfig.GetVChunkIndex(), vChunkIndex);
        UNIT_ASSERT_VALUES_EQUAL(THostIndex{0}, hostIndex);

        if (RangeData.empty()) {
            RangeData = GenerateRandomString(CopyRangeSize);
        }

        const ui64 offsetBlocks = range.Start - ExpectedRange.Start;
        const ui64 offsetBytes = offsetBlocks * BlockSize;
        const ui64 sizeBytes = range.Size() * BlockSize;
        SgListCopy(
            TBlockDataRef{RangeData.data() + offsetBytes, sizeBytes},
            guardedSglist.Acquire().Get());

        auto promise = NewPromise<TDBGReadBlocksResponse>();
        auto future = promise.GetFuture();
        auto guard = TGuard(PromisesGuard);
        ReadPromises.push_back(std::move(promise));
        return future;
    };

    DirectBlockGroup->ReadBlocksFromPBufferHandler = [&]   //
        (ui32 vChunkIndex,
         THostIndex hostIndex,
         ui64 lsn,
         TBlockRange64 range,
         const TGuardedSgList& guardedSglist,
         const NWilson::TTraceId& traceId)
    {
        Y_UNUSED(lsn);
        Y_UNUSED(traceId);
        UNIT_ASSERT_VALUES_EQUAL(VChunkConfig.GetVChunkIndex(), vChunkIndex);
        UNIT_ASSERT_VALUES_EQUAL(THostIndex{0}, hostIndex);

        const ui64 offsetBlocks = range.Start - ExpectedRange.Start;
        const ui64 offsetBytes = offsetBlocks * BlockSize;
        const ui64 sizeBytes = range.Size() * BlockSize;

        SgListCopy(
            TBlockDataRef{RangeData.data() + offsetBytes, sizeBytes},
            guardedSglist.Acquire().Get());

        auto promise = NewPromise<TDBGReadBlocksResponse>();
        auto future = promise.GetFuture();
        auto guard = TGuard(PromisesGuard);
        ReadPromises.push_back(std::move(promise));
        return future;
    };

    DirectBlockGroup->WriteBlocksToPBufferHandler = [&]   //
        (ui32 vChunkIndex,
         THostIndex hostIndex,
         ui64 lsn,
         TBlockRange64 range,
         const TGuardedSgList& guardedSglist,
         const NWilson::TTraceId& traceId)
    {
        Y_UNUSED(traceId);
        Y_UNUSED(hostIndex);
        Y_UNUSED(lsn);

        UNIT_ASSERT_VALUES_EQUAL(VChunkConfig.GetVChunkIndex(), vChunkIndex);
        UNIT_ASSERT_VALUES_EQUAL(ExpectedRange, range);

        const ui64 offsetBlocks = range.Start - ExpectedRange.Start;
        const ui64 offsetBytes = offsetBlocks * BlockSize;
        const ui64 sizeBytes = range.Size() * BlockSize;

        TString copiedData;
        copiedData.resize(sizeBytes);
        SgListCopy(
            guardedSglist.Acquire().Get(),
            TBlockDataRef{copiedData.data(), copiedData.size()});

        TString expectedData =
            TString(RangeData.data() + offsetBytes, sizeBytes);
        UNIT_ASSERT_VALUES_EQUAL(expectedData, copiedData);

        auto promise = NewPromise<TDBGWriteBlocksResponse>();
        auto future = promise.GetFuture();
        auto guard = TGuard(PromisesGuard);
        WritePromises.push_back(std::move(promise));
        return future;
    };

    DirectBlockGroup->WriteBlocksToDDiskHandler = [&]   //
        (ui32 vChunkIndex,
         THostIndex hostIndex,
         TBlockRange64 range,
         const TGuardedSgList& guardedSglist,
         const NWilson::TTraceId& traceId)
    {
        Y_UNUSED(traceId);

        UNIT_ASSERT_VALUES_EQUAL(VChunkConfig.GetVChunkIndex(), vChunkIndex);
        UNIT_ASSERT_VALUES_EQUAL(FreshDDisk, hostIndex);
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

        auto promise = NewPromise<TDBGWriteBlocksResponse>();
        auto future = promise.GetFuture();
        auto guard = TGuard(PromisesGuard);
        WritePromises.push_back(std::move(promise));
        return future;
    };

    DirectBlockGroup->SyncWithPBufferHandler = [&]   //
        (ui32 vChunkIndex,
         THostIndex pbufferHostIndex,
         THostIndex ddiskHostIndex,
         const TVector<TPBufferSegment>& segments,
         const NWilson::TTraceId& traceId)
    {
        Y_UNUSED(vChunkIndex);
        Y_UNUSED(pbufferHostIndex);
        Y_UNUSED(ddiskHostIndex);
        Y_UNUSED(segments);
        Y_UNUSED(traceId);

        auto promise = NewPromise<TDBGFlushResponse>();
        auto future = promise.GetFuture();
        auto guard = TGuard(PromisesGuard);
        FlushPromises.push_back(std::move(promise));
        return future;
    };

    DirectBlockGroup->EraseFromPBufferHandler = [&]   //
        (ui32 vChunkIndex,
         THostIndex hostIndex,
         const TVector<TPBufferSegment>& segments,
         const NWilson::TTraceId& traceId)
    {
        Y_UNUSED(vChunkIndex);
        Y_UNUSED(hostIndex);
        Y_UNUSED(segments);
        Y_UNUSED(traceId);

        auto promise = NewPromise<TDBGEraseResponse>();
        auto future = promise.GetFuture();
        auto guard = TGuard(PromisesGuard);
        ErasePromises.push_back(std::move(promise));
        return future;
    };

    DirectBlockGroup->RestoreDBGPBuffersHandler = [&]   //
        (const auto&...)
    {
        return NThreading::MakeFuture<TDBGRestoreResponse>(
            {.Error = MakeError(S_OK)});
    };
}

TGuardedSgList TBaseFixture::MakeSgList() const
{
    return TGuardedSgList(
        TSgList{TBlockDataRef{RangeData.data(), RangeData.size()}});
}

bool TBaseFixture::WaitScheduledTasks(size_t count, TDuration timeout)
{
    return Wait(ScheduledTasks, count, timeout);
}

NThreading::TFuture<void> TBaseFixture::RunScheduledTasks()
{
    TPromise<void> promise = NewPromise<void>();
    auto result = promise.GetFuture();
    auto guard = TGuard(PromisesGuard);
    auto f = [scheduledTasks = std::move(ScheduledTasks),
              promise = std::move(promise)]   //
        () mutable
    {
        for (auto& task: scheduledTasks) {
            task.Callback();
        }
        promise.SetValue();
    };

    DirectBlockGroup->GetExecutor()->ExecuteSimple(f);
    return result;
}

void TBaseFixture::SetReadResult(TDBGReadBlocksResponse response, bool async)
{
    SetResult(ReadPromises, std::move(response), async);
}

bool TBaseFixture::WaitReadRequests(size_t count, TDuration timeout)
{
    return Wait(ReadPromises, count, timeout);
}

void TBaseFixture::SetWriteResult(TDBGWriteBlocksResponse response, bool async)
{
    SetResult(WritePromises, std::move(response), async);
}

bool TBaseFixture::WaitWriteRequests(size_t count, TDuration timeout)
{
    return Wait(WritePromises, count, timeout);
}

void TBaseFixture::SetFlushResult(TDBGFlushResponse response, bool async)
{
    SetResult(FlushPromises, std::move(response), async);
}

bool TBaseFixture::WaitFlushRequests(size_t count, TDuration timeout)
{
    return Wait(FlushPromises, count, timeout);
}

void TBaseFixture::SetEraseResult(TDBGEraseResponse response, bool async)
{
    SetResult(ErasePromises, std::move(response), async);
}

bool TBaseFixture::WaitEraseRequests(size_t count, TDuration timeout)
{
    return Wait(ErasePromises, count, timeout);
}

template <typename T>
void TBaseFixture::SetResult(
    TVector<NThreading::TPromise<T>>& promises,
    T response,
    bool async)
{
    PromisesGuard.Acquire();
    auto f = [response = std::move(response),
              promises = std::move(promises)]   //
        () mutable
    {
        for (auto& promise: promises) {
            promise.SetValue(response);
        }
    };
    PromisesGuard.Release();

    if (async) {
        DirectBlockGroup->GetExecutor()->ExecuteSimple(f);
    } else {
        f();
    }
}

template <typename T>
bool TBaseFixture::Wait(TVector<T>& items, size_t count, TDuration timeout)
{
    TInstant now = TInstant::Now();
    while (true) {
        {
            auto guard = TGuard(PromisesGuard);
            if (items.size() >= count) {
                return true;
            }
        }
        if (TInstant::Now() > now + timeout) {
            break;
        }
        Sleep(TDuration::MilliSeconds(1));
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

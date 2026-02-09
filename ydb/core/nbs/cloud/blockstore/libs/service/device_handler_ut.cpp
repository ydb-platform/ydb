#include "device_handler.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/storage_test.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/sglist.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/sglist_test.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <array>

namespace NYdb::NBS::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////
class TTestEnvironment
{
private:
    const ui64 BlocksCount;
    const ui32 BlockSize;
    const ui32 SectorSize;

    TSgList SgList;
    TVector<TString> Blocks;
    TPromise<void> WriteTrigger;
    IDeviceHandlerPtr DeviceHandler;

    TVector<TFuture<NProto::TError>> Futures;

    ui32 ReadRequestCount = 0;
    ui32 WriteRequestCount = 0;
    ui32 ZeroRequestCount = 0;

    ui32 ZeroedBlocksCount = 0;

public:
    TTestEnvironment(
        ui64 blocksCount,
        ui32 blockSize,
        ui32 sectorsPerBlock,
        ui32 maxBlockCount = 1024,
        bool unalignedRequestsDisabled = false,
        ui32 maxZeroBlocksSubRequestSize = 0)
        : BlocksCount(blocksCount)
        , BlockSize(blockSize)
        , SectorSize(BlockSize / sectorsPerBlock)
    {
        UNIT_ASSERT(SectorSize * sectorsPerBlock == BlockSize);

        SgList = ResizeBlocks(Blocks, BlocksCount, TString(BlockSize, '0'));
        WriteTrigger = NewPromise<void>();

        auto testStorage = std::make_shared<TTestStorage>();
        testStorage->ReadBlocksLocalHandler =
            [&](TCallContextPtr ctx,
                std::shared_ptr<TReadBlocksLocalRequest> request)
        {
            ++ReadRequestCount;
            ctx->AddTime(EProcessingStage::Postponed, TDuration::Seconds(1));

            auto startIndex = request->Range.Start;
            auto guard = request->Sglist.Acquire();
            const auto& dst = guard.Get();

            for (const auto& buffer: dst) {
                Y_ABORT_UNLESS(buffer.Size() % BlockSize == 0);
            }

            auto src = SgList;
            src.erase(src.begin(), src.begin() + startIndex);
            auto sz = SgListCopy(src, dst);
            UNIT_ASSERT(sz == request->Range.Size() * BlockSize);

            return MakeFuture(TReadBlocksLocalResponse());
        };
        testStorage->WriteBlocksLocalHandler =
            [&](TCallContextPtr ctx,
                std::shared_ptr<TWriteBlocksLocalRequest> request)
        {
            ++WriteRequestCount;
            ctx->AddTime(EProcessingStage::Postponed, TDuration::Seconds(10));

            auto future = WriteTrigger.GetFuture();
            return future.Apply(
                [=, this](const auto& f)
                {
                    Y_UNUSED(f);

                    auto startIndex = request->Range.Start;
                    auto guard = request->Sglist.Acquire();
                    const auto& src = guard.Get();

                    for (const auto& buffer: src) {
                        Y_ABORT_UNLESS(buffer.Size() % BlockSize == 0);
                    }

                    auto dst = SgList;
                    dst.erase(dst.begin(), dst.begin() + startIndex);
                    auto sz = SgListCopy(src, dst);
                    UNIT_ASSERT(sz == request->Range.Size() * BlockSize);

                    return TWriteBlocksLocalResponse();
                });
        };
        testStorage->ZeroBlocksLocalHandler =
            [&](TCallContextPtr ctx,
                std::shared_ptr<TZeroBlocksLocalRequest> request)
        {
            ++ZeroRequestCount;
            ZeroedBlocksCount += request->Range.Size();
            ctx->AddTime(EProcessingStage::Postponed, TDuration::Seconds(100));

            auto future = WriteTrigger.GetFuture();
            return future.Apply(
                [=, this](const auto& f)
                {
                    Y_UNUSED(f);

                    auto startIndex = request->Range.Start;
                    TSgList src(
                        request->Range.Size(),
                        TBlockDataRef::CreateZeroBlock(BlockSize));

                    auto dst = SgList;
                    dst.erase(dst.begin(), dst.begin() + startIndex);
                    auto sz = SgListCopy(src, dst);
                    UNIT_ASSERT(sz == request->Range.Size() * BlockSize);

                    return TZeroBlocksLocalResponse();
                });
        };

        auto factory =
            CreateDeviceHandlerFactoryForTesting(maxBlockCount * BlockSize);
        DeviceHandler = factory->CreateDeviceHandler(
            TDeviceHandlerParams{
                .Storage = std::move(testStorage),
                .DiskId = "disk1",
                .ClientId = "testClientId",
                .BlockSize = BlockSize,
                .MaxZeroBlocksSubRequestSize = maxZeroBlocksSubRequestSize,
                .UnalignedRequestsDisabled = unalignedRequestsDisabled,
                .StorageMediaKind = NProto::STORAGE_MEDIA_SSD_NONREPLICATED});
    }

    TCallContextPtr WriteSectors(ui64 firstSector, ui64 totalSectors, char data)
    {
        auto ctx = MakeIntrusive<TCallContext>();
        auto buffer = TString(totalSectors * SectorSize, data);
        TSgList sgList;
        for (size_t i = 0; i < totalSectors; ++i) {
            sgList.emplace_back(buffer.data() + i * SectorSize, SectorSize);
        }

        auto future = DeviceHandler
                          ->Write(
                              ctx,
                              firstSector * SectorSize,
                              totalSectors * SectorSize,
                              TGuardedSgList(sgList))
                          .Apply(
                              [buf = std::move(buffer)](const auto& f)
                              {
                                  Y_UNUSED(buf);

                                  return f.GetValue().Error;
                              });

        Futures.push_back(future);
        return ctx;
    }

    TCallContextPtr ZeroSectors(ui64 firstSector, ui64 totalSectors)
    {
        auto ctx = MakeIntrusive<TCallContext>();
        auto future =
            DeviceHandler
                ->Zero(ctx, firstSector * SectorSize, totalSectors * SectorSize)
                .Apply([](const auto& f) { return f.GetValue().Error; });

        Futures.push_back(future);
        return ctx;
    }

    std::pair<TPromise<void>, TVector<TFuture<NProto::TError>>>
    TakeWriteTrigger()
    {
        TPromise<void> result = NewPromise<void>();
        WriteTrigger.Swap(result);
        return std::make_pair(std::move(result), std::move(Futures));
    }

    void RunWriteService()
    {
        WriteTrigger.SetValue();

        for (const auto& future: Futures) {
            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
        }
    }

    void ReadSectorsAndCheck(
        ui64 firstSector,
        ui64 totalSectors,
        const TString& expected)
    {
        UNIT_ASSERT(expected.size() == totalSectors);

        TString buffer = TString::Uninitialized(totalSectors * SectorSize);
        TSgList sgList;
        for (size_t i = 0; i < totalSectors; ++i) {
            sgList.emplace_back(buffer.data() + i * SectorSize, SectorSize);
        }
        TString checkpointId;

        auto future = DeviceHandler->Read(
            MakeIntrusive<TCallContext>(),
            firstSector * SectorSize,
            totalSectors * SectorSize,
            TGuardedSgList(sgList),
            checkpointId);

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(!HasError(response.Error));

        TString read;
        read.resize(expected.size(), 0);
        const char* ptr = buffer.data();
        bool allOk = true;
        for (size_t i = 0; i != expected.size(); ++i) {
            char c = expected[i];
            read[i] = *ptr == 0 ? 'Z' : *ptr;
            for (size_t j = 0; j < SectorSize; ++j) {
                if (c == 'Z') {
                    allOk = allOk && *ptr == 0;
                } else {
                    allOk = allOk && *ptr == c;
                }
                ++ptr;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(expected, read);
        UNIT_ASSERT(allOk);
    }

    ui32 GetReadRequestCount() const
    {
        return ReadRequestCount;
    }

    ui32 GetWriteRequestCount() const
    {
        return WriteRequestCount;
    }

    ui32 GetZeroRequestCount() const
    {
        return ZeroRequestCount;
    }

    void ResetRequestCounters()
    {
        ReadRequestCount = 0;
        WriteRequestCount = 0;
        ZeroRequestCount = 0;
        ZeroedBlocksCount = 0;
    }

    ui32 GetZeroedBlocksCount() const
    {
        return ZeroedBlocksCount;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDeviceHandlerTest)
{
    Y_UNIT_TEST(ShouldHandleUnalignedReadRequests)
    {
        TTestEnvironment env(2, DefaultBlockSize, 8);

        env.WriteSectors(0, 4, 'a');
        env.WriteSectors(4, 4, 'b');
        env.WriteSectors(8, 4, 'c');
        env.WriteSectors(12, 4, 'd');

        env.RunWriteService();
        env.ReadSectorsAndCheck(0, 16, "aaaabbbbccccdddd");
        env.ReadSectorsAndCheck(8, 1, "c");
        env.ReadSectorsAndCheck(3, 4, "abbb");
        env.ReadSectorsAndCheck(7, 5, "bcccc");
        env.ReadSectorsAndCheck(3, 11, "abbbbccccdd");
    }

    Y_UNIT_TEST(ShouldHandleReadModifyWriteRequests)
    {
        TTestEnvironment env(2, DefaultBlockSize, 8);

        env.WriteSectors(1, 2, 'a');
        env.WriteSectors(4, 4, 'b');
        env.ZeroSectors(5, 2);

        env.RunWriteService();
        env.ReadSectorsAndCheck(0, 8, "0aa0bZZb");
    }

    Y_UNIT_TEST(ShouldHandleAlignedAndRMWRequests)
    {
        TTestEnvironment env(2, DefaultBlockSize, 8);

        env.ZeroSectors(0, 8);
        env.WriteSectors(1, 1, 'a');
        env.WriteSectors(5, 2, 'b');

        env.RunWriteService();
        env.ReadSectorsAndCheck(0, 8, "ZaZZZbbZ");
    }

    Y_UNIT_TEST(ShouldHandleRMWAndAlignedRequests)
    {
        TTestEnvironment env(2, DefaultBlockSize, 8);

        env.ZeroSectors(1, 1);
        env.WriteSectors(5, 2, 'a');
        env.WriteSectors(0, 8, 'b');

        env.RunWriteService();
        env.ReadSectorsAndCheck(0, 8, "bbbbbbbb");
    }

    Y_UNIT_TEST(ShouldHandleComplicatedRMWRequests)
    {
        TTestEnvironment env(2, DefaultBlockSize, 8);

        env.WriteSectors(5, 2, 'a');
        env.ZeroSectors(13, 2);
        env.WriteSectors(0, 16, 'c');
        env.WriteSectors(1, 2, 'd');
        env.ZeroSectors(6, 4);
        env.WriteSectors(12, 3, 'f');
        env.WriteSectors(0, 8, 'g');

        env.RunWriteService();
        env.ReadSectorsAndCheck(0, 16, "ggggggggZZccfffc");
    }

    Y_UNIT_TEST(ShouldSliceHugeZeroRequest)
    {
        const auto diskId = "disk1";
        const auto clientId = "testClientId";
        const ui32 blockSize = DefaultBlockSize;
        const ui64 deviceBlocksCount = 8 * 1024;
        const ui64 blocksCountLimit = deviceBlocksCount / 4;
        const ui32 maxZeroBlocksSubRequestSize = 0;

        auto storage = std::make_shared<TTestStorage>();

        auto factory =
            CreateDeviceHandlerFactoryForTesting(blocksCountLimit * blockSize);
        auto deviceHandler = factory->CreateDeviceHandler(
            TDeviceHandlerParams{
                .Storage = storage,
                .DiskId = diskId,
                .ClientId = clientId,
                .BlockSize = blockSize,
                .MaxZeroBlocksSubRequestSize = maxZeroBlocksSubRequestSize,
                .UnalignedRequestsDisabled = false,
                .StorageMediaKind = NProto::STORAGE_MEDIA_SSD_NONREPLICATED});

        std::array<bool, deviceBlocksCount> zeroBlocks;
        for (auto& zeroBlock: zeroBlocks) {
            zeroBlock = false;
        }

        ui32 requestCounter = 0;

        storage->ZeroBlocksLocalHandler =
            [&](TCallContextPtr callContext,
                std::shared_ptr<TZeroBlocksLocalRequest> request)
        {
            Y_UNUSED(callContext);

            UNIT_ASSERT(request->Headers.ClientId == clientId);
            UNIT_ASSERT(request->Range.Size() <= blocksCountLimit);
            UNIT_ASSERT(
                request->Range.Start + request->Range.Size() <=
                deviceBlocksCount);

            for (ui32 i = 0; i < request->Range.Size(); ++i) {
                auto index = request->Range.Start + i;
                auto& zeroBlock = zeroBlocks[index];

                UNIT_ASSERT(!zeroBlock);
                zeroBlock = true;
            }

            ++requestCounter;
            return MakeFuture<TZeroBlocksLocalResponse>();
        };

        ui64 startIndex = 3;
        ui64 blocksCount = deviceBlocksCount - 9;

        auto future = deviceHandler->Zero(
            MakeIntrusive<TCallContext>(),
            startIndex * blockSize,
            blocksCount * blockSize);

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(!HasError(response.Error));

        UNIT_ASSERT(requestCounter > 1);

        for (ui64 i = 0; i < deviceBlocksCount; ++i) {
            const auto& zeroBlock = zeroBlocks[i];
            auto contains = (startIndex <= i && i < (startIndex + blocksCount));
            UNIT_ASSERT(zeroBlock == contains);
        }
    }

    Y_UNIT_TEST(ShouldNotSliceZeroRequest)
    {
        const auto diskId = "disk1";
        const auto clientId = "testClientId";
        const ui32 blockSize = DefaultBlockSize;
        const ui64 deviceBlocksCount = 8 * 1024;
        const ui64 blocksCountLimit = deviceBlocksCount / 4;
        const ui32 maxZeroBlocksSubRequestSize = 512 * 1024 * 1024;

        auto storage = std::make_shared<TTestStorage>();

        auto factory =
            CreateDeviceHandlerFactoryForTesting(blocksCountLimit * blockSize);
        auto deviceHandler = factory->CreateDeviceHandler(
            TDeviceHandlerParams{
                .Storage = storage,
                .DiskId = diskId,
                .ClientId = clientId,
                .BlockSize = blockSize,
                .MaxZeroBlocksSubRequestSize = maxZeroBlocksSubRequestSize,
                .UnalignedRequestsDisabled = false,
                .StorageMediaKind = NProto::STORAGE_MEDIA_SSD_NONREPLICATED});

        std::array<bool, deviceBlocksCount> zeroBlocks;
        for (auto& zeroBlock: zeroBlocks) {
            zeroBlock = false;
        }

        ui32 requestCounter = 0;

        storage->ZeroBlocksLocalHandler =
            [&](TCallContextPtr callContext,
                std::shared_ptr<TZeroBlocksLocalRequest> request)
        {
            Y_UNUSED(callContext);
            Y_UNUSED(request);
            ++requestCounter;
            return MakeFuture<TZeroBlocksLocalResponse>();
        };

        auto future = deviceHandler->Zero(
            MakeIntrusive<TCallContext>(),
            0,
            deviceBlocksCount * blockSize);

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(!HasError(response.Error));
        UNIT_ASSERT_EQUAL_C(1, requestCounter, requestCounter);
    }

    Y_UNIT_TEST(ShouldHandleAlignedRequestsWhenUnalignedRequestsDisabled)
    {
        const auto diskId = "disk1";
        const auto clientId = "testClientId";
        const ui32 blockSize = DefaultBlockSize;
        const ui32 maxZeroBlocksSubRequestSize = 0;

        auto storage = std::make_shared<TTestStorage>();

        auto factory = CreateDefaultDeviceHandlerFactory();
        auto deviceHandler = factory->CreateDeviceHandler(
            TDeviceHandlerParams{
                .Storage = storage,
                .DiskId = diskId,
                .ClientId = clientId,
                .BlockSize = blockSize,
                .MaxZeroBlocksSubRequestSize = maxZeroBlocksSubRequestSize,
                .UnalignedRequestsDisabled = true,
                .StorageMediaKind = NProto::STORAGE_MEDIA_SSD_NONREPLICATED});

        ui32 startIndex = 42;
        ui32 blocksCount = 17;

        auto buffer = TString::Uninitialized(blocksCount * DefaultBlockSize);
        TSgList sgList{{buffer.data(), buffer.size()}};

        storage->ReadBlocksLocalHandler =
            [&](TCallContextPtr ctx,
                std::shared_ptr<TReadBlocksLocalRequest> request)
        {
            Y_UNUSED(ctx);

            UNIT_ASSERT(request->Headers.ClientId == clientId);
            UNIT_ASSERT(request->Range.Start == startIndex);
            UNIT_ASSERT(request->Range.Size() == blocksCount);

            return MakeFuture<TReadBlocksLocalResponse>();
        };

        {
            auto future = deviceHandler->Read(
                MakeIntrusive<TCallContext>(),
                startIndex * blockSize,
                blocksCount * blockSize,
                TGuardedSgList(sgList),
                {});

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response.Error));
        }

        storage->WriteBlocksLocalHandler =
            [&](TCallContextPtr ctx,
                std::shared_ptr<TWriteBlocksLocalRequest> request)
        {
            Y_UNUSED(ctx);

            UNIT_ASSERT(request->Headers.ClientId == clientId);
            UNIT_ASSERT(request->Range.Start == startIndex);
            UNIT_ASSERT(request->Range.Size() == blocksCount);

            return MakeFuture<TWriteBlocksLocalResponse>();
        };

        {
            auto future = deviceHandler->Write(
                MakeIntrusive<TCallContext>(),
                startIndex * blockSize,
                blocksCount * blockSize,
                TGuardedSgList(sgList));

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response.Error));
        }

        storage->ZeroBlocksLocalHandler =
            [&](TCallContextPtr ctx,
                std::shared_ptr<TZeroBlocksLocalRequest> request)
        {
            Y_UNUSED(ctx);

            UNIT_ASSERT(request->Headers.ClientId == clientId);
            UNIT_ASSERT(request->Range.Start == startIndex);
            UNIT_ASSERT(request->Range.Size() == blocksCount);

            return MakeFuture<TZeroBlocksLocalResponse>();
        };

        {
            auto future = deviceHandler->Zero(
                MakeIntrusive<TCallContext>(),
                startIndex * blockSize,
                blocksCount * blockSize);

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response.Error));
        }
    }

    Y_UNIT_TEST(ShouldNotHandleUnalignedRequestsWhenUnalignedRequestsDisabled)
    {
        const auto diskId = "disk1";
        const auto clientId = "testClientId";
        const ui32 blockSize = DefaultBlockSize;
        const ui32 maxZeroBlocksSubRequestSize = 0;

        auto storage = std::make_shared<TTestStorage>();

        auto device = CreateDefaultDeviceHandlerFactory()->CreateDeviceHandler(
            TDeviceHandlerParams{
                .Storage = storage,
                .DiskId = diskId,
                .ClientId = clientId,
                .BlockSize = blockSize,
                .MaxZeroBlocksSubRequestSize = maxZeroBlocksSubRequestSize,
                .UnalignedRequestsDisabled = true,
                .StorageMediaKind = NProto::STORAGE_MEDIA_SSD_NONREPLICATED});

        {
            auto future = device->Read(
                MakeIntrusive<TCallContext>(),
                blockSize * 5 / 2,
                blockSize * 8 / 3,
                TGuardedSgList(),
                {});

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(HasError(response.Error));
            UNIT_ASSERT(response.Error.GetCode() == E_ARGUMENT);
        }

        {
            auto future = device->Write(
                MakeIntrusive<TCallContext>(),
                blockSize * 3,
                blockSize * 7 / 3,
                TGuardedSgList());

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(HasError(response.Error));
            UNIT_ASSERT(response.Error.GetCode() == E_ARGUMENT);
        }

        {
            auto future = device->Zero(
                MakeIntrusive<TCallContext>(),
                blockSize * 3 / 2,
                blockSize * 4);

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(HasError(response.Error));
            UNIT_ASSERT(response.Error.GetCode() == E_ARGUMENT);
        }
    }

    void ShouldSliceHugeAlignedRequests(bool unalignedRequestsDisabled)
    {
        TTestEnvironment
            env(24, DefaultBlockSize, 1, 4, unalignedRequestsDisabled);

        env.RunWriteService();

        env.ZeroSectors(0, 24);
        UNIT_ASSERT_VALUES_EQUAL(6, env.GetZeroRequestCount());
        env.WriteSectors(0, 8, 'a');
        env.WriteSectors(8, 8, 'b');
        UNIT_ASSERT_VALUES_EQUAL(4, env.GetWriteRequestCount());

        env.ReadSectorsAndCheck(0, 24, "aaaaaaaabbbbbbbbZZZZZZZZ");
        UNIT_ASSERT_VALUES_EQUAL(6, env.GetReadRequestCount());

        env.ResetRequestCounters();
        env.ZeroSectors(4, 8);
        UNIT_ASSERT_VALUES_EQUAL(2, env.GetZeroRequestCount());
        env.ReadSectorsAndCheck(0, 24, "aaaaZZZZZZZZbbbbZZZZZZZZ");
        UNIT_ASSERT_VALUES_EQUAL(6, env.GetReadRequestCount());
    }

    Y_UNIT_TEST(ShouldSliceHugeAlignedRequestsInAlignedBackend)
    {
        ShouldSliceHugeAlignedRequests(true);
    }

    Y_UNIT_TEST(ShouldSliceHugeAlignedRequestsInUnalignedBackend)
    {
        ShouldSliceHugeAlignedRequests(false);
    }

    Y_UNIT_TEST(ShouldSliceHugeUnalignedRequests)
    {
        TTestEnvironment env(24, DefaultBlockSize, 2, 4, false);

        env.RunWriteService();

        // An unaligned request requires the execution of a read-modify-write
        // pattern for first and last blocks with unaligned offsets.
        env.ZeroSectors(1, 46);
        UNIT_ASSERT_VALUES_EQUAL(2, env.GetReadRequestCount());
        UNIT_ASSERT_VALUES_EQUAL(2, env.GetWriteRequestCount());
        UNIT_ASSERT_VALUES_EQUAL(6, env.GetZeroRequestCount());
        UNIT_ASSERT_VALUES_EQUAL(22, env.GetZeroedBlocksCount());
        env.ResetRequestCounters();

        env.ZeroSectors(1, 39);
        UNIT_ASSERT_VALUES_EQUAL(1, env.GetReadRequestCount());
        UNIT_ASSERT_VALUES_EQUAL(1, env.GetWriteRequestCount());
        UNIT_ASSERT_VALUES_EQUAL(5, env.GetZeroRequestCount());
        UNIT_ASSERT_VALUES_EQUAL(19, env.GetZeroedBlocksCount());
        env.ResetRequestCounters();

        env.ZeroSectors(8, 37);
        UNIT_ASSERT_VALUES_EQUAL(1, env.GetReadRequestCount());
        UNIT_ASSERT_VALUES_EQUAL(1, env.GetWriteRequestCount());
        UNIT_ASSERT_VALUES_EQUAL(5, env.GetZeroRequestCount());
        UNIT_ASSERT_VALUES_EQUAL(18, env.GetZeroedBlocksCount());
        env.ResetRequestCounters();

        env.WriteSectors(3, 8, 'a');
        UNIT_ASSERT_VALUES_EQUAL(2, env.GetReadRequestCount());
        UNIT_ASSERT_VALUES_EQUAL(2, env.GetWriteRequestCount());
        env.ResetRequestCounters();

        env.WriteSectors(13, 3, 'b');
        UNIT_ASSERT_VALUES_EQUAL(1, env.GetReadRequestCount());
        UNIT_ASSERT_VALUES_EQUAL(1, env.GetWriteRequestCount());
        env.ResetRequestCounters();

        env.ReadSectorsAndCheck(
            0,
            48,
            "0ZZaaaaaaaaZZbbbZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ0");
        UNIT_ASSERT_VALUES_EQUAL(6, env.GetReadRequestCount());
    }

    void DoShouldSliceHugeZeroRequest(
        bool requestUnaligned,
        bool unalignedRequestDisabled)
    {
        const auto diskId = "disk1";
        const auto clientId = "testClientId";
        const ui32 blockSize = DefaultBlockSize;
        const ui64 deviceBlocksCount = 12;
        const ui64 blocksCountLimit = deviceBlocksCount / 4;
        const ui32 maxZeroBlocksSubRequestSize = 0;

        TString device(deviceBlocksCount * blockSize, 1);
        TString zeroBlock(blockSize, 0);

        auto storage = std::make_shared<TTestStorage>();

        auto factory =
            CreateDeviceHandlerFactoryForTesting(blocksCountLimit * blockSize);
        auto deviceHandler = factory->CreateDeviceHandler(
            TDeviceHandlerParams{
                .Storage = storage,
                .DiskId = diskId,
                .ClientId = clientId,
                .BlockSize = blockSize,
                .MaxZeroBlocksSubRequestSize = maxZeroBlocksSubRequestSize,
                .UnalignedRequestsDisabled = unalignedRequestDisabled,
                .StorageMediaKind = NProto::STORAGE_MEDIA_SSD_NONREPLICATED});

        storage->ZeroBlocksLocalHandler =
            [&](TCallContextPtr callContext,
                std::shared_ptr<TZeroBlocksLocalRequest> request)
        {
            Y_UNUSED(callContext);

            UNIT_ASSERT(request->Headers.ClientId == clientId);
            UNIT_ASSERT(request->Range.Size() <= blocksCountLimit);
            UNIT_ASSERT(
                request->Range.Start + request->Range.Size() <=
                deviceBlocksCount);

            TSgList src(
                request->Range.Size(),
                TBlockDataRef(zeroBlock.data(), zeroBlock.size()));

            TBlockDataRef dst(
                device.data() + request->Range.Start * blockSize,
                src.size() * blockSize);

            auto bytesCount = SgListCopy(src, dst);
            UNIT_ASSERT_VALUES_EQUAL(dst.Size(), bytesCount);

            return MakeFuture(TZeroBlocksLocalResponse());
        };

        storage->WriteBlocksLocalHandler =
            [&](TCallContextPtr callContext,
                std::shared_ptr<TWriteBlocksLocalRequest> request)
        {
            Y_UNUSED(callContext);

            UNIT_ASSERT(request->Headers.ClientId == clientId);
            UNIT_ASSERT(request->Range.Size() <= blocksCountLimit);
            UNIT_ASSERT(
                request->Range.Start + request->Range.Size() <=
                deviceBlocksCount);

            TBlockDataRef dst(
                device.data() + request->Range.Start * blockSize,
                request->Range.Size() * blockSize);

            auto guard = request->Sglist.Acquire();
            UNIT_ASSERT(guard);

            auto bytesCount = SgListCopy(guard.Get(), dst);
            UNIT_ASSERT_VALUES_EQUAL(dst.Size(), bytesCount);

            return MakeFuture(TWriteBlocksLocalResponse());
        };

        storage->ReadBlocksLocalHandler =
            [&](TCallContextPtr callContext,
                std::shared_ptr<TReadBlocksLocalRequest> request)
        {
            Y_UNUSED(callContext);

            UNIT_ASSERT(request->Headers.ClientId == clientId);
            UNIT_ASSERT(request->Range.Size() <= blocksCountLimit);
            UNIT_ASSERT(
                request->Range.Start + request->Range.Size() <=
                deviceBlocksCount);

            TBlockDataRef src(
                device.data() + request->Range.Start * blockSize,
                request->Range.Size() * blockSize);

            TReadBlocksLocalResponse response;

            auto guard = request->Sglist.Acquire();
            UNIT_ASSERT(guard);

            auto bytesCount = SgListCopy(src, guard.Get());
            UNIT_ASSERT_VALUES_EQUAL(src.Size(), bytesCount);

            return MakeFuture(std::move(response));
        };

        ui64 from = requestUnaligned ? 4567 : 0;
        ui64 length =
            deviceBlocksCount * blockSize - (requestUnaligned ? 9876 : 0);

        auto future =
            deviceHandler->Zero(MakeIntrusive<TCallContext>(), from, length);

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(response.Error), FormatError(response.Error));

        for (ui64 i = 0; i < deviceBlocksCount * blockSize; ++i) {
            bool isZero = (from <= i && i < (from + length));
            UNIT_ASSERT_VALUES_EQUAL_C(isZero ? 0 : 1, device[i], i);
        }
    }

    Y_UNIT_TEST(ShouldSliceHugeUnalignedZeroRequest)
    {
        DoShouldSliceHugeZeroRequest(true, false);
    }

    Y_UNIT_TEST(ShouldSliceHugeAlignedZeroRequest)
    {
        DoShouldSliceHugeZeroRequest(false, false);
    }

    Y_UNIT_TEST(ShouldSliceHugeZeroRequestWhenUnalignedDisabled)
    {
        DoShouldSliceHugeZeroRequest(false, true);
    }

    Y_UNIT_TEST(ShouldReturnErrorForHugeUnalignedReadWriteRequests)
    {
        const auto diskId = "disk1";
        const auto clientId = "testClientId";
        const ui32 blockSize = DefaultBlockSize;
        const ui32 maxZeroBlocksSubRequestSize = 0;

        auto storage = std::make_shared<TTestStorage>();

        auto deviceHandler =
            CreateDefaultDeviceHandlerFactory()->CreateDeviceHandler(
                TDeviceHandlerParams{
                    .Storage = storage,
                    .DiskId = diskId,
                    .ClientId = clientId,
                    .BlockSize = blockSize,
                    .MaxZeroBlocksSubRequestSize = maxZeroBlocksSubRequestSize,
                    .UnalignedRequestsDisabled = false,
                    .StorageMediaKind =
                        NProto::STORAGE_MEDIA_SSD_NONREPLICATED});

        storage->WriteBlocksLocalHandler =
            [&](TCallContextPtr callContext,
                std::shared_ptr<TWriteBlocksLocalRequest> request)
        {
            Y_UNUSED(callContext);
            Y_UNUSED(request);
            return MakeFuture(TWriteBlocksLocalResponse());
        };

        storage->ReadBlocksLocalHandler =
            [&](TCallContextPtr callContext,
                std::shared_ptr<TReadBlocksLocalRequest> request)
        {
            Y_UNUSED(callContext);
            Y_UNUSED(request);
            return MakeFuture(TReadBlocksLocalResponse());
        };

        ui64 from = 1;
        ui64 length = 64_MB;

        {
            TGuardedSgList sgList;
            TString checkpointId;
            auto future = deviceHandler->Read(
                MakeIntrusive<TCallContext>(),
                from,
                length,
                sgList,
                checkpointId);

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(HasError(response.Error));
            UNIT_ASSERT(response.Error.GetCode() == E_ARGUMENT);
        }

        {
            TGuardedSgList sgList;
            auto future = deviceHandler->Write(
                MakeIntrusive<TCallContext>(),
                from,
                length,
                sgList);

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(HasError(response.Error));
            UNIT_ASSERT(response.Error.GetCode() == E_ARGUMENT);
        }
    }

    Y_UNIT_TEST(ShouldReturnErrorForInvalidBufferSize)
    {
        const auto diskId = "disk1";
        const auto clientId = "testClientId";
        const ui32 blockSize = DefaultBlockSize;
        const ui32 maxZeroBlocksSubRequestSize = 0;

        auto storage = std::make_shared<TTestStorage>();

        auto deviceHandler =
            CreateDefaultDeviceHandlerFactory()->CreateDeviceHandler(
                TDeviceHandlerParams{
                    .Storage = storage,
                    .DiskId = diskId,
                    .ClientId = clientId,
                    .BlockSize = blockSize,
                    .MaxZeroBlocksSubRequestSize = maxZeroBlocksSubRequestSize,
                    .UnalignedRequestsDisabled = false,
                    .StorageMediaKind =
                        NProto::STORAGE_MEDIA_SSD_NONREPLICATED});

        storage->WriteBlocksLocalHandler =
            [&](TCallContextPtr callContext,
                std::shared_ptr<TWriteBlocksLocalRequest> request)
        {
            Y_UNUSED(callContext);
            Y_UNUSED(request);
            return MakeFuture(TWriteBlocksLocalResponse());
        };

        storage->ReadBlocksLocalHandler =
            [&](TCallContextPtr callContext,
                std::shared_ptr<TReadBlocksLocalRequest> request)
        {
            Y_UNUSED(callContext);
            Y_UNUSED(request);
            return MakeFuture(TReadBlocksLocalResponse());
        };

        ui64 from = 0;
        ui64 length = blockSize;
        auto buffer = TString::Uninitialized(blockSize + 1);
        TSgList sgList{{buffer.data(), buffer.size()}};

        {
            TString checkpointId;
            auto future = deviceHandler->Read(
                MakeIntrusive<TCallContext>(),
                from,
                length,
                TGuardedSgList(sgList),
                checkpointId);

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(HasError(response.Error));
            UNIT_ASSERT(response.Error.GetCode() == E_ARGUMENT);
        }

        {
            auto future = deviceHandler->Write(
                MakeIntrusive<TCallContext>(),
                from,
                length,
                TGuardedSgList(sgList));

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(HasError(response.Error));
            UNIT_ASSERT(response.Error.GetCode() == E_ARGUMENT);
        }
    }

    Y_UNIT_TEST(ShouldSumPostponedTimeForReadModifyWriteRequests)
    {
        TTestEnvironment env(2, DefaultBlockSize, 8);

        auto ctx1 = env.WriteSectors(1, 2, 'a');
        auto ctx2 = env.WriteSectors(4, 4, 'b');
        auto ctx3 = env.ZeroSectors(5, 2);

        env.RunWriteService();

        UNIT_ASSERT_VALUES_EQUAL(
            ctx1->Time(EProcessingStage::Postponed),
            TDuration::Seconds(11));
        UNIT_ASSERT(
            ctx2->Time(EProcessingStage::Postponed) > TDuration::Seconds(11));
        UNIT_ASSERT(
            ctx3->Time(EProcessingStage::Postponed) > TDuration::Seconds(11));
    }

    Y_UNIT_TEST(ShouldNotOverflowStack)
    {
        // We are running a very long series of overlapping queries. If the
        // executed requests are not destroyed gradually, but do so after the
        // end of the cycle, stack overflow will occur. Therefore, let's choose
        // the number of iterations large enough so that stack overflow is
        // guaranteed to happen.
#if defined(_tsan_enabled_) || defined(_asan_enabled_)
        constexpr ui32 RequestCount = 1000;
#else
#if defined(NDEBUG)
        constexpr ui32 RequestCount = 1000000;
#else
        constexpr ui32 RequestCount = 100000;
#endif   // defined(NDEBUG)
#endif   // defined(_tsan_enabled_) || defined(_asan_enabled_)

        TTestEnvironment env(2, DefaultBlockSize, 8);

        // Create first request.
        env.WriteSectors(0, 4, 'a');

        for (ui32 i = 0; i < RequestCount; ++i) {
            // Take request trigger for previous request.
            auto [writeTrigger, futures] = env.TakeWriteTrigger();
            UNIT_ASSERT_VALUES_EQUAL(1, futures.size());

            // Create new request
            env.WriteSectors(i % 4, 4, 'b');

            // Execute previous request
            writeTrigger.SetValue();
            for (const auto& future: futures) {
                const auto& response = future.GetValue(TDuration::Seconds(5));
                UNIT_ASSERT(!HasError(response));
            }
        }

        // Execute last request
        env.RunWriteService();
    }
}

}   // namespace NYdb::NBS::NBlockStore

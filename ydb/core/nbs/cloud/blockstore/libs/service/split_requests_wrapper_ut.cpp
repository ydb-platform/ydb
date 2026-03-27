#include "split_requests_wrapper.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/storage_test.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/sglist.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/sglist_test.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestEnvironment
{
    std::shared_ptr<TTestStorage> Storage;
    std::shared_ptr<IStorage> Wrapper;

    template <typename TRequest, typename TResponse>
    struct TRequestInfo
    {
        TPromise<TResponse> Promise = NewPromise<TResponse>();
        std::shared_ptr<TRequest> Request;
    };

    TMap<
        TBlockRange64,
        TRequestInfo<TReadBlocksLocalRequest, TReadBlocksLocalResponse>,
        TBlockRangeComparator>
        ReadPromises;
    TMap<
        TBlockRange64,
        TRequestInfo<TWriteBlocksLocalRequest, TWriteBlocksLocalResponse>,
        TBlockRangeComparator>
        WritePromises;
    TMap<
        TBlockRange64,
        TRequestInfo<TZeroBlocksLocalRequest, TZeroBlocksLocalResponse>,
        TBlockRangeComparator>
        ZeroPromises;

    TTestEnvironment()
    {
        Storage = std::make_shared<TTestStorage>();
        Wrapper = CreateSplitRequestsStorageWrapper(Storage);

        Storage->ReadBlocksLocalHandler =
            [&](TCallContextPtr callContext,
                std::shared_ptr<TReadBlocksLocalRequest> request)
            -> TFuture<TReadBlocksLocalResponse>
        {
            Y_UNUSED(callContext);

            auto& info = ReadPromises[request->Range] = {.Request = request};
            return info.Promise;
        };

        Storage->WriteBlocksLocalHandler =
            [&](TCallContextPtr callContext,
                std::shared_ptr<TWriteBlocksLocalRequest> request)
            -> TFuture<TWriteBlocksLocalResponse>
        {
            Y_UNUSED(callContext);

            auto& info = WritePromises[request->Range] = {.Request = request};
            return info.Promise;
        };

        Storage->ZeroBlocksLocalHandler =
            [&](TCallContextPtr callContext,
                std::shared_ptr<TZeroBlocksLocalRequest> request)
            -> TFuture<TZeroBlocksLocalResponse>
        {
            Y_UNUSED(callContext);

            auto& info = ZeroPromises[request->Range] = {.Request = request};
            return info.Promise;
        };
    }
};

void FillSgListWithData(TGuardedSgList& guardedSgList, const TString& data)
{
    TBlockDataRef ref(data.data(), data.size());

    auto guard = guardedSgList.Acquire();
    const auto& sgList = guard.Get();
    SgListCopy(ref, sgList);
}

TString GetDataFromSgList(TGuardedSgList& guardedSgList)
{
    auto guard = guardedSgList.Acquire();
    const auto& sgList = guard.Get();
    TString data;
    data.resize(SgListGetSize(sgList), 0);
    SgListCopy(sgList, TBlockDataRef(data.data(), data.size()));
    return data;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSplitRequestsStorageWrapperTest)
{
    Y_UNIT_TEST(ShouldForwardSmallReads)
    {
        constexpr ui32 BlockSize = 2;
        constexpr ui32 BlockCount = 10;
        constexpr ui32 TotalBlockCount = 1000;
        constexpr ui32 StripeSize = 100;
        TString diskId("disk-1");

        auto volumeConfig(std::make_shared<TVolumeConfig>(
            diskId,
            BlockSize,
            TotalBlockCount,
            StripeSize));

        TTestEnvironment env;

        TString data;
        data.resize(BlockSize * BlockCount, 0);
        TBlockDataRef ref(data.data(), data.size());
        TGuardedSgList guardedSgList({ref});

        auto range = TBlockRange64::WithLength(1, BlockCount);

        // Run request
        auto request1 = std::make_shared<TReadBlocksLocalRequest>(
            TRequestHeaders{.VolumeConfig = volumeConfig, .RequestId = 1},
            range);
        request1->Sglist = guardedSgList;
        auto future1 = env.Wrapper->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request1));
        UNIT_ASSERT_VALUES_EQUAL(false, future1.HasValue());

        // Should get one reads. after splitting
        UNIT_ASSERT_VALUES_EQUAL(1, env.ReadPromises.size());
        auto* firstRead =
            env.ReadPromises.FindPtr(TBlockRange64::WithLength(1, 10));
        UNIT_ASSERT(firstRead != nullptr);

        // Complete read
        {
            TString data = "aabbccddeeffgghhjjkk";
            FillSgListWithData(firstRead->Request->Sglist, data);
            firstRead->Promise.SetValue(TReadBlocksLocalResponse());
        }

        // Should get response for user read
        const auto& result = future1.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result.Error.code(),
            FormatError(result.Error));
        UNIT_ASSERT_VALUES_EQUAL("aabbccddeeffgghhjjkk", data);
    }

    Y_UNIT_TEST(ShouldSplitReads)
    {
        constexpr ui32 BlockSize = 2;
        constexpr ui32 BlockCount = 10;
        constexpr ui32 TotalBlockCount = 1000;
        constexpr ui32 StripeSize = 6;

        TString diskId("disk-1");

        auto volumeConfig(std::make_shared<TVolumeConfig>(
            diskId,
            BlockSize,
            TotalBlockCount,
            StripeSize));

        TTestEnvironment env;

        TString data;
        data.resize(BlockSize * BlockCount, 0);
        TBlockDataRef ref(data.data(), data.size());
        TGuardedSgList guardedSgList({ref});

        auto range = TBlockRange64::WithLength(1, BlockCount);

        // Run request
        auto request1 = std::make_shared<TReadBlocksLocalRequest>(
            TRequestHeaders{.VolumeConfig = volumeConfig, .RequestId = 1},
            range);
        request1->Sglist = guardedSgList;
        auto future1 = env.Wrapper->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request1));
        UNIT_ASSERT_VALUES_EQUAL(false, future1.HasValue());

        // Should get two reads after splitting
        UNIT_ASSERT_VALUES_EQUAL(2, env.ReadPromises.size());
        auto* firstRead =
            env.ReadPromises.FindPtr(TBlockRange64::WithLength(1, 5));
        auto* secondRead =
            env.ReadPromises.FindPtr(TBlockRange64::WithLength(6, 5));
        UNIT_ASSERT(firstRead != nullptr);
        UNIT_ASSERT(secondRead != nullptr);

        // Complete first read
        {
            TString data = "aabbccddee";
            FillSgListWithData(firstRead->Request->Sglist, data);
            firstRead->Promise.SetValue(TReadBlocksLocalResponse());
        }

        // Complete second read
        {
            TString data = "ffgghhjjkk";
            FillSgListWithData(secondRead->Request->Sglist, data);
            secondRead->Promise.SetValue(TReadBlocksLocalResponse());
        }

        // Should get response for user read
        const auto& result3 = future1.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result3.Error.code(),
            FormatError(result3.Error));
        UNIT_ASSERT_VALUES_EQUAL("aabbccddeeffgghhjjkk", data);
    }

    Y_UNIT_TEST(ShouldSplitWrites)
    {
        constexpr ui32 BlockSize = 2;
        constexpr ui32 BlockCount = 10;
        constexpr ui32 TotalBlockCount = 1000;
        constexpr ui32 StripeSize = 6;

        TString diskId("disk-1");

        auto volumeConfig(std::make_shared<TVolumeConfig>(
            diskId,
            BlockSize,
            TotalBlockCount,
            StripeSize));

        TTestEnvironment env;

        TString data = "aabbccddeeffgghhjjkk";
        TBlockDataRef ref(data.data(), data.size());
        TGuardedSgList guardedSgList({ref});

        auto range = TBlockRange64::WithLength(1, BlockCount);

        // Run request
        auto request = std::make_shared<TWriteBlocksLocalRequest>(
            TRequestHeaders{.VolumeConfig = volumeConfig},
            range);
        request->Sglist = guardedSgList;
        auto future1 = env.Wrapper->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(false, future1.HasValue());

        // Should get two writes after splitting
        UNIT_ASSERT_VALUES_EQUAL(2, env.WritePromises.size());
        auto* firstWrite =
            env.WritePromises.FindPtr(TBlockRange64::WithLength(1, 5));
        auto* secondWrite =
            env.WritePromises.FindPtr(TBlockRange64::WithLength(6, 5));
        UNIT_ASSERT(firstWrite != nullptr);
        UNIT_ASSERT(secondWrite != nullptr);

        // Complete first write
        {
            UNIT_ASSERT_VALUES_EQUAL(
                "aabbccddee",
                GetDataFromSgList(firstWrite->Request->Sglist));
            firstWrite->Promise.SetValue(TWriteBlocksLocalResponse());
        }

        // Complete second write
        {
            UNIT_ASSERT_VALUES_EQUAL(
                "ffgghhjjkk",
                GetDataFromSgList(secondWrite->Request->Sglist));
            secondWrite->Promise.SetValue(TWriteBlocksLocalResponse());
        }

        // Should get response for user write
        const auto& result = future1.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result.Error.code(),
            FormatError(result.Error));
    }

    Y_UNIT_TEST(ShouldSplitZeroBlocks)
    {
        constexpr ui32 BlockSize = 2;
        constexpr ui32 BlockCount = 10;
        constexpr ui32 TotalBlockCount = 1000;
        constexpr ui32 StripeSize = 6;

        TString diskId("disk-1");

        auto volumeConfig(std::make_shared<TVolumeConfig>(
            diskId,
            BlockSize,
            TotalBlockCount,
            StripeSize));

        TTestEnvironment env;

        auto range = TBlockRange64::WithLength(1, BlockCount);

        // Run request
        auto request = std::make_shared<TZeroBlocksLocalRequest>(
            TRequestHeaders{.VolumeConfig = volumeConfig},
            range);
        auto future1 = env.Wrapper->ZeroBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(false, future1.HasValue());

        // Should get two ZeroBlocks requests after splitting
        UNIT_ASSERT_VALUES_EQUAL(2, env.ZeroPromises.size());
        auto* firstZeroBlocks =
            env.ZeroPromises.FindPtr(TBlockRange64::WithLength(1, 5));
        auto* secondZeroBlocks =
            env.ZeroPromises.FindPtr(TBlockRange64::WithLength(6, 5));
        UNIT_ASSERT(firstZeroBlocks != nullptr);
        UNIT_ASSERT(secondZeroBlocks != nullptr);

        // Complete first ZeroBlocks
        {
            UNIT_ASSERT_VALUES_EQUAL(
                TBlockRange64::WithLength(1, 5),
                firstZeroBlocks->Request->Range);
            firstZeroBlocks->Promise.SetValue(TZeroBlocksLocalResponse());
        }

        // Complete second ZeroBlocks
        {
            UNIT_ASSERT_VALUES_EQUAL(
                TBlockRange64::WithLength(6, 5),
                secondZeroBlocks->Request->Range);
            secondZeroBlocks->Promise.SetValue(TZeroBlocksLocalResponse());
        }

        // Should get response for user ZeroBlocks
        const auto& result = future1.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result.Error.code(),
            FormatError(result.Error));
    }

    Y_UNIT_TEST(ShouldReplyErrorOnFirstSubRequestError)
    {
        constexpr ui32 BlockSize = 2;
        constexpr ui32 BlockCount = 10;
        constexpr ui32 TotalBlockCount = 1000;
        constexpr ui32 StripeSize = 6;

        TString diskId("disk-1");

        auto volumeConfig(std::make_shared<TVolumeConfig>(
            diskId,
            BlockSize,
            TotalBlockCount,
            StripeSize));

        TTestEnvironment env;

        auto range = TBlockRange64::WithLength(1, BlockCount);

        // Run request
        auto request = std::make_shared<TZeroBlocksLocalRequest>(
            TRequestHeaders{.VolumeConfig = volumeConfig},
            range);
        auto future1 = env.Wrapper->ZeroBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(false, future1.HasValue());

        // Should get two ZeroBlocks requests after splitting
        UNIT_ASSERT_VALUES_EQUAL(2, env.ZeroPromises.size());
        auto* firstZeroBlocks =
            env.ZeroPromises.FindPtr(TBlockRange64::WithLength(1, 5));
        auto* secondZeroBlocks =
            env.ZeroPromises.FindPtr(TBlockRange64::WithLength(6, 5));
        UNIT_ASSERT(firstZeroBlocks != nullptr);
        UNIT_ASSERT(secondZeroBlocks != nullptr);

        // Complete first ZeroBlocks with error
        {
            UNIT_ASSERT_VALUES_EQUAL(
                TBlockRange64::WithLength(1, 5),
                firstZeroBlocks->Request->Range);
            firstZeroBlocks->Promise.SetValue(
                TZeroBlocksLocalResponse{.Error = MakeError(E_REJECTED)});
        }

        // Should get error response for user ZeroBlocks
        const auto& result = future1.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            result.Error.code(),
            FormatError(result.Error));

        // Complete second ZeroBlocks
        {
            UNIT_ASSERT_VALUES_EQUAL(
                TBlockRange64::WithLength(6, 5),
                secondZeroBlocks->Request->Range);
            secondZeroBlocks->Promise.SetValue(TZeroBlocksLocalResponse());
        }
    }

    Y_UNIT_TEST(ShouldReplyErrorOnLastSubRequestError)
    {
        constexpr ui32 BlockSize = 2;
        constexpr ui32 BlockCount = 10;
        constexpr ui32 TotalBlockCount = 1000;
        constexpr ui32 StripeSize = 6;

        TString diskId("disk-1");

        auto volumeConfig(std::make_shared<TVolumeConfig>(
            diskId,
            BlockSize,
            TotalBlockCount,
            StripeSize));

        TTestEnvironment env;

        auto range = TBlockRange64::WithLength(1, BlockCount);

        // Run request
        auto request = std::make_shared<TZeroBlocksLocalRequest>(
            TRequestHeaders{.VolumeConfig = volumeConfig},
            range);
        auto future1 = env.Wrapper->ZeroBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(false, future1.HasValue());

        // Should get two ZeroBlocks requests after splitting
        UNIT_ASSERT_VALUES_EQUAL(2, env.ZeroPromises.size());
        auto* firstZeroBlocks =
            env.ZeroPromises.FindPtr(TBlockRange64::WithLength(1, 5));
        auto* secondZeroBlocks =
            env.ZeroPromises.FindPtr(TBlockRange64::WithLength(6, 5));
        UNIT_ASSERT(firstZeroBlocks != nullptr);
        UNIT_ASSERT(secondZeroBlocks != nullptr);

        // Complete first ZeroBlocks with success
        {
            UNIT_ASSERT_VALUES_EQUAL(
                TBlockRange64::WithLength(1, 5),
                firstZeroBlocks->Request->Range);
            firstZeroBlocks->Promise.SetValue(TZeroBlocksLocalResponse());
        }

        // Complete second ZeroBlocks with error
        {
            UNIT_ASSERT_VALUES_EQUAL(
                TBlockRange64::WithLength(6, 5),
                secondZeroBlocks->Request->Range);
            secondZeroBlocks->Promise.SetValue(
                TZeroBlocksLocalResponse{.Error = MakeError(E_REJECTED)});
        }

        // Should get error response for user ZeroBlocks
        const auto& result = future1.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            result.Error.code(),
            FormatError(result.Error));
    }
}

}   // namespace NYdb::NBS::NBlockStore

#include "overlapped_requests_guard_wrapper.h"

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

    TMap<ui64, TPromise<TReadBlocksLocalResponse>> ReadPromises;
    TMap<ui64, TPromise<TWriteBlocksLocalResponse>> WritePromises;
    TMap<ui64, TPromise<TZeroBlocksLocalResponse>> ZeroPromises;

    TString TestData;
    TSgList Sglist10;
    TSgList Sglist5;

    TTestEnvironment()
    {
        Storage = std::make_shared<TTestStorage>();
        Wrapper = CreateOverlappedRequestsGuardStorageWrapper(Storage);

        Storage->ReadBlocksLocalHandler =
            [&](TCallContextPtr callContext,
                std::shared_ptr<TReadBlocksLocalRequest> request)
            -> TFuture<TReadBlocksLocalResponse>
        {
            Y_UNUSED(callContext);

            return ReadPromises[request->Headers.RequestId] =
                       NewPromise<TReadBlocksLocalResponse>();
        };

        Storage->WriteBlocksLocalHandler =
            [&](TCallContextPtr callContext,
                std::shared_ptr<TWriteBlocksLocalRequest> request)
            -> TFuture<TWriteBlocksLocalResponse>
        {
            Y_UNUSED(callContext);

            return WritePromises[request->Headers.RequestId] =
                       NewPromise<TWriteBlocksLocalResponse>();
        };

        Storage->ZeroBlocksLocalHandler =
            [&](TCallContextPtr callContext,
                std::shared_ptr<TZeroBlocksLocalRequest> request)
            -> TFuture<TZeroBlocksLocalResponse>
        {
            Y_UNUSED(callContext);

            return ZeroPromises[request->Headers.RequestId] =
                       NewPromise<TZeroBlocksLocalResponse>();
        };

        TestData.resize(10 * DefaultBlockSize, 0);
        Sglist10.push_back(
            {TBlockDataRef(TestData.data(), 10 * DefaultBlockSize)});
        Sglist5.push_back(
            {TBlockDataRef(TestData.data(), 5 * DefaultBlockSize)});
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TOverlappedRequestsGuardStorageWrapperTest)
{
    Y_UNIT_TEST(OverlappingReadsShouldNotBlockEachOther)
    {
        // [-----1-----]
        // [-----2-----]
        //
        // Read requests are executed independently and do not delay each
        // other's execution.

        TTestEnvironment env;

        auto range = TBlockRange64::WithLength(1, 10);

        // Run first request
        auto request1 = std::make_shared<TReadBlocksLocalRequest>(
            TRequestHeaders{.RequestId = 1},
            range);
        request1->Sglist = TGuardedSgList(env.Sglist10);
        auto future1 = env.Wrapper->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request1));
        UNIT_ASSERT_VALUES_EQUAL(false, future1.HasValue());

        // Run second request
        auto request2 = std::make_shared<TReadBlocksLocalRequest>(
            TRequestHeaders{.RequestId = 2},
            range);
        request2->Sglist = TGuardedSgList(env.Sglist10);
        auto future2 = env.Wrapper->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request2));
        UNIT_ASSERT_VALUES_EQUAL(false, future2.HasValue());

        // Both requests should be submitted for execution
        UNIT_ASSERT_VALUES_EQUAL(2, env.ReadPromises.size());

        // Complete second request
        env.ReadPromises[2].SetValue(TReadBlocksLocalResponse());
        const auto& result2 = future2.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result2.Error.code(),
            FormatError(result2.Error));

        // Complete first request
        env.ReadPromises[1].SetValue(TReadBlocksLocalResponse());
        const auto& result1 = future1.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result1.Error.code(),
            FormatError(result1.Error));
    }

    Y_UNIT_TEST(WritesSholdNotDelayOverlappingReads)
    {
        // [-----1-----]
        // [-----2-----]
        //
        // The execution of read request #2 is not delayed by the execution of
        // write request #1

        TTestEnvironment env;

        auto range = TBlockRange64::WithLength(1, 10);

        // Run first write request
        auto request1 = std::make_shared<TWriteBlocksLocalRequest>(
            TRequestHeaders{.RequestId = 1},
            range);
        request1->Sglist = TGuardedSgList(env.Sglist10);
        auto future1 = env.Wrapper->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request1));
        UNIT_ASSERT_VALUES_EQUAL(false, future1.HasValue());

        // Run read request #2
        auto request2 = std::make_shared<TReadBlocksLocalRequest>(
            TRequestHeaders{.RequestId = 2},
            range);
        request2->Sglist = TGuardedSgList(env.Sglist10);
        auto future2 = env.Wrapper->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request2));
        UNIT_ASSERT_VALUES_EQUAL(false, future2.HasValue());

        // Both requests should be submitted for execution
        UNIT_ASSERT_VALUES_EQUAL(1, env.WritePromises.size());
        UNIT_ASSERT_VALUES_EQUAL(1, env.ReadPromises.size());

        // Complete second request
        env.ReadPromises[2].SetValue(TReadBlocksLocalResponse());
        const auto& result2 = future2.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result2.Error.code(),
            FormatError(result2.Error));

        // Complete first request
        env.WritePromises[1].SetValue(TWriteBlocksLocalResponse());
        const auto& result1 = future1.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result1.Error.code(),
            FormatError(result1.Error));
    }

    Y_UNIT_TEST(CoveredWritesShouldNotBeExecuted)
    {
        // [------1-----]
        // [--2--]
        //        [--3--]
        //
        // Request #1 should delay the execution of requests #2 and #3. When
        // request #1 completes, it will result in requests #2 and #3 being
        // completed without real execution.

        TTestEnvironment env;

        // Run write request [1, 10]
        auto request1 = std::make_shared<TWriteBlocksLocalRequest>(
            TRequestHeaders{.RequestId = 1},
            TBlockRange64::WithLength(1, 10));
        request1->Sglist = TGuardedSgList(env.Sglist10);
        auto future1 = env.Wrapper->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request1));
        UNIT_ASSERT_VALUES_EQUAL(false, future1.HasValue());

        // Run zero request covered by first write request
        auto request2 = std::make_shared<TZeroBlocksLocalRequest>(
            TRequestHeaders{.RequestId = 2},
            TBlockRange64::WithLength(1, 5));
        auto future2 = env.Wrapper->ZeroBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request2));
        UNIT_ASSERT_VALUES_EQUAL(false, future2.HasValue());

        // Run write request covered by first write request
        auto request3 = std::make_shared<TWriteBlocksLocalRequest>(
            TRequestHeaders{.RequestId = 3},
            TBlockRange64::WithLength(5, 5));
        request3->Sglist = TGuardedSgList(env.Sglist5);

        auto future3 = env.Wrapper->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request3));
        UNIT_ASSERT_VALUES_EQUAL(false, future3.HasValue());

        // Only first write request should be submitted for execution
        UNIT_ASSERT_VALUES_EQUAL(1, env.WritePromises.size());
        UNIT_ASSERT_VALUES_EQUAL(true, env.WritePromises.contains(1));
        UNIT_ASSERT_VALUES_EQUAL(0, env.ZeroPromises.size());

        // Complete first write request
        env.WritePromises[1].SetValue(
            TWriteBlocksLocalResponse{.Error = MakeError(E_REJECTED)});
        const auto& result1 = future1.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            result1.Error.code(),
            FormatError(result1.Error));

        // The zeroing should be completed too with same error.
        const auto& result2 = future2.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            result2.Error.code(),
            FormatError(result2.Error));

        // The second write should be completed too with same error.
        const auto& result3 = future3.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            result3.Error.code(),
            FormatError(result3.Error));

        // Only first write request should be submitted for execution
        UNIT_ASSERT_VALUES_EQUAL(1, env.WritePromises.size());
        UNIT_ASSERT_VALUES_EQUAL(0, env.ZeroPromises.size());
    }

    Y_UNIT_TEST(OverlappedWritesShouldBeDelayed)
    {
        //      [-------1------]
        //   [--2--]
        //                   [--3--]
        //
        // Request #1 delays the execution of requests 2 and 3.

       TTestEnvironment env;

        TString data;
        data.resize(10 * DefaultBlockSize, 0);
        TSgList sglist{TBlockDataRef(data.data(), data.size())};
        TSgList sglist5{TBlockDataRef(data.data(), 5 * DefaultBlockSize)};

        // Run write request [1, 10]
        auto request1 = std::make_shared<TWriteBlocksLocalRequest>(
            TRequestHeaders{.RequestId = 1},
            TBlockRange64::WithLength(1, 10));
        request1->Sglist = TGuardedSgList(env.Sglist10);
        auto future1 = env.Wrapper->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request1));
        UNIT_ASSERT_VALUES_EQUAL(false, future1.HasValue());

        // Run zero request overlapped with first write request
        auto request2 = std::make_shared<TZeroBlocksLocalRequest>(
            TRequestHeaders{.RequestId = 2},
            TBlockRange64::WithLength(0, 5));
        auto future2 = env.Wrapper->ZeroBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request2));
        UNIT_ASSERT_VALUES_EQUAL(false, future2.HasValue());

        // Run write request overlapped with first write request
        auto request3 = std::make_shared<TWriteBlocksLocalRequest>(
            TRequestHeaders{.RequestId = 3},
            TBlockRange64::WithLength(9, 5));
        request3->Sglist = TGuardedSgList(env.Sglist5);

        auto future3 = env.Wrapper->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request3));
        UNIT_ASSERT_VALUES_EQUAL(false, future3.HasValue());

        // Only first write request should be submitted for execution
        UNIT_ASSERT_VALUES_EQUAL(1, env.WritePromises.size());
        UNIT_ASSERT_VALUES_EQUAL(true, env.WritePromises.contains(1));
        UNIT_ASSERT_VALUES_EQUAL(0, env.ZeroPromises.size());

        // Complete first write request with error
        env.WritePromises[1].SetValue(
            TWriteBlocksLocalResponse{.Error = MakeError(E_REJECTED)});
        const auto& result1 = future1.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            result1.Error.code(),
            FormatError(result1.Error));

        // Zero and second write requests should be submitted for execution
        UNIT_ASSERT_VALUES_EQUAL(2, env.WritePromises.size());
        UNIT_ASSERT_VALUES_EQUAL(true, env.WritePromises.contains(1));
        UNIT_ASSERT_VALUES_EQUAL(true, env.WritePromises.contains(3));
        UNIT_ASSERT_VALUES_EQUAL(1, env.ZeroPromises.size());
        UNIT_ASSERT_VALUES_EQUAL(true, env.ZeroPromises.contains(2));

        // Complete zero request
        env.ZeroPromises[2].SetValue(TZeroBlocksLocalResponse{});
        const auto& result2 = future2.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result2.Error.code(),
            FormatError(result2.Error));

        // Complete write request
        env.WritePromises[3].SetValue(TWriteBlocksLocalResponse{});
        const auto& result3 = future3.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result3.Error.code(),
            FormatError(result3.Error));
    }

    Y_UNIT_TEST(OverlappedRunningWritesShouldDelayOtherWrites)
    {
        // [---1---]
        //     [---2---]
        //          [---3---]
        // Request #1 should delay the execution of request #2. But request #2
        // should not delay the execution of request #3 while it is itself
        // deferred.

        TTestEnvironment env;

        // Run write request [1, 10]
        auto request1 = std::make_shared<TWriteBlocksLocalRequest>(
            TRequestHeaders{.RequestId = 1},
            TBlockRange64::WithLength(1, 10));
        request1->Sglist = TGuardedSgList(env.Sglist10);
        auto future1 = env.Wrapper->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request1));
        UNIT_ASSERT_VALUES_EQUAL(false, future1.HasValue());

        // Run zero request overlapped with first write request
        auto request2 = std::make_shared<TZeroBlocksLocalRequest>(
            TRequestHeaders{.RequestId = 2},
            TBlockRange64::WithLength(9, 5));
        auto future2 = env.Wrapper->ZeroBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request2));
        UNIT_ASSERT_VALUES_EQUAL(false, future2.HasValue());

        // Run write request overlapped with zero request
        auto request3 = std::make_shared<TWriteBlocksLocalRequest>(
            TRequestHeaders{.RequestId = 3},
            TBlockRange64::WithLength(12, 5));
        request3->Sglist = TGuardedSgList(env.Sglist5);

        auto future3 = env.Wrapper->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request3));
        UNIT_ASSERT_VALUES_EQUAL(false, future3.HasValue());

        // Both write request should be submitted for execution
        UNIT_ASSERT_VALUES_EQUAL(2, env.WritePromises.size());
        UNIT_ASSERT_VALUES_EQUAL(true, env.WritePromises.contains(1));
        UNIT_ASSERT_VALUES_EQUAL(true, env.WritePromises.contains(3));
        UNIT_ASSERT_VALUES_EQUAL(0, env.ZeroPromises.size());

        // Complete first write request with error
        env.WritePromises[1].SetValue(
            TWriteBlocksLocalResponse{.Error = MakeError(E_REJECTED)});
        const auto& result1 = future1.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            result1.Error.code(),
            FormatError(result1.Error));

        // Zero request should delayed since it overlapped with second write
        UNIT_ASSERT_VALUES_EQUAL(0, env.ZeroPromises.size());

        // Complete write request with S_OK
        env.WritePromises[3].SetValue(TWriteBlocksLocalResponse{});
        const auto& result3 = future3.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result3.Error.code(),
            FormatError(result3.Error));

        // Zero request should running
        UNIT_ASSERT_VALUES_EQUAL(1, env.ZeroPromises.size());
        UNIT_ASSERT_VALUES_EQUAL(true, env.ZeroPromises.contains(2));

        // Complete zero request
        env.ZeroPromises[2].SetValue(TZeroBlocksLocalResponse{});
        const auto& result2 = future2.GetValue(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            result2.Error.code(),
            FormatError(result2.Error));
    }
}

}   // namespace NYdb::NBS::NBlockStore

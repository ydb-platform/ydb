#include "storage_gate.h"

#include "context.h"
#include "storage_test.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

auto MakeReadRequest()
{
    return std::make_shared<TReadBlocksLocalRequest>(TRequestHeaders{});
}

auto MakeWriteRequest()
{
    return std::make_shared<TWriteBlocksLocalRequest>(TRequestHeaders{});
}

auto MakeZeroRequest()
{
    return std::make_shared<TZeroBlocksLocalRequest>(TRequestHeaders{});
}

std::shared_ptr<TTestStorage>
MakeCountingStorage(ui32& readCount, ui32& writeCount, ui32& zeroCount)
{
    auto storage = std::make_shared<TTestStorage>();

    storage->ReadBlocksLocalHandler =
        [&](TCallContextPtr callContext,
            std::shared_ptr<TReadBlocksLocalRequest> request)
        -> TFuture<TReadBlocksLocalResponse>
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);
        ++readCount;
        return MakeFuture(TReadBlocksLocalResponse());
    };

    storage->WriteBlocksLocalHandler =
        [&](TCallContextPtr callContext,
            std::shared_ptr<TWriteBlocksLocalRequest> request)
        -> TFuture<TWriteBlocksLocalResponse>
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);
        ++writeCount;
        return MakeFuture(TWriteBlocksLocalResponse());
    };

    storage->ZeroBlocksLocalHandler =
        [&](TCallContextPtr callContext,
            std::shared_ptr<TZeroBlocksLocalRequest> request)
        -> TFuture<TZeroBlocksLocalResponse>
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);
        ++zeroCount;
        return MakeFuture(TZeroBlocksLocalResponse());
    };

    return storage;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TStorageGateTest)
{
    Y_UNIT_TEST(ShouldForwardRequestsToAttachedStorage)
    {
        ui32 readCount = 0;
        ui32 writeCount = 0;
        ui32 zeroCount = 0;

        auto storage = MakeCountingStorage(readCount, writeCount, zeroCount);
        TStorageGate gate(storage);

        {
            auto response = gate.ReadBlocksLocal(
                                    MakeIntrusive<TCallContext>(),
                                    MakeReadRequest())
                                .GetValue(TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.Error.GetCode(),
                FormatError(response.Error));
        }

        {
            auto response = gate.WriteBlocksLocal(
                                    MakeIntrusive<TCallContext>(),
                                    MakeWriteRequest())
                                .GetValue(TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.Error.GetCode(),
                FormatError(response.Error));
        }

        {
            auto response = gate.ZeroBlocksLocal(
                                    MakeIntrusive<TCallContext>(),
                                    MakeZeroRequest())
                                .GetValue(TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.Error.GetCode(),
                FormatError(response.Error));
        }

        UNIT_ASSERT_VALUES_EQUAL(1, readCount);
        UNIT_ASSERT_VALUES_EQUAL(1, writeCount);
        UNIT_ASSERT_VALUES_EQUAL(1, zeroCount);
    }

    Y_UNIT_TEST(ShouldRejectRequestsWhenDetached)
    {
        ui32 readCount = 0;
        ui32 writeCount = 0;
        ui32 zeroCount = 0;

        auto storage = MakeCountingStorage(readCount, writeCount, zeroCount);
        TStorageGate gate(storage);

        gate.Detach();

        {
            auto response = gate.ReadBlocksLocal(
                                    MakeIntrusive<TCallContext>(),
                                    MakeReadRequest())
                                .GetValue(TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response.Error.GetCode(),
                FormatError(response.Error));
        }

        {
            auto response = gate.WriteBlocksLocal(
                                    MakeIntrusive<TCallContext>(),
                                    MakeWriteRequest())
                                .GetValue(TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response.Error.GetCode(),
                FormatError(response.Error));
        }

        {
            auto response = gate.ZeroBlocksLocal(
                                    MakeIntrusive<TCallContext>(),
                                    MakeZeroRequest())
                                .GetValue(TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response.Error.GetCode(),
                FormatError(response.Error));
        }

        // No requests should reach the underlying storage.
        UNIT_ASSERT_VALUES_EQUAL(0, readCount);
        UNIT_ASSERT_VALUES_EQUAL(0, writeCount);
        UNIT_ASSERT_VALUES_EQUAL(0, zeroCount);
    }

    Y_UNIT_TEST(ShouldForwardRequestsAfterReattach)
    {
        ui32 readCount = 0;
        ui32 writeCount = 0;
        ui32 zeroCount = 0;

        auto storage = MakeCountingStorage(readCount, writeCount, zeroCount);
        TStorageGate gate(storage);

        gate.Detach();

        {
            auto response = gate.ReadBlocksLocal(
                                    MakeIntrusive<TCallContext>(),
                                    MakeReadRequest())
                                .GetValue(TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response.Error.GetCode());
        }

        gate.Attach(storage);

        {
            auto response = gate.ReadBlocksLocal(
                                    MakeIntrusive<TCallContext>(),
                                    MakeReadRequest())
                                .GetValue(TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.Error.GetCode(),
                FormatError(response.Error));
        }

        UNIT_ASSERT_VALUES_EQUAL(1, readCount);
    }

    Y_UNIT_TEST(ShouldSwitchToNewStorageOnAttach)
    {
        ui32 firstReadCount = 0;
        ui32 firstWriteCount = 0;
        ui32 firstZeroCount = 0;
        auto firstStorage = MakeCountingStorage(
            firstReadCount,
            firstWriteCount,
            firstZeroCount);

        ui32 secondReadCount = 0;
        ui32 secondWriteCount = 0;
        ui32 secondZeroCount = 0;
        auto secondStorage = MakeCountingStorage(
            secondReadCount,
            secondWriteCount,
            secondZeroCount);

        TStorageGate gate(firstStorage);

        gate.ReadBlocksLocal(MakeIntrusive<TCallContext>(), MakeReadRequest())
            .GetValue(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(1, firstReadCount);
        UNIT_ASSERT_VALUES_EQUAL(0, secondReadCount);

        gate.Attach(secondStorage);

        gate.ReadBlocksLocal(MakeIntrusive<TCallContext>(), MakeReadRequest())
            .GetValue(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(1, firstReadCount);
        UNIT_ASSERT_VALUES_EQUAL(1, secondReadCount);
    }
}

}   // namespace NYdb::NBS::NBlockStore

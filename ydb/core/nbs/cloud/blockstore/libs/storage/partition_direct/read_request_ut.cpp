#include "base_test_fixture.h"
#include "read_request_executor.h"
#include "read_request_multiple_location.h"
#include "read_request_single_location.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NThreading;

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TReadRequestTest)
{
    Y_UNIT_TEST_F(ShouldCreateSingleLocationExecutor, TBaseFixture)
    {
        Init();

        const TBlockRange64 range = TBlockRange64::WithLength(10, 1000);
        ExpectedRange = range;

        auto callContext = MakeIntrusive<TCallContext>(static_cast<ui64>(0));
        auto originalRequest =
            std::make_shared<TReadBlocksLocalRequest>(TRequestHeaders{
                .VolumeConfig = PartitionDirectService->GetVolumeConfig(),
                .RequestId = 1,
                .Range = range});

        auto readHint = DirtyMap.MakeReadHint(range);
        auto readRequest = CreateReadRequestExecutor(
            Runtime->GetActorSystem(0),
            LogTitle,
            VChunkConfig,
            DirectBlockGroup,
            std::move(readHint),
            std::move(callContext),
            std::move(originalRequest),
            NWilson::TTraceId());

        UNIT_ASSERT(
            std::dynamic_pointer_cast<TReadSingleLocationRequestExecutor>(
                readRequest) != nullptr);
        UNIT_ASSERT(
            std::dynamic_pointer_cast<TReadMultipleLocationRequestExecutor>(
                readRequest) == nullptr);

        auto future = readRequest->GetFuture();
        readRequest->Run();
        SetReadResult({.Error = MakeError(S_OK)}, false);
    }

    Y_UNIT_TEST_F(ShouldCreateMultipleLocationExecutor, TBaseFixture)
    {
        Init();

        const TBlockRange64 range = TBlockRange64::WithLength(10, 1000);
        ExpectedRange = range;

        auto callContext = MakeIntrusive<TCallContext>(static_cast<ui64>(0));
        auto originalRequest =
            std::make_shared<TReadBlocksLocalRequest>(TRequestHeaders{
                .VolumeConfig = PartitionDirectService->GetVolumeConfig(),
                .RequestId = 1,
                .Range = range});

        DirtyMap.RegisterInflightWrite(100, TBlockRange64::WithLength(20, 10));
        DirtyMap.WriteFinished(
            100,
            TBlockRange64::WithLength(20, 10),
            VChunkConfig.GetDesiredPBuffers(),
            VChunkConfig.GetDesiredPBuffers());
        auto readHint = DirtyMap.MakeReadHint(range);
        auto readRequest = CreateReadRequestExecutor(
            Runtime->GetActorSystem(0),
            LogTitle,
            VChunkConfig,
            DirectBlockGroup,
            std::move(readHint),
            std::move(callContext),
            std::move(originalRequest),
            NWilson::TTraceId());

        UNIT_ASSERT(
            std::dynamic_pointer_cast<TReadMultipleLocationRequestExecutor>(
                readRequest) != nullptr);
        UNIT_ASSERT(
            std::dynamic_pointer_cast<TReadSingleLocationRequestExecutor>(
                readRequest) == nullptr);

        auto future = readRequest->GetFuture();
        readRequest->Run();
        SetReadResult({.Error = MakeError(S_OK)}, false);
    }

    Y_UNIT_TEST_F(ShouldRead, TBaseFixture)
    {
        Init();

        const TBlockRange64 range = TBlockRange64::WithLength(10, 10);
        ExpectedRange = range;

        auto readHint = DirtyMap.MakeReadHint(range);
        auto callContext = MakeIntrusive<TCallContext>(static_cast<ui64>(0));
        auto originalRequest =
            std::make_shared<TReadBlocksLocalRequest>(TRequestHeaders{
                .VolumeConfig = PartitionDirectService->GetVolumeConfig(),
                .RequestId = 1,
                .Range = range});

        auto readRequest = CreateReadRequestExecutor(
            Runtime->GetActorSystem(0),
            LogTitle,
            VChunkConfig,
            DirectBlockGroup,
            std::move(readHint),
            std::move(callContext),
            std::move(originalRequest),
            NWilson::TTraceId());
        auto future = readRequest->GetFuture();
        readRequest->Run();
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());

        SetReadResult({.Error = MakeError(S_OK)}, false);
        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
    }

    Y_UNIT_TEST_F(ShouldReadMultipleLocations, TBaseFixture)
    {
        Init();

        DirtyMap.RegisterInflightWrite(100, TBlockRange64::WithLength(20, 10));
        DirtyMap.WriteFinished(
            100,
            TBlockRange64::WithLength(20, 10),
            VChunkConfig.GetDesiredPBuffers(),
            VChunkConfig.GetDesiredPBuffers());

        DirtyMap.RegisterInflightWrite(200, TBlockRange64::WithLength(40, 10));
        DirtyMap.WriteFinished(
            200,
            TBlockRange64::WithLength(40, 10),
            VChunkConfig.GetDesiredPBuffers(),
            VChunkConfig.GetDesiredPBuffers());

        const TBlockRange64 range = TBlockRange64::WithLength(10, 100);
        ExpectedRange = range;
        RangeData = GenerateRandomString(ExpectedRange.Size() * BlockSize);

        auto readHint = DirtyMap.MakeReadHint(range);
        UNIT_ASSERT_VALUES_EQUAL(5, readHint.RangeHints.size());

        auto callContext = MakeIntrusive<TCallContext>(static_cast<ui64>(0));
        auto originalRequest =
            std::make_shared<TReadBlocksLocalRequest>(TRequestHeaders{
                .VolumeConfig = PartitionDirectService->GetVolumeConfig(),
                .RequestId = 1,
                .Range = range});
        TSgList sglist;
        TString readBuffer(RangeData.size(), '\0');
        sglist.push_back(TBlockDataRef{readBuffer.data(), readBuffer.size()});
        originalRequest->Sglist = TGuardedSgList(std::move(sglist));

        auto readRequest = CreateReadRequestExecutor(
            Runtime->GetActorSystem(0),
            LogTitle,
            VChunkConfig,
            DirectBlockGroup,
            std::move(readHint),
            std::move(callContext),
            std::move(originalRequest),
            NWilson::TTraceId());
        auto future = readRequest->GetFuture();
        readRequest->Run();
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());

        SetReadResult({.Error = MakeError(S_OK)}, false);
        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());

        UNIT_ASSERT_VALUES_EQUAL(RangeData, readBuffer);
    }

    Y_UNIT_TEST_F(ShouldHedgeReading, TBaseFixture)
    {
        Init();

        DirectBlockGroup->Oracle.ReadHedgingDelay = TDuration::Seconds(1);
        DirectBlockGroup->Oracle.ReadRequestTimeout = TDuration::Seconds(10);

        const TBlockRange64 range = TBlockRange64::WithLength(10, 10);
        ExpectedRange = range;

        auto readHint = DirtyMap.MakeReadHint(range);
        auto callContext = MakeIntrusive<TCallContext>(static_cast<ui64>(0));
        auto originalRequest =
            std::make_shared<TReadBlocksLocalRequest>(TRequestHeaders{
                .VolumeConfig = PartitionDirectService->GetVolumeConfig(),
                .RequestId = 1,
                .Range = range});

        auto readRequest = CreateReadRequestExecutor(
            Runtime->GetActorSystem(0),
            LogTitle,
            VChunkConfig,
            DirectBlockGroup,
            std::move(readHint),
            std::move(callContext),
            std::move(originalRequest),
            NWilson::TTraceId());
        auto future = readRequest->GetFuture();
        readRequest->Run();
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(1, ReadPromises.size());

        UNIT_ASSERT_VALUES_EQUAL(2, ScheduledTasks.size());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(10),
            ScheduledTasks[0].Delay);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1),
            ScheduledTasks[1].Delay);

        // Run hedging task
        ExpectedHost = 1;
        ScheduledTasks[1].Callback();
        UNIT_ASSERT_VALUES_EQUAL(2, ReadPromises.size());

        // Response with error for first request. This will trigger another
        // request to the third host.
        ExpectedHost = 2;
        ReadPromises[0].SetValue({.Error = MakeError(E_FAIL)});
        UNIT_ASSERT_VALUES_EQUAL(3, ReadPromises.size());
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());

        // Response with error for third request. This will not trigger another
        // request.
        ReadPromises[2].SetValue({.Error = MakeError(E_FAIL)});
        UNIT_ASSERT_VALUES_EQUAL(3, ReadPromises.size());
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());

        // Response with success for second request.
        ReadPromises[1].SetValue({.Error = MakeError(S_OK)});
        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

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
        SetReadResult({.Error = MakeError(S_OK)});
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

        DirtyMap.WriteFinished(
            100,
            TBlockRange64::WithLength(20, 10),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());
        auto readHint = DirtyMap.MakeReadHint(range);
        auto readRequest = CreateReadRequestExecutor(
            Runtime->GetActorSystem(0),
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
        SetReadResult({.Error = MakeError(S_OK)});
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
            VChunkConfig,
            DirectBlockGroup,
            std::move(readHint),
            std::move(callContext),
            std::move(originalRequest),
            NWilson::TTraceId());
        auto future = readRequest->GetFuture();
        readRequest->Run();
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());

        SetReadResult({.Error = MakeError(S_OK)});
        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
    }

    Y_UNIT_TEST_F(ShouldReadMultipleLocations, TBaseFixture)
    {
        Init();

        DirtyMap.WriteFinished(
            100,
            TBlockRange64::WithLength(20, 10),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());

        DirtyMap.WriteFinished(
            200,
            TBlockRange64::WithLength(40, 10),
            TLocationMask::MakePrimaryPBuffers(),
            TLocationMask::MakePrimaryPBuffers());

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
            VChunkConfig,
            DirectBlockGroup,
            std::move(readHint),
            std::move(callContext),
            std::move(originalRequest),
            NWilson::TTraceId());
        auto future = readRequest->GetFuture();
        readRequest->Run();
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());

        SetReadResult({.Error = MakeError(S_OK)});
        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());

        UNIT_ASSERT_VALUES_EQUAL(RangeData, readBuffer);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

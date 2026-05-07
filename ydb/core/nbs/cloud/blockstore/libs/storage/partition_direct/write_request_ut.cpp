#include "write_request.h"

#include "base_test_fixture.h"
#include "write_with_direct_replication_request.h"
#include "write_with_pb_replication_request.h"
#include "write_with_pb_test_fixture.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NThreading;

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

TRequestHeaders MakeWriteTestRequestHeaders(
    const TBlockRange64& range,
    ui32 blockSize)
{
    auto volumeConfig = std::make_shared<TVolumeConfig>(TVolumeConfig{
        .DiskId = "disk-1",
        .BlockSize = blockSize,
        .BlockCount = 65536,
        .BlocksPerStripe = 1024,
        .VChunkSize = DefaultVChunkSize});

    return TRequestHeaders{
        .VolumeConfig = std::move(volumeConfig),
        .RequestId = 1,
        .Range = range};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TWriteRequestTest)
{
    Y_UNIT_TEST_F(ShouldWrite, TBaseFixture)
    {
        const ui64 userLsn = 123;
        const TBlockRange64 range = TBlockRange64::WithLength(10, 10);
        const auto hedgeDelay = TDuration::MilliSeconds(1000);
        const auto timeout = TDuration::MilliSeconds(1000);

        Init();

        TVector<std::pair<TDuration, TCallback>> scheduled;
        DirectBlockGroup->ScheduleHandler =
            [&](TDuration delay, TCallback callback)
        {
            scheduled.emplace_back(delay, std::move(callback));
        };

        TMap<ui8, TPromise<TDBGWriteBlocksResponse>> writePBufferPromises;
        DirectBlockGroup->WriteBlocksToPBufferHandler = [&]   //
            (ui32 vChunkIndex,
             ui8 hostIndex,
             ui64 lsn,
             TBlockRange64 range,
             const TGuardedSgList& guardedSglist,
             const NWilson::TTraceId& traceId)
        {
            Y_UNUSED(traceId);
            Y_UNUSED(guardedSglist);

            UNIT_ASSERT_C(userLsn, lsn);
            UNIT_ASSERT_VALUES_EQUAL(VChunkConfig.VChunkIndex, vChunkIndex);
            UNIT_ASSERT_VALUES_EQUAL(ExpectedRange, range);

            writePBufferPromises.emplace(
                hostIndex,
                NewPromise<TDBGWriteBlocksResponse>());

            return writePBufferPromises[hostIndex].GetFuture();
        };

        ExpectedRange = range;
        RangeData = GenerateRandomString(BlockSize * range.Size());

        auto callContext = MakeIntrusive<TCallContext>(static_cast<ui64>(0));
        auto originalRequest = std::make_shared<TWriteBlocksLocalRequest>(
            MakeWriteTestRequestHeaders(range, BlockSize));
        originalRequest->Sglist = MakeSgList();

        auto writeRequest =
            std::make_shared<TWriteWithDirectReplicationRequestExecutor>(
                Runtime->GetActorSystem(0),
                VChunkConfig,
                DirectBlockGroup,
                range,
                std::move(callContext),
                std::move(originalRequest),
                userLsn,
                NWilson::TTraceId(),
                hedgeDelay,
                timeout);
        auto future = writeRequest->GetFuture();
        writeRequest->Run();
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(2, scheduled.size());
        UNIT_ASSERT_VALUES_EQUAL(timeout, scheduled[0].first);
        UNIT_ASSERT_VALUES_EQUAL(hedgeDelay, scheduled[1].first);

        UNIT_ASSERT_VALUES_EQUAL(3, writePBufferPromises.size());
        writePBufferPromises[0].SetValue({.Error = MakeError(S_OK)});
        writePBufferPromises[1].SetValue({.Error = MakeError(S_OK)});
        writePBufferPromises[2].SetValue({.Error = MakeError(S_OK)});

        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
    }

    Y_UNIT_TEST_F(ShouldReturnErrorAfterTimeout, TBaseFixture)
    {
        const ui64 userLsn = 123;
        const TBlockRange64 range = TBlockRange64::WithLength(10, 10);
        const auto hedgeDelay = TDuration::MilliSeconds(1000);
        const auto timeout = TDuration::MilliSeconds(1000);

        Init();

        TVector<std::pair<TDuration, TCallback>> scheduled;
        DirectBlockGroup->ScheduleHandler =
            [&](TDuration delay, TCallback callback)
        {
            scheduled.emplace_back(delay, std::move(callback));
        };

        TMap<ui8, TPromise<TDBGWriteBlocksResponse>> writePBufferPromises;
        DirectBlockGroup->WriteBlocksToPBufferHandler = [&]   //
            (ui32 vChunkIndex,
             ui8 hostIndex,
             ui64 lsn,
             TBlockRange64 range,
             const TGuardedSgList& guardedSglist,
             const NWilson::TTraceId& traceId)
        {
            Y_UNUSED(traceId);
            Y_UNUSED(guardedSglist);

            UNIT_ASSERT_C(userLsn, lsn);
            UNIT_ASSERT_VALUES_EQUAL(VChunkConfig.VChunkIndex, vChunkIndex);
            UNIT_ASSERT_VALUES_EQUAL(ExpectedRange, range);

            writePBufferPromises.emplace(
                hostIndex,
                NewPromise<TDBGWriteBlocksResponse>());

            return writePBufferPromises[hostIndex].GetFuture();
        };

        ExpectedRange = range;
        RangeData = GenerateRandomString(BlockSize * range.Size());

        auto callContext = MakeIntrusive<TCallContext>(static_cast<ui64>(0));
        auto originalRequest = std::make_shared<TWriteBlocksLocalRequest>(
            MakeWriteTestRequestHeaders(range, BlockSize));
        originalRequest->Sglist = MakeSgList();

        auto writeRequest =
            std::make_shared<TWriteWithDirectReplicationRequestExecutor>(
                Runtime->GetActorSystem(0),
                VChunkConfig,
                DirectBlockGroup,
                range,
                std::move(callContext),
                std::move(originalRequest),
                userLsn,
                NWilson::TTraceId(),
                hedgeDelay,
                timeout);
        auto future = writeRequest->GetFuture();
        writeRequest->Run();
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(2, scheduled.size());
        UNIT_ASSERT_VALUES_EQUAL(timeout, scheduled[0].first);
        UNIT_ASSERT_VALUES_EQUAL(hedgeDelay, scheduled[1].first);

        UNIT_ASSERT_VALUES_EQUAL(3, writePBufferPromises.size());

        // Run hedge callback.
        scheduled[0].second();

        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, response.Error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("Write request timeout"),
            response.Error.GetMessage());
    }

    Y_UNIT_TEST_F(
        ShouldSucceedWithHedgingWhenPrimariesHangAndHandoffsOkWithDirectReplication,
        TBaseFixture)
    {
        const ui64 userLsn = 123;
        const TBlockRange64 range = TBlockRange64::WithLength(10, 10);
        const auto hedgeDelay = TDuration::MilliSeconds(1000);
        const auto timeout = TDuration::MilliSeconds(1000);

        Init();

        TVector<std::pair<TDuration, TCallback>> scheduled;
        DirectBlockGroup->ScheduleHandler =
            [&](TDuration delay, TCallback callback)
        {
            scheduled.emplace_back(delay, std::move(callback));
        };

        TMap<ui8, TPromise<TDBGWriteBlocksResponse>> writePBufferPromises;
        DirectBlockGroup->WriteBlocksToPBufferHandler = [&]   //
            (ui32 vChunkIndex,
             ui8 hostIndex,
             ui64 lsn,
             TBlockRange64 range,
             const TGuardedSgList& guardedSglist,
             const NWilson::TTraceId& traceId)
        {
            Y_UNUSED(traceId);
            Y_UNUSED(guardedSglist);

            UNIT_ASSERT_C(userLsn, lsn);
            UNIT_ASSERT_VALUES_EQUAL(VChunkConfig.VChunkIndex, vChunkIndex);
            UNIT_ASSERT_VALUES_EQUAL(ExpectedRange, range);

            writePBufferPromises.emplace(
                hostIndex,
                NewPromise<TDBGWriteBlocksResponse>());

            return writePBufferPromises[hostIndex].GetFuture();
        };

        ExpectedRange = range;
        RangeData = GenerateRandomString(BlockSize * range.Size());

        auto callContext = MakeIntrusive<TCallContext>(static_cast<ui64>(0));
        auto originalRequest = std::make_shared<TWriteBlocksLocalRequest>(
            MakeWriteTestRequestHeaders(range, BlockSize));
        originalRequest->Sglist = MakeSgList();

        auto writeRequest =
            std::make_shared<TWriteWithDirectReplicationRequestExecutor>(
                Runtime->GetActorSystem(0),
                VChunkConfig,
                DirectBlockGroup,
                range,
                std::move(callContext),
                std::move(originalRequest),
                userLsn,
                NWilson::TTraceId(),
                hedgeDelay,
                timeout);
        auto future = writeRequest->GetFuture();
        writeRequest->Run();
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(2, scheduled.size());
        UNIT_ASSERT_VALUES_EQUAL(timeout, scheduled[0].first);
        UNIT_ASSERT_VALUES_EQUAL(hedgeDelay, scheduled[1].first);

        UNIT_ASSERT_VALUES_EQUAL(3, writePBufferPromises.size());

        // Run hedge callback.
        scheduled[1].second();

        UNIT_ASSERT_VALUES_EQUAL(5, writePBufferPromises.size());

        writePBufferPromises[VChunkConfig.HandOffHost0].SetValue(
            {.Error = MakeError(S_OK)});
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());

        writePBufferPromises[VChunkConfig.HandOffHost1].SetValue(
            {.Error = MakeError(S_OK)});
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());

        writePBufferPromises[VChunkConfig.PrimaryHost2].SetValue(
            {.Error = MakeError(S_OK)});

        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
        UNIT_ASSERT_EQUAL(
            TLocationMask::MakePBuffer(true, true, true, true, true),
            response.RequestedWrites);
        UNIT_ASSERT_EQUAL(
            TLocationMask::MakePBuffer(false, false, true, true, true),
            response.CompletedWrites);
    }

    Y_UNIT_TEST_F(ShouldNotSendWriteRequestAfterQuorumIsReached, TBaseFixture)
    {
        const ui64 userLsn = 123;
        const TBlockRange64 range = TBlockRange64::WithLength(10, 10);
        const auto hedgeDelay = TDuration::MilliSeconds(100);
        const auto timeout = TDuration::MilliSeconds(10000);

        Init();

        TVector<std::pair<TDuration, TCallback>> scheduled;
        DirectBlockGroup->ScheduleHandler =
            [&](TDuration delay, TCallback callback)
        {
            scheduled.emplace_back(delay, std::move(callback));
        };

        TMap<ui8, TPromise<TDBGWriteBlocksResponse>> writePBufferPromises;
        DirectBlockGroup->WriteBlocksToPBufferHandler = [&]   //
            (ui32 vChunkIndex,
             ui8 hostIndex,
             ui64 lsn,
             TBlockRange64 range,
             const TGuardedSgList& guardedSglist,
             const NWilson::TTraceId& traceId)
        {
            Y_UNUSED(traceId);
            Y_UNUSED(guardedSglist);

            UNIT_ASSERT_C(userLsn, lsn);
            UNIT_ASSERT_VALUES_EQUAL(VChunkConfig.VChunkIndex, vChunkIndex);
            UNIT_ASSERT_VALUES_EQUAL(ExpectedRange, range);

            writePBufferPromises.emplace(
                hostIndex,
                NewPromise<TDBGWriteBlocksResponse>());

            return writePBufferPromises[hostIndex].GetFuture();
        };

        ExpectedRange = range;
        RangeData = GenerateRandomString(BlockSize * range.Size());

        auto callContext = MakeIntrusive<TCallContext>(static_cast<ui64>(0));
        auto originalRequest = std::make_shared<TWriteBlocksLocalRequest>(
            MakeWriteTestRequestHeaders(range, BlockSize));
        originalRequest->Sglist = MakeSgList();

        auto writeRequest =
            std::make_shared<TWriteWithDirectReplicationRequestExecutor>(
                Runtime->GetActorSystem(0),
                VChunkConfig,
                DirectBlockGroup,
                range,
                std::move(callContext),
                std::move(originalRequest),
                userLsn,
                NWilson::TTraceId(),
                hedgeDelay,
                timeout);
        auto future = writeRequest->GetFuture();
        writeRequest->Run();

        UNIT_ASSERT_VALUES_EQUAL(2, scheduled.size());
        UNIT_ASSERT_VALUES_EQUAL(timeout, scheduled[0].first);
        UNIT_ASSERT_VALUES_EQUAL(hedgeDelay, scheduled[1].first);

        UNIT_ASSERT_VALUES_EQUAL(3, writePBufferPromises.size());
        writePBufferPromises[VChunkConfig.PrimaryHost0].SetValue(
            {.Error = MakeError(S_OK)});
        writePBufferPromises[VChunkConfig.PrimaryHost1].SetValue(
            {.Error = MakeError(S_OK)});
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());

        scheduled[1].second();
        UNIT_ASSERT_VALUES_EQUAL(4, writePBufferPromises.size());
        UNIT_ASSERT(!writePBufferPromises.contains(VChunkConfig.HandOffHost1));

        writePBufferPromises[VChunkConfig.PrimaryHost2].SetValue(
            {.Error = MakeError(S_OK)});

        writePBufferPromises[VChunkConfig.HandOffHost0].SetValue(
            {.Error = MakeError(E_FAIL, "hedged handoff failed")});

        UNIT_ASSERT_VALUES_EQUAL(4, writePBufferPromises.size());
        UNIT_ASSERT(!writePBufferPromises.contains(VChunkConfig.HandOffHost1));

        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
        UNIT_ASSERT_EQUAL(
            TLocationMask::MakePBuffer(true, true, true, true, false),
            response.RequestedWrites);
        UNIT_ASSERT_EQUAL(
            TLocationMask::MakePBuffer(true, true, true, false, false),
            response.CompletedWrites);
    }
//------------------------

    // @brief we want to sure that base path with no errors works
    Y_UNIT_TEST_F(
        ShouldBaseSuccessPathWithPbReplication,
        TWriteWithPbTestFixture)
    {
        // make main request ok reply
        DirectBlockGroup->WriteBlocksToManyPBuffersHandler = GetManyPBuffersHandlerWithImmediateOkResponse();

        // prepare and call main request
        auto writeRequest = CreateRequest(MakeWriteTestRequestHeaders(Range, BlockSize));
        auto future = writeRequest->GetFuture();
        writeRequest->Run();

        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(true, ManyPBufferPromise.HasValue());

        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
        UNIT_ASSERT_EQUAL(
            TLocationMask::MakePBuffer(true, true, true, false, false),
            response.RequestedWrites);
        UNIT_ASSERT_EQUAL(
            TLocationMask::MakePBuffer(true, true, true, false, false),
            response.CompletedWrites);
    }

    // @brief we want to sure that in case of hanging main 'multi' request,
    // hedge mechanism will work
    Y_UNIT_TEST_F(
        ShouldSucceedWithHedgingWhenPrimariesHangAndHandoffsOkWithPbReplication,
        TWriteWithPbTestFixture)
    {
        // make main request hanging
        DirectBlockGroup->WriteBlocksToManyPBuffersHandler = GetManyPBuffersHandlerHanging();

        // prepare and call main request
        auto writeRequest = CreateRequest(MakeWriteTestRequestHeaders(Range, BlockSize));
        auto future = writeRequest->GetFuture();
        writeRequest->Run();

        // as response is hanging, there is no results
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(false, ManyPBufferPromise.HasValue());

        // call hedge mechanism. It will work with default response's handler
        // from base fixture
        RunScheduledHedge();

        UNIT_ASSERT_VALUES_EQUAL(false, ManyPBufferPromise.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
        UNIT_ASSERT_EQUAL(
            TLocationMask::MakePBuffer(true, true, true, true, true),
            response.RequestedWrites);

        // make sure that there were successful hedge requests
        UNIT_ASSERT_EQUAL(
            true,
            response.CompletedWrites.Get(ELocation::HOPBuffer0));
        UNIT_ASSERT_EQUAL(
            true,
            response.CompletedWrites.Get(ELocation::HOPBuffer1));
    }

    // @brief sending main request then hedge requests.
    // main's responses come before hedge and return reply to the caller
    Y_UNIT_TEST_F(
        ShouldMainPlusHedgeAndReplyFromMainWithPbReplication,
        TWriteWithPbTestFixture)
    {
        // make main request hanging
        DirectBlockGroup->WriteBlocksToManyPBuffersHandler = GetManyPBuffersHandlerHanging();

        DirectBlockGroup->WriteBlocksToPBufferHandler = GetDirectWriteHandlerHanging();

        // prepare and call main request
        auto writeRequest = CreateRequest(MakeWriteTestRequestHeaders(Range, BlockSize));
        auto future = writeRequest->GetFuture();
        writeRequest->Run();

        // as response is hanging, there is no results
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(false, ManyPBufferPromise.HasValue());

        // call hedge mechanism
        RunScheduledHedge();

        // hedge is hanging too
        UNIT_ASSERT_VALUES_EQUAL(false, ManyPBufferPromise.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());

        // reply from main request
        ManyPBufferPromise.SetValue(CreateOkResponse());
        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());

        // but there were sent requests to HO too
        UNIT_ASSERT_EQUAL(
            TLocationMask::MakePBuffer(true, true, true, true, true),
            response.RequestedWrites);

        UNIT_ASSERT_EQUAL(
            TLocationMask::MakePBuffer(true, true, true, false, false),
            response.CompletedWrites);
    }

    // @brief sending main request then hedge requests.
    // main's responses come partially before hedge
    // return reply with mix of main's and hedge replies
    Y_UNIT_TEST_F(
        ShouldMainPlusHedgeAndReplyWithMixWithPbReplication,
        TWriteWithPbTestFixture)
    {
        // make main request hanging
        DirectBlockGroup->WriteBlocksToManyPBuffersHandler = GetManyPBuffersHandlerHanging();

        DirectBlockGroup->WriteBlocksToPBufferHandler = GetDirectWriteHandlerHanging();

        // prepare and call main request
        auto writeRequest = CreateRequest(MakeWriteTestRequestHeaders(Range, BlockSize));
        auto future = writeRequest->GetFuture();
        writeRequest->Run();

        // as response is hanging, there is no results
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(false, ManyPBufferPromise.HasValue());

        // call hedge mechanism
        RunScheduledHedge();

        // hedge is hanging too
        UNIT_ASSERT_VALUES_EQUAL(false, ManyPBufferPromise.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());

        // partially reply from main request
        ManyPBufferPromise.SetValue(CreateOneOkResponse());
        auto manyPBufferResult = ManyPBufferPromise.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(1, manyPBufferResult.Responses.size());

        UNIT_ASSERT_VALUES_EQUAL(false, future.HasValue());

        UNIT_ASSERT_VALUES_EQUAL(3, DirectWritePromises.size());
        DirectWritePromises[0].SetValue(CreateOkDirectResponse());
        DirectWritePromises[1].SetValue(CreateOkDirectResponse());

        // reply is ready
        UNIT_ASSERT_VALUES_EQUAL(true, future.HasValue());
        const auto& response = future.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(3, response.CompletedWrites.Count());

        // but there were sent requests to HO too
        UNIT_ASSERT_EQUAL(
            TLocationMask::MakePBuffer(true, true, true, true, true),
            response.RequestedWrites);

        UNIT_ASSERT_EQUAL(
            TLocationMask::MakePBuffer(true, false, false, true, true),
            response.CompletedWrites);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

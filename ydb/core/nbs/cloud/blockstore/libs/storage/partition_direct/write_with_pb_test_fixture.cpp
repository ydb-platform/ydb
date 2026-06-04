#include "write_with_pb_test_fixture.h"

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NThreading;

TDBGWriteBlocksResponse TWriteWithPbTestFixture::CreateOkDirectResponse()
{
    return {.Error = MakeError(S_OK)};
}

TDBGWriteBlocksResponse TWriteWithPbTestFixture::CreateFailDirectResponse()
{
    return {.Error = MakeError(E_FAIL, "direct write failed")};
}

TWriteWithPbTestFixture::TWriteWithPbTestFixture()
    : UserLsn{123}
    , Range{TBlockRange64::WithLength(10, 10)}
    , HedgeDelay{TDuration::MilliSeconds(1000)}
    , Timeout{TDuration::MilliSeconds(1000)}
    , PBufferReplyTimeout{TDuration::MilliSeconds(500)}
{
    Init();
    DirectBlockGroup->Oracle.WriteHedgingDelay = HedgeDelay;
    DirectBlockGroup->Oracle.WriteRequestTimeout = Timeout;

    ExpectedRange = Range;
    RangeData = GenerateRandomString(BlockSize * Range.Size());

    DirectBlockGroup->ScheduleHandler = [&](TDuration delay, TCallback callback)
    {
        Scheduled.emplace_back(delay, std::move(callback));
    };
}

TDirectBlockGroupMock::TWriteBlocksToManyPBuffersHandler
TWriteWithPbTestFixture::GetManyPBuffersHandlerWithImmediateOkResponse()
{
    auto result =
        [this](
            ui32 vChunkIndex,
            THostIndex coordinatorHostIndex,
            std::vector<THostIndex> hostIndexes,
            ui64 lsn,
            TBlockRange64 range,
            TDuration replyTimeout,
            const TGuardedSgList& guardedSglist,
            const NWilson::TTraceId& traceId,
            IDirectBlockGroup::TWriteBlocksToManyPBuffersCallback callback)
    {
        Y_UNUSED(coordinatorHostIndex, replyTimeout, guardedSglist, traceId);

        UNIT_ASSERT_VALUES_EQUAL(UserLsn, lsn);
        UNIT_ASSERT_VALUES_EQUAL(VChunkConfig.GetVChunkIndex(), vChunkIndex);
        UNIT_ASSERT_VALUES_EQUAL(ExpectedRange, range);

        UNIT_ASSERT_VALUES_EQUAL(3u, hostIndexes.size());

        UNIT_ASSERT_EQUAL(
            true,
            VChunkConfig.GetDesiredPBuffers().Get(hostIndexes[0]));
        UNIT_ASSERT_EQUAL(
            true,
            VChunkConfig.GetDesiredPBuffers().Get(hostIndexes[1]));
        UNIT_ASSERT_EQUAL(
            true,
            VChunkConfig.GetDesiredPBuffers().Get(hostIndexes[2]));

        callback(CreateOkResponse());
    };

    return result;
}

TDirectBlockGroupMock::TWriteBlocksToManyPBuffersHandler
TWriteWithPbTestFixture::GetManyPBuffersHandlerHanging()
{
    auto result =
        [this](
            ui32 vChunkIndex,
            THostIndex coordinatorHostIndex,
            std::vector<THostIndex> hostIndexes,
            ui64 lsn,
            TBlockRange64 range,
            TDuration replyTimeout,
            const TGuardedSgList& guardedSglist,
            const NWilson::TTraceId& traceId,
            IDirectBlockGroup::TWriteBlocksToManyPBuffersCallback callback)
    {
        Y_UNUSED(
            vChunkIndex,
            coordinatorHostIndex,
            hostIndexes,
            lsn,
            range,
            replyTimeout,
            guardedSglist,
            traceId);

        // Store the callback so tests can invoke it later to simulate a
        // response (possibly multiple times).
        ManyPBufferCallback = std::move(callback);
    };

    return result;
}

TDirectBlockGroupMock::TWriteBlocksToPBufferHandler
TWriteWithPbTestFixture::GetDirectWriteHandlerHanging()
{
    auto result = [this]   //
        (ui32 vChunkIndex,
         ui8 hostIndex,
         ui64 lsn,
         TBlockRange64 range,
         const TGuardedSgList& guardedSglist,
         const NWilson::TTraceId& traceId)
    {
        Y_UNUSED(hostIndex, lsn, traceId, guardedSglist);

        UNIT_ASSERT_VALUES_EQUAL(VChunkConfig.GetVChunkIndex(), vChunkIndex);
        UNIT_ASSERT_VALUES_EQUAL(ExpectedRange, range);

        TPromise<TDBGWriteBlocksResponse> response(
            NewPromise<TDBGWriteBlocksResponse>());
        DirectWritePromises.push_back(std::move(response));

        return DirectWritePromises.back().GetFuture();
    };

    return result;
}

std::shared_ptr<TWriteWithPbReplicationRequestExecutor>
TWriteWithPbTestFixture::CreateRequest(TRequestHeaders headers)
{
    auto callContext = MakeIntrusive<TCallContext>(static_cast<ui64>(0));
    auto originalRequest =
        std::make_shared<TWriteBlocksLocalRequest>(std::move(headers));
    originalRequest->Sglist = MakeSgList();

    CallbackResult.reset();
    auto request = std::make_shared<TWriteWithPbReplicationRequestExecutor>(
        Runtime->GetActorSystem(0),
        LogTitle.GetChild(GetCycleCount()),
        VChunkConfig,
        DirectBlockGroup,
        Range,
        std::move(callContext),
        std::move(originalRequest),
        UserLsn,
        NWilson::TTraceId());
    request->SetReplyCallback(
        [this](TBaseWriteRequestExecutor::TResponse response)
        {
            AllCompletedWrites =
                AllCompletedWrites.Include(response.CompletedWrites);
            CallbackResult = std::move(response);
        });
    request->SetNotifyBelatedCallback(
        [this](THostMask completedWrites, ui64 lsn)
        {
            Y_UNUSED(lsn);
            AllCompletedWrites = AllCompletedWrites.Include(completedWrites);
        });
    return request;
}

// static
TDBGWriteBlocksToManyPBuffersResponse
TWriteWithPbTestFixture::CreateOkResponse()
{
    TDBGWriteBlocksToManyPBuffersResponse okResponse;
    okResponse.OverallError = MakeError(S_OK);
    okResponse.Responses.push_back(
        {.HostIndex = THostIndex{0}, .Error = MakeError(S_OK)});
    okResponse.Responses.push_back(
        {.HostIndex = THostIndex{1}, .Error = MakeError(S_OK)});
    okResponse.Responses.push_back(
        {.HostIndex = THostIndex{2}, .Error = MakeError(S_OK)});

    return okResponse;
}

// static
TDBGWriteBlocksToManyPBuffersResponse
TWriteWithPbTestFixture::CreateOneOkResponse(THostIndex hostIndex)
{
    TDBGWriteBlocksToManyPBuffersResponse partiallyOkResponse;
    partiallyOkResponse.OverallError = MakeError(S_OK);
    partiallyOkResponse.Responses.push_back(
        {.HostIndex = hostIndex, .Error = MakeError(S_OK)});

    return partiallyOkResponse;
}

// static
TDBGWriteBlocksToManyPBuffersResponse
TWriteWithPbTestFixture::CreateDBGErrorResponse()
{
    TDBGWriteBlocksToManyPBuffersResponse dbgErrorResponse;
    dbgErrorResponse.OverallError = MakeError(E_FAIL);

    return dbgErrorResponse;
}

void TWriteWithPbTestFixture::RunScheduledHedge()
{
    UNIT_ASSERT_VALUES_EQUAL(2, Scheduled.size());
    UNIT_ASSERT_VALUES_EQUAL(Timeout, Scheduled[0].first);
    UNIT_ASSERT_VALUES_EQUAL(HedgeDelay, Scheduled[1].first);

    Scheduled[1].second();
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

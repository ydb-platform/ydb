#include "write_request_test_fixture.h"

#include "write_with_direct_replication_request.h"
#include "write_with_pb_replication_request.h"

using namespace NThreading;

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

void TWriteClientMock::OnWriteBlocksResponse(
    std::shared_ptr<TWriteRequestBundle> bundle,
    const TWriteRequestResponse& response)
{
    Y_UNUSED(bundle);
    AllCompletedWrites = AllCompletedWrites.Include(response.CompletedWrites);
    Response = response;
}

void TWriteClientMock::OnBelatedWriteBlocksResponse(
    std::shared_ptr<TWriteRequestBundle> bundle,
    THostMask hosts)
{
    Y_UNUSED(bundle);
    AllCompletedWrites = AllCompletedWrites.Include(hosts);
}

////////////////////////////////////////////////////////////////////////////////

void TWriteRequestTestFixture::Init()
{
    TBaseFixture::Init();

    DirectBlockGroup->Oracle.WriteHedgingDelay = HedgeDelay;
    DirectBlockGroup->Oracle.WriteRequestTimeout = Timeout;

    ExpectedRange = Range;
    RangeData = GenerateRandomString(BlockSize * Range.Size());

    DirectBlockGroup->WriteBlocksToPBufferHandler = [this]   //
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

    DirectBlockGroup->WriteBlocksToManyPBuffersHandler =
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

    DirectBlockGroup->ScheduleHandler = [&](TDuration delay, TCallback callback)
    {
        Scheduled.emplace_back(delay, std::move(callback));
    };
}

// static
TDBGWriteBlocksResponse TWriteRequestTestFixture::CreateOkDirectResponse()
{
    return {.Error = MakeError(S_OK)};
}

// static
TDBGWriteBlocksResponse TWriteRequestTestFixture::CreateFailDirectResponse()
{
    return {.Error = MakeError(E_FAIL, "direct write failed")};
}

// static
TDBGWriteBlocksToManyPBuffersResponse
TWriteRequestTestFixture::CreateOkResponse()
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
TWriteRequestTestFixture::CreateOneOkResponse(THostIndex hostIndex)
{
    TDBGWriteBlocksToManyPBuffersResponse partiallyOkResponse;
    partiallyOkResponse.OverallError = MakeError(S_OK);
    partiallyOkResponse.Responses.push_back(
        {.HostIndex = hostIndex, .Error = MakeError(S_OK)});

    return partiallyOkResponse;
}

// static
TDBGWriteBlocksToManyPBuffersResponse
TWriteRequestTestFixture::CreateDBGErrorResponse()
{
    TDBGWriteBlocksToManyPBuffersResponse dbgErrorResponse;
    dbgErrorResponse.OverallError = MakeError(E_FAIL);

    return dbgErrorResponse;
}

TDirectBlockGroupMock::TWriteBlocksToManyPBuffersHandler
TWriteRequestTestFixture::GetManyPBuffersHandlerWithImmediateOkResponse()
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

std::shared_ptr<TBaseWriteRequestExecutor>
TWriteRequestTestFixture::CreatePBufferReplicationExecutor(
    TRequestHeaders headers)
{
    auto originalRequest =
        std::make_shared<TWriteBlocksLocalRequest>(std::move(headers));
    originalRequest->Sglist = MakeSgList();

    auto bundle = std::make_shared<TWriteRequestBundle>(
        Runtime->GetActorSystem(0),
        WriteClient,
        std::move(originalRequest),
        NWilson::TTraceId(),
        MakeIntrusive<TCallContext>(),
        Range,
        UserLsn);

    WriteClient->Response.reset();

    auto request = std::make_shared<TWriteWithPbReplicationRequestExecutor>(
        Runtime->GetActorSystem(0),
        LogTitle.GetChild(GetCycleCount()),
        VChunkConfig,
        DirectBlockGroup,
        std::move(bundle));

    return request;
}

std::shared_ptr<TBaseWriteRequestExecutor>
TWriteRequestTestFixture::CreateDirectReplicationExecutor(
    TRequestHeaders headers)
{
    auto originalRequest =
        std::make_shared<TWriteBlocksLocalRequest>(std::move(headers));
    originalRequest->Sglist = MakeSgList();

    auto bundle = std::make_shared<TWriteRequestBundle>(
        Runtime->GetActorSystem(0),
        WriteClient,
        std::move(originalRequest),
        NWilson::TTraceId(),
        MakeIntrusive<TCallContext>(),
        Range,
        UserLsn);

    WriteClient->Response.reset();

    auto request = std::make_shared<TWriteWithDirectReplicationRequestExecutor>(
        Runtime->GetActorSystem(0),
        LogTitle.GetChild(GetCycleCount()),
        VChunkConfig,
        DirectBlockGroup,
        std::move(bundle));

    return request;
}

void TWriteRequestTestFixture::RunScheduledHedge()
{
    UNIT_ASSERT_VALUES_EQUAL(2, Scheduled.size());
    UNIT_ASSERT_VALUES_EQUAL(Timeout, Scheduled[0].first);
    UNIT_ASSERT_VALUES_EQUAL(HedgeDelay, Scheduled[1].first);

    Scheduled[1].second();
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

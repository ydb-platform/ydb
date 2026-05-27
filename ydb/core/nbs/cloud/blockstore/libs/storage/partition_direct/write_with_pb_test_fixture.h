#pragma once

#include "base_test_fixture.h"
#include "write_request.h"
#include "write_with_pb_replication_request.h"

#include <optional>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

struct TWriteWithPbTestFixture: public TBaseFixture
{
    static TDBGWriteBlocksResponse CreateOkDirectResponse();
    static TDBGWriteBlocksResponse CreateFailDirectResponse();

    TWriteWithPbTestFixture();

    const ui64 UserLsn;
    const TBlockRange64 Range;
    const TDuration HedgeDelay;
    const TDuration Timeout;
    const TDuration PBufferReplyTimeout;

    TVector<NThreading::TPromise<TDBGWriteBlocksResponse>> DirectWritePromises;

    // Stored callback from the last WriteBlocksToManyPBuffers call.
    // Tests can invoke it to simulate a response (possibly multiple times).
    IDirectBlockGroup::TWriteBlocksToManyPBuffersCallback ManyPBufferCallback;
    TVector<std::pair<TDuration, TCallback>> Scheduled;

    std::optional<TBaseWriteRequestExecutor::TResponse> CallbackResult;
    THostMask AllCompletedWrites;

    TDirectBlockGroupMock::TWriteBlocksToManyPBuffersHandler
    GetManyPBuffersHandlerWithImmediateOkResponse();

    TDirectBlockGroupMock::TWriteBlocksToManyPBuffersHandler
    GetManyPBuffersHandlerHanging();

    TDirectBlockGroupMock::TWriteBlocksToPBufferHandler
    GetDirectWriteHandlerHanging();

    std::shared_ptr<TWriteWithPbReplicationRequestExecutor> CreateRequest(
        TRequestHeaders headers);
    static TDBGWriteBlocksToManyPBuffersResponse CreateOkResponse();
    static TDBGWriteBlocksToManyPBuffersResponse CreateOneOkResponse(
        THostIndex hostIndex);
    static TDBGWriteBlocksToManyPBuffersResponse CreateDBGErrorResponse();

    void RunScheduledHedge();
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

#pragma once

#include "base_test_fixture.h"
#include "write_with_pb_replication_request.h"

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

    NThreading::TPromise<TDBGWriteBlocksToManyPBuffersResponse>
        ManyPBufferPromise;
    TVector<std::pair<TDuration, TCallback>> Scheduled;

    TDirectBlockGroupMock::TWriteBlocksToManyPBuffersHandler
    GetManyPBuffersHandlerWithImmediateOkResponse();

    TDirectBlockGroupMock::TWriteBlocksToManyPBuffersHandler
    GetManyPBuffersHandlerHanging();

    TDirectBlockGroupMock::TWriteBlocksToPBufferHandler
    GetDirectWriteHandlerHanging();

    std::shared_ptr<TWriteWithPbReplicationRequestExecutor> CreateRequest(
        TRequestHeaders headers);
    TDBGWriteBlocksToManyPBuffersResponse CreateOkResponse();
    TDBGWriteBlocksToManyPBuffersResponse CreateOneOkResponse(
        THostIndex hostIndex);

    void RunScheduledHedge();
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

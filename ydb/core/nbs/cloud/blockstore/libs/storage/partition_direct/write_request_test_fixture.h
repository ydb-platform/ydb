#pragma once

#include "base_test_fixture.h"
#include "write_request.h"

#include <optional>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////
struct TWriteClientMock: IWriteClient
{
    THostMask AllCompletedWrites;
    std::optional<TWriteRequestResponse> Response;

    void OnWriteBlocksResponse(
        std::shared_ptr<TWriteRequestBundle> bundle,
        const TWriteRequestResponse& response) override;

    void OnBelatedWriteBlocksResponse(
        std::shared_ptr<TWriteRequestBundle> bundle,
        THostMask hosts) override;
};

////////////////////////////////////////////////////////////////////////////////
struct TWriteRequestTestFixture: public TBaseFixture
{
    ui64 UserLsn = 123;
    TBlockRange64 Range = TBlockRange64::WithLength(10, 10);
    TDuration HedgeDelay = TDuration::MilliSeconds(1000);
    TDuration Timeout = TDuration::MilliSeconds(1000);
    TDuration PBufferReplyTimeout = TDuration::MilliSeconds(500);

    TVector<NThreading::TPromise<TDBGWriteBlocksResponse>> DirectWritePromises;

    // Stored callback from the last WriteBlocksToManyPBuffers call.
    // Tests can invoke it to simulate a response (possibly multiple times).
    IDirectBlockGroup::TWriteBlocksToManyPBuffersCallback ManyPBufferCallback;
    TVector<std::pair<TDuration, TCallback>> Scheduled;

    std::shared_ptr<TWriteClientMock> WriteClient =
        std::make_shared<TWriteClientMock>();

    void Init() override;

    static TDBGWriteBlocksResponse CreateOkDirectResponse();
    static TDBGWriteBlocksResponse CreateFailDirectResponse();
    static TDBGWriteBlocksToManyPBuffersResponse CreateOkResponse();
    static TDBGWriteBlocksToManyPBuffersResponse CreateOneOkResponse(
        THostIndex hostIndex);
    static TDBGWriteBlocksToManyPBuffersResponse CreateDBGErrorResponse();

    TDirectBlockGroupMock::TWriteBlocksToManyPBuffersHandler
    GetManyPBuffersHandlerWithImmediateOkResponse();

    std::shared_ptr<TBaseWriteRequestExecutor> CreatePBufferReplicationExecutor(
        TRequestHeaders headers);

    std::shared_ptr<TBaseWriteRequestExecutor> CreateDirectReplicationExecutor(
        TRequestHeaders headers);

    void RunScheduledHedge();
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

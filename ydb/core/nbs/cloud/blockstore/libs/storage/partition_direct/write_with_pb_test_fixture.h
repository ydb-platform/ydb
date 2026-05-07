#pragma once

#include "base_test_fixture.h"
#include "write_with_pb_replication_request.h"

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

struct TWriteWithPbTestFixture : public TBaseFixture {
    TWriteWithPbTestFixture();

    const ui64 UserLsn;
    const TBlockRange64 Range;
    const TDuration HedgeDelay;
    const TDuration Timeout;
    const TDuration PBufferReplyTimeout;

    NThreading::TPromise<TDBGWriteBlocksToManyPBuffersResponse> ManyPBufferPromise;
    TVector<std::pair<TDuration, TCallback>> Scheduled;

    TDirectBlockGroupMock::TWriteBlocksToManyPBuffersHandler GetManyPBuffersHandlerWithImmediateOkResponse();
    std::shared_ptr<TWriteWithPbReplicationRequestExecutor> CreateRequest(TRequestHeaders headers);
};

} // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

#pragma once

#include "vchunk_config.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/direct_block_group.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/dirty_map.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/read_request_single_location.h>

#include <ydb/library/actors/core/actorsystem.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// Class works with a multiple readHints.
// It incapsulates logic of splitting original request into N subrequests,
// sending them to different sources and collecting responses.
class TReadMultipleLocationRequestExecutor
    : public std::enable_shared_from_this<TReadMultipleLocationRequestExecutor>
{
public:
    using TResponse = TReadSingleLocationRequestExecutor::TResponse;

    TReadMultipleLocationRequestExecutor(
        NActors::TActorSystem const* actorSystem,
        const TVChunkConfig& vChunkConfig,
        IDirectBlockGroupPtr directBlockGroup,
        TReadHint readHint,
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request,
        NWilson::TTraceId traceId);

    ~TReadMultipleLocationRequestExecutor();

    void Run();

    NThreading::TFuture<TResponse> GetFuture() const;

private:
    void OnSubRequestComplete(const TResponse& response, size_t index);

    NActors::TActorSystem const* ActorSystem;
    const TVChunkConfig VChunkConfig;
    const IDirectBlockGroupPtr DirectBlockGroup;
    const TCallContextPtr CallContext;
    const std::shared_ptr<TReadBlocksLocalRequest> Request;
    const NWilson::TTraceId TraceId;

    TVector<TReadSingleLocationRequestExecutorPtr> SubRequestExecutors;
    size_t CompletedCount{0};
    NThreading::TPromise<TResponse> Promise =
        NThreading::NewPromise<TResponse>();
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

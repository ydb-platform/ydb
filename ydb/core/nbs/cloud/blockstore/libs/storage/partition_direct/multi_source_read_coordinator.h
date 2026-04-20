#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/direct_block_group.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/read_request.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TMultiSourceReadCoordinator
    : public std::enable_shared_from_this<TMultiSourceReadCoordinator>
{
public:
    TMultiSourceReadCoordinator(
        NActors::TActorSystem* actorSystem,
        const TVChunkConfig& vChunkConfig,
        IDirectBlockGroupPtr directBlockGroup,
        TReadHint readHint,
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request,
        NWilson::TTraceId traceId);

    void Run();
    NThreading::TFuture<TReadRequestExecutor::TResponse> GetFuture();

private:
    struct TSubRequest
    {
        std::shared_ptr<TReadRequestExecutor> Executor;
        // TReadRangeHint Hint;
        size_t SglistOffset;   // Смещение в байтах
    };

    void OnSubRequestComplete(size_t index);

    NActors::TActorSystem* const ActorSystem;
    const TVChunkConfig VChunkConfig;
    const IDirectBlockGroupPtr DirectBlockGroup;
    const TReadHint ReadHint;
    const TCallContextPtr CallContext;
    const std::shared_ptr<TReadBlocksLocalRequest> Request;
    const NWilson::TTraceId TraceId;

    TVector<TSubRequest> SubRequests;
    std::atomic<size_t> CompletedCount{0};
    NThreading::TPromise<TReadRequestExecutor::TResponse> Promise;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

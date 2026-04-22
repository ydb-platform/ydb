#pragma once

#include "vchunk_config.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/direct_block_group.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/dirty_map.h>

#include <ydb/library/actors/core/actorsystem.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TReadSingleLocationRequestExecutor;

class TReadRequestExecutor
    : public std::enable_shared_from_this<TReadRequestExecutor>
{
public:
    struct TResponse
    {
        NProto::TError Error;
    };

    TReadRequestExecutor(
        NActors::TActorSystem* actorSystem,
        const TVChunkConfig& vChunkConfig,
        IDirectBlockGroupPtr directBlockGroup,
        TReadHint readHint,
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request,
        NWilson::TTraceId traceId);

    void Run();
    NThreading::TFuture<TResponse> GetFuture();

private:
    struct TSubRequest
    {
        std::shared_ptr<TReadSingleLocationRequestExecutor> Executor;
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
    NThreading::TPromise<TResponse> Promise;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

#pragma once

#include "vchunk_config.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/direct_block_group.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/dirty_map.h>

#include <ydb/library/actors/core/actorsystem.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class IReadRequestExecutor
{
public:
    struct TResponse
    {
        NProto::TError Error;
    };

    virtual ~IReadRequestExecutor() = default;

    virtual void Run() = 0;
    [[nodiscard]] virtual NThreading::TFuture<TResponse> GetFuture() const = 0;
};

using IReadRequestExecutorPtr = std::shared_ptr<IReadRequestExecutor>;

// Fabric method for creation appropriate executor based on readHints size
IReadRequestExecutorPtr CreateReadRequestExecutor(
    NActors::TActorSystem const* actorSystem,
    const TVChunkConfig& vChunkConfig,
    IDirectBlockGroupPtr directBlockGroup,
    TReadHint readHint,
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request,
    NWilson::TTraceId traceId);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

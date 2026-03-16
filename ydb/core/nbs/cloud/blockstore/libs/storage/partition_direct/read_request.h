#pragma once

#include "vchunk_config.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/direct_block_group.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/dirty_map.h>

#include <ydb/library/actors/core/actorsystem.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

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
        TVector<TReadHint> hints,
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request,
        NWilson::TTraceId traceId);

    ~TReadRequestExecutor();

    void Run();

    NThreading::TFuture<TResponse> GetFuture() const;

private:
    void OnReadResponse(const TDBGReadBlocksResponse& response);
    void Reply(NProto::TError error);

    NActors::TActorSystem const* ActorSystem;
    const TVChunkConfig VChunkConfig;
    const IDirectBlockGroupPtr DirectBlockGroup;
    const TVector<TReadHint> Hints;
    const TCallContextPtr CallContext;
    const std::shared_ptr<TReadBlocksLocalRequest> Request;
    const NWilson::TTraceId TraceId;

    size_t TryNumber = 0;

    NThreading::TPromise<TResponse> Promise =
        NThreading::NewPromise<TResponse>();
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

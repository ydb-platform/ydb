#pragma once

#include "direct_block_group.h"
#include "vchunk_config.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/dirty_map.h>

#include <ydb/library/actors/core/actorsystem.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TWriteRequestExecutor
    : public std::enable_shared_from_this<TWriteRequestExecutor>
{
public:
    struct TResponse
    {
        NProto::TError Error;
        ui64 Lsn = 0;
        // The PBuffers mask where the attempt was made to write the data.
        TLocationMask RequestedWrites;
        // The PBuffers mask where exactly the data was written and confirmed.
        TLocationMask CompletedWrites;
    };

    TWriteRequestExecutor(
        NActors::TActorSystem* actorSystem,
        const TVChunkConfig& vChunkConfig,
        IDirectBlockGroupPtr directBlockGroup,
        TCallContextPtr callContext,
        std::shared_ptr<TWriteBlocksLocalRequest> request,
        NWilson::TTraceId traceId);

    ~TWriteRequestExecutor();

    void Run();

    NThreading::TFuture<TResponse> GetFuture() const;

private:
    void SendWriteRequest(ELocation location);
    void OnWriteResponse(
        ELocation location,
        const TDBGWriteBlocksResponse& response);
    void Reply(NProto::TError error);

    NActors::TActorSystem const* ActorSystem;
    const TVChunkConfig VChunkConfig;
    const IDirectBlockGroupPtr DirectBlockGroup;
    const TCallContextPtr CallContext;
    const std::shared_ptr<TWriteBlocksLocalRequest> Request;
    const NWilson::TTraceId TraceId;
    const ui64 Lsn;

    NThreading::TPromise<TResponse> Promise =
        NThreading::NewPromise<TResponse>();
    TLocationMask RequestedWrites;
    TLocationMask CompletedWrites;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

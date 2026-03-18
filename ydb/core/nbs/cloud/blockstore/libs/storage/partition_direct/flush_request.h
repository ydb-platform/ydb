#pragma once

#include "direct_block_group.h"
#include "vchunk_config.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/dirty_map.h>

#include <ydb/library/actors/core/actorsystem.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TFlushRequestExecutor
    : public std::enable_shared_from_this<TFlushRequestExecutor>
{
public:
    struct TResponse
    {
        ELocation Location;
        TVector<ui64> FlushOk;
        TVector<ui64> FlushFailed;
    };

    TFlushRequestExecutor(
        NActors::TActorSystem* actorSystem,
        const TVChunkConfig& vChunkConfig,
        IDirectBlockGroupPtr directBlockGroup,
        ELocation location,
        TFlushHint hint,
        NWilson::TTraceId traceId);

    ~TFlushRequestExecutor();

    void Run();

    NThreading::TFuture<TResponse> GetFuture() const;

private:
    void SendFlushRequest(ELocation location);
    void OnFlushResponse(const TDBGFlushResponse& response);
    void Reply(TVector<ui64> flushOk, TVector<ui64> flushFailed);

    NActors::TActorSystem const* ActorSystem;
    const TVChunkConfig VChunkConfig;
    const IDirectBlockGroupPtr DirectBlockGroup;
    const NWilson::TTraceId TraceId;
    const ELocation Location;
    const TFlushHint Hint;

    NThreading::TPromise<TResponse> Promise =
        NThreading::NewPromise<TResponse>();
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

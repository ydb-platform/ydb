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
        THostRoute Route;
        TVector<ui64> FlushOk;
        TVector<ui64> FlushFailed;
    };

    TFlushRequestExecutor(
        NActors::TActorSystem* actorSystem,
        const TVChunkConfig& vChunkConfig,
        IDirectBlockGroupPtr directBlockGroup,
        THostRoute route,
        TFlushHint hint,
        NWilson::TSpan span);

    ~TFlushRequestExecutor();

    void Run();

    NThreading::TFuture<TResponse> GetFuture() const;

private:
    void SendFlushRequest(THostIndex host);
    void OnFlushResponse(const TDBGFlushResponse& response);
    void Reply(TVector<ui64> flushOk, TVector<ui64> flushFailed);

    NActors::TActorSystem const* ActorSystem;
    const TVChunkConfig VChunkConfig;
    const IDirectBlockGroupPtr DirectBlockGroup;
    const NWilson::TSpan Span;
    const THostRoute Route;
    const TFlushHint Hint;

    NThreading::TPromise<TResponse> Promise =
        NThreading::NewPromise<TResponse>();
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

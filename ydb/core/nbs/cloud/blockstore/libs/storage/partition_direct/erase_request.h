#pragma once

#include "direct_block_group.h"
#include "vchunk_config.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/dirty_map.h>

#include <ydb/library/actors/core/actorsystem.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TEraseRequestExecutor
    : public std::enable_shared_from_this<TEraseRequestExecutor>
{
public:
    struct TResponse
    {
        THostIndex Host = InvalidHostIndex;
        TVector<ui64> EraseOk;
        TVector<ui64> EraseFailed;
    };

    TEraseRequestExecutor(
        NActors::TActorSystem* actorSystem,
        const TVChunkConfig& vChunkConfig,
        IDirectBlockGroupPtr directBlockGroup,
        THostIndex host,
        TEraseHint hint,
        NWilson::TSpan span);

    ~TEraseRequestExecutor();

    void Run();

    NThreading::TFuture<TResponse> GetFuture() const;

private:
    void SendEraseRequest(THostIndex host);
    void OnEraseResponse(const TDBGEraseResponse& response);
    void Reply(TVector<ui64> eraseOk, TVector<ui64> eraseFailed);

    NActors::TActorSystem const* ActorSystem;
    const TVChunkConfig VChunkConfig;
    const IDirectBlockGroupPtr DirectBlockGroup;
    const NWilson::TSpan Span;
    const THostIndex Host;
    const TEraseHint Hint;

    NThreading::TPromise<TResponse> Promise =
        NThreading::NewPromise<TResponse>();
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

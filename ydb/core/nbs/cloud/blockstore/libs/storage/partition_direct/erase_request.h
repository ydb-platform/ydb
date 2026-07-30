#pragma once

#include "direct_block_group.h"
#include "request_executor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/model/log_title.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/dirty_map.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>

#include <ydb/library/actors/core/actorsystem.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TEraseRequestExecutor
    : public IRequestExecutor
    , public std::enable_shared_from_this<TEraseRequestExecutor>
{
public:
    struct TResponse
    {
        THostIndex Host = InvalidHostIndex;
        TVector<TRecordId> EraseOk;
        TVector<TRecordId> EraseFailed;
    };

    TEraseRequestExecutor(
        NActors::TActorSystem* actorSystem,
        const TLogTitle& logTitle,
        const TVChunkConfig& vChunkConfig,
        IDirectBlockGroupPtr directBlockGroup,
        THostIndex host,
        TEraseHint hint,
        NWilson::TSpan span);

    ~TEraseRequestExecutor() override;

    // Implementation of IRequestExecutor
    void Run() override;
    TString Print() override;

    NThreading::TFuture<TResponse> GetFuture() const;

private:
    void SendEraseRequest(THostIndex host);
    void OnEraseResponse(const TDBGEraseResponse& response);
    void Reply(TVector<TRecordId> eraseOk, TVector<TRecordId> eraseFailed);

    void ScheduleRequestTimeout();
    void OnRequestTimeout();

    NActors::TActorSystem const* ActorSystem;
    const TChildLogTitle LogTitle;
    const TVChunkConfig VChunkConfig;
    const IDirectBlockGroupPtr DirectBlockGroup;
    const NWilson::TSpan Span;
    const THostIndex Host;
    const TEraseHint Hint;
    const TDuration RequestTimeout;

    NThreading::TPromise<TResponse> Promise =
        NThreading::NewPromise<TResponse>();
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

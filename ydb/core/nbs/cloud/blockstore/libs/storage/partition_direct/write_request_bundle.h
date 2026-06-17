#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/trace_helpers.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_mask.h>

#include <ydb/library/wilson_ids/wilson.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct TWriteRequestResponse
{
    NProto::TError Error;
    ui64 Lsn = 0;
    // The PBuffer hosts where the attempt was made to write the data.
    THostMask RequestedWrites;
    // The PBuffer hosts where exactly the data was written and confirmed.
    THostMask CompletedWrites;
};

// The class is designed to store the state during the execution of a write
// blocks request. All the layers through which the request passes exchange data
// through this object.
class TWriteRequestBundle
    : TDisableCopyMove
    , public std::enable_shared_from_this<TWriteRequestBundle>
{
public:
    TWriteRequestBundle(
        NActors::TActorSystem* const actorSystem,
        IWriteClientWeakPtr writeClient,
        std::shared_ptr<TWriteBlocksLocalRequest> request,
        const NWilson::TTraceId& traceId,
        TCallContextPtr callContext,
        TBlockRange64 vchunkRange);

    // Respond via WriteClient to VChunk.
    void Reply(
        NProto::TError error,
        THostMask requestedWrites,
        THostMask completedWrites);
    // Notify VChunk about belated writes.
    void NotifyBelated(THostMask hosts);

    // Respond via Promise to FastPathService.
    void SendFinalReply(TWriteBlocksLocalResponse response);

    NThreading::TFuture<TWriteBlocksLocalResponse> GetFuture();
    NWilson::TSpan& GetSpan();
    TBlockRange64 GetVChunkRange() const;
    void SetLsn(ui64 lsn);
    ui64 GetLsn() const;
    TGuardedSgList& GetSgList();

private:
    IWriteClientWeakPtr WriteClient;
    std::shared_ptr<TWriteBlocksLocalRequest> Request;
    NWilson::TSpan Span;
    TCallContextPtr CallContext;
    TBlockRange64 VChunkRange;
    ui64 Lsn = 0;

    NThreading::TPromise<TWriteBlocksLocalResponse> Promise;
};

struct IWriteClient
{
    virtual ~IWriteClient() = default;

    virtual void OnWriteBlocksResponse(
        std::shared_ptr<TWriteRequestBundle> bundle,
        const TWriteRequestResponse& response) = 0;

    virtual void OnBelatedWriteBlocksResponse(
        std::shared_ptr<TWriteRequestBundle> bundle,
        THostMask hosts) = 0;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

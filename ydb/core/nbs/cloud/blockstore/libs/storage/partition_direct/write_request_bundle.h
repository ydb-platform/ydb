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

struct TWriteRequestBundle
    : TDisableCopyMove
    , public std::enable_shared_from_this<TWriteRequestBundle>
{
    IWriteClientWeakPtr WriteClient;
    std::shared_ptr<TWriteBlocksLocalRequest> Request;
    NWilson::TSpan Span;
    TCallContextPtr CallContext;
    TTracedPromise2<TWriteBlocksLocalResponse> Promise;
    TBlockRange64 VChunkRange;
    ui64 Lsn = 0;

    TWriteRequestBundle(
        NActors::TActorSystem* const actorSystem,
        IWriteClientWeakPtr writeClient,
        std::shared_ptr<TWriteBlocksLocalRequest> request,
        const NWilson::TTraceId& traceId,
        TCallContextPtr callContext,
        TBlockRange64 vchunkRange,
        ui64 lsn);

    void Reply(
        NProto::TError error,
        THostMask requestedWrites,
        THostMask completedWrites);

    void NotifyBelated(THostMask hosts);
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

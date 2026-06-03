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

struct TWriteRequestBundle: TDisableCopyMove
{
    std::shared_ptr<TWriteBlocksLocalRequest> Request;
    NWilson::TSpan Span;
    TCallContextPtr CallContext;
    TTracedPromise2<TWriteBlocksLocalResponse> Promise =
        TTracedPromise2<TWriteBlocksLocalResponse>(
            &Span,
            NKikimr::TWilsonNbs::NbsBasic);
    TBlockRange64 VChunkRange;
    ui64 Lsn = 0;

    TWriteRequestBundle(
        NActors::TActorSystem* const actorSystem,
        std::shared_ptr<TWriteBlocksLocalRequest> request,
        const NWilson::TTraceId& traceId,
        TCallContextPtr callContext,
        TBlockRange64 vchunkRange,
        ui64 lsn);
};

struct IWriteClient
{
    virtual ~IWriteClient() = default;

    virtual void OnWriteBlocksResponse(
        std::shared_ptr<TWriteRequestBundle> bundle,
        const TWriteRequestResponse& response) = 0;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

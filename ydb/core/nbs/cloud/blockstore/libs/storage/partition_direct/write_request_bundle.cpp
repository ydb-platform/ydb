
#include "write_request_bundle.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TWriteRequestBundle::TWriteRequestBundle(
    NActors::TActorSystem* const actorSystem,
    std::shared_ptr<TWriteBlocksLocalRequest> request,
    const NWilson::TTraceId& traceId,
    TCallContextPtr callContext,
    TBlockRange64 vchunkRange,
    ui64 lsn)
    : Request(std::move(request))
    , Span(NWilson::TSpan(
          NKikimr::TWilsonNbs::NbsBasic,
          traceId.Clone(),
          "TVChunk.Write",
          NWilson::EFlags::AUTO_END,
          actorSystem))
    , CallContext(std::move(callContext))
    , VChunkRange(vchunkRange)
    , Lsn(lsn)
{}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

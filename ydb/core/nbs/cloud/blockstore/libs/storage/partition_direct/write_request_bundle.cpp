
#include "write_request_bundle.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TWriteRequestBundle::TWriteRequestBundle(
    NActors::TActorSystem* const actorSystem,
    IWriteClientWeakPtr writeClient,
    std::shared_ptr<TWriteBlocksLocalRequest> request,
    const NWilson::TTraceId& traceId,
    TCallContextPtr callContext,
    TBlockRange64 vchunkRange,
    ui64 lsn)
    : WriteClient(std::move(writeClient))
    , Request(std::move(request))
    , Span(
          NKikimr::TWilsonNbs::NbsBasic,
          traceId.Clone(),
          "TVChunk.Write",
          NWilson::EFlags::AUTO_END,
          actorSystem)
    , CallContext(std::move(callContext))
    , Promise(&Span, NKikimr::TWilsonNbs::NbsBasic)
    , VChunkRange(vchunkRange)
    , Lsn(lsn)
{}

void TWriteRequestBundle::Reply(
    NProto::TError error,
    THostMask requestedWrites,
    THostMask completedWrites)
{
    Request->Sglist.Close();

    if (auto client = WriteClient.lock()) {
        client->OnWriteBlocksResponse(
            shared_from_this(),
            TWriteRequestResponse{
                .Error = std::move(error),
                .Lsn = Lsn,
                .RequestedWrites = requestedWrites,
                .CompletedWrites = completedWrites});
    } else {
        Promise.SetValue(
            TWriteBlocksLocalResponse{.Error = MakeError(E_CANCELLED)});
    }
}

void TWriteRequestBundle::NotifyBelated(THostMask hosts)
{
    if (auto client = WriteClient.lock()) {
        client->OnBelatedWriteBlocksResponse(shared_from_this(), hosts);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

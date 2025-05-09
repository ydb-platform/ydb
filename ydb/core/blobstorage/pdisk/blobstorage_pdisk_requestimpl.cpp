#include "blobstorage_pdisk_requestimpl.h"
#include "blobstorage_pdisk_completion_impl.h"

namespace NKikimr {
namespace NPDisk {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TRequestBase
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void TRequestBase::AbortDelete(TRequestBase* request, TActorSystem* actorSystem) {
    request->Span.EndError("Abort");

    switch(request->GetType()) {
    case ERequestType::RequestChunkRead:
    {
        TIntrusivePtr<TChunkRead> read = std::move(static_cast<TChunkRead*>(request)->SelfPointer);
        request->Abort(actorSystem);
        break;
    }
    case ERequestType::RequestChunkReadPiece:
    {
        TIntrusivePtr<TChunkReadPiece> piece = std::move(static_cast<TChunkReadPiece*>(request)->SelfPointer);
        request->Abort(actorSystem);
        break;
    }
    case ERequestType::RequestLogWrite:
    {
        auto* log = static_cast<TLogWrite*>(request);
        while (log) {
            auto batch = log->PopFromBatch();
            log->Abort(actorSystem);
            delete log;

            log = batch;
        }
        break;
    }
    default:
        request->Abort(actorSystem);
        delete request;
        break;
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TChunkWrite
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
TChunkWrite::TChunkWrite(const NPDisk::TEvChunkWrite &ev, const TActorId &sender, TReqId reqId, NWilson::TSpan span)

    : TRequestBase(sender, reqId, ev.Owner, ev.OwnerRound, ev.PriorityClass, std::move(span))
    , ChunkIdx(ev.ChunkIdx)
    , Offset(ev.Offset)
    , PartsPtr(ev.PartsPtr)
    , Cookie(ev.Cookie)
    , DoFlush(ev.DoFlush)
    , IsSeqWrite(ev.IsSeqWrite)
{
    if (PartsPtr) {
        for (size_t i = 0; i < PartsPtr->Size(); ++i) {
            RemainingSize += (*PartsPtr)[i].second;
        }
    }
    TotalSize = RemainingSize;
    SlackSize = Max<ui32>();
}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TChunkRead
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void TChunkRead::Abort(TActorSystem* actorSystem) {
    if (FinalCompletion) {
        FinalCompletion->PartDeleted(actorSystem);
    } else {
        Y_VERIFY(!IsReplied);
        TStringStream error;
        error << "ReqId# " << ReqId << " ChunkRead is deleted because of PDisk stoppage";
        THolder<NPDisk::TEvChunkReadResult> result = MakeHolder
            <NPDisk::TEvChunkReadResult>(NKikimrProto::ERROR,
                    ChunkIdx, Offset, Cookie,
                    NKikimrBlobStorage::StatusIsValid, error.Str());
        result->Data.SetDebugInfoGenerator(std::move(DebugInfoGenerator));
        actorSystem->Send(Sender, result.Release());
        IsReplied = true;
    }
}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TChunkReadPiece
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

TChunkReadPiece::TChunkReadPiece(TIntrusivePtr<TChunkRead> &read, ui64 pieceCurrentSector, ui64 pieceSizeLimit,
        bool isTheLastPiece)
        : TRequestBase(read->Sender, read->ReqId, read->Owner, read->OwnerRound, read->PriorityClass)
        , ChunkRead(read)
        , PieceCurrentSector(pieceCurrentSector)
        , PieceSizeLimit(pieceSizeLimit)
        , IsTheLastPiece(isTheLastPiece)
{
    Y_VERIFY(ChunkRead->FinalCompletion);
    if (!IsTheLastPiece) {
        ChunkRead->FinalCompletion->AddPart();
    }
}

void TChunkReadPiece::Abort(TActorSystem* actorSystem) {
    ChunkRead->FinalCompletion->PartDeleted(actorSystem);
}

void TChunkReadPiece::OnSuccessfulDestroy(TActorSystem* actorSystem) {
    if (!IsTheLastPiece) {
        ChunkRead->FinalCompletion->PartReadComplete(actorSystem);
    }
}


} // NPDisk
} // NKikimr

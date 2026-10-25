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
    , PreformattedSectors(ev.PreformattedSectors)
    , FirstSectorNonce(ev.FirstSectorNonce)
    , BlobId(ev.BlobId)
{
    if (PartsPtr) {
        for (size_t i = 0; i < PartsPtr->Size(); ++i) {
            RemainingSize += (*PartsPtr)[i].second;
        }
    }
    TotalSize = RemainingSize;
    SlackSize = Max<ui32>();
}

void TChunkWrite::AbortPiece(TActorSystem *actorSystem) {
    AtomicSet(PieceAborted, 1);
    if (PreformattedSectors && PreformattedPieceCount) {
        // counted with the pieces in flight; the last one to finish answers with the error
        AtomicSet(PreformattedPieceFailed, 1);
        if (AtomicDecrement(PreformattedPiecesInFlight) == 0) {
            ReplyAborted(actorSystem);
        }
    } else if (AtomicDecrement(Pieces) == 0 && Completion) {
        ReplyAborted(actorSystem);
    }
}

void TChunkWrite::PieceDone(TActorSystem *actorSystem) {
    Y_VERIFY(!PreformattedSectors); // counted by their own completions
    // The last piece hands Completion to the device; until then it is here and answers if a piece was dropped.
    if (AtomicDecrement(Pieces) == 0 && AtomicGet(PieceAborted) && Completion) {
        ReplyAborted(actorSystem);
    }
}

void TChunkWrite::ReplyAborted(TActorSystem *actorSystem) {
    TCompletionChunkWrite *completion = Completion.Release();
    completion->ErrorReason = "TChunkWrite is being aborted";
    completion->Release(actorSystem); // replies CORRUPTED, as Abort does, and deletes itself
    IsReplied = true;
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

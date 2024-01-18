#include "blobstorage_pdisk_requestimpl.h"
#include "blobstorage_pdisk_completion_impl.h"

namespace NKikimr {
namespace NPDisk {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TRequestBase
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void TRequestBase::AbortDelete(TRequestBase* request, TActorSystem* actorSystem) {
    while (auto span = request->SpanStack.Pop()) {
        span.EndError("Abort");
    }
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
    default:
        request->Abort(actorSystem);
        delete request;
        break;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TChunkWrite
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

TAtomic TChunkWrite::LastIndex = 0;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TChunkRead
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

TAtomic TChunkRead::LastIndex = 0;

void TChunkRead::Abort(TActorSystem* actorSystem) {
    if (FinalCompletion) {
        FinalCompletion->PartDeleted(actorSystem);
    } else {
        Y_ABORT_UNLESS(!IsReplied);
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
        bool isTheLastPiece, NWilson::TSpan span)
        : TRequestBase(read->Sender, read->ReqId, read->Owner, read->OwnerRound, read->PriorityClass, std::move(span))
        , ChunkRead(read)
        , PieceCurrentSector(pieceCurrentSector)
        , PieceSizeLimit(pieceSizeLimit)
        , IsTheLastPiece(isTheLastPiece)
{
    Y_ABORT_UNLESS(ChunkRead->FinalCompletion);
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


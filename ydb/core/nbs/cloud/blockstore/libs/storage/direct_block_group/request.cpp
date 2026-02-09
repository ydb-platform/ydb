#include "request.h"

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TWriteRequestHandler::TWriteRequestHandler(std::shared_ptr<TWriteBlocksLocalRequest> request)
    : Request(std::move(request))
{
    Future = NThreading::NewPromise<TWriteBlocksLocalResponse>();
}

ui64 TWriteRequestHandler::GetStartIndex() const
{
    return Request->Range.Start;
}

ui64 TWriteRequestHandler::GetStartOffset() const
{
    return Request->Range.Start * BlockSize;
}

ui64 TWriteRequestHandler::GetSize() const
{
    return Request->Range.Size() * BlockSize;
}

bool TWriteRequestHandler::IsCompleted(ui64 requestId)
{
    auto processedPersistentBufferIndex = WriteMetaByRequestId.at(requestId).Index;
    if (!(AcksMask & (1 << processedPersistentBufferIndex))) {
        AcksMask |= (1 << processedPersistentBufferIndex);
        AckCount++;
    }

    if (AckCount >= RequiredAckCount) {
        Future.SetValue(TWriteBlocksLocalResponse());
        return true;
    }

    return false;
}

void TWriteRequestHandler::OnWriteRequested(
    ui64 requestId, ui8 persistentBufferIndex, ui64 lsn)
{
    WriteMetaByRequestId.emplace(requestId,
        TPersistentBufferWriteMeta(persistentBufferIndex, lsn));
}

TVector<TWriteRequestHandler::TPersistentBufferWriteMeta> TWriteRequestHandler::GetWritesMeta() const
{
    TVector<TPersistentBufferWriteMeta> result;
    for (const auto& [_, writeMeta] : WriteMetaByRequestId) {
        result.push_back(writeMeta);
    }

    return result;
}

NThreading::TFuture<TWriteBlocksLocalResponse> TWriteRequestHandler::GetFuture() const
{
    return Future.GetFuture();
}

TGuardedSgList TWriteRequestHandler::GetData()
{
    return Request->Sglist;
}

////////////////////////////////////////////////////////////////////////////////


TFlushRequestHandler::TFlushRequestHandler(ui64 startIndex, ui8 persistentBufferIndex, ui64 lsn)
    : StartIndex(startIndex)
    , PersistentBufferIndex(persistentBufferIndex)
    , Lsn(lsn)
{}

ui64 TFlushRequestHandler::GetStartIndex() const
{
    return StartIndex;
}

ui64 TFlushRequestHandler::GetStartOffset() const
{
    return StartIndex * BlockSize;
}

ui64 TFlushRequestHandler::GetSize() const
{
    return BlockSize;
}

bool TFlushRequestHandler::IsCompleted(ui64 requestId)
{
    Y_UNUSED(requestId);

    return true;
}

ui64 TFlushRequestHandler::GetLsn() const
{
    return Lsn;
}

ui8 TFlushRequestHandler::GetPersistentBufferIndex() const
{
    return PersistentBufferIndex;
}

////////////////////////////////////////////////////////////////////////////////

TEraseRequestHandler::TEraseRequestHandler(ui64 startIndex, ui8 persistentBufferIndex, ui64 lsn)
    : StartIndex(startIndex)
    , PersistentBufferIndex(persistentBufferIndex)
    , Lsn(lsn)
{}

ui64 TEraseRequestHandler::GetStartIndex() const
{
    return StartIndex;
}

ui64 TEraseRequestHandler::GetStartOffset() const
{
    return StartIndex * BlockSize;
}

ui64 TEraseRequestHandler::GetSize() const
{
    return BlockSize;
}

bool TEraseRequestHandler::IsCompleted(ui64 requestId)
{
    Y_UNUSED(requestId);

    return true;
}

ui64 TEraseRequestHandler::GetLsn() const
{
    return Lsn;
}

ui8 TEraseRequestHandler::GetPersistentBufferIndex() const
{
    return PersistentBufferIndex;
}

////////////////////////////////////////////////////////////////////////////////

TReadRequestHandler::TReadRequestHandler(std::shared_ptr<TReadBlocksLocalRequest> request)
    : Request(std::move(request))
{
    Future = NThreading::NewPromise<TReadBlocksLocalResponse>();
}

ui64 TReadRequestHandler::GetStartIndex() const
{
    return Request->Range.Start;
}

ui64 TReadRequestHandler::GetStartOffset() const
{
    return Request->Range.Start * BlockSize;
}

ui64 TReadRequestHandler::GetSize() const
{
    return Request->Range.Size() * BlockSize;
}


bool TReadRequestHandler::IsCompleted(ui64 requestId)
{
    Y_UNUSED(requestId);

    Future.SetValue(TReadBlocksLocalResponse());
    return true;
}

NThreading::TFuture<TReadBlocksLocalResponse> TReadRequestHandler::GetFuture() const
{
    return Future.GetFuture();
}

TGuardedSgList TReadRequestHandler::GetData()
{
    return Request->Sglist;
}

}// namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

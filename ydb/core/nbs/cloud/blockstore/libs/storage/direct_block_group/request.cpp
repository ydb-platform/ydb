#include "request.h"

namespace NYdb::NBS::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

IRequest::IRequest(TActorId sender, ui64 startIndex)
    : Sender(sender)
    , StartIndex(startIndex)
{}

ui64 IRequest::GetStartOffset() const
{
    return StartIndex * BlockSize;
}

////////////////////////////////////////////////////////////////////////////////

TWriteRequest::TWriteRequest(TActorId sender, ui64 startIndex, TString data)
    : IRequest(sender, startIndex)
    , Data(std::move(data))
{}

const TString& TWriteRequest::GetData() const
{
    return Data;
}

ui64 TWriteRequest::GetDataSize() const
{
    return Data.size();
}

void TWriteRequest::OnWriteRequested(ui64 requestId, ui8 persistentBufferIndex, ui64 lsn)
{
    WriteMetaByRequestId.emplace(requestId, TPersistentBufferWriteMeta(persistentBufferIndex, lsn));
}

bool TWriteRequest::IsCompleted(ui64 requestId)
{
    auto processedPersistentBufferIndex = WriteMetaByRequestId.at(requestId).Index;
    if (!(AcksMask & (1 << processedPersistentBufferIndex))) {
        AcksMask |= (1 << processedPersistentBufferIndex);
        AckCount++;
    }

    return AckCount >= RequiredAckCount;
}

TVector<TWriteRequest::TPersistentBufferWriteMeta> TWriteRequest::GetWritesMeta() const
{
    TVector<TPersistentBufferWriteMeta> result;
    for (const auto& [_, writeMeta] : WriteMetaByRequestId) {
        result.push_back(writeMeta);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

TReadRequest::TReadRequest(TActorId sender, ui64 startIndex, ui64 blocksCount)
    : IRequest(sender, startIndex)
    , BlocksCount(blocksCount)
{}

ui64 TReadRequest::GetDataSize() const
{
    return BlocksCount * BlockSize;
}

bool TReadRequest::IsCompleted(ui64 requestId)
{
    Y_UNUSED(requestId);
    return true;
}

}   // namespace NYdb::NBS::NStorage::NPartitionDirect

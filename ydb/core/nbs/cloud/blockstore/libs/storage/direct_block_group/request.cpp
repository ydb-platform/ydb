#include "request.h"

namespace NYdb::NBS::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

IRequest::IRequest(TActorId sender, ui64 startIndex)
    : Sender(sender)
    , StartIndex(startIndex)
{}

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

void TWriteRequest::OnDDiskWriteRequested(ui64 requestId, ui8 ddiskIndex)
{
    DDiskIndexByRequestId[requestId] = ddiskIndex;
}

bool TWriteRequest::IsCompleted(ui64 requestId)
{
    auto processedDDiskIndex = DDiskIndexByRequestId[requestId];
    if (!(DDisksAcksMask & (1 << processedDDiskIndex))) {
        DDisksAcksMask |= (1 << processedDDiskIndex);
        AckCount++;
    }

    return AckCount >= RequiredAckCount;
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

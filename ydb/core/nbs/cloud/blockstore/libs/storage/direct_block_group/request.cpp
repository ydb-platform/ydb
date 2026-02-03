#include "request.h"

namespace NYdb::NBS::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

IRequest::IRequest(ui64 startIndex, NWilson::TSpan span)
    : StartIndex(startIndex)
    , Span(std::move(span))
{}

ui64 IRequest::GetStartOffset() const
{
    return StartIndex * BlockSize;
}

void IRequest::ChildSpanEndOk(ui64 requestId)
{
    auto it = ChildSpanByRequestId.find(requestId);
    if (it != ChildSpanByRequestId.end()) {
        it->second.EndOk();
        ChildSpanByRequestId.erase(it);
    }
}

void IRequest::ChildSpanEndError(ui64 requestId, const TString& errorMessage)
{
    auto it = ChildSpanByRequestId.find(requestId);
    if (it != ChildSpanByRequestId.end()) {
        it->second.EndError(errorMessage);
        ChildSpanByRequestId.erase(it);
    }
}

////////////////////////////////////////////////////////////////////////////////

TWriteRequest::TWriteRequest(
    TActorId sender,
    ui64 cookie,
    ui64 startIndex,
    TString data,
    NWilson::TSpan span)
    : IRequest(startIndex, std::move(span))
    , Sender(sender)
    , Cookie(cookie)
    , Data(std::move(data))
{}

TActorId TWriteRequest::GetSender() const
{
    return Sender;
}

ui64 TWriteRequest::GetCookie() const
{
    return Cookie;
}

const TString& TWriteRequest::GetData() const
{
    return Data;
}

ui64 TWriteRequest::GetDataSize() const
{
    return Data.size();
}

void TWriteRequest::OnWriteRequested(
    ui64 requestId, ui8 persistentBufferIndex, ui64 lsn, NWilson::TSpan span)
{
    WriteMetaByRequestId.emplace(requestId,
        TPersistentBufferWriteMeta(persistentBufferIndex, lsn));
    ChildSpanByRequestId.emplace(requestId, std::move(span));
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

TFlushRequest::TFlushRequest(ui64 startIndex, bool isErase, ui8 persistentBufferIndex, ui64 lsn, NWilson::TSpan span)
    : IRequest(startIndex, std::move(span))
    , IsErase(isErase)
    , PersistentBufferIndex(persistentBufferIndex)
    , Lsn(lsn)
{}

ui64 TFlushRequest::GetDataSize() const
{
    return BlockSize;
}

bool TFlushRequest::IsCompleted(ui64 requestId)
{
    Y_UNUSED(requestId);
    return true;
}

bool TFlushRequest::GetIsErase() const
{
    return IsErase;
}

ui8 TFlushRequest::GetPersistentBufferIndex() const
{
    return PersistentBufferIndex;
}

ui64 TFlushRequest::GetLsn() const
{
    return Lsn;
}

void TFlushRequest::OnFlushRequested(ui64 requestId, NWilson::TSpan span)
{
    ChildSpanByRequestId.emplace(requestId, std::move(span));
}

////////////////////////////////////////////////////////////////////////////////

TReadRequest::TReadRequest(TActorId sender, ui64 cookie, ui64 startIndex, ui64 blocksCount, NWilson::TSpan span)
    : IRequest(startIndex, std::move(span))
    , Sender(sender)
    , Cookie(cookie)
    , BlocksCount(blocksCount)
{}

TActorId TReadRequest::GetSender() const
{
    return Sender;
}

ui64 TReadRequest::GetCookie() const
{
    return Cookie;
}

ui64 TReadRequest::GetDataSize() const
{
    return BlocksCount * BlockSize;
}

bool TReadRequest::IsCompleted(ui64 requestId)
{
    Y_UNUSED(requestId);
    return true;

}

void TReadRequest::OnReadRequested(ui64 requestId, NWilson::TSpan span)
{
    ChildSpanByRequestId.emplace(requestId, std::move(span));
}

}   // namespace NYdb::NBS::NStorage::NPartitionDirect

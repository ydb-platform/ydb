#include "request.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/wilson_ids/wilson.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void IRequestHandler::ChildSpanEndOk(ui64 requestId)
{
    auto& span = ChildSpanByRequestId.at(requestId);
    span.EndOk();
    ChildSpanByRequestId.erase(requestId);
}

void IRequestHandler::ChildSpanEndError(ui64 requestId, const TString& errorMessage)
{
    auto& span = ChildSpanByRequestId.at(requestId);
    span.EndError(errorMessage);
    ChildSpanByRequestId.erase(requestId);
}

////////////////////////////////////////////////////////////////////////////////

TWriteRequestHandler::TWriteRequestHandler(
    std::shared_ptr<TWriteBlocksLocalRequest> request,
    NWilson::TTraceId traceId,
    ui64 tabletId)
    : Request(std::move(request))
{
    Future = NThreading::NewPromise<TWriteBlocksLocalResponse>();

    Span = NWilson::TSpan(TWilsonNbs::NbsTopLevel,
        std::move(traceId), "NbsPartition.WriteBlocks",
        NWilson::EFlags::NONE, NActors::TActivationContext::ActorSystem());
    Span.Attribute("tablet_id", static_cast<i64>(tabletId));
    Span.Attribute("vChunkIndex", static_cast<i64>(0));
    Span.Attribute("startIndex", static_cast<i64>(Request->Range.Start));
    Span.Attribute("size", static_cast<i64>(GetSize()));
}

NWilson::TTraceId TWriteRequestHandler::GetChildSpan(ui64 requestId, ui8 persistentBufferIndex)
{
    auto childSpan = NWilson::TSpan(NKikimr::TWilsonNbs::NbsBasic,
        std::move(Span.GetTraceId()), "NbsPartition.WriteBlocks.PBWrite",
        NWilson::EFlags::NONE, NActors::TActivationContext::ActorSystem());
    childSpan.Attribute("RequestId", static_cast<i64>(requestId));
    childSpan.Attribute("PersistentBufferIndex", static_cast<i64>(persistentBufferIndex));
    childSpan.Event("Send_TEvWritePersistentBuffer");

    ChildSpanByRequestId[requestId] = std::move(childSpan);

    return ChildSpanByRequestId[requestId].GetTraceId();
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

void TWriteRequestHandler::SetResponse()
{
    Future.SetValue(TWriteBlocksLocalResponse());
}

////////////////////////////////////////////////////////////////////////////////


TSyncRequestHandler::TSyncRequestHandler(
    ui64 startIndex,
    ui8 persistentBufferIndex,
    ui64 lsn,
    NWilson::TTraceId traceId,
    ui64 tabletId)
    : StartIndex(startIndex)
    , PersistentBufferIndex(persistentBufferIndex)
    , Lsn(lsn)
{
    Span = NWilson::TSpan(TWilsonNbs::NbsBasic,
        std::move(traceId), "NbsPartition.PBFlush.SyncRequest",
        NWilson::EFlags::NONE, NActors::TActivationContext::ActorSystem());
    Span.Attribute("tablet_id", static_cast<i64>(tabletId));
    Span.Attribute("vChunkIndex", static_cast<i64>(0));
    Span.Attribute("startIndex", static_cast<i64>(startIndex));
    Span.Attribute("persistentBufferIndex", static_cast<i64>(persistentBufferIndex));
    Span.Attribute("lsn", static_cast<i64>(lsn));
}

ui64 TSyncRequestHandler::GetStartIndex() const
{
    return StartIndex;
}

ui64 TSyncRequestHandler::GetStartOffset() const
{
    return StartIndex * BlockSize;
}

ui64 TSyncRequestHandler::GetSize() const
{
    return BlockSize;
}

bool TSyncRequestHandler::IsCompleted(ui64 requestId)
{
    Y_UNUSED(requestId);

    return true;
}

ui64 TSyncRequestHandler::GetLsn() const
{
    return Lsn;
}

ui8 TSyncRequestHandler::GetPersistentBufferIndex() const
{
    return PersistentBufferIndex;
}

////////////////////////////////////////////////////////////////////////////////

TEraseRequestHandler::TEraseRequestHandler(
    ui64 startIndex,
    ui8 persistentBufferIndex,
    ui64 lsn,
    NWilson::TTraceId traceId,
    ui64 tabletId)
    : StartIndex(startIndex)
    , PersistentBufferIndex(persistentBufferIndex)
    , Lsn(lsn)
{
    Span = NWilson::TSpan(TWilsonNbs::NbsBasic,
        std::move(traceId), "NbsPartition.PBFlush.EraseRequest",
        NWilson::EFlags::NONE, NActors::TActivationContext::ActorSystem());
    Span.Attribute("tablet_id", static_cast<i64>(tabletId));
    Span.Attribute("vChunkIndex", static_cast<i64>(0));
    Span.Attribute("startIndex", static_cast<i64>(startIndex));
    Span.Attribute("persistentBufferIndex", static_cast<i64>(persistentBufferIndex));
    Span.Attribute("lsn", static_cast<i64>(lsn));
}

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

TReadRequestHandler::TReadRequestHandler(
    std::shared_ptr<TReadBlocksLocalRequest> request,
    NWilson::TTraceId traceId,
    ui64 tabletId)
    : Request(std::move(request))
{
    Future = NThreading::NewPromise<TReadBlocksLocalResponse>();

    Span = NWilson::TSpan(NKikimr::TWilsonNbs::NbsTopLevel,
        std::move(traceId), "NbsPartition.ReadBlocks",
        NWilson::EFlags::NONE, NActors::TActivationContext::ActorSystem());
        Span.Attribute("tablet_id", static_cast<i64>(tabletId));
    Span.Attribute("vChunkIndex", static_cast<i64>(0));
    Span.Attribute("startIndex", static_cast<i64>(GetStartIndex()));
    Span.Attribute("blocksCount", static_cast<i64>(GetSize()));
}

NWilson::TTraceId TReadRequestHandler::GetChildSpan(ui64 requestId, bool isReadPersistentBuffer)
{
    auto childSpan = NWilson::TSpan(NKikimr::TWilsonNbs::NbsBasic,
        std::move(Span.GetTraceId()), "NbsPartition.ReadBlocks.Read",
        NWilson::EFlags::NONE, NActors::TActivationContext::ActorSystem());
    childSpan.Attribute("RequestId", static_cast<i64>(requestId));

    if (isReadPersistentBuffer) {
        childSpan.Event("Send_TEvReadPersistentBuffer");
    } else {
        childSpan.Event("Send_TEvRead");
    }

    ChildSpanByRequestId[requestId] = std::move(childSpan);

    return ChildSpanByRequestId[requestId].GetTraceId();
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

void TReadRequestHandler::SetResponse()
{
    Future.SetValue(TReadBlocksLocalResponse());
}

}// namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

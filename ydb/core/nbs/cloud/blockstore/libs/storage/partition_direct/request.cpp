#include "request.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/wilson_ids/wilson.h>

using namespace NKikimr;

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t BlockSize = 4096;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TBaseRequestHandler::TBaseRequestHandler(
    NActors::TActorSystem* actorSystem,
    TBlockRange64 range)
    : ActorSystem(actorSystem)
    , Range(range)
{}

NActors::TActorSystem* TBaseRequestHandler::GetActorSystem() const
{
    return ActorSystem;
}

ui64 TBaseRequestHandler::GetStartIndex() const
{
    return Range.Start;
}

ui64 TBaseRequestHandler::GetStartOffset() const
{
    return Range.Start * BlockSize;
}

ui64 TBaseRequestHandler::GetSize() const
{
    return Range.Size() * BlockSize;
}

void TBaseRequestHandler::ChildSpanEndOk(ui64 requestId)
{
    auto& span = ChildSpanByRequestId.at(requestId);
    span.EndOk();
    ChildSpanByRequestId.erase(requestId);
}

void TBaseRequestHandler::ChildSpanEndError(
    ui64 requestId,
    const TString& errorMessage)
{
    auto& span = ChildSpanByRequestId.at(requestId);
    span.EndError(errorMessage);
    ChildSpanByRequestId.erase(requestId);
}

////////////////////////////////////////////////////////////////////////////////

TWriteRequestHandler::TWriteRequestHandler(
    NActors::TActorSystem* actorSystem,
    std::shared_ptr<TWriteBlocksLocalRequest> request,
    NWilson::TTraceId traceId,
    ui64 tabletId)
    : TBaseRequestHandler(actorSystem, request->Range)
    , Request(std::move(request))
{
    Future = NThreading::NewPromise<TWriteBlocksLocalResponse>();

    Span = NWilson::TSpan(
        TWilsonNbs::NbsTopLevel,
        std::move(traceId),
        "NbsPartition.WriteBlocks",
        NWilson::EFlags::NONE,
        GetActorSystem());
    Span.Attribute("tablet_id", static_cast<i64>(tabletId));
    Span.Attribute("vChunkIndex", static_cast<i64>(0));
    Span.Attribute("startIndex", static_cast<i64>(Request->Range.Start));
    Span.Attribute("size", static_cast<i64>(GetSize()));
}

NWilson::TTraceId TWriteRequestHandler::GetChildSpan(
    ui64 requestId,
    ui8 persistentBufferIndex)
{
    auto childSpan = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        std::move(Span.GetTraceId()),
        "NbsPartition.WriteBlocks.PBWrite",
        NWilson::EFlags::NONE,
        GetActorSystem());
    childSpan.Attribute("RequestId", static_cast<i64>(requestId));
    childSpan.Attribute(
        "PersistentBufferIndex",
        static_cast<i64>(persistentBufferIndex));
    childSpan.Event("Send_TEvWritePersistentBuffer");

    ChildSpanByRequestId[requestId] = std::move(childSpan);

    return ChildSpanByRequestId[requestId].GetTraceId();
}

bool TWriteRequestHandler::IsCompleted(ui64 requestId)
{
    auto processedPersistentBufferIndex =
        WriteMetaByRequestId.at(requestId).Index;
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
    ui64 requestId,
    ui8 persistentBufferIndex,
    ui64 lsn)
{
    WriteMetaByRequestId.emplace(
        requestId,
        TPersistentBufferWriteMeta(persistentBufferIndex, lsn));
}

TVector<TWriteRequestHandler::TPersistentBufferWriteMeta>
TWriteRequestHandler::GetWritesMeta() const
{
    TVector<TPersistentBufferWriteMeta> result;
    for (const auto& [_, writeMeta]: WriteMetaByRequestId) {
        result.push_back(writeMeta);
    }

    return result;
}

NThreading::TFuture<TWriteBlocksLocalResponse>
TWriteRequestHandler::GetFuture() const
{
    return Future.GetFuture();
}

TGuardedSgList TWriteRequestHandler::GetData()
{
    return Request->Sglist;
}

void TWriteRequestHandler::SetResponse(NProto::TError error)
{
    TWriteBlocksLocalResponse response;
    response.Error = std::move(error);
    Future.SetValue(std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

TSyncRequestHandler::TSyncRequestHandler(
    NActors::TActorSystem* actorSystem,
    ui64 startIndex,
    ui8 persistentBufferIndex,
    ui64 lsn,
    NWilson::TTraceId traceId,
    ui64 tabletId)
    : TBaseRequestHandler(actorSystem, TBlockRange64::WithLength(startIndex, 1))
    , PersistentBufferIndex(persistentBufferIndex)
    , Lsn(lsn)
{
    Span = NWilson::TSpan(
        TWilsonNbs::NbsBasic,
        std::move(traceId),
        "NbsPartition.PBFlush.SyncRequest",
        NWilson::EFlags::NONE,
        GetActorSystem());
    Span.Attribute("tablet_id", static_cast<i64>(tabletId));
    Span.Attribute("vChunkIndex", static_cast<i64>(0));
    Span.Attribute("startIndex", static_cast<i64>(startIndex));
    Span.Attribute(
        "persistentBufferIndex",
        static_cast<i64>(persistentBufferIndex));
    Span.Attribute("lsn", static_cast<i64>(lsn));
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
    NActors::TActorSystem* actorSystem,
    ui64 startIndex,
    ui8 persistentBufferIndex,
    ui64 lsn,
    NWilson::TTraceId traceId,
    ui64 tabletId)
    : TBaseRequestHandler(actorSystem, TBlockRange64::WithLength(startIndex, 1))
    , PersistentBufferIndex(persistentBufferIndex)
    , Lsn(lsn)
{
    Span = NWilson::TSpan(
        TWilsonNbs::NbsBasic,
        std::move(traceId),
        "NbsPartition.PBFlush.EraseRequest",
        NWilson::EFlags::NONE,
        GetActorSystem());
    Span.Attribute("tablet_id", static_cast<i64>(tabletId));
    Span.Attribute("vChunkIndex", static_cast<i64>(0));
    Span.Attribute("startIndex", static_cast<i64>(startIndex));
    Span.Attribute(
        "persistentBufferIndex",
        static_cast<i64>(persistentBufferIndex));
    Span.Attribute("lsn", static_cast<i64>(lsn));
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
    NActors::TActorSystem* actorSystem,
    std::shared_ptr<TReadBlocksLocalRequest> request,
    NWilson::TTraceId traceId,
    ui64 tabletId)
    : TBaseRequestHandler(actorSystem, request->Range)
    , Request(std::move(request))
{
    Y_UNUSED(traceId);
    Y_UNUSED(tabletId);
    Future = NThreading::NewPromise<TReadBlocksLocalResponse>();

    Span = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsTopLevel,
        std::move(traceId),
        "NbsPartition.ReadBlocks",
        NWilson::EFlags::NONE,
        GetActorSystem());
    Span.Attribute("tablet_id", static_cast<i64>(tabletId));
    Span.Attribute("vChunkIndex", static_cast<i64>(0));
    Span.Attribute("startIndex", static_cast<i64>(GetStartIndex()));
    Span.Attribute("blocksCount", static_cast<i64>(GetSize()));
}

NWilson::TTraceId TReadRequestHandler::GetChildSpan(
    ui64 requestId,
    bool isReadPersistentBuffer)
{
    auto childSpan = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        std::move(Span.GetTraceId()),
        "NbsPartition.ReadBlocks.Read",
        NWilson::EFlags::NONE,
        GetActorSystem());
    childSpan.Attribute("RequestId", static_cast<i64>(requestId));

    if (isReadPersistentBuffer) {
        childSpan.Event("Send_TEvReadPersistentBuffer");
    } else {
        childSpan.Event("Send_TEvRead");
    }

    ChildSpanByRequestId[requestId] = std::move(childSpan);

    return ChildSpanByRequestId[requestId].GetTraceId();
}

bool TReadRequestHandler::IsCompleted(ui64 requestId)
{
    Y_UNUSED(requestId);

    return true;
}

NThreading::TFuture<TReadBlocksLocalResponse>
TReadRequestHandler::GetFuture() const
{
    return Future.GetFuture();
}

TGuardedSgList TReadRequestHandler::GetData()
{
    return Request->Sglist;
}

void TReadRequestHandler::SetResponse(NProto::TError error)
{
    TReadBlocksLocalResponse response;
    response.Error = std::move(error);
    Future.SetValue(std::move(response));
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

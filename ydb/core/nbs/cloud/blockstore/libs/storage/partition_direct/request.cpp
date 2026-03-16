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
    ui32 vChunkIndex)
    : ActorSystem(actorSystem)
    , VChunkIndex(vChunkIndex)
{}

NActors::TActorSystem* TBaseRequestHandler::GetActorSystem() const
{
    return ActorSystem;
}

ui32 TBaseRequestHandler::GetVChunkIndex() const
{
    return VChunkIndex;
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

TIORequestsHandler::TIORequestsHandler(
    NActors::TActorSystem* actorSystem,
    ui32 vChunkIndex,
    TBlockRange64 range)
    : TBaseRequestHandler(actorSystem, vChunkIndex)
    , Range(range)
{}

ui64 TIORequestsHandler::GetStartIndex() const
{
    return Range.Start;
}

ui64 TIORequestsHandler::GetStartOffset() const
{
    return Range.Start * BlockSize;
}

ui64 TIORequestsHandler::GetSize() const
{
    return Range.Size() * BlockSize;
}

////////////////////////////////////////////////////////////////////////////////

TWriteRequestHandler::TWriteRequestHandler(
    NActors::TActorSystem* actorSystem,
    ui32 vChunkIndex,
    std::shared_ptr<TWriteBlocksLocalRequest> request,
    NWilson::TTraceId traceId,
    ui64 tabletId)
    : TIORequestsHandler(actorSystem, vChunkIndex, request->Range)
    , Request(std::move(request))
{
    Span = NWilson::TSpan(
        TWilsonNbs::NbsTopLevel,
        std::move(traceId),
        "NbsPartition.WriteBlocks",
        NWilson::EFlags::NONE,
        GetActorSystem());
    Span.Attribute("tablet_id", static_cast<i64>(tabletId));
    Span.Attribute("vChunkIndex", static_cast<i64>(vChunkIndex));
    Span.Attribute("startIndex", static_cast<i64>(Request->Range.Start));
    Span.Attribute("size", static_cast<i64>(GetSize()));
}

NWilson::TSpan& TWriteRequestHandler::GetChildSpan(
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

    return ChildSpanByRequestId[requestId];
}

void TWriteRequestHandler::SetCompleted(ui64 requestId)
{
    auto processedPersistentBufferIndex =
        WriteMetaByRequestId.at(requestId).Index;
    if (!(AcksMask & (1 << processedPersistentBufferIndex))) {
        AcksMask |= (1 << processedPersistentBufferIndex);
        AckCount++;
    }
}

bool TWriteRequestHandler::IsCompleted() const
{
    return AckCount >= RequiredAckCount;
}

void TWriteRequestHandler::OnWriteRequested(
    ui64 requestId,
    ui8 persistentBufferIndex,
    ui64 lsn)
{
    auto guard = TGuard(Lock);

    WriteMetaByRequestId.emplace(
        requestId,
        TPersistentBufferWriteMeta(persistentBufferIndex, lsn));
}

void TWriteRequestHandler::OnWriteFinished(
    ui64 requestId,
    const NKikimrBlobStorage::NDDisk::TEvWritePersistentBufferResult& result)
{
    auto guard = TGuard(Lock);

    auto execSpan = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        std::move(Span.GetTraceId()),
        "NbsPartition.WriteBlocks.HandlePBWriteResult.Exec",
        NWilson::EFlags::NONE,
        GetActorSystem());

    if (result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        ChildSpanEndOk(requestId);
        SetCompleted(requestId);
        if (IsCompleted()) {
            execSpan.Event("Start SetResponse");
            SetResponse(MakeError(S_OK));
            execSpan.Event("Finish SetResponse");
            Span.EndOk();
        }
    } else {
        // TODO: add error handling
        ChildSpanEndError(
            requestId,
            "HandleWritePersistentBufferResult failed");
        Span.EndError("HandleWritePersistentBufferResult failed");

        SetResponse(MakeError(E_FAIL, result.GetErrorReason()));
    }

    execSpan.EndOk();
}

TVector<TPersistentBufferWriteMeta> TWriteRequestHandler::GetWritesMeta() const
{
    TVector<TPersistentBufferWriteMeta> result;
    for (const auto& [_, writeMeta]: WriteMetaByRequestId) {
        result.push_back(writeMeta);
    }

    return result;
}

TGuardedSgList TWriteRequestHandler::GetData()
{
    return Request->Sglist;
}

NThreading::TFuture<TDBGWriteBlocksResponse> TWriteRequestHandler::GetFuture()
{
    return Promise.GetFuture();
}

void TWriteRequestHandler::SetResponse(NProto::TError error)
{
    Promise.TrySetValue(TDBGWriteBlocksResponse{
        .Meta = GetWritesMeta(),
        .Error = std::move(error)});
}

////////////////////////////////////////////////////////////////////////////////

TSyncAndEraseRequestHandler::TSyncAndEraseRequestHandler(
    NActors::TActorSystem* actorSystem,
    ui32 vChunkIndex,
    ui8 persistentBufferIndex,
    NWilson::TTraceId traceId,
    ui64 tabletId,
    TVector<TSyncRequest> syncRequests)
    : TBaseRequestHandler(actorSystem, vChunkIndex)
    , PersistentBufferIndex(persistentBufferIndex)
    , SyncRequests(std::move(syncRequests))
{
    Span = NWilson::TSpan(
        TWilsonNbs::NbsBasic,
        std::move(traceId),
        "NbsPartition.Sync",
        NWilson::EFlags::NONE,
        GetActorSystem());
    Span.Attribute("tablet_id", static_cast<i64>(tabletId));
    Span.Attribute(
        "persistentBufferIndex",
        static_cast<i64>(persistentBufferIndex));
}

NWilson::TSpan& TSyncAndEraseRequestHandler::GetChildSpan(ui64 requestId)
{
    auto childSpan = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        std::move(Span.GetTraceId()),
        "NbsPartition.PBFlush.SyncRequest",
        NWilson::EFlags::NONE,
        GetActorSystem());

    TStringBuilder lsnsStr;
    for (ui64 lsn: GetLsns()) {
        if (!lsnsStr.empty()) {
            lsnsStr << ";";
        }
        lsnsStr << lsn;
    }
    childSpan.Attribute("Lsns", lsnsStr);

    ChildSpanByRequestId[requestId] = std::move(childSpan);

    return ChildSpanByRequestId[requestId];
}

ui8 TSyncAndEraseRequestHandler::GetPersistentBufferIndex() const
{
    return PersistentBufferIndex;
}

ui64 TSyncAndEraseRequestHandler::OnSyncRequested(ui64 startIndex, ui64 lsn)
{
    SyncRequests.emplace_back(startIndex, lsn);
    return SyncRequests.size();
}

const TVector<TSyncRequest>&
TSyncAndEraseRequestHandler::GetSyncRequests() const
{
    return SyncRequests;
}

TVector<NKikimr::NDDisk::TBlockSelector>
TSyncAndEraseRequestHandler::GetBlockSelectors() const
{
    TVector<NKikimr::NDDisk::TBlockSelector> selectors;
    for (const auto& request: SyncRequests) {
        selectors.push_back(NKikimr::NDDisk::TBlockSelector(
            GetVChunkIndex(),
            request.StartIndex * BlockSize,
            BlockSize));
    }

    return selectors;
}

TVector<ui64> TSyncAndEraseRequestHandler::GetLsns() const
{
    TVector<ui64> lsns;
    for (const auto& request: SyncRequests) {
        lsns.push_back(request.Lsn);
    }

    return lsns;
}

NThreading::TFuture<TDBGSyncBlocksResponse>
TSyncAndEraseRequestHandler::GetFuture()
{
    return Promise.GetFuture();
}

void TSyncAndEraseRequestHandler::SetResponse(NProto::TError error)
{
    Promise.TrySetValue(TDBGSyncBlocksResponse{.Error = std::move(error)});
}

////////////////////////////////////////////////////////////////////////////////
/*
TEraseRequestHandler::TEraseRequestHandler(
    NActors::TActorSystem* actorSystem,
    std::shared_ptr<TSyncRequestHandler> syncRequestHandler)
    : TBaseRequestHandler(actorSystem, syncRequestHandler->GetVChunkIndex())
    , SyncRequestHandler(std::move(syncRequestHandler))
{
    Span = NWilson::TSpan(
        TWilsonNbs::NbsBasic,
        std::move(SyncRequestHandler->Span.GetTraceId()),
        "NbsPartition.PBFlush.EraseRequest",
        NWilson::EFlags::NONE,
        GetActorSystem());
}

NWilson::TSpan& TEraseRequestHandler::GetChildSpan(ui64 requestId)
{
    auto childSpan = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        std::move(Span.GetTraceId()),
        "NbsPartition.PBFlush.EraseRequest",
        NWilson::EFlags::NONE,
        GetActorSystem());

    TStringBuilder lsnsStr;
    for (ui64 lsn: GetLsns()) {
        if (!lsnsStr.empty()) {
            lsnsStr << ";";
        }
        lsnsStr << lsn;
    }
    childSpan.Attribute("Lsns", lsnsStr);

    ChildSpanByRequestId[requestId] = std::move(childSpan);

    return ChildSpanByRequestId[requestId];
}
*/
////////////////////////////////////////////////////////////////////////////////

TReadRequestHandler::TReadRequestHandler(
    NActors::TActorSystem* actorSystem,
    ui32 vChunkIndex,
    std::shared_ptr<TReadBlocksLocalRequest> request,
    NWilson::TTraceId traceId,
    ui64 tabletId)
    : TIORequestsHandler(actorSystem, vChunkIndex, request->Range)
    , Request(std::move(request))
{
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

NWilson::TSpan& TReadRequestHandler::GetChildSpan(
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

    return ChildSpanByRequestId[requestId];
}

TGuardedSgList TReadRequestHandler::GetData()
{
    return Request->Sglist;
}

NThreading::TFuture<TDBGReadBlocksResponse> TReadRequestHandler::GetFuture()
{
    return Promise;
}

void TReadRequestHandler::SetResponse(NProto::TError error)
{
    Promise.TrySetValue(TDBGReadBlocksResponse{.Error = std::move(error)});
}

TOverallAckRequestHandler::TOverallAckRequestHandler(
    NActors::TActorSystem* actorSystem,
    NWilson::TTraceId traceId,
    TString name,
    ui64 tabletId,
    ui32 vChunkIndex,
    ui8 requiredAckCount)
    : TBaseRequestHandler(actorSystem, vChunkIndex)
    , RequiredAckCount(requiredAckCount)
    , Name(std::move(name))
    , Promise(NThreading::NewPromise<void>())
{
    Span = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsTopLevel,
        std::move(traceId),
        Name,
        NWilson::EFlags::NONE,
        GetActorSystem());
    Span.Attribute("tablet_id", static_cast<i64>(tabletId));
}

NWilson::TSpan TOverallAckRequestHandler::GetChildSpan(
    ui64 requestId,
    TString eventName)
{
    auto childSpan = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        std::move(Span.GetTraceId()),
        Name,
        NWilson::EFlags::NONE,
        GetActorSystem());
    childSpan.Attribute("RequestId", static_cast<i64>(requestId));
    childSpan.Event(eventName);

    return childSpan;
}

bool TOverallAckRequestHandler::IsCompleted() const
{
    return AckCount == RequiredAckCount;
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

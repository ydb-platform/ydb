#include "direct_block_group.h"

#include <ydb/library/wilson_ids/wilson.h>
#include <util/string/builder.h>

namespace NYdb::NBS::NStorage::NPartitionDirect {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

TDirectBlockGroup::TDirectBlockGroup(
    ui64 tabletId,
    ui32 generation,
    const TVector<TDDiskId>& ddisksIds,
    const TVector<TDDiskId>& persistentBufferDDiskIds)
    : BlocksMeta(BlocksCount, TBlockMeta(persistentBufferDDiskIds.size()))
{
    auto addDDiskConnections = [&](const TVector<TDDiskId>& ddisksIds,
                                   TVector<TDDiskConnection>& ddiskConnections,
                                   bool fromPersistentBuffer)
    {
        for (const auto& ddiskId: ddisksIds) {
            ddiskConnections.emplace_back(
                ddiskId,
                NDDisk::TQueryCredentials(
                    tabletId,
                    generation,
                    std::nullopt,
                    fromPersistentBuffer));
        }
    };

    // Now we assume that ddisksIds and persistentBufferDDiskIds have the same size
    // since we flush each persistent buffer to ddisk with the same index
    Y_ABORT_UNLESS(ddisksIds.size() == persistentBufferDDiskIds.size());

    addDDiskConnections(ddisksIds, DDiskConnections, false);
    addDDiskConnections(persistentBufferDDiskIds, PersistentBufferConnections, true);
}

void TDirectBlockGroup::EstablishConnections(const TActorContext& ctx)
{
    LOG_DEBUG(ctx, NKikimrServices::NBS_PARTITION, "EstablishConnections");

    auto sendConnectRequests = [&](const TVector<TDDiskConnection>& connections, ui64 startRequestId = 0) {
        for (size_t i = 0; i < connections.size(); i++) {
            auto request =
                std::make_unique<NDDisk::TEvConnect>(connections[i].Credentials);

            ctx.Send(
                connections[i].GetServiceId(),
                request.release(),
                0,   // flags
                startRequestId + i);
        }
    };

    sendConnectRequests(PersistentBufferConnections);
    // Send connect requests to ddisks with offset by persistent buffer connections count
    sendConnectRequests(DDiskConnections, PersistentBufferConnections.size());
}

void TDirectBlockGroup::HandleDDiskConnectResult(
    const NDDisk::TEvConnectResult::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(ctx, NKikimrServices::NBS_PARTITION,
                "HandleDDiskConnectResult record is: %s", msg->Record.DebugString().data());

    TDDiskConnection* ddiskConnection = nullptr;
    if (ev->Cookie < PersistentBufferConnections.size()) {
        ddiskConnection = &PersistentBufferConnections[ev->Cookie];
    } else {
        ddiskConnection = &DDiskConnections[ev->Cookie - PersistentBufferConnections.size()];
    }

    if (msg->Record.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        ddiskConnection->Credentials.DDiskInstanceGuid =
            msg->Record.GetDDiskInstanceGuid();
    } else {
        LOG_ERROR(
            ctx, NKikimrServices::NBS_PARTITION,
            "HandleDDiskConnectResult finished with error: %d, reason: %s",
            msg->Record.GetStatus(),
            msg->Record.GetErrorReason().data());
    }
}

void TDirectBlockGroup::HandleWriteBlocksRequest(
    const TEvService::TEvWriteBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(ctx, NKikimrServices::NBS_PARTITION,
                "HandleWriteBlocksRequest record is: %s",
                msg->Record.DebugString().data());

    const ui64 startIndex = msg->Record.GetStartIndex();
    const auto& blocks = msg->Record.GetBlocks();

    size_t totalSize = 0;
    for (const auto& buffer: blocks.GetBuffers()) {
        totalSize += buffer.size();
    }

    auto vChunkIndex = 0;

    auto span = NWilson::TSpan(NKikimr::TWilsonNbs::NbsTopLevel,
        std::move(ev->TraceId), "NbsPartition.WriteBlocks",
        NWilson::EFlags::NONE, TActivationContext::ActorSystem());
    span.Attribute("tablet_id", static_cast<i64>(TabletId));
    span.Attribute("vChunkIndex", static_cast<i64>(vChunkIndex));
    span.Attribute("startIndex", static_cast<i64>(startIndex));
    span.Attribute("size", static_cast<i64>(totalSize));

    // Round up
    // Move to separate function
    totalSize = (totalSize + 4096 - 1) / 4096 * 4096;

    TString data = TString::Uninitialized(totalSize);
    char* ptr = data.Detach();
    for (const auto& buffer: blocks.GetBuffers()) {
        memcpy(ptr, buffer.data(), buffer.size());
        ptr += buffer.size();
    }
    memset(ptr, 0, data.end() - ptr);

    // now we expect only 1 block writes
    Y_ABORT_UNLESS(totalSize == 4096);

    auto request = std::make_shared<TWriteRequest>(
        ev->Sender,
        ev->Cookie,
        startIndex,
        std::move(data),
        std::move(span));

    for (size_t i = 0; i < 3; i++) {
        ++RequestId;

        auto childSpan = NWilson::TSpan(NKikimr::TWilsonNbs::NbsBasic,
            std::move(request->Span.GetTraceId()), "NbsPartition.WriteBlocks.PBWrite",
            NWilson::EFlags::NONE, TActivationContext::ActorSystem());
        childSpan.Attribute("RequestId", static_cast<i64>(RequestId));
        childSpan.Attribute("PersistentBufferIndex", static_cast<i64>(i));

        auto requestToDDisk = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
            PersistentBufferConnections[i].Credentials,
            NDDisk::TBlockSelector(
                0, // vChunkIndex
                request->GetStartOffset(),
                request->GetDataSize()),
            RequestId, // lsn
            NDDisk::TWriteInstruction(0));

        requestToDDisk->AddPayload(TRope(request->GetData()));

        childSpan.Event("Send_TEvWritePersistentBuffer");
        ctx.Send(MakeHolder<IEventHandle>(
            PersistentBufferConnections[i].GetServiceId(),
            ctx.SelfID,
            requestToDDisk.release(),
            0, // flags
            RequestId,
            nullptr,
            std::move(childSpan.GetTraceId())));

        RequestById[RequestId] = request;
        request->OnWriteRequested(
            RequestId,
            i, // persistentBufferIndex
            RequestId, // lsn
            std::move(childSpan)
        );
    }
}

void TDirectBlockGroup::HandlePersistentBufferWriteResult(
    const NDDisk::TEvWritePersistentBufferResult::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(ctx, NKikimrServices::NBS_PARTITION,
                "HandlePersistentBufferWriteResult record is: %s",
                msg->Record.DebugString().data());

    auto requestId = ev->Cookie;

    // That means that request is already completed
    if (!RequestById.contains(requestId)) {
        LOG_ERROR(ctx, NKikimrServices::NBS_PARTITION,
                  "HandlePersistentBufferWriteResult request id %d not found", requestId);

        return;
    }

    auto& request = static_cast<TWriteRequest&>(*RequestById[requestId]);
    if (msg->Record.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        // End child span for this sub-request
        request.ChildSpanEndOk(requestId);

        if (request.IsCompleted(requestId)) {
            auto& blockMeta = BlocksMeta[request.StartIndex];
            const auto& writesMeta = request.GetWritesMeta();
            if (!blockMeta.WriteMetaIsOutdated(writesMeta)) {
                for (const auto& meta : writesMeta) {
                    blockMeta.OnWriteCompleted(meta);
                }

                RequestBlockFlush(
                    ctx,
                    request,
                    false, // isErase
                    request.Span.GetTraceId()
                );
            } else {
                RequestBlockFlush(
                    ctx,
                    request,
                    true, // isErase
                    request.Span.GetTraceId()
                );
            }

            // End top-level write span when all sub-requests are completed successfully
            request.Span.EndOk();

            auto response = std::make_unique<TEvService::TEvWriteBlocksResponse>(MakeError(S_OK));
            ctx.Send(
                request.GetSender(),
                std::move(response),
                0, // flags
                request.GetCookie());

            RequestById.erase(requestId);
        }
    } else {
        LOG_ERROR(ctx, NKikimrServices::NBS_PARTITION,
                  "HandlePersistentBufferWriteResult finished with error: %d, reason: %s",
                  msg->Record.GetStatus(),
                  msg->Record.GetErrorReason().data());

        auto response =
            std::make_unique<TEvService::TEvWriteBlocksResponse>(MakeError(E_FAIL));

        ctx.Send(
            request.GetSender(),
            std::move(response),
            0, // flags
            request.GetCookie());

        request.ChildSpanEndError(requestId, "HandlePersistentBufferWriteResult failed");
        request.Span.EndError("HandlePersistentBufferWriteResult failed");
        RequestById.erase(requestId);
    }
}

void TDirectBlockGroup::RequestBlockFlush(
    const TActorContext& ctx,
    const TWriteRequest& request,
    bool isErase,
    NWilson::TTraceId traceId)
{
    const auto& blockMeta = BlocksMeta[request.StartIndex];

    auto parentSpan = NWilson::TSpan(NKikimr::TWilsonNbs::NbsBasic,
        std::move(traceId), "NbsPartition.PBFlush",
        NWilson::EFlags::NONE, TActivationContext::ActorSystem());

    for (size_t i = 0; i < 3; i++) {
        auto span = NWilson::TSpan(NKikimr::TWilsonNbs::NbsBasic,
            std::move(parentSpan.GetTraceId()), "NbsPartition.PBFlush.FlushRequest",
            NWilson::EFlags::NONE, TActivationContext::ActorSystem());

        auto flushRequest = std::make_shared<TFlushRequest>(
            request.StartIndex,
            isErase,
            i, // persistentBufferIndex
            blockMeta.LsnByPersistentBufferIndex[i],
            std::move(span)
        );

        ++RequestId;

        auto requestToDDisk = std::make_unique<NDDisk::TEvFlushPersistentBuffer>(
            PersistentBufferConnections[i].Credentials,
            NDDisk::TBlockSelector(
                0, // vChunkIndex
                flushRequest->GetStartOffset(),
                flushRequest->GetDataSize()),
            blockMeta.LsnByPersistentBufferIndex[i],
            std::nullopt,
            std::nullopt);

        if (!isErase) {
            // Now we flush data to ddisk with same index as persistent buffer
            const auto& ddiskConnection = DDiskConnections[i];
            ddiskConnection.DDiskId.Serialize(requestToDDisk->Record.MutableDDiskId());
            requestToDDisk->Record.SetDDiskInstanceGuid(*ddiskConnection.Credentials.DDiskInstanceGuid);
        }

        ctx.Send(MakeHolder<IEventHandle>(
            PersistentBufferConnections[i].GetServiceId(),
            ctx.SelfID,
            requestToDDisk.release(),
            0, // flags
            RequestId,
            nullptr,
            std::move(flushRequest->Span.GetTraceId())));

        RequestById[RequestId] = flushRequest;
    }

    parentSpan.EndOk();
}

void TDirectBlockGroup::HandlePersistentBufferFlushResult(
    const NDDisk::TEvFlushPersistentBufferResult::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(ctx, NKikimrServices::NBS_PARTITION,
                "HandlePersistentBufferFlushResult record is: %s",
                msg->Record.DebugString().data());

    auto requestId = ev->Cookie;
    auto& request = static_cast<TFlushRequest&>(*RequestById[requestId]);
    if (msg->Record.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {

        BlocksMeta[request.StartIndex].OnFlushCompleted(
            request.GetIsErase(),
            request.GetPersistentBufferIndex(),
            request.GetLsn());

        request.Span.EndOk();
        RequestById.erase(requestId);
    } else {
        LOG_ERROR(ctx, NKikimrServices::NBS_PARTITION,
                  "HandlePersistentBufferFlushResult finished with error: %d, reason: %s",
                  msg->Record.GetStatus(),
                  msg->Record.GetErrorReason().data());

        request.Span.EndError("HandlePersistentBufferFlushResult failed");
    }
}

void TDirectBlockGroup::HandleReadBlocksRequest(
    const TEvService::TEvReadBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(ctx, NKikimrServices::NBS_PARTITION,
                "HandleReadBlocksRequest record is: %s ",
                msg->Record.DebugString().data());

    const ui64 startIndex = msg->Record.GetStartIndex();
    const ui64 blocksCount = msg->Record.GetBlocksCount();
    Y_ABORT_UNLESS(blocksCount == 1);

    auto vChunkIndex = 0;

    // Create top-level span for the read request
    auto span = NWilson::TSpan(NKikimr::TWilsonNbs::NbsTopLevel,
        std::move(ev->TraceId), "NbsPartition.ReadBlocks",
        NWilson::EFlags::NONE, TActivationContext::ActorSystem());
    span.Attribute("tablet_id", static_cast<i64>(TabletId));
    span.Attribute("vChunkIndex", static_cast<i64>(vChunkIndex));
    span.Attribute("startIndex", static_cast<i64>(startIndex));
    span.Attribute("blocksCount", static_cast<i64>(blocksCount));

    // Block is not writed
    if (!BlocksMeta[startIndex].IsWrited())
    {
        auto response = std::make_unique<TEvService::TEvReadBlocksResponse>(MakeError(S_OK));
        auto& blocks = *response->Record.MutableBlocks();
        blocks.AddBuffers(TString(4096, 0));

        span.EndOk();
        ctx.Send(
            ev->Sender,
            std::move(response),
            0, // flags
            ev->Cookie);
        return;
    }

    auto request = std::make_shared<TReadRequest>(
        ev->Sender, ev->Cookie, startIndex, blocksCount, std::move(span));
    ++RequestId;

    // Create child span for read request
    auto childSpan = NWilson::TSpan(NKikimr::TWilsonNbs::NbsBasic,
        std::move(request->Span.GetTraceId()), "NbsPartition.ReadBlocks.Read",
        NWilson::EFlags::NONE, TActivationContext::ActorSystem());
    childSpan.Attribute("RequestId", static_cast<i64>(RequestId));

    if (!BlocksMeta[startIndex].IsFlushedToDDisk()) {
        const auto& ddiskConnection = PersistentBufferConnections[0];
        LOG_DEBUG(ctx, NKikimrServices::NBS_PARTITION,
                  "read from persistent buffer %d with lsn %d and start index %d", 0, BlocksMeta[startIndex].LsnByPersistentBufferIndex[0], startIndex);

        auto requestToPersistentBuffer = std::make_unique<NDDisk::TEvReadPersistentBuffer>(
            ddiskConnection.Credentials,
            NDDisk::TBlockSelector(
                0, // vChunkIndex
                request->GetStartOffset(),
                request->GetDataSize()),
            BlocksMeta[startIndex].LsnByPersistentBufferIndex[0],
            NDDisk::TReadInstruction(1));

        childSpan.Event("Send_TEvReadPersistentBuffer");
        ctx.Send(MakeHolder<IEventHandle>(
            ddiskConnection.GetServiceId(),
            ctx.SelfID,
            requestToPersistentBuffer.release(),
            0, // flags
            RequestId,
            nullptr,
            std::move(childSpan.GetTraceId())));
    } else {
        const auto& ddiskConnection = DDiskConnections[0];

        auto requestToDDisk = std::make_unique<NDDisk::TEvRead>(
            ddiskConnection.Credentials,
            NDDisk::TBlockSelector(
                0, // vChunkIndex
                request->GetStartOffset(),
                request->GetDataSize()),
            NDDisk::TReadInstruction(1));

        childSpan.Event("Send_TEvRead");
        ctx.Send(MakeHolder<IEventHandle>(
            ddiskConnection.GetServiceId(),
            ctx.SelfID,
            requestToDDisk.release(),
            0, // flags
            RequestId,
            nullptr,
            std::move(childSpan.GetTraceId())));
    }

        RequestById[RequestId] = request;
        request->OnReadRequested(RequestId, std::move(childSpan));
}

template void TDirectBlockGroup::HandleReadResult<NDDisk::TEvReadPersistentBufferResult>(
    const NDDisk::TEvReadPersistentBufferResult::TPtr& ev,
    const TActorContext& ctx);

template void TDirectBlockGroup::HandleReadResult<NDDisk::TEvReadResult>(
    const NDDisk::TEvReadResult::TPtr& ev,
    const TActorContext& ctx);

template <typename TEvent>
void TDirectBlockGroup::HandleReadResult(
    const typename TEvent::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(ctx, NKikimrServices::NBS_PARTITION,
                "HandleReadResult record is: %s",
                msg->Record.DebugString().data());

    auto requestId = ev->Cookie;

    // That means that request is already completed
    if (!RequestById.contains(requestId)) {
        LOG_ERROR(ctx, NKikimrServices::NBS_PARTITION,
                  "HandlePersistentBufferWriteResult request id %d not found", requestId);

        return;
    }

    auto& request = static_cast<TReadRequest&>(*RequestById[requestId]);
    if (msg->Record.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        if (request.IsCompleted(requestId)) {
            Y_ABORT_UNLESS(msg->Record.HasReadResult());
            const auto& result = msg->Record.GetReadResult();
            Y_ABORT_UNLESS(result.HasPayloadId());
            Y_ABORT_UNLESS(result.GetPayloadId() == 0);
            TRope rope = msg->GetPayload(0);
            LOG_INFO(ctx, NKikimrServices::NBS_PARTITION,
                     "HandleDDiskReadResult finished with success, data: %s",
                     rope.ConvertToString().data());

            // End child span for this sub-request
            request.ChildSpanEndOk(requestId);

            auto response =
                std::make_unique<TEvService::TEvReadBlocksResponse>(MakeError(S_OK));
            auto& blocks = *response->Record.MutableBlocks();
            blocks.AddBuffers(rope.ConvertToString());

            ctx.Send(
                request.GetSender(),
                std::move(response),
                0, // flags
                request.GetCookie());

            // End top-level span
            request.Span.EndOk();
            RequestById.erase(requestId);
        }
    } else {
        LOG_ERROR(ctx, NKikimrServices::NBS_PARTITION,
            "HandleReadResult finished with error: %d, reason: %s",
                  msg->Record.GetStatus(),
                  msg->Record.GetErrorReason().data());

        auto response =
            std::make_unique<TEvService::TEvReadBlocksResponse>(MakeError(E_FAIL));

        ctx.Send(
            request.GetSender(),
            std::move(response),
            0, // flags
            request.GetCookie());

        request.ChildSpanEndError(requestId, "HandleReadResult failed");
        request.Span.EndError("HandleReadResult failed");

        RequestById.erase(requestId);
    }
}

}   // namespace NYdb::NBS::NStorage::NPartitionDirect

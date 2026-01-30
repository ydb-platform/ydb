#include "direct_block_group.h"

namespace NYdb::NBS::NStorage::NPartitionDirect {

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
        std::move(data));

    for (size_t i = 0; i < 3; i++) {
        ++RequestId;

        auto requestToDDisk = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
            PersistentBufferConnections[i].Credentials,
            NDDisk::TBlockSelector(
                0, // vChunkIndex
                request->GetStartOffset(),
                request->GetDataSize()),
            RequestId, // lsn
            NDDisk::TWriteInstruction(0));

        requestToDDisk->AddPayload(TRope(request->GetData()));

        ctx.Send(
            PersistentBufferConnections[i].GetServiceId(),
            requestToDDisk.release(),
            0, // flags
            RequestId);

        RequestById[RequestId] = request;
        request->OnWriteRequested(
            RequestId,
            i, // persistentBufferIndex
            RequestId // lsn
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
                    false // isErase
                );
            } else {
                RequestBlockFlush(
                    ctx,
                    request,
                    true // isErase
                );
            }

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
        
        RequestById.erase(requestId);
    }
}

void TDirectBlockGroup::RequestBlockFlush(
    const TActorContext& ctx,
    const TWriteRequest& request,
    bool isErase)
{
    const auto& blockMeta = BlocksMeta[request.StartIndex];
    for (size_t i = 0; i < 3; i++) {
        auto flushRequest = std::make_shared<TFlushRequest>(
            request.StartIndex,
            isErase,
            i, // persistentBufferIndex
            blockMeta.LsnByPersistentBufferIndex[i]
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

        ctx.Send(
            PersistentBufferConnections[i].GetServiceId(),
            requestToDDisk.release(),
            0, // flags
            RequestId);

        RequestById[RequestId] = flushRequest;
    }
}

void TDirectBlockGroup::HandlePersistentBufferFlushResult(
    const NDDisk::TEvFlushPersistentBufferResult::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(ctx, NKikimrServices::NBS_PARTITION,
                "HandlePersistentBufferFlushResult record is: %s",
                msg->Record.DebugString().data());

    if (msg->Record.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        auto requestId = ev->Cookie;
        auto& request = static_cast<TFlushRequest&>(*RequestById[requestId]);
        BlocksMeta[request.StartIndex].OnFlushCompleted(
            request.GetIsErase(),
            request.GetPersistentBufferIndex(),
            request.GetLsn());

        RequestById.erase(requestId);
    } else {
        LOG_ERROR(ctx, NKikimrServices::NBS_PARTITION,
                  "HandlePersistentBufferFlushResult finished with error: %d, reason: %s",
                  msg->Record.GetStatus(),
                  msg->Record.GetErrorReason().data());
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

    // Block is not writed
    if (!BlocksMeta[startIndex].IsWrited())
    {
        auto response = std::make_unique<TEvService::TEvReadBlocksResponse>(MakeError(S_OK));
        auto& blocks = *response->Record.MutableBlocks();
        blocks.AddBuffers(TString(4096, 0));

        ctx.Send(
            ev->Sender,
            std::move(response),
            0, // flags
            ev->Cookie);
        return;
    }

    auto request = std::make_shared<TReadRequest>(ev->Sender, ev->Cookie, startIndex, blocksCount);
    ++RequestId;

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

        ctx.Send(
            ddiskConnection.GetServiceId(),
            requestToPersistentBuffer.release(),
            0, // flags
            RequestId);
    } else {
        const auto& ddiskConnection = DDiskConnections[0];

        auto requestToDDisk = std::make_unique<NDDisk::TEvRead>(
            ddiskConnection.Credentials,
            NDDisk::TBlockSelector(
                0, // vChunkIndex
                request->GetStartOffset(),
                request->GetDataSize()),
            NDDisk::TReadInstruction(1));
        
        ctx.Send(
            ddiskConnection.GetServiceId(),
            requestToDDisk.release(),
            0, // flags
            RequestId);
    }

    RequestById[RequestId] = request;
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

            auto response =
                std::make_unique<TEvService::TEvReadBlocksResponse>(MakeError(S_OK));
            auto& blocks = *response->Record.MutableBlocks();
            blocks.AddBuffers(rope.ConvertToString());

            ctx.Send(
                request.GetSender(),
                std::move(response),
                0, // flags
                request.GetCookie());

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
        
        RequestById.erase(requestId);
    }
}

}   // namespace NYdb::NBS::NStorage::NPartitionDirect

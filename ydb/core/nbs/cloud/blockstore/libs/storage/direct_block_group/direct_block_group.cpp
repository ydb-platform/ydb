#include "direct_block_group.h"

namespace NYdb::NBS::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TDirectBlockGroup::TDirectBlockGroup(
    ui64 tabletId,
    ui32 generation,
    const TVector<TDDiskId>& ddisksIds,
    const TVector<TDDiskId>& persistentBufferDDiskIds)
{
    auto addDDiskConnections = [&](const TVector<TDDiskId>& ddisksIds,
                                   TVector<TDDiskConnection>& ddiskConnections,
                                   bool fromPersistentBuffer)
    {
        if (fromPersistentBuffer) {
            PersistentBufferBlocksLsn.resize(ddisksIds.size());
        }

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

    addDDiskConnections(ddisksIds, DDiskConnections, false);
    addDDiskConnections(persistentBufferDDiskIds, PersistentBufferConnections, true);
}

void TDirectBlockGroup::EstablishConnections(const TActorContext& ctx)
{
    LOG_DEBUG(ctx, NKikimrServices::NBS_PARTITION, "EstablishConnections");

    for (size_t i = 0; i < PersistentBufferConnections.size(); i++) {
        auto request =
            std::make_unique<NDDisk::TEvConnect>(PersistentBufferConnections[i].Credentials);

        ctx.Send(
            PersistentBufferConnections[i].GetServiceId(),
            request.release(),
            0,   // flags
            i);
    };
}

void TDirectBlockGroup::HandleDDiskConnectResult(
    const NDDisk::TEvConnectResult::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(ctx, NKikimrServices::NBS_PARTITION,
                "HandleDDiskConnectResult record is: %s", msg->Record.DebugString().data());

    Y_ABORT_UNLESS(ev->Cookie < PersistentBufferConnections.size());
    auto& ddiskConnection = PersistentBufferConnections[ev->Cookie];

    if (msg->Record.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        ddiskConnection.Credentials.DDiskInstanceGuid =
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

    if (msg->Record.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        auto requestId = ev->Cookie;
        auto& request = static_cast<TWriteRequest&>(*RequestById[requestId]);
        if (request.IsCompleted(requestId)) {
            for (const auto& meta : request.GetWritesMeta()) {
                if (PersistentBufferBlocksLsn[meta.Index][request.StartIndex] < meta.Lsn) {
                    PersistentBufferBlocksLsn[meta.Index][request.StartIndex] = meta.Lsn;
                }
            }

            auto response = std::make_unique<TEvService::TEvWriteBlocksResponse>();
            response->Record.MutableError()->CopyFrom(MakeError(S_OK));
            ctx.Send(request.Sender, std::move(response));

            RequestById.erase(requestId);
        }
    } else {
        LOG_ERROR(ctx, NKikimrServices::NBS_PARTITION,
                  "HandlePersistentBufferWriteResult finished with error: %d, reason: %s",
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

    if (!PersistentBufferBlocksLsn[0].contains(startIndex)) {
        auto response = std::make_unique<TEvService::TEvReadBlocksResponse>();
        response->Record.MutableError()->CopyFrom(MakeError(S_OK));
        auto& blocks = *response->Record.MutableBlocks();
        blocks.AddBuffers(TString(4096, 0));

        ctx.Send(ev->Sender, std::move(response));
        return;
    }

    auto request = std::make_shared<TReadRequest>(ev->Sender, startIndex, blocksCount);
    auto& ddiskConnection = PersistentBufferConnections[0];
    ++RequestId;

    auto requestToDDisk = std::make_unique<NDDisk::TEvReadPersistentBuffer>(
        ddiskConnection.Credentials,
        NDDisk::TBlockSelector(
            0, // vChunkIndex
            request->GetStartOffset(),
            request->GetDataSize()),
        PersistentBufferBlocksLsn[0][startIndex],
        NDDisk::TReadInstruction(1));

    ctx.Send(
        PersistentBufferConnections[0].GetServiceId(),
        requestToDDisk.release(),
        0, // flags
        RequestId);

    RequestById[RequestId] = request;
}

void TDirectBlockGroup::HandlePersistentBufferReadResult(
    const NDDisk::TEvReadPersistentBufferResult::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(ctx, NKikimrServices::NBS_PARTITION,
                "HandlePersistentBufferReadResult record is: %s",
                msg->Record.DebugString().data());

    if (msg->Record.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        auto requestId = ev->Cookie;
        auto& request = static_cast<TReadRequest&>(*RequestById[requestId]);
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
                std::make_unique<TEvService::TEvReadBlocksResponse>();
            response->Record.MutableError()->CopyFrom(MakeError(S_OK));
            auto& blocks = *response->Record.MutableBlocks();
            blocks.AddBuffers(rope.ConvertToString());

            ctx.Send(request.Sender, std::move(response));

            RequestById.erase(requestId);
        }
    } else {
        LOG_ERROR(ctx, NKikimrServices::NBS_PARTITION,
                  "HandlePersistentBufferReadResult finished with error: %d, reason: %s",
                  msg->Record.GetStatus(),
                  msg->Record.GetErrorReason().data());
    }
}

}   // namespace NYdb::NBS::NStorage::NPartitionDirect
